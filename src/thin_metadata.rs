use anyhow::{anyhow, Result};
use devicemapper::*;
use nom::IResult;
use roaring::bitmap::RoaringBitmap;
use std::collections::*;
use std::fs::OpenOptions;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use thinp::io_engine::*;
use thinp::pdata::btree;
use thinp::pdata::btree::*;
use thinp::pdata::btree_walker::*;
use thinp::thin::block_time::*;
use thinp::thin::device_detail::*;
use thinp::thin::superblock::*;

//---------------------------------

#[derive(Default)]
struct MappingCollector {
    provisioned: Mutex<RoaringBitmap>,
}

impl MappingCollector {
    fn provisioned(self) -> RoaringBitmap {
        self.provisioned.into_inner().unwrap()
    }
}

impl NodeVisitor<BlockTime> for MappingCollector {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _header: &NodeHeader,
        keys: &[u64],
        _values: &[BlockTime],
    ) -> btree::Result<()> {
        let mut bits = self.provisioned.lock().unwrap();
        for k in keys {
            assert!(*k <= u32::MAX as u64);
            bits.insert(*k as u32);
        }
        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct ThinInfo {
    pub thin_id: u32,
    pub data_block_size: u32,
    pub details: DeviceDetail,
    pub provisioned_blocks: RoaringBitmap,
}

fn read_info(metadata: &PathBuf, thin_id: u32) -> Result<ThinInfo> {
    let engine = Arc::new(SyncIoEngine::new_with(metadata.as_path(), 8, false, false)?);

    // Read metadata superblock
    let sb = read_superblock_snap(&*engine)?;

    // look up dev details, we don't actually have a lookup method, but the details
    // tree is small, so we slurp the whole lot.
    let details: DeviceDetail = {
        let mut path = vec![];
        let details: BTreeMap<u64, DeviceDetail> =
            btree_to_map(&mut path, engine.clone(), true, sb.details_root)?;
        let thin_id = thin_id as u64;
        if let Some(d) = details.get(&thin_id) {
            d.clone()
        } else {
            return Err(anyhow!("couldn't find thin device with that id"));
        }
    };

    // Build map of the dev mapping roots.  Again a btree lookup method
    // would be helpful.
    let mut path = vec![];
    let roots: BTreeMap<u64, u64> =
        btree_to_map(&mut path, engine.clone(), true, sb.mapping_root)?;

    // walk mapping tree
    let ignore_non_fatal = true;
    let walker = BTreeWalker::new(engine, ignore_non_fatal);
    let collector = MappingCollector::default();
    let mut path = vec![];
    let sub_tree = roots.get(&(thin_id as u64)).unwrap();
    walker.walk(&mut path, &collector, *sub_tree)?;
    let provisioned_blocks = collector.provisioned();

    Ok(ThinInfo {
        thin_id,
        data_block_size: sb.data_block_size,
        details,
        provisioned_blocks,
    })
}

//---------------------------------

fn collect_dm_devs() -> Result<BTreeMap<(u32, u32), DmNameBuf>> {
    let dm = DM::new()?;

    let mut devs_by_nr = BTreeMap::new();
    for (name, dev, _) in dm.list_devices()? {
        devs_by_nr.insert((dev.major, dev.minor), name);
    }

    Ok(devs_by_nr)
}

#[derive(Debug)]
struct ThinDetails {
    pool_major: u32,
    pool_minor: u32,
    id: u32,
}

fn parse_dev(input: &str) -> IResult<&str, (u32, u32)> {
    use nom::character::complete::*;

    let (input, major) = u32(input)?;
    let (input, _) = char(':')(input)?;
    let (input, minor) = u32(input)?;

    Ok((input, (major, minor)))
}

fn parse_thin_table(input: &str) -> IResult<&str, ThinDetails> {
    use nom::character::complete::*;

    let (input, (pool_major, pool_minor)) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, id) = u32(input)?;

    Ok((
        input,
        ThinDetails {
            pool_major,
            pool_minor,
            id,
        },
    ))
}

#[allow(dead_code)]
#[derive(Debug)]
struct PoolDetails {
    metadata_major: u32,
    metadata_minor: u32,
    data_block_size: u32,
}

fn parse_pool_table(input: &str) -> IResult<&str, PoolDetails> {
    use nom::character::complete::*;

    let (input, (metadata_major, metadata_minor)) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, (_data_major, _data_minor)) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, data_block_size) = u32(input)?;

    Ok((
        input,
        PoolDetails {
            metadata_major,
            metadata_minor,
            data_block_size,
        },
    ))
}

fn get_table(dm: &mut DM, dev: &DevId, expected_target_type: &str) -> Result<String> {
    let (_info, table) = dm.table_status(
        dev,
        DmOptions::default().set_flags(DmFlags::DM_STATUS_TABLE),
    )?;
    if table.len() != 1 {
        return Err(anyhow!(
            "thin table has too many rows (is it really a thin/pool device?)"
        ));
    }

    let (_offset, _len, target_type, args) = &table[0];
    if target_type != expected_target_type {
        // FIXME: better error message
        return Err(anyhow!("thin has incorrect target type"));
    }

    Ok(args.to_string())
}

pub fn read_thin_mappings<P: AsRef<Path>>(thin: P) -> Result<ThinInfo> {
    let dm_devs = collect_dm_devs()?;

    let thin = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        // .custom_flags(nix::fcntl::OFlag::O_EXCL as i32)
        .open(thin)?;

    let metadata = thin.metadata()?;

    if !metadata.file_type().is_block_device() {
        return Err(anyhow!("Thin is not a block device"));
    }

    // Get the major:minor of the device at the given path
    let rdev = metadata.rdev();
    let thin_major = (rdev >> 8) as u32;
    let thin_minor = (rdev & 0xff) as u32;
    let thin_name = dm_devs.get(&(thin_major, thin_minor)).unwrap().clone();
    let thin_id = DevId::Name(&thin_name);

    // Confirm this is a thin device
    let mut dm = DM::new()?;

    let thin_args = get_table(&mut dm, &thin_id, "thin")?;
    let (_, thin_details) =
        parse_thin_table(&thin_args).map_err(|_| anyhow!("couldn't parse thin table"))?;

    let pool_name = dm_devs
        .get(&(thin_details.pool_major, thin_details.pool_minor))
        .unwrap()
        .clone();
    let pool_id = DevId::Name(&pool_name);
    let pool_args = get_table(&mut dm, &pool_id, "thin-pool")?;
    let (_, pool_details) =
        parse_pool_table(&pool_args).map_err(|_| anyhow!("couldn't parse pool table"))?;

    // Find the metadata dev
    let metadata_name = dm_devs
        .get(&(pool_details.metadata_major, pool_details.metadata_minor))
        .unwrap()
        .clone();
    let metadata_name = std::str::from_utf8(metadata_name.as_bytes())?;
    let metadata_path: PathBuf = ["/dev", "mapper", &metadata_name]
        .iter()
        .collect();

    // Parse thin metadata
    dm.target_msg(&pool_id, None, "reserve_metadata_snap")?;
    let r = read_info(&metadata_path, thin_details.id)?;
    dm.target_msg(&pool_id, None, "release_metadata_snap")?;

    Ok(r)
}

//---------------------------------
