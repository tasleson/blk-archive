use anyhow::{anyhow, Result};
use clap::ArgMatches;
use io::Read;
use nom::{bytes::complete::*, multi::*, number::complete::*, IResult};
use std::collections::BTreeMap;
use std::env;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thinp::report::*;

use crate::hash::*;
use crate::slab::*;
use crate::stream;
use crate::stream::*;

//-----------------------------------------

#[allow(dead_code)]
struct SlabInfo {
    offsets: Vec<(Hash256, u32, u32)>,
}

#[allow(dead_code)]
struct Verifier {
    data_file: SlabFile,
    hashes_file: SlabFile,
    stream_file: SlabFile,

    slabs: BTreeMap<u32, Arc<SlabInfo>>,
    total_verified: u64,
}

impl Verifier {
    // Assumes current directory is the root of the archive.
    fn new(stream: &str) -> Result<Self> {
        let data_path: PathBuf = ["data", "data"].iter().collect();
        let data_file = SlabFile::open_for_read(data_path)?;

        let hashes_path: PathBuf = ["data", "hashes"].iter().collect();
        let hashes_file = SlabFile::open_for_read(hashes_path)?;

        let stream_path: PathBuf = ["streams", stream, "stream"].iter().collect();
        let stream_file = SlabFile::open_for_read(stream_path)?;

        Ok(Self {
            data_file,
            hashes_file,
            stream_file,
            slabs: BTreeMap::new(),
            total_verified: 0,
        })
    }

    // Returns the len of the data entry
    fn parse_hash_entry(input: &[u8]) -> IResult<&[u8], (Hash256, u32)> {
        let (input, hash) = take(std::mem::size_of::<Hash256>())(input)?;
        let hash = Hash256::clone_from_slice(hash);
        let (input, len) = le_u32(input)?;
        Ok((input, (hash, len)))
    }

    fn parse_slab_info(input: &[u8]) -> IResult<&[u8], Vec<(Hash256, u32, u32)>> {
        let (input, lens) = many0(Self::parse_hash_entry)(input)?;

        let mut r = Vec::with_capacity(lens.len());
        let mut total = 0;
        for (h, l) in lens {
            r.push((h, total, l));
            total += l;
        }

        Ok((input, r))
    }

    fn read_info(&mut self, slab: u32) -> Result<Arc<SlabInfo>> {
        // Read the hashes slab
        let hashes = self.hashes_file.read(slab)?;

        // Find location and length of data
        let (_, offsets) =
            Self::parse_slab_info(&hashes).map_err(|_| anyhow!("unable to parse slab hashes"))?;

        Ok(Arc::new(SlabInfo { offsets }))
    }

    fn get_info(&mut self, slab: u32) -> Result<Arc<SlabInfo>> {
        if let Some(info) = self.slabs.get(&slab) {
            Ok(info.clone())
        } else {
            let info = self.read_info(slab)?;
            self.slabs.insert(slab, info.clone());
            Ok(info)
        }
    }

    fn verify_entry<R: Read>(&mut self, e: &MapEntry, r: &mut R) -> Result<u64> {
        use MapEntry::*;

        let len = match e {
            Fill { byte, len } => {
                // FIXME: don't keep initialising this buffer,
                // keep a suitable one around instead
                // FIXME: put in a loop if len is too long
                let expected: Vec<u8> = vec![*byte; *len as usize];
                let mut actual = vec![*byte; *len as usize];
                r.read_exact(&mut actual)?;
                assert_eq!(&actual, &expected);
                self.total_verified += *len as u64;
                *len as u64
            }
            Unmapped { len } => {
                todo!();
                *len as u64
            }
            Data {
                slab,
                offset,
                nr_entries,
            } => {
                let mut total_len = 0;
                for entry in 0..*nr_entries {
                    let info = self.get_info(*slab)?;
                    let data = self.data_file.read(*slab)?;
                    let (expected_hash, offset, len) =
                        info.offsets[*offset as usize + entry as usize];
                    let data_begin = offset as usize;
                    let data_end = data_begin + len as usize;
                    assert!(data_end <= data.len());

                    // FIXME: make this paranioa check optional
                    // Verify hash
                    let actual_hash = hash_256(&data[data_begin..data_end]);
                    assert_eq!(actual_hash, expected_hash);

                    // Verify data
                    let mut actual = vec![0; data_end - data_begin];
                    r.read_exact(&mut actual)?;
                    if actual != &data[data_begin..data_end] {
                        eprintln!("mismatched data at offset {}", self.total_verified);
                        assert!(false);
                    }

                    self.total_verified += actual.len() as u64;
                    total_len += len as u64;
                }
                total_len
            }
        };

        Ok(len)
    }

    pub fn verify<R: Read>(&mut self, report: &Arc<Report>, r: &mut R) -> Result<()> {
        report.progress(0);

        let nr_slabs = self.stream_file.get_nr_slabs();
        let mut current_pos = 0;

        for s in 0..nr_slabs {
            let stream_data = self.stream_file.read(s as u32)?;
            let (entries, positions) = stream::unpack(&stream_data[..])?;
            let nr_entries = entries.len();
            let mut pos_iter = positions.iter();
            let mut next_pos = pos_iter.next();

            for (i, e) in entries.iter().enumerate() {
                if let Some(pos) = next_pos {
                    if pos.1 == i {
                        if pos.0 != current_pos {
                            eprintln!("pos didn't match: expected {} != actual {}", pos.0, current_pos);
                            assert!(false);
                        }
                        next_pos = pos_iter.next();
                    }
                }

                let entry_len = self.verify_entry(&e, r)?;

		// FIXME: shouldn't inc before checking pos
                current_pos += entry_len;

                if i % 10240 == 0 {
                    // update progress bar
                    let entry_fraction = i as f64 / nr_entries as f64;
                    let slab_fraction = s as f64 / nr_slabs as f64;
                    let percent =
                        ((slab_fraction + (entry_fraction / nr_slabs as f64)) * 100.0) as u8;
                    report.progress(percent as u8);
                }
            }
        }

        Ok(())
    }
}

//-----------------------------------------

pub fn run(matches: &ArgMatches) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let stream = matches.value_of("STREAM").unwrap();
    let report = std::sync::Arc::new(mk_progress_bar_report());

    let mut input = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(input_file)?;

    env::set_current_dir(&archive_dir)?;

    report.set_title(&format!(
        "Verifying {} and {} match ...",
        input_file.display(),
        &stream
    ));
    let mut v = Verifier::new(&stream)?;
    v.verify(&report, &mut input)
}

//-----------------------------------------
