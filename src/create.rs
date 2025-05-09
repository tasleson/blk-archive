use anyhow::{anyhow, Result};
use clap::ArgMatches;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thinp::report::*;

use crate::config::*;
use crate::cuckoo_filter::*;
use crate::paths;
use crate::paths::*;
use crate::slab::builder::*;

//-----------------------------------------

fn create_sub_dir(root: &Path, sub: &str) -> Result<()> {
    let mut p = PathBuf::new();
    p.push(root);
    p.push(sub);
    fs::create_dir(p)?;
    Ok(())
}

fn write_config(
    root: &Path,
    block_size: usize,
    hash_cache_size_meg: usize,
    data_cache_size_meg: usize,
) -> Result<()> {
    let mut p = PathBuf::new();
    p.push(root);
    p.push("dm-archive.yaml");

    let mut output = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(p)?;

    let config = Config {
        block_size,
        splitter_alg: "RollingHashV0".to_string(),
        hash_cache_size_meg,
        data_cache_size_meg,
    };

    write!(output, "{}", &serde_yaml_ng::to_string(&config).unwrap())?;
    Ok(())
}

fn adjust_block_size(n: usize) -> usize {
    // We have a max block size of 1M currently
    let max_bs = 1024 * 1024;
    if n > max_bs {
        return max_bs;
    }

    let mut p = 1;
    while p < n {
        p *= 2;
    }

    p
}

fn numeric_option<T: std::str::FromStr>(matches: &ArgMatches, name: &str, dflt: T) -> Result<T> {
    match matches.try_get_one::<String>(name) {
        Ok(Some(s)) => s
            .parse::<T>()
            .map_err(|_| anyhow!("could not parse {} argument", name)),
        Ok(None) => Ok(dflt),
        Err(_) => Err(anyhow!("Error retrieving {} argument", name)),
    }
}

/*
fn numeric_option<T: std::str::FromStr>(matches: &ArgMatches, name: &str, dflt: T) -> Result<T> {
    matches
        .value_of(name)
        .map(|s| s.parse::<T>())
        .unwrap_or(Ok(dflt))
        .map_err(|_| anyhow!(format!("could not parse {} argument", name)))
}
*/

pub fn run(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap());
    let data_compression = matches.get_one::<String>("DATA_COMPRESSION").unwrap() == "y";

    let mut block_size = numeric_option::<usize>(matches, "BLOCK_SIZE", 4096)?;
    let new_block_size = adjust_block_size(block_size);
    if new_block_size != block_size {
        report.info(&format!("adjusting block size to {}", new_block_size));
        block_size = new_block_size;
    }
    let hash_cache_size_meg = numeric_option::<usize>(matches, "HASH_CACHE_SIZE_MEG", 1024)?;
    let data_cache_size_meg = numeric_option::<usize>(matches, "DATA_CACHE_SIZE_MEG", 1024)?;

    fs::create_dir(dir)?;
    write_config(dir, block_size, hash_cache_size_meg, data_cache_size_meg)?;
    create_sub_dir(dir, "data")?;
    create_sub_dir(dir, "streams")?;
    create_sub_dir(dir, "indexes")?;

    std::env::set_current_dir(dir)?;

    // Create empty data and hash slab files
    let mut data_file = SlabFileBuilder::create(data_path())
        .queue_depth(1)
        .compressed(data_compression)
        .build()?;
    data_file.close()?;

    let mut hashes_file = SlabFileBuilder::create(hashes_path())
        .queue_depth(1)
        .compressed(false)
        .build()?;
    hashes_file.close()?;

    // Write empty index
    let index = CuckooFilter::with_capacity(1 << 10);
    index.write(paths::index_path())?;

    Ok(())
}

//-----------------------------------------
