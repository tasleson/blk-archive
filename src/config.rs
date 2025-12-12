use crate::hash_dispatch;
use anyhow::{anyhow, Context, Result};
use chrono::prelude::*;
use clap::ArgMatches;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

use crate::hash_algorithm::HashAlgorithmStored;

//-----------------------------------------

#[derive(Deserialize, Serialize)]
pub struct Config {
    pub block_size: usize,
    pub splitter_alg: String,
    pub hash_cache_size_meg: usize,
    pub data_cache_size_meg: usize,
    #[serde(default)]
    pub block_hash_algorithm: HashAlgorithmStored,
    #[serde(default)]
    pub file_hash_algorithm: HashAlgorithmStored,
}

fn numeric_override<T: std::str::FromStr>(matches: &ArgMatches, name: &str) -> Result<Option<T>> {
    match matches.try_get_one::<String>(name) {
        Ok(Some(s)) => s
            .parse::<T>()
            .map_err(|_| anyhow!("could not parse {} argument", name))
            .map(|n| Some(n)),
        Ok(None) => Ok(None),
        Err(_) => Err(anyhow!("Error retrieving {} argument", name)),
    }
}

pub fn read_config<P: AsRef<Path>>(root: P, overrides: &ArgMatches) -> Result<Config> {
    let mut p = PathBuf::new();
    p.push(root);
    p.push("dm-archive.yaml");
    let input = fs::read_to_string(p).context("couldn't read config file")?;
    let mut config: Config =
        serde_yaml_ng::from_str(&input).context("couldn't parse config file")?;

    if let Some(data_cache_meg) = numeric_override::<usize>(overrides, "DATA_CACHE_SIZE_MEG")? {
        config.data_cache_size_meg = data_cache_meg;
    }

    let (blk_algr, blk_size) = config.block_hash_algorithm.convert();
    let (stream_algr, _) = config.file_hash_algorithm.convert();

    // We need to ensure whe have initialized these global function pointers for hashing
    hash_dispatch::init_hash_functions(blk_size, blk_algr, stream_algr);

    Ok(config)
}

//-----------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct StreamConfig {
    pub name: Option<String>,
    pub source_path: String,
    pub pack_time: String,
    pub size: u64,
    pub mapped_size: u64,
    pub packed_size: u64,
    pub thin_id: Option<u32>,
    pub source_sig: Option<String>,

    // Stream mapping location in binary format
    #[serde(default)]
    pub first_mapping_slab: u32,
    #[serde(default)]
    pub num_mapping_slabs: u32,
}

pub fn now() -> String {
    let dt = Utc::now();
    dt.to_rfc3339()
}

pub fn to_date_time(t: &str) -> chrono::DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339(t).unwrap()
}

//-----------------------------------------

#[cfg(test)]
mod config_tests {

    use super::*;

    #[test]
    fn test_simple() {
        let config = StreamConfig {
            name: Some(String::from("test_file")),
            source_path: String::from("/home/some_user/test_file"),
            pack_time: String::from("2023-11-14T22:06:02.101221624+00:00"),
            size: u64::MAX,
            mapped_size: u64::MAX,
            packed_size: u64::MAX,
            thin_id: None,
            source_sig: None,
            first_mapping_slab: 0,
            num_mapping_slabs: 0,
        };

        let ser = serde_yaml_ng::to_string(&config).unwrap();
        let des_config: StreamConfig = serde_yaml_ng::from_str(&ser).unwrap();
        assert!(config == des_config);
    }
}
