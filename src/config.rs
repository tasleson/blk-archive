use anyhow::{Context, Result};
use chrono::prelude::*;
use serde_derive::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

//-----------------------------------------

#[derive(Deserialize, Serialize)]
pub struct Config {
    pub block_size: usize,
    pub splitter_alg: String,
}

pub fn read_config<P: AsRef<Path>>(root: P) -> Result<Config> {
    let mut p = PathBuf::new();
    p.push(root);
    p.push("dm-archive.toml");
    let input = fs::read_to_string(p).context("couldn't read config file")?;
    let config: Config = toml::from_str(&input).context("couldn't parse config file")?;
    Ok(config)
}

//-----------------------------------------

#[derive(Deserialize, Serialize)]
pub struct StreamConfig {
    pub name: Option<String>,
    pub origin_path: String,
    pub pack_time: toml::value::Datetime,
    pub len: u64,
    pub compression: f64,
}

fn stream_cfg_path(stream_id: &str) -> PathBuf {
    ["streams", stream_id, "config.toml"].iter().collect()
}

pub fn read_stream_config(stream_id: &str) -> Result<StreamConfig> {
    let p = stream_cfg_path(stream_id);
    let input = fs::read_to_string(p).context("couldn't read stream config")?;
    let config: StreamConfig =
        toml::from_str(&input).context("couldn't parse stream config file")?;
    Ok(config)
}

pub fn write_stream_config(stream_id: &str, cfg: &StreamConfig) -> Result<()> {
    let p = stream_cfg_path(stream_id);
    let mut output = fs::OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .open(p)?;
    let toml = toml::to_string(cfg).unwrap();
    output.write_all(toml.as_bytes())?;
    Ok(())
}

pub fn now() -> toml::value::Datetime {
    let dt = Utc::now();
    let str = dt.to_rfc3339();
    str.parse::<toml::value::Datetime>().unwrap()
}

//-----------------------------------------
