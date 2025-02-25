use std::path::PathBuf;

use anyhow::Result;

mod common;

use common::fixture::*;
use common::test_dir::*;

//-----------------------------------------

#[test]
fn pack_one_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td)?;

    let file_size = 16 * 1024 * 1024;
    let seed = 1;
    let input = create_input_file(&mut td, file_size, seed)?;
    let stream = archive.pack(&input)?;
    archive.verify(&input, &stream)
}

#[test]
fn pack_same_file_multiple_times() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td)?;

    let file_size = 16 * 1024 * 1024;
    let seed = 1;
    let input = create_input_file(&mut td, file_size, seed)?;
    let streams = (0..3)
        .map(|_| archive.pack(&input))
        .collect::<Result<Vec<_>>>()?;
    streams.iter().try_for_each(|s| archive.verify(&input, s))
}

#[test]
fn pack_test_bd_zero() -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td)?;
    let input = PathBuf::from("/dev/ublkb1");

    let (data_start, hashes_start) = archive.data_hash_sizes()?;

    let response = archive.pack_resp(&input)?;

    let (data_end, hashes_end) = archive.data_hash_sizes()?;
    assert_eq!(data_start, data_end);
    assert_eq!(hashes_start, hashes_end);
    assert!(response.stats.data_written == 0);
    assert!(response.stats.hashes_written == 0);
    assert_eq!(response.stats.fill_size, response.stats.mapped_size);
    archive.verify(&input, &response.stream_id)
}

fn pack_test(input_path: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let archive = create_archive(&mut td)?;
    let input = PathBuf::from(input_path);

    let (data_start, hashes_start) = archive.data_hash_sizes()?;

    let response = archive.pack_resp(&input)?;

    let (data_end, hashes_end) = archive.data_hash_sizes()?;

    let data_written = data_end - data_start;
    let hashes_written = hashes_end - hashes_start;

    assert_eq!(data_written, response.stats.data_written);
    assert_eq!(hashes_written, response.stats.hashes_written);
    assert!(response.stats.fill_size == 0);

    archive.verify(&input, &response.stream_id)
}

#[test]
fn pack_test_bd_dedupe() -> Result<()> {
    pack_test("/dev/ublkb2")
}

#[test]
fn pack_test_bd_random() -> Result<()> {
    pack_test("/dev/ublkb3")
}

//-----------------------------------------
