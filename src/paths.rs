use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

use rand::prelude::*;
use rand::Rng;
use rand_chacha::ChaCha20Rng;

use crate::recovery::SyncParentExt;
//------------------------------

fn make<P, I, S>(base: P, parts: I) -> PathBuf
where
    P: AsRef<Path>,
    I: IntoIterator<Item = S>,
    S: AsRef<Path>,
{
    parts
        .into_iter()
        .fold(base.as_ref().to_path_buf(), |p, s| p.join(s))
}

pub fn index_path<P: AsRef<std::path::Path>>(base: P) -> PathBuf {
    make(&base, ["indexes", "seen"])
}

pub fn data_path<P: AsRef<std::path::Path>>(base: P) -> PathBuf {
    make(&base, ["data", "data"])
}

pub fn hashes_path<P: AsRef<std::path::Path>>(base: P) -> PathBuf {
    make(&base, ["data", "hashes"])
}

pub fn stream_path_dir<P: AsRef<std::path::Path>>(base: P, stream: &str) -> PathBuf {
    stream_name_to_path(base.as_ref(), stream)
}

pub fn stream_path_file<P: AsRef<std::path::Path>>(base: P, stream: &str) -> PathBuf {
    stream_name_to_path(base.as_ref(), stream).join("stream")
}

pub fn stream_config<P: AsRef<std::path::Path>>(base: P, stream: &str) -> PathBuf {
    stream_name_to_path(base.as_ref(), stream).join("config.yaml")
}

/// Clean up all temporary stream directories
///
/// This removes any `.tmp_*` directories in the streams directory.
/// These are uncommitted pack operations that were interrupted.
/// This should be called by any command that accesses the archive.
pub fn cleanup_temp_streams(streams_dir: &Path) -> Result<()> {
    if !streams_dir.exists() {
        return Ok(());
    }

    let paths = fs::read_dir(streams_dir)?;

    for entry in paths {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_str().unwrap_or("");

        if name.starts_with(".tmp_") {
            if let Err(e) = fs::remove_dir_all(entry.path()) {
                eprintln!("Warning: Failed to remove temp directory {}: {}", name, e);
            }
        }
    }
    Ok(())
}

/// How deep the shard tree is and how many hex chars per level.
/// With LEVELS=2 and WIDTH=2, layout is streams/aa/bb/<name>.
const STREAM_SHARD_LEVELS: usize = 2;
const STREAM_SHARD_WIDTH: usize = 2;

/// Given a 16-char hex stream name, return shard path segments.
/// e.g. name="a1b2c3d4e5f60789" -> ["a1","b2"] for LEVELS=2, WIDTH=2
fn shard_segments(name: &str) -> Vec<&str> {
    let mut segs = Vec::with_capacity(STREAM_SHARD_LEVELS);
    let mut i = 0usize;
    for _ in 0..STREAM_SHARD_LEVELS {
        let end = i + STREAM_SHARD_WIDTH;
        segs.push(&name[i..end]);
        i = end;
    }
    segs
}

fn stream_name_to_path(archive_dir: &Path, name: &str) -> PathBuf {
    let mut parent = archive_dir.join("streams");
    for seg in shard_segments(name) {
        parent.push(seg);
    }
    parent.join(name)
}

/// Computes {parent_dir, temp_dir, final_dir} for a stream name.
/// Parent will be `<archive>/streams/<seg1>/<seg2>/...`
fn stream_dirs_for_name(archive_dir: &Path, name: &str) -> (PathBuf, PathBuf, PathBuf) {
    let mut parent = archive_dir.join("streams");
    for seg in shard_segments(name) {
        parent.push(seg);
    }

    let temp = parent.join(format!(".tmp_{name}"));
    let final_ = parent.join(name);
    (parent, temp, final_)
}

/// New: create a sharded path for a fresh stream.
/// Returns (name, temp_path, final_path). Caller can mkdir temp_path, populate, then rename.
pub fn new_stream_path_sharded(archive_dir: &Path) -> Result<(String, PathBuf, PathBuf)> {
    let mut rng = ChaCha20Rng::from_entropy();
    // choose a random number; hex-encode to 16 chars
    loop {
        let n: u64 = rng.gen();
        let name = format!("{:016x}", n);

        // build sharded parent and candidate paths
        let (parent, temp_path, final_path) = stream_dirs_for_name(archive_dir, &name);

        // create parent directories upfront (safe & idempotent)
        fs::create_dir_all(&parent)
            .with_context(|| format!("creating stream parent dir: {}", parent.display()))?;

        // Ensure no collision
        if temp_path.exists() || final_path.exists() {
            continue;
        }
        return Ok((name, temp_path, final_path));
    }
}

pub fn finalize_stream_dir(temp_path: &Path, final_path: &Path) -> Result<()> {
    fs::rename(temp_path, final_path).with_context(|| {
        format!(
            "renaming {} -> {}",
            temp_path.display(),
            final_path.display()
        )
    })?;
    // fsync parent for stronger durability
    final_path.sync_parent()?;
    Ok(())
}

/// Lazy iterator over stream directories in an archive.
/// A "stream directory" is any directory containing a child named "stream".
pub struct StreamIter {
    stack: Vec<fs::ReadDir>,
}

impl StreamIter {
    /// Create a lazy iterator that yields every stream directory under `<archive>/streams`.
    pub fn new(archive_dir: &Path) -> std::io::Result<Self> {
        let root = archive_dir.join("streams");
        let mut stack = Vec::new();
        if let Ok(rd) = fs::read_dir(root) {
            stack.push(rd);
        }
        Ok(Self { stack })
    }
}

impl Iterator for StreamIter {
    type Item = PathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // No more directories to traverse
            let top = self.stack.last_mut()?;

            match top.next() {
                Some(Ok(entry)) => {
                    // Use file_type() to avoid an extra metadata syscall where possible
                    let Ok(ft) = entry.file_type() else { continue };
                    if !ft.is_dir() {
                        continue;
                    }

                    let path = entry.path();

                    // Skip temp dirs like ".tmp_*"
                    if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                        if name.starts_with(".tmp_") {
                            continue;
                        }
                    }

                    // If the directory contains a child named "stream" => it's a leaf stream dir
                    let marker = path.join("stream");
                    if marker.exists() {
                        // Yield the stream directory itself; do NOT descend into it
                        return Some(path);
                    }

                    // Otherwise, descend lazily into this directory
                    if let Ok(rd) = fs::read_dir(&path) {
                        self.stack.push(rd);
                    }
                    // and continue the loop to fetch from the new top
                }
                Some(Err(_)) => {
                    // On error, skip this entry
                    continue;
                }
                None => {
                    // Finished this directory; pop and continue with previous level
                    self.stack.pop();
                }
            }
        }
    }
}

/// Convenience function
pub fn stream_iter_lazy(archive_dir: &Path) -> std::io::Result<StreamIter> {
    StreamIter::new(archive_dir)
}

//------------------------------
