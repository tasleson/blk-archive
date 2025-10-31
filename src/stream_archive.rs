//! Binary stream archive management
//!
//! This module provides a high-level interface for managing binary stream metadata
//! and mappings. It encapsulates the slab file implementation details.

use anyhow::{anyhow, Context, Result};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::config::StreamConfig;
use crate::paths::{stream_mappings_path, streams_metadata_path};
use crate::slab::builder::SlabFileBuilder;
use crate::slab::{SlabFile, StreamData};
use crate::stream_metadata::{deserialize_stream_config, serialize_stream_config};

/// Stream archive manager
///
/// Manages the binary stream metadata and mappings files, providing
/// a clean interface for reading and writing stream information.
pub struct StreamArchive<'a> {
    metadata_file: Arc<Mutex<SlabFile<'a>>>,
    mappings_file: Arc<Mutex<SlabFile<'a>>>,
}

pub struct MemorySlab {
    slab: Arc<Vec<u8>>,
}

impl MemorySlab {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            slab: Arc::new(data),
        }
    }
}

impl StreamData for MemorySlab {
    fn read(&mut self, slab: u32) -> Result<Arc<Vec<u8>>> {
        if slab == 0 {
            Ok(self.slab.clone())
        } else {
            Err(anyhow!("invalid slab index (only 0 exists)"))
        }
    }

    fn get_nr_slabs(&self) -> usize {
        1
    }
}

impl<'a> StreamArchive<'a> {
    /// Open stream archive for reading
    pub fn open_read(archive_dir: &Path) -> Result<StreamArchive<'static>> {
        let metadata_file = Arc::new(Mutex::new(
            SlabFileBuilder::open(streams_metadata_path(archive_dir))
                .build()
                .context("couldn't open stream metadata file")?,
        ));

        let mappings_file = Arc::new(Mutex::new(
            SlabFileBuilder::open(stream_mappings_path(archive_dir))
                .build()
                .context("couldn't open stream mappings file")?,
        ));

        Ok(StreamArchive {
            metadata_file,
            mappings_file,
        })
    }

    /// Open stream archive for writing
    pub fn open_write(archive_dir: &Path, queue_depth: usize) -> Result<StreamArchive<'static>> {
        let metadata_file = Arc::new(Mutex::new(
            SlabFileBuilder::open(streams_metadata_path(archive_dir))
                .write(true)
                .queue_depth(queue_depth)
                .build()
                .context("couldn't open stream metadata file for writing")?,
        ));

        let mappings_file = Arc::new(Mutex::new(
            SlabFileBuilder::open(stream_mappings_path(archive_dir))
                .write(true)
                .queue_depth(queue_depth)
                .build()
                .context("couldn't open stream mappings file for writing")?,
        ));

        Ok(StreamArchive {
            metadata_file,
            mappings_file,
        })
    }

    /// Read stream configuration by stream ID
    pub fn read_config(&self, stream_id: u32) -> Result<StreamConfig> {
        let mut metadata = self.metadata_file.lock().unwrap();
        let data = metadata
            .read(stream_id)
            .with_context(|| format!("Failed to read metadata for stream {}", stream_id))?;
        deserialize_stream_config(&data)
            .with_context(|| format!("Failed to deserialize metadata for stream {}", stream_id))
    }

    /// Write stream configuration (allocates next stream ID)
    ///
    /// Returns the allocated stream ID
    pub fn write_config(&self, config: &StreamConfig) -> Result<u32> {
        let stream_id = {
            let metadata = self.metadata_file.lock().unwrap();
            metadata.get_nr_slabs() as u32
        };

        let metadata_bytes =
            serialize_stream_config(config).context("Failed to serialize stream config")?;

        let mut metadata = self.metadata_file.lock().unwrap();
        metadata
            .write_slab(&metadata_bytes)
            .context("Failed to write stream metadata slab")?;
        metadata
            .sync_all()
            .context("Failed to sync stream metadata")?;

        Ok(stream_id)
    }

    /// Get the number of streams in the archive
    pub fn stream_count(&self) -> usize {
        let metadata = self.metadata_file.lock().unwrap();
        metadata.get_nr_slabs()
    }

    /// Get a reference to the metadata file (for advanced use)
    pub fn metadata_file(&self) -> Arc<Mutex<SlabFile<'a>>> {
        self.metadata_file.clone()
    }

    /// Get a reference to the mappings file (for advanced use)
    pub fn mappings_file(&self) -> Arc<Mutex<SlabFile<'a>>> {
        self.mappings_file.clone()
    }

    /// Open stream mappings for a specific stream, returns the bytes
    /// TODO: Make this work for unlimited stream sizes
    pub fn get_stream(&self, stream_id: u32) -> Result<MemorySlab> {
        let config = self.read_config(stream_id)?;

        // Read all mapping slabs for this stream
        let mut combined_data = Vec::new();
        {
            let mut mappings = self.mappings_file.lock().unwrap();
            for slab_offset in 0..config.num_mapping_slabs {
                let slab_id = config.first_mapping_slab + slab_offset;
                let slab_data = mappings.read(slab_id).with_context(|| {
                    format!(
                        "Failed to read mapping slab {} for stream {}",
                        slab_id, stream_id
                    )
                })?;
                combined_data.extend_from_slice(&slab_data);
            }
        }

        Ok(MemorySlab::new(combined_data))
    }

    /// Close the stream archive files
    ///
    /// This ensures all offset files are written correctly
    pub fn close(&mut self) -> Result<()> {
        {
            let mut metadata = self.metadata_file.lock().unwrap();
            metadata
                .close()
                .context("Failed to close stream metadata file")?;
        }
        {
            let mut mappings = self.mappings_file.lock().unwrap();
            mappings
                .close()
                .context("Failed to close stream mappings file")?;
        }
        Ok(())
    }
}

pub fn retrieve_stream_slab(archive_dir: &Path, stream_id: u32) -> Result<impl StreamData> {
    let stream_archive = StreamArchive::open_read(archive_dir)?;
    stream_archive.get_stream(stream_id)
}

/// Helper function to read stream configuration by stream ID string
///
/// Supports both hex (16-char) and decimal formats
pub fn read_stream_config(archive_dir: &Path, stream: &str) -> Result<StreamConfig> {
    let stream_id = parse_stream_id(stream)?;
    let archive = StreamArchive::open_read(archive_dir)?;
    archive.read_config(stream_id)
}

/// Parse stream ID from string (supports hex and decimal)
pub fn parse_stream_id(stream: &str) -> Result<u32> {
    if stream.len() == 16 && stream.chars().all(|c| c.is_ascii_hexdigit()) {
        u32::from_str_radix(stream, 16)
            .with_context(|| format!("Invalid hex stream ID: {}", stream))
    } else {
        stream
            .parse::<u32>()
            .with_context(|| format!("Invalid stream ID: {}", stream))
    }
}
