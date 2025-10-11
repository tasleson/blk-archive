use anyhow::{anyhow, Context, Result};
use chrono::prelude::*;
use clap::ArgMatches;
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use serde_json::json;
use serde_json::to_string_pretty;
use size_display::Size;
use std::boxed::Box;
use std::env;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::archive::*;
use crate::chunkers::*;
use crate::config;
use crate::content_sensitive_splitter::*;
use crate::hash::*;
use crate::iovec::*;
use crate::output::Output;
use crate::paths::*;
use crate::recovery;
use crate::run_iter::*;
use crate::slab::builder::*;
use crate::slab::*;
use crate::splitter::*;
use crate::stream::*;
use crate::stream_builders::*;
use crate::thin_metadata::*;

//-----------------------------------------

fn iov_len_(iov: &IoVec) -> u64 {
    let mut len = 0;
    for v in iov {
        len += v.len() as u64;
    }

    len
}

fn first_b_(iov: &IoVec) -> Option<u8> {
    if let Some(v) = iov.iter().find(|v| !v.is_empty()) {
        return Some(v[0]);
    }

    None
}

fn all_same(iov: &IoVec) -> Option<u8> {
    if let Some(first_b) = first_b_(iov) {
        for v in iov.iter() {
            for b in *v {
                if *b != first_b {
                    return None;
                }
            }
        }
        Some(first_b)
    } else {
        None
    }
}

//-----------------------------------------
#[derive(serde::Serialize, Default)]
struct DedupStats {
    data_written: u64,
    mapped_size: u64,
    fill_size: u64,
}

struct DedupHandler<'a, S: SlabStorage = SlabFile<'static>> {
    nr_chunks: usize,

    stream_file: SlabFile<'a>,
    stream_buf: Vec<u8>,

    mapping_builder: Arc<Mutex<dyn Builder>>,

    stats: DedupStats,
    archive: Data<'a, S>,
}

impl<'a, S: SlabStorage> DedupHandler<'a, S> {
    fn new(
        stream_file: SlabFile<'a>,
        mapping_builder: Arc<Mutex<dyn Builder>>,
        archive: Data<'a, S>,
    ) -> Result<Self> {
        let stats = DedupStats::default();

        Ok(Self {
            nr_chunks: 0,
            stream_file,
            stream_buf: Vec::new(),
            mapping_builder,
            stats,
            archive,
        })
    }

    fn maybe_complete_stream(&mut self) -> Result<()> {
        complete_slab(
            &mut self.stream_file,
            &mut self.stream_buf,
            SLAB_SIZE_TARGET,
        )?;
        Ok(())
    }

    fn add_stream_entry(&mut self, e: &MapEntry, len: u64) -> Result<()> {
        let mut builder = self.mapping_builder.lock().unwrap();
        builder.next(e, len, &mut self.stream_buf)
    }

    fn handle_gap(&mut self, len: u64) -> Result<()> {
        self.add_stream_entry(&MapEntry::Unmapped { len }, len)?;
        self.maybe_complete_stream()?;

        Ok(())
    }

    fn handle_ref(&mut self, len: u64) -> Result<()> {
        self.add_stream_entry(&MapEntry::Ref { len }, len)?;
        self.maybe_complete_stream()?;

        Ok(())
    }

    // TODO: Is there a better way to handle this and what are the ramifications with
    // client server with multiple clients and one server?
    fn ensure_extra_capacity(&mut self, blocks: usize) -> Result<()> {
        self.archive.ensure_extra_capacity(blocks)
    }
}

impl<'a, S: SlabStorage> IoVecHandler for DedupHandler<'a, S> {
    fn handle_data(&mut self, iov: &IoVec) -> Result<()> {
        self.nr_chunks += 1;
        let len = iov_len_(iov);
        self.stats.mapped_size += len;
        assert!(len != 0);

        if let Some(first_byte) = all_same(iov) {
            self.stats.fill_size += len;
            self.add_stream_entry(
                &MapEntry::Fill {
                    byte: first_byte,
                    len,
                },
                len,
            )?;
            self.maybe_complete_stream()?;
        } else {
            let h = hash_256_iov(iov);
            // Note: add_data_entry returns existing entry if present, else returns newly inserted
            // entry.
            let (entry_location, data_written) = self.archive.data_add(h, iov, len)?;
            let me = MapEntry::Data {
                slab: entry_location.0,
                offset: entry_location.1,
                nr_entries: 1,
            };
            self.stats.data_written += data_written;
            self.add_stream_entry(&me, len)?;
            self.maybe_complete_stream()?;
        }

        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        let mut builder = self.mapping_builder.lock().unwrap();
        builder.complete(&mut self.stream_buf)?;
        drop(builder);

        complete_slab(&mut self.stream_file, &mut self.stream_buf, 0)?;
        self.stream_file.close()?;

        Ok(())
    }
}

//-----------------------------------------

// Assumes we've chdir'd to the archive
// Returns (stream_id, temp_path, final_path)
fn new_stream_path_(rng: &mut ChaCha20Rng) -> Result<Option<(String, PathBuf, PathBuf)>> {
    // choose a random number
    let n: u64 = rng.gen();

    // turn this into a path
    let name = format!("{:>016x}", n);
    let temp_name = format!(".tmp_{}", name);
    let temp_path: PathBuf = ["streams", &temp_name].iter().collect();
    let final_path: PathBuf = ["streams", &name].iter().collect();

    // Check both temp and final paths don't exist
    if temp_path.exists() || final_path.exists() {
        Ok(None)
    } else {
        Ok(Some((name, temp_path, final_path)))
    }
}

fn new_stream_path() -> Result<(String, PathBuf, PathBuf)> {
    let mut rng = ChaCha20Rng::from_entropy();
    loop {
        if let Some(r) = new_stream_path_(&mut rng)? {
            return Ok(r);
        }
    }

    // Can't get here
}

struct Packer {
    output: Arc<Output>,
    input_path: PathBuf,
    stream_name: String,
    it: Box<dyn Iterator<Item = Result<Chunk>>>,
    input_size: u64,
    mapping_builder: Arc<Mutex<dyn Builder>>,
    mapped_size: u64,
    block_size: usize,
    thin_id: Option<u32>,
    hash_cache_size_meg: usize,
    sync_point_secs: u64,
}

impl Packer {
    #[allow(clippy::too_many_arguments)]
    fn new(
        output: Arc<Output>,
        input_path: PathBuf,
        stream_name: String,
        it: Box<dyn Iterator<Item = Result<Chunk>>>,
        input_size: u64,
        mapping_builder: Arc<Mutex<dyn Builder>>,
        mapped_size: u64,
        block_size: usize,
        thin_id: Option<u32>,
        hash_cache_size_meg: usize,
        sync_point_secs: u64,
    ) -> Self {
        Self {
            output,
            input_path,
            stream_name,
            it,
            input_size,
            mapping_builder,
            mapped_size,
            block_size,
            thin_id,
            hash_cache_size_meg,
            sync_point_secs,
        }
    }

    fn pack(
        mut self,
        hashes_file: Arc<Mutex<SlabFile<'static>>>,
        last_checkpoint_time: &mut Instant,
        data_archive_opt: Option<Data<'static, MultiFile>>,
    ) -> Result<(u32, Data<'static, MultiFile>)> {
        let mut splitter = ContentSensitiveSplitter::new(self.block_size as u32);

        // Reuse existing Data archive or create a new one
        let ad = if let Some(archive) = data_archive_opt {
            archive
        } else {
            let hashes_per_slab = std::cmp::max(SLAB_SIZE_TARGET / self.block_size, 1);
            let slab_capacity = ((self.hash_cache_size_meg * 1024 * 1024)
                / std::mem::size_of::<Hash256>())
                / hashes_per_slab;

            let data_file = MultiFile::open_for_write(data_path(), 128, slab_capacity)
                .with_context(|| {
                    format!(
                        "Failed to open data file for writing (path: {:?}, capacity: {})",
                        data_path(),
                        slab_capacity
                    )
                })?;
            Data::new(data_file, hashes_file.clone(), slab_capacity).with_context(|| {
                format!(
                    "Failed to create Data archive (block_size: {}, slab_capacity: {})",
                    self.block_size, slab_capacity
                )
            })?
        };

        let (stream_id, temp_stream_dir, final_stream_dir) =
            new_stream_path().with_context(|| {
                format!(
                    "Failed to generate new stream path for {}",
                    self.input_path.display()
                )
            })?;

        // Create temporary stream directory
        std::fs::create_dir(&temp_stream_dir).with_context(|| {
            format!(
                "Failed to create temporary stream directory: {:?} for {}",
                temp_stream_dir,
                self.input_path.display()
            )
        })?;
        let mut stream_path = temp_stream_dir.clone();
        stream_path.push("stream");

        let stream_file = SlabFileBuilder::create(&stream_path)
            .queue_depth(16)
            .compressed(true)
            .build()
            .with_context(|| {
                format!(
                    "Failed to create stream slab file at {:?} for {}",
                    stream_path,
                    self.input_path.display()
                )
            })?;

        let mut handler = DedupHandler::new(stream_file, self.mapping_builder.clone(), ad)
            .with_context(|| {
                format!(
                    "Failed to create DedupHandler for {}",
                    self.input_path.display()
                )
            })?;

        handler
            .ensure_extra_capacity(self.mapped_size as usize / self.block_size)
            .with_context(|| {
                format!(
                    "Failed to ensure extra capacity for {} (mapped_size: {}, block_size: {})",
                    self.input_path.display(),
                    self.mapped_size,
                    self.block_size
                )
            })?;

        self.output.report.progress(0);
        let start_time: DateTime<Utc> = Utc::now();

        let mut total_read = 0u64;
        let checkpoint_interval = Duration::from_secs(self.sync_point_secs);

        for chunk in &mut self.it {
            let chunk_result = chunk.with_context(|| {
                format!(
                    "Failed to read chunk from input file {} at offset {}",
                    self.input_path.display(),
                    total_read
                )
            })?;

            match chunk_result {
                Chunk::Mapped(buffer) => {
                    let len = buffer.len();
                    splitter.next_data(buffer, &mut handler).with_context(|| {
                        format!(
                            "Failed to process mapped data chunk for {} (offset: {}, len: {})",
                            self.input_path.display(),
                            total_read,
                            len
                        )
                    })?;
                    total_read += len as u64;
                    self.output
                        .report
                        .progress(((100 * total_read) / self.mapped_size) as u8);
                }
                Chunk::Unmapped(len) => {
                    assert!(len > 0);
                    splitter.next_break(&mut handler).with_context(|| {
                        format!(
                            "Failed to process unmapped break for {} at offset {}",
                            self.input_path.display(),
                            total_read
                        )
                    })?;
                    handler.handle_gap(len).with_context(|| {
                        format!(
                            "Failed to handle gap for {} (offset: {}, len: {})",
                            self.input_path.display(),
                            total_read,
                            len
                        )
                    })?;
                }
                Chunk::Ref(len) => {
                    splitter.next_break(&mut handler).with_context(|| {
                        format!(
                            "Failed to process ref break for {} at offset {}",
                            self.input_path.display(),
                            total_read
                        )
                    })?;
                    handler.handle_ref(len).with_context(|| {
                        format!(
                            "Failed to handle ref for {} (offset: {}, len: {})",
                            self.input_path.display(),
                            total_read,
                            len
                        )
                    })?;
                }
            }

            // Check if it's time to create a checkpoint (at slab boundaries)
            if handler.archive.slab_just_completed()
                && last_checkpoint_time.elapsed() >= checkpoint_interval
            {
                handler.archive.sync_checkpoint().with_context(|| {
                    format!(
                        "Failed to sync checkpoint for {} at {} bytes",
                        self.input_path.display(),
                        total_read
                    )
                })?;
                let cwd = std::env::current_dir()
                    .context("Failed to get current directory during checkpoint")?;
                let data_slab_file_id =
                    handler.archive.get_nr_data_slabs() / crate::slab::multi_file::SLABS_PER_FILE;
                let checkpoint_path = cwd.join(recovery::check_point_file());
                let checkpoint = recovery::create_checkpoint_from_files(&cwd, data_slab_file_id)
                    .with_context(|| {
                        format!(
                            "Failed to create checkpoint for {} (slab_id: {})",
                            self.input_path.display(),
                            data_slab_file_id
                        )
                    })?;
                checkpoint.write(&checkpoint_path).with_context(|| {
                    format!(
                        "Failed to write checkpoint to {:?} for {}",
                        checkpoint_path,
                        self.input_path.display()
                    )
                })?;
                *last_checkpoint_time = Instant::now();
            }
        }

        splitter.complete(&mut handler).with_context(|| {
            format!(
                "Failed to complete splitter for {}",
                self.input_path.display()
            )
        })?;
        self.output.report.progress(100);

        /*
         * if we flush here we will finish a slab which isn't ideal if we want to maximize the data
         * in each data slab file.  This really isn't ever ok as we consume too much slab address
         * space which will limit the amout of data we can have in the archive.
        handler.archive.flush().with_context(|| {
            format!(
                "Failed to flush archive after packing {}",
                self.input_path.display()
            )
        })?;
        */

        // Get the data slab file ID before handler is dropped
        let data_slab_file_id =
            handler.archive.get_nr_data_slabs() / crate::slab::multi_file::SLABS_PER_FILE;

        let end_time: DateTime<Utc> = Utc::now();
        let elapsed = end_time - start_time;
        let elapsed = elapsed.num_milliseconds() as f64 / 1000.0;
        let stream_written = handler.stream_file.get_file_size();
        let ratio =
            (self.mapped_size as f64) / ((handler.stats.data_written + stream_written) as f64);

        if self.output.json {
            // Should all the values simply be added to the json too?  We can always add entries, but
            // we can never take any away to maintains backwards compatibility with JSON consumers.
            let result = json!({ "stream_id": stream_id, "stats": handler.stats, });
            println!("{}", to_string_pretty(&result).unwrap());
        } else {
            self.output
                .report
                .info(&format!("elapsed          : {}", elapsed));
            self.output
                .report
                .info(&format!("stream id        : {}", stream_id));
            self.output
                .report
                .info(&format!("file size        : {:.2}", Size(self.input_size)));
            self.output
                .report
                .info(&format!("mapped size      : {:.2}", Size(self.mapped_size)));
            self.output
                .report
                .info(&format!("total read       : {:.2}", Size(total_read)));
            self.output.report.info(&format!(
                "fills size       : {:.2}",
                Size(handler.stats.fill_size)
            ));
            self.output.report.info(&format!(
                "duplicate data   : {:.2}",
                Size(total_read - handler.stats.data_written - handler.stats.fill_size)
            ));

            self.output.report.info(&format!(
                "data written     : {:.2}",
                Size(handler.stats.data_written)
            ));
            self.output
                .report
                .info(&format!("stream written   : {:.2}", Size(stream_written)));
            self.output
                .report
                .info(&format!("ratio            : {:.2}", ratio));
            self.output.report.info(&format!(
                "speed            : {:.2}/s",
                Size((total_read as f64 / elapsed) as u64)
            ));
        }

        // Write stream config to temp directory
        let temp_stream_id = temp_stream_dir
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid temp stream directory: {:?}", temp_stream_dir))
            .with_context(|| {
                format!(
                    "Failed to extract stream ID from temp directory path for {}",
                    self.input_path.display()
                )
            })?;

        let cfg = config::StreamConfig {
            name: Some(self.stream_name.to_string()),
            source_path: self.input_path.display().to_string(),
            pack_time: config::now(),
            size: self.input_size,
            mapped_size: self.mapped_size,
            packed_size: handler.stats.data_written + stream_written,
            thin_id: self.thin_id,
        };
        config::write_stream_config(temp_stream_id, &cfg).with_context(|| {
            format!(
                "Failed to write stream config for {} (stream_id: {}, temp_dir: {:?})",
                self.input_path.display(),
                temp_stream_id,
                temp_stream_dir
            )
        })?;

        // Atomically move temp stream directory to final location
        // This is the commit point - either the stream exists completely or not at all
        std::fs::rename(&temp_stream_dir, &final_stream_dir).with_context(|| {
            format!(
                "Failed to atomically commit stream directory for {}: {:?} -> {:?} (stream_id: {})",
                self.input_path.display(),
                temp_stream_dir,
                final_stream_dir,
                stream_id
            )
        })?;

        //TODO: Sync stream directory?

        // Extract the Data archive from the handler to return it for reuse
        let archive = handler.archive;

        Ok((data_slab_file_id, archive))
    }
}

//-----------------------------------------

fn thick_packer(
    output: Arc<Output>,
    input_file: &Path,
    input_name: String,
    config: &config::Config,
    sync_point_secs: u64,
) -> Result<Packer> {
    let input_size = thinp::file_utils::file_size(input_file)?;

    let mapped_size = input_size;
    let input_iter = Box::new(ThickChunker::new(input_file, 16 * 1024 * 1024)?);
    let thin_id = None;
    let builder = Arc::new(Mutex::new(MappingBuilder::default()));

    Ok(Packer::new(
        output,
        input_file.to_path_buf(),
        input_name,
        input_iter,
        input_size,
        builder,
        mapped_size,
        config.block_size,
        thin_id,
        config.hash_cache_size_meg,
        sync_point_secs,
    ))
}

fn thin_packer(
    output: Arc<Output>,
    input_file: &Path,
    input_name: String,
    config: &config::Config,
    sync_point_secs: u64,
) -> Result<Packer> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(input_file)?;

    let mappings = read_thin_mappings(input_file)?;
    let mapped_size = mappings.provisioned_blocks.len() * mappings.data_block_size as u64 * 512;
    let run_iter = RunIter::new(
        mappings.provisioned_blocks,
        (input_size / (mappings.data_block_size as u64 * 512)) as u32,
    );
    let input_iter = Box::new(ThinChunker::new(
        input,
        run_iter,
        mappings.data_block_size as u64 * 512,
    ));
    let thin_id = Some(mappings.thin_id);
    let builder = Arc::new(Mutex::new(MappingBuilder::default()));

    output
        .report
        .set_title(&format!("Packing {} ...", input_file.display()));
    Ok(Packer::new(
        output,
        input_file.to_path_buf(),
        input_name,
        input_iter,
        input_size,
        builder,
        mapped_size,
        config.block_size,
        thin_id,
        config.hash_cache_size_meg,
        sync_point_secs,
    ))
}

// FIXME: slow
fn open_thin_stream(stream_id: &str) -> Result<SlabFile<'static>> {
    SlabFileBuilder::open(stream_path(stream_id))
        .build()
        .context("couldn't open old stream file")
}

#[allow(clippy::too_many_arguments)]
fn thin_delta_packer(
    output: Arc<Output>,
    input_file: &Path,
    input_name: String,
    config: &config::Config,
    delta_device: &Path,
    delta_id: &str,
    hashes_file: Arc<Mutex<SlabFile<'static>>>,
    sync_point_secs: u64,
) -> Result<Packer> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(input_file)?;

    let mappings = read_thin_delta(delta_device, input_file)?;
    let old_config = config::read_stream_config(delta_id)?;
    let mapped_size = old_config.mapped_size;

    let run_iter = DualIter::new(
        mappings.additions,
        mappings.removals,
        (input_size / (mappings.data_block_size as u64 * 512)) as u32,
    );

    let input_iter = Box::new(DeltaChunker::new(
        input,
        run_iter,
        mappings.data_block_size as u64 * 512,
    ));
    let thin_id = Some(mappings.thin_id);

    let old_stream = open_thin_stream(delta_id)?;
    let old_entries = StreamIter::new(old_stream)?;
    let builder = Arc::new(Mutex::new(DeltaBuilder::new(old_entries, hashes_file)));

    output
        .report
        .set_title(&format!("Packing {} ...", input_file.display()));
    Ok(Packer::new(
        output,
        input_file.to_path_buf(),
        input_name,
        input_iter,
        input_size,
        builder,
        mapped_size,
        config.block_size,
        thin_id,
        config.hash_cache_size_meg,
        sync_point_secs,
    ))
}

// Looks up both --delta-stream and --delta-device
fn get_delta_args(matches: &ArgMatches) -> Result<Option<(String, PathBuf)>> {
    match (
        matches.get_one::<String>("DELTA_STREAM"),
        matches.get_one::<String>("DELTA_DEVICE"),
    ) {
        (None, None) => Ok(None),
        (Some(stream), Some(device)) => {
            let mut buf = PathBuf::new();
            buf.push(device);
            Ok(Some((stream.to_string(), buf)))
        }
        _ => Err(anyhow!(
            "--delta-stream and --delta-device must both be given"
        )),
    }
}

/// Resolve `path_str`, ensure it refers to a readable regular file or block device,
/// and return the canonical absolute path (symlinks resolved).
///
/// Linux-only (uses `FileTypeExt::is_block_device`).
pub fn canonicalize_readable_regular_or_block(path_str: String) -> Result<PathBuf> {
    let input = Path::new(&path_str);

    // 1) Canonicalize (resolves symlinks, returns absolute path)
    let canon = fs::canonicalize(input).with_context(|| format!("canonicalizing {:?}", input))?;

    // 2) Stat the resolved target
    let meta = fs::metadata(&canon).with_context(|| format!("metadata for {:?}", canon))?;
    let ft = meta.file_type();

    // 3) Must be a regular file OR a block device
    let is_ok_type = ft.is_file() || {
        #[cfg(target_os = "linux")]
        {
            ft.is_block_device()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    };
    if !is_ok_type {
        return Err(anyhow!("not a regular file or block device: {:?}", canon));
    }

    // 4) Verify we can actually read it (covers permissions, RO mounts, etc.)
    let _fh = OpenOptions::new()
        .read(true)
        .open(&canon)
        .with_context(|| format!("opening for read {:?}", canon))?;
    // (File handle drops here; open succeeded ⇒ readable)

    Ok(canon)
}

pub fn run(matches: &ArgMatches, output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
    let input_files: Vec<&String> = matches.get_many::<String>("INPUT").unwrap().collect();

    let sync_point_secs = *matches.get_one::<u64>("SYNC_POINT_SECS").unwrap_or(&15u64);

    env::set_current_dir(&archive_dir)?;

    let config = config::read_config(".", matches)?;

    let hashes_file = Arc::new(Mutex::new(
        SlabFileBuilder::open(hashes_path())
            .write(true)
            .queue_depth(16)
            .build()
            .context("couldn't open hashes slab file")?,
    ));

    // Categorize input files into delta, thin, and thick lists
    let mut delta_files = Vec::new();
    let mut thin_files = Vec::new();
    let mut thick_files = Vec::new();

    let delta_args = get_delta_args(matches)?;

    for input_file_str in input_files {
        let result = canonicalize_readable_regular_or_block(input_file_str.to_string());

        if let Err(e) = result {
            eprintln!(
                "error processing {} for reason {}, skipping!",
                input_file_str, e
            );
            continue;
        }

        let input_file = result.unwrap();

        if delta_args.is_some() {
            delta_files.push(input_file);
        } else if is_thin_device(&input_file).with_context(|| {
            format!(
                "Failed to check if {} is a thin device",
                input_file.display()
            )
        })? {
            thin_files.push(input_file);
        } else {
            let meta = std::fs::metadata(input_file.clone())?;
            if meta.is_file() {
                thick_files.push(input_file);
            }
        }
    }

    // Initialize checkpoint timer once for all files
    let mut last_checkpoint_time = Instant::now();
    let mut data_slab_file_id = 0u32;

    // Process delta devices with one packer
    if !delta_files.is_empty() {
        if let Some((delta_stream, delta_device)) = &delta_args {
            data_slab_file_id = process_file_list(
                &delta_files,
                output.clone(),
                &config,
                hashes_file.clone(),
                sync_point_secs,
                &mut last_checkpoint_time,
                |output, input_file, input_name, config, hashes_file, sync_point_secs| {
                    thin_delta_packer(
                        output,
                        input_file,
                        input_name,
                        config,
                        delta_device,
                        delta_stream,
                        hashes_file,
                        sync_point_secs,
                    )
                },
            )
            .with_context(|| {
                format!(
                    "Failed to process delta device list ({} files)",
                    delta_files.len()
                )
            })?;
        }
    }

    // Process thin devices with one packer
    if !thin_files.is_empty() {
        data_slab_file_id = process_file_list(
            &thin_files,
            output.clone(),
            &config,
            hashes_file.clone(),
            sync_point_secs,
            &mut last_checkpoint_time,
            |output, input_file, input_name, config, _hashes_file, sync_point_secs| {
                thin_packer(output, input_file, input_name, config, sync_point_secs)
            },
        )
        .with_context(|| {
            format!(
                "Failed to process thin device list ({} files)",
                thin_files.len()
            )
        })?;
    }

    // Process thick devices/files with one packer
    if !thick_files.is_empty() {
        data_slab_file_id = process_file_list(
            &thick_files,
            output.clone(),
            &config,
            hashes_file.clone(),
            sync_point_secs,
            &mut last_checkpoint_time,
            |output, input_file, input_name, config, _hashes_file, sync_point_secs| {
                thick_packer(output, input_file, input_name, config, sync_point_secs)
            },
        )
        .with_context(|| {
            format!(
                "Failed to process thick device/file list ({} files)",
                thick_files.len()
            )
        })?;
    }

    // Create final checkpoint after all files are processed
    let cwd = std::env::current_dir()
        .context("Failed to get current working directory for final checkpoint")?;
    let checkpoint =
        recovery::create_checkpoint_from_files(&cwd, data_slab_file_id).with_context(|| {
            format!(
                "Failed to create final checkpoint from files (data_slab_file_id={})",
                data_slab_file_id
            )
        })?;
    let checkpoint_path = cwd.join(recovery::check_point_file());
    checkpoint
        .write(&checkpoint_path)
        .with_context(|| format!("Failed to write final checkpoint to {:?}", checkpoint_path))?;

    Ok(())
}

fn process_file_list<F>(
    files: &[PathBuf],
    output: Arc<Output>,
    config: &config::Config,
    hashes_file: Arc<Mutex<SlabFile<'static>>>,
    sync_point_secs: u64,
    last_checkpoint_time: &mut Instant,
    packer_factory: F,
) -> Result<u32>
where
    F: Fn(
        Arc<Output>,
        &Path,
        String,
        &config::Config,
        Arc<Mutex<SlabFile<'static>>>,
        u64,
    ) -> Result<Packer>,
{
    let mut data_slab_file_id = 0u32;
    let mut data_archive_opt: Option<Data<'static, MultiFile>> = None;

    for input_file in files {
        let input_name = input_file
            .file_name()
            .ok_or_else(|| anyhow!("Input file has no filename: {:?}", input_file))
            .with_context(|| format!("Failed to extract filename from path: {:?}", input_file))?
            .to_str()
            .ok_or_else(|| anyhow!("Input filename is not valid UTF-8: {:?}", input_file))
            .with_context(|| format!("Failed to convert filename to string: {:?}", input_file))?
            .to_string();

        output
            .report
            .set_title(&format!("Building packer {} ...", input_file.display()));

        let packer = packer_factory(
            output.clone(),
            input_file,
            input_name.clone(),
            config,
            hashes_file.clone(),
            sync_point_secs,
        )
        .with_context(|| format!("Failed to create packer for file: {}", input_file.display()))?;

        output
            .report
            .set_title(&format!("Packing {} ...", input_file.display()));
        let (slab_id, archive) = packer
            .pack(hashes_file.clone(), last_checkpoint_time, data_archive_opt)
            .with_context(|| {
                format!(
                    "Failed to pack file: {} (name: {})",
                    input_file.display(),
                    input_name
                )
            })?;
        data_slab_file_id = slab_id;
        data_archive_opt = Some(archive);
    }

    Ok(data_slab_file_id)
}

//-----------------------------------------
