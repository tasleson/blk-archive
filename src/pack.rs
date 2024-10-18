use anyhow::{anyhow, Context, Result};
use chrono::prelude::*;
use clap::ArgMatches;
//use size_display::Size;
use std::boxed::Box;
use std::env;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::chunkers::*;
use crate::client::*;
use crate::content_sensitive_splitter::*;
use crate::db::*;
use crate::handshake::HandShake;
use crate::hash::*;
use crate::iovec::*;
use crate::output::Output;
use crate::paths::*;
use crate::run_iter::*;
use crate::slab::*;
use crate::splitter::*;
use crate::stream::*;
use crate::stream_builders::*;
use crate::stream_meta;
use crate::stream_orderer::*;
use crate::thin_metadata::*;

//-----------------------------------------

enum Tp {
    Local(Db),
    Remote(
        Arc<Mutex<ClientRequests>>,
        Option<JoinHandle<std::result::Result<(), anyhow::Error>>>,
    ),
}

struct DedupHandler {
    nr_chunks: usize,
    stream_buf: Vec<u8>,
    stream_meta: stream_meta::StreamMeta,
    mapping_builder: Arc<Mutex<dyn Builder>>,
    pub stats: stream_meta::StreamStats,
    so: Arc<Mutex<StreamOrder>>,
    transport: Tp,
}

impl DedupHandler {
    fn new(
        names: stream_meta::StreamNames,
        stats: stream_meta::StreamStats,
        thin_id: Option<u32>,
        mapping_builder: Arc<Mutex<dyn Builder>>,
        server_addr: Option<String>,
    ) -> Result<Self> {
        let so = Arc::new(Mutex::new(StreamOrder::new()?));
        let mut sending = false;

        let tp: Tp = if let Some(s_conn) = server_addr {
            sending = true;
            println!("Client is connecting to server using {}", s_conn);
            let mut client = Client::new(s_conn, so.clone())?;
            let rq = client.get_request_queue();
            // Start a thread to handle client communication
            let h = thread::Builder::new()
                .name("client socket handler".to_string())
                .spawn(move || client.run())?;
            Tp::Remote(rq, Some(h))
        } else {
            Tp::Local(Db::new()?)
        };

        // Create the stream meta and store
        let stream_meta = stream_meta::StreamMeta::new(names, thin_id, sending)?;

        Ok(Self {
            nr_chunks: 0,
            stream_meta,
            stream_buf: Vec::new(),
            mapping_builder,
            stats,
            so,
            transport: tp,
        })
    }

    fn process_stream_entry(&mut self, e: &MapEntry, len: u64) -> Result<()> {
        let mut builder = self.mapping_builder.lock().unwrap();
        builder.next(e, len, &mut self.stream_buf)
    }

    fn process_stream(&mut self) -> Result<bool> {
        let mut me: MapEntry;
        let mut len: u64;

        let rc = self.so.lock().unwrap().drain();

        for e in rc.0 {
            me = e.e;
            len = e.len;
            self.process_stream_entry(&me, len)?;
            self.maybe_complete_stream()?
        }
        Ok(rc.1)
    }

    fn maybe_complete_stream(&mut self) -> Result<()> {
        complete_slab(
            &mut self.stream_meta.stream_file,
            &mut self.stream_buf,
            SLAB_SIZE_TARGET,
        )?;
        Ok(())
    }

    fn get_next_stream_id(&self) -> u64 {
        let mut so = self.so.lock().unwrap();
        so.entry_start()
    }

    fn enqueue_entry(&mut self, e: MapEntry, len: u64) -> Result<()> {
        {
            let mut so = self.so.lock().unwrap();
            let id = so.entry_start();
            so.entry_complete(id, e, len)?;
        }
        //self.process_stream()?;
        Ok(())
    }

    fn handle_gap(&mut self, len: u64) -> Result<()> {
        self.enqueue_entry(MapEntry::Unmapped { len }, len)
    }

    fn handle_ref(&mut self, len: u64) -> Result<()> {
        self.enqueue_entry(MapEntry::Ref { len }, len)
    }

    fn db_sizes(&mut self) -> (u64, u64) {
        // TODO fix me
        //self.db.file_sizes()
        (1, 1)
    }

    // TODO: Is there a better way to handle this and what are the ramifications with
    // client server with multiple clients and one server?
    //fn ensure_extra_capacity(&mut self, blocks: usize) -> Result<()> {
    //    self.c.ensure_extra_capacity(blocks)
    //}
}

impl IoVecHandler for DedupHandler {
    fn handle_data(&mut self, iov: &IoVec) -> Result<()> {
        self.nr_chunks += 1;
        let len = iov_len_(iov);

        // TODO: Understand why the code was incrementing mapped size here previously
        //self.stats.mapped_size += len;

        if let Some(first_byte) = all_same(iov) {
            self.stats.fill_size += len;
            self.enqueue_entry(
                MapEntry::Fill {
                    byte: first_byte,
                    len,
                },
                len,
            )?;
        } else {
            match self.transport {
                Tp::Local(ref mut db) => {
                    let len = iov_len_(iov);
                    let h = hash_256_iov(iov);
                    let ((slab, offset), len_written) = db.add_data_entry(h, iov, len)?;
                    self.enqueue_entry(
                        MapEntry::Data {
                            slab,
                            offset,
                            nr_entries: 1,
                        },
                        len,
                    )?;
                    self.stats.written += len_written;
                }
                Tp::Remote(ref rq, _) => {
                    let h = hash_256_iov(iov);
                    let data = Data {
                        id: self.get_next_stream_id(),
                        h: hash256_to_bytes(&h),
                        len,
                        d: Some(io_vec_to_vec(iov)),
                    };
                    let mut req = rq.lock().unwrap();
                    req.handle_data(data);
                }
            }
        }

        self.process_stream()?;

        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        // We need to process everything that is outstanding which could be
        // quite a bit
        // TODO: Make this handle errors, like if we end up hanging forever
        let h: HandShake;
        println!("IoVecHandler::complete, waiting for stream order to complete!");

        loop {
            if !self.process_stream()? {
                print!(".");
                thread::sleep(Duration::from_millis(100));
            } else {
                break;
            }
        }

        println!("stream is complete!");
        let mut builder = self.mapping_builder.lock().unwrap();
        builder.complete(&mut self.stream_buf)?;
        drop(builder);

        complete_slab(&mut self.stream_meta.stream_file, &mut self.stream_buf, 0)?;
        self.stream_meta.stream_file.close()?;

        // The stream file is done, lets put the file in the correct place which could be the
        // correct local archive directory or remote archive directory.

        match self.transport {
            Tp::Local(ref _db) => {
                self.stream_meta.complete(&self.stats)?;
            }
            Tp::Remote(ref mut rq, ref mut handle) => {
                // Send the stream file to the server and wait for it to complete
                {
                    let mut req = rq.lock().unwrap();
                    self.stats.written = req.data_written;

                    // Send the stream metadata & stream itself to the server side
                    let to_send = self.stream_meta.package(self.stats.clone())?;
                    let cmd = SyncCommand::new(Command::Cmd(to_send));
                    h = cmd.h.clone();
                    req.handle_control(cmd);
                }

                // You need to make sure we are not holding a lock when we call wait,
                // this is achieved by using different scopes.
                h.wait();

                //TODO: Change this to use the command queue instead of data queue
                {
                    let mut req = rq.lock().unwrap();
                    req.handle_data(END);
                }

                if let Some(worker) = handle.take() {
                    let rc = worker.join();
                    println!("client worker thread ended with {:?}", rc);
                }
            }
        }

        Ok(())
    }
}

//-----------------------------------------

struct Packer {
    output: Arc<Output>,
    names: stream_meta::StreamNames,
    it: Box<dyn Iterator<Item = Result<Chunk>>>,
    mapping_builder: Arc<Mutex<dyn Builder>>,
    block_size: usize,
    thin_id: Option<u32>,
    stats: stream_meta::StreamStats,
}

impl Packer {
    #[allow(clippy::too_many_arguments)]
    fn new(
        output: Arc<Output>,
        names: stream_meta::StreamNames,
        it: Box<dyn Iterator<Item = Result<Chunk>>>,
        stats: stream_meta::StreamStats,
        mapping_builder: Arc<Mutex<dyn Builder>>,
        block_size: usize,
        thin_id: Option<u32>,
    ) -> Self {
        Self {
            output,
            names,
            it,
            mapping_builder,
            block_size,
            thin_id,
            stats,
        }
    }

    fn pack(mut self, server_addr: Option<String>) -> Result<()> {
        let mut splitter = ContentSensitiveSplitter::new(self.block_size as u32);

        let mapped_size = self.stats.mapped_size;

        let mut handler = DedupHandler::new(
            self.names,
            self.stats,
            self.thin_id,
            self.mapping_builder.clone(),
            server_addr,
        )?;

        // TODO handle this
        //handler.ensure_extra_capacity(self.mapped_size as usize / self.block_size)?;

        self.output.report.progress(0);
        let start_time: DateTime<Utc> = Utc::now();

        let mut total_read = 0u64;
        for chunk in &mut self.it {
            match chunk? {
                Chunk::Mapped(buffer) => {
                    let len = buffer.len();
                    splitter.next_data(buffer, &mut handler)?;
                    total_read += len as u64;
                    self.output
                        .report
                        .progress(((100 * total_read) / mapped_size) as u8);
                }
                Chunk::Unmapped(len) => {
                    assert!(len > 0);
                    splitter.next_break(&mut handler)?;
                    handler.handle_gap(len)?;
                }
                Chunk::Ref(len) => {
                    splitter.next_break(&mut handler)?;
                    handler.handle_ref(len)?;
                }
            }
        }

        splitter.complete(&mut handler)?;
        self.output.report.progress(100);
        let end_time: DateTime<Utc> = Utc::now();
        let elapsed = end_time - start_time;
        let _elapsed = elapsed.num_milliseconds() as f64 / 1000.0;

        let _db_sizes = handler.db_sizes();

        // TODO fix stats!

        //let data_written = db_sizes.0 - data_size;
        //let hashes_written = db_sizes.1 - hashes_size;

        //let stream_written = handler.stream_file.get_file_size();
        //let ratio =
        //    (self.mapped_size as f64) / ((data_written + hashes_written + stream_written) as f64);

        if self.output.json {
            // Should all the values simply be added to the json too?  We can always add entries, but
            // we can never take any away to maintains backwards compatibility with JSON consumers.
            //let result = json!({ "stream_id": stream_id });
            //println!("{}", to_string_pretty(&result).unwrap());
        } else {
            /*
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
                Size(handler.fill_size)
            ));
            self.output.report.info(&format!(
                "duplicate data   : {:.2}",
                Size(total_read - handler.data_written - handler.fill_size)
            ));

            self.output
                .report
                .info(&format!("data written     : {:.2}", Size(data_written)));
            self.output
                .report
                .info(&format!("hashes written   : {:.2}", Size(hashes_written)));
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
            */
        }

        // write the stream config
        /*
        let cfg = config::StreamConfig {
            name: Some(self.stream_name.to_string()),
            source_path: self.input_path.display().to_string(),
            pack_time: config::now(),
            size: self.input_size,
            mapped_size: self.mapped_size,
            packed_size: stream_written, //data_written + hashes_written + stream_written,
            thin_id: self.thin_id,
        };
        config::write_stream_config(&stream_id, &cfg)?;
        */

        Ok(())
    }
}

//-----------------------------------------

fn thick_packer(output: Arc<Output>, names: stream_meta::StreamNames) -> Result<Packer> {
    let mut stats = stream_meta::StreamStats::zero();

    stats.size = thinp::file_utils::file_size(names.input_file.clone())?;
    stats.mapped_size = stats.size;

    let input_iter = Box::new(ThickChunker::new(
        &names.input_file.clone(),
        16 * 1024 * 1024,
    )?);
    let thin_id = None;
    let builder = Arc::new(Mutex::new(MappingBuilder::default()));

    Ok(Packer::new(
        output, names, input_iter, stats, builder, 4096, thin_id,
    ))
}

fn thin_packer(output: Arc<Output>, names: stream_meta::StreamNames) -> Result<Packer> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(names.input_file.clone())
        .context("couldn't open input file/dev")?;
    let mut stats = stream_meta::StreamStats::zero();
    stats.size = thinp::file_utils::file_size(names.input_file.clone())?;

    let mappings = read_thin_mappings(names.input_file.clone())?;
    stats.mapped_size = mappings.provisioned_blocks.len() * mappings.data_block_size as u64 * 512;
    let run_iter = RunIter::new(
        mappings.provisioned_blocks,
        (stats.size / (mappings.data_block_size as u64 * 512)) as u32,
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
        .set_title(&format!("Packing {} ...", names.input_file.display()));
    Ok(Packer::new(
        output, names, input_iter, stats, builder, 4096, thin_id,
    ))
}

// FIXME: slow
#[allow(dead_code)]
fn open_thin_stream(stream_id: &str) -> Result<SlabFile> {
    SlabFileBuilder::open(stream_path(stream_id))
        .build()
        .context("couldn't open old stream file")
}

#[allow(dead_code)]
fn thin_delta_packer(
    output: Arc<Output>,
    names: stream_meta::StreamNames,
    delta_device: &Path,
    delta_id: &str,
    hashes_file: Arc<Mutex<SlabFile>>,
) -> Result<Packer> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(names.input_file.clone())
        .context("couldn't open input file/dev")?;

    let mut stats = stream_meta::StreamStats::zero();
    stats.size = thinp::file_utils::file_size(names.input_file.clone())?;

    let mappings = read_thin_delta(delta_device, &names.input_file.clone())?;
    let old_config = stream_meta::read_stream_config(delta_id)?;
    stats.mapped_size = old_config.mapped_size;

    let run_iter = DualIter::new(
        mappings.additions,
        mappings.removals,
        (stats.size / (mappings.data_block_size as u64 * 512)) as u32,
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
        .set_title(&format!("Packing {} ...", names.input_file.display()));
    Ok(Packer::new(
        output, names, input_iter, stats, builder, 4096, thin_id,
    ))
}

// Looks up both --delta-stream and --delta-device
#[allow(dead_code)]
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

pub fn run(matches: &ArgMatches, output: Arc<Output>, server: Option<String>) -> Result<()> {
    // TODO we need to remove this and move it to the server side.  Replace with creating a temp.
    // directory and stream file and when successfully done, we'll then transfer that to the server
    // which may or may not be the same machine

    if server.is_none() {
        let archive_dir =
            Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
        env::set_current_dir(archive_dir)?;
    }

    let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
    let input_name = input_file
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap()).canonicalize()?;

    let names = stream_meta::StreamNames {
        name: input_name,
        input_file: input_file.clone(),
    };

    output
        .report
        .set_title(&format!("Building packer {} ...", input_file.display()));

    // TODO figure out how to make delta work in a remote send/receive environment
    /*let packer = if let Some((delta_stream, delta_device)) = get_delta_args(matches)? {
    thin_delta_packer(
        output.clone(),
        &input_file,
        input_name,
        &delta_device,
        &delta_stream,
        hashes_file.clone(),
    )? */
    let packer = if is_thin_device(&input_file)? {
        thin_packer(output.clone(), names)?
    } else {
        thick_packer(output.clone(), names)?
    };

    output
        .report
        .set_title(&format!("Packing {} ...", input_file.display()));
    packer.pack(server)
}

//-----------------------------------------
