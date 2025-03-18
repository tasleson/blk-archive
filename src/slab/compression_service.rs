use anyhow::Result;
use std::io::Write;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

use crate::slab::SlabData;

//-----------------------------------------

#[derive(Clone, Eq, PartialEq)]
pub enum ShutdownMode {
    /// Process all queued items before shutting down
    Graceful,
    /// Stop as soon as possible, abandoning queued work
    Immediate,
}

type ShutdownRx = Arc<Mutex<Receiver<ShutdownMode>>>;

pub struct CompressionService {
    pool: ThreadPool,

    // Option so we can 'take' it and prevent two calls to shutdown.
    shutdown_tx: Option<SyncSender<ShutdownMode>>,
}

fn compression_worker_(
    rx: Arc<Mutex<Receiver<SlabData>>>,
    tx: SyncSender<SlabData>,
    shutdown_rx: ShutdownRx,
) -> Result<()> {
    let mut shutdown_mode = None;

    loop {
        // Check for shutdown signal (non-blocking)
        {
            let shutdown_rx = shutdown_rx.lock().unwrap();
            match shutdown_rx.try_recv() {
                Ok(mode) => {
                    shutdown_mode = Some(mode);
                    if matches!(shutdown_mode, Some(ShutdownMode::Immediate)) {
                        break;
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
            }
        }

        // Try to receive data with timeout
        let data = {
            let rx = rx.lock().unwrap();
            match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(data) => Some(data),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // If we're in graceful shutdown and no data is available, we can exit
                    if matches!(shutdown_mode, Some(ShutdownMode::Graceful)) {
                        break;
                    }
                    continue;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        };

        if let Some(data) = data {
            let mut packer = zstd::Encoder::new(Vec::new(), 0)?;
            packer.write_all(&data.data)?;
            tx.send(SlabData {
                index: data.index,
                data: packer.finish()?,
            })?;
        }
    }

    Ok(())
}

fn compression_worker(
    rx: Arc<Mutex<Receiver<SlabData>>>,
    tx: SyncSender<SlabData>,
    shutdown_rx: ShutdownRx,
) {
    // FIXME: handle error
    compression_worker_(rx, tx, shutdown_rx).unwrap();
}

impl CompressionService {
    pub fn new(nr_threads: usize, tx: SyncSender<SlabData>) -> (Self, SyncSender<SlabData>) {
        let pool = ThreadPool::new(nr_threads);
        let (self_tx, rx) = sync_channel(nr_threads * 64);
        let (shutdown_tx, shutdown_rx) = sync_channel(nr_threads);

        // we can only have a single receiver
        let rx = Arc::new(Mutex::new(rx));
        let shutdown_rx = Arc::new(Mutex::new(shutdown_rx));

        for _ in 0..nr_threads {
            let tx = tx.clone();
            let rx = rx.clone();
            let shutdown_rx = shutdown_rx.clone();
            pool.execute(move || compression_worker(rx, tx, shutdown_rx));
        }

        (
            Self {
                pool,
                shutdown_tx: Some(shutdown_tx),
            },
            self_tx,
        )
    }

    pub fn shutdown(&mut self, mode: ShutdownMode) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            // Send shutdown signal to all workers
            for _ in 0..self.pool.max_count() {
                let _ = shutdown_tx.send(mode.clone());
            }
        }
    }

    pub fn join(mut self) {
        // Default to graceful shutdown if not already shutting down
        if self.shutdown_tx.is_some() {
            self.shutdown(ShutdownMode::Graceful);
        }

        self.pool.join();
    }
}

//-----------------------------------------
