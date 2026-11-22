//! Ingest: receive frames over TCP, store them, acknowledge, compute features.
//!
//! Per connection, a reader thread decodes and CRC-checks frames and hands
//! them to the writer (the calling thread) through a bounded queue. The
//! writer collects frames for one batch interval, then:
//!
//! 1. drops duplicates (a sender resending after a reconnect) and counts
//!    missing frames and samples from each channel's sequence numbers,
//! 2. stores the batch durably ([`Store::put_samples`]),
//! 3. acknowledges, per channel, the highest sequence number now durable,
//! 4. hands the frames to a feature thread, which cuts windows and stores
//!    their features through its own writer on the features table.
//!
//! Storage `fsync` latency is the slow step on some disks, so the queue
//! between reader and writer is large (about 20 s of the default layout):
//! while a flush waits on the disk, the reader keeps draining TCP and the
//! sender is not pushed back; only acknowledgements wait. Features run on
//! their own thread so they never delay a sample batch's acknowledgement.
//!
//! A frame is acknowledged only after step 2 returns (R8). On start the
//! store is read to recover each channel's last durable sequence number, so
//! drop counting continues correctly across a restart.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytelanes::cpu::CpuBackend;
use diagsteno_store::{SampleBlock, Store};

use crate::config::ChannelSpec;
use crate::features::{Engine, Windower};
use crate::frame::{Frame, write_ack};

/// Error type for ingest.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Ingest settings.
#[derive(Clone, Debug)]
pub struct IngestConfig {
    /// Store directory.
    pub store: PathBuf,
    /// Channels accepted; frames for other channels are counted and dropped.
    pub layout: Vec<ChannelSpec>,
    /// How long frames collect before a batch is stored and acknowledged.
    pub batch: Duration,
    /// Frames queued between the reader and the writer before the reader
    /// waits (TCP then pushes back on the sender).
    pub queue: usize,
}

impl IngestConfig {
    /// Defaults: 100 ms batches and a 131,072-frame queue (about 20 s of
    /// the default 64-channel layout at 100 frames per channel per second).
    pub fn new(store: impl Into<PathBuf>, layout: Vec<ChannelSpec>) -> Self {
        IngestConfig {
            store: store.into(),
            layout,
            batch: Duration::from_millis(100),
            queue: 131_072,
        }
    }
}

/// Counters for an ingest run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Frames stored.
    pub frames: u64,
    /// Samples stored.
    pub samples: u64,
    /// Frames missing from a channel's sequence (dropped samples).
    pub missing_frames: u64,
    /// Samples in the missing frames, from the gap in sample indices.
    pub missing_samples: u64,
    /// Frames received again after they were already stored.
    pub duplicate_frames: u64,
    /// Frames for channels not in the layout.
    pub unknown_channel_frames: u64,
    /// Feature rows stored.
    pub feature_rows: u64,
    /// Windows abandoned because of missing samples.
    pub window_gaps: u64,
    /// Batches stored.
    pub batches: u64,
    /// Time spent storing samples, in milliseconds.
    pub put_samples_ms: u64,
    /// Time spent computing features, in milliseconds.
    pub features_ms: u64,
    /// Time spent storing features, in milliseconds.
    pub put_features_ms: u64,
    /// Slowest single batch flush, in milliseconds.
    pub max_flush_ms: u64,
}

/// An ingest endpoint bound to one store.
pub struct Ingest {
    cfg: IngestConfig,
    store: Store,
    specs: HashMap<u16, ChannelSpec>,
    /// Per channel: next expected sequence number and its first sample.
    next: HashMap<u16, (u64, u64)>,
    stats: Stats,
    features: Option<FeatureWorker>,
}

#[derive(Default)]
struct FeatureState {
    rows: u64,
    gaps: u64,
    compute_ms: u64,
    put_ms: u64,
    error: Option<String>,
}

/// Lock the feature counters; a poisoned lock still holds plain counters.
fn lock(st: &Mutex<FeatureState>) -> std::sync::MutexGuard<'_, FeatureState> {
    match st.lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    }
}

struct FeatureWorker {
    tx: mpsc::Sender<Vec<Frame>>,
    state: Arc<Mutex<FeatureState>>,
    thread: JoinHandle<()>,
}

impl FeatureWorker {
    fn start(layout: &[ChannelSpec], mut store: Store) -> FeatureWorker {
        let (tx, rx) = mpsc::channel::<Vec<Frame>>();
        let state = Arc::new(Mutex::new(FeatureState::default()));
        let st = Arc::clone(&state);
        let mut windower = Windower::new(layout);
        let mut engine = Engine::new(CpuBackend::new());
        let thread = std::thread::spawn(move || {
            for frames in rx {
                let windows: Vec<_> = frames.iter().flat_map(|f| windower.push(f)).collect();
                lock(&st).gaps = windower.gaps();
                if windows.is_empty() {
                    continue;
                }
                let t = Instant::now();
                let rows = match engine.compute(&windows) {
                    Ok(r) => r,
                    Err(e) => {
                        lock(&st).error = Some(e.to_string());
                        return;
                    }
                };
                let compute_ms = t.elapsed().as_millis() as u64;
                let t = Instant::now();
                let put = store.put_features(&rows);
                let mut g = lock(&st);
                g.compute_ms += compute_ms;
                g.put_ms += t.elapsed().as_millis() as u64;
                match put {
                    Ok(()) => g.rows += rows.len() as u64,
                    Err(e) => {
                        g.error = Some(e.to_string());
                        return;
                    }
                }
            }
        });
        FeatureWorker { tx, state, thread }
    }
}

impl Ingest {
    /// Open the store and recover each channel's position from it.
    pub fn open(cfg: IngestConfig) -> Result<Ingest, Error> {
        let store = Store::open(&cfg.store)?;
        let mut next = HashMap::new();
        for b in store.read_samples()? {
            let after = (b.seq + 1, b.first_sample + b.samples.len() as u64);
            let e = next.entry(b.channel).or_insert(after);
            if after.0 > e.0 {
                *e = after;
            }
        }
        let features = Some(FeatureWorker::start(&cfg.layout, store.table_writer()));
        Ok(Ingest {
            specs: cfg.layout.iter().map(|c| (c.id, *c)).collect(),
            cfg,
            store,
            next,
            stats: Stats::default(),
            features,
        })
    }

    /// Counters so far (feature counters may lag behind the samples).
    pub fn stats(&self) -> Stats {
        let mut s = self.stats.clone();
        if let Some(w) = &self.features {
            let g = lock(&w.state);
            s.feature_rows = g.rows;
            s.window_gaps = g.gaps;
            s.features_ms = g.compute_ms;
            s.put_features_ms = g.put_ms;
        }
        s
    }

    /// Wait for all pending features to be stored and return the final
    /// counters.
    pub fn close(mut self) -> Result<Stats, Error> {
        let stats = self.finish_features()?;
        Ok(stats)
    }

    fn finish_features(&mut self) -> Result<Stats, Error> {
        let Some(w) = self.features.take() else {
            return Ok(self.stats.clone());
        };
        drop(w.tx);
        w.thread
            .join()
            .map_err(|_| Error::from("feature thread panicked"))?;
        let g = lock(&w.state);
        if let Some(e) = &g.error {
            return Err(format!("features: {e}").into());
        }
        let mut s = self.stats.clone();
        s.feature_rows = g.rows;
        s.window_gaps = g.gaps;
        s.features_ms = g.compute_ms;
        s.put_features_ms = g.put_ms;
        Ok(s)
    }

    /// Accept and serve `connections` connections in turn (or forever if
    /// `None`), then return the counters so far. Call [`Ingest::close`] to
    /// wait for the last features.
    pub fn serve(
        &mut self,
        listener: &TcpListener,
        connections: Option<usize>,
    ) -> Result<Stats, Error> {
        let mut served = 0;
        while connections.is_none_or(|n| served < n) {
            let (stream, _) = listener.accept()?;
            self.serve_connection(stream)?;
            served += 1;
        }
        Ok(self.stats())
    }

    /// Serve one connection until the sender closes it.
    pub fn serve_connection(&mut self, stream: TcpStream) -> Result<(), Error> {
        stream.set_nodelay(true)?;
        let (tx, rx) = mpsc::sync_channel::<io::Result<Frame>>(self.cfg.queue);
        let read_half = stream.try_clone()?;
        let reader = std::thread::spawn(move || {
            let mut r = BufReader::with_capacity(1 << 16, read_half);
            loop {
                match Frame::read_from(&mut r) {
                    Ok(Some(f)) => {
                        if tx.send(Ok(f)).is_err() {
                            return;
                        }
                    }
                    Ok(None) => return,
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                }
            }
        });
        let mut acks = BufWriter::new(stream);
        let mut batch = Vec::new();
        let mut deadline = Instant::now() + self.cfg.batch;
        let mut result = Ok(());
        loop {
            let wait = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(wait) {
                Ok(Ok(f)) => {
                    batch.push(f);
                    continue;
                }
                Ok(Err(e)) => {
                    result = Err(e.into());
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {
                    self.flush(&mut batch, &mut acks)?;
                    deadline = Instant::now() + self.cfg.batch;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        self.flush(&mut batch, &mut acks)?;
        let _ = acks.flush();
        let _ = reader.join();
        result
    }

    /// Store, acknowledge and featurise one batch.
    fn flush(&mut self, batch: &mut Vec<Frame>, acks: &mut impl Write) -> Result<(), Error> {
        if batch.is_empty() {
            return Ok(());
        }
        let t0 = Instant::now();
        let r = self.flush_inner(batch, acks);
        self.stats.max_flush_ms = self.stats.max_flush_ms.max(t0.elapsed().as_millis() as u64);
        r
    }

    fn flush_inner(&mut self, batch: &mut Vec<Frame>, acks: &mut impl Write) -> Result<(), Error> {
        let mut keep = Vec::with_capacity(batch.len());
        let mut durable: BTreeMap<u16, u64> = BTreeMap::new();
        for f in batch.drain(..) {
            if !self.specs.contains_key(&f.channel) {
                self.stats.unknown_channel_frames += 1;
                continue;
            }
            let (want_seq, want_sample) = self.next.get(&f.channel).copied().unwrap_or((0, 0));
            if f.seq < want_seq {
                self.stats.duplicate_frames += 1;
                continue;
            }
            if f.seq > want_seq {
                self.stats.missing_frames += f.seq - want_seq;
                self.stats.missing_samples += f.first_sample.saturating_sub(want_sample);
            }
            self.next.insert(
                f.channel,
                (f.seq + 1, f.first_sample + f.samples.len() as u64),
            );
            durable.insert(f.channel, f.seq);
            keep.push(f);
        }
        if keep.is_empty() {
            return Ok(());
        }
        let blocks: Vec<SampleBlock> = keep
            .iter()
            .map(|f| SampleBlock {
                channel: f.channel,
                seq: f.seq,
                first_sample: f.first_sample,
                sample_rate: f.sample_rate,
                samples: f.samples.clone(),
            })
            .collect();
        let t = Instant::now();
        self.store.put_samples(&blocks)?;
        self.stats.put_samples_ms += t.elapsed().as_millis() as u64;
        self.stats.batches += 1;
        self.stats.frames += keep.len() as u64;
        self.stats.samples += keep.iter().map(|f| f.samples.len() as u64).sum::<u64>();
        // Durable: acknowledge. A sender that has gone away is not an error
        // for the store; its data is safe either way.
        let entries: Vec<(u16, u64)> = durable.into_iter().collect();
        let _ = write_ack(acks, &entries).and_then(|()| acks.flush());
        if let Some(w) = &self.features {
            if w.tx.send(keep).is_err() {
                // The feature thread stopped on an error; close() reports it.
            }
        }
        Ok(())
    }
}
