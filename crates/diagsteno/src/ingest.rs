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
//! 4. cuts windows and stores their features.
//!
//! A frame is acknowledged only after step 2 returns (R8). On start the
//! store is read to recover each channel's last durable sequence number, so
//! drop counting continues correctly across a restart.

use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc::{self, RecvTimeoutError};
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
    /// Defaults: 100 ms batches, an 8,192-frame queue.
    pub fn new(store: impl Into<PathBuf>, layout: Vec<ChannelSpec>) -> Self {
        IngestConfig {
            store: store.into(),
            layout,
            batch: Duration::from_millis(100),
            queue: 8192,
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
}

/// An ingest endpoint bound to one store.
pub struct Ingest {
    cfg: IngestConfig,
    store: Store,
    specs: HashMap<u16, ChannelSpec>,
    /// Per channel: next expected sequence number and its first sample.
    next: HashMap<u16, (u64, u64)>,
    windower: Windower,
    engine: Engine<CpuBackend>,
    stats: Stats,
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
        Ok(Ingest {
            specs: cfg.layout.iter().map(|c| (c.id, *c)).collect(),
            windower: Windower::new(&cfg.layout),
            engine: Engine::new(CpuBackend::new()),
            cfg,
            store,
            next,
            stats: Stats::default(),
        })
    }

    /// Counters so far.
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Accept and serve `connections` connections in turn (or forever if
    /// `None`), then return the counters.
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
        Ok(self.stats.clone())
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
        self.store.put_samples(&blocks)?;
        self.stats.batches += 1;
        self.stats.frames += keep.len() as u64;
        self.stats.samples += keep.iter().map(|f| f.samples.len() as u64).sum::<u64>();
        // Durable: acknowledge. A sender that has gone away is not an error
        // for the store; its data is safe either way.
        let entries: Vec<(u16, u64)> = durable.into_iter().collect();
        let _ = write_ack(acks, &entries).and_then(|()| acks.flush());
        let windows: Vec<_> = keep.iter().flat_map(|f| self.windower.push(f)).collect();
        self.stats.window_gaps = self.windower.gaps();
        if !windows.is_empty() {
            let rows = self.engine.compute(&windows)?;
            self.store.put_features(&rows)?;
            self.stats.feature_rows += rows.len() as u64;
        }
        Ok(())
    }
}
