//! A sender: streams frames to an ingest endpoint and collects
//! acknowledgements. Used by the `send` command and the tests.
//!
//! In real-time mode, tick `k` (one frame per channel) is sent at
//! `start + k * TICK_MS`. A tick is "late" if it could not be sent before
//! the next tick was due; with an ingest that keeps up there are none.

use std::collections::HashMap;
use std::io::{self, BufWriter, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::TICK_MS;
use crate::frame::{Frame, read_ack};

/// What happened during a send.
#[derive(Clone, Debug, Default)]
pub struct SendReport {
    /// Frames sent.
    pub frames: u64,
    /// Samples sent.
    pub samples: u64,
    /// Ticks sent.
    pub ticks: u64,
    /// Ticks sent after the following tick was already due.
    pub late_ticks: u64,
    /// Highest acknowledged sequence number per channel.
    pub acked: HashMap<u16, u64>,
    /// Acknowledgement delay per acknowledged tick: from sending the tick to
    /// receiving its acknowledgement (in microseconds), sorted.
    pub ack_delay_us: Vec<u64>,
    /// Wall-clock time spent writing frames.
    pub send_elapsed: Duration,
    /// Wall-clock time of the send, until the last acknowledgement.
    pub elapsed: Duration,
    /// Why the send stopped early, if it did (for example, the ingest
    /// process died). The other fields describe what happened up to then.
    pub error: Option<String>,
}

impl SendReport {
    /// The `p`-th percentile (0 to 100) of acknowledgement delay, in
    /// microseconds.
    pub fn ack_delay_percentile(&self, p: f64) -> Option<u64> {
        if self.ack_delay_us.is_empty() {
            return None;
        }
        let i = ((p / 100.0) * (self.ack_delay_us.len() - 1) as f64).round() as usize;
        self.ack_delay_us.get(i).copied()
    }
}

#[derive(Default)]
struct AckState {
    acked: HashMap<u16, u64>,
    delays: Vec<u64>,
}

/// Send `frames` (tick-major: `per_tick` frames per tick, sequence number =
/// tick) to `addr`. With `realtime`, pace ticks at [`TICK_MS`]; otherwise
/// send as fast as the connection takes them. Returns after the ingest side
/// closes the connection, so every acknowledgement has been read. If the
/// connection fails part way, the report says so in `error` and still lists
/// what was acknowledged; only a failure to connect is an `Err`.
pub fn send(
    addr: impl ToSocketAddrs,
    frames: impl IntoIterator<Item = Frame>,
    per_tick: usize,
    realtime: bool,
) -> io::Result<SendReport> {
    send_observed(
        addr,
        frames,
        per_tick,
        realtime,
        &Arc::new(AtomicU64::new(0)),
    )
}

/// [`send`], also counting acknowledged ticks into `acked_ticks` as
/// acknowledgements arrive, so another thread can watch progress.
pub fn send_observed(
    addr: impl ToSocketAddrs,
    frames: impl IntoIterator<Item = Frame>,
    per_tick: usize,
    realtime: bool,
    acked_ticks: &Arc<AtomicU64>,
) -> io::Result<SendReport> {
    let stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true)?;
    let tick_sent: Arc<Mutex<Vec<Instant>>> = Arc::new(Mutex::new(Vec::new()));
    let state = Arc::new(Mutex::new(AckState::default()));
    let reader = {
        let mut r = io::BufReader::new(stream.try_clone()?);
        let tick_sent = Arc::clone(&tick_sent);
        let state = Arc::clone(&state);
        let progress = Arc::clone(acked_ticks);
        std::thread::spawn(move || -> io::Result<()> {
            while let Some(entries) = read_ack(&mut r)? {
                let now = Instant::now();
                let sent = tick_sent.lock().unwrap_or_else(|e| e.into_inner());
                let mut st = state.lock().unwrap_or_else(|e| e.into_inner());
                let mut ticks = Vec::new();
                for (ch, seq) in entries {
                    let prev = st.acked.insert(ch, seq);
                    // Each acknowledged tick's delay is counted once, on the
                    // first channel that acknowledges it.
                    let from = prev.map_or(0, |p| p + 1);
                    if ch == 0 {
                        ticks.extend(from..=seq);
                    }
                }
                progress.fetch_add(ticks.len() as u64, Ordering::Relaxed);
                for t in ticks {
                    if let Some(s) = sent.get(t as usize) {
                        st.delays.push(now.duration_since(*s).as_micros() as u64);
                    }
                }
            }
            Ok(())
        })
    };

    let mut report = SendReport::default();
    let start = Instant::now();
    let write_result = write_frames(
        &stream,
        frames,
        per_tick,
        realtime,
        &tick_sent,
        &mut report,
        start,
    );
    report.send_elapsed = start.elapsed();
    let _ = stream.shutdown(Shutdown::Write);
    let ack_result = reader.join();
    report.elapsed = start.elapsed();
    let st = state.lock().unwrap_or_else(|e| e.into_inner());
    report.acked = st.acked.clone();
    report.ack_delay_us = st.delays.clone();
    report.ack_delay_us.sort_unstable();
    if let Err(e) = write_result {
        report.error = Some(format!("send: {e}"));
    }
    match ack_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            report.error.get_or_insert(format!("acks: {e}"));
        }
        Err(_) => {
            report
                .error
                .get_or_insert("ack reader panicked".to_string());
        }
    }
    Ok(report)
}

fn write_frames(
    stream: &TcpStream,
    frames: impl IntoIterator<Item = Frame>,
    per_tick: usize,
    realtime: bool,
    tick_sent: &Mutex<Vec<Instant>>,
    report: &mut SendReport,
    start: Instant,
) -> io::Result<()> {
    let mut w = BufWriter::with_capacity(1 << 16, stream.try_clone()?);
    let tick = Duration::from_millis(u64::from(TICK_MS));
    let mut in_tick = 0;
    for f in frames {
        if in_tick == 0 {
            let k = report.ticks;
            if realtime {
                let due = start + tick * k as u32;
                let now = Instant::now();
                if now < due {
                    std::thread::sleep(due - now);
                } else if now > due + tick {
                    report.late_ticks += 1;
                }
            }
            tick_sent
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(Instant::now());
        }
        report.frames += 1;
        report.samples += f.samples.len() as u64;
        w.write_all(&f.encode())?;
        in_tick += 1;
        if in_tick == per_tick {
            in_tick = 0;
            report.ticks += 1;
            w.flush()?;
        }
    }
    w.flush()
}
