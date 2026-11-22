//! Windows and features: time statistics and FFT band energies per window.
//!
//! Windows are aligned to the channel's absolute sample index: window `w`
//! covers samples `w*N .. (w+1)*N`. A replay after a restart therefore cuts
//! the same windows, and a gap in the input drops only the windows it
//! touches.
//!
//! The FFT runs through a `bytelanes` [`ChannelBackend`], so the same code
//! uses the CPU reference today and a GPU backend once one exists.
//! [`reference`] computes the same features by calling the CPU reference FFT
//! directly; the two must agree exactly ("features equal the CPU
//! reference"). They can, because the FFT is integer arithmetic and the
//! feature sums are exact integers; only the final conversion to `f64` and
//! a square root (both correctly rounded in IEEE 754) are floating point.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytelanes::abi::{BufId, ChannelCmd, ChannelProgram, Completion, Extent, Status, Tag};
use bytelanes::cpu::fft::{fft_frame, output_len};
use bytelanes::{Buffers, ChannelBackend};
use diagsteno_store::FeatureRow;

use crate::config::{ChannelSpec, Kind};
use crate::frame::Frame;

/// One window of raw counts from one channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Window {
    /// The channel.
    pub spec: ChannelSpec,
    /// Window number: covers samples `index*N .. (index+1)*N`.
    pub index: u64,
    /// Raw counts, `N` of them.
    pub samples: Vec<i32>,
}

impl Window {
    /// Index of the first sample.
    pub fn first_sample(&self) -> u64 {
        self.index * self.spec.window_len() as u64
    }
}

#[derive(Debug)]
struct ChanBuf {
    spec: ChannelSpec,
    index: u64,
    samples: Vec<i32>,
}

/// Cuts each channel's frames into aligned windows.
#[derive(Debug)]
pub struct Windower {
    chans: HashMap<u16, ChanBuf>,
    gaps: u64,
}

impl Windower {
    /// A windower for the channels in `layout`.
    pub fn new(layout: &[ChannelSpec]) -> Self {
        let chans = layout
            .iter()
            .map(|&spec| {
                let buf = ChanBuf {
                    spec,
                    index: 0,
                    samples: Vec::with_capacity(spec.window_len()),
                };
                (spec.id, buf)
            })
            .collect();
        Windower { chans, gaps: 0 }
    }

    /// Windows abandoned because samples in them were missing.
    pub fn gaps(&self) -> u64 {
        self.gaps
    }

    /// Feed a frame; returns the windows it completes. Frames for unknown
    /// channels are ignored.
    pub fn push(&mut self, f: &Frame) -> Vec<Window> {
        let mut out = Vec::new();
        let Some(c) = self.chans.get_mut(&f.channel) else {
            return out;
        };
        let n = c.spec.window_len() as u64;
        for (i, &s) in f.samples.iter().enumerate() {
            let at = f.first_sample + i as u64;
            let index = at / n;
            let expected = c.index * n + c.samples.len() as u64;
            if at != expected {
                // A gap, or a jump back (a replay): start again at this
                // sample's window, dropping what was buffered.
                if !c.samples.is_empty() {
                    self.gaps += 1;
                }
                c.samples.clear();
                c.index = index;
                if at % n != 0 {
                    // Mid-window: skip to the next window boundary.
                    c.index = index + 1;
                    continue;
                }
            }
            c.samples.push(s);
            if c.samples.len() as u64 == n {
                out.push(Window {
                    spec: c.spec,
                    index: c.index,
                    samples: std::mem::replace(&mut c.samples, Vec::with_capacity(n as usize)),
                });
                c.index += 1;
            }
        }
        out
    }
}

/// Mean, RMS, min and max of raw counts, from exact integer sums.
pub fn time_stats(samples: &[i32]) -> (f64, f64, i32, i32) {
    let n = samples.len().max(1) as f64;
    let sum: i64 = samples.iter().map(|&s| i64::from(s)).sum();
    let sum_sq: u128 = samples
        .iter()
        .map(|&s| (i64::from(s) * i64::from(s)) as u128)
        .sum();
    let min = samples.iter().copied().min().unwrap_or(0);
    let max = samples.iter().copied().max().unwrap_or(0);
    (sum as f64 / n, (sum_sq as f64 / n).sqrt(), min, max)
}

/// Energy per band: the sum of `re² + im²` over each band's bins, summed
/// exactly in `u128` and converted to `f64` once.
pub fn band_energies(spec: &ChannelSpec, bins: &[(i32, i32)]) -> Vec<f64> {
    spec.band_bins()
        .iter()
        .map(|&(lo, hi)| {
            let e: u128 = bins[lo.min(bins.len())..hi.min(bins.len())]
                .iter()
                .map(|&(r, i)| {
                    let (r, i) = (i64::from(r), i64::from(i));
                    (r * r) as u128 + (i * i) as u128
                })
                .sum();
            e as f64
        })
        .collect()
}

/// FFT input for a window: counts shifted into the FFT's input range.
pub fn fft_input(w: &Window) -> Vec<i32> {
    let shift = w.spec.kind.fft_shift();
    w.samples.iter().map(|&s| s << shift).collect()
}

/// The feature row for a window, given its FFT bins.
pub fn row(w: &Window, bins: &[(i32, i32)]) -> FeatureRow {
    let (mean, rms, min, max) = time_stats(&w.samples);
    FeatureRow {
        channel: w.spec.id,
        kind: w.spec.kind.name().to_string(),
        window: w.index,
        first_sample: w.first_sample(),
        sample_rate: w.spec.rate_hz,
        n: w.samples.len() as u32,
        mean,
        rms,
        min,
        max,
        band_edges_hz: w
            .spec
            .kind
            .band_edges_hz()
            .iter()
            .map(|&e| f64::from(e))
            .collect(),
        band_energy: band_energies(&w.spec, bins),
    }
}

/// Features of `w` computed with the CPU reference FFT directly.
pub fn reference(w: &Window) -> FeatureRow {
    let mut bins = Vec::new();
    fft_frame(w.spec.kind.log2n(), &fft_input(w), &mut bins);
    row(w, &bins)
}

/// Failure to compute features through a backend.
#[derive(Debug, PartialEq, Eq)]
pub enum EngineError {
    /// Loading buffers failed.
    Setup(Status),
    /// The program completed with a non-`Ok` status, or its result failed
    /// the CRC check.
    Completion(Status),
    /// No completion arrived in time.
    Timeout,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Setup(s) => write!(f, "buffer setup failed: {s:?}"),
            EngineError::Completion(s) => write!(f, "fft program failed: {s:?}"),
            EngineError::Timeout => f.write_str("fft program timed out"),
        }
    }
}

impl std::error::Error for EngineError {}

const IN: BufId = BufId(1);
const OUT: BufId = BufId(2);
const TIMEOUT: Duration = Duration::from_secs(30);

/// Computes features through a channel backend.
pub struct Engine<B> {
    backend: B,
    next_tag: u64,
}

impl<B: ChannelBackend + Buffers> Engine<B> {
    /// An engine running FFTs on `backend`.
    pub fn new(backend: B) -> Self {
        Engine {
            backend,
            next_tag: 1,
        }
    }

    /// Features for `windows`, in the same order. Windows of the same kind
    /// go to the backend as one multi-frame FFT program.
    pub fn compute(&mut self, windows: &[Window]) -> Result<Vec<FeatureRow>, EngineError> {
        let mut rows: Vec<Option<FeatureRow>> = vec![None; windows.len()];
        for kind in [Kind::Accel, Kind::Strain] {
            let idx: Vec<usize> = (0..windows.len())
                .filter(|&i| windows[i].spec.kind == kind)
                .collect();
            if idx.is_empty() {
                continue;
            }
            let bins = self.fft(kind.log2n(), idx.iter().map(|&i| &windows[i]))?;
            let per = (1usize << kind.log2n()) / 2 + 1;
            for (k, &i) in idx.iter().enumerate() {
                rows[i] = Some(row(&windows[i], &bins[k * per..(k + 1) * per]));
            }
        }
        Ok(rows.into_iter().flatten().collect())
    }

    fn fft<'a>(
        &mut self,
        log2n: u8,
        windows: impl Iterator<Item = &'a Window>,
    ) -> Result<Vec<(i32, i32)>, EngineError> {
        let mut input = Vec::new();
        let mut frames = 0;
        for w in windows {
            input.extend(fft_input(w).iter().flat_map(|s| s.to_le_bytes()));
            frames += 1;
        }
        let in_len = input.len() as u64;
        let out_len = output_len(log2n, frames);
        self.backend
            .put_host(IN, input)
            .map_err(EngineError::Setup)?;
        self.backend
            .put_host(OUT, vec![0; out_len])
            .map_err(EngineError::Setup)?;
        let prog = ChannelProgram::from_commands(&[ChannelCmd::fft(
            Extent::host(IN, 0, in_len),
            Extent::host(OUT, 0, out_len as u64),
            log2n,
        )])
        .map_err(|e| EngineError::Completion(e.status))?;
        let tag = Tag(self.next_tag);
        self.next_tag += 1;
        let c = self.run(&prog, tag)?;
        let out = self
            .backend
            .get_host(OUT)
            .ok_or(EngineError::Completion(Status::UnknownBuffer))?;
        let result = out
            .get(..c.bytes() as usize)
            .ok_or(EngineError::Completion(Status::OutOfBounds))?;
        bytelanes::verify(&c, result).map_err(EngineError::Completion)?;
        Ok(result
            .chunks_exact(8)
            .map(|b| {
                (
                    i32::from_le_bytes([b[0], b[1], b[2], b[3]]),
                    i32::from_le_bytes([b[4], b[5], b[6], b[7]]),
                )
            })
            .collect())
    }

    fn run(&self, prog: &ChannelProgram, tag: Tag) -> Result<Completion, EngineError> {
        let deadline = Instant::now() + TIMEOUT;
        while self.backend.submit(prog, tag).is_err() {
            if Instant::now() > deadline {
                return Err(EngineError::Timeout);
            }
            std::thread::yield_now();
        }
        let mut out = [Completion::error(Tag(0), 0, Status::DeviceError, 0)];
        loop {
            if self.backend.poll(&mut out) == 1 && out[0].tag() == tag {
                return Ok(out[0]);
            }
            if Instant::now() > deadline {
                return Err(EngineError::Timeout);
            }
            std::thread::yield_now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::default_layout;
    use crate::synth::Synth;
    use bytelanes::cpu::CpuBackend;

    #[test]
    fn windows_are_aligned_and_complete() {
        let layout = default_layout();
        let mut w = Windower::new(&layout);
        let mut windows = Vec::new();
        // 2 s of data: 12 accel windows (4096 of 51,200) and 1 strain window
        // (1024 of 2,000) per channel.
        for f in Synth::new(&layout, 1, Some(200)) {
            windows.extend(w.push(&f));
        }
        let accel = windows
            .iter()
            .filter(|x| x.spec.kind == Kind::Accel)
            .count();
        let strain = windows
            .iter()
            .filter(|x| x.spec.kind == Kind::Strain)
            .count();
        assert_eq!(accel, 32 * 12);
        assert_eq!(strain, 32);
        assert_eq!(w.gaps(), 0);
        for x in &windows {
            assert_eq!(x.samples.len(), x.spec.window_len());
        }
    }

    #[test]
    fn a_gap_drops_only_the_windows_it_touches() {
        let layout = default_layout();
        let strain0 = layout[32];
        let mut w = Windower::new(&[strain0]);
        let frames: Vec<Frame> = Synth::new(&[strain0], 3, Some(400))
            .filter(|f| f.seq != 150) // lose 10 samples in window 1 (1024..2048)
            .collect();
        let windows: Vec<u64> = frames
            .iter()
            .flat_map(|f| w.push(f))
            .map(|x| x.index)
            .collect();
        // 4,000 samples: windows 0, 1, 2 complete; window 1 lost to the gap.
        assert_eq!(windows, vec![0, 2]);
        assert_eq!(w.gaps(), 1);
    }

    #[test]
    fn engine_features_equal_the_reference() {
        let layout = default_layout();
        let mut w = Windower::new(&layout);
        let windows: Vec<Window> = Synth::new(&layout, 9, Some(110))
            .flat_map(|f| w.push(&f))
            .collect();
        assert!(!windows.is_empty());
        let mut engine = Engine::new(CpuBackend::new());
        let got = engine.compute(&windows).unwrap();
        let want: Vec<FeatureRow> = windows.iter().map(reference).collect();
        assert_eq!(got, want);
        // The tones show up as band energy.
        assert!(got.iter().all(|r| r.band_energy.iter().any(|&e| e > 0.0)));
    }

    #[test]
    fn time_stats_are_exact() {
        let (mean, rms, min, max) = time_stats(&[3, -4, 3, -4]);
        assert_eq!((mean, rms, min, max), (-0.5, 12.5f64.sqrt(), -4, 3));
    }
}
