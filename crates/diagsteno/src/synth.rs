//! Synthetic strain and vibration data, identical on every machine.
//!
//! Each channel is a sum of three tones plus noise (and, for strain, a
//! static offset and a slow drift). Tones come from integer phase
//! accumulators and the FFT's Q30 sine table, and noise from SplitMix64, so
//! no floating point is involved: a seed gives the same bytes everywhere.
//! A recorded dataset is therefore fully described by its seed, layout and
//! length.

use bytelanes::conformance::SplitMix64;
use bytelanes::cpu::fft::twiddles;

use crate::config::{ChannelSpec, Kind, TICK_MS};
use crate::frame::Frame;

/// `sin` of a 32-bit phase (a full turn is 2^32), in Q30.
pub fn sin_q30(phase: u32) -> i64 {
    let t = (phase >> 16) as usize; // 65,536 steps per turn
    let w = twiddles(); // W[t] = (cos, -sin) for t in 0..32,768
    if t < 32_768 {
        -i64::from(w[t].1)
    } else {
        i64::from(w[t - 32_768].1)
    }
}

#[derive(Clone, Debug)]
struct Tone {
    phase: u32,
    step: u32,
    amp: i64,
}

/// Generator for one channel.
#[derive(Clone, Debug)]
pub struct ChannelSynth {
    spec: ChannelSpec,
    rng: SplitMix64,
    tones: Vec<Tone>,
    noise: i64,
    offset: i64,
    drift: Tone,
    next_sample: u64,
    next_seq: u64,
}

impl ChannelSynth {
    /// A generator for `spec`, seeded by `seed` and the channel id.
    pub fn new(spec: ChannelSpec, seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ (u64::from(spec.id) << 48) ^ 0x6469_6167);
        let limit = i64::from(spec.kind.count_limit());
        let rate = u64::from(spec.rate_hz);
        // Tone frequencies in Hz, chosen per kind, as a 32-bit phase step.
        let tone = |lo_hz: u64, hi_hz: u64, amp: i64, rng: &mut SplitMix64| {
            let hz = lo_hz + rng.below(hi_hz - lo_hz);
            Tone {
                phase: rng.next_u64() as u32,
                step: ((hz << 32) / rate) as u32,
                amp: amp / 2 + rng.below(amp as u64 / 2) as i64,
            }
        };
        let (tones, noise, offset, drift) = match spec.kind {
            Kind::Accel => (
                vec![
                    tone(20, 200, limit / 4, &mut rng),        // shaft rate
                    tone(200, 2_000, limit / 6, &mut rng),     // gear mesh
                    tone(2_000, 10_000, limit / 10, &mut rng), // bearing
                ],
                limit / 50,
                0,
                Tone {
                    phase: 0,
                    step: 0,
                    amp: 0,
                },
            ),
            Kind::Strain => (
                vec![
                    tone(1, 5, limit / 8, &mut rng),
                    tone(5, 50, limit / 12, &mut rng),
                    tone(50, 200, limit / 20, &mut rng),
                ],
                limit / 200,
                rng.below(limit as u64 / 4) as i64 - limit / 8,
                // Drift: one cycle per ~100 s.
                Tone {
                    phase: rng.next_u64() as u32,
                    step: ((1u64 << 32) / (rate * 100)) as u32,
                    amp: limit / 16,
                },
            ),
        };
        ChannelSynth {
            spec,
            rng,
            tones,
            noise,
            offset,
            drift,
            next_sample: 0,
            next_seq: 0,
        }
    }

    /// The channel this generator produces.
    pub fn spec(&self) -> ChannelSpec {
        self.spec
    }

    fn sample(&mut self) -> i32 {
        let mut v = self.offset;
        for t in self
            .tones
            .iter_mut()
            .chain(std::iter::once(&mut self.drift))
        {
            v += (t.amp * sin_q30(t.phase)) >> 30;
            t.phase = t.phase.wrapping_add(t.step);
        }
        v += self.rng.below(2 * self.noise as u64 + 1) as i64 - self.noise;
        let limit = i64::from(self.spec.kind.count_limit());
        v.clamp(-limit, limit) as i32
    }

    /// The next frame: one tick of samples.
    pub fn next_frame(&mut self) -> Frame {
        let n = self.spec.samples_per_tick(TICK_MS) as usize;
        let samples: Vec<i32> = (0..n).map(|_| self.sample()).collect();
        let f = Frame {
            channel: self.spec.id,
            seq: self.next_seq,
            first_sample: self.next_sample,
            sample_rate: self.spec.rate_hz,
            width: self.spec.kind.wire_width(),
            samples,
        };
        self.next_seq += 1;
        self.next_sample += n as u64;
        f
    }
}

/// Generator for a whole layout: yields frames tick by tick, channels in
/// layout order within a tick.
#[derive(Clone, Debug)]
pub struct Synth {
    channels: Vec<ChannelSynth>,
    tick: u64,
    ticks: Option<u64>,
    next: usize,
}

impl Synth {
    /// A generator for `layout` with `seed`, producing `ticks` ticks (or
    /// without end if `None`).
    pub fn new(layout: &[ChannelSpec], seed: u64, ticks: Option<u64>) -> Self {
        Synth {
            channels: layout.iter().map(|&c| ChannelSynth::new(c, seed)).collect(),
            tick: 0,
            ticks,
            next: 0,
        }
    }

    /// The tick the next frame belongs to.
    pub fn tick(&self) -> u64 {
        self.tick
    }
}

impl Iterator for Synth {
    type Item = Frame;

    fn next(&mut self) -> Option<Frame> {
        if self.channels.is_empty() || self.ticks.is_some_and(|t| self.tick >= t) {
            return None;
        }
        let f = self.channels[self.next].next_frame();
        self.next += 1;
        if self.next == self.channels.len() {
            self.next = 0;
            self.tick += 1;
        }
        Some(f)
    }
}

/// Ticks in `seconds` seconds.
pub const fn ticks_for_seconds(seconds: u64) -> u64 {
    seconds * 1000 / TICK_MS as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::default_layout;

    #[test]
    fn sine_table_lookup() {
        assert_eq!(sin_q30(0), 0);
        assert_eq!(sin_q30(1 << 30), 1 << 30); // quarter turn
        assert_eq!(sin_q30(1 << 31), 0); // half turn
        assert_eq!(sin_q30(3 << 30), -(1 << 30)); // three quarters
    }

    #[test]
    fn frames_are_deterministic_contiguous_and_in_range() {
        let layout = default_layout();
        let a: Vec<Frame> = Synth::new(&layout, 42, Some(3)).collect();
        let b: Vec<Frame> = Synth::new(&layout, 42, Some(3)).collect();
        assert_eq!(a, b);
        assert_eq!(a.len(), 3 * 64);
        assert_ne!(a, Synth::new(&layout, 43, Some(3)).collect::<Vec<_>>());
        for f in &a {
            let spec = layout[usize::from(f.channel)];
            assert_eq!(f.samples.len() as u32, spec.samples_per_tick(TICK_MS));
            assert_eq!(f.first_sample, f.seq * f.samples.len() as u64);
            let lim = spec.kind.count_limit();
            assert!(f.samples.iter().all(|s| (-lim..=lim).contains(s)));
            // Encodable at its wire width.
            let _ = f.encode();
        }
    }

    /// Pins the generator's output so an accidental change shows up: the
    /// recorded M7 dataset is defined by this generator and its seed.
    #[test]
    fn generator_output_is_pinned() {
        let frames: Vec<Frame> = Synth::new(&default_layout(), 7, Some(2)).collect();
        let bytes: Vec<u8> = frames.iter().flat_map(|f| f.encode()).collect();
        assert_eq!(
            crc32c::crc32c(&bytes),
            PINNED_CRC,
            "generator output changed"
        );
    }

    const PINNED_CRC: u32 = 1_372_242_029;
}
