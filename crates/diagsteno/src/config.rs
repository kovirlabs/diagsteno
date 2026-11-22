//! Channel layout: sensor kinds, rates, window sizes and feature bands.

/// Sensor kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    /// Accelerometer: 16-bit signed counts.
    Accel,
    /// Strain gauge: 24-bit signed counts.
    Strain,
}

impl Kind {
    /// Name stored with features.
    pub const fn name(self) -> &'static str {
        match self {
            Kind::Accel => "accel",
            Kind::Strain => "strain",
        }
    }

    /// Largest magnitude of a raw count.
    pub const fn count_limit(self) -> i32 {
        match self {
            Kind::Accel => i16::MAX as i32,
            Kind::Strain => (1 << 23) - 1,
        }
    }

    /// Bytes per sample on the wire.
    pub const fn wire_width(self) -> u8 {
        match self {
            Kind::Accel => 2,
            Kind::Strain => 4,
        }
    }

    /// Left shift that moves a raw count into the FFT's input range: the
    /// largest count becomes at most 2^29, inside the `±2^30` the FFT takes.
    pub const fn fft_shift(self) -> u32 {
        match self {
            Kind::Accel => 14,
            Kind::Strain => 6,
        }
    }

    /// `log2` of the feature window length in samples.
    pub const fn log2n(self) -> u8 {
        match self {
            // 4096 samples at 25.6 kHz: 160 ms, 6.25 Hz bins.
            Kind::Accel => 12,
            // 1024 samples at 1 kHz: 1.024 s, about 0.98 Hz bins.
            Kind::Strain => 10,
        }
    }

    /// Feature band edges in Hz; band `i` covers `edges[i]..edges[i+1]`.
    pub const fn band_edges_hz(self) -> &'static [u32] {
        match self {
            Kind::Accel => &[10, 100, 500, 1_000, 2_000, 5_000, 12_800],
            Kind::Strain => &[1, 5, 20, 50, 150, 500],
        }
    }
}

/// One input channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ChannelSpec {
    /// Channel id, unique in a layout.
    pub id: u16,
    /// Sensor kind.
    pub kind: Kind,
    /// Samples per second.
    pub rate_hz: u32,
}

impl ChannelSpec {
    /// Samples in one tick of `tick_ms` milliseconds. Rates in a layout are
    /// chosen so this is a whole number.
    pub const fn samples_per_tick(&self, tick_ms: u32) -> u32 {
        self.rate_hz * tick_ms / 1000
    }

    /// Window length in samples.
    pub const fn window_len(&self) -> usize {
        1 << self.kind.log2n()
    }

    /// FFT bin range `lo..hi` for each band: bin `k` is at `k * rate / N` Hz,
    /// and a band takes the bins whose frequency lies in `[lo_hz, hi_hz)`,
    /// with the top band including `hi_hz` itself. Integer arithmetic only.
    pub fn band_bins(&self) -> Vec<(usize, usize)> {
        let n = self.window_len() as u64;
        let rate = u64::from(self.rate_hz);
        let edges = self.kind.band_edges_hz();
        let bin_at_or_above = |hz: u32| -> usize { (u64::from(hz) * n).div_ceil(rate) as usize };
        let nyquist = self.window_len() / 2;
        edges
            .windows(2)
            .enumerate()
            .map(|(i, e)| {
                let lo = bin_at_or_above(e[0]).min(nyquist + 1);
                let mut hi = bin_at_or_above(e[1]).min(nyquist + 1);
                if i == edges.len() - 2 && (u64::from(e[1]) * n) % rate == 0 {
                    hi = (hi + 1).min(nyquist + 1);
                }
                (lo, hi.max(lo))
            })
            .collect()
    }
}

/// Length of one tick: every channel sends one frame per tick.
pub const TICK_MS: u32 = 10;

/// The M7 target layout: 32 accelerometers at 25.6 kHz (channels 0..32) and
/// 32 strain gauges at 1 kHz (channels 32..64), 851,200 samples per second.
pub fn default_layout() -> Vec<ChannelSpec> {
    let accel = (0..32).map(|id| ChannelSpec {
        id,
        kind: Kind::Accel,
        rate_hz: 25_600,
    });
    let strain = (32..64).map(|id| ChannelSpec {
        id,
        kind: Kind::Strain,
        rate_hz: 1_000,
    });
    accel.chain(strain).collect()
}

/// Samples per second across a layout.
pub fn samples_per_second(layout: &[ChannelSpec]) -> u64 {
    layout.iter().map(|c| u64::from(c.rate_hz)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_layout_matches_the_m7_target() {
        let l = default_layout();
        assert_eq!(l.len(), 64);
        assert_eq!(samples_per_second(&l), 32 * 25_600 + 32 * 1_000);
        for c in &l {
            assert_eq!(c.rate_hz * TICK_MS % 1000, 0, "whole samples per tick");
            let shifted = i64::from(c.kind.count_limit() + 1) << c.kind.fft_shift();
            assert!(shifted <= 1 << 29);
        }
    }

    #[test]
    fn band_bins_cover_the_edges() {
        let accel = ChannelSpec {
            id: 0,
            kind: Kind::Accel,
            rate_hz: 25_600,
        };
        // 6.25 Hz bins: 10 Hz -> bin 2 (12.5 Hz), 100 Hz -> bin 16 exactly.
        let b = accel.band_bins();
        assert_eq!(b[0], (2, 16));
        assert_eq!(b[1], (16, 80));
        // Top band ends at Nyquist (12,800 Hz = bin 2048) and includes it.
        assert_eq!(b.last().copied(), Some((800, 2049)));
        let strain = ChannelSpec {
            id: 32,
            kind: Kind::Strain,
            rate_hz: 1_000,
        };
        let b = strain.band_bins();
        assert_eq!(b[0], (2, 6)); // 1 Hz -> bin 1.024 -> 2; 5 Hz -> 5.12 -> 6
        assert_eq!(b.last().copied(), Some((154, 513))); // 500 Hz is bin 512 exactly; included
        for w in b.windows(2) {
            assert_eq!(w[0].1, w[1].0, "bands are contiguous");
        }
    }
}
