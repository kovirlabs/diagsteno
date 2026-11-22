//! The ingest wire format: sample frames in, acknowledgements out.
//!
//! A frame carries one tick of samples from one channel:
//!
//! | Bytes | Field |
//! | --- | --- |
//! | 0..4 | magic `DSF1` |
//! | 4 | version, 1 |
//! | 5 | sample width in bytes: 2 (`i16`) or 4 (`i32`) |
//! | 6..8 | channel, little-endian `u16` |
//! | 8..16 | frame sequence number within the channel, from 0 |
//! | 16..24 | index of the first sample within the channel |
//! | 24..28 | sample rate in Hz |
//! | 28..32 | sample count |
//! | 32.. | samples, little-endian, `count * width` bytes |
//! | last 4 | CRC-32C of every byte before it |
//!
//! An acknowledgement lists, for each channel in a stored batch, the highest
//! frame sequence number now durable in the store:
//!
//! | Bytes | Field |
//! | --- | --- |
//! | 0..4 | magic `DSA1` |
//! | 4..8 | entry count |
//! | 8.. | entries: channel `u16`, sequence `u64` (10 bytes each) |
//! | last 4 | CRC-32C of every byte before it |

use std::io::{self, Read, Write};

use crc32c::{crc32c as checksum, crc32c_append};

const FRAME_MAGIC: [u8; 4] = *b"DSF1";
const ACK_MAGIC: [u8; 4] = *b"DSA1";
const HEADER: usize = 32;
/// Largest sample count a frame may carry.
pub const MAX_SAMPLES: u32 = 1 << 20;

/// One tick of samples from one channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    /// Channel id.
    pub channel: u16,
    /// Frame sequence number within the channel.
    pub seq: u64,
    /// Index of the first sample within the channel.
    pub first_sample: u64,
    /// Samples per second.
    pub sample_rate: u32,
    /// Bytes per sample on the wire: 2 or 4.
    pub width: u8,
    /// Raw counts. With width 2 each must fit in `i16`.
    pub samples: Vec<i32>,
}

/// A malformed frame or acknowledgement.
#[derive(Debug, PartialEq, Eq)]
pub enum WireError {
    /// Wrong magic bytes.
    Magic,
    /// Unknown version or sample width.
    Version,
    /// Sample count above [`MAX_SAMPLES`].
    TooLarge,
    /// CRC-32C mismatch.
    Crc,
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            WireError::Magic => "bad magic",
            WireError::Version => "unknown version or sample width",
            WireError::TooLarge => "too many samples",
            WireError::Crc => "CRC-32C mismatch",
        })
    }
}

impl std::error::Error for WireError {}

fn invalid(e: WireError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

impl Frame {
    /// Encode to wire bytes.
    ///
    /// # Panics
    ///
    /// If `width` is not 2 or 4, or a sample does not fit in `width` bytes.
    pub fn encode(&self) -> Vec<u8> {
        let w = usize::from(self.width);
        assert!(w == 2 || w == 4, "frame width must be 2 or 4");
        let mut b = Vec::with_capacity(HEADER + self.samples.len() * w + 4);
        b.extend_from_slice(&FRAME_MAGIC);
        b.push(1);
        b.push(self.width);
        b.extend_from_slice(&self.channel.to_le_bytes());
        b.extend_from_slice(&self.seq.to_le_bytes());
        b.extend_from_slice(&self.first_sample.to_le_bytes());
        b.extend_from_slice(&self.sample_rate.to_le_bytes());
        b.extend_from_slice(&(self.samples.len() as u32).to_le_bytes());
        for &s in &self.samples {
            if w == 2 {
                let s = i16::try_from(s).expect("sample fits in i16");
                b.extend_from_slice(&s.to_le_bytes());
            } else {
                b.extend_from_slice(&s.to_le_bytes());
            }
        }
        let crc = checksum(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        b
    }

    /// Read one frame. Returns `Ok(None)` at a clean end of stream (no bytes
    /// of a new frame read).
    pub fn read_from(r: &mut impl Read) -> io::Result<Option<Frame>> {
        let mut head = [0u8; HEADER];
        if !read_exact_or_eof(r, &mut head)? {
            return Ok(None);
        }
        if head[0..4] != FRAME_MAGIC {
            return Err(invalid(WireError::Magic));
        }
        let width = head[5];
        if head[4] != 1 || (width != 2 && width != 4) {
            return Err(invalid(WireError::Version));
        }
        let le64 = |i: usize| u64::from_le_bytes(head[i..i + 8].try_into().unwrap_or([0; 8]));
        let le32 = |i: usize| u32::from_le_bytes(head[i..i + 4].try_into().unwrap_or([0; 4]));
        let count = le32(28);
        if count > MAX_SAMPLES {
            return Err(invalid(WireError::TooLarge));
        }
        let mut body = vec![0u8; count as usize * usize::from(width) + 4];
        r.read_exact(&mut body)?;
        let (payload, crc) = body.split_at(body.len() - 4);
        let want = u32::from_le_bytes([crc[0], crc[1], crc[2], crc[3]]);
        let got = crc32c_append(checksum(&head), payload);
        if got != want {
            return Err(invalid(WireError::Crc));
        }
        let samples = if width == 2 {
            payload
                .chunks_exact(2)
                .map(|c| i32::from(i16::from_le_bytes([c[0], c[1]])))
                .collect()
        } else {
            payload
                .chunks_exact(4)
                .map(|c| i32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect()
        };
        Ok(Some(Frame {
            channel: u16::from_le_bytes([head[6], head[7]]),
            seq: le64(8),
            first_sample: le64(16),
            sample_rate: le32(24),
            width,
            samples,
        }))
    }
}

/// Fill `buf`, or return `Ok(false)` if the stream ends before any byte.
fn read_exact_or_eof(r: &mut impl Read, buf: &mut [u8]) -> io::Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) if filled == 0 => return Ok(false),
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

/// Write an acknowledgement of `(channel, highest durable seq)` entries.
pub fn write_ack(w: &mut impl Write, entries: &[(u16, u64)]) -> io::Result<()> {
    let mut b = Vec::with_capacity(8 + entries.len() * 10 + 4);
    b.extend_from_slice(&ACK_MAGIC);
    b.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (ch, seq) in entries {
        b.extend_from_slice(&ch.to_le_bytes());
        b.extend_from_slice(&seq.to_le_bytes());
    }
    let crc = checksum(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    w.write_all(&b)
}

/// Read one acknowledgement; `Ok(None)` at a clean end of stream.
pub fn read_ack(r: &mut impl Read) -> io::Result<Option<Vec<(u16, u64)>>> {
    let mut head = [0u8; 8];
    if !read_exact_or_eof(r, &mut head)? {
        return Ok(None);
    }
    if head[0..4] != ACK_MAGIC {
        return Err(invalid(WireError::Magic));
    }
    let n = u32::from_le_bytes([head[4], head[5], head[6], head[7]]);
    if n > 1 << 16 {
        return Err(invalid(WireError::TooLarge));
    }
    let mut body = vec![0u8; n as usize * 10 + 4];
    r.read_exact(&mut body)?;
    let (entries, crc) = body.split_at(body.len() - 4);
    let want = u32::from_le_bytes([crc[0], crc[1], crc[2], crc[3]]);
    if crc32c_append(checksum(&head), entries) != want {
        return Err(invalid(WireError::Crc));
    }
    Ok(Some(
        entries
            .chunks_exact(10)
            .map(|e| {
                let mut s = [0u8; 8];
                s.copy_from_slice(&e[2..10]);
                (u16::from_le_bytes([e[0], e[1]]), u64::from_le_bytes(s))
            })
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(width: u8) -> Frame {
        Frame {
            channel: 33,
            seq: 7,
            first_sample: 70,
            sample_rate: 1000,
            width,
            samples: if width == 2 {
                vec![i16::MIN.into(), -1, 0, i16::MAX.into()]
            } else {
                vec![-(1 << 23), 0, (1 << 23) - 1]
            },
        }
    }

    #[test]
    fn frames_roundtrip_and_end_cleanly() {
        let mut wire = frame(2).encode();
        wire.extend(frame(4).encode());
        let mut r = &wire[..];
        assert_eq!(Frame::read_from(&mut r).unwrap(), Some(frame(2)));
        assert_eq!(Frame::read_from(&mut r).unwrap(), Some(frame(4)));
        assert_eq!(Frame::read_from(&mut r).unwrap(), None);
    }

    #[test]
    fn corruption_is_detected() {
        let good = frame(4).encode();
        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0x10;
            let r = Frame::read_from(&mut &bad[..]);
            assert!(
                r.is_err() || r.as_ref().unwrap().as_ref() != Some(&frame(4)),
                "byte {i}"
            );
        }
        // Truncated mid-frame is an error, not a clean end.
        assert!(Frame::read_from(&mut &good[..good.len() - 1]).is_err());
    }

    #[test]
    fn acks_roundtrip() {
        let mut wire = Vec::new();
        write_ack(&mut wire, &[(0, 5), (63, u64::MAX)]).unwrap();
        write_ack(&mut wire, &[]).unwrap();
        let mut r = &wire[..];
        assert_eq!(
            read_ack(&mut r).unwrap(),
            Some(vec![(0, 5), (63, u64::MAX)])
        );
        assert_eq!(read_ack(&mut r).unwrap(), Some(vec![]));
        assert_eq!(read_ack(&mut r).unwrap(), None);
    }
}
