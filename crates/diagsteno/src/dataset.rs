//! Recorded datasets: frames written back to back to a file, in tick order.
//!
//! Each frame carries its own CRC, so a recorded file is checked as it is
//! read back.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::Path;

use crate::frame::Frame;

/// Write `frames` to `path`; returns the number written.
pub fn write(path: impl AsRef<Path>, frames: impl IntoIterator<Item = Frame>) -> io::Result<u64> {
    let mut w = BufWriter::with_capacity(1 << 20, File::create(path.as_ref())?);
    let mut n = 0;
    for f in frames {
        w.write_all(&f.encode())?;
        n += 1;
    }
    w.into_inner().map_err(|e| e.into_error())?.sync_all()?;
    Ok(n)
}

/// Read every frame from `path`.
pub fn read(path: impl AsRef<Path>) -> io::Result<Vec<Frame>> {
    let mut r = BufReader::with_capacity(1 << 20, File::open(path.as_ref())?);
    let mut out = Vec::new();
    while let Some(f) = Frame::read_from(&mut r)? {
        out.push(f);
    }
    Ok(out)
}
