//! Sideband-64k multiplex helper for upload-pack responses.
//!
//! Wraps a `Write` so that any bytes written to it are emitted as pkt-lines
//! prefixed with band byte `1` (data). Up to 65515 bytes per pkt-line.

use std::io::{self, Write};

const BAND_DATA: u8 = 1;

/// Maximum payload bytes per pkt-line, after the 4-byte length prefix and
/// the 1-byte band ID. (65520 max line length - 4 length prefix - 1 band)
const MAX_PAYLOAD: usize = 65515;

/// `Write` adapter that frames every chunk into a band-1 pkt-line.
pub(crate) struct SidebandWriter<W: Write> {
    inner: W,
}

impl<W: Write> SidebandWriter<W> {
    pub(crate) fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl<W: Write> Write for SidebandWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let n = buf.len().min(MAX_PAYLOAD);
        let chunk = &buf[..n];
        let line_len = chunk.len() + 4 + 1; // 4 hdr + 1 band + payload
        write!(self.inner, "{:04x}", line_len)?;
        self.inner.write_all(&[BAND_DATA])?;
        self.inner.write_all(chunk)?;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
