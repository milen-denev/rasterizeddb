//! Positional file I/O on every platform this crate builds for.
//!
//! `std::os::unix::fs::FileExt` has `read_exact_at` / `write_all_at`; Windows
//! has only `seek_read` / `seek_write`, which may transfer fewer bytes than
//! asked and also move the handle's cursor. This trait gives both the Unix
//! names, so call sites read the same everywhere:
//! `use crate::core::pos_io::FileExt;`.
//!
//! On Windows the cursor moves as a side effect. Every caller of this trait
//! addresses the file only by offset, never through `Read`/`Write` after a
//! positional call on the same handle, so that side effect is unobserved.

use std::fs::File;
use std::io;

pub trait FileExt {
    /// Reads exactly `buf.len()` bytes starting at `offset`.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
    /// Writes all of `buf` starting at `offset`.
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
}

#[cfg(unix)]
impl FileExt for File {
    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(self, buf, offset)
    }

    #[inline]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        std::os::unix::fs::FileExt::write_all_at(self, buf, offset)
    }
}

#[cfg(windows)]
impl FileExt for File {
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        use std::os::windows::fs::FileExt as _;
        while !buf.is_empty() {
            match self.seek_read(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer"));
                }
                Ok(n) => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                // ERROR_HANDLE_EOF: a read that starts at or past the end.
                Err(e) if e.raw_os_error() == Some(38) => {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer"));
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        use std::os::windows::fs::FileExt as _;
        while !buf.is_empty() {
            match self.seek_write(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write whole buffer"));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::FileExt;

    #[test]
    fn positional_round_trip_and_short_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pos_io");
        let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
        file.write_all_at(b"world", 6).unwrap();
        file.write_all_at(b"hello ", 0).unwrap();
        let mut buf = [0u8; 11];
        file.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello world");
        let mut past = [0u8; 4];
        let err = file.read_exact_at(&mut past, 9).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        let err = file.read_exact_at(&mut past, 100).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
