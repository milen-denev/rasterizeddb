//! Flushing a file or a directory named by its path, on every platform.
//!
//! Two Unix habits do not carry over to Windows:
//!
//! - **`File::open(path)?.sync_all()`.** `fsync` takes any descriptor, but
//!   Windows' `FlushFileBuffers` needs a handle with write access, and on a
//!   read-only one fails with "Access is denied". [`sync_file`] opens the
//!   handle writable there.
//! - **Syncing a directory.** `File::open(dir)` fails on Windows: a directory
//!   opens only with `FILE_FLAG_BACKUP_SEMANTICS`. [`sync_dir`] opens it that
//!   way, writable, and flushes it, which on NTFS commits the directory's
//!   entries. Where the volume does not support that (the open or the flush
//!   is refused), it returns `Ok(false)` having flushed nothing, as
//!   PostgreSQL does there: NTFS journals metadata in order, so the entries
//!   become durable with the next flush that commits the journal.
//!
//! Callers that note a sync for the power-loss harness note it only when one
//! happened ([`open_dir`] first, note, then [`flush_dir`]), so the harness
//! never takes a flush that did not happen for a durable directory.

use std::fs::File;
use std::io;
use std::path::Path;

/// Flushes the file at `path`: its bytes and its metadata.
pub fn sync_file(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    let file = File::options().read(true).write(true).open(path)?;
    #[cfg(not(windows))]
    let file = File::open(path)?;
    file.sync_all()
}

/// A handle `dir` can be flushed through, or `None` where this platform or
/// volume cannot flush a directory.
pub fn open_dir(dir: &Path) -> io::Result<Option<File>> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        match File::options().read(true).write(true).custom_flags(FILE_FLAG_BACKUP_SEMANTICS).open(dir) {
            Ok(f) => Ok(Some(f)),
            Err(e) if unsupported(&e) => Ok(None),
            Err(e) => Err(e),
        }
    }
    #[cfg(not(windows))]
    {
        File::open(dir).map(Some)
    }
}

/// Flushes a directory handle from [`open_dir`]. `Ok(false)` when the
/// volume refused the flush as unsupported.
pub fn flush_dir(handle: &File) -> io::Result<bool> {
    match handle.sync_all() {
        Ok(()) => Ok(true),
        Err(e) if cfg!(windows) && unsupported(&e) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Flushes the entries of directory `dir`. `Ok(true)` when it did,
/// `Ok(false)` where the platform cannot (see the module docs).
pub fn sync_dir(dir: &Path) -> io::Result<bool> {
    match open_dir(dir)? {
        Some(handle) => flush_dir(&handle),
        None => Ok(false),
    }
}

/// The errors a volume answers a directory flush with when it does not
/// support one: access denied (the handle cannot be had writable, or the
/// flush is refused) and ERROR_INVALID_FUNCTION.
#[cfg_attr(not(windows), allow(dead_code))]
fn unsupported(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::PermissionDenied
        || e.kind() == io::ErrorKind::Unsupported
        || (cfg!(windows) && e.raw_os_error() == Some(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_and_its_directory_flush_by_path() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f.db");
        std::fs::write(&file, b"bytes").unwrap();
        sync_file(&file).unwrap();
        let flushed = sync_dir(dir.path()).unwrap();
        // Unix always flushes a directory. On Windows the answer is the
        // volume's: printed, so CI shows which model the crash tests ran.
        if cfg!(unix) {
            assert!(flushed);
        }
        println!("directory flush supported here: {flushed}");
    }

    #[test]
    fn a_missing_path_is_an_error_not_a_flush() {
        let dir = tempfile::tempdir().unwrap();
        assert!(sync_file(&dir.path().join("absent")).is_err());
        assert!(sync_dir(&dir.path().join("absent")).is_err());
    }
}
