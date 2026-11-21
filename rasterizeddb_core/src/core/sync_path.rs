//! Filesystem calls whose Unix habits do not carry over to Windows: flushing
//! a file or a directory by its path, a file's identity, a directory entry's
//! length.
//!
//! - **`DirEntry::metadata` for a length.** On Windows it comes from the
//!   directory listing, which NTFS updates lazily for a file someone has
//!   open, so a file being written reads short. [`entry_metadata`] asks the
//!   file there.
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

/// `entry`'s metadata, its length current: the file's own on Windows, where
/// the listing's lags a file that is open (see the module docs).
pub fn entry_metadata(entry: &std::fs::DirEntry) -> io::Result<std::fs::Metadata> {
    #[cfg(windows)]
    {
        std::fs::metadata(entry.path())
    }
    #[cfg(not(windows))]
    {
        entry.metadata()
    }
}

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

/// Renames `from` to `to`. On Windows a directory with a file open inside
/// cannot be renamed (access denied, or a sharing violation), and a database
/// just detached still has tasks letting go of its files: the rename is
/// retried there for up to five seconds. Linux renames it at once.
pub fn rename_when_released(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        let mut tries = 0;
        loop {
            match std::fs::rename(from, to) {
                Err(e)
                    if tries < 50
                        && (e.kind() == io::ErrorKind::PermissionDenied || e.raw_os_error() == Some(32)) =>
                {
                    tries += 1;
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                done => return done,
            }
        }
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(from, to)
    }
}

/// The volume serial number and the file index of the file or directory at
/// `path`: who it is on Windows, kept through a rename, never shared by two
/// files on a volume at once. Read through a handle that asks for no access
/// (a directory opens only with `FILE_FLAG_BACKUP_SEMANTICS`).
#[cfg(windows)]
pub fn volume_and_index(path: &Path) -> io::Result<(u64, u64)> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    #[repr(C)]
    #[allow(non_snake_case)]
    struct ByHandleFileInformation {
        dwFileAttributes: u32,
        ftCreationTime: [u32; 2],
        ftLastAccessTime: [u32; 2],
        ftLastWriteTime: [u32; 2],
        dwVolumeSerialNumber: u32,
        nFileSizeHigh: u32,
        nFileSizeLow: u32,
        nNumberOfLinks: u32,
        nFileIndexHigh: u32,
        nFileIndexLow: u32,
    }
    unsafe extern "system" {
        fn GetFileInformationByHandle(file: *mut std::ffi::c_void, info: *mut ByHandleFileInformation) -> i32;
    }
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    let file = File::options().access_mode(0).custom_flags(FILE_FLAG_BACKUP_SEMANTICS).open(path)?;
    // SAFETY: the handle is open for the call and `info` is a plain struct
    // the call fills.
    let mut info: ByHandleFileInformation = unsafe { std::mem::zeroed() };
    if unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let index = ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64;
    Ok((info.dwVolumeSerialNumber as u64, index))
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
