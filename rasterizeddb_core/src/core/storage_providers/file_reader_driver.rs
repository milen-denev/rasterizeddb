#![allow(unused_imports)]

use std::{sync::Arc, time::Instant};
 
use compio::{
    buf::IntoInner,
    driver::{
        AsRawFd, OpCode, OwnedFd, Proactor, PushEntry, SharedFd,
        op::{CloseFile, ReadAt},
    },
};

use parking_lot::{RwLock, RwLockWriteGuard};
use std::cell::RefCell;

#[cfg(windows)]
pub fn open_file(driver: &mut Proactor, file_location: String) -> OwnedFd {
    use std::os::windows::{
        fs::OpenOptionsExt,
        io::{FromRawHandle, IntoRawHandle, OwnedHandle},
    };

    use compio::{BufResult, driver::op::Asyncify};

    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_NORMAL, 
        FILE_FLAG_OVERLAPPED, 
        FILE_FLAG_SEQUENTIAL_SCAN, 
        FILE_FLAG_SESSION_AWARE
    };

    let flags = 
        FILE_FLAG_OVERLAPPED | 
        FILE_FLAG_SEQUENTIAL_SCAN | 
        FILE_FLAG_SESSION_AWARE |
        FILE_ATTRIBUTE_NORMAL;

    let op = Asyncify::new(move || {
        BufResult(
            std::fs::OpenOptions::new()
                .read(true)
                .attributes(flags)
                .open(file_location)
                .map(|f| f.into_raw_handle() as usize),
            (),
        )
    });
    let (fd, _) = push_and_wait(driver, op);
    OwnedFd::File(unsafe { OwnedHandle::from_raw_handle(fd as _) })
}

#[cfg(unix)]
pub fn open_file_impl(driver: &mut Proactor, file_location: String) -> OwnedFd {
    use std::{ffi::CString, os::fd::FromRawFd};
    use compio_driver::op::OpenFile;

    let op = OpenFile::new(
        CString::new(file_location).unwrap(),
        libc::O_CLOEXEC | libc::O_RDONLY | libc::O_NOATIME, // Add O_NOATIME for less metadata writes
        0o666,
    );

    let (fd, _) = push_and_wait(driver, op);

    // Optionally: set posix_fadvise for sequential access
    #[cfg(target_os = "linux")]
    unsafe {
        libc::posix_fadvise(fd as i32, 0, 0, libc::POSIX_FADV_SEQUENTIAL);
    }

    unsafe { OwnedFd::from_raw_fd(fd as _) }
}

fn push_and_wait<O: OpCode + 'static>(driver: &mut Proactor, op: O) -> (usize, O) {
    match driver.push(op) {
        PushEntry::Ready(res) => res.unwrap(),
        PushEntry::Pending(mut user_data) => loop {
            driver.poll(None).unwrap();
            match driver.pop(user_data) {
                PushEntry::Pending(k) => user_data = k,
                PushEntry::Ready((res, _)) => break res.unwrap(),
            }
        },
    }
}

pub struct FileReader {
    pub inner: Arc<RwLock<Proactor>>,
    pub fd: Option<SharedFd<OwnedFd>>, // Option so we can take ownership safely
}

unsafe impl Sync for FileReader {}
unsafe impl Send for FileReader {}

impl FileReader {
    pub fn new(location: String) -> Self {
        let mut driver = Proactor::new().unwrap();

        let fd = open_file(&mut driver, location);
        let fd = SharedFd::new(fd);
        driver.attach(fd.as_raw_fd()).unwrap();

        Self {
            inner: Arc::new(RwLock::new(driver)),
            fd: Some(fd),
        }
    }

    pub fn read_at(&self, position: u64, size: usize) -> Vec<u8> {
        let fd = self
            .fd
            .as_ref()
            .expect("File descriptor not available")
            .clone();
        let op = ReadAt::new(fd, position, Vec::with_capacity(size));
        let (n, op) = push_and_wait(&mut self.inner.write(), op);

        let mut buffer = op.into_inner();

        unsafe {
            buffer.set_len(n);
        }

        buffer
    }

    pub fn read_data_into_buffer(&self, position: u64, buffer: &mut [u8]) -> usize {
        if buffer.is_empty() {
            return 0;
        }

        let fd = self
            .fd
            .as_ref()
            .expect("File descriptor not available")
            .clone();

        // Thread-local scratch buffer: avoids locking and reduces allocator churn.
        thread_local! {
            static SCRATCH: RefCell<Vec<u8>> = RefCell::new(Vec::new());
        }

        let needed = buffer.len();
        let n_read = SCRATCH.with(|cell| {
            let mut local = cell.borrow_mut();
            let mut vec = std::mem::take(&mut *local);
            if vec.capacity() < needed {
                // Round capacity up to next power of two to reduce future reallocations.
                let mut cap = 1usize;
                while cap < needed { cap <<= 1; }
                vec.reserve(cap - vec.capacity());
            }

            let op = ReadAt::new(fd, position, vec);
            let (n, op) = push_and_wait(&mut self.inner.write(), op);
            let mut read_buf = op.into_inner();

            unsafe { read_buf.set_len(n); }
            buffer[..n].copy_from_slice(&read_buf[..n]);

            // restore scratch buffer (keep capacity)
            read_buf.clear();
            *local = read_buf;
            n
        });

        n_read
    }

    pub fn close(&mut self) {
        // Take the SharedFd out so we can try to close it explicitly.
        if let Some(fd_shared) = self.fd.take() {
            if let Ok(fd_owned) = fd_shared.try_unwrap() {
                let op = CloseFile::new(fd_owned);
                push_and_wait(&mut self.inner.write(), op);
            } else {
                // Multiple references exist; skip explicit close.
            }
        }
    }
}

impl Drop for FileReader {
    fn drop(&mut self) {
        // Best-effort close on drop; avoid panicking if multiple references exist.
        if let Some(fd_shared) = self.fd.take() {
            let mut guard = self.inner.write();
            close_file(fd_shared, &mut guard);
        }
    }
}

fn close_file(f: SharedFd<OwnedFd>, inner: &mut RwLockWriteGuard<Proactor>) {
    if let Ok(fd_owned) = f.try_unwrap() {
        let op = CloseFile::new(fd_owned);
        push_and_wait(inner, op);
    } else {
        // Multiple references exist; skip explicit close to avoid panic.
        // The file will be closed when the last owner drops.
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;

    use crate::core::storage_providers::file_reader_driver::FileReader;

    fn setup_test_file(file_name: &str, content: &[u8]) -> String {
        let path = format!("./tests/{}", file_name);
        let path_obj = Path::new(&path);
        if path_obj.exists() {
            std::fs::remove_file(&path).unwrap();
        }
        let mut file = File::create(&path).unwrap();
        file.write_all(content).unwrap();
        path
    }

    #[test]
    fn test_file_reader_new() {
        let file_name = "test_file_reader_new.txt";
        let content = b"Hello, RasterizedDB!";
        let path = setup_test_file(file_name, content);

        let _file_reader = FileReader::new(path.clone());
        assert!(Path::new(&path).exists());

        // Cleanup
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_file_reader_read_at() {
        let file_name = "test_file_reader_read_at.txt";
        let content = b"Hello, RasterizedDB!";
        let path = setup_test_file(file_name, content);

        let file_reader = FileReader::new(path.clone());
        let result = file_reader.read_at(0, content.len());

        assert_eq!(result, content);

        // Cleanup
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_file_reader_read_data_into_buffer() {
        let file_name = "test_file_reader_read_data_into_buffer.txt";
        let content = b"Hello, RasterizedDB!";
        let path = setup_test_file(file_name, content);

        let file_reader = FileReader::new(path.clone());
        let mut buffer = vec![0; content.len()];
        let bytes_read = file_reader.read_data_into_buffer(0, &mut buffer);

        assert_eq!(bytes_read, content.len());
        assert_eq!(buffer, content);

        // Cleanup
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_file_reader_close() {
        let file_name = "test_file_reader_close.txt";
        let content = b"Hello, RasterizedDB!";
        let path = setup_test_file(file_name, content);

        let mut file_reader = FileReader::new(path.clone());
        file_reader.close();

        // Ensure the file still exists after closing
        assert!(Path::new(&path).exists());

        // Cleanup
        std::fs::remove_file(path).unwrap();
    }
}