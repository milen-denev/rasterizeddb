use std::{
    cmp::min,
    io::{self, ErrorKind, SeekFrom},
    sync::atomic::{AtomicBool, Ordering},
};

use rand::RngCore;
use rclite::Arc;
use xutex::AsyncMutex;

use super::traits::StorageIO;

pub struct MemoryStorageProvider {
    buf: Arc<AsyncMutex<Vec<u8>>>,
    dropped: Arc<AtomicBool>,
    random_u32: u32,
    name: String,
}

impl Clone for MemoryStorageProvider {
    fn clone(&self) -> Self {
        Self {
            buf: self.buf.clone(),
            dropped: self.dropped.clone(),
            random_u32: self.random_u32,
            name: self.name.clone(),
        }
    }
}

impl MemoryStorageProvider {
    pub fn new(name: &str) -> MemoryStorageProvider {
        MemoryStorageProvider {
            buf: Arc::new(AsyncMutex::new(Vec::new())),
            dropped: Arc::new(AtomicBool::new(false)),
            random_u32: rand::rng().next_u32(),
            name: name.to_string(),
        }
    }

    #[inline(always)]
    fn maybe_clear_dropped(&self, buf: &mut Vec<u8>) {
        if self.dropped.swap(false, Ordering::AcqRel) {
            buf.clear();
        }
    }
}

impl StorageIO for MemoryStorageProvider {
    async fn write_data(&self, position: u64, buffer: &[u8]) {
        if buffer.is_empty() {
            return;
        }

        let end = position
            .checked_add(buffer.len() as u64)
            .expect("write would overflow u64");

        let mut buf_guard = self.buf.lock_sync();
        self.maybe_clear_dropped(&mut buf_guard);
        let end_usize = end as usize;
        if buf_guard.len() < end_usize {
            buf_guard.resize(end_usize, 0);
        }

        let start = position as usize;
        buf_guard[start..end_usize].copy_from_slice(buffer);
    }

    async fn write_data_seek(&self, seek: std::io::SeekFrom, buffer: &[u8]) {
        match seek {
            SeekFrom::Current(_index) => {
                panic!("Not supported")
            }
            SeekFrom::End(_index) => panic!("Not supported"),
            SeekFrom::Start(index) => {
                self.write_data(index, buffer).await;
            }
        }
    }

    async fn read_data_into_buffer(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
    ) -> Result<(), std::io::Error> {
        if buffer.is_empty() {
            return Ok(());
        }

        let mut buf_guard = self.buf.lock_sync();
        self.maybe_clear_dropped(&mut buf_guard);
        let len = buf_guard.len() as u64;

        if *position >= len {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "Position is past end of buffer",
            ));
        }

        let available = (len - *position) as usize;
        let to_copy = min(buffer.len(), available);

        let start = *position as usize;
        buffer[..to_copy].copy_from_slice(&buf_guard[start..start + to_copy]);
        *position += to_copy as u64;

        if to_copy == buffer.len() {
            Ok(())
        } else {
            Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "Not enough bytes to fill buffer",
            ))
        }
    }

    async fn read_vectored(&self, reads: &mut [(u64, &mut [u8])]) -> Result<(), std::io::Error> {
        // Match `file_sync`: the u64 offsets in `reads` are treated as absolute positions
        // and MUST NOT be mutated as a side-effect of this call.
        for (pos, buf) in reads.iter_mut() {
            let mut p = *pos;
            self.read_data_into_buffer(&mut p, buf).await?;
        }
        Ok(())
    }

    async fn append_data(&self, buffer: &[u8], _immediate: bool) {
        if buffer.is_empty() {
            return;
        }

        let mut buf_guard = self.buf.lock_sync();
        self.maybe_clear_dropped(&mut buf_guard);
        buf_guard.extend_from_slice(buffer);
    }

    async fn get_len(&self) -> u64 {
        let mut buf_guard = self.buf.lock_sync();
        self.maybe_clear_dropped(&mut buf_guard);
        buf_guard.len() as u64
    }

    fn exists(location: &str, table_name: &str) -> bool {
        let _location = location;
        let _table_name = table_name;
        true
    }

    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        let mut buf_guard = self.buf.lock_sync();
        self.maybe_clear_dropped(&mut buf_guard);
        let start = position as usize;
        let end = match start.checked_add(buffer.len()) {
            Some(v) => v,
            None => return false,
        };

        if end > buf_guard.len() {
            return false;
        }

        &buf_guard[start..end] == buffer
    }

    async fn create_temp(&self) -> Self {
        Self {
            buf: Arc::new(AsyncMutex::new(Vec::new())),
            dropped: Arc::new(AtomicBool::new(false)),
            random_u32: rand::rng().next_u32(),
            name: "temp".to_string(),
        }
    }

    async fn swap_temp(&self, _temp_io_sync: &mut Self) {
        // Swap the in-memory contents. This intentionally affects all clones
        // (like swapping backing files would for the file-based provider).
        let a_ptr = Arc::as_ptr(&self.buf) as usize;
        let b_ptr = Arc::as_ptr(&_temp_io_sync.buf) as usize;

        if a_ptr == b_ptr {
            return;
        }

        if a_ptr < b_ptr {
            let mut a = self.buf.lock_sync();
            let mut b = _temp_io_sync.buf.lock_sync();
            self.maybe_clear_dropped(&mut a);
            _temp_io_sync.maybe_clear_dropped(&mut b);
            std::mem::swap(&mut *a, &mut *b);
        } else {
            let mut b = _temp_io_sync.buf.lock_sync();
            let mut a = self.buf.lock_sync();
            self.maybe_clear_dropped(&mut a);
            _temp_io_sync.maybe_clear_dropped(&mut b);
            std::mem::swap(&mut *a, &mut *b);
        }
    }

    fn get_location(&self) -> Option<String> {
        None
    }

    #[allow(refining_impl_trait)]
    async fn create_new(&self, name: String) -> Self {
        MemoryStorageProvider {
            buf: Arc::new(AsyncMutex::new(Vec::new())),
            dropped: Arc::new(AtomicBool::new(false)),
            random_u32: rand::rng().next_u32(),
            name: name,
        }
    }

    fn drop_io(&self) {
        // Sync method in the trait: avoid blocking on an async mutex.
        // Mark as dropped and lazily clear on the next operation that acquires the lock.
        self.dropped.store(true, Ordering::Release);
    }

    fn get_hash(&self) -> u32 {
        self.random_u32
    }

    async fn start_service(&self) {}

    fn get_name(&self) -> String {
        self.name.clone()
    }
}
