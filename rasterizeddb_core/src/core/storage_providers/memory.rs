use std::{
    cmp::min,
    io::{self, ErrorKind, SeekFrom},
    sync::atomic::{AtomicBool, Ordering},
};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rand::Rng;
use rclite::Arc;

use super::traits::StorageIO;

/// The named buffers one in-memory database is made of.
///
/// `create_new` is how every caller opens a sibling file, and for the file
/// backend two handles to the same name see the same bytes. The in-memory
/// backend has to match that, or a write through one handle is invisible to
/// another — which is what silently discarded every schema change, since
/// `TableSchema::add_field` writes through a handle it opens itself.
///
/// The registry is per family rather than global: a provider created by
/// `new` starts a fresh one and every handle derived from it shares it, so two
/// in-memory databases in one process stay isolated even when their tables have
/// the same names.
type BufferRegistry = Arc<dashmap::DashMap<String, Arc<RwLock<Vec<u8>>>>>;

/// One buffer per file, behind a reader-writer lock.
///
/// It was an exclusive mutex taken for every read. Several scan workers
/// reading one table then took turns on it, and the hand-off between them
/// parks the waiting OS threads — tokio workers, all of them at once: a point
/// read on an unrelated table waited 4–6 s for a worker to come free
/// (`latency_bench` in its in-memory mode). Reads now share the lock; writes,
/// appends, truncation and the drop-clear take it exclusively, and each of
/// those is one copy long. Every operation is still atomic with respect to
/// every other, so nothing a caller can observe changed except that
/// concurrent reads no longer wait for each other.
pub struct MemoryStorageProvider {
    buf: Arc<RwLock<Vec<u8>>>,
    dropped: Arc<AtomicBool>,
    random_u32: u32,
    name: String,
    registry: BufferRegistry,
}

impl Clone for MemoryStorageProvider {
    fn clone(&self) -> Self {
        Self {
            buf: self.buf.clone(),
            dropped: self.dropped.clone(),
            random_u32: self.random_u32,
            name: self.name.clone(),
            registry: self.registry.clone(),
        }
    }
}

impl MemoryStorageProvider {
    pub fn new(name: &str) -> MemoryStorageProvider {
        let buf = Arc::new(RwLock::new(Vec::new()));
        let registry: BufferRegistry = Arc::new(dashmap::DashMap::new());
        registry.insert(name.to_string(), buf.clone());

        MemoryStorageProvider {
            buf,
            dropped: Arc::new(AtomicBool::new(false)),
            random_u32: rand::rng().next_u32(),
            name: name.to_string(),
            registry,
        }
    }

    #[inline(always)]
    fn maybe_clear_dropped(&self, buf: &mut Vec<u8>) {
        if self.dropped.swap(false, Ordering::AcqRel) {
            buf.clear();
        }
    }

    /// Exclusive access, with a pending drop applied first.
    #[inline(always)]
    fn write_guard(&self) -> RwLockWriteGuard<'_, Vec<u8>> {
        let mut guard = self.buf.write();
        self.maybe_clear_dropped(&mut guard);
        guard
    }

    /// Shared access. A pending drop is applied under the write lock first —
    /// a reader must not see bytes this handle has already dropped — and only
    /// then is the read lock taken. The flag is almost never set, so the
    /// common path is a single shared acquisition.
    #[inline(always)]
    fn read_guard(&self) -> RwLockReadGuard<'_, Vec<u8>> {
        if self.dropped.load(Ordering::Acquire) {
            drop(self.write_guard());
        }
        self.buf.read()
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

        let mut buf_guard = self.write_guard();
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

        let buf_guard = self.read_guard();
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

    async fn append_data(&self, buffer: &[u8], immediate: bool) {
        let _ = self.append_data_returning_offset(buffer, immediate).await;
    }

    async fn append_data_returning_offset(&self, buffer: &[u8], _immediate: bool) -> u64 {
        let mut buf_guard = self.write_guard();

        // Offset is read under the same lock that performs the append, so
        // concurrent appenders get distinct, accurate offsets.
        let offset = buf_guard.len() as u64;

        if buffer.is_empty() {
            return offset;
        }

        buf_guard.extend_from_slice(buffer);
        offset
    }

    async fn truncate(&self, len: u64) -> Result<(), std::io::Error> {
        let mut buf_guard = self.write_guard();
        let current = buf_guard.len() as u64;
        if len > current {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("truncate to {} would grow a {}-byte buffer", len, current),
            ));
        }
        buf_guard.truncate(len as usize);
        // Hand the pages back rather than keeping a buffer sized to the table's
        // high-water mark for the life of the process.
        buf_guard.shrink_to_fit();
        Ok(())
    }

    async fn get_len(&self) -> u64 {
        self.read_guard().len() as u64
    }

    fn exists(location: &str, table_name: &str) -> bool {
        let _location = location;
        let _table_name = table_name;
        true
    }

    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        let buf_guard = self.read_guard();
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
        // Deliberately unregistered: a scratch buffer is not a named file, and
        // registering it would let a later `create_new("temp")` find it.
        Self {
            buf: Arc::new(RwLock::new(Vec::new())),
            dropped: Arc::new(AtomicBool::new(false)),
            random_u32: rand::rng().next_u32(),
            name: "temp".to_string(),
            registry: self.registry.clone(),
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

        // Locked in address order, as before, so two opposite swaps cannot
        // deadlock.
        if a_ptr < b_ptr {
            let mut a = self.write_guard();
            let mut b = _temp_io_sync.write_guard();
            std::mem::swap(&mut *a, &mut *b);
        } else {
            let mut b = _temp_io_sync.write_guard();
            let mut a = self.write_guard();
            std::mem::swap(&mut *a, &mut *b);
        }
    }

    fn get_location(&self) -> Option<String> {
        None
    }

    #[allow(refining_impl_trait)]
    async fn create_new(&self, name: String) -> Self {
        // Reopening the same name returns a handle onto the same bytes, the way
        // reopening a file does.
        let buf = self
            .registry
            .entry(name.clone())
            .or_insert_with(|| Arc::new(RwLock::new(Vec::new())))
            .clone();

        MemoryStorageProvider {
            buf,
            dropped: Arc::new(AtomicBool::new(false)),
            random_u32: rand::rng().next_u32(),
            name,
            registry: self.registry.clone(),
        }
    }

    fn drop_io(&self) {
        // Sync method in the trait: avoid blocking on the lock.
        // Mark as dropped and lazily clear on the next operation that acquires the lock.
        self.dropped.store(true, Ordering::Release);
        // Forget the name too, so reopening it later starts empty rather than
        // finding the dropped buffer — which is what deleting a file does.
        self.registry.remove(&self.name);
    }

    fn get_hash(&self) -> u32 {
        self.random_u32
    }

    async fn start_service(&self) {}

    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[cfg(test)]
mod concurrency_tests {
    //! The buffer is behind a reader-writer lock. These pin what that must
    //! not change: appenders get distinct offsets that match where their bytes
    //! landed, readers running alongside them only ever see whole records, and
    //! a dropped handle reads back empty.
    use super::*;

    const RECORD: usize = 64;

    fn record(writer: u8, seq: u32) -> [u8; RECORD] {
        let mut r = [writer; RECORD];
        r[..4].copy_from_slice(&seq.to_le_bytes());
        r
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_appends_and_reads_stay_consistent() {
        let io = MemoryStorageProvider::new("concurrency");
        let per_writer = 2_000u32;
        let mut tasks = Vec::new();
        for w in 0..4u8 {
            let io = io.clone();
            tasks.push(tokio::spawn(async move {
                let mut offsets = Vec::with_capacity(per_writer as usize);
                for seq in 0..per_writer {
                    let rec = record(w + 1, seq);
                    offsets.push((io.append_data_returning_offset(&rec, false).await, rec));
                }
                offsets
            }));
        }
        // Readers alongside: whatever length they see, every whole record in
        // it is one writer's record, never a mix.
        let mut readers = Vec::new();
        for _ in 0..4 {
            let io = io.clone();
            readers.push(tokio::spawn(async move {
                for _ in 0..200 {
                    let len = io.get_len().await as usize;
                    let whole = len / RECORD * RECORD;
                    let mut buf = vec![0u8; whole];
                    let mut at = 0u64;
                    if whole > 0 {
                        io.read_data_into_buffer(&mut at, &mut buf).await.unwrap();
                    }
                    for r in buf.chunks(RECORD) {
                        assert!(r[4..].iter().all(|b| *b == r[RECORD - 1]), "torn record");
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }
        let mut all = Vec::new();
        for t in tasks {
            all.extend(t.await.unwrap());
        }
        for r in readers {
            r.await.unwrap();
        }
        assert_eq!(io.get_len().await as usize, 4 * per_writer as usize * RECORD);
        let mut seen = std::collections::HashSet::new();
        for (offset, rec) in &all {
            assert!(seen.insert(*offset), "offset {offset} handed out twice");
            let mut back = [0u8; RECORD];
            let mut at = *offset;
            io.read_data_into_buffer(&mut at, &mut back).await.unwrap();
            assert_eq!(&back, rec, "bytes at {offset} are not the appender's");
        }
    }

    #[tokio::test]
    async fn a_dropped_handle_reads_back_empty() {
        let io = MemoryStorageProvider::new("dropped");
        io.append_data(b"abcdef", false).await;
        let reader = io.clone();
        io.drop_io();
        assert_eq!(reader.get_len().await, 0);
        let mut buf = [0u8; 3];
        let mut at = 0;
        assert!(reader.read_data_into_buffer(&mut at, &mut buf).await.is_err());
        assert!(!reader.verify_data(0, b"abc").await);
        // Writes after the drop start from empty.
        io.write_data(2, b"xy").await;
        assert_eq!(io.get_len().await, 4);
    }
}
