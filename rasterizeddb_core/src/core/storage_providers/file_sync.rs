use std::{
    cmp::min, fs::{self, remove_file, OpenOptions}, path::Path, sync::{
        atomic::{AtomicBool, AtomicU64, Ordering}, Arc
    }, usize
};

use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};

use arc_swap::ArcSwap;
use crossbeam_queue::SegQueue;
use parking_lot::{Mutex, RwLock};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt}, task::yield_now
};

use memmap2::{Mmap, MmapOptions};

#[cfg(windows)]
use std::os::windows::fs::FileExt;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use crate::{
    core::storage_providers::helpers::Chunk, memory_pool::{MemoryBlock}, IMMEDIATE_WRITE, WRITE_BATCH_SIZE, WRITE_SLEEP_DURATION
};

use super::{CRC, traits::StorageIO};

#[cfg(unix)]
use super::io_uring_reader::AsyncUringReader;

type StdResult<T, E> = std::result::Result<T, E>;

const DIRECT_READ_BYPASS_FACTOR: usize = 4; // if remaining >= chunk_size * this => direct read
const CACHED_CHUNK_SIZE: usize = 32 * 1024 * 1024; // 32MB chunks

pub enum FastPathResult {
    Done,              // fully satisfied
    NeedChunk,         // must load a chunk that is not present
    EofPartial(usize), // partial bytes copied (usize) then EOF
    EofNone,           // position at/after EOF before copying
}

pub struct LocalStorageProvider {
    pub(super) append_file: Arc<RwLock<std::fs::File>>,
    pub(super) write_file: Arc<RwLock<std::fs::File>>,
    pub(crate) location: String,
    pub(crate) table_name: String,
    pub(crate) file_str: String,
    // temp staging file used for append_data()
    pub(crate) temp_file_str: String,
    pub(super) temp_file: Arc<RwLock<std::fs::File>>,
    // length of staged temp file bytes, maintained atomically (no metadata in read paths)
    pub(super) temp_file_len: AtomicU64,
    pub file_len: AtomicU64,
    // Memory map published with ArcSwap and owned by Arc to allow thread-local caches to own a handle safely.
    pub(crate) _memory_map: Arc<ArcSwap<Mmap>>,
    pub(crate) hash: u32,
    _locked: AtomicBool,
    // true when there is data in temp_file waiting to be flushed
    temp_has_data: AtomicBool,
    // used to temporarily block appends during flush
    append_blocked: AtomicBool,
    appender: SegQueue<MemoryBlock>,
    #[cfg(unix)]
    io_uring_reader: AsyncUringReader,
    chunk: ArcSwap<Chunk>,
    chunk_reload: Mutex<()>,
    chunk_size: usize
}

unsafe impl Sync for LocalStorageProvider {}
unsafe impl Send for LocalStorageProvider {}

impl Clone for LocalStorageProvider {
    fn clone(&self) -> Self {
        let map = Arc::new(unsafe {
            MmapOptions::new()
                .map(&std::fs::File::open(&self.file_str).unwrap())
                .unwrap()
        });

        Self {
            append_file: self.append_file.clone(),
            write_file: self.write_file.clone(),
            location: self.location.clone(),
            table_name: self.table_name.clone(),
            file_str: self.file_str.clone(),
            temp_file_str: self.temp_file_str.clone(),
            temp_file: self.temp_file.clone(),
            temp_file_len: AtomicU64::new(self.temp_file_len.load(Ordering::Acquire)),
            file_len: AtomicU64::new(self.file_len.load(std::sync::atomic::Ordering::SeqCst)),
            _memory_map: Arc::new(arc_swap::ArcSwap::new(map)),
            hash: CRC.checksum(format!("{}+++{}", self.location, self.table_name).as_bytes()),
            appender: SegQueue::new(),
            _locked: AtomicBool::new(false),
            temp_has_data: AtomicBool::new(self.temp_has_data.load(Ordering::Relaxed)),
            append_blocked: AtomicBool::new(false),
            #[cfg(unix)]
            io_uring_reader: self.io_uring_reader.clone(),
            chunk: ArcSwap::new(Chunk::empty()),
            chunk_reload: Mutex::new(()),
            chunk_size: CACHED_CHUNK_SIZE, // 16MB chunks
        }
    }
}

impl LocalStorageProvider {
    pub async fn new(location: &str, table_name: Option<&str>) -> LocalStorageProvider {
        if let Some(table_name) = table_name {
            let delimiter = if cfg!(unix) {
                "/"
            } else if cfg!(windows) {
                "\\"
            } else {
                panic!("OS not supported");
            };

            let location_str = format!("{}", location);

            let location_path = Path::new(&location_str);

            if !location_path.exists() {
                _ = std::fs::create_dir_all(location_path);
            }

            let final_location = if location.ends_with(delimiter) {
                location[location.len() - 1..].replace(delimiter, "")
            } else {
                format!("{}", location)
            };

            let file_str = format!("{}{}{}", final_location, delimiter, table_name);

            let file_path = Path::new(&file_str);

            if !file_path.exists() && !file_path.is_file() {
                _ = std::fs::File::create(&file_str).unwrap();
            }

            let file_append = std::fs::File::options()
                .read(true)
                .append(true)
                .open(&file_str)
                .unwrap();

            let file_write = std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .unwrap();

            let file_read_mmap = std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .unwrap();

            // Ensure temp staging file exists next to main file
            let temp_file_str = format!("{}.tmp", file_str);

            if !Path::new(&temp_file_str).exists() {
                _ = std::fs::File::create(&temp_file_str).unwrap();
            }

            let temp_file = std::fs::File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(&temp_file_str)
                .unwrap();
            
            #[cfg(not(test))]
            let temp_len = std::fs::metadata(&temp_file_str).map(|m| m.len()).unwrap_or(0);

            #[cfg(test)]
            let temp_len = {
                // If we're operating under the system temp directory (typical for tests),
                // ensure a clean slate by truncating any previous content.
                let sys_tmp = std::env::temp_dir().to_string_lossy().to_string();

                if final_location.starts_with(&sys_tmp) {
                    let _ = file_read_mmap.set_len(0);
                }

                // Determine temp length; under system temp dir, truncate temp as well.
                let temp_len = if final_location.starts_with(&sys_tmp) {
                    let _ = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&temp_file_str)
                        .and_then(|f| {
                            f.set_len(0)?;
                            Ok(())
                        });
                    0u64
                } else {
                    std::fs::metadata(&temp_file_str).map(|m| m.len()).unwrap_or(0)
                };

                temp_len
            };
            
            let map = Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            })));
            
            let file_len = file_read_mmap.metadata().unwrap().len();

            let local_storage_provider =LocalStorageProvider {
                append_file: Arc::new(RwLock::new(file_append)),
                write_file: Arc::new(RwLock::new(file_write)),
                location: final_location.to_string(),
                table_name: table_name.to_string(),
                file_str: file_str.clone(),
                temp_file_str,
                temp_file: Arc::new(RwLock::new(temp_file)),
                file_len: AtomicU64::new(file_len),
                _memory_map: map,
                hash: CRC.checksum(format!("{}+++{}", final_location, table_name).as_bytes()),
                appender: SegQueue::new(),
                _locked: AtomicBool::new(false),
                temp_file_len: AtomicU64::new(temp_len),
                temp_has_data: AtomicBool::new(false),
                append_blocked: AtomicBool::new(false),
                #[cfg(unix)]
                io_uring_reader: AsyncUringReader::new(&file_str, 1024).unwrap(),
                chunk: ArcSwap::new(Chunk::empty()),
                chunk_reload: Mutex::new(()),
                chunk_size: CACHED_CHUNK_SIZE, // 16MB chunks
            };

            // If there are staged bytes in temp file, flush them into the main file.
            if temp_len > 0 && file_len > 0 {
                let _ = local_storage_provider.flush_temp_to_main().await;
            }

            local_storage_provider
        } else {
            let delimiter = if cfg!(unix) {
                "/"
            } else if cfg!(windows) {
                "\\"
            } else {
                panic!("OS not supported");
            };

            let location_str = if location.ends_with(delimiter) {
                location[location.len() - 1..].replace(delimiter, "")
            } else {
                format!("{}", location)
            };

            let location_path = Path::new(&location_str);

            if !location_path.exists() {
                _ = std::fs::create_dir_all(location_path);
            }

            let file_str = format!("{}{}{}", location_str, delimiter, "CONFIG_TABLE.db");

            _ = std::fs::File::create(&file_str).unwrap();

            let file_append = std::fs::File::options()
                .read(true)
                .append(true)
                .open(&file_str)
                .unwrap();

            let file_write = std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .unwrap();

            let file_read_mmap = std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .unwrap();

            // Ensure temp staging file exists next to main file
            let temp_file_str = format!("{}.tmp", file_str);
            if !Path::new(&temp_file_str).exists() {
                _ = std::fs::File::create(&temp_file_str).unwrap();
            }
            let temp_file = std::fs::File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(&temp_file_str)
                .unwrap();
            #[cfg(not(test))]
            let temp_len = std::fs::metadata(&temp_file_str).map(|m| m.len()).unwrap_or(0);
                
            #[cfg(test)]
            let temp_len = {
                // If we're operating under the system temp directory (typical for tests),
                // ensure a clean slate by truncating any previous content.
                let sys_tmp = std::env::temp_dir().to_string_lossy().to_string();

                if location_str.starts_with(&sys_tmp) {
                    let _ = file_read_mmap.set_len(0);
                    let _ = temp_file.set_len(0);
                }

                // Determine temp length; under system temp dir, truncate temp as well.
                let temp_len = if location_str.starts_with(&sys_tmp) {
                    let _ = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&temp_file_str)
                        .and_then(|f| {
                            f.set_len(0)?;
                            Ok(())
                        });
                    0u64
                } else {
                    std::fs::metadata(&temp_file_str).map(|m| m.len()).unwrap_or(0)
                };

                temp_len
            };

            let map = Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            })));

            let file_len = file_read_mmap.metadata().unwrap().len();

            let local_storage_provider = LocalStorageProvider {
                append_file: Arc::new(RwLock::new(file_append)),
                write_file: Arc::new(RwLock::new(file_write)),
                location: location_str.to_string(),
                table_name: "temp.db".to_string(),
                file_str: file_str.clone(),
                temp_file_str,
                temp_file: Arc::new(RwLock::new(temp_file)),
                file_len: AtomicU64::new(file_len),
                _memory_map: map,
                hash: CRC.checksum(format!("{}+++{}", location_str, "CONFIG_TABLE.db").as_bytes()),
                appender: SegQueue::new(),
                _locked: AtomicBool::new(false),
                temp_file_len: AtomicU64::new(temp_len),
                temp_has_data: AtomicBool::new(false),
                append_blocked: AtomicBool::new(false),
                #[cfg(unix)]
                io_uring_reader: AsyncUringReader::new(&file_str, 1024).unwrap(),
                chunk: ArcSwap::new(Chunk::empty()),
                chunk_reload: Mutex::new(()),
                chunk_size: CACHED_CHUNK_SIZE, // 16MB chunks
            };

            if temp_len > 0 && file_len > 0 {
                let _ = local_storage_provider.flush_temp_to_main().await;
            }

            local_storage_provider
        }
    }

    pub async fn close_files(&self) {
        let file_read = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();

        let file_append = std::fs::File::options()
            .read(true)
            .append(true)
            .open(&self.file_str)
            .unwrap();

        let file_write = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&self.file_str)
            .unwrap();

        _ = file_read.sync_all();
        _ = file_append.sync_all();
        _ = file_write.sync_all();
    }

    pub async fn start_append_data_service(&self) {
        if IMMEDIATE_WRITE {
            return;
        }

        let mut idle_count = 0;
        let mut buffer: Vec<u8> = Vec::with_capacity(WRITE_BATCH_SIZE);

        loop {
            let mut total_size = 0;
            let mut has_data = false;

            // Collect data in batches
            while let Some(block) = self.appender.pop() {
                has_data = true;
                let slice = block.into_slice();
                buffer.extend_from_slice(slice);
                total_size += slice.len();

                // Break if we've reached our batch size limit
                if total_size >= WRITE_BATCH_SIZE {
                    break;
                }
            }

            if has_data {
                idle_count = 0;
                self.write_batch_data(&mut buffer, total_size).await;
            } else {
                idle_count += 1;

                // Adaptive sleep - sleep longer when idle for extended periods
                let sleep_duration = if idle_count < 100 {
                    WRITE_SLEEP_DURATION
                } else if idle_count < 1000 {
                    tokio::time::Duration::from_millis(50)
                } else {
                    tokio::time::Duration::from_millis(100)
                };

                tokio::time::sleep(sleep_duration).await;
            }
        }
    }

    async fn write_batch_data(&self, buffer: &mut Vec<u8>, total_size: usize) {
        if total_size == 0 {
            return;
        }

        // Write the data
        {
            let mut file = self.append_file.write();

            file.write_all(buffer).unwrap();
            file.flush().unwrap();
        } // file handle is dropped here

        // Update file length atomically
        self.file_len
            .fetch_add(total_size as u64, std::sync::atomic::Ordering::Release);

        // Update memory map after writing
        self.update_memory_map();

        buffer.clear(); // Clear the buffer for the next batch
    }

    // Move any staged bytes from temp file into the main file atomically with respect to appends.
    async fn flush_temp_to_main(&self) -> io::Result<usize> {
        // Try to acquire the append block; if already blocked, another flush is running.
        if self
            .append_blocked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Ok(0);
        }

        // Always clear the block at the end
        struct Reset<'a>(&'a AtomicBool);

        impl<'a> Drop for Reset<'a> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }

        let _reset = Reset(&self.append_blocked);

        // Snapshot temp length from atomic and read payload
        let mut temp = self.temp_file.write();
        let len = self.temp_file_len.swap(0, Ordering::AcqRel) as usize;
        if len == 0 {
            // nothing to do
            self.temp_has_data.store(false, Ordering::Release);
            return Ok(0);
        }

        // Read all staged bytes
        temp.seek(SeekFrom::Start(0))?;
        let mut buf = vec![0u8; len];
        temp.read_exact(&mut buf)?;

        // Append to main file
        {
            let mut main = self.append_file.write();
            main.write_all(&buf)?;
            main.flush()?;
            // Optional: fsync for durability
            let _ = main.sync_all();
        }

        // Truncate temp file back to zero for next round
        temp.set_len(0)?;
        temp.seek(SeekFrom::Start(0))?;
        let _ = temp.flush();

        // Publish new file_len and refresh mmap
        self.file_len.fetch_add(len as u64, Ordering::Release);
        self.update_memory_map();

        // Clear pending flag
        self.temp_has_data.store(false, Ordering::Release);
        Ok(len)
    }

    fn update_memory_map(&self) {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_str)
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to open file for memory mapping: {}", e);
                return;
            }
        };

        // Acquire a simple spin lock to serialize remapping
        while self
            ._locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }

        // Create new memory map with the current file size
        self._memory_map
            .store(Arc::new(unsafe { MmapOptions::new().map(&file).unwrap() }));

    self._locked.store(false, Ordering::Release);
    }

    #[allow(dead_code)]
    fn read_from_memory_map_or_file(
        &self,
        position: u64,
        buffer: &mut [u8],
    ) -> std::io::Result<()> {
        let memory_map = self._memory_map.load();

        let buffer_len = buffer.len();

        if memory_map.len() >= position as usize + buffer_len {
            // Read from memory map
            let start_idx = position as usize;
            let end_idx = start_idx + buffer_len;

            // Bounds check
            if end_idx <= memory_map.len() {
                buffer.copy_from_slice(&memory_map[start_idx..end_idx]);

                return Ok(());
            }
        }

        // Fallback to file reading
        self.read_from_file_direct(position, buffer)
    }

    #[allow(dead_code)]
    fn read_from_file_direct(&self, position: u64, buffer: &mut [u8]) -> std::io::Result<()> {
        let mut read_file = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to open file: {}", e),
                )
            })?;

        read_file.seek(SeekFrom::Start(position)).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to seek to position {}: {}", position, e),
            )
        })?;

        read_file.read_exact(buffer).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Failed to read {} bytes at position {}: {}",
                    buffer.len(),
                    position,
                    e
                ),
            )
        })?;

        Ok(())
    }

    /// Compute the rolling cache window for a given position.
    /// Rules:
    /// 1. End never exceeds file_len.
    /// 2. Window length equals cache_size when file_len >= cache_size.
    /// 3. Window "rolls" in fixed-size buckets aligned to cache_size, except
    ///    near EOF where it is shifted backward to keep full size.
    #[allow(dead_code)]
    fn compute_range_centered(pos: u64, file_len: u64, cache_size: u64) -> (u64, u64) {
        if file_len <= cache_size {
            return (0, file_len);
        }
        let half = cache_size / 2;
        let mut start = pos.saturating_sub(half);
        if start + cache_size > file_len {
            start = file_len - cache_size;
        }
        let end = start + cache_size;
        (start, end)
    }

    #[inline(always)]
    fn load_file_len(&self) -> u64 {
        // If writers publish length with Release, Acquire here would be stricter.
        self.file_len.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn is_locked(&self) -> bool {
        self._locked.load(Ordering::Relaxed)
    }

    /* --------- Low-level disk read (pread style) --------- */
    #[inline(always)]
    fn pread_exact_or_partial(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
        // Choose which file descriptor to read from; assuming append_file.
        // We do not take a lock because we use read_at / seek_read that doesn't modify cursor.
        let file_guard = self.append_file.read();

        #[cfg(unix)]
        {
            let n = file_guard.read_at(buf, pos)?;
            Ok(n)
        }

        #[cfg(windows)]
        {
            let n = file_guard.seek_read(buf, pos)?;
            Ok(n)
        }
    }

    #[inline(always)]
    fn temp_pread_exact_or_partial(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
        let file_guard = self.temp_file.read();

        #[cfg(unix)]
        {
            let n = file_guard.read_at(buf, pos)?;
            Ok(n)
        }
        #[cfg(windows)]
        {
            let n = file_guard.seek_read(buf, pos)?;
            Ok(n)
        }
    }

    /* --------- Chunk loading (single-flight) --------- */
    #[cold]
    async fn load_chunk_for(&self, pos: u64) -> io::Result<Arc<Chunk>> {
        let aligned = self.align_chunk_start(pos);
        let _g = self.chunk_reload.lock();

        // Double-check after acquiring lock (another task may have loaded it).
        if let Some(hit) = self.try_chunk_hit(pos) {
            return Ok(hit);
        }

        let mut temp = vec![0u8; self.chunk_size];
        let n = self.pread_exact_or_partial(aligned, &mut temp)?;
        if n == 0 {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "EOF loading chunk",
            ));
        }
        temp.truncate(n);
        let new_chunk = Arc::new(Chunk {
            start: aligned,
            data: temp.into_boxed_slice().into(),
        });
        self.chunk.store(new_chunk.clone());
        Ok(new_chunk)
    }

    #[inline(always)]
    fn align_chunk_start(&self, pos: u64) -> u64 {
        let cs = self.chunk_size as u64;
        (pos / cs) * cs
    }

    #[inline(always)]
    fn try_chunk_hit(&self, pos: u64) -> Option<Arc<Chunk>> {
        let c = self.chunk.load_full();
        if c.contains(pos) { Some(c) } else { None }
    }

    /* --------- Legacy direct file path (if you keep it) --------- */
    #[allow(dead_code)]
    fn read_from_file_direct_legacy(&self, pos: u64, dst: &mut [u8]) -> io::Result<usize> {
        self.pread_exact_or_partial(pos, dst)
    }

    // Single large mmap copy if possible. Returns bytes copied.
    #[inline(always)]
    fn mmap_copy_region(&self, pos: u64, buf: &mut [u8], file_len: u64) -> usize {
        let mmap = self._memory_map.load();

        let mmap_len = mmap.len() as u64;
        if pos >= mmap_len {
            return 0;
        }
        // effective end limited by both mmap coverage and logical file length
        let max_span = (mmap_len - pos).min(file_len - pos);
        if max_span == 0 {
            return 0;
        }
        let to_copy = min(buf.len(), max_span as usize);
        let start = pos as usize;
        let end = start + to_copy;
        // bounds guaranteed by max_span
        buf[..to_copy].copy_from_slice(&mmap[start..end]);
        to_copy
    }

    #[inline(always)]
    fn direct_large_read(&self, pos: u64, buf: &mut [u8], file_len: u64) -> io::Result<usize> {
        // Clamp size to remain within file_len.
        if pos >= file_len {
            return Ok(0);
        }
        let max_avail = (file_len - pos) as usize;
        let to_read = buf.len().min(max_avail);
        let slice = &mut buf[..to_read];
        let n = self.pread_exact_or_partial(pos, slice)?;
        Ok(n)
    }

    #[inline(always)]
    fn fast_chunk_copy(
        &self,
        pos: u64,
        buf: &mut [u8],
        file_len: u64,
    ) -> StdResult<(usize, bool), FastPathResult> {
        // Returns (bytes_copied, fully_satisfied?). On miss returns Err(NeedChunk).
        if pos >= file_len {
            return Err(FastPathResult::EofNone);
        }
        if let Some(chunk) = self.try_chunk_hit(pos) {
            let offset = (pos - chunk.start) as usize;
            let avail = chunk.data.len() - offset;
            let to_copy = avail.min(buf.len()).min((file_len - pos) as usize);
            if to_copy == 0 {
                return Err(FastPathResult::EofNone);
            }
            buf[..to_copy].copy_from_slice(&chunk.data[offset..offset + to_copy]);
            Ok((to_copy, to_copy == buf.len()))
        } else {
            Err(FastPathResult::NeedChunk)
        }
    }

    // Synchronous fast path attempt: returns a FastPathResult; never awaits.
    fn read_data_into_buffer_fast(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
        file_len: u64,
    ) -> FastPathResult {
        if buffer.is_empty() {
            return FastPathResult::Done;
        }
        if *position >= file_len {
            return FastPathResult::EofNone;
        }

        let mut total_copied = 0usize;
        let mut pos = *position;

        // (1) Single mmap region copy first (most common path)
        let copied = self.mmap_copy_region(pos, &mut buffer[total_copied..], file_len);
        if copied > 0 {
            pos += copied as u64;
            total_copied += copied;
            if total_copied == buffer.len() {
                *position = pos;
                return FastPathResult::Done;
            }
        }

        // (2) If large remaining region, bypass chunk caching and read directly
        let remaining = buffer.len() - total_copied;
        if remaining >= self.chunk_size * DIRECT_READ_BYPASS_FACTOR {
            match self.direct_large_read(pos, &mut buffer[total_copied..], file_len) {
                Ok(n) if n == 0 => {
                    // EOF right here
                    if total_copied == 0 {
                        return FastPathResult::EofNone;
                    } else {
                        *position = pos;
                        return FastPathResult::EofPartial(total_copied);
                    }
                }
                Ok(n) => {
                    pos += n as u64;
                    total_copied += n;
                    if total_copied == buffer.len() {
                        *position = pos;
                        return FastPathResult::Done;
                    }
                    // If still remaining, proceed to chunk stage
                }
                Err(e) => {
                    // Treat errors other than EOF immediately
                    if e.kind() == ErrorKind::UnexpectedEof {
                        if total_copied == 0 {
                            return FastPathResult::EofNone;
                        } else {
                            *position = pos;
                            return FastPathResult::EofPartial(total_copied);
                        }
                    }
                    // For simplicity bubble up as partial EOF (or you can wrap in io::Error later)
                    if total_copied == 0 {
                        return FastPathResult::EofNone;
                    } else {
                        *position = pos;
                        return FastPathResult::EofPartial(total_copied);
                    }
                }
            }
        }

        // (3) Chunk loop (fast hits or single-flight miss)
        while total_copied < buffer.len() {
            match self.fast_chunk_copy(pos, &mut buffer[total_copied..], file_len) {
                Ok((n, full)) => {
                    pos += n as u64;
                    total_copied += n;
                    if full {
                        *position = pos;
                        return FastPathResult::Done;
                    }
                    continue; // more buffer to fill
                }
                Err(FastPathResult::NeedChunk) => {
                    // We must jump to async path for chunk load
                    *position = pos; // update progress before handing off
                    if total_copied > 0 {
                        return FastPathResult::EofPartial(total_copied).into_need_chunk_hint();
                    }
                    return FastPathResult::NeedChunk;
                }
                Err(FastPathResult::EofNone) => {
                    if total_copied == 0 {
                        return FastPathResult::EofNone;
                    } else {
                        *position = pos;
                        return FastPathResult::EofPartial(total_copied);
                    }
                }
                Err(FastPathResult::EofPartial(_)) => unreachable!(),
                Err(other) => return other,
            }
        }

        *position = pos;
        FastPathResult::Done
    }

    async fn read_data_into_buffer_internal(&self, position: &mut u64, buffer: &mut [u8]) -> io::Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        // Snapshot lengths
        let main_len = self.load_file_len();
        
        while self.append_blocked.load(Ordering::Relaxed) {
            yield_now().await;
        }

        let temp_len = self.temp_file_len.load(Ordering::Relaxed);

        let file_len = main_len + temp_len; // logical length

        // Immediate EOF if the requested position is beyond the logical end.
        // This ensures callers get UnexpectedEof and position remains unchanged.
        if *position >= file_len {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "Position beyond file end",
            ));
        }

        let total_len = buffer.len();

        let mut overall_copied = 0usize;

        loop {
            match self.read_data_into_buffer_fast(position, &mut buffer[overall_copied..], file_len)
            {
                FastPathResult::Done => {
                    return Ok(());
                }
                FastPathResult::NeedChunk => {
                    // If we're in the temp region, don't try to load chunks from main file.
                    if *position >= main_len {
                        let temp_off = *position - main_len;
                        let slice = &mut buffer[overall_copied..];
                        let n = self.temp_pread_exact_or_partial(temp_off, slice)?;
                        *position += n as u64;
                        overall_copied += n;
                        if overall_copied == total_len {
                            return Ok(());
                        }
                        return Err(io::Error::new(
                            ErrorKind::UnexpectedEof,
                            "Reached logical EOF",
                        ));
                    }
                    // Load chunk for current position in main file
                    self.load_chunk_for(*position).await?;
                    continue; // retry fast path
                }
                FastPathResult::EofNone => {
                    // If EOF relative to main file but temp has data and our position is inside logical range,
                    // try to read from temp staging directly.
                    if *position < file_len && *position >= main_len {
                        let temp_off = *position - main_len;
                        if overall_copied < total_len {
                            let slice = &mut buffer[overall_copied..];
                            let n = self.temp_pread_exact_or_partial(temp_off, slice)?;
                            *position += n as u64;
                            overall_copied += n;
                        }
                        if overall_copied == total_len {
                            return Ok(());
                        } else {
                            return Err(io::Error::new(
                                ErrorKind::UnexpectedEof,
                                "Reached logical EOF",
                            ));
                        }
                    }
                    return Err(io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "Position beyond file end",
                    ));
                }
                FastPathResult::EofPartial(copied) => {
                    overall_copied += copied;
                    // If we hit EOF in main region but temp has data and pos is in temp, continue from temp
                    if *position < file_len && *position >= main_len && overall_copied < total_len {
                        let temp_off = *position - main_len;
                        let slice = &mut buffer[overall_copied..];
                        let n = self.temp_pread_exact_or_partial(temp_off, slice)?;
                        *position += n as u64;
                        overall_copied += n;
                        if overall_copied == total_len {
                            return Ok(());
                        }
                    }
                    return Err(io::Error::new(
                        ErrorKind::UnexpectedEof,
                        format!(
                            "Requested {} bytes, got {} (reached logical EOF)",
                            total_len, overall_copied
                        ),
                    ));
                }
            }
        }
    }
}

impl StorageIO for LocalStorageProvider {
    async fn write_data_unsync(&self, position: u64, buffer: &[u8]) {
        let mut file = self.write_file.write();
        file.seek(SeekFrom::Start(position)).unwrap();
        file.write_all(buffer).unwrap();
    }

    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        let mut file_read = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();

        file_read.seek(SeekFrom::Start(position)).unwrap();
        let mut file_buffer = vec![0; buffer.len() as usize];
        file_read.read_exact(&mut file_buffer).unwrap();
        buffer.eq(&file_buffer)
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        let mut file = self.write_file.write();
        file.seek(SeekFrom::Start(position)).unwrap();
        file.write_all(buffer).unwrap();
        file.flush().unwrap();
        file.sync_all().unwrap();
    }

    async fn append_data(&self, buffer: &[u8], immediate: bool) {
        if immediate {
            // Bypass staging: write directly to the main file and publish length
            let mut file = self.append_file.write();
            file.write_all(buffer).unwrap();
            file.flush().unwrap();
            // Ensure file length and data are durable for any new handles
            let _ = file.sync_all();
            // Update file length atomically and refresh mmap so readers see new bytes
            self.file_len
                .fetch_add(buffer.len() as u64, Ordering::Release);
            self.update_memory_map();
            return;
        }

        // If a flush is ongoing, wait briefly until it completes to keep ordering simple.
        while self.append_blocked.load(Ordering::Acquire) {
            yield_now().await;
        }

        // Stage data into the temp file
        {
            let mut tf = self.temp_file.write();
            // Ensure writes go to the end
            tf.seek(SeekFrom::End(0)).unwrap();
            tf.write_all(buffer).unwrap();
            let _ = tf.flush();
        }
        // Mark that we have staged data
        self.temp_has_data.store(true, Ordering::Release);
        self.temp_file_len
            .fetch_add(buffer.len() as u64, Ordering::Release);
    }

    async fn get_len(&self) -> u64 {
        // Logical length = main persisted on disk + staged temp
        // Re-read filesystem metadata to avoid stale length on freshly opened handles.
        let main = self.load_file_len();
        let temp = self.temp_file_len.load(Ordering::Acquire);
        main + temp
    }

    fn exists(location: &str, table_name: &str) -> bool {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let file_str = format!("{}{}{}", location, delimiter, table_name);

        let path = Path::new(&file_str);

        if path.exists() && path.is_file() {
            return true;
        } else {
            return false;
        }
    }

    async fn read_data_into_buffer(&self, position: &mut u64, buffer: &mut [u8]) -> io::Result<()> {
        self.read_data_into_buffer_internal(position, buffer).await
    }

    async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
        let mut file = self.write_file.write();
        file.seek(seek).unwrap();
        file.write_all(buffer).unwrap();
        file.flush().unwrap();
        file.sync_all().unwrap();
    }

    async fn verify_data_and_sync(&self, position: u64, buffer: &[u8]) -> bool {
        let mut file = tokio::fs::File::options()
            .read(true)
            .write(true)
            .open(&self.file_str)
            .await
            .unwrap();

        file.seek(SeekFrom::Start(position)).await.unwrap();
        let mut file_buffer = vec![0; buffer.len() as usize];
        file.read_exact(&mut file_buffer).await.unwrap();

        if buffer.eq(&file_buffer) {
            file.sync_data().await.unwrap();
            true
        } else {
            false
        }
    }

    async fn append_data_unsync(&self, buffer: &[u8]) {
        // Keep behavior for unsynced path consistent: stage to temp as well.
        let mut tf = self.temp_file.write();
        tf.seek(SeekFrom::End(0)).unwrap();
        tf.write_all(&buffer).unwrap();
        let _ = tf.flush();
        self.temp_has_data.store(true, Ordering::Release);
        self.temp_file_len
            .fetch_add(buffer.len() as u64, Ordering::Release);
    }

    async fn create_temp(&self) -> Self {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let location_path = Path::new(&self.location);

        if !location_path.exists() {
            _ = std::fs::create_dir_all(location_path);
        }

        let final_location = if self.location.ends_with(delimiter) {
            self.location[self.location.len() - 1..].replace(delimiter, "")
        } else {
            format!("{}", self.location)
        };

        let file_str = format!("{}{}{}", final_location, delimiter, "temp.db");

        _ = std::fs::File::create(&file_str).unwrap();

        let file_append = std::fs::File::options()
            .read(true)
            .append(true)
            .open(&file_str)
            .unwrap();

        let file_write = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&file_str)
            .unwrap();

        let file_read_mmap = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&file_str)
            .unwrap();

        // Do NOT truncate here. We may be opening an existing companion file (e.g. FIELDS.db)
        // from tests or runtime, and truncation would destroy previously appended data.
        let file_len = file_read_mmap.metadata().unwrap().len();

        let temp_file_str = format!("{}.tmp", file_str);
        if !Path::new(&temp_file_str).exists() {
            _ = std::fs::File::create(&temp_file_str).unwrap();
        }
        let temp_file = std::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(&temp_file_str)
            .unwrap();

        // Preserve any existing staged temp bytes; tests may rely on persistence across openings.
        let mut staged_len = std::fs::metadata(&temp_file_str).map(|m| m.len()).unwrap_or(0);

        if file_len == 0 && staged_len > 0 {
            let _ = std::fs::OpenOptions::new()
                .write(true)
                .open(&temp_file_str)
                .and_then(|f| {
                    f.set_len(0)?;
                    Ok(())
                });
            staged_len = 0;
        }

        let local_storage_provider = Self {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            location: final_location.to_string(),
            table_name: "temp.db".to_string(),
            file_str: file_str.clone(),
            temp_file_str,
            temp_file: Arc::new(RwLock::new(temp_file)),
            temp_file_len: AtomicU64::new(staged_len),
            file_len: AtomicU64::new(file_len),
            _memory_map: Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            }))),
            hash: CRC.checksum(format!("{}+++{}", final_location, "temp.db").as_bytes()),
            appender: SegQueue::new(),
            _locked: AtomicBool::new(false),
            temp_has_data: AtomicBool::new(false),
            append_blocked: AtomicBool::new(false),
            #[cfg(unix)]
            io_uring_reader: AsyncUringReader::new(&file_str, 1024).unwrap(),
            chunk: ArcSwap::new(Chunk::empty()),
            chunk_reload: Mutex::new(()),
            chunk_size: CACHED_CHUNK_SIZE, // 16MB chunks
        };

        if staged_len > 0 && file_len > 0 {
            let _ = local_storage_provider.flush_temp_to_main().await;
        }

        local_storage_provider
    }

    async fn swap_temp(&self, _temp_io_sync: &mut Self) {
        yield_now().await;

        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let temp_file_str = format!("{}{}{}", self.location, delimiter, "temp.db");
        let actual_file_str = self.file_str.clone();

        // Rename the temp file to the actual file name
        _ = fs::remove_file(&actual_file_str);
        _ = fs::rename(&temp_file_str, &actual_file_str);

        self.close_files().await;
    }

    fn get_location(&self) -> Option<String> {
        Some(self.location.clone())
    }

    #[allow(refining_impl_trait)]
    async fn create_new(&self, name: String) -> Self {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let location_path = Path::new(&self.location);

        if !location_path.exists() {
            _ = std::fs::create_dir_all(location_path);
        }

        let final_location = if self.location.ends_with(delimiter) {
            self.location[self.location.len() - 1..].replace(delimiter, "")
        } else {
            format!("{}", self.location)
        };

        let new_table = format!("{}{}{}", final_location, delimiter, name);
        let file_path = Path::new(&new_table);

        if !file_path.exists() {
            _ = std::fs::File::create(&file_path).unwrap();
        }

        let file_append = std::fs::File::options()
            .read(true)
            .append(true)
            .open(&file_path)
            .unwrap();

        let file_write = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        let file_read_mmap = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        let temp_file_str = format!("{}.tmp", new_table);

        if !Path::new(&temp_file_str).exists() {
            _ = std::fs::File::create(&temp_file_str).unwrap();
        }

        let temp_file = std::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(&temp_file_str)
            .unwrap();
        
        let staged_len = std::fs::metadata(&temp_file_str).map(|m| m.len()).unwrap_or(0);

        let file_len = file_read_mmap.metadata().unwrap().len();

        let local_storage_provider = Self {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            location: final_location.to_string(),
            table_name: name.to_string(),
            file_str: new_table.clone(),
            temp_file_str,
            temp_file: Arc::new(RwLock::new(temp_file)),
            temp_file_len: AtomicU64::new(staged_len),
            file_len: AtomicU64::new(file_len),
            _memory_map: Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            }))),
            hash: CRC.checksum(format!("{}+++{}", final_location, name).as_bytes()),
            appender: SegQueue::new(),
            _locked: AtomicBool::new(false),
            temp_has_data: AtomicBool::new(false),
            append_blocked: AtomicBool::new(false),
            #[cfg(unix)]
            io_uring_reader: AsyncUringReader::new(&new_table, 1024).unwrap(),
            chunk: ArcSwap::new(Chunk::empty()),
            chunk_reload: Mutex::new(()),
            chunk_size: CACHED_CHUNK_SIZE, // 16MB chunks
        };

        if staged_len > 0 && file_len > 0 {
            let _ = local_storage_provider.flush_temp_to_main().await;
        }

        local_storage_provider
    }

    fn drop_io(&self) {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let file_str = format!("{}{}{}", self.location, delimiter, self.table_name);

        let path = Path::new(&file_str);

        remove_file(&path).unwrap();
    }

    fn get_hash(&self) -> u32 {
        self.hash
    }

    async fn start_service(&self) {
        // Run both the legacy batch service (if used elsewhere) and the temp flush service.
        async fn flush_loop(p: &LocalStorageProvider) {
            loop {
                // Fast path: only flush when we know there's staged data
                if p.temp_has_data.load(Ordering::Acquire) {
                    let _ = p.flush_temp_to_main().await; // ignore errors for now
                } else {
                    // avoid hot spinning
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                }
            }
        }

        tokio::join!(self.start_append_data_service(), flush_loop(self));
    }

    fn get_name(&self) -> String {
        self.table_name.clone()
    }
}

// Helper: transform EofPartial to NeedChunk hint if you want to treat partial before miss differently.
// Here just a simple extension method (optional).
trait IntoNeedChunk {
    fn into_need_chunk_hint(self) -> Self;
}

impl IntoNeedChunk for FastPathResult {
    fn into_need_chunk_hint(self) -> Self {
        self
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::OpenOptions,
        io::{Seek, SeekFrom, Write},
        sync::atomic::Ordering,
    };

    use rand::{RngCore, SeedableRng, rngs::StdRng};

    use crate::core::storage_providers::{file_sync::LocalStorageProvider, traits::StorageIO};

    #[tokio::test]
    async fn test_basic_read_full_within_mmap() {
        let file_size = 16 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        let mut pos = 0u64;
        let mut buf = vec![0u8; 3000];
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap();

        assert_eq!(pos, 3000);
        assert_eq!(&buf, &data[0..3000]);
    }

    #[tokio::test]
    async fn test_read_cross_multiple_chunks_inside_mmap() {
        // Make buffer span several chunk boundaries but stay inside initial mmap.
        let file_size = 64 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        let start = 1500u64; // not aligned
        let mut pos = start;
        let mut buf = vec![0u8; 10_000]; // covers multiple chunks
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap();

        assert_eq!(pos, start + 10_000);
        assert_eq!(&buf, &data[start as usize..start as usize + 10_000]);
    }

    #[tokio::test]
    async fn test_read_beyond_initial_mmap_uses_chunk_path() {
        // Create a file, mmap only the initial part, then append more bytes
        // and update file_len without remapping to force the chunk fallback.
        let initial = 8 * 1024;
        let extra = 6 * 1024;
        let (provider, _initial_data, extra_data) = create_provider(initial, extra).await;

        // Position exactly at end of original mmap -> mmap path misses.
        let mut pos = initial as u64;
        let mut buf = vec![0u8; 3000]; // entirely inside the "extra" region
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap();

        assert_eq!(pos, (initial + 3000) as u64);
        assert_eq!(&buf, &extra_data[..3000]);

        // Second read continuing inside extra area (should hit cached chunk).
        let mut buf2 = vec![0u8; 2000];
        provider
            .read_data_into_buffer(&mut pos, &mut buf2)
            .await
            .unwrap();
        assert_eq!(pos, (initial + 5000) as u64);
        assert_eq!(&buf2, &extra_data[3000..5000]);
    }

    #[tokio::test]
    async fn test_partial_eof_error_after_partial_copy() {
        let file_size = 10 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        // Start 100 bytes before EOF, ask for 500 bytes -> expect UnexpectedEof after copying 100.
        let start = (file_size - 100) as u64;
        let mut pos = start;
        let mut buf = vec![0u8; 500];
        let err = provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        // Position advanced by bytes successfully copied (100)
        assert_eq!(pos, file_size as u64);

        assert_eq!(&buf[..100], &data[file_size - 100..file_size]);
    }

    #[tokio::test]
    async fn test_position_beyond_file_end_immediate_eof() {
        let file_size = 8192;
        let (provider, _data, _extra) = create_provider(file_size, 0).await;

        let mut pos = (file_size + 10) as u64;
        let mut buf = vec![0u8; 128];
        let err = provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(pos, (file_size + 10) as u64); // unchanged
    }

    #[tokio::test]
    async fn test_large_read_entire_file() {
        let file_size = 32 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        let mut pos = 0u64;
        let mut buf = vec![0u8; file_size];
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap();

        assert_eq!(pos, file_size as u64);
        assert_eq!(&buf, &data[..]);
    }

    #[tokio::test]
    async fn test_multiple_sequential_reads_advancing_position() {
        let file_size = 16 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        let mut pos = 0u64;

        for step in [500usize, 1500, 4096, 320].iter() {
            let mut buf = vec![0u8; *step];
            provider
                .read_data_into_buffer(&mut pos, &mut buf)
                .await
                .unwrap();
            assert_eq!(&buf, &data[(pos as usize - *step)..(pos as usize)]);
        }
    }

    /* ---------- Helper to create a provider with optional "extra" appended data ---------- */

    async fn create_provider(
        initial_size: usize,
        extra_size: usize,
    ) -> (LocalStorageProvider, Vec<u8>, Vec<u8>) {
        let mut rng = StdRng::seed_from_u64(0xDEADBEEF);

        // Create temp file
        let mut path = std::env::temp_dir();
        path.push(unique_name("lsp_test"));

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("open temp file");

        // Write initial data
        let mut initial_data = vec![0u8; initial_size];
        rng.fill_bytes(&mut initial_data);
        file.write_all(&initial_data).unwrap();
        file.flush().unwrap();

        let provider = LocalStorageProvider::new(
            path.parent().unwrap().to_str().unwrap(),
            Some(path.file_name().unwrap().to_str().unwrap()),
        )
        .await;

        // Append extra data AFTER provider creation (so mmap does not cover it).
        let mut extra_data = Vec::new();
        if extra_size > 0 {
            extra_data = vec![0u8; extra_size];
            rng.fill_bytes(&mut extra_data);
            // Extend underlying file
            file.seek(SeekFrom::End(0)).unwrap();
            file.write_all(&extra_data).unwrap();
            file.flush().unwrap();

            let new_len = (initial_size + extra_size) as u64;
            provider.file_len.store(new_len, Ordering::Release);
        }

        (provider, initial_data, extra_data)
    }

    fn unique_name(prefix: &str) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}_{nanos}_{}", std::process::id())
    }

    #[tokio::test]
    async fn test_cross_boundary_read_main_then_temp() {
        let init = 2048usize;
        let (provider, initial_data, _extra) = create_provider(init, 0).await;

        // Stage 1024 bytes
        let mut rng = StdRng::seed_from_u64(0x1234);
        let mut staged = vec![0u8; 1024];
        rng.fill_bytes(&mut staged);
        StorageIO::append_data(&provider, &staged, false).await;

        // Start 100 bytes before end of main, read 200 -> 100 main + 100 temp
        let mut pos = (init - 100) as u64;
        let mut buf = vec![0u8; 200];
        provider.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();
        assert_eq!(pos, (init + 100) as u64);
        assert_eq!(&buf[..100], &initial_data[init - 100 .. init]);
        assert_eq!(&buf[100..], &staged[..100]);
    }

    #[tokio::test]
    async fn test_flush_then_read_full() {
        let init = 1024usize;
        let (provider, _initial_data, _extra) = create_provider(init, 0).await;

        let mut rng = StdRng::seed_from_u64(0xBEEF);
        let mut staged = vec![0u8; 2000];
        rng.fill_bytes(&mut staged);
        StorageIO::append_data(&provider, &staged, false).await;

        // Flush staged data to main
        let flushed = provider.flush_temp_to_main().await.unwrap();
        assert_eq!(flushed, staged.len());
        assert_eq!(provider.temp_file_len.load(Ordering::Acquire), 0);

        // Read back the flushed range from main
        let mut pos = init as u64;
        let mut out = vec![0u8; staged.len()];
        provider.read_data_into_buffer(&mut pos, &mut out).await.unwrap();
        assert_eq!(pos, (init + staged.len()) as u64);
        assert_eq!(out, staged);
    }

    #[tokio::test]
    async fn test_multiple_appends_then_read_back() {
        let init = 4096usize;
        let (provider, _initial_data, _extra) = create_provider(init, 0).await;

        // Prepare many chunks simulating multiple appended rows
        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut expected: Vec<u8> = Vec::new();

        for _ in 0..20 {
            let size = 64 + (rng.next_u32() as usize % 1536); // 64..1600 bytes
            let mut buf = vec![0u8; size];
            rng.fill_bytes(&mut buf);
            expected.extend_from_slice(&buf);
            chunks.push(buf);
        }

        // Append sequentially
        for ch in &chunks {
            StorageIO::append_data(&provider, ch, false).await;
        }

        // Verify logical len
        let total = expected.len();
        let len = provider.get_len().await as usize;
        assert_eq!(len, init + total);

        // Single full read of all staged bytes
        let mut pos = init as u64;
        let mut out = vec![0u8; total];
        provider.read_data_into_buffer(&mut pos, &mut out).await.unwrap();
        assert_eq!(pos, (init + total) as u64);
        assert_eq!(out, expected);

        // Also test segmented reads across boundaries
        let mut pos2 = init as u64;
        let mut consumed = 0usize;
        while consumed < total {
            let to_read = (700).min(total - consumed);
            let mut buf = vec![0u8; to_read];
            let before = pos2;
            let _ = provider.read_data_into_buffer(&mut pos2, &mut buf).await; // may return EOF on boundary; position still advances
            let delta = (pos2 - before) as usize;
            assert!(delta <= to_read);
            assert_eq!(&buf[..delta], &expected[consumed..consumed + delta]);
            consumed += delta;
        }
        assert_eq!(pos2, (init + total) as u64);
    }

    #[tokio::test]
    async fn test_concurrent_appends_and_reads() {
        use std::sync::Arc;
        use tokio::time::{sleep, Duration};

        let init = 2048usize;
        let (provider, _initial_data, _extra) = create_provider(init, 0).await;
        let provider = Arc::new(provider);
        let writer = provider.clone();
        let reader = provider.clone();

        // Precompute chunks and expected data
        let mut rng = StdRng::seed_from_u64(0x1337);
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut expected: Vec<u8> = Vec::new();
        for _ in 0..40 {
            let size = 128 + (rng.next_u32() as usize % 512);
            let mut buf = vec![0u8; size];
            rng.fill_bytes(&mut buf);
            expected.extend_from_slice(&buf);
            chunks.push(buf);
        }
        let expected = Arc::new(expected);
        let chunks = Arc::new(chunks);

        // Writer task: append chunks with tiny delays
        let w_expected = expected.clone();
        let w_chunks = chunks.clone();
        let writer_task = tokio::spawn(async move {
            for ch in w_chunks.iter() {
                StorageIO::append_data(&*writer, ch, false).await;
                // small jitter
                sleep(Duration::from_millis(1)).await;
            }
            // ensure final len is as expected
            let len = (*writer).get_len().await as usize;
            assert_eq!(len, init + w_expected.len());
        });

        // Reader task: keep reading as data becomes available
        let r_expected = expected.clone();
        let reader_task = tokio::spawn(async move {
            let mut pos = init as u64;
            let mut consumed = 0usize;
            let total = r_expected.len();

            while consumed < total {
                let current_len = reader.get_len().await;
                if pos >= current_len {
                    // nothing new yet
                    sleep(Duration::from_millis(1)).await;
                    continue;
                }
                let avail = (current_len - pos) as usize;
                let to_read = (700).min(avail);
                let mut buf = vec![0u8; to_read];
                let before = pos;
                let _ = reader.read_data_into_buffer(&mut pos, &mut buf).await; // may Err on boundary; position still advances
                let delta = (pos - before) as usize;
                assert!(delta <= to_read);
                assert_eq!(&buf[..delta], &r_expected[consumed..consumed + delta]);
                consumed += delta;
            }

            // Final assertions
            assert_eq!(pos, (init + total) as u64);
        });

        let _ = tokio::join!(writer_task, reader_task);

        // Final length check
        let final_len = provider.get_len().await as usize;
        assert_eq!(final_len, init + expected.len());
    }
}
