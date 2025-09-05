use std::{
    cmp::min, 
    fs::{
        self, 
        remove_file, 
        OpenOptions
    }, 
    path::Path, 
    sync::{
        atomic::{
            AtomicBool, 
            AtomicU64, 
            Ordering
        }, 
        Arc
    }, 
    usize
};

use std::io::{self, Cursor, ErrorKind, Read, Seek, SeekFrom, Write};

use arc_swap::ArcSwap;
use crossbeam_queue::SegQueue;
use futures::future::join_all;
use parking_lot::{Mutex, RwLock};
use tokio::{io::{AsyncReadExt, AsyncSeekExt}, task::yield_now};

use memmap2::{Mmap, MmapOptions};

#[cfg(windows)]
use std::os::windows::fs::FileExt;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use crate::{core::storage_providers::helpers::Chunk, memory_pool::MemoryBlock, IMMEDIATE_WRITE, WRITE_BATCH_SIZE, WRITE_SLEEP_DURATION};

use super::{traits::StorageIO, CRC};

#[cfg(unix)]
use super::io_uring_reader::AsyncUringReader;

type StdResult<T, E> = std::result::Result<T, E>;

const DIRECT_READ_BYPASS_FACTOR: usize = 4; // if remaining >= chunk_size * this => direct read
const CACHED_CHUNK_SIZE: usize = 32 * 1024 * 1024; // 32MB chunks

pub enum FastPathResult {
    Done,                // fully satisfied
    NeedChunk,           // must load a chunk that is not present
    EofPartial(usize),   // partial bytes copied (usize) then EOF
    EofNone,             // position at/after EOF before copying
}

pub struct LocalStorageProvider {
    pub(super) append_file: Arc<RwLock<std::fs::File>>,
    pub(super) write_file: Arc<RwLock<std::fs::File>>,
    pub(crate) location: String,
    pub(crate) table_name: String,
    pub(crate) file_str: String,
    pub file_len: AtomicU64,
    pub(crate) _memory_map: ArcSwap<Mmap>,
    pub(crate) hash: u32,
    _locked: AtomicBool,
    appender: SegQueue<MemoryBlock>,
    #[cfg(unix)]
    io_uring_reader: AsyncUringReader,
    chunk: ArcSwap<Chunk>,
    chunk_reload: Mutex<()>,
    chunk_size: usize, 
}

unsafe impl Sync for LocalStorageProvider { }
unsafe impl Send for LocalStorageProvider { }

impl Clone for LocalStorageProvider {
    fn clone(&self) -> Self {
        let map = Arc::new(unsafe { MmapOptions::new()
            .map(&std::fs::File::open(&self.file_str).unwrap())
            .unwrap() });

        Self {
            append_file: self.append_file.clone(),
            write_file: self.write_file.clone(),
            location: self.location.clone(),
            table_name: self.table_name.clone(),
            file_str: self.file_str.clone(),
            file_len: AtomicU64::new(self.file_len.load(std::sync::atomic::Ordering::SeqCst)),
            _memory_map: arc_swap::ArcSwap::new(map),
            hash: CRC.checksum(format!("{}+++{}", self.location, self.table_name).as_bytes()),
            appender: SegQueue::new(),
            _locked: AtomicBool::new(false),
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
    
            if !location_path.read_dir().is_err() {
                _ = std::fs::create_dir(location_path);
            }

            let final_location = if location.ends_with(delimiter) {
                location[location.len()-1..].replace(delimiter, "")
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

            let file_len = file_read_mmap.metadata().unwrap().len();

            let map = ArcSwap::new(Arc::new(unsafe { MmapOptions::new()
                .map(&file_read_mmap)
                .unwrap() }));

            LocalStorageProvider {
                append_file: Arc::new(RwLock::new(file_append)),
                write_file: Arc::new(RwLock::new(file_write)),
                location: final_location.to_string(),
                table_name: table_name.to_string(),
                file_str: file_str.clone(),
                file_len: AtomicU64::new(file_len),
                _memory_map: map,
                hash: CRC.checksum(format!("{}+++{}", final_location, table_name).as_bytes()),
                appender: SegQueue::new(),
                _locked: AtomicBool::new(false),
                #[cfg(unix)]
                io_uring_reader: AsyncUringReader::new(&file_str, 1024).unwrap(),
                chunk: ArcSwap::new(Chunk::empty()),
                chunk_reload: Mutex::new(()),
                chunk_size: CACHED_CHUNK_SIZE, // 16MB chunks
            }
        } else {
            let delimiter = if cfg!(unix) {
                "/"
            } else if cfg!(windows) {
                "\\"
            } else {
                panic!("OS not supported");
            };
    
            let location_str = if location.ends_with(delimiter) {
                location[location.len()-1..].replace(delimiter, "")
            } else {
                format!("{}", location)
            };

            let location_path = Path::new(&location_str);
    
            if !location_path.read_dir().is_err() {
                _ = std::fs::create_dir(location_path);
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

            let file_len = file_read_mmap.metadata().unwrap().len();

            let map = ArcSwap::new(Arc::new(unsafe { MmapOptions::new()
                .map(&file_read_mmap)
                .unwrap() }));

            LocalStorageProvider {
                append_file: Arc::new(RwLock::new(file_append)),
                write_file: Arc::new(RwLock::new(file_write)),
                location: location_str.to_string(),
                table_name: "temp.db".to_string(),
                file_str: file_str.clone(),
                file_len: AtomicU64::new(file_len),
                _memory_map: map,
                hash: CRC.checksum(format!("{}+++{}", location_str, "CONFIG_TABLE.db").as_bytes()),
                appender: SegQueue::new(),
                _locked: AtomicBool::new(false),
                #[cfg(unix)]
                io_uring_reader: AsyncUringReader::new(&file_str, 1024).unwrap(),
                chunk: ArcSwap::new(Chunk::empty()),
                chunk_reload: Mutex::new(()),
                chunk_size: CACHED_CHUNK_SIZE, // 16MB chunks
            }
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
        self.file_len.fetch_add(total_size as u64, std::sync::atomic::Ordering::Release);
        
        // Update memory map after writing
        self.update_memory_map();

        buffer.clear(); // Clear the buffer for the next batch
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

        loop {
            if self._locked.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                continue;
            } else {
                break;
            }
        }

        // Create new memory map with the current file size
        self._memory_map.store(Arc::new(unsafe {
            MmapOptions::new()
                .map(&file)
                .unwrap()
        }));

        self._locked.store(false, Ordering::Release);
    }

    #[allow(dead_code)]
    fn read_from_memory_map_or_file(
        &self, 
        position: u64, 
        buffer: &mut [u8]
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
    fn read_from_file_direct(
        &self, 
        position: u64, 
        buffer: &mut [u8]
    ) -> std::io::Result<()> {
        let mut read_file = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, 
                format!("Failed to open file: {}", e)))?;

        read_file.seek(SeekFrom::Start(position))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, 
                format!("Failed to seek to position {}: {}", position, e)))?;
        
        read_file.read_exact(buffer)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, 
                format!("Failed to read {} bytes at position {}: {}", buffer.len(), position, e)))?;

        Ok(())
    }

    /// Compute the rolling cache window for a given position.
    /// Rules:
    /// 1. End never exceeds file_len.
    /// 2. Window length equals cache_size when file_len >= cache_size.
    /// 3. Window "rolls" in fixed-size buckets aligned to cache_size, except
    ///    near EOF where it is shifted backward to keep full size.
    #[allow(dead_code)]
    fn compute_range_centered(pos: u64, file_len: u64, cache_size: u64) -> (u64,u64) {
        if file_len <= cache_size { return (0, file_len); }
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
        self.file_len.load(Ordering::Acquire)
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
        if c.contains(pos) {
            Some(c)
        } else {
            None
        }
    }

    /* --------- Legacy direct file path (if you keep it) --------- */
    #[allow(dead_code)]
    fn read_from_file_direct_legacy(&self, pos: u64, dst: &mut [u8]) -> io::Result<usize> {
        self.pread_exact_or_partial(pos, dst)
    }

     // Single large mmap copy if possible. Returns bytes copied.
    #[inline(always)]
    fn mmap_copy_region(
        &self,
        pos: u64,
        buf: &mut [u8],
        file_len: u64,
    ) -> usize {
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

    async fn append_data(&self, buffer: &[u8], _immediate: bool) {
        let buffer_len = buffer.len();

        let mut file = self.append_file.write();
        file.write_all(&buffer).unwrap();
        file.flush().unwrap();
        file.sync_all().unwrap();

        drop(file);

        self.file_len.fetch_add(buffer_len as u64, std::sync::atomic::Ordering::Release);

        self.update_memory_map();
    }

    async fn read_data(&self, position: &mut u64, length: u32) -> Vec<u8> {
        let mut read_file = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();

        read_file.seek(SeekFrom::Start(*position)).unwrap();
        let mut buffer: Vec<u8> = vec![0; length as usize];
        let read_result = read_file.read_exact(&mut buffer);
        
        if read_result.is_err() {
            return Vec::default();
        } else {
            *position += length as u64;
            return buffer;
        }
    }

    async fn read_data_to_end(&self, position: u64) -> Vec<u8> {
        let mut read_file = tokio::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .await
            .unwrap();

        read_file.seek(SeekFrom::Start(position)).await.unwrap();
        let mut buffer: Vec<u8> = Vec::default();
        read_file.read_to_end(&mut buffer).await.unwrap();
        return buffer;
    }

    async fn get_len(&self) -> u64 {
        let read_file = tokio::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .await
            .unwrap();
        
        let len = read_file.metadata().await.unwrap().len();
        len
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

    async fn read_data_into_buffer(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
    ) -> io::Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        let file_len = self.load_file_len();
        let total_len = buffer.len();

        let mut overall_copied = 0usize;

        loop {
            match self.read_data_into_buffer_fast(position, &mut buffer[overall_copied..], file_len) {
                FastPathResult::Done => {
                    return Ok(());
                }
                FastPathResult::NeedChunk => {
                    // Load chunk for current position
                    self.load_chunk_for(*position).await?;
                    continue; // retry fast path
                }
                FastPathResult::EofNone => {
                    return Err(io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "Position beyond file end",
                    ));
                }
                FastPathResult::EofPartial(copied) => {
                    overall_copied += copied;
                    // We advanced *position inside fast path.
                    return Err(io::Error::new(
                        ErrorKind::UnexpectedEof,
                        format!(
                            "Requested {} bytes, got {} (reached EOF)",
                            total_len, overall_copied
                        ),
                    ));
                }
            }
        }
        // unreachable
    }

    async fn read_data_to_cursor(&self, position: &mut u64, length: u32) -> Cursor<Vec<u8>> {
        let mut read_file = tokio::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .await
            .unwrap();

        read_file.seek(SeekFrom::Start(*position)).await.unwrap();
        let mut buffer: Vec<u8> = vec![0; length as usize];
        let result = read_file.read_exact(&mut buffer).await;
        if result.is_err() {
            Cursor::new(Vec::default())
        } else {
            *position += length as u64;
            let mut cursor = Cursor::new(buffer);
            cursor.set_position(0);
            return cursor;
        }
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
        let mut file = self.append_file.write();
        file.write_all(&buffer).unwrap();
        self.file_len.fetch_add(buffer.len() as u64, std::sync::atomic::Ordering::SeqCst);
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
            _ = std::fs::create_dir(location_path);
        }

        let final_location = if self.location.ends_with(delimiter) {
            self.location[self.location.len()-1..].replace(delimiter, "")
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

        let file_len = file_read_mmap.metadata().unwrap().len();

        Self {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            location: final_location.to_string(),
            table_name: "temp.db".to_string(),
            file_str: file_str.clone(),
            file_len: AtomicU64::new(file_len),
            _memory_map: ArcSwap::new(Arc::new(unsafe { MmapOptions::new()
                .map(&file_read_mmap)
                .unwrap() })),
            hash: CRC.checksum(format!("{}+++{}", final_location, "temp.db").as_bytes()),
            appender: SegQueue::new(),
            _locked: AtomicBool::new(false),
            #[cfg(unix)]
            io_uring_reader: AsyncUringReader::new(&file_str, 1024).unwrap(),
            chunk: ArcSwap::new(Chunk::empty()),
            chunk_reload: Mutex::new(()),
            chunk_size: CACHED_CHUNK_SIZE, // 16MB chunks
        }
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

        let location_path = Path::new(& self.location);

        if !location_path.exists() {
            _ = std::fs::create_dir(location_path);
        }

        let final_location = if self.location.ends_with(delimiter) {
            self.location[self.location.len()-1..].replace(delimiter, "")
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

        let file_len = file_read_mmap.metadata().unwrap().len();

        LocalStorageProvider {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            location: final_location.to_string(),
            table_name: name.to_string(),
            file_str: new_table.clone(),
            file_len: AtomicU64::new(file_len),
            _memory_map: ArcSwap::new(Arc::new(unsafe { MmapOptions::new()
                .map(&file_read_mmap)
                .unwrap() })),
            hash: CRC.checksum(format!("{}+++{}", final_location, name).as_bytes()),
            appender: SegQueue::new(),
            _locked: AtomicBool::new(false),
            #[cfg(unix)]
            io_uring_reader: AsyncUringReader::new(&new_table, 1024).unwrap(),
            chunk: ArcSwap::new(Chunk::empty()),
            chunk_reload: Mutex::new(()),
            chunk_size: CACHED_CHUNK_SIZE, // 16MB chunks
        }
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
        let vec = vec![self.start_append_data_service()];

        join_all(vec).await;
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
    fn into_need_chunk_hint(self) -> Self { self }
}

#[cfg(test)]
mod test {
    use std::{fs::OpenOptions, io::{Seek, SeekFrom, Write}, sync::atomic::Ordering};

    use rand::{rngs::StdRng, RngCore, SeedableRng};

    use crate::core::storage_providers::{file_sync::LocalStorageProvider, traits::StorageIO};

    #[tokio::test]
    async fn test_basic_read_full_within_mmap() {
        let file_size = 16 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        let mut pos = 0u64;
        let mut buf = vec![0u8; 3000];
        provider.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();

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
        provider.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();

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
        provider.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();

        assert_eq!(pos, (initial + 3000) as u64);
        assert_eq!(&buf, &extra_data[..3000]);

        // Second read continuing inside extra area (should hit cached chunk).
        let mut buf2 = vec![0u8; 2000];
        provider.read_data_into_buffer(&mut pos, &mut buf2).await.unwrap();
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
        let err = provider.read_data_into_buffer(&mut pos, &mut buf).await.unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        // Position advanced by bytes successfully copied (100)
        assert_eq!(pos, file_size as u64);

        assert_eq!(&buf[..100], &data[file_size - 100 .. file_size]);
    }

    #[tokio::test]
    async fn test_position_beyond_file_end_immediate_eof() {
        let file_size = 8192;
        let (provider, _data, _extra) = create_provider(file_size, 0).await;

        let mut pos = (file_size + 10) as u64;
        let mut buf = vec![0u8; 128];
        let err = provider.read_data_into_buffer(&mut pos, &mut buf).await.unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(pos, (file_size + 10) as u64); // unchanged
    }

    #[tokio::test]
    async fn test_large_read_entire_file() {
        let file_size = 32 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        let mut pos = 0u64;
        let mut buf = vec![0u8; file_size];
        provider.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();

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
            provider.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();
            assert_eq!(&buf, &data[(pos as usize - *step)..(pos as usize)]);
        }
    }

    /* ---------- Helper to create a provider with optional "extra" appended data ---------- */

    async fn create_provider(initial_size: usize, extra_size: usize)
        -> (LocalStorageProvider, Vec<u8>, Vec<u8>)
    {
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
        ).await;

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
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        format!("{prefix}_{nanos}_{}", std::process::id())
    }
}