use std::{
    cmp::min, fs::{self, remove_file}, path::Path, sync::{
        atomic::{AtomicBool, AtomicU64, Ordering}, Arc
    }, usize
};

use std::cell::RefCell;

use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};

use arc_swap::ArcSwap;
use log::error;
use parking_lot::RwLock;
use rclite::Arc as RcArc;
use opool::{Pool, PoolAllocator};
use std::sync::OnceLock;
use tokio::task::yield_now;

use memmap2::{Mmap, MmapOptions};

#[cfg(windows)]
use std::os::windows::fs::FileExt;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

// Removed chunk cache; no longer need helpers::Chunk

use super::{CRC, traits::StorageIO};

pub enum FastPathResult {
    Done,              // fully satisfied
    EofPartial(usize), // partial bytes copied (usize) then EOF
    EofNone,           // position at/after EOF before copying
}

pub struct LocalStorageProvider {
    pub(super) append_file: Arc<RwLock<std::fs::File>>,
    pub(super) write_file: Arc<RwLock<std::fs::File>>,
    // Dedicated read-only handles so read paths avoid lock contention.
    pub(super) read_file: RcArc<std::fs::File>,
    pub(crate) location: String,
    pub(crate) table_name: String,
    pub(crate) file_str: String,
    // temp staging file used for append_data()
    pub(crate) temp_file_str: String,
    pub(super) temp_file: Arc<RwLock<std::fs::File>>,
    pub(super) temp_read_file: RcArc<std::fs::File>,
    // In-memory mirror of staged temp bytes (fast path for reads).
    pub(super) temp_mem: RwLock<Vec<u8>>,
    pub(super) temp_mem_len: AtomicU64,
    // length of staged temp file bytes, maintained atomically (no metadata in read paths)
    pub(super) temp_file_len: AtomicU64,
    pub file_len: AtomicU64,
    // Memory map published with ArcSwap and owned by Arc to allow thread-local caches to own a handle safely.
    pub(crate) _memory_map: Arc<ArcSwap<Mmap>>,
    // Monotonic generation counter; bumped whenever the mmap is refreshed.
    // Used to cheaply invalidate per-thread cached mmap handles.
    pub(crate) mmap_gen: AtomicU64,
    pub(crate) hash: u32,
    _locked: AtomicBool,
    // true when there is data in temp_file waiting to be flushed
    temp_has_data: AtomicBool,
    // used to temporarily block appends during flush
    append_blocked: AtomicBool
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

        let read_file = RcArc::new(
            std::fs::File::options()
                .read(true)
                .open(&self.file_str)
                .unwrap(),
        );
        let temp_read_file = RcArc::new(
            std::fs::File::options()
                .read(true)
                .open(&self.temp_file_str)
                .unwrap(),
        );

        Self {
            append_file: self.append_file.clone(),
            write_file: self.write_file.clone(),
            read_file,
            location: self.location.clone(),
            table_name: self.table_name.clone(),
            file_str: self.file_str.clone(),
            temp_file_str: self.temp_file_str.clone(),
            temp_file: self.temp_file.clone(),
            temp_read_file,
            temp_mem: RwLock::new(self.temp_mem.read().clone()),
            temp_mem_len: AtomicU64::new(self.temp_mem_len.load(Ordering::Acquire)),
            temp_file_len: AtomicU64::new(self.temp_file_len.load(Ordering::Acquire)),
            file_len: AtomicU64::new(self.file_len.load(std::sync::atomic::Ordering::SeqCst)),
            _memory_map: Arc::new(arc_swap::ArcSwap::new(map)),
            mmap_gen: AtomicU64::new(1),
            hash: CRC.checksum(format!("{}+++{}", self.location, self.table_name).as_bytes()),
            _locked: AtomicBool::new(false),
            temp_has_data: AtomicBool::new(self.temp_has_data.load(Ordering::Relaxed)),
            append_blocked: AtomicBool::new(false)
        }
    }
}

struct ByteVecAllocator;

impl PoolAllocator<Vec<u8>> for ByteVecAllocator {
    #[inline]
    fn allocate(&self) -> Vec<u8> {
        Vec::with_capacity(1024 * 1024)
    }

    #[inline]
    fn reset(&self, obj: &mut Vec<u8>) {
        obj.clear();
    }
}

static BYTE_VEC_POOL: OnceLock<Pool<ByteVecAllocator, Vec<u8>>> = OnceLock::new();

#[inline]
fn byte_vec_pool() -> &'static Pool<ByteVecAllocator, Vec<u8>> {
    BYTE_VEC_POOL.get_or_init(|| Pool::new(64, ByteVecAllocator))
}

// Remapping a growing file can be expensive. We remap lazily only when the mmap coverage
// falls behind the logical file length by a meaningful amount.
const REMAP_GROWTH_THRESHOLD: u64 = 8 * 1024 * 1024;

thread_local! {
    // (provider_key, generation, cached mmap)
    // IMPORTANT: this must be keyed per provider; otherwise different providers would
    // incorrectly share a cached mmap within the same thread.
    static TLS_MMAP_CACHE: RefCell<(u64, u64, Option<Arc<Mmap>>)> = RefCell::new((0, 0, None));
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

            let read_file = RcArc::new(
                std::fs::File::options()
                    .read(true)
                    .open(&file_str)
                    .unwrap(),
            );

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

            let temp_read_file = RcArc::new(
                std::fs::File::options()
                    .read(true)
                    .open(&temp_file_str)
                    .unwrap(),
            );
            
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
                read_file,
                location: final_location.to_string(),
                table_name: table_name.to_string(),
                file_str: file_str.clone(),
                temp_file_str,
                temp_file: Arc::new(RwLock::new(temp_file)),
                temp_read_file,
                temp_mem: RwLock::new(Vec::new()),
                temp_mem_len: AtomicU64::new(0),
                file_len: AtomicU64::new(file_len),
                _memory_map: map,
                mmap_gen: AtomicU64::new(1),
                hash: CRC.checksum(format!("{}+++{}", final_location, table_name).as_bytes()),
                _locked: AtomicBool::new(false),
                temp_file_len: AtomicU64::new(temp_len),
                temp_has_data: AtomicBool::new(false),
                append_blocked: AtomicBool::new(false)
            };

            // If there are staged bytes in temp file, flush them into the main file.
            // This must run even when the main file is empty; otherwise bytes can be stranded
            // in the temp file across restarts and readers will observe an empty main file.
            if temp_len > 0 {
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

            let read_file = RcArc::new(
                std::fs::File::options()
                    .read(true)
                    .open(&file_str)
                    .unwrap(),
            );

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

            let temp_read_file = RcArc::new(
                std::fs::File::options()
                    .read(true)
                    .open(&temp_file_str)
                    .unwrap(),
            );
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
                read_file,
                location: location_str.to_string(),
                table_name: "temp.db".to_string(),
                file_str: file_str.clone(),
                temp_file_str,
                temp_file: Arc::new(RwLock::new(temp_file)),
                temp_read_file,
                temp_mem: RwLock::new(Vec::new()),
                temp_mem_len: AtomicU64::new(0),
                file_len: AtomicU64::new(file_len),
                _memory_map: map,
                mmap_gen: AtomicU64::new(1),
                hash: CRC.checksum(format!("{}+++{}", location_str, "CONFIG_TABLE.db").as_bytes()),
                _locked: AtomicBool::new(false),
                temp_file_len: AtomicU64::new(temp_len),
                temp_has_data: AtomicBool::new(false),
                append_blocked: AtomicBool::new(false)
            };

            if temp_len > 0 {
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

        // Snapshot temp length from atomic.
        let len = self.temp_file_len.swap(0, Ordering::AcqRel) as usize;

        if len == 0 {
            // nothing to do
            self.temp_has_data.store(false, Ordering::Release);
            return Ok(0);
        }

        // Prefer writing from the in-memory staging buffer (if it fully covers the staged range).
        let mut wrote_from_mem = false;
        if self.temp_mem_len.load(Ordering::Acquire) == len as u64 {
            let mut mem = self.temp_mem.write();
            if mem.len() == len {
                let mut main = self.append_file.write();
                main.write_all(&mem)?;
                main.flush()?;
                let _ = main.sync_all();
                mem.clear();
                self.temp_mem_len.store(0, Ordering::Release);
                wrote_from_mem = true;
            }
        }

        if !wrote_from_mem {
            let mut buf = byte_vec_pool().get();
            let cap = buf.capacity();
            if cap < len {
                buf.reserve(len - cap);
            }
            buf.resize(len, 0u8);

            // Read all staged bytes from the temp file.
            // NOTE: we keep using the locked temp handle to avoid cursor races with staging writes.
            {
                let mut temp = self.temp_file.write();
                temp.seek(SeekFrom::Start(0))?;
                temp.read_exact(&mut buf[..])?;
            }

            let mut main = self.append_file.write();
            main.write_all(&buf[..])?;
            main.flush()?;
            let _ = main.sync_all();

            // We flushed staged bytes; drop any in-memory mirror.
            self.temp_mem.write().clear();
            self.temp_mem_len.store(0, Ordering::Release);
        }

        // Truncate temp file back to zero for next round
        {
            let mut temp = self.temp_file.write();
            temp.seek(SeekFrom::Start(0))?;
            temp.set_len(0)?;
            let _ = temp.flush();
        }

        // Publish new file_len and refresh mmap
        self.file_len.fetch_add(len as u64, Ordering::Release);
        self.update_memory_map();

        // Clear pending flag
        self.temp_has_data.store(false, Ordering::Release);
        Ok(len)
    }

    fn update_memory_map(&self) {
        // Reuse the dedicated read-only handle; avoids repeated open/close overhead.
        let file = &*self.read_file;

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
            .store(Arc::new(unsafe { MmapOptions::new().map(file).unwrap() }));

        // Bump generation to invalidate thread-local caches.
        // Acquire/Release pairing ensures that if a thread observes a new generation,
        // it also observes the newly stored mmap.
        self.mmap_gen.fetch_add(1, Ordering::Release);

    self._locked.store(false, Ordering::Release);
    }

    #[inline(always)]
    fn with_cached_mmap<R>(&self, f: impl FnOnce(&Mmap) -> R) -> R {
        let provider_key = Arc::as_ptr(&self._memory_map) as usize as u64;
        let generation = self.mmap_gen.load(Ordering::Acquire);
        TLS_MMAP_CACHE.with(|cell| {
            let mut state = cell.borrow_mut();
            if state.2.is_none() || state.0 != provider_key || state.1 != generation {
                // Clone the Arc<Mmap> behind ArcSwap's guard and keep it in TLS.
                state.0 = provider_key;
                state.1 = generation;
                state.2 = Some(Arc::clone(&*self._memory_map.load()));
            }
            let mmap_arc = state.2.as_ref().unwrap();
            f(&*mmap_arc)
        })
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

    #[inline(always)]
    fn temp_mem_copy_region(&self, temp_off: u64, dst: &mut [u8]) -> usize {
        if dst.is_empty() {
            return 0;
        }

        let mem_len = self.temp_mem_len.load(Ordering::Acquire);
        if mem_len == 0 {
            return 0;
        }
        if temp_off >= mem_len {
            return 0;
        }

        let mem = self.temp_mem.read();
        let start = temp_off as usize;
        if start >= mem.len() {
            return 0;
        }
        let avail = mem.len() - start;
        let to_copy = dst.len().min(avail);
        dst[..to_copy].copy_from_slice(&mem[start..start + to_copy]);
        to_copy
    }

    // Single large mmap copy if possible. Returns bytes copied.
    #[inline(always)]
    fn mmap_copy_region(&self, pos: u64, buf: &mut [u8], file_len: u64) -> usize {
        self.with_cached_mmap(|mmap| {
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
        })
    }

    // Synchronous fast path attempt: returns bytes copied from mmap.
    fn read_from_mmap(
        &self,
        position: u64,
        buffer: &mut [u8],
        file_len: u64,
    ) -> usize {
        if buffer.is_empty() {
            return 0;
        }
        if position >= file_len {
            return 0;
        }

        // (1) Single mmap region copy first (most common path)
        self.mmap_copy_region(position, buffer, file_len)
    }

    #[inline(always)]
    async fn read_data_into_buffer_internal(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
    ) -> io::Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        // Snapshot lengths
        let main_len = self.load_file_len();
        // Avoid `await` in the hot read path; if a flush is in progress, do a short spin.
        // Flushes are expected to be rare relative to reads.
        while self.append_blocked.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }

        let temp_len = self.temp_file_len.load(Ordering::Acquire);
        let file_len = main_len + temp_len; // logical length

        // Immediate EOF if the requested position is beyond the logical end.
        // This ensures callers get UnexpectedEof and position remains unchanged.
        if *position >= file_len {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "Position beyond file end"));
        }

        let total_len = buffer.len();
        let mut overall_copied = 0usize;

        // Phase 1: read from main file only (mmap + pread), clamped to main_len.
        if *position < main_len {
            let avail = main_len - *position;
            let phase1_target = min(avail as usize, total_len - overall_copied);
            let start_copied = overall_copied;

            // 1. Try mmap
            let mmap_copied = self.read_from_mmap(*position, &mut buffer[overall_copied..overall_copied+phase1_target], main_len);
            overall_copied += mmap_copied;
            *position += mmap_copied as u64;

            // 2. Fallback to disk (blocking)
            while overall_copied < start_copied + phase1_target {
                let needed = (start_copied + phase1_target) - overall_copied;
                let file_ref = self.read_file.clone();
                let read_pos = *position;

                let mut temp_buf = byte_vec_pool().get();
                let cap = temp_buf.capacity();
                if cap < needed {
                    temp_buf.reserve(needed - cap);
                }
                unsafe { temp_buf.set_len(needed); }

                let res = tokio::task::spawn_blocking(move || {
                    #[cfg(unix)]
                    let r = file_ref.read_at(&mut temp_buf, read_pos);
                    #[cfg(windows)]
                    let r = file_ref.seek_read(&mut temp_buf, read_pos);
                    (r, temp_buf)
                }).await.map_err(|e| io::Error::new(ErrorKind::Other, e))?;

                let (read_res, temp_buf) = res;
                let n = read_res?;

                if n == 0 {
                     return Err(io::Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF in main file"));
                }

                buffer[overall_copied..overall_copied + n].copy_from_slice(&temp_buf[..n]);
                overall_copied += n;
                *position += n as u64;
            }
        }

        // Phase 2: if logical length extends into temp, read remaining bytes from temp.
        if overall_copied < total_len {
            if *position >= main_len {
                 let temp_pos = *position - main_len;
                 let to_read = min((file_len - *position) as usize, total_len - overall_copied);

                 // 1. Try memory mirror
                 let copied = self.temp_mem_copy_region(temp_pos, &mut buffer[overall_copied..overall_copied+to_read]);
                 overall_copied += copied;
                 *position += copied as u64;

                 // 2. Fallback to disk (blocking)
                 if copied < to_read {
                     let needed = to_read - copied;
                     let current_temp_pos = *position - main_len;
                     let file_ref = self.temp_read_file.clone();

                     let mut temp_buf = byte_vec_pool().get();
                     let cap = temp_buf.capacity();
                     if cap < needed {
                         temp_buf.reserve(needed - cap);
                     }
                     unsafe { temp_buf.set_len(needed); }

                     let res = tokio::task::spawn_blocking(move || {
                        #[cfg(unix)]
                        let r = file_ref.read_at(&mut temp_buf, current_temp_pos);
                        #[cfg(windows)]
                        let r = file_ref.seek_read(&mut temp_buf, current_temp_pos);
                        (r, temp_buf)
                     }).await.map_err(|e| io::Error::new(ErrorKind::Other, e))?;

                     let (read_res, temp_buf) = res;
                     let n = read_res?;

                     buffer[overall_copied..overall_copied + n].copy_from_slice(&temp_buf[..n]);
                     overall_copied += n;
                     *position += n as u64;
                 }
            }
        }

        if overall_copied == total_len {
            Ok(())
        } else {
            Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                format!("Requested {} bytes, got {} (reached logical EOF)", total_len, overall_copied),
            ))
        }
    }
}

impl StorageIO for LocalStorageProvider {
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
            // Update file length atomically and refresh mmap only if it is far behind.
            let new_len = self
                .file_len
                .fetch_add(buffer.len() as u64, Ordering::Release)
                + buffer.len() as u64;
            let mmap_len = self._memory_map.load().len() as u64;
            if new_len > mmap_len + REMAP_GROWTH_THRESHOLD {
                self.update_memory_map();
            }
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

        // Mirror staged bytes in memory for faster read-back.
        {
            let mut mem = self.temp_mem.write();
            mem.extend_from_slice(buffer);
        }
        self.temp_mem_len
            .fetch_add(buffer.len() as u64, Ordering::Release);

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

    async fn read_vectored(&self, reads: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        let mut pending = Vec::with_capacity(reads.len());
        let main_len = self.load_file_len();

        for (i, (pos, buf)) in reads.iter_mut().enumerate() {
            if buf.is_empty() { continue; }

            // Try mmap if within main file
            if *pos < main_len {
                let copied = self.read_from_mmap(*pos, buf, main_len);
                if copied == buf.len() {
                    continue; // Done
                }
                // Partial or no mmap.
                pending.push((i, *pos + copied as u64, copied));
            } else {
                // Temp file or EOF
                pending.push((i, *pos, 0));
            }
        }

        if pending.is_empty() { return Ok(()); }

        // Process pending reads
        for (idx, pos, offset) in pending {
            let (_, buf) = &mut reads[idx];
            let mut p = pos;
            self.read_data_into_buffer_internal(&mut p, &mut buf[offset..]).await?;
        }
        Ok(())
    }

    async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
        let mut file = self.write_file.write();
        file.seek(seek).unwrap();
        file.write_all(buffer).unwrap();
        file.flush().unwrap();
        file.sync_all().unwrap();
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

        let read_file = RcArc::new(
            std::fs::File::options()
                .read(true)
                .open(&file_str)
                .unwrap(),
        );

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

        let temp_read_file = RcArc::new(
            std::fs::File::options()
                .read(true)
                .open(&temp_file_str)
                .unwrap(),
        );

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
            read_file,
            location: final_location.to_string(),
            table_name: "temp.db".to_string(),
            file_str: file_str.clone(),
            temp_file_str,
            temp_file: Arc::new(RwLock::new(temp_file)),
            temp_read_file,
            temp_mem: RwLock::new(Vec::new()),
            temp_mem_len: AtomicU64::new(0),
            temp_file_len: AtomicU64::new(staged_len),
            file_len: AtomicU64::new(file_len),
            _memory_map: Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            }))),
            mmap_gen: AtomicU64::new(1),
            hash: CRC.checksum(format!("{}+++{}", final_location, "temp.db").as_bytes()),
            _locked: AtomicBool::new(false),
            temp_has_data: AtomicBool::new(false),
            append_blocked: AtomicBool::new(false),
        };

        if staged_len > 0 {
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

        let read_file = RcArc::new(
            std::fs::File::options()
                .read(true)
                .open(&file_path)
                .unwrap(),
        );

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

        let temp_read_file = RcArc::new(
            std::fs::File::options()
                .read(true)
                .open(&temp_file_str)
                .unwrap(),
        );
        
        let staged_len = std::fs::metadata(&temp_file_str).map(|m| m.len()).unwrap_or(0);

        let file_len = file_read_mmap.metadata().unwrap().len();

        let local_storage_provider = Self {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            read_file,
            location: final_location.to_string(),
            table_name: name.to_string(),
            file_str: new_table.clone(),
            temp_file_str,
            temp_file: Arc::new(RwLock::new(temp_file)),
            temp_read_file,
            temp_mem: RwLock::new(Vec::new()),
            temp_mem_len: AtomicU64::new(0),
            temp_file_len: AtomicU64::new(staged_len),
            file_len: AtomicU64::new(file_len),
            _memory_map: Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            }))),
            mmap_gen: AtomicU64::new(1),
            hash: CRC.checksum(format!("{}+++{}", final_location, name).as_bytes()),
            _locked: AtomicBool::new(false),
            temp_has_data: AtomicBool::new(false),
            append_blocked: AtomicBool::new(false)
        };

        if staged_len > 0 {
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
                    let res = p.flush_temp_to_main().await; // ignore errors for now
                    if res.is_err() {
                        error!("Error flushing temp to main: {:?}", res.err());
                    }
                } else {
                    // avoid hot spinning
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }

        tokio::join!(flush_loop(self));
    }

    fn get_name(&self) -> String {
        self.table_name.clone()
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
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}_{nanos}_{}_{}", std::process::id(), count)
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

    #[tokio::test]
    async fn test_append_immediate_then_read_back() {
        use rand::{RngCore, SeedableRng, rngs::StdRng};

        let init = 4096usize;
        let (provider, initial_data, _extra) = create_provider(init, 0).await;

        // Prepare data to append immediately (persisted directly to main file)
        let mut rng = StdRng::seed_from_u64(0xA11CE);
        let mut appended = vec![0u8; 5000];
        rng.fill_bytes(&mut appended);

        // Append with immediate=true
        StorageIO::append_data(&provider, &appended, true).await;

        // Length should reflect persisted bytes only (no temp staging)
        let len = provider.get_len().await as usize;
        assert_eq!(len, init + appended.len());

        // Verify file contents at the appended position
        let mut pos = init as u64;
        let mut out = vec![0u8; appended.len()];
        provider.read_data_into_buffer(&mut pos, &mut out).await.unwrap();
        assert_eq!(pos, (init + appended.len()) as u64);
        assert_eq!(out, appended);

        // Spot-check: earlier bytes (from initial) are unchanged
        let mut pos2 = (init - 128) as u64;
        let mut head = vec![0u8; 128];
        provider.read_data_into_buffer(&mut pos2, &mut head).await.unwrap();
        assert_eq!(head, initial_data[init - 128 .. init]);

        // verify_data API should also pass for the appended range
        let ok = StorageIO::verify_data(&provider, init as u64, &appended).await;
        assert!(ok);
    }

    #[tokio::test]
    async fn test_append_immediate_then_staged_then_read_across_both() {
        use rand::{RngCore, SeedableRng, rngs::StdRng};

        let init = 2048usize;
        let (provider, _initial_data, _extra) = create_provider(init, 0).await;

        let mut rng = StdRng::seed_from_u64(0xBADA55);
        let mut a_immediate = vec![0u8; 1500];
        rng.fill_bytes(&mut a_immediate);
        let mut b_staged = vec![0u8; 1700];
        rng.fill_bytes(&mut b_staged);

        // First append persisted immediately
        StorageIO::append_data(&provider, &a_immediate, true).await;
        // Then stage more data (not flushed yet)
        StorageIO::append_data(&provider, &b_staged, false).await;

        // Logical length should include both
        let len = provider.get_len().await as usize;
        assert_eq!(len, init + a_immediate.len() + b_staged.len());

        // Read a buffer spanning the end of immediate part into staged part
        let mut pos = (init + a_immediate.len() - 200) as u64; // start 200 bytes before boundary
        let mut out = vec![0u8; 600]; // 200 from A + 400 from B
        provider.read_data_into_buffer(&mut pos, &mut out).await.unwrap();
        assert_eq!(pos, (init + a_immediate.len() - 200 + 600) as u64);
        assert_eq!(&out[..200], &a_immediate[a_immediate.len() - 200 ..]);
        assert_eq!(&out[200..600], &b_staged[..400]);

        // Full read of the whole appended logical tail should also succeed
        let mut pos2 = init as u64;
        let mut all = vec![0u8; a_immediate.len() + b_staged.len()];
        provider.read_data_into_buffer(&mut pos2, &mut all).await.unwrap();
        assert_eq!(pos2, (init + all.len()) as u64);
        assert_eq!(all, [a_immediate.as_slice(), b_staged.as_slice()].concat());
    }

    #[tokio::test]
    async fn test_many_small_appends_mixed_then_piecewise_reads() {
        use rand::{RngCore, SeedableRng, rngs::StdRng};

        let init = 1024usize;
        let (provider, _initial_data, _extra) = create_provider(init, 0).await;

        let mut rng = StdRng::seed_from_u64(0xFACEFEED);
        let mut expected_main: Vec<u8> = Vec::new();
        let mut expected_temp: Vec<u8> = Vec::new();
        // Alternate between immediate and staged appends, variable sizes
        for i in 0..30 {
            let size = 50 + (rng.next_u32() as usize % 700); // 50..749 bytes
            let mut buf = vec![0u8; size];
            rng.fill_bytes(&mut buf);
            let immediate = i % 2 == 0; // even -> immediate, odd -> staged
            StorageIO::append_data(&provider, &buf, immediate).await;
            if immediate {
                expected_main.extend_from_slice(&buf);
            } else {
                expected_temp.extend_from_slice(&buf);
            }
        }

        // Verify logical length matches
        let len = provider.get_len().await as usize;
        assert_eq!(len, init + expected_main.len() + expected_temp.len());

        // Piecewise reads of random sizes, ensuring correctness across boundaries
        let mut pos = init as u64;
        let mut consumed = 0usize;
        let combined: Vec<u8> = {
            let mut v = expected_main.clone();
            v.extend_from_slice(&expected_temp);
            v
        };
        while consumed < combined.len() {
            let remaining = combined.len() - consumed;
            let step = 1 + (rng.next_u32() as usize % 500); // 1..500 bytes
            let to_read = step.min(remaining);
            let mut buf = vec![0u8; to_read];
            let before = pos;
            let _ = provider.read_data_into_buffer(&mut pos, &mut buf).await; // may EOF at exact logical end
            let got = (pos - before) as usize;
            assert_eq!(&buf[..got], &combined[consumed .. consumed + got]);
            consumed += got;
        }
        assert_eq!(pos, (init + combined.len()) as u64);
    }

    #[tokio::test]
    async fn test_service_flush_from_temp_to_main_with_length_checks() {
        use rand::{RngCore, SeedableRng, rngs::StdRng};
        use tokio::time::{sleep, Duration};
        use std::sync::Arc;

        // Initial main file content
        let init = 4096usize;
        let (provider_raw, initial_data, _extra) = create_provider(init, 0).await;
        let provider = Arc::new(provider_raw);

        // Prepare staged bytes (append to temp only)
        let mut rng = StdRng::seed_from_u64(0xFEEDFACE);
        let mut staged = vec![0u8; 3000];
        rng.fill_bytes(&mut staged);

        // Append to TMP (no immediate write)
        StorageIO::append_data(&*provider, &staged, false).await;

        // Length checks BEFORE flush
        let main_len_before = provider.file_len.load(Ordering::Acquire) as usize;
        let temp_len_before = provider.temp_file_len.load(Ordering::Acquire) as usize;
        let total_len_before = provider.get_len().await as usize;
        assert_eq!(main_len_before, init);
        assert_eq!(temp_len_before, staged.len());
        assert_eq!(total_len_before, init + staged.len());

        // Read across boundary BEFORE flush: last 100 of main + first 200 of temp
        let mut pos = (init - 100) as u64;
        let mut out = vec![0u8; 300];
        provider.read_data_into_buffer(&mut pos, &mut out).await.unwrap();
        assert_eq!(pos, (init + 200) as u64);
        assert_eq!(&out[..100], &initial_data[init - 100 .. init]);
        assert_eq!(&out[100..], &staged[..200]);

        // Read within temp BEFORE flush
        let mut pos2 = init as u64;
        let mut temp_slice = vec![0u8; 512];
        provider.read_data_into_buffer(&mut pos2, &mut temp_slice).await.unwrap();
        assert_eq!(&temp_slice, &staged[..512]);

        // Start background service which includes the flush loop
        let p = provider.clone();

        tokio::spawn(async move {
            tokio::join!(p.start_service());
        });

        // Wait for flush to complete (temp_len -> 0 and main_len increased)
        let expected_main_after = init + staged.len();
        let mut waited = 0u64;
        
        loop {
            let ml = provider.file_len.load(Ordering::Acquire) as usize;
            let tl = provider.temp_file_len.load(Ordering::Acquire) as usize;
            if ml == expected_main_after && tl == 0 { break; }
            if waited > 5_000 { // 5s timeout
                panic!("Timed out waiting for background flush: main_len={ml}, temp_len={tl}");
            }
            sleep(Duration::from_millis(10)).await;
            waited += 10;
        }

        // Length checks AFTER flush
        let main_len_after = provider.file_len.load(Ordering::Acquire) as usize;
        let temp_len_after = provider.temp_file_len.load(Ordering::Acquire) as usize;
        let total_len_after = provider.get_len().await as usize;
        assert_eq!(main_len_after, init + staged.len());
        assert_eq!(temp_len_after, 0);
        assert_eq!(total_len_after, init + staged.len());

        // Read appended region AFTER flush (should be from main now)
        let mut pos3 = init as u64;
        let mut out2 = vec![0u8; staged.len()];
        provider.read_data_into_buffer(&mut pos3, &mut out2).await.unwrap();
        assert_eq!(pos3, (init + staged.len()) as u64);
        assert_eq!(out2, staged);
    }

    #[tokio::test]
    async fn test_read_vectored() {
        let init = 4096usize;
        let (provider, data, _extra) = create_provider(init, 0).await;

        let mut buf1 = vec![0u8; 100];
        let mut buf2 = vec![0u8; 200];
        let mut buf3 = vec![0u8; 50];

        // We need to construct the slice of tuples.
        // Since &mut [u8] is not Copy, we can't easily put them in a vec and then take a slice of mutable references?
        // Wait, `read_vectored` takes `&mut [(u64, &mut [u8])]`.
        // We can create a mutable array or vec of tuples.
        
        let mut reads: Vec<(u64, &mut [u8])> = vec![
            (0, &mut buf1),
            (1000, &mut buf2),
            (4000, &mut buf3),
        ];

        provider.read_vectored(&mut reads).await.unwrap();

        assert_eq!(&buf1[..], &data[0..100]);
        assert_eq!(&buf2[..], &data[1000..1200]);
        assert_eq!(&buf3[..], &data[4000..4050]);
    }
}
