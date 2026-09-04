use std::{
    cmp::min,
    fs::{self, remove_file},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    usize,
};

use std::cell::RefCell;

use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};

use arc_swap::ArcSwap;
use cacheguard::CacheGuard;
use log::error;
use opool::{Pool, PoolAllocator};
use parking_lot::{Mutex, RwLock};
use rclite::Arc as RcArc;
use std::sync::OnceLock;
use tokio::task::yield_now;

use memmap2::{Mmap, MmapOptions};
use smallvec::SmallVec;

// Removed chunk cache; no longer need helpers::Chunk

use super::{CRC, traits::StorageIO};

pub enum FastPathResult {
    Done,              // fully satisfied
    EofPartial(usize), // partial bytes copied (usize) then EOF
    EofNone,           // position at/after EOF before copying
}

/// Lets one flush make several concurrent appends durable.
///
/// Every immediate append used to issue its own `fsync`, so N clients inserting
/// at once paid N device flushes even though a single flush persists everything
/// written before it. This tracks which appends are already covered so waiters
/// can share one.
#[derive(Default)]
struct CommitState {
    /// Appends whose `write` has completed. Bumped under the append lock, so it
    /// can never run ahead of the bytes it stands for.
    ///
    /// Not padded. Splitting these two onto separate cache lines was tried and
    /// measured *worse* — `insert_shape_100_rows` went from -2.0% to +2.3%
    /// against the same baseline (p = 0.00 both ways). The waiters that poll
    /// `durable` are already serialised behind `gate`, so the false sharing the
    /// padding was meant to remove was not there to remove, and the cost of
    /// filling a second line on every append was.
    issued: AtomicU64,
    /// Highest `issued` value known to be on stable storage.
    durable: AtomicU64,
    /// Serializes the flush itself.
    gate: Mutex<()>,
    /// Flush handle, opened on first use. Deliberately *not* `append_file`: a
    /// flush must not hold the append lock, or the next appender would block on
    /// it and there would be no group left to commit.
    handle: OnceLock<std::fs::File>,
    /// The first flush failure, after which nothing here is durable again.
    ///
    /// Permanent for the reason the log's poisoning is: Linux drops the dirty
    /// pages an `fdatasync` reported as failed, so a later flush that succeeds
    /// does not cover them. The failure used to be discarded and the sequence
    /// published as durable anyway, which on the no-log path is the only
    /// durability mechanism reporting a success it did not have.
    poisoned: OnceLock<String>,
}

impl CommitState {
    /// Records a completed append. Call with the append lock held.
    #[inline]
    fn issue(&self) -> u64 {
        self.issued.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Returns once every append up to `seq` is on stable storage, or the
    /// error that means it never will be.
    fn flush_through(&self, seq: u64, path: &str) -> io::Result<()> {
        if let Some(why) = self.poisoned.get() {
            return Err(Self::poisoned_error(why));
        }
        if self.durable.load(Ordering::Acquire) >= seq {
            return Ok(()); // an earlier flush already covered us
        }

        let _gate = self.gate.lock();

        if self.durable.load(Ordering::Acquire) >= seq {
            return Ok(()); // covered while we waited for the gate
        }

        // Read the counter *before* flushing. The flush also persists every
        // append issued after ours, so publishing the highest sequence written
        // so far — not just our own — is what collapses a burst of concurrent
        // appends into a single device flush.
        let covered = self.issued.load(Ordering::Acquire);

        if self.handle.get().is_none() {
            match std::fs::File::options().read(true).write(true).open(path) {
                Ok(file) => {
                    let _ = self.handle.set(file);
                }
                Err(e) => return Err(self.poison(format!("open {} for flush: {}", path, e))),
            }
        }
        let file = self.handle.get().expect("set just above");

        // `sync_data` (fdatasync) rather than `sync_all` (fsync): an append only
        // needs its bytes and the new file size durable to be readable after a
        // crash. Forcing out mtime/ctime costs an extra journal transaction and
        // buys nothing here.
        if let Err(e) = file.sync_data() {
            return Err(self.poison(format!("fdatasync {}: {}", path, e)));
        }

        self.durable.fetch_max(covered, Ordering::AcqRel);
        Ok(())
    }

    /// Records the first failure and returns it as the permanent kind.
    fn poison(&self, why: String) -> io::Error {
        error!("flush failed; file is no longer durable: {}", why);
        let why = self.poisoned.get_or_init(|| why);
        Self::poisoned_error(why)
    }

    fn poisoned_error(why: &str) -> io::Error {
        io::Error::other(format!("{}: {}", super::traits::POISONED_LOG, why))
    }

    /// The failure that stopped this file being durable, if one happened.
    fn poison_error(&self) -> Option<io::Error> {
        self.poisoned.get().map(|why| Self::poisoned_error(why))
    }
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
    pub(super) temp_mem_len: CacheGuard<AtomicU64>,
    // length of staged temp file bytes, maintained atomically (no metadata in read paths)
    pub(super) temp_file_len: CacheGuard<AtomicU64>,
    /// Logical file length — and, for `append_data_txn`, the **allocator**:
    /// each append takes its offset from `file_len.fetch_add(len)`.
    ///
    /// It therefore *leads* the on-disk length by construction. **Never refresh
    /// it from `metadata()`.** Doing so is not a correction but a race against
    /// the allocator: an append that has written its bytes but not yet claimed
    /// its offset leaves the file momentarily longer than `file_len`, so a
    /// metadata-derived `fetch_max` raises it and the append's own `fetch_add`
    /// then double-counts — the length ends one record too long and that append
    /// takes an offset that overlaps its predecessor.
    ///
    /// That is a real bug that has been in this file, not a hypothetical. It
    /// reproduced at 2 runs in 12 in release, isolated, which is exactly the
    /// rate that gets written off as flake. The trap is that the *staged*
    /// append path (`append_data(.., immediate = false)`, writing to the `.tmp`
    /// sidecar) genuinely does let the on-disk length lag, so the two append
    /// paths look interchangeable at the call site and disagree about whether
    /// the disk is authoritative. It is not, here.
    ///
    /// To make an in-place write's extension visible, use [`Self::inplace_end`],
    /// which derives the target from the write rather than from the file.
    pub file_len: CacheGuard<AtomicU64>,
    /// Highest end offset (`position + len`) an in-place write in flight needs
    /// the file to reach, raised at commit once the bytes are actually applied.
    ///
    /// The apply in `commit_txn` writes by path and bypasses this provider, so
    /// `file_len` would otherwise never learn that an in-place write extended
    /// the file. Deriving the target from the *write* rather than from
    /// `metadata()` is what keeps it safe: `append_data_txn` treats `file_len`
    /// as the allocator — it hands out each append's offset from `fetch_add` —
    /// so a value read back off disk can race an append that has written its
    /// bytes but not yet claimed its offset, and double-count. A write's own
    /// `position + len` cannot outrun the allocator, because a provider that
    /// appends only ever overwrites *within* the length it already handed out.
    pub(super) inplace_end: CacheGuard<AtomicU64>,
    // Memory map published with ArcSwap and owned by Arc to allow thread-local caches to own a handle safely.
    pub(crate) _memory_map: Arc<ArcSwap<Mmap>>,
    // Monotonic generation counter; bumped whenever the mmap is refreshed.
    // Used to cheaply invalidate per-thread cached mmap handles.
    pub(crate) mmap_gen: CacheGuard<AtomicU64>,
    /// Identifies this provider's mapping in the per-thread cache, for the life
    /// of the process. See [`next_mmap_key`].
    pub(crate) mmap_key: u64,
    pub(crate) hash: u32,
    _locked: CacheGuard<AtomicBool>,
    // true when there is data in temp_file waiting to be flushed
    temp_has_data: CacheGuard<AtomicBool>,
    // used to temporarily block appends during flush
    append_blocked: CacheGuard<AtomicBool>,
    /// Shared across clones so all handles to a file commit as one group.
    commit: Arc<CommitState>,
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
            temp_mem_len: AtomicU64::new(self.temp_mem_len.load(Ordering::Acquire)).into(),
            temp_file_len: AtomicU64::new(self.temp_file_len.load(Ordering::Acquire)).into(),
            file_len: AtomicU64::new(self.file_len.load(std::sync::atomic::Ordering::SeqCst))
                .into(),
            inplace_end: AtomicU64::new(self.inplace_end.load(std::sync::atomic::Ordering::SeqCst))
                .into(),
            _memory_map: Arc::new(arc_swap::ArcSwap::new(map)),
            mmap_gen: AtomicU64::new(1).into(),
            mmap_key: next_mmap_key(),
            hash: CRC.checksum(format!("{}+++{}", self.location, self.table_name).as_bytes()),
            _locked: AtomicBool::new(false).into(),
            temp_has_data: AtomicBool::new(self.temp_has_data.load(Ordering::Relaxed)).into(),
            append_blocked: AtomicBool::new(false).into(),
            commit: self.commit.clone(),
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

/// Hands each provider a key that is never reused.
///
/// The per-thread cache above used to key on `Arc::as_ptr(&self._memory_map)`,
/// which is an address — and an address is only unique among the objects alive
/// at one moment. Drop a provider and build another and the allocator hands
/// back the same block, so the new provider looked up the old one's entry; with
/// `mmap_gen` at 1 on both, as it is on every freshly-opened file, the
/// generation did not tell them apart either and the read was served from the
/// *previous file's* mapping.
///
/// What that looks like from outside is a file whose length is right and whose
/// bytes belong to a different file. `STORAGE MOVE TABLE` was the clearest
/// case, because it opens a table's files one after another and drops each
/// before opening the next: a moved table arrived with its CHECK-constraint
/// sidecar full of the field sidecar's bytes and the constraint silently
/// stopped being enforced. Anything that opens files in sequence on one thread
/// was exposed.
///
/// A counter costs the same to read as the pointer did and cannot repeat.
fn next_mmap_key() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
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

            // Trim any trailing separators. The previous form sliced off the
            // last character and stripped it, which turned "/data/db/" into an
            // empty string — so files were created at the filesystem root and
            // the constructor panicked.
            let final_location = location.trim_end_matches(delimiter).to_string();

            let file_str = format!("{}{}{}", final_location, delimiter, table_name);

            let file_path = Path::new(&file_str);

            if !file_path.exists() && !file_path.is_file() {
                _ = std::fs::File::create(&file_str).unwrap();
                let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&file_str));
                // Creating a file writes a directory entry, which is only
                // durable once the directory itself is flushed. Without this a
                // crash can lose the file even though its contents were synced.
                let _ = crate::core::wal::sync_parent_dir(file_path);
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

            let read_file =
                RcArc::new(std::fs::File::options().read(true).open(&file_str).unwrap());

            // Ensure temp staging file exists next to main file
            let temp_file_str = format!("{}.tmp", file_str);

            if !Path::new(&temp_file_str).exists() {
                _ = std::fs::File::create(&temp_file_str).unwrap();
                let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&temp_file_str));
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
            let temp_len = std::fs::metadata(&temp_file_str)
                .map(|m| m.len())
                .unwrap_or(0);

            #[cfg(test)]
            let temp_len = {
                // If we're operating under the system temp directory (typical for tests),
                // ensure temp file is clean; keep main file intact to avoid nuking freshly written data.
                let sys_tmp = std::env::temp_dir().to_string_lossy().to_string();

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
                    std::fs::metadata(&temp_file_str)
                        .map(|m| m.len())
                        .unwrap_or(0)
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
                location: final_location.to_string(),
                table_name: table_name.to_string(),
                file_str: file_str.clone(),
                temp_file_str,
                temp_file: Arc::new(RwLock::new(temp_file)),
                temp_read_file,
                temp_mem: RwLock::new(Vec::new()),
                temp_mem_len: AtomicU64::new(0).into(),
                file_len: AtomicU64::new(file_len).into(),
                inplace_end: AtomicU64::new(0).into(),
                _memory_map: map,
                mmap_gen: AtomicU64::new(1).into(),
                mmap_key: next_mmap_key(),
                hash: CRC.checksum(format!("{}+++{}", final_location, table_name).as_bytes()),
                _locked: AtomicBool::new(false).into(),
                temp_file_len: AtomicU64::new(temp_len).into(),
                temp_has_data: AtomicBool::new(false).into(),
                append_blocked: AtomicBool::new(false).into(),
                commit: Default::default(),
            };

            // If there are staged bytes in temp file, flush them into the main file.
            // This must run even when the main file is empty; otherwise bytes can be stranded
            // in the temp file across restarts and readers will observe an empty main file.
            if temp_len > 0 {
                let _ = local_storage_provider.flush_temp_to_main().await;
            }

            local_storage_provider.register_with_wal();
            local_storage_provider
        } else {
            let delimiter = if cfg!(unix) {
                "/"
            } else if cfg!(windows) {
                "\\"
            } else {
                panic!("OS not supported");
            };

            // Same trailing-separator fix as above.
            let location_str = location.trim_end_matches(delimiter).to_string();

            let location_path = Path::new(&location_str);

            if !location_path.exists() {
                _ = std::fs::create_dir_all(location_path);
            }

            let file_str = format!("{}{}{}", location_str, delimiter, "CONFIG_TABLE.db");

            _ = std::fs::File::create(&file_str).unwrap();
            let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&file_str));

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

            let read_file =
                RcArc::new(std::fs::File::options().read(true).open(&file_str).unwrap());

            // Ensure temp staging file exists next to main file
            let temp_file_str = format!("{}.tmp", file_str);
            if !Path::new(&temp_file_str).exists() {
                _ = std::fs::File::create(&temp_file_str).unwrap();
                let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&temp_file_str));
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
            let temp_len = std::fs::metadata(&temp_file_str)
                .map(|m| m.len())
                .unwrap_or(0);

            #[cfg(test)]
            let temp_len = {
                // If we're operating under the system temp directory (typical for tests),
                // ensure temp file is clean; keep main file intact to avoid nuking freshly written data.
                let sys_tmp = std::env::temp_dir().to_string_lossy().to_string();

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
                    std::fs::metadata(&temp_file_str)
                        .map(|m| m.len())
                        .unwrap_or(0)
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
                temp_mem_len: AtomicU64::new(0).into(),
                file_len: AtomicU64::new(file_len).into(),
                inplace_end: AtomicU64::new(0).into(),
                _memory_map: map,
                mmap_gen: AtomicU64::new(1).into(),
                mmap_key: next_mmap_key(),
                hash: CRC.checksum(format!("{}+++{}", location_str, "CONFIG_TABLE.db").as_bytes()),
                _locked: AtomicBool::new(false).into(),
                temp_file_len: AtomicU64::new(temp_len).into(),
                temp_has_data: AtomicBool::new(false).into(),
                append_blocked: AtomicBool::new(false).into(),
                commit: Default::default(),
            };

            if temp_len > 0 {
                let _ = local_storage_provider.flush_temp_to_main().await;
            }

            local_storage_provider.register_with_wal();
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

        // Read the staged length; it is NOT cleared here. Clearing it up
        // front made the logical length — `file_len + temp_file_len`, which is
        // what `get_len` reports and what a scan turns into a slot count —
        // DIP for the whole duration of the write and its fsync below: the
        // staged rows had stopped counting while the main file had not yet
        // grown. A scan in that window saw fewer slots than exist and simply
        // did not reach the newest version of a row, so a reader could miss a
        // row that was merely being updated, and a writer could land on the
        // older version and report a serialization conflict against a
        // transaction that had already finished. Appends are blocked for the
        // whole of this function, so nothing can add to the staged length
        // while it is being published.
        let len = self.temp_file_len.load(Ordering::Acquire) as usize;

        if len == 0 {
            // nothing to do
            self.temp_has_data.store(false, Ordering::Release);
            return Ok(0);
        }

        // Prefer writing from the in-memory staging buffer (if it fully covers the staged range).
        let mut wrote_from_mem = false;
        if self.temp_mem_len.load(Ordering::Acquire) == len as u64 {
            let mem = self.temp_mem.write();
            if mem.len() == len {
                let mut main = self.append_file.write();
                main.write_all(&mem)?;
                main.flush()?;
                let _ = main.sync_all();
                // The mirror is deliberately NOT cleared yet. Until the length
                // is published below, a read of these offsets is still served
                // from the staged copy; dropping it here would leave the bytes
                // reachable from neither place.
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

            // As above: the mirror is dropped after the length is published,
            // not before.
        }

        // The map has to cover the bytes before the length that describes them
        // is published, or a scan is told about slots the map cannot serve.
        self.update_memory_map();

        // Publish. The two counters move together under the append lock, which
        // `get_len` also takes, so no reader can observe the instant between
        // them — one order would dip and the other would overshoot, and both
        // are wrong for a scan that turns the number into a slot count.
        {
            let _publish = self.append_file.write();
            self.file_len.fetch_add(len as u64, Ordering::Release);
            self.temp_file_len.fetch_sub(len as u64, Ordering::Release);
        }

        // Only now is the staged copy redundant: these offsets are served from
        // the main file, and the length says so.
        self.temp_mem.write().clear();
        self.temp_mem_len.store(0, Ordering::Release);
        {
            let mut temp = self.temp_file.write();
            temp.seek(SeekFrom::Start(0))?;
            temp.set_len(0)?;
            let _ = temp.flush();
        }

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
        let provider_key = self.mmap_key;
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

    /// This file's id within its database directory, as the log records it.
    #[inline]
    fn wal_file_id(&self) -> u64 {
        crate::core::wal::file_id_for_name(&self.table_name)
    }

    /// Makes a write durable without a log: flush this file, group-committed.
    ///
    /// The failure is recorded rather than returned — the append paths that
    /// call this cannot report one — and surfaces from `commit_durable`, which
    /// every statement runs before it reports success.
    #[inline]
    fn flush_self(&self, seq: u64) {
        let _ = self.commit.flush_through(seq, &self.file_str);
    }

    /// Publishes this file so a replay can find it by path.
    #[inline]
    fn register_with_wal(&self) {
        if let Some(w) = crate::core::wal::wal_for(&self.location) {
            w.register_file(self.wal_file_id(), &self.file_str);
        }
    }

    #[inline(always)]
    fn load_file_len(&self) -> u64 {
        // Writers publish length with Release; use Acquire to avoid seeing stale lengths
        // that can produce spurious UnexpectedEof under concurrency.
        self.file_len.load(Ordering::Acquire)
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
    fn read_from_mmap(&self, position: u64, buffer: &mut [u8], file_len: u64) -> usize {
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

        // Avoid `await` in the hot read path; if a flush is in progress, do a
        // short spin. Flushes are expected to be rare relative to reads.
        while self.append_blocked.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }

        // BOTH lengths after the spin, and both under the publish lock.
        // Reading the main length *before* waiting for the flush and the
        // staged length *after* it meant a flush landing in between was
        // counted in neither: the main length was the one from before it
        // published and the staged length had already been cleared. The
        // logical end then came out short, and this returned `UnexpectedEof`
        // for a position that is well inside the file — which a scan reads as
        // the end of the table, so it stops early and the rows past that point
        // simply do not appear. A reader could miss a row that exists while
        // another connection was updating it.
        let (main_len, temp_len) = {
            let _publish = self.append_file.read();
            (
                self.load_file_len(),
                self.temp_file_len.load(Ordering::Acquire),
            )
        };
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

        // Phase 1: read from main file only (mmap + pread), clamped to main_len.
        if *position < main_len {
            let avail = main_len - *position;
            let phase1_target = min(avail as usize, total_len - overall_copied);
            let start_copied = overall_copied;

            // 1. Try mmap
            let mut mmap_copied = self.read_from_mmap(
                *position,
                &mut buffer[overall_copied..overall_copied + phase1_target],
                main_len,
            );
            overall_copied += mmap_copied;
            *position += mmap_copied as u64;

            // 1b. If mmap is stale (file extended past current mmap coverage),
            // refresh it once and retry. This avoids the very expensive
            // spawn_blocking pread fallback below — a single remap covers
            // millions of subsequent reads, while spawn_blocking incurs tokio
            // scheduler + parking_lot mutex overhead on every call.
            if overall_copied < start_copied + phase1_target {
                let mmap_len_now = self._memory_map.load().len() as u64;
                if main_len > mmap_len_now {
                    self.update_memory_map();
                    let still_needed = (start_copied + phase1_target) - overall_copied;
                    let extra = self.read_from_mmap(
                        *position,
                        &mut buffer[overall_copied..overall_copied + still_needed],
                        main_len,
                    );
                    overall_copied += extra;
                    *position += extra as u64;
                    mmap_copied += extra;
                }
            }
            let _ = mmap_copied;

            // 2. Fallback to synchronous pread (no spawn_blocking). pread is a
            // single syscall that returns immediately for in-cache pages; the
            // overhead of spawn_blocking (tokio blocking-pool mutex + condvar)
            // dwarfs the syscall itself. Briefly blocking the worker is fine
            // here because the only reason we get here is a true cache miss
            // beyond mmap coverage that the remap above couldn't fix.
            while overall_copied < start_copied + phase1_target {
                let needed = (start_copied + phase1_target) - overall_copied;
                let dst = &mut buffer[overall_copied..overall_copied + needed];

                #[cfg(unix)]
                let n = {
                    use std::os::unix::fs::FileExt;
                    self.read_file.read_at(dst, *position)?
                };
                #[cfg(windows)]
                let n = {
                    use std::os::windows::fs::FileExt;
                    self.read_file.seek_read(dst, *position)?
                };

                if n == 0 {
                    return Err(io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "Unexpected EOF in main file",
                    ));
                }

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
                let copied = self.temp_mem_copy_region(
                    temp_pos,
                    &mut buffer[overall_copied..overall_copied + to_read],
                );
                overall_copied += copied;
                *position += copied as u64;

                // 2. Fallback to synchronous pread (no spawn_blocking).
                if copied < to_read {
                    let needed = to_read - copied;
                    let current_temp_pos = *position - main_len;
                    let dst = &mut buffer[overall_copied..overall_copied + needed];

                    #[cfg(unix)]
                    let n = {
                        use std::os::unix::fs::FileExt;
                        self.temp_read_file.read_at(dst, current_temp_pos)?
                    };
                    #[cfg(windows)]
                    let n = {
                        use std::os::windows::fs::FileExt;
                        self.temp_read_file.seek_read(dst, current_temp_pos)?
                    };

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
                format!(
                    "Requested {} bytes, got {} (reached logical EOF)",
                    total_len, overall_copied
                ),
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
        let txn = self.begin_txn();
        let token = self.write_data_txn(txn, position, buffer).await;
        // `write_data` has no way to report this; callers that need to know use
        // the transactional form and commit it themselves.
        if let Err(e) = self.commit_durable(token).await {
            error!("in-place write to {} is not durable: {}", self.file_str, e);
        }
    }

    fn begin_txn(&self) -> u64 {
        match crate::core::wal::wal_for(&self.location) {
            Some(w) => w.begin(),
            None => 0,
        }
    }

    async fn write_data_txn(&self, txn: u64, position: u64, buffer: &[u8]) -> u64 {
        // Recorded now, applied to `file_len` at commit — see `inplace_end`.
        // Raising the length here instead would publish a length whose bytes
        // are not in the file yet, since the apply is deferred to commit.
        self.inplace_end
            .fetch_max(position + buffer.len() as u64, Ordering::AcqRel);
        if let Some(w) = crate::core::wal::wal_for(&self.location) {
            match w.log_in_place(txn, self.wal_file_id(), position, buffer) {
                // The data file is deliberately untouched here; `commit_durable`
                // applies it once the record is durable. Overwriting first would
                // destroy the old bytes with no undo record to restore them.
                Ok(_) => return txn,
                Err(e) => error!(
                    "WAL in-place log failed ({}); writing {} directly",
                    e, self.file_str
                ),
            }
        }

        {
            let mut file = self.write_file.write();
            file.seek(SeekFrom::Start(position)).unwrap();
            file.write_all(buffer).unwrap();
            file.flush().unwrap();
        }
        self.flush_self(self.commit.issue());
        0
    }

    async fn append_data_txn(&self, txn: u64, buffer: &[u8]) -> (u64, u64) {
        let mut file = self.append_file.write();
        file.write_all(buffer).unwrap();
        file.flush().unwrap();
        let offset = self
            .file_len
            .fetch_add(buffer.len() as u64, Ordering::Release);
        let seq = self.commit.issue();
        let new_len = offset + buffer.len() as u64;

        // Release the append lock before any flush. Holding it across the flush
        // would serialize appends behind the device and leave nothing for the
        // shared commit to batch.
        drop(file);

        // A directory being bulk-loaded takes neither a log record nor a flush:
        // the loader syncs every file once at the end, and a load that dies
        // before then is discarded whole. See `core::bulk_load`.
        let token = if crate::core::bulk_load::is_active(&self.location) {
            0
        } else {
            match crate::core::wal::wal_for(&self.location) {
                Some(w) => match w.log(txn, self.wal_file_id(), offset, buffer) {
                    Ok(_) => txn,
                    Err(e) => {
                        error!(
                            "WAL append failed ({}); flushing {} directly",
                            e, self.file_str
                        );
                        self.flush_self(seq);
                        0
                    }
                },
                None => {
                    self.flush_self(seq);
                    0
                }
            }
        };

        let mmap_len = self._memory_map.load().len() as u64;
        if new_len > mmap_len + REMAP_GROWTH_THRESHOLD {
            self.update_memory_map();
        }

        (offset, token)
    }

    async fn commit_durable(&self, token: u64) -> io::Result<()> {
        // Checked for every token, including the no-log path's `0`: a flush
        // that failed after an append is this file's failure to report, and
        // this is the one call every writer makes before reporting success.
        if let Some(e) = self.commit.poison_error() {
            return Err(e);
        }
        if token == 0 {
            return Ok(());
        }
        let Some(w) = crate::core::wal::wal_for(&self.location) else {
            return Ok(());
        };
        // Deferred: this is a row or pointer write, and the statement that
        // asked for it takes one sync for all of them before reporting success.
        if let Err(e) = w.commit_txn_deferred(token) {
            // Logged as well as returned. The error goes to the caller so it can
            // stop reporting success; the log line stays because it is what
            // surfaced this from a live server, and an operator watching logs
            // should not have to also be reading the client's errors.
            error!("WAL commit failed: {}", e);
            // Poisoning is permanent by design, so it has to arrive at the
            // caller as something other than "try again".
            if w.is_poisoned() {
                return Err(io::Error::other(format!(
                    "{}: {}",
                    super::traits::POISONED_LOG,
                    e
                )));
            }
            return Err(e);
        }

        // The apply inside `commit_txn` writes to the data file *by path*, with
        // its own handle — this provider is bypassed entirely, so its cached
        // `file_len` and its mmap never learn that their own file grew. Without
        // this refresh a provider cannot read back an in-place write it just
        // made: the bytes are on disk and durable, and its next read still EOFs.
        //
        // `fetch_max` rather than `store`, because appends maintain `file_len`
        // themselves and stage through a temp file — the on-disk length can
        // legitimately lag the logical one, and a length must never go
        // backwards.

        // The apply inside `commit_txn` wrote by path, bypassing this provider,
        // so its cached length and mmap have not seen the bytes. Raise them to
        // what the in-place writes required. Without this a provider cannot
        // read back an in-place write it just made: the bytes are durably on
        // disk and its own next read still EOFs.
        //
        // `fetch_max`, never `store` — a length must not go backwards past a
        // concurrent append.
        let need = self.inplace_end.load(Ordering::Acquire);
        if need > 0 {
            // The MAP is refreshed before the LENGTH that describes those
            // bytes is published, never after. The other order left a window
            // in which `get_len` already reported the longer file while the
            // map still covered less of it — and a read past the end of the
            // mapped region does not fail, it copies **zero bytes** (see the
            // `pos >= mmap_len` guard in the mmap read path). A scan turns the
            // length into a slot count, so in that window it walked slots the
            // map could not serve, read them as empty, and skipped the rows
            // living there: a reader could miss a row that exists while
            // another connection was merely updating it in place, and a writer
            // that missed the current version could then collide with the
            // older one and report a serialization conflict against a
            // transaction that had already finished.
            //
            // Mapping first is safe and costs nothing: `update_memory_map`
            // maps the file at its real on-disk size, and these bytes are
            // already written and synced by the time this runs — so the map
            // covers `need` before anything is told to look there.
            if need > self.file_len.load(Ordering::Acquire) {
                self.update_memory_map();
            }
            self.file_len.fetch_max(need, Ordering::AcqRel);
        }

        if w.bytes_since_checkpoint() >= crate::wal_checkpoint_bytes() {
            // Off the caller's thread: a checkpoint fsyncs every data file, and
            // making whichever writer happened to cross the threshold wait for
            // all of that would show up as an unpredictable latency spike.
            // `Wal::checkpoint` already ensures only one runs at a time.
            let w = w.clone();
            tokio::task::spawn_blocking(move || {
                if let Err(e) = w.checkpoint() {
                    error!("WAL checkpoint failed: {}", e);
                }
            });
        }
        Ok(())
    }

    async fn truncate(&self, len: u64) -> io::Result<()> {
        // Staged appends live past the current end, so they are exactly the
        // bytes a truncation would drop. Land them first and let the caller see
        // a length that accounts for them, rather than silently discarding
        // writes someone has already been told succeeded.
        let _ = self.flush_temp_to_main().await;

        let current = self.load_file_len();
        if len > current {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("truncate to {} would grow a {}-byte file", len, current),
            ));
        }
        if len == current {
            return Ok(());
        }

        // Appends must not land past the new end while we shorten the file.
        while self
            .append_blocked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            yield_now().await;
        }
        struct Unblock<'a>(&'a AtomicBool);
        impl Drop for Unblock<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _unblock = Unblock(&self.append_blocked);

        {
            let file = self.write_file.write();
            file.set_len(len)?;
            file.sync_all()?;
        }
        let _ = crate::core::wal::sync_parent_dir(Path::new(&self.file_str));

        self.file_len.store(len, Ordering::Release);

        // `inplace_end` is a high-water claim about where in-place writes have
        // reached, and `commit_durable` raises `file_len` to it. Truncation
        // makes any claim past the new end false, so leaving it would have the
        // *next* in-place commit restore the pre-truncation length: the provider
        // then reports a file longer than it is, and `RowPointerIterator` walks
        // records that do not exist. Measured — truncate to 1024 with a mark at
        // 4096, and the next commit reported 4096 against 1024 bytes on disk.
        //
        // `fetch_min` rather than `store`, for the same reason the other
        // direction uses `fetch_max`: a length claim must only ever move the way
        // the operation making it can justify.
        self.inplace_end.fetch_min(len, Ordering::AcqRel);
        // The old mapping still spans the discarded range; reading through it
        // after the file shrank is a SIGBUS, not a short read.
        self.update_memory_map();

        Ok(())
    }

    async fn append_data(&self, buffer: &[u8], immediate: bool) {
        let _ = self.append_data_returning_offset(buffer, immediate).await;
    }

    async fn append_data_returning_offset(&self, buffer: &[u8], immediate: bool) -> u64 {
        if immediate {
            let txn = self.begin_txn();
            let (offset, token) = self.append_data_txn(txn, buffer).await;
            if let Err(e) = self.commit_durable(token).await {
                error!("append to {} is not durable: {}", self.file_str, e);
            }
            return offset;
        }

        // If a flush is ongoing, wait briefly until it completes to keep ordering simple.
        while self.append_blocked.load(Ordering::Acquire) {
            yield_now().await;
        }

        // Stage the file write, the in-memory mirror and the published length in
        // one critical section. Taking these separately let two concurrent
        // stagers land in one order in the temp file and the opposite order in
        // `temp_mem`, leaving the mirror disagreeing with the file it mirrors.
        //
        // Lock order is `temp_mem` then `temp_file`, matching the flush path
        // (which nests `temp_mem` -> `append_file` and never takes `temp_file`
        // while holding `temp_mem`).
        let mut mem = self.temp_mem.write();
        let mut tf = self.temp_file.write();

        // Ensure writes go to the end
        tf.seek(SeekFrom::End(0)).unwrap();
        tf.write_all(buffer).unwrap();
        let _ = tf.flush();

        // Mirror staged bytes in memory for faster read-back.
        mem.extend_from_slice(buffer);

        let staged_before = self
            .temp_file_len
            .fetch_add(buffer.len() as u64, Ordering::Release);
        self.temp_mem_len
            .fetch_add(buffer.len() as u64, Ordering::Release);

        // Mark that we have staged data
        self.temp_has_data.store(true, Ordering::Release);

        drop(tf);
        drop(mem);

        // Logical offset = persisted main file + staged bytes ahead of us.
        self.file_len.load(Ordering::Acquire) + staged_before
    }

    fn try_borrow_range(&self, position: u64, len: usize) -> Option<super::traits::BorrowedBytes> {
        if len == 0 {
            return None;
        }

        // Staged writes live outside the mapping, so part of the range might
        // not be visible through it. Decline and let the caller copy.
        if self.temp_has_data.load(Ordering::Relaxed) {
            return None;
        }

        let main_len = self.load_file_len();
        let end = position.checked_add(len as u64)?;
        if end > main_len {
            return None;
        }

        // Hold our own handle to the mapping. If a concurrent append re-maps
        // the file, this Arc keeps the region we handed out alive.
        let map: std::sync::Arc<Mmap> = std::sync::Arc::clone(&*self._memory_map.load());
        if end > map.len() as u64 {
            return None;
        }

        let ptr = unsafe { map.as_ptr().add(position as usize) };
        // SAFETY: the range was just bounds-checked against the mapping, and
        // `map` keeps it mapped for as long as the guard lives.
        Some(unsafe { super::traits::BorrowedBytes::new(map, ptr, len) })
    }

    async fn get_len(&self) -> u64 {
        // Logical length = main persisted on disk + staged temp
        // Re-read filesystem metadata to avoid stale length on freshly opened handles.
        // Both counters under the publish lock: `flush_temp_to_main` moves a
        // run of bytes from the staged file to the main one, and the sum is
        // only meaningful either side of that move, never inside it.
        let _publish = self.append_file.read();
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
        // Pending entries are rare (only when mmap can't satisfy the read fully),
        // so a SmallVec avoids a per-call heap allocation in the common case.
        let mut pending: SmallVec<[(usize, u64, usize); 16]> = SmallVec::new();
        let main_len = self.load_file_len();

        // Hoist the TLS borrow + Arc<Mmap> resolution out of the per-entry loop.
        // Previously each entry called `mmap_copy_region` -> `with_cached_mmap`,
        // which performed a TLS RefCell borrow and an `ArcSwap::load` per entry.
        self.with_cached_mmap(|mmap| {
            let mmap_bytes: &[u8] = &mmap[..];
            let mmap_len = mmap_bytes.len() as u64;

            for (i, (pos, buf)) in reads.iter_mut().enumerate() {
                if buf.is_empty() {
                    continue;
                }

                if *pos < main_len && *pos < mmap_len {
                    let max_span = (mmap_len - *pos).min(main_len - *pos);
                    let to_copy = min(buf.len(), max_span as usize);
                    if to_copy > 0 {
                        let start = *pos as usize;
                        buf[..to_copy].copy_from_slice(&mmap_bytes[start..start + to_copy]);
                    }
                    if to_copy == buf.len() {
                        continue;
                    }
                    pending.push((i, *pos + to_copy as u64, to_copy));
                } else {
                    pending.push((i, *pos, 0));
                }
            }
        });

        if pending.is_empty() {
            return Ok(());
        }

        // Process pending reads (temp file or mmap-miss fallbacks)
        for (idx, pos, offset) in pending {
            let (_, buf) = &mut reads[idx];
            let mut p = pos;
            self.read_data_into_buffer_internal(&mut p, &mut buf[offset..])
                .await?;
        }
        Ok(())
    }

    /// Synchronous fast path: copies every read directly out of the mmap if
    /// possible. Returns true only when every entry was fully satisfied — the
    /// caller can then skip the async `read_vectored` entirely (no future
    /// poll, no waker setup, no SmallVec for pending).
    #[inline]
    fn try_read_vectored_sync(&self, reads: &mut [(u64, &mut [u8])]) -> bool {
        let main_len = self.load_file_len();
        // Hot path: do not even attempt this if there is staged temp data —
        // some reads might land there and we would have to fall back anyway.
        if self.temp_has_data.load(Ordering::Relaxed) {
            return false;
        }

        // Single TLS+ArcSwap resolution, then plain memcpy per entry.
        self.with_cached_mmap(|mmap| {
            let mmap_bytes: &[u8] = &mmap[..];
            let mmap_len = mmap_bytes.len() as u64;

            for (pos, buf) in reads.iter_mut() {
                if buf.is_empty() {
                    continue;
                }
                let p = *pos;
                let blen = buf.len();
                // Need every byte to come from mmap (and within main_len).
                if p >= main_len || p >= mmap_len {
                    return false;
                }
                let avail = (mmap_len - p).min(main_len - p);
                if (blen as u64) > avail {
                    return false;
                }
                let start = p as usize;
                buf.copy_from_slice(&mmap_bytes[start..start + blen]);
            }
            true
        })
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

        let final_location = self
            .location
            .trim_end_matches(|c| c == '\\' || c == '/')
            .to_string();

        let file_str = format!("{}{}{}", final_location, delimiter, "temp.db");

        _ = std::fs::File::create(&file_str).unwrap();
        let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&file_str));

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

        let read_file = RcArc::new(std::fs::File::options().read(true).open(&file_str).unwrap());

        // Do NOT truncate here. We may be opening an existing companion file (e.g. FIELDS.db)
        // from tests or runtime, and truncation would destroy previously appended data.
        let file_len = file_read_mmap.metadata().unwrap().len();

        let temp_file_str = format!("{}.tmp", file_str);
        if !Path::new(&temp_file_str).exists() {
            _ = std::fs::File::create(&temp_file_str).unwrap();
            let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&temp_file_str));
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
        let mut staged_len = std::fs::metadata(&temp_file_str)
            .map(|m| m.len())
            .unwrap_or(0);

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
            temp_mem_len: AtomicU64::new(0).into(),
            temp_file_len: AtomicU64::new(staged_len).into(),
            file_len: AtomicU64::new(file_len).into(),
            inplace_end: AtomicU64::new(0).into(),
            _memory_map: Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            }))),
            mmap_gen: AtomicU64::new(1).into(),
            mmap_key: next_mmap_key(),
            hash: CRC.checksum(format!("{}+++{}", final_location, "temp.db").as_bytes()),
            _locked: AtomicBool::new(false).into(),
            temp_has_data: AtomicBool::new(false).into(),
            append_blocked: AtomicBool::new(false).into(),
            commit: Default::default(),
        };

        if staged_len > 0 {
            let _ = local_storage_provider.flush_temp_to_main().await;
        }

        local_storage_provider.register_with_wal();
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
    async fn create_new_at(&self, location: &str, name: String) -> Self {
        // `new` and `create_new` build the same thing from the same inputs; the
        // only difference is which directory the name is resolved against, and
        // `new` takes that as an argument. Duplicating the body here to avoid
        // one `Option` would be a second copy of the opening sequence to keep
        // in step.
        Self::new(location, Some(&name)).await
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

        let final_location = self
            .location
            .trim_end_matches(|c| c == '\\' || c == '/')
            .to_string();

        let new_table = format!("{}{}{}", final_location, delimiter, name);
        let file_path = Path::new(&new_table);

        if !file_path.exists() {
            _ = std::fs::File::create(&file_path).unwrap();
            let _ = crate::core::wal::sync_parent_dir(file_path);
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
            let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&temp_file_str));
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

        let staged_len = std::fs::metadata(&temp_file_str)
            .map(|m| m.len())
            .unwrap_or(0);

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
            temp_mem_len: AtomicU64::new(0).into(),
            temp_file_len: AtomicU64::new(staged_len).into(),
            file_len: AtomicU64::new(file_len).into(),
            inplace_end: AtomicU64::new(0).into(),
            _memory_map: Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            }))),
            mmap_gen: AtomicU64::new(1).into(),
            mmap_key: next_mmap_key(),
            hash: CRC.checksum(format!("{}+++{}", final_location, name).as_bytes()),
            _locked: AtomicBool::new(false).into(),
            temp_has_data: AtomicBool::new(false).into(),
            append_blocked: AtomicBool::new(false).into(),
            commit: Default::default(),
        };

        if staged_len > 0 {
            let _ = local_storage_provider.flush_temp_to_main().await;
        }

        local_storage_provider.register_with_wal();
        local_storage_provider
    }

    fn drop_io(&self) {
        // Best-effort cleanup. These files are temporary staging artifacts and
        // may already be gone (or still open on Windows). Do not panic.
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let file_str = format!("{}{}{}", self.location, delimiter, self.table_name);
        let _ = remove_file(Path::new(&file_str));

        // Also remove the companion temp staging file created by this provider.
        let _ = remove_file(Path::new(&self.temp_file_str));
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

    use rand::{Rng, SeedableRng, rngs::StdRng};

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
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};
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
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap();
        assert_eq!(pos, (init + 100) as u64);
        assert_eq!(&buf[..100], &initial_data[init - 100..init]);
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
        provider
            .read_data_into_buffer(&mut pos, &mut out)
            .await
            .unwrap();
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
        provider
            .read_data_into_buffer(&mut pos, &mut out)
            .await
            .unwrap();
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
    async fn an_in_place_write_is_visible_through_the_provider_that_made_it() {
        // With a log installed, `write_data_txn` defers the bytes and
        // `commit_txn` applies them by opening the target *by path* — bypassing
        // the provider entirely. The provider's cached `file_len` and its mmap
        // therefore never learn that their own file grew.
        //
        // The failure shape is nasty: the write succeeds, is durable, and is on
        // disk, while the writer's own next read reports EOF. Nothing hits it
        // today by luck rather than design — the filter writes its checkpoint
        // and never re-reads it in-process — but that is one refactor from
        // being wrong, so it is pinned here.
        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().to_str().unwrap().to_string();

        let wal = std::sync::Arc::new(
            crate::core::wal::Wal::open(&dir.path().join("rastdb.wal")).unwrap(),
        );
        crate::core::wal::install_for(&location, wal);

        let provider = LocalStorageProvider::new(&location, Some("inplace_vis.db")).await;
        assert_eq!(provider.get_len().await, 0, "fresh file should be empty");

        let payload = vec![7u8; 512];
        provider.write_data(0, &payload).await;

        let on_disk = std::fs::metadata(dir.path().join("inplace_vis.db"))
            .map(|m| m.len())
            .unwrap_or(0);
        assert_eq!(on_disk, 512, "the bytes must reach disk");

        assert_eq!(
            provider.get_len().await,
            512,
            "the provider that issued the write must see its own file grow"
        );

        let mut buf = vec![0u8; 512];
        let mut pos = 0u64;
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .expect("the writer must be able to read back what it just wrote");
        assert_eq!(buf, payload);
    }

    #[tokio::test]
    async fn test_concurrent_appends_and_reads() {
        use std::sync::Arc;
        use tokio::time::{Duration, sleep};

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
        use rand::{Rng, SeedableRng, rngs::StdRng};

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
        provider
            .read_data_into_buffer(&mut pos, &mut out)
            .await
            .unwrap();
        assert_eq!(pos, (init + appended.len()) as u64);
        assert_eq!(out, appended);

        // Spot-check: earlier bytes (from initial) are unchanged
        let mut pos2 = (init - 128) as u64;
        let mut head = vec![0u8; 128];
        provider
            .read_data_into_buffer(&mut pos2, &mut head)
            .await
            .unwrap();
        assert_eq!(head, initial_data[init - 128..init]);

        // verify_data API should also pass for the appended range
        let ok = StorageIO::verify_data(&provider, init as u64, &appended).await;
        assert!(ok);
    }

    #[tokio::test]
    async fn test_append_immediate_then_staged_then_read_across_both() {
        use rand::{Rng, SeedableRng, rngs::StdRng};

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
        provider
            .read_data_into_buffer(&mut pos, &mut out)
            .await
            .unwrap();
        assert_eq!(pos, (init + a_immediate.len() - 200 + 600) as u64);
        assert_eq!(&out[..200], &a_immediate[a_immediate.len() - 200..]);
        assert_eq!(&out[200..600], &b_staged[..400]);

        // Full read of the whole appended logical tail should also succeed
        let mut pos2 = init as u64;
        let mut all = vec![0u8; a_immediate.len() + b_staged.len()];
        provider
            .read_data_into_buffer(&mut pos2, &mut all)
            .await
            .unwrap();
        assert_eq!(pos2, (init + all.len()) as u64);
        assert_eq!(all, [a_immediate.as_slice(), b_staged.as_slice()].concat());
    }

    #[tokio::test]
    async fn test_many_small_appends_mixed_then_piecewise_reads() {
        use rand::{Rng, SeedableRng, rngs::StdRng};

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
            assert_eq!(&buf[..got], &combined[consumed..consumed + got]);
            consumed += got;
        }
        assert_eq!(pos, (init + combined.len()) as u64);
    }

    #[tokio::test]
    async fn test_service_flush_from_temp_to_main_with_length_checks() {
        use rand::{Rng, SeedableRng, rngs::StdRng};
        use std::sync::Arc;
        use tokio::time::{Duration, sleep};

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
        provider
            .read_data_into_buffer(&mut pos, &mut out)
            .await
            .unwrap();
        assert_eq!(pos, (init + 200) as u64);
        assert_eq!(&out[..100], &initial_data[init - 100..init]);
        assert_eq!(&out[100..], &staged[..200]);

        // Read within temp BEFORE flush
        let mut pos2 = init as u64;
        let mut temp_slice = vec![0u8; 512];
        provider
            .read_data_into_buffer(&mut pos2, &mut temp_slice)
            .await
            .unwrap();
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
            if ml == expected_main_after && tl == 0 {
                break;
            }
            if waited > 5_000 {
                // 5s timeout
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
        provider
            .read_data_into_buffer(&mut pos3, &mut out2)
            .await
            .unwrap();
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

        let mut reads: Vec<(u64, &mut [u8])> =
            vec![(0, &mut buf1), (1000, &mut buf2), (4000, &mut buf3)];

        provider.read_vectored(&mut reads).await.unwrap();

        assert_eq!(&buf1[..], &data[0..100]);
        assert_eq!(&buf2[..], &data[1000..1200]);
        assert_eq!(&buf3[..], &data[4000..4050]);
    }

    /// A provider's per-thread cache key is never one a dropped provider used.
    ///
    /// This is the invariant, and it is the one that broke. The cache was keyed
    /// on `Arc::as_ptr(&self._memory_map)` — an address, which is unique only
    /// among the objects alive at one moment. Drop a provider, open another,
    /// and the allocator hands back the same block; with `mmap_gen` at 1 on
    /// both, as it is on every freshly-opened file, nothing else told them
    /// apart and the second file's reads were served from the first file's
    /// mapping. The read reported success and returned another file's bytes at
    /// this file's length.
    ///
    /// Asserted on the key rather than on the bytes because the bytes only go
    /// wrong when the allocator actually reuses the block, and whether it does
    /// is not something a test controls — see
    /// [`a_reopened_file_does_not_read_the_previous_one`], which exercises the
    /// same path and did *not* reproduce it in-process even though a server
    /// reproduced it every time.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_providers_cache_key_is_never_reissued() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_string_lossy().to_string();
        std::fs::write(dir.path().join("first.db"), b"A").unwrap();
        std::fs::write(dir.path().join("second.db"), b"B").unwrap();

        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            for name in ["first.db", "second.db"] {
                let io = LocalStorageProvider::new(&root, Some(name)).await;
                assert!(
                    seen.insert(io.mmap_key),
                    "{name} was given a cache key a dropped provider already used, so a \
                     read through it can be answered from that provider's mapping"
                );
                // A clone maps the file again rather than sharing the handle,
                // so it is a second mapping and needs a key of its own — two
                // containers that can be remapped independently must not share
                // one cache entry.
                assert!(
                    seen.insert(io.clone().mmap_key),
                    "a clone reused a key, but it holds a mapping of its own"
                );
            }
        }
    }

    /// The same hazard from the outside: two files read one after the other,
    /// each provider dropped before the next is opened, must each return their
    /// own bytes.
    ///
    /// Kept even though it does not reproduce the bug in-process. It is the
    /// shape `copy_between` uses — siblings off one parent provider, opened and
    /// dropped in turn — and a future change that breaks the read path in a way
    /// the key invariant does not cover fails here.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reopened_file_does_not_read_the_previous_one() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_string_lossy().to_string();

        // Different lengths, so a wrong answer is a wrong *prefix* rather than
        // only wrong bytes — the way the real corruption showed up.
        let first = vec![b'A'; 306];
        let second = vec![b'B'; 643];
        std::fs::write(dir.path().join("first.db"), &first).unwrap();
        std::fs::write(dir.path().join("second.db"), &second).unwrap();

        // Opened as a table's files are: siblings off one parent provider, each
        // dropped at the end of its turn before the next is opened. That is the
        // shape `copy_between` uses, and the shape that reproduced.
        let parent = LocalStorageProvider::new(&root, Some("parent.db")).await;
        for pass in 0..64 {
            for (name, expected) in [("first.db", &first), ("second.db", &second)] {
                let io = parent.create_new(name.to_string()).await;
                let mut buffer = vec![0u8; expected.len()];
                io.read_data_into_buffer(&mut 0, &mut buffer).await.unwrap();
                assert_eq!(
                    &buffer, expected,
                    "pass {pass}: {name} returned another file's bytes at its own length"
                );
            }
        }
    }
}
