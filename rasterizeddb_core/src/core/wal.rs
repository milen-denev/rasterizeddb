//! Write-ahead log.
//!
//! # Why
//!
//! Without a log, every durable write has to reach stable storage on its own: an
//! INSERT appends row bytes to one file and a pointer record to another, so it
//! pays two device flushes, and a crash between them leaves the row half-written
//! with no way to tell.
//!
//! The log turns that into one sequential append plus one flush of a single
//! file. Because a flush persists everything written before it, concurrent
//! writers share it. Data files then need no flush on the write path at all;
//! they are made durable in bulk at a checkpoint.
//!
//! # What it guarantees
//!
//! **Durability** — a write that has been committed is on stable storage.
//!
//! **Atomicity** — records carry a transaction id, and replay applies a record
//! only if some record for the same transaction carried the commit flag. A crash
//! partway through a multi-file write therefore leaves *none* of it applied
//! rather than half. This is what lets an INSERT (row bytes + pointer record) or
//! an in-place UPDATE be all-or-nothing.
//!
//! It does **not** provide isolation: readers can still observe a committed
//! write while a later transaction is in flight.
//!
//! # What is logged
//!
//! Physical redo records: "these bytes belong at this offset in this file".
//! Replaying one is idempotent, so recovery can be re-run and it does not matter
//! whether the data file already received the write before the crash. Ordering
//! between the data write and the log write is therefore irrelevant to
//! correctness; what matters is that the log record is durable before the write
//! is acknowledged.
//!
//! # Record layout
//!
//! ```text
//! magic u32 | version u8 | flags u8 | _pad u16 | txn u64
//!           | file_id u64 | offset u64 | len u32 | crc u32 | payload[len]
//! ```
//!
//! The CRC covers the header up to (not including) the CRC field, plus the
//! payload. A crash leaves a partially written record at the tail; recovery
//! stops at the first record that is short, mis-magicked, of an unknown version,
//! or fails its CRC, and treats everything before it as the durable prefix.
//!
//! `file_id` is derived from the file's *name within the database directory*,
//! never its absolute path, so a database directory can be moved or restored
//! under a different mount point and its log still replays.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use branches::{likely, unlikely};
use parking_lot::Mutex;

use super::storage_providers::CRC;

/// `"RDWL"`, so a stale or foreign file is rejected rather than replayed.
const RECORD_MAGIC: u32 = 0x5244_574C;

/// Bumped whenever the record layout changes. Recovery refuses versions it does
/// not understand instead of misreading them as data.
const FORMAT_VERSION: u8 = 1;

/// This record completes its transaction; everything logged under the same txn
/// id becomes eligible for replay.
const FLAG_COMMIT: u8 = 1 << 0;

/// Once its transaction commits, every record for this `file_id` logged before
/// it is void: the file it described was deleted. See [`Wal::forget_files`].
///
/// A flag rather than a version bump. A build that predates it reads the
/// record as a zero-length write and applies nothing, so its replay is the
/// one it always did rather than a refusal to open the log at all.
const FLAG_FORGET: u8 = 1 << 1;

/// magic + version + flags + pad + txn + file_id + offset + len + crc
const HEADER_LEN: usize = 4 + 1 + 1 + 2 + 8 + 8 + 8 + 4 + 4;
/// Bytes of the header covered by the CRC (everything before the CRC field).
const HEADER_CRC_LEN: usize = HEADER_LEN - 4;

/// The mark that carries `wal_path`'s commit sequence across a truncation.
fn lsn_mark_path(wal_path: &Path) -> PathBuf {
    let mut name = wal_path.file_name().unwrap_or_default().to_os_string();
    name.push(LSN_MARK_SUFFIX);
    wal_path.with_file_name(name)
}

/// The sequence the last truncation recorded, or 0 when there is no usable
/// mark.
///
/// Unreadable is treated as absent rather than fatal: the log itself still
/// carries every sequence it has not checkpointed away, and refusing to open a
/// database over a stray file would be the worse failure.
fn read_lsn_mark(wal_path: &Path) -> u64 {
    let path = lsn_mark_path(wal_path);
    match std::fs::read_to_string(&path) {
        Ok(text) => match text.trim().parse::<u64>() {
            Ok(lsn) => lsn,
            Err(_) => {
                log::warn!(
                    "{} does not hold a commit sequence; the log's own records will have to \
                     supply it",
                    path.display()
                );
                0
            }
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
        Err(e) => {
            log::warn!("Could not read {}: {}", path.display(), e);
            0
        }
    }
}

/// Records `lsn` as the floor a later open resumes from.
///
/// Written to a temporary name and renamed, because an in-place rewrite that
/// tore across a crash could leave a *lower* number than the one already
/// issued — which is the one outcome this file exists to prevent.
fn write_lsn_mark(wal_path: &Path, lsn: u64) -> io::Result<()> {
    let path = lsn_mark_path(wal_path);
    let temp = path.with_extension("lsn.partial");
    {
        let mut file = File::create(&temp)?;
        file.write_all(lsn.to_string().as_bytes())?;
        crate::core::sync_audit::note("wal.lsn_mark", &path);
        file.sync_all()?;
    }
    std::fs::rename(&temp, &path)?;
    sync_parent_dir(&path)
}

/// Refuses absurd record lengths before allocating for them, so a corrupt header
/// cannot make recovery try to reserve gigabytes.
const MAX_RECORD_PAYLOAD: usize = 256 * 1024 * 1024;

/// One process-wide order for writers coming to be and for fences being taken:
/// see [`Incarnation`].
static INCARNATIONS: AtomicU64 = AtomicU64::new(1);

/// When a file's writer came to be, in the one order [`Wal::forget_files`]
/// fences are taken in. A writer born before a fence of its file id writes to
/// the deleted file, and the log refuses its records (see [`Wal::log_from`]):
/// a record logged after the barrier is not voided by it, and the next open
/// would write it into whatever file took the name since.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Incarnation(u64);

impl Incarnation {
    /// Exempt from every fence: for callers that are no file's writer.
    pub const UNFENCED: Incarnation = Incarnation(u64::MAX);

    pub fn new() -> Incarnation {
        Incarnation(INCARNATIONS.fetch_add(1, Ordering::AcqRel))
    }
}

impl Default for Incarnation {
    fn default() -> Self {
        Self::new()
    }
}

/// Name of the log inside a database directory.
pub const DEFAULT_WAL_NAME: &str = "rasterized.wal";

/// Extension of a cut log segment waiting in the staging directory.
pub const SEGMENT_SUFFIX: &str = ".wseg";

/// Suffix of the file that carries the commit sequence across a truncation.
///
/// A checkpoint discards the log once its records are in the data files, so a
/// cleanly stopped server leaves nothing to recover the sequence from and the
/// counter would restart at zero. That is not merely untidy: the sequence is
/// the point-in-time token a backup records on every snapshot and every
/// archived bundle, and one that repeats itself makes a bundle from an earlier
/// run compare as newer than a snapshot taken after it. This file is the
/// floor, and the records still in the log are anything above it.
pub const LSN_MARK_SUFFIX: &str = ".lsn";

/// Extension a segment carries while it is still being copied. Renamed to
/// [`SEGMENT_SUFFIX`] once complete, so the archiver never uploads a half file.
pub const SEGMENT_PARTIAL_SUFFIX: &str = ".wseg.partial";

/// Identifies a data file within its database directory.
///
/// Derived from the file name alone, so the id survives the directory being
/// moved. Renaming a *table* would change it — the engine has no table rename,
/// and if one is added it must rewrite or checkpoint the log first.
pub fn file_id_for_name(name: &str) -> u64 {
    // 64-bit FNV-1a: wider than the CRC32 used for provider hashes, so ids do
    // not start colliding once a database has many files.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}

/// Forces a directory entry to stable storage.
///
/// Creating a file writes a directory entry, and that entry is only durable once
/// the *directory* is flushed. Without this a crash can lose the log file
/// itself — leaving nothing to replay, which defeats the whole mechanism.
pub fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    // Opening a directory read-only and fsyncing it is the portable-on-Unix way
    // to flush its entries. On Windows there is no directory handle to sync and
    // file creation is already ordered, so this is a no-op there.
    #[cfg(unix)]
    {
        let dir = File::open(parent)?;
        crate::core::sync_audit::note("dir", path);
        dir.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
    }
    Ok(())
}

/// Outcome of replaying a log.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Records replayed into data files.
    pub applied: u64,
    /// Highest commit sequence seen in the log. A reopened log resumes from
    /// here, so the sequence stays monotonic across restarts.
    pub last_commit_lsn: u64,
    /// Records belonging to a transaction that never committed. Discarded —
    /// this is the atomicity guarantee doing its job.
    pub rolled_back: u64,
    /// Records skipped because their `file_id` is not registered.
    pub unresolved: u64,
    /// Records skipped because a later committed [`Wal::forget_files`]
    /// barrier voided them: their file was deleted after they were logged.
    pub forgotten: u64,
    /// Bytes of trailing garbage ignored — a partial record from a crash.
    pub truncated_tail: u64,
    /// Highest transaction id any record in the log carries. A log that is
    /// kept after recovery must not see these ids handed out again: its
    /// commit markers would mark the new transactions committed.
    pub last_txn: u64,
}

/// Trailing bytes a scan did not validate, given the log's length and how far
/// the scan got.
///
/// Saturating, because the two arguments are measured at different instants:
/// the length before the scan and the progress after it. `valid` only counts
/// records whose header and payload both read whole and whose CRC matched, so
/// it can never claim bytes that were not there — but it can out-run a length
/// read earlier, if anything appended in between.
///
/// Re-measuring the length after the scan instead would make the subtraction
/// non-negative and be worse: it would count whatever the other writer appended
/// as trailing garbage, and [`open_and_recover`] answers a non-zero tail by
/// truncating the log. That turns a stale statistic into deleted records. Zero
/// is the honest answer — every byte the scan validated was kept, so nothing
/// was discarded.
///
/// Plain subtraction panicked in debug and wrapped to nearly 2^64 in release,
/// where that same truncate would then fire on a log being actively written.
fn discarded_tail(total: u64, valid: u64) -> u64 {
    total.saturating_sub(valid)
}

/// The log.
///
/// The atomics here are deliberately **not** cache-line padded, and that is a
/// measured decision rather than an oversight. Wrapping all twelve in a
/// `CacheGuard` was tried and reverted: on `append_benchmarks` in WAL mode it
/// cost +13% on `insert_shape_100_rows` and **+31% on `concurrent_4x50`**
/// (p = 0.00), and removing the padding again returned every case to parity.
///
/// The reason is that padding here only ever had a downside to trade. False
/// sharing needs concurrent *independent* access, and these words do not have
/// it — `write_pos` is maintained under the writer lock, the flush is
/// serialised by `gate`, and a commit reads or updates `issued`, `next_txn`,
/// `commit_lsn`, `bytes_since_checkpoint`, `pending_count` and `pending_sync`
/// one after another in the same call. Packed, that is one or two lines the
/// committing core already owns. Padded, it is seven separate 128-byte lines
/// to fill per commit, for contention the surrounding mutexes had already
/// removed.
///
/// If this is revisited, measure `append_benchmarks` on tmpfs
/// (`RASTDB_BENCH_DIR=/dev/shm RASTDB_BENCH_WAL=1`) — on a real device the
/// fsync buries the entire effect in both directions.
pub struct Wal {
    path: PathBuf,
    /// Append handle. Held only while framing a record.
    writer: Mutex<File>,
    /// Records whose bytes have been written to the log file.
    issued: AtomicU64,
    /// Byte offset of the log's append point, maintained under the writer lock.
    write_pos: AtomicU64,
    /// Highest `issued` value known to be on stable storage.
    durable: AtomicU64,
    /// Serializes the flush so waiters share one.
    gate: Mutex<()>,
    /// Flush handle. Separate from `writer` so a flush in progress does not
    /// block the next record being framed — without that overlap there is no
    /// group left to commit.
    ///
    /// Behind a lock rather than set once, because a checkpoint that keeps a
    /// tail replaces the log file (see [`Wal::checkpoint`]) and the next sync
    /// must reach the new one. Cloned out and synced with the lock released.
    sync_handle: Mutex<Option<Arc<File>>>,
    /// Next transaction id. Never reused within a log generation.
    next_txn: AtomicU64,
    /// Monotonic sequence stamped on each commit, in commit order.
    ///
    /// Carried in the commit marker's otherwise-unused `offset` field, so it
    /// costs no extra bytes. It is the point-in-time token that snapshot reads,
    /// replication and point-in-time recovery all need; nothing consumes it yet.
    commit_lsn: AtomicU64,
    /// Bytes appended since the last checkpoint, for triggering the next one.
    bytes_since_checkpoint: AtomicU64,
    /// Set while a checkpoint runs, so a burst of writers past the threshold
    /// produces one checkpoint rather than one each.
    checkpointing: AtomicBool,
    /// Set when a flush has failed. Durability can no longer be promised, so
    /// every later commit reports an error rather than lying about it.
    poisoned: AtomicBool,
    /// What poisoned the log, kept so an operator can see it without having
    /// caught the one log line that announced it. Set once, by the first
    /// failure; later ones are consequences of it.
    poison_reason: OnceLock<String>,
    /// Checkpoints that returned without discarding the log because a
    /// transaction was open or another checkpoint was running. Expected in
    /// small numbers; one that only ever grows means the log never shrinks.
    checkpoints_declined: AtomicU64,
    /// Checkpoints that returned an error, and the most recent such error.
    checkpoint_failures: AtomicU64,
    last_checkpoint_error: Mutex<Option<String>>,
    /// In-place overwrites waiting for their transaction to commit.
    ///
    /// Only overwrites appear here; appends never take this lock, which keeps
    /// the common path free of it.
    pending: Mutex<HashMap<u64, Vec<PendingWrite>>>,
    /// Number of transactions with pending overwrites, so a commit can skip the
    /// lock entirely when there are none.
    pending_count: AtomicU64,

    /// Highest commit sequence whose `fdatasync` has been deferred.
    ///
    /// Raised by an append-only commit and cleared by [`Wal::sync_pending`].
    /// Zero means nothing is owed.
    pending_sync: AtomicU64,
    /// Total bytes held in `pending`, so a long statement can bound how much it
    /// accumulates before committing.
    pending_bytes: AtomicU64,
    /// `file_id` -> path, for this database only. Kept per-log rather than
    /// globally: ids are scoped to a directory, so a process holding two
    /// databases must not resolve one's ids against the other's files.
    files: Mutex<HashMap<u64, PathBuf>>,
    /// Where a checkpoint hands the log's contents before discarding them.
    ///
    /// `None` — the default — is the historical behaviour: a checkpoint proves
    /// the records are in the data files and drops them. Point-in-time recovery
    /// needs those records *after* they have been applied, because a snapshot
    /// taken while writes were in flight is only made consistent by replaying
    /// them, so the archiver sets this to a staging directory and the checkpoint
    /// copies the log there on its way out.
    archive_dir: Mutex<Option<PathBuf>>,
    /// Names the staged segments in the order they were cut. Replay is only
    /// valid in log order, and a lexicographic listing of zero-padded numbers
    /// reproduces it without the archiver having to track state of its own.
    archive_seq: AtomicU64,
    /// Every commit sequence up to this one is in a staged segment: the
    /// checkpoints that dropped its records copied them to the archive first.
    staged_through_lsn: AtomicU64,
    /// Commit sequences up to this one may have been dropped by a checkpoint
    /// *without* being staged — archiving was off or the copy failed. A
    /// snapshot window that reaches below it cannot be made consistent.
    unstaged_through_lsn: AtomicU64,
    /// `bytes_since_checkpoint` when [`should_break_transaction`] last told a
    /// long writer to commit. See there.
    break_granted_at: AtomicU64,
    /// Data files that records in the live log describe: what the next
    /// checkpoint has to sync before it may drop those records. Filled under
    /// the writer lock as records are framed, taken by a checkpoint at the
    /// point it fixes what it covers.
    dirty: Mutex<DirtyFiles>,
    /// The file id last added to `dirty`, so consecutive records for one file
    /// do not take that lock. `u64::MAX` when unset. Written under the writer
    /// lock.
    last_dirty: AtomicU64,
    /// `file_id` -> the fence [`Wal::forget_files`] took for it: records from
    /// writers born before it are refused. Written and read under the writer
    /// lock, so a record is either framed before the barrier (and voided by
    /// it) or refused. Never shrinks: one entry per name ever deleted.
    fences: Mutex<HashMap<u64, u64>>,
}

/// See [`Wal::dirty`].
#[derive(Default)]
struct DirtyFiles {
    ids: HashSet<u64>,
    /// Every registered file: set only when the log was opened holding
    /// records whose headers could not be read, so their files are unknown.
    all: bool,
}

/// The files the records already in the log at `path` describe, read from
/// their headers: what this process's first checkpoint owes a sync.
///
/// A reopened log used to be answered with "every registered file", and the
/// registry of a reopened directory is every file in it — each table's
/// `.tmp` staging siblings, the status file, the LSN mark — none of which a
/// record names. On a database of 2,045 table files that was 4,080 syncs, so
/// the first checkpoint after any restart with a non-empty log, typically a
/// `VACUUM FULL`, cost two full passes' worth of device flushes.
///
/// Stops where replay stops, at the first record that does not read whole:
/// records past a torn one are never replayed, so their files are owed
/// nothing. `None` if the log cannot be read at all, for the caller to fall
/// back to syncing everything.
fn files_named_by_log(path: &Path) -> Option<HashSet<u64>> {
    let mut scan = LogScanner::open(path).ok()?;
    let mut ids = HashSet::new();
    loop {
        match scan.next() {
            // A commit marker names no file, and a barrier describes no bytes.
            Ok(Some(h)) if h.flags & FLAG_COMMIT != 0 && h.file_id == 0 => {}
            Ok(Some(h)) if h.flags & FLAG_FORGET != 0 => {}
            Ok(Some(h)) => {
                ids.insert(h.file_id);
            }
            Ok(None) => return Some(ids),
            Err(_) => return None,
        }
    }
}

impl Wal {
    /// Opens (creating if absent) the log at `path`, positioned to append.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let existed = path.exists();
        let mut writer = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // A newly created log is only durable once its directory entry is.
        if !existed {
            sync_parent_dir(&path)?;
        }

        let end = writer.seek(SeekFrom::End(0))?;

        // Whatever the last truncation recorded. Recovery raises it further
        // from the records still in the log; this is only the floor beneath
        // them, and it is all a cleanly checkpointed log leaves behind.
        let resume_lsn = read_lsn_mark(&path);

        // Records this process did not write still have to reach their files
        // before a checkpoint may drop them; their headers say which files.
        let dirty = if end == 0 {
            DirtyFiles::default()
        } else {
            match files_named_by_log(&path) {
                Some(ids) => DirtyFiles { ids, all: false },
                None => DirtyFiles {
                    ids: HashSet::new(),
                    all: true,
                },
            }
        };

        Ok(Self {
            path,
            writer: Mutex::new(writer),
            issued: AtomicU64::new(0),
            write_pos: AtomicU64::new(end),
            durable: AtomicU64::new(0),
            gate: Mutex::new(()),
            sync_handle: Mutex::new(None),
            next_txn: AtomicU64::new(1),
            commit_lsn: AtomicU64::new(resume_lsn),
            bytes_since_checkpoint: AtomicU64::new(end),
            checkpointing: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            poison_reason: OnceLock::new(),
            checkpoints_declined: AtomicU64::new(0),
            checkpoint_failures: AtomicU64::new(0),
            last_checkpoint_error: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
            pending_count: AtomicU64::new(0),
            pending_sync: AtomicU64::new(0),
            pending_bytes: AtomicU64::new(0),
            files: Mutex::new(HashMap::new()),
            archive_dir: Mutex::new(None),
            archive_seq: AtomicU64::new(0),
            staged_through_lsn: AtomicU64::new(0),
            unstaged_through_lsn: AtomicU64::new(0),
            break_granted_at: AtomicU64::new(0),
            dirty: Mutex::new(dirty),
            last_dirty: AtomicU64::new(u64::MAX),
            fences: Mutex::new(HashMap::new()),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn bytes_since_checkpoint(&self) -> u64 {
        self.bytes_since_checkpoint.load(Ordering::Relaxed)
    }

    /// True once a flush has failed and durability can no longer be promised.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// The failure that poisoned the log, if one has.
    pub fn poison_reason(&self) -> Option<String> {
        self.poison_reason.get().cloned()
    }

    /// How many checkpoints declined to discard the log, and how many failed.
    pub fn checkpoint_counts(&self) -> (u64, u64) {
        (
            self.checkpoints_declined.load(Ordering::Relaxed),
            self.checkpoint_failures.load(Ordering::Relaxed),
        )
    }

    /// The most recent checkpoint error, if any checkpoint has failed.
    pub fn last_checkpoint_error(&self) -> Option<String> {
        self.last_checkpoint_error.lock().clone()
    }

    /// Stops the log promising durability for the rest of the process.
    ///
    /// For any failure after which the log or the data files may no longer
    /// hold what a commit was told they hold. The reason is recorded before
    /// the flag is raised, so anyone who sees the flag can also read why.
    fn poison(&self, why: String) {
        log::error!(
            "WAL {} is poisoned and refuses every further commit: {}",
            self.path.display(),
            why
        );
        let _ = self.poison_reason.set(why);
        self.poisoned.store(true, Ordering::Release);
    }

    fn poisoned_error(&self) -> io::Error {
        io::Error::other(format!(
            "WAL is poisoned by an earlier failure ({}); durability cannot be guaranteed",
            self.poison_reason
                .get()
                .map(String::as_str)
                .unwrap_or("reason not recorded")
        ))
    }

    /// Refuses to drop any record from a poisoned log.
    ///
    /// A poisoned log may be the only place some committed bytes still exist:
    /// a data-file flush or an in-place apply failed, so the data files cannot
    /// be trusted to hold what the log describes. Dropping records would throw
    /// away the copy a restart replays from. Called under the writer lock, as
    /// late as possible before the drop, rather than only by the callers: a
    /// commit can poison the log between a caller's check and the drop.
    fn refuse_if_poisoned(&self) -> io::Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(self.poisoned_error());
        }
        Ok(())
    }

    /// Highest record sequence known to be on stable storage.
    ///
    /// For a caller holding bytes that may only be copied elsewhere once the
    /// log record describing them is durable — see
    /// [`crate::core::mvcc::status::StatusLog`], whose file copy would
    /// otherwise be able to reach the disk ahead of the log that justifies it.
    pub fn durable_through(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }

    /// Associates a data file's id with its path so replay can find it.
    pub fn register_file(&self, file_id: u64, path: impl Into<PathBuf>) {
        self.files.lock().insert(file_id, path.into());
    }

    pub fn registered_files(&self) -> Vec<(u64, PathBuf)> {
        self.files
            .lock()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Starts a transaction.
    ///
    /// Records logged under the returned id are replayed only if one of them
    /// carries the commit flag, so a crash mid-transaction applies none of it.
    pub fn begin(&self) -> u64 {
        self.next_txn.fetch_add(1, Ordering::AcqRel)
    }

    /// Appends a redo record and returns its sequence number.
    ///
    /// `commit` marks this as the last record of `txn`. The bytes are in the log
    /// file when this returns but not yet on stable storage; pass the sequence
    /// to [`Wal::commit_through`] for that.
    pub fn log(&self, txn: u64, file_id: u64, offset: u64, bytes: &[u8]) -> io::Result<u64> {
        self.log_from(txn, file_id, offset, bytes, Incarnation::UNFENCED)
    }

    /// [`Wal::log`] for a file's writer born at `born`: refused, and nothing
    /// logged, when the file was deleted since (see [`Wal::forget_files`]).
    pub fn log_from(
        &self,
        txn: u64,
        file_id: u64,
        offset: u64,
        bytes: &[u8],
        born: Incarnation,
    ) -> io::Result<u64> {
        self.log_at(txn, file_id, offset, bytes, None, born)
            .map(|(seq, _)| seq)
    }

    /// As [`Wal::log`], but also reports the payload's offset within the log.
    ///
    /// `pending_path` registers the record as an in-place overwrite to be
    /// applied at commit. It is passed in rather than registered by the caller
    /// afterwards because the registration must happen under the *same* writer
    /// lock that framed the record — see the comment at the registration site.
    fn log_at(
        &self,
        txn: u64,
        file_id: u64,
        offset: u64,
        bytes: &[u8],
        pending_path: Option<PathBuf>,
        born: Incarnation,
    ) -> io::Result<(u64, u64)> {
        if bytes.len() > MAX_RECORD_PAYLOAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("WAL record payload of {} bytes is too large", bytes.len()),
            ));
        }

        let header = record_header(txn, file_id, offset, bytes);

        // Header and payload go out under one lock, and as one `writev`: one
        // system call per record instead of two. Under the lock either way, so
        // no concurrent writer can interleave between them.
        let (seq, payload_at) = {
            let mut file = self.writer.lock();
            // Under the writer lock, as the fence is taken: either this record
            // precedes the barrier and is voided by it, or it is refused.
            if let Some(&fence) = self.fences.lock().get(&file_id)
                && born.0 <= fence
            {
                return Err(io::Error::other(format!(
                    "refusing a log record for file id {file_id}: its file was deleted after \
                     this writer opened it, and the record would replay into the next file \
                     of that name"
                )));
            }
            let record_at = self.write_pos.load(Ordering::Relaxed);
            self.append_locked(&mut file, &mut [io::IoSlice::new(&header), io::IoSlice::new(bytes)])?;
            self.mark_dirty_locked(file_id);
            self.write_pos.store(
                record_at + (HEADER_LEN + bytes.len()) as u64,
                Ordering::Relaxed,
            );
            // Bumped while the lock is held, so the counter can never run ahead
            // of the bytes it stands for.
            let seq = self.issued.fetch_add(1, Ordering::AcqRel) + 1;
            let payload_at = record_at + HEADER_LEN as u64;

            // Registered under the writer lock, not after releasing it.
            //
            // A pending overwrite stores an offset *into the log* and reads its
            // payload back from there at commit. `checkpoint` truncates the log
            // and skips itself while `pending_count` is non-zero — but if the
            // counter were bumped after this lock were released, a checkpoint
            // could read the counter as zero, take this same lock, and truncate
            // between the two. The offset recorded below would then point past
            // EOF and the commit would fail with "failed to fill whole buffer".
            //
            // Doing it here makes appending the record and declaring the
            // dependency on it a single atomic step against the truncation.
            if let Some(path) = pending_path {
                let mut pending = self.pending.lock();
                let entry = pending.entry(txn).or_default();
                if entry.is_empty() {
                    self.pending_count.fetch_add(1, Ordering::AcqRel);
                }
                entry.push(PendingWrite {
                    path,
                    offset,
                    log_offset: payload_at,
                    len: bytes.len(),
                });
                self.pending_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            }

            (seq, payload_at)
        };

        self.bytes_since_checkpoint
            .fetch_add((HEADER_LEN + bytes.len()) as u64, Ordering::Relaxed);

        Ok((seq, payload_at))
    }

    /// Logs an in-place overwrite, holding the data-file write until commit.
    ///
    /// This is the "write-ahead" in write-ahead log, and it only matters for
    /// overwrites. An append that is cut short leaves harmless garbage past the
    /// old end of file, which replay overwrites. An *overwrite* that is cut
    /// short has already destroyed the previous bytes, and a redo-only log has
    /// no undo record to put them back — so the data file must not be touched
    /// until the record describing the new bytes is durable.
    pub fn log_in_place(
        &self,
        txn: u64,
        file_id: u64,
        offset: u64,
        bytes: &[u8],
    ) -> io::Result<u64> {
        self.log_in_place_from(txn, file_id, offset, bytes, Incarnation::UNFENCED)
    }

    /// [`Wal::log_in_place`] for a file's writer born at `born`; see
    /// [`Wal::log_from`].
    pub fn log_in_place_from(
        &self,
        txn: u64,
        file_id: u64,
        offset: u64,
        bytes: &[u8],
        born: Incarnation,
    ) -> io::Result<u64> {
        // Resolved before the record is written: failing afterwards would
        // leave a logged overwrite that nothing is tracking.
        let path = self.files.lock().get(&file_id).cloned();
        let Some(path) = path else {
            return Err(io::Error::other(format!(
                "WAL has no path registered for file id {file_id}"
            )));
        };

        let (seq, _) = self.log_at(txn, file_id, offset, bytes, Some(path), born)?;
        Ok(seq)
    }

    /// Commits `txn`: makes its records durable, then applies the overwrites it
    /// was holding back.
    pub fn commit_txn(&self, txn: u64) -> io::Result<()> {
        // A marker record, rather than a flag on the transaction's last data
        // record. A statement touching many rows cannot know in advance which
        // write will turn out to be its last — a row may be skipped — and
        // guessing wrong would leave the whole statement uncommitted.
        self.finish_txn(txn, false).map(|_| ())
    }

    /// Commits `txn`, leaving its `fdatasync` to [`Wal::sync_pending`].
    ///
    /// For the row write path, where one statement commits many transactions
    /// and the caller takes a single sync before reporting success. Everything
    /// else — the transaction *outcome* records above all — uses `commit_txn`,
    /// whose contract is that it is durable when it returns.
    ///
    /// A transaction holding in-place payloads syncs regardless: those may only
    /// be applied over the old bytes once the record describing them is on
    /// disk, and that is not something a later sync can repair.
    pub fn commit_txn_deferred(&self, txn: u64) -> io::Result<()> {
        self.finish_txn(txn, true).map(|_| ())
    }

    /// Commits `txn` without syncing, and hands back its commit marker's
    /// sequence so the caller can take the sync itself.
    ///
    /// For a caller that must write its record under a lock but must *not* hold
    /// that lock across the `fdatasync` — [`crate::core::mvcc::status::StatusLog`]
    /// is the one that matters. Passing the returned sequence to
    /// [`Wal::commit_through`] after dropping the lock is what lets concurrent
    /// committers collapse into one sync instead of queueing one each.
    pub fn commit_txn_deferred_seq(&self, txn: u64) -> io::Result<u64> {
        self.finish_txn(txn, true)
    }

    fn finish_txn(&self, txn: u64, defer: bool) -> io::Result<u64> {
        // Checked up front so a poisoned log refuses promptly, rather than
        // accepting commits that would fail later at the sync.
        //
        // Poisoning is permanent and terminal: once set, the database is not
        // taking durable commits again for the life of the process. So this is
        // one of the safest `unlikely` calls in the tree, and it pairs with the
        // padding on `poisoned` — the load stays in L1, and the branch that
        // follows it stays out of the fall-through path.
        if unlikely(self.poisoned.load(Ordering::Acquire)) {
            return Err(self.poisoned_error());
        }

        let seq = self.log_commit_marker(txn)?;

        // The common case, and worth not taking a lock for: nothing anywhere is
        // holding payloads, so this commit is certainly append-only. Hinted
        // `likely` to say so — appends never register pending payloads, so the
        // counter is zero for every workload that is not doing in-place
        // overwrites, and this early return is then the whole function.
        if likely(defer && self.pending_count.load(Ordering::Acquire) == 0) {
            self.pending_sync.fetch_max(seq, Ordering::AcqRel);
            return Ok(seq);
        }

        // Whether this transaction holds payloads that have to be applied over
        // existing bytes. Taken before the flush decision, because the two
        // answers differ: an in-place apply may only start once the record
        // describing it is durable.
        let held = {
            let mut pending = self.pending.lock();
            pending.remove(&txn)
        };

        if defer && held.is_none() {
            self.pending_sync.fetch_max(seq, Ordering::AcqRel);
            return Ok(seq);
        }

        if let Err(e) = self.commit_through(seq) {
            // The entry left `pending` above, so `abort_txn` can no longer
            // find it: give its claim back here, or `pending_count` stays
            // raised and every later checkpoint declines.
            if let Some(held) = held {
                let bytes: u64 = held.iter().map(|w| w.len as u64).sum();
                self.pending_bytes.fetch_sub(bytes, Ordering::Relaxed);
                self.pending_count.fetch_sub(1, Ordering::AcqRel);
            }
            return Err(e);
        }

        let Some(held) = held else {
            return Ok(seq);
        };

        // `pending_count` stays raised until the payloads below have been read
        // back out of the log, and is dropped by this guard however we leave.
        //
        // It is the only thing stopping `checkpoint` from truncating the log,
        // and these reads are *from* the log. Decrementing at the point the
        // entry left the map — before the loop rather than after it — released
        // the claim while the payloads were still needed, so a checkpoint could
        // truncate mid-apply and the read would come back short. That is the
        // wider of the two windows that produced "failed to fill whole buffer";
        // it stays open for as long as the apply takes, rather than for the few
        // instructions between framing a record and registering it.
        struct Claim<'a>(&'a AtomicU64);
        impl Drop for Claim<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let _claim = Claim(&self.pending_count);

        // Only now is it safe to disturb the old bytes: the record describing
        // them is durable, so if we die partway replay finishes the job.
        //
        // A failure here is past the point of no return. The commit marker is
        // durable, so the transaction *is* committed, and the log is now the
        // only copy of these bytes: the data file has some, none or a torn
        // part of them. Returning the error alone left the log free to be
        // checkpointed — and a checkpoint drops the only redo copy, after
        // which the data file keeps whatever the failed apply left behind.
        // Poisoning keeps the log, so a restart replays it, and refuses every
        // later commit rather than building on bytes that are not there.
        if let Err(e) = self.apply_held(held) {
            self.poison(format!(
                "applying a committed in-place write of transaction {} failed: {}",
                txn, e
            ));
            return Err(e);
        }

        Ok(seq)
    }

    /// Copies each held overwrite's payload from the log into its data file.
    fn apply_held(&self, held: Vec<PendingWrite>) -> io::Result<()> {
        // Released up front for every entry, so an apply that fails partway
        // does not leave the remainder counted forever.
        let held_bytes: u64 = held.iter().map(|w| w.len as u64).sum();
        self.pending_bytes.fetch_sub(held_bytes, Ordering::Relaxed);

        let mut log_reader: Option<File> = None;
        let mut buf: Vec<u8> = Vec::new();

        for w in held {
            // The payload is already on stable storage in the log, so read it
            // back from there rather than having kept a second copy in memory.
            let reader = match log_reader {
                Some(ref mut f) => f,
                None => {
                    log_reader = Some(File::open(&self.path)?);
                    log_reader.as_mut().expect("just opened")
                }
            };

            buf.clear();
            buf.resize(w.len, 0);
            reader.seek(SeekFrom::Start(w.log_offset))?;
            reader.read_exact(&mut buf)?;

            // `create(true)`, to match what recovery does with the very same
            // record at line ~830. Without it the first in-place write to a
            // file that does not exist yet failed with `NotFound` — and since
            // `write_data` has no way to report, the write was simply lost.
            //
            // That is not a hypothetical: a table's filter sidecar is created
            // by its first checkpoint, which is an in-place write at offset 0,
            // so filter checkpoints never reached disk at all while a WAL was
            // enabled. Replaying the log after a crash would have created the
            // file correctly, which is what made the two paths disagree.
            let existed = w.path.exists();
            let mut f = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(&w.path)?;
            f.seek(SeekFrom::Start(w.offset))?;
            f.write_all(&buf)?;
            // A new file is only durable once its directory entry is.
            if !existed {
                sync_parent_dir(&w.path)?;
            }
        }

        Ok(())
    }

    /// Returns once every record up to `seq` is on stable storage.
    ///
    /// Concurrent callers collapse into a single flush: the flusher reads the
    /// issued counter *before* flushing and publishes it afterwards, claiming
    /// every record written so far rather than only its own.
    pub fn commit_through(&self, seq: u64) -> io::Result<()> {
        if unlikely(self.poisoned.load(Ordering::Acquire)) {
            return Err(self.poisoned_error());
        }

        if self.durable.load(Ordering::Acquire) >= seq {
            return Ok(()); // an earlier flush already covered us
        }

        // Everything from here can park the thread for an fsync — the leader
        // in `sync_data`, every follower on `gate`. Called from a statement on
        // a runtime worker, that parked the worker and every task queued on
        // it: with as many concurrent committers as workers, reads stalled for
        // whole syncs. See [`blocking_io`].
        blocking_io(|| self.commit_through_slow(seq))
    }

    /// The part of [`Wal::commit_through`] that may block: waiting for the
    /// gate, and the flush itself. Order and protocol are exactly the ones
    /// `commit_through` always had; only the thread it blocks changed.
    fn commit_through_slow(&self, seq: u64) -> io::Result<()> {
        let _gate = self.gate.lock();

        if self.durable.load(Ordering::Acquire) >= seq {
            return Ok(()); // covered while we waited for the gate
        }

        let covered = self.issued.load(Ordering::Acquire);

        let handle = {
            let mut slot = self.sync_handle.lock();
            match &*slot {
                Some(h) => h.clone(),
                None => {
                    let h = Arc::new(File::options().read(true).write(true).open(&self.path)?);
                    *slot = Some(h.clone());
                    h
                }
            }
        };

        // `sync_data`: the log only needs its bytes and length durable to be
        // replayable. Timestamps would cost an extra journal transaction.
        //
        // A failed flush is not retried. On Linux the kernel may drop the dirty
        // pages after reporting the error, so a second call could return success
        // while the data is gone. Poisoning instead makes every later commit
        // fail loudly rather than silently downgrading durability.
        crate::core::sync_audit::note("wal.log", &self.path);
        if let Err(e) = handle.sync_data() {
            self.poison(format!("fdatasync of the log failed: {}", e));
            return Err(e);
        }

        self.durable.fetch_max(covered, Ordering::AcqRel);
        Ok(())
    }

    /// Forces out every commit whose sync was deferred.
    ///
    /// Group commit: a transaction that only *appends* — every `INSERT` — writes
    /// its commit marker and returns without an `fdatasync`, and the sync is
    /// taken once here, at the point the caller is about to report success. One
    /// `fdatasync` costs about four milliseconds on a consumer SSD, so paying
    /// it per row put a hard ceiling of roughly 260 rows a second on bulk
    /// loading: 17,468 rows took 67 seconds, and wrapping them in an explicit
    /// transaction changed nothing because the sync was per row rather than per
    /// commit.
    ///
    /// Durability is unchanged, because *this* is the point durability is
    /// promised. Nothing tells a client its statement succeeded before this has
    /// run; inside an explicit block nothing is promised until `COMMIT`, which
    /// is the other place it runs.
    pub fn sync_pending(&self) -> io::Result<()> {
        let owed = self.pending_sync.load(Ordering::Acquire);
        if owed == 0 || self.durable.load(Ordering::Acquire) >= owed {
            return Ok(());
        }
        self.commit_through(owed)
    }

    /// Highest commit sequence assigned so far.
    pub fn commit_lsn(&self) -> u64 {
        self.commit_lsn.load(Ordering::Acquire)
    }

    /// Writes `bufs` at the append point, which the caller holds the writer
    /// lock for, and leaves the file exactly as it was if that fails.
    ///
    /// A failed write can still have put part of the record in the file — a
    /// short write on a full disk, EIO midway — and moved the cursor past
    /// `write_pos`. Left there, the next record lands *after* the partial
    /// one, so replay stops at the garbage and never reaches it, and every
    /// offset `write_pos` hands out (an in-place payload's `log_offset`)
    /// points at the wrong bytes. So the file is cut back to `write_pos` and
    /// the cursor moved there, and the caller's statement fails. If even that
    /// fails the log can no longer be trusted, and it is poisoned: the torn
    /// record would hide every later commit from recovery, including ones
    /// already acknowledged, and nothing here can repair that.
    ///
    /// A failure that *was* cut back does not poison. The log is then exactly
    /// as it was before the write, every record in it still replays, and only
    /// the statement that wrote it has lost anything.
    fn append_locked(&self, file: &mut File, bufs: &mut [io::IoSlice<'_>]) -> io::Result<()> {
        let at = self.write_pos.load(Ordering::Relaxed);
        #[cfg(test)]
        let written = match test_hooks::SHORT_WAL_WRITE.with(|h| h.take()) {
            Some(keep) => {
                let all: Vec<u8> = bufs.iter().flat_map(|b| b.iter().copied()).collect();
                file.write_all(&all[..keep.min(all.len())])?;
                Err(io::Error::other("injected short WAL write"))
            }
            None => write_all_vectored(file, bufs),
        };
        #[cfg(not(test))]
        let written = write_all_vectored(file, bufs);
        if let Err(e) = written {
            #[cfg(test)]
            let rolled_back = if test_hooks::FAIL_WAL_ROLLBACK.with(|f| f.replace(false)) {
                Err(io::Error::other("injected failure to cut the log back"))
            } else {
                file.set_len(at)
                    .and_then(|()| file.seek(SeekFrom::Start(at)).map(|_| ()))
            };
            #[cfg(not(test))]
            let rolled_back = file
                .set_len(at)
                .and_then(|()| file.seek(SeekFrom::Start(at)).map(|_| ()));
            if let Err(cut) = rolled_back {
                self.poison(format!(
                    "appending to the log failed ({}) and the partial record could not be cut \
                     back: {}",
                    e, cut
                ));
            }
            return Err(e);
        }
        Ok(())
    }

    /// Notes that a record for `file_id` is in the live log. Call under the
    /// writer lock, after the record is written.
    #[inline]
    fn mark_dirty_locked(&self, file_id: u64) {
        if file_id == u64::MAX || self.last_dirty.load(Ordering::Relaxed) != file_id {
            self.dirty.lock().ids.insert(file_id);
            self.last_dirty.store(file_id, Ordering::Relaxed);
        }
    }

    /// Writes the zero-payload record that marks `txn` as committed.
    fn log_commit_marker(&self, txn: u64) -> io::Result<u64> {
        let seq = {
            let mut file = self.writer.lock();
            // Assigned under the writer lock, so commit sequences reach the
            // file in order and `commit_lsn` read under that lock names every
            // marker already written — what a checkpoint records as staged.
            let lsn = self.commit_lsn.fetch_add(1, Ordering::AcqRel) + 1;
            let header = marker_header(txn, lsn);
            #[cfg(test)]
            let written = if test_hooks::FAIL_COMMIT_MARKER.with(|f| f.replace(false)) {
                Err(io::Error::other("injected commit-marker failure"))
            } else {
                self.append_locked(&mut file, &mut [io::IoSlice::new(&header)])
            };
            #[cfg(not(test))]
            let written = self.append_locked(&mut file, &mut [io::IoSlice::new(&header)]);
            if let Err(e) = written {
                // Handed out under this lock, so nothing has taken a later
                // one: give it back rather than leave a hole in the sequence.
                self.commit_lsn.fetch_sub(1, Ordering::AcqRel);
                return Err(e);
            }
            self.write_pos
                .fetch_add(HEADER_LEN as u64, Ordering::Relaxed);
            self.issued.fetch_add(1, Ordering::AcqRel) + 1
        };

        self.bytes_since_checkpoint
            .fetch_add(HEADER_LEN as u64, Ordering::Relaxed);

        Ok(seq)
    }

    /// Logs a single self-contained write and makes it durable.
    pub fn log_and_commit(&self, file_id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let seq = self.log_with_marker(file_id, offset, bytes)?;
        self.commit_through(seq)
    }

    /// A one-record transaction: the record and its commit marker framed
    /// together and written in one `writev` under one acquisition of the
    /// writer lock, where `log` + `commit_txn` took the lock twice and made
    /// three system calls. Returns the marker's sequence; nothing is synced.
    ///
    /// Equivalent to `begin` + `log` + `commit_txn_deferred_seq` for a
    /// transaction with no in-place payloads — which a fresh transaction
    /// holding one appended record never has — down to the counters: `issued`
    /// rises by two, the marker carrying the second.
    ///
    /// Torn writes: replay reads records in file order and stops at the first
    /// one that is short or fails its CRC (`LogScanner::next`), and only a
    /// marker inside that valid prefix commits anything. A crash mid-write can
    /// therefore leave (a) a partial record — replay stops before it, the
    /// transaction has no marker and is not applied; (b) the whole record and
    /// a partial marker — replay stops at the marker, the record is rolled
    /// back; or (c) both whole. Those are exactly the states two separate
    /// writes could leave, and pages persisted out of order collapse to (a) or
    /// (b) because a gap fails the CRC of the record it falls in.
    fn log_with_marker(&self, file_id: u64, offset: u64, bytes: &[u8]) -> io::Result<u64> {
        // `finish_txn` refuses a poisoned log before writing its marker; this
        // refuses before writing either half, which leaves less behind.
        if unlikely(self.poisoned.load(Ordering::Acquire)) {
            return Err(self.poisoned_error());
        }
        if bytes.len() > MAX_RECORD_PAYLOAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("WAL record payload of {} bytes is too large", bytes.len()),
            ));
        }
        let txn = self.begin();
        let header = record_header(txn, file_id, offset, bytes);
        let framed = (2 * HEADER_LEN + bytes.len()) as u64;

        let seq = {
            let mut file = self.writer.lock();
            // Under the lock, as in `log_commit_marker`.
            let lsn = self.commit_lsn.fetch_add(1, Ordering::AcqRel) + 1;
            let marker = marker_header(txn, lsn);
            if let Err(e) = self.append_locked(
                &mut file,
                &mut [
                    io::IoSlice::new(&header),
                    io::IoSlice::new(bytes),
                    io::IoSlice::new(&marker),
                ],
            ) {
                // As in `log_commit_marker`: no hole in the sequence.
                self.commit_lsn.fetch_sub(1, Ordering::AcqRel);
                return Err(e);
            }
            self.mark_dirty_locked(file_id);
            self.write_pos.fetch_add(framed, Ordering::Relaxed);
            // Two records, bumped under the lock like their bytes: the marker's
            // sequence is the second.
            self.issued.fetch_add(2, Ordering::AcqRel) + 2
        };

        self.bytes_since_checkpoint
            .fetch_add(framed, Ordering::Relaxed);
        Ok(seq)
    }

    /// Logs a single self-contained write and marks it committed, leaving the
    /// `fdatasync` to the caller.
    ///
    /// Returns the commit marker's sequence, for [`Wal::commit_through`]. The
    /// sync is owed to [`Wal::sync_pending`] as well, so a caller that is about
    /// to take one sync for a whole statement can let that cover this too —
    /// which is how an append-only write and the transaction outcome record
    /// that validates it come to cost one `fdatasync` between them rather than
    /// one each.
    pub fn log_and_commit_deferred(
        &self,
        file_id: u64,
        offset: u64,
        bytes: &[u8],
    ) -> io::Result<u64> {
        let seq = self.log_with_marker(file_id, offset, bytes)?;
        // Owed to `sync_pending`, as `finish_txn` records a deferred commit.
        self.pending_sync.fetch_max(seq, Ordering::AcqRel);
        Ok(seq)
    }

    /// Voids every record this log holds for `file_ids`, for a caller about to
    /// delete those files. Durable when it returns.
    ///
    /// A record names its file only by an id hashed from the file's name, so a
    /// file deleted and later created again under that name (a dropped table
    /// recreated, which an `ALTER … ADD COLUMN` rewrite does, or a table
    /// created under a renamed table's old name) has the id and path the old
    /// one had. A replay before the next checkpoint (and every open replays)
    /// wrote the old file's records into the new one, and wherever the new
    /// file was shorter the old bytes came back, rows included.
    ///
    /// Not a checkpoint, though one would void them too: a checkpoint declines
    /// while any transaction holds an in-place write, the dropping one's
    /// included, so it cannot be relied on, and it flushes every file written
    /// since the last one. This is one header per file and one log sync.
    ///
    /// Positional: replay skips the records for these ids that precede the
    /// barrier, whenever their transactions commit, and applies the ones after
    /// it, which belong to whatever file takes the name next. A barrier whose
    /// commit marker is lost voids nothing, which is the state before the call.
    /// A failure that poisoned the log came after the append, so the barrier
    /// may be durable; a failure that did not left nothing that can commit.
    ///
    /// The one sync also makes everything logged before it durable, so a
    /// caller that needs an earlier record of this log durable first (a
    /// catalogue tombstone) owes no sync of its own.
    pub fn forget_files(&self, file_ids: &[u64]) -> io::Result<()> {
        if file_ids.is_empty() {
            return Ok(());
        }
        if unlikely(self.poisoned.load(Ordering::Acquire)) {
            return Err(self.poisoned_error());
        }
        #[cfg(test)]
        if test_hooks::FAIL_FORGET.with(|f| f.get())
            && test_hooks::FORGETS_BEFORE_FAILING.with(|n| {
                let left = n.get();
                n.set(left.saturating_sub(1));
                left == 0
            })
        {
            test_hooks::FAIL_FORGET.with(|f| f.set(false));
            return Err(io::Error::other("injected failure to log a barrier"));
        }
        let txn = self.begin();
        let headers: Vec<[u8; HEADER_LEN]> =
            file_ids.iter().map(|id| forget_header(txn, *id)).collect();
        let framed = (headers.len() * HEADER_LEN) as u64;
        {
            let mut file = self.writer.lock();
            let mut bufs: Vec<io::IoSlice<'_>> =
                headers.iter().map(|h| io::IoSlice::new(h)).collect();
            self.append_locked(&mut file, &mut bufs)?;
            // Fenced from here, under the same lock: a writer that opened
            // these files before now cannot log after the barrier, where it
            // would not be voided. Only once the barrier is in the file: a
            // failed append leaves the caller free to keep the table.
            let fence = INCARNATIONS.fetch_add(1, Ordering::AcqRel);
            let mut fences = self.fences.lock();
            for id in file_ids {
                fences.insert(*id, fence);
            }
            drop(fences);
            // Not marked dirty: a barrier gives a checkpoint nothing to flush.
            self.write_pos.fetch_add(framed, Ordering::Relaxed);
            self.issued
                .fetch_add(headers.len() as u64, Ordering::AcqRel);
        }
        self.bytes_since_checkpoint
            .fetch_add(framed, Ordering::Relaxed);
        #[cfg(test)]
        if test_hooks::FAIL_BARRIER_SYNC.with(|f| f.replace(false)) {
            self.log_commit_marker(txn)?;
            self.poison("injected fdatasync failure".to_string());
            return Err(io::Error::other("injected fdatasync failure"));
        }
        self.commit_txn(txn)
    }

    /// Bytes of in-place overwrites currently held back, across all open
    /// transactions. Callers batching a long statement use this to bound how
    /// much a single transaction is allowed to accumulate.
    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    /// Discards the log after its contents are known to be in the data files.
    ///
    /// Only safe once every file the log refers to has been flushed; see
    /// [`Wal::checkpoint`].
    pub fn truncate(&self) -> io::Result<()> {
        let mut file = self.writer.lock();
        // Before the mark below, which would otherwise record as dropped
        // records a refused truncation kept.
        self.refuse_if_poisoned()?;
        // Not staged: whatever this drops is not in any archive.
        self.unstaged_through_lsn
            .fetch_max(self.commit_lsn.load(Ordering::Acquire), Ordering::AcqRel);
        self.truncate_locked(&mut file)?;
        // The log is empty, so it describes no file. Recovery truncates after
        // syncing what it applied, and left the files named by the replayed
        // records owed a second sync at the next checkpoint.
        *self.dirty.lock() = DirtyFiles::default();
        self.last_dirty.store(u64::MAX, Ordering::Relaxed);
        Ok(())
    }

    /// The body of [`Wal::truncate`], for callers that already hold the writer.
    ///
    /// Staging has to happen under the same lock that truncates, or a record
    /// appended between the copy and the `set_len` would be discarded without
    /// ever having been archived.
    fn truncate_locked(&self, file: &mut File) -> io::Result<()> {
        self.refuse_if_poisoned()?;

        // Before the log is dropped, never after. A crash in between then
        // leaves a mark that is merely ahead of the log, which the next open
        // reconciles by taking the higher of the two. The other order would
        // leave an empty log behind a stale mark, and the sequence would hand
        // out numbers it had already used.
        //
        // Best-effort: failing the checkpoint over it would let the log grow
        // without bound, and the cost of the mark not landing is a sequence
        // that restarts — bad for backups, harmless for durability.
        self.write_lsn_mark_best_effort();

        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        crate::core::sync_audit::note("wal.truncate", &self.path);
        file.sync_all()?;
        // The append point moves back with the file. The writer is not in
        // append mode — records are placed at `write_pos` explicitly — so
        // leaving it at its pre-truncation value made every later record report
        // a `log_offset` far past the real end of the log. Appends never notice,
        // because nothing reads them back; an in-place overwrite reads its
        // payload from that offset at commit and got "failed to fill whole
        // buffer" every time. Deterministic after any checkpoint, not a race.
        self.write_pos.store(0, Ordering::Relaxed);
        self.bytes_since_checkpoint.store(0, Ordering::Relaxed);
        // Everything issued so far is now vacuously durable.
        let issued = self.issued.load(Ordering::Acquire);
        self.durable.fetch_max(issued, Ordering::AcqRel);
        Ok(())
    }

    fn write_lsn_mark_best_effort(&self) {
        let lsn = self.commit_lsn.load(Ordering::Acquire);
        if lsn > 0 {
            if let Err(e) = write_lsn_mark(&self.path, lsn) {
                log::error!(
                    "Could not record the commit sequence for {}: {}. Point-in-time recovery \
                     across the next restart may be affected; durability is not.",
                    self.path.display(),
                    e
                );
            }
        }
    }

    /// Sends every future checkpoint's log contents to `dir` before discarding
    /// them, or stops doing so when given `None`.
    ///
    /// The sequence resumes past whatever the directory already holds, so a
    /// restart with segments still waiting to be uploaded does not reuse their
    /// names and overwrite them.
    pub fn set_archive_dir(&self, dir: Option<PathBuf>) -> io::Result<()> {
        if let Some(ref d) = dir {
            std::fs::create_dir_all(d)?;
            let mut highest = 0u64;
            for entry in std::fs::read_dir(d)?.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                let Some(stem) = name.strip_suffix(SEGMENT_SUFFIX) else {
                    continue;
                };
                if let Ok(n) = u64::from_str_radix(stem, 16) {
                    highest = highest.max(n + 1);
                }
            }
            self.archive_seq.fetch_max(highest, Ordering::AcqRel);
        }
        *self.archive_dir.lock() = dir;
        Ok(())
    }

    /// Whether a snapshot window ending at `end_lsn` is fully archived: every
    /// commit through `end_lsn` has left the live log by way of a staged
    /// segment, and nothing after `start_lsn` was dropped without one.
    ///
    /// Not "the live log is empty": a checkpoint keeps the records logged
    /// during its own sync pass, so under load the log is never empty, and
    /// asking for that discarded every snapshot taken while writes ran. Not
    /// only the window either: a record committed before `start_lsn` can
    /// still be in the live log, unstaged, if every checkpoint declined — an
    /// in-place write it carries may not have reached the files the copy
    /// read, so it too must be in the archive. Records dropped unstaged
    /// before the window opened are the exception: a checkpoint only drops
    /// what is already in the data files.
    pub fn staged_covers(&self, start_lsn: u64, end_lsn: u64) -> bool {
        self.staged_through_lsn.load(Ordering::Acquire) >= end_lsn
            && self.unstaged_through_lsn.load(Ordering::Acquire) <= start_lsn
    }

    /// Whether checkpoints are currently archiving.
    pub fn is_archiving(&self) -> bool {
        self.archive_dir.lock().is_some()
    }

    /// Copies the live log into the staging directory, under the writer lock.
    ///
    /// `copy` rather than `rename`: the log is held open by an append handle
    /// and a flush handle, both of which follow the inode, so moving the file
    /// out from under them would leave the next record appended to the segment
    /// that was just cut. On Linux the copy is served by `copy_file_range`, so
    /// the bytes do not travel through user space and a reflinking filesystem
    /// makes it near-free.
    ///
    /// Only the first `upto` bytes: a checkpoint that keeps a tail stages the
    /// tail with the segment after this one, not twice.
    ///
    /// `Ok(true)` when everything up to `upto` is now in staging (or there was
    /// nothing to stage), `Ok(false)` when archiving is off.
    fn stage_locked(&self, file: &mut File, upto: u64) -> io::Result<bool> {
        let dir = match self.archive_dir.lock().clone() {
            Some(d) => d,
            None => return Ok(false),
        };

        // An empty log describes nothing; staging it would cost an upload and
        // a manifest rewrite to say so.
        file.flush()?;
        if upto == 0 || file.metadata()?.len() == 0 {
            return Ok(true);
        }

        let seq = self.archive_seq.fetch_add(1, Ordering::AcqRel);
        // Written under a temporary name and renamed into place, so the
        // archiver never picks up a segment that is still being copied.
        let partial = dir.join(format!("{:016x}{}", seq, SEGMENT_PARTIAL_SUFFIX));
        let final_path = dir.join(format!("{:016x}{}", seq, SEGMENT_SUFFIX));

        std::fs::copy(&self.path, &partial)?;
        let staged = File::options().write(true).open(&partial)?;
        if staged.metadata()?.len() > upto {
            staged.set_len(upto)?;
        }
        crate::core::sync_audit::note("wal.stage_segment", &final_path);
        staged.sync_data()?;
        drop(staged);
        std::fs::rename(&partial, &final_path)?;
        sync_parent_dir(&final_path)?;
        Ok(true)
    }

    /// Gives up `txn` without committing it: its pending in-place overwrites
    /// are dropped unapplied, and with them its claim on the log.
    ///
    /// A transaction that is never committed leaves no commit marker, so
    /// replay already discards its records; what it also left behind was its
    /// entry here, which kept `pending_count` raised and every later
    /// `checkpoint` declined — the log then grew for the life of the process.
    /// Only for a transaction whose commit was never attempted.
    pub fn abort_txn(&self, txn: u64) {
        let held = self.pending.lock().remove(&txn);
        if let Some(held) = held {
            let bytes: u64 = held.iter().map(|w| w.len as u64).sum();
            self.pending_bytes.fetch_sub(bytes, Ordering::Relaxed);
            self.pending_count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Transactions holding pending in-place overwrites. Diagnostic: a
    /// non-zero value with nothing in flight is a transaction nobody ended.
    pub fn pending_transactions(&self) -> u64 {
        self.pending_count.load(Ordering::Acquire)
    }

    /// Flushes the data files the live log names (those dirtied since the last
    /// checkpoint), then discards the log.
    ///
    /// Ordering matters: the log may only be dropped once everything it
    /// describes is on stable storage, otherwise a crash right after would lose
    /// those writes with no record left to replay.
    ///
    /// Returns whether the log was actually discarded. A checkpoint declines —
    /// `Ok(false)` — while a transaction is open or another checkpoint is
    /// running; the caller must not treat that as "the log is empty now".
    /// Compaction does, and truncated a data file with live records still
    /// describing offsets past the cut, so a crash re-materialised bytes beyond
    /// the new end. `Ok(true)` means the log is gone and the files are flushed.
    ///
    /// A poisoned log is never checkpointed; that is an error, not a decline,
    /// because it will not change until the process restarts.
    pub fn checkpoint(&self) -> io::Result<bool> {
        let outcome = self.checkpoint_inner();
        match &outcome {
            Ok(true) => {}
            Ok(false) => {
                self.checkpoints_declined.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.checkpoint_failures.fetch_add(1, Ordering::Relaxed);
                *self.last_checkpoint_error.lock() = Some(e.to_string());
            }
        }
        outcome
    }

    fn checkpoint_inner(&self) -> io::Result<bool> {
        // Refused before any data file is touched: nothing a checkpoint does
        // can be finished on a poisoned log, and the drop refuses too.
        self.refuse_if_poisoned()?;

        // Never while a transaction is still open. Checkpointing discards the
        // log, and a pending overwrite points *into* the log for its payload —
        // truncating underneath it would leave the transaction unable to apply.
        // Skipping is safe: the next commit past the threshold tries again.
        if self.pending_count.load(Ordering::Acquire) != 0 {
            return Ok(false);
        }

        // One checkpoint at a time; the rest carry on writing.
        if self.checkpointing.swap(true, Ordering::AcqRel) {
            return Ok(false);
        }
        struct Reset<'a>(&'a AtomicBool);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _reset = Reset(&self.checkpointing);

        // What the sync pass below will cover: every record issued by now had
        // its data written before it was logged. Read together, under the lock
        // that moves both.
        //
        // The files to sync are taken at the same point: exactly those that
        // records before `synced_end` describe. Syncing every registered file
        // instead cost one device flush per table — ~2.3 ms each on the
        // deployed disk, 4.7 s a pass at 2,042 files — however few had been
        // written. A file no record names has nothing this checkpoint drops.
        let (synced_end, synced_issued, synced_lsn, taken) = {
            let _writer = self.writer.lock();
            let taken = std::mem::take(&mut *self.dirty.lock());
            self.last_dirty.store(u64::MAX, Ordering::Relaxed);
            (
                self.write_pos.load(Ordering::Relaxed),
                self.issued.load(Ordering::Acquire),
                // Every marker through this sequence is before `synced_end`:
                // sequences are assigned under this lock.
                self.commit_lsn.load(Ordering::Acquire),
                taken,
            )
        };
        // Put back if this checkpoint does not drop the records they cover —
        // it declined, or a sync failed — so the next one still syncs them.
        struct Restore<'a> {
            wal: &'a Wal,
            taken: Option<DirtyFiles>,
        }
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                if let Some(taken) = self.taken.take() {
                    let mut dirty = self.wal.dirty.lock();
                    dirty.all |= taken.all;
                    dirty.ids.extend(taken.ids);
                }
            }
        }
        let mut restore = Restore {
            wal: self,
            taken: Some(taken),
        };

        let paths: Vec<PathBuf> = {
            let taken = restore.taken.as_ref().expect("just set");
            if taken.all {
                self.registered_files().into_iter().map(|(_, p)| p).collect()
            } else {
                let files = self.files.lock();
                taken.ids.iter().filter_map(|id| files.get(id).cloned()).collect()
            }
        };
        for path in paths {
            match File::options().read(true).write(true).open(&path) {
                Ok(f) => {
                    #[cfg(test)]
                    test_hooks::CHECKPOINT_SYNCS.with(|n| n.set(n.get() + 1));
                    crate::core::sync_audit::note("wal.checkpoint.data_file", &path);
                    // A failed flush is not something the next checkpoint can
                    // retry. On Linux the kernel reports the writeback error
                    // once and may drop the dirty pages with it, so a second
                    // `fdatasync` can succeed over data that never reached the
                    // disk — and that success would drop the log, the only
                    // remaining copy. Poisoning keeps the log for replay and
                    // stops commits pretending to be durable.
                    if let Err(e) = f.sync_data() {
                        self.poison(format!(
                            "checkpoint could not flush {}: {}",
                            path.display(),
                            e
                        ));
                        return Err(e);
                    }
                }
                // A file that no longer exists cannot need flushing.
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                // Failing to *open* loses nothing — no flush was attempted, so
                // no error was consumed — and the next checkpoint may retry.
                Err(e) => return Err(e),
            }
        }

        #[cfg(test)]
        test_hooks::AFTER_CHECKPOINT_SYNC.with(|hook| {
            if let Some(f) = hook.borrow_mut().as_mut() {
                f();
            }
        });

        let mut file = self.writer.lock();

        // Re-checked under the writer lock, because the check at the top of
        // this function is only a cheap early-out. A transaction can register a
        // pending overwrite between that check and this lock; truncating then
        // would strand its payload offset past EOF. Records are appended under
        // this same lock, so once it is held the answer cannot change.
        if self.pending_count.load(Ordering::Acquire) != 0 {
            return Ok(false);
        }
        // Again before staging, since a commit can poison the log while the
        // sync pass runs: a checkpoint that will not drop the records has no
        // business staging them and advancing what the archive claims.
        self.refuse_if_poisoned()?;

        // Staged before the truncation and under the same lock: the records are
        // in the data files by now, which is what makes them safe to *drop*,
        // but a snapshot taken while they were being written still needs them
        // to reach a consistent state.
        // Records logged while the files were being synced are past
        // `synced_end`, and their data may have reached a file after its sync.
        // Discarding them — and declaring them durable, as a truncation does —
        // would lose them on a crash. So they are kept: the log is cut back to
        // exactly that tail, and only what the sync pass covered is dropped.
        let end = self.write_pos.load(Ordering::Relaxed);
        match self.stage_locked(&mut file, synced_end) {
            Ok(true) => {
                self.staged_through_lsn.fetch_max(synced_lsn, Ordering::AcqRel);
            }
            // Not archiving: these records are dropped with no copy.
            Ok(false) => {
                self.unstaged_through_lsn.fetch_max(synced_lsn, Ordering::AcqRel);
            }
            Err(e) => {
                // Losing the segment costs point-in-time coverage, not
                // durability — the data is already flushed. Failing the
                // checkpoint instead would let the log grow without bound
                // because a full disk is the most likely cause. Recorded, so
                // no snapshot window over these records is reported complete.
                self.unstaged_through_lsn.fetch_max(synced_lsn, Ordering::AcqRel);
                log::error!("WAL segment could not be staged for archiving: {}", e);
            }
        }
        let dropped = if end == synced_end {
            self.truncate_locked(&mut file)
        } else {
            self.keep_tail_locked(&mut file, synced_end, end, synced_issued)
        };
        if dropped.is_ok() {
            // The records those files backed are gone; nothing to put back.
            restore.taken = None;
        }
        dropped.map(|()| true)
    }

    /// Replaces the log with its records from `from` to `end`, the ones a
    /// checkpoint's sync pass did not cover, and marks durable only what it
    /// did cover (`covered_issued`).
    ///
    /// Written to a new file and renamed over the log, never shifted in place:
    /// a crash before the rename leaves the whole old log (replay is redo, so
    /// its already-flushed head applies again harmlessly), and after it the
    /// tail alone. Shifting in place would, for a moment, put a torn record at
    /// offset 0, and replay stops at the first one — taking the tail with it.
    /// The sync handle is dropped so the next sync opens the new file.
    fn keep_tail_locked(
        &self,
        file: &mut File,
        from: u64,
        end: u64,
        covered_issued: u64,
    ) -> io::Result<()> {
        // Drops the head of the log just as a truncation drops all of it.
        self.refuse_if_poisoned()?;

        self.write_lsn_mark_best_effort();
        file.flush()?;
        let tail_len = end - from;
        let temp = self.path.with_extension("ckpt");
        {
            let mut source = File::open(&self.path)?;
            source.seek(SeekFrom::Start(from))?;
            let mut target = File::create(&temp)?;
            let copied = io::copy(&mut (&mut source).take(tail_len), &mut target)?;
            if copied != tail_len {
                let _ = std::fs::remove_file(&temp);
                return Err(io::Error::other(format!(
                    "checkpoint copied {copied} of the {tail_len}-byte tail of {}",
                    self.path.display()
                )));
            }
            crate::core::sync_audit::note("wal.keep_tail", &self.path);
            target.sync_data()?;
        }
        std::fs::rename(&temp, &self.path)?;
        sync_parent_dir(&self.path)?;

        let mut reopened = File::options().read(true).write(true).open(&self.path)?;
        reopened.seek(SeekFrom::Start(tail_len))?;
        *file = reopened;
        *self.sync_handle.lock() = None;

        self.write_pos.store(tail_len, Ordering::Relaxed);
        self.bytes_since_checkpoint.store(tail_len, Ordering::Relaxed);
        // Only what the sync pass covered. The tail is durable too — it was
        // just synced into the new file — but its own committers will say so.
        self.durable.fetch_max(covered_issued, Ordering::AcqRel);
        Ok(())
    }

    /// Replays `path`, resolving `file_id` through `resolve`.
    ///
    /// Two passes: the first collects the transactions that committed, the
    /// second applies only their records. Anything belonging to a transaction
    /// with no commit record is discarded, which is what makes a multi-file
    /// write all-or-nothing across a crash.
    ///
    /// Both passes stream the log a record at a time, so peak memory is the
    /// largest single record rather than the whole file.
    pub fn recover(
        path: impl AsRef<Path>,
        resolve: impl Fn(u64) -> Option<PathBuf>,
    ) -> io::Result<RecoveryStats> {
        Self::recover_upto(path, resolve, None)
    }

    /// Replays `path`, stopping at commit sequence `upto`.
    ///
    /// This is [`Wal::recover`] with a ceiling, and it is what makes recovery to
    /// a point in time exact rather than approximate. The cut is taken on the
    /// *commit* sequence, so a transaction is either wholly applied or wholly
    /// skipped: a record whose transaction commits past the ceiling is left out
    /// even though the record itself was written before it.
    ///
    /// `upto` of `None` replays everything, which is ordinary crash recovery.
    pub fn recover_upto(
        path: impl AsRef<Path>,
        resolve: impl Fn(u64) -> Option<PathBuf>,
        upto: Option<u64>,
    ) -> io::Result<RecoveryStats> {
        let path = path.as_ref();
        let mut stats = RecoveryStats::default();

        let total = match std::fs::metadata(path) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(stats),
            Err(e) => return Err(e),
        };
        if total == 0 {
            return Ok(stats);
        }

        // Pass 1: which transactions reached a commit record?
        let mut committed: HashSet<u64> = HashSet::new();
        // `(txn, file_id, position)` of every barrier, committed or not.
        let mut barriers: Vec<(u64, u64, u64)> = Vec::new();
        let valid_bytes;
        {
            let mut scan = LogScanner::open(path)?;
            let mut ordinal = 0u64;
            while let Some(h) = scan.next()? {
                let at = ordinal;
                ordinal += 1;
                stats.last_txn = stats.last_txn.max(h.txn);
                if h.flags & FLAG_COMMIT != 0 {
                    // A marker carries its commit sequence in `offset`.
                    if upto.is_some_and(|ceiling| h.offset > ceiling) {
                        continue;
                    }
                    committed.insert(h.txn);
                    stats.last_commit_lsn = stats.last_commit_lsn.max(h.offset);
                } else if h.flags & FLAG_FORGET != 0 {
                    barriers.push((h.txn, h.file_id, at));
                }
            }
            valid_bytes = scan.consumed;
        }

        // Per file, the position of its last committed barrier: its records
        // before that describe a file that was deleted, and the file under
        // that name now, if any, is another one. Known only once pass 1 has
        // seen every marker, since a barrier's commit comes after it.
        let mut void_before: HashMap<u64, u64> = HashMap::new();
        for (txn, file_id, at) in barriers {
            if committed.contains(&txn) {
                let before = void_before.entry(file_id).or_insert(0);
                *before = (*before).max(at);
            }
        }

        // Pass 2: apply the records those transactions own.
        let mut open_files: HashMap<u64, Option<File>> = HashMap::new();
        {
            let mut scan = LogScanner::open(path)?;
            let mut ordinal = 0u64;
            while let Some(h) = scan.next()? {
                let at = ordinal;
                ordinal += 1;
                // Commit markers and barriers are control records: they
                // describe no bytes and must not be replayed as if they did.
                if h.flags & (FLAG_COMMIT | FLAG_FORGET) != 0 {
                    continue;
                }

                if !committed.contains(&h.txn) {
                    stats.rolled_back += 1;
                    continue;
                }

                if void_before.get(&h.file_id).is_some_and(|&before| at < before) {
                    stats.forgotten += 1;
                    continue;
                }

                let entry = open_files.entry(h.file_id).or_insert_with(|| {
                    resolve(h.file_id).and_then(|p| {
                        File::options()
                            .read(true)
                            .write(true)
                            .create(true)
                            .open(&p)
                            .ok()
                    })
                });

                match entry {
                    Some(target) => {
                        target.seek(SeekFrom::Start(h.offset))?;
                        target.write_all(&scan.payload)?;
                        stats.applied += 1;
                    }
                    None => stats.unresolved += 1,
                }
            }
        }

        // `total` was measured before the scan and `valid_bytes` after it, so
        // the two do not describe the same instant. `valid_bytes` only counts
        // records whose header and payload both read whole and whose CRC
        // matched, so it can never claim bytes that were not there — but it can
        // out-run a length that was read earlier, if anything appended in
        // between.
        //
        // Saturating rather than re-measuring, and the difference matters.
        // Reading the length again after the scan would make the subtraction
        // non-negative, and would also count whatever the other writer appended
        // as trailing garbage — which `open_and_recover` reads as "there was a
        // torn tail" and answers by truncating the log. That turns a stale
        // statistic into deleted records. Zero is the honest answer here: every
        // byte this scan validated was kept, so nothing was discarded.
        //
        // Unsaturated this panicked in debug and wrapped to nearly 2^64 in
        // release, where the same truncate would then fire on a log being
        // actively written. `recovery_racing_a_writer_does_not_underflow_the_tail_count`
        // is that race.
        //
        // None of which makes recovery racing a writer *correct* — it has the
        // log to itself by design, and `open_and_recover` truncating at the end
        // says so. This is the arithmetic refusing to make a violated
        // assumption worse than it already is.
        stats.truncated_tail = discarded_tail(total, valid_bytes);

        // Recovery is only complete once what it wrote is itself durable.
        for target in open_files.values_mut().flatten() {
            crate::core::sync_audit::note("wal.recover.data_file", path);
            target.sync_data()?;
        }

        Ok(stats)
    }
}

/// Header fields of one record.
struct RecordHeader {
    flags: u8,
    txn: u64,
    file_id: u64,
    offset: u64,
}

/// Streams a log one record at a time.
///
/// Stops at the first record that is short, mis-magicked, of an unknown version
/// or fails its CRC — the torn tail a crash leaves behind. `consumed` then holds
/// the length of the durable prefix.
struct LogScanner {
    reader: io::BufReader<File>,
    /// Bytes belonging to records that parsed cleanly.
    consumed: u64,
    /// Payload of the record returned by the last `next` call.
    payload: Vec<u8>,
}

impl LogScanner {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            reader: io::BufReader::with_capacity(64 * 1024, File::open(path)?),
            consumed: 0,
            payload: Vec::new(),
        })
    }

    fn next(&mut self) -> io::Result<Option<RecordHeader>> {
        let mut header = [0u8; HEADER_LEN];
        match self.reader.read_exact(&mut header) {
            Ok(()) => {}
            // A short read here is the end of the log, clean or torn.
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        if u32::from_le_bytes(header[0..4].try_into().unwrap()) != RECORD_MAGIC {
            return Ok(None);
        }
        if header[4] != FORMAT_VERSION {
            return Ok(None); // a log this build does not understand
        }

        let flags = header[5];
        let txn = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let file_id = u64::from_le_bytes(header[16..24].try_into().unwrap());
        let offset = u64::from_le_bytes(header[24..32].try_into().unwrap());
        let len = u32::from_le_bytes(header[32..36].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(header[HEADER_CRC_LEN..HEADER_LEN].try_into().unwrap());

        if len > MAX_RECORD_PAYLOAD {
            return Ok(None);
        }

        self.payload.clear();
        self.payload.resize(len, 0);
        match self.reader.read_exact(&mut self.payload) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let mut digest = CRC.digest();
        digest.update(&header[0..HEADER_CRC_LEN]);
        digest.update(&self.payload);
        if digest.finalize() != crc {
            return Ok(None); // torn or corrupt
        }

        self.consumed += (HEADER_LEN + len) as u64;

        Ok(Some(RecordHeader {
            flags,
            txn,
            file_id,
            offset,
        }))
    }
}

/// An in-place overwrite held back until its transaction commits.
/// An in-place overwrite held back until its transaction commits.
///
/// It stores *where* the bytes are in the log, not the bytes themselves: they
/// are already durable there, so a second copy in memory only limits how large a
/// statement is allowed to be. This keeps the footprint of a pending write at a
/// few dozen bytes regardless of row size.
struct PendingWrite {
    path: PathBuf,
    offset: u64,
    /// Offset of this record's payload within the log file.
    log_offset: u64,
    len: usize,
}

/// The header of a data record: magic, version, ids, length and a CRC over the
/// header's first `HEADER_CRC_LEN` bytes and the payload.
#[inline]
fn record_header(txn: u64, file_id: u64, offset: u64, bytes: &[u8]) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[0..4].copy_from_slice(&RECORD_MAGIC.to_le_bytes());
    header[4] = FORMAT_VERSION;
    header[5] = 0;
    // header[6..8] reserved, left zero
    header[8..16].copy_from_slice(&txn.to_le_bytes());
    header[16..24].copy_from_slice(&file_id.to_le_bytes());
    header[24..32].copy_from_slice(&offset.to_le_bytes());
    header[32..36].copy_from_slice(&(bytes.len() as u32).to_le_bytes());

    let mut digest = CRC.digest();
    digest.update(&header[0..HEADER_CRC_LEN]);
    digest.update(bytes);
    header[HEADER_CRC_LEN..HEADER_LEN].copy_from_slice(&digest.finalize().to_le_bytes());
    header
}

/// The zero-payload record voiding `file_id`'s earlier records once `txn`
/// commits. See [`Wal::forget_files`].
#[inline]
fn forget_header(txn: u64, file_id: u64) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[0..4].copy_from_slice(&RECORD_MAGIC.to_le_bytes());
    header[4] = FORMAT_VERSION;
    header[5] = FLAG_FORGET;
    header[8..16].copy_from_slice(&txn.to_le_bytes());
    header[16..24].copy_from_slice(&file_id.to_le_bytes());

    let mut digest = CRC.digest();
    digest.update(&header[0..HEADER_CRC_LEN]);
    header[HEADER_CRC_LEN..HEADER_LEN].copy_from_slice(&digest.finalize().to_le_bytes());
    header
}

/// How many barrier records the log at `path` holds, committed or not: for
/// tests that pin when a caller writes one.
#[cfg(test)]
pub(crate) fn barriers_in(path: &Path) -> usize {
    let Ok(mut scan) = LogScanner::open(path) else {
        return 0;
    };
    let mut n = 0;
    while let Ok(Some(h)) = scan.next() {
        if h.flags & FLAG_FORGET != 0 {
            n += 1;
        }
    }
    n
}

/// The zero-payload record that marks `txn` committed, carrying `lsn`.
#[inline]
fn marker_header(txn: u64, lsn: u64) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[0..4].copy_from_slice(&RECORD_MAGIC.to_le_bytes());
    header[4] = FORMAT_VERSION;
    header[5] = FLAG_COMMIT;
    header[8..16].copy_from_slice(&txn.to_le_bytes());
    // A marker describes no bytes, so `file_id` and `len` stay zero and the
    // `offset` field carries the commit sequence instead.
    header[24..32].copy_from_slice(&lsn.to_le_bytes());

    let mut digest = CRC.digest();
    digest.update(&header[0..HEADER_CRC_LEN]);
    header[HEADER_CRC_LEN..HEADER_LEN].copy_from_slice(&digest.finalize().to_le_bytes());
    header
}

/// `write_all` for several buffers in one `writev` where the kernel takes them
/// whole, which for a regular file it does; a short write continues from where
/// it stopped, as `write_all` would.
fn write_all_vectored(file: &mut File, mut bufs: &mut [io::IoSlice<'_>]) -> io::Result<()> {
    io::IoSlice::advance_slices(&mut bufs, 0);
    while !bufs.is_empty() {
        match file.write_vectored(bufs) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole WAL record",
                ));
            }
            Ok(n) => io::IoSlice::advance_slices(&mut bufs, n),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod test_hooks {
    thread_local! {
        /// Makes the next commit marker written on this thread fail before
        /// touching the file, as a full disk would.
        pub static FAIL_COMMIT_MARKER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };

        /// Makes the next `Wal::forget_files` on this thread fail before
        /// touching the file, as a full disk would.
        pub static FAIL_FORGET: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };

        /// How many `Wal::forget_files` calls on this thread `FAIL_FORGET`
        /// lets through before failing one: the second log of a drop.
        pub static FORGETS_BEFORE_FAILING: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };

        /// Makes the next `Wal::forget_files` on this thread fail at its
        /// sync, after the barrier and its commit marker are in the file, and
        /// poison the log, as a failed `fdatasync` does. What a crash keeps
        /// of those bytes is then undecided, which is the case to test.
        pub static FAIL_BARRIER_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };

        /// Makes the next WAL write on this thread put only this many bytes
        /// in the file and then fail, as a full disk or EIO would.
        pub static SHORT_WAL_WRITE: std::cell::Cell<Option<usize>> =
            const { std::cell::Cell::new(None) };

        /// Makes the next failed WAL write on this thread also fail to be cut
        /// back, leaving its partial record in the log: the failure that
        /// poisons. No real file fails `ftruncate` on demand.
        pub static FAIL_WAL_ROLLBACK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };

        /// Data files a checkpoint on this thread has synced.
        pub static CHECKPOINT_SYNCS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };

        /// Runs inside `Wal::checkpoint` after its data-file sync pass and
        /// before it takes the writer lock: the window a concurrent writer's
        /// records fall into.
        pub static AFTER_CHECKPOINT_SYNC: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
            const { std::cell::RefCell::new(None) };
    }

    /// Directories whose log `open_and_recover` refuses, as it refuses one it
    /// cannot read, whatever the uid (a mode-000 log refuses nothing to root).
    /// Keyed by path, not by thread: the opens under test run on their own.
    pub static REFUSE_RECOVERY: parking_lot::Mutex<Vec<std::path::PathBuf>> =
        parking_lot::Mutex::new(Vec::new());
}

/// Runs `f`, which may block on disk I/O, without parking a runtime worker.
///
/// On a multi-threaded Tokio runtime this is `block_in_place`: the worker's
/// queue is handed to another thread for the duration, so the tasks behind it
/// — other clients' reads above all — keep running while this thread sits in
/// `fdatasync`. The closure still runs on this thread, synchronously and in
/// the same order, so nothing about what is durable when changes.
///
/// `block_in_place` panics on a current-thread runtime (every plain
/// `#[tokio::test]`), so there — and outside any runtime, or on a
/// `spawn_blocking` thread, where blocking is already fine — `f` simply runs.
#[inline]
pub(crate) fn blocking_io<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

// ---------------------------------------------------------------------------
// Per-database registry
// ---------------------------------------------------------------------------

/// Logs, keyed by database directory.
///
/// Deliberately not one process-global log. A process can open more than one
/// database — every test binary does — and a global would make the second
/// database log into the first one's directory, so its own recovery would find
/// nothing to replay and the writes it deferred would be lost.
///
/// Read on every storage write (several lookups per row), changed only when a
/// database is opened, created or dropped. So it is an immutable snapshot
/// behind an `ArcSwap`: a lookup is a lock-free load and a borrowed-`&str`
/// probe, with no `String` built and no global mutex taken. A change copies
/// the map under `WALS_WRITE` and publishes the copy; every lookup that starts
/// after the store sees it, which is exactly what the mutex gave — a lookup
/// that raced a `remove_for` could already get the old log back then too.
static WALS: OnceLock<arc_swap::ArcSwap<Registry>> = OnceLock::new();

/// Serialises the copy-and-publish of [`WALS`], so two concurrent changes
/// cannot each copy the same snapshot and lose the other's edit.
static WALS_WRITE: Mutex<()> = Mutex::new(());

type Registry = HashMap<String, Arc<Wal>, ahash::RandomState>;

fn wals() -> &'static arc_swap::ArcSwap<Registry> {
    WALS.get_or_init(|| arc_swap::ArcSwap::from_pointee(Registry::default()))
}

/// Applies `edit` to a copy of the registry and publishes the copy.
fn edit_wals<R>(edit: impl FnOnce(&mut Registry) -> R) -> R {
    let _writer = WALS_WRITE.lock();
    let mut next: Registry = (**wals().load()).clone();
    let out = edit(&mut next);
    wals().store(Arc::new(next));
    out
}

/// [`dir_key`] without the allocation, for lookups.
#[inline]
fn dir_key_str(dir: &str) -> &str {
    let trimmed = dir.trim_end_matches(['/', '\\']);
    if trimmed.is_empty() { dir } else { trimmed }
}

/// Canonical key for a database directory, so the path a provider was built
/// with and the one the database was opened with agree.
pub fn dir_key(dir: &str) -> String {
    dir_key_str(dir).to_string()
}

/// The log for `dir`, or `None` when that database has none.
#[inline]
pub fn wal_for(dir: &str) -> Option<Arc<Wal>> {
    wals().load().get(dir_key_str(dir)).cloned()
}

/// Runs `f` with the log for `dir` borrowed rather than cloned out.
///
/// For the storage write path, which looks the log up several times per row:
/// cloning the `Arc` there is an atomic increment and decrement on one cache
/// line shared by every writer thread, and under concurrency that bouncing
/// line — not the lookup — was the cost. Resolved fresh on every call, so a
/// dropped and recreated directory can never be answered with a stale log.
#[inline]
pub fn with_wal_for<R>(dir: &str, f: impl FnOnce(Option<&Wal>) -> R) -> R {
    let snapshot = wals().load();
    f(snapshot.get(dir_key_str(dir)).map(|w| &**w))
}

/// Every installed log, with the directory it belongs to.
///
/// For [`crate::core::log_writer`], which flushes on a timer for commits that
/// did not wait for a sync of their own and so has no one database to ask.
pub fn registered() -> Vec<(String, Arc<Wal>)> {
    wals()
        .load()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Installs the log for `dir`, replacing any previous one.
pub fn install_for(dir: &str, w: Arc<Wal>) {
    let key = dir_key(dir);
    edit_wals(|m| m.insert(key.clone(), w.clone()));

    // Cloned out and the lock released before any hook runs: a hook that looked
    // the log back up — the obvious thing for one to do — would deadlock on a
    // registry still held here.
    let hooks: Vec<InstallHook> = install_hooks().lock().iter().cloned().collect();
    for hook in hooks {
        hook(&key, &w);
    }
}

/// Told about every log installed from now on.
type InstallHook = Arc<dyn Fn(&str, &Arc<Wal>) + Send + Sync>;

static INSTALL_HOOKS: OnceLock<Mutex<Vec<InstallHook>>> = OnceLock::new();

fn install_hooks() -> &'static Mutex<Vec<InstallHook>> {
    INSTALL_HOOKS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Runs `hook` whenever a log is installed.
///
/// For the backup service, which has to start archiving the log of a database
/// created while it is running. Asking `CREATE DATABASE` to notify it instead
/// would cover that one path and miss every other way a log appears, and would
/// put a backup dependency in the catalogue; this way the registry that knows
/// about the log is the thing that says so.
///
/// Hooks are never removed — they are held for the life of the process — so a
/// hook that outlives what it was registered for must hold a weak reference and
/// do nothing once it cannot be upgraded.
pub fn on_log_installed(hook: impl Fn(&str, &Arc<Wal>) + Send + Sync + 'static) {
    install_hooks().lock().push(Arc::new(hook));
}

/// Forgets the log for `dir`, returning it if one was registered.
///
/// The registry holds an `Arc<Wal>` and an open handle for every directory ever
/// installed, and nothing else removes them — so a directory that goes away
/// leaves both behind for the life of the process.
///
/// **A log must not outlive its directory.** It holds physical redo records
/// naming files *inside* that directory; once the directory is gone the records
/// describe nothing and the log is meaningless. Keeping it registered is not
/// merely wasteful: `DROP DATABASE` unlinks the directory, so the retained
/// handle refers to an unlinked file that no later reader can ever see.
///
/// The `Arc` is returned rather than dropped here, so a caller holding one is
/// unaffected — the handle closes when the last reference goes, not when the
/// registry forgets it.
pub fn remove_for(dir: &str) -> Option<Arc<Wal>> {
    edit_wals(|m| m.remove(dir_key_str(dir)))
}

/// Registers every regular file in `dir` with `wal`, so a log left by a previous
/// run can be resolved before any table has been opened.
pub fn register_directory(dir: &str, wal: &Wal) -> io::Result<usize> {
    let mut n = 0;
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    for entry in entries.flatten() {
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        // The log is not a participant in its own replay.
        if name == DEFAULT_WAL_NAME {
            continue;
        }
        wal.register_file(file_id_for_name(&name), entry.path());
        n += 1;
    }

    Ok(n)
}

/// Opens the log for `dir`, replaying anything left behind, and installs it.
///
/// Replay happens whether or not logging is currently enabled: a log on disk
/// means writes that may not have reached their data files, and skipping it
/// because the feature was switched off would lose exactly those writes.
pub fn open_and_recover(dir: &str) -> io::Result<(Arc<Wal>, RecoveryStats)> {
    #[cfg(test)]
    if test_hooks::REFUSE_RECOVERY
        .lock()
        .iter()
        .any(|refused| refused.as_path() == Path::new(dir))
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Permission denied (os error 13), injected: the log cannot be read",
        ));
    }
    let wal_path = Path::new(dir).join(DEFAULT_WAL_NAME);
    let wal = Arc::new(Wal::open(&wal_path)?);
    register_directory(dir, &wal)?;

    let files = wal.clone();
    let stats = Wal::recover(&wal_path, move |id| files.files.lock().get(&id).cloned())?;

    // Resume the commit sequence where the previous run left off, so it stays
    // monotonic across restarts and can serve as a durable point-in-time token.
    wal.commit_lsn
        .fetch_max(stats.last_commit_lsn, Ordering::AcqRel);
    // And transaction ids past every one in the log, which a log kept below
    // still holds: pass 1 of the next recovery reads committed-ness by id
    // alone, so an old marker would commit a new transaction's partial writes.
    wal.next_txn
        .fetch_max(stats.last_txn.saturating_add(1), Ordering::AcqRel);

    // A voided record never needs replaying again, so a log holding only those
    // is done with as much as one whose records were applied.
    if stats.applied > 0
        || stats.rolled_back > 0
        || stats.truncated_tail > 0
        || stats.forgotten > 0
    {
        log::info!(
            "WAL recovery in {}: applied {}, rolled back {}, voided {}, unresolved {}, discarded {} trailing bytes",
            dir,
            stats.applied,
            stats.rolled_back,
            stats.forgotten,
            stats.unresolved,
            stats.truncated_tail
        );
        // The log has served its purpose; a fresh one starts empty.
        wal.truncate()?;
    }

    Ok((wal, stats))
}

/// What a stretch of log bytes covers, without replaying it.
///
/// Returns `(first_lsn, last_lsn, commit_count)`, where the sequences are zero
/// when the stretch holds no commit marker at all. Only headers are examined,
/// so this is a linear walk that skips over every payload rather than reading
/// it — which is what lets the archiver label a 64 MiB segment for the manifest
/// without a second pass over the data.
///
/// Stops at the first malformed record, exactly as replay does, so a torn tail
/// cannot make the range claim more than it covers.
pub fn scan_commit_range(bytes: &[u8]) -> (u64, u64, u64) {
    let mut first = 0u64;
    let mut last = 0u64;
    let mut commits = 0u64;
    let mut offset = 0usize;

    while offset + HEADER_LEN <= bytes.len() {
        let header = &bytes[offset..offset + HEADER_LEN];

        let magic = u32::from_le_bytes(header[0..4].try_into().expect("4 bytes"));
        if magic != RECORD_MAGIC || header[4] != FORMAT_VERSION {
            break;
        }

        let len = u32::from_le_bytes(header[32..36].try_into().expect("4 bytes")) as usize;
        if len > MAX_RECORD_PAYLOAD || offset + HEADER_LEN + len > bytes.len() {
            break;
        }

        let stored_crc = u32::from_le_bytes(
            header[HEADER_CRC_LEN..HEADER_LEN]
                .try_into()
                .expect("4 bytes"),
        );
        let mut digest = CRC.digest();
        digest.update(&header[0..HEADER_CRC_LEN]);
        digest.update(&bytes[offset + HEADER_LEN..offset + HEADER_LEN + len]);
        if digest.finalize() != stored_crc {
            break;
        }

        if header[5] & FLAG_COMMIT != 0 {
            // A marker carries its commit sequence in the otherwise unused
            // `offset` field.
            let lsn = u64::from_le_bytes(header[24..32].try_into().expect("8 bytes"));
            if first == 0 || lsn < first {
                first = lsn;
            }
            last = last.max(lsn);
            commits += 1;
        }

        offset += HEADER_LEN + len;
    }

    (first, last, commits)
}

/// Whether a long-running statement should commit and open a fresh transaction.
///
/// An open transaction blocks checkpointing — the log cannot be reclaimed while
/// a pending overwrite still points into it — so a statement that runs for
/// millions of rows would otherwise grow the log without bound. Breaking it into
/// batches lets the log be reclaimed, at the cost of the statement being atomic
/// per batch rather than as a whole.
///
/// Returns `false` when there is no log, where no such limit applies.
pub fn should_break_transaction<S: crate::core::storage_providers::traits::StorageIO>(
    io: &S,
) -> bool {
    match io.get_location().and_then(|loc| wal_for(&loc)) {
        Some(w) => w.grant_break(crate::wal_checkpoint_bytes()),
        None => false,
    }
}

impl Wal {
    /// Whether a long transaction should commit now so the log can be
    /// checkpointed: past the threshold, at most once per eighth of it.
    ///
    /// It used to answer yes for *every* call once the log passed the
    /// threshold, and the log stays past it for as long as checkpoints
    /// decline — so a compaction under write load committed after every row,
    /// one transaction per row for the rest of the run. Spacing the answers by
    /// what has been logged since the last yes keeps the batches large while
    /// still letting the log be cut.
    fn grant_break(&self, threshold: u64) -> bool {
        let logged = self.bytes_since_checkpoint.load(Ordering::Relaxed);
        if logged < threshold {
            return false;
        }
        let last = self.break_granted_at.load(Ordering::Relaxed);
        // A checkpoint since the last grant reset the counter below it.
        let due = logged < last || logged - last >= (threshold / 8).max(1);
        due && self
            .break_granted_at
            .compare_exchange(last, logged, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }
}

/// Checkpoints every installed log, for process shutdown.
pub fn checkpoint_all() -> io::Result<()> {
    let logs: Vec<Arc<Wal>> = wals().load().values().cloned().collect();
    let mut first_err = None;
    for w in logs {
        if let Err(e) = w.checkpoint() {
            log::error!("WAL checkpoint failed for {:?}: {}", w.path(), e);
            first_err.get_or_insert(e);
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("rastdb_wal_")
            .tempdir()
            .unwrap()
    }

    fn read_file(p: &Path) -> Vec<u8> {
        std::fs::read(p).unwrap_or_default()
    }

    fn resolver(p: PathBuf) -> impl Fn(u64) -> Option<PathBuf> {
        move |_| Some(p.clone())
    }

    #[test]
    fn logs_and_replays_committed_records() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");

        let wal = Wal::open(&wal_path).unwrap();
        wal.log_and_commit(1, 0, b"hello ").unwrap();
        wal.log_and_commit(1, 6, b"world").unwrap();

        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied, 2);
        assert_eq!(stats.rolled_back, 0);
        assert_eq!(read_file(&data), b"hello world");
    }

    /// The atomicity guarantee: a transaction that never reached its commit
    /// record contributes nothing, even though its bytes are in the log.
    #[test]
    fn uncommitted_transaction_is_rolled_back() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");

        let wal = Wal::open(&wal_path).unwrap();

        // A committed transaction.
        let t1 = wal.begin();
        wal.log(t1, 1, 0, b"AAAA").unwrap();
        wal.commit_txn(t1).unwrap();

        // A transaction that died before committing — as if the process was
        // killed between writing the row bytes and the pointer record.
        // Logged and flushed, but the transaction never ends: no marker.
        let t2 = wal.begin();
        let s = wal.log(t2, 1, 4, b"BBBB").unwrap();
        wal.commit_through(s).unwrap();

        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied, 1);
        assert_eq!(stats.rolled_back, 1);
        assert_eq!(
            read_file(&data),
            b"AAAA",
            "the half-finished write must not appear"
        );
    }

    /// A multi-file write is all-or-nothing: both records carry the same txn and
    /// only the last one commits.
    #[test]
    fn multi_file_transaction_applies_together() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let rows = dir.path().join("rows.db");
        let ptrs = dir.path().join("ptrs.db");

        let rows_id = file_id_for_name("rows.db");
        let ptrs_id = file_id_for_name("ptrs.db");

        let wal = Wal::open(&wal_path).unwrap();
        let txn = wal.begin();
        wal.log(txn, rows_id, 0, b"ROWDATA").unwrap();
        wal.log(txn, ptrs_id, 0, b"PTR").unwrap();
        wal.commit_txn(txn).unwrap();

        let rows_c = rows.clone();
        let ptrs_c = ptrs.clone();
        let stats = Wal::recover(&wal_path, move |id| {
            if id == rows_id {
                Some(rows_c.clone())
            } else if id == ptrs_id {
                Some(ptrs_c.clone())
            } else {
                None
            }
        })
        .unwrap();

        assert_eq!(stats.applied, 2);
        assert_eq!(read_file(&rows), b"ROWDATA");
        assert_eq!(read_file(&ptrs), b"PTR");
    }

    /// A committed barrier voids its file's earlier records and only those:
    /// what comes after it, and every other file's records, still replay. A
    /// file deleted and created again under one name has one id, so without
    /// this the deleted file's bytes landed in the new one.
    #[test]
    fn a_barrier_voids_only_its_files_earlier_records() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let reborn = dir.path().join("reborn.db");
        let other = dir.path().join("other.db");
        let reborn_id = file_id_for_name("reborn.db");
        let other_id = file_id_for_name("other.db");

        let wal = Wal::open(&wal_path).unwrap();
        // The first incarnation, longer than the second will be. An uncommitted
        // transaction spans the barrier and commits after it: voided too, as
        // the bytes describe the deleted file whenever they commit.
        let late = wal.begin();
        wal.log(late, reborn_id, 12, b"LATE").unwrap();
        wal.log_and_commit(reborn_id, 0, b"OLD-OLD-OLD-").unwrap();
        wal.log_and_commit(other_id, 0, b"keep").unwrap();
        wal.forget_files(&[reborn_id]).unwrap();
        wal.commit_txn(late).unwrap();
        // The second incarnation, deleted in turn: the last barrier is the one
        // that counts, or a second rewrite before a checkpoint would bring
        // this one's bytes into the third.
        wal.log_and_commit(reborn_id, 0, b"MID-MID").unwrap();
        wal.forget_files(&[reborn_id]).unwrap();
        // The third.
        wal.log_and_commit(reborn_id, 0, b"new").unwrap();
        wal.log_and_commit(other_id, 4, b"!").unwrap();

        let (r, o) = (reborn.clone(), other.clone());
        let stats = Wal::recover(&wal_path, move |id| {
            (id == reborn_id)
                .then(|| r.clone())
                .or_else(|| (id == other_id).then(|| o.clone()))
        })
        .unwrap();
        assert_eq!(read_file(&reborn), b"new", "a voided record reached the file");
        assert_eq!(read_file(&other), b"keep!", "another file's records were voided");
        assert_eq!(stats.forgotten, 3);
        assert_eq!(stats.applied, 3);
    }

    /// A log whose records were all voided is done with, like one whose
    /// records were applied, and is truncated: kept, its old commit markers
    /// would outlive the process that wrote them.
    #[test]
    fn a_log_of_only_voided_records_is_truncated_by_recovery() {
        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().into_owned();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let id = file_id_for_name("gone.db");
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_and_commit(id, 0, b"gone").unwrap();
            wal.forget_files(&[id]).unwrap();
        }
        let (_wal, stats) = open_and_recover(&dir_str).unwrap();
        assert_eq!((stats.applied, stats.forgotten), (0, 1));
        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(),
            0,
            "a log recovery finished with was kept"
        );
        remove_for(&dir_str);
    }

    /// A log recovery keeps (here, one naming a file that is not there) still
    /// holds its commit markers, which name transactions by id. The next
    /// process must not hand those ids out again: one of its transactions,
    /// cut off by a crash before committing, read as committed by the old
    /// marker and its partial writes were replayed.
    #[test]
    fn a_kept_log_does_not_commit_a_later_processs_transaction() {
        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().into_owned();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("t.db");
        std::fs::write(&data, b"").unwrap();
        let missing = file_id_for_name("missing.db");
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_and_commit(missing, 0, b"elsewhere").unwrap();
        }
        let (wal, stats) = open_and_recover(&dir_str).unwrap();
        assert_eq!(stats.unresolved, 1);
        assert!(
            std::fs::metadata(&wal_path).unwrap().len() > 0,
            "the log was truncated, so it no longer tests a kept one"
        );
        // A transaction cut off before its commit.
        let txn = wal.begin();
        wal.log(txn, file_id_for_name("t.db"), 0, b"torn").unwrap();
        drop(wal);
        remove_for(&dir_str);

        let (_wal, stats) = open_and_recover(&dir_str).unwrap();
        assert_eq!(read_file(&data), b"", "an uncommitted transaction was replayed");
        assert_eq!(stats.applied, 0);
        remove_for(&dir_str);
    }

    /// A barrier whose commit marker never reached the log voids nothing, and
    /// recovery to a point before the barrier committed keeps what it voids:
    /// the file still existed then.
    #[test]
    fn an_uncommitted_or_later_barrier_voids_nothing() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("t.db");
        let id = file_id_for_name("t.db");

        let wal = Wal::open(&wal_path).unwrap();
        wal.log_and_commit(id, 0, b"kept").unwrap(); // commit sequence 1
        let before_barrier = std::fs::metadata(&wal_path).unwrap().len();
        wal.forget_files(&[id]).unwrap(); // commit sequence 2
        let with_barrier = std::fs::read(&wal_path).unwrap();

        // Cut just before the barrier's marker: a torn tail.
        let torn = dir.path().join("torn.log");
        std::fs::write(&torn, &with_barrier[..with_barrier.len() - HEADER_LEN]).unwrap();
        assert!(with_barrier.len() as u64 > before_barrier + HEADER_LEN as u64);
        let stats = Wal::recover(&torn, resolver(data.clone())).unwrap();
        assert_eq!(read_file(&data), b"kept", "an uncommitted barrier voided a record");
        assert_eq!(stats.forgotten, 0);

        std::fs::remove_file(&data).unwrap();
        let stats = Wal::recover_upto(&wal_path, resolver(data.clone()), Some(1)).unwrap();
        assert_eq!(read_file(&data), b"kept", "a barrier past the ceiling voided a record");
        assert_eq!(stats.forgotten, 0);

        std::fs::remove_file(&data).unwrap();
        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert!(!data.exists(), "replay recreated a file whose records were all voided");
        assert_eq!(stats.forgotten, 1);
    }

    /// A barrier names a file but describes no bytes, so a checkpoint owes the
    /// file nothing for it.
    #[test]
    fn a_barrier_gives_a_checkpoint_nothing_to_flush() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("t.db");
        std::fs::write(&data, b"").unwrap();
        let id = file_id_for_name("t.db");

        let wal = Wal::open(&wal_path).unwrap();
        wal.register_file(id, &data);
        wal.forget_files(&[id]).unwrap();
        test_hooks::CHECKPOINT_SYNCS.with(|n| n.set(0));
        assert!(wal.checkpoint().unwrap());
        assert_eq!(test_hooks::CHECKPOINT_SYNCS.with(|n| n.get()), 0);
        drop(wal);

        // Nor does a log reopened with a barrier still in it.
        let wal = Wal::open(&wal_path).unwrap();
        wal.forget_files(&[id]).unwrap();
        drop(wal);
        assert_eq!(files_named_by_log(&wal_path), Some(HashSet::new()));
    }

    /// The write-ahead ordering: an overwrite must not reach the data file until
    /// its record is durable, or a crash mid-write would destroy the old bytes
    /// with no undo record to restore them.
    #[test]
    fn in_place_overwrite_waits_for_commit() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");
        std::fs::write(&data, b"OLDOLDOLD").unwrap();

        let wal = Wal::open(&wal_path).unwrap();
        let id = file_id_for_name("table.db");
        wal.register_file(id, &data);

        let txn = wal.begin();
        wal.log_in_place(txn, id, 0, b"NEW").unwrap();

        assert_eq!(
            read_file(&data),
            b"OLDOLDOLD",
            "the data file must be untouched before the commit"
        );

        wal.commit_txn(txn).unwrap();

        assert_eq!(
            read_file(&data),
            b"NEWOLDOLD",
            "commit applies the overwrite"
        );
    }

    /// An overwrite whose transaction never commits leaves the old value intact
    /// *and* is rolled back on replay.
    #[test]
    fn uncommitted_overwrite_never_reaches_the_file() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");
        std::fs::write(&data, b"ORIGINAL").unwrap();

        {
            let wal = Wal::open(&wal_path).unwrap();
            let id = file_id_for_name("table.db");
            wal.register_file(id, &data);

            let txn = wal.begin();
            // Logged, flushed, but never committed - the process "dies" here.
            let seq = wal.log_in_place(txn, id, 0, b"CLOBBER!").unwrap();
            wal.commit_through(seq).unwrap();
        }

        assert_eq!(read_file(&data), b"ORIGINAL");

        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied, 0);
        assert_eq!(stats.rolled_back, 1);
        assert_eq!(
            read_file(&data),
            b"ORIGINAL",
            "an uncommitted overwrite must not survive recovery either"
        );
    }

    /// A statement that touches many rows is one transaction, so a crash
    /// partway through it must leave none of those rows changed — not a
    /// prefix of them.
    #[test]
    fn an_unfinished_statement_rolls_back_every_row() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let rows = dir.path().join("rows.db");
        let ptrs = dir.path().join("ptrs.db");
        std::fs::write(&rows, vec![b'.'; 64]).unwrap();
        std::fs::write(&ptrs, vec![b'.'; 64]).unwrap();

        let rows_id = file_id_for_name("rows.db");
        let ptrs_id = file_id_for_name("ptrs.db");

        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.register_file(rows_id, &rows);
            wal.register_file(ptrs_id, &ptrs);

            // One statement, eight rows, two files per row - and no commit.
            let txn = wal.begin();
            for i in 0..8u64 {
                wal.log_in_place(txn, rows_id, i * 4, b"NEWR").unwrap();
                wal.log_in_place(txn, ptrs_id, i * 4, b"NEWP").unwrap();
            }
            wal.commit_through(wal.issued.load(Ordering::Acquire))
                .unwrap();
        }

        // Nothing reached the files: overwrites wait for the commit.
        assert_eq!(read_file(&rows), vec![b'.'; 64]);
        assert_eq!(read_file(&ptrs), vec![b'.'; 64]);

        let rows_c = rows.clone();
        let ptrs_c = ptrs.clone();
        let stats = Wal::recover(&wal_path, move |id| {
            if id == rows_id {
                Some(rows_c.clone())
            } else if id == ptrs_id {
                Some(ptrs_c.clone())
            } else {
                None
            }
        })
        .unwrap();

        assert_eq!(stats.applied, 0);
        assert_eq!(
            stats.rolled_back, 16,
            "all sixteen writes belong to the statement"
        );
        assert_eq!(
            read_file(&rows),
            vec![b'.'; 64],
            "no row may be half-updated"
        );
        assert_eq!(read_file(&ptrs), vec![b'.'; 64]);
    }

    #[test]
    fn replay_is_idempotent() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");

        let wal = Wal::open(&wal_path).unwrap();
        wal.log_and_commit(7, 0, b"abcdef").unwrap();
        wal.log_and_commit(7, 2, b"ZZ").unwrap();

        let first = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        let after_first = read_file(&data);
        let second = Wal::recover(&wal_path, resolver(data.clone())).unwrap();

        assert_eq!(first.applied, second.applied);
        assert_eq!(after_first, read_file(&data));
        assert_eq!(after_first, b"abZZef");
    }

    /// A WAL-backed table of `(id U64, label STRING)` holding `n` rows.
    async fn logged_table(
        dir: &Path,
        n: u64,
    ) -> (
        crate::core::row::table::Table<crate::core::storage_providers::file_sync::LocalStorageProvider>,
        Arc<Wal>,
    ) {
        use crate::core::db_type::DbType;
        use crate::core::row::row::{ColumnWritePayload, RowWrite};
        use crate::core::rql::lexer_ct::CreateColumnData;
        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(512);
        let dir_str = dir.to_string_lossy().to_string();
        let wal = Arc::new(Wal::open(dir.join(DEFAULT_WAL_NAME)).unwrap());
        install_for(&dir_str, wal.clone());
        let io = rclite::Arc::new(
            crate::core::storage_providers::file_sync::LocalStorageProvider::new(&dir_str, Some("lt"))
                .await,
        );
        let t = crate::core::row::table::Table::new(
            "lt",
            io,
            vec![
                CreateColumnData { name: "id".into(), data_type: DbType::U64 },
                CreateColumnData { name: "label".into(), data_type: DbType::STRING },
            ],
        )
        .await;
        let payload = |bytes: &[u8], write_order: u32, column_type: DbType| {
            let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(bytes);
            let size = column_type.get_size();
            ColumnWritePayload { data: mb, write_order, column_type, size }
        };
        for i in 0..n {
            let mut rw = RowWrite { columns_writing_data: smallvec::SmallVec::new() };
            rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
            rw.columns_writing_data
                .push(payload(format!("label-{i:04}").as_bytes(), 1, DbType::STRING));
            t.insert_row(rw).await.unwrap();
        }
        (t, wal)
    }

    /// A storage transaction opened inside a helper must be ended on every
    /// path. One left open after a failed commit kept the log's pending count
    /// raised, and no checkpoint could run again — how a deployed node's log
    /// grew without bound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_delete_commit_leaves_no_transaction_open() {
        use crate::core::row::row_pointer::RowPointerIterator;
        let dir = temp_dir();
        let (t, wal) = logged_table(dir.path(), 4).await;
        let mut it = RowPointerIterator::new(t.io_pointers.clone()).await.unwrap();
        let (pos, mut pointer) = it.next_row_pointer_with_offset().await.unwrap().unwrap();

        test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(true));
        let outcome = pointer
            .delete(t.io_pointers.clone(), pos, None, crate::core::mvcc::transaction::FROZEN_TX)
            .await;
        test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(false));
        assert!(outcome.is_err(), "the injected commit failure was swallowed");
        assert_eq!(wal.pending_transactions(), 0, "the failed delete left its transaction open");
        assert!(wal.checkpoint().unwrap(), "the log can no longer be checkpointed");
        let _ = remove_for(&dir.path().to_string_lossy());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_in_place_update_commit_leaves_no_transaction_open() {
        use crate::core::db_type::DbType;
        use crate::core::row::row::{ColumnWritePayload, RowWrite};
        use crate::core::row::row_pointer::RowPointerIterator;
        let dir = temp_dir();
        let (t, wal) = logged_table(dir.path(), 4).await;
        let mut it = RowPointerIterator::new(t.io_pointers.clone()).await.unwrap();
        let (pos, mut pointer) = it.next_row_pointer_with_offset().await.unwrap().unwrap();
        // Same length, outside any transaction: the in-place path.
        let mut rw = RowWrite { columns_writing_data: smallvec::SmallVec::new() };
        for (bytes, order, ty) in [
            (0u64.to_le_bytes().to_vec(), 0u32, DbType::U64),
            (b"label-XXXX".to_vec(), 1, DbType::STRING),
        ] {
            let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            let size = ty.get_size();
            rw.columns_writing_data.push(ColumnWritePayload { data: mb, write_order: order, column_type: ty, size });
        }

        test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(true));
        let outcome = pointer
            .update_with_pointer_record_pos(
                t.io_pointers.clone(),
                t.io_rows.clone(),
                pos,
                &t.last_row_id,
                &t.len,
                #[cfg(feature = "enable_long_row")]
                0,
                &rw,
                None,
                None,
                crate::core::mvcc::transaction::FROZEN_TX,
            )
            .await;
        test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(false));
        assert!(outcome.is_err(), "the injected commit failure was swallowed");
        assert_eq!(wal.pending_transactions(), 0, "the failed update left its transaction open");
        assert!(wal.checkpoint().unwrap(), "the log can no longer be checkpointed");
        let _ = remove_for(&dir.path().to_string_lossy());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_compaction_commit_leaves_no_transaction_open() {
        let dir = temp_dir();
        let (t, wal) = logged_table(dir.path(), 40).await;
        t.wait_filters_ready().await;
        t.delete_rows(
            "id < 20",
            {
                let calc = crate::core::row::schema::SchemaCalculator;
                let mut cols = smallvec::SmallVec::new();
                for f in t.schema.fields.iter() {
                    let (column_offset, schema_id) = calc.calculate_schema_offset(&f.name, &t.schema.fields);
                    cols.push(crate::core::row::row::ColumnFetchingData {
                        column_offset,
                        column_type: f.db_type.clone(),
                        size: f.size,
                        schema_id,
                    });
                }
                crate::core::row::row::RowFetch { columns_fetching_data: cols }
            },
            None,
        )
        .await
        .unwrap();

        test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(true));
        let outcome = t.compact().await;
        test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(false));
        assert!(outcome.is_err(), "the injected commit failure was swallowed");
        assert_eq!(wal.pending_transactions(), 0, "the failed compaction left its transaction open");
        assert!(wal.checkpoint().unwrap(), "the log can no longer be checkpointed");
        let _ = remove_for(&dir.path().to_string_lossy());
    }

    #[test]
    fn a_failed_write_leaves_the_log_as_it_was() {
        // Each write site: a data record, a commit marker, a record+marker.
        for site in 0..3 {
            let dir = temp_dir();
            let wal_path = dir.path().join(DEFAULT_WAL_NAME);
            let data = dir.path().join("data.db");
            std::fs::write(&data, vec![0u8; 64]).unwrap();
            let wal = Wal::open(&wal_path).unwrap();
            let id = file_id_for_name("data.db");
            wal.register_file(id, &data);
            wal.log_and_commit(id, 8, b"PRIOR").unwrap();
            let before = read_file(&wal_path).len();

            let t = wal.begin();
            if site == 1 {
                wal.log(t, id, 0, &[2u8; 300]).unwrap();
            }
            test_hooks::SHORT_WAL_WRITE.with(|h| h.set(Some(HEADER_LEN + 20)));
            let failed = match site {
                0 => wal.log(t, id, 0, &[2u8; 4096]).map(|_| ()),
                1 => wal.commit_txn(t),
                _ => wal.log_and_commit(id, 0, &[2u8; 4096]),
            };
            test_hooks::SHORT_WAL_WRITE.with(|h| h.set(None));
            assert!(failed.is_err(), "site {site}: the injected failure was swallowed");
            let expect = if site == 1 { before + HEADER_LEN + 300 } else { before };
            assert_eq!(
                read_file(&wal_path).len(),
                expect,
                "site {site}: a partial record was left in the log"
            );

            // The next in-place write reads its payload back from the offset
            // `write_pos` gave it; after a desync that was foreign bytes.
            let t3 = wal.begin();
            wal.log_in_place(t3, id, 0, b"GOOD").unwrap();
            wal.commit_txn(t3).unwrap();
            assert_eq!(&read_file(&data)[..4], b"GOOD", "site {site}");

            // And replay reads every later record: nothing sits behind garbage.
            std::fs::write(&data, vec![0u8; 64]).unwrap();
            let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
            assert_eq!(stats.truncated_tail, 0, "site {site}");
            let bytes = read_file(&data);
            assert_eq!(&bytes[..4], b"GOOD", "site {site}");
            assert_eq!(&bytes[8..13], b"PRIOR", "site {site}");
        }
    }

    /// A failed write that cannot be cut back leaves a torn record, which
    /// hides every later commit from recovery: the log must be poisoned, with
    /// a reason, at each write site.
    #[test]
    fn a_failed_write_that_cannot_be_cut_back_poisons_the_log() {
        for site in 0..3 {
            let dir = temp_dir();
            let wal = Wal::open(dir.path().join(DEFAULT_WAL_NAME)).unwrap();
            let t = wal.begin();
            if site == 1 {
                wal.log(t, 1, 0, &[2u8; 30]).unwrap();
            }
            test_hooks::SHORT_WAL_WRITE.with(|h| h.set(Some(HEADER_LEN + 2)));
            test_hooks::FAIL_WAL_ROLLBACK.with(|f| f.set(true));
            let failed = match site {
                0 => wal.log(t, 1, 0, &[2u8; 300]).map(|_| ()),
                1 => wal.commit_txn(t),
                _ => wal.log_and_commit(1, 0, &[2u8; 300]),
            };
            test_hooks::SHORT_WAL_WRITE.with(|h| h.set(None));
            test_hooks::FAIL_WAL_ROLLBACK.with(|f| f.set(false));
            assert!(failed.is_err(), "site {site}: the injected failure was swallowed");
            assert!(wal.is_poisoned(), "site {site}: a torn record was left in a live log");
            assert!(
                wal.poison_reason().is_some_and(|r| r.contains("cut back")),
                "site {site}: {:?}",
                wal.poison_reason()
            );
            assert!(wal.log_and_commit(1, 0, b"later").is_err(), "site {site}");
        }
    }

    /// A failed append to a real file that cannot be cut back must poison
    /// the log. `/dev/full` refuses every write with `ENOSPC`, and `ftruncate`
    /// on it fails too.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_failed_log_append_poisons_the_log() {
        let wal = Wal::open("/dev/full").unwrap();
        let txn = wal.begin();
        assert!(wal.log(txn, 1, 0, b"bytes").is_err());
        assert!(wal.is_poisoned(), "a failed log append must poison the log");
        assert!(wal.commit_txn(wal.begin()).is_err());
    }

    /// fsyncgate: a data file that fails its flush at checkpoint must poison
    /// the log, and no later checkpoint may drop it.
    ///
    /// `/dev/null` stands in for the failing file — `fdatasync` on it fails
    /// with `EINVAL` every time, the way a writeback error fails it once.
    /// Re-pointing the id at a healthy file afterwards models what Linux does
    /// next: the retry succeeds, because the dirty pages that failed are gone.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_failed_data_file_flush_at_checkpoint_keeps_the_log() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");
        std::fs::write(&data, b"").unwrap();

        let wal = Wal::open(&wal_path).unwrap();
        let id = file_id_for_name("table.db");
        wal.register_file(id, &data);
        wal.log_and_commit(id, 0, b"committed").unwrap();

        // A checkpoint syncs only the files records name, so the flaky file
        // gets one. Never committed, so replay leaves it out.
        let flaky = file_id_for_name("flaky.db");
        wal.register_file(flaky, "/dev/null");
        wal.log(wal.begin(), flaky, 0, b"flaky").unwrap();
        let logged = std::fs::metadata(&wal_path).unwrap().len();
        assert!(logged > 0);

        assert!(wal.checkpoint().is_err(), "the failed flush must be reported");
        assert!(wal.is_poisoned(), "a failed data-file flush must poison the log");
        assert!(wal.poison_reason().unwrap().contains("/dev/null"));

        wal.register_file(flaky, &data);
        assert!(
            wal.checkpoint().is_err(),
            "a retried checkpoint must not succeed over the failed flush"
        );
        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(),
            logged,
            "the log is the only copy of what the failed flush lost; it must not be dropped"
        );
        assert_eq!(wal.checkpoint_counts().1, 2);
        assert!(wal.last_checkpoint_error().is_some());
        assert!(wal.truncate().is_err(), "a poisoned log was truncated directly");
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), logged);

        let txn = wal.begin();
        wal.log(txn, id, 9, b"later").unwrap();
        assert!(wal.commit_txn(txn).is_err(), "later commits must fail loudly");

        // And the record the data file may have lost is still there to replay.
        drop(wal);
        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied, 1);
        assert_eq!(read_file(&data), b"committed");
    }

    /// A committed in-place write whose apply fails must poison the log.
    ///
    /// The commit marker is durable by then, so the log holds the only correct
    /// copy of the bytes; a checkpoint dropping it would leave the data file
    /// with whatever the failed apply did not write. A directory where the
    /// data file should be makes the apply's `open` fail.
    #[test]
    fn a_failed_in_place_apply_poisons_the_log_and_keeps_it() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let target = dir.path().join("not_a_file");
        std::fs::create_dir(&target).unwrap();

        let wal = Wal::open(&wal_path).unwrap();
        let id = file_id_for_name("not_a_file");
        wal.register_file(id, &target);

        let txn = wal.begin();
        wal.log_in_place(txn, id, 0, b"OVERWRITE").unwrap();
        assert!(wal.commit_txn(txn).is_err(), "the failed apply must be reported");
        assert!(wal.is_poisoned(), "a failed apply after the commit marker must poison the log");
        assert_eq!(wal.pending_bytes(), 0);
        assert_eq!(wal.pending_transactions(), 0);

        let logged = std::fs::metadata(&wal_path).unwrap().len();
        // The directory is replaced by a file a checkpoint could flush, so
        // only the poisoning stands between it and dropping the log.
        std::fs::remove_dir(&target).unwrap();
        std::fs::write(&target, b"").unwrap();
        assert!(wal.checkpoint().is_err());
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), logged);

        drop(wal);
        let stats = Wal::recover(&wal_path, resolver(target.clone())).unwrap();
        assert_eq!(stats.applied, 1);
        assert_eq!(read_file(&target), b"OVERWRITE");
    }

    /// Declines and failures are counted apart: a decline is expected and
    /// transient, a failure is not.
    #[test]
    fn checkpoint_declines_are_counted() {
        let dir = temp_dir();
        let wal = Wal::open(dir.path().join(DEFAULT_WAL_NAME)).unwrap();
        let data = dir.path().join("t.db");
        std::fs::write(&data, b"").unwrap();
        let id = file_id_for_name("t.db");
        wal.register_file(id, &data);
        let txn = wal.begin();
        wal.log_in_place(txn, id, 0, b"held").unwrap();
        assert!(!wal.checkpoint().unwrap(), "a pending overwrite must hold the log");
        assert_eq!(wal.checkpoint_counts(), (1, 0));
        wal.abort_txn(txn);
        assert!(wal.checkpoint().unwrap());
        assert_eq!(wal.checkpoint_counts(), (1, 0));
        assert_eq!(wal.last_checkpoint_error(), None);
    }

    /// A record logged while a checkpoint is syncing the data files may
    /// describe bytes that reached a file after that file's sync. The
    /// checkpoint must keep it in the log and must not report it durable.
    #[test]
    fn a_checkpoint_keeps_records_logged_during_its_sync() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("data.db");
        std::fs::write(&data, vec![0u8; 64]).unwrap();
        let wal = Arc::new(Wal::open(&wal_path).unwrap());
        let id = file_id_for_name("data.db");
        wal.register_file(id, &data);
        wal.log_and_commit(id, 0, b"EARLY").unwrap();

        let late_marker = Arc::new(AtomicU64::new(0));
        {
            let wal = wal.clone();
            let late_marker = late_marker.clone();
            test_hooks::AFTER_CHECKPOINT_SYNC.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    // Logged, its data never written to the file: what a crash
                    // right after the checkpoint would leave.
                    let t = wal.begin();
                    wal.log(t, id, 32, b"LATE").unwrap();
                    late_marker.store(wal.commit_txn_deferred_seq(t).unwrap(), Ordering::SeqCst);
                }));
            });
        }
        assert!(wal.checkpoint().unwrap(), "checkpoint declined");
        test_hooks::AFTER_CHECKPOINT_SYNC.with(|hook| *hook.borrow_mut() = None);

        let marker = late_marker.load(Ordering::SeqCst);
        assert!(marker > 0);
        assert!(
            wal.durable_through() < marker,
            "the checkpoint reported a record durable that it never synced"
        );
        // The log still holds it, and only it: replay restores the bytes.
        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied, 1, "the record logged during the sync pass was discarded");
        assert_eq!(&read_file(&data)[32..36], b"LATE");

        // And the log keeps working on the new file: a sync reaches it.
        wal.commit_through(marker).unwrap();
        wal.log_and_commit(id, 40, b"AFTER").unwrap();
        std::fs::write(&data, vec![0u8; 64]).unwrap();
        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied, 2);
        assert_eq!(&read_file(&data)[40..45], b"AFTER");
    }

    /// Which call sites sync what during a VACUUM FULL of one table among
    /// many, after a restart, with a log well under the checkpoint threshold.
    /// The deployed node's "two full passes" were one checkpoint over every
    /// registered file — and `register_directory` registers each table's
    /// `.tmp` sibling too, doubling the count after any restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn vacuum_full_syncs_only_what_it_wrote() {
        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);
        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
        for i in 0..40 {
            exec_sql(&db, &format!("CREATE TABLE s{i} (id UBIGINT, v VARCHAR);")).await;
            exec_sql(&db, &format!("INSERT INTO s{i} (id, v) VALUES (1, 'a');")).await;
        }
        let values: Vec<String> = (1..=100).map(|i| format!("({i}, 'xxxxxxxx')")).collect();
        exec_sql(&db, &format!("INSERT INTO s0 (id, v) VALUES {};", values.join(", "))).await;
        exec_sql(&db, "DELETE FROM s0 WHERE id > 10;").await;
        drop(db);
        let _ = remove_for(&dir_str);
        let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
        let wal = wal_for(&dir_str).expect("a log");
        // Opening tables can briefly hold an in-place write; wait for a cut.
        let mut cut = false;
        for _ in 0..200 {
            if wal.checkpoint().unwrap() {
                cut = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(cut, "the log could not be checkpointed after reopening");
        let registered = wal.registered_files().len();
        assert!(registered >= 40 * 5, "only {registered} files registered");

        let watch = crate::core::sync_audit::watch(dir.path());
        let out = exec_sql(&db, "VACUUM FULL s0;").await;
        assert_eq!(out[0], 0, "{}", String::from_utf8_lossy(&out[1..]));
        let seen = watch.take();
        let data_syncs: Vec<_> = seen
            .iter()
            .filter(|(tag, _)| *tag == "wal.checkpoint.data_file")
            .collect();
        eprintln!("{} registered; syncs by site: {:?}", registered, {
            let mut c = std::collections::BTreeMap::new();
            for (t, _) in &seen {
                *c.entry(*t).or_insert(0) += 1;
            }
            c
        });
        // Only s0's own files: rows, pointers, filter — never the other
        // tables, and never a `.tmp` sibling.
        assert!(
            data_syncs.len() <= 6,
            "a checkpoint synced {} data files for a one-table VACUUM FULL: {:?}",
            data_syncs.len(),
            data_syncs
        );
        // Background maintenance (each table's vacuum/forget sweep runs on its
        // own timer after a reopen) can legitimately dirty another table in
        // this window, so a stray sync or two is not the regression. The
        // regression is the old full pass over every registered file (800+
        // here), which the bound above already excludes. s0's own files must
        // be among them.
        let foreign: Vec<_> = data_syncs
            .iter()
            .filter(|(_, p)| !p.to_string_lossy().contains("s0"))
            .collect();
        assert!(
            foreign.len() <= 2,
            "VACUUM FULL s0 synced other tables' files: {:?}",
            foreign
        );
        assert!(
            data_syncs.iter().any(|(_, p)| p.to_string_lossy().contains("s0")),
            "VACUUM FULL s0 did not sync s0's own files: {:?}",
            data_syncs
        );
        drop(watch);
        let _ = remove_for(&dir_str);
    }

    /// The VACUUM FULL scenario from the deployed node, small: many tables, a
    /// log well under the checkpoint threshold, VACUUM FULL on one table.
    /// `RASTDB_SYNC_SCENARIO_DIR` places it on a real disk (for strace).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn vacuum_full_sync_scenario() {
        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);
        let base = std::env::var("RASTDB_SYNC_SCENARIO_DIR").unwrap_or_else(|_| "/tmp".into());
        let dir = tempfile::tempdir_in(base).unwrap();
        let dir_str = dir.path().to_string_lossy().to_string();
        let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
        for i in 0..200 {
            exec_sql(&db, &format!("CREATE TABLE t{i} (id UBIGINT, v VARCHAR);")).await;
            exec_sql(&db, &format!("INSERT INTO t{i} (id, v) VALUES (1, 'a');")).await;
        }
        let values: Vec<String> = (1..=200).map(|i| format!("({i}, '{}')", "x".repeat(50))).collect();
        exec_sql(&db, &format!("INSERT INTO t0 (id, v) VALUES {};", values.join(", "))).await;
        exec_sql(&db, "DELETE FROM t0 WHERE id > 20;").await;
        // A server is restarted far more often than it is created: reopen, as
        // the deployed node had been, so `register_directory` runs.
        let db = if std::env::var("RASTDB_SYNC_SCENARIO_REOPEN").is_ok() {
            drop(db);
            let _ = remove_for(&dir_str);
            rclite::Arc::new(crate::core::database::Database::new(&dir_str).await)
        } else {
            db
        };
        let wal = wal_for(&dir_str).expect("a log");
        // Start from a cut log, as on the node.
        assert!(wal.checkpoint().unwrap());
        assert!(wal.bytes_since_checkpoint() < crate::wal_checkpoint_bytes());
        eprintln!(
            "SCENARIO: {} files registered with the log",
            wal.registered_files().len()
        );
        eprintln!("SCENARIO: VACUUM FULL begins");
        let out = exec_sql(&db, "VACUUM FULL t0;").await;
        eprintln!("SCENARIO: VACUUM FULL ends");
        assert_eq!(out[0], 0, "{}", String::from_utf8_lossy(&out[1..]));
        let _ = remove_for(&dir_str);
    }

    /// `VACUUM FULL` while the log cannot be checkpointed used to move every
    /// row, keep both tails, log a warning nobody sees at the production log
    /// level, and report success. It must say so, and count it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn vacuum_full_says_when_it_could_not_return_space() {
        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);
        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
        exec_sql(&db, "CREATE TABLE t (id UBIGINT, v VARCHAR);").await;
        let values: Vec<String> = (1..=200).map(|i| format!("({i}, '{}')", "x".repeat(50))).collect();
        exec_sql(&db, &format!("INSERT INTO t (id, v) VALUES {};", values.join(", "))).await;
        exec_sql(&db, "DELETE FROM t WHERE id > 20;").await;

        // An open transaction holding an in-place write: every checkpoint
        // declines while it is there.
        let wal = wal_for(&dir_str).expect("a log");
        let scratch = dir.path().join("scratch.db");
        std::fs::write(&scratch, vec![0u8; 16]).unwrap();
        let scratch_id = file_id_for_name("scratch.db");
        wal.register_file(scratch_id, &scratch);
        let pending = wal.begin();
        wal.log_in_place(pending, scratch_id, 0, b"held").unwrap();

        // It succeeds, as PostgreSQL's does, and says why with a WARNING the
        // wire layer sends ahead of the completion.
        let (out, notices) = crate::core::notice::collect(async {
            let out = exec_sql(&db, "VACUUM FULL t;").await;
            (out, crate::core::notice::drain())
        })
        .await;
        assert_eq!(out[0], 0, "VACUUM FULL failed: {}", String::from_utf8_lossy(&out[1..]));
        assert_eq!(notices.len(), 1, "no warning that the space was not returned");
        assert_eq!((notices[0].severity, notices[0].sqlstate), ("WARNING", "01000"));
        assert!(notices[0].message.contains("could not return the space"), "{:?}", notices[0]);
        let table = db.tables.get("t").unwrap().value().clone();
        assert_eq!(table.vacuum_stats().full_untruncated.load(Ordering::Relaxed), 1);
        // The data is intact.
        let rows = exec_sql(&db, "SELECT id FROM t WHERE id > 0").await;
        assert_eq!(crate::core::row::row::vec_into_rows(&rows[1..]).unwrap().len(), 20);

        // Once the log can be cut, the same statement returns the space.
        wal.commit_txn(pending).unwrap();
        let notices = crate::core::notice::collect(async {
            let out = exec_sql(&db, "VACUUM FULL t;").await;
            assert_eq!(out[0], 0, "{}", String::from_utf8_lossy(&out[1..]));
            crate::core::notice::drain()
        })
        .await;
        assert!(notices.is_empty(), "{notices:?}");
        assert_eq!(table.vacuum_stats().full_untruncated.load(Ordering::Relaxed), 1);
    }

    /// A checkpoint syncs the files the log's records describe, not every
    /// registered file: at 2,042 tables that was ~4,700 device flushes a pass.
    #[test]
    fn a_checkpoint_syncs_only_the_files_written() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let wal = Wal::open(&wal_path).unwrap();
        let mut ids = Vec::new();
        for i in 0..50 {
            let name = format!("t{i}.db");
            let path = dir.path().join(&name);
            std::fs::write(&path, vec![0u8; 16]).unwrap();
            let id = file_id_for_name(&name);
            wal.register_file(id, &path);
            ids.push((id, path));
        }
        let syncs = || test_hooks::CHECKPOINT_SYNCS.with(|n| n.replace(0));
        syncs();

        wal.log_and_commit(ids[7].0, 0, b"one").unwrap();
        wal.log_and_commit(ids[7].0, 3, b"two").unwrap();
        assert!(wal.checkpoint().unwrap());
        assert_eq!(syncs(), 1, "one file written, so one file synced");

        assert!(wal.checkpoint().unwrap());
        assert_eq!(syncs(), 0, "nothing written since the last checkpoint");

        // A declined checkpoint keeps the files owed for the next one.
        wal.log_and_commit(ids[3].0, 0, b"x").unwrap();
        let held = wal.begin();
        wal.log_in_place(held, ids[9].0, 0, b"held").unwrap();
        assert!(!wal.checkpoint().unwrap());
        wal.commit_txn(held).unwrap();
        syncs();
        assert!(wal.checkpoint().unwrap());
        assert_eq!(syncs(), 2, "the declined checkpoint's files were forgotten");

        // A log opened with records this process did not write syncs the files
        // those records name — read from the log — and not every registered
        // file: a reopened directory registers each table's `.tmp` sibling
        // too, which made this one checkpoint cost two passes.
        wal.log_and_commit(ids[1].0, 0, b"left").unwrap();
        let held = wal.begin();
        wal.log(held, ids[2].0, 0, b"uncommitted").unwrap();
        drop(wal);
        let reopened = Wal::open(&wal_path).unwrap();
        for (i, (id, path)) in ids.iter().enumerate() {
            reopened.register_file(*id, path);
            // What `register_directory` finds beside every table file.
            let tmp = dir.path().join(format!("t{i}.db.tmp"));
            std::fs::write(&tmp, b"staging").unwrap();
            reopened.register_file(file_id_for_name(&format!("t{i}.db.tmp")), tmp);
        }
        syncs();
        assert!(reopened.checkpoint().unwrap());
        assert_eq!(
            syncs(),
            2,
            "the files the reopened log's records name, committed or not, and no other"
        );

        // The read stops where replay does, at a torn tail.
        reopened.log_and_commit(ids[4].0, 0, b"left").unwrap();
        drop(reopened);
        let mut garbage = std::fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
        garbage.write_all(b"torn tail").unwrap();
        drop(garbage);
        let torn = Wal::open(&wal_path).unwrap();
        for (id, path) in &ids {
            torn.register_file(*id, path);
        }
        syncs();
        assert!(torn.checkpoint().unwrap());
        assert_eq!(syncs(), 1, "a torn tail names nothing replay would apply");

        // Truncation, as recovery does, leaves nothing owed.
        torn.log_and_commit(ids[5].0, 0, b"left").unwrap();
        drop(torn);
        let recovered = Wal::open(&wal_path).unwrap();
        for (id, path) in &ids {
            recovered.register_file(*id, path);
        }
        recovered.truncate().unwrap();
        syncs();
        assert!(recovered.checkpoint().unwrap());
        assert_eq!(syncs(), 0, "an empty log owes no file a sync");
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// checkpoint_file_count_bench --nocapture`, with RASTDB_BENCH_DIR on a
    /// real disk. A checkpoint of 2,000 registered files with one written.
    #[test]
    #[ignore]
    fn checkpoint_file_count_bench() {
        let base = std::env::var("RASTDB_BENCH_DIR").unwrap_or_else(|_| "/var/tmp".to_string());
        let dir = tempfile::tempdir_in(base).unwrap();
        let wal = Wal::open(dir.path().join(DEFAULT_WAL_NAME)).unwrap();
        let mut first = 0;
        for i in 0..2_000 {
            let name = format!("t{i}.db");
            let path = dir.path().join(&name);
            std::fs::write(&path, vec![1u8; 4096]).unwrap();
            let id = file_id_for_name(&name);
            if i == 0 {
                first = id;
            }
            wal.register_file(id, &path);
        }
        // Everything clean on disk before timing.
        for i in 0..2_000 {
            File::open(dir.path().join(format!("t{i}.db"))).unwrap().sync_all().unwrap();
        }
        for round in 0..3 {
            // What every checkpoint used to do first: sync each registered file.
            let t = std::time::Instant::now();
            for (_, path) in wal.registered_files() {
                File::options().read(true).write(true).open(&path).unwrap().sync_data().unwrap();
            }
            let old_pass = t.elapsed();
            wal.log_and_commit(first, 0, b"dirty").unwrap();
            let t = std::time::Instant::now();
            assert!(wal.checkpoint().unwrap());
            println!(
                "round {round}: 2000 files, 1 dirty: old sync pass alone {old_pass:?}, \
                 whole checkpoint now {:?}",
                t.elapsed()
            );
        }
    }

    #[test]
    fn a_long_transaction_is_broken_in_batches_not_per_row() {
        let dir = temp_dir();
        let wal = Wal::open(dir.path().join(DEFAULT_WAL_NAME)).unwrap();
        let threshold = 64 * 1024;
        let record = vec![0u8; 100];
        let per_record = (HEADER_LEN + record.len()) as u64;
        // Past the threshold and staying there, as with declined checkpoints.
        while wal.bytes_since_checkpoint() < threshold {
            let t = wal.begin();
            wal.log(t, 1, 0, &record).unwrap();
        }
        let mut grants = 0;
        let rows = 2_000u64;
        for _ in 0..rows {
            let t = wal.begin();
            wal.log(t, 1, 0, &record).unwrap();
            grants += u64::from(wal.grant_break(threshold));
        }
        // One per eighth of the threshold logged, give or take the first.
        let expected = rows * per_record / (threshold / 8);
        assert!(
            grants <= expected + 2,
            "{grants} breaks for {rows} rows: committing per row, not in batches"
        );
        assert!(grants >= 1, "never told to commit");
        // A checkpoint resets the counter, and the next pass is granted again.
        assert!(wal.checkpoint().unwrap());
        while wal.bytes_since_checkpoint() < threshold {
            let t = wal.begin();
            wal.log(t, 1, 0, &record).unwrap();
        }
        assert!(wal.grant_break(threshold));
    }

    #[test]
    fn a_combined_record_and_marker_tears_cleanly_at_every_byte() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_and_commit(1, 0, b"AAAA").unwrap();
            let seq = wal.log_and_commit_deferred(1, 4, b"BBBB").unwrap();
            // Two records went out: the data record and its marker.
            assert_eq!(seq, 4);
            wal.sync_pending().unwrap();
            assert_eq!(wal.durable_through(), 4);
        }
        let full = read_file(&wal_path);
        let frame = 2 * HEADER_LEN + 4;
        let start = full.len() - frame;
        // The frame is one record and one marker, in that order.
        assert_eq!(full[start + 5], 0);
        assert_eq!(full[start + HEADER_LEN + 4 + 5], FLAG_COMMIT);

        // Every length a crash could leave of the second write: it commits
        // only once the whole frame is there, and never applies a torn record.
        for cut in start..=full.len() {
            std::fs::write(&wal_path, &full[..cut]).unwrap();
            let _ = std::fs::remove_file(&data);
            let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
            if cut == full.len() {
                assert_eq!(stats.applied, 2, "cut {cut}");
                assert_eq!(read_file(&data), b"AAAABBBB");
            } else {
                assert_eq!(stats.applied, 1, "cut {cut}: a torn frame was applied");
                assert_eq!(read_file(&data), b"AAAA", "cut {cut}");
            }
        }
    }

    #[test]
    fn stops_at_a_torn_tail_and_keeps_the_prefix() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");

        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_and_commit(1, 0, b"AAAA").unwrap();
            wal.log_and_commit(1, 4, b"BBBB").unwrap();
        }

        // A third record cut off mid-header, as a crash would leave it.
        let mut raw = read_file(&wal_path);
        let intact = raw.len();
        raw.extend_from_slice(&RECORD_MAGIC.to_le_bytes());
        raw.push(FORMAT_VERSION);
        raw.push(FLAG_COMMIT);
        std::fs::write(&wal_path, &raw).unwrap();

        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied, 2, "the intact prefix must still apply");
        assert_eq!(stats.truncated_tail, (raw.len() - intact) as u64);
        assert_eq!(read_file(&data), b"AAAABBBB");
    }

    #[test]
    fn stops_at_a_corrupt_record() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");

        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_and_commit(1, 0, b"GOOD").unwrap();
            wal.log_and_commit(1, 4, b"BAD!").unwrap();
        }

        // Layout now: [data "GOOD"][marker][data "BAD!"][marker]. Damage the
        // payload of the second data record.
        let mut raw = read_file(&wal_path);
        let second_payload = (HEADER_LEN + 4) + HEADER_LEN + HEADER_LEN;
        raw[second_payload] ^= 0xFF;
        std::fs::write(&wal_path, &raw).unwrap();

        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied, 1);
        assert_eq!(read_file(&data), b"GOOD");
    }

    /// A log written by a build with a different record layout must be refused,
    /// not misread as data.
    #[test]
    fn refuses_an_unknown_format_version() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");

        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_and_commit(1, 0, b"XXXX").unwrap();
        }

        let mut raw = read_file(&wal_path);
        raw[4] = FORMAT_VERSION.wrapping_add(1);
        std::fs::write(&wal_path, &raw).unwrap();

        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied, 0);
        assert!(read_file(&data).is_empty());
    }

    #[test]
    fn unresolved_files_are_counted_not_fatal() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);

        let wal = Wal::open(&wal_path).unwrap();
        wal.log_and_commit(42, 0, b"orphan").unwrap();

        let stats = Wal::recover(&wal_path, |_| None).unwrap();
        assert_eq!(stats.applied, 0);
        assert_eq!(stats.unresolved, 1);
    }

    #[test]
    fn reopening_appends_after_existing_records() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");

        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_and_commit(1, 0, b"first").unwrap();
        }
        {
            let wal = Wal::open(&wal_path).unwrap();
            wal.log_and_commit(1, 5, b"second").unwrap();
        }

        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied, 2);
        assert_eq!(read_file(&data), b"firstsecond");
    }

    #[test]
    fn checkpoint_flushes_files_then_clears_the_log() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");
        std::fs::write(&data, b"").unwrap();

        let wal = Wal::open(&wal_path).unwrap();
        wal.register_file(file_id_for_name("table.db"), &data);
        wal.log_and_commit(file_id_for_name("table.db"), 0, b"payload")
            .unwrap();
        assert!(wal.bytes_since_checkpoint() > 0);

        wal.checkpoint().unwrap();
        assert_eq!(wal.bytes_since_checkpoint(), 0);

        let stats = Wal::recover(&wal_path, |_| None).unwrap();
        assert_eq!(stats.applied, 0);
    }

    /// A pending overwrite points into the log for its payload, so a checkpoint
    /// must not truncate the log while a transaction is still open.
    #[test]
    fn checkpoint_defers_while_a_transaction_is_open() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");
        std::fs::write(&data, b"OLDOLDOLD").unwrap();

        let wal = Wal::open(&wal_path).unwrap();
        let id = file_id_for_name("table.db");
        wal.register_file(id, &data);

        let txn = wal.begin();
        wal.log_in_place(txn, id, 0, b"NEW").unwrap();

        // Would otherwise pull the payload out from under the open transaction.
        wal.checkpoint().unwrap();
        assert!(
            wal.bytes_since_checkpoint() > 0,
            "the log must not be discarded while a transaction is open"
        );

        // The transaction can still find its bytes and finish.
        wal.commit_txn(txn).unwrap();
        assert_eq!(read_file(&data), b"NEWOLDOLD");

        // With nothing outstanding, a checkpoint now proceeds.
        wal.checkpoint().unwrap();
        assert_eq!(wal.bytes_since_checkpoint(), 0);
    }

    /// An in-place write must create its target if it does not exist yet.
    ///
    /// With a WAL enabled, `write_data_txn` deliberately leaves the data file
    /// untouched — the bytes are applied by `commit_txn` once the record is
    /// durable. That apply opened the target without `create`, so the *first*
    /// write to a file that did not exist yet failed with `NotFound`, and
    /// `write_data` has no channel to report it on. The write vanished.
    ///
    /// A table's filter sidecar is created by its first checkpoint, which is an
    /// in-place write at offset 0 — so no filter checkpoint ever reached disk
    /// while a WAL was on. Recovery replaying the identical record *did* create
    /// the file, so a crash produced a more correct result than a clean run.
    #[test]
    fn an_in_place_write_creates_a_file_that_does_not_exist_yet() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("sidecar.db");
        assert!(
            !data.exists(),
            "the point of this test is that it is missing"
        );

        let wal = Wal::open(&wal_path).unwrap();
        let id = file_id_for_name("sidecar.db");
        wal.register_file(id, &data);

        let txn = wal.begin();
        wal.log_in_place(txn, id, 0, b"HELLO").unwrap();
        wal.commit_txn(txn)
            .expect("commit must create the target rather than fail on NotFound");

        assert_eq!(read_file(&data), b"HELLO");

        // And a second write to the now-existing file still lands.
        let txn = wal.begin();
        wal.log_in_place(txn, id, 5, b"THERE").unwrap();
        wal.commit_txn(txn).unwrap();
        assert_eq!(read_file(&data), b"HELLOTHERE");
    }

    /// An in-place overwrite must still find its payload after a checkpoint.
    ///
    /// Single-threaded and deterministic: this is not a race. `truncate_locked`
    /// reset the log file but left `write_pos` at its pre-truncation value, so
    /// the next record was framed at offset 0 while reporting a `log_offset`
    /// of wherever the log happened to have reached before. Commit then seeked
    /// past EOF and failed with "failed to fill whole buffer".
    ///
    /// The existing checkpoint tests all finish their transactions *before*
    /// checkpointing, so none of them wrote a pending overwrite on the far side
    /// of a truncation — which is exactly the order a long-running server hits
    /// constantly and a test suite never did.
    #[test]
    fn in_place_writes_survive_a_preceding_checkpoint() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");
        std::fs::write(&data, b"OLDOLDOLD").unwrap();

        let wal = Wal::open(&wal_path).unwrap();
        let id = file_id_for_name("table.db");
        wal.register_file(id, &data);

        // Push the log out to a non-trivial length, so a stale `write_pos`
        // lands well past the end of the truncated file rather than close
        // enough to it to read something by luck.
        for _ in 0..64 {
            let txn = wal.begin();
            wal.log(txn, id, 0, &[b'x'; 128]).unwrap();
            wal.commit_txn(txn).unwrap();
        }
        assert!(wal.bytes_since_checkpoint() > 0);

        // Nothing outstanding, so this really does truncate.
        wal.checkpoint().unwrap();
        assert_eq!(wal.bytes_since_checkpoint(), 0);

        // The first in-place write after the truncation.
        let txn = wal.begin();
        wal.log_in_place(txn, id, 0, b"NEW").unwrap();
        wal.commit_txn(txn)
            .expect("commit must find its payload in the truncated log");

        assert_eq!(read_file(&data), b"NEWOLDOLD");

        // And again, to confirm the append point kept advancing correctly
        // rather than happening to work once.
        let txn = wal.begin();
        wal.log_in_place(txn, id, 3, b"TWO").unwrap();
        wal.commit_txn(txn).unwrap();
        assert_eq!(read_file(&data), b"NEWTWOOLD");
    }

    /// The concurrent form of the test above, which the sequential one cannot
    /// catch.
    ///
    /// `checkpoint` truncates the log, and a pending overwrite holds an offset
    /// *into* the log that it reads its payload back from at commit. The guard
    /// against truncating underneath it is `pending_count`, so registering a
    /// pending write and appending the record it points at have to be one
    /// atomic step against a concurrent checkpoint.
    ///
    /// When they were not, this was the failure a live server hit under
    /// `--concurrent-threads 16`: a checkpoint read `pending_count` as zero,
    /// took the writer lock and truncated between a record being framed and the
    /// transaction registering its dependency on it, leaving the offset past
    /// EOF and the commit reporting `failed to fill whole buffer`.
    ///
    /// The window is a few instructions wide, so this hammers it rather than
    /// trying to hit it once.
    #[test]
    fn checkpoints_do_not_truncate_under_concurrent_in_place_writes() {
        use std::sync::Arc as StdArc;
        use std::sync::atomic::AtomicUsize;

        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);

        const WRITERS: usize = 8;
        const ROUNDS: usize = 200;

        // One file per writer: overlapping offsets would make the final
        // contents depend on thread interleaving rather than on correctness.
        let wal = StdArc::new(Wal::open(&wal_path).unwrap());
        let mut ids = Vec::new();
        for w in 0..WRITERS {
            let name = format!("table{w}.db");
            let path = dir.path().join(&name);
            std::fs::write(&path, vec![b'.'; 8]).unwrap();
            let id = file_id_for_name(&name);
            wal.register_file(id, &path);
            ids.push((id, path));
        }

        let stop = StdArc::new(AtomicBool::new(false));
        let failures = StdArc::new(AtomicUsize::new(0));

        let checkpointer = {
            let wal = StdArc::clone(&wal);
            let stop = StdArc::clone(&stop);
            let failures = StdArc::clone(&failures);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if wal.checkpoint().is_err() {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        };

        let writers: Vec<_> = ids
            .iter()
            .cloned()
            .map(|(id, _)| {
                let wal = StdArc::clone(&wal);
                let failures = StdArc::clone(&failures);
                std::thread::spawn(move || {
                    for r in 0..ROUNDS {
                        let txn = wal.begin();
                        let byte = b'a' + (r % 26) as u8;
                        if wal.log_in_place(txn, id, 0, &[byte; 4]).is_err() {
                            failures.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        // The commit is where a truncated-out payload surfaces,
                        // as a short read rather than as corruption.
                        if wal.commit_txn(txn).is_err() {
                            failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();

        for w in writers {
            w.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        checkpointer.join().unwrap();

        assert_eq!(
            failures.load(Ordering::Relaxed),
            0,
            "a checkpoint truncated the log out from under an open transaction"
        );

        // Every committed overwrite must actually be in its data file: a commit
        // that silently applied nothing would pass the check above.
        let last = b'a' + ((ROUNDS - 1) % 26) as u8;
        for (_, path) in &ids {
            assert_eq!(
                &read_file(path)[0..4],
                &[last; 4],
                "committed overwrite never reached {}",
                path.display()
            );
        }
    }

    /// A finer power-loss model than "everything since the snapshot is gone".
    ///
    /// Real hardware loses writes at page granularity and in no particular
    /// order, so this restores a pseudo-random subset of 4 KiB pages from the
    /// snapshot and leaves the rest as the process wrote them. Recovery has to
    /// produce the same answer whichever pages happened to survive.
    #[tokio::test(flavor = "multi_thread")]
    async fn committed_work_survives_partial_page_loss() {
        use crate::core::row::row::vec_into_rows;

        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);
        let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);

        // Several seeds, so one lucky page layout cannot make this pass.
        for seed in [1u64, 7, 13, 29] {
            let dir = temp_dir();
            let dir_str = dir.path().to_string_lossy().to_string();
            let snap = temp_dir();

            {
                let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
                let _ = exec_sql(&db, "CREATE TABLE torn (id UBIGINT, payload VARCHAR);").await;
                for id in 1..=4u64 {
                    let sql = format!(
                        "INSERT INTO torn (id, payload) VALUES ({}, '{}');",
                        id,
                        payload_for(id)
                    );
                    let _ = exec_sql(&db, &sql).await;
                }
                if let Some(w) = wal_for(&dir_str) {
                    w.checkpoint().unwrap();
                }
            }

            snapshot_data_files(dir.path(), snap.path());

            {
                let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
                for id in 5..=24u64 {
                    let sql = format!(
                        "INSERT INTO torn (id, payload) VALUES ({}, '{}');",
                        id,
                        payload_for(id)
                    );
                    let _ = exec_sql(&db, &sql).await;
                }
            }

            restore_random_pages(snap.path(), dir.path(), seed);

            let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
            let res = exec_sql(&db, "SELECT id, payload FROM torn WHERE id > 0").await;
            assert_eq!(res.first(), Some(&2u8), "seed {}: expected rows", seed);
            let rows = vec_into_rows(&res[1..]).expect("decode rows");

            let table = db.tables.get("torn").expect("table").clone();
            let schema = &table.schema.fields;

            let mut seen: Vec<u64> = Vec::new();
            for row in rows {
                let id = u64::from_le_bytes(
                    row.get_column("id", schema)
                        .expect("id")
                        .into_slice()
                        .try_into()
                        .expect("u64"),
                );
                let payload = String::from_utf8_lossy(
                    row.get_column("payload", schema)
                        .expect("payload")
                        .into_slice(),
                )
                .to_string();
                assert_eq!(
                    payload,
                    payload_for(id),
                    "seed {}: row {} came back corrupted",
                    seed,
                    id
                );
                seen.push(id);
            }
            seen.sort_unstable();

            assert_eq!(
                seen,
                (1..=24u64).collect::<Vec<_>>(),
                "seed {}: every committed row must survive an arbitrary subset \
                 of pages being lost",
                seed
            );
        }
    }

    /// The commit sequence must be monotonic and survive a restart, or it is
    /// useless as a point-in-time token.
    #[test]
    fn commit_lsn_is_monotonic_and_resumes_after_restart() {
        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        let data = dir.path().join("table.db");
        std::fs::write(&data, b"").unwrap();

        let before;
        {
            let wal = Wal::open(dir.path().join(DEFAULT_WAL_NAME)).unwrap();
            let id = file_id_for_name("table.db");
            wal.register_file(id, &data);

            assert_eq!(wal.commit_lsn(), 0);
            wal.log_and_commit(id, 0, b"AAAA").unwrap();
            let first = wal.commit_lsn();
            wal.log_and_commit(id, 4, b"BBBB").unwrap();
            let second = wal.commit_lsn();

            assert!(second > first, "each commit must advance the sequence");
            before = second;
        }

        // Reopening replays the log and picks the sequence back up.
        let (wal, stats) = open_and_recover(&dir_str).unwrap();
        assert_eq!(stats.last_commit_lsn, before);
        assert!(
            wal.commit_lsn() >= before,
            "the sequence must not restart from zero after recovery"
        );

        let id = file_id_for_name("table.db");
        wal.register_file(id, &data);
        wal.log_and_commit(id, 8, b"CCCC").unwrap();
        assert!(wal.commit_lsn() > before);
    }

    /// The case the test above does not reach: a *clean* stop, where the log
    /// was checkpointed away and there is nothing left to recover a sequence
    /// from. This is the ordinary state of a server that shuts down tidily, and
    /// a counter that restarted here reported `start_lsn: 0` on every snapshot
    /// a backup took afterwards — which made a rollover keep every archived
    /// bundle it was meant to remove, and would have had a restore replay
    /// bundles from an earlier run over the copy.
    #[test]
    fn commit_lsn_resumes_after_a_checkpoint_emptied_the_log() {
        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        let data = dir.path().join("table.db");
        std::fs::write(&data, b"").unwrap();
        let id = file_id_for_name("table.db");

        let before;
        {
            let wal = Wal::open(dir.path().join(DEFAULT_WAL_NAME)).unwrap();
            wal.register_file(id, &data);
            wal.log_and_commit(id, 0, b"AAAA").unwrap();
            wal.log_and_commit(id, 4, b"BBBB").unwrap();
            before = wal.commit_lsn();
            assert!(before > 0);

            assert!(
                wal.checkpoint().unwrap(),
                "the log should have been discarded"
            );
            assert_eq!(
                std::fs::metadata(dir.path().join(DEFAULT_WAL_NAME))
                    .unwrap()
                    .len(),
                0,
                "nothing is left in the log to recover the sequence from"
            );
        }

        let (wal, stats) = open_and_recover(&dir_str).unwrap();
        assert_eq!(
            stats.last_commit_lsn, 0,
            "an empty log recovers no sequence"
        );
        assert_eq!(
            wal.commit_lsn(),
            before,
            "the sequence must resume past everything already issued"
        );

        wal.register_file(id, &data);
        wal.log_and_commit(id, 8, b"CCCC").unwrap();
        assert!(wal.commit_lsn() > before, "and keep going up from there");
    }

    #[test]
    fn an_unreadable_lsn_mark_does_not_stop_the_log_opening() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        std::fs::write(lsn_mark_path(&wal_path), b"not a number").unwrap();

        let wal = Wal::open(&wal_path).unwrap();
        assert_eq!(wal.commit_lsn(), 0);
    }

    #[test]
    fn concurrent_writers_all_end_up_durable() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");

        let wal = Arc::new(Wal::open(&wal_path).unwrap());
        const WRITERS: usize = 8;
        const PER_WRITER: usize = 64;

        let mut handles = Vec::new();
        for w in 0..WRITERS {
            let wal = wal.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..PER_WRITER {
                    let slot = (w * PER_WRITER + i) as u64;
                    wal.log_and_commit(1, slot, &[b'a' + w as u8]).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied as usize, WRITERS * PER_WRITER);
        assert_eq!(read_file(&data).len(), WRITERS * PER_WRITER);
        assert!(wal.durable.load(Ordering::Acquire) >= (WRITERS * PER_WRITER) as u64);
    }

    /// Ids must not depend on where the database directory lives, or a restored
    /// backup would replay into nothing.
    #[test]
    fn file_ids_are_independent_of_the_directory() {
        assert_eq!(file_id_for_name("rows.db"), file_id_for_name("rows.db"));
        assert_ne!(file_id_for_name("rows.db"), file_id_for_name("ptrs.db"));
    }

    /// A directory path with a trailing separator used to make the provider
    /// compute an empty location, so it tried to create files at the filesystem
    /// root and panicked.
    #[tokio::test]
    async fn trailing_separator_in_the_location_is_handled() {
        use crate::core::storage_providers::file_sync::LocalStorageProvider;

        let dir = temp_dir();
        let with_slash = format!("{}/", dir.path().to_string_lossy());

        let _p = LocalStorageProvider::new(&with_slash, Some("t.db")).await;

        assert!(
            dir.path().join("t.db").exists(),
            "the file belongs in the requested directory"
        );
    }

    #[test]
    fn dir_key_ignores_a_trailing_separator() {
        assert_eq!(dir_key("/data/db/"), dir_key("/data/db"));
        assert_eq!(dir_key("/data/db//"), "/data/db");
    }

    /// A failed flush must not be papered over: once durability cannot be
    /// promised, later commits report it instead of returning success.
    #[test]
    fn poisoned_log_refuses_further_commits() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);

        let wal = Wal::open(&wal_path).unwrap();
        wal.log_and_commit(1, 0, b"ok").unwrap();

        wal.poisoned.store(true, Ordering::Release);
        let txn = wal.begin();
        wal.log(txn, 1, 2, b"more").unwrap();
        assert!(wal.commit_txn(txn).is_err());
        assert!(wal.is_poisoned());
    }

    /// `block_in_place` panics on a current-thread runtime; the commit path
    /// runs under every flavour (plain `#[tokio::test]` is current-thread), so
    /// `blocking_io` must be callable from each without panicking.
    #[test]
    fn blocking_io_runs_under_every_runtime_flavour() {
        assert_eq!(blocking_io(|| 1), 1);
        let current = tokio::runtime::Builder::new_current_thread().build().unwrap();
        assert_eq!(current.block_on(async { blocking_io(|| 2) }), 2);
        let multi = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        assert_eq!(multi.block_on(async { blocking_io(|| 3) }), 3);
        let on_worker = multi.block_on(async { tokio::spawn(async { blocking_io(|| 4) }).await });
        assert_eq!(on_worker.unwrap(), 4);
        let on_blocking =
            multi.block_on(async { tokio::task::spawn_blocking(|| blocking_io(|| 5)).await });
        assert_eq!(on_blocking.unwrap(), 5);
        // And a commit really syncs through it on a worker.
        let dir = temp_dir();
        let wal = Arc::new(Wal::open(dir.path().join(DEFAULT_WAL_NAME)).unwrap());
        let w = wal.clone();
        multi
            .block_on(async move { tokio::spawn(async move { w.log_and_commit(1, 0, b"x") }).await })
            .unwrap()
            .unwrap();
        assert_eq!(wal.durable_through(), 2);
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// wal_for_bench --nocapture`. The registry lookup every storage write
    /// makes several of, against the mutex + `String` key it replaced, from
    /// 1 and 8 threads.
    #[test]
    #[ignore]
    fn wal_for_bench() {
        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        let wal = Arc::new(Wal::open(dir.path().join(DEFAULT_WAL_NAME)).unwrap());
        install_for(&dir_str, wal.clone());
        // The old registry, kept here as the reference.
        let old: Arc<Mutex<HashMap<String, Arc<Wal>>>> = Arc::new(Mutex::new(HashMap::new()));
        old.lock().insert(dir_key(&dir_str), wal);
        const N: usize = 2_000_000;
        for threads in [1usize, 8] {
            for round in 0..3 {
                let t = std::time::Instant::now();
                std::thread::scope(|s| {
                    for _ in 0..threads {
                        s.spawn(|| {
                            for _ in 0..N {
                                std::hint::black_box(
                                    old.lock().get(&dir_key(std::hint::black_box(&dir_str))).cloned(),
                                );
                            }
                        });
                    }
                });
                let o = t.elapsed();
                let t = std::time::Instant::now();
                std::thread::scope(|s| {
                    for _ in 0..threads {
                        s.spawn(|| {
                            for _ in 0..N {
                                std::hint::black_box(wal_for(std::hint::black_box(&dir_str)));
                            }
                        });
                    }
                });
                let n = t.elapsed();
                let t = std::time::Instant::now();
                std::thread::scope(|s| {
                    for _ in 0..threads {
                        s.spawn(|| {
                            for _ in 0..N {
                                with_wal_for(std::hint::black_box(&dir_str), |w| {
                                    std::hint::black_box(w.map(|w| w.is_poisoned()))
                                });
                            }
                        });
                    }
                });
                let b = t.elapsed();
                println!(
                    "threads {threads} round {round}: old {:.1}  wal_for {:.1}  with_wal_for {:.1} ns/lookup",
                    o.as_nanos() as f64 / N as f64,
                    n.as_nanos() as f64 / N as f64,
                    b.as_nanos() as f64 / N as f64
                );
            }
        }
        let _ = remove_for(&dir_str);
    }

    /// `DROP DATABASE` then `CREATE DATABASE` of the same name: the second
    /// database must log to its own, visible log. Resolving to the dropped
    /// one's (an unlinked file) would report every write durable while a crash
    /// replayed nothing — durability silently gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_recreated_database_logs_to_its_new_log() {
        use crate::core::database::catalog::DatabaseCatalog;
        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);

        let root = temp_dir();
        let root_str = root.path().to_string_lossy().to_string();
        let path = root.path().join("d");
        let path_str = path.to_string_lossy().to_string();
        let catalog = DatabaseCatalog::open(Some(&root_str), "main").await.unwrap();

        let first = catalog.create("d").await.unwrap();
        let old = wal_for(&path_str).expect("a created database has a log");
        exec_sql(&first, "CREATE TABLE t (id UBIGINT, v VARCHAR);").await;
        exec_sql(&first, "INSERT INTO t (id, v) VALUES (1, 'old'), (2, 'old');").await;
        drop(first);
        catalog.drop_database("d", "main").await.unwrap();
        assert!(wal_for(&path_str).is_none(), "a dropped database still resolves to a log");
        let old_bytes = old.bytes_since_checkpoint();

        let second = catalog.create("d").await.unwrap();
        let new = wal_for(&path_str).expect("the recreated database has a log");
        assert!(!Arc::ptr_eq(&old, &new), "the recreated database resolved to the dropped log");
        // Trailing separators resolve to the same, new, log.
        assert!(Arc::ptr_eq(&new, &wal_for(&format!("{path_str}/")).unwrap()));

        let log_file = path.join(DEFAULT_WAL_NAME);
        let before = std::fs::metadata(&log_file).map(|m| m.len()).unwrap_or(0);
        exec_sql(&second, "CREATE TABLE t (id UBIGINT, v VARCHAR);").await;
        let out = exec_sql(&second, "INSERT INTO t (id, v) VALUES (7, 'new');").await;
        assert_eq!(out.first(), Some(&0u8), "{:?}", String::from_utf8_lossy(&out));
        let after = std::fs::metadata(&log_file).unwrap().len();
        assert!(after > before, "the write never reached the visible log ({before} -> {after})");
        assert_eq!(old.bytes_since_checkpoint(), old_bytes, "a write went to the dropped log");
        drop(second);
        drop(catalog);

        // And what a restart replays is the new database's rows only.
        let reopened = rclite::Arc::new(crate::core::database::Database::new(&path_str).await);
        let res = exec_sql(&reopened, "SELECT id FROM t WHERE id > 0").await;
        assert_eq!(res.first(), Some(&2u8));
        let rows = crate::core::row::row::vec_into_rows(&res[1..]).unwrap();
        assert_eq!(rows.len(), 1, "expected only the recreated database's row");
        let _ = remove_for(&path_str);
    }

    /// A log must not outlive the directory it describes.
    ///
    /// `DROP DATABASE` unlinks the directory, and the registry used to keep the
    /// `Arc<Wal>` — and its open handle to the now-unlinked log — for the life
    /// of the process. Nothing removed entries at all.
    #[test]
    fn a_removed_directory_stops_resolving_to_its_log() {
        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        let wal = Arc::new(Wal::open(&dir.path().join(DEFAULT_WAL_NAME)).unwrap());
        install_for(&dir_str, wal);
        assert!(wal_for(&dir_str).is_some());

        // Returned rather than dropped: a caller mid-commit keeps working.
        let taken = remove_for(&dir_str);
        assert!(taken.is_some(), "removal must hand back the log it forgot");
        assert!(
            wal_for(&dir_str).is_none(),
            "a forgotten directory must not resolve to a stale log"
        );

        // Idempotent, so a second drop is not an error.
        assert!(remove_for(&dir_str).is_none());
    }

    #[test]
    fn open_and_recover_restores_lost_writes() {
        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        let data = dir.path().join("table.db");

        // A data file that lost its tail, as if the process died before the
        // pages reached the device.
        std::fs::write(&data, b"AAAA").unwrap();

        {
            let wal = Wal::open(dir.path().join(DEFAULT_WAL_NAME)).unwrap();
            let id = file_id_for_name("table.db");
            wal.log_and_commit(id, 4, b"BBBB").unwrap();
            wal.log_and_commit(id, 8, b"CCCC").unwrap();
        }

        let (_wal, stats) = open_and_recover(&dir_str).unwrap();
        assert_eq!(stats.applied, 2);
        assert_eq!(read_file(&data), b"AAAABBBBCCCC");

        // The log is cleared once spent, so a second open replays nothing.
        let (_wal, again) = open_and_recover(&dir_str).unwrap();
        assert_eq!(again.applied, 0);
        assert_eq!(read_file(&data), b"AAAABBBBCCCC");
    }

    // -----------------------------------------------------------------------
    // Crash testing
    // -----------------------------------------------------------------------
    //
    // Truncating a file by hand proves the parser rejects a torn record; it does
    // not prove the engine survives being killed mid-write. These tests do that
    // by SIGKILLing a real child process that is inserting through the full
    // stack, then reopening the database and checking what came back.

    /// Env var carrying the database directory to the child process.
    const CRASH_DIR_ENV: &str = "RASTDB_CRASH_DIR";

    fn payload_for(id: u64) -> String {
        // Length varies with id, so a row that picked up a neighbour's bytes is
        // detectable rather than accidentally plausible.
        let len = 1 + (id as usize % 29);
        std::iter::repeat(char::from(b'a' + (id % 26) as u8))
            .take(len)
            .collect()
    }

    async fn exec_sql(db: &rclite::Arc<crate::core::database::Database>, sql: &str) -> Vec<u8> {
        use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};
        let purpose = recognize_query_purpose(sql).expect("parse");
        executor::execute(purpose, db.clone()).await
    }

    /// The child half: inserts until it is killed. Inert unless the parent asks
    /// for it, so a normal test run just skips it.
    #[tokio::test(flavor = "multi_thread")]
    async fn crash_child_writer() {
        let Ok(dir) = std::env::var(CRASH_DIR_ENV) else {
            return;
        };

        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);
        let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);

        let db = rclite::Arc::new(crate::core::database::Database::new(&dir).await);
        let _ = exec_sql(&db, "CREATE TABLE crashy (id UBIGINT, payload VARCHAR);").await;

        for id in 1u64.. {
            let sql = format!(
                "INSERT INTO crashy (id, payload) VALUES ({}, '{}');",
                id,
                payload_for(id)
            );
            let _ = exec_sql(&db, &sql).await;
        }
    }

    /// Reopens `dir` and returns every (id, payload) it can read.
    async fn read_back(dir: &str) -> Vec<(u64, String)> {
        use crate::core::row::row::vec_into_rows;

        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);
        let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);

        let db = rclite::Arc::new(crate::core::database::Database::new(dir).await);
        let res = exec_sql(&db, "SELECT id, payload FROM crashy WHERE id > 0").await;
        if res.first() != Some(&2u8) {
            return Vec::new();
        }
        let rows = vec_into_rows(&res[1..]).expect("decode rows");

        let table = db.tables.get("crashy").expect("table").clone();
        let schema = &table.schema.fields;

        rows.into_iter()
            .map(|row| {
                let id = u64::from_le_bytes(
                    row.get_column("id", schema)
                        .expect("id")
                        .into_slice()
                        .try_into()
                        .expect("u64"),
                );
                let payload = String::from_utf8_lossy(
                    row.get_column("payload", schema)
                        .expect("payload")
                        .into_slice(),
                )
                .to_string();
                (id, payload)
            })
            .collect()
    }

    /// Copies every data file (not the log) into `dest`.
    fn snapshot_data_files(dir: &Path, dest: &Path) {
        std::fs::create_dir_all(dest).unwrap();
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                continue;
            }
            let name = entry.file_name();
            if name == DEFAULT_WAL_NAME {
                continue;
            }
            std::fs::copy(entry.path(), dest.join(name)).unwrap();
        }
    }

    /// Restores a pseudo-random subset of 4 KiB pages from the snapshot,
    /// modelling a power cut in which some pages reached the device and others
    /// did not, with no ordering guarantee between them.
    fn restore_random_pages(snapshot: &Path, dir: &Path, seed: u64) {
        const PAGE: usize = 4096;
        // Deterministic, so a failure can be reproduced from the seed alone.
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };

        for entry in std::fs::read_dir(snapshot).unwrap().flatten() {
            let name = entry.file_name();
            let live_path = dir.join(&name);
            let old = std::fs::read(entry.path()).unwrap();
            let Ok(mut live) = std::fs::read(&live_path) else {
                continue;
            };

            // Any page that existed before the crash may have reverted.
            let pages = old.len().div_ceil(PAGE);
            for p in 0..pages {
                if next() % 2 == 0 {
                    continue; // this page made it to the device
                }
                let start = p * PAGE;
                let end = (start + PAGE).min(old.len()).min(live.len());
                if start < end {
                    live[start..end].copy_from_slice(&old[start..end]);
                }
            }

            // Bytes appended after the snapshot may simply not be there.
            if next() % 3 == 0 {
                live.truncate(old.len());
            }

            std::fs::write(&live_path, &live).unwrap();
        }
    }

    /// Puts the snapshot back, modelling a power cut in which none of the
    /// data-file writes since the snapshot reached the device.
    fn restore_data_files(snapshot: &Path, dir: &Path) {
        for entry in std::fs::read_dir(snapshot).unwrap().flatten() {
            let name = entry.file_name();
            std::fs::copy(entry.path(), dir.join(name)).unwrap();
        }
    }

    /// Power-loss simulation.
    ///
    /// A SIGKILL cannot demonstrate this: the kernel completes the write and the
    /// page cache outlives the process, so everything the process wrote is still
    /// there. Losing un-flushed data needs the machine to go away, which is
    /// modelled here by restoring the data files to an earlier snapshot while
    /// keeping the log.
    ///
    /// Data files are never flushed on the write path — that is the point of the
    /// log — so this is the worst case the design has to survive: every
    /// data-file write since the last checkpoint is gone, and recovery must
    /// rebuild all of it from the log alone.
    #[tokio::test(flavor = "multi_thread")]
    async fn committed_work_survives_losing_every_data_file_write() {
        use crate::core::row::row::vec_into_rows;

        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);
        let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);

        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        let snap = temp_dir();

        // Baseline: some rows, then a checkpoint so they live in the data files.
        {
            let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
            let _ = exec_sql(&db, "CREATE TABLE durable (id UBIGINT, payload VARCHAR);").await;
            for id in 1..=4u64 {
                let sql = format!(
                    "INSERT INTO durable (id, payload) VALUES ({}, '{}');",
                    id,
                    payload_for(id)
                );
                let _ = exec_sql(&db, &sql).await;
            }
            // With logging off there is nothing to checkpoint; the baseline rows
            // were flushed individually instead, so they are in the snapshot
            // either way and the assertion below still measures the right thing.
            if let Some(w) = wal_for(&dir_str) {
                w.checkpoint().unwrap();
            }
        }

        snapshot_data_files(dir.path(), snap.path());

        // Work that exists only in the log until the next checkpoint.
        {
            let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
            for id in 5..=12u64 {
                let sql = format!(
                    "INSERT INTO durable (id, payload) VALUES ({}, '{}');",
                    id,
                    payload_for(id)
                );
                let _ = exec_sql(&db, &sql).await;
            }
        }

        // The power goes out: nothing written to the data files since the
        // snapshot survived. The log did, because it is the one thing flushed.
        restore_data_files(snap.path(), dir.path());

        // Reopening replays it.
        let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
        let res = exec_sql(&db, "SELECT id, payload FROM durable WHERE id > 0").await;
        assert_eq!(res.first(), Some(&2u8), "query should have returned rows");
        let rows = vec_into_rows(&res[1..]).expect("decode rows");

        let table = db.tables.get("durable").expect("table").clone();
        let schema = &table.schema.fields;

        let mut seen: Vec<u64> = Vec::new();
        for row in rows {
            let id = u64::from_le_bytes(
                row.get_column("id", schema)
                    .expect("id")
                    .into_slice()
                    .try_into()
                    .expect("u64"),
            );
            let payload = String::from_utf8_lossy(
                row.get_column("payload", schema)
                    .expect("payload")
                    .into_slice(),
            )
            .to_string();
            assert_eq!(payload, payload_for(id), "row {} came back corrupted", id);
            seen.push(id);
        }
        seen.sort_unstable();

        assert_eq!(
            seen,
            (1..=12u64).collect::<Vec<_>>(),
            "every committed row must be rebuilt from the log after the data \
             files lost their un-flushed writes"
        );
    }

    /// Env var carrying the database directory to the update-crash child.
    const CRASH_UPDATE_ENV: &str = "RASTDB_CRASH_UPDATE_DIR";

    /// Payload large enough to span several pages, so a write cut short by a
    /// kill actually lands partially. A single small write is usually atomic in
    /// practice, which would make this test unable to fail.
    const BLOB_LEN: usize = 16 * 1024;

    fn blob_of(c: u8) -> String {
        std::iter::repeat(c as char).take(BLOB_LEN).collect()
    }

    /// The child half: rewrites every row in place, over and over, until killed.
    ///
    /// Each update sets the payload to a single repeated character, and the
    /// length never changes — so this takes the in-place path rather than the
    /// tombstone-and-append one, which is the path that can be torn.
    #[tokio::test(flavor = "multi_thread")]
    async fn crash_update_child() {
        let Ok(dir) = std::env::var(CRASH_UPDATE_ENV) else {
            return;
        };

        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);
        let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);

        let db = rclite::Arc::new(crate::core::database::Database::new(&dir).await);
        let _ = exec_sql(&db, "CREATE TABLE churn (id UBIGINT, blob VARCHAR);").await;

        const ROWS: u64 = 8;
        for id in 1..=ROWS {
            let sql = format!(
                "INSERT INTO churn (id, blob) VALUES ({}, '{}');",
                id,
                blob_of(b'a')
            );
            let _ = exec_sql(&db, &sql).await;
        }

        // Cycle through characters so a torn row shows two different ones.
        for round in 0u64.. {
            let c = b'a' + (round % 26) as u8;
            let sql = format!("UPDATE churn SET blob = '{}' WHERE id > 0", blob_of(c));
            let _ = exec_sql(&db, &sql).await;
        }
    }

    /// Every payload must be one repeated character. A row carrying two
    /// different characters is a half-applied overwrite: proof that a write was
    /// torn across the crash.
    async fn read_back_blobs(dir: &str) -> Vec<(u64, usize, char)> {
        use crate::core::row::row::vec_into_rows;

        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);
        let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);

        let db = rclite::Arc::new(crate::core::database::Database::new(dir).await);
        let res = exec_sql(&db, "SELECT id, blob FROM churn WHERE id > 0").await;
        if res.first() != Some(&2u8) {
            return Vec::new();
        }
        let Ok(rows) = vec_into_rows(&res[1..]) else {
            return Vec::new();
        };

        let Some(table) = db.tables.get("churn").map(|t| t.clone()) else {
            return Vec::new();
        };
        let schema = &table.schema.fields;

        rows.into_iter()
            .filter_map(|row| {
                let id =
                    u64::from_le_bytes(row.get_column("id", schema)?.into_slice().try_into().ok()?);
                let blob = row.get_column("blob", schema)?;
                let bytes = blob.into_slice();
                let distinct: std::collections::BTreeSet<u8> = bytes.iter().copied().collect();
                Some((id, distinct.len(), *bytes.first().unwrap_or(&b'?') as char))
            })
            .collect()
    }

    /// Same, but with enough traffic to force the log to checkpoint and
    /// truncate underneath. Truncation discards records already applied to their
    /// files, so an outcome record that was never applied is lost with them.
    #[tokio::test(flavor = "multi_thread")]
    async fn outcomes_survive_the_log_being_checkpointed() {
        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);

        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        {
            let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
            let _ = exec_sql(&db, "CREATE TABLE t (id UBIGINT, blob VARCHAR);").await;
            let _ = exec_sql(&db, "INSERT INTO t (id, blob) VALUES (1, 'x');").await;
            // Churn until the log has certainly checkpointed at least once.
            for round in 0..400u64 {
                let sql = format!(
                    "UPDATE t SET blob = '{}' WHERE id > 0",
                    blob_of(b'a' + (round % 26) as u8)
                );
                let _ = exec_sql(&db, &sql).await;
            }
            if let Some(w) = crate::core::wal::wal_for(&dir_str) {
                w.checkpoint().unwrap();
            }
        }

        let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
        let res = exec_sql(&db, "SELECT id FROM t WHERE id > 0").await;
        assert!(
            res.len() > 1,
            "the insert's outcome record must survive the log being truncated"
        );
    }

    /// A committed row must still be visible after a clean reopen.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_committed_row_is_visible_after_reopen() {
        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(1024);

        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        {
            let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
            let _ = exec_sql(&db, "CREATE TABLE t (id UBIGINT, blob VARCHAR);").await;
            let _ = exec_sql(&db, "INSERT INTO t (id, blob) VALUES (1, 'x');").await;
            let res = exec_sql(&db, "SELECT id FROM t WHERE id > 0").await;
            assert!(
                res.len() > 1,
                "the row must be visible in the session that wrote it"
            );
        }

        let db = rclite::Arc::new(crate::core::database::Database::new(&dir_str).await);
        let res = exec_sql(&db, "SELECT id FROM t WHERE id > 0").await;
        assert!(
            res.len() > 1,
            "a committed row must survive a reopen; the outcome record did not"
        );
    }

    /// An in-place UPDATE must be all-or-nothing across a crash.
    ///
    /// Without a log the row bytes are overwritten directly, so a kill partway
    /// through leaves a row that is half old and half new — durably. With one,
    /// the overwrite is withheld until its record is durable, and replay
    /// finishes it.
    #[test]
    #[cfg(unix)]
    fn survives_sigkill_during_in_place_updates() {
        use std::process::{Command, Stdio};

        if std::env::var(CRASH_UPDATE_ENV).is_ok() {
            return;
        }

        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        let exe = std::env::current_exe().expect("test binary");

        let mut child = Command::new(&exe)
            .args([
                "--exact",
                "core::wal::tests::crash_update_child",
                "--nocapture",
            ])
            .env(CRASH_UPDATE_ENV, &dir_str)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn update child");

        std::thread::sleep(std::time::Duration::from_millis(2500));
        let _ = child.kill();
        let _ = child.wait();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let rows = rt.block_on(read_back_blobs(&dir_str));
        assert!(
            !rows.is_empty(),
            "child produced no readable rows; the test proved nothing"
        );

        for (id, distinct, first) in &rows {
            assert_eq!(
                *distinct, 1,
                "row {} holds {} different characters (starts with {:?}): an overwrite was torn \
                 across the crash and left half-applied",
                id, distinct, first
            );
        }
    }

    #[test]
    #[cfg(unix)]
    fn survives_sigkill_during_inserts() {
        use std::process::{Command, Stdio};

        // Guard against the child recursing into the parent half.
        if std::env::var(CRASH_DIR_ENV).is_ok() {
            return;
        }

        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        let exe = std::env::current_exe().expect("test binary");

        let mut child = Command::new(&exe)
            .args([
                "--exact",
                "core::wal::tests::crash_child_writer",
                "--nocapture",
            ])
            .env(CRASH_DIR_ENV, &dir_str)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn writer child");

        // Let it get well past the first checkpoint-free burst of inserts.
        std::thread::sleep(std::time::Duration::from_millis(1500));

        // SIGKILL: no unwinding, no destructors, no checkpoint. Whatever is on
        // disk is exactly what a power loss would have left.
        let _ = child.kill();
        let _ = child.wait();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let first = rt.block_on(read_back(&dir_str));

        // The child ran for over a second; if it wrote nothing the test is not
        // exercising anything and should say so rather than pass vacuously.
        assert!(
            !first.is_empty(),
            "child wrote no readable rows; the crash test proved nothing"
        );

        // 1. Every surviving row is internally consistent. A torn write — row
        //    bytes from one insert paired with another's pointer — shows up here.
        for (id, payload) in &first {
            assert_eq!(
                payload,
                &payload_for(*id),
                "row {} came back with another row's bytes",
                id
            );
        }

        // 2. No duplicates: a replayed record must not resurrect a row twice.
        let mut ids: Vec<u64> = first.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        let before = ids.len();
        ids.dedup();
        assert_eq!(before, ids.len(), "duplicate ids after recovery");

        // 3. Recovery is idempotent: opening again must not change the answer.
        let second = rt.block_on(read_back(&dir_str));
        assert_eq!(
            first.len(),
            second.len(),
            "a second open changed the row count; recovery is not idempotent"
        );
    }

    #[test]
    fn register_directory_skips_the_log_itself() {
        let dir = temp_dir();
        let dir_str = dir.path().to_string_lossy().to_string();
        std::fs::write(dir.path().join("a.db"), b"x").unwrap();
        std::fs::write(dir.path().join(DEFAULT_WAL_NAME), b"").unwrap();

        let wal = Wal::open(dir.path().join(DEFAULT_WAL_NAME)).unwrap();
        let n = register_directory(&dir_str, &wal).unwrap();
        assert_eq!(n, 1, "the log must not be a replay target");
    }

    /// A deferred commit is replayable only once the sync is taken.
    ///
    /// This is the whole bargain: the sync moves from per row to per statement,
    /// and the caller promises to take it before telling anyone the write
    /// worked.
    #[test]
    fn a_deferred_commit_becomes_durable_at_the_sync() {
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let data = dir.path().join("table.db");

        let wal = Wal::open(&wal_path).unwrap();
        let before = wal.durable.load(Ordering::Acquire);

        for slot in 0..64u64 {
            let txn = wal.begin();
            wal.log(txn, 1, slot, b"x").unwrap();
            wal.commit_txn_deferred(txn).unwrap();
        }

        // Nothing has been forced out yet: that is what makes it one sync
        // instead of sixty-four.
        assert_eq!(
            wal.durable.load(Ordering::Acquire),
            before,
            "a deferred commit synced anyway"
        );

        wal.sync_pending().unwrap();
        assert!(
            wal.durable.load(Ordering::Acquire) > before,
            "the sync did not cover the deferred commits"
        );

        // And every one of them replays.
        let stats = Wal::recover(&wal_path, resolver(data.clone())).unwrap();
        assert_eq!(stats.applied, 64);
    }

    #[test]
    fn an_ordinary_commit_is_still_durable_when_it_returns() {
        // `log_and_commit` writes the transaction *outcome* records, which the
        // executor relies on being durable the moment the call returns.
        let dir = temp_dir();
        let wal_path = dir.path().join(DEFAULT_WAL_NAME);
        let wal = Wal::open(&wal_path).unwrap();

        wal.log_and_commit(1, 0, b"outcome").unwrap();
        assert!(
            wal.durable.load(Ordering::Acquire) > 0,
            "an ordinary commit deferred its sync"
        );
    }

    #[test]
    fn syncing_with_nothing_owed_is_free() {
        let dir = temp_dir();
        let wal = Wal::open(&dir.path().join(DEFAULT_WAL_NAME)).unwrap();
        wal.sync_pending().unwrap();
        wal.sync_pending().unwrap();
    }

    #[test]
    fn a_poisoned_log_refuses_a_deferred_commit_too() {
        let dir = temp_dir();
        let wal = Wal::open(&dir.path().join(DEFAULT_WAL_NAME)).unwrap();
        let txn = wal.begin();
        wal.log(txn, 1, 0, b"x").unwrap();
        wal.poisoned.store(true, Ordering::Release);
        assert!(wal.commit_txn_deferred(txn).is_err());
    }
}

#[cfg(test)]
mod recovery_race_tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    /// Records the writer may add before it stops on its own.
    ///
    /// A budget, not a stop flag, and that distinction is the point. The first
    /// version of this test let the writer append until the reader finished,
    /// and once the underflow was fixed the reader stopped dying on the first
    /// pass — so the writer ran for the whole loop and put 16 GB into a shared
    /// tmpfs, taking every process on the machine down with it. A test that can
    /// only be stopped by something else finishing is not bounded.
    ///
    /// It also invalidated its own result: past `ENOSPC` every write fails for
    /// a reason that has nothing to do with the race being tested.
    const WRITER_BUDGET: usize = 2_000;

    /// Seed records. Deliberately many and tiny: what the scan has to be slow
    /// enough for is the *window* between the length measurement and the walk,
    /// and that is bought with the number of `next()` calls rather than with
    /// bytes. Twenty thousand of these is roughly 900 KB.
    const SEED_RECORDS: usize = 20_000;

    /// The arithmetic on its own, with no thread and no timing.
    ///
    /// The race below demonstrated the bug once and is worth keeping as a smoke
    /// test, but it can only ever catch a regression when the scheduler
    /// cooperates. This cannot miss: it is the exact condition the race
    /// produces — a scan that validated more bytes than the length measured
    /// before it.
    #[test]
    fn a_scan_that_outran_a_stale_length_reports_no_tail() {
        // The ordinary case: a torn tail is the bytes past what validated.
        assert_eq!(discarded_tail(1_000, 900), 100);
        // Nothing torn.
        assert_eq!(discarded_tail(1_000, 1_000), 0);
        // The race: the log grew between the two measurements. Zero, not a
        // panic and not 2^64 minus one.
        assert_eq!(discarded_tail(900, 1_000), 0);
        assert_eq!(discarded_tail(0, u64::MAX), 0);
    }

    /// Recovery measures the file's length *before* the scan and the bytes it
    /// validated *after* it. Those are two different instants, and anything
    /// appending in between makes the second larger than the first.
    ///
    /// Unsaturated the subtraction panicked in a debug build, and in a release
    /// build wrapped to nearly 2^64 — which [`open_and_recover`] reads as "there
    /// was trailing garbage" and answers by truncating the log, on a log
    /// something is actively writing to. That is a stale statistic turning into
    /// deleted records.
    ///
    /// None of which makes recovery racing a writer correct; it has the log to
    /// itself by design. This pins the arithmetic surviving the moment.
    #[test]
    fn recovery_racing_a_writer_does_not_underflow_the_tail_count() {
        let dir = tempfile::Builder::new()
            .prefix("rastdb_race_")
            .tempdir()
            .unwrap();
        let path = dir.path().join("race.wal");
        let wal = std::sync::Arc::new(Wal::open(&path).unwrap());

        for _ in 0..SEED_RECORDS {
            let txn = wal.begin();
            wal.log(txn, 1, 0, &[7u8; 8]).unwrap();
            wal.commit_txn_deferred(txn).unwrap();
        }

        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let writer = {
            let wal = wal.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut written = 0usize;
                while written < WRITER_BUDGET && !stop.load(Ordering::Relaxed) {
                    let txn = wal.begin();
                    if wal.log(txn, 1, 0, &[9u8; 8]).is_err() {
                        break;
                    }
                    let _ = wal.commit_txn_deferred(txn);
                    written += 1;
                }
                written
            })
        };

        // Nothing resolves, so no data file is touched and each pass is the
        // scan and the arithmetic under test.
        for _ in 0..20 {
            let stats = Wal::recover_upto(&path, |_| None, None)
                .expect("recovery should not fail while the log is being written");
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            assert!(
                stats.truncated_tail <= size,
                "truncated_tail of {} exceeds the whole log of {} bytes, so the \
                 subtraction wrapped",
                stats.truncated_tail,
                size
            );
        }

        stop.store(true, Ordering::Relaxed);
        let written = writer.join().unwrap();
        assert!(written <= WRITER_BUDGET, "the writer ran past its budget");

        // The budget has to hold whatever the timing does, or this test can
        // fill a shared filesystem again.
        let size = std::fs::metadata(&path).unwrap().len();
        // A data record *and* the commit marker that follows it, which is a
        // bare header. Counting only the data records put this ceiling at half
        // the true size and failed a test that was behaving correctly.
        let per_record = (HEADER_LEN + 8) + HEADER_LEN;
        let ceiling = ((SEED_RECORDS + WRITER_BUDGET) * per_record) as u64;
        assert!(
            size <= ceiling,
            "the log grew to {} bytes, past its {} ceiling",
            size,
            ceiling
        );
    }
}
