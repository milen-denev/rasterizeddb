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
        file.sync_all()?;
    }
    std::fs::rename(&temp, &path)?;
    sync_parent_dir(&path)
}

/// Refuses absurd record lengths before allocating for them, so a corrupt header
/// cannot make recovery try to reserve gigabytes.
const MAX_RECORD_PAYLOAD: usize = 256 * 1024 * 1024;

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
    /// Bytes of trailing garbage ignored — a partial record from a crash.
    pub truncated_tail: u64,
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
    sync_handle: OnceLock<File>,
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

        Ok(Self {
            path,
            writer: Mutex::new(writer),
            issued: AtomicU64::new(0),
            write_pos: AtomicU64::new(end),
            durable: AtomicU64::new(0),
            gate: Mutex::new(()),
            sync_handle: OnceLock::new(),
            next_txn: AtomicU64::new(1),
            commit_lsn: AtomicU64::new(resume_lsn),
            bytes_since_checkpoint: AtomicU64::new(end),
            checkpointing: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            pending: Mutex::new(HashMap::new()),
            pending_count: AtomicU64::new(0),
            pending_sync: AtomicU64::new(0),
            pending_bytes: AtomicU64::new(0),
            files: Mutex::new(HashMap::new()),
            archive_dir: Mutex::new(None),
            archive_seq: AtomicU64::new(0),
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
        self.log_at(txn, file_id, offset, bytes, None)
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
    ) -> io::Result<(u64, u64)> {
        if bytes.len() > MAX_RECORD_PAYLOAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("WAL record payload of {} bytes is too large", bytes.len()),
            ));
        }

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

        // Header and payload go out under one lock: split across two `write_all`
        // calls a concurrent writer could interleave between them.
        let (seq, payload_at) = {
            let mut file = self.writer.lock();
            let record_at = self.write_pos.load(Ordering::Relaxed);
            file.write_all(&header)?;
            file.write_all(bytes)?;
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
        // Resolved before the record is written: failing afterwards would
        // leave a logged overwrite that nothing is tracking.
        let path = self.files.lock().get(&file_id).cloned();
        let Some(path) = path else {
            return Err(io::Error::other(format!(
                "WAL has no path registered for file id {file_id}"
            )));
        };

        let (seq, _) = self.log_at(txn, file_id, offset, bytes, Some(path))?;
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
            return Err(io::Error::other(
                "WAL is poisoned by an earlier flush failure; durability cannot be guaranteed",
            ));
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

        self.commit_through(seq)?;

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
        let mut log_reader: Option<File> = None;
        let mut buf: Vec<u8> = Vec::new();

        for w in held {
            self.pending_bytes
                .fetch_sub(w.len as u64, Ordering::Relaxed);

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

        Ok(seq)
    }

    /// Returns once every record up to `seq` is on stable storage.
    ///
    /// Concurrent callers collapse into a single flush: the flusher reads the
    /// issued counter *before* flushing and publishes it afterwards, claiming
    /// every record written so far rather than only its own.
    pub fn commit_through(&self, seq: u64) -> io::Result<()> {
        if unlikely(self.poisoned.load(Ordering::Acquire)) {
            return Err(io::Error::other(
                "WAL is poisoned by an earlier flush failure; durability cannot be guaranteed",
            ));
        }

        if self.durable.load(Ordering::Acquire) >= seq {
            return Ok(()); // an earlier flush already covered us
        }

        let _gate = self.gate.lock();

        if self.durable.load(Ordering::Acquire) >= seq {
            return Ok(()); // covered while we waited for the gate
        }

        let covered = self.issued.load(Ordering::Acquire);

        let handle = match self.sync_handle.get() {
            Some(h) => h,
            None => {
                let h = File::options().read(true).write(true).open(&self.path)?;
                let _ = self.sync_handle.set(h);
                self.sync_handle.get().expect("just set")
            }
        };

        // `sync_data`: the log only needs its bytes and length durable to be
        // replayable. Timestamps would cost an extra journal transaction.
        //
        // A failed flush is not retried. On Linux the kernel may drop the dirty
        // pages after reporting the error, so a second call could return success
        // while the data is gone. Poisoning instead makes every later commit
        // fail loudly rather than silently downgrading durability.
        if let Err(e) = handle.sync_data() {
            self.poisoned.store(true, Ordering::Release);
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

    /// Writes the zero-payload record that marks `txn` as committed.
    fn log_commit_marker(&self, txn: u64) -> io::Result<u64> {
        let lsn = self.commit_lsn.fetch_add(1, Ordering::AcqRel) + 1;

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

        let seq = {
            let mut file = self.writer.lock();
            file.write_all(&header)?;
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
        let txn = self.begin();
        self.log(txn, file_id, offset, bytes)?;
        self.commit_txn(txn)
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
        let txn = self.begin();
        self.log(txn, file_id, offset, bytes)?;
        self.commit_txn_deferred_seq(txn)
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
        self.truncate_locked(&mut file)
    }

    /// The body of [`Wal::truncate`], for callers that already hold the writer.
    ///
    /// Staging has to happen under the same lock that truncates, or a record
    /// appended between the copy and the `set_len` would be discarded without
    /// ever having been archived.
    fn truncate_locked(&self, file: &mut File) -> io::Result<()> {
        // Before the log is dropped, never after. A crash in between then
        // leaves a mark that is merely ahead of the log, which the next open
        // reconciles by taking the higher of the two. The other order would
        // leave an empty log behind a stale mark, and the sequence would hand
        // out numbers it had already used.
        //
        // Best-effort: failing the checkpoint over it would let the log grow
        // without bound, and the cost of the mark not landing is a sequence
        // that restarts — bad for backups, harmless for durability.
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

        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
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
    fn stage_locked(&self, file: &mut File) -> io::Result<()> {
        let dir = match self.archive_dir.lock().clone() {
            Some(d) => d,
            None => return Ok(()),
        };

        // An empty log describes nothing; staging it would cost an upload and
        // a manifest rewrite to say so.
        file.flush()?;
        if file.metadata()?.len() == 0 {
            return Ok(());
        }

        let seq = self.archive_seq.fetch_add(1, Ordering::AcqRel);
        // Written under a temporary name and renamed into place, so the
        // archiver never picks up a segment that is still being copied.
        let partial = dir.join(format!("{:016x}{}", seq, SEGMENT_PARTIAL_SUFFIX));
        let final_path = dir.join(format!("{:016x}{}", seq, SEGMENT_SUFFIX));

        std::fs::copy(&self.path, &partial)?;
        File::open(&partial)?.sync_data()?;
        std::fs::rename(&partial, &final_path)?;
        sync_parent_dir(&final_path)?;
        Ok(())
    }

    /// Flushes every registered data file, then discards the log.
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
    pub fn checkpoint(&self) -> io::Result<bool> {
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

        for (_, path) in self.registered_files() {
            match File::options().read(true).write(true).open(&path) {
                Ok(f) => f.sync_data()?,
                // A file that no longer exists cannot need flushing.
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            }
        }

        let mut file = self.writer.lock();

        // Re-checked under the writer lock, because the check at the top of
        // this function is only a cheap early-out. A transaction can register a
        // pending overwrite between that check and this lock; truncating then
        // would strand its payload offset past EOF. Records are appended under
        // this same lock, so once it is held the answer cannot change.
        if self.pending_count.load(Ordering::Acquire) != 0 {
            return Ok(false);
        }

        // Staged before the truncation and under the same lock: the records are
        // in the data files by now, which is what makes them safe to *drop*,
        // but a snapshot taken while they were being written still needs them
        // to reach a consistent state.
        if let Err(e) = self.stage_locked(&mut file) {
            // Losing the segment costs point-in-time coverage, not durability —
            // the data is already flushed. Failing the checkpoint instead would
            // let the log grow without bound because a full disk is the most
            // likely cause.
            log::error!("WAL segment could not be staged for archiving: {}", e);
        }
        self.truncate_locked(&mut file).map(|()| true)
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
        let valid_bytes;
        {
            let mut scan = LogScanner::open(path)?;
            while let Some(h) = scan.next()? {
                if h.flags & FLAG_COMMIT != 0 {
                    // A marker carries its commit sequence in `offset`.
                    if upto.is_some_and(|ceiling| h.offset > ceiling) {
                        continue;
                    }
                    committed.insert(h.txn);
                    stats.last_commit_lsn = stats.last_commit_lsn.max(h.offset);
                }
            }
            valid_bytes = scan.consumed;
        }

        // Pass 2: apply the records those transactions own.
        let mut open_files: HashMap<u64, Option<File>> = HashMap::new();
        {
            let mut scan = LogScanner::open(path)?;
            while let Some(h) = scan.next()? {
                // Commit markers are control records: they describe no bytes and
                // must not be replayed as if they did.
                if h.flags & FLAG_COMMIT != 0 {
                    continue;
                }

                if !committed.contains(&h.txn) {
                    stats.rolled_back += 1;
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

// ---------------------------------------------------------------------------
// Per-database registry
// ---------------------------------------------------------------------------

/// Logs, keyed by database directory.
///
/// Deliberately not one process-global log. A process can open more than one
/// database — every test binary does — and a global would make the second
/// database log into the first one's directory, so its own recovery would find
/// nothing to replay and the writes it deferred would be lost.
static WALS: OnceLock<Mutex<HashMap<String, Arc<Wal>>>> = OnceLock::new();

fn wals() -> &'static Mutex<HashMap<String, Arc<Wal>>> {
    WALS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Canonical key for a database directory, so the path a provider was built
/// with and the one the database was opened with agree.
pub fn dir_key(dir: &str) -> String {
    let trimmed = dir.trim_end_matches(['/', '\\']);
    if trimmed.is_empty() {
        dir.to_string()
    } else {
        trimmed.to_string()
    }
}

/// The log for `dir`, or `None` when that database has none.
#[inline]
pub fn wal_for(dir: &str) -> Option<Arc<Wal>> {
    wals().lock().get(&dir_key(dir)).cloned()
}

/// Every installed log, with the directory it belongs to.
///
/// For [`crate::core::log_writer`], which flushes on a timer for commits that
/// did not wait for a sync of their own and so has no one database to ask.
pub fn registered() -> Vec<(String, Arc<Wal>)> {
    wals()
        .lock()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Installs the log for `dir`, replacing any previous one.
pub fn install_for(dir: &str, w: Arc<Wal>) {
    let key = dir_key(dir);
    wals().lock().insert(key.clone(), w.clone());

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
    wals().lock().remove(&dir_key(dir))
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
    let wal_path = Path::new(dir).join(DEFAULT_WAL_NAME);
    let wal = Arc::new(Wal::open(&wal_path)?);
    register_directory(dir, &wal)?;

    let files = wal.clone();
    let stats = Wal::recover(&wal_path, move |id| files.files.lock().get(&id).cloned())?;

    // Resume the commit sequence where the previous run left off, so it stays
    // monotonic across restarts and can serve as a durable point-in-time token.
    wal.commit_lsn
        .fetch_max(stats.last_commit_lsn, Ordering::AcqRel);

    if stats.applied > 0 || stats.rolled_back > 0 || stats.truncated_tail > 0 {
        log::info!(
            "WAL recovery in {}: applied {}, rolled back {}, unresolved {}, discarded {} trailing bytes",
            dir,
            stats.applied,
            stats.rolled_back,
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
        Some(w) => w.bytes_since_checkpoint() >= crate::wal_checkpoint_bytes(),
        None => false,
    }
}

/// Checkpoints every installed log, for process shutdown.
pub fn checkpoint_all() -> io::Result<()> {
    let logs: Vec<Arc<Wal>> = wals().lock().values().cloned().collect();
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
