//! A file set's write gate (docs/persistent-indexes.md §2, §3, §6; slice
//! B11): the packed [`WriteGate`] word, the marker log it mirrors, the I/O
//! its slow path does, and the file set's slot epoch.
//!
//! # One word per file set
//!
//! The gate belongs to the file set, not to a `Table` instance (RV1, RV14).
//! [`FileGate::open`] is how every instance gets it: the **first** open of a
//! file set reads `.ixm` and derives the state (N21); every later open of the
//! same file set while the word is alive, a reload's included, gets the same
//! word, count and flags, untouched (P6). The table's map slot keeps a strong
//! reference (`database::slots::Slot`), so the word outlives any one
//! instance; a ticket keeps one too, so a ticket that outlives a DROP gives
//! its count back to a detached word. A file set is its directory, its stem
//! (the physical name its rows file uses, never `schema.name`, P32) and its
//! incarnation (C7): a DROP and CREATE of the name, or an ALTER rewrite, is a
//! new incarnation and so a new word, derived afresh.
//!
//! A partition is its own file set (its own stem), so each partition has its
//! own word. A table on a backend with no directory (memory, a key/value
//! store, a mirrored pool) is excluded from snapshots (P33): its word is
//! detached, `Dirty` for good, and its slow path never runs.
//!
//! # The state at open (§1, §2, N21)
//!
//! | `.ixm` | state |
//! |---|---|
//! | missing | `Dirty`: no `CLEAN` was ever durable |
//! | another incarnation's `HEADER` | `Dirty`: a leftover of a dropped table proves nothing about this one |
//! | last good record `CLEAN` (bytes after it or not) | `Clean` |
//! | last good record `DIRTY` or `HEADER` | `Dirty` |
//! | unreadable, or a bad first record | `Unknown` |
//!
//! An open writes nothing.
//!
//! # The slow path (T1)
//!
//! Run single-flight by [`WriteGate::acquire`] before the first ticket after
//! `Clean` or `Unknown`, before anything is issued:
//! - **`Clean`**: append `DIRTY` after the last good record by
//!   `marker::MarkerScan::plan_append`: `pwrite`, fdatasync, and when stale
//!   bytes follow it, `ftruncate` past it and fdatasync again, with nothing
//!   issued in between (owner's decision (a)): `rec_seq` binds a record to
//!   its position, so a stale record that is valid and sits right after the
//!   new one reads as its successor until the truncate is durable.
//! - **`Unknown`**: trust none of the log's bytes: rewrite it as `HEADER` +
//!   `DIRTY` through `.ixm.tmp` (write, fsync, rename, directory fsync), or
//!   else unlink `.ixs` and fsync the directory; only then delete a
//!   lingering `_flt.db` (§2 R9: it is the only evidence that an old binary
//!   wrote after `CLEAN`, so it goes only once the log durably ends `DIRTY`
//!   or `.ixs` is durably gone). The word leaves `Unknown` only once that
//!   delete returned `Ok` (Q12).
//! - If `DIRTY` cannot be made durable (ENOSPC, EIO), `.ixs` is unlinked and
//!   the directory fsynced, which needs no free blocks; the write proceeds
//!   with nothing left to protect. If that fails too, the ticket fails, and
//!   nothing is issued.
//!
//! # `_flt.db`, retired (§5, B14)
//!
//! The filter's old sidecar is never created, trusted or written again; the
//! filter persists only as a snapshot's FILTER section. Every open where it
//! exists deletes it, before anything is written ([`FileGate::retire_flt`]).
//! Beside an existing `.ixm` it is **evidence** (RV5): a binary that keeps
//! no marker log opened the table since the last `CLEAN` and may have
//! overwritten rows in place, which no file length shows. So the open's
//! proof fails, and `.ixs` is unlinked and the directory fsynced **before**
//! `_flt.db` is deleted: a crash between the two leaves the evidence, never
//! a stale `CLEAN` with nothing to contradict it. If that unlink fails,
//! `_flt.db` is kept and the word starts `Unknown`; if the delete fails, the
//! word starts `Unknown` too, and its slow path retries it before the first
//! write. The delete voids the directory log's records for the file first,
//! or the next open's replay would write it back. A `<t>_flt.db` that may be
//! another table's rows (a table named `<t>_flt`, which older builds
//! accepted beside `<t>`: its schema or pointers file is there, or the file
//! is not a filter image) is never deleted, and `<t>` takes no snapshot.
//!
//! # The slot epoch (§2 Deferred work, §6)
//!
//! [`FileGate::bump_epoch`] appends `epoch + 1` to the directory's `rdb.ixe`
//! as its own WAL transaction and waits for the directory log's sync, before
//! `VACUUM FULL`'s first effect. The open reads the recovered record.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::epoch_log::{self, EPOCH_LOG_FILE, NO_PID};
use super::gate_word::{GateState, WriteGate};
use super::marker::{self, Append};
use crate::core::storage_providers::traits::StorageIO;

/// Where a file set's index-store files are, and whose they are.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GateFiles {
    dir: PathBuf,
    stem: String,
    incarnation: u128,
    pid: u32,
}

impl GateFiles {
    fn ixm(&self) -> PathBuf {
        self.dir.join(format!("{}.ixm", self.stem))
    }

    fn ixm_tmp(&self) -> PathBuf {
        self.dir.join(format!("{}.ixm.tmp", self.stem))
    }

    fn ixs(&self) -> PathBuf {
        self.dir.join(format!("{}.ixs", self.stem))
    }

    fn ixs_tmp(&self) -> PathBuf {
        self.dir.join(format!("{}.ixs.tmp", self.stem))
    }

    /// `<stem>_flt.db`, the filter's retired sidecar (§5).
    fn flt_name(&self) -> String {
        format!("{}_flt.db", self.stem)
    }

    fn flt(&self) -> PathBuf {
        self.dir.join(self.flt_name())
    }

    fn flt_tmp(&self) -> PathBuf {
        self.dir.join(format!("{}.tmp", self.flt_name()))
    }
}

/// The first bytes of every image the filter's old sidecar held
/// (`filter::persist`'s magic).
const FLT_MAGIC: &[u8; 8] = b"RDBFLT01";

/// What [`FileGate::retire_flt`] found and did at open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FltAtOpen {
    /// No `_flt.db`.
    Absent,
    /// Deleted, its log records voided first. `evidence`: an `.ixm` was
    /// beside it, so `.ixs` was unlinked (durably) before it went.
    Deleted { evidence: bool },
    /// May be another table's rows (module documentation): kept, and the
    /// table takes no snapshot.
    LegacyPair,
    /// Kept: the evidence could not be acted on or the delete failed. The
    /// word was started `Unknown` (when it was this open's to start).
    Kept { why: String },
}

impl FltAtOpen {
    /// Whether the open's proof must fail because of it (§4's third row).
    pub fn refuses_snapshot(&self) -> bool {
        !matches!(self, FltAtOpen::Absent | FltAtOpen::Deleted { evidence: false })
    }
}

/// Whether `<stem>_flt.db` may be another table's rows file: a table named
/// `<stem>_flt` has its schema or pointers file beside it, or the file holds
/// bytes that are not a filter image.
fn flt_may_be_a_table(files: &GateFiles) -> bool {
    let other = format!("{}_flt", files.stem);
    if files.dir.join(format!("{other}_schema.db")).exists() || files.dir.join(format!("{other}_pointers.db")).exists() {
        return true;
    }
    match std::fs::File::open(files.flt()) {
        Ok(f) => {
            use std::io::Read;
            let mut head = [0u8; 8];
            let mut got = 0;
            let mut f = f;
            while got < head.len() {
                match f.read(&mut head[got..]) {
                    Ok(0) => break,
                    Ok(n) => got += n,
                    // Unreadable: it cannot be shown to be ours.
                    Err(_) => return true,
                }
            }
            got != 0 && (got < head.len() || &head != FLT_MAGIC)
        }
        Err(e) => e.kind() != io::ErrorKind::NotFound,
    }
}

/// Deletes `files`' `_flt.db` and its staging sibling, the directory log's
/// records for it voided first, then fsyncs the directory. `Ok(false)` when
/// there was none. The caller has ruled out a legacy pair.
fn delete_flt(files: &GateFiles) -> io::Result<bool> {
    let (flt, tmp) = (files.flt(), files.flt_tmp());
    if !flt.exists() && !tmp.exists() {
        return Ok(false);
    }
    failpoint("unknown_flt_delete_order", &flt)?;
    // Voided first: every open replays the log, and a replayed record would
    // write the file back.
    if let Some(wal) = crate::core::wal::wal_for(&files.dir.to_string_lossy()) {
        wal.forget_files(&[crate::core::wal::file_id_for_name(&files.flt_name())])?;
    }
    for path in [&flt, &tmp] {
        dir_op(DirOp::Unlink(path));
        match std::fs::remove_file(path) {
            Ok(()) => crate::core::sync_audit::note("unlink", path),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    crate::core::wal::sync_parent_dir(&flt)?;
    Ok(true)
}

/// Counters for `_flt.db` at open, for the tests and the logs.
static FLT_DELETED_TOTAL: AtomicU64 = AtomicU64::new(0);
static FLT_EVIDENCE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// `_flt.db` files deleted at open or by an `Unknown` slow path.
pub fn flt_deleted_total() -> u64 {
    FLT_DELETED_TOTAL.load(Ordering::Relaxed)
}

/// `_flt.db` files found beside an `.ixm` (evidence, RV5).
pub fn flt_evidence_total() -> u64 {
    FLT_EVIDENCE_TOTAL.load(Ordering::Relaxed)
}

/// A file set's gate. Shared by its map slot, every instance of the table,
/// and every ticket on it.
pub struct FileGate {
    word: Arc<WriteGate>,
    /// `None`: excluded from snapshots, the word detached and `Dirty`.
    files: Option<GateFiles>,
    /// The slot epoch: the `rdb.ixe` record's, or 0 where none was ever
    /// written; one more after each `VACUUM FULL`.
    slot_epoch: AtomicU64,
    /// Whether `slot_epoch` is the recovered record's. False when `rdb.ixe`
    /// could not be read or decoded: no snapshot can then prove its epoch.
    epoch_proven: AtomicBool,
    /// The maintenance lock (§3, P15): held by a seal for its whole run, and
    /// waited for by another seal, by retirement (§7 step 2) and by a
    /// RENAME's or MOVE's carry of the file set. No writer path takes it.
    maintenance: tokio::sync::Mutex<()>,
    /// Whether the `CLEAN` the log ends in describes the in-memory
    /// structures of this process: set by a publish (B13), and by B14's load.
    /// A word that is `Clean` and vouched has nothing a seal could add; one
    /// that is `Clean` without it (a log a previous run wrote, which this
    /// open did not load) is rewritten at the next clean point (§3 rule 2).
    vouched: AtomicBool,
    /// Test-only: `GatedIo` judges this word's writes as a release build
    /// does (the slow path inline, `CANCEL` on a ticketless seal) instead of
    /// panicking, so tests can exercise production's verdicts (B12).
    #[cfg(test)]
    release_verdicts: AtomicBool,
    /// Test-only: the next slow path fails with nothing written, as one
    /// whose `DIRTY` could not be made durable and whose `.ixs` could not be
    /// unlinked does (B12: the failure goes through `failed_appends`).
    #[cfg(test)]
    fail_slow_path: AtomicBool,
}

impl std::fmt::Debug for FileGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileGate")
            .field("word", &self.word.load())
            .field("files", &self.files)
            .field("slot_epoch", &self.slot_epoch.load(Ordering::Relaxed))
            .finish()
    }
}

/// Every file set's live word, so a later open of a file set finds it.
static GATES: Mutex<Registry> = Mutex::new(Registry {
    map: None,
    pruned_at: 0,
});

struct Registry {
    map: Option<HashMap<GateFiles, Weak<FileGate>>>,
    /// The map's size after the last sweep of dead entries.
    pruned_at: usize,
}

/// Runs blocking file I/O from async code without stalling a runtime worker
/// where the runtime can spare one.
pub(super) fn blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(f),
        _ => f(),
    }
}

/// A named failpoint (`core::power_loss::failpoint`); nothing outside tests
/// and the `index_store_failpoints` feature.
#[inline]
pub(super) fn failpoint(name: &str, path: &Path) -> io::Result<()> {
    #[cfg(any(test, feature = "index_store_failpoints"))]
    {
        crate::core::power_loss::failpoint::hit(name, path)
    }
    #[cfg(not(any(test, feature = "index_store_failpoints")))]
    {
        let _ = (name, path);
        Ok(())
    }
}

/// Announces a directory operation to the crash-image recorder.
#[inline]
pub(super) fn dir_op(op: DirOp<'_>) {
    #[cfg(any(test, feature = "index_store_failpoints"))]
    crate::core::sync_audit::dir_op(match op {
        DirOp::Create(p) => crate::core::sync_audit::DirOp::Create(p),
        DirOp::Rename { from, to } => crate::core::sync_audit::DirOp::Rename { from, to },
        DirOp::Unlink(p) => crate::core::sync_audit::DirOp::Unlink(p),
    });
    #[cfg(not(any(test, feature = "index_store_failpoints")))]
    let _ = op;
}

#[derive(Clone, Copy)]
#[cfg_attr(not(any(test, feature = "index_store_failpoints")), allow(dead_code))]
pub(super) enum DirOp<'a> {
    Create(&'a Path),
    Rename { from: &'a Path, to: &'a Path },
    Unlink(&'a Path),
}

/// The sync tag of the marker log's appends and truncates.
pub const SYNC_MARKER: &str = "index_store.marker";
/// The sync tag of `.ixm.tmp`, before it is renamed over the log.
pub const SYNC_MARKER_TMP: &str = "index_store.marker_tmp";
/// The sync tag of `rdb.ixe` in a directory with no log.
pub const SYNC_EPOCH: &str = "index_store.epoch";

/// What `.ixm` says, read at a file set's first open (see the module table).
fn state_at_open(files: &GateFiles) -> GateState {
    let path = files.ixm();
    if let Err(e) = failpoint("ixm_read_eio_at_open", &path) {
        log::warn!("{}: {e}; the marker log proves nothing", path.display());
        return GateState::Unknown;
    }
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return GateState::Dirty,
        Err(e) => {
            log::warn!("{} could not be read ({e}); the marker log proves nothing", path.display());
            return GateState::Unknown;
        }
    };
    match marker::scan(&bytes) {
        Ok(scan) if scan.incarnation != files.incarnation => GateState::Dirty,
        Ok(scan) => match scan.state() {
            marker::LogState::Clean => GateState::Clean,
            marker::LogState::Dirty => GateState::Dirty,
        },
        Err(e) => {
            log::warn!("{} is not a marker log this build reads ({e}); it proves nothing", path.display());
            GateState::Unknown
        }
    }
}

/// The recovered slot epoch of `files`, and whether it is proven.
fn epoch_at_open(files: &GateFiles) -> (u64, bool) {
    let path = files.dir.join(EPOCH_LOG_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => match epoch_log::decode(&bytes) {
            Ok(log) => (log.epoch_of(files.incarnation, files.pid).unwrap_or(0), true),
            Err(e) => {
                log::warn!("{} cannot be read ({e}); no snapshot here can prove its slot epoch", path.display());
                (0, false)
            }
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => (0, true),
        Err(e) => {
            log::warn!("{} cannot be read ({e}); no snapshot here can prove its slot epoch", path.display());
            (0, false)
        }
    }
}

/// The partition id a partition's stem `__pt_<uid:016x>_<pid:08x>` carries,
/// or [`NO_PID`] for a plain table's stem.
pub fn pid_of_stem(stem: &str) -> u32 {
    let Some(rest) = stem.strip_prefix(crate::core::database::PARTITION_FILE_PREFIX) else {
        return NO_PID;
    };
    match rest.split_once('_') {
        Some((uid, pid)) if uid.len() == 16 && pid.len() == 8 => u32::from_str_radix(pid, 16).unwrap_or(NO_PID),
        _ => NO_PID,
    }
}

impl FileGate {
    /// A word for a table with no snapshot: `Dirty` for good, no files.
    pub fn detached() -> Arc<FileGate> {
        Arc::new(FileGate {
            word: Arc::new(WriteGate::new(GateState::Dirty)),
            files: None,
            slot_epoch: AtomicU64::new(0),
            epoch_proven: AtomicBool::new(false),
            maintenance: tokio::sync::Mutex::const_new(()),
            vouched: AtomicBool::new(false),
            #[cfg(test)]
            release_verdicts: AtomicBool::new(false),
            #[cfg(test)]
            fail_slow_path: AtomicBool::new(false),
        })
    }

    /// The gate of the file set `stem` in `dir` at `incarnation`: the live
    /// word if an earlier open of the same file set still holds it,
    /// otherwise a new one derived from `.ixm` and `rdb.ixe`. `dir` is `None`
    /// for a table excluded from snapshots. Writes nothing.
    pub fn open(dir: Option<PathBuf>, stem: &str, incarnation: u128) -> Arc<FileGate> {
        let Some(dir) = dir else {
            return FileGate::detached();
        };
        let files = GateFiles {
            dir,
            stem: stem.to_string(),
            incarnation,
            pid: pid_of_stem(stem),
        };
        if let Some(live) = GATES
            .lock()
            .map
            .as_ref()
            .and_then(|m| m.get(&files))
            .and_then(Weak::upgrade)
        {
            return live;
        }
        // Read outside the registry lock; a racing first open of the same
        // file set is settled under it below, and both read the same files.
        let state = state_at_open(&files);
        let (epoch, proven) = epoch_at_open(&files);
        let gate = Arc::new(FileGate {
            word: Arc::new(WriteGate::new(state)),
            files: Some(files.clone()),
            slot_epoch: AtomicU64::new(epoch),
            epoch_proven: AtomicBool::new(proven),
            maintenance: tokio::sync::Mutex::const_new(()),
            vouched: AtomicBool::new(false),
            #[cfg(test)]
            release_verdicts: AtomicBool::new(false),
            #[cfg(test)]
            fail_slow_path: AtomicBool::new(false),
        });
        let mut reg = GATES.lock();
        let pruned_at = reg.pruned_at;
        let map = reg.map.get_or_insert_with(HashMap::new);
        if let Some(live) = map.get(&files).and_then(Weak::upgrade) {
            return live;
        }
        map.insert(files, Arc::downgrade(&gate));
        if map.len() >= 2 * pruned_at + 64 {
            map.retain(|_, w| w.strong_count() > 0);
            let len = map.len();
            reg.pruned_at = len;
        }
        gate
    }

    /// The word.
    #[inline]
    pub fn word(&self) -> &Arc<WriteGate> {
        &self.word
    }

    /// The state the word holds now.
    pub fn state(&self) -> GateState {
        self.word.load().state()
    }

    /// Whether this file set can have a snapshot.
    pub fn has_files(&self) -> bool {
        self.files.is_some()
    }

    /// The marker log's path, for a file set that has one.
    pub fn ixm_path(&self) -> Option<PathBuf> {
        self.files.as_ref().map(GateFiles::ixm)
    }

    /// The snapshot's path, for a file set that has one.
    pub fn ixs_path(&self) -> Option<PathBuf> {
        self.files.as_ref().map(GateFiles::ixs)
    }

    /// The slot epoch now.
    pub fn slot_epoch(&self) -> u64 {
        self.slot_epoch.load(Ordering::Acquire)
    }

    /// Whether the slot epoch is the recovered `rdb.ixe` record's.
    pub fn epoch_proven(&self) -> bool {
        self.epoch_proven.load(Ordering::Acquire)
    }

    /// Who the census attributes this gate's holds to.
    pub fn census_id(&self) -> usize {
        self.word.census_id()
    }

    /// The slow path's I/O for `state` (module documentation, T1). Called by
    /// `gate::ticket` through [`WriteGate::acquire`], single-flight; the word
    /// moves to `Dirty` only once this returned `Ok`.
    pub(crate) async fn slow_path(&self, state: GateState) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_slow_path.swap(false, Ordering::AcqRel) {
            return Err(io::Error::other("injected slow-path failure: DIRTY not durable, .ixs not unlinked"));
        }
        let Some(files) = self.files.as_ref() else {
            return Ok(());
        };
        let done = blocking(|| match state {
            GateState::Dirty => Ok(()),
            GateState::Clean => append_dirty(files),
            // §2 R9: only once the log durably ends `DIRTY` or `.ixs` is
            // durably gone does a lingering `_flt.db` go, and the word leaves
            // `Unknown` only once that delete returned `Ok` (Q12).
            GateState::Unknown => rewrite_unknown(files).and_then(|()| {
                if flt_may_be_a_table(files) {
                    return Ok(());
                }
                if delete_flt(files)? {
                    FLT_DELETED_TOTAL.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }),
        });
        if done.is_ok() {
            super::census::dirty_durable(self.census_id());
        }
        done
    }

    /// `_flt.db` at open (module documentation, §5): deleted before anything
    /// is written, after `.ixs` is durably unlinked when an `.ixm` makes it
    /// evidence; kept, with the word started `Unknown`, when either step
    /// fails; never deleted when it may be another table's rows. A file set
    /// with no directory has nothing here: its provider family deletes it
    /// (the row layer's open).
    pub(crate) fn retire_flt(&self) -> FltAtOpen {
        let Some(files) = self.files.as_ref() else {
            return FltAtOpen::Absent;
        };
        blocking(|| {
            if !files.flt().exists() && !files.flt_tmp().exists() {
                return FltAtOpen::Absent;
            }
            if flt_may_be_a_table(files) {
                log::info!(
                    "{} may be the rows of a table named {}_flt, so it is kept and {} takes no index                      snapshot",
                    files.flt().display(),
                    files.stem,
                    files.stem
                );
                return FltAtOpen::LegacyPair;
            }
            let evidence = match files.ixm().try_exists() {
                Ok(e) => e,
                // Cannot tell: act as if it were there.
                Err(_) => true,
            };
            if evidence {
                FLT_EVIDENCE_TOTAL.fetch_add(1, Ordering::Relaxed);
                log::info!(
                    "{} is beside {}: a binary without marker logs opened the table since its last                      snapshot, so the snapshot is discarded and the filter rebuilt",
                    files.flt().display(),
                    files.ixm().display()
                );
                if let Err(e) = unlink_snapshot(files).and_then(|()| failpoint("flt_evidence_before_ixs_unlink", &files.ixs())) {
                    let why = format!("{} could not be unlinked ({e}); {} is kept as evidence", files.ixs().display(), files.flt().display());
                    log::error!("{why}; the gate starts Unknown");
                    self.word.start_unknown();
                    return FltAtOpen::Kept { why };
                }
            }
            match delete_flt(files) {
                Ok(_) => {
                    FLT_DELETED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    FltAtOpen::Deleted { evidence }
                }
                Err(e) => {
                    let why = format!("{} could not be deleted ({e})", files.flt().display());
                    log::error!("{why}; the gate starts Unknown, and the first write retries it");
                    self.word.start_unknown();
                    FltAtOpen::Kept { why }
                }
            }
        })
    }

    /// Appends `epoch + 1` to the directory's `rdb.ixe` as its own WAL
    /// transaction and waits for the directory log's sync (§6 step 2): what
    /// `VACUUM FULL` does under its exclusive gate before its first effect.
    /// `seed` is any provider of the file set, which `rdb.ixe` is opened
    /// beside. Returns the new epoch; a file set with no snapshot moves only
    /// its in-memory epoch.
    pub(crate) async fn bump_epoch<S: StorageIO>(&self, seed: &S) -> io::Result<u64> {
        let Some(files) = self.files.as_ref() else {
            return Ok(self.slot_epoch.fetch_add(1, Ordering::AcqRel) + 1);
        };
        let epoch = bump_in_epoch_log(seed, files, self.slot_epoch()).await?;
        self.slot_epoch.store(epoch, Ordering::Release);
        self.epoch_proven.store(true, Ordering::Release);
        Ok(epoch)
    }

    /// Test-only "force Clean" (B11, until B13 publishes `CLEAN`s): writes a
    /// log of `HEADER` + `CLEAN` durably and sets the word `Clean`. Panics on
    /// a word in use or a file set with no files.
    #[cfg(test)]
    pub fn force_clean_for_tests(&self) {
        let files = self.files.as_ref().expect("a file set with no snapshot cannot be clean");
        let mut log = marker::new_log(files.incarnation);
        log.extend_from_slice(&marker::encode_record(1, &marker::MarkerRecord::Clean(dummy_clean(files.incarnation))));
        write_durably(&files.ixm(), &log);
        self.set_vouched(false);
        self.word.force_state_for_tests(GateState::Clean);
    }

    /// Test-only: sets the word `Unknown`, as an unreadable log at open does.
    #[cfg(test)]
    pub fn force_unknown_for_tests(&self) {
        self.word.force_state_for_tests(GateState::Unknown);
    }

    /// Test-only: `GatedIo` judges this word's writes as a release build
    /// does (B12).
    #[cfg(test)]
    pub fn set_release_verdicts_for_tests(&self, on: bool) {
        self.release_verdicts.store(on, Ordering::Release);
    }

    /// Whether `GatedIo` judges this word's writes as a release build does:
    /// always outside tests.
    #[inline(always)]
    pub fn release_verdicts(&self) -> bool {
        #[cfg(test)]
        {
            self.release_verdicts.load(Ordering::Acquire)
        }
        #[cfg(not(test))]
        {
            true
        }
    }

    /// Test-only: the next slow path fails with nothing written (B12).
    #[cfg(test)]
    pub fn fail_next_slow_path_for_tests(&self) {
        self.fail_slow_path.store(true, Ordering::Release);
    }

    /// The file set's directory, for a file set that has one.
    pub fn dir(&self) -> Option<&Path> {
        self.files.as_ref().map(|f| f.dir.as_path())
    }
    /// The file set's stem, for a file set that has one.
    pub fn stem(&self) -> Option<&str> {
        self.files.as_ref().map(|f| f.stem.as_str())
    }

    /// The incarnation the file set was opened at (0 for a detached word).
    pub fn incarnation(&self) -> u128 {
        self.files.as_ref().map_or(0, |f| f.incarnation)
    }

    /// `.ixs.tmp`, for a file set that has one.
    pub fn ixs_tmp_path(&self) -> Option<PathBuf> {
        self.files.as_ref().map(GateFiles::ixs_tmp)
    }

    /// `.ixm.tmp`, for a file set that has one.
    pub fn ixm_tmp_path(&self) -> Option<PathBuf> {
        self.files.as_ref().map(GateFiles::ixm_tmp)
    }

    /// The maintenance lock (§3, P15). See the field.
    pub fn maintenance(&self) -> &tokio::sync::Mutex<()> {
        &self.maintenance
    }

    /// Whether the `CLEAN` the log ends in describes this process's
    /// structures (a publish of this process, or B14's load).
    pub fn vouched(&self) -> bool {
        self.vouched.load(Ordering::Acquire)
    }

    /// Records that a publish (or a load) made the log's `CLEAN` describe
    /// this process's structures.
    pub(crate) fn set_vouched(&self, on: bool) {
        self.vouched.store(on, Ordering::Release);
    }

    /// The word of this file set, carried to the stem `stem` in `dir`: what
    /// RENAME and MOVE do with the gate (§2 P6, §6; B11 deviation 2, built
    /// in B13). The new file set shares this one's word, so its state, its
    /// flags and its count (the caller family's ACCESS EXCLUSIVE ticket
    /// among them, which the target's seals then wait for until the
    /// outcome is final, N26) move with it. The incarnation moves too: a
    /// RENAME or MOVE keeps `_schema.db`'s. Registered under the new key,
    /// replacing whatever was there, so the target's open
    /// ([`FileGate::open`]) finds it while the caller holds the returned
    /// gate. Within one directory the slot epoch moves as it is; into
    /// another it is that directory's recovered `rdb.ixe` record, which is
    /// what a snapshot there must match. Not vouched: the target's open
    /// rebuilds its structures rather than loading the snapshot (§3 rule 2).
    ///
    /// The caller copies `.ixm` and `.ixs` with [`copy_files`] first, under
    /// the maintenance lock. A word with no files carries nothing: the
    /// target opens its own.
    pub fn carry_to(&self, dir: PathBuf, stem: &str) -> Option<Arc<FileGate>> {
        let from = self.files.as_ref()?;
        let files = GateFiles {
            dir,
            stem: stem.to_string(),
            incarnation: from.incarnation,
            pid: pid_of_stem(stem),
        };
        let (epoch, proven) = if files.dir == from.dir {
            (self.slot_epoch(), self.epoch_proven())
        } else {
            epoch_at_open(&files)
        };
        let gate = Arc::new(FileGate {
            word: self.word.clone(),
            files: Some(files.clone()),
            slot_epoch: AtomicU64::new(epoch),
            epoch_proven: AtomicBool::new(proven),
            maintenance: tokio::sync::Mutex::const_new(()),
            vouched: AtomicBool::new(false),
            #[cfg(test)]
            release_verdicts: AtomicBool::new(self.release_verdicts.load(Ordering::Acquire)),
            #[cfg(test)]
            fail_slow_path: AtomicBool::new(false),
        });
        GATES
            .lock()
            .map
            .get_or_insert_with(HashMap::new)
            .insert(files, Arc::downgrade(&gate));
        Some(gate)
    }
}

/// Copies the index-store pair of `from` (its `.ixm` and `.ixs`) to the stem
/// `to_stem` in `to_dir`, each made durable, then the target directory: a
/// RENAME's or MOVE's carry (B13), before its commit point, so the
/// recovery of a rename that did not commit sweeps the copies with the rest
/// of the target's files. A file the source lacks is removed at the target,
/// so a leftover there never pairs with the carried one; the target's
/// `.tmp`s go too. The caller holds `from`'s maintenance lock, so no seal is
/// writing either file meanwhile.
pub fn copy_files(from: &FileGate, to_dir: &Path, to_stem: &str) -> io::Result<()> {
    use std::io::Write;
    let Some(files) = from.files.as_ref() else {
        return Ok(());
    };
    let mut last = None;
    for (src, ext) in [(files.ixm(), ".ixm"), (files.ixs(), ".ixs")] {
        let dst = to_dir.join(format!("{to_stem}{ext}"));
        if src == dst {
            continue;
        }
        let tmp = to_dir.join(format!("{to_stem}{ext}.tmp"));
        if tmp.exists() {
            dir_op(DirOp::Unlink(&tmp));
            let _ = std::fs::remove_file(&tmp);
        }
        match std::fs::read(&src) {
            Ok(bytes) => {
                dir_op(DirOp::Create(&dst));
                let mut f = std::fs::File::create(&dst)?;
                f.write_all(&bytes)?;
                crate::core::sync_audit::note("index_store.carry", &dst);
                f.sync_data()?;
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => match std::fs::remove_file(&dst) {
                Ok(()) => dir_op(DirOp::Unlink(&dst)),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            },
            Err(e) => return Err(e),
        }
        last = Some(dst);
    }
    match last {
        Some(dst) => crate::core::wal::sync_parent_dir(&dst),
        None => Ok(()),
    }
}

/// Sets `INDEX_STORE_V1` in `dir`, and in the database's root when that is
/// another directory (a storage pool's): the root is the one directory
/// `incompat::check` reads at open (B11 deviation 4, fixed in B13). Called
/// before the first `.ixm` of a directory is created.
pub fn require_v1(dir: &Path, root: Option<&Path>) -> io::Result<()> {
    #[cfg(feature = "persistent_indexes")]
    {
        use crate::core::database::incompat::{INDEX_STORE_V1, require};
        require(dir, INDEX_STORE_V1)?;
        if let Some(root) = root
            && root != dir
        {
            require(root, INDEX_STORE_V1)?;
        }
    }
    #[cfg(not(feature = "persistent_indexes"))]
    let _ = (dir, root);
    Ok(())
}

/// Test-only: the live word of the file set `file` (a file name in `dir`)
/// belongs to, if one is open: what `GatedIo` asks when a provider with no
/// word writes, to catch a write that reaches a live file set around its
/// gate (B12). A file belongs to a stem when it is the stem plus one of a
/// table's file suffixes (`database::RENAMED_SUFFIXES`); the database's
/// clash guard keeps that unambiguous among live tables.
#[cfg(test)]
pub fn live_gate_for(dir: &Path, file: &str) -> Option<Arc<FileGate>> {
    let reg = GATES.lock();
    let map = reg.map.as_ref()?;
    map.iter()
        .filter(|(f, _)| f.dir == dir && owns(&f.stem, file))
        .find_map(|(_, w)| w.upgrade())
}

/// Whether `file` is one of the stem's files.
#[cfg(test)]
fn owns(stem: &str, file: &str) -> bool {
    file.strip_prefix(stem)
        .is_some_and(|rest| crate::core::database::RENAMED_SUFFIXES.contains(&rest))
}

/// A `CLEAN` that vouches for a snapshot that does not exist: what the
/// force-Clean hook writes.
#[cfg(test)]
pub(crate) fn dummy_clean(incarnation: u128) -> marker::CleanRecord {
    use super::identity::{Identity, VersionTuple};
    marker::CleanRecord {
        identity: Identity {
            incarnation,
            slot_epoch: 0,
            n_slots: 0,
            pointers_len: 0,
            rows_len: 0,
            versions: VersionTuple::current(0),
            shape_hash: 0,
            filter_config_hash: 0,
        },
        snapshot_len: 0,
        footer: 0,
    }
}

#[cfg(test)]
fn write_durably(path: &Path, bytes: &[u8]) {
    use std::io::Write;
    let mut f = std::fs::File::create(path).expect("create the marker log");
    f.write_all(bytes).expect("write the marker log");
    f.sync_all().expect("sync the marker log");
    crate::core::wal::sync_parent_dir(path).expect("sync the directory");
}

/// The `Clean` slow path: `DIRTY` after the last good record (module
/// documentation). Falls back to unlinking `.ixs` when it cannot be made
/// durable.
fn append_dirty(files: &GateFiles) -> io::Result<()> {
    match try_append_dirty(files) {
        Ok(()) => Ok(()),
        Err(e) => {
            log::error!(
                "{}: DIRTY could not be made durable ({e}); the snapshot is unlinked instead",
                files.ixm().display()
            );
            unlink_snapshot(files).map_err(|u| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "{}: DIRTY could not be made durable ({e}), and the snapshot could not be \
                         unlinked either ({u}); nothing was written",
                        files.ixm().display()
                    ),
                )
            })
        }
    }
}

fn try_append_dirty(files: &GateFiles) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    let path = files.ixm();
    let file = match std::fs::OpenOptions::new().read(true).write(true).open(&path) {
        Ok(f) => f,
        // Gone since the open read it: no CLEAN is left to revoke.
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let mut bytes = Vec::new();
    {
        use std::io::Read;
        (&file).read_to_end(&mut bytes)?;
    }
    let scan = match marker::scan(&bytes) {
        Ok(scan) if scan.incarnation == files.incarnation => scan,
        // Another incarnation's log, or one that no longer reads: trust none
        // of it.
        _ => {
            drop(file);
            return rewrite_unknown(files);
        }
    };
    // Already `DIRTY` (a past run's, durable since it is what the file
    // reads after a restart, or this run's, synced before it returned).
    if scan.state() == marker::LogState::Dirty {
        return Ok(());
    }
    for op in scan.plan_append(Append::Dirty).ops {
        match op {
            marker::MarkerOp::Write { offset, bytes } => {
                file.write_all_at(&bytes, offset)?;
                failpoint("dirty_append_before_fsync", &path)?;
                failpoint("dirty_enospc_after_first_write", &path)?;
            }
            marker::MarkerOp::Truncate { len } => {
                file.set_len(len)?;
            }
        }
        // After every step, before the next one is issued: until the
        // truncate is durable a stale record past the new one could read as
        // its successor (owner's decision (a)).
        crate::core::sync_audit::note("index_store.marker", &path);
        file.sync_data()?;
    }
    failpoint("dirty_after_fsync", &path)?;
    Ok(())
}

/// The `Unknown` slow path: the log rewritten as `HEADER` + `DIRTY`, or
/// `.ixs` unlinked.
fn rewrite_unknown(files: &GateFiles) -> io::Result<()> {
    match try_rewrite(files) {
        Ok(()) => Ok(()),
        Err(e) => {
            log::error!(
                "{}: the marker log could not be rewritten ({e}); the snapshot is unlinked instead",
                files.ixm().display()
            );
            unlink_snapshot(files).map_err(|u| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "{}: the marker log could not be rewritten ({e}), and the snapshot could \
                         not be unlinked either ({u}); nothing was written",
                        files.ixm().display()
                    ),
                )
            })
        }
    }
}

fn try_rewrite(files: &GateFiles) -> io::Result<()> {
    use std::io::Write;
    #[cfg(feature = "persistent_indexes")]
    crate::core::database::incompat::require(&files.dir, crate::core::database::incompat::INDEX_STORE_V1)?;
    let tmp = files.ixm_tmp();
    let path = files.ixm();
    dir_op(DirOp::Create(&tmp));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&marker::compacted_log(files.incarnation))?;
        crate::core::sync_audit::note("index_store.marker_tmp", &tmp);
        f.sync_all()?;
    }
    failpoint("unknown_rewrite_after_first_write", &path)?;
    dir_op(DirOp::Rename { from: &tmp, to: &path });
    std::fs::rename(&tmp, &path)?;
    crate::core::wal::sync_parent_dir(&path)
}

/// Unlinks `.ixs` and fsyncs the directory: with no snapshot, nothing is
/// left for a `CLEAN` to vouch for.
fn unlink_snapshot(files: &GateFiles) -> io::Result<()> {
    let ixs = files.ixs();
    dir_op(DirOp::Unlink(&ixs));
    match std::fs::remove_file(&ixs) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    crate::core::sync_audit::note("unlink", &ixs);
    crate::core::wal::sync_parent_dir(&ixs)
}

/// Every index-store file a stem has (§1).
pub const INDEX_STORE_SUFFIXES: [&str; 4] = [".ixm", ".ixm.tmp", ".ixs", ".ixs.tmp"];

/// Unlinks the index-store files of the stem `stem` in `dir` (`.ixm`, `.ixs`
/// and their `.tmp`s), the directory sync left to the caller's sweep: what a
/// DROP, and a RENAME's or MOVE's source, leaves of the file set. Failures
/// are logged; a leftover is rejected by its incarnation at the next open of
/// the name, unless the same table comes back to it, which is why it is
/// removed here.
pub fn remove_files(dir: &Path, stem: &str) {
    for suffix in INDEX_STORE_SUFFIXES {
        let path = dir.join(format!("{stem}{suffix}"));
        if !path.exists() {
            continue;
        }
        dir_op(DirOp::Unlink(&path));
        if let Err(e) = std::fs::remove_file(&path) {
            if e.kind() != io::ErrorKind::NotFound {
                log::error!("Could not remove {}: {e}", path.display());
            }
        }
    }
}

/// Serialises appends to every directory's `rdb.ixe` in the process: a bump
/// reads the file to choose its offset and epoch. Bumps are rare (one per
/// `VACUUM FULL`), so one lock for all directories costs nothing.
static EPOCH_APPEND: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn bump_in_epoch_log<S: StorageIO>(seed: &S, files: &GateFiles, in_memory: u64) -> io::Result<u64> {
    let _serial = EPOCH_APPEND.lock().await;
    let io = seed.create_new(EPOCH_LOG_FILE.to_string()).await;
    let len = io.get_len().await;
    let mut bytes = vec![0u8; len as usize];
    if len > 0 {
        let mut pos = 0u64;
        io.read_data_into_buffer(&mut pos, &mut bytes).await?;
    }
    let path = files.dir.join(EPOCH_LOG_FILE);
    let log = epoch_log::decode(&bytes).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} cannot be read ({e}), so VACUUM FULL cannot record its slot epoch and \
                 moved nothing. The file is WAL'd, so a bad record is damage: restore it, or \
                 remove it (every index snapshot in {} is then rebuilt once)",
                path.display(),
                files.dir.display()
            ),
        )
    })?;
    let mut record = log.bump(files.incarnation, files.pid);
    record.slot_epoch = record.slot_epoch.max(in_memory + 1);
    let mut out = Vec::with_capacity(epoch_log::HEADER_LEN + epoch_log::RECORD_LEN);
    if len == 0 {
        out.extend_from_slice(&epoch_log::header());
    }
    out.extend_from_slice(&record.encode());
    let txn = io.begin_txn();
    let token = io.write_data_txn(txn, len, &out).await;
    if let Err(e) = io.commit_durable(token).await {
        // Ended either way, as compaction's own batches are: a transaction
        // left open holds the log's pending count up and it never
        // checkpoints again. Nothing has moved yet, and a bump that did reach
        // the disk only rejects snapshots, which is safe.
        io.abort_txn(txn).await;
        return Err(e);
    }
    if token == 0 {
        // No log in this directory: the provider wrote in place, and the
        // record must be durable before the first effect all the same.
        crate::core::sync_audit::note("index_store.epoch", &path);
        std::fs::File::options().write(true).open(&path)?.sync_data()?;
    }
    Ok(record.slot_epoch)
}

#[cfg(test)]
#[path = "file_gate_tests.rs"]
mod tests;
