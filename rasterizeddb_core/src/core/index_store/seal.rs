//! Seal and publish: a file set's snapshot written at a clean point
//! (docs/persistent-indexes.md §3; slice B13). Phase 1 writes the FILTER
//! section only; B17 adds INDEX sections (B-tree, hash and path, under
//! `index_snapshot.index_storage = snapshot`), B14 and B17 load what this
//! writes.
//!
//! # The sequence, under the file set's maintenance lock
//!
//! 0. **Whether to write.** A file set with no directory is excluded (P33).
//!    A word that is `Clean` and vouched (this process published the
//!    `CLEAN`, or loaded it) has nothing to add. A `Clean` word that is not
//!    vouched, or an `Unknown` one, first takes and drops a ticket, so
//!    `DIRTY` is durable before `.ixs` is replaced (§3 rule 2). The target's
//!    `before_seal` runs then, under that transient ticket: the filter's
//!    checkpoint bookkeeping is reset (B14; §3 step 7's `mark_checkpointed`,
//!    moved here since no ticket may be held after `CLEAN`).
//! 1. **Seal** (`fetch_or(SEALED)`, with `MANDATORY` for shutdown and
//!    `rdb_index_checkpoint`). An optional seal that finds tickets skips at
//!    once; a mandatory one waits up to `seal_timeout` for them, new ones
//!    queueing behind it. A ticket during an optional seal sets `CANCEL`
//!    (B10/B12): the sealer checks it before each step and abandons.
//! 2. **Land** every provider's staged appends durably, through the
//!    sealer's own count ([`WriteGate::seal_guard`]), dropped before step 6.
//! 3. **Eligibility**: the target's (indexes and filter ready, not demoted,
//!    no bulk load, an epoch `rdb.ixe` proves, `outcome_revocations`
//!    unchanged since the table opened).
//! 4. **Barrier**: the status log flushed, then the root's and the
//!    directory's logs synced through [`Wal::sync_issued`] (poison checked
//!    first; never `sync_pending`).
//! 5. **Capture**: the identity, sampled after the landing, and the
//!    sections, into `.ixs.tmp` with `sync_file_range` writeback every
//!    `writeback_mb`; fdatasync.
//! 6. **Publish (T3)**: rename to `.ixs`, fsync the directory; set
//!    `INDEX_STORE_V1` in the directory and the root; compact the marker
//!    log when it is missing, foreign, not ending `DIRTY` or long
//!    (`HEADER` + `DIRTY` through `.ixm.tmp`, fsync, rename, directory
//!    fsync); re-read `outcome_revocations`; the `PUBLISHING`
//!    compare-and-swap (`CANCEL` clear and a count of zero); append `CLEAN`
//!    (identity, `.ixs` length, footer) by `MarkerScan::plan_append`:
//!    pwrite, fdatasync, ftruncate past it if stale bytes followed,
//!    fdatasync.
//! 7. **Unseal**: `Clean` and vouched on success. A failure before
//!    `PUBLISHING` leaves `Dirty` and unlinks `.ixs.tmp`; a failure once the
//!    `CLEAN` write was issued leaves `Unknown`, whose next ticket rewrites
//!    the log or unlinks `.ixs` before anything is issued.
//!
//! So a `CLEAN` is never durable before the `.ixs` it names is: the
//! snapshot's fdatasync, rename and directory fsync all return before the
//! `CLEAN`'s `pwrite` is issued.
//!
//! [`WriteGate::seal_guard`]: super::gate_word::WriteGate::seal_guard
//! [`Wal::sync_issued`]: crate::core::wal::Wal::sync_issued

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::file_gate::{DirOp, FileGate, blocking, dir_op, failpoint};
use super::gate_word::{GateState, PublishEnd, SealEnd, SealKind, SealStart};
use super::identity::Identity;
use super::marker::{self, Append, CleanRecord, LogState, MarkerScan};
use super::snapshot::{self, SectionKind};

/// The tunables of §3 (`index_snapshot.*`), process-wide, changed by `SET
/// CONFIG index_snapshot.<key> = …`.
pub mod settings {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static IDLE_CHECKPOINT_SECS: AtomicU64 = AtomicU64::new(300);
    static SEAL_TIMEOUT_MS: AtomicU64 = AtomicU64::new(5_000);
    static SHUTDOWN_DEADLINE_SECS: AtomicU64 = AtomicU64::new(120);
    static WRITEBACK_MB: AtomicU64 = AtomicU64::new(64);

    /// How long a plain table or partition must go with no ticket before
    /// the idle checkpoint seals it (Q6, default 300 s). 0 turns it off.
    pub fn idle_checkpoint_secs() -> u64 {
        IDLE_CHECKPOINT_SECS.load(Ordering::Relaxed)
    }

    pub fn set_idle_checkpoint_secs(secs: u64) {
        IDLE_CHECKPOINT_SECS.store(secs, Ordering::Relaxed);
    }

    /// How long a mandatory seal waits for outstanding tickets (5 s).
    pub fn seal_timeout() -> Duration {
        Duration::from_millis(SEAL_TIMEOUT_MS.load(Ordering::Relaxed))
    }

    pub fn set_seal_timeout_ms(ms: u64) {
        SEAL_TIMEOUT_MS.store(ms, Ordering::Relaxed);
    }

    /// The whole of shutdown's sealing, `seal_timeout` waits included
    /// (120 s). Keep it below the unit's `TimeoutStopSec`.
    pub fn shutdown_deadline() -> Duration {
        Duration::from_secs(SHUTDOWN_DEADLINE_SECS.load(Ordering::Relaxed))
    }

    pub fn set_shutdown_deadline_secs(secs: u64) {
        SHUTDOWN_DEADLINE_SECS.store(secs, Ordering::Relaxed);
    }

    /// How many MiB of `.ixs.tmp` are written between two
    /// `sync_file_range(SYNC_FILE_RANGE_WRITE)` calls (64).
    pub fn writeback_mb() -> u64 {
        WRITEBACK_MB.load(Ordering::Relaxed).max(1)
    }

    pub fn set_writeback_mb(mb: u64) {
        WRITEBACK_MB.store(mb.max(1), Ordering::Relaxed);
    }
}

/// `rdb_index_snapshot_failed_total`: every seal that skipped or failed
/// after it started (a file set with nothing to do is not counted).
static FAILED_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Seals that published a `CLEAN`.
static PUBLISHED_TOTAL: AtomicU64 = AtomicU64::new(0);
/// `rdb_index_snapshot_skipped_shutdown_total`: file sets shutdown's
/// deadline left unsealed.
static SKIPPED_SHUTDOWN_TOTAL: AtomicU64 = AtomicU64::new(0);

/// `rdb_index_snapshot_failed_total`.
pub fn failed_total() -> u64 {
    FAILED_TOTAL.load(Ordering::Relaxed)
}

/// Seals that published.
pub fn published_total() -> u64 {
    PUBLISHED_TOTAL.load(Ordering::Relaxed)
}

/// `rdb_index_snapshot_skipped_shutdown_total`.
pub fn skipped_shutdown_total() -> u64 {
    SKIPPED_SHUTDOWN_TOTAL.load(Ordering::Relaxed)
}

pub(crate) fn note_skipped_shutdown(n: u64) {
    SKIPPED_SHUTDOWN_TOTAL.fetch_add(n, Ordering::Relaxed);
}

/// Why a seal wrote no `CLEAN`, with the state left as it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Skip {
    /// The instance was retired (§7 step 2): a retired instance never
    /// snapshots.
    Retired,
    /// An optional seal found tickets, or a count at its `PUBLISHING`
    /// compare-and-swap.
    Busy,
    /// A mandatory seal's tickets outlasted `seal_timeout`.
    Timeout,
    /// A writer (or a ticketless write, or retirement) cancelled it.
    Cancelled,
    /// Not eligible (§3 step 3): why.
    Ineligible(String),
    /// `outcome_revocations` moved (N26).
    Revoked,
    /// The word was not `Dirty` when the seal looked.
    NotDirty,
}

/// How a seal ended.
#[derive(Debug)]
pub enum SealOutcome {
    /// `CLEAN` is durable; the word is `Clean`.
    Published,
    /// The word is `Clean` and vouched: the snapshot is current.
    AlreadyClean,
    /// No directory: no snapshot (P33).
    Excluded,
    /// Nothing published; the state is what it was (`Dirty`).
    Skipped(Skip),
    /// An I/O error before `PUBLISHING`: the state stays `Dirty`.
    Failed(io::Error),
    /// An error once `CLEAN`'s write was issued: the state is `Unknown`.
    FailedAfterClean(io::Error),
}

impl SealOutcome {
    /// Whether the file set's log now proves a snapshot of its data.
    pub fn is_clean(&self) -> bool {
        matches!(self, SealOutcome::Published | SealOutcome::AlreadyClean)
    }

    /// One line for a log or a notice.
    pub fn describe(&self) -> String {
        match self {
            SealOutcome::Published => "published".to_string(),
            SealOutcome::AlreadyClean => "already clean".to_string(),
            SealOutcome::Excluded => "excluded (no directory)".to_string(),
            SealOutcome::Skipped(s) => format!("skipped: {s:?}"),
            SealOutcome::Failed(e) => format!("failed: {e}"),
            SealOutcome::FailedAfterClean(e) => format!("failed after CLEAN was issued: {e}"),
        }
    }
}

/// What a seal captured: the identity and the sections, in table order.
pub struct Capture {
    pub identity: Identity,
    pub sections: Vec<(SectionKind, u16, Vec<u8>)>,
}

/// What a seal needs of the table whose file set it seals. Implemented by
/// the row layer's `SealParts` (one per open instance).
pub trait SealTarget: Send + Sync {
    /// The file set's gate.
    fn gate(&self) -> &Arc<FileGate>;
    /// For logs.
    fn name(&self) -> &str;
    /// Whether the instance was retired (§7 step 2).
    fn retired(&self) -> bool;
    /// Waits for a load of the snapshot's INDEX sections in flight (B17):
    /// until it has finished or failed, the seal cannot tell whether the
    /// snapshot is current, and the indexes are not ready.
    fn settle(&self) -> impl Future<Output = ()> + Send;
    /// Whether the snapshot the log's `CLEAN` names holds everything a seal
    /// of this instance would write now (B17: its INDEX sections, when
    /// `index_storage` asks for them). A vouched word whose snapshot is not
    /// current is sealed again (§3 rule 2).
    fn snapshot_current(&self) -> bool;
    /// Run before the seal, under a transient ticket the seal holds: what
    /// must change while the word is still `Dirty` and would otherwise
    /// change after the `CLEAN` (B14: the filter's checkpoint bookkeeping).
    fn before_seal(&self, ticket: &super::gate::WriteTicket) -> impl Future<Output = ()> + Send;
    /// §3 step 2: every provider's staged appends landed durably.
    fn land_staged(&self) -> impl Future<Output = io::Result<()>> + Send;
    /// §3 step 3: `Some(why)` when the snapshot must not be taken.
    fn ineligible(&self) -> Option<String>;
    /// Whether the instance's database revoked a commit after it opened
    /// (N26; `mvcc::status::outcome_revocations`).
    fn revoked_since_open(&self) -> bool;
    /// §3 step 4.
    fn barrier(&self) -> io::Result<()>;
    /// The database's root directory, where `incompat::check` reads.
    fn root_dir(&self) -> Option<PathBuf>;
    /// §3 step 5's sample, after the landing.
    fn capture(&self) -> impl Future<Output = io::Result<Capture>> + Send;
}

/// A named failpoint at `path` (`core::power_loss::failpoint`), for a
/// seal target's barrier; nothing outside tests.
pub(crate) fn failpoint_at(name: &str, path: &Path) -> io::Result<()> {
    failpoint(name, path)
}

/// A marker log longer than this is compacted at the next publish.
const COMPACT_AT: u64 = 64 * 1024;

/// Seals and publishes `t`'s file set (module documentation).
pub async fn seal<T: SealTarget>(t: &T, kind: SealKind) -> SealOutcome {
    let gate = t.gate().clone();
    if !gate.has_files() {
        return SealOutcome::Excluded;
    }
    let _maintenance = gate.maintenance().lock().await;
    if t.retired() {
        return SealOutcome::Skipped(Skip::Retired);
    }
    t.settle().await;
    let state = gate.state();
    if state == GateState::Clean && gate.vouched() && t.snapshot_current() {
        return SealOutcome::AlreadyClean;
    }
    // §3: "When the state is not Dirty, the snapshot task first takes and
    // drops a WriteTicket, so DIRTY is durable before .ixs is replaced."
    // Taken in every state: it is also the ticket `before_seal` writes
    // under.
    match super::gate::ticket(&gate, None).await {
        Ok(ticket) => {
            t.before_seal(&ticket).await;
            drop(ticket);
        }
        Err(e) => {
            FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return SealOutcome::Failed(e);
        }
    }
    let word = gate.word().clone();
    match word.seal(kind) {
        SealStart::NotDirty => return skipped(Skip::NotDirty),
        SealStart::Sealed { outstanding } if outstanding > 0 => match kind {
            SealKind::Optional => return abandon(&gate, Skip::Busy),
            SealKind::Mandatory => {
                if let Err(why) = drained_within(&word, settings::seal_timeout()).await {
                    return abandon(&gate, why);
                }
            }
        },
        SealStart::Sealed { .. } => {}
    }
    match sealed(t, &gate, kind).await {
        Ok(()) => {
            PUBLISHED_TOTAL.fetch_add(1, Ordering::Relaxed);
            SealOutcome::Published
        }
        Err(Ended::Skip(s)) => abandon(&gate, s),
        Err(Ended::Failed(e)) => {
            cleanup_tmp(&gate);
            word.unseal(SealEnd::Abandoned);
            FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
            log::warn!("index snapshot of {} failed: {e}", t.name());
            SealOutcome::Failed(e)
        }
        Err(Ended::AfterClean(e)) => {
            // Before any waiter is released: the CLEAN page may persist now
            // or later.
            word.unseal(SealEnd::FailedAfterCleanIssued);
            FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
            log::error!(
                "index snapshot of {}: the CLEAN record's write was issued and then failed ({e}); \
                 the marker log is untrusted until the next write rewrites it",
                t.name()
            );
            SealOutcome::FailedAfterClean(e)
        }
    }
}

enum Ended {
    Skip(Skip),
    Failed(io::Error),
    AfterClean(io::Error),
}

impl From<io::Error> for Ended {
    fn from(e: io::Error) -> Ended {
        Ended::Failed(e)
    }
}

fn skipped(s: Skip) -> SealOutcome {
    FAILED_TOTAL.fetch_add(1, Ordering::Relaxed);
    SealOutcome::Skipped(s)
}

/// A seal that publishes nothing: `.ixs.tmp` unlinked, the word unsealed
/// with its state as it was, waiters woken.
fn abandon(gate: &FileGate, s: Skip) -> SealOutcome {
    cleanup_tmp(gate);
    gate.word().unseal(SealEnd::Abandoned);
    skipped(s)
}

fn cleanup_tmp(gate: &FileGate) {
    if let Some(tmp) = gate.ixs_tmp_path()
        && tmp.exists()
    {
        dir_op(DirOp::Unlink(&tmp));
        if let Err(e) = std::fs::remove_file(&tmp)
            && e.kind() != io::ErrorKind::NotFound
        {
            log::warn!("{} could not be removed: {e}", tmp.display());
        }
    }
}

/// Waits until no ticket is counted, up to `limit`, or until the seal is
/// cancelled (retirement cancels a mandatory seal too, §7 step 2(a), and
/// must not wait out `seal_timeout` for a ticket its own DDL's family
/// holds).
async fn drained_within(word: &super::gate_word::WriteGate, limit: Duration) -> Result<(), Skip> {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        let w = word.load();
        if w.count() == 0 {
            return Ok(());
        }
        if w.cancelled() {
            return Err(Skip::Cancelled);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(Skip::Timeout);
        }
        let step = (deadline - now).min(Duration::from_millis(10));
        let _ = tokio::time::timeout(step, word.drained()).await;
    }
}

fn check_cancel(gate: &FileGate) -> Result<(), Ended> {
    if gate.word().load().cancelled() {
        Err(Ended::Skip(Skip::Cancelled))
    } else {
        Ok(())
    }
}

/// Steps 2-7 on a sealed, drained word.
async fn sealed<T: SealTarget>(t: &T, gate: &Arc<FileGate>, kind: SealKind) -> Result<(), Ended> {
    let word = gate.word().clone();
    check_cancel(gate)?;
    // Step 2, under the sealer's own count.
    {
        let _guard = word.seal_guard();
        t.land_staged().await?;
    }
    check_cancel(gate)?;
    // Step 3.
    if let Some(why) = t.ineligible() {
        return Err(Ended::Skip(Skip::Ineligible(why)));
    }
    if t.revoked_since_open() {
        return Err(Ended::Skip(Skip::Revoked));
    }
    // Step 4.
    blocking(|| t.barrier())?;
    check_cancel(gate)?;
    // Step 5. A capture that a writer cancelled midway is a skip.
    let capture = match t.capture().await {
        Ok(c) => c,
        Err(e) => {
            check_cancel(gate)?;
            return Err(e.into());
        }
    };
    check_cancel(gate)?;
    let sections: Vec<(SectionKind, u16, &[u8])> =
        capture.sections.iter().map(|(k, o, b)| (*k, *o, b.as_slice())).collect();
    let bytes = snapshot::encode(capture.identity, &sections);
    let footer = u64::from_le_bytes(bytes[bytes.len() - snapshot::FOOTER_LEN..].try_into().expect("eight bytes"));
    let tmp = gate.ixs_tmp_path().expect("a file set with files");
    let ixs = gate.ixs_path().expect("a file set with files");
    let ixm = gate.ixm_path().expect("a file set with files");
    blocking(|| write_tmp(&tmp, &bytes))?;
    check_cancel(gate)?;
    // Step 6.
    blocking(|| publish_ixs(&tmp, &ixs))?;
    let root = t.root_dir();
    let dir = gate.dir().expect("a file set with files").to_path_buf();
    let scan = blocking(|| prepare_marker(gate, &ixm, &dir, root.as_deref()))?;
    if t.revoked_since_open() {
        return Err(Ended::Skip(Skip::Revoked));
    }
    publishing(gate, kind).await?;
    let record = CleanRecord {
        identity: capture.identity,
        snapshot_len: bytes.len() as u64,
        footer,
    };
    match blocking(|| append_clean(&ixm, &scan, record)) {
        Ok(()) => {}
        Err((false, e)) => {
            // Nothing of the CLEAN reached the file: the PUBLISHING window
            // ends with the state as it was.
            return Err(Ended::Failed(e));
        }
        Err((true, e)) => return Err(Ended::AfterClean(e)),
    }
    // Step 7.
    gate.set_vouched(true);
    word.unseal(SealEnd::Published);
    Ok(())
}

/// The `PUBLISHING` compare-and-swap (§3 step 6): an optional seal skips
/// on any count, a mandatory one retries while the count is transient
/// back-outs or a registered write still returning, up to `seal_timeout`.
async fn publishing(gate: &FileGate, kind: SealKind) -> Result<(), Ended> {
    let deadline = tokio::time::Instant::now() + settings::seal_timeout();
    loop {
        match gate.word().try_publish() {
            PublishEnd::Publishing => return Ok(()),
            PublishEnd::Cancelled => return Err(Ended::Skip(Skip::Cancelled)),
            PublishEnd::NotSealed => {
                return Err(Ended::Failed(io::Error::other(
                    "the seal's word was unsealed under it (a bug)",
                )));
            }
            PublishEnd::Busy { .. } => match kind {
                SealKind::Optional => return Err(Ended::Skip(Skip::Busy)),
                SealKind::Mandatory => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(Ended::Skip(Skip::Timeout));
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            },
        }
    }
}

/// `.ixs.tmp`, written whole and fdatasynced (§3 steps 5 and 6).
fn write_tmp(tmp: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    dir_op(DirOp::Create(tmp));
    let mut f = std::fs::File::create(tmp)?;
    let step = (settings::writeback_mb() as usize).saturating_mul(1 << 20).max(1 << 20);
    let mut done = 0usize;
    for chunk in bytes.chunks(step) {
        f.write_all(chunk)?;
        // Start writeback of what is written, so the fdatasync below has
        // little left and a large snapshot never queues gigabytes ahead of
        // a writer's commit fsync on the same device (P12).
        crate::core::wal::start_writeback(&f, done as u64, chunk.len() as u64);
        done += chunk.len();
    }
    let _ = done;
    failpoint("snapshot_before_fsync", tmp)?;
    crate::core::sync_audit::note("index_store.snapshot", tmp);
    f.sync_data()?;
    failpoint("snapshot_after_fsync", tmp)
}

/// Renames `.ixs.tmp` over `.ixs` and fsyncs the directory.
fn publish_ixs(tmp: &Path, ixs: &Path) -> io::Result<()> {
    dir_op(DirOp::Rename { from: tmp, to: ixs });
    std::fs::rename(tmp, ixs)?;
    failpoint("snapshot_after_rename", ixs)?;
    crate::core::wal::sync_parent_dir(ixs)?;
    failpoint("snapshot_after_dir_fsync", ixs)
}

/// The marker log a `CLEAN` is appended to, compacted first when it is
/// missing, another incarnation's, unreadable, not ending `DIRTY`, or long
/// (§1). Sets `INDEX_STORE_V1` in the directory and the root first: the
/// first `.ixm` of a directory is created here.
fn prepare_marker(gate: &FileGate, ixm: &Path, dir: &Path, root: Option<&Path>) -> io::Result<MarkerScan> {
    super::file_gate::require_v1(dir, root)?;
    let bytes = match std::fs::read(ixm) {
        Ok(b) => Some(b),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };
    let current = bytes
        .as_deref()
        .and_then(|b| marker::scan(b).ok())
        .filter(|s| s.incarnation == gate.incarnation());
    if let Some(s) = current
        && s.state() == LogState::Dirty
        && s.file_len <= COMPACT_AT
    {
        return Ok(s);
    }
    let log = marker::compacted_log(gate.incarnation());
    let tmp = gate.ixm_tmp_path().expect("a file set with files");
    {
        use std::io::Write;
        dir_op(DirOp::Create(&tmp));
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&log)?;
        crate::core::sync_audit::note("index_store.marker_tmp", &tmp);
        f.sync_all()?;
    }
    dir_op(DirOp::Rename { from: &tmp, to: ixm });
    std::fs::rename(&tmp, ixm)?;
    failpoint("marker_compaction_after_rename", ixm)?;
    crate::core::wal::sync_parent_dir(ixm)?;
    marker::scan(&log).map_err(|e| io::Error::other(format!("a compacted log does not read back: {e}")))
}

/// Appends `CLEAN` by `plan_append`: pwrite, fdatasync, and when stale bytes
/// followed, ftruncate and fdatasync, nothing issued in between. The error
/// says whether any of the record's write was issued.
fn append_clean(ixm: &Path, scan: &MarkerScan, record: CleanRecord) -> Result<(), (bool, io::Error)> {
    use std::os::unix::fs::FileExt;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(ixm)
        .map_err(|e| (false, e))?;
    let mut issued = false;
    for op in scan.plan_append(Append::Clean(record)).ops {
        match op {
            marker::MarkerOp::Write { offset, bytes } => {
                issued = true;
                file.write_all_at(&bytes, offset).map_err(|e| (true, e))?;
                failpoint("clean_append_before_fsync", ixm).map_err(|e| (true, e))?;
            }
            marker::MarkerOp::Truncate { len } => file.set_len(len).map_err(|e| (true, e))?,
        }
        crate::core::sync_audit::note("index_store.marker", ixm);
        failpoint("clean_fsync_fails", ixm).map_err(|e| (issued, e))?;
        file.sync_data().map_err(|e| (issued, e))?;
    }
    failpoint("clean_after_fsync", ixm).map_err(|e| (issued, e))
}

#[cfg(test)]
#[path = "seal_tests.rs"]
mod tests;
