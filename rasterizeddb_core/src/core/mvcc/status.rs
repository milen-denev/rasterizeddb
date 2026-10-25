//! The durable record of how each transaction ended.
//!
//! Version stamps on disk are only meaningful next to the outcome of the
//! transaction that wrote them: a row stamped `xmin = 41` is either real data or
//! the debris of a transaction that aborted, and nothing in the row itself says
//! which. This is where that is written down, so the answer survives a restart.
//!
//! Two bits per transaction, packed four to a byte and indexed by id, so the
//! whole thing is an array lookup rather than a search. PostgreSQL's `pg_xact`
//! keeps the same shape for the same reason.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering};

use arc_swap::ArcSwap;

use cacheguard::CacheGuard;

use parking_lot::Mutex;

use super::transaction::{TxId, TxOutcome};
use crate::core::wal::Wal;

/// Bit patterns. `InProgress` is zero so a freshly extended file reads as
/// "started but unfinished", which is the safe reading of a torn write: such a
/// transaction is treated as aborted by anything that outlives it.
const BITS_IN_PROGRESS: u8 = 0b00;
const BITS_COMMITTED: u8 = 0b01;
const BITS_ABORTED: u8 = 0b10;

const ENTRIES_PER_BYTE: u64 = 4;

/// Grow the file in whole pages rather than a byte at a time.
const GROWTH_CHUNK: usize = 4096;

/// Bytes per level, and how many levels there are.
///
/// Level `l` holds `BASE << l` bytes and starts at byte `BASE * ((1 << l) - 1)`,
/// so 32 levels cover more of the id space than a machine could hold bitmap for
/// — the allocation of a level fails long before the table runs out.
const BASE: usize = GROWTH_CHUNK;
const LEVELS: usize = 32;

/// The packed outcome bits, readable without taking a lock.
///
/// `outcome` is asked about one or two transactions for *every row* a scan
/// looks at, from every worker at once. Holding the log's `Mutex` to answer
/// made a two-bit lookup a contended acquire: eight workers spent 29% of their
/// cycles in `lock_slow`, and the scan finished no sooner than it did on one
/// worker. See `concurrent_readers_see_a_writers_outcomes`.
///
/// So the bytes live in levels that are allocated once and never move. A reader
/// needs one atomic load of the level pointer and one of the byte; a writer
/// still holds the `Mutex`, which is what keeps the read-modify-write of a byte
/// shared by four transactions atomic, and still guards everything else in the
/// log. Growth only ever publishes a new level, so a pointer a reader is
/// holding stays valid.
struct Bitmap {
    levels: [AtomicPtr<AtomicU8>; LEVELS],
    /// Logical length: bytes that have been recorded into, rounded up to
    /// [`GROWTH_CHUNK`] exactly as the old `Vec::resize` did. Deliberately not
    /// the allocated capacity — an id past this reads as aborted, and moving
    /// that boundary would change which unrecorded ids read as in-progress.
    /// Absolute: it counts the `base` bytes this map does not hold.
    len: AtomicUsize,
    /// The first byte this map holds. Everything below it was settled into a
    /// [`Settled`] at open and is never asked of the map — see
    /// [`StatusLog::outcome`]. Fixed at construction; zero without settling.
    base: usize,
}

impl Bitmap {
    fn new() -> Self {
        Self::with_base(0)
    }

    /// A map holding bytes from `base` on; the levels are indexed from there,
    /// so nothing is allocated for the settled prefix.
    fn with_base(base: usize) -> Self {
        Bitmap {
            levels: [const { AtomicPtr::new(std::ptr::null_mut()) }; LEVELS],
            len: AtomicUsize::new(base),
            base,
        }
    }

    /// Which level a byte lives in, and where inside it.
    #[inline]
    fn locate(index: usize) -> (usize, usize) {
        let v = index / BASE + 1;
        let level = (usize::BITS - 1 - v.leading_zeros()) as usize;
        (level, index - BASE * ((1 << level) - 1))
    }

    #[inline]
    fn level_len(level: usize) -> usize {
        BASE << level
    }

    /// The byte at `index`, or `None` past the recorded length.
    #[inline]
    fn get(&self, index: usize) -> Option<u8> {
        if index >= self.len.load(Ordering::Acquire) || index < self.base {
            return None;
        }
        let (level, offset) = Self::locate(index - self.base);
        // Within `len`, so the level is published — but a null here must read as
        // absent rather than dereference.
        let base = self.levels.get(level)?.load(Ordering::Acquire);
        if base.is_null() {
            return None;
        }
        // Safe: `base` points at a `[AtomicU8; level_len(level)]` that was
        // allocated before it was published and is freed only in `Drop`, and
        // `offset` is inside it because `index` is inside the level.
        Some(unsafe { (*base.add(offset)).load(Ordering::Acquire) })
    }

    /// The byte at `index`, for a writer that already holds the log's `Mutex`
    /// and has grown the map past `index`.
    #[inline]
    fn load_for_update(&self, index: usize) -> u8 {
        assert!(index >= self.base, "a settled outcome is never rewritten");
        let (level, offset) = Self::locate(index - self.base);
        let base = self.levels[level].load(Ordering::Acquire);
        debug_assert!(!base.is_null(), "writer must grow before it writes");
        unsafe { (*base.add(offset)).load(Ordering::Acquire) }
    }

    /// Writes the byte at `index`. Caller holds the log's `Mutex`, so no other
    /// writer can be mid-update of the four transactions that share this byte.
    #[inline]
    fn store(&self, index: usize, byte: u8) {
        assert!(index >= self.base, "a settled outcome is never rewritten");
        let (level, offset) = Self::locate(index - self.base);
        let base = self.levels[level].load(Ordering::Acquire);
        debug_assert!(!base.is_null(), "writer must grow before it writes");
        unsafe { (*base.add(offset)).store(byte, Ordering::Release) }
    }

    /// Makes `len` bytes readable, allocating whatever levels that needs.
    ///
    /// The length is published last, so a reader either sees the old length —
    /// and reads a byte that was already there — or the new one, by which point
    /// every level under it is allocated and zeroed.
    fn grow_to(&self, len: usize) {
        let relative = len.saturating_sub(self.base);
        let mut covered = 0usize;
        for level in 0..LEVELS {
            if covered >= relative {
                break;
            }
            if self.levels[level].load(Ordering::Acquire).is_null() {
                let chunk: Box<[AtomicU8]> = (0..Self::level_len(level))
                    .map(|_| AtomicU8::new(0))
                    .collect();
                // Leaked deliberately: a level outlives every reader that may
                // be holding a pointer into it, and `Drop` takes it back.
                self.levels[level].store(Box::into_raw(chunk) as *mut AtomicU8, Ordering::Release);
            }
            covered += Self::level_len(level);
        }
        assert!(
            covered >= relative,
            "transaction status bitmap exceeded {} levels ({} bytes)",
            LEVELS,
            covered
        );
        if len > self.len.load(Ordering::Acquire) {
            self.len.store(len, Ordering::Release);
        }
    }

    /// The recorded length.
    #[inline]
    fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// Fills the map from bytes read off disk. Called before publication.
    fn fill_from(&self, bytes: &[u8]) {
        self.fill_from_at(0, bytes);
    }

    /// Fills the map from bytes read off disk starting at byte `at`.
    fn fill_from_at(&self, at: usize, bytes: &[u8]) {
        self.grow_to(at + bytes.len());
        for (i, b) in bytes.iter().enumerate() {
            if *b != 0 {
                self.store(at + i, *b);
            }
        }
    }

    /// Bytes the levels hold allocated. Diagnostic.
    fn allocated_bytes(&self) -> usize {
        (0..LEVELS)
            .filter(|l| !self.levels[*l].load(Ordering::Acquire).is_null())
            .map(Self::level_len)
            .sum()
    }
}

impl Drop for Bitmap {
    fn drop(&mut self) {
        for level in 0..LEVELS {
            let ptr = *self.levels[level].get_mut();
            if !ptr.is_null() {
                // Reclaims exactly what `grow_to` leaked with `mem::forget`.
                unsafe {
                    drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                        ptr,
                        Self::level_len(level),
                    )));
                }
            }
        }
    }
}

pub struct StatusLog {
    inner: Mutex<Inner>,
    /// What every lookup reads: the settled prefix and the bitmap above it,
    /// outside the `Mutex` so a scan can read them without it.
    ///
    /// Swapped whole when the settled horizon moves. Memory reclamation is
    /// ownership, not reasoning: a lookup holds a guard on the generation it
    /// loaded, so the bitmap levels a settle retires are freed only when the
    /// last reader of that generation lets go — `Arc`'s rule, enforced by the
    /// compiler. Without settling the generation never changes and the log is
    /// exactly the bitmap it always was.
    view: ArcSwap<Generation>,
    /// Whether this log settles at all — at open and as it runs. Fixed at
    /// construction from `set_settle_on_open` / `RASTERIZEDDB_SETTLE_STATUS`.
    settle: bool,
    /// Bumped after every generation published; see [`StatusLog::with_view`].
    version: std::sync::atomic::AtomicU64,
    /// This log's key in the per-thread view cache. Never reused within the
    /// process: keying a cache on an address once returned another file's
    /// bytes when the address was reissued.
    id: u64,
    /// Width of the unflushed byte range, readable without taking the lock.
    ///
    /// A read-only transaction skips the durable flush entirely, so on a
    /// read-mostly server nothing would ever clear `dirty` and the range would
    /// grow with the id counter — until the next write had to push hundreds of
    /// megabytes of it out in one go. Callers watch this and fold the range
    /// away with a `flush_deferred` long before it gets there; see
    /// [`StatusLog::dirty_bytes`].
    ///
    /// Padded: it is `fetch_add`ed on every status write while its unpadded
    /// neighbour is the `Mutex` guarding the log itself, so the counter update
    /// and the lock word were contending for one line from different threads.
    dirty_bytes: CacheGuard<AtomicUsize>,
}

/// See [`outcome_revocations`].
static OUTCOME_REVOCATIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many times, since the process started, a transaction recorded
/// `Committed` was then recorded `Aborted` (N26).
///
/// The commit path records `Committed` before its last durability steps, and a
/// failure in one of them records `Aborted` over it. Anything decided on the
/// in-memory outcome in between — a vacuum forget, a filter clear — removed a
/// version that is visible again. Restart heals that today, because every
/// index and the filter are rebuilt from rows; a persisted snapshot taken in
/// the window would not be. So a snapshot is refused if this moved since the
/// partition opened and again just before it is published.
pub fn outcome_revocations() -> u64 {
    OUTCOME_REVOCATIONS.load(Ordering::Acquire)
}

/// Whether writing outcome bits `new` over `old` revokes a commit.
#[inline]
fn revokes_a_commit(old: u8, new: u8) -> bool {
    old == BITS_COMMITTED && new == BITS_ABORTED
}

/// Ids for [`StatusLog::id`].
static NEXT_LOG_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

thread_local! {
    /// The generations this thread last read, per log: `(log id, version,
    /// generation)`. Two slots — a thread usually serves one database at a
    /// time, and each slot can keep one retired generation alive until this
    /// thread next looks something up.
    static VIEW_CACHE: std::cell::RefCell<[Option<(u64, u64, Arc<Generation>)>; 2]> =
        const { std::cell::RefCell::new([None, None]) };
}

/// One immutable-in-shape view of every outcome: ids below `settled.below`
/// from the settled set, the rest from the bitmap. The bitmap's bytes still
/// change in place (under the log's `Mutex`); its *extent* — which ids it
/// holds — never does. A settle builds the next generation and swaps it in.
struct Generation {
    settled: Option<Settled>,
    /// Shared between generations that differ only in their settled set: a
    /// forget swaps the settled set and keeps the live bitmap, which records
    /// keep writing into either way.
    bits: Arc<Bitmap>,
}

impl Generation {
    fn plain(bits: Bitmap) -> Self {
        Generation {
            settled: None,
            bits: Arc::new(bits),
        }
    }

    #[inline]
    fn below(&self) -> TxId {
        self.settled.as_ref().map_or(0, |s| s.below)
    }
}

impl std::fmt::Debug for StatusLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("StatusLog")
            .field("path", &inner.path)
            .field("highest", &inner.highest)
            .field("logged", &inner.wal.is_some())
            .finish()
    }
}

struct Inner {
    /// `None` for an in-memory log, which is what tests and `--in-memory` use.
    file: Option<File>,
    path: Option<PathBuf>,
    /// Highest id ever recorded, so ids resume above it after a restart.
    highest: TxId,
    /// The log this file's writes go through, and its id there.
    ///
    /// A commit record has to be exactly as durable as the data it validates,
    /// and by the same mechanism. Written to its own file it is not: a crash can
    /// leave rows recovered from the log whose transaction has no recorded
    /// outcome, so they come back as debris and the write is lost even though
    /// the log had it. Routing this through the same log makes replay restore
    /// both together.
    wal: Option<(std::sync::Arc<Wal>, u64)>,
    /// Byte range touched since the last flush, as `[low, high)`.
    ///
    /// A flush happens per committing transaction, so writing the whole bitmap
    /// each time would make the cost of a commit grow with the number of
    /// transactions the database has ever run.
    dirty: Option<(usize, usize)>,
    /// Ranges that are in the log but not yet copied into the file.
    ///
    /// The copy may only happen once the log record describing it is durable.
    /// In the other order the file can reach the disk carrying an outcome the
    /// log does not have, and a crash there leaves a transaction reading as
    /// committed while the rows it committed are missing — a write reported as
    /// successful that silently is not there.
    ///
    /// Applied in sequence order, so a flusher that was overtaken can never put
    /// an older byte back over a newer one and lose the outcome in it.
    staged: Vec<Staged>,
    /// Bytes of the dirty range that a settle moved out of the bitmap before
    /// they were flushed: `(first byte, bytes)`, ending exactly where the
    /// current generation's bitmap begins. `stage` writes these, so a settle
    /// never makes an unflushed outcome unwritable — and never writes zeros
    /// over a settled one. Cleared with `dirty`.
    captured: Option<(usize, Vec<u8>)>,
    /// The horizon the sidecar on disk records, so a running settle writes a
    /// new one only every [`SIDECAR_PERSIST_STEP`] ids.
    persisted_below: TxId,
    /// The forgotten horizon the sidecar on disk records, for the same reason.
    persisted_forgotten: TxId,
    /// [`SIDECAR_PERSIST_STEP`], unless a test shrank it.
    persist_step: TxId,
}

/// A range of the bitmap that has been logged but not yet copied to the file.
struct Staged {
    /// Sequence of the commit marker covering these bytes.
    seq: u64,
    /// Where the range starts, in both the file and the bitmap.
    low: usize,
    bytes: Vec<u8>,
}

impl StatusLog {
    /// A log backed by a file, read into memory if it already exists.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with(path, settle_on_open())
    }

    /// [`StatusLog::open`] with settling chosen explicitly rather than by the
    /// process-wide switch.
    pub fn open_with(path: impl AsRef<Path>, settle: bool) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        refuse_rewritten_format(&mut file, &path)?;
        if settle {
            return Self::open_settled(path, file);
        }

        let mut bits = Vec::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut bits)?;

        // The highest id that has anything but "in progress" recorded. Scanned
        // once at open; a transaction still marked in-progress from a previous
        // run ended by crashing, which is an abort.
        let mut highest = 0u64;
        for (index, byte) in bits.iter().enumerate() {
            if *byte != 0 {
                for slot in 0..ENTRIES_PER_BYTE {
                    if (byte >> (slot * 2)) & 0b11 != BITS_IN_PROGRESS {
                        highest = index as u64 * ENTRIES_PER_BYTE + slot;
                    }
                }
            }
        }

        let map = Bitmap::new();
        map.fill_from(&bits);

        Ok(StatusLog {
            view: ArcSwap::from_pointee(Generation::plain(map)),
            settle: false,
            version: std::sync::atomic::AtomicU64::new(0),
            id: NEXT_LOG_ID.fetch_add(1, Ordering::Relaxed),
            inner: Mutex::new(Inner {
                file: Some(file),
                path: Some(path),
                highest,
                wal: None,
                dirty: None,
                staged: Vec::new(),
                captured: None,
                persisted_below: 0,
                persisted_forgotten: 0,
                persist_step: SIDECAR_PERSIST_STEP,
            }),
            dirty_bytes: AtomicUsize::new(0).into(),
        })
    }

    /// A log that is never written anywhere.
    ///
    /// Correct for a database with no files: nothing outlives the process, so
    /// there is nothing for the record to be consulted by.
    pub fn in_memory() -> Self {
        Self::in_memory_with(settle_on_open())
    }

    /// [`StatusLog::in_memory`] with settling chosen explicitly.
    pub fn in_memory_with(settle: bool) -> Self {
        StatusLog {
            view: ArcSwap::from_pointee(Generation::plain(Bitmap::new())),
            settle,
            version: std::sync::atomic::AtomicU64::new(0),
            id: NEXT_LOG_ID.fetch_add(1, Ordering::Relaxed),
            inner: Mutex::new(Inner {
                file: None,
                path: None,
                highest: 0,
                wal: None,
                dirty: None,
                staged: Vec::new(),
                captured: None,
                persisted_below: 0,
                persisted_forgotten: 0,
                persist_step: SIDECAR_PERSIST_STEP,
            }),
            dirty_bytes: AtomicUsize::new(0).into(),
        }
    }

    /// Records how `tx` ended.
    ///
    /// An `Aborted` written over `Committed` is counted in
    /// [`outcome_revocations`].
    pub fn record(&self, tx: TxId, outcome: TxOutcome) {
        let bits = match outcome {
            TxOutcome::InProgress => BITS_IN_PROGRESS,
            TxOutcome::Committed => BITS_COMMITTED,
            TxOutcome::Aborted => BITS_ABORTED,
        };

        // The lock still serializes writers: four transactions share a byte, so
        // the read-modify-write below is only atomic because nobody else is in
        // it. Readers need no lock — see [`Bitmap`]. It also serializes writers
        // against a settle, which swaps the generation under the same lock, so
        // the generation loaded here is the one this write lands in.
        let mut inner = self.inner.lock();
        let view = self.view.load();
        // Every settled id is below every running transaction — a settle never
        // moves past `oldest_active` — so its outcome is final and nothing
        // records it again. Asserted rather than tolerated: a write below the
        // horizon would land nowhere a lookup reads, silently.
        assert!(
            tx >= view.below(),
            "transaction {} is settled (below {}) and cannot be recorded again",
            tx,
            view.below()
        );
        let index = (tx / ENTRIES_PER_BYTE) as usize;
        if index >= view.bits.len() {
            view.bits
                .grow_to((index + 1).next_multiple_of(GROWTH_CHUNK));
        }

        let shift = (tx % ENTRIES_PER_BYTE) * 2;
        let byte = view.bits.load_for_update(index);
        if revokes_a_commit((byte >> shift) & 0b11, bits) {
            OUTCOME_REVOCATIONS.fetch_add(1, Ordering::AcqRel);
            log::warn!(
                "transaction {} was recorded committed and is now recorded aborted; \
                 work decided on the committed outcome in between is healed by a restart",
                tx
            );
        }
        view.bits
            .store(index, (byte & !(0b11 << shift)) | (bits << shift));

        if tx > inner.highest {
            inner.highest = tx;
        }
        let dirty = match inner.dirty {
            Some((low, high)) => (low.min(index), high.max(index + 1)),
            None => (index, index + 1),
        };
        inner.dirty = Some(dirty);
        // Published for `dirty_bytes`, which a read-only statement consults
        // without taking this lock. Written under it, so the two never disagree.
        self.dirty_bytes.store(dirty.1 - dirty.0, Ordering::Relaxed);
    }

    /// How `tx` ended.
    ///
    /// An id the log has never heard of reads as aborted rather than in
    /// progress: either it was never assigned, or it was assigned by a run that
    /// crashed before recording anything. Both mean its writes must not be seen.
    ///
    /// Reads through [`StatusLog::with_view`], which hands over a generation
    /// at least as new as every record made before this call.
    #[inline]
    pub fn outcome(&self, tx: TxId) -> TxOutcome {
        self.with_view(|view| Self::outcome_in(view, tx))
    }

    #[inline]
    fn outcome_in(view: &Generation, tx: TxId) -> TxOutcome {
        if let Some(settled) = &view.settled {
            if tx < settled.below {
                return settled.outcome(tx);
            }
        }
        let index = (tx / ENTRIES_PER_BYTE) as usize;
        let Some(byte) = view.bits.get(index) else {
            return TxOutcome::Aborted;
        };
        let shift = (tx % ENTRIES_PER_BYTE) * 2;
        match (byte >> shift) & 0b11 {
            BITS_COMMITTED => TxOutcome::Committed,
            BITS_ABORTED => TxOutcome::Aborted,
            // In progress. To a caller that is not the transaction itself this
            // is indistinguishable from aborted and must be treated as such.
            _ => TxOutcome::InProgress,
        }
    }

    /// Runs `f` on the current generation, from this thread's cache when the
    /// log's `version` says nothing newer has been published.
    ///
    /// A plain `ArcSwap::load` per lookup measured 13–47 ns against the bare
    /// bitmap's 2; the cache brings the common case back to a load and a
    /// compare. It is still exactly as fresh as a load per lookup:
    ///
    /// * a settle publishes a generation and then bumps `version` (Release),
    ///   both under the log's `Mutex`;
    /// * a record reaches a generation only under that `Mutex`, so any record
    ///   into a newer generation happens after its `version` bump;
    /// * a lookup that must see a record happens after it (the transaction
    ///   retired before the reader's snapshot was taken), so its Acquire load
    ///   of `version` sees the bump, misses the cache and loads the newest.
    ///
    /// Memory safety is ownership: the cache holds an `Arc`, so a retired
    /// generation lives exactly as long as some thread can still read it.
    #[inline]
    fn with_view<R>(&self, f: impl FnOnce(&Generation) -> R) -> R {
        let version = self.version.load(Ordering::Acquire);
        VIEW_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            let hit = cache
                .iter()
                .position(|slot| slot.as_ref().is_some_and(|(id, v, _)| *id == self.id && *v == version));
            let index = match hit {
                Some(index) => index,
                None => {
                    // Loaded after `version`, so it is at least that new.
                    let view = self.view.load_full();
                    // This log's slot, else an empty one, else the older of
                    // the two (slot 0, after shifting slot 1 down).
                    let index = match cache
                        .iter()
                        .position(|slot| slot.as_ref().is_some_and(|(id, _, _)| *id == self.id))
                        .or_else(|| cache.iter().position(Option::is_none))
                    {
                        Some(index) => index,
                        None => {
                            cache.swap(0, 1);
                            1
                        }
                    };
                    cache[index] = Some((self.id, version, view));
                    index
                }
            };
            let (_, _, view) = cache[index].as_ref().expect("filled above");
            f(view)
        })
    }

    /// The highest id this log has a record of.
    pub fn highest_recorded(&self) -> TxId {
        self.inner.lock().highest
    }

    /// Writes the record to disk and fsyncs it.
    ///
    /// Called on commit, because a commit that is not durable is not a commit:
    /// the rows would come back after a restart with their transaction recorded
    /// as unfinished, and be read as debris.
    ///
    /// The `fdatasync` is taken with no lock held. Holding this one across it
    /// put every committing transaction in the process behind whichever one was
    /// syncing, one at a time, which defeated the log's own group commit:
    /// concurrent flushers there collapse into a single sync, and this lock was
    /// what stopped them from ever being concurrent. Eight clients doing
    /// read-only statements cost 126 syncs where 16 would have done.
    pub fn flush(&self) -> io::Result<()> {
        let Some((wal, seq)) = self.stage()? else {
            return Ok(());
        };
        wal.commit_through(seq)?;
        self.apply_durable(&wal)
    }

    /// Writes the record, leaving its `fdatasync` to the caller.
    ///
    /// The sync is owed to [`Wal::sync_pending`], so a statement about to take
    /// one for its own rows pays for its outcome record with the same one —
    /// the two were separate syncs of the same file, back to back, and cost an
    /// `INSERT` twice what it needed to. Nothing may report success before that
    /// sync has run; [`StatusLog::flush`] is the variant whose contract is that
    /// it is durable when it returns.
    ///
    /// The caller takes its sync and then calls [`StatusLog::apply_staged`].
    pub fn flush_deferred(&self) -> io::Result<()> {
        self.stage()?;
        Ok(())
    }

    /// Makes every outcome recorded so far durable, including ones another
    /// caller staged with [`StatusLog::flush_deferred`] and has not synced
    /// yet: [`StatusLog::flush`] syncs only what it staged itself, and returns
    /// at once when nothing is dirty, so an asynchronous commit's outcome can
    /// still be waiting in the log's page cache when it does.
    ///
    /// For callers about to persist something that is only true if those
    /// outcomes are: vacuum's filter clears.
    pub fn flush_all(&self) -> io::Result<()> {
        self.stage()?;
        let wal = self.inner.lock().wal.as_ref().map(|(w, _)| w.clone());
        match wal {
            Some(wal) => {
                wal.sync_pending()?;
                self.apply_durable(&wal)
            }
            // No log: `stage` wrote and synced the file itself.
            None => Ok(()),
        }
    }

    /// The log the outcome records go to, or `None` when there is none (the
    /// file is written and synced directly).
    pub fn outcome_wal(&self) -> Option<Arc<Wal>> {
        self.inner.lock().wal.as_ref().map(|(w, _)| w.clone())
    }

    /// Copies over any staged range the log has since made durable.
    ///
    /// Call after the sync that [`StatusLog::flush_deferred`] left owed.
    pub fn apply_staged(&self) -> io::Result<()> {
        let wal = self.inner.lock().wal.as_ref().map(|(w, _)| w.clone());
        match wal {
            Some(wal) => self.apply_durable(&wal),
            None => Ok(()),
        }
    }

    /// Width of the range waiting to be written, without taking the lock.
    ///
    /// A read-only transaction skips the flush entirely, so on a read-mostly
    /// server nothing clears `dirty` and the range grows with the id counter —
    /// until the next write has to push the whole accumulation out in one go.
    /// Callers watch this and fold it away with a `flush_deferred`, which costs
    /// no sync, long before it gets that far.
    pub fn dirty_bytes(&self) -> usize {
        self.dirty_bytes.load(Ordering::Relaxed)
    }

    /// Puts the dirty bytes in the log and queues them for the file.
    ///
    /// Returns the log and the sequence the sync is owed on, or `None` when
    /// there was nothing to write or nowhere to write it.
    fn stage(&self) -> io::Result<Option<(std::sync::Arc<Wal>, u64)>> {
        let mut inner = self.inner.lock();
        let Some((low, high)) = inner.dirty else {
            return Ok(None);
        };
        if inner.file.is_none() {
            inner.dirty = None;
            inner.captured = None;
            self.dirty_bytes.store(0, Ordering::Relaxed);
            return Ok(None);
        }

        // One copy of the range, because it may span levels of the bitmap and
        // both the log and the file want it contiguous. The part a settle has
        // since moved below the bitmap comes from what it captured.
        let bytes = {
            let view = self.view.load();
            let captured = inner.captured.as_ref();
            (low..high)
                .map(|i| match captured {
                    Some((start, held)) if i >= *start && i < start + held.len() => held[i - start],
                    _ => view.bits.get(i).unwrap_or(0),
                })
                .collect::<Vec<u8>>()
        };

        // With no log the file is the only record, so it is written and synced
        // here and there is nothing to stage.
        let Some((wal, file_id)) = inner.wal.clone() else {
            let file = inner.file.as_mut().expect("checked above");
            file.seek(SeekFrom::Start(low as u64))?;
            file.write_all(&bytes)?;
            file.sync_data()?;
            inner.dirty = None;
            inner.captured = None;
            self.dirty_bytes.store(0, Ordering::Relaxed);
            // The bytes under the horizons are all in the file now, which may
            // be what a sidecar rewrite was waiting for.
            self.persist_sidecar(&mut inner);
            return Ok(None);
        };

        let seq = wal.log_and_commit_deferred(file_id, low as u64, &bytes)?;

        // The file is written too, even when logging. A checkpoint does not
        // replay records into files — it fsyncs the files their writers already
        // wrote and then discards the log. A participant that wrote only to the
        // log would have its records thrown away at the first checkpoint, which
        // is exactly how `outcomes_survive_the_log_being_checkpointed` failed:
        // every outcome older than the last checkpoint came back as unfinished,
        // so committed rows read as debris.
        //
        // It is queued rather than written here, because it may not reach the
        // disk before the record that justifies it. The file is not fsynced, so
        // the kernel is free to write its page back at any time; if that landed
        // before the log's own sync and the machine then died, the file would
        // name a transaction as committed whose rows never became durable.
        inner.staged.push(Staged { seq, low, bytes });
        inner.dirty = None;
        inner.captured = None;
        self.dirty_bytes.store(0, Ordering::Relaxed);
        Ok(Some((wal, seq)))
    }

    /// Copies staged ranges into the file, oldest first, stopping at the first
    /// one the log has not made durable yet.
    fn apply_durable(&self, wal: &Wal) -> io::Result<()> {
        let durable = wal.durable_through();
        let mut inner = self.inner.lock();
        if inner.staged.is_empty() {
            return Ok(());
        }

        // Sequence order. Two ranges can share a byte — four transactions share
        // every byte — so writing an older snapshot after a newer one would put
        // the byte back the way it was and lose the outcome recorded in it.
        inner.staged.sort_by_key(|s| s.seq);
        let ready = inner.staged.iter().take_while(|s| s.seq <= durable).count();
        if ready == 0 {
            return Ok(());
        }

        let batch: Vec<Staged> = inner.staged.drain(..ready).collect();
        let mut rest = batch.into_iter();
        while let Some(s) = rest.next() {
            let file = inner
                .file
                .as_mut()
                .expect("staged only when there is a file");
            let written = file
                .seek(SeekFrom::Start(s.low as u64))
                .and_then(|_| file.write_all(&s.bytes));
            if let Err(e) = written {
                // Put back what did not go out. Dropping it would leave the
                // file missing outcomes that only the log still has, and the
                // next checkpoint would discard the log over the top of them.
                inner.staged.push(s);
                inner.staged.extend(rest);
                inner.staged.sort_by_key(|s| s.seq);
                return Err(e);
            }
        }
        // As in `stage`: a sidecar rewrite may have been waiting on these.
        self.persist_sidecar(&mut inner);
        Ok(())
    }

    pub fn path(&self) -> Option<PathBuf> {
        self.inner.lock().path.clone()
    }

    /// The log this one's writes go through (see [`StatusLog::attach_wal`]):
    /// the log whose sync makes the outcomes durable.
    pub fn wal(&self) -> Option<std::sync::Arc<Wal>> {
        self.inner.lock().wal.as_ref().map(|(w, _)| w.clone())
    }

    /// Routes this log's writes through `wal`, so an outcome and the rows it
    /// validates are recovered together.
    ///
    /// Registering the file is what lets replay write back into it. Without the
    /// registration the log would hold records addressed to a file it does not
    /// know how to open, and would drop them.
    ///
    /// Not attached when the registration fails — another file's name has the
    /// same id, and the log would refuse every record under it anyway. The
    /// file is then written and synced directly, as with no log.
    pub fn attach_wal(&self, wal: std::sync::Arc<Wal>) -> std::io::Result<()> {
        let mut inner = self.inner.lock();
        let Some(path) = inner.path.clone() else {
            return Ok(());
        };
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            return Ok(());
        };
        let file_id = crate::core::wal::file_id_for_name(name);
        wal.register_file(file_id, path)?;
        inner.wal = Some((wal, file_id));
        Ok(())
    }
}

impl StatusLog {
    /// Records every id in `[from, to)` that has no outcome yet as aborted.
    /// Returns how many it recorded.
    ///
    /// For the ids a previous run may have handed out without ever recording
    /// how they ended — its reserved-but-unrecorded tail. None of them is
    /// running now, and none committed (a commit is recorded durably before it
    /// is acknowledged), so aborted is the truth. Left unrecorded they would
    /// read as *in progress* once this run's records grow the bitmap over
    /// them, and a row one of them stamped would look like another
    /// transaction's unfinished write for good: a uniqueness check reports it
    /// as a retryable conflict on every attempt, and reclamation never frees
    /// it.
    ///
    /// A byte at a time under one acquisition of the lock, so the cost is the
    /// range over four, not one locked record per id. An id that already has
    /// an outcome keeps it.
    pub fn record_unfinished_as_aborted(&self, from: TxId, to: TxId) -> u64 {
        if from >= to {
            return 0;
        }
        let mut inner = self.inner.lock();
        let view = self.view.load();
        assert!(
            from >= view.below(),
            "transaction {} is settled (below {}) and cannot be recorded again",
            from,
            view.below()
        );
        let first = (from / ENTRIES_PER_BYTE) as usize;
        let last = ((to - 1) / ENTRIES_PER_BYTE) as usize;
        if last >= view.bits.len() {
            view.bits.grow_to((last + 1).next_multiple_of(GROWTH_CHUNK));
        }
        let mut recorded = 0;
        for index in first..=last {
            let byte = view.bits.load_for_update(index);
            let mut next = byte;
            for slot in 0..ENTRIES_PER_BYTE {
                let tx = index as u64 * ENTRIES_PER_BYTE + slot;
                let shift = slot * 2;
                if tx >= from && tx < to && (byte >> shift) & 0b11 == BITS_IN_PROGRESS {
                    next |= BITS_ABORTED << shift;
                    recorded += 1;
                }
            }
            if next != byte {
                view.bits.store(index, next);
            }
        }
        if to - 1 > inner.highest {
            inner.highest = to - 1;
        }
        let dirty = match inner.dirty {
            Some((low, high)) => (low.min(first), high.max(last + 1)),
            None => (first, last + 1),
        };
        inner.dirty = Some(dirty);
        self.dirty_bytes.store(dirty.1 - dirty.0, Ordering::Relaxed);
        recorded
    }
}

// ---------------------------------------------------------------------------
// Id reservation: the ids a run may have handed out, durable before it does
// ---------------------------------------------------------------------------

/// How many ids one durable reservation covers.
///
/// The trade-off is one durable write per block of transactions against up to
/// a block of ids skipped at every open. A write is a log sync (or, with no
/// log, a file write, `fsync`, rename and directory `fsync`) taken by the one
/// `begin` that crosses the mark, with every other `begin` waiting behind it:
/// at 65 536 it is about one per second at 65 000 transactions a second, which
/// is not measurable. Skipped ids cost two bits each in the status file and in
/// the settled record (16 KiB a restart), and nothing else: ids are `u64`.
pub const ID_RESERVATION_BLOCK: u64 = 1 << 16;

/// File content: magic, the mark, and a CRC over both.
const RESERVATION_MAGIC: &[u8; 8] = b"RDBTXNH1";
const RESERVATION_LEN: usize = 8 + 8 + 4;

/// The file beside `status_path` that holds its reservation mark: for
/// `db_transactions.status`, `db_transactions.next`. Per database directory,
/// because the status file is.
pub fn reservation_path(status_path: &Path) -> PathBuf {
    status_path.with_extension("next")
}

fn encode_reservation(mark: TxId) -> [u8; RESERVATION_LEN] {
    let mut out = [0u8; RESERVATION_LEN];
    out[..8].copy_from_slice(RESERVATION_MAGIC);
    out[8..16].copy_from_slice(&mark.to_le_bytes());
    let crc = super::super::storage_providers::CRC.checksum(&out[..16]);
    out[16..].copy_from_slice(&crc.to_le_bytes());
    out
}

fn decode_reservation(bytes: &[u8]) -> Option<TxId> {
    if bytes.len() < RESERVATION_LEN || &bytes[..8] != RESERVATION_MAGIC {
        return None;
    }
    let crc = u32::from_le_bytes(bytes[16..20].try_into().ok()?);
    if super::super::storage_providers::CRC.checksum(&bytes[..16]) != crc {
        return None;
    }
    Some(u64::from_le_bytes(bytes[8..16].try_into().ok()?))
}

/// The mark persisted beside `status_path`, or `None` if there is no valid
/// one. For tests and diagnostics; an open goes through
/// [`IdReservation::open_for`].
pub fn read_reservation(status_path: &Path) -> Option<TxId> {
    decode_reservation(&std::fs::read(reservation_path(status_path)).ok()?)
}

/// The durable high-water mark of transaction ids: every id a run may have
/// handed out is below it.
///
/// The status log alone cannot say where ids should resume. It knows the
/// highest id with a recorded outcome, but `begin` records nothing durable and
/// an abort's outcome is not synced, so a writer whose rows reached the disk
/// can sit above every outcome. Resuming at the highest outcome handed that
/// id out again, and its new owner read the old rows as its own writes and
/// committed them. With a mark, an id is handed out only below it, the mark is
/// raised durably *before* any id above the old one is, and an open resumes at
/// or above it.
///
/// Written through the status log's write-ahead log when it has one, so a
/// point-in-time restore replays the mark along with the rows: a restored
/// database resumes above every id its replayed rows can carry. The file is
/// written first and the record second, the order every logged write keeps,
/// so a checkpoint that flushes the file before discarding the record
/// covers it; a mark on disk that is *higher* than the record is harmless.
/// Without a log: temporary file, `fsync`, rename, directory `fsync`.
///
/// Monotonic in every path: each write carries a larger value than the last,
/// replay applies records in order, and a log is replayed and emptied at open
/// before the mark is read.
pub struct IdReservation {
    /// `None` for an in-memory log: nothing outlives the process.
    path: Option<PathBuf>,
    /// The mark on disk. `TxId::MAX` when there is no file, so the check in
    /// `begin` never fires.
    reserved: TxId,
    /// Ids per raise. [`ID_RESERVATION_BLOCK`] unless a test shrank it.
    block: u64,
    /// The last raise failed; logged once until one succeeds.
    failing: bool,
}

impl std::fmt::Debug for IdReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdReservation")
            .field("path", &self.path)
            .field("reserved", &self.reserved)
            .finish()
    }
}

impl IdReservation {
    /// The reservation beside `status`'s file, read from disk. A missing or
    /// unreadable file reads as 0: nothing reserved, so ids resume above the
    /// status log alone, as before the file existed.
    pub fn open_for(status: &StatusLog) -> IdReservation {
        let Some(status_path) = status.path() else {
            return IdReservation {
                path: None,
                reserved: TxId::MAX,
                block: ID_RESERVATION_BLOCK,
                failing: false,
            };
        };
        let path = reservation_path(&status_path);
        let reserved = match std::fs::read(&path) {
            Ok(bytes) => decode_reservation(&bytes).unwrap_or_else(|| {
                log::error!(
                    "{} does not hold a transaction id reservation; ids resume above the \
                     status log alone, which can reissue the id of a writer whose outcome \
                     was never recorded",
                    path.display()
                );
                0
            }),
            Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
            Err(e) => {
                log::error!("Could not read {}: {}", path.display(), e);
                0
            }
        };
        IdReservation {
            path: Some(path),
            reserved,
            block: ID_RESERVATION_BLOCK,
            failing: false,
        }
    }

    /// The mark: no id at or above it has been handed out, by any run.
    /// `TxId::MAX` when nothing is persisted.
    #[inline]
    pub fn reserved(&self) -> TxId {
        self.reserved
    }

    /// Whether the mark is persisted at all.
    pub fn is_durable(&self) -> bool {
        self.path.is_some()
    }

    /// The file the mark is persisted in; `None` for an in-memory log.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    #[cfg(test)]
    pub fn set_block(&mut self, ids: u64) {
        self.block = ids.max(1);
    }

    /// Makes `id` handable: raises the mark durably past it, by a block, if it
    /// is not below it already. `log` is the status log's write-ahead log.
    ///
    /// On failure the mark is unchanged and the error is returned. The caller
    /// still hands the id out, so reads go on, but no transaction at or above
    /// the mark may commit a write until a later call succeeds (see
    /// `TransactionManager::check_ids_reserved`): a restart resumes at the
    /// mark, hands the id out again, and its new owner would adopt the rows.
    /// The next `begin` past the mark tries again.
    pub fn cover(&mut self, id: TxId, log: Option<&std::sync::Arc<Wal>>) -> io::Result<()> {
        if id < self.reserved {
            return Ok(());
        }
        let mark = id.saturating_add(self.block);
        match self.write(mark, log) {
            Ok(()) => {
                self.reserved = mark;
                if self.failing {
                    log::info!("transaction id reservation is being written again");
                    self.failing = false;
                }
                Ok(())
            }
            Err(e) => {
                if !self.failing {
                    log::error!(
                        "Could not reserve transaction ids through {}: {}. Transactions that \
                         write are refused at commit until this succeeds; reads go on.",
                        id.saturating_add(self.block),
                        e
                    );
                    self.failing = true;
                }
                Err(e)
            }
        }
    }

    fn write(&self, mark: TxId, log: Option<&std::sync::Arc<Wal>>) -> io::Result<()> {
        let Some(path) = self.path.as_deref() else {
            return Ok(());
        };
        #[cfg(test)]
        if reservation_test_hooks::fails(path) {
            // What a full disk answers.
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        let bytes = encode_reservation(mark);
        if let Some(wal) = log {
            use std::os::unix::fs::FileExt;
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| io::Error::other("reservation path has no file name"))?;
            let file_id = crate::core::wal::file_id_for_name(name);
            // The file has to exist durably before a record names it: replay
            // only writes into files it finds in the directory.
            let existed = path.exists();
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?;
            if !existed {
                file.sync_all()?;
                crate::core::wal::sync_parent_dir(path)?;
            }
            wal.register_file(file_id, path)?;
            // File first, then the record: see the type's documentation.
            file.write_all_at(&bytes, 0)?;
            return wal.log_and_commit(file_id, 0, &bytes);
        }
        let mut temp_name = path.file_name().unwrap_or_default().to_os_string();
        temp_name.push(".partial");
        let temp = path.with_file_name(temp_name);
        {
            let mut file = File::create(&temp)?;
            file.write_all(&bytes)?;
            crate::core::sync_audit::note("mvcc.id_reservation", path);
            file.sync_all()?;
        }
        std::fs::rename(&temp, path)?;
        crate::core::wal::sync_parent_dir(path)
    }
}

/// Fails the reservation writes of chosen databases, as a full disk would.
///
/// Keyed by directory rather than by thread: `begin` runs on whichever worker
/// thread the statement landed on.
#[cfg(test)]
pub(crate) mod reservation_test_hooks {
    use std::path::{Path, PathBuf};

    static FAILING: parking_lot::Mutex<Vec<PathBuf>> = parking_lot::Mutex::new(Vec::new());

    /// Every reservation write in `dir` fails from now until [`heal`].
    pub(crate) fn fail_in(dir: &Path) {
        FAILING.lock().push(dir.to_path_buf());
    }

    pub(crate) fn heal(dir: &Path) {
        FAILING.lock().retain(|d| d != dir);
    }

    pub(super) fn fails(path: &Path) -> bool {
        let Some(dir) = path.parent() else {
            return false;
        };
        FAILING.lock().iter().any(|d| d == dir)
    }
}

// ---------------------------------------------------------------------------
// Settling: answering long-finished transactions without a bitmap
// ---------------------------------------------------------------------------

/// Settling: on unless switched off. `0` unset, `1` off, `2` on.
static SETTLE: AtomicU8 = AtomicU8::new(0);

/// Turns settling on or off for logs opened from now on — at open and while
/// they run. The kill switch behind `--settle-status-at-open false`.
pub fn set_settle_on_open(enabled: bool) {
    SETTLE.store(if enabled { 2 } else { 1 }, Ordering::Relaxed);
}

/// Whether logs opened now settle: an explicit [`set_settle_on_open`] wins,
/// then `RASTERIZEDDB_SETTLE_STATUS` (`0`/`false` or `1`/`true`), then the
/// default, which is on.
pub fn settle_on_open() -> bool {
    match SETTLE.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => match std::env::var("RASTERIZEDDB_SETTLE_STATUS").as_deref() {
            Ok("0") | Ok("false") | Ok("off") => false,
            _ => true,
        },
    }
}

/// The first bytes of a status file in a rewritten (non-bitmap) format. No
/// build writes one; this build refuses one, so that a format that rewrites the
/// file can ship later without an older binary reading its header as outcome
/// bits. A real bitmap never starts with it: byte 0 holds ids 0 and 1, which
/// are never recorded, so its low four bits are zero, and `R` (0x52) is not.
pub const REWRITTEN_FORMAT_MAGIC: &[u8; 8] = b"RDBSTAT2";

fn refuse_rewritten_format(file: &mut File, path: &Path) -> io::Result<()> {
    let mut head = [0u8; 8];
    file.seek(SeekFrom::Start(0))?;
    let mut read = 0;
    while read < head.len() {
        match file.read(&mut head[read..])? {
            0 => break,
            n => read += n,
        }
    }
    if read == head.len() && &head == REWRITTEN_FORMAT_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} is in a newer transaction status format than this build reads; \
                 refusing to read it as a bitmap",
                path.display()
            ),
        ));
    }
    Ok(())
}

/// Ids per block of the settled set: 4096, a kilobyte of the bitmap it
/// replaces. The settled horizon only ever stands on a block boundary.
const SETTLED_BLOCK_SHIFT: u32 = 12;
const BLOCK_IDS: u64 = 1 << SETTLED_BLOCK_SHIFT;
const BLOCK_BYTES: usize = (BLOCK_IDS / ENTRIES_PER_BYTE) as usize;

/// A block with more not-committed ids than this keeps its raw kilobyte
/// instead: at nine bytes an entry the sparse form would be the larger. So a
/// settled block never costs more than the bitmap it replaced, whatever the
/// abort rate — which is what keeps memory bounded on a workload that fails
/// a lot of statements.
const DENSE_THRESHOLD: usize = BLOCK_BYTES / 9;

/// The horizon advances while running once this many ids have accumulated
/// above it — 64 KiB of bitmap.
pub const DEFAULT_SETTLE_WINDOW: u64 = 1 << 18;

/// A running log rewrites its sidecar only after the horizon has moved this
/// far past what the sidecar records: it is a cache for the next open, and
/// each write costs two `fsync`s.
const SIDECAR_PERSIST_STEP: u64 = 1 << 22;

#[derive(Clone, Copy, Debug)]
struct Block {
    /// Into the chunk's `offsets`/`codes` for a sparse block, into its `dense`
    /// for a dense one.
    start: u32,
    /// Sparse entries; unused for a dense block.
    len: u16,
    dense: bool,
}

/// What a settled block holds.
enum BlockView<'a> {
    /// Every id in it committed.
    Empty,
    /// The raw bitmap bytes.
    Dense(&'a [u8]),
    /// Offsets within the block of the ids that did not commit, and their codes.
    Sparse(&'a [u16], &'a [u8]),
}

/// Blocks per chunk: 64, so 256 Ki ids and at most 64 KiB. Full chunks are
/// immutable and shared between generations; a settle copies only the last,
/// partial one, and forgetting drops whole chunks — so the chunk is also the
/// granularity memory is given back at. Without chunks every settle cloned the
/// whole history — 20 MB at 80 M ids and one in ten aborted, every 64 Ki ids.
const CHUNK_BLOCKS: usize = 64;

/// A run of [`CHUNK_BLOCKS`] settled blocks.
#[derive(Clone, Debug, Default)]
struct Chunk {
    /// Blocks pushed so far.
    len: usize,
    /// One bit per block: whether it holds anything not committed. A clear
    /// bit — every block of a commit-only stretch — costs nothing more.
    nonempty: Vec<u64>,
    /// `rank[w]`: bits set in `nonempty[..w]`, so a block's index in `blocks`
    /// is a popcount away.
    rank: Vec<u32>,
    /// The non-empty blocks, in order.
    blocks: Vec<Block>,
    /// Sparse blocks: the id's offset within its block, sorted per block.
    offsets: Vec<u16>,
    codes: Vec<u8>,
    /// Dense blocks: their raw bitmap bytes, [`BLOCK_BYTES`] each.
    dense: Vec<u8>,
}

impl Chunk {
    fn append(&mut self, block: Option<Block>) {
        let b = self.len;
        if b % 64 == 0 {
            self.nonempty.push(0);
            self.rank.push(self.blocks.len() as u32);
        }
        if let Some(block) = block {
            self.nonempty[b / 64] |= 1 << (b % 64);
            self.blocks.push(block);
        }
        self.len += 1;
        if self.len == CHUNK_BLOCKS {
            // Immutable from here on, and shared: give back the doubling slack.
            self.nonempty.shrink_to_fit();
            self.rank.shrink_to_fit();
            self.blocks.shrink_to_fit();
            self.offsets.shrink_to_fit();
            self.codes.shrink_to_fit();
            self.dense.shrink_to_fit();
        }
    }

    fn push_dense(&mut self, raw: &[u8]) {
        let start = self.dense.len() as u32;
        self.dense.extend_from_slice(raw);
        self.append(Some(Block {
            start,
            len: 0,
            dense: true,
        }));
    }

    fn push_sparse(&mut self, entries: impl Iterator<Item = (u16, u8)>) {
        let start = self.offsets.len() as u32;
        for (offset, code) in entries {
            self.offsets.push(offset);
            self.codes.push(code);
        }
        let len = self.offsets.len() as u32 - start;
        self.append((len > 0).then_some(Block {
            start,
            len: len as u16,
            dense: false,
        }));
    }

    #[inline]
    fn block(&self, b: usize) -> Option<Block> {
        let word = self.nonempty[b / 64];
        let bit = 1u64 << (b % 64);
        if word & bit == 0 {
            return None;
        }
        let index = self.rank[b / 64] as usize + (word & (bit - 1)).count_ones() as usize;
        Some(self.blocks[index])
    }

    fn view(&self, b: usize) -> BlockView<'_> {
        match self.block(b) {
            None => BlockView::Empty,
            Some(block) if block.dense => {
                BlockView::Dense(&self.dense[block.start as usize..block.start as usize + BLOCK_BYTES])
            }
            Some(block) => {
                let range = block.start as usize..block.start as usize + block.len as usize;
                BlockView::Sparse(&self.offsets[range.clone()], &self.codes[range])
            }
        }
    }

    fn heap_bytes(&self) -> usize {
        self.nonempty.capacity() * 8
            + self.rank.capacity() * 4
            + self.blocks.capacity() * std::mem::size_of::<Block>()
            + self.offsets.capacity() * 2
            + self.codes.capacity()
            + self.dense.capacity()
    }
}

/// Outcomes of every id below `below`, kept as the ids that did *not* commit.
///
/// Sound to settle an id once its outcome can never change and nothing will
/// record it again: below every running transaction (`oldest_active`). The
/// horizon is further held at or below the oldest live snapshot's `xmin` — see
/// `TransactionManager::settle_bound` — though outcomes do not need that.
///
/// The 2-bit codes are kept exactly — an id still marked in progress by a run
/// that crashed answers `InProgress` as it did before — so settling changes no
/// answer at all, only where it comes from.
#[derive(Clone, Debug)]
struct Settled {
    /// A multiple of [`BLOCK_IDS`].
    below: TxId,
    /// Ids below this are answered "committed" without a lookup: vacuum has
    /// proven no row carries any of them that did not commit, so nothing that
    /// could ask about one remains. See `ForgetRegistry`.
    forgotten: TxId,
    /// The global index of `chunks[0]`: chunks wholly below `forgotten` are
    /// dropped, which is what keeps the history bounded.
    first_chunk: usize,
    chunks: Vec<Arc<Chunk>>,
    /// Highest id below `below` with an outcome recorded (an upper bound).
    highest: TxId,
}

impl Settled {
    fn empty() -> Self {
        Settled {
            below: 0,
            forgotten: 0,
            first_chunk: 0,
            chunks: Vec::new(),
            highest: 0,
        }
    }

    /// The chunk the next block goes into, copied first only if a previous
    /// generation still shares it — which, for the last chunk, it does.
    fn tail(&mut self) -> &mut Chunk {
        if self.chunks.last().is_none_or(|c| c.len == CHUNK_BLOCKS) {
            self.chunks.push(Arc::new(Chunk::default()));
        }
        Arc::make_mut(self.chunks.last_mut().expect("pushed above"))
    }

    /// Appends the next block from its raw bitmap bytes.
    fn push_block(&mut self, raw: &[u8]) {
        debug_assert_eq!(raw.len(), BLOCK_BYTES);
        let not_committed = raw
            .iter()
            .map(|b| (0..ENTRIES_PER_BYTE).filter(|s| (b >> (s * 2)) & 0b11 != BITS_COMMITTED).count())
            .sum::<usize>();
        if not_committed > DENSE_THRESHOLD {
            self.push_dense(raw);
            return;
        }
        let entries = raw.iter().enumerate().flat_map(|(i, byte)| {
            (0..ENTRIES_PER_BYTE).filter_map(move |slot| {
                let code = (byte >> (slot * 2)) & 0b11;
                (code != BITS_COMMITTED).then_some(((i as u64 * ENTRIES_PER_BYTE + slot) as u16, code))
            })
        });
        self.tail().push_sparse(entries);
        self.below += BLOCK_IDS;
    }

    fn push_dense(&mut self, raw: &[u8]) {
        self.tail().push_dense(raw);
        self.below += BLOCK_IDS;
    }

    fn push_sparse(&mut self, entries: &[(u16, u8)]) {
        self.tail().push_sparse(entries.iter().copied());
        self.below += BLOCK_IDS;
    }

    fn block_count(&self) -> usize {
        (self.below / BLOCK_IDS) as usize
    }

    fn view(&self, b: usize) -> BlockView<'_> {
        self.chunks[b / CHUNK_BLOCKS - self.first_chunk].view(b % CHUNK_BLOCKS)
    }

    #[inline]
    fn outcome(&self, tx: TxId) -> TxOutcome {
        if tx < self.forgotten {
            return TxOutcome::Committed;
        }
        let b = (tx >> SETTLED_BLOCK_SHIFT) as usize;
        let chunk = &self.chunks[b / CHUNK_BLOCKS - self.first_chunk];
        let Some(block) = chunk.block(b % CHUNK_BLOCKS) else {
            return TxOutcome::Committed;
        };
        let within = tx & (BLOCK_IDS - 1);
        if block.dense {
            let byte = chunk.dense[block.start as usize + (within / ENTRIES_PER_BYTE) as usize];
            return decode(byte >> ((within % ENTRIES_PER_BYTE) * 2));
        }
        let range = block.start as usize..block.start as usize + block.len as usize;
        match chunk.offsets[range.clone()].binary_search(&(within as u16)) {
            Err(_) => TxOutcome::Committed,
            Ok(i) => decode(chunk.codes[range.start + i]),
        }
    }

    /// The first `blocks` blocks, as a settled set of their own.
    fn prefix(&self, blocks: usize) -> Settled {
        let mut out = Settled::empty();
        out.first_chunk = self.first_chunk;
        // Whole chunks are shared, not copied.
        let whole = (blocks / CHUNK_BLOCKS).max(self.first_chunk);
        out.chunks
            .extend(self.chunks[..whole - self.first_chunk].iter().cloned());
        out.below = (whole * CHUNK_BLOCKS) as u64 * BLOCK_IDS;
        for b in whole * CHUNK_BLOCKS..blocks {
            match self.view(b) {
                BlockView::Empty => out.push_sparse(&[]),
                BlockView::Dense(raw) => out.push_dense(raw),
                BlockView::Sparse(offsets, codes) => {
                    let entries: Vec<(u16, u8)> =
                        offsets.iter().copied().zip(codes.iter().copied()).collect();
                    out.push_sparse(&entries);
                }
            }
        }
        out.highest = if out.below > 0 { self.highest.min(out.below - 1) } else { 0 };
        out.forgotten = self.forgotten.min(out.below);
        out
    }

    /// A copy that forgets every id below `to` (capped at `below`), dropping
    /// the chunks that lie wholly under it.
    fn forgetting(&self, to: TxId) -> Settled {
        let mut out = self.clone();
        out.forgotten = self.forgotten.max(to.min(self.below));
        let drop_to = (out.forgotten / BLOCK_IDS) as usize / CHUNK_BLOCKS;
        if drop_to > out.first_chunk {
            // Never the last, partial chunk: settling appends into it.
            let drop_to = drop_to.min(out.first_chunk + out.chunks.len().saturating_sub(1));
            out.chunks.drain(..drop_to - out.first_chunk);
            out.first_chunk = drop_to;
        }
        out
    }

    fn heap_bytes(&self) -> usize {
        self.chunks.capacity() * 8 + self.chunks.iter().map(|c| c.heap_bytes()).sum::<usize>()
    }
}

#[inline]
fn decode(code: u8) -> TxOutcome {
    match code & 0b11 {
        BITS_COMMITTED => TxOutcome::Committed,
        BITS_ABORTED => TxOutcome::Aborted,
        _ => TxOutcome::InProgress,
    }
}

const SIDECAR_MAGIC: &[u8; 8] = b"RDBSETL1";
/// Version 3: block-structured, with the forgotten horizon and the blocks from
/// the first kept chunk only. Older versions are not read; a status file is
/// always enough to rebuild from.
const SIDECAR_VERSION: u32 = 3;
/// Header bytes before the blocks.
const SIDECAR_HEADER: usize = 64;

/// The sidecar next to the status file. A cache of a fact the status file
/// still holds in full — the file itself is never rewritten or truncated — so
/// an older build, or this one with settling off, reads the file as always and
/// ignores this.
fn sidecar_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    name.push(".settled");
    path.with_file_name(name)
}

/// Bytes of the status file just below the horizon that a sidecar carries a
/// checksum of, so a sidecar sitting beside a *different* status file — one
/// restored from a backup, or replaced — is recognised and voided without
/// reading the whole settled prefix it exists to avoid reading.
const SIDECAR_WITNESS: u64 = 4096;

fn witness_crc(file: &mut File, below: TxId) -> io::Result<u32> {
    let end = below / ENTRIES_PER_BYTE;
    let start = end.saturating_sub(SIDECAR_WITNESS);
    let mut window = vec![0u8; (end - start) as usize];
    file.seek(SeekFrom::Start(start))?;
    file.read_exact(&mut window)?;
    Ok(super::super::storage_providers::CRC.checksum(&window))
}

/// Reads a sidecar, or `None` if it is missing, torn, corrupt, of another
/// version, or does not fit the status file it claims to summarise.
fn read_sidecar(path: &Path, file: &mut File) -> Option<Settled> {
    let status_len = file.metadata().ok()?.len();
    let bytes = std::fs::read(sidecar_path(path)).ok()?;
    if bytes.len() < SIDECAR_HEADER + 4 || &bytes[..8] != SIDECAR_MAGIC {
        return None;
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    if super::super::storage_providers::CRC.checksum(body) != u32::from_le_bytes(crc.try_into().ok()?) {
        return None;
    }
    let u64_at = |at: usize| u64::from_le_bytes(body[at..at + 8].try_into().unwrap());
    if u32::from_le_bytes(body[8..12].try_into().ok()?) != SIDECAR_VERSION {
        return None;
    }
    let below = u64_at(12);
    let highest = u64_at(20);
    let block_count = u64_at(28) as usize;
    let witness = u32::from_le_bytes(body[36..40].try_into().ok()?);
    let forgotten = u64_at(40);
    let first_block = u64_at(48) as usize;
    if below % BLOCK_IDS != 0
        || below / BLOCK_IDS != block_count as u64
        || first_block % CHUNK_BLOCKS != 0
        || first_block > block_count
        || forgotten > below
        || first_block as u64 * BLOCK_IDS > forgotten
    {
        return None;
    }
    // The status file only ever grows; one shorter than this summary is not
    // the file it was made from (restored, replaced), so the summary is void.
    if status_len < below / ENTRIES_PER_BYTE {
        return None;
    }
    if witness_crc(file, below).ok()? != witness {
        return None;
    }
    let mut settled = Settled::empty();
    settled.first_chunk = first_block / CHUNK_BLOCKS;
    settled.below = first_block as u64 * BLOCK_IDS;
    let mut at = SIDECAR_HEADER;
    for _ in first_block..block_count {
        let kind = *body.get(at)?;
        at += 1;
        match kind {
            1 => {
                settled.push_dense(body.get(at..at + BLOCK_BYTES)?);
                at += BLOCK_BYTES;
            }
            0 => {
                let len = u16::from_le_bytes(body.get(at..at + 2)?.try_into().ok()?) as usize;
                at += 2;
                let mut entries = Vec::with_capacity(len);
                for _ in 0..len {
                    let offset = u16::from_le_bytes(body.get(at..at + 2)?.try_into().ok()?);
                    let code = *body.get(at + 2)?;
                    if offset as u64 >= BLOCK_IDS || entries.last().is_some_and(|(o, _)| *o >= offset) {
                        return None;
                    }
                    entries.push((offset, code));
                    at += 3;
                }
                settled.push_sparse(&entries);
            }
            _ => return None,
        }
    }
    if at != body.len() {
        return None;
    }
    settled.highest = highest;
    settled.forgotten = forgotten;
    Some(settled)
}

/// Writes a sidecar crash-safely: a temporary file, synced, renamed over the
/// old one, and the directory synced. A crash at any point leaves either the
/// old sidecar or the new one — or a stray temporary, which `read_sidecar`
/// never looks at.
///
/// Only for a horizon whose outcome bytes are all in the status file already
/// (nothing below it dirty or staged): the witness is read from the file.
fn write_sidecar(path: &Path, file: &mut File, settled: &Settled) -> io::Result<()> {
    let witness = witness_crc(file, settled.below)?;
    let first_block = settled.first_chunk * CHUNK_BLOCKS;
    let mut body = Vec::with_capacity(SIDECAR_HEADER + settled.block_count() * 3);
    body.extend_from_slice(SIDECAR_MAGIC);
    body.extend_from_slice(&SIDECAR_VERSION.to_le_bytes());
    body.extend_from_slice(&settled.below.to_le_bytes());
    body.extend_from_slice(&settled.highest.to_le_bytes());
    body.extend_from_slice(&(settled.block_count() as u64).to_le_bytes());
    body.extend_from_slice(&witness.to_le_bytes());
    body.extend_from_slice(&settled.forgotten.to_le_bytes());
    body.extend_from_slice(&(first_block as u64).to_le_bytes());
    body.resize(SIDECAR_HEADER, 0);
    for b in first_block..settled.block_count() {
        match settled.view(b) {
            BlockView::Empty => {
                body.push(0);
                body.extend_from_slice(&0u16.to_le_bytes());
            }
            BlockView::Dense(raw) => {
                body.push(1);
                body.extend_from_slice(raw);
            }
            BlockView::Sparse(offsets, codes) => {
                body.push(0);
                body.extend_from_slice(&(offsets.len() as u16).to_le_bytes());
                for (offset, code) in offsets.iter().zip(codes.iter()) {
                    body.extend_from_slice(&offset.to_le_bytes());
                    body.push(*code);
                }
            }
        }
    }
    let crc = super::super::storage_providers::CRC.checksum(&body);
    body.extend_from_slice(&crc.to_le_bytes());

    let target = sidecar_path(path);
    let mut temp_name = target.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    temp_name.push(".tmp");
    let temp = target.with_file_name(temp_name);
    {
        let mut f = File::create(&temp)?;
        f.write_all(&body)?;
        f.sync_all()?;
    }
    std::fs::rename(&temp, &target)?;
    if let Some(dir) = target.parent() {
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

impl StatusLog {
    /// [`StatusLog::open`] with settling on.
    ///
    /// Reads the file from where the last sidecar stopped — the settled prefix
    /// is never read again — and settles every whole block below the highest
    /// recorded id. What is above goes into the bitmap; this run's ids start
    /// above it. Two streaming passes, so a long history costs no more memory
    /// at open than it will hold settled.
    fn open_settled(path: PathBuf, mut file: File) -> io::Result<Self> {
        let previous = read_sidecar(&path, &mut file);
        let mut settled = previous.clone().unwrap_or_else(Settled::empty);
        let start_byte = settled.below / ENTRIES_PER_BYTE;
        let mut highest = settled.highest;

        // Pass 1: the highest recorded id.
        file.seek(SeekFrom::Start(start_byte))?;
        let mut chunk = vec![0u8; 1 << 20];
        let mut at = start_byte;
        loop {
            let n = file.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            for (i, byte) in chunk[..n].iter().enumerate() {
                if *byte != 0 {
                    for slot in 0..ENTRIES_PER_BYTE {
                        if (byte >> (slot * 2)) & 0b11 != BITS_IN_PROGRESS {
                            highest = highest.max((at + i as u64) * ENTRIES_PER_BYTE + slot);
                        }
                    }
                }
            }
            at += n as u64;
        }

        // Pass 2: whole blocks below it into the settled set. Ids above the
        // highest recorded are this run's to assign, and the block holding it
        // may hold some.
        let below = ((highest + 1) / BLOCK_IDS * BLOCK_IDS).max(settled.below);
        let end_byte = below / ENTRIES_PER_BYTE;
        file.seek(SeekFrom::Start(start_byte))?;
        let mut block = vec![0u8; BLOCK_BYTES];
        let mut byte = start_byte;
        while byte < end_byte {
            file.read_exact(&mut block)?;
            settled.push_block(&block);
            byte += BLOCK_BYTES as u64;
        }
        settled.highest = if below > 0 { highest.min(below - 1) } else { 0 };

        let previous_below = previous.as_ref().map_or(0, |p| p.below);
        if below > previous_below {
            if let Err(e) = write_sidecar(&path, &mut file, &settled) {
                // Still correct for this run, which settles from what it just
                // read; the next open will simply do the work again.
                log::warn!("transaction status sidecar could not be written: {}", e);
            }
        }

        let settled_forgotten = settled.forgotten;

        // The bitmap holds what is above the horizon.
        let base = (below / ENTRIES_PER_BYTE) as usize;
        let map = Bitmap::with_base(base);
        let mut tail = Vec::new();
        file.seek(SeekFrom::Start(base as u64))?;
        file.read_to_end(&mut tail)?;
        map.fill_from_at(base, &tail);

        Ok(StatusLog {
            view: ArcSwap::from_pointee(Generation {
                settled: (below > 0).then_some(settled),
                bits: Arc::new(map),
            }),
            settle: true,
            version: std::sync::atomic::AtomicU64::new(0),
            id: NEXT_LOG_ID.fetch_add(1, Ordering::Relaxed),
            inner: Mutex::new(Inner {
                file: Some(file),
                path: Some(path),
                highest,
                wal: None,
                dirty: None,
                staged: Vec::new(),
                captured: None,
                persisted_below: below,
                persisted_forgotten: settled_forgotten,
                persist_step: SIDECAR_PERSIST_STEP,
            }),
            dirty_bytes: AtomicUsize::new(0).into(),
        })
    }

    /// How far the horizon must move before a running log rewrites its
    /// sidecar. For tests; the default is [`SIDECAR_PERSIST_STEP`].
    pub fn set_persist_step(&self, ids: TxId) {
        self.inner.lock().persist_step = ids.max(1);
    }

    /// Whether this log settles. See [`settle_on_open`].
    pub fn settles(&self) -> bool {
        self.settle
    }

    /// Moves the settled horizon up to `bound` (rounded down to a block) and
    /// releases the bitmap below it. Returns the new horizon.
    ///
    /// **The caller's contract:** every id below `bound` has a final outcome
    /// and will never be recorded again — `bound` is at most the oldest
    /// running transaction. `TransactionManager::maybe_settle` passes
    /// `min(oldest_active, oldest live snapshot xmin)`.
    ///
    /// Under the log's `Mutex`, so no record lands between the copy and the
    /// swap: the new generation holds every byte the old one did. Readers still
    /// holding the old one keep it alive and read correct, if older, bytes —
    /// and nothing they could be asked about changed: a byte below the new
    /// horizon is final, and one above it is recorded only into the new
    /// generation, which any lookup that must see it will load.
    pub fn settle_below(&self, bound: TxId) -> TxId {
        if !self.settle {
            return 0;
        }
        let mut inner = self.inner.lock();
        let view = self.view.load_full();
        let old_below = view.below();
        let len_ids = view.bits.len() as u64 * ENTRIES_PER_BYTE;
        let below = (bound.min(len_ids) / BLOCK_IDS) * BLOCK_IDS;
        if below <= old_below {
            return old_below;
        }
        let old_base = (old_below / ENTRIES_PER_BYTE) as usize;
        let new_base = (below / ENTRIES_PER_BYTE) as usize;

        let mut settled = view.settled.clone().unwrap_or_else(Settled::empty);
        let mut raw = vec![0u8; BLOCK_BYTES];
        let mut byte = old_base;
        while byte < new_base {
            for (i, slot) in raw.iter_mut().enumerate() {
                *slot = view.bits.get(byte + i).unwrap_or(0);
            }
            settled.push_block(&raw);
            byte += BLOCK_BYTES;
        }
        settled.highest = inner.highest.min(below - 1);

        // The bitmap above the horizon, copied whole.
        let len = view.bits.len();
        let tail: Vec<u8> = (new_base..len).map(|i| view.bits.get(i).unwrap_or(0)).collect();
        let bits = Bitmap::with_base(new_base);
        bits.fill_from_at(new_base, &tail);
        if bits.len() < len {
            bits.grow_to(len);
        }

        // Dirty bytes that are about to leave the bitmap before reaching the
        // file: kept, so `stage` can still write them.
        if inner.file.is_some() {
            if let Some((low, _)) = inner.dirty {
                if low < new_base {
                    let (start, mut held) = inner
                        .captured
                        .take()
                        .unwrap_or_else(|| (low, Vec::new()));
                    let from = start + held.len();
                    held.extend((from.max(low)..new_base).map(|i| view.bits.get(i).unwrap_or(0)));
                    inner.captured = Some((start, held));
                }
            }
        }

        self.view.store(Arc::new(Generation {
            settled: Some(settled),
            bits: Arc::new(bits),
        }));
        // After the store, and before the `Mutex` lets any writer near the new
        // generation: see `with_view`.
        self.version.fetch_add(1, Ordering::Release);

        self.persist_sidecar(&mut inner);
        below
    }

    /// Rewrites the sidecar for the part of the horizon whose bytes are all in
    /// the status file: below anything dirty, captured or staged. Bytes there
    /// were written after the log record that justifies them was durable, so
    /// the witness read from the file now is what a restart will read.
    fn persist_sidecar(&self, inner: &mut Inner) {
        let Some(path) = inner.path.clone() else {
            return;
        };
        let view = self.view.load();
        let Some(settled) = view.settled.as_ref() else {
            return;
        };
        let mut durable_bytes = (settled.below / ENTRIES_PER_BYTE) as usize;
        if let Some((low, _)) = inner.dirty {
            durable_bytes = durable_bytes.min(low);
        }
        if let Some((start, _)) = &inner.captured {
            durable_bytes = durable_bytes.min(*start);
        }
        for s in &inner.staged {
            durable_bytes = durable_bytes.min(s.low);
        }
        let blocks = durable_bytes / BLOCK_BYTES;
        let horizon = blocks as u64 * BLOCK_IDS;
        // Chunks below the forgotten horizon are gone; a durable horizon that
        // falls among them cannot be written out, and the next settle will.
        if blocks < settled.first_chunk * CHUNK_BLOCKS {
            return;
        }
        let forgotten = settled.forgotten.min(horizon);
        if horizon < inner.persisted_below + inner.persist_step
            && forgotten < inner.persisted_forgotten + inner.persist_step
        {
            return;
        }
        let prefix = settled.prefix(blocks);
        let Some(file) = inner.file.as_mut() else {
            return;
        };
        match write_sidecar(&path, file, &prefix) {
            Ok(()) => {
                inner.persisted_below = horizon;
                inner.persisted_forgotten = prefix.forgotten;
            }
            Err(e) => log::warn!("transaction status sidecar could not be written: {}", e),
        }
    }

    /// Heap bytes the outcome record holds: bitmap levels and the settled set.
    pub fn memory_bytes(&self) -> usize {
        let view = self.view.load();
        view.bits.allocated_bytes() + view.settled.as_ref().map_or(0, |s| s.heap_bytes())
    }

    /// Forgets which ids below `to` (capped at the settled horizon) did not
    /// commit: they read as committed from here on. Returns the forgotten
    /// horizon.
    ///
    /// **The caller's contract** — `TransactionManager::try_forget` keeps it —
    /// is that no row version anywhere still carries any of those ids as
    /// `xmin` or `xmax`, durably, and that no reader still holds a pointer
    /// read before that was made so. Nothing can then ask about them.
    ///
    /// The status file is untouched: it still says which aborted, so a build
    /// that does not forget, or an older one, reads the same answers it always
    /// did for every id a row can carry.
    pub fn forget_below(&self, to: TxId) -> TxId {
        if !self.settle {
            return 0;
        }
        let mut inner = self.inner.lock();
        let view = self.view.load_full();
        let Some(settled) = view.settled.as_ref() else {
            return 0;
        };
        if to.min(settled.below) <= settled.forgotten {
            return settled.forgotten;
        }
        let next = settled.forgetting(to);
        let forgotten = next.forgotten;
        self.view.store(Arc::new(Generation {
            settled: Some(next),
            bits: Arc::clone(&view.bits),
        }));
        self.version.fetch_add(1, Ordering::Release);
        self.persist_sidecar(&mut inner);
        forgotten
    }

    /// The id below which this log answers "committed" without a lookup.
    pub fn forgotten_below(&self) -> TxId {
        self.view
            .load()
            .settled
            .as_ref()
            .map_or(0, |s| s.forgotten)
    }

    /// The id below which outcomes are settled, or 0.
    pub fn settled_below(&self) -> TxId {
        self.view.load().below()
    }

    /// The horizon the sidecar on disk records. Diagnostic.
    pub fn persisted_below(&self) -> TxId {
        self.inner.lock().persisted_below
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// N26: an `Aborted` recorded over `Committed` is counted, process-wide,
    /// for the snapshot that must not publish across it. Nothing else counts:
    /// an abort of a running or never-recorded id is the ordinary case.
    #[test]
    fn a_committed_outcome_rewritten_as_aborted_is_counted() {
        let log = StatusLog::in_memory();
        let before = outcome_revocations();
        log.record(5, TxOutcome::Committed);
        log.record(5, TxOutcome::Aborted);
        assert_eq!(log.outcome(5), TxOutcome::Aborted);
        assert!(
            outcome_revocations() > before,
            "a Committed outcome was revoked and nothing counted it"
        );

        for (old, new, revokes) in [
            (BITS_COMMITTED, BITS_ABORTED, true),
            (BITS_IN_PROGRESS, BITS_ABORTED, false),
            (BITS_ABORTED, BITS_ABORTED, false),
            (BITS_IN_PROGRESS, BITS_COMMITTED, false),
            (BITS_COMMITTED, BITS_COMMITTED, false),
            (BITS_IN_PROGRESS, BITS_IN_PROGRESS, false),
        ] {
            assert_eq!(revokes_a_commit(old, new), revokes, "{old:#04b} -> {new:#04b}");
        }
    }

    /// The bitmap grows in doubling levels, so an id can land in a level that
    /// did not exist when its neighbour was recorded. Every byte has to survive
    /// that, including the ones at the seams.
    #[test]
    fn outcomes_survive_growth_across_levels() {
        let log = StatusLog::in_memory();

        // Level `l` starts at byte BASE * ((1 << l) - 1), so these ids sit on
        // both sides of the first several seams as well as far past them.
        let mut ids: Vec<TxId> = vec![0, 1, 2, 3, 7, 4095];
        for level in 0..6u64 {
            let first_byte = BASE as u64 * ((1 << level) - 1);
            let first_id = first_byte * ENTRIES_PER_BYTE;
            ids.extend([first_id.saturating_sub(1), first_id, first_id + 1]);
        }
        ids.push(4_000_000);
        ids.sort_unstable();
        ids.dedup();

        for (n, id) in ids.iter().enumerate() {
            log.record(
                *id,
                if n % 2 == 0 {
                    TxOutcome::Committed
                } else {
                    TxOutcome::Aborted
                },
            );
        }
        for (n, id) in ids.iter().enumerate() {
            let expected = if n % 2 == 0 {
                TxOutcome::Committed
            } else {
                TxOutcome::Aborted
            };
            assert_eq!(log.outcome(*id), expected, "id {}", id);
        }
    }

    /// An id past everything recorded reads as aborted, and recording a high id
    /// must not make the ids below it start reading as in-progress instead.
    #[test]
    fn ids_past_the_recorded_length_are_aborted() {
        let log = StatusLog::in_memory();
        assert_eq!(
            log.outcome(5),
            TxOutcome::Aborted,
            "nothing recorded at all"
        );

        log.record(5, TxOutcome::Committed);
        // Same byte, never recorded: in progress, as it was before.
        assert_eq!(log.outcome(4), TxOutcome::InProgress);
        // Past the rounded-up length.
        assert_eq!(log.outcome(4 * GROWTH_CHUNK as TxId), TxOutcome::Aborted);
    }

    /// Readers take no lock, so a writer's byte updates have to reach them
    /// whole: four transactions share a byte, and a reader must never see one
    /// of them lose its recorded outcome to a neighbour being written.
    #[test]
    fn concurrent_readers_see_a_writers_outcomes() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;

        const IDS: TxId = 20_000;
        let log = Arc::new(StatusLog::in_memory());
        let stop = Arc::new(AtomicBool::new(false));

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let log = log.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    // Once an id reads committed it must keep reading committed:
                    // the only writes are of *other* ids, three of which share
                    // its byte.
                    let mut seen = vec![false; IDS as usize];
                    while !stop.load(Ordering::Relaxed) {
                        for id in 0..IDS {
                            match log.outcome(id) {
                                TxOutcome::Committed => seen[id as usize] = true,
                                other => assert!(
                                    !seen[id as usize],
                                    "id {} was committed and now reads {:?}",
                                    id, other
                                ),
                            }
                        }
                    }
                })
            })
            .collect();

        for id in 0..IDS {
            log.record(id, TxOutcome::Committed);
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }

        for id in 0..IDS {
            assert_eq!(log.outcome(id), TxOutcome::Committed, "id {}", id);
        }
    }

    #[test]
    fn an_outcome_survives_being_written_and_read_back() {
        let log = StatusLog::in_memory();
        log.record(7, TxOutcome::Committed);
        log.record(8, TxOutcome::Aborted);
        log.record(9, TxOutcome::InProgress);

        assert_eq!(log.outcome(7), TxOutcome::Committed);
        assert_eq!(log.outcome(8), TxOutcome::Aborted);
        assert_eq!(log.outcome(9), TxOutcome::InProgress);
    }

    /// Four entries share a byte, so writing one must not disturb its
    /// neighbours.
    #[test]
    fn neighbouring_ids_do_not_overwrite_each_other() {
        let log = StatusLog::in_memory();
        for id in 4..12 {
            log.record(
                id,
                if id % 2 == 0 {
                    TxOutcome::Committed
                } else {
                    TxOutcome::Aborted
                },
            );
        }
        for id in 4..12 {
            let expected = if id % 2 == 0 {
                TxOutcome::Committed
            } else {
                TxOutcome::Aborted
            };
            assert_eq!(log.outcome(id), expected, "id {}", id);
        }
    }

    /// An outcome can be rewritten — a transaction is recorded as in-progress
    /// when it starts and again when it ends.
    #[test]
    fn an_outcome_can_be_replaced_in_place() {
        let log = StatusLog::in_memory();
        log.record(5, TxOutcome::InProgress);
        log.record(6, TxOutcome::Committed);
        log.record(5, TxOutcome::Committed);

        assert_eq!(log.outcome(5), TxOutcome::Committed);
        assert_eq!(
            log.outcome(6),
            TxOutcome::Committed,
            "the neighbour is untouched"
        );
    }

    /// An id from a run that crashed before recording anything must not be read
    /// as live data.
    #[test]
    fn an_unknown_id_reads_as_aborted() {
        let log = StatusLog::in_memory();
        assert_eq!(log.outcome(99_999), TxOutcome::Aborted);
    }

    #[test]
    fn outcomes_survive_a_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");

        {
            let log = StatusLog::open(&path).expect("open");
            log.record(2, TxOutcome::Committed);
            log.record(3, TxOutcome::Aborted);
            log.record(40, TxOutcome::Committed);
            log.flush().expect("flush");
        }

        let log = StatusLog::open(&path).expect("reopen");
        assert_eq!(log.outcome(2), TxOutcome::Committed);
        assert_eq!(log.outcome(3), TxOutcome::Aborted);
        assert_eq!(log.outcome(40), TxOutcome::Committed);
        assert_eq!(
            log.highest_recorded(),
            40,
            "ids must resume above what was used"
        );
    }

    /// The unflushed span is what a read-only statement watches to decide when
    /// the accumulation is worth folding away.
    #[test]
    fn the_dirty_span_grows_with_records_and_clears_on_flush() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = StatusLog::open(dir.path().join("status")).expect("open");
        assert_eq!(log.dirty_bytes(), 0, "nothing recorded yet");

        log.record(4, TxOutcome::Committed);
        let one = log.dirty_bytes();
        assert!(one > 0, "a record left nothing dirty");

        // Four ids to a byte, so this is a byte apiece and the span widens.
        log.record(4_000, TxOutcome::Committed);
        assert!(
            log.dirty_bytes() > one,
            "the span did not widen: {}",
            log.dirty_bytes()
        );

        log.flush().expect("flush");
        assert_eq!(log.dirty_bytes(), 0, "the flush did not clear the span");
    }

    /// The file copy may not reach the disk before the log record that
    /// justifies it.
    ///
    /// Written the other way round, a crash in between leaves the file naming a
    /// transaction as committed whose rows never became durable — a write
    /// reported as successful that silently is not there. So a deferred flush
    /// stages the bytes and only `apply_staged` puts them in the file, once the
    /// log holding them has been synced.
    #[test]
    fn a_deferred_flush_reaches_the_file_only_after_the_log_is_durable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");
        let log = StatusLog::open(&path).expect("open");
        let wal = std::sync::Arc::new(Wal::open(dir.path().join("test.wal")).expect("wal"));
        log.attach_wal(wal.clone()).unwrap();

        // id 7 lives in byte 1, in the top two bits of it.
        log.record(7, TxOutcome::Committed);
        log.flush_deferred().expect("deferred flush");

        let staged = std::fs::read(&path).expect("read");
        assert!(
            staged.get(1).copied().unwrap_or(0) == 0,
            "the bytes reached the file before the log was durable: {:?}",
            staged
        );
        // Applying now must do nothing: the record is not durable yet.
        log.apply_staged().expect("apply with nothing durable");
        let still = std::fs::read(&path).expect("read");
        assert!(
            still.get(1).copied().unwrap_or(0) == 0,
            "applied before the sync"
        );

        // The sync the caller owed, then the copy.
        wal.sync_pending().expect("sync");
        log.apply_staged().expect("apply");

        let applied = std::fs::read(&path).expect("read");
        let expected = BITS_COMMITTED << ((7 % ENTRIES_PER_BYTE) * 2);
        assert_eq!(
            applied.get(1).copied().unwrap_or(0),
            expected,
            "the outcome never reached the file"
        );
    }

    /// `flush` is the durable variant: the bytes are in the file by the time it
    /// returns, with no second call needed.
    #[test]
    fn a_plain_flush_is_durable_on_return_even_with_a_log_attached() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");
        let log = StatusLog::open(&path).expect("open");
        let wal = std::sync::Arc::new(Wal::open(dir.path().join("test.wal")).expect("wal"));
        log.attach_wal(wal).unwrap();

        log.record(7, TxOutcome::Committed);
        log.flush().expect("flush");

        let bytes = std::fs::read(&path).expect("read");
        let expected = BITS_COMMITTED << ((7 % ENTRIES_PER_BYTE) * 2);
        assert_eq!(
            bytes.get(1).copied().unwrap_or(0),
            expected,
            "flush was not durable"
        );
    }

    /// A staged range left over from a deferred flush is not lost when a later
    /// flush comes along — both end up in the file.
    #[test]
    fn a_deferred_range_is_still_applied_by_a_later_flush() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");
        let log = StatusLog::open(&path).expect("open");
        let wal = std::sync::Arc::new(Wal::open(dir.path().join("test.wal")).expect("wal"));
        log.attach_wal(wal).unwrap();

        log.record(7, TxOutcome::Committed);
        log.flush_deferred().expect("deferred");
        log.record(4_000, TxOutcome::Aborted);
        log.flush().expect("flush");

        let bytes = std::fs::read(&path).expect("read");
        assert_eq!(
            bytes.get(1).copied().unwrap_or(0),
            BITS_COMMITTED << ((7 % ENTRIES_PER_BYTE) * 2),
            "the deferred range was dropped"
        );
        let far = (4_000 / ENTRIES_PER_BYTE) as usize;
        assert_eq!(
            bytes.get(far).copied().unwrap_or(0),
            BITS_ABORTED << ((4_000 % ENTRIES_PER_BYTE) * 2),
            "the later range never landed"
        );
    }

    /// A transaction still marked in-progress at reopen ended by crashing.
    #[test]
    fn a_transaction_interrupted_by_a_crash_is_not_committed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");

        {
            let log = StatusLog::open(&path).expect("open");
            log.record(2, TxOutcome::Committed);
            log.record(3, TxOutcome::InProgress);
            log.flush().expect("flush");
        }

        let log = StatusLog::open(&path).expect("reopen");
        assert_ne!(log.outcome(3), TxOutcome::Committed);
    }

    // -- Settling ---------------------------------------------------------

    /// Records a random history into a fresh or reopened log and returns the
    /// outcome each id was given. `crash` leaves some ids in progress, as a
    /// run that died would.
    fn random_run(log: &StatusLog, from: TxId, n: u64, rng: &mut u64, crash: bool) -> TxId {
        random_run_rate(log, from, n, rng, crash, 400)
    }

    /// `one_in`: how rarely an id aborts (or, with `crash`, is left unfinished).
    fn random_run_rate(
        log: &StatusLog,
        from: TxId,
        n: u64,
        rng: &mut u64,
        crash: bool,
        one_in: u64,
    ) -> TxId {
        let mut next = || {
            *rng ^= *rng << 13;
            *rng ^= *rng >> 7;
            *rng ^= *rng << 17;
            *rng
        };
        for id in from..from + n {
            log.record(id, TxOutcome::InProgress);
            match next() % one_in {
                0 => log.record(id, TxOutcome::Aborted),
                1 if crash => {}
                _ => log.record(id, TxOutcome::Committed),
            }
        }
        log.flush().expect("flush");
        from + n
    }

    fn answers(log: &StatusLog, upto: TxId) -> Vec<TxOutcome> {
        (0..upto).map(|id| log.outcome(id)).collect()
    }

    fn first_difference(a: &[TxOutcome], b: &[TxOutcome]) -> Option<(usize, TxOutcome, TxOutcome)> {
        a.iter().zip(b.iter()).enumerate().find(|(_, (x, y))| x != y).map(|(i, (x, y))| (i, *x, *y))
    }

    /// Settling changes where an answer comes from and never the answer:
    /// across random histories, runs that crash with ids in progress, and
    /// settle-on and settle-off opens interleaved on one file, every outcome
    /// and every resumed id counter agrees with the plain bitmap.
    #[test]
    fn settling_changes_no_answer() {
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut settled_opens = 0;
        for round in 0..12 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("tx.status");
            let mut next_id = FIRST_TX_FOR_TESTS;
            for run in 0..5 {
                let settle_this_run = (round + run) % 2 == 0;
                let log = StatusLog::open_with(&path, settle_this_run).unwrap();
                // Compare against a plain open of the same file, before this
                // run writes anything.
                let plain = StatusLog::open_with(&path, false).unwrap();
                if log.settled_below() > 0 {
                    settled_opens += 1;
                }
                let upto = next_id + 64;
                assert_eq!(
                    first_difference(&answers(&log, upto), &answers(&plain, upto)),
                    None,
                    "round {} run {} settle {} below {}",
                    round,
                    run,
                    settle_this_run,
                    log.settled_below()
                );
                assert_eq!(log.highest_recorded(), plain.highest_recorded());
                drop(plain);
                // Ids resume above everything recorded, as the manager does.
                let from = log.highest_recorded().max(FIRST_TX_FOR_TESTS - 1) + 1;
                let n = 1 + (rng % 20_000);
                // Some runs abort often enough that the sparse form would be
                // the larger, and settling must decline rather than bloat.
                let one_in = if run == 3 { 4 } else { 400 };
                next_id = random_run_rate(&log, from, n, &mut rng, run % 2 == 1, one_in);
                // In the same run, the new ids answer as recorded.
                let plain = StatusLog::open_with(&path, false).unwrap();
                assert_eq!(
                    // Up to what was recorded: past it the live map has grown
                    // in whole chunks and the file has not, which is not
                    // settling's doing.
                    first_difference(&answers(&log, next_id), &answers(&plain, next_id)),
                    None,
                    "after run {} (settle {}, below {})",
                    run,
                    settle_this_run,
                    log.settled_below()
                );
            }
        }
        assert!(settled_opens >= 10, "only {} opens settled anything", settled_opens);
    }

    const FIRST_TX_FOR_TESTS: TxId = crate::core::mvcc::transaction::FIRST_TX;

    /// Visibility, not just outcomes, is identical with settling on: the same
    /// rows are visible to the same snapshots.
    #[test]
    fn settling_changes_no_visibility() {
        use crate::core::mvcc::transaction::{NO_TX, TransactionManager};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tx.status");
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        let end = random_run(&StatusLog::open_with(&path, false).unwrap(), 2, 20_000, &mut rng, true);

        let on = TransactionManager::new(StatusLog::open_with(&path, true).unwrap());
        // Two live managers over one file is this test's device, not a real
        // state: the second would resume above the first's id reservation
        // (and record the first's reserved block as aborted). Each must start
        // from the file as the run left it.
        std::fs::remove_file(reservation_path(&path)).unwrap();
        let off = TransactionManager::new(StatusLog::open_with(&path, false).unwrap());
        assert!(on.status().settled_below() > 0, "the run settled something");
        assert_eq!(on.next_id(), off.next_id());
        // A few transactions running in this process, as snapshots would see.
        let a = on.begin();
        let b = off.begin();
        assert_eq!(a, b);
        let (sa, sb) = (on.snapshot(NO_TX), off.snapshot(NO_TX));
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for _ in 0..200_000 {
            let xmin = next() % (end + 4);
            let xmax = if next() % 3 == 0 { NO_TX } else { next() % (end + 4) };
            assert_eq!(
                sa.sees(xmin, xmax, &|id| on.outcome(id)),
                sb.sees(xmin, xmax, &|id| off.outcome(id)),
                "xmin {} xmax {}",
                xmin,
                xmax
            );
        }
    }

    /// A crash while writing the sidecar leaves a stray temporary or a torn
    /// file; neither is trusted, and the open settles from the status file.
    #[test]
    fn a_torn_or_stray_sidecar_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tx.status");
        let mut rng = 7u64;
        let end = random_run(&StatusLog::open_with(&path, false).unwrap(), 2, 9_000, &mut rng, true);
        let expected = answers(&StatusLog::open_with(&path, false).unwrap(), end + 8);

        // A stray temporary from a conversion that died before its rename.
        std::fs::write(sidecar_path(&path).with_extension("settled.tmp"), b"RDBSETL1garbage").unwrap();
        let log = StatusLog::open_with(&path, true).unwrap();
        assert!(log.settled_below() > 0);
        assert_eq!(answers(&log, end + 8), expected);
        drop(log);

        // A sidecar torn mid-write, and one with a flipped byte.
        let good = std::fs::read(sidecar_path(&path)).unwrap();
        for bad in [good[..good.len() / 2].to_vec(), {
            let mut b = good.clone();
            let mid = b.len() / 2;
            b[mid] ^= 0x40;
            b
        }] {
            std::fs::write(sidecar_path(&path), &bad).unwrap();
            let mut f = File::open(&path).unwrap();
            assert!(read_sidecar(&path, &mut f).is_none());
            let log = StatusLog::open_with(&path, true).unwrap();
            assert_eq!(answers(&log, end + 8), expected);
        }

        // A sidecar beside a different status file — restored, replaced — is
        // void: shorter, or the same length with other bytes under the horizon.
        let log = StatusLog::open_with(&path, true).unwrap();
        let below = log.settled_below();
        drop(log);
        let mut f = File::open(&path).unwrap();
        assert!(read_sidecar(&path, &mut f).is_some(), "the good sidecar reads");
        let original = std::fs::read(&path).unwrap();
        let mut altered = original.clone();
        let at = (below / ENTRIES_PER_BYTE) as usize - 1;
        altered[at] ^= 0b11;
        std::fs::write(&path, &altered).unwrap();
        let mut f = File::open(&path).unwrap();
        assert!(read_sidecar(&path, &mut f).is_none(), "different bytes under the horizon");
        std::fs::write(&path, &original[..64]).unwrap();
        let mut f = File::open(&path).unwrap();
        assert!(read_sidecar(&path, &mut f).is_none(), "a shorter file");
    }

    /// Log replay writes into the status file by offset, including below the
    /// settled horizon. Replay only re-applies history older than the sidecar
    /// (ending at the same final bytes), so the answers stand — and a write
    /// there never touches the bitmap, which does not hold those bytes.
    #[test]
    fn a_replayed_write_below_the_horizon_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tx.status");
        let mut rng = 11u64;
        let end = random_run(&StatusLog::open_with(&path, false).unwrap(), 2, 9_000, &mut rng, false);
        let expected = answers(&StatusLog::open_with(&path, false).unwrap(), end + 8);
        let settled = StatusLog::open_with(&path, true).unwrap();
        let below = settled.settled_below();
        assert!(below > 400);
        drop(settled);

        // An old record (ids still in progress) and then its successor, as a
        // replay applies them, at an offset well below the horizon.
        let offset = 100u64;
        let final_byte = std::fs::read(&path).unwrap()[offset as usize];
        let mut f = OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(offset)).unwrap();
        f.write_all(&[0]).unwrap();
        f.seek(SeekFrom::Start(offset)).unwrap();
        f.write_all(&[final_byte]).unwrap();
        drop(f);

        let log = StatusLog::open_with(&path, true).unwrap();
        assert_eq!(log.settled_below(), below);
        assert_eq!(answers(&log, end + 8), expected);
    }

    /// The format a future build may rewrite the file in is refused, with or
    /// without settling, rather than read as outcome bits.
    #[test]
    fn a_rewritten_format_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tx.status");
        let mut bytes = REWRITTEN_FORMAT_MAGIC.to_vec();
        bytes.extend_from_slice(&[0x55; 64]);
        std::fs::write(&path, &bytes).unwrap();
        for settle in [false, true] {
            let err = StatusLog::open_with(&path, settle).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{}", err);
        }
        // And no bitmap can begin with it: ids 0 and 1 are never recorded.
        let log = StatusLog::open_with(dir.path().join("plain.status"), false).unwrap();
        log.record(2, TxOutcome::Committed);
        log.flush().unwrap();
        assert_eq!(std::fs::read(dir.path().join("plain.status")).unwrap()[0] & 0x0F, 0);
    }

    /// Settled, a long history costs a few bytes per transaction that did not
    /// commit, instead of two bits per transaction ever run.
    #[test]
    fn a_settled_log_holds_far_less_than_its_bitmap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tx.status");
        let log = StatusLog::open_with(&path, false).unwrap();
        for id in 2..400_002u64 {
            log.record(
                id,
                if id % 1000 == 0 {
                    TxOutcome::Aborted
                } else {
                    TxOutcome::Committed
                },
            );
        }
        log.flush().unwrap();
        drop(log);
        let plain = StatusLog::open_with(&path, false).unwrap();
        let settled = StatusLog::open_with(&path, true).unwrap();
        assert!(plain.memory_bytes() >= 100_000, "{}", plain.memory_bytes());
        assert!(
            settled.memory_bytes() * 10 < plain.memory_bytes(),
            "settled {} vs plain {}",
            settled.memory_bytes(),
            plain.memory_bytes()
        );
    }

    /// RSS of a log after 12 million transactions, plain and settled.
    ///
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored status_log_memory_at_scale --nocapture`
    #[test]
    #[ignore]
    fn status_log_memory_at_scale() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tx.status");
        const N: u64 = 12_000_000;
        {
            let log = StatusLog::open_with(&path, false).unwrap();
            for id in 2..N {
                log.record(
                    id,
                    if id % 997 == 0 {
                        TxOutcome::Aborted
                    } else {
                        TxOutcome::Committed
                    },
                );
            }
            log.flush().unwrap();
        }
        let rss = || memory_stats::memory_stats().map_or(0, |m| m.physical_mem);
        for settle in [false, true, false, true] {
            let before = rss();
            let t = std::time::Instant::now();
            let log = StatusLog::open_with(&path, settle).unwrap();
            let opened = t.elapsed();
            let after = rss();
            let t = std::time::Instant::now();
            let mut committed = 0u64;
            for id in 2..N {
                if log.outcome(id) == TxOutcome::Committed {
                    committed += 1;
                }
            }
            let lookups = t.elapsed();
            println!(
                "settle {:>5}: open {:?}, held {} KiB (RSS +{} KiB), {:.2} ns/outcome, {} committed",
                settle,
                opened,
                log.memory_bytes() / 1024,
                after.saturating_sub(before) / 1024,
                lookups.as_nanos() as f64 / (N - 2) as f64,
                committed
            );
        }
    }

    // -- Runtime settling ---------------------------------------------------

    fn xorshift(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    /// Lookups racing a horizon that moves while they run, checked against a
    /// reference model.
    ///
    /// Writers begin transactions and commit or abort them, some held open for
    /// a while and some holding snapshots, so the horizon both stalls and
    /// jumps; each records the outcome in the model only after `commit` or
    /// `abort` has returned. Readers pick random ids and, wherever the model
    /// has an outcome, require the log to give exactly it. A settler forces
    /// settles back to back on top of the ones commits trigger, and a flusher
    /// writes the dirty range out between them, so captured bytes and staged
    /// copies are exercised too. At the end every outcome is checked again —
    /// live, and after reopening the file with settling on and off — and the
    /// memory held must be a small fraction of the bitmap it replaced.
    ///
    /// `RDB_SETTLE_STRESS_SECS` sets the duration (default 4 s).
    #[test]
    fn settling_under_concurrent_lookups_matches_a_reference_model() {
        use crate::core::mvcc::transaction::TransactionManager;
        use std::sync::atomic::{AtomicBool, AtomicU64};
        use std::sync::Arc;

        let secs: u64 = std::env::var("RDB_SETTLE_STRESS_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4);
        let cap: usize = std::env::var("RDB_SETTLE_STRESS_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(40_000_000);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tx.status");
        let m = Arc::new(TransactionManager::new(StatusLog::open_with(&path, true).unwrap()));
        m.set_settle_window(4096);
        // 0 unknown, 1 committed, 2 aborted.
        let model: Arc<Vec<AtomicU8>> = Arc::new((0..cap).map(|_| AtomicU8::new(0)).collect());
        let stop = Arc::new(AtomicBool::new(false));
        let checked = Arc::new(AtomicU64::new(0));
        let full = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::new();

        for w in 0..4u64 {
            let (m, model, stop, full) = (m.clone(), model.clone(), stop.clone(), full.clone());
            threads.push(std::thread::spawn(move || {
                let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (w + 1) * 0x1234_5678;
                let mut held: Vec<(TxId, Option<crate::core::mvcc::transaction::Snapshot>)> = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let tx = m.begin();
                    if tx as usize >= cap - 1024 {
                        full.store(true, Ordering::Relaxed);
                        m.abort(tx);
                        break;
                    }
                    let roll = xorshift(&mut rng) % 1000;
                    if roll < 3 && held.len() < 4 {
                        // Held open, sometimes with a snapshot pinning the horizon.
                        let snapshot = (roll == 0).then(|| m.snapshot(tx));
                        held.push((tx, snapshot));
                        continue;
                    }
                    let commit = roll % 10 != 0;
                    if commit {
                        m.commit(tx);
                    } else {
                        m.abort(tx);
                    }
                    model[tx as usize].store(if commit { 1 } else { 2 }, Ordering::Release);
                    if !held.is_empty() && xorshift(&mut rng) % 200 == 0 {
                        let (old, snapshot) = held.remove(0);
                        drop(snapshot);
                        m.commit(old);
                        model[old as usize].store(1, Ordering::Release);
                    }
                }
                for (old, snapshot) in held {
                    drop(snapshot);
                    m.abort(old);
                    model[old as usize].store(2, Ordering::Release);
                }
            }));
        }
        for r in 0..4u64 {
            let (m, model, stop, checked) = (m.clone(), model.clone(), stop.clone(), checked.clone());
            threads.push(std::thread::spawn(move || {
                let mut rng = 0xD1B5_4A32_D192_ED03u64 ^ (r + 7) * 0x9876_5431;
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let next = m.next_id();
                    if next <= 2 {
                        continue;
                    }
                    // Mostly near the moving horizon, where the races are.
                    let below = m.status().settled_below();
                    let id = if xorshift(&mut rng) % 2 == 0 {
                        let lo = below.saturating_sub(8192).max(2);
                        lo + xorshift(&mut rng) % (next - lo).max(1)
                    } else {
                        2 + xorshift(&mut rng) % (next - 2)
                    };
                    let expected = model[id as usize].load(Ordering::Acquire);
                    let got = m.status().outcome(id);
                    match expected {
                        1 => assert_eq!(got, TxOutcome::Committed, "id {} (settled below {})", id, below),
                        2 => assert_eq!(got, TxOutcome::Aborted, "id {} (settled below {})", id, below),
                        _ => {}
                    }
                    n += 1;
                }
                checked.fetch_add(n, Ordering::Relaxed);
            }));
        }
        {
            let (m, stop) = (m.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    m.settle_now();
                    std::thread::yield_now();
                }
            }));
        }
        {
            let (m, stop) = (m.clone(), stop.clone());
            threads.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    m.flush().unwrap();
                    std::thread::sleep(std::time::Duration::from_millis(3));
                }
            }));
        }

        let started = std::time::Instant::now();
        let mut peak = 0usize;
        while started.elapsed() < std::time::Duration::from_secs(secs) && !full.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(50));
            peak = peak.max(m.status().memory_bytes());
        }
        stop.store(true, Ordering::Relaxed);
        for t in threads {
            t.join().unwrap();
        }

        let next = m.next_id();
        assert!(checked.load(Ordering::Relaxed) > 10_000, "readers barely ran");
        assert!(next > 100_000, "writers barely ran: {}", next);
        let settled = m.settle_now();
        assert!(settled + 8192 >= next, "nothing holds the horizon now: {} of {}", settled, next);
        for id in 2..next {
            let expected = model[id as usize].load(Ordering::Acquire);
            let got = m.status().outcome(id);
            match expected {
                1 => assert_eq!(got, TxOutcome::Committed, "id {}", id),
                2 => assert_eq!(got, TxOutcome::Aborted, "id {}", id),
                _ => panic!("id {} has no model outcome", id),
            }
        }
        let bitmap_bytes = (next / ENTRIES_PER_BYTE) as usize;
        let held_now = m.status().memory_bytes();
        eprintln!(
            "stress: {} ids, {} lookups checked, held {} KiB now (peak {} KiB) vs {} KiB of bitmap",
            next,
            checked.load(Ordering::Relaxed),
            held_now / 1024,
            peak / 1024,
            bitmap_bytes / 1024
        );
        // One in ten aborts makes every block dense: the settled set then
        // costs what the bitmap did plus its index (under 2 %), and the live
        // window on top.
        assert!(
            held_now <= bitmap_bytes + bitmap_bytes / 32 + 256 * 1024,
            "settled memory {} exceeds the bitmap's {} plus the window",
            held_now,
            bitmap_bytes
        );

        // And on disk: both readings of the file give the model's answers.
        m.flush().unwrap();
        drop(m);
        for settle in [true, false] {
            let log = StatusLog::open_with(&path, settle).unwrap();
            for id in 2..next {
                let want = match model[id as usize].load(Ordering::Acquire) {
                    1 => TxOutcome::Committed,
                    _ => TxOutcome::Aborted,
                };
                assert_eq!(log.outcome(id), want, "reopened (settle {}), id {}", settle, id);
            }
        }
    }

    /// The same random history, with crashes, applied to a log that settles as
    /// it runs and to one that never settles: every answer agrees at every
    /// step, and the two status files stay byte-identical — settling never
    /// changes what reaches the disk, which is what keeps older builds able to
    /// read it.
    #[test]
    fn runtime_settling_changes_no_answer_and_no_byte() {
        for round in 0..6u64 {
            let dir = tempfile::tempdir().unwrap();
            let path_on = dir.path().join("on.status");
            let path_off = dir.path().join("off.status");
            let mut rng = 0x2545_F491_4F6C_DD1Du64 ^ (round + 1) * 0x5555;
            let mut on = StatusLog::open_with(&path_on, true).unwrap();
            let mut off = StatusLog::open_with(&path_off, false).unwrap();
            let mut next: TxId = 2;
            let mut open: Vec<TxId> = Vec::new();
            for step in 0..400 {
                // A burst of transactions, some left open.
                for _ in 0..(xorshift(&mut rng) % 600) {
                    let tx = next;
                    next += 1;
                    on.record(tx, TxOutcome::InProgress);
                    off.record(tx, TxOutcome::InProgress);
                    match xorshift(&mut rng) % 50 {
                        0 => open.push(tx),
                        1..=4 => {
                            on.record(tx, TxOutcome::Aborted);
                            off.record(tx, TxOutcome::Aborted);
                        }
                        _ => {
                            on.record(tx, TxOutcome::Committed);
                            off.record(tx, TxOutcome::Committed);
                        }
                    }
                }
                // Some held transactions finish.
                open.retain(|tx| {
                    if xorshift(&mut rng) % 8 == 0 {
                        let outcome = if xorshift(&mut rng) % 2 == 0 {
                            TxOutcome::Committed
                        } else {
                            TxOutcome::Aborted
                        };
                        on.record(*tx, outcome);
                        off.record(*tx, outcome);
                        false
                    } else {
                        true
                    }
                });
                // Settle as the manager would: below everything still open.
                let bound = open.iter().copied().min().unwrap_or(next);
                on.settle_below(bound);
                if xorshift(&mut rng) % 3 == 0 {
                    on.flush().unwrap();
                    off.flush().unwrap();
                    assert_eq!(
                        std::fs::read(&path_on).unwrap(),
                        std::fs::read(&path_off).unwrap(),
                        "round {} step {}: the files differ",
                        round,
                        step
                    );
                }
                let probe = (0..2_000)
                    .map(|_| xorshift(&mut rng) % (next + 64))
                    .chain(on.settled_below().saturating_sub(4)..on.settled_below() + 4);
                for id in probe {
                    assert_eq!(on.outcome(id), off.outcome(id), "round {} step {} id {}", round, step, id);
                }
                // Now and then, a crash: whatever was not flushed is gone, open
                // transactions with it, and both reopen from their files.
                if xorshift(&mut rng) % 40 == 0 {
                    drop(on);
                    drop(off);
                    on = StatusLog::open_with(&path_on, true).unwrap();
                    off = StatusLog::open_with(&path_off, false).unwrap();
                    assert_eq!(on.highest_recorded(), off.highest_recorded());
                    next = on.highest_recorded().max(1) + 1;
                    open.clear();
                    for id in 0..next + 64 {
                        assert_eq!(on.outcome(id), off.outcome(id), "after a crash, id {}", id);
                    }
                }
            }
            on.flush().unwrap();
            off.flush().unwrap();
            assert_eq!(std::fs::read(&path_on).unwrap(), std::fs::read(&path_off).unwrap());
            assert!(on.settled_below() > 0 || next < 8192, "round {} never settled", round);
        }
    }

    /// Memory held over millions of transactions run through the manager:
    /// bounded with settling, two bits a transaction without it.
    #[test]
    fn a_settling_log_stays_small_over_millions_of_transactions() {
        use crate::core::mvcc::transaction::TransactionManager;
        let run = |settle: bool, n: u64| -> (usize, usize) {
            let m = TransactionManager::new(StatusLog::in_memory_with(settle));
            let mut peak = 0usize;
            for i in 0..n {
                let tx = m.begin();
                if i % 997 == 0 {
                    m.abort(tx);
                } else {
                    m.commit(tx);
                }
                if i % 100_000 == 0 {
                    peak = peak.max(m.status().memory_bytes());
                }
            }
            (m.status().memory_bytes(), peak)
        };
        let n: u64 = std::env::var("RDB_SETTLE_MEMORY_TXNS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2_000_000);
        let (plain, _) = run(false, n);
        let (settled, peak) = run(true, n);
        eprintln!(
            "{} transactions: plain {} KiB, settled {} KiB (peak {} KiB)",
            n,
            plain / 1024,
            settled / 1024,
            peak / 1024
        );
        assert!(plain as u64 >= n / ENTRIES_PER_BYTE / 2, "{}", plain);
        // The window is 2^18 ids — 64 KiB of bitmap at most, plus a level of
        // slack — and the settled set is a few bytes per abort and per block.
        assert!(peak < 512 * 1024, "peak {} bytes", peak);
        assert!(settled * 4 < plain, "settled {} vs plain {}", settled, plain);
    }

    /// One transaction in ten aborting: without forgetting, the settled
    /// history keeps raw blocks and grows as the bitmap did; with a table that
    /// is swept as it goes (the vacuum service's part, simulated by advancing
    /// its handle to the settle bound), it stays bounded.
    #[test]
    fn a_forgetting_log_stays_small_with_heavy_aborts() {
        use crate::core::mvcc::transaction::TransactionManager;
        let n: u64 = std::env::var("RDB_SETTLE_MEMORY_TXNS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2_000_000);
        let run = |forget: bool| -> (usize, usize, TxId) {
            let m = TransactionManager::new(StatusLog::in_memory_with(true));
            let table = m.register_table();
            let mut peak = 0usize;
            for i in 0..n {
                let tx = m.begin();
                if i % 10 == 0 {
                    m.abort(tx);
                } else {
                    m.commit(tx);
                }
                if i % 100_000 == 0 {
                    if forget {
                        m.settle_now();
                        table.advance(m.settle_bound());
                        m.try_forget();
                    }
                    peak = peak.max(m.status().memory_bytes());
                }
            }
            (m.status().memory_bytes(), peak, m.status().forgotten_below())
        };
        let (kept, kept_peak, _) = run(false);
        let (forgot, forgot_peak, forgotten) = run(true);
        eprintln!(
            "{} transactions, 10% aborted: without forgetting {} KiB (peak {} KiB), with {} KiB (peak {} KiB), forgotten below {}",
            n,
            kept / 1024,
            kept_peak / 1024,
            forgot / 1024,
            forgot_peak / 1024,
            forgotten
        );
        assert!(forgotten > n / 2, "{}", forgotten);
        // Raw blocks: about the bitmap's two bits per transaction.
        assert!(kept as u64 >= n / ENTRIES_PER_BYTE / 2, "{}", kept);
        // Bounded by the window and a chunk or two of history, not by `n`.
        assert!(forgot_peak < 1024 * 1024, "peak {}", forgot_peak);
        assert!(forgot * 2 < kept, "forgot {} vs kept {}", forgot, kept);
    }
}
