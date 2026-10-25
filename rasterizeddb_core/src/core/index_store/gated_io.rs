//! `GatedIo`: the provider backstop for disk writes
//! (docs/persistent-indexes.md §2 layer 2, slice B12).
//!
//! Tickets (layer 1) are taken at named write paths, and a guard that
//! enumerates its cases misses one ("a guard is an allowlist"). So every
//! write a table's storage provider issues also registers with the file
//! set's gate word, structurally, at the provider: the B10 [`IoOp`] RMW
//! (`fetch_add` on the count, judged on the word it returned), held until the
//! write returns, so a seal's count covers a write parked inside the provider
//! (RV15). A plain load would let a write checked before `SEALED` land after
//! `CLEAN`.
//!
//! # Where it sits
//!
//! Not as a wrapper type around the provider (the plan's `GatedIo<S>`): a
//! table's providers are `Arc<S>` throughout the row layer, and wrapping
//! them would change every signature that takes one. Instead each file
//! provider (`LocalStorageProvider`, the only kind with a directory, so the
//! only kind whose word can ever be `Clean`; every other backend's word is
//! detached and `Dirty` for good, P33) carries a [`GatedIo`] in the commit
//! state its clones share, and every write entry point calls
//! [`GatedIo::admit_now`] first. A table attaches its word to the providers it
//! owns when it is assembled ([`GatedIo::attach`]: rows, pointers, `_flt.db`,
//! and for a plain table `_schema.db` and `_seq.db`); a provider created from
//! an attached one for a file named after it (`<t>_schema.db` creates
//! `<t>_schema_FIELDS.db` and every `_schema_*` sidecar) inherits the word
//! ([`GatedIo::inherit_from`]). A partition's schema and row-id files are the
//! logical table's, shared by every partition, and attach to none.
//!
//! # Verdicts, on the word the registration's `fetch_add` returned
//!
//! | word | tests | release |
//! |---|---|---|
//! | `Dirty`, unsealed; or sealed with a count (maybe a ticket holder's) | issued | issued |
//! | `Dirty`, `SEALED` and `CANCEL`, no `PUBLISHING` (a writer preempted an optional seal, P8) | issued | issued |
//! | `Dirty`, `SEALED`, no `CANCEL`, count zero: no ticket anywhere | **panic** with the call site | `CANCEL` by compare-and-swap (P7), issued |
//! | `PUBLISHING` | **panic**: no ticket can be held then | wait for the window, then judge again |
//! | `Clean` or `Unknown` | **panic** with the call site | the slow path inline (`DIRTY` durable, or the `Unknown` rewrite), then issued |
//!
//! In release a slow path that fails cannot be returned by the write
//! (`append_data_txn` returns offsets, `write_data` nothing): the write is
//! skipped and the failure recorded in `failed_appends`, under the write's
//! transaction (so the commit of that transaction fails, through whichever
//! file of the directory it commits: an `INSERT` writes its row and commits
//! through the pointers) or, for a write with no transaction, under the
//! directory's gate key, which the next commit in the directory reports.
//! `truncate` and the commit's landing return it.
//!
//! In production a registration costs one `fetch_add` and one `fetch_sub`
//! on the word; the verdict branches and every census hook compile away.
//!
//! # Writes that are not registered, or never panic (the allowlist)
//!
//! [`UNGATED`] lists them, each with its reason; the census test
//! `ungated_writes_are_exactly_the_listed_ones` pins that a workload over
//! every family writes a live file set around its word only inside those,
//! and reaches each. In tests a provider with no word that writes a file of
//! a live file set ([`super::file_gate::live_gate_for`]) outside a declared
//! [`ungated`] scope panics if that word is `Clean` or `Unknown`, so a
//! provider that missed its attach is caught like a missed ticket. Writes
//! that are not a provider's at all, and so outside this by construction:
//! - the gate's own files, `.ixm`, `.ixm.tmp` and `.ixs` (the slow path, the
//!   seal), which are what `DIRTY` and `CLEAN` are written to;
//! - `rdb.ixe`, written under `VACUUM FULL`'s ticket before its first effect,
//!   a directory file no table owns;
//! - the directory's log, its floors file `rasterized.wal.floors` and its
//!   status log: directory files, written by the commit path the family's
//!   ticket covers (§2 Ticket lifetime), not by a table's provider;
//! - a table's files before its word exists: `CREATE TABLE`'s, a `RENAME`'s
//!   or `MOVE`'s target, a rewrite's backup copy under its own name: no file
//!   set is live under those names, so nothing can be `CLEAN` over them;
//! - a partitioned table's own files (`__pt_<uid>` schema, sidecars, row ids,
//!   range map): the logical table's, which has no word of its own (its
//!   partitions do).
//!
//! [`IoOp`]: super::gate_word::IoOp

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};

use super::file_gate::FileGate;
use super::gate_word::{Hold, IoRegistration, SlowReason, WriteGate};

/// Which provider entry point a write came through: what a panic names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Site {
    /// `write_data_txn` (and `write_data`, which calls it).
    WriteInPlace,
    /// `append_data_txn` (and an immediate `append_data`).
    Append,
    /// A staged `append_data(.., false)` into the `.tmp` sidecar.
    Stage,
    /// `write_data_seek`.
    WriteSeek,
    /// `truncate`.
    Truncate,
    /// The commit's landing of staged bytes in the main file.
    Land,
    /// `swap_temp`.
    SwapTemp,
}

/// Why a write reaches a live file set without a registration that can
/// panic: the entries of [`UNGATED`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Ungated {
    /// `_flt.db`, through its registered provider, judged as release does.
    FilterCheckpoint,
    /// Staged bytes landed when a provider of a live file set opens.
    OpenLanding,
    /// An `ALTER` rewrite's files and its schema backup's restore, written
    /// through the database's seed while the old file set's word is live.
    SchemaRewrite,
}

/// The allowlist: every way a write reaches a live file set without a
/// registration that panics on a ticketless write, with why that is sound.
pub const UNGATED: &[(Ungated, &str)] = &[
    (
        Ungated::FilterCheckpoint,
        "_flt.db's runtime checkpoint loop writes on its own task under no ticket until B14 \
         retires the file (B11's census exempts it from the held-ticket rule likewise). Its \
         provider is registered, so a seal counts the write, but a ticketless write is judged as \
         release judges it: on a Clean or Unknown word the slow path makes DIRTY durable before \
         the write is issued, so T1 holds; during an unpublished seal it cancels the seal.",
    ),
    (
        Ungated::OpenLanding,
        "A provider that opens a file whose .tmp sidecar still holds staged bytes lands them \
         before anything can attach a word to it. The bytes were issued, to the .tmp, by an \
         earlier staging write that was registered on that provider's word; a seal lands every \
         .tmp before it publishes CLEAN (B13), so staged bytes exist only after the last clean \
         point, under its DIRTY.",
    ),
    (
        Ungated::SchemaRewrite,
        "An ALTER rewrite writes the new file set's files, and on failure puts the schema \
         backup back, through the database's seed under the table's own names while the old \
         file set's word is still live. Both run inside the reload, under the statement's \
         ACCESS EXCLUSIVE and the reload's ticket on the old word (so that word is Dirty, its \
         DIRTY durable); the new file set gets a word of its own when it opens.",
    ),
];

/// How a provider's registered writes are judged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// Ticketless writes panic in tests (the backstop).
    Backstop,
    /// Registered, but judged as release judges them, tests included: an
    /// [`UNGATED`] entry.
    Exempt(Ungated),
}

/// The word, held strongly (as the slot, the instances and the tickets
/// hold it), and its file set weakly: a provider clone a background task
/// keeps for a moment after its table is dropped must not keep the file set
/// in the registry, or the next open of it would reuse the old word instead
/// of deriving one from `.ixm` (§2: only the first open derives it).
struct Attached {
    word: Arc<WriteGate>,
    file_set: Weak<FileGate>,
    policy: Policy,
}

impl Attached {
    /// Whether a ticketless verdict panics: in tests, under the backstop,
    /// unless the test asked for release verdicts. Never outside tests.
    #[inline(always)]
    fn panics(&self) -> bool {
        #[cfg(test)]
        {
            matches!(self.policy, Policy::Backstop)
                && !self.file_set.upgrade().is_some_and(|g| g.release_verdicts())
        }
        #[cfg(not(test))]
        {
            false
        }
    }
}

/// A provider's place on its file set's gate: empty until the table that
/// owns the file attaches its word. Shared by the provider's clones.
#[derive(Default)]
pub struct GatedIo {
    slot: OnceLock<Attached>,
}

impl std::fmt::Debug for GatedIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatedIo")
            .field("gate", &self.slot.get().map(|a| a.word.census_id()))
            .field("policy", &self.slot.get().map(|a| a.policy))
            .finish()
    }
}

/// What a registered write holds until it returns.
#[must_use = "a registration covers the write only while it is held"]
#[derive(Debug)]
pub enum Registration<'a> {
    /// The provider has no word.
    Ungated,
    /// One count on the word.
    Io(IoRegistration<'a>),
    /// Release only: the slow path's count, taken after it made `DIRTY`
    /// durable.
    Held(Hold),
}

/// Where a failed slow path is recorded for a write that cannot return it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    /// Under the write's transaction: its commit fails, through any file of
    /// the directory.
    Txn(u64),
    /// Under the directory's gate key: the next commit in it fails.
    Untokened,
}

impl GatedIo {
    pub const fn new() -> GatedIo {
        GatedIo { slot: OnceLock::new() }
    }

    /// Attaches the file set's word, once. A word with no files (memory, a
    /// key/value store, a mirrored pool: P33) is detached and `Dirty` for
    /// good, and is not attached: registering on it could never refuse.
    pub fn attach(&self, gate: &Arc<FileGate>, policy: Policy) {
        if !gate.has_files() {
            return;
        }
        let attached = self.slot.get_or_init(|| Attached {
            word: gate.word().clone(),
            file_set: Arc::downgrade(gate),
            policy,
        });
        debug_assert!(
            Arc::ptr_eq(&attached.word, gate.word()),
            "a provider attached to a second file set's word"
        );
    }

    /// Gives a provider just opened for `child` the word of `parent`'s
    /// provider (`parent_name` its file name) when `child` is named after it:
    /// `<t>_schema.db`'s `<t>_schema_FIELDS.db` and `_schema_*` sidecars.
    /// Anything else a table's provider opens (`rdb.ixe`, a rename intent)
    /// is not the file set's.
    pub fn inherit_from(&self, parent: &GatedIo, parent_name: &str, child: &str) {
        let Some(a) = parent.slot.get() else {
            return;
        };
        let stem = parent_name.strip_suffix(".db").unwrap_or(parent_name);
        if child.len() > stem.len() + 1 && child.starts_with(stem) && child.as_bytes()[stem.len()] == b'_' {
            let _ = self.slot.set(Attached {
                word: a.word.clone(),
                file_set: a.file_set.clone(),
                policy: a.policy,
            });
        }
    }

    /// The attached word.
    pub fn word(&self) -> Option<&Arc<WriteGate>> {
        self.slot.get().map(|a| &a.word)
    }

    /// The attached policy.
    pub fn policy(&self) -> Option<Policy> {
        self.slot.get().map(|a| a.policy)
    }

    /// Registers a write of `file` in `location` through `site` (module
    /// documentation, Verdicts): the fast path, synchronous, so a provider's
    /// write future carries no state machine for it. `Ok` is the
    /// registration to hold until the write returns (or none, for a provider
    /// with no word); `Err` is release's slow path, to await before issuing
    /// (tests panic before ever returning one, unless they asked for release
    /// verdicts). The caller then calls [`GatedIo::note_write`]; file
    /// providers do all three through `file_sync`'s `gated!`.
    #[inline]
    pub fn admit_now(&self, site: Site, location: &str, file: &str) -> Result<Registration<'_>, SlowPath> {
        let Some(a) = self.slot.get() else {
            return Ok(Registration::Ungated);
        };
        match a.word.register_write() {
            Ok(reg) => {
                if reg.ticketless() && a.panics() {
                    ticketless(site, location, file, "an unpublished seal, and no ticket held anywhere");
                }
                Ok(Registration::Io(reg))
            }
            Err(reason) => {
                if a.panics() {
                    ticketless(site, location, file, reason_text(reason));
                }
                Err(slow(&a.file_set))
            }
        }
    }

    /// Records a failed slow path for a write that cannot return it, where
    /// `on_fail` says (module documentation): the write is then skipped.
    #[cold]
    pub fn record_failure(location: &str, file: &str, on_fail: Failure, e: io::Error) {
        log::error!(
            "{}/{}: the write gate's slow path failed ({}); the write was skipped",
            location,
            file,
            e
        );
        use crate::core::storage_providers::file_sync::failed_appends;
        match on_fail {
            Failure::Txn(txn) => failed_appends::record(location, file, txn, e),
            Failure::Untokened => failed_appends::record_gate(location, e),
        }
    }

    /// Records a write of `file` in the census, with the word it registered
    /// on, or, for a provider with no word, the live file set it reaches
    /// around its gate (test builds only; nothing in production). A write
    /// through no word to a live file set whose word is `Clean` or `Unknown`
    /// panics unless it runs inside an [`ungated`] scope.
    #[inline(always)]
    pub fn note_write(&self, location: &str, file: &str) {
        #[cfg(test)]
        {
            use super::census;
            match self.slot.get() {
                Some(a) => census::write(census::Written {
                    location,
                    file,
                    gate: Some(a.word.census_id()),
                    live: None,
                    ungated: match a.policy {
                        Policy::Backstop => None,
                        Policy::Exempt(why) => Some(why),
                    },
                }),
                None => {
                    let live = super::file_gate::live_gate_for(std::path::Path::new(location), file);
                    let scope = scope::current();
                    census::write(census::Written {
                        location,
                        file,
                        gate: None,
                        live: live.as_ref().map(|g| g.census_id()),
                        ungated: scope,
                    });
                    if let Some(g) = live
                        && scope.is_none()
                        && !g.release_verdicts()
                        && matches!(
                            g.state(),
                            super::gate_word::GateState::Clean | super::gate_word::GateState::Unknown
                        )
                    {
                        panic!(
                            "GatedIo: a write to {location}/{file} reached a live file set whose word is {:?} \
                             through a provider with no word attached, outside every listed exemption: a \
                             provider that missed its attach (docs/persistent-indexes.md §2 layer 2, B12)",
                            g.state()
                        );
                    }
                }
            }
        }
        #[cfg(not(test))]
        let _ = (location, file);
    }
}

fn reason_text(reason: SlowReason) -> &'static str {
    match reason {
        SlowReason::Clean => "the word is Clean: no durable DIRTY",
        SlowReason::Unknown => "the word is Unknown: the log proves nothing",
        SlowReason::Publishing => "a seal is publishing its CLEAN, so no ticket can be held",
        SlowReason::SealRunning => "a mandatory seal is running",
    }
}

/// The backstop's verdict in tests: a write with no ticket on its file set.
#[cold]
#[inline(never)]
#[track_caller]
fn ticketless(site: Site, location: &str, file: &str, why: &str) -> ! {
    panic!(
        "GatedIo: a {site:?} write to {location}/{file} was issued with no ticket held on its file set's \
         gate ({why}): a missed ticket site (docs/persistent-indexes.md §2 layer 2, B12)"
    )
}

/// Release's slow path for one write, boxed: awaiting it gives the
/// registration to hold, or the error that means nothing may be issued.
pub type SlowPath = Pin<Box<dyn Future<Output = io::Result<Registration<'static>>> + Send + Sync + 'static>>;

/// Release's verdict on a write that found no ticket possible: the slow
/// path inline (wait out a `PUBLISHING` window or a mandatory seal; in
/// `Clean` or `Unknown`, `DIRTY` durable first), its count held for the
/// write. Boxed, as `gate::ticket`'s slow path is: it runs at most once per
/// clean point, and every provider write's future would otherwise carry it.
/// A file set no longer open (its table dropped, a provider clone still
/// writing) cannot make `DIRTY` durable: the write fails instead.
#[cold]
fn slow(file_set: &Weak<FileGate>) -> SlowPath {
    let file_set = file_set.upgrade();
    Box::pin(async move {
        let Some(gate) = file_set else {
            return Err(io::Error::other(
                "the write gate's file set is no longer open, so DIRTY cannot be made durable before the write",
            ));
        };
        gate.word().acquire(|state| gate.slow_path(state)).await.map(Registration::Held)
    })
}

/// Runs `fut` as an [`UNGATED`] entry `why`: its writes to a live file set
/// through a provider with no word are the listed exemption's, not a missed
/// attach. Only tests look; in production this is `fut`.
pub async fn ungated<F: Future>(why: Ungated, fut: F) -> F::Output {
    #[cfg(test)]
    {
        scope::SCOPE.scope(why, fut).await
    }
    #[cfg(not(test))]
    {
        let _ = why;
        fut.await
    }
}

#[cfg(test)]
mod scope {
    use super::Ungated;

    tokio::task_local! {
        pub(super) static SCOPE: Ungated;
    }

    pub(super) fn current() -> Option<Ungated> {
        SCOPE.try_with(|why| *why).ok()
    }
}

/// Test-only: parks the next registered write of a named file between its
/// registration and its issue, until the test releases it (`gated!` awaits
/// [`park::at`] there in test builds).
#[cfg(test)]
pub(crate) mod park {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use parking_lot::Mutex;

    /// One armed park: `reached` is notified when the write gets there, and
    /// it waits for `release`.
    #[derive(Default)]
    pub struct Parked {
        pub reached: tokio::sync::Notify,
        pub release: tokio::sync::Notify,
    }

    static ARMED: AtomicBool = AtomicBool::new(false);
    static PARKS: Mutex<Vec<(String, Arc<Parked>)>> = Mutex::new(Vec::new());

    /// Parks the next registered write of `file` (a file name).
    pub fn arm(file: &str) -> Arc<Parked> {
        let parked = Arc::new(Parked::default());
        PARKS.lock().push((file.to_string(), parked.clone()));
        ARMED.store(true, Ordering::Release);
        parked
    }

    pub async fn at(file: &str) {
        if !ARMED.load(Ordering::Acquire) {
            return;
        }
        let parked = {
            let mut parks = PARKS.lock();
            let found = parks.iter().position(|(f, _)| f == file).map(|i| parks.swap_remove(i).1);
            if parks.is_empty() {
                ARMED.store(false, Ordering::Release);
            }
            found
        };
        if let Some(p) = parked {
            p.reached.notify_one();
            p.release.notified().await;
        }
    }
}

#[cfg(test)]
#[path = "gated_io_tests.rs"]
mod tests;
