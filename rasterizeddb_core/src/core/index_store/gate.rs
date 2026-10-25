//! The write gate's proofs (docs/persistent-indexes.md §2, hook C10).
//!
//! Every in-memory mutator of [`LiveIndex`], [`GinIndex`], [`TableIndexes`]
//! and [`TableFilters`] takes a [`Proof`]: a borrowed [`WriteTicket`] (a write
//! path, which phase 1 counts against the partition's gate word) or a borrowed
//! [`BuildToken`] (a rebuild of a structure that is not ready, which no
//! snapshot can capture). Visibility cannot fence the mutators in, since every
//! ancestor path that compiles admits `vacuum.rs`, `database/*` and the
//! executor; the proof in the signature does, together with the source test
//! in `gate_source_tests.rs`, which pins that the tokens are built only by the
//! mints below.
//!
//! # Phase 1: the ticket is real (slice B11)
//!
//! A [`WriteTicket`] holds a count on its file set's gate word
//! ([`super::gate_word::Hold`]), taken by [`ticket`] through the word's
//! slow path when it must: in `Clean` or `Unknown` the slow path makes
//! `DIRTY` durable (or the log durably `HEADER` + `DIRTY`, or `.ixs` gone)
//! before the ticket exists, so before anything it proves is issued (T1).
//! Dropping the ticket gives the count back. A [`Proof`] made from one
//! proves that a ticket is held while the proof lives.
//!
//! **One ticket per (family, file set)** (§2 Ticket ownership, P2): a
//! family's first ticket on a file set is also kept in its lock owner's
//! [`FamilyTickets`] record, and given back only when the owner is released,
//! after its outcome is final and its post-commit work is done (P2, N26).
//! Later statements and savepoints of the family take theirs beside the kept
//! one, which never waits. Maintenance (vacuum, `neutralize_aborted`, a
//! landing of staged bytes) takes transient tickets of its own.
//!
//! [`build_token`] still checks nothing: a rebuild of a structure that is not
//! ready appends no `DIRTY` and is not counted by a seal (§2 `BuildToken`).
//!
//! Neither token implements `Clone`, `Copy`, `Default` or `From`: a
//! `WriteTicket` holds a count on the gate word, and a copy would give it back
//! twice.
//!
//! # The signature checks
//!
//! rustdoc builds each example as a crate of its own against the library
//! built without `cfg(test)`, so these check signatures, not behaviour.
//! Stable rustdoc does not check a `compile_fail` example's error code, so
//! each one has a twin that compiles and differs only in the marked line: the
//! twin is what says the `compile_fail` failed for the intended reason, and
//! not because a path stopped resolving.
//!
//! A mutator called without its proof does not compile:
//!
//! ```compile_fail
//! use rasterizeddb_core::core::index::LiveIndex;
//! use rasterizeddb_core::core::index_store::gate::{Proof, WriteTicket};
//! fn _f(ix: &LiveIndex, t: &WriteTicket) {
//!     let _ = &t;
//!     ix.insert(7, None); // the line
//! }
//! ```
//!
//! Its twin, with the proof:
//!
//! ```
//! use rasterizeddb_core::core::index::LiveIndex;
//! use rasterizeddb_core::core::index_store::gate::{Proof, WriteTicket};
//! fn _f(ix: &LiveIndex, t: &WriteTicket) {
//!     let _ = &t;
//!     ix.insert(Proof::from(t), 7, None); // the line
//! }
//! ```
//!
//! A token cannot be built outside this crate, by a literal:
//!
//! ```compile_fail
//! use rasterizeddb_core::core::filter::TableFilters;
//! use rasterizeddb_core::core::index_store::gate::{BuildToken, Proof};
//! fn _f(tf: &TableFilters, b: &BuildToken) {
//!     let b = &BuildToken { _private: () }; // the line
//!     tf.set_unindexed_prefix(Proof::from(b), 7);
//! }
//! ```
//!
//! or by the production mint:
//!
//! ```compile_fail
//! use rasterizeddb_core::core::filter::TableFilters;
//! use rasterizeddb_core::core::index_store::gate::{BuildToken, Proof};
//! fn _f(tf: &TableFilters, b: &BuildToken) {
//!     let b = &rasterizeddb_core::core::index_store::gate::build_token(); // the line
//!     tf.set_unindexed_prefix(Proof::from(b), 7);
//! }
//! ```
//!
//! Their twin, which takes the token it was handed:
//!
//! ```
//! use rasterizeddb_core::core::filter::TableFilters;
//! use rasterizeddb_core::core::index_store::gate::{BuildToken, Proof};
//! fn _f(tf: &TableFilters, b: &BuildToken) {
//!     let b = b; // the line
//!     tf.set_unindexed_prefix(Proof::from(b), 7);
//! }
//! ```
//!
//! A proof cannot be built from its kind, without a token:
//!
//! ```compile_fail
//! use rasterizeddb_core::core::filter::TableFilters;
//! use rasterizeddb_core::core::index_store::gate::{BuildToken, Proof, ProofKind};
//! use std::marker::PhantomData;
//! fn _f(tf: &TableFilters, b: &BuildToken) {
//!     let _ = (&b, ProofKind::Build, PhantomData::<()>);
//!     let p = Proof { kind: ProofKind::Build, _borrow: PhantomData }; // the line
//!     tf.set_unindexed_prefix(p, 7);
//! }
//! ```
//!
//! Its twin, from the token:
//!
//! ```
//! use rasterizeddb_core::core::filter::TableFilters;
//! use rasterizeddb_core::core::index_store::gate::{BuildToken, Proof, ProofKind};
//! use std::marker::PhantomData;
//! fn _f(tf: &TableFilters, b: &BuildToken) {
//!     let _ = (&b, ProofKind::Build, PhantomData::<()>);
//!     let p = Proof::from(b); // the line
//!     tf.set_unindexed_prefix(p, 7);
//! }
//! ```
//!
//! [`LiveIndex`]: crate::core::index::LiveIndex
//! [`GinIndex`]: crate::core::index::gin::GinIndex
//! [`TableIndexes`]: crate::core::index::TableIndexes
//! [`TableFilters`]: crate::core::filter::TableFilters

use std::io;
use std::marker::PhantomData;
use std::sync::Arc;

use parking_lot::Mutex;
use smallvec::SmallVec;

use super::file_gate::FileGate;
use super::gate_word::{Hold, WriteGate};

/// Held by a write path while it mutates an index or a filter, and while it
/// issues the writes those mutations describe: a count on its file set's
/// gate word.
///
/// Built only by [`ticket`] (and [`test_mint`] in tests).
#[derive(Debug)]
pub struct WriteTicket {
    hold: Hold,
}

/// Held by a rebuild of an index or a filter that is not ready.
///
/// Built only by [`build_token`].
#[derive(Debug)]
pub struct BuildToken {
    _private: (),
}

/// What a mutator is handed: the caller holds one of the two tokens, and
/// which. One byte, passed by value.
///
/// Built only by the two `From` impls below, from a borrowed token, which the
/// lifetime keeps borrowed while the proof lives. With no address in it, a
/// forged kind would be a forged proof, so the fields are private and neither
/// this nor [`ProofKind`] implements `Default` or any conversion from a kind;
/// `gate_source_tests.rs` pins that.
///
/// It proves that the caller holds *a* token, not that it is this structure's
/// gate's; that is the census's job (§13 V3).
#[derive(Debug, Clone, Copy)]
pub struct Proof<'a> {
    kind: ProofKind,
    _borrow: PhantomData<&'a ()>,
}

/// Which token a [`Proof`] was made from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProofKind {
    Ticket,
    Build,
}

impl Proof<'_> {
    /// Which token this proof was made from.
    pub fn kind(self) -> ProofKind {
        self.kind
    }
}

impl<'a> From<&'a WriteTicket> for Proof<'a> {
    fn from(_: &'a WriteTicket) -> Self {
        Proof {
            kind: ProofKind::Ticket,
            _borrow: PhantomData,
        }
    }
}

impl<'a> From<&'a BuildToken> for Proof<'a> {
    fn from(_: &'a BuildToken) -> Self {
        Proof {
            kind: ProofKind::Build,
            _borrow: PhantomData,
        }
    }
}

// The size the mutators are passed by value at.
const PROOF_SIZE: usize = std::mem::size_of::<Proof<'static>>();
const _: () = assert!(PROOF_SIZE == 1);

impl WriteTicket {
    /// Whether this ticket counts on `gate`'s word.
    pub fn is_on(&self, gate: &FileGate) -> bool {
        Arc::ptr_eq(self.hold.gate(), gate.word())
    }
}

/// The lock owner a ticket is kept for (§2 Ticket ownership): the database's
/// transactions, whose [`FamilyTickets`] record keeps it, and the owner's id
/// (a block's root, a statement's O, a named owner).
pub(crate) struct Family {
    pub(crate) manager: Arc<crate::core::mvcc::TransactionManager>,
    pub(crate) owner: u64,
}

/// A ticket on `gate` for a write path (§2 layer 1).
///
/// With `family`, the lock owner's record: when the owner already keeps a
/// ticket on this file set, the new one is a second count beside it, which
/// never waits; otherwise the word's fast path, or its slow path (a
/// mandatory seal or `PUBLISHING` window waited out; in `Clean` or `Unknown`,
/// `DIRTY` made durable first), and a second count is kept in the record
/// until the owner is released. Without, a transient ticket (maintenance, or
/// a write outside every owner scope). An error from the slow path fails the
/// ticket with nothing issued.
pub(crate) async fn ticket(gate: &FileGate, family: Option<Family>) -> io::Result<WriteTicket> {
    if let Some(f) = &family
        && let Some(hold) = f.manager.family_tickets().beside(f.owner, gate.word())
    {
        return Ok(WriteTicket { hold });
    }
    let hold = match gate.word().try_acquire() {
        Ok(hold) => hold,
        // The slow path behind a type-erased box: it runs once per writing
        // session at most (or waits out a seal), and every write path awaits
        // this, so its state machine stays out of every statement future's
        // type (and out of the `Send` proof the deepest of those already
        // strains).
        Err(_) => {
            let slow: std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Hold>> + Send + '_>> =
                Box::pin(gate.word().acquire(|state| gate.slow_path(state)));
            slow.await?
        }
    };
    if let Some(f) = &family {
        f.manager
            .family_tickets()
            .keep(f.owner, WriteTicket { hold: hold.beside() });
    }
    Ok(WriteTicket { hold })
}

/// Fast-path shards of [`FamilyTickets`], by owner.
const FAMILY_SHARDS: usize = 64;

/// The tickets each lock owner keeps: one per file set it wrote, held until
/// the owner is released (`RowLocks::release_owner`, after the family's last
/// durability step). Per database, in its `TransactionManager`.
pub struct FamilyTickets {
    shards: Box<[Mutex<FamilyShard>]>,
    /// Owners with a kept ticket, so releasing an owner that kept none (a
    /// read-only statement) costs one load.
    families: std::sync::atomic::AtomicUsize,
}

#[derive(Default)]
struct FamilyShard {
    /// Owner id and its kept tickets, one per file set. Few owners per shard
    /// at a time, so a vector.
    held: Vec<(u64, SmallVec<[WriteTicket; 2]>)>,
}

impl Default for FamilyTickets {
    fn default() -> Self {
        FamilyTickets::new()
    }
}

impl std::fmt::Debug for FamilyTickets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FamilyTickets")
            .field("families", &self.families.load(std::sync::atomic::Ordering::Relaxed))
            .finish()
    }
}

impl FamilyTickets {
    pub fn new() -> FamilyTickets {
        FamilyTickets {
            shards: (0..FAMILY_SHARDS).map(|_| Mutex::new(FamilyShard::default())).collect(),
            families: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn shard(&self, owner: u64) -> &Mutex<FamilyShard> {
        // Fibonacci hashing: consecutive ids spread over every shard.
        &self.shards[(owner.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - FAMILY_SHARDS.trailing_zeros())) as usize]
    }

    /// A count beside the ticket `owner` keeps on `word`, if it keeps one.
    fn beside(&self, owner: u64, word: &Arc<WriteGate>) -> Option<Hold> {
        if self.families.load(std::sync::atomic::Ordering::Acquire) == 0 {
            return None;
        }
        let shard = self.shard(owner).lock();
        let (_, kept) = shard.held.iter().find(|(o, _)| *o == owner)?;
        kept.iter()
            .find(|t| Arc::ptr_eq(t.hold.gate(), word))
            .map(|t| t.hold.beside())
    }

    /// Keeps `ticket` for `owner`, unless it already keeps one on that file
    /// set (a racing statement of the family), when `ticket` is given back.
    fn keep(&self, owner: u64, ticket: WriteTicket) {
        let extra = {
            let mut shard = self.shard(owner).lock();
            match shard.held.iter_mut().find(|(o, _)| *o == owner) {
                Some((_, kept)) => {
                    if kept.iter().any(|t| Arc::ptr_eq(t.hold.gate(), ticket.hold.gate())) {
                        Some(ticket)
                    } else {
                        kept.push(ticket);
                        None
                    }
                }
                None => {
                    let mut kept = SmallVec::new();
                    kept.push(ticket);
                    shard.held.push((owner, kept));
                    self.families.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    None
                }
            }
        };
        // Given back outside the shard lock: a drop can wake a sealer.
        drop(extra);
    }

    /// Gives back every ticket `owner` keeps: called when the owner is
    /// released, after its outcome is final and its post-commit work done.
    pub fn release(&self, owner: u64) {
        if self.families.load(std::sync::atomic::Ordering::Acquire) == 0 {
            return;
        }
        let kept = {
            let mut shard = self.shard(owner).lock();
            match shard.held.iter().position(|(o, _)| *o == owner) {
                Some(i) => {
                    self.families.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                    Some(shard.held.swap_remove(i))
                }
                None => None,
            }
        };
        drop(kept);
    }

    /// How many file sets `owner` keeps a ticket on. Diagnostic and for
    /// tests.
    pub fn kept_by(&self, owner: u64) -> usize {
        let shard = self.shard(owner).lock();
        shard
            .held
            .iter()
            .find(|(o, _)| *o == owner)
            .map_or(0, |(_, kept)| kept.len())
    }

    /// Whether `owner` keeps a ticket on `gate`.
    pub fn keeps_on(&self, owner: u64, gate: &FileGate) -> bool {
        let shard = self.shard(owner).lock();
        shard
            .held
            .iter()
            .find(|(o, _)| *o == owner)
            .is_some_and(|(_, kept)| kept.iter().any(|t| t.is_on(gate)))
    }
}

/// A token for a rebuild. Checks nothing: see the module documentation.
pub(crate) fn build_token() -> BuildToken {
    BuildToken { _private: () }
}

/// A ticket for tests, doctests, benches and `rasterizeddb_filter_tests`,
/// which link the library built without `cfg(test)` and enable the dev-only
/// `test_tokens` feature instead. It counts on a word of its own, detached
/// from every file set and `Dirty`, so it holds what a real ticket holds and
/// gates nothing.
#[cfg(any(test, feature = "test_tokens"))]
pub fn test_mint() -> WriteTicket {
    let word = Arc::new(WriteGate::new(super::gate_word::GateState::Dirty));
    let hold = word.try_acquire().expect("a Dirty word with nothing on it holds at once");
    WriteTicket { hold }
}
