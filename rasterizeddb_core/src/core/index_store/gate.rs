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
//! # Phase 0: a stub
//!
//! [`ticket`] and [`build_token`] check nothing and record nothing: no gate
//! word, no `DIRTY`, no seal. They exist so every mutation already names the
//! proof it will need, and phase 1 changes what minting one costs, not where
//! it happens.
//!
//! Neither token implements `Clone`, `Copy`, `Default` or `From`. In phase 1 a
//! `WriteTicket` holds a count on the gate word, and a copy would drop it
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

use std::marker::PhantomData;

/// Held by a write path while it mutates an index or a filter.
///
/// Built only by [`ticket`] (and [`test_mint`] in tests).
#[derive(Debug)]
pub struct WriteTicket {
    _private: (),
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

/// A ticket for one write path's mutations. Phase 0: checks nothing.
pub(crate) fn ticket() -> WriteTicket {
    WriteTicket { _private: () }
}

/// A token for a rebuild. Phase 0: checks nothing.
pub(crate) fn build_token() -> BuildToken {
    BuildToken { _private: () }
}

/// A ticket for tests, doctests, benches and `rasterizeddb_filter_tests`,
/// which link the library built without `cfg(test)` and enable the dev-only
/// `test_tokens` feature instead.
#[cfg(any(test, feature = "test_tokens"))]
pub fn test_mint() -> WriteTicket {
    WriteTicket { _private: () }
}
