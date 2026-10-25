//! Indexes and the filter persisted as snapshots (docs/persistent-indexes.md).
//!
//! Phase 0 ships [`gate`]: the proof every in-memory mutator of an index or a
//! filter takes (hook C10). Nothing here is persisted yet.
//!
//! Slice B9 adds the pure codecs of the files phase 1 will write, used by
//! nothing yet: [`marker`] (`.ixm`), [`snapshot`] (`.ixs`), [`identity`] (what
//! both record about the snapshot) and [`epoch_log`] (`rdb.ixe`), over the
//! shared [`codec`].
//!
//! Slice B10 adds [`gate_word`]: the packed gate word, every transition on
//! it as a one-atomic-operation-per-step machine, [`gate_word::WriteGate`]
//! over them, and an exhaustive interleaving model.
//!
//! Slice B11 wires the gate in: [`file_gate`] (each file set's word, the
//! marker log's state at open, the slow path's `DIRTY`, the `Unknown`
//! rewrite, and the slot epoch in `rdb.ixe`), the real [`gate::ticket`] mint
//! with tickets kept on lock owners ([`gate::FamilyTickets`]), and the
//! [`census`] journal tests check T1 and T2 with.
//!
//! Slice B12 adds [`gated_io`]: the provider backstop, every write of a
//! table's providers registered on its word (§2 layer 2), and the allowlist
//! of writes that are not.
//!
//! Slice B13 adds [`seal`]: seal and publish of FILTER-only snapshots at a
//! clean point (shutdown, `rdb_index_checkpoint`, the idle checkpoint), and
//! the gate word carried across RENAME and MOVE ([`file_gate::FileGate::carry_to`]).
//!
//! Slice B14 puts the filter under T4: [`open_proof`] (the open's checks,
//! writing nothing), the FILTER section loaded instead of rebuilt when they
//! pass, and `_flt.db` retired ([`file_gate::FileGate::retire_flt`]).
//!
//! Slice B17 lets an open skip its index build: seals write an INDEX
//! section per B-tree, hash and path index ([`index_section`]) under
//! `index_snapshot.index_storage = snapshot`, and an open whose proof
//! passes loads them ([`load`]), writes and readers waiting for it.

pub mod census;
pub mod codec;
pub mod epoch_log;
pub mod file_gate;
pub mod gate;
pub mod gated_io;
pub mod gate_word;
pub mod identity;
pub mod index_section;
pub mod load;
pub mod marker;
pub mod open_proof;
pub mod seal;
pub mod snapshot;

#[cfg(test)]
mod gate_source_tests;
#[cfg(test)]
mod wiring_tests;
