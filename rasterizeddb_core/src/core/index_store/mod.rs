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

pub mod census;
pub mod codec;
pub mod epoch_log;
pub mod file_gate;
pub mod gate;
pub mod gated_io;
pub mod gate_word;
pub mod identity;
pub mod marker;
pub mod snapshot;

#[cfg(test)]
mod gate_source_tests;
#[cfg(test)]
mod wiring_tests;
