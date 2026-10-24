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
//! over them, and an exhaustive interleaving model; also used by nothing yet.

pub mod codec;
pub mod epoch_log;
pub mod gate;
pub mod gate_word;
pub mod identity;
pub mod marker;
pub mod snapshot;

#[cfg(test)]
mod gate_source_tests;
