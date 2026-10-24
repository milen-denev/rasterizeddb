//! Indexes and the filter persisted as snapshots (docs/persistent-indexes.md).
//!
//! Phase 0 ships [`gate`]: the proof every in-memory mutator of an index or a
//! filter takes (hook C10). Nothing here is persisted yet.
//!
//! Slice B9 adds the pure codecs of the files phase 1 will write, used by
//! nothing yet: [`marker`] (`.ixm`), [`snapshot`] (`.ixs`), [`identity`] (what
//! both record about the snapshot) and [`epoch_log`] (`rdb.ixe`), over the
//! shared [`codec`].

pub mod codec;
pub mod epoch_log;
pub mod gate;
pub mod identity;
pub mod marker;
pub mod snapshot;

#[cfg(test)]
mod gate_source_tests;
