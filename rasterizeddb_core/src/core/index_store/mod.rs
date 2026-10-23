//! Indexes and the filter persisted as snapshots (docs/persistent-indexes.md).
//!
//! Phase 0 ships only [`gate`]: the proof every in-memory mutator of an index
//! or a filter takes (hook C10). Nothing here is persisted yet.

pub mod gate;

#[cfg(test)]
mod gate_source_tests;
