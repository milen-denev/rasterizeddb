//! Multi-version concurrency control.
//!
//! Two things have to be true at once for a database to be safe under
//! concurrency: a reader must never see a half-finished write, and a writer must
//! never have to wait for a reader. Locking gives the first by giving up the
//! second. MVCC gives both, by never overwriting a row a reader might still be
//! looking at — an update writes a *new* version, and each statement reads
//! through a snapshot that decides which versions existed when it began.
//!
//! ## The pieces
//!
//! - [`transaction`] — transaction identifiers, their outcomes, and the
//!   snapshot that turns an outcome into a yes/no per row version.
//! - [`status`] — the durable record of which transactions committed, so that
//!   the answer survives a restart. PostgreSQL's `pg_xact` under another name.
//!
//! ## Where the version metadata lives
//!
//! In the pointer record itself: `RowPointer` carries `xmin` and `xmax`
//! alongside `position` and `deleted`.
//!
//! The alternative considered was a positional shadow of the pointer file, on
//! the theory that two more `u64`s would make a 21-byte record 76% larger and
//! slow every scan down to serve the concurrent case.
//!
//! Measured, once the record was actually widened — 50k-row sweep:
//!
//! | record | sweep |
//! |---|---|
//! | 21 bytes, pre-MVCC | 227 µs |
//! | 37 bytes, this layout | 281 µs (+23.8%) |
//! | 92 bytes, `enable_long_row` | 329 µs |
//!
//! **The bandwidth cost is real**, and larger than the ~10% first predicted.
//! That prediction came from fitting a line through the 21- and 92-byte points,
//! which are not on the same curve: the 92-byte figure is `enable_long_row`, a
//! *different decoder* reading eleven fields where this one reads five. The
//! fixed term it derived absorbed decoding work that exists in only one of the
//! two builds, so it understated the marginal cost of bytes. Part of the +23.8%
//! is also the sweep gaining per-record work in the meantime, so it is an upper
//! bound on the width's own share rather than a clean attribution.
//!
//! So the bandwidth argument does not carry the decision, and never did. What
//! decides it is compaction. Compaction copies whole pointer records, so version
//! stamps carried inside one survive it for free. A shadow
//! would have to be renumbered atomically with the records — and unlike the
//! filter index, which is rebuilt from storage because storage is its authority,
//! **version data is its own authority**. There is nothing to rebuild it from,
//! so a renumbering bug loses data rather than costing a rebuild.
//!
//! The fast path is kept regardless, and it is the one that matters: a snapshot
//! taken with nothing else in flight sees every live row and no dead one, which
//! is exactly what a non-versioned scan computes. See
//! [`transaction::Snapshot::sees_everything`].

pub mod row_locks;
pub mod status;
pub mod table_locks;
pub mod transaction;
pub mod visibility;

pub use transaction::{Snapshot, TransactionManager, TxId, TxOutcome};
pub use visibility::Visibility;

#[cfg(test)]
mod invariants {
    use super::transaction::{FIRST_TX, FROZEN_TX, NO_TX};

    /// The reserved ids are load-bearing and their values are not arbitrary:
    /// `NO_TX` must be zero so a freshly zeroed record reads as "nothing deleted
    /// this row", and `FROZEN_TX` must be below every real id so a pre-existing
    /// row is older than anything that could hide it.
    ///
    /// Pinned because a wrong value here corrupts silently rather than failing:
    /// rows would read as debris, or a deleted row would come back.
    #[test]
    fn the_reserved_ids_keep_their_values() {
        assert_eq!(NO_TX, 0, "a zeroed xmax must mean 'not deleted'");
        assert_eq!(FROZEN_TX, 1);
        assert_eq!(
            FIRST_TX, 2,
            "real ids must start above both reserved values"
        );
        assert!(
            FROZEN_TX < FIRST_TX,
            "frozen rows must predate every transaction"
        );
    }
}
