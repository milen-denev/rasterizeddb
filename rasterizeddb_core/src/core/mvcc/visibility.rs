//! Deciding whether a scan may return a row.
//!
//! This is the read-side counterpart to the version stamps. It exists as its own
//! value rather than as a method on the transaction manager because a scan
//! evaluates it once per row: it has to be cheap, and it has to be answerable
//! without taking a lock.

use std::sync::Arc;

use super::transaction::{FROZEN_TX, NO_TX, Snapshot, TransactionManager, TxId, TxOutcome};

/// The visibility rule a scan applies to each row it passes.
#[derive(Clone)]
pub enum Visibility {
    /// Every row that is not tombstoned is visible.
    ///
    /// Not a snapshot rule, and deliberately not reachable from one. It is for
    /// callers that must see the table as it physically is — vacuum's own scan,
    /// and a database running with no transaction manager at all.
    ///
    /// It was tempting to collapse a snapshot into this whenever nothing was in
    /// flight, on the argument that no one could then disagree about any row.
    /// The argument is wrong, and
    /// `the_rule_does_not_collapse_to_the_tombstone_bit` is the counterexample:
    /// `deleted` is set by **vacuum**, not by the delete, so between a committed
    /// delete and the next reclamation pass a removed row still has
    /// `deleted == false`. Only its `xmax` says it is gone.
    All,
    /// Visible as of a snapshot, resolving outcomes through the manager.
    AsOf {
        snapshot: Snapshot,
        manager: Arc<TransactionManager>,
    },
}

impl Visibility {
    /// The rule for a statement running under `snapshot`.
    pub fn for_snapshot(snapshot: Snapshot, manager: Arc<TransactionManager>) -> Self {
        Visibility::AsOf { snapshot, manager }
    }

    /// Whether a row with these stamps may be returned.
    #[inline]
    pub fn visible(&self, deleted: bool, xmin: TxId, xmax: TxId) -> bool {
        // `deleted` is a fact about the *slot*, not the row: it says this
        // pointer record holds no live entry. Visibility is a fact about the
        // *row*. They are not interchangeable, and the place they come apart is
        // compaction, which moves a record to a lower slot and tombstones the
        // source as a **full copy** — same id, same `xmin`, same `xmax`. Both
        // copies are visible to exactly the same snapshots, so a check on
        // visibility alone would return the row once per compaction that ever
        // moved it, with nothing downstream able to tell the copies apart.
        //
        // So this stays, and it stays first. See §17 of
        // `docs/space-reclamation.md`.
        //
        // **It has a second job, and the second one is easier to lose.** The
        // query cache holds pointers matching a predicate rather than pointers a
        // snapshot could see, which is safe only because a reclaimable version is
        // invisible to every snapshot.
        //
        // `VersionHorizon::is_reclaimable` has four return paths, and this line
        // guards two of them:
        //
        // | branch | condition | what protects it |
        // |---|---|---|
        // | unversioned table | `deleted`, no bound consulted | **this line** |
        // | aborted insert | `outcome(xmin) == Aborted` | the abort itself |
        // | stamped | `outcome(xmax) == Committed && xmax < oldest_active` | `reclamation_lemma` |
        // | pre-versioning row | `xmax == NO_TX && deleted` | **this line** |
        //
        // So it is not a legacy special case: it covers **every path where no
        // `xmax` is involved** except the aborted-insert one, which needs no
        // bound: a version whose creator aborted was visible to no snapshot
        // that ever existed, so there is nothing to outlive.
        // Do not remove it on the grounds that compaction is handled elsewhere —
        // the cache would start serving reclaimed rows with no edit anywhere
        // near the cache. `a_pre_versioning_tombstone_is_invisible` and
        // `an_unversioned_tombstone_is_invisible` both fail if it goes.
        //
        // For the same reason there must be no `visible_stamped(xmin, xmax)`
        // convenience overload. Taking `deleted` as a parameter is what makes
        // the safe call the natural one; an overload without it would make
        // dropping the flag the easy path.
        if deleted {
            return false;
        }

        match self {
            Visibility::All => true,
            Visibility::AsOf { snapshot, manager } => {
                // The fast path is per row, not per scan: a row that no
                // transaction has ever touched is visible to everyone, and
                // saying so costs two integer compares and no lock. Rows that
                // *have* been touched pay for the full check, which is the
                // right place for the cost to fall.
                if xmin == FROZEN_TX && xmax == NO_TX {
                    return true;
                }
                snapshot.sees(xmin, xmax, &|id| manager.outcome(id))
            }
        }
    }

    /// True when this rule admits every untombstoned row without consulting the
    /// stamps. See [`Visibility::All`] for why a snapshot never reaches this.
    #[inline]
    pub fn is_unrestricted(&self) -> bool {
        matches!(self, Visibility::All)
    }
}

impl Default for Visibility {
    fn default() -> Self {
        Visibility::All
    }
}

impl std::fmt::Debug for Visibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Visibility::All => f.write_str("Visibility::All"),
            Visibility::AsOf { snapshot, .. } => {
                write!(f, "Visibility::AsOf({:?})", snapshot.xmax)
            }
        }
    }
}

/// Whether `outcome` means the writing transaction's row may ever be seen again.
pub fn is_dead(outcome: TxOutcome) -> bool {
    outcome == TxOutcome::Aborted
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compaction tombstones the source slot as a full copy of the record it
    /// moved, stamps included. A rule that consulted only the stamps would
    /// return the row twice.
    #[test]
    fn a_compaction_leftover_is_hidden_by_the_slot_flag_alone() {
        let m = Arc::new(TransactionManager::in_memory());
        let writer = m.begin();
        m.commit(writer);
        let v = Visibility::for_snapshot(m.snapshot(NO_TX), m.clone());

        // The moved record: live, visible.
        assert!(v.visible(false, writer, NO_TX));
        // The source it was moved from: byte-identical but for the flag.
        assert!(!v.visible(true, writer, NO_TX));
    }

    #[test]
    fn a_tombstoned_row_is_invisible_under_every_rule() {
        let m = Arc::new(TransactionManager::in_memory());

        assert!(!Visibility::All.visible(true, FROZEN_TX, NO_TX));
        let v = Visibility::for_snapshot(m.snapshot(NO_TX), m.clone());
        assert!(!v.visible(true, FROZEN_TX, NO_TX));
    }

    /// The per-row shortcut has to be free for untouched rows and must not fire
    /// for anything else.
    #[test]
    fn an_untouched_row_is_visible_without_consulting_anything() {
        let m = Arc::new(TransactionManager::in_memory());
        let running = m.begin();
        let v = Visibility::for_snapshot(m.snapshot(NO_TX), m.clone());

        assert!(v.visible(false, FROZEN_TX, NO_TX));
        assert!(
            !v.visible(false, running, NO_TX),
            "not untouched: someone is writing it"
        );
    }

    /// A snapshot must never reduce to "return everything not tombstoned",
    /// however quiet the database is.
    ///
    /// This is the counterexample that killed the per-scan fast path. Nothing is
    /// in flight, so it is tempting to say every stamp is settled and `deleted`
    /// can be trusted — but `deleted` is set by vacuum, and vacuum has not run.
    /// The row is gone and the tombstone bit does not know it yet.
    #[test]
    fn the_rule_does_not_collapse_to_the_tombstone_bit() {
        let m = Arc::new(TransactionManager::in_memory());
        let deleter = m.begin();
        m.commit(deleter);
        assert!(m.quiescent(), "nothing is in flight");

        let v = Visibility::for_snapshot(m.snapshot(NO_TX), m.clone());
        assert!(!v.is_unrestricted());
        assert!(
            !v.visible(false, FROZEN_TX, deleter),
            "a committed delete hides the row before vacuum sets the bit"
        );
    }

    #[test]
    fn a_row_written_by_a_transaction_in_flight_is_hidden() {
        let m = Arc::new(TransactionManager::in_memory());
        let writer = m.begin();
        let v = Visibility::for_snapshot(m.snapshot(NO_TX), m.clone());

        assert!(!v.visible(false, writer, NO_TX));
        assert!(
            v.visible(false, FROZEN_TX, NO_TX),
            "existing rows are unaffected"
        );
    }

    #[test]
    fn a_row_deleted_by_a_transaction_in_flight_is_still_visible() {
        let m = Arc::new(TransactionManager::in_memory());
        let deleter = m.begin();
        let v = Visibility::for_snapshot(m.snapshot(NO_TX), m.clone());

        assert!(v.visible(false, FROZEN_TX, deleter));
    }

    /// The frozen shortcut must not be reachable for a row a transaction has
    /// deleted, or a delete would never take effect.
    #[test]
    fn the_frozen_shortcut_does_not_swallow_a_pending_delete() {
        let m = Arc::new(TransactionManager::in_memory());
        let deleter = m.begin();
        m.commit(deleter);
        let other = m.begin();
        let v = Visibility::for_snapshot(m.snapshot(other), m.clone());

        assert!(
            !v.visible(false, FROZEN_TX, deleter),
            "committed before the snapshot"
        );
    }

    #[test]
    fn a_transaction_sees_its_own_row_through_the_rule() {
        let m = Arc::new(TransactionManager::in_memory());
        let me = m.begin();
        let _other = m.begin();
        let v = Visibility::for_snapshot(m.snapshot(me), m.clone());

        assert!(v.visible(false, me, NO_TX));
    }
}

#[cfg(test)]
mod reclamation_lemma {
    use super::*;

    /// The lemma the query cache depends on.
    ///
    /// A cache entry holds the pointers matching a predicate, not the ones a
    /// particular snapshot could see — that is what lets one entry serve every
    /// snapshot. The cost is that an entry can name a version reclamation has
    /// since freed, and reclamation has no idea the entry references it.
    ///
    /// It is safe because reclaimable and visible are mutually exclusive: a
    /// version is only reclaimable once its `xmax` is committed and below
    /// `oldest_active`, and that is precisely the condition under which no
    /// snapshot that can still be taken will see it. The serve path tests
    /// visibility before fetching, so the freed extent is never read.
    ///
    /// If this ever stops holding, the cache stops being safe — hence a test
    /// rather than a comment.
    #[test]
    fn a_reclaimable_version_is_invisible_to_every_snapshot() {
        let m = Arc::new(TransactionManager::in_memory());
        let writer = m.begin();
        m.commit(writer);
        let deleter = m.begin();
        m.commit(deleter);

        // Reclaimable: xmax committed, and below the oldest transaction that
        // could still be running.
        assert_eq!(m.outcome(deleter), TxOutcome::Committed);
        assert!(
            deleter < m.oldest_active(),
            "the bound has not advanced past it"
        );

        // Every snapshot from here on — including one belonging to a
        // transaction that starts later — must decline it.
        for own in [NO_TX, m.begin()] {
            let v = Visibility::for_snapshot(m.snapshot(own), m.clone());
            assert!(
                !v.visible(false, writer, deleter),
                "a reclaimable version was visible to a snapshot ({}), so a cache \
                 entry naming it could have been read after its extent was freed",
                own
            );
        }
    }

    /// The lemma's third branch, added when reclamation learned about aborts.
    ///
    /// `VersionHorizon::is_reclaimable` also frees a version whose *creator*
    /// aborted — a rolled-back `INSERT`, which has no `xmax` and is not
    /// `deleted`, so neither of the other two branches covers it. The scan path
    /// in `concurrent_processor` skips on `is_reclaimable` before fetching, so
    /// if such a version were visible to any snapshot the scan would drop a row
    /// it should have returned.
    ///
    /// Unlike the `xmax` branch this needs no bound: a version whose `xmin`
    /// aborted was visible to no snapshot that ever existed, so there is nothing
    /// to outlive.
    #[test]
    fn a_version_whose_insert_aborted_is_invisible_to_every_snapshot() {
        let m = Arc::new(TransactionManager::in_memory());
        let inserter = m.begin();
        m.abort(inserter);

        // A snapshot taken before the abort resolved, one taken after, and one
        // belonging to no transaction at all.
        let during = m.begin();
        for own in [NO_TX, during, m.begin()] {
            let v = Visibility::for_snapshot(m.snapshot(own), m.clone());
            assert!(
                !v.visible(false, inserter, NO_TX),
                "a rolled-back INSERT was visible to a snapshot ({}), so the scan                  would skip a row it should return — or serve one it should not",
                own
            );
        }
    }

    /// The lemma's second branch, which a different mechanism upholds.
    ///
    /// `VersionHorizon::is_reclaimable` also frees a version with no `xmax` that
    /// is merely flagged `deleted` — a row predating versioning. The transaction
    /// bound says nothing about that case; the only thing keeping it out of a
    /// cache hit is `Visibility::visible` short-circuiting on `deleted` before it
    /// consults the snapshot at all.
    ///
    /// That short-circuit is commented as being about compaction leftovers,
    /// which it also is. This test exists so that removing it — on the entirely
    /// reasonable grounds that compaction is handled elsewhere — fails here
    /// rather than silently making the query cache serve reclaimed rows.
    #[test]
    fn a_pre_versioning_tombstone_is_invisible() {
        let m = Arc::new(TransactionManager::in_memory());
        let running = m.begin();
        let v = Visibility::for_snapshot(m.snapshot(NO_TX), m.clone());

        // Reclaimable by the legacy branch: flagged, and never stamped.
        assert!(!v.visible(true, FROZEN_TX, NO_TX));
        // ...and to a transaction's own snapshot, which is the case a family
        // check could otherwise wave through.
        let mine = Visibility::for_snapshot(m.snapshot(running), m.clone());
        assert!(!mine.visible(true, running, NO_TX));
    }

    /// The third branch: a table with no version manager at all.
    ///
    /// `is_reclaimable` reduces to `deleted` there, consulting no bound. That
    /// path became reachable in a cache context when the cache stopped opting
    /// out of versioned tables — before that, unversioned tables were the *only*
    /// ones that cached, and this is exactly their reclamation rule.
    #[test]
    fn an_unversioned_tombstone_is_invisible() {
        assert!(!Visibility::All.visible(true, FROZEN_TX, NO_TX));
        // A stamped row on an unversioned table is not a shape that occurs, but
        // the rule must not depend on that.
        assert!(!Visibility::All.visible(true, 42, 43));
        assert!(
            Visibility::All.visible(false, FROZEN_TX, NO_TX),
            "and a live row still shows"
        );
    }

    /// The converse: a version still visible to *someone* must not satisfy the
    /// reclamation predicate, or the lemma is vacuous.
    #[test]
    fn a_version_an_open_snapshot_can_see_is_not_reclaimable() {
        let m = Arc::new(TransactionManager::in_memory());
        let writer = m.begin();
        m.commit(writer);

        // A reader takes its snapshot before the delete commits.
        let reader = m.begin();
        let reader_view = Visibility::for_snapshot(m.snapshot(reader), m.clone());

        let deleter = m.begin();
        m.commit(deleter);

        assert!(
            reader_view.visible(false, writer, deleter),
            "the reader still sees it"
        );
        assert!(
            deleter > m.oldest_active(),
            "the bound must not have passed a delete an open reader predates"
        );
    }
}
