//! Row locks taken by `SELECT … FOR UPDATE`.
//!
//! A lock is held by a transaction and released when that transaction ends, so
//! the lock table is keyed by `(table, row key)` and holds the owning [`TxId`].
//!
//! # Why a key rather than a position
//!
//! A row's position in the file moves when it is updated, so a lock keyed by
//! position would be released against a row that no longer lives there. The key
//! is the value of the table's unique column, which is what the statement
//! taking the lock is addressing the row by in the first place.
//!
//! # What a lock is worth
//!
//! Only if writers consult it. `UPDATE` and `DELETE` take the same lock before
//! rewriting a row, so a row another transaction holds `FOR UPDATE` cannot be
//! written underneath it — which is the whole point of taking one.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::core::mvcc::TxId;
use crate::core::rql::row_lock::LockStrength;

/// Who holds one row's lock.
#[derive(Debug, Clone)]
enum Holder {
    /// One transaction, exclusively.
    Exclusive(TxId),
    /// Any number of transactions, none of which may write.
    Shared(Vec<TxId>),
}

/// The outcome of asking for a lock.
#[derive(Debug, PartialEq, Eq)]
pub enum Acquired {
    /// The caller now holds it.
    Yes,
    /// Someone else does. The transaction named is the one to wait for.
    Held(TxId),
}

/// A table-level lock taken by `LOCK TABLE`.
///
/// PostgreSQL distinguishes eight modes whose conflicts form a matrix. They are
/// mapped onto two here — shared and exclusive — chosen so the engine never
/// permits what PostgreSQL would refuse. A mode that only conflicts with itself
/// in PostgreSQL therefore conflicts a little more widely here, which costs
/// concurrency and never costs correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableLockMode {
    /// `ACCESS SHARE`, `ROW SHARE`, `SHARE` — readers coexist, writers wait.
    Shared,
    /// `ROW EXCLUSIVE` through `ACCESS EXCLUSIVE` — one holder, nobody else.
    Exclusive,
}

#[derive(Default)]
pub struct RowLocks {
    /// `(table, key) -> holder`.
    ///
    /// A plain mutex rather than a sharded map: a lock is taken once per
    /// claimed row and held for a transaction, so this is nowhere near a hot
    /// path, and one lock makes the "is it still held" question answerable
    /// without a second acquisition.
    held: Mutex<HashMap<(String, Vec<u8>), Holder>>,
    /// Whole tables locked by `LOCK TABLE`, keyed by name.
    ///
    /// Kept beside the row locks rather than in a structure of its own so that
    /// ending a transaction releases both in one step: a table lock that
    /// outlived its holder would wedge the table until restart, exactly as a
    /// stranded row lock would.
    tables: Mutex<HashMap<String, Holder>>,
}

impl RowLocks {
    /// Takes `strength` on one row for `tx`, or reports who has it.
    ///
    /// Re-taking a lock the caller already holds succeeds: the claim idiom
    /// selects rows `FOR UPDATE` and then updates them in the same
    /// transaction, which asks for the same lock twice.
    pub fn acquire(
        &self,
        table: &str,
        key: &[u8],
        tx: TxId,
        strength: LockStrength,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> Acquired {
        // `FROZEN_TX` is not a transaction: nothing ever commits or aborts it,
        // so a lock recorded under it is never released and wedges the row for
        // the life of the process. A statement running outside a transaction
        // cannot hold a lock past its own execution anyway, so it only ever
        // *checks* — which is what `conflict` does.
        if tx == crate::core::mvcc::transaction::FROZEN_TX {
            return match self.conflict(table, key, tx, strength, is_live) {
                Some(owner) => Acquired::Held(owner),
                None => Acquired::Yes,
            };
        }

        let mut held = self.held.lock().expect("row lock map poisoned");
        let entry_key = (table.to_string(), key.to_vec());

        match held.get_mut(&entry_key) {
            None => {}
            Some(Holder::Exclusive(owner)) => {
                if *owner == tx {
                    return Acquired::Yes;
                }
                // A lock whose owner has finished is nobody's: transactions end
                // in ways that do not run cleanup — a crashed session, an
                // aborted block — and a lock that outlived its holder would
                // wedge the row until restart.
                if is_live(*owner) {
                    return Acquired::Held(*owner);
                }
            }
            Some(Holder::Shared(owners)) => {
                owners.retain(|owner| is_live(*owner) || *owner == tx);
                match strength {
                    LockStrength::Share => {
                        if !owners.contains(&tx) {
                            owners.push(tx);
                        }
                        return Acquired::Yes;
                    }
                    LockStrength::Update => {
                        if let Some(other) = owners.iter().find(|owner| **owner != tx) {
                            return Acquired::Held(*other);
                        }
                        // Only this transaction shares it, so it may upgrade.
                    }
                }
            }
        }

        held.insert(
            entry_key,
            match strength {
                LockStrength::Update => Holder::Exclusive(tx),
                LockStrength::Share => Holder::Shared(vec![tx]),
            },
        );
        Acquired::Yes
    }

    /// Who holds a conflicting lock on this row, without taking one.
    ///
    /// The read-only half of [`RowLocks::acquire`], for callers that must not
    /// record ownership.
    pub fn conflict(
        &self,
        table: &str,
        key: &[u8],
        tx: TxId,
        strength: LockStrength,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> Option<TxId> {
        let held = self.held.lock().expect("row lock map poisoned");
        match held.get(&(table.to_string(), key.to_vec()))? {
            Holder::Exclusive(owner) if *owner != tx && is_live(*owner) => Some(*owner),
            Holder::Shared(owners) if matches!(strength, LockStrength::Update) => {
                owners.iter().find(|o| **o != tx && is_live(**o)).copied()
            }
            _ => None,
        }
    }

    /// Takes a whole-table lock for `tx`, or reports who holds it.
    pub fn acquire_table(
        &self,
        table: &str,
        tx: TxId,
        mode: TableLockMode,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> Acquired {
        // As in `acquire`: a lock under `FROZEN_TX` would never be released.
        if tx == crate::core::mvcc::transaction::FROZEN_TX {
            return match self.table_conflict(table, tx, true, is_live) {
                Some(owner) => Acquired::Held(owner),
                None => Acquired::Yes,
            };
        }

        let mut tables = self.tables.lock().expect("table lock map poisoned");
        match tables.get_mut(table) {
            None => {}
            Some(Holder::Exclusive(owner)) => {
                if *owner == tx {
                    return Acquired::Yes;
                }
                if is_live(*owner) {
                    return Acquired::Held(*owner);
                }
            }
            Some(Holder::Shared(owners)) => {
                owners.retain(|owner| is_live(*owner) || *owner == tx);
                match mode {
                    TableLockMode::Shared => {
                        if !owners.contains(&tx) {
                            owners.push(tx);
                        }
                        return Acquired::Yes;
                    }
                    TableLockMode::Exclusive => {
                        if let Some(other) = owners.iter().find(|owner| **owner != tx) {
                            return Acquired::Held(*other);
                        }
                    }
                }
            }
        }
        tables.insert(
            table.to_string(),
            match mode {
                TableLockMode::Exclusive => Holder::Exclusive(tx),
                TableLockMode::Shared => Holder::Shared(vec![tx]),
            },
        );
        Acquired::Yes
    }

    /// Who, other than `tx`, holds a table lock that a write would conflict
    /// with.
    ///
    /// A shared lock blocks writers but not readers; an exclusive one blocks
    /// both. `for_write` says which question is being asked.
    pub fn table_conflict(
        &self,
        table: &str,
        tx: TxId,
        for_write: bool,
        is_live: &dyn Fn(TxId) -> bool,
    ) -> Option<TxId> {
        let tables = self.tables.lock().expect("table lock map poisoned");
        match tables.get(table)? {
            Holder::Exclusive(owner) if *owner != tx && is_live(*owner) => Some(*owner),
            Holder::Shared(owners) if for_write => {
                owners.iter().find(|o| **o != tx && is_live(**o)).copied()
            }
            _ => None,
        }
    }

    /// Releases everything `tx` holds. Called when a transaction ends, however
    /// it ends.
    pub fn release_all(&self, tx: TxId) {
        {
            let mut tables = self.tables.lock().expect("table lock map poisoned");
            tables.retain(|_, holder| match holder {
                Holder::Exclusive(owner) => *owner != tx,
                Holder::Shared(owners) => {
                    owners.retain(|owner| *owner != tx);
                    !owners.is_empty()
                }
            });
        }
        let mut held = self.held.lock().expect("row lock map poisoned");
        held.retain(|_, holder| match holder {
            Holder::Exclusive(owner) => *owner != tx,
            Holder::Shared(owners) => {
                owners.retain(|owner| *owner != tx);
                !owners.is_empty()
            }
        });
    }

    /// How many rows are locked. Diagnostic.
    pub fn len(&self) -> usize {
        self.held.lock().expect("row lock map poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIVE: &dyn Fn(TxId) -> bool = &|_| true;
    const DEAD: &dyn Fn(TxId) -> bool = &|_| false;

    #[test]
    fn an_exclusive_lock_excludes_everyone_else() {
        let locks = RowLocks::default();
        assert_eq!(
            locks.acquire("t", b"1", 10, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire("t", b"1", 20, LockStrength::Update, LIVE),
            Acquired::Held(10)
        );
        // A different row is unaffected.
        assert_eq!(
            locks.acquire("t", b"2", 20, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        // As is the same key in a different table.
        assert_eq!(
            locks.acquire("u", b"1", 20, LockStrength::Update, LIVE),
            Acquired::Yes
        );
    }

    #[test]
    fn a_holder_may_take_its_own_lock_again() {
        // The claim idiom locks rows and then updates them in the same
        // transaction, which asks for the same lock a second time. Refusing
        // that would deadlock a statement against itself.
        let locks = RowLocks::default();
        assert_eq!(
            locks.acquire("t", b"1", 10, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire("t", b"1", 10, LockStrength::Update, LIVE),
            Acquired::Yes
        );
    }

    #[test]
    fn releasing_a_transaction_frees_every_row_it_held() {
        let locks = RowLocks::default();
        locks.acquire("t", b"1", 10, LockStrength::Update, LIVE);
        locks.acquire("t", b"2", 10, LockStrength::Update, LIVE);
        assert_eq!(locks.len(), 2);
        locks.release_all(10);
        assert!(locks.is_empty());
        assert_eq!(
            locks.acquire("t", b"1", 20, LockStrength::Update, LIVE),
            Acquired::Yes
        );
    }

    #[test]
    fn a_statement_outside_a_transaction_never_records_a_lock() {
        // Nothing commits or aborts `FROZEN_TX`, so a lock recorded under it is
        // released by nothing and wedges the row until the process restarts.
        // One `SELECT … FOR UPDATE` run outside a transaction block did exactly
        // that, and every later write to the row was refused as "locked by
        // transaction 1".
        use crate::core::mvcc::transaction::FROZEN_TX;
        let locks = RowLocks::default();
        assert_eq!(
            locks.acquire("t", b"1", FROZEN_TX, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        assert!(locks.is_empty(), "nothing may be recorded");
        // And a real transaction is still free to take it afterwards.
        assert_eq!(
            locks.acquire("t", b"1", 20, LockStrength::Update, LIVE),
            Acquired::Yes
        );
        // It still *sees* a lock somebody else holds, so SKIP LOCKED and
        // NOWAIT keep working outside a transaction.
        assert_eq!(
            locks.acquire("t", b"1", FROZEN_TX, LockStrength::Update, LIVE),
            Acquired::Held(20)
        );
    }

    #[test]
    fn a_lock_whose_owner_has_ended_is_not_held() {
        // Transactions end in ways that run no cleanup. A lock that outlived
        // its owner would wedge the row until the process restarted.
        let locks = RowLocks::default();
        locks.acquire("t", b"1", 10, LockStrength::Update, LIVE);
        assert_eq!(
            locks.acquire("t", b"1", 20, LockStrength::Update, DEAD),
            Acquired::Yes
        );
    }

    #[test]
    fn a_table_lock_excludes_other_transactions() {
        let locks = RowLocks::default();
        assert_eq!(
            locks.acquire_table("t", 10, TableLockMode::Exclusive, LIVE),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire_table("t", 20, TableLockMode::Exclusive, LIVE),
            Acquired::Held(10)
        );
        // It blocks readers as well as writers, which is what ACCESS EXCLUSIVE
        // means.
        assert_eq!(locks.table_conflict("t", 20, false, LIVE), Some(10));
        assert_eq!(locks.table_conflict("t", 20, true, LIVE), Some(10));
        // Its own holder is never blocked by it.
        assert_eq!(locks.table_conflict("t", 10, true, LIVE), None);
        // Another table is unaffected.
        assert_eq!(locks.table_conflict("u", 20, true, LIVE), None);
    }

    #[test]
    fn a_shared_table_lock_admits_readers_and_blocks_writers() {
        let locks = RowLocks::default();
        locks.acquire_table("t", 10, TableLockMode::Shared, LIVE);
        assert_eq!(
            locks.acquire_table("t", 20, TableLockMode::Shared, LIVE),
            Acquired::Yes
        );
        assert_eq!(locks.table_conflict("t", 30, false, LIVE), None);
        assert_eq!(locks.table_conflict("t", 30, true, LIVE), Some(10));
    }

    #[test]
    fn ending_a_transaction_releases_its_table_locks_too() {
        // A stranded table lock would wedge the whole table until restart.
        let locks = RowLocks::default();
        locks.acquire_table("t", 10, TableLockMode::Exclusive, LIVE);
        locks.acquire("t", b"1", 10, LockStrength::Update, LIVE);
        locks.release_all(10);
        assert_eq!(
            locks.acquire_table("t", 20, TableLockMode::Exclusive, LIVE),
            Acquired::Yes
        );
        assert!(locks.is_empty());
    }

    #[test]
    fn shared_locks_coexist_but_exclude_a_writer() {
        let locks = RowLocks::default();
        assert_eq!(
            locks.acquire("t", b"1", 10, LockStrength::Share, LIVE),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire("t", b"1", 20, LockStrength::Share, LIVE),
            Acquired::Yes
        );
        assert_eq!(
            locks.acquire("t", b"1", 30, LockStrength::Update, LIVE),
            Acquired::Held(10)
        );
        // Once the other sharer is gone, the remaining one may upgrade.
        locks.release_all(20);
        assert_eq!(
            locks.acquire("t", b"1", 10, LockStrength::Update, LIVE),
            Acquired::Yes
        );
    }
}
