//! `sp_getapplock` / `sp_releaseapplock`: named locks between connections,
//! which EF Core 9+ takes around `Migrate()` (`__EFMigrationsLock`).
//!
//! The locks live in this process, per database. A replica does not see
//! them, as it does not run migrations. A lock is held by a connection
//! (`@LockOwner = 'Session'`) or by its transaction (`'Transaction'`), and
//! counts: each get needs its release. `Shared` is compatible with
//! `Shared`; every other mode is taken as exclusive, which can only make a
//! caller wait longer than SQL Server would, never let two in at once.
//! Session locks go when the connection closes; transaction locks when its
//! transaction ends.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Owner {
    Session,
    Transaction,
}

#[derive(Default)]
struct Lock {
    /// (connection, owner, shared) → how many times it was taken.
    holders: HashMap<(u64, Owner), (bool, u32)>,
}

impl Lock {
    fn grantable(&self, conn: u64, shared: bool) -> bool {
        self.holders
            .iter()
            .all(|((c, _), (s, _))| *c == conn || (shared && *s))
    }
}

static LOCKS: std::sync::LazyLock<Mutex<HashMap<(usize, String), Lock>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// The resource name as SQL Server compares it: case-insensitively.
fn key(database: usize, resource: &str) -> (usize, String) {
    (database, resource.to_lowercase())
}

/// 0 granted at once, 1 granted after waiting, -1 timed out. `timeout`
/// `None` waits for ever (`@LockTimeout = -1`, the default).
pub(crate) async fn get(
    database: usize,
    resource: &str,
    conn: u64,
    owner: Owner,
    shared: bool,
    timeout: Option<Duration>,
) -> i32 {
    let started = Instant::now();
    let mut waited = false;
    loop {
        {
            let mut locks = LOCKS.lock();
            let lock = locks.entry(key(database, resource)).or_default();
            if lock.grantable(conn, shared) {
                let held = lock.holders.entry((conn, owner)).or_insert((shared, 0));
                // A connection taking it again in exclusive mode holds it
                // exclusively.
                held.0 &= shared;
                held.1 += 1;
                return if waited { 1 } else { 0 };
            }
        }
        if timeout.is_some_and(|t| started.elapsed() >= t) {
            return -1;
        }
        waited = true;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// 0 released, -999 when the connection does not hold it.
pub(crate) fn release(database: usize, resource: &str, conn: u64, owner: Owner) -> i32 {
    let mut locks = LOCKS.lock();
    let k = key(database, resource);
    let Some(lock) = locks.get_mut(&k) else {
        return -999;
    };
    let Some(held) = lock.holders.get_mut(&(conn, owner)) else {
        return -999;
    };
    held.1 -= 1;
    if held.1 == 0 {
        lock.holders.remove(&(conn, owner));
    }
    if lock.holders.is_empty() {
        locks.remove(&k);
    }
    0
}

/// Whether the connection holds the lock (APPLOCK_TEST's "held" side).
pub(crate) fn holds(database: usize, resource: &str, conn: u64) -> bool {
    LOCKS
        .lock()
        .get(&key(database, resource))
        .is_some_and(|l| l.holders.keys().any(|(c, _)| *c == conn))
}

/// Everything a connection holds under `owner` (or under any owner) goes.
pub(crate) fn release_all(conn: u64, owner: Option<Owner>) {
    let mut locks = LOCKS.lock();
    locks.retain(|_, lock| {
        lock.holders
            .retain(|(c, o), _| *c != conn || owner.is_some_and(|w| w != *o));
        !lock.holders.is_empty()
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn exclusive_waits_shared_shares_and_close_releases() {
        let db = 0xa11c;
        assert_eq!(get(db, "Mig", 1, Owner::Session, false, None).await, 0);
        assert_eq!(get(db, "MIG", 2, Owner::Session, false, Some(Duration::from_millis(20))).await, -1);
        // Counted: a second get needs a second release.
        assert_eq!(get(db, "mig", 1, Owner::Session, false, None).await, 0);
        assert_eq!(release(db, "mig", 1, Owner::Session), 0);
        assert!(holds(db, "mig", 1));
        assert_eq!(release(db, "mig", 2, Owner::Session), -999);
        let waiter = tokio::spawn(get(db, "mig", 2, Owner::Session, false, None));
        tokio::time::sleep(Duration::from_millis(20)).await;
        release_all(1, None);
        assert_eq!(waiter.await.unwrap(), 1);
        release_all(2, None);
        assert_eq!(get(db, "s", 3, Owner::Transaction, true, None).await, 0);
        assert_eq!(get(db, "s", 4, Owner::Session, true, None).await, 0);
        assert_eq!(get(db, "s", 5, Owner::Session, false, Some(Duration::ZERO)).await, -1);
        release_all(3, Some(Owner::Session));
        assert!(holds(db, "s", 3));
        release_all(3, Some(Owner::Transaction));
        release_all(4, None);
        assert_eq!(get(db, "s", 5, Owner::Session, false, Some(Duration::ZERO)).await, 0);
        release_all(5, None);
    }
}
