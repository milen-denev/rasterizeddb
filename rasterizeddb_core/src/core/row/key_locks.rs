//! Locks over unique-key values, striped by hash.
//!
//! A uniqueness check has to be atomic with the write it guards, but only
//! against writes of the *same* key. One lock per table made every insert into
//! a keyed table, and every upsert, take turns. Here a key hashes to one of
//! [`STRIPES`] locks, so two writers serialize only when their keys share a
//! stripe.
//!
//! Every holder takes its stripes in ascending order, and while waiting for a
//! stripe holds none above it, so no two holders can wait on each other.
//! [`KeyLocks::lock_more`] is the one place that may be asked for a stripe
//! below one already held: it tries that stripe once instead of waiting, and
//! the caller falls back to the table-wide path when it is taken.

use smallvec::SmallVec;
use tokio::sync::{Mutex, MutexGuard};

/// Enough that 64 writers on random keys rarely meet; few enough that a
/// statement taking all of them pays little.
pub const STRIPES: usize = 64;

pub struct KeyLocks {
    stripes: Box<[Mutex<()>]>,
}

/// The stripes one statement holds, released together on drop.
pub struct KeyGuard<'a> {
    held: SmallVec<[(usize, MutexGuard<'a, ()>); 4]>,
}

impl KeyGuard<'_> {
    pub fn holds(&self, stripe: usize) -> bool {
        self.held.iter().any(|(s, _)| *s == stripe)
    }
}

impl Default for KeyLocks {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyLocks {
    pub fn new() -> Self {
        Self {
            stripes: (0..STRIPES).map(|_| Mutex::new(())).collect(),
        }
    }

    /// The stripe for one constraint's key. `canonical` is `None` when the
    /// key has no canonical form; every value of that constraint then shares
    /// one stripe, which is the single lock this replaced.
    pub fn stripe_of(constraint: usize, canonical: Option<&[u8]>) -> usize {
        use std::hash::{BuildHasher, Hasher};
        // Fixed seeds: two statements must agree on a key's stripe.
        let mut h = ahash::RandomState::with_seeds(0x5eed_0001, 0x5eed_0002, 0x5eed_0003, 0x5eed_0004).build_hasher();
        h.write_usize(constraint);
        if let Some(key) = canonical {
            h.write(key);
        }
        (h.finish() as usize) % STRIPES
    }

    /// Takes these stripes, in ascending order.
    pub async fn lock(&self, stripes: &[usize]) -> KeyGuard<'_> {
        let mut sorted: SmallVec<[usize; 4]> = stripes.iter().copied().collect();
        sorted.sort_unstable();
        sorted.dedup();
        let mut held = SmallVec::new();
        for s in sorted {
            held.push((s, self.stripes[s].lock().await));
        }
        KeyGuard { held }
    }

    /// Takes every stripe: for a statement whose keys are not known before it
    /// writes.
    pub async fn lock_all(&self) -> KeyGuard<'_> {
        let mut held = SmallVec::with_capacity(STRIPES);
        for (s, m) in self.stripes.iter().enumerate() {
            held.push((s, m.lock().await));
        }
        KeyGuard { held }
    }

    /// Adds `stripes` to what `guard` holds. A stripe above every one held is
    /// waited for; one below is only tried, since waiting for it could close
    /// a cycle. `false` when such a stripe was taken: the caller must drop
    /// the guard and start again on a path that does not need this.
    pub async fn lock_more<'a>(&'a self, guard: &mut KeyGuard<'a>, stripes: &[usize]) -> bool {
        let mut wanted: SmallVec<[usize; 4]> = stripes.iter().copied().filter(|s| !guard.holds(*s)).collect();
        wanted.sort_unstable();
        wanted.dedup();
        let top = guard.held.iter().map(|(s, _)| *s).max();
        for s in wanted {
            if top.is_some_and(|t| s < t) {
                match self.stripes[s].try_lock() {
                    Ok(g) => guard.held.push((s, g)),
                    Err(_) => return false,
                }
            } else {
                guard.held.push((s, self.stripes[s].lock().await));
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn a_key_always_lands_on_one_stripe() {
        let a = KeyLocks::stripe_of(0, Some(b"alpha"));
        assert_eq!(a, KeyLocks::stripe_of(0, Some(b"alpha")));
        // Not every key shares it, or this is one lock again.
        let distinct: std::collections::HashSet<usize> =
            (0..1000u32).map(|i| KeyLocks::stripe_of(0, Some(&i.to_le_bytes()))).collect();
        assert!(distinct.len() > STRIPES / 2, "{} stripes used", distinct.len());
        // A key without a canonical form serializes its whole constraint.
        assert_eq!(KeyLocks::stripe_of(3, None), KeyLocks::stripe_of(3, None));
    }

    #[tokio::test]
    async fn the_same_stripe_excludes_and_others_do_not() {
        let locks = Arc::new(KeyLocks::new());
        let g = locks.lock(&[5, 1, 5]).await;
        assert!(g.holds(1) && g.holds(5));
        assert!(locks.stripes[5].try_lock().is_err());
        assert!(locks.stripes[2].try_lock().is_ok());
        drop(g);
        assert!(locks.stripes[5].try_lock().is_ok());
    }

    #[tokio::test]
    async fn lock_more_never_waits_below_what_it_holds() {
        let locks = KeyLocks::new();
        let other = locks.lock(&[3]).await;
        let mut g = locks.lock(&[10]).await;
        // 3 is below 10 and taken: refused rather than waited for.
        assert!(!locks.lock_more(&mut g, &[3, 20]).await);
        drop(g);
        drop(other);
        let mut g = locks.lock(&[10]).await;
        assert!(locks.lock_more(&mut g, &[3, 20, 10]).await);
        assert!(g.holds(3) && g.holds(10) && g.holds(20));
    }

    #[tokio::test]
    async fn concurrent_holders_of_overlapping_sets_finish() {
        let locks = Arc::new(KeyLocks::new());
        let mut tasks = Vec::new();
        for i in 0..32usize {
            let locks = locks.clone();
            tasks.push(tokio::spawn(async move {
                for j in 0..200usize {
                    let set = [(i * 7 + j) % STRIPES, (i * 13 + j * 3) % STRIPES, (j * 5) % STRIPES];
                    let _g = if j % 50 == 0 { locks.lock_all().await } else { locks.lock(&set).await };
                    tokio::task::yield_now().await;
                }
            }));
        }
        let all = futures::future::join_all(tasks);
        tokio::time::timeout(std::time::Duration::from_secs(30), all)
            .await
            .expect("stripe locking deadlocked");
    }
}
