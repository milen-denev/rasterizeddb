//! Clients parked in `BLPOP`/`BRPOP`/`BLMOVE` until a key gets an element.
//!
//! A push checks one relaxed counter and returns when nobody waits, so the
//! common case costs a load. When someone does wait, the push wakes as many
//! waiters on that key as it pushed elements, oldest first. A woken waiter
//! retries its pop under the shard lock like any other command. If it loses
//! the race, or times out holding a wake-up it never used, it passes the
//! wake-up on. That way an element is never left in a list while a client
//! sleeps on it.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;
use tokio::sync::Notify;

#[derive(Default)]
pub struct Waiters {
    count: AtomicUsize,
    map: Mutex<HashMap<Box<[u8]>, VecDeque<Arc<Notify>>, ahash::RandomState>>,
}

impl Waiters {
    pub fn register(&self, keys: &[Vec<u8>], notify: &Arc<Notify>) {
        let mut map = self.map.lock();
        for key in keys {
            map.entry(key.as_slice().into())
                .or_default()
                .push_back(notify.clone());
        }
        self.count.fetch_add(keys.len(), Ordering::Release);
    }

    pub fn unregister(&self, keys: &[Vec<u8>], notify: &Arc<Notify>) {
        let mut map = self.map.lock();
        let mut removed = 0;
        for key in keys {
            if let Some(queue) = map.get_mut(key.as_slice()) {
                if let Some(at) = queue.iter().position(|n| Arc::ptr_eq(n, notify)) {
                    queue.remove(at);
                    removed += 1;
                }
                if queue.is_empty() {
                    map.remove(key.as_slice());
                }
            }
        }
        self.count.fetch_sub(removed, Ordering::Release);
    }

    /// Wakes up to `n` waiters on `key`, oldest first.
    #[inline]
    pub fn wake(&self, key: &[u8], n: usize) {
        if self.count.load(Ordering::Acquire) == 0 {
            return;
        }
        let map = self.map.lock();
        if let Some(queue) = map.get(key) {
            for notify in queue.iter().take(n.max(1)) {
                notify.notify_one();
            }
        }
    }

    pub fn waiting(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }
}
