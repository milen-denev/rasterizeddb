//! `PUBLISH` / `SUBSCRIBE`: messages fanned out to connections.
//!
//! A subscriber is a connection's bounded queue. `PUBLISH` takes a read lock on
//! the channel map, clones two `Arc`s per subscriber and `try_send`s. It never
//! waits on a slow reader. A subscriber whose queue is full is marked
//! overflowed and its connection closes itself, which is what Redis does to a
//! pub/sub client past its output-buffer limit. A publisher is never stalled
//! by the slowest subscriber, and a subscriber that falls behind is never
//! quietly sent a stream with holes in it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use parking_lot::RwLock;
use tokio::sync::mpsc;

/// Messages a subscriber may have queued before it is cut off.
pub const QUEUE: usize = 65_536;

#[derive(Debug, Clone)]
pub enum Message {
    Message {
        channel: Arc<[u8]>,
        data: Arc<[u8]>,
    },
    PMessage {
        pattern: Arc<[u8]>,
        channel: Arc<[u8]>,
        data: Arc<[u8]>,
    },
}

#[derive(Clone)]
pub struct Subscriber {
    pub id: u64,
    pub tx: mpsc::Sender<Message>,
    pub overflowed: Arc<AtomicBool>,
}

impl Subscriber {
    fn deliver(&self, message: Message) -> bool {
        match self.tx.try_send(message) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.overflowed.store(true, Ordering::Release);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }
}

#[derive(Default)]
pub struct PubSub {
    channels: RwLock<HashMap<Box<[u8]>, Vec<Subscriber>, ahash::RandomState>>,
    patterns: RwLock<Vec<(Box<[u8]>, Subscriber)>>,
    pattern_count: AtomicUsize,
}

impl PubSub {
    /// Delivers `data` on `channel`; how many subscribers received it.
    pub fn publish(&self, channel: &[u8], data: &[u8]) -> usize {
        let mut delivered = 0;
        let channel_arc: Arc<[u8]> = Arc::from(channel);
        let data_arc: Arc<[u8]> = Arc::from(data);
        if let Some(subs) = self.channels.read().get(channel) {
            for sub in subs {
                if sub.deliver(Message::Message {
                    channel: channel_arc.clone(),
                    data: data_arc.clone(),
                }) {
                    delivered += 1;
                }
            }
        }
        if self.pattern_count.load(Ordering::Acquire) > 0 {
            for (pattern, sub) in self.patterns.read().iter() {
                if super::glob::matches(pattern, channel)
                    && sub.deliver(Message::PMessage {
                        pattern: Arc::from(&**pattern),
                        channel: channel_arc.clone(),
                        data: data_arc.clone(),
                    })
                {
                    delivered += 1;
                }
            }
        }
        delivered
    }

    /// `true` when this subscriber was not already on `channel`.
    pub fn subscribe(&self, channel: &[u8], sub: &Subscriber) -> bool {
        let mut map = self.channels.write();
        let subs = map.entry(channel.into()).or_default();
        if subs.iter().any(|s| s.id == sub.id) {
            return false;
        }
        subs.push(sub.clone());
        true
    }

    pub fn unsubscribe(&self, channel: &[u8], id: u64) -> bool {
        let mut map = self.channels.write();
        let Some(subs) = map.get_mut(channel) else {
            return false;
        };
        let before = subs.len();
        subs.retain(|s| s.id != id);
        let removed = subs.len() != before;
        if subs.is_empty() {
            map.remove(channel);
        }
        removed
    }

    pub fn psubscribe(&self, pattern: &[u8], sub: &Subscriber) -> bool {
        let mut patterns = self.patterns.write();
        if patterns.iter().any(|(p, s)| s.id == sub.id && **p == *pattern) {
            return false;
        }
        patterns.push((pattern.into(), sub.clone()));
        self.pattern_count.store(patterns.len(), Ordering::Release);
        true
    }

    pub fn punsubscribe(&self, pattern: &[u8], id: u64) -> bool {
        let mut patterns = self.patterns.write();
        let before = patterns.len();
        patterns.retain(|(p, s)| !(s.id == id && **p == *pattern));
        self.pattern_count.store(patterns.len(), Ordering::Release);
        patterns.len() != before
    }

    /// Drops every subscription `id` holds, when its connection goes.
    pub fn forget(&self, id: u64, channels: &[Vec<u8>], patterns: &[Vec<u8>]) {
        for c in channels {
            self.unsubscribe(c, id);
        }
        for p in patterns {
            self.punsubscribe(p, id);
        }
    }

    /// `PUBSUB CHANNELS [pattern]`.
    pub fn channels(&self, pattern: Option<&[u8]>) -> Vec<Vec<u8>> {
        self.channels
            .read()
            .keys()
            .filter(|c| pattern.is_none_or(|p| super::glob::matches(p, c)))
            .map(|c| c.to_vec())
            .collect()
    }

    pub fn numsub(&self, channel: &[u8]) -> usize {
        self.channels.read().get(channel).map_or(0, Vec::len)
    }

    pub fn numpat(&self) -> usize {
        self.pattern_count.load(Ordering::Relaxed)
    }
}
