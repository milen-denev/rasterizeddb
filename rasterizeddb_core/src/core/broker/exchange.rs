//! Exchanges: named routing tables from a routing key (and headers) to topics
//! (docs/broker.md §5).
//!
//! Bindings name their destination topic. Each exchange compiles its bindings
//! into the table its kind needs: a hash map for `direct`, a list for
//! `fanout`, a [`Trie`] for `topic` and `mqtt`, and a list of rules for
//! `headers`. A route is a set of topic ids, deduplicated, so a topic bound
//! twice gets one copy.

use std::collections::HashMap;

use rasterizeddb_mq::wire::{Binding, ExchangeKind, XMatch};
use smallvec::SmallVec;

use super::trie::{self, Trie};

/// Topics a publish goes to.
pub type Route = SmallVec<[u32; 4]>;

pub struct Exchange {
    pub name: String,
    pub kind: ExchangeKind,
    /// Where an unroutable publish goes instead.
    pub alternate: Option<String>,
    pub durable: bool,
    /// Every binding, with its destination's id.
    pub bindings: Vec<(Binding, u32)>,
    direct: HashMap<String, Route>,
    trie: Option<Trie<u32>>,
}

impl Exchange {
    pub fn new(name: &str, kind: ExchangeKind, alternate: Option<String>, durable: bool) -> Exchange {
        let trie = match kind {
            ExchangeKind::Topic => Some(Trie::new(trie::AMQP)),
            ExchangeKind::Mqtt => Some(Trie::new(trie::MQTT)),
            _ => None,
        };
        Exchange {
            name: name.to_string(),
            kind,
            alternate,
            durable,
            bindings: Vec::new(),
            direct: HashMap::new(),
            trie,
        }
    }

    /// Adds a binding; `false` when it was already there.
    pub fn bind(&mut self, b: Binding, topic: u32) -> bool {
        if self.bindings.iter().any(|(x, t)| *t == topic && x.key == b.key && x.args == b.args && x.x_match == b.x_match) {
            return false;
        }
        match self.kind {
            ExchangeKind::Direct => {
                let r = self.direct.entry(b.key.clone()).or_default();
                if !r.contains(&topic) {
                    r.push(topic);
                }
            }
            ExchangeKind::Topic | ExchangeKind::Mqtt => {
                self.trie.as_mut().expect("trie").insert(b.key.as_bytes(), topic);
            }
            ExchangeKind::Fanout | ExchangeKind::Headers => {}
        }
        self.bindings.push((b, topic));
        true
    }

    /// Removes a binding; `false` when there was none.
    pub fn unbind(&mut self, b: &Binding, topic: u32) -> bool {
        let before = self.bindings.len();
        self.bindings.retain(|(x, t)| !(*t == topic && x.key == b.key && x.args == b.args));
        if self.bindings.len() == before {
            return false;
        }
        self.rebuild();
        true
    }

    /// Drops every binding to `topic` (it was deleted).
    pub fn unbind_topic(&mut self, topic: u32) -> bool {
        let before = self.bindings.len();
        self.bindings.retain(|(_, t)| *t != topic);
        if self.bindings.len() == before {
            return false;
        }
        self.rebuild();
        true
    }

    fn rebuild(&mut self) {
        let bindings = std::mem::take(&mut self.bindings);
        self.direct.clear();
        if let Some(t) = self.trie.as_mut() {
            *t = Trie::new(if self.kind == ExchangeKind::Mqtt { trie::MQTT } else { trie::AMQP });
        }
        for (b, t) in bindings {
            self.bind(b, t);
        }
    }

    /// The topics a message with `key` and `headers` goes to.
    pub fn route<'h>(&self, key: &str, headers: impl Fn(&str) -> Option<&'h [u8]>) -> Route {
        let mut out = Route::new();
        match self.kind {
            ExchangeKind::Direct => {
                if let Some(r) = self.direct.get(key) {
                    out.extend_from_slice(r);
                }
            }
            ExchangeKind::Fanout => {
                for (_, t) in &self.bindings {
                    if !out.contains(t) {
                        out.push(*t);
                    }
                }
            }
            ExchangeKind::Topic | ExchangeKind::Mqtt => {
                self.trie.as_ref().expect("trie").matches(key.as_bytes(), |t| {
                    if !out.contains(t) {
                        out.push(*t);
                    }
                });
            }
            ExchangeKind::Headers => {
                for (b, t) in &self.bindings {
                    if out.contains(t) {
                        continue;
                    }
                    let mut wanted = b.args.iter().filter(|(k, _)| !k.starts_with("x-"));
                    let hit = match b.x_match {
                        XMatch::All => wanted.all(|(k, v)| headers(k).is_some_and(|h| h == v.as_slice())),
                        XMatch::Any => wanted.any(|(k, v)| headers(k).is_some_and(|h| h == v.as_slice())),
                    };
                    if hit {
                        out.push(*t);
                    }
                }
            }
        }
        out
    }
}
