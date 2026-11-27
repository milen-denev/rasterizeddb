//! A wildcard trie: AMQP topic-exchange bindings (`.`, `*`, `#`) and MQTT
//! filters (`/`, `+`, `#`), as one structure with the separator and wildcards
//! as parameters (docs/broker.md §5).
//!
//! A filter is split into words. `one` matches exactly one word, and `many`
//! matches zero or more, anywhere in an AMQP key and last in an MQTT filter.
//! MQTT's rule that a wildcard at the first level does not match a topic
//! starting with `$` is [`Syntax::dollar_hidden`].
//!
//! Matching walks one path per exact word plus the wildcard branches, so it
//! costs about the topic's depth times the wildcard nodes met, however many
//! filters are stored. One topic can reach the same value by two paths
//! (`#.#`), so callers deduplicate.

use std::collections::HashMap;

#[derive(Clone, Copy, Debug)]
pub struct Syntax {
    pub sep: u8,
    pub one: &'static [u8],
    pub many: &'static [u8],
    pub dollar_hidden: bool,
}

pub const AMQP: Syntax = Syntax { sep: b'.', one: b"*", many: b"#", dollar_hidden: false };
pub const MQTT: Syntax = Syntax { sep: b'/', one: b"+", many: b"#", dollar_hidden: true };

struct Node<V> {
    exact: HashMap<Box<[u8]>, Node<V>>,
    one: Option<Box<Node<V>>>,
    many: Option<Box<Node<V>>>,
    values: Vec<V>,
}

impl<V> Default for Node<V> {
    fn default() -> Self {
        Node { exact: HashMap::new(), one: None, many: None, values: Vec::new() }
    }
}

impl<V> Node<V> {
    fn is_empty(&self) -> bool {
        self.values.is_empty() && self.exact.is_empty() && self.one.is_none() && self.many.is_none()
    }
}

pub struct Trie<V> {
    root: Node<V>,
    syntax: Syntax,
    len: usize,
}

impl<V: PartialEq + Clone> Trie<V> {
    pub fn new(syntax: Syntax) -> Self {
        Trie { root: Node::default(), syntax, len: 0 }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Adds `value` under `filter`; a value already there is not added twice.
    pub fn insert(&mut self, filter: &[u8], value: V) -> bool {
        let syn = self.syntax;
        let mut node = &mut self.root;
        for word in filter.split(|&b| b == syn.sep) {
            node = if word == syn.one {
                node.one.get_or_insert_with(Default::default)
            } else if word == syn.many {
                node.many.get_or_insert_with(Default::default)
            } else {
                node.exact.entry(word.into()).or_default()
            };
        }
        if node.values.contains(&value) {
            return false;
        }
        node.values.push(value);
        self.len += 1;
        true
    }

    /// Removes `value` from under `filter`. Empty branches are pruned.
    pub fn remove(&mut self, filter: &[u8], value: &V) -> bool {
        let words: Vec<&[u8]> = filter.split(|&b| b == self.syntax.sep).collect();
        let removed = remove_at(&mut self.root, &words, self.syntax, value);
        if removed {
            self.len -= 1;
        }
        removed
    }

    /// Calls `emit` for every value whose filter matches `topic`.
    pub fn matches(&self, topic: &[u8], mut emit: impl FnMut(&V)) {
        let words: smallvec::SmallVec<[&[u8]; 16]> = topic.split(|&b| b == self.syntax.sep).collect();
        let hidden = self.syntax.dollar_hidden && topic.first() == Some(&b'$');
        walk(&self.root, &words, 0, hidden, &mut emit);
    }
}

fn walk<V>(node: &Node<V>, words: &[&[u8]], i: usize, hidden_first: bool, emit: &mut impl FnMut(&V)) {
    let first_level = i == 0 && hidden_first;
    if i == words.len() {
        node.values.iter().for_each(&mut *emit);
        // `many` matches zero words too: `a.#` takes `a`.
        if let Some(m) = &node.many {
            walk(m, words, i, false, emit);
        }
        return;
    }
    if let Some(child) = node.exact.get(words[i]) {
        walk(child, words, i + 1, false, emit);
    }
    if first_level {
        return;
    }
    if let Some(one) = &node.one {
        walk(one, words, i + 1, false, emit);
    }
    if let Some(m) = &node.many {
        // Zero or more words: try every remaining length. A trailing `#` (the
        // common case) has no children and matches at once.
        if m.exact.is_empty() && m.one.is_none() && m.many.is_none() {
            m.values.iter().for_each(&mut *emit);
        } else {
            for j in i..=words.len() {
                walk(m, words, j, false, emit);
            }
        }
    }
}

fn remove_at<V: PartialEq>(node: &mut Node<V>, words: &[&[u8]], syn: Syntax, value: &V) -> bool {
    let Some((word, rest)) = words.split_first() else {
        let before = node.values.len();
        node.values.retain(|v| v != value);
        return node.values.len() != before;
    };
    if *word == syn.one {
        let Some(child) = node.one.as_mut() else { return false };
        let r = remove_at(child, rest, syn, value);
        if child.is_empty() {
            node.one = None;
        }
        r
    } else if *word == syn.many {
        let Some(child) = node.many.as_mut() else { return false };
        let r = remove_at(child, rest, syn, value);
        if child.is_empty() {
            node.many = None;
        }
        r
    } else {
        let Some(child) = node.exact.get_mut(*word) else { return false };
        let r = remove_at(child, rest, syn, value);
        if child.is_empty() {
            node.exact.remove(*word);
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hits(t: &Trie<u32>, topic: &str) -> Vec<u32> {
        let mut v = Vec::new();
        t.matches(topic.as_bytes(), |x| {
            if !v.contains(x) {
                v.push(*x)
            }
        });
        v.sort();
        v
    }

    #[test]
    fn amqp_words() {
        let mut t = Trie::new(AMQP);
        t.insert(b"stock.*.nyse", 1);
        t.insert(b"stock.#", 2);
        t.insert(b"#", 3);
        t.insert(b"*.usd.#", 4);
        t.insert(b"a.#.z", 5);
        assert_eq!(hits(&t, "stock.ibm.nyse"), [1, 2, 3]);
        assert_eq!(hits(&t, "stock"), [2, 3]);
        assert_eq!(hits(&t, "fx.usd"), [3, 4]);
        assert_eq!(hits(&t, "a.z"), [3, 5]);
        assert_eq!(hits(&t, "a.b.c.z"), [3, 5]);
        assert!(t.remove(b"#", &3));
        assert_eq!(hits(&t, "x"), Vec::<u32>::new());
    }

    #[test]
    fn mqtt_filters() {
        let mut t = Trie::new(MQTT);
        t.insert(b"sport/+/player1", 1);
        t.insert(b"sport/#", 2);
        t.insert(b"#", 3);
        t.insert(b"+/monitor", 4);
        t.insert(b"$SYS/#", 5);
        assert_eq!(hits(&t, "sport/tennis/player1"), [1, 2, 3]);
        assert_eq!(hits(&t, "sport"), [2, 3]);
        assert_eq!(hits(&t, "$SYS/monitor"), [5]);
        assert_eq!(hits(&t, "a/monitor"), [3, 4]);
    }
}
