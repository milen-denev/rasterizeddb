//! An inverted index over a document column, PostgreSQL's GIN for `jsonb`.
//!
//! ```sql
//! CREATE INDEX docs_gin ON docs USING gin (doc);                 -- jsonb_ops
//! CREATE INDEX docs_gin ON docs USING gin (doc jsonb_path_ops);  -- path ops
//! ```
//!
//! Each document is reduced to a set of 64-bit *items* (`jsonb::gin_items`):
//! its keys and scalars under `jsonb_ops`, its leaf paths under
//! `jsonb_path_ops`. The index maps an item to the slots whose document has
//! it. `doc @> q` is answered by intersecting the lists of `q`'s items, `?`
//! and `?&` likewise for key items, `?|` by their union.
//!
//! # A superset, never a subset
//!
//! The rest of the index layer's rule (see the parent module) holds here
//! through one property, pinned by a property test in `jsonb`: whenever
//! `contains(doc, q)` is true, every item of `q` is an item of `doc`. Items
//! are hashes, so two items may collide; a collision only adds candidates,
//! which the scan rechecks against the whole `WHERE` clause.
//!
//! A query with no items (`doc @> '{}'`, which every object satisfies) cannot
//! be narrowed and is declined.
//!
//! # Removal is lazy
//!
//! A common item (`"status": "shipped"`) has a posting list of a large share
//! of the table, and vacuum forgets reclaimed slots one at a time. Removing
//! each from every list it is on would cost a list's length per slot. Slots
//! are never reused until a rebuild renumbers them (`VACUUM FULL`), so a
//! forgotten slot only has to stop being *returned*: its presence bit is
//! cleared and lookups skip it. The lists are compacted once forgotten
//! entries outnumber a quarter of them.
//!
//! An in-place overwrite adds the new document's items and leaves the old
//! ones: a stale item on a live slot costs a candidate, never an answer.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::RandomState;
use parking_lot::RwLock;

use crate::core::index::IndexDecl;
use crate::core::jsonb::{self, GinOpclass};

const SHARDS: usize = 64;

pub struct GinIndex {
    pub decl: IndexDecl,
    /// The document column's `write_order`.
    pub column: u32,
    pub opclass: GinOpclass,
    /// Item to the slots holding it, each list sorted and distinct.
    shards: Box<[RwLock<HashMap<u64, Vec<u64>, RandomState>>]>,
    /// One bit per slot this index holds. A cleared bit is a slot vacuum has
    /// forgotten; its entries stay in the lists until a compaction.
    present: RwLock<Vec<u64>>,
    /// Slots whose document could not be read into items. Returned with
    /// every lookup.
    unkeyed: RwLock<Vec<u64>>,
    /// Entries in the lists, and how many of them belong to forgotten slots.
    entries: AtomicU64,
    forgotten: AtomicU64,
}

/// What a lookup is asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GinQuery {
    /// Slots having every item (`@>`, `?`, `?&`).
    All(Vec<u64>),
    /// Slots having any item (`?|`).
    Any(Vec<u64>),
}

impl GinIndex {
    pub fn new(decl: IndexDecl, column: u32, opclass: GinOpclass) -> GinIndex {
        GinIndex {
            decl,
            column,
            opclass,
            shards: (0..SHARDS).map(|_| RwLock::new(HashMap::with_hasher(RandomState::new()))).collect(),
            present: RwLock::new(Vec::new()),
            unkeyed: RwLock::new(Vec::new()),
            entries: AtomicU64::new(0),
            forgotten: AtomicU64::new(0),
        }
    }

    #[inline]
    fn shard(item: u64) -> usize {
        // The items are already hashes; their low bits spread evenly.
        (item as usize) % SHARDS
    }

    fn set_present(&self, slot: u64, on: bool) {
        let w = (slot / 64) as usize;
        let mut bits = self.present.write();
        if bits.len() <= w {
            if !on {
                return;
            }
            bits.resize((w + 1).next_power_of_two(), 0);
        }
        if on {
            bits[w] |= 1 << (slot % 64);
        } else {
            bits[w] &= !(1 << (slot % 64));
        }
    }

    pub fn holds(&self, slot: u64) -> bool {
        let w = (slot / 64) as usize;
        self.present.read().get(w).is_some_and(|word| word & (1 << (slot % 64)) != 0)
    }

    /// Indexes the document a version holds (its bytes; empty for `NULL`).
    pub fn insert(&self, slot: u64, document: Option<&[u8]>) {
        let mut items = Vec::new();
        match document.map(|d| jsonb::gin_items(d, self.opclass, &mut items)) {
            Some(Some(())) => {}
            // Unreadable: a candidate for every lookup, never a missed row.
            _ => {
                let mut unkeyed = self.unkeyed.write();
                if !unkeyed.contains(&slot) {
                    unkeyed.push(slot);
                }
                drop(unkeyed);
                self.set_present(slot, true);
                return;
            }
        }
        items.sort_unstable();
        items.dedup();
        let mut added = 0u64;
        for item in items {
            let mut shard = self.shards[Self::shard(item)].write();
            let list = shard.entry(item).or_default();
            // Slots arrive in ascending order almost always: a new version
            // is appended. Out of order only for concurrent writers and
            // in-place overwrites.
            match list.last() {
                Some(&last) if last < slot => list.push(slot),
                None => list.push(slot),
                _ => match list.binary_search(&slot) {
                    Ok(_) => continue,
                    Err(at) => list.insert(at, slot),
                },
            }
            added += 1;
        }
        self.entries.fetch_add(added, Ordering::Relaxed);
        self.set_present(slot, true);
    }

    /// Forgets a reclaimed slot. See the module notes on why the lists keep
    /// it until a compaction.
    pub fn forget(&self, slot: u64) {
        if !self.holds(slot) {
            return;
        }
        self.unkeyed.write().retain(|s| *s != slot);
        self.set_present(slot, false);
        let forgotten = self.forgotten.fetch_add(1, Ordering::Relaxed) + 1;
        // Each forgotten slot averages a handful of entries; compact once
        // they are a sizeable share of the lists.
        if forgotten.saturating_mul(8) > self.entries.load(Ordering::Relaxed).max(1024) {
            self.compact();
        }
    }

    /// Drops every entry of a forgotten slot from every list.
    pub fn compact(&self) {
        let present = self.present.read().clone();
        let live = |s: u64| present.get((s / 64) as usize).is_some_and(|w| w & (1 << (s % 64)) != 0);
        let mut kept = 0u64;
        for shard in self.shards.iter() {
            let mut shard = shard.write();
            shard.retain(|_, list| {
                list.retain(|s| live(*s));
                kept += list.len() as u64;
                !list.is_empty()
            });
        }
        self.entries.store(kept, Ordering::Relaxed);
        self.forgotten.store(0, Ordering::Relaxed);
    }

    pub fn clear(&self) {
        for shard in self.shards.iter() {
            shard.write().clear();
        }
        self.present.write().clear();
        self.unkeyed.write().clear();
        self.entries.store(0, Ordering::Relaxed);
        self.forgotten.store(0, Ordering::Relaxed);
    }

    /// Candidate slots, sorted and distinct, or `None` when the answer would
    /// pass `cap` or the query has no items to narrow by.
    pub fn lookup(&self, query: &GinQuery, cap: usize) -> Option<Vec<u64>> {
        let present = self.present.read();
        let live = |s: u64| present.get((s / 64) as usize).is_some_and(|w| w & (1 << (s % 64)) != 0);
        let mut out = match query {
            GinQuery::All(items) => {
                if items.is_empty() {
                    return None;
                }
                // Shortest list first: every other list only filters it.
                let mut lens: Vec<(usize, u64)> = items
                    .iter()
                    .map(|i| (self.shards[Self::shard(*i)].read().get(i).map_or(0, |l| l.len()), *i))
                    .collect();
                lens.sort_unstable();
                let (_, first) = lens[0];
                let mut candidates: Vec<u64> = match self.shards[Self::shard(first)].read().get(&first) {
                    Some(list) => list.iter().copied().filter(|s| live(*s)).collect(),
                    None => Vec::new(),
                };
                for &(_, item) in &lens[1..] {
                    if candidates.is_empty() {
                        break;
                    }
                    let shard = self.shards[Self::shard(item)].read();
                    match shard.get(&item) {
                        Some(list) => candidates.retain(|s| list.binary_search(s).is_ok()),
                        None => candidates.clear(),
                    }
                }
                if candidates.len() > cap {
                    return None;
                }
                candidates
            }
            GinQuery::Any(items) => {
                if items.is_empty() {
                    return None;
                }
                let mut out = Vec::new();
                for item in items {
                    if let Some(list) = self.shards[Self::shard(*item)].read().get(item) {
                        out.extend(list.iter().copied().filter(|s| live(*s)));
                    }
                    if out.len() > cap {
                        return None;
                    }
                }
                out.sort_unstable();
                out.dedup();
                out
            }
        };
        out.extend(self.unkeyed.read().iter().copied().filter(|s| live(*s)));
        out.sort_unstable();
        out.dedup();
        if out.len() > cap {
            return None;
        }
        Some(out)
    }

    /// An upper bound on what [`lookup`](Self::lookup) would return, from
    /// the lists' lengths alone: the shortest list for `All`, their sum for
    /// `Any`. One hash probe per item, and nothing copied.
    pub fn estimate(&self, query: &GinQuery) -> usize {
        let len = |i: &u64| self.shards[Self::shard(*i)].read().get(i).map_or(0, |l| l.len());
        let unkeyed = self.unkeyed.read().len();
        match query {
            GinQuery::All(items) if items.is_empty() => usize::MAX,
            GinQuery::All(items) => items.iter().map(len).min().unwrap_or(0) + unkeyed,
            GinQuery::Any(items) if items.is_empty() => usize::MAX,
            GinQuery::Any(items) => items.iter().map(len).fold(unkeyed, usize::saturating_add),
        }
    }

    /// Distinct items, and entries under them.
    pub fn sizes(&self) -> (usize, usize) {
        self.shards.iter().fold((0, 0), |(k, e), s| {
            let s = s.read();
            (k + s.len(), e + s.values().map(|v| v.len()).sum::<usize>())
        })
    }
}

/// The items a `@>` query document asks for, or `None` when the literal is
/// not a document.
pub fn contains_query(literal: &[u8], opclass: GinOpclass) -> Option<GinQuery> {
    let mut items = Vec::new();
    jsonb::gin_items(literal, opclass, &mut items)?;
    items.sort_unstable();
    items.dedup();
    Some(GinQuery::All(items))
}

/// The items `?` (one key), `?&` (`all`) or `?|` asks for.
pub fn keys_query(keys: &[String], all: bool) -> GinQuery {
    let mut items: Vec<u64> = keys.iter().map(|k| jsonb::gin_key_item(k.as_bytes())).collect();
    items.sort_unstable();
    items.dedup();
    if all { GinQuery::All(items) } else { GinQuery::Any(items) }
}

// ---------------------------------------------------------------------------
// planning
// ---------------------------------------------------------------------------

use crate::core::db_type::DbType;
use crate::core::tokenizer::query_tokenizer::Token;

/// A `WHERE` clause as far as an inverted index can read it: its JSON
/// operators under the clause's `AND`/`OR` structure, everything else opaque.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonExpr {
    /// Not something an inverted index can narrow.
    Opaque,
    /// `doc @> 'lit'`, `doc ? 'k'`, `doc ?| …`, `doc ?& …`.
    Leaf { column: u32, op: JsonOp },
    And(Vec<JsonExpr>),
    Or(Vec<JsonExpr>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum JsonOp {
    /// The encoded right-hand document.
    Contains(Vec<u8>),
    /// `?` (one key), `?&` (`all`) or `?|` (`!all`).
    Keys { keys: Vec<String>, all: bool },
}

/// Reads the inverted-index predicates out of a tokenized `WHERE` clause.
/// Same grammar as `sarg`'s extractor: `AND` binds tighter than `OR`,
/// parentheses group, and the tokenizer has no `NOT`. `None` when the
/// structure could not be read.
pub fn parse_json_where(tokens: &[Token]) -> Option<JsonExpr> {
    struct P<'a> {
        toks: &'a [Token],
        pos: usize,
        bad: bool,
    }
    impl<'a> P<'a> {
        fn next_is(&self, kind: &str) -> bool {
            matches!(self.toks.get(self.pos), Some(Token::Next(k)) if k.eq_ignore_ascii_case(kind))
        }
        fn or(&mut self) -> JsonExpr {
            let mut branches = vec![self.and()];
            while self.next_is("or") {
                self.pos += 1;
                branches.push(self.and());
            }
            if branches.len() == 1 { branches.pop().unwrap() } else { JsonExpr::Or(branches) }
        }
        fn and(&mut self) -> JsonExpr {
            let mut branches = vec![self.atom()];
            while self.next_is("and") {
                self.pos += 1;
                branches.push(self.atom());
            }
            if branches.len() == 1 { branches.pop().unwrap() } else { JsonExpr::And(branches) }
        }
        fn skip_to_separator(&mut self) {
            let mut depth = 0usize;
            while let Some(t) = self.toks.get(self.pos) {
                match t {
                    Token::LPar => depth += 1,
                    Token::RPar => {
                        if depth == 0 {
                            return;
                        }
                        depth -= 1;
                    }
                    Token::Next(_) if depth == 0 => return,
                    _ => {}
                }
                self.pos += 1;
            }
        }
        fn atom(&mut self) -> JsonExpr {
            if matches!(self.toks.get(self.pos), Some(Token::LPar)) {
                let open = self.pos;
                self.pos += 1;
                let inner = self.or();
                match self.toks.get(self.pos) {
                    Some(Token::RPar) => self.pos += 1,
                    _ => {
                        self.bad = true;
                        return JsonExpr::Opaque;
                    }
                }
                // A parenthesised *operand* (`(a + 1) = 5`), not a group.
                if matches!(self.toks.get(self.pos), Some(Token::Op(_))) {
                    self.pos = open;
                    self.skip_to_separator();
                    return JsonExpr::Opaque;
                }
                return inner;
            }
            let start = self.pos;
            self.skip_to_separator();
            leaf(&self.toks[start..self.pos])
        }
    }
    if tokens.is_empty() {
        return None;
    }
    let mut p = P { toks: tokens, pos: 0, bad: false };
    let root = p.or();
    if p.bad || p.pos != tokens.len() {
        return None;
    }
    Some(root)
}

/// One `[jsonb column, operator, literal]` conjunct, or opaque.
fn leaf(toks: &[Token]) -> JsonExpr {
    let [Token::Ident((_, DbType::JSONB, column)), Token::Op(op), Token::StringLit(lit)] = toks else {
        return JsonExpr::Opaque;
    };
    let column = *column as u32;
    let op = match op.as_str() {
        // Encoded exactly as the evaluator encodes the literal
        // (`value_codec::encode_literal` for JSONB is `jsonb::encode_text`),
        // so the items are the items of the document it compares against.
        "@>" => match jsonb::encode_text(lit) {
            Ok(bytes) => JsonOp::Contains(bytes),
            Err(_) => return JsonExpr::Opaque,
        },
        "?" => JsonOp::Keys { keys: vec![lit.clone()], all: true },
        "?|" | "?&" => {
            // The rewrite hands the key list over as one `'{"a","b"}'`
            // literal; a NULL element matches nothing, as in PostgreSQL.
            let Some(elements) = crate::core::tokenizer::predicate_rewrite::text_array_elements(lit) else {
                return JsonExpr::Opaque;
            };
            let all = op == "?&";
            if all && elements.iter().any(|e| e.is_none()) {
                return JsonExpr::Opaque;
            }
            JsonOp::Keys {
                keys: elements.into_iter().flatten().collect(),
                all,
            }
        }
        _ => return JsonExpr::Opaque,
    };
    JsonExpr::Leaf { column, op }
}

impl GinIndex {
    /// Whether this index can answer `op`: `jsonb_path_ops` keeps no key
    /// items, so it answers containment only.
    pub fn answers(&self, op: &JsonOp) -> bool {
        match op {
            JsonOp::Contains(_) => true,
            JsonOp::Keys { .. } => self.opclass == GinOpclass::Ops,
        }
    }

    pub fn query_for(&self, op: &JsonOp) -> Option<GinQuery> {
        match op {
            JsonOp::Contains(literal) => contains_query(literal, self.opclass),
            JsonOp::Keys { keys, all } => Some(keys_query(keys, *all)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decl(name: &str) -> IndexDecl {
        IndexDecl {
            name: name.into(),
            method: crate::core::index::Method::GinOps,
            unique: false,
            columns: vec![0],
            is_deleted: false,
        }
    }

    fn doc(t: &str) -> Vec<u8> {
        jsonb::encode_text(t).unwrap()
    }

    #[test]
    fn lookups_find_every_containing_document() {
        for opclass in [GinOpclass::Ops, GinOpclass::PathOps] {
            let g = GinIndex::new(decl("g"), 0, opclass);
            let docs = [
                r#"{"a":1,"b":{"c":"x"}}"#,
                r#"{"a":1.0,"tags":["t1","t2"]}"#,
                r#"{"a":2,"b":{"c":"y"}}"#,
                r#"[1,2,"k"]"#,
                r#""k""#,
            ];
            for (slot, d) in docs.iter().enumerate() {
                g.insert(slot as u64, Some(&doc(d)));
            }
            g.insert(5, None); // unreadable: always a candidate
            g.insert(6, Some(&[])); // NULL: never
            let ask = |q: &str| g.lookup(&contains_query(&doc(q), opclass).unwrap(), usize::MAX);
            for (q, want) in [
                (r#"{"a":1}"#, vec![0, 1]),
                (r#"{"b":{"c":"x"}}"#, vec![0]),
                (r#"{"tags":["t2"]}"#, vec![1]),
                (r#"2"#, vec![3]),
            ] {
                let got = ask(q).unwrap();
                for w in &want {
                    assert!(got.contains(w), "{opclass:?}: {q} missed slot {w}, got {got:?}");
                }
                assert!(got.contains(&5), "the unreadable slot is a candidate");
                assert!(!got.contains(&6), "NULL contains nothing");
            }
            // Nothing to narrow by: declined, not answered with everything.
            assert_eq!(ask("{}"), None);
            // A forgotten slot stops being returned, compaction or not.
            g.forget(0);
            assert!(!ask(r#"{"a":1}"#).unwrap().contains(&0));
            g.compact();
            assert!(!ask(r#"{"a":1}"#).unwrap().contains(&0));
            assert!(ask(r#"{"a":1}"#).unwrap().contains(&1));
        }
        // `?` under jsonb_ops: top-level keys, string elements, a bare string.
        let g = GinIndex::new(decl("k"), 0, GinOpclass::Ops);
        g.insert(1, Some(&doc(r#"{"k":1}"#)));
        g.insert(2, Some(&doc(r#"["k"]"#)));
        g.insert(3, Some(&doc(r#""k""#)));
        g.insert(4, Some(&doc(r#"{"other":1}"#)));
        let got = g.lookup(&keys_query(&["k".into()], true), usize::MAX).unwrap();
        assert_eq!(got, vec![1, 2, 3]);
        let any = g.lookup(&keys_query(&["k".into(), "other".into()], false), usize::MAX).unwrap();
        assert_eq!(any, vec![1, 2, 3, 4]);
        let all = g.lookup(&keys_query(&["k".into(), "other".into()], true), usize::MAX).unwrap();
        assert!(all.is_empty());
    }

    #[test]
    fn the_where_structure_is_read_like_the_filters() {
        use crate::core::tokenizer::query_tokenizer::Token as T;
        let doc = T::Ident(("doc".into(), DbType::JSONB, 3));
        let other = T::Ident(("n".into(), DbType::I64, 1));
        let and = || T::Next("AND".into());
        let or = || T::Next("OR".into());
        let contains = |lit: &str| vec![doc.clone(), T::Op("@>".into()), T::StringLit(lit.into())];
        // a = 1 AND doc @> '{"x":1}'
        let mut toks = vec![other.clone(), T::Op("=".into()), T::Number(crate::core::tokenizer::query_tokenizer::NumericValue::I64(1)), and()];
        toks.extend(contains(r#"{"x":1}"#));
        let e = parse_json_where(&toks).unwrap();
        assert!(matches!(&e, JsonExpr::And(v) if v.len() == 2 && v[0] == JsonExpr::Opaque && matches!(v[1], JsonExpr::Leaf { column: 3, .. })));
        // doc @> 'x' OR n = 1: one opaque branch, so the OR cannot be answered.
        let mut toks = contains(r#"{"x":1}"#);
        toks.push(or());
        toks.extend([other.clone(), T::Op("=".into()), T::Number(crate::core::tokenizer::query_tokenizer::NumericValue::I64(1))]);
        assert!(matches!(parse_json_where(&toks).unwrap(), JsonExpr::Or(_)));
        // Not a document: opaque, never a guess.
        assert_eq!(parse_json_where(&contains("{not json")).unwrap(), JsonExpr::Opaque);
        // Unbalanced: declined.
        let mut toks = vec![T::LPar];
        toks.extend(contains("{}"));
        assert_eq!(parse_json_where(&toks), None);
    }

    /// The planner asks the index with the smallest estimate first and takes
    /// its answer, so an estimate below the answer would only misorder, but
    /// an estimate that is not a bound makes the order meaningless.
    #[test]
    fn an_estimate_bounds_its_lookup() {
        for opclass in [GinOpclass::Ops, GinOpclass::PathOps] {
            let g = GinIndex::new(decl("e"), 0, opclass);
            for slot in 0..300u64 {
                let d = format!(
                    r#"{{"user":{{"id":{}}},"status":"s{}","tags":["t{}"]}}"#,
                    slot % 37,
                    slot % 5,
                    slot % 3
                );
                g.insert(slot, Some(&doc(&d)));
            }
            g.insert(300, None);
            g.forget(7);
            for q in [r#"{"user":{"id":3}}"#, r#"{"status":"s1","tags":["t2"]}"#, r#"{"user":{}}"#, r#"{"nope":1}"#] {
                let query = contains_query(&doc(q), opclass).unwrap();
                let got = g.lookup(&query, usize::MAX).map_or(0, |a| a.len());
                assert!(g.estimate(&query) >= got, "{opclass:?} {q}: estimate {} < {got}", g.estimate(&query));
            }
            if opclass == GinOpclass::Ops {
                let keys = ["status".to_string(), "nope".to_string()];
                for all in [true, false] {
                    let query = keys_query(&keys, all);
                    let got = g.lookup(&query, usize::MAX).map_or(0, |a| a.len());
                    assert!(g.estimate(&query) >= got, "keys all={all}");
                }
            }
            // `{"user":{"id":3}}` under path ops is one list: its estimate is
            // the answer's size, which is what lets it win over `jsonb_ops`.
            if opclass == GinOpclass::PathOps {
                let query = contains_query(&doc(r#"{"user":{"id":3}}"#), opclass).unwrap();
                assert_eq!(g.estimate(&query), 9 + 1, "nine documents and the unreadable one");
            }
        }
    }
}
