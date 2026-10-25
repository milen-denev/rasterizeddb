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
//! slots are a quarter of the slots the lists hold.
//!
//! A compaction removes only slots it was told were forgotten, never every
//! slot whose presence bit is clear: an insert writes a new slot's entries
//! before it sets the slot's bit, and a compaction in between would take
//! that slot off its lists for good.
//!
//! An in-place overwrite adds the new document's items and leaves the old
//! ones: a stale item on a live slot costs a candidate, never an answer.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::RandomState;
use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;

use crate::core::index::IndexDecl;
use crate::core::index::sorted::{SORTED_BLOCK, Sorted};
use crate::core::index_store::gate::Proof;
use crate::core::jsonb::{self, GinOpclass};

const SHARDS: usize = 64;

/// The version of how a document becomes items: [`jsonb::gin_items`] and
/// [`jsonb::gin_key_item`], meaning the item tags, the number
/// canonicalisation in `hash_scalar`, `jsonb_path_ops` folding, the seed and
/// the byte feed. A persisted inverted index records it and is rebuilt when
/// it differs. Bump it with any change that moves one item: the item vectors
/// in `jsonb`'s tests fail on such a change; freeze the new items as the next
/// set, raise this and append that set's row, leaving the old set and row as
/// they are. Vectors that only add coverage go in the `*_ADDED` tables there,
/// with no bump.
///
/// 1: items under XXH64 (`GIN_HASHER_ID` 1). 2: the same byte feed under
/// rapidhash V3 (`GIN_HASHER_ID` 2).
pub const ITEM_DERIVE_VERSION: u32 = 2;

/// The hash under the items, in memory and on disk alike, since a lookup
/// probes every structure with one set of query items. 2: the in-tree
/// rapidhash, frozen at upstream `rapidhash_v3` (`core::hash::rapidhash`),
/// fed through `ItemFeed::rapid`. 1 was the in-tree XXH64, which everything
/// else persisted or routed still uses: an item is about forty bytes hashed
/// once per document and query, where XXH64's serial multiplies made builds
/// 22-25% slower than ahash, and a GIN index rechecks every candidate, so a
/// weaker hash costs only false positives. Raised with `ITEM_DERIVE_VERSION`.
pub const GIN_HASHER_ID: u32 = 2;

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
    /// Entries in the lists, and how many slots were forgotten since the
    /// last compaction. A forgotten slot is on a list at most once, so a
    /// list's length less `forgotten` bounds the live slots on it.
    entries: AtomicU64,
    forgotten: AtomicU64,
    /// The slots `forgotten` counts: what the next compaction removes.
    forgotten_slots: Mutex<Vec<u64>>,
    /// Set bits in `present`, kept under its lock: what a list is measured
    /// against to tell whether it filters anything at all.
    live: AtomicU64,
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
            forgotten_slots: Mutex::new(Vec::new()),
            live: AtomicU64::new(0),
        }
    }

    /// What decides this index's entries, for `TableIndexes::shape_hash`.
    /// Destructured whole, so a new field does not compile until it is placed
    /// on one side or the other; the rest is the entries themselves.
    pub(super) fn shape(&self) -> (&IndexDecl, u32, GinOpclass) {
        let GinIndex {
            decl,
            column,
            opclass,
            shards: _,
            present: _,
            unkeyed: _,
            entries: _,
            forgotten: _,
            forgotten_slots: _,
            live: _,
        } = self;
        (decl, *column, *opclass)
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
        let was = bits[w] & (1 << (slot % 64)) != 0;
        if on {
            bits[w] |= 1 << (slot % 64);
        } else {
            bits[w] &= !(1 << (slot % 64));
        }
        if was != on {
            if on {
                self.live.fetch_add(1, Ordering::Relaxed);
            } else {
                self.live.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    pub fn holds(&self, slot: u64) -> bool {
        let w = (slot / 64) as usize;
        self.present.read().get(w).is_some_and(|word| word & (1 << (slot % 64)) != 0)
    }

    /// Indexes the document a version holds (its bytes; empty for `NULL`).
    pub fn insert(&self, _proof: Proof<'_>, slot: u64, document: Option<&[u8]>) {
        crate::core::index_store::census::mutation(_proof, self as *const Self as usize, "GinIndex::insert");
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
    pub fn forget(&self, proof: Proof<'_>, slot: u64) {
        crate::core::index_store::census::mutation(proof, self as *const Self as usize, "GinIndex::forget");
        if !self.holds(slot) {
            return;
        }
        self.unkeyed.write().retain(|s| *s != slot);
        self.set_present(slot, false);
        // Counted before it is listed, so a compaction that takes the slot
        // never subtracts it before it was added: the count stays at least
        // the forgotten entries on any one list, which lookups rely on.
        let forgotten = self.forgotten.fetch_add(1, Ordering::Relaxed) + 1;
        self.forgotten_slots.lock().push(slot);
        // Compact once forgotten slots are a quarter of the slots the lists
        // hold. Counted in slots on both sides: a slot leaves as many
        // entries as its document had items, and a slot count measured
        // against entries never fired for documents of eight items or more,
        // so their lists grew until a rebuild.
        let held = self.live.load(Ordering::Relaxed) + forgotten;
        if forgotten.saturating_mul(4) > held.max(512) {
            self.compact(proof);
        }
    }

    /// Drops every entry of the slots forgotten so far from every list.
    ///
    /// Only those slots: a clear presence bit does not mean forgotten, it is
    /// also a slot whose insert has written its entries and not yet set its
    /// bit, and removing those would drop a live row from every answer.
    pub fn compact(&self, _proof: Proof<'_>) {
        crate::core::index_store::census::mutation(_proof, self as *const Self as usize, "GinIndex::compact");
        let gone = std::mem::take(&mut *self.forgotten_slots.lock());
        let Some(&max) = gone.iter().max() else {
            return;
        };
        let mut bits = vec![0u64; (max / 64) as usize + 1];
        for &s in &gone {
            bits[(s / 64) as usize] |= 1 << (s % 64);
        }
        let mut removed = 0u64;
        for shard in self.shards.iter() {
            let mut shard = shard.write();
            shard.retain(|_, list| {
                let before = list.len();
                list.retain(|s| bits.get((*s / 64) as usize).is_none_or(|w| w & (1 << (s % 64)) == 0));
                removed += (before - list.len()) as u64;
                !list.is_empty()
            });
        }
        // Subtracted, not reset: inserts and forgets that ran beside this
        // were counted and are not part of what it removed. Saturating, as a
        // `clear` beside it may already have zeroed them.
        let sub = |counter: &AtomicU64, n: u64| {
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| Some(c.saturating_sub(n)));
        };
        sub(&self.entries, removed);
        sub(&self.forgotten, gone.len() as u64);
    }

    pub fn clear(&self, _proof: Proof<'_>) {
        crate::core::index_store::census::mutation(_proof, self as *const Self as usize, "GinIndex::clear");
        for shard in self.shards.iter() {
            shard.write().clear();
        }
        let mut present = self.present.write();
        present.clear();
        self.live.store(0, Ordering::Relaxed);
        drop(present);
        self.unkeyed.write().clear();
        self.entries.store(0, Ordering::Relaxed);
        self.forgotten.store(0, Ordering::Relaxed);
        self.forgotten_slots.lock().clear();
    }

    /// Every item's list, then the unkeyed slots, then the presence bits, as
    /// one sorted stream (hook C3, see [`super::sorted`]): what a snapshot's
    /// GIN section holds. Stops at the first error `f` returns, and returns
    /// it.
    ///
    /// The lists go out without the slots in `forgotten_slots`, exactly
    /// those, as a compaction would leave them (owner's invariant 3, N37).
    /// Not fewer: a forgotten slot whose entries went out would come back
    /// from a load with its presence bit clear and no record that it was
    /// forgotten, so no compaction would ever take it off its lists, and
    /// every later snapshot would carry it forward. Not more: a clear
    /// presence bit is also a slot whose insert has written its entries and
    /// not yet set its bit, so filtering by the bit would drop a live row
    /// from every answer after a load (the compaction bug of de3651a). This
    /// relies on slots never being reused until a rebuild renumbers them: a
    /// forgotten slot's entries are all stale.
    ///
    /// Each list is read under its shard's lock and copied out, a block of
    /// [`super::sorted::SORTED_BLOCK`] items at a time, never across `f`; as
    /// for a `LiveIndex`, the stream is one state of the index only while
    /// nothing writes it, which a seal guarantees.
    pub fn for_each_sorted<E>(&self, mut f: impl FnMut(Sorted<'_, u64>) -> Result<(), E>) -> Result<(), E> {
        // As a compaction marks them.
        let gone = self.forgotten_slots.lock().clone();
        let mut bits = vec![0u64; gone.iter().max().map_or(0, |m| (m / 64) as usize + 1)];
        for &s in &gone {
            bits[(s / 64) as usize] |= 1 << (s % 64);
        }
        let kept = |s: &u64| bits.get((*s / 64) as usize).is_none_or(|w| w & (1 << (s % 64)) == 0);
        let mut items: Vec<u64> = Vec::new();
        for shard in self.shards.iter() {
            items.extend(shard.read().keys().copied());
        }
        items.sort_unstable();
        let mut block: Vec<(u64, Vec<u64>)> = Vec::with_capacity(SORTED_BLOCK);
        for chunk in items.chunks(SORTED_BLOCK) {
            block.clear();
            for &item in chunk {
                let list: Vec<u64> = match self.shards[Self::shard(item)].read().get(&item) {
                    Some(list) if gone.is_empty() => list.clone(),
                    Some(list) => list.iter().copied().filter(kept).collect(),
                    None => continue,
                };
                // A list of forgotten slots only is one a compaction drops.
                if !list.is_empty() {
                    block.push((item, list));
                }
            }
            for (item, list) in &block {
                f(Sorted::Entry(*item, list))?;
            }
        }
        // `forget` takes a slot off the unkeyed list itself.
        let mut unkeyed = self.unkeyed.read().clone();
        unkeyed.sort_unstable();
        unkeyed.dedup();
        f(Sorted::Unkeyed(&unkeyed))?;
        let present = self.present.read().clone();
        f(Sorted::Present(super::sorted::trimmed(&present)))
    }

    /// The lists an `All` query walks, as `(length, item)`: the shortest
    /// first, then each other one that removes enough to be worth walking.
    ///
    /// A list holding nearly every live slot (`jsonb_ops`'s key item for
    /// `status`, which every document has) removes almost nothing, and
    /// walking it costs a probe per candidate. Leaving a list out only keeps
    /// candidates it would have removed, so the answer stays a superset.
    ///
    /// A list's length still counts the entries of forgotten slots, and a
    /// `jsonb_ops` index whose documents have many items may go a long time
    /// between compactions, while `live` counts only present slots. So a
    /// list is measured by the live slots it is sure to hold, its length
    /// less `forgotten` (read by the caller before any list), as the
    /// single-list decline in `lookup` does. Measured by raw length, an
    /// update-heavy table's lists outgrow `live` and a list of two fifths of
    /// the table would be left out as if it held all of it.
    fn walked_lists(&self, items: &[u64], forgotten: usize) -> SmallVec<[(usize, u64); 8]> {
        let mut lists: SmallVec<[(usize, u64); 8]> = items
            .iter()
            .map(|i| (self.shards[Self::shard(*i)].read().get(i).map_or(0, |l| l.len()), *i))
            .collect();
        lists.sort_unstable();
        let live = self.live.load(Ordering::Relaxed) as usize;
        let universal = live - live / 10;
        let mut first = true;
        lists.retain(|(len, _)| {
            std::mem::take(&mut first) || live < 64 || len.saturating_sub(forgotten) < universal
        });
        lists
    }

    /// Candidate slots, sorted and distinct, or `None` when the answer would
    /// pass `cap` or the query has no items to narrow by.
    ///
    /// Declines as soon as the answer is known to pass `cap`: before copying
    /// anything when one list decides it, and otherwise the moment the walk
    /// has found more. A broad answer used to be built in full, through a
    /// binary search per candidate into every other list, only to be thrown
    /// away at the end.
    pub fn lookup(&self, query: &GinQuery, cap: usize) -> Option<Vec<u64>> {
        // Read first: a compaction that runs after this only makes the
        // bounds below more conservative, never less.
        let forgotten = self.forgotten.load(Ordering::Relaxed) as usize;
        let present = self.present.read();
        let live = |s: u64| present.get((s / 64) as usize).is_some_and(|w| w & (1 << (s % 64)) != 0);
        let mut out = match query {
            GinQuery::All(items) => {
                if items.is_empty() {
                    return None;
                }
                let lists = self.walked_lists(items, forgotten);
                // A forgotten slot is on a list at most once, so a list holds
                // at least its length less the forgotten count of live slots.
                // With one list those are the answer.
                if lists.len() == 1 && lists[0].0.saturating_sub(forgotten) > cap {
                    return None;
                }
                // Every list is read at once, so their shards are locked in
                // ascending order, each once. Another lookup takes them in
                // the same order and a writer holds one at a time, so no two
                // can wait on each other.
                let mut shards: SmallVec<[usize; 8]> = lists.iter().map(|(_, i)| Self::shard(*i)).collect();
                shards.sort_unstable();
                shards.dedup();
                let guards: SmallVec<[_; 8]> = shards.iter().map(|s| self.shards[*s].read()).collect();
                let walked: SmallVec<[&[u64]; 8]> = lists
                    .iter()
                    .map(|(_, item)| {
                        let at = shards.binary_search(&Self::shard(*item)).expect("locked above");
                        guards[at].get(item).map_or(&[][..], |l| l.as_slice())
                    })
                    .collect();
                // Every list is sorted, so each is walked front to back once,
                // galloping to the next candidate, instead of searched from
                // the start for every one.
                let (shortest, rest) = walked.split_first().expect("an item");
                let mut at: SmallVec<[usize; 8]> = smallvec::smallvec![0; rest.len()];
                let mut out = Vec::new();
                'slots: for &s in shortest.iter() {
                    if !live(s) {
                        continue;
                    }
                    for (k, list) in rest.iter().enumerate() {
                        at[k] = super::gallop_to(list, at[k], s);
                        if list.get(at[k]) != Some(&s) {
                            continue 'slots;
                        }
                    }
                    out.push(s);
                    if out.len() > cap {
                        return None;
                    }
                }
                out
            }
            GinQuery::Any(items) => {
                if items.is_empty() {
                    return None;
                }
                let mut out = Vec::new();
                for item in items {
                    if let Some(list) = self.shards[Self::shard(*item)].read().get(item) {
                        // Every live slot of every list is in the answer.
                        if list.len().saturating_sub(forgotten) > cap {
                            return None;
                        }
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

    /// What [`lookup`](Self::lookup) answers, restricted to slots below
    /// `n_slots`, as one bit per slot: for an answer too broad for a slot
    /// list that still leaves most of the table out. `None` when it would
    /// name more than `limit` slots, or the query has no items.
    ///
    /// `n_slots` is the table length the caller sampled before asking. A
    /// listed slot at or past it is left out here because the caller's
    /// bitmap counts every such slot as a candidate anyway.
    ///
    /// Built a list at a time, each into its own bits, and combined a word
    /// at a time: no list is searched, and the presence bits drop every
    /// forgotten slot in one pass at the end.
    pub fn lookup_dense(&self, query: &GinQuery, n_slots: u64, limit: usize) -> Option<Vec<u64>> {
        let forgotten = self.forgotten.load(Ordering::Relaxed) as usize;
        let present = self.present.read();
        let n_words = crate::core::filter::bitmap_words(n_slots);
        // The slots of a sorted list below `n_slots`, as bits, and how many.
        let set = |list: &[u64], into: &mut [u64]| -> usize {
            let below = list.partition_point(|s| *s < n_slots);
            for &s in &list[..below] {
                into[(s / 64) as usize] |= 1 << (s % 64);
            }
            below
        };
        let mut words = vec![0u64; n_words];
        match query {
            GinQuery::All(items) => {
                if items.is_empty() {
                    return None;
                }
                let lists = self.walked_lists(items, forgotten);
                let (_, shortest) = lists[0];
                if let Some(list) = self.shards[Self::shard(shortest)].read().get(&shortest) {
                    let listed = set(list, &mut words);
                    // As in `lookup`: one list's live slots are the answer.
                    if lists.len() == 1 && listed.saturating_sub(forgotten) > limit {
                        return None;
                    }
                }
                let mut bits = Vec::new();
                for &(_, item) in &lists[1..] {
                    bits.clear();
                    bits.resize(n_words, 0u64);
                    if let Some(list) = self.shards[Self::shard(item)].read().get(&item) {
                        set(list, &mut bits);
                    }
                    for (w, b) in words.iter_mut().zip(&bits) {
                        *w &= *b;
                    }
                }
            }
            GinQuery::Any(items) => {
                if items.is_empty() {
                    return None;
                }
                for item in items {
                    if let Some(list) = self.shards[Self::shard(*item)].read().get(item) {
                        if set(list, &mut words).saturating_sub(forgotten) > limit {
                            return None;
                        }
                    }
                }
            }
        }
        // A forgotten slot stays on its lists until a compaction. Its
        // presence bit is clear, and a word past `present` holds no slot this
        // index has.
        for (i, w) in words.iter_mut().enumerate() {
            *w &= present.get(i).copied().unwrap_or(0);
        }
        // Unkeyed slots are candidates for every lookup, and are live
        // exactly while their presence bit is set.
        for &s in self.unkeyed.read().iter() {
            let w = (s / 64) as usize;
            if s < n_slots && present.get(w).is_some_and(|p| p & (1 << (s % 64)) != 0) {
                words[w] |= 1 << (s % 64);
            }
        }
        let count: usize = words.iter().map(|w| w.count_ones() as usize).sum();
        if count > limit {
            return None;
        }
        Some(words)
    }

    /// An upper bound on what [`lookup`](Self::lookup) would return, from
    /// the lists' lengths alone: the shortest list for `All`, their sum for
    /// `Any`. One hash probe per item, and nothing copied.
    pub fn estimate(&self, query: &GinQuery) -> usize {
        self.estimate_and_cost(query).0
    }

    /// [`estimate`](Self::estimate), and the sum of the query's list
    /// lengths: what the planner orders indexes by. Two indexes often bound
    /// the answer equally (`{"status":"paid"}` is one list of a fifth of the
    /// table under `jsonb_path_ops`, and under `jsonb_ops` that value's list
    /// beside the `status` key list every document is on). The one with less
    /// to read is the cheaper to ask, and its lists name fewer extras.
    pub fn estimate_and_cost(&self, query: &GinQuery) -> (usize, usize) {
        let len = |i: &u64| self.shards[Self::shard(*i)].read().get(i).map_or(0, |l| l.len());
        let unkeyed = self.unkeyed.read().len();
        let items = match query {
            GinQuery::All(items) | GinQuery::Any(items) if items.is_empty() => return (usize::MAX, usize::MAX),
            GinQuery::All(items) | GinQuery::Any(items) => items,
        };
        let lens: SmallVec<[usize; 8]> = items.iter().map(len).collect();
        let cost = lens.iter().fold(unkeyed, |a, b| a.saturating_add(*b));
        let estimate = match query {
            GinQuery::All(_) => lens.iter().copied().min().unwrap_or(0) + unkeyed,
            GinQuery::Any(_) => cost,
        };
        (estimate, cost)
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
    use crate::core::index_store::gate::test_mint;

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
        let ticket = test_mint();
        let proof = Proof::from(&ticket);
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
                g.insert(proof, slot as u64, Some(&doc(d)));
            }
            g.insert(proof, 5, None); // unreadable: always a candidate
            g.insert(proof, 6, Some(&[])); // NULL: never
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
            g.forget(proof, 0);
            assert!(!ask(r#"{"a":1}"#).unwrap().contains(&0));
            g.compact(proof);
            assert!(!ask(r#"{"a":1}"#).unwrap().contains(&0));
            assert!(ask(r#"{"a":1}"#).unwrap().contains(&1));
        }
        // `?` under jsonb_ops: top-level keys, string elements, a bare string.
        let g = GinIndex::new(decl("k"), 0, GinOpclass::Ops);
        g.insert(proof, 1, Some(&doc(r#"{"k":1}"#)));
        g.insert(proof, 2, Some(&doc(r#"["k"]"#)));
        g.insert(proof, 3, Some(&doc(r#""k""#)));
        g.insert(proof, 4, Some(&doc(r#"{"other":1}"#)));
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
        let ticket = test_mint();
        let proof = Proof::from(&ticket);
        for opclass in [GinOpclass::Ops, GinOpclass::PathOps] {
            let g = GinIndex::new(decl("e"), 0, opclass);
            for slot in 0..300u64 {
                let d = format!(
                    r#"{{"user":{{"id":{}}},"status":"s{}","tags":["t{}"]}}"#,
                    slot % 37,
                    slot % 5,
                    slot % 3
                );
                g.insert(proof, slot, Some(&doc(&d)));
            }
            g.insert(proof, 300, None);
            g.forget(proof, 7);
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

    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    /// An index over `n` random documents, most of them objects with a
    /// `status` key (so its key item is on nearly every list), with some
    /// unreadable, some `NULL`, and some forgotten. Returns the documents
    /// by slot, `None` where nothing can match.
    fn random_index(r: &mut Rng, opclass: GinOpclass, n: u64) -> (GinIndex, Vec<Option<Vec<u8>>>) {
        let ticket = test_mint();
        let proof = Proof::from(&ticket);
        let g = GinIndex::new(decl("r"), 0, opclass);
        let mut docs = Vec::new();
        for slot in 0..n {
            let d = match r.below(40) {
                0 => {
                    g.insert(proof, slot, None);
                    docs.push(None);
                    continue;
                }
                1 => {
                    g.insert(proof, slot, Some(&[]));
                    docs.push(None);
                    continue;
                }
                2 => format!(r#"["t{}","t{}"]"#, r.below(6), r.below(6)),
                _ => format!(
                    r#"{{"status":"s{}","cat":{},"tags":["t{}"],"user":{{"id":{}}}}}"#,
                    r.below(5),
                    r.below(10),
                    r.below(6),
                    r.below(7)
                ),
            };
            let bytes = doc(&d);
            g.insert(proof, slot, Some(&bytes));
            docs.push(Some(bytes));
        }
        for _ in 0..n / 20 {
            let slot = r.below(n);
            g.forget(proof, slot);
            docs[slot as usize] = None;
        }
        (g, docs)
    }

    fn random_query(r: &mut Rng, opclass: GinOpclass) -> GinQuery {
        let q = match r.below(6) {
            0 => format!(r#"{{"status":"s{}"}}"#, r.below(5)),
            1 => format!(r#"{{"status":"s{}","tags":["t{}"]}}"#, r.below(5), r.below(6)),
            2 => format!(r#"{{"cat":{},"user":{{"id":{}}}}}"#, r.below(10), r.below(7)),
            3 => format!(r#"["t{}"]"#, r.below(6)),
            4 if opclass == GinOpclass::Ops => {
                let keys = ["status".to_string(), format!("t{}", r.below(6)), "nope".to_string()];
                return keys_query(&keys[..1 + r.below(3) as usize], r.below(2) == 0);
            }
            _ => format!(r#"{{"tags":["t{}"],"user":{{}}}}"#, r.below(6)),
        };
        contains_query(&doc(&q), opclass).unwrap()
    }

    /// The walk gallops through every list at once and leaves out lists
    /// that hold nearly every slot. Over the lists it walks, it must keep
    /// exactly what a binary search per candidate kept, and every document
    /// that contains the query must be among what it keeps.
    #[test]
    fn the_walk_keeps_what_a_search_per_candidate_keeps() {
        let mut r = Rng(0x9e37_79b9);
        for round in 0..40 {
            let opclass = if round % 2 == 0 { GinOpclass::Ops } else { GinOpclass::PathOps };
            let n = 200 + r.below(400);
            let (g, docs) = random_index(&mut r, opclass, n);
            let live = |s: u64| g.holds(s);
            for _ in 0..30 {
                let query = random_query(&mut r, opclass);
                let got = g.lookup(&query, usize::MAX);
                let GinQuery::All(items) = &query else {
                    continue;
                };
                // The old walk, over the lists the new one walks.
                let lists = g.walked_lists(items, g.forgotten.load(Ordering::Relaxed) as usize);
                let list = |i: u64| g.shards[GinIndex::shard(i)].read().get(&i).cloned().unwrap_or_default();
                let mut want: Vec<u64> = list(lists[0].1).into_iter().filter(|s| live(*s)).collect();
                for &(_, item) in &lists[1..] {
                    let l = list(item);
                    want.retain(|s| l.binary_search(s).is_ok());
                }
                want.extend(g.unkeyed.read().iter().copied().filter(|s| live(*s)));
                want.sort_unstable();
                want.dedup();
                assert_eq!(got.as_ref(), Some(&want), "{opclass:?} {query:?}");
                // And never short of a document having every item, whatever
                // lists were left out.
                let got = got.unwrap();
                for (slot, d) in docs.iter().enumerate() {
                    let Some(d) = d else { continue };
                    let mut have = Vec::new();
                    jsonb::gin_items(d, opclass, &mut have).unwrap();
                    if items.iter().all(|i| have.contains(i)) {
                        assert!(got.contains(&(slot as u64)), "{opclass:?} {query:?} dropped slot {slot}");
                    }
                }
            }
        }
    }

    /// A bitmap answer is the slot-list answer below `n_slots`, unkeyed and
    /// forgotten slots included as the list has them, and nothing at or
    /// past `n_slots`, which the caller's bitmap counts as candidates.
    #[test]
    fn a_dense_answer_is_the_slot_list_below_n_slots() {
        let mut r = Rng(0x0dd_ba11);
        for round in 0..40 {
            let opclass = if round % 2 == 0 { GinOpclass::Ops } else { GinOpclass::PathOps };
            let n = 200 + r.below(400);
            let (g, _) = random_index(&mut r, opclass, n);
            for _ in 0..30 {
                let query = random_query(&mut r, opclass);
                // Below, at and past what the index holds.
                let n_slots = match r.below(3) {
                    0 => r.below(n),
                    1 => n,
                    _ => n + 1 + r.below(200),
                };
                let sparse = g.lookup(&query, usize::MAX).unwrap();
                let words = g.lookup_dense(&query, n_slots, usize::MAX).unwrap();
                assert_eq!(words.len(), crate::core::filter::bitmap_words(n_slots));
                let mut from_words = Vec::new();
                for (w, word) in words.iter().enumerate() {
                    let mut bits = *word;
                    while bits != 0 {
                        from_words.push(w as u64 * 64 + bits.trailing_zeros() as u64);
                        bits &= bits - 1;
                    }
                }
                let want: Vec<u64> = sparse.into_iter().filter(|s| *s < n_slots).collect();
                assert_eq!(from_words, want, "{opclass:?} {query:?} n_slots {n_slots}");
                // Past `n_slots` the bitmap admits everything.
                let bitmap = crate::core::filter::CandidateBitmap::dense(words, n_slots);
                for s in n_slots..n_slots + 70 {
                    assert!(bitmap.get(s), "appended slot {s} excluded");
                }
            }
        }
    }

    /// Both lookups decline exactly when the answer passes their bound, and
    /// answer when it does not, wherever in the walk that is decided.
    #[test]
    fn broad_answers_are_declined_at_their_bound() {
        let mut r = Rng(0xb0_0b5);
        for round in 0..20 {
            let opclass = if round % 2 == 0 { GinOpclass::Ops } else { GinOpclass::PathOps };
            let n = 300 + r.below(300);
            let (g, _) = random_index(&mut r, opclass, n);
            for _ in 0..30 {
                let query = random_query(&mut r, opclass);
                let size = g.lookup(&query, usize::MAX).unwrap().len();
                match &query {
                    GinQuery::All(_) => {
                        assert_eq!(g.lookup(&query, size).map(|a| a.len()), Some(size), "{query:?} at its size")
                    }
                    // A union is counted before its duplicates go, so it may
                    // decline early; declining is always allowed.
                    GinQuery::Any(_) => assert!(g.lookup(&query, size).is_none_or(|a| a.len() == size)),
                }
                if size > 0 {
                    assert_eq!(g.lookup(&query, size - 1), None, "{query:?} below its size");
                    assert!(g.lookup_dense(&query, n, size - 1).is_none(), "{query:?} dense below its size");
                }
                assert!(g.lookup_dense(&query, n, size).is_some(), "{query:?} dense at its size");
            }
        }
    }

    /// An update-heavy table: the old versions were forgotten, and are
    /// fewer than a quarter of the slots, so no compaction has run and the
    /// lists still hold them. `paid` and `eu` each hold seven tenths of the
    /// live rows, but by raw length more than nine tenths of `live`. They
    /// must still be walked as filters, and only the key lists every
    /// document is on left out, or the answer is the whole `paid` list and
    /// passes a cap the intersection fits under.
    #[test]
    fn forgotten_entries_do_not_make_a_list_look_universal() {
        let ticket = test_mint();
        let proof = Proof::from(&ticket);
        for opclass in [GinOpclass::Ops, GinOpclass::PathOps] {
            let g = GinIndex::new(decl("u"), 0, opclass);
            let d = |status: &str, region: &str, n: u64| {
                doc(&format!(
                    r#"{{"status":"{status}","region":"{region}","user":{{"id":{n}}},"tags":["x","y"],"n":{n}}}"#
                ))
            };
            // 300 old versions, all paid in eu.
            for slot in 0..300u64 {
                g.insert(proof, slot, Some(&d("paid", "eu", slot)));
            }
            // 1000 live rows: paid where slot % 10 < 7, eu where >= 3.
            let mut want = Vec::new();
            for slot in 300..1300u64 {
                let paid = slot % 10 < 7;
                let eu = slot % 10 >= 3;
                g.insert(proof, slot, Some(&d(if paid { "paid" } else { "open" }, if eu { "eu" } else { "us" }, slot)));
                if paid && eu {
                    want.push(slot);
                }
            }
            assert_eq!(want.len(), 400);
            // Vacuum forgets the old versions.
            for slot in 0..300u64 {
                g.forget(proof, slot);
            }
            assert_eq!(g.forgotten.load(Ordering::Relaxed), 300, "no compaction ran");
            let query = contains_query(&doc(r#"{"status":"paid","region":"eu"}"#), opclass).unwrap();
            let GinQuery::All(items) = &query else { unreachable!() };
            // Under a cap the intersection fits and `paid` alone does not.
            assert_eq!(g.lookup(&query, 500), Some(want.clone()), "{opclass:?}");
            let words = g.lookup_dense(&query, 1300, 500).expect("the intersection fits the limit");
            let mut from_words = Vec::new();
            for (w, word) in words.iter().enumerate() {
                let mut bits = *word;
                while bits != 0 {
                    from_words.push(w as u64 * 64 + bits.trailing_zeros() as u64);
                    bits &= bits - 1;
                }
            }
            assert_eq!(from_words, want, "{opclass:?} dense");
            // The key lists every document is on are still left out.
            let walked = g.walked_lists(items, g.forgotten.load(Ordering::Relaxed) as usize);
            assert_eq!(walked.len(), 2, "{opclass:?}: paid and eu walked, the key lists left out: {walked:?}");
        }
    }

    /// Documents of ten items each, as `jsonb_ops` makes of an ordinary
    /// object, and most of them forgotten. The lists are compacted once a
    /// quarter of the slots they hold are forgotten, so they stay within a
    /// third of the live slots, and every live document is still found.
    #[test]
    fn many_item_documents_are_compacted() {
        let ticket = test_mint();
        let proof = Proof::from(&ticket);
        let g = GinIndex::new(decl("c"), 0, GinOpclass::Ops);
        let d = |n: u64| {
            doc(&format!(
                r#"{{"status":"s{}","region":"r{}","user":{{"id":{n}}},"tags":["x","y"],"n":{n}}}"#,
                n % 5,
                n % 3
            ))
        };
        for slot in 0..4000u64 {
            g.insert(proof, slot, Some(&d(slot)));
        }
        let (_, full) = g.sizes();
        assert!(full >= 4000 * 9, "{full} entries: fewer items than this test means");
        // Every slot but each fourth, as vacuum reclaims old versions.
        for slot in (0..4000u64).filter(|s| s % 4 != 0) {
            g.forget(proof, slot);
        }
        let live = 1000u64;
        let forgotten = g.forgotten.load(Ordering::Relaxed);
        assert!(forgotten * 3 <= live, "{forgotten} forgotten slots still on the lists of {live} live");
        let (_, entries) = g.sizes();
        assert!(entries < full / 2, "{entries} of {full} entries left: the lists were not compacted");
        assert_eq!(g.entries.load(Ordering::Relaxed) as usize, entries);
        for status in 0..5u64 {
            let q = contains_query(&doc(&format!(r#"{{"status":"s{status}"}}"#)), GinOpclass::Ops).unwrap();
            let want: Vec<u64> = (0..4000u64).filter(|s| s % 4 == 0 && s % 5 == status).collect();
            assert_eq!(g.lookup(&q, usize::MAX), Some(want), "s{status}");
        }
    }

    /// An insert writes a new slot's entries and then sets its presence
    /// bit. A compaction between the two must leave the entries: the slot
    /// is live, and off its lists it would be dropped from every answer.
    #[test]
    fn a_compaction_keeps_a_slot_whose_insert_is_under_way() {
        let ticket = test_mint();
        let proof = Proof::from(&ticket);
        for opclass in [GinOpclass::Ops, GinOpclass::PathOps] {
            let g = GinIndex::new(decl("w"), 0, opclass);
            let d = doc(r#"{"k":1}"#);
            for slot in 0..10u64 {
                g.insert(proof, slot, Some(&d));
            }
            g.forget(proof, 3);
            // Slot 10's insert, stopped after its entries are written.
            let mut items = Vec::new();
            jsonb::gin_items(&d, opclass, &mut items).unwrap();
            for item in &items {
                g.shards[GinIndex::shard(*item)].write().entry(*item).or_default().push(10);
            }
            g.compact(proof);
            // The insert finishes.
            g.set_present(10, true);
            let q = contains_query(&d, opclass).unwrap();
            let want: Vec<u64> = (0..11u64).filter(|s| *s != 3).collect();
            assert_eq!(g.lookup(&q, usize::MAX), Some(want), "{opclass:?}");
        }
    }

    /// The same, with real threads: inserts of new slots while vacuum
    /// forgets the old ones and compactions run.
    #[test]
    fn compactions_beside_inserts_drop_no_live_slot() {
        use std::sync::Arc;
        let ticket = test_mint();
        let proof = Proof::from(&ticket);
        for round in 0..8 {
            let g = Arc::new(GinIndex::new(decl("t"), 0, GinOpclass::PathOps));
            let d = doc(r#"{"k":1}"#);
            for slot in 0..2000u64 {
                g.insert(proof, slot, Some(&d));
            }
            let writer = {
                let (g, d) = (g.clone(), d.clone());
                std::thread::spawn(move || {
                    let ticket = test_mint();
                    let proof = Proof::from(&ticket);
                    for slot in 10_000..30_000u64 {
                        g.insert(proof, slot, Some(&d));
                    }
                })
            };
            let vacuum = {
                let g = g.clone();
                std::thread::spawn(move || {
                    let ticket = test_mint();
                    let proof = Proof::from(&ticket);
                    for slot in 0..2000u64 {
                        g.forget(proof, slot);
                    }
                    for _ in 0..100 {
                        g.compact(proof);
                    }
                })
            };
            writer.join().unwrap();
            vacuum.join().unwrap();
            let got = g.lookup(&contains_query(&d, GinOpclass::PathOps).unwrap(), usize::MAX).unwrap();
            let missing = (10_000..30_000u64).filter(|s| got.binary_search(s).is_err()).count();
            assert_eq!(missing, 0, "round {round}: {missing} live slots dropped");
            assert!(got.iter().all(|s| *s >= 10_000), "a forgotten slot returned");
        }
    }

    /// What `for_each_sorted` gave, with the stream's order checked on the
    /// way: the lists, the unkeyed slots, the presence words.
    fn capture(g: &GinIndex) -> (Vec<(u64, Vec<u64>)>, Vec<u64>, Vec<u64>) {
        let (mut entries, mut unkeyed, mut present) = (Vec::new(), None, None);
        g.for_each_sorted(|part| -> Result<(), ()> {
            match part {
                Sorted::Entry(item, list) => {
                    assert!(unkeyed.is_none(), "an entry after the unkeyed slots");
                    entries.push((item, list.to_vec()));
                }
                Sorted::Unkeyed(slots) => {
                    assert!(unkeyed.is_none() && present.is_none(), "unkeyed slots out of place");
                    unkeyed = Some(slots.to_vec());
                }
                Sorted::Present(words) => {
                    assert!(unkeyed.is_some() && present.is_none(), "presence bits out of place");
                    present = Some(words.to_vec());
                }
            }
            Ok(())
        })
        .unwrap();
        (entries, unkeyed.expect("unkeyed slots"), present.expect("presence bits"))
    }

    /// An index from [`random_index`], plus documents with an item of their
    /// own each, so the stream spans several blocks and some lists hold only
    /// forgotten slots, and a slot whose insert has written its entries and
    /// not yet set its presence bit. Returns that slot.
    fn index_for_streaming(r: &mut Rng, opclass: GinOpclass) -> (GinIndex, u64) {
        let ticket = test_mint();
        let proof = Proof::from(&ticket);
        let n = 600 + r.below(400);
        let (g, _) = random_index(r, opclass, n);
        for slot in n..n + 1500 {
            g.insert(proof, slot, Some(&doc(&format!(r#"{{"own":{slot}}}"#))));
        }
        for _ in 0..40 {
            g.forget(proof, n + r.below(1500));
        }
        let under_way = n + 1500 + r.below(10);
        let mut items = Vec::new();
        jsonb::gin_items(&doc(r#"{"status":"s1","own":-1}"#), opclass, &mut items).unwrap();
        items.sort_unstable();
        items.dedup();
        for item in &items {
            g.shards[GinIndex::shard(*item)].write().entry(*item).or_default().push(under_way);
        }
        (g, under_way)
    }

    /// C3 leaves out the entries of the slots `forget` recorded, exactly
    /// those: a slot whose presence bit is clear because its insert is under
    /// way goes out, and a list of forgotten slots only does not.
    #[test]
    fn the_sorted_stream_drops_exactly_the_forgotten_slots() {
        let mut r = Rng(0xc3_9e17);
        for round in 0..12 {
            let opclass = if round % 2 == 0 { GinOpclass::Ops } else { GinOpclass::PathOps };
            let (g, under_way) = index_for_streaming(&mut r, opclass);
            let forgotten = g.forgotten_slots.lock().clone();
            assert!(!forgotten.is_empty(), "nothing forgotten to leave out");
            assert_eq!(g.forgotten.load(Ordering::Relaxed) as usize, forgotten.len(), "a compaction ran");
            let mut want: Vec<(u64, Vec<u64>)> = Vec::new();
            for shard in g.shards.iter() {
                for (item, list) in shard.read().iter() {
                    let kept: Vec<u64> = list.iter().copied().filter(|s| !forgotten.contains(s)).collect();
                    if !kept.is_empty() {
                        want.push((*item, kept));
                    }
                }
            }
            want.sort_unstable();
            let (entries, unkeyed, present) = capture(&g);
            assert!(entries.len() > SORTED_BLOCK, "{opclass:?}: too few items to span blocks");
            assert!(entries.windows(2).all(|w| w[0].0 < w[1].0), "{opclass:?}: items not ascending");
            assert_eq!(entries, want, "{opclass:?}");
            let listed = |s: u64| entries.iter().any(|(_, l)| l.contains(&s));
            assert!(listed(under_way), "{opclass:?}: the slot under way was filtered by its presence bit");
            assert!(!forgotten.iter().any(|s| listed(*s)), "{opclass:?}: a forgotten slot went out");
            let mut want_unkeyed = g.unkeyed.read().clone();
            want_unkeyed.sort_unstable();
            assert_eq!(unkeyed, want_unkeyed, "{opclass:?}");
            assert_eq!(present.as_slice(), crate::core::index::sorted::trimmed(&g.present.read()), "{opclass:?}");
        }
    }
}
