//! Secondary indexes: B-tree and hash, over one or more columns.
//!
//! ```sql
//! CREATE INDEX orders_customer ON orders (customer_id);          -- method chosen
//! CREATE INDEX orders_placed ON orders USING btree (placed_at);
//! CREATE INDEX sessions_token ON sessions USING hash (token);
//! CREATE INDEX orders_cust_day ON orders (customer_id, placed_at); -- composite
//! DROP INDEX orders_customer;
//! ```
//!
//! `PRIMARY KEY` and `UNIQUE` columns, and composite unique constraints, get a
//! B-tree implicitly, as PostgreSQL does.
//!
//! # Choosing the method
//!
//! With no `USING`, the method is chosen from the column types:
//!
//! - **Hash** when every column is one nothing asks a range of: a UUID, a
//!   document, a hardware address. One probe, no ordered structure.
//! - **B-tree** for everything else. It answers equality as well, so it is
//!   the safe default whenever a range might be asked.
//!
//! A type with no proven encoding for the chosen method is refused at
//! `CREATE INDEX`, with the reason. See [`key`].
//!
//! # A superset, never a subset
//!
//! A query asks an index for *candidate* slots. The scan then checks each
//! candidate's visibility and re-evaluates the whole `WHERE` clause, exactly
//! as without an index. So an index may hold extra entries, but it must never
//! miss a version a query could need. Everything here follows from that rule:
//!
//! - **Adding** happens when a version is written, before its transaction can
//!   commit, so no snapshot can see a version its index does not hold.
//! - **Removing** happens only when the removed value can never be read again:
//!   - after an in-place overwrite is durable (in-place writes happen only
//!     when nothing can roll them back and nobody else holds a snapshot);
//!   - when vacuum reclaims a version no snapshot can reach. It reads the key
//!     from the row before the space is freed.
//! - A version whose key cannot be built is held in an **unkeyed** list that is
//!   returned with every lookup, so a malformed row costs a candidate, never
//!   an answer.
//! - A slot appended after a lookup sampled the table's length is a
//!   candidate by default (the candidate bitmap's own rule).
//!
//! The index lives in memory and is rebuilt from the table at open, and after
//! `VACUUM FULL` renumbers slots. Until a build finishes, queries scan as they
//! would without it.

pub mod ddl;
pub mod gin;
pub mod decl;
pub mod key;
pub mod sorted;

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::RandomState;
use parking_lot::RwLock;
use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::filter::SargOp;
use crate::core::filter::sarg::{MaskExpr, SargPredicate};
use crate::core::index_store::gate::Proof;
use crate::core::row::row::RowWrite;
use crate::core::row::schema::SchemaField;

pub use decl::{IndexDecl, Method};
pub use sorted::{Sorted, SortedLoad, SortedReject};

/// A key, inline when short (most are: an integer, a UUID, a short string).
pub type KeyBuf = SmallVec<[u8; 24]>;
pub(super) type Slots = SmallVec<[u64; 2]>;

const HASH_SHARDS: usize = 64;

/// Threads a hash index's load fills its shards with, at most: the shards
/// are independent, and past this the deal before them is the bound.
const INSTALL_WORKERS: usize = 8;

/// Above this many candidates an index answer is not worth returning: the
/// filter or a plain scan reads that many rows about as cheaply, and the
/// candidate list itself becomes the cost.
pub fn candidate_cap(n_slots: u64) -> usize {
    ((n_slots / 8) as usize).max(4096)
}

/// Above this many candidates an inverted index's answer is declined even as
/// a bitmap, which is how an answer past [`candidate_cap`] is returned.
///
/// A bitmap costs the scan a bit test per slot instead of a slot list's
/// jumps, and at a random scatter almost no 64-slot word is empty, so every
/// pointer record is still read. A fetched row costs 12-50 ns, against well
/// under a nanosecond for a masked one (the filter's measured ratio), so even
/// half the table left out pays for the bitmap many times over. The filter's
/// own gate sits at nine tenths, measured on contiguous masks. A scattered
/// mask mispredicts more and has not been measured, so this stays well short
/// of that.
pub fn dense_cap(n_slots: u64) -> usize {
    (n_slots / 2) as usize
}

enum Data {
    BTree(RwLock<BTreeMap<KeyBuf, Slots>>),
    Hash(Box<[RwLock<HashMap<KeyBuf, Slots, RandomState>>]>, RandomState),
}

/// One index, live.
pub struct LiveIndex {
    pub decl: IndexDecl,
    /// `(write_order, type)` per key column, in key order.
    pub columns: Vec<(u32, DbType)>,
    /// Whether it came from a constraint rather than `CREATE INDEX`.
    pub implicit: bool,
    data: Data,
    /// One bit per slot this index holds an entry for. Vacuum uses it to find
    /// the reclaimed versions whose keys must go, whatever retired them.
    present: RwLock<Vec<u64>>,
    /// Slots whose key could not be built. Returned with every lookup.
    unkeyed: RwLock<Vec<u64>>,
    /// For an index over a path into a document (`((doc->>'k'))`): the
    /// document column and the path. Its one key column is then a virtual
    /// one (see [`TableIndexes::path_column`]), whose value is the path's
    /// text, read out of the document.
    pub path: Option<(u32, Vec<crate::core::jsonb::Step>)>,
}

impl LiveIndex {
    pub fn new(decl: IndexDecl, columns: Vec<(u32, DbType)>, implicit: bool) -> LiveIndex {
        let data = match decl.method {
            Method::BTree => Data::BTree(RwLock::new(BTreeMap::new())),
            // An inverted index is a `gin::GinIndex`, never one of these;
            // `for_schema` routes it there. A hash is the harmless reading.
            Method::Hash | Method::GinOps | Method::GinPathOps => Data::Hash(
                (0..HASH_SHARDS)
                    .map(|_| RwLock::new(HashMap::with_hasher(RandomState::new())))
                    .collect(),
                RandomState::new(),
            ),
        };
        LiveIndex {
            decl,
            columns,
            implicit,
            data,
            present: RwLock::new(Vec::new()),
            unkeyed: RwLock::new(Vec::new()),
            path: None,
        }
    }

    /// An index over `doc->>'path'`: one virtual `STRING` key column,
    /// `virtual_column`, read out of the document in `document_column`.
    pub fn over_path(
        decl: IndexDecl,
        virtual_column: u32,
        document_column: u32,
        steps: Vec<crate::core::jsonb::Step>,
    ) -> LiveIndex {
        let mut index = LiveIndex::new(decl, vec![(virtual_column, DbType::STRING)], false);
        index.path = Some((document_column, steps));
        index
    }

    pub fn method(&self) -> Method {
        self.decl.method
    }

    pub fn leading(&self) -> u32 {
        self.columns[0].0
    }

    /// The key for a version, from a lookup of its column bytes.
    pub fn key_of<'a>(&self, get: impl Fn(u32) -> Option<&'a [u8]>) -> Option<KeyBuf> {
        if let Some((document, steps)) = &self.path {
            // `->>` of a missing path, or of a JSON null, is SQL NULL, which
            // no comparison matches. It is kept under an empty key rather than
            // as unkeyed: a range from below may return it, an equality never
            // asks for it, and either way it is rechecked.
            let text = get(*document)
                .and_then(|doc| crate::core::jsonb::get_path(doc, steps))
                .and_then(crate::core::jsonb::as_text);
            let Some(text) = text else {
                return Some(KeyBuf::new());
            };
            let mut out = Vec::with_capacity(text.len() + 2);
            key::encode_ordered(&DbType::STRING, text.as_bytes(), true, &mut out)?;
            return Some(KeyBuf::from_vec(out));
        }
        self.key_of_values(get)
    }

    /// The key for values given per key column, as the planner has them from
    /// a predicate. For a path index that is the path's text under its
    /// virtual column, not a document: reading it through [`Self::key_of`]
    /// would find no document and look up the empty key that `NULL` paths
    /// are kept under, missing every row with a value.
    pub fn key_of_values<'a>(&self, get: impl Fn(u32) -> Option<&'a [u8]>) -> Option<KeyBuf> {
        let mut out = Vec::with_capacity(24);
        let n = self.columns.len();
        for (i, (wo, ty)) in self.columns.iter().enumerate() {
            let value = get(*wo)?;
            match self.decl.method {
                Method::BTree => key::encode_ordered(ty, value, i + 1 == n, &mut out)?,
                Method::Hash | Method::GinOps | Method::GinPathOps => key::encode_canonical(ty, value, &mut out)?,
            }
        }
        Some(KeyBuf::from_vec(out))
    }

    pub fn key_of_row(&self, row: &RowWrite) -> Option<KeyBuf> {
        self.key_of(|wo| {
            row.columns_writing_data
                .iter()
                .find(|c| c.write_order == wo)
                .map(|c| c.data.into_slice())
        })
    }

    fn shard(&self, key: &[u8]) -> usize {
        match &self.data {
            Data::Hash(_, state) => (state.hash_one(key) as usize) % HASH_SHARDS,
            Data::BTree(_) => 0,
        }
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

    /// Whether this index holds an entry for `slot`.
    pub fn holds(&self, slot: u64) -> bool {
        let w = (slot / 64) as usize;
        self.present.read().get(w).is_some_and(|word| word & (1 << (slot % 64)) != 0)
    }

    /// Adds `slot` under `key`, or to the unkeyed list when there is no key.
    pub fn insert(&self, _proof: Proof<'_>, slot: u64, key: Option<KeyBuf>) {
        crate::core::index_store::census::mutation(_proof, self as *const Self as usize, "LiveIndex::insert");
        match key {
            None => {
                let mut unkeyed = self.unkeyed.write();
                if !unkeyed.contains(&slot) {
                    unkeyed.push(slot);
                }
            }
            Some(key) => match &self.data {
                Data::BTree(map) => add_sorted(map.write().entry(key).or_default(), slot),
                Data::Hash(shards, _) => add_sorted(shards[self.shard(&key)].write().entry(key).or_default(), slot),
            },
        }
        self.set_present(slot, true);
    }

    /// Removes `slot` from under `key`. Removing a pair that is not there is a
    /// no-op, which is what makes a late or repeated removal harmless.
    pub fn remove(&self, _proof: Proof<'_>, slot: u64, key: &[u8]) {
        crate::core::index_store::census::mutation(_proof, self as *const Self as usize, "LiveIndex::remove");
        fn drop_slot<M: MapLike>(map: &mut M, key: &[u8], slot: u64) {
            let empty = match map.slots_mut(key) {
                Some(slots) => {
                    if let Ok(at) = slots.binary_search(&slot) {
                        slots.remove(at);
                    }
                    slots.is_empty()
                }
                None => false,
            };
            if empty {
                map.drop_key(key);
            }
        }
        match &self.data {
            Data::BTree(map) => drop_slot(&mut *map.write(), key, slot),
            Data::Hash(shards, _) => drop_slot(&mut *shards[self.shard(key)].write(), key, slot),
        }
    }

    /// Forgets a slot that is gone for everyone: under `key` (read back from
    /// the row before its space was freed) and from the unkeyed list.
    pub fn forget(&self, proof: Proof<'_>, slot: u64, key: Option<&[u8]>) {
        crate::core::index_store::census::mutation(proof, self as *const Self as usize, "LiveIndex::forget");
        if let Some(key) = key {
            self.remove(proof, slot, key);
        }
        self.unkeyed.write().retain(|s| *s != slot);
        self.set_present(slot, false);
    }

    pub fn clear(&self, _proof: Proof<'_>) {
        crate::core::index_store::census::mutation(_proof, self as *const Self as usize, "LiveIndex::clear");
        match &self.data {
            Data::BTree(map) => map.write().clear(),
            Data::Hash(shards, _) => shards.iter().for_each(|s| s.write().clear()),
        }
        self.present.write().clear();
        self.unkeyed.write().clear();
    }

    /// Distinct keys held, and entries (slots) under them.
    pub fn sizes(&self) -> (usize, usize) {
        match &self.data {
            Data::BTree(map) => {
                let map = map.read();
                (map.len(), map.values().map(|s| s.len()).sum())
            }
            Data::Hash(shards, _) => shards.iter().fold((0, 0), |(k, e), s| {
                let s = s.read();
                (k + s.len(), e + s.values().map(|v| v.len()).sum::<usize>())
            }),
        }
    }

    /// Every entry, then the unkeyed slots, then the presence bits, as one
    /// sorted stream (hook C3, see [`sorted`]): what a snapshot's INDEX
    /// section holds. Stops at the first error `f` returns, and returns it.
    ///
    /// A superset, never a subset: nothing is filtered by the presence bit,
    /// so the entries of a slot forgotten without its key go out too, as a
    /// lookup returns them too.
    ///
    /// The presence bits are read **before** the entries are swept, though
    /// they go out last. `insert` lists a slot's entry before it sets the
    /// slot's bit, so every slot whose bit the stream carries had its entry
    /// listed before the sweep began, and the sweep finds it: a loaded index
    /// never holds a slot it cannot find. Read after the sweep, the bits
    /// would also carry a slot inserted under a key the sweep had already
    /// passed, held and listed nowhere. That much no longer depends on the
    /// seal's gate; a slot inserted during the sweep goes out with its bit
    /// clear, and its entry if the sweep reached it, which is a superset too.
    ///
    /// The read lock is held for one block of [`sorted::SORTED_BLOCK`]
    /// entries at a time, never across `f`. So the stream is one state of
    /// the index only while nothing writes it, which is what a seal
    /// guarantees: it captures under a gate no ticket holds, and a ticket
    /// taken meanwhile cancels the capture (docs/persistent-indexes.md §3).
    /// An in-place move during the sweep (the new key behind it, the old
    /// one ahead) can still leave a slot listed under neither, so a capture
    /// that a write overlapped is never published.
    pub fn for_each_sorted<E>(&self, mut f: impl FnMut(Sorted<'_, &[u8]>) -> Result<(), E>) -> Result<(), E> {
        // Before the sweep: see above.
        let present = self.present.read().clone();
        match &self.data {
            Data::BTree(map) => {
                // Each block resumes after the last key of the one before,
                // so it holds no position a writer could move.
                let mut block: Vec<(KeyBuf, Slots)> = Vec::with_capacity(sorted::SORTED_BLOCK);
                let mut after: Option<KeyBuf> = None;
                loop {
                    block.clear();
                    let from = after.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
                    block.extend(
                        map.read()
                            .range::<[u8], _>((from, Bound::Unbounded))
                            .take(sorted::SORTED_BLOCK)
                            .map(|(key, slots)| (key.clone(), slots.clone())),
                    );
                    for (key, slots) in &block {
                        f(Sorted::Entry(key, slots))?;
                    }
                    if block.len() < sorted::SORTED_BLOCK {
                        break;
                    }
                    after = block.pop().map(|(key, _)| key);
                }
            }
            Data::Hash(shards, _) => {
                // A hash map has no order, so the keys are gathered and
                // sorted by their bytes, which the per-process hasher never
                // reaches. A key gone since the sweep is skipped.
                let mut keys: Vec<KeyBuf> = Vec::new();
                for shard in shards.iter() {
                    keys.extend(shard.read().keys().cloned());
                }
                keys.sort_unstable();
                let mut block: Vec<(&KeyBuf, Slots)> = Vec::with_capacity(sorted::SORTED_BLOCK);
                for chunk in keys.chunks(sorted::SORTED_BLOCK) {
                    block.clear();
                    for key in chunk {
                        if let Some(slots) = shards[self.shard(key)].read().get(key) {
                            block.push((key, slots.clone()));
                        }
                    }
                    for (key, slots) in &block {
                        f(Sorted::Entry(key, slots))?;
                    }
                }
            }
        }
        // Distinct already (`insert` checks), but in arrival order.
        let mut unkeyed = self.unkeyed.read().clone();
        unkeyed.sort_unstable();
        unkeyed.dedup();
        f(Sorted::Unkeyed(&unkeyed))?;
        f(Sorted::Present(sorted::trimmed(&present)))
    }

    /// Builds this index from a sorted stream (hook C9, see [`sorted`]): a
    /// snapshot's INDEX section, as [`Self::for_each_sorted`] gave it. The
    /// stream was checked as it arrived and is checked complete here, before
    /// anything is touched, so a refused one leaves the index as it was,
    /// never part loaded; the caller then rebuilds from rows.
    ///
    /// Entries go in before the presence bits, as `insert` orders them. The
    /// unkeyed slots and presence bits are restored as captured, never
    /// inferred from the entries: a slot forgotten without its key is listed
    /// and not held, and vacuum's `holds` must keep reading it so.
    ///
    /// A superset, never a subset: the load adds and removes nothing. Into
    /// the empty index an open builds, that is the snapshot. Beside a write
    /// that got in first (writes wait for the load, §4, but nothing here
    /// relies on it) it is the union, which lists every version either did.
    pub fn load_sorted(&self, proof: Proof<'_>, stream: sorted::SortedLoad<KeyBuf>) -> Result<(), sorted::SortedReject> {
        crate::core::index_store::census::mutation(proof, self as *const Self as usize, "LiveIndex::load_sorted");
        let (entries, unkeyed, present) = stream.into_parts()?;
        match &self.data {
            Data::BTree(map) => {
                let mut map = map.write();
                if map.is_empty() {
                    // In key order already, so built in one pass, from the
                    // decoded vector itself (`collect` over its own
                    // `into_iter` reuses the allocation).
                    *map = entries.into_iter().collect();
                } else {
                    for (key, listed) in entries {
                        merge_sorted(map.entry(key).or_default(), listed);
                    }
                }
            }
            Data::Hash(shards, _) => {
                // Dealt out to the shards first, so each is locked once, and
                // the shards then filled in parallel: they share nothing, and
                // filling 64 of them one after another was the slowest part of
                // a load (the hash section's install, 0.14 s for 1.5M keys).
                let mut dealt: Vec<Vec<(KeyBuf, Slots)>> = (0..HASH_SHARDS).map(|_| Vec::new()).collect();
                for (key, listed) in entries {
                    dealt[self.shard(&key)].push((key, listed));
                }
                let fill = |pairs: &mut [(&RwLock<HashMap<KeyBuf, Slots, RandomState>>, Vec<(KeyBuf, Slots)>)]| {
                    for (shard, part) in pairs.iter_mut() {
                        let part = std::mem::take(part);
                        let mut shard = shard.write();
                        shard.reserve(part.len());
                        for (key, listed) in part {
                            merge_sorted(shard.entry(key).or_default(), listed);
                        }
                    }
                };
                let mut pairs: Vec<_> = shards.iter().zip(dealt).collect();
                let workers = std::thread::available_parallelism().map_or(1, |n| n.get()).clamp(1, INSTALL_WORKERS);
                if workers == 1 {
                    fill(&mut pairs);
                } else {
                    std::thread::scope(|scope| {
                        for run in pairs.chunks_mut(HASH_SHARDS.div_ceil(workers)) {
                            scope.spawn(move || fill(run));
                        }
                    });
                }
            }
        }
        {
            let mut held = self.unkeyed.write();
            if held.is_empty() {
                *held = unkeyed;
            } else {
                held.extend(unkeyed);
                held.sort_unstable();
                held.dedup();
            }
        }
        let mut bits = self.present.write();
        if bits.len() < present.len() {
            bits.resize(present.len(), 0);
        }
        for (held, loaded) in bits.iter_mut().zip(&present) {
            *held |= *loaded;
        }
        Ok(())
    }

    /// Appends the slots under exactly `key`. `false` when `cap` was exceeded.
    fn lookup_eq(&self, key: &[u8], out: &mut Vec<u64>, cap: usize) -> bool {
        // Measured before copying: a common key (`status = 'shipped'`) lists
        // a fifth of the table, and copying it only to find it over `cap`
        // cost more than the scan the answer was meant to save.
        let mut take = |slots: Option<&Slots>| match slots {
            Some(slots) if out.len() + slots.len() > cap => false,
            Some(slots) => {
                out.extend_from_slice(slots);
                true
            }
            None => out.len() <= cap,
        };
        match &self.data {
            Data::BTree(map) => take(map.read().get(key)),
            Data::Hash(shards, _) => take(shards[self.shard(key)].read().get(key)),
        }
    }

    /// Appends the slots with keys in a range (B-tree only). `false` when `cap`
    /// was exceeded or the index is not ordered.
    fn lookup_range(&self, lo: Bound<&[u8]>, hi: Bound<&[u8]>, out: &mut Vec<u64>, cap: usize) -> bool {
        let Data::BTree(map) = &self.data else {
            return false;
        };
        // An inverted or empty range is legitimately empty: `BTreeMap::range`
        // panics on one, so it is answered here.
        if let (Bound::Included(l) | Bound::Excluded(l), Bound::Included(h) | Bound::Excluded(h)) = (lo, hi) {
            if l > h || (l == h && (matches!(lo, Bound::Excluded(_)) || matches!(hi, Bound::Excluded(_)))) {
                return true;
            }
        }
        let map = map.read();
        for (_, slots) in map.range::<[u8], _>((lo, hi)) {
            out.extend_from_slice(slots);
            if out.len() > cap {
                return false;
            }
        }
        true
    }

    /// Every slot that may hold exactly `key`: those under it, and the
    /// unkeyed ones.
    pub fn slots_for_key(&self, key: &[u8]) -> Vec<u64> {
        let mut out = Vec::new();
        self.lookup_eq(key, &mut out, usize::MAX);
        self.append_unkeyed(&mut out);
        out.sort_unstable();
        out.dedup();
        out
    }

    fn append_unkeyed(&self, out: &mut Vec<u64>) {
        out.extend_from_slice(&self.unkeyed.read());
    }

    /// How many slots are listed under `key`, without reading them.
    fn listed_under(&self, key: &[u8]) -> usize {
        match &self.data {
            Data::BTree(map) => map.read().get(key).map_or(0, |s| s.len()),
            Data::Hash(shards, _) => shards[self.shard(key)].read().get(key).map_or(0, |s| s.len()),
        }
    }

    /// Keeps the candidates that may hold exactly `key`: those listed under
    /// it, and the unkeyed ones. `candidates` must be sorted and distinct.
    ///
    /// Both lists are sorted, so this walks them together, galloping ahead
    /// in the posting list: sequential reads, and a few thousand candidates
    /// against a list of a fifth of the table cost microseconds, never a
    /// random probe per candidate into megabytes. The index's lock is held
    /// only for that walk.
    fn retain_under(&self, key: &[u8], candidates: &mut Vec<u64>) {
        // Almost always empty; otherwise unsorted, so sorted once here.
        let unkeyed: Vec<u64> = {
            let unkeyed = self.unkeyed.read();
            let mut copy = unkeyed.clone();
            copy.sort_unstable();
            copy
        };
        let keep = |slots: Option<&Slots>, candidates: &mut Vec<u64>| {
            let slots: &[u64] = slots.map_or(&[], |s| s.as_slice());
            let (mut at, mut at_unkeyed) = (0usize, 0usize);
            candidates.retain(|&c| {
                at = gallop_to(slots, at, c);
                if slots.get(at) == Some(&c) {
                    return true;
                }
                if unkeyed.is_empty() {
                    return false;
                }
                at_unkeyed = gallop_to(&unkeyed, at_unkeyed, c);
                unkeyed.get(at_unkeyed) == Some(&c)
            });
        };
        match &self.data {
            Data::BTree(map) => keep(map.read().get(key), candidates),
            Data::Hash(shards, _) => keep(shards[self.shard(key)].read().get(key), candidates),
        }
    }
}

/// The first position at or after `from` whose value is not below `target`,
/// in a sorted slice: doubling steps from `from`, then a binary search
/// inside the last step. `O(log d)` for a distance `d`, so a sweep over
/// ascending targets reads the slice front to back once.
///
/// Correct only when every value before `from` is below `target`; the
/// pointer iterator's walk over a candidate list relies on the same rule.
pub(crate) fn gallop_to(sorted: &[u64], from: usize, target: u64) -> usize {
    if from >= sorted.len() || sorted[from] >= target {
        return from;
    }
    let mut step = 1;
    let mut lo = from;
    while lo + step < sorted.len() && sorted[lo + step] < target {
        lo += step;
        step *= 2;
    }
    let hi = (lo + step).min(sorted.len());
    lo + 1 + sorted[lo + 1..hi].partition_point(|v| *v < target)
}

/// Adds `slot` to a posting list, which is kept sorted and distinct.
///
/// Sorted so that membership is a binary search: a duplicate check by scan
/// cost a list's length per insert, which made building an index on a
/// column of five values quadratic (a million rows, five lists of 200k), and
/// every later insert into it read 1.6 MB. Slots almost always arrive in
/// ascending order, a new version being appended, so the common case is a
/// push.
fn add_sorted(slots: &mut Slots, slot: u64) {
    match slots.last() {
        None => slots.push(slot),
        Some(&last) if last < slot => slots.push(slot),
        Some(_) => {
            if let Err(at) = slots.binary_search(&slot) {
                slots.insert(at, slot);
            }
        }
    }
}

/// Adds a loaded posting list, sorted and distinct, to the one already held
/// under its key, which stays sorted and distinct: taken whole when there is
/// none, which is every key of a load into an empty index.
fn merge_sorted(into: &mut Slots, listed: Slots) {
    if into.is_empty() {
        *into = listed;
    } else {
        for slot in listed {
            add_sorted(into, slot);
        }
    }
}

/// The two maps behind an index, for the one operation they share.
trait MapLike {
    fn slots_mut(&mut self, key: &[u8]) -> Option<&mut Slots>;
    fn drop_key(&mut self, key: &[u8]);
}

impl MapLike for BTreeMap<KeyBuf, Slots> {
    fn slots_mut(&mut self, key: &[u8]) -> Option<&mut Slots> {
        self.get_mut(key)
    }
    fn drop_key(&mut self, key: &[u8]) {
        self.remove(key);
    }
}

impl MapLike for HashMap<KeyBuf, Slots, RandomState> {
    fn slots_mut(&mut self, key: &[u8]) -> Option<&mut Slots> {
        self.get_mut(key)
    }
    fn drop_key(&mut self, key: &[u8]) {
        self.remove(key);
    }
}

/// Byte bounds on a B-tree for an equality prefix and the constraints on
/// the column after it.
///
/// `prefix` is the encoding of the first `k` columns, each fixed by an
/// equality. Every component encoding is prefix-free (fixed width, or a
/// terminated string), so the keys holding those `k` values are exactly the
/// keys that start with `prefix`, and the bounds on column `k` are its own
/// bounds with `prefix` in front.
struct LeadingRange {
    prefix: Vec<u8>,
    lo: Option<(Vec<u8>, bool)>, // (bytes, inclusive)
    hi: Option<(Vec<u8>, bool)>,
}

/// The encodings a leading-column value bounds a composite key with.
///
/// Keys whose first component equals `v` are exactly those in
/// `[prefix_lo(v), prefix_hi(v))`. For a fixed-width type `prefix_lo` is the
/// encoding and `prefix_hi` its successor. For a string that is not the last
/// component, the encoding ends with the terminator `00 00`, and nothing with a
/// different first component falls between that and `… 00 01`.
fn prefix_bounds(ty: &DbType, v: &[u8], whole_key: bool) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
    let mut lo = Vec::new();
    key::encode_ordered(ty, v, whole_key, &mut lo)?;
    if whole_key {
        return Some((lo.clone(), None));
    }
    let hi = if *ty == DbType::STRING {
        let mut h = lo.clone();
        let n = h.len();
        h[n - 1] = 1; // terminator 00 00 -> 00 01
        Some(h)
    } else {
        key::prefix_successor(&lo)
    };
    Some((lo, hi))
}

impl LeadingRange {
    /// The keys that start with `prefix`: all of them when it is empty.
    fn within(prefix: Vec<u8>) -> Self {
        if prefix.is_empty() {
            return LeadingRange { prefix, lo: None, hi: None };
        }
        let hi = key::prefix_successor(&prefix).map(|h| (h, false));
        LeadingRange {
            lo: Some((prefix.clone(), true)),
            hi,
            prefix,
        }
    }

    /// Narrows by one predicate on the column after the prefix. `whole_key`
    /// is true when that column is the key's last, whose encoding is then
    /// not a prefix of anything longer.
    fn narrow(&mut self, ty: &DbType, p: &SargPredicate, whole_key: bool) -> Option<()> {
        let (plo, phi) = prefix_bounds(ty, &p.value, whole_key)?;
        let with_prefix = |b: Vec<u8>| {
            let mut out = Vec::with_capacity(self.prefix.len() + b.len());
            out.extend_from_slice(&self.prefix);
            out.extend_from_slice(&b);
            out
        };
        let plo = with_prefix(plo);
        let phi = phi.map(with_prefix);
        // For a whole key, "equal to v" is [enc, enc]; for a prefix,
        // [plo, phi) with phi exclusive (or unbounded).
        let (eq_lo, eq_hi): ((Vec<u8>, bool), Option<(Vec<u8>, bool)>) = if whole_key {
            ((plo.clone(), true), Some((plo.clone(), true)))
        } else {
            ((plo.clone(), true), phi.clone().map(|h| (h, false)))
        };
        match p.op {
            SargOp::Eq => {
                self.raise_lo(eq_lo);
                if let Some(h) = eq_hi {
                    self.lower_hi(h);
                }
            }
            SargOp::Ge => self.raise_lo(eq_lo),
            SargOp::Gt => {
                if whole_key {
                    self.raise_lo((plo, false));
                } else {
                    match phi {
                        Some(h) => self.raise_lo((h, true)),
                        // Nothing sorts above every key starting with `v`:
                        // the range is empty.
                        None => self.lower_hi((Vec::new(), false)),
                    }
                }
            }
            SargOp::Le => match eq_hi {
                Some(h) => self.lower_hi(h),
                None => {}
            },
            SargOp::Lt => self.lower_hi((plo, false)),
        }
        Some(())
    }

    fn raise_lo(&mut self, b: (Vec<u8>, bool)) {
        let replace = match &self.lo {
            None => true,
            Some((cur, inc)) => b.0 > *cur || (b.0 == *cur && *inc && !b.1),
        };
        if replace {
            self.lo = Some(b);
        }
    }

    fn lower_hi(&mut self, b: (Vec<u8>, bool)) {
        let replace = match &self.hi {
            None => true,
            Some((cur, inc)) => b.0 < *cur || (b.0 == *cur && *inc && !b.1),
        };
        if replace {
            self.hi = Some(b);
        }
    }

    fn bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        fn f(b: &Option<(Vec<u8>, bool)>) -> Bound<&[u8]> {
            match b {
                None => Bound::Unbounded,
                Some((v, true)) => Bound::Included(v.as_slice()),
                Some((v, false)) => Bound::Excluded(v.as_slice()),
            }
        }
        (f(&self.lo), f(&self.hi))
    }
}

/// Lookups an inverted index answered, and lookups a path index answered,
/// process-wide. For tests: an index that is never consulted is invisible in
/// query results by design.
pub static GIN_LOOKUPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PATH_LOOKUPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Of `GIN_LOOKUPS`, those answered as a bitmap (see [`dense_cap`]).
pub static GIN_DENSE_LOOKUPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Lookups a B-tree or hash index over columns (not a path) answered. Test
/// builds only: it sits on every point lookup, and a process-wide counter
/// nothing reads would be one contended cache line per lookup.
#[cfg(test)]
pub static KEY_LOOKUPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[cfg(test)]
thread_local! {
    /// `KEY_LOOKUPS` counted on this thread only, so a test's check is not
    /// passed by another test's lookup landing in between.
    pub(crate) static KEY_LOOKUPS_HERE: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[inline(always)]
fn count_key_lookup() {
    #[cfg(test)]
    {
        KEY_LOOKUPS.fetch_add(1, Ordering::Relaxed);
        KEY_LOOKUPS_HERE.with(|n| n.set(n.get() + 1));
    }
}

/// The slots in both sorted, distinct lists.
pub fn intersect_sorted(a: &[u64], b: &[u64]) -> Vec<u64> {
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

/// The bytes a write holds for one column, if it writes it.
fn document_of(row: &RowWrite, column: u32) -> Option<&[u8]> {
    row.columns_writing_data
        .iter()
        .find(|c| c.write_order == column)
        .map(|c| c.data.into_slice())
}

/// Every index a table has, built or building.
pub struct TableIndexes {
    pub list: Vec<Arc<LiveIndex>>,
    /// Inverted indexes over document columns. Kept apart from `list`: they
    /// hold many keys per row, and nothing that asks `list` for a whole-key
    /// lookup (a uniqueness probe, an equality plan) may ever be answered by
    /// one.
    pub gin: Vec<Arc<gin::GinIndex>>,
    /// Set once the build that follows open (or compaction) has finished.
    /// Until then a lookup would miss rows the build has not reached, so
    /// queries do not ask, and scan instead.
    ///
    /// Bit 0 is that flag; the rest is the renumbering epoch, which
    /// [`TableIndexes::invalidate`] bumps when slot numbers start and stop
    /// changing. One word, so a build that read its epoch before a
    /// renumbering can never publish ready after it: see
    /// [`TableIndexes::set_ready_at`].
    state: AtomicU64,
}

/// A table's indexes, read-only: what a reader (the catalog views) is given,
/// so it never holds what the mutators are called on and never needs a proof
/// (`core::index_store::gate`).
#[derive(Clone, Copy)]
pub struct IndexView<'a> {
    indexes: &'a TableIndexes,
}

impl<'a> IndexView<'a> {
    /// The B-tree and hash indexes.
    pub fn list(&self) -> &'a [Arc<LiveIndex>] {
        &self.indexes.list
    }

    /// The inverted indexes.
    pub fn gin(&self) -> &'a [Arc<gin::GinIndex>] {
        &self.indexes.gin
    }

    /// See [`TableIndexes::is_ready`].
    pub fn is_ready(&self) -> bool {
        self.indexes.is_ready()
    }
}

/// The `write_order`s handed to the virtual key columns of path indexes,
/// counting down from here. No table has that many columns.
const FIRST_PATH_COLUMN: u32 = u32::MAX - 1;

impl TableIndexes {
    pub fn new(list: Vec<Arc<LiveIndex>>) -> TableIndexes {
        TableIndexes::with_gin(list, Vec::new())
    }

    pub fn with_gin(list: Vec<Arc<LiveIndex>>, gin: Vec<Arc<gin::GinIndex>>) -> TableIndexes {
        TableIndexes {
            list,
            gin,
            state: AtomicU64::new(0),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty() && self.gin.is_empty()
    }

    /// These indexes, read-only.
    pub fn view(&self) -> IndexView<'_> {
        IndexView { indexes: self }
    }

    /// How many indexes there are, of either kind.
    pub fn len(&self) -> usize {
        self.list.len() + self.gin.len()
    }

    /// What these indexes are, as one number a persisted snapshot records and
    /// is checked against: equal shapes key every row under equal entries.
    /// Reached through [`shape_of`], which builds the indexes from the same
    /// schema whose fields it passes, so the two cannot come from different
    /// schemas.
    ///
    /// Per index, in this order (which [`for_schema`] fixes, dedup survivor
    /// included): kind, method, uniqueness, implicitness, and per key column
    /// its `write_order`, type and NUMERIC scale; a path index's document
    /// column and steps; a gin index's column and operator class. Never a
    /// name, which keys nothing. `fields` gives the scales, which the planner
    /// encodes literals at. Frozen like the keys (`core::hash`), since it is
    /// persisted. Every struct is destructured whole: a new field does not
    /// compile until it is hashed here or excluded with its reason.
    fn shape_hash(&self, fields: &[SchemaField]) -> u64 {
        use crate::core::jsonb::{GinOpclass, Step};
        let method = |m: Method| match m {
            Method::BTree => 1u8,
            Method::Hash => 2,
            Method::GinOps => 3,
            Method::GinPathOps => 4,
        };
        // `state` is readiness and the renumbering epoch, not what is keyed.
        let TableIndexes { list, gin, state: _ } = self;
        let mut h = crate::core::hash::ItemFeed::new(0);
        h.u64_le(list.len() as u64);
        for index in list {
            // The entries themselves are not the shape; `decl.columns` is
            // `columns` without the types.
            let LiveIndex {
                decl,
                columns,
                implicit,
                path,
                data: _,
                present: _,
                unkeyed: _,
            } = &**index;
            let IndexDecl {
                name: _,
                method: m,
                unique,
                columns: _,
                is_deleted: _,
            } = decl;
            h.u8(1);
            h.u8(method(*m));
            h.u8(*unique as u8);
            h.u8(*implicit as u8);
            h.u64_le(columns.len() as u64);
            for (wo, ty) in columns {
                h.u64_le(*wo as u64);
                h.u8(ty.to_byte());
                let scale = fields
                    .iter()
                    .find(|f| f.write_order as u32 == *wo && !f.is_deleted)
                    .and_then(|f| f.scale);
                match scale {
                    None => h.u8(0),
                    Some(s) => {
                        h.u8(1);
                        h.u64_le(s as u64);
                    }
                }
            }
            match path {
                None => h.u8(0),
                Some((document, steps)) => {
                    h.u8(1);
                    h.u64_le(*document as u64);
                    h.u64_le(steps.len() as u64);
                    for step in steps {
                        match step {
                            Step::Key(k) => {
                                h.u8(1);
                                h.u64_le(k.len() as u64);
                                h.bytes(k.as_bytes());
                            }
                            Step::Index(n) => {
                                h.u8(2);
                                h.i64_le(*n);
                            }
                            Step::Either(t) => {
                                h.u8(3);
                                h.u64_le(t.len() as u64);
                                h.bytes(t.as_bytes());
                            }
                        }
                    }
                }
            }
        }
        h.u64_le(gin.len() as u64);
        for g in gin {
            let (decl, column, opclass) = g.shape();
            // `decl.columns` is `[column]`.
            let IndexDecl {
                name: _,
                method: m,
                unique,
                columns: _,
                is_deleted: _,
            } = decl;
            h.u8(2);
            h.u8(method(*m));
            h.u8(*unique as u8);
            h.u64_le(column as u64);
            h.u8(match opclass {
                GinOpclass::Ops => 1,
                GinOpclass::PathOps => 2,
            });
        }
        h.finish()
    }

    /// The virtual column a path index keys `doc->>…` under, when one
    /// indexes exactly this path of this document column.
    pub fn path_column(&self, document: u32, steps: &[crate::core::jsonb::Step]) -> Option<u32> {
        self.list.iter().find_map(|i| match &i.path {
            Some((d, s)) if *d == document && s.as_slice() == steps => Some(i.columns[0].0),
            _ => None,
        })
    }

    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) & 1 == 1
    }

    pub fn set_ready(&self, _proof: Proof<'_>, ready: bool) {
        crate::core::index_store::census::mutation(_proof, self as *const Self as usize, "TableIndexes::set_ready");
        let _ = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |s| Some((s & !1) | ready as u64));
    }

    /// The renumbering epoch. A build reads it before its first slot and
    /// publishes with [`TableIndexes::set_ready_at`].
    pub fn epoch(&self) -> u64 {
        self.state.load(Ordering::Acquire) >> 1
    }

    /// Marks the indexes unready and starts a new epoch, in one step. Called
    /// when slot numbers start changing (`Table::compact` phase 2) and again
    /// when they stop, so any build that overlapped the renumbering, or read
    /// its epoch before it, cannot mark ready a structure keyed by slots
    /// that moved. Returns the new epoch.
    pub fn invalidate(&self, _proof: Proof<'_>) -> u64 {
        crate::core::index_store::census::mutation(_proof, self as *const Self as usize, "TableIndexes::invalidate");
        let prev = self
            .state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |s| Some(((s >> 1) + 1) << 1))
            .unwrap_or_else(|s| s);
        (prev >> 1) + 1
    }

    /// Marks ready, but only if the epoch is still `epoch`: no renumbering
    /// began or ended since the caller read it. Returns whether it did.
    pub fn set_ready_at(&self, _proof: Proof<'_>, epoch: u64) -> bool {
        crate::core::index_store::census::mutation(_proof, self as *const Self as usize, "TableIndexes::set_ready_at");
        self.state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |s| (s >> 1 == epoch).then_some(s | 1))
            .is_ok()
    }

    /// The index whose key is exactly these columns, in any order, once built.
    pub fn exact(&self, columns: &[u32]) -> Option<&Arc<LiveIndex>> {
        if !self.is_ready() {
            return None;
        }
        self.list.iter().find(|i| {
            i.columns.len() == columns.len() && columns.iter().all(|c| i.columns.iter().any(|(wo, _)| wo == c))
        })
    }

    /// Whether any index can answer a predicate on this column.
    pub fn covers(&self, write_order: u32) -> bool {
        self.list.iter().any(|i| i.columns.iter().any(|(wo, _)| *wo == write_order))
    }

    /// Every column some index reads, for the fetch vacuum and the build use.
    /// A path index reads its document column, not its virtual one.
    pub fn columns(&self) -> Vec<u32> {
        let mut cols: Vec<u32> = self
            .list
            .iter()
            .flat_map(|i| match &i.path {
                Some((document, _)) => vec![*document],
                None => i.columns.iter().map(|c| c.0).collect(),
            })
            .chain(self.gin.iter().map(|g| g.column))
            .collect();
        cols.sort_unstable();
        cols.dedup();
        cols
    }

    /// Indexes a freshly written version.
    pub fn on_insert(&self, proof: Proof<'_>, slot: u64, row: &RowWrite) {
        crate::core::index_store::census::mutation(proof, self as *const Self as usize, "TableIndexes::on_insert");
        for index in &self.list {
            index.insert(proof, slot, index.key_of_row(row));
        }
        for g in &self.gin {
            g.insert(proof, slot, document_of(row, g.column));
        }
    }

    /// Indexes a version read back from the table (the build).
    pub fn index_row<'a>(
        &self,
        proof: Proof<'_>,
        slot: u64,
        get: &dyn Fn(u32) -> Option<&'a [u8]>,
    ) {
        crate::core::index_store::census::mutation(proof, self as *const Self as usize, "TableIndexes::index_row");
        for index in &self.list {
            index.insert(proof, slot, index.key_of(get));
        }
        for g in &self.gin {
            g.insert(proof, slot, get(g.column));
        }
    }

    /// A version whose columns could not be read: a candidate everywhere.
    pub fn index_unreadable(&self, proof: Proof<'_>, slot: u64) {
        crate::core::index_store::census::mutation(proof, self as *const Self as usize, "TableIndexes::index_unreadable");
        for index in &self.list {
            index.insert(proof, slot, None);
        }
        for g in &self.gin {
            g.insert(proof, slot, None);
        }
    }

    /// The keys a version has, one per index, for removing later.
    pub fn keys_of(&self, row: &RowWrite) -> Vec<Option<KeyBuf>> {
        self.list.iter().map(|i| i.key_of_row(row)).collect()
    }

    /// An in-place overwrite, now durable: the slot moves from its old keys
    /// to its new ones.
    pub fn on_replace(&self, proof: Proof<'_>, slot: u64, old: &[Option<KeyBuf>], row: &RowWrite) {
        crate::core::index_store::census::mutation(proof, self as *const Self as usize, "TableIndexes::on_replace");
        for (i, index) in self.list.iter().enumerate() {
            let new = index.key_of_row(row);
            let old = old.get(i).cloned().flatten();
            if old.as_deref() != new.as_deref() {
                index.insert(proof, slot, new);
                if let Some(old) = old {
                    index.remove(proof, slot, &old);
                }
            } else {
                index.insert(proof, slot, new);
            }
        }
        // The new document's items are added; the old ones stay, a stale
        // candidate rather than a missed row (see `gin`).
        for g in &self.gin {
            g.insert(proof, slot, document_of(row, g.column));
        }
    }

    /// Whether any index holds this slot (vacuum asks, per reclaimable slot).
    pub fn holds(&self, slot: u64) -> bool {
        self.list.iter().any(|i| i.holds(slot)) || self.gin.iter().any(|g| g.holds(slot))
    }

    /// Forgets a reclaimed slot, given its column bytes read back from the row
    /// (or `None` when they could not be read, which removes only what needs
    /// no key).
    pub fn forget<'a>(
        &self,
        proof: Proof<'_>,
        slot: u64,
        get: Option<&dyn Fn(u32) -> Option<&'a [u8]>>,
    ) {
        crate::core::index_store::census::mutation(proof, self as *const Self as usize, "TableIndexes::forget");
        for index in &self.list {
            let key = get.and_then(|g| index.key_of(g));
            index.forget(proof, slot, key.as_deref());
        }
        for g in &self.gin {
            g.forget(proof, slot);
        }
    }

    pub fn clear(&self, proof: Proof<'_>) {
        crate::core::index_store::census::mutation(proof, self as *const Self as usize, "TableIndexes::clear");
        self.list.iter().for_each(|i| i.clear(proof));
        self.gin.iter().for_each(|g| g.clear(proof));
    }

    /// Candidate slots for a predicate tree, sorted and distinct, or `None`
    /// when no index narrows it enough to be worth using.
    pub fn plan(&self, expr: &MaskExpr, fields: &[SchemaField], cap: usize) -> Option<Vec<u64>> {
        let mut out = self.eval(expr, fields, cap)?;
        out.sort_unstable();
        out.dedup();
        Some(out)
    }

    /// Candidate slots an inverted index gives for a clause's JSON operators,
    /// sorted and distinct, or `None` when none can narrow it within `cap`.
    ///
    /// `AND` narrows by every answerable child (the intersection of supersets
    /// is a superset); `OR` needs every branch answerable.
    pub fn plan_json(&self, expr: &gin::JsonExpr, cap: usize) -> Option<Vec<u64>> {
        use gin::JsonExpr;
        if self.gin.is_empty() || !self.is_ready() {
            return None;
        }
        match expr {
            JsonExpr::Opaque => None,
            JsonExpr::Leaf { column, op } => {
                // Each index is asked in turn until one answers. One that
                // declines says nothing about another's answer: a `jsonb_ops`
                // query with an empty container (`{"a":{}}`) has a key item
                // where `jsonb_path_ops` has none at all.
                let answer = self.ask_order(*column, op).iter().find_map(|(_, g, q)| g.lookup(q, cap));
                if answer.is_some() {
                    GIN_LOOKUPS.fetch_add(1, Ordering::Relaxed);
                }
                answer
            }
            JsonExpr::And(children) => {
                let mut answers: Vec<Vec<u64>> = children.iter().filter_map(|c| self.plan_json(c, cap)).collect();
                answers.sort_by_key(|a| a.len());
                let mut it = answers.into_iter();
                let mut acc = it.next()?;
                for next in it {
                    acc = intersect_sorted(&acc, &next);
                }
                Some(acc)
            }
            JsonExpr::Or(children) => {
                let mut out = Vec::new();
                for c in children {
                    out.extend(self.plan_json(c, cap)?);
                    if out.len() > cap {
                        return None;
                    }
                }
                out.sort_unstable();
                out.dedup();
                Some(out)
            }
        }
    }

    /// The inverted indexes that can answer `op` on `column`, each with its
    /// query, in the order to ask them.
    ///
    /// With both operator classes declared, the two answers differ in cost
    /// far more than in size: `{"user":{"id":7}}` is one short list under
    /// `jsonb_path_ops`, and under `jsonb_ops` a value list filtered through
    /// the key lists of `user` and `id`, which every document has. The index
    /// whose shortest list is shortest is asked first, and between equal
    /// bounds the one with less to read.
    fn ask_order(&self, column: u32, op: &gin::JsonOp) -> Vec<((usize, usize), &gin::GinIndex, gin::GinQuery)> {
        let mut asked: Vec<((usize, usize), &gin::GinIndex, gin::GinQuery)> = self
            .gin
            .iter()
            .filter(|g| g.column == column && g.answers(op))
            .filter_map(|g| {
                let q = g.query_for(op)?;
                Some((g.estimate_and_cost(&q), g.as_ref(), q))
            })
            .collect();
        asked.sort_by_key(|(order, ..)| *order);
        asked
    }

    /// The answer [`plan_json`](Self::plan_json) declines as too broad for a
    /// slot list, as one bit per slot below `n_slots` (the table length
    /// sampled before asking), or `None` past `limit` slots.
    ///
    /// Only for a single operator, or for operators `AND`ed together: a
    /// `doc @> '{"status":"paid"}'` that names a fifth of the table is the
    /// case this exists for. Everything else stays as `plan_json` left it, so
    /// bitmaps are never mixed with slot lists or unioned. Any child of an
    /// `AND` may be left out, since the intersection of the rest is still a
    /// superset. The caller asks only after `plan_json` declined every one.
    ///
    /// `AND`s nest: the rewriter guards each operator with its column's
    /// `NULL` companion, so `a @> x AND a @> y` arrives as
    /// `(__null_a = 0 AND a @> x) AND (__null_a = 0 AND a @> y)`.
    pub fn plan_json_dense(&self, expr: &gin::JsonExpr, n_slots: u64, limit: usize) -> Option<Vec<u64>> {
        use gin::JsonExpr;
        if self.gin.is_empty() || !self.is_ready() {
            return None;
        }
        match expr {
            JsonExpr::Leaf { column, op } => {
                let answer = self
                    .ask_order(*column, op)
                    .iter()
                    .find_map(|(_, g, q)| g.lookup_dense(q, n_slots, limit));
                if answer.is_some() {
                    GIN_LOOKUPS.fetch_add(1, Ordering::Relaxed);
                    GIN_DENSE_LOOKUPS.fetch_add(1, Ordering::Relaxed);
                }
                answer
            }
            JsonExpr::And(children) => {
                let mut acc: Option<Vec<u64>> = None;
                for child in children.iter().filter(|c| matches!(c, JsonExpr::Leaf { .. } | JsonExpr::And(_))) {
                    let Some(words) = self.plan_json_dense(child, n_slots, limit) else {
                        continue;
                    };
                    acc = Some(match acc {
                        None => words,
                        Some(mut acc) => {
                            for (a, w) in acc.iter_mut().zip(&words) {
                                *a &= *w;
                            }
                            acc
                        }
                    });
                }
                acc
            }
            JsonExpr::Opaque | JsonExpr::Or(_) => None,
        }
    }

    /// A key column's type: the field's, or `STRING` for a path index's
    /// virtual column, since `->>` yields text.
    fn type_of(&self, fields: &[SchemaField], wo: u32) -> Option<DbType> {
        if self.list.iter().any(|i| i.path.is_some() && i.columns[0].0 == wo) {
            return Some(DbType::STRING);
        }
        fields.iter().find(|f| f.write_order as u32 == wo && !f.is_deleted).map(|f| f.db_type.clone())
    }

    fn eval(&self, expr: &MaskExpr, fields: &[SchemaField], cap: usize) -> Option<Vec<u64>> {
        match expr {
            MaskExpr::All => None,
            MaskExpr::Pred(p) => self.eval_conjunction(&[p], fields, cap),
            MaskExpr::And(children) => {
                let preds: Vec<&SargPredicate> = children
                    .iter()
                    .filter_map(|c| match c {
                        MaskExpr::Pred(p) => Some(p),
                        _ => None,
                    })
                    .collect();
                if let Some(found) = self.eval_conjunction(&preds, fields, cap) {
                    return Some(found);
                }
                // No index answers the predicates at this level: any one
                // child that is answerable bounds the conjunction.
                children
                    .iter()
                    .filter(|c| !matches!(c, MaskExpr::Pred(_)))
                    .find_map(|c| self.eval(c, fields, cap))
            }
            MaskExpr::Or(children) => {
                // A union is only as good as its worst branch: one branch no
                // index can answer means any row may match.
                let mut out = Vec::new();
                for c in children {
                    out.extend(self.eval(c, fields, cap)?);
                    if out.len() > cap {
                        return None;
                    }
                }
                Some(out)
            }
        }
    }

    /// The best index answer for a set of ANDed predicates.
    fn eval_conjunction(&self, preds: &[&SargPredicate], fields: &[SchemaField], cap: usize) -> Option<Vec<u64>> {
        if preds.is_empty() {
            return None;
        }
        let eq_of = |wo: u32| preds.iter().find(|p| p.write_order == wo && p.op == SargOp::Eq);

        // 1. A whole key by equality: the most selective lookup there is.
        //    Prefer the index with the most columns so matched.
        let mut whole: Vec<&Arc<LiveIndex>> = self
            .list
            .iter()
            .filter(|i| i.columns.iter().all(|(wo, _)| eq_of(*wo).is_some()))
            .collect();
        whole.sort_by_key(|i| std::cmp::Reverse(i.columns.len()));
        let keyed: Vec<(&Arc<LiveIndex>, KeyBuf)> = whole
            .into_iter()
            .filter_map(|index| Some((index, index.key_of_values(|wo| eq_of(wo).map(|p| p.value.as_slice()))?)))
            .collect();
        let mut best: Option<Vec<u64>> = None;
        // Which of `keyed` gave `best`, so it is not asked to filter itself.
        let mut best_from: Option<usize> = None;
        for (i, (index, key)) in keyed.iter().enumerate() {
            let mut out = Vec::new();
            if index.lookup_eq(key, &mut out, cap) {
                index.append_unkeyed(&mut out);
                if index.path.is_some() {
                    PATH_LOOKUPS.fetch_add(1, Ordering::Relaxed);
                } else {
                    count_key_lookup();
                }
                best = Some(out);
                best_from = Some(i);
                break;
            }
        }
        // A whole key is usually one row, and then nothing can beat it. It
        // can also be a fifth of the table (`doc->>'status' = 'paid'`), when
        // a range on another index (`id < 2000`) is far smaller: so it is
        // only the answer to beat, and each range below stops reading once
        // it is no longer smaller.
        if best.as_ref().is_some_and(|b| b.len() <= 1) {
            return best;
        }

        // 2. A B-tree constrained on a leading run of its columns: equality
        //    on the first `k`, and any bounds on column `k`, as one range
        //    scan. `cat = 5 AND price > 990` over `(cat, price)` reads the
        //    `cat = 5` keys above `990` and no others.
        'index: for index in self.list.iter().filter(|i| i.method() == Method::BTree) {
            let n = index.columns.len();
            let mut prefix = Vec::new();
            let mut k = 0;
            while k < n {
                let (wo, _) = &index.columns[k];
                let Some(p) = eq_of(*wo) else { break };
                let Some(ty) = self.type_of(fields, *wo) else { continue 'index };
                if key::encode_ordered(&ty, &p.value, k + 1 == n, &mut prefix).is_none() {
                    continue 'index;
                }
                k += 1;
            }
            // Every column fixed is a whole key, which step 1 asked.
            if k == n {
                continue;
            }
            let next = index.columns[k].0;
            let on_next: Vec<&&SargPredicate> = preds.iter().filter(|p| p.write_order == next).collect();
            if k == 0 && on_next.is_empty() {
                continue;
            }
            let Some(ty) = self.type_of(fields, next) else { continue };
            let whole_key = k + 1 == n;
            let mut range = LeadingRange::within(prefix);
            if on_next.iter().any(|p| range.narrow(&ty, p, whole_key).is_none()) {
                continue;
            }
            let (lo, hi) = range.bounds();
            let mut out = Vec::new();
            let limit = best.as_ref().map_or(cap, |b| b.len().saturating_sub(1));
            if !index.lookup_range(lo, hi, &mut out, limit) {
                continue;
            }
            index.append_unkeyed(&mut out);
            if best.as_ref().is_none_or(|b| out.len() < b.len()) {
                if index.path.is_some() {
                    PATH_LOOKUPS.fetch_add(1, Ordering::Relaxed);
                } else {
                    count_key_lookup();
                }
                best = Some(out);
                best_from = None;
            }
        }

        // 3. Every other whole key the conjunction fixes still narrows the
        //    answer: a row matching all of it is listed under each such key
        //    (or unkeyed), so a candidate under none of them cannot match.
        //    `status = 'paid' AND id < 2000` reads the two thousand from the
        //    primary key and keeps the fifth of them listed under 'paid'
        //    rather than fetching every one.
        //    Shortest list first, so each narrows what the next is walked
        //    against. A list of half the table or more keeps too much to pay
        //    for the walk (`cap` is an eighth of the table), and is skipped.
        if let Some(found) = best.as_mut() {
            let mut filters: Vec<(usize, usize)> = keyed
                .iter()
                .enumerate()
                .filter(|(i, _)| best_from != Some(*i))
                .map(|(i, (index, key))| (index.listed_under(key), i))
                .filter(|(listed, _)| *listed < cap.saturating_mul(4))
                .collect();
            if !filters.is_empty() && found.len() > 1 {
                found.sort_unstable();
                found.dedup();
                filters.sort_unstable();
                for (_, i) in filters {
                    if found.len() <= 1 {
                        break;
                    }
                    let (index, key) = &keyed[i];
                    index.retain_under(key, found);
                }
            }
        }
        best
    }
}

/// The live indexes a table has: what `CREATE INDEX` declared, and a B-tree
/// (or, for a type with no order, a hash) for each `PRIMARY KEY`, `UNIQUE`
/// column and composite unique constraint, as PostgreSQL creates one for
/// each.
///
/// A declaration whose column has since been dropped, or whose types no
/// longer allow its method, is left out with a warning rather than built
/// wrongly.
/// The shape ([`TableIndexes::shape_hash`]) of the indexes [`for_schema`]
/// builds for `schema`: what a persisted snapshot records, and what it is
/// checked against at open.
pub fn shape_of(schema: &crate::core::row::schema::TableSchema) -> u64 {
    for_schema(schema).shape_hash(&schema.fields)
}

pub fn for_schema(schema: &crate::core::row::schema::TableSchema) -> TableIndexes {
    let live = |wo: u32| {
        schema
            .fields
            .iter()
            .find(|f| f.write_order as u32 == wo && !f.is_deleted)
    };
    let by_name = |name: &str| {
        schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
            .map(|f| f.write_order as u32)
    };
    let mut out: Vec<Arc<LiveIndex>> = Vec::new();
    let add = |decl: IndexDecl, implicit: bool, out: &mut Vec<Arc<LiveIndex>>| {
        if out.iter().any(|i| i.decl.columns == decl.columns && i.decl.method == decl.method) {
            return;
        }
        let mut columns = Vec::with_capacity(decl.columns.len());
        for wo in &decl.columns {
            match live(*wo) {
                Some(f) => columns.push((*wo, f.db_type.clone())),
                None => {
                    if !implicit {
                        log::warn!(
                            "index {} on {} names a column that no longer exists; not built",
                            decl.name,
                            schema.name
                        );
                    }
                    return;
                }
            }
        }
        let types: Vec<DbType> = columns.iter().map(|c| c.1.clone()).collect();
        let requested = if implicit { None } else { Some(decl.method) };
        match choose_method(requested, &types) {
            Ok(method) => {
                let decl = IndexDecl { method, ..decl };
                out.push(Arc::new(LiveIndex::new(decl, columns, implicit)));
            }
            Err(e) => {
                if !implicit {
                    log::warn!("index {} on {} not built: {}", decl.name, schema.name, e);
                }
            }
        }
    };
    let mut gins: Vec<Arc<gin::GinIndex>> = Vec::new();
    for decl in &schema.secondary_indexes {
        if let Some(opclass) = decl.method.opclass() {
            let column = decl.columns.first().copied();
            match column.and_then(live) {
                Some(f) if decl.columns.len() == 1 && f.db_type == DbType::JSONB => {
                    gins.push(Arc::new(gin::GinIndex::new(decl.clone(), f.write_order as u32, opclass)));
                }
                _ => log::warn!(
                    "gin index {} on {} no longer names one jsonb column; not built",
                    decl.name,
                    schema.name
                ),
            }
            continue;
        }
        add(decl.clone(), false, &mut out);
    }
    // Paths declared with `CREATE INDEX ((doc->>'k'))`: a B-tree over the
    // text each path reads, under a virtual column of its own.
    let mut next_path_column = FIRST_PATH_COLUMN;
    for declared in schema.path_indexes.iter().filter(|p| !p.is_deleted) {
        let Some(path) = crate::core::jsonb::parse_path_expression(&declared.path) else {
            log::warn!("path index {} on {} has an unreadable path; not built", declared.name, schema.name);
            continue;
        };
        let document = schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&path.column) && f.db_type == DbType::JSONB);
        let Some(document) = document else {
            log::warn!(
                "path index {} on {} names no jsonb column {}; not built",
                declared.name,
                schema.name,
                path.column
            );
            continue;
        };
        let decl = IndexDecl {
            name: declared.name.clone(),
            method: Method::BTree,
            unique: false,
            columns: vec![next_path_column],
            is_deleted: false,
        };
        out.push(Arc::new(LiveIndex::over_path(
            decl,
            next_path_column,
            document.write_order as u32,
            path.steps,
        )));
        next_path_column -= 1;
    }
    let implicit = |name: String, columns: Vec<u32>| IndexDecl {
        name,
        method: Method::BTree,
        unique: true,
        columns,
        is_deleted: false,
    };
    if let Some(pk) = schema.primary_key.as_deref().and_then(by_name) {
        add(implicit(format!("{}_pkey", schema.name), vec![pk]), true, &mut out);
    }
    for f in schema.fields.iter().filter(|f| f.is_unique && !f.is_deleted) {
        add(
            implicit(format!("{}_{}_key", schema.name, f.name), vec![f.write_order as u32]),
            true,
            &mut out,
        );
    }
    // A partial unique key gets the same index as a full one, over every row,
    // not only the rows its predicate holds for. That keeps it a superset for
    // any query, so the planner may use it without knowing about predicates;
    // only the uniqueness check reads the predicate.
    for uk in schema.unique_keys.iter().filter(|u| !u.is_deleted) {
        let columns: Option<Vec<u32>> = uk.columns.iter().map(|c| by_name(c)).collect();
        if let Some(columns) = columns.filter(|c| !c.is_empty() && c.len() <= decl::MAX_COLUMNS) {
            add(implicit(uk.name.clone(), columns), true, &mut out);
        }
    }
    TableIndexes::with_gin(out, gins)
}

/// How the method is chosen when `CREATE INDEX` names none, and whether the
/// types allow the one asked for.
pub fn choose_method(requested: Option<Method>, types: &[DbType]) -> Result<Method, String> {
    let all = |f: fn(&DbType) -> bool| types.iter().all(f);
    match requested {
        Some(m @ (Method::GinOps | Method::GinPathOps)) => {
            if types.len() == 1 && types[0] == DbType::JSONB {
                Ok(m)
            } else {
                Err("a gin index covers exactly one jsonb column".to_string())
            }
        }
        Some(Method::BTree) => {
            if let Some(t) = types.iter().find(|t| !key::btree_supported(t)) {
                return Err(format!(
                    "a B-tree cannot order a {t:?} column: its values have no order this engine's \
                     comparisons and an index could both keep; use USING hash for equality lookups"
                ));
            }
            Ok(Method::BTree)
        }
        Some(Method::Hash) => {
            if let Some(t) = types.iter().find(|t| !key::hash_supported(t)) {
                return Err(format!(
                    "a hash index cannot key a {t:?} column: two equal values of that type can \
                     differ in their bytes, so a lookup by one would miss the other"
                ));
            }
            Ok(Method::Hash)
        }
        None => {
            if all(key::equality_only) && all(key::hash_supported) {
                Ok(Method::Hash)
            } else if all(key::btree_supported) {
                Ok(Method::BTree)
            } else if all(key::hash_supported) {
                Ok(Method::Hash)
            } else {
                let t = types
                    .iter()
                    .find(|t| !key::hash_supported(t))
                    .expect("some type is unsupported");
                Err(format!(
                    "a {t:?} column cannot be indexed: neither an order nor an equality on its bytes \
                     matches how this engine compares it"
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) mod vectors;
