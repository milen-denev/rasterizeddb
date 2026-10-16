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

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ahash::RandomState;
use parking_lot::RwLock;
use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::filter::SargOp;
use crate::core::filter::sarg::{MaskExpr, SargPredicate};
use crate::core::row::row::RowWrite;
use crate::core::row::schema::SchemaField;

pub use decl::{IndexDecl, Method};

/// A key, inline when short (most are: an integer, a UUID, a short string).
pub type KeyBuf = SmallVec<[u8; 24]>;
type Slots = SmallVec<[u64; 2]>;

const HASH_SHARDS: usize = 64;

/// Above this many candidates an index answer is not worth returning: the
/// filter or a plain scan reads that many rows about as cheaply, and the
/// candidate list itself becomes the cost.
pub fn candidate_cap(n_slots: u64) -> usize {
    ((n_slots / 8) as usize).max(4096)
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
    pub fn insert(&self, slot: u64, key: Option<KeyBuf>) {
        match key {
            None => {
                let mut unkeyed = self.unkeyed.write();
                if !unkeyed.contains(&slot) {
                    unkeyed.push(slot);
                }
            }
            Some(key) => match &self.data {
                Data::BTree(map) => {
                    let mut map = map.write();
                    let slots = map.entry(key).or_default();
                    if !slots.contains(&slot) {
                        slots.push(slot);
                    }
                }
                Data::Hash(shards, _) => {
                    let mut map = shards[self.shard(&key)].write();
                    let slots = map.entry(key).or_default();
                    if !slots.contains(&slot) {
                        slots.push(slot);
                    }
                }
            },
        }
        self.set_present(slot, true);
    }

    /// Removes `slot` from under `key`. Removing a pair that is not there is a
    /// no-op, which is what makes a late or repeated removal harmless.
    pub fn remove(&self, slot: u64, key: &[u8]) {
        fn drop_slot<M: MapLike>(map: &mut M, key: &[u8], slot: u64) {
            let empty = match map.slots_mut(key) {
                Some(slots) => {
                    slots.retain(|s| *s != slot);
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
    pub fn forget(&self, slot: u64, key: Option<&[u8]>) {
        if let Some(key) = key {
            self.remove(slot, key);
        }
        self.unkeyed.write().retain(|s| *s != slot);
        self.set_present(slot, false);
    }

    pub fn clear(&self) {
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

    /// Appends the slots under exactly `key`. `false` when `cap` was exceeded.
    fn lookup_eq(&self, key: &[u8], out: &mut Vec<u64>, cap: usize) -> bool {
        let slots: Option<Slots> = match &self.data {
            Data::BTree(map) => map.read().get(key).cloned(),
            Data::Hash(shards, _) => shards[self.shard(key)].read().get(key).cloned(),
        };
        if let Some(slots) = slots {
            out.extend_from_slice(&slots);
        }
        out.len() <= cap
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
    ready: AtomicBool,
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
            ready: AtomicBool::new(false),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.list.is_empty() && self.gin.is_empty()
    }

    /// How many indexes there are, of either kind.
    pub fn len(&self) -> usize {
        self.list.len() + self.gin.len()
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
        self.ready.load(Ordering::Acquire)
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
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
    pub fn on_insert(&self, slot: u64, row: &RowWrite) {
        for index in &self.list {
            index.insert(slot, index.key_of_row(row));
        }
        for g in &self.gin {
            g.insert(slot, document_of(row, g.column));
        }
    }

    /// Indexes a version read back from the table (the build).
    pub fn index_row<'a>(&self, slot: u64, get: &dyn Fn(u32) -> Option<&'a [u8]>) {
        for index in &self.list {
            index.insert(slot, index.key_of(get));
        }
        for g in &self.gin {
            g.insert(slot, get(g.column));
        }
    }

    /// A version whose columns could not be read: a candidate everywhere.
    pub fn index_unreadable(&self, slot: u64) {
        for index in &self.list {
            index.insert(slot, None);
        }
        for g in &self.gin {
            g.insert(slot, None);
        }
    }

    /// The keys a version has, one per index, for removing later.
    pub fn keys_of(&self, row: &RowWrite) -> Vec<Option<KeyBuf>> {
        self.list.iter().map(|i| i.key_of_row(row)).collect()
    }

    /// An in-place overwrite, now durable: the slot moves from its old keys
    /// to its new ones.
    pub fn on_replace(&self, slot: u64, old: &[Option<KeyBuf>], row: &RowWrite) {
        for (i, index) in self.list.iter().enumerate() {
            let new = index.key_of_row(row);
            let old = old.get(i).cloned().flatten();
            if old.as_deref() != new.as_deref() {
                index.insert(slot, new);
                if let Some(old) = old {
                    index.remove(slot, &old);
                }
            } else {
                index.insert(slot, new);
            }
        }
        // The new document's items are added; the old ones stay, a stale
        // candidate rather than a missed row (see `gin`).
        for g in &self.gin {
            g.insert(slot, document_of(row, g.column));
        }
    }

    /// Whether any index holds this slot (vacuum asks, per reclaimable slot).
    pub fn holds(&self, slot: u64) -> bool {
        self.list.iter().any(|i| i.holds(slot)) || self.gin.iter().any(|g| g.holds(slot))
    }

    /// Forgets a reclaimed slot, given its column bytes read back from the row
    /// (or `None` when they could not be read, which removes only what needs
    /// no key).
    pub fn forget<'a>(&self, slot: u64, get: Option<&dyn Fn(u32) -> Option<&'a [u8]>>) {
        for index in &self.list {
            let key = get.and_then(|g| index.key_of(g));
            index.forget(slot, key.as_deref());
        }
        for g in &self.gin {
            g.forget(slot);
        }
    }

    pub fn clear(&self) {
        self.list.iter().for_each(|i| i.clear());
        self.gin.iter().for_each(|g| g.clear());
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
                let answer = self
                    .gin
                    .iter()
                    .filter(|g| g.column == *column && g.answers(op))
                    .filter_map(|g| g.lookup(&g.query_for(op)?, cap))
                    .min_by_key(|answer| answer.len());
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
        for index in whole {
            let key = index.key_of_values(|wo| eq_of(wo).map(|p| p.value.as_slice()));
            if let Some(key) = key {
                let mut out = Vec::new();
                if index.lookup_eq(&key, &mut out, cap) {
                    index.append_unkeyed(&mut out);
                    if index.path.is_some() {
                        PATH_LOOKUPS.fetch_add(1, Ordering::Relaxed);
                    }
                    return Some(out);
                }
            }
        }

        // 2. A B-tree constrained on a leading run of its columns: equality
        //    on the first `k`, and any bounds on column `k`, as one range
        //    scan. `cat = 5 AND price > 990` over `(cat, price)` reads the
        //    `cat = 5` keys above `990` and no others.
        let mut best: Option<Vec<u64>> = None;
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
            if !index.lookup_range(lo, hi, &mut out, cap) {
                continue;
            }
            index.append_unkeyed(&mut out);
            if best.as_ref().is_none_or(|b| out.len() < b.len()) {
                if index.path.is_some() {
                    PATH_LOOKUPS.fetch_add(1, Ordering::Relaxed);
                }
                best = Some(out);
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
