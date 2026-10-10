//! What a key can hold, each structure keeping count of its own bytes.
//!
//! The store charges every key for what it holds so `maxmemory` can be
//! enforced, and a structure is mutated a field at a time. If the charge were
//! recomputed by walking the structure, an `HSET` on a hash of a million
//! fields would cost a million steps. So every structure keeps a running
//! byte total that each insert and removal adjusts, and the store reads it in
//! O(1) before and after a command to charge the difference.
//!
//! The totals are estimates. They count the bytes held plus a fixed overhead
//! per element, which approximates the allocator's header and the table's
//! slot. They are close enough to make `maxmemory` mean what it says.

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};

use ahash::RandomState;

/// Charged per element beyond its own bytes: the `Box` (16), the allocator's
/// header (16), and the table or deque slot it sits in.
pub const ELEMENT_OVERHEAD: usize = 48;

pub fn hasher() -> RandomState {
    // One seed for every structure in the process: the seeds only need to be
    // unpredictable to clients, not distinct per hash.
    static SEED: std::sync::OnceLock<RandomState> = std::sync::OnceLock::new();
    SEED.get_or_init(RandomState::new).clone()
}

// ---------------------------------------------------------------------------
// Hash
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
pub struct HashVal {
    map: HashMap<Box<[u8]>, Box<[u8]>, RandomState>,
    bytes: usize,
}

impl HashVal {
    pub fn new() -> Self {
        HashVal {
            map: HashMap::with_hasher(hasher()),
            bytes: 0,
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        HashVal {
            map: HashMap::with_capacity_and_hasher(n, hasher()),
            bytes: 0,
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn get(&self, field: &[u8]) -> Option<&[u8]> {
        self.map.get(field).map(|v| &**v)
    }

    /// Sets `field`; `true` when it is new.
    pub fn insert(&mut self, field: &[u8], value: &[u8]) -> bool {
        match self.map.get_mut(field) {
            Some(old) => {
                self.bytes = self.bytes - old.len() + value.len();
                *old = value.into();
                false
            }
            None => {
                self.bytes += field.len() + value.len() + ELEMENT_OVERHEAD;
                self.map.insert(field.into(), value.into());
                true
            }
        }
    }

    pub fn remove(&mut self, field: &[u8]) -> bool {
        match self.map.remove(field) {
            Some(old) => {
                self.bytes -= field.len() + old.len() + ELEMENT_OVERHEAD;
                true
            }
            None => false,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.map.iter().map(|(f, v)| (&**f, &**v))
    }
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
pub struct ListVal {
    items: VecDeque<Box<[u8]>>,
    bytes: usize,
}

impl ListVal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn push_front(&mut self, value: &[u8]) {
        self.bytes += value.len() + ELEMENT_OVERHEAD;
        self.items.push_front(value.into());
    }

    pub fn push_back(&mut self, value: &[u8]) {
        self.bytes += value.len() + ELEMENT_OVERHEAD;
        self.items.push_back(value.into());
    }

    pub fn pop_front(&mut self) -> Option<Box<[u8]>> {
        let v = self.items.pop_front()?;
        self.bytes -= v.len() + ELEMENT_OVERHEAD;
        Some(v)
    }

    pub fn pop_back(&mut self) -> Option<Box<[u8]>> {
        let v = self.items.pop_back()?;
        self.bytes -= v.len() + ELEMENT_OVERHEAD;
        Some(v)
    }

    pub fn get(&self, index: usize) -> Option<&[u8]> {
        self.items.get(index).map(|v| &**v)
    }

    pub fn set(&mut self, index: usize, value: &[u8]) -> bool {
        match self.items.get_mut(index) {
            Some(slot) => {
                self.bytes = self.bytes - slot.len() + value.len();
                *slot = value.into();
                true
            }
            None => false,
        }
    }

    pub fn insert(&mut self, index: usize, value: &[u8]) {
        self.bytes += value.len() + ELEMENT_OVERHEAD;
        self.items.insert(index, value.into());
    }

    pub fn remove(&mut self, index: usize) -> Option<Box<[u8]>> {
        let v = self.items.remove(index)?;
        self.bytes -= v.len() + ELEMENT_OVERHEAD;
        Some(v)
    }

    /// Keeps `start..=stop` (already normalised and in range).
    pub fn retain_range(&mut self, start: usize, stop_exclusive: usize) {
        let stop_exclusive = stop_exclusive.min(self.items.len());
        if start >= stop_exclusive {
            self.items.clear();
            self.bytes = 0;
            return;
        }
        for v in self.items.drain(stop_exclusive..) {
            self.bytes -= v.len() + ELEMENT_OVERHEAD;
        }
        for v in self.items.drain(..start) {
            self.bytes -= v.len() + ELEMENT_OVERHEAD;
        }
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &[u8]> + ExactSizeIterator {
        self.items.iter().map(|v| &**v)
    }

    pub fn range(&self, start: usize, stop_exclusive: usize) -> impl Iterator<Item = &[u8]> {
        self.items.range(start..stop_exclusive).map(|v| &**v)
    }

    /// `LREM`: removes up to `count` elements equal to `value` (0 is all,
    /// negative counts from the tail). Returns how many went.
    pub fn remove_matching(&mut self, value: &[u8], count: i64) -> usize {
        let limit = if count == 0 { usize::MAX } else { count.unsigned_abs() as usize };
        let mut removed = 0usize;
        if count >= 0 {
            let mut i = 0;
            while i < self.items.len() && removed < limit {
                if *self.items[i] == *value {
                    self.remove(i);
                    removed += 1;
                } else {
                    i += 1;
                }
            }
        } else {
            let mut i = self.items.len();
            while i > 0 && removed < limit {
                i -= 1;
                if *self.items[i] == *value {
                    self.remove(i);
                    removed += 1;
                }
            }
        }
        removed
    }

    pub fn position(&self, value: &[u8]) -> Option<usize> {
        self.items.iter().position(|v| **v == *value)
    }
}

// ---------------------------------------------------------------------------
// Set
// ---------------------------------------------------------------------------

/// Members in a hashbrown table rather than a `HashSet`, so `SPOP` and
/// `SRANDMEMBER` can sample a bucket in O(1) instead of walking to the nth.
#[derive(Clone)]
pub struct SetVal {
    table: hashbrown::HashTable<Box<[u8]>>,
    hasher: RandomState,
    bytes: usize,
}

impl Default for SetVal {
    fn default() -> Self {
        Self::new()
    }
}

impl SetVal {
    pub fn new() -> Self {
        SetVal {
            table: hashbrown::HashTable::new(),
            hasher: hasher(),
            bytes: 0,
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        SetVal {
            table: hashbrown::HashTable::with_capacity(n),
            hasher: hasher(),
            bytes: 0,
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn contains(&self, member: &[u8]) -> bool {
        let h = self.hasher.hash_one(member);
        self.table.find(h, |m| **m == *member).is_some()
    }

    pub fn insert(&mut self, member: &[u8]) -> bool {
        let h = self.hasher.hash_one(member);
        if self.table.find(h, |m| **m == *member).is_some() {
            return false;
        }
        self.bytes += member.len() + ELEMENT_OVERHEAD;
        let hasher = &self.hasher;
        self.table
            .insert_unique(h, member.into(), |m| hasher.hash_one(&**m));
        true
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        let h = self.hasher.hash_one(member);
        match self.table.find_entry(h, |m| **m == *member) {
            Ok(entry) => {
                entry.remove();
                self.bytes -= member.len() + ELEMENT_OVERHEAD;
                true
            }
            Err(_) => false,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.table.iter().map(|v| &**v)
    }

    /// A member near bucket `r`: the first occupied bucket at or after it.
    pub fn random(&self, r: u64) -> Option<&[u8]> {
        let buckets = self.table.num_buckets();
        if self.table.is_empty() || buckets == 0 {
            return None;
        }
        let start = (r as usize) % buckets;
        (0..buckets)
            .map(|i| (start + i) % buckets)
            .find_map(|i| self.table.get_bucket(i))
            .map(|v| &**v)
    }

    /// Removes and returns a member near bucket `r`.
    pub fn pop_random(&mut self, r: u64) -> Option<Box<[u8]>> {
        let buckets = self.table.num_buckets();
        if self.table.is_empty() || buckets == 0 {
            return None;
        }
        let start = (r as usize) % buckets;
        let index = (0..buckets)
            .map(|i| (start + i) % buckets)
            .find(|&i| self.table.get_bucket(i).is_some())?;
        let entry = self.table.get_bucket_entry(index).ok()?;
        let (member, _) = entry.remove();
        self.bytes -= member.len() + ELEMENT_OVERHEAD;
        Some(member)
    }
}

// ---------------------------------------------------------------------------
// Sorted set
// ---------------------------------------------------------------------------

/// Members ordered by `(score, member)`, with O(1) score lookup and a rank
/// in O(n / CHUNK).
///
/// Redis uses a skip list with span counts. This is a list of sorted chunks of
/// at most `CHUNK` entries: an insert is a binary search for the chunk, a
/// shift inside it, and a split when it overflows. A rank is the sum of the
/// lengths of the chunks before it plus a position inside one. A set of a
/// million members is about 4,000 chunks, so a rank sums 4,000 lengths, which
/// takes about a microsecond. No pointer chasing, and a contiguous chunk
/// serves a range read.
#[derive(Clone)]
pub struct ZSet {
    scores: HashMap<Box<[u8]>, f64, RandomState>,
    chunks: Vec<Vec<(f64, Box<[u8]>)>>,
    len: usize,
    bytes: usize,
}

const CHUNK: usize = 256;

impl Default for ZSet {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn order(a: (f64, &[u8]), b: (f64, &[u8])) -> Ordering {
    a.0.total_cmp(&b.0).then_with(|| a.1.cmp(b.1))
}

/// A score bound for `ZRANGEBYSCORE` and friends.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScoreBound {
    pub value: f64,
    pub exclusive: bool,
}

impl ScoreBound {
    fn admits_low(&self, score: f64) -> bool {
        if self.exclusive {
            score > self.value
        } else {
            score >= self.value
        }
    }
    fn admits_high(&self, score: f64) -> bool {
        if self.exclusive {
            score < self.value
        } else {
            score <= self.value
        }
    }
}

/// A member bound for `ZRANGEBYLEX`: `-`, `+`, `[x` or `(x`.
#[derive(Clone, Debug, PartialEq)]
pub enum LexBound {
    NegInf,
    PosInf,
    Incl(Vec<u8>),
    Excl(Vec<u8>),
}

impl LexBound {
    fn admits_low(&self, m: &[u8]) -> bool {
        match self {
            LexBound::NegInf => true,
            LexBound::PosInf => false,
            LexBound::Incl(v) => m >= v.as_slice(),
            LexBound::Excl(v) => m > v.as_slice(),
        }
    }
    fn admits_high(&self, m: &[u8]) -> bool {
        match self {
            LexBound::NegInf => false,
            LexBound::PosInf => true,
            LexBound::Incl(v) => m <= v.as_slice(),
            LexBound::Excl(v) => m < v.as_slice(),
        }
    }
}

impl ZSet {
    pub fn new() -> Self {
        ZSet {
            scores: HashMap::with_hasher(hasher()),
            chunks: Vec::new(),
            len: 0,
            bytes: 0,
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.scores.get(member).copied()
    }

    /// The chunk an entry belongs in: the first whose last entry is not below
    /// it, or the last chunk when it sorts after everything.
    fn chunk_for(&self, score: f64, member: &[u8]) -> usize {
        let i = self.chunks.partition_point(|c| {
            let last = c.last().expect("chunks are never empty");
            order((last.0, &last.1), (score, member)) == Ordering::Less
        });
        i.min(self.chunks.len().saturating_sub(1))
    }

    fn position(&self, score: f64, member: &[u8]) -> Option<(usize, usize)> {
        if self.chunks.is_empty() {
            return None;
        }
        let c = self.chunk_for(score, member);
        let chunk = &self.chunks[c];
        chunk
            .binary_search_by(|e| order((e.0, &e.1), (score, member)))
            .ok()
            .map(|i| (c, i))
    }

    fn insert_ordered(&mut self, score: f64, member: &[u8]) {
        if self.chunks.is_empty() {
            self.chunks.push(vec![(score, member.into())]);
            return;
        }
        let c = self.chunk_for(score, member);
        let chunk = &mut self.chunks[c];
        let i = chunk.partition_point(|e| order((e.0, &e.1), (score, member)) == Ordering::Less);
        chunk.insert(i, (score, member.into()));
        if chunk.len() > CHUNK {
            let tail = chunk.split_off(CHUNK / 2);
            self.chunks.insert(c + 1, tail);
        }
    }

    fn remove_ordered(&mut self, score: f64, member: &[u8]) {
        if let Some((c, i)) = self.position(score, member) {
            self.chunks[c].remove(i);
            if self.chunks[c].is_empty() {
                self.chunks.remove(c);
            } else if self.chunks[c].len() < CHUNK / 8 && c + 1 < self.chunks.len() {
                // Fold a near-empty chunk into its successor so deletions do
                // not leave thousands of tiny chunks for a rank to sum.
                if self.chunks[c].len() + self.chunks[c + 1].len() <= CHUNK {
                    let next = self.chunks.remove(c + 1);
                    self.chunks[c].extend(next);
                }
            }
        }
    }

    /// Adds or rescores `member`. Returns the previous score, if any.
    pub fn insert(&mut self, member: &[u8], score: f64) -> Option<f64> {
        match self.scores.get(member).copied() {
            Some(old) => {
                if old.total_cmp(&score) != Ordering::Equal {
                    self.remove_ordered(old, member);
                    self.insert_ordered(score, member);
                    *self.scores.get_mut(member).expect("present") = score;
                }
                Some(old)
            }
            None => {
                self.insert_ordered(score, member);
                self.scores.insert(member.into(), score);
                self.len += 1;
                // The member is held twice: in the score map and in its chunk.
                self.bytes += 2 * member.len() + 2 * ELEMENT_OVERHEAD;
                None
            }
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self.scores.remove(member) {
            Some(score) => {
                self.remove_ordered(score, member);
                self.len -= 1;
                self.bytes -= 2 * member.len() + 2 * ELEMENT_OVERHEAD;
                true
            }
            None => false,
        }
    }

    /// 0-based rank in ascending order.
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.score(member)?;
        let (c, i) = self.position(score, member)?;
        Some(self.chunks[..c].iter().map(Vec::len).sum::<usize>() + i)
    }

    /// The entry at ascending rank `r`.
    pub fn at(&self, mut r: usize) -> Option<(f64, &[u8])> {
        for chunk in &self.chunks {
            if r < chunk.len() {
                let e = &chunk[r];
                return Some((e.0, &e.1));
            }
            r -= chunk.len();
        }
        None
    }

    /// Ascending iterator from rank `start`.
    pub fn iter_from(&self, start: usize) -> impl Iterator<Item = (f64, &[u8])> {
        let mut skip = start;
        let mut first_chunk = self.chunks.len();
        for (ci, chunk) in self.chunks.iter().enumerate() {
            if skip < chunk.len() {
                first_chunk = ci;
                break;
            }
            skip -= chunk.len();
        }
        self.chunks
            .iter()
            .skip(first_chunk)
            .enumerate()
            .flat_map(move |(n, chunk)| {
                let from = if n == 0 { skip } else { 0 };
                chunk[from.min(chunk.len())..].iter().map(|e| (e.0, &*e.1))
            })
    }

    /// Descending iterator from ascending rank `start` downward.
    pub fn iter_rev_from(&self, start: usize) -> impl Iterator<Item = (f64, &[u8])> {
        let take = if self.len == 0 { 0 } else { start.min(self.len - 1) + 1 };
        let mut budget = take;
        // Walk chunks from the front to find the chunk holding `start`, then
        // go backwards from there.
        let mut acc = 0usize;
        let mut end_chunk = 0usize;
        let mut end_index = 0usize;
        for (ci, chunk) in self.chunks.iter().enumerate() {
            if take > 0 && acc + chunk.len() >= take {
                end_chunk = ci;
                end_index = take - acc; // exclusive
                break;
            }
            acc += chunk.len();
        }
        let chunks = &self.chunks;
        (0..if take == 0 { 0 } else { end_chunk + 1 })
            .rev()
            .flat_map(move |ci| {
                let chunk = &chunks[ci];
                let hi = if ci == end_chunk { end_index } else { chunk.len() };
                chunk[..hi].iter().rev().map(|e| (e.0, &*e.1))
            })
            .take_while(move |_| {
                if budget == 0 {
                    false
                } else {
                    budget -= 1;
                    true
                }
            })
    }

    /// Rank of the first entry whose score `min` admits.
    pub fn first_rank_by_score(&self, min: ScoreBound) -> usize {
        let mut rank = 0usize;
        for chunk in &self.chunks {
            let last = chunk.last().expect("non-empty");
            if !min.admits_low(last.0) {
                rank += chunk.len();
                continue;
            }
            return rank + chunk.partition_point(|e| !min.admits_low(e.0));
        }
        rank
    }

    /// Rank one past the last entry whose score `max` admits.
    pub fn end_rank_by_score(&self, max: ScoreBound) -> usize {
        let mut rank = 0usize;
        for chunk in &self.chunks {
            let last = chunk.last().expect("non-empty");
            if max.admits_high(last.0) {
                rank += chunk.len();
                continue;
            }
            return rank + chunk.partition_point(|e| max.admits_high(e.0));
        }
        rank
    }

    pub fn first_rank_by_lex(&self, min: &LexBound) -> usize {
        let mut rank = 0usize;
        for chunk in &self.chunks {
            let last = chunk.last().expect("non-empty");
            if !min.admits_low(&last.1) {
                rank += chunk.len();
                continue;
            }
            return rank + chunk.partition_point(|e| !min.admits_low(&e.1));
        }
        rank
    }

    pub fn end_rank_by_lex(&self, max: &LexBound) -> usize {
        let mut rank = 0usize;
        for chunk in &self.chunks {
            let last = chunk.last().expect("non-empty");
            if max.admits_high(&last.1) {
                rank += chunk.len();
                continue;
            }
            return rank + chunk.partition_point(|e| max.admits_high(&e.1));
        }
        rank
    }

    /// Removes ascending ranks `start..end`. Returns how many went.
    pub fn remove_rank_range(&mut self, start: usize, end: usize) -> usize {
        if start >= end {
            return 0;
        }
        let doomed: Vec<Box<[u8]>> = self
            .iter_from(start)
            .take(end - start)
            .map(|(_, m)| Box::from(m))
            .collect();
        for m in &doomed {
            self.remove(m);
        }
        doomed.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (f64, &[u8])> {
        self.iter_from(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zset_orders_by_score_then_member_and_ranks_across_chunks() {
        let mut z = ZSet::new();
        // Enough members to split into many chunks, inserted out of order.
        let n = 5_000usize;
        for i in (0..n).rev() {
            let m = format!("m{i:05}");
            assert_eq!(z.insert(m.as_bytes(), (i % 97) as f64), None);
        }
        assert_eq!(z.len(), n);
        let all: Vec<(f64, Vec<u8>)> = z.iter().map(|(s, m)| (s, m.to_vec())).collect();
        let mut sorted = all.clone();
        sorted.sort_by(|a, b| order((a.0, &a.1), (b.0, &b.1)));
        assert_eq!(all, sorted);
        for (rank, (_, m)) in all.iter().enumerate().step_by(37) {
            assert_eq!(z.rank(m), Some(rank));
            assert_eq!(z.at(rank).map(|(_, m)| m.to_vec()), Some(m.clone()));
        }
        // Rescore moves the member; removal shrinks.
        z.insert(b"m00000", 1000.0);
        assert_eq!(z.rank(b"m00000"), Some(n - 1));
        assert!(z.remove(b"m00000"));
        assert_eq!(z.len(), n - 1);
        assert_eq!(z.rank(b"m00000"), None);
        // Score ranges agree with a filter.
        let lo = ScoreBound { value: 10.0, exclusive: true };
        let hi = ScoreBound { value: 20.0, exclusive: false };
        let (a, b) = (z.first_rank_by_score(lo), z.end_rank_by_score(hi));
        let expect = z.iter().filter(|(s, _)| *s > 10.0 && *s <= 20.0).count();
        assert_eq!(b - a, expect);
        // Reverse iteration is the forward order reversed.
        let rev: Vec<Vec<u8>> = z.iter_rev_from(z.len() - 1).map(|(_, m)| m.to_vec()).collect();
        let mut fwd: Vec<Vec<u8>> = z.iter().map(|(_, m)| m.to_vec()).collect();
        fwd.reverse();
        assert_eq!(rev, fwd);
        let rev3: Vec<Vec<u8>> = z.iter_rev_from(2).map(|(_, m)| m.to_vec()).collect();
        assert_eq!(rev3.len(), 3);
        assert_eq!(rev3[0], z.at(2).unwrap().1.to_vec());
        assert_eq!(rev3[2], z.at(0).unwrap().1.to_vec());
    }

    #[test]
    fn empty_set_takes_its_first_member_and_refills() {
        let mut z = ZSet::new();
        assert_eq!(z.insert(b"a", 1.0), None);
        assert_eq!(z.rank(b"a"), Some(0));
        assert!(z.remove(b"a"));
        assert!(z.is_empty());
        assert_eq!(z.insert(b"b", 2.0), None);
        assert_eq!(z.at(0).map(|(s, m)| (s, m.to_vec())), Some((2.0, b"b".to_vec())));
    }

    #[test]
    fn byte_totals_return_to_zero() {
        let mut h = HashVal::new();
        h.insert(b"a", b"1");
        h.insert(b"a", b"22");
        h.insert(b"b", b"3");
        h.remove(b"a");
        h.remove(b"b");
        assert_eq!(h.bytes(), 0);
        let mut l = ListVal::new();
        l.push_back(b"x");
        l.push_front(b"yy");
        l.set(0, b"zzz");
        l.retain_range(1, 2);
        l.pop_back();
        assert_eq!(l.bytes(), 0);
        let mut z = ZSet::new();
        z.insert(b"a", 1.0);
        z.insert(b"a", 2.0);
        z.remove(b"a");
        assert_eq!(z.bytes(), 0);
    }
}
