//! An INDEX section's bytes (docs/persistent-indexes.md §1, §4; slice B17):
//! one B-tree, hash or path index of a snapshot, as `LiveIndex::
//! for_each_sorted` (hook C3) streams it and `LiveIndex::load_sorted` (hook
//! C9) takes it back.
//!
//! Pure: a stream in, bytes out, and back. The snapshot container
//! ([`super::snapshot`]) checksums the section as a whole (crc32c) and the
//! file (xxh64); this layer checks that the bytes are a well-formed stream,
//! and [`SortedLoad`] that the stream is one an index could have given.
//!
//! # Layout, version 2 (little-endian; `varint` is unsigned LEB128, at most
//! ten bytes, never overlong)
//!
//! ```text
//! ver u16                 INDEX_SECTION_VER
//! flags u16               0; anything else is refused
//! n_entries u64           (key, slot) pairs that follow
//! entries                 in (key ‖ slot) order, each:
//!   shared varint         bytes of the previous key this one starts with
//!   unshared varint       bytes that follow them
//!   unshared bytes
//!   slot varint           the slot; or, when the key is the previous
//!                         entry's and this is no restart, the slot minus
//!                         the previous slot, minus one
//! n_unkeyed varint        then the unkeyed slots, ascending: the first
//!                         whole, each next minus the one before, minus one
//! n_runs varint           then the presence bits as runs, alternating,
//!                         zeros first: the first run may be empty, every
//!                         other is not, and the last is a run of ones
//! chunks                  where a reader may start decoding entries:
//!   n_chunks varint       0 when there are no entries, else at least 1
//!   per chunk, each a difference from the chunk before (the first's from
//!   0, 0 and 0):
//!     entry varint        its first entry's ordinal
//!     offset varint       that entry's byte offset in the section
//!     key varint          that entry's key's ordinal among the keys
//!   n_keys varint         distinct keys in the entries
//!   tail varint           the byte offset of n_unkeyed
//! chunks_len u32          bytes of `chunks`; the section's last four
//! ```
//!
//! Every [`RESTART_EVERY`]th entry (the first included) is a **restart**: its
//! key is whole (`shared` 0) and its slot whole, so a reader can start there
//! and a rotted byte cannot reach past the next one. A **chunk** starts at a
//! restart that is a key's first entry, about every [`CHUNK_ENTRIES`]
//! entries, so the chunks decode on threads of their own and their entries
//! join without a key split between two (the open's critical path was one
//! section's serial decode, docs/persistent-indexes.md §4). The first chunk
//! starts at entry 0; together the chunks tile the entries exactly, each
//! ending where the next starts and the last at `tail`, and a decoder checks
//! that, and each chunk's key count, as it joins them. Nothing else is in
//! the section: no index identity (the snapshot's identity and its section
//! ordinal name the index), no checksum of its own.
//!
//! Version 1 is version 2 without `chunks` and `chunks_len`; it is still
//! read, as one chunk, so a snapshot written before 2 loads rather than
//! rebuilds.
//!
//! Keys are the evaluator's bytes (`key::encode_ordered` for B-tree and path,
//! `encode_canonical` for hash), sorted by those bytes; `RandomState` never
//! reaches the section. A change to this layout is a new
//! [`INDEX_SECTION_VER`], a new frozen vector set and a new history row
//! (see the tests). An open whose section carries a version this build does
//! not read refuses the load and rebuilds (§4 Fallback); the next clean point
//! rewrites the section (§3 rule 2), so a version bump rebuilds once, then
//! loads.

use super::codec::{FormatError, Reader};
use crate::core::index::KeyBuf;
use crate::core::index::sorted::{Sorted, SortedLoad, SortedReject};

/// This section's layout version.
pub const INDEX_SECTION_VER: u16 = 2;
/// The oldest version still read.
const OLDEST_READ: u16 = 1;
/// Entries between two restarts.
pub const RESTART_EVERY: u64 = 16;
/// Entries a chunk holds at least, unless it is the last: a key whose
/// first entry is a restart this many entries past the chunk's start
/// starts the next. 1.5M entries make about 23 chunks.
pub const CHUNK_ENTRIES: u64 = 1 << 16;
/// The fixed head: version, flags, entry count.
const HEAD_LEN: usize = 2 + 2 + 8;
const WHAT: &str = "index section";

/// Why a section was not loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SectionError {
    /// The bytes are not a well-formed section.
    Format(FormatError),
    /// They are, but the stream they hold is not one an index gives.
    Reject(SortedReject),
}

impl From<FormatError> for SectionError {
    fn from(e: FormatError) -> SectionError {
        SectionError::Format(e)
    }
}

impl From<SortedReject> for SectionError {
    fn from(e: SortedReject) -> SectionError {
        SectionError::Reject(e)
    }
}

impl std::fmt::Display for SectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SectionError::Format(e) => write!(f, "{e}"),
            SectionError::Reject(r) => write!(f, "{WHAT}: the stream is refused: {r:?}"),
        }
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn varint(r: &mut Reader<'_>) -> Result<u64, FormatError> {
    let at = r.pos();
    let mut v = 0u64;
    for i in 0..10 {
        let b = r.u8()?;
        if i == 9 && b > 1 {
            return Err(FormatError::Malformed { what: WHAT, at, why: "varint past 64 bits" });
        }
        v |= u64::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            if b == 0 && i > 0 {
                return Err(FormatError::Malformed { what: WHAT, at, why: "overlong varint" });
            }
            return Ok(v);
        }
    }
    Err(FormatError::Malformed { what: WHAT, at, why: "varint past ten bytes" })
}

/// Which part of the stream a writer expects next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Entries,
    Present,
    Done,
}

/// Builds a section from a C3 stream, part by part.
pub struct SectionWriter {
    out: Vec<u8>,
    prev_key: Vec<u8>,
    prev_slot: u64,
    n: u64,
    n_keys: u64,
    /// Each chunk's first entry, its byte offset and its key's ordinal.
    chunks: Vec<(u64, u64, u64)>,
    tail: u64,
    stage: Stage,
}

impl Default for SectionWriter {
    fn default() -> Self {
        SectionWriter::new()
    }
}

impl SectionWriter {
    pub fn new() -> SectionWriter {
        let mut out = Vec::with_capacity(4096);
        out.extend_from_slice(&INDEX_SECTION_VER.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&0u64.to_le_bytes());
        SectionWriter {
            out,
            prev_key: Vec::new(),
            prev_slot: 0,
            n: 0,
            n_keys: 0,
            chunks: Vec::new(),
            tail: 0,
            stage: Stage::Entries,
        }
    }

    fn entry(&mut self, key: &[u8], slot: u64) {
        let restart = self.n % RESTART_EVERY == 0;
        let shared = if restart {
            0
        } else {
            key.iter().zip(&self.prev_key).take_while(|(a, b)| a == b).count()
        };
        let same = !restart && shared == self.prev_key.len() && shared == key.len();
        put_varint(&mut self.out, shared as u64);
        put_varint(&mut self.out, (key.len() - shared) as u64);
        self.out.extend_from_slice(&key[shared..]);
        if same {
            put_varint(&mut self.out, slot - self.prev_slot - 1);
        } else {
            put_varint(&mut self.out, slot);
        }
        if !same {
            self.prev_key.clear();
            self.prev_key.extend_from_slice(key);
        }
        self.prev_slot = slot;
        self.n += 1;
    }

    /// Takes the next part. The stream is C3's, so it is well formed; a part
    /// out of order is a bug in the caller and refused rather than written.
    pub fn push(&mut self, part: Sorted<'_, &[u8]>) -> Result<(), SortedReject> {
        match (self.stage, part) {
            (Stage::Entries, Sorted::Entry(key, slots)) => {
                if slots.is_empty() {
                    return Err(SortedReject::EmptyEntry { at: self.n as usize });
                }
                if slots.windows(2).any(|w| w[0] >= w[1]) {
                    return Err(SortedReject::SlotOrder { at: Some(self.n as usize) });
                }
                // A key's first entry: one that is a restart may start a
                // chunk (the stream's keys are distinct, so a key equal to
                // the last is never one).
                if self.n == 0 || key != self.prev_key.as_slice() {
                    let due = self.chunks.last().is_none_or(|&(first, ..)| self.n - first >= CHUNK_ENTRIES);
                    if self.n % RESTART_EVERY == 0 && due {
                        self.chunks.push((self.n, self.out.len() as u64, self.n_keys));
                    }
                    self.n_keys += 1;
                }
                for &slot in slots {
                    self.entry(key, slot);
                }
                Ok(())
            }
            (Stage::Entries, Sorted::Unkeyed(slots)) => {
                if slots.windows(2).any(|w| w[0] >= w[1]) {
                    return Err(SortedReject::SlotOrder { at: None });
                }
                self.tail = self.out.len() as u64;
                put_varint(&mut self.out, slots.len() as u64);
                let mut prev = None;
                for &s in slots {
                    put_varint(&mut self.out, prev.map_or(s, |p: u64| s - p - 1));
                    prev = Some(s);
                }
                self.stage = Stage::Present;
                Ok(())
            }
            (Stage::Present, Sorted::Present(words)) => {
                put_runs(&mut self.out, words);
                self.stage = Stage::Done;
                Ok(())
            }
            _ => Err(SortedReject::PartOrder),
        }
    }

    /// The section's bytes, once the stream has ended.
    pub fn finish(mut self) -> Result<Vec<u8>, SortedReject> {
        if self.stage != Stage::Done {
            return Err(SortedReject::Truncated);
        }
        self.out[4..12].copy_from_slice(&self.n.to_le_bytes());
        let table = self.out.len();
        put_varint(&mut self.out, self.chunks.len() as u64);
        let mut prev = (0, 0, 0);
        for &(entry, offset, key) in &self.chunks {
            put_varint(&mut self.out, entry - prev.0);
            put_varint(&mut self.out, offset - prev.1);
            put_varint(&mut self.out, key - prev.2);
            prev = (entry, offset, key);
        }
        put_varint(&mut self.out, self.n_keys);
        put_varint(&mut self.out, self.tail);
        let len = (self.out.len() - table) as u32;
        self.out.extend_from_slice(&len.to_le_bytes());
        Ok(self.out)
    }
}

/// The presence words as alternating runs, zeros first, ending in ones.
fn put_runs(out: &mut Vec<u8>, words: &[u64]) {
    let mut runs: Vec<u64> = Vec::new();
    let (mut ones, mut len) = (false, 0u64);
    for &word in words {
        if word == 0 && !ones {
            len += 64;
            continue;
        }
        if word == u64::MAX && ones {
            len += 64;
            continue;
        }
        for b in 0..64 {
            let bit = word >> b & 1 == 1;
            if bit != ones {
                runs.push(len);
                ones = bit;
                len = 0;
            }
            len += 1;
        }
    }
    if ones {
        runs.push(len);
    }
    // A trailing run of zeros is dropped with the words that held it.
    put_varint(out, runs.len() as u64);
    for r in runs {
        put_varint(out, r);
    }
}

fn malformed(at: u64, why: &'static str) -> SectionError {
    SectionError::Format(FormatError::Malformed { what: WHAT, at, why })
}

/// A stretch of entries one decoder takes: a chunk, or a version 1 section's
/// whole entries.
#[derive(Debug, Clone, Copy)]
struct Stretch {
    /// Its first entry's ordinal, and one past its last.
    first: u64,
    end: u64,
    /// Its byte range: where it ends is known for a chunk, and is wherever
    /// the last entry ends for a version 1 section.
    start: usize,
    stop: Option<usize>,
    /// The keys it holds, for a chunk.
    keys: Option<u64>,
}

/// Where a section's parts are: its entries as stretches, and where the
/// unkeyed slots start (unknown before the entries are read, in version 1)
/// and the presence runs end.
struct Layout {
    stretches: Vec<Stretch>,
    n_keys: Option<u64>,
    tail: Option<usize>,
    body_end: usize,
}

fn layout(bytes: &[u8]) -> Result<Layout, SectionError> {
    let mut r = Reader::new(bytes, WHAT);
    let ver = r.u16()?;
    if !(OLDEST_READ..=INDEX_SECTION_VER).contains(&ver) {
        return Err(FormatError::UnsupportedVersion { what: WHAT, found: ver as u32 }.into());
    }
    if r.u16()? != 0 {
        return Err(malformed(2, "unknown flags"));
    }
    let n = r.u64()?;
    debug_assert_eq!(r.pos() as usize, HEAD_LEN);
    // Each entry takes at least three bytes.
    if n > (r.remaining() / 3) as u64 {
        return Err(malformed(4, "more entries than bytes"));
    }
    if ver == 1 {
        let whole = Stretch { first: 0, end: n, start: HEAD_LEN, stop: None, keys: None };
        return Ok(Layout { stretches: vec![whole], n_keys: None, tail: None, body_end: bytes.len() });
    }
    // The chunk table, from the section's end.
    let last4 = bytes.len().checked_sub(4).filter(|&at| at >= HEAD_LEN).ok_or_else(|| {
        SectionError::Format(FormatError::Truncated { what: WHAT, at: bytes.len() as u64, need: 4 })
    })?;
    let table_len = u32::from_le_bytes(bytes[last4..].try_into().expect("four bytes")) as usize;
    let table = last4.checked_sub(table_len).filter(|&at| at >= HEAD_LEN).ok_or(malformed(last4 as u64, "the chunk table is longer than the section"))?;
    let mut t = Reader::within(bytes, table, last4, WHAT);
    let n_chunks = varint(&mut t)?;
    if n_chunks > (t.remaining() / 3) as u64 {
        return Err(malformed(table as u64, "more chunks than bytes"));
    }
    let mut starts: Vec<(u64, usize, u64)> = Vec::with_capacity(n_chunks as usize);
    let mut prev = (0u64, 0u64, 0u64);
    for i in 0..n_chunks {
        let at = t.pos();
        let (de, doff, dk) = (varint(&mut t)?, varint(&mut t)?, varint(&mut t)?);
        let entry = prev.0.checked_add(de).ok_or(malformed(at, "a chunk past 64 bits"))?;
        let offset = prev.1.checked_add(doff).ok_or(malformed(at, "a chunk past 64 bits"))?;
        let key = prev.2.checked_add(dk).ok_or(malformed(at, "a chunk past 64 bits"))?;
        if i == 0 && (entry, offset, key) != (0, HEAD_LEN as u64, 0) {
            return Err(malformed(at, "the first chunk does not start at the first entry"));
        }
        if i > 0 && (de == 0 || doff == 0 || dk == 0) {
            return Err(malformed(at, "a chunk does not follow the one before"));
        }
        if entry % RESTART_EVERY != 0 || entry >= n || offset >= table as u64 {
            return Err(malformed(at, "a chunk starts where no chunk can"));
        }
        starts.push((entry, offset as usize, key));
        prev = (entry, offset, key);
    }
    let n_keys = varint(&mut t)?;
    let at = t.pos();
    let tail = varint(&mut t)?;
    if t.remaining() != 0 {
        return Err(malformed(t.pos(), "bytes after the chunk table"));
    }
    if (n == 0) != (n_chunks == 0) || n_keys > n || (n == 0) != (n_keys == 0) || prev.2 >= n_keys.max(1) {
        return Err(malformed(at, "the chunks do not hold the entries"));
    }
    let tail = usize::try_from(tail).ok().filter(|&tail| tail >= HEAD_LEN && tail <= table && starts.last().is_none_or(|s| s.1 < tail));
    let tail = tail.ok_or(malformed(at, "the unkeyed slots start where they cannot"))?;
    let stretches = starts
        .iter()
        .enumerate()
        .map(|(i, &(first, start, key))| {
            let next = starts.get(i + 1).copied();
            Stretch {
                first,
                end: next.map_or(n, |s| s.0),
                start,
                stop: Some(next.map_or(tail, |s| s.1)),
                keys: Some(next.map_or(n_keys, |s| s.2) - key),
            }
        })
        .collect();
    Ok(Layout { stretches, n_keys: Some(n_keys), tail: Some(tail), body_end: table })
}

/// Decodes one stretch's entries to `sink`; returns where they ended.
fn decode_stretch(
    bytes: &[u8],
    body_end: usize,
    st: Stretch,
    mut sink: impl FnMut(Sorted<'_, &[u8]>) -> Result<(), SortedReject>,
) -> Result<usize, SectionError> {
    let mut r = Reader::within(bytes, st.start, st.stop.unwrap_or(body_end), WHAT);
    let mut key: Vec<u8> = Vec::new();
    let mut group: Vec<u64> = Vec::new();
    let mut prev_slot = 0u64;
    let mut keys = 0u64;
    for i in st.first..st.end {
        let at = r.pos();
        let restart = i % RESTART_EVERY == 0;
        let shared = varint(&mut r)?;
        let unshared = varint(&mut r)?;
        if restart && shared != 0 {
            return Err(malformed(at, "a restart shares a prefix"));
        }
        if shared > key.len() as u64 {
            return Err(malformed(at, "shares more than the previous key"));
        }
        if unshared > r.remaining() as u64 {
            return Err(malformed(at, "key past the section"));
        }
        let suffix = r.take(unshared as usize)?;
        let first = i == st.first;
        let same = !restart && !first && shared == key.len() as u64 && unshared == 0;
        let raw = varint(&mut r)?;
        let slot = if same {
            prev_slot
                .checked_add(raw)
                .and_then(|s| s.checked_add(1))
                .ok_or_else(|| malformed(at, "slot past 64 bits"))?
        } else {
            raw
        };
        if !same {
            // A restart may carry the group's key whole: then it continues
            // (never at a stretch's first entry, which starts a key).
            let continues = restart && !first && unshared == key.len() as u64 && suffix == key.as_slice();
            if !continues {
                if !group.is_empty() {
                    sink(Sorted::Entry(&key, &group))?;
                    group.clear();
                }
                key.truncate(shared as usize);
                key.extend_from_slice(suffix);
                keys += 1;
                if st.keys.is_some_and(|k| keys > k) {
                    return Err(malformed(at, "a chunk holds another number of keys"));
                }
            }
        }
        group.push(slot);
        prev_slot = slot;
    }
    if !group.is_empty() {
        sink(Sorted::Entry(&key, &group))?;
    }
    let end = r.pos() as usize;
    if st.stop.is_some_and(|stop| stop != end) {
        return Err(malformed(end as u64, "a chunk does not end where the next starts"));
    }
    if st.keys.is_some_and(|k| k != keys) {
        return Err(malformed(st.start as u64, "a chunk holds another number of keys"));
    }
    Ok(end)
}

/// The unkeyed slots and presence runs from `start`, to `sink`; they must
/// end the section's body.
fn decode_tail(
    bytes: &[u8],
    start: usize,
    body_end: usize,
    n_slots: u64,
    mut sink: impl FnMut(Sorted<'_, &[u8]>) -> Result<(), SortedReject>,
) -> Result<(), SectionError> {
    let mut r = Reader::within(bytes, start, body_end, WHAT);
    let n_unkeyed = varint(&mut r)?;
    if n_unkeyed > r.remaining() as u64 {
        return Err(malformed(r.pos(), "more unkeyed slots than bytes"));
    }
    let mut unkeyed = Vec::with_capacity(n_unkeyed as usize);
    for _ in 0..n_unkeyed {
        let at = r.pos();
        let v = varint(&mut r)?;
        let s = match unkeyed.last() {
            None => v,
            Some(&p) => p_next(p, v).ok_or_else(|| malformed(at, "slot past 64 bits"))?,
        };
        unkeyed.push(s);
    }
    sink(Sorted::Unkeyed(&unkeyed))?;
    let n_runs = varint(&mut r)?;
    if n_runs > r.remaining() as u64 {
        return Err(malformed(r.pos(), "more runs than bytes"));
    }
    if n_runs % 2 != 0 {
        return Err(malformed(r.pos(), "the runs do not end in ones"));
    }
    let mut runs = Vec::with_capacity(n_runs as usize);
    let mut total = 0u64;
    for i in 0..n_runs {
        let at = r.pos();
        let run = varint(&mut r)?;
        if run == 0 && i > 0 {
            return Err(malformed(at, "an empty run"));
        }
        total = total.checked_add(run).filter(|t| *t <= n_slots).ok_or_else(|| {
            SectionError::Reject(SortedReject::PastEnd {
                slot: total.saturating_add(run).saturating_sub(1),
                n_slots,
            })
        })?;
        runs.push(run);
    }
    let mut words = vec![0u64; total.div_ceil(64) as usize];
    let mut at = 0u64;
    for (i, run) in runs.iter().enumerate() {
        if i % 2 == 1 {
            for s in at..at + run {
                words[(s / 64) as usize] |= 1 << (s % 64);
            }
        }
        at += run;
    }
    sink(Sorted::Present(&words))?;
    if r.remaining() != 0 {
        return Err(malformed(r.pos(), "bytes after the presence runs"));
    }
    Ok(())
}

/// Every part of the section's stream, in order, to `sink`, one chunk after
/// another; checks the bytes as it goes. `n_slots` bounds what the presence
/// runs may allocate.
pub fn decode_into<'b>(
    bytes: &'b [u8],
    n_slots: u64,
    mut sink: impl FnMut(Sorted<'_, &[u8]>) -> Result<(), SortedReject>,
) -> Result<(), SectionError> {
    let l = layout(bytes)?;
    let mut end = HEAD_LEN;
    for &st in &l.stretches {
        end = decode_stretch(bytes, l.body_end, st, &mut sink)?;
    }
    decode_tail(bytes, l.tail.unwrap_or(end), l.body_end, n_slots, sink)
}

fn p_next(prev: u64, delta: u64) -> Option<u64> {
    prev.checked_add(delta)?.checked_add(1)
}

/// The threads a section's chunks decode on, at most.
const DECODE_WORKERS: usize = 8;

/// A section decoded into a stream an index's `load_sorted` takes: a version
/// 2 section's chunks on threads of their own, each straight into its share
/// of the one vector of entries ([`SortedLoad::assemble`], which checks each
/// chunk's first key against the last one's), then its tail.
pub fn decode(bytes: &[u8], n_slots: u64) -> Result<SortedLoad<KeyBuf>, SectionError> {
    let l = layout(bytes)?;
    let workers = std::thread::available_parallelism().map_or(1, |n| n.get()).min(DECODE_WORKERS);
    let mut load = match (l.n_keys, l.tail) {
        (Some(_), Some(_)) if l.stretches.len() > 1 && workers > 1 => {
            // Each chunk's key count, bounded as the layout bounds the
            // entries: no more keys than a third of the section's bytes.
            let counts: Vec<usize> = l
                .stretches
                .iter()
                .map(|st| usize::try_from(st.keys.unwrap_or(0)).unwrap_or(usize::MAX).min(bytes.len() / 3))
                .collect();
            SortedLoad::<KeyBuf>::assemble(n_slots, &counts, workers, |i, stretch| {
                decode_stretch(bytes, l.body_end, l.stretches[i], |part| match part {
                    Sorted::Entry(key, slots) => stretch.push(key, slots),
                    _ => Err(SortedReject::PartOrder),
                })
                .map(drop)
            })?
        }
        _ => {
            // One chunk, or a version 1 section: one pass, sized from the
            // header's pair count (an upper bound on the keys) as far as the
            // section's length can hold.
            let pairs = bytes.get(4..HEAD_LEN).and_then(|b| b.try_into().ok()).map_or(0, u64::from_le_bytes);
            let capacity = usize::try_from(l.n_keys.unwrap_or(pairs)).unwrap_or(0).min(bytes.len() / 3);
            let mut load = SortedLoad::<KeyBuf>::with_capacity(n_slots, capacity);
            let mut end = HEAD_LEN;
            for &st in &l.stretches {
                end = decode_stretch(bytes, l.body_end, st, |part| load.push(part))?;
            }
            if l.tail.is_none() {
                decode_tail(bytes, end, l.body_end, n_slots, |part| load.push(part))?;
                return Ok(load);
            }
            load
        }
    };
    let tail = l.tail.expect("a version 2 layout knows where its tail starts");
    decode_tail(bytes, tail, l.body_end, n_slots, |part| load.push(part))?;
    Ok(load)
}

/// A whole C3 stream as section bytes.
pub fn encode_stream<'a>(parts: impl IntoIterator<Item = Sorted<'a, &'a [u8]>>) -> Result<Vec<u8>, SortedReject> {
    let mut w = SectionWriter::new();
    for p in parts {
        w.push(p)?;
    }
    w.finish()
}

#[cfg(test)]
#[path = "index_section_tests.rs"]
mod tests;
