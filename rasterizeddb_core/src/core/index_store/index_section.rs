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
//! # Layout, version 1 (little-endian; `varint` is unsigned LEB128, at most
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
//! ```
//!
//! Every [`RESTART_EVERY`]th entry (the first included) is a **restart**: its
//! key is whole (`shared` 0) and its slot whole, so a reader can start there
//! and a rotted byte cannot reach past the next one. Nothing else is in the
//! section: no count of keys, no index identity (the snapshot's identity and
//! its section ordinal name the index), no checksum of its own.
//!
//! Keys are the evaluator's bytes (`key::encode_ordered` for B-tree and path,
//! `encode_canonical` for hash), sorted by those bytes; `RandomState` never
//! reaches the section. A change to this layout is a new
//! [`INDEX_SECTION_VER`], a new frozen vector set and a new history row
//! (see the tests). An open whose section carries another version refuses
//! the load and rebuilds (§4 Fallback); the next clean point rewrites the
//! section (§3 rule 2), so a version bump rebuilds once, then loads.

use super::codec::{FormatError, Reader};
use crate::core::index::KeyBuf;
use crate::core::index::sorted::{Sorted, SortedLoad, SortedReject};

/// This section's layout version.
pub const INDEX_SECTION_VER: u16 = 1;
/// Entries between two restarts.
pub const RESTART_EVERY: u64 = 16;
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
                for &slot in slots {
                    self.entry(key, slot);
                }
                Ok(())
            }
            (Stage::Entries, Sorted::Unkeyed(slots)) => {
                if slots.windows(2).any(|w| w[0] >= w[1]) {
                    return Err(SortedReject::SlotOrder { at: None });
                }
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

/// Every part of the section's stream, in order, to `sink`; checks the
/// bytes as it goes. `n_slots` bounds what the presence runs may allocate.
pub fn decode_into<'b>(
    bytes: &'b [u8],
    n_slots: u64,
    mut sink: impl FnMut(Sorted<'_, &[u8]>) -> Result<(), SortedReject>,
) -> Result<(), SectionError> {
    let mut r = Reader::new(bytes, WHAT);
    let ver = r.u16()?;
    if ver != INDEX_SECTION_VER {
        return Err(FormatError::UnsupportedVersion { what: WHAT, found: ver as u32 }.into());
    }
    let malformed = |at: u64, why| SectionError::Format(FormatError::Malformed { what: WHAT, at, why });
    if r.u16()? != 0 {
        return Err(malformed(2, "unknown flags"));
    }
    let n = r.u64()?;
    debug_assert_eq!(r.pos() as usize, HEAD_LEN);
    // Each entry takes at least three bytes.
    if n > (r.remaining() / 3) as u64 {
        return Err(malformed(4, "more entries than bytes"));
    }
    let mut key: Vec<u8> = Vec::new();
    let mut group: Vec<u64> = Vec::new();
    let mut prev_slot = 0u64;
    for i in 0..n {
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
        let same = !restart && shared == key.len() as u64 && unshared == 0;
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
            // A restart may carry the group's key whole: then it continues.
            let continues = restart && i > 0 && unshared == key.len() as u64 && suffix == key.as_slice();
            if !continues {
                if !group.is_empty() {
                    sink(Sorted::Entry(&key, &group))?;
                    group.clear();
                }
                key.truncate(shared as usize);
                key.extend_from_slice(suffix);
            }
        }
        group.push(slot);
        prev_slot = slot;
    }
    if !group.is_empty() {
        sink(Sorted::Entry(&key, &group))?;
    }
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

fn p_next(prev: u64, delta: u64) -> Option<u64> {
    prev.checked_add(delta)?.checked_add(1)
}

/// A section decoded into a stream an index's `load_sorted` takes.
pub fn decode(bytes: &[u8], n_slots: u64) -> Result<SortedLoad<KeyBuf>, SectionError> {
    // Sized from the header's pair count (an upper bound on the keys), as
    // far as the section's length can hold: `decode_into` refuses more
    // entries than a third of the bytes, so this never asks for more.
    let pairs = bytes.get(4..HEAD_LEN).and_then(|b| b.try_into().ok()).map_or(0, u64::from_le_bytes);
    let capacity = usize::try_from(pairs).unwrap_or(0).min(bytes.len() / 3);
    let mut load = SortedLoad::<KeyBuf>::with_capacity(n_slots, capacity);
    decode_into(bytes, n_slots, |part| load.push(part))?;
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
