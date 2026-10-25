//! An index's contents as one sorted stream: read out by `for_each_sorted`
//! (hook C3) and built back by `load_sorted` (hook C9), on
//! [`LiveIndex`](super::LiveIndex) and [`GinIndex`](super::gin::GinIndex).
//! What a persisted snapshot's INDEX and GIN sections hold
//! (docs/persistent-indexes.md §1, §3 step 5, §4); the bytes are theirs.
//!
//! The stream is, in this order:
//!
//! 1. **every entry**, a key and the slots listed under it. Keys strictly
//!    ascending: by their bytes for a B-tree, hash or path index, so a hash
//!    index comes out in one order whatever its hasher; by the item for gin.
//!    Each key's slots strictly ascending, and never none;
//! 2. **the unkeyed slots**, strictly ascending, once;
//! 3. **the presence bits**, as words, without trailing zero words, once.
//!
//! # A superset, never a subset
//!
//! What comes out is everything the index lists, filtered by nothing: not by
//! the presence bit, which is also clear on a slot whose insert has written
//! its entries and not yet set it. The one exception is gin's entries of the
//! slots `forget` recorded, exactly those (see `GinIndex::for_each_sorted`).
//!
//! The presence bits are read before the entries are swept and go out last.
//! A writer lists a slot's entries before it sets the slot's bit, so every
//! bit the stream carries names a slot whose entries the sweep finds: a
//! loaded index holds no slot it cannot find, with or without the seal's
//! gate keeping writers out of the capture.
//!
//! What goes back in is the whole stream or nothing. [`SortedLoad`] checks
//! each part as it arrives and refuses the first one out of order, and an
//! index is touched only once the stream has been checked to its end. A load
//! adds to what the index holds and removes nothing, so beside a write that
//! got in first its result is the union, a superset of both.

use super::KeyBuf;

/// Entries `for_each_sorted` copies out under one hold of an index's read
/// lock (§3 step 5, P15). A writer waits for at most this many to be copied,
/// never for what the caller does with them: encoding, the token bucket, I/O.
pub const SORTED_BLOCK: usize = 1024;

/// One part of the stream. `K` is the key: its bytes (`&[u8]`) for a
/// [`LiveIndex`](super::LiveIndex), the item (`u64`) for a gin index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sorted<'a, K> {
    /// A key and the slots listed under it.
    Entry(K, &'a [u64]),
    /// The slots whose key could not be built.
    Unkeyed(&'a [u64]),
    /// One bit per slot the index holds, as `holds` reads it.
    Present(&'a [u64]),
}

/// Why a stream was refused. Every refusal leaves the index as it was; the
/// caller rebuilds from rows instead (§4, Fallback).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortedReject {
    /// Entry `at` has a key not above the one before it.
    KeyOrder { at: usize },
    /// Entry `at` lists no slots. The index drops a key when its last slot
    /// goes, so it never has one.
    EmptyEntry { at: usize },
    /// A slot not above the one before it, under entry `at` or (`None`)
    /// among the unkeyed.
    SlotOrder { at: Option<usize> },
    /// A slot, or a presence bit, at or past the table's length.
    PastEnd { slot: u64, n_slots: u64 },
    /// A part after the one that follows it, or a part given twice.
    PartOrder,
    /// The stream ended without its unkeyed slots or its presence bits.
    Truncated,
}

/// A stream being checked, on its way into an index's `load_sorted`.
///
/// Slots are kept in one list, each entry recording where its slots end, so
/// a million single-slot keys cost no allocation each.
pub struct SortedLoad<K> {
    n_slots: u64,
    keys: Vec<(K, usize)>,
    slots: Vec<u64>,
    unkeyed: Option<Vec<u64>>,
    present: Option<Vec<u64>>,
}

/// A checked stream's parts: the keys, each with the end of its slots in
/// the second list, then the unkeyed slots and the presence words.
pub(super) type SortedParts<K> = (Vec<(K, usize)>, Vec<u64>, Vec<u64>, Vec<u64>);

/// Strictly ascending, and every slot below `n_slots`.
fn check_slots(slots: &[u64], n_slots: u64, at: Option<usize>) -> Result<(), SortedReject> {
    if slots.windows(2).any(|w| w[0] >= w[1]) {
        return Err(SortedReject::SlotOrder { at });
    }
    match slots.last() {
        Some(&slot) if slot >= n_slots => Err(SortedReject::PastEnd { slot, n_slots }),
        _ => Ok(()),
    }
}

impl<K: Ord> SortedLoad<K> {
    /// A stream for a table of `n_slots` slots (the snapshot identity's,
    /// checked against the recovered files). An index lists only slots the
    /// table had when it was captured, so anything past that is not what
    /// was captured; it also bounds what a rotted stream can allocate.
    pub fn new(n_slots: u64) -> SortedLoad<K> {
        SortedLoad {
            n_slots,
            keys: Vec::new(),
            slots: Vec::new(),
            unkeyed: None,
            present: None,
        }
    }

    /// Entries taken so far.
    pub fn entries(&self) -> usize {
        self.keys.len()
    }

    fn entry(&mut self, key: K, slots: &[u64]) -> Result<(), SortedReject> {
        let at = self.keys.len();
        if self.unkeyed.is_some() || self.present.is_some() {
            return Err(SortedReject::PartOrder);
        }
        if slots.is_empty() {
            return Err(SortedReject::EmptyEntry { at });
        }
        if self.keys.last().is_some_and(|(last, _)| *last >= key) {
            return Err(SortedReject::KeyOrder { at });
        }
        check_slots(slots, self.n_slots, Some(at))?;
        self.slots.extend_from_slice(slots);
        self.keys.push((key, self.slots.len()));
        Ok(())
    }

    fn unkeyed(&mut self, slots: &[u64]) -> Result<(), SortedReject> {
        if self.unkeyed.is_some() || self.present.is_some() {
            return Err(SortedReject::PartOrder);
        }
        check_slots(slots, self.n_slots, None)?;
        self.unkeyed = Some(slots.to_vec());
        Ok(())
    }

    fn present(&mut self, words: &[u64]) -> Result<(), SortedReject> {
        if self.unkeyed.is_none() || self.present.is_some() {
            return Err(SortedReject::PartOrder);
        }
        // The highest bit set, if any, must name a slot the table has.
        let words = trimmed(words);
        if let Some(&last) = words.last() {
            let slot = (words.len() as u64 - 1) * 64 + 63 - last.leading_zeros() as u64;
            if slot >= self.n_slots {
                return Err(SortedReject::PastEnd { slot, n_slots: self.n_slots });
            }
        }
        self.present = Some(words.to_vec());
        Ok(())
    }

    /// The checked parts, once both closing parts have come.
    pub(super) fn into_parts(self) -> Result<SortedParts<K>, SortedReject> {
        match (self.unkeyed, self.present) {
            (Some(unkeyed), Some(present)) => Ok((self.keys, self.slots, unkeyed, present)),
            _ => Err(SortedReject::Truncated),
        }
    }
}

impl SortedLoad<KeyBuf> {
    /// Takes the next part of a B-tree, hash or path index's stream.
    pub fn push(&mut self, part: Sorted<'_, &[u8]>) -> Result<(), SortedReject> {
        match part {
            Sorted::Entry(key, slots) => self.entry(KeyBuf::from_slice(key), slots),
            Sorted::Unkeyed(slots) => self.unkeyed(slots),
            Sorted::Present(words) => self.present(words),
        }
    }
}

impl SortedLoad<u64> {
    /// Takes the next part of a gin index's stream.
    pub fn push(&mut self, part: Sorted<'_, u64>) -> Result<(), SortedReject> {
        match part {
            Sorted::Entry(item, slots) => self.entry(item, slots),
            Sorted::Unkeyed(slots) => self.unkeyed(slots),
            Sorted::Present(words) => self.present(words),
        }
    }
}

/// The presence words without trailing zero words: what the stream carries,
/// so equal contents give equal streams however the bitmap grew.
pub(super) fn trimmed(words: &[u64]) -> &[u64] {
    &words[..words.iter().rposition(|w| *w != 0).map_or(0, |w| w + 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(parts: &[Sorted<'_, u64>], n_slots: u64) -> Result<SortedParts<u64>, SortedReject> {
        let mut load = SortedLoad::<u64>::new(n_slots);
        for p in parts {
            load.push(*p)?;
        }
        load.into_parts()
    }

    #[test]
    fn a_well_formed_stream_is_taken_whole() {
        let (keys, slots, unkeyed, present) = load(
            &[
                Sorted::Entry(1, &[0, 5]),
                Sorted::Entry(9, &[2]),
                Sorted::Unkeyed(&[3]),
                Sorted::Present(&[0b10_1101, 0, 0]),
            ],
            6,
        )
        .unwrap();
        assert_eq!(keys, vec![(1, 2), (9, 3)]);
        assert_eq!(slots, vec![0, 5, 2]);
        assert_eq!(unkeyed, vec![3]);
        assert_eq!(present, vec![0b10_1101], "trailing zero words are dropped");
        // Nothing at all is well formed: an empty index.
        assert!(load(&[Sorted::Unkeyed(&[]), Sorted::Present(&[])], 0).is_ok());
    }

    /// Each rule, broken once.
    #[test]
    fn every_rule_is_checked() {
        use SortedReject::*;
        let present: &[u64] = &[1];
        for (parts, want) in [
            (vec![Sorted::Entry(2, &[0][..]), Sorted::Entry(2, &[1])], KeyOrder { at: 1 }),
            (vec![Sorted::Entry(2, &[0][..]), Sorted::Entry(1, &[1])], KeyOrder { at: 1 }),
            (vec![Sorted::Entry(2, &[][..])], EmptyEntry { at: 0 }),
            (vec![Sorted::Entry(2, &[1, 1][..])], SlotOrder { at: Some(0) }),
            (vec![Sorted::Entry(2, &[3, 1][..])], SlotOrder { at: Some(0) }),
            (vec![Sorted::Unkeyed(&[4, 4][..])], SlotOrder { at: None }),
            (vec![Sorted::Entry(2, &[8][..])], PastEnd { slot: 8, n_slots: 8 }),
            (vec![Sorted::Unkeyed(&[9][..])], PastEnd { slot: 9, n_slots: 8 }),
            (vec![Sorted::Unkeyed(&[][..]), Sorted::Present(&[1 << 8])], PastEnd { slot: 8, n_slots: 8 }),
            (vec![Sorted::Unkeyed(&[][..]), Sorted::Present(&[0, 1])], PastEnd { slot: 64, n_slots: 8 }),
            (vec![Sorted::Unkeyed(&[][..]), Sorted::Entry(2, &[1])], PartOrder),
            (vec![Sorted::Unkeyed(&[][..]), Sorted::Unkeyed(&[])], PartOrder),
            (vec![Sorted::Present(present)], PartOrder),
            (vec![Sorted::Unkeyed(&[][..]), Sorted::Present(present), Sorted::Present(present)], PartOrder),
            (vec![Sorted::Unkeyed(&[][..]), Sorted::Present(present), Sorted::Entry(3, &[1])], PartOrder),
            (vec![Sorted::Entry(2, &[1][..])], Truncated),
            (vec![Sorted::Entry(2, &[1][..]), Sorted::Unkeyed(&[])], Truncated),
        ] {
            assert_eq!(load(&parts, 8).unwrap_err(), want, "{parts:?}");
        }
    }
}
