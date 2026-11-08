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

use super::{KeyBuf, Slots};

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
/// Kept as the `(key, slots)` pairs an index holds, so the load takes them
/// over as they are: a B-tree is built from this very vector in place, where
/// pairs rebuilt from a separate slot list were copied into fresh pages
/// first, 1.5M of them at a load. A key's slots sit inline up to two
/// ([`Slots`]), so a million single-slot keys still cost no allocation each.
pub struct SortedLoad<K> {
    n_slots: u64,
    /// The entries, in order, as one or more runs: one when pushed one by
    /// one, one per stretch when assembled ([`SortedLoad::assemble`]), so a
    /// B-tree builds a shard from each run in place and a hash index deals
    /// from each without the runs ever being copied into one.
    segments: Vec<Vec<(K, Slots)>>,
    unkeyed: Option<Vec<u64>>,
    present: Option<Vec<u64>>,
}

/// A checked stream's parts: the entries, then the unkeyed slots and the
/// presence words.
pub(super) type SortedParts<K> = (Vec<(K, Slots)>, Vec<u64>, Vec<u64>);
/// [`SortedParts`] with the entries as the runs they were taken in.
pub(super) type SegmentedParts<K> = (Vec<Vec<(K, Slots)>>, Vec<u64>, Vec<u64>);

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
        Self::with_capacity(n_slots, 0)
    }

    /// [`Self::new`], with room for `entries` entries: what a section's
    /// header says it holds, which its decoder bounds by the section's
    /// length, so rotted bytes cannot make this allocate past the section.
    /// The pairs' vector then grows by none of its doublings, each of which
    /// copied every entry so far into fresh pages.
    pub fn with_capacity(n_slots: u64, entries: usize) -> SortedLoad<K> {
        SortedLoad {
            n_slots,
            segments: vec![Vec::with_capacity(entries)],
            unkeyed: None,
            present: None,
        }
    }

    /// Entries taken so far.
    pub fn entries(&self) -> usize {
        self.segments.iter().map(Vec::len).sum()
    }

    fn last_key(&self) -> Option<&K> {
        self.segments.iter().rev().find_map(|s| s.last()).map(|(k, _)| k)
    }

    fn entry(&mut self, key: K, slots: &[u64]) -> Result<(), SortedReject> {
        let at = self.entries();
        if self.unkeyed.is_some() || self.present.is_some() {
            return Err(SortedReject::PartOrder);
        }
        if slots.is_empty() {
            return Err(SortedReject::EmptyEntry { at });
        }
        if self.last_key().is_some_and(|last| *last >= key) {
            return Err(SortedReject::KeyOrder { at });
        }
        check_slots(slots, self.n_slots, Some(at))?;
        if self.segments.is_empty() {
            self.segments.push(Vec::new());
        }
        self.segments.last_mut().expect("one segment").push((key, Slots::from_slice(slots)));
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

    /// The checked parts, once both closing parts have come, the entries as
    /// one vector (copied into one only when they came as several runs).
    pub(super) fn into_parts(self) -> Result<SortedParts<K>, SortedReject> {
        let (mut segments, unkeyed, present) = self.into_segments()?;
        let entries = if segments.len() == 1 {
            segments.pop().expect("one segment")
        } else {
            let mut all = Vec::with_capacity(segments.iter().map(Vec::len).sum());
            for s in segments {
                all.extend(s);
            }
            all
        };
        Ok((entries, unkeyed, present))
    }

    /// The checked parts, the entries as the runs they were taken in, in
    /// order (none empty).
    pub(super) fn into_segments(self) -> Result<SegmentedParts<K>, SortedReject> {
        let segments = self.segments.into_iter().filter(|s| !s.is_empty()).collect();
        match (self.unkeyed, self.present) {
            (Some(unkeyed), Some(present)) => Ok((segments, unkeyed, present)),
            _ => Err(SortedReject::Truncated),
        }
    }
}

/// One stretch of the entries [`SortedLoad::assemble`] takes: exactly its
/// share, written in order, into a vector of its own.
pub struct Stretch {
    entries: Vec<(KeyBuf, Slots)>,
    share: usize,
    /// Where its first entry sits in the whole stream, for the refusals.
    base: usize,
    n_slots: u64,
}

impl Stretch {
    /// Takes the stretch's next entry, checked as [`SortedLoad::push`] checks
    /// one: listing slots, strictly ascending, each below the table's
    /// length, under a key above the one before it in this stretch.
    pub fn push(&mut self, key: &[u8], slots: &[u64]) -> Result<(), SortedReject> {
        let at = self.base + self.entries.len();
        if self.entries.len() == self.share {
            // More entries than its share: the caller's count was wrong.
            return Err(SortedReject::KeyOrder { at });
        }
        if slots.is_empty() {
            return Err(SortedReject::EmptyEntry { at });
        }
        if self.entries.last().is_some_and(|(last, _)| last.as_slice() >= key) {
            return Err(SortedReject::KeyOrder { at });
        }
        check_slots(slots, self.n_slots, Some(at))?;
        self.entries.push((KeyBuf::from_slice(key), Slots::from_slice(slots)));
        Ok(())
    }
}

impl SortedLoad<KeyBuf> {
    /// A stream whose entries come as `counts.len()` stretches, each decoded
    /// by `decode(i, stretch)` on one of up to `workers` threads into a
    /// vector of its own: stretch `i` holds exactly `counts[i]` entries.
    /// Each is checked as it is written (see [`Stretch::push`]), and then
    /// each stretch's first key against the one before's last, so the result
    /// is what pushing every entry in order would have taken. A stream's
    /// closing parts are pushed after, as usual.
    ///
    /// The stretches stay the stream's runs: a B-tree builds a shard from
    /// each in place and a hash index deals from each (`load_sorted`).
    /// Joining them into one vector copied every entry once more, into pages
    /// faulted in on one thread: as long as the decode the threads saved (a
    /// section of 1.5M entries, docs/persistent-indexes.md §4).
    ///
    /// The first error in stretch order is returned.
    pub fn assemble<E: From<SortedReject> + Send>(
        n_slots: u64,
        counts: &[usize],
        workers: usize,
        decode: impl Fn(usize, &mut Stretch) -> Result<(), E> + Sync,
    ) -> Result<SortedLoad<KeyBuf>, E> {
        let bases: Vec<usize> = counts
            .iter()
            .scan(0usize, |at, &n| {
                let base = *at;
                *at += n;
                Some(base)
            })
            .collect();
        let outcomes: Vec<parking_lot::Mutex<Option<Result<Vec<(KeyBuf, Slots)>, E>>>> =
            counts.iter().map(|_| parking_lot::Mutex::new(None)).collect();
        let next = std::sync::atomic::AtomicUsize::new(0);
        let failed = std::sync::atomic::AtomicBool::new(false);
        let work = || {
            use std::sync::atomic::Ordering;
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= counts.len() || failed.load(Ordering::Relaxed) {
                    return;
                }
                let mut stretch =
                    Stretch { entries: Vec::with_capacity(counts[i]), share: counts[i], base: bases[i], n_slots };
                let outcome = match decode(i, &mut stretch) {
                    Ok(()) if stretch.entries.len() == counts[i] => Ok(stretch.entries),
                    Ok(()) => Err(SortedReject::Truncated.into()),
                    Err(e) => Err(e),
                };
                if outcome.is_err() {
                    failed.store(true, Ordering::Relaxed);
                }
                *outcomes[i].lock() = Some(outcome);
            }
        };
        let workers = workers.clamp(1, counts.len().max(1));
        if workers == 1 {
            work();
        } else {
            std::thread::scope(|scope| {
                for _ in 0..workers {
                    scope.spawn(work);
                }
            });
        }
        // The workers stop taking stretches once one fails, so a stretch may
        // be left undecoded; then some stretch failed, and the first failure
        // in order is the answer.
        let mut outcomes: Vec<Option<Result<Vec<(KeyBuf, Slots)>, E>>> =
            outcomes.into_iter().map(|o| o.into_inner()).collect();
        if let Some(e) = outcomes.iter_mut().find_map(|o| match o {
            Some(Err(_)) => o.take().and_then(Result::err),
            _ => None,
        }) {
            return Err(e);
        }
        let mut segments = Vec::with_capacity(counts.len());
        for (o, &base) in outcomes.into_iter().zip(&bases) {
            let Some(Ok(entries)) = o else { return Err(SortedReject::Truncated.into()) };
            let prev_last = segments.iter().rev().find_map(|s: &Vec<(KeyBuf, Slots)>| s.last()).map(|(k, _)| k);
            if let (Some(last), Some((first, _))) = (prev_last, entries.first())
                && last >= first
            {
                return Err(SortedReject::KeyOrder { at: base }.into());
            }
            segments.push(entries);
        }
        Ok(SortedLoad { n_slots, segments, unkeyed: None, present: None })
    }

    /// The entries taken so far, owned (the section codec's tests compare
    /// a parallel decode with a serial one).
    #[cfg(test)]
    pub(crate) fn pairs(&self) -> Vec<(Vec<u8>, Vec<u64>)> {
        self.segments.iter().flatten().map(|(k, s)| (k.to_vec(), s.to_vec())).collect()
    }

    /// The closing parts, owned, once both have come.
    #[cfg(test)]
    pub(crate) fn into_parts_for_test(self) -> Result<(Vec<u64>, Vec<u64>), SortedReject> {
        self.into_parts().map(|(_, unkeyed, present)| (unkeyed, present))
    }

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
        let (entries, unkeyed, present) = load(
            &[
                Sorted::Entry(1, &[0, 5]),
                Sorted::Entry(9, &[2]),
                Sorted::Unkeyed(&[3]),
                Sorted::Present(&[0b10_1101, 0, 0]),
            ],
            6,
        )
        .unwrap();
        let entries: Vec<(u64, Vec<u64>)> = entries.into_iter().map(|(k, s)| (k, s.to_vec())).collect();
        assert_eq!(entries, vec![(1, vec![0, 5]), (9, vec![2])]);
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
