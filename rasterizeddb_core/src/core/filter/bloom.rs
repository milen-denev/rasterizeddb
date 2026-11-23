//! Span summaries: the L0/L1 tiers of §3, built as one structure.
//!
//! # What this buys, and what it does not
//!
//! A fingerprint plane answers "which rows in this zone might hold `v`" and
//! costs `f` bits per row to stream. This answers the weaker question "is `v`
//! anywhere in these 16 384 rows" and costs a *fixed* 512 bytes to answer it —
//! **0.25 bits/row**, a thirty-second of an `f = 8` sweep. A superzone the
//! summary rules out needs no plane read at all, so a point lookup over a large
//! table becomes a walk over summaries rather than a sweep over planes.
//!
//! It does **not** reduce false row *fetches*. That number is `rows × 2^-f`
//! whatever sits above the planes, because within any span the summary admits,
//! the fingerprints alone decide. This tier buys latency, and only latency —
//! which is why it is off by default and why §3 calls it the lowest-value item
//! in the design.
//!
//! # Why a fixed size rather than one sized from cardinality
//!
//! The arithmetic that decides whether a Bloom tier is worth anything: a Bloom
//! costs ~5–10 bits per *distinct value*, while the planes cost `f` bits per
//! *row*. Streaming the summary is cheaper than streaming the planes only when
//! `10 × distinct/rows < f` — for `f = 8`, when fewer than about four rows in
//! five hold a distinct value. Sizing each block from the span's cardinality
//! would make the structure track that ratio and grow until it cost as much as
//! what it was meant to avoid reading.
//!
//! A fixed 4096 bits per superzone inverts the trade so it can only be cheap. A
//! span of a few hundred distinct values fits comfortably and rules itself out
//! most of the time. A span of 16 384 distinct values saturates the block, which
//! answers "maybe" to everything — useless, but it cost 0.25 bits/row to find
//! out, and the planes below are unaffected. The failure mode is a small
//! constant overhead rather than a structure that quietly doubles the index.
//!
//! # Safety
//!
//! A Bloom filter has no false negatives and cannot delete. Both properties fall
//! the right way here: [`SuperBloom::may_contain`] returning `false` is a
//! guarantee that no *inserted* value hashed there, and a deleted row simply
//! leaves its key behind, which makes the answer more conservative rather than
//! less. So the mask stays a superset under every mutation, which is the one
//! thing this crate must never get wrong.

use super::kernels::ZONE_ROWS;

/// Zones summarised by one block. 32 × 512 = 16 384 rows.
pub const SUPERZONE_ZONES: u64 = 32;

/// Rows summarised by one block.
pub const SUPERZONE_ROWS: u64 = SUPERZONE_ZONES * ZONE_ROWS as u64;

/// Bits in one superzone's block: 512 bytes, or 0.25 bits/row.
pub const BLOCK_BITS: usize = 4096;

/// `u32` lanes in a split-block. One bit is set per lane, so a probe is eight
/// bit tests and the false-positive rate is `fill^8`.
pub(super) const LANES: usize = 8;

/// Words in one superzone's block.
const BLOCK_WORDS: usize = BLOCK_BITS / 32;

/// Split-blocks within one superzone's block. A probe touches exactly one.
const SUB_BLOCKS: usize = BLOCK_WORDS / LANES;

/// Odd multipliers, one per lane, from the Parquet split-block Bloom filter.
///
/// Each maps the key's low bits to a different bit position, and being odd each
/// is invertible, so no two lanes collapse onto the same bit for all keys.
pub(super) const SALT: [u32; LANES] = [
    0x47b6_137b,
    0x4497_4d91,
    0x8824_ad5b,
    0xa2b7_289d,
    0x7054_95c7,
    0x2df1_424b,
    0x9efc_4947,
    0x5c6b_fb31,
];

/// Decorrelate the summary from the fingerprint.
///
/// [`super::hash::fingerprint`] takes the *high* half of the hash. Taking the
/// low half here would be enough in principle, but one more mixing round costs
/// five ALU operations and removes the question entirely — the two uses cannot
/// share structure they were never handed.
#[inline(always)]
fn bloom_hash(h: u64) -> u64 {
    let mut x = h ^ 0x9E37_79B9_7F4A_7C15;
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// The eight lane masks for one key.
#[inline(always)]
fn lane_masks(key: u32) -> [u32; LANES] {
    let mut m = [0u32; LANES];
    for i in 0..LANES {
        // The top five bits of the product pick one of 32 positions.
        m[i] = 1u32 << (key.wrapping_mul(SALT[i]) >> 27);
    }
    m
}

/// The superzone a slot belongs to.
#[inline]
pub fn superzone_of(slot: u64) -> u64 {
    slot / SUPERZONE_ROWS
}

/// The first zone of a superzone.
#[inline]
pub fn first_zone_of(superzone: u64) -> u64 {
    superzone * SUPERZONE_ZONES
}

/// One split-block Bloom per superzone, for one column.
#[derive(Clone, Default)]
pub struct SuperBloom {
    /// `superzone_count * BLOCK_WORDS` lanes.
    words: Vec<u32>,
}

impl SuperBloom {
    pub fn new() -> Self {
        Self { words: Vec::new() }
    }

    /// Rebuild from persisted words. `None` when the length cannot describe a
    /// whole number of superzones.
    pub fn from_words(words: Vec<u32>) -> Option<Self> {
        if words.len() % BLOCK_WORDS != 0 {
            return None;
        }
        Some(Self { words })
    }

    #[inline]
    pub fn words(&self) -> &[u32] {
        &self.words
    }

    #[inline]
    pub fn superzone_count(&self) -> u64 {
        (self.words.len() / BLOCK_WORDS) as u64
    }

    #[inline]
    pub fn bytes(&self) -> usize {
        self.words.len() * 4
    }

    /// Words for superzones `[from, ..)`, for an incremental checkpoint.
    pub fn tail_from(&self, from_superzone: u64) -> &[u32] {
        let at = (from_superzone as usize).saturating_mul(BLOCK_WORDS);
        self.words.get(at..).unwrap_or(&[])
    }

    /// Splice a tail back in, replacing every superzone from `from` on.
    pub fn apply_tail(&mut self, from_superzone: u64, words: &[u32]) -> bool {
        if words.len() % BLOCK_WORDS != 0 {
            return false;
        }
        let Some(keep) = (from_superzone as usize).checked_mul(BLOCK_WORDS) else {
            return false;
        };
        if self.words.len() < keep {
            self.words.resize(keep, 0);
        }
        self.words.truncate(keep);
        self.words.extend_from_slice(words);
        true
    }

    #[inline]
    fn locate(&self, superzone: u64, h: u64) -> Option<(usize, [u32; LANES])> {
        let bh = bloom_hash(h);
        // The sub-block comes from the high half and the lane key from the low,
        // so the two choices are independent.
        let sub = ((bh >> 32) as usize) % SUB_BLOCKS;
        let base = (superzone as usize).checked_mul(BLOCK_WORDS)? + sub * LANES;
        Some((base, lane_masks(bh as u32)))
    }

    /// Record that `hash` occurs in `superzone`.
    pub fn add(&mut self, superzone: u64, hash: u64) {
        let needed = (superzone as usize + 1) * BLOCK_WORDS;
        if self.words.len() < needed {
            self.words.resize(needed, 0);
        }
        let Some((base, masks)) = self.locate(superzone, hash) else {
            return;
        };
        for (w, m) in self.words[base..base + LANES].iter_mut().zip(masks) {
            *w |= m;
        }
    }

    /// Might `hash` occur anywhere in `superzone`?
    ///
    /// `true` for a superzone that has never been written, which is the
    /// conservative answer and the one the watermark expects.
    #[inline]
    pub fn may_contain(&self, superzone: u64, hash: u64) -> bool {
        if superzone >= self.superzone_count() {
            return true;
        }
        let Some((base, masks)) = self.locate(superzone, hash) else {
            return true;
        };
        self.words[base..base + LANES]
            .iter()
            .zip(masks)
            .all(|(w, m)| *w & m != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geometry_is_consistent() {
        assert_eq!(SUPERZONE_ROWS, 16_384);
        assert_eq!(BLOCK_WORDS, 128);
        assert_eq!(SUB_BLOCKS, 16);
        // 512 bytes per 16 384 rows.
        assert_eq!(BLOCK_WORDS * 4, 512);
        assert!((BLOCK_WORDS * 4 * 8) as f64 / SUPERZONE_ROWS as f64 - 0.25 < 1e-9);
    }

    #[test]
    fn what_was_added_is_always_found() {
        // The property that makes this safe to prune on: no false negatives,
        // ever, for any key that was inserted.
        let mut b = SuperBloom::new();
        let keys: Vec<u64> = (0..2000).map(|_| fastrand::u64(..)).collect();
        for (i, k) in keys.iter().enumerate() {
            b.add((i % 5) as u64, *k);
        }
        for (i, k) in keys.iter().enumerate() {
            assert!(b.may_contain((i % 5) as u64, *k), "lost key {}", k);
        }
    }

    #[test]
    fn an_unwritten_superzone_answers_maybe() {
        let b = SuperBloom::new();
        assert!(b.may_contain(0, 12345));
        assert!(b.may_contain(9999, 12345));
    }

    #[test]
    fn a_sparse_block_rejects_almost_everything() {
        // 200 distinct values in 4096 bits: the regime the tier exists for.
        let mut b = SuperBloom::new();
        for i in 0..200u64 {
            b.add(0, i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        }
        let trials = 20_000;
        let hits = (0..trials)
            .filter(|_| b.may_contain(0, fastrand::u64(..)))
            .count();
        let fpp = hits as f64 / trials as f64;
        assert!(
            fpp < 0.02,
            "200 keys in 4096 bits gave {:.3} false positives",
            fpp
        );
    }

    #[test]
    fn a_saturated_block_stays_sound() {
        // 16 384 distinct values in 4096 bits is hopeless, and the failure mode
        // has to be "answers maybe", never "answers no".
        let mut b = SuperBloom::new();
        let keys: Vec<u64> = (0..SUPERZONE_ROWS).map(|_| fastrand::u64(..)).collect();
        for k in &keys {
            b.add(0, *k);
        }
        for k in &keys {
            assert!(b.may_contain(0, *k));
        }
    }

    #[test]
    fn superzones_do_not_leak_into_each_other() {
        let mut b = SuperBloom::new();
        let k = 0xDEAD_BEEF_1234_5678u64;
        b.add(3, k);
        assert!(b.may_contain(3, k));
        // Neighbouring blocks were never written, so they are all zero and can
        // answer "no" — a written block is what makes the answer meaningful.
        b.add(2, 1);
        b.add(4, 1);
        assert!(!b.may_contain(2, k));
        assert!(!b.may_contain(4, k));
    }

    #[test]
    fn tails_round_trip() {
        let mut b = SuperBloom::new();
        for sz in 0..6u64 {
            b.add(sz, sz.wrapping_mul(7919));
        }
        let tail = b.tail_from(4).to_vec();
        let mut c = SuperBloom::new();
        for sz in 0..4u64 {
            c.add(sz, sz.wrapping_mul(7919));
        }
        assert!(c.apply_tail(4, &tail));
        for sz in 0..6u64 {
            assert!(c.may_contain(sz, sz.wrapping_mul(7919)), "superzone {}", sz);
        }
        assert_eq!(c.words(), b.words());
    }
}
