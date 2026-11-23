//! Hashes whose outputs may be persisted, and so must never move.
//!
//! `ahash`, `std`'s `DefaultHasher` and every `RandomState` are free to change
//! their output between crate versions, CPU features and pointer widths, and
//! the randomly seeded ones between processes. Anything written to disk, or
//! compared against something that was, hashes through here instead.
//!
//! [`xxh64`] is the default, for everything persisted or routed. GIN items,
//! alone, hash through [`ItemFeed::rapid`] (rapidhash at one frozen upstream
//! version, in the private `rapidhash` module): see that module for why, and
//! `source_tests` for the fence that keeps it to them. [`crc32c`] is the
//! index store's file checksum.

pub mod crc32c;
pub mod xxh64;
mod rapidhash;
#[cfg(test)]
mod source_tests;
#[cfg(test)]
mod test_buffers;

pub use xxh64::{Xxh64, xxh64};

use smallvec::SmallVec;

/// A fixed-width, little-endian byte feed into a frozen hash: XXH64 from
/// [`ItemFeed::new`], rapidhash from [`ItemFeed::rapid`].
///
/// Deliberately not a [`std::hash::Hasher`]: that trait's inherited
/// `write_u16` through `write_i128`, `write_usize` and `write_isize` feed
/// native-endian bytes, and `usize` changes width with the target, so one
/// stray call would make a persisted value depend on the machine that wrote
/// it. Here every method names its width and byte order, and nothing else
/// exists to call by mistake.
///
/// The digest is [`xxh64`] (or rapidhash) of exactly the bytes fed, in
/// order, under the seed given to the constructor.
///
/// An item is short, so its bytes gather in a stack buffer and are hashed by
/// one call at [`ItemFeed::finish`]. Feeding each 1- and 8-byte field through
/// [`Xxh64::update`] instead cost about a fifth of a GIN build. Only a feed
/// longer than the buffer goes further: XXH64 streams it through [`Xxh64`],
/// and rapidhash, which is one-shot, gathers the rest too. Either way the
/// digest is the same, and the item vectors pin it.
#[derive(Clone)]
pub struct ItemFeed {
    hash: Hash,
    seed: u64,
    len: usize,
    buf: [u8; INLINE],
    /// The bytes already moved out of `buf`, once a feed outgrows it.
    spilled: Option<Spilled>,
}

/// The function an [`ItemFeed`] finishes with, fixed by its constructor.
#[derive(Clone, Copy)]
enum Hash {
    Xxh64,
    Rapid,
}

#[derive(Clone)]
enum Spilled {
    /// XXH64 has streamed them.
    Xxh64(Xxh64),
    /// rapidhash has not: they wait, in order, for `finish`.
    Rapid(SmallVec<[u8; INLINE]>),
}

/// The bytes an item gathers before it spills.
const INLINE: usize = 64;

impl ItemFeed {
    /// A feed into [`xxh64`]: what everything persisted or routed hashes
    /// under (shape hashes, the vector history digests, partition routing).
    pub fn new(seed: u64) -> ItemFeed {
        ItemFeed::with(Hash::Xxh64, seed)
    }

    /// A feed into rapidhash (V3, frozen in tree). Only GIN items hash
    /// under it (`jsonb::item_hasher`, and `gin::GIN_HASHER_ID` names it);
    /// `source_tests` fails on any other caller, since anything else
    /// persisted stays on [`ItemFeed::new`].
    pub(crate) fn rapid(seed: u64) -> ItemFeed {
        ItemFeed::with(Hash::Rapid, seed)
    }

    #[inline]
    fn with(hash: Hash, seed: u64) -> ItemFeed {
        ItemFeed {
            hash,
            seed,
            len: 0,
            buf: [0; INLINE],
            spilled: None,
        }
    }

    #[inline]
    pub fn u8(&mut self, v: u8) {
        if self.len < INLINE {
            self.buf[self.len] = v;
            self.len += 1;
        } else {
            self.spill(&[v]);
        }
    }

    #[inline]
    pub fn u64_le(&mut self, v: u64) {
        self.put(&v.to_le_bytes());
    }

    #[inline]
    pub fn i64_le(&mut self, v: i64) {
        self.put(&v.to_le_bytes());
    }

    /// `v` as it stands, with no length: a caller that follows it with more
    /// fields feeds the length itself.
    #[inline]
    pub fn bytes(&mut self, v: &[u8]) {
        self.put(v);
    }

    #[inline]
    fn put(&mut self, v: &[u8]) {
        if let Some(room) = self.buf.get_mut(self.len..self.len + v.len()) {
            room.copy_from_slice(v);
            self.len += v.len();
        } else {
            self.spill(v);
        }
    }

    /// Moves what `buf` holds, then `v`, out (streamed or gathered, by the
    /// hash), and empties `buf`.
    #[cold]
    fn spill(&mut self, v: &[u8]) {
        let (hash, seed) = (self.hash, self.seed);
        let spilled = self.spilled.get_or_insert_with(|| match hash {
            Hash::Xxh64 => Spilled::Xxh64(Xxh64::new(seed)),
            Hash::Rapid => Spilled::Rapid(SmallVec::new()),
        });
        match spilled {
            Spilled::Xxh64(h) => {
                h.update(&self.buf[..self.len]);
                h.update(v);
            }
            Spilled::Rapid(gathered) => {
                gathered.extend_from_slice(&self.buf[..self.len]);
                gathered.extend_from_slice(v);
            }
        }
        self.len = 0;
    }

    pub fn finish(&self) -> u64 {
        let tail = &self.buf[..self.len];
        match (&self.spilled, self.hash) {
            (None, Hash::Xxh64) => xxh64(tail, self.seed),
            (None, Hash::Rapid) => rapidhash::rapidhash(tail, self.seed),
            (Some(Spilled::Xxh64(h)), _) => {
                let mut h = h.clone();
                h.update(tail);
                h.digest()
            }
            (Some(Spilled::Rapid(gathered)), _) => {
                let mut all = gathered.clone();
                all.extend_from_slice(tail);
                rapidhash::rapidhash(&all, self.seed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each constructor's feed, beside the one-shot function it must equal.
    fn feeds() -> [(&'static str, fn(u64) -> ItemFeed, fn(&[u8], u64) -> u64); 2] {
        [("xxh64", ItemFeed::new, xxh64), ("rapidhash", ItemFeed::rapid, rapidhash::rapidhash)]
    }

    /// Each method feeds exactly the bytes its name says, little-endian on
    /// every target, into the hash its constructor names.
    #[test]
    fn the_feed_is_the_bytes_its_methods_name() {
        let seed = 0x0123_4567_89AB_CDEF;
        for (name, feed, hash) in feeds() {
            let mut f = feed(seed);
            f.u8(7);
            f.u64_le(0x0102_0304_0506_0708);
            f.i64_le(-2);
            f.bytes(b"ab");
            let mut want = vec![7u8];
            want.extend_from_slice(&[8, 7, 6, 5, 4, 3, 2, 1]);
            want.extend_from_slice(&[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
            want.extend_from_slice(b"ab");
            assert_eq!(f.finish(), hash(&want, seed), "{name}");
            assert_eq!(feed(seed).finish(), hash(&[], seed), "{name}");
        }
        // `new` is XXH64 and stays so: shape hashes and the vector history
        // digests are made with it.
        assert_eq!(ItemFeed::new(0).finish(), 0xEF46_DB37_51D8_E999);
        assert_ne!(ItemFeed::rapid(0).finish(), ItemFeed::new(0).finish());
    }

    /// The digest does not depend on where the feed crosses from the stack
    /// buffer to streaming or gathering: every length either side of it, fed
    /// in every piece size, and finished mid-feed as a clone, hashes the same
    /// bytes. The lengths pass rapidhash's 112-byte block too.
    #[test]
    fn the_digest_is_the_same_either_side_of_the_buffer() {
        let seed = 0x6a73_6f6e_6769_6e00;
        let data: Vec<u8> = (0..3 * INLINE as u32 + 7).map(|i| (i * 31 + 7) as u8).collect();
        for (name, feed, hash) in feeds() {
            for n in 0..data.len() {
                for piece in [1, 3, 8, 13, INLINE - 1, INLINE, INLINE + 1] {
                    let mut f = feed(seed);
                    for (i, chunk) in data[..n].chunks(piece).enumerate() {
                        match (chunk.len(), i % 3) {
                            (1, _) => f.u8(chunk[0]),
                            (8, 0) => f.u64_le(u64::from_le_bytes(chunk.try_into().unwrap())),
                            (8, 1) => f.i64_le(i64::from_le_bytes(chunk.try_into().unwrap())),
                            _ => f.bytes(chunk),
                        }
                        let fed: usize = data[..n].chunks(piece).take(i + 1).map(<[u8]>::len).sum();
                        assert_eq!(f.clone().finish(), hash(&data[..fed], seed), "{name} n {n} piece {piece} at {fed}");
                    }
                    assert_eq!(f.finish(), hash(&data[..n], seed), "{name} n {n} piece {piece}");
                }
            }
        }
    }
}
