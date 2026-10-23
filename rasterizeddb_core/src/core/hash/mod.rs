//! Hashes whose outputs may be persisted, and so must never move.
//!
//! `ahash`, `std`'s `DefaultHasher` and every `RandomState` are free to change
//! their output between crate versions, CPU features and pointer widths, and
//! the randomly seeded ones between processes. Anything written to disk, or
//! compared against something that was, hashes through here instead.

pub mod xxh64;

pub use xxh64::{Xxh64, xxh64};

/// A fixed-width, little-endian byte feed into XXH64.
///
/// Deliberately not a [`std::hash::Hasher`]: that trait's inherited
/// `write_u16` through `write_i128`, `write_usize` and `write_isize` feed
/// native-endian bytes, and `usize` changes width with the target, so one
/// stray call would make a persisted value depend on the machine that wrote
/// it. Here every method names its width and byte order, and nothing else
/// exists to call by mistake.
///
/// The digest is [`xxh64`] of exactly the bytes fed, in order, under the seed
/// given to [`ItemFeed::new`].
///
/// An item is short, so its bytes gather in a stack buffer and are hashed by
/// one [`xxh64`] call at [`ItemFeed::finish`]; only a feed longer than the
/// buffer streams through [`Xxh64`]. Feeding each 1- and 8-byte field through
/// [`Xxh64::update`] instead cost about a fifth of a GIN build. Either way
/// the digest is the same, and the item vectors pin it.
#[derive(Clone)]
pub struct ItemFeed {
    seed: u64,
    len: usize,
    buf: [u8; INLINE],
    /// The bytes already streamed out of `buf`, once a feed outgrows it.
    spilled: Option<Xxh64>,
}

/// The bytes an item gathers before it streams.
const INLINE: usize = 64;

impl ItemFeed {
    pub fn new(seed: u64) -> ItemFeed {
        ItemFeed {
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

    /// Streams what `buf` holds, then `v`, and empties `buf`.
    #[cold]
    fn spill(&mut self, v: &[u8]) {
        let h = self.spilled.get_or_insert_with(|| Xxh64::new(self.seed));
        h.update(&self.buf[..self.len]);
        h.update(v);
        self.len = 0;
    }

    pub fn finish(&self) -> u64 {
        match &self.spilled {
            None => xxh64(&self.buf[..self.len], self.seed),
            Some(h) => {
                let mut h = h.clone();
                h.update(&self.buf[..self.len]);
                h.digest()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each method feeds exactly the bytes its name says, little-endian on
    /// every target.
    #[test]
    fn the_feed_is_the_bytes_its_methods_name() {
        let seed = 0x0123_4567_89AB_CDEF;
        let mut f = ItemFeed::new(seed);
        f.u8(7);
        f.u64_le(0x0102_0304_0506_0708);
        f.i64_le(-2);
        f.bytes(b"ab");
        let mut want = vec![7u8];
        want.extend_from_slice(&[8, 7, 6, 5, 4, 3, 2, 1]);
        want.extend_from_slice(&[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
        want.extend_from_slice(b"ab");
        assert_eq!(f.finish(), xxh64(&want, seed));
        assert_eq!(ItemFeed::new(seed).finish(), xxh64(&[], seed));
    }

    /// The digest does not depend on where the feed crosses from the stack
    /// buffer to streaming: every length either side of it, fed in every
    /// piece size, and finished mid-feed as a clone, hashes the same bytes.
    #[test]
    fn the_digest_is_the_same_either_side_of_the_buffer() {
        let seed = 0x6a73_6f6e_6769_6e00;
        let data: Vec<u8> = (0..3 * INLINE as u32 + 7).map(|i| (i * 31 + 7) as u8).collect();
        for n in 0..data.len() {
            for piece in [1, 3, 8, 13, INLINE - 1, INLINE, INLINE + 1] {
                let mut f = ItemFeed::new(seed);
                for (i, chunk) in data[..n].chunks(piece).enumerate() {
                    match (chunk.len(), i % 3) {
                        (1, _) => f.u8(chunk[0]),
                        (8, 0) => f.u64_le(u64::from_le_bytes(chunk.try_into().unwrap())),
                        (8, 1) => f.i64_le(i64::from_le_bytes(chunk.try_into().unwrap())),
                        _ => f.bytes(chunk),
                    }
                    let fed: usize = data[..n].chunks(piece).take(i + 1).map(<[u8]>::len).sum();
                    assert_eq!(f.clone().finish(), xxh64(&data[..fed], seed), "n {n} piece {piece} at {fed}");
                }
                assert_eq!(f.finish(), xxh64(&data[..n], seed), "n {n} piece {piece}");
            }
        }
    }
}
