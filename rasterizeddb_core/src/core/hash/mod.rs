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
#[derive(Clone)]
pub struct ItemFeed(Xxh64);

impl ItemFeed {
    pub fn new(seed: u64) -> ItemFeed {
        ItemFeed(Xxh64::new(seed))
    }

    #[inline]
    pub fn u8(&mut self, v: u8) {
        self.0.update(&[v]);
    }

    #[inline]
    pub fn u64_le(&mut self, v: u64) {
        self.0.update(&v.to_le_bytes());
    }

    #[inline]
    pub fn i64_le(&mut self, v: i64) {
        self.0.update(&v.to_le_bytes());
    }

    /// `v` as it stands, with no length: a caller that follows it with more
    /// fields feeds the length itself.
    #[inline]
    pub fn bytes(&mut self, v: &[u8]) {
        self.0.update(v);
    }

    pub fn finish(&self) -> u64 {
        self.0.digest()
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
}
