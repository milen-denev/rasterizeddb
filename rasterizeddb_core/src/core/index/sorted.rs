//! An index's contents as one sorted stream, read out by `for_each_sorted`
//! (hook C3) on [`LiveIndex`](super::LiveIndex) and
//! [`GinIndex`](super::gin::GinIndex). What a persisted snapshot's INDEX and
//! GIN sections hold (docs/persistent-indexes.md §1, §3 step 5); the bytes
//! are theirs.
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

/// The presence words without trailing zero words: what the stream carries,
/// so equal contents give equal streams however the bitmap grew.
pub(super) fn trimmed(words: &[u64]) -> &[u64] {
    &words[..words.iter().rposition(|w| *w != 0).map_or(0, |w| w + 1)]
}
