//! Readers of a file's bytes never see a write into those bytes half done.
//!
//! An in-place write rewrites bytes a reader may be copying at that moment:
//! a pointer record's `xmax` stamped by an UPDATE or DELETE, applied from
//! the log at commit (`wal::Wal::apply_held`) or without one
//! (`LocalStorageProvider::overwrite_with_undo`). The file is read through
//! a shared mapping and `pread`, and neither a `write` into the page cache
//! nor a `memcpy` out of it is atomic: a reader copying while the bytes
//! land can take some old and some new. Measured on the engine's own paths
//! (2026-11-11): a record whose `xmax` alternated between 0 and `0xabcde`
//! read back as `0xbcde` once in about 200 million reads, past the 8 MB the
//! mapping covers. A torn `xmax` names some older transaction, usually
//! committed, so the version reads as deleted: a reader missed the row for
//! that read, and a sweep could judge a live version reclaimable — free its
//! extent, clear its filter fingerprint and drop its index entries.
//!
//! So each file has a sequence count, a seqlock. A writer of bytes already
//! in the file makes it odd for the duration of the write and even after,
//! one writer at a time; a reader takes it before copying or decoding and
//! checks it after, and copies again if a write began or ended meanwhile.
//! Reading costs two loads and no store, so readers never contend with each
//! other; a retry costs only when a write lands during the copy.
//!
//! The count is per file, shared by every handle on it — the providers
//! ([`for_path`]), and the log, which applies through its own handle
//! ([`for_file`]). Keyed by the file's identity (device and inode), not its
//! name: the log knows a file by the path its directory scan found, which is
//! not always the string a provider was opened with (`dir//name`).
//! Appends are not in-place writes: their bytes are past every length a
//! reader has been given until they are published.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering, fence};
use std::sync::{Arc, Weak};

#[derive(Debug, Default)]
pub struct InPlaceSeq {
    seq: AtomicU64,
    /// One writer at a time: two at once would leave the count even while
    /// both are writing.
    writers: parking_lot::Mutex<()>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Key {
    Inode(u64, u64),
    /// Where the identity could not be read: shared only with handles that
    /// name the file the same way.
    Path(String),
}

#[cfg(unix)]
fn key_of(metadata: std::io::Result<std::fs::Metadata>, path: &str) -> Key {
    use std::os::unix::fs::MetadataExt;
    match metadata {
        Ok(m) => Key::Inode(m.dev(), m.ino()),
        Err(e) => {
            log::warn!("in-place reads of {path}: no file identity ({e}); keyed by name");
            Key::Path(path.to_string())
        }
    }
}

#[cfg(not(unix))]
fn key_of(_metadata: std::io::Result<std::fs::Metadata>, path: &str) -> Key {
    Key::Path(
        std::fs::canonicalize(path)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| path.to_string()),
    )
}

static REGISTRY: parking_lot::Mutex<Option<HashMap<Key, Weak<InPlaceSeq>>>> =
    parking_lot::Mutex::new(None);

/// The count of the file at `path`, which exists: for a provider, once it
/// has the file open.
pub fn for_path(path: &str) -> Arc<InPlaceSeq> {
    shared(key_of(std::fs::metadata(path), path))
}

/// The count of the open file `file` (at `path`, for the fallback).
pub fn for_file(file: &std::fs::File, path: &str) -> Arc<InPlaceSeq> {
    shared(key_of(file.metadata(), path))
}

fn shared(key: Key) -> Arc<InPlaceSeq> {
    let mut registry = REGISTRY.lock();
    let map = registry.get_or_insert_with(HashMap::new);
    if let Some(seq) = map.get(&key).and_then(Weak::upgrade) {
        return seq;
    }
    // Entries of files nobody has open any more, swept as the map doubles.
    if map.len() >= 64 && map.len().is_power_of_two() {
        map.retain(|_, seq| seq.strong_count() > 0);
    }
    let seq = Arc::new(InPlaceSeq::default());
    map.insert(key, Arc::downgrade(&seq));
    seq
}

impl InPlaceSeq {
    /// Runs `write`, an overwrite of bytes already in the file, as one step
    /// to every reader.
    pub fn write<R>(&self, write: impl FnOnce() -> R) -> R {
        let _one = self.writers.lock();
        self.seq.fetch_add(1, Ordering::AcqRel);
        let result = write();
        self.seq.fetch_add(1, Ordering::Release);
        result
    }

    /// Before a read: the count, once no write is under way.
    #[inline]
    pub fn read_begin(&self) -> u64 {
        let mut spins = 0u32;
        loop {
            let seq = self.seq.load(Ordering::Acquire);
            if seq & 1 == 0 {
                return seq;
            }
            spins += 1;
            if spins < 128 {
                std::hint::spin_loop();
            } else {
                // A writer held up mid-write (a slow device, a preemption).
                std::thread::yield_now();
            }
        }
    }

    /// After a read begun at `begin`: whether no write began or ended since,
    /// so what was read is all from before or all from after each write.
    #[inline]
    pub fn read_valid(&self, begin: u64) -> bool {
        fence(Ordering::Acquire);
        self.seq.load(Ordering::Relaxed) == begin
    }

    /// `read`, repeated until no write overlapped it.
    #[inline]
    pub fn read<R>(&self, mut read: impl FnMut() -> R) -> R {
        loop {
            let begin = self.read_begin();
            let result = read();
            if self.read_valid(begin) {
                return result;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The log and a provider name one file differently (`dir//name`,
    /// `dir/name`) and open their own handles; they share one count.
    ///
    /// Mutation: keying by the path string fails the first assert.
    #[test]
    fn handles_on_one_file_share_its_count_however_it_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let plain = format!("{}/a", dir.path().display());
        let doubled = format!("{}//a", dir.path().display());
        std::fs::write(&plain, b"x").unwrap();
        std::fs::write(format!("{}/c", dir.path().display()), b"x").unwrap();
        let a = for_path(&doubled);
        let b = for_file(&std::fs::File::open(&plain).unwrap(), &plain);
        let c = for_path(&format!("{}/c", dir.path().display()));
        assert!(Arc::ptr_eq(&a, &b), "two names of one file got two counts");
        assert!(!Arc::ptr_eq(&a, &c));
    }

    /// A read overlapping a write is done again; one before or after it
    /// is not.
    #[test]
    fn a_read_overlapping_a_write_is_repeated() {
        let seq = InPlaceSeq::default();
        let begin = seq.read_begin();
        assert!(seq.read_valid(begin));
        seq.write(|| ());
        assert!(!seq.read_valid(begin), "a read across a whole write was taken as valid");
        let mut tries = 0;
        let value = seq.read(|| {
            tries += 1;
            if tries == 1 {
                // A write lands during the first attempt.
                seq.seq.fetch_add(2, Ordering::AcqRel);
            }
            tries
        });
        assert_eq!(value, 2, "the read overlapped by a write was not repeated");
    }
}
