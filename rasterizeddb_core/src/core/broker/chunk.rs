//! An append-only buffer that one writer fills and any number of readers read
//! without a lock.
//!
//! A partition's newest batches live in a run of chunks (docs/broker.md §2.4).
//! An append copies a batch into the current chunk under the partition's
//! mutex and then publishes the new length with `Release`. A reader clones
//! the chunk's `Arc` (under the mutex, which is cheap), drops the mutex, and
//! copies from `[..published]` with an `Acquire` load. A byte is never
//! written again once its length has been published, so a reader can never
//! see a write in progress, and its copy holds up no producer.

use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Chunk {
    /// The buffer, from a leaked `Box<[u8]>` of `cap` bytes, freed on drop.
    /// Held as a pointer so a reader's slice of `[..len]` and the writer's of
    /// `[len..len + n]` never overlap one reference to the whole.
    ptr: NonNull<u8>,
    cap: usize,
    len: AtomicUsize,
    /// Bytes the flusher has written to the segment file.
    flushed: AtomicUsize,
    /// The offset of the first record in this chunk.
    pub first_offset: u64,
    /// Where the first byte goes in its segment file, and which segment
    /// (by base offset): the flusher writes `[written..len]` there.
    pub segment_base: u64,
    pub file_pos: u64,
}

// SAFETY: bytes below `len` are immutable once published (Release), and only
// the holder of the partition mutex writes above it. A reader only touches
// `[..len]` it loaded with Acquire.
unsafe impl Sync for Chunk {}
unsafe impl Send for Chunk {}

impl Chunk {
    pub fn new(capacity: usize, first_offset: u64, segment_base: u64, file_pos: u64) -> Chunk {
        let boxed: Box<[u8]> = vec![0u8; capacity.max(1)].into_boxed_slice();
        let cap = boxed.len();
        // SAFETY: a Box's pointer is never null.
        let ptr = unsafe { NonNull::new_unchecked(Box::into_raw(boxed) as *mut u8) };
        Chunk {
            ptr,
            cap,
            len: AtomicUsize::new(0),
            flushed: AtomicUsize::new(0),
            first_offset,
            segment_base,
            file_pos,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    /// Bytes written to the segment file.
    #[inline]
    pub fn flushed(&self) -> usize {
        self.flushed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_flushed(&self, n: usize) {
        self.flushed.fetch_max(n, Ordering::AcqRel);
    }

    #[inline]
    pub fn room(&self) -> usize {
        self.capacity() - self.len.load(Ordering::Relaxed)
    }

    /// The published bytes.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        let n = self.len();
        // SAFETY: `[..n]` was published and is never written again.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), n) }
    }

    /// Room for `n` more bytes, to be filled and then published by
    /// [`Chunk::publish`]. Only the holder of the partition's mutex may call
    /// this, and only one such reservation may be open at a time.
    ///
    /// # Safety
    /// The caller is the only writer (it holds the partition mutex) and
    /// `n <= self.room()`.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn reserve(&self, n: usize) -> &mut [u8] {
        let at = self.len.load(Ordering::Relaxed);
        assert!(at + n <= self.cap, "chunk overrun");
        // SAFETY: `[at..at + n]` is inside the buffer, above every published
        // byte, and the caller is the only writer.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr().add(at), n) }
    }

    /// Publishes `n` bytes written through [`Chunk::reserve`].
    #[inline]
    pub fn publish(&self, n: usize) {
        let at = self.len.load(Ordering::Relaxed);
        self.len.store(at + n, Ordering::Release);
    }

    /// Copies `bytes` in and publishes them.
    ///
    /// # Safety
    /// As [`Chunk::reserve`]: the caller is the only writer and `bytes` fits.
    #[inline]
    pub unsafe fn append(&self, bytes: &[u8]) {
        // SAFETY: forwarded to our caller.
        unsafe { self.reserve(bytes.len()).copy_from_slice(bytes) };
        self.publish(bytes.len());
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`cap` came from `Box::into_raw` of a `[u8]` of `cap`.
        unsafe {
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(self.ptr.as_ptr(), self.cap)));
        }
    }
}
