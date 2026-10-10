use std::{
    arch::x86_64::{_MM_HINT_T0, _mm_prefetch},
    fmt,
};

#[cfg(feature = "memory_pool_reuse")]
use std::cell::UnsafeCell;

use crate::instructions::{copy_ptr_to_ptr, copy_vec_to_ptr};

pub static MEMORY_POOL: MemoryPool = MemoryPool::new();

use libmimalloc_sys::{mi_free, mi_malloc};

#[cfg(feature = "memory_pool_reuse")]
const MAX_REUSE_BYTES: usize = 1024 * 128; // 128 KiB

/// Heap size classes are quarter powers of two: `2^k × {1.25, 1.5, 1.75, 2}`.
/// `INLINE_CAPACITY` (16) bytes and below never reach the heap, so the first
/// class is 20 and the series runs 20, 24, 28, 32, 40, 48, 56, 64, 80, … up to
/// 128 KiB: four classes per doubling, from `2^4` to `2^17`.
///
/// Rounding to the next power of two wasted up to half of every cell — a
/// 17-byte value took 32, a 1025-byte one 2048. A quarter step wastes under
/// 25% at worst and ~11% on average for uniformly spread lengths.
#[cfg(feature = "memory_pool_reuse")]
const MIN_CLASS_SHIFT: u32 = 4; // classes (16, 32]
#[cfg(feature = "memory_pool_reuse")]
const MAX_CLASS_SHIFT: u32 = 17; // up to 128 KiB
#[cfg(feature = "memory_pool_reuse")]
const N_CLASSES: usize = ((MAX_CLASS_SHIFT - MIN_CLASS_SHIFT) * 4) as usize;

/// Blocks cached per size class. Deep enough that a row holding several columns
/// of the same class is fully served from cache, shallow enough for the large
/// classes that the retained footprint stays small.
#[cfg(feature = "memory_pool_reuse")]
const MAX_DEPTH: usize = 8;

/// Index of the class of a 4 KiB block: the last one that keeps eight.
#[cfg(feature = "memory_pool_reuse")]
const LAST_DEEP_CLASS: usize = ((12 - MIN_CLASS_SHIFT) * 4 - 1) as usize;

#[cfg(feature = "memory_pool_reuse")]
#[inline(always)]
const fn class_depth(class: usize) -> usize {
    // <= 4 KiB: keep 8. Above that, keep 2 — worst-case retained bytes per
    // thread stay under ~1.2 MiB with four classes per doubling.
    if class <= LAST_DEEP_CLASS { MAX_DEPTH } else { 2 }
}

/// The class a request of `size` bytes (17 ..= 128 KiB) falls in, as an
/// index, and that class's byte size.
///
/// A pure function of the size, so a block's class can always be recomputed
/// from its length: `heap_size_class(len)` is the allocation behind every
/// pooled block of that length. Applied to a class size it returns that size,
/// which is what `class_index` relies on when a block comes back.
#[cfg(feature = "memory_pool_reuse")]
#[inline(always)]
fn quarter_class(size: usize) -> (usize, usize) {
    debug_assert!(size > (1 << MIN_CLASS_SHIFT) && size <= MAX_REUSE_BYTES);
    let s = size - 1;
    // `s >= 16`, so the top bit is at position 4 or above, and the two bits
    // below it pick the quarter.
    let top = usize::BITS - 1 - s.leading_zeros();
    let quarter = (s >> (top - 2)) & 3;
    let class = ((top - MIN_CLASS_SHIFT) as usize) * 4 + quarter;
    let bytes = (4 + quarter + 1) << (top - 2);
    (class, bytes)
}

#[cfg(feature = "memory_pool_reuse")]
#[inline(always)]
fn heap_size_class(size: usize) -> usize {
    #[cfg(test)]
    if tests::POW2_CLASSES.load(std::sync::atomic::Ordering::Relaxed) {
        // The previous policy, for the A/B bench only. Every power of two from
        // 32 up is also a quarter class, so the free lists serve it unchanged.
        return size.next_power_of_two();
    }
    quarter_class(size).1
}

/// Per-thread free lists, indexed directly by size class.
///
/// This replaces a global `AtomicGenericCache<usize, ReuseSlot>` lookup. That
/// design hashed the size and then linear-scanned 64 cache-line-padded entries
/// (~20 KiB of loads) on *every* heap acquire and *every* release, cloned an
/// `Arc` per probe, and funnelled every thread through one atomic per class.
/// Here the class index is a shift, the lookup is an array index, and no thread
/// touches another thread's list — so there is no shared atomic at all.
/// The size class a heap block of logical length `len` is recycled under —
/// a pure function of `len`, which is what lets a block not store its
/// allocation size.
///
/// Deliberately the quarter class even while the test-only `POW2_CLASSES`
/// switch makes `acquire` allocate powers of two: a power of two is never
/// smaller than the quarter class of the same length, so a block filed here
/// is always at least as large as its class promises. The reverse (filing a
/// quarter-class block under its power of two) could hand out a block too
/// small for its class, so the free side must not follow the switch.
#[cfg(feature = "memory_pool_reuse")]
#[inline(always)]
fn recycle_class_size(len: usize) -> usize {
    if len <= MAX_REUSE_BYTES {
        quarter_class(len).1
    } else {
        len
    }
}

#[cfg(feature = "memory_pool_reuse")]
struct ThreadFreeLists {
    slots: [[*mut u8; MAX_DEPTH]; N_CLASSES],
    len: [u8; N_CLASSES],
}

#[cfg(feature = "memory_pool_reuse")]
impl ThreadFreeLists {
    const fn new() -> Self {
        Self {
            slots: [[std::ptr::null_mut(); MAX_DEPTH]; N_CLASSES],
            len: [0; N_CLASSES],
        }
    }
}

#[cfg(feature = "memory_pool_reuse")]
impl Drop for ThreadFreeLists {
    fn drop(&mut self) {
        // Hand everything still parked here back to the allocator rather than
        // leaking it when the thread goes away.
        for class in 0..N_CLASSES {
            for i in 0..self.len[class] as usize {
                let ptr = self.slots[class][i];
                if !ptr.is_null() {
                    unsafe { mi_free(ptr as *mut _) };
                }
            }
            self.len[class] = 0;
        }
    }
}

#[cfg(feature = "memory_pool_reuse")]
thread_local! {
    static FREE_LISTS: UnsafeCell<ThreadFreeLists> =
        const { UnsafeCell::new(ThreadFreeLists::new()) };
}

/// Class index for an already-rounded size class, or `None` if the size is
/// outside the reusable range.
#[cfg(feature = "memory_pool_reuse")]
#[inline(always)]
fn class_index(alloc_size: usize) -> Option<usize> {
    if alloc_size <= (1 << MIN_CLASS_SHIFT) || alloc_size > MAX_REUSE_BYTES {
        return None;
    }
    let (class, bytes) = quarter_class(alloc_size);
    debug_assert_eq!(bytes, alloc_size, "a pooled block's size must be a class size");
    Some(class)
}

/// Pops a cached block of `alloc_size` bytes from this thread's free list.
#[cfg(feature = "memory_pool_reuse")]
#[inline(always)]
fn reuse_pop(alloc_size: usize) -> *mut u8 {
    let Some(class) = class_index(alloc_size) else {
        return std::ptr::null_mut();
    };

    // `try_with` matters: a `MemoryBlock` can be dropped while TLS is being torn
    // down, after this key's destructor has already run.
    FREE_LISTS
        .try_with(|cell| {
            let lists = unsafe { &mut *cell.get() };
            let n = lists.len[class] as usize;
            if n == 0 {
                return std::ptr::null_mut();
            }
            lists.len[class] = (n - 1) as u8;
            std::mem::replace(&mut lists.slots[class][n - 1], std::ptr::null_mut())
        })
        .unwrap_or(std::ptr::null_mut())
}

/// Parks a block on this thread's free list. Returns `false` if the list for
/// that class is full and the caller should free the block outright.
#[cfg(feature = "memory_pool_reuse")]
#[inline(always)]
fn reuse_push(ptr: *mut u8, alloc_size: usize) -> bool {
    let Some(class) = class_index(alloc_size) else {
        return false;
    };

    FREE_LISTS
        .try_with(|cell| {
            let lists = unsafe { &mut *cell.get() };
            let n = lists.len[class] as usize;
            if n >= class_depth(class) {
                return false;
            }
            lists.slots[class][n] = ptr;
            lists.len[class] = (n + 1) as u8;
            true
        })
        .unwrap_or(false)
}

// Define the maximum size for inline allocation
// NOTE: Keep this small enough that common "large" test sizes (e.g. 100/128)
// are forced onto the heap.
const INLINE_CAPACITY: usize = 16;

pub struct MemoryPool;

unsafe impl Send for MemoryPool {}
unsafe impl Sync for MemoryPool {}

impl MemoryPool {
    #[inline(always)]
    pub const fn new() -> Self {
        Self
    }

    #[inline(always)]
    #[track_caller]
    pub fn acquire(&self, size: usize) -> MemoryBlock {
        if size <= INLINE_CAPACITY {
            // Allocate on the stack (within the union's inline_data)
            let data = [0u8; INLINE_CAPACITY];
            // No actual allocation needed, just zeroing the bytes for safety.
            // We can't directly copy bytes into `inline_data` from an external slice here
            // without more `unsafe` unless we take a `&[u8]` argument to `acquire`.
            // For now, we'll just initialize to zeros.
            MemoryBlock {
                data: MemoryBlockData { inline_data: data },
                len: size,
            }
        } else {
            self.acquire_heap(size)
        }
    }

    /// Heap half of `acquire`.
    ///
    /// Split out purely for readability — `acquire` is `#[inline(always)]` and
    /// most of its callers are small fixed-width columns that take the inline
    /// branch, so this half is the rarer one at most call sites.
    #[inline(always)]
    fn acquire_heap(&self, size: usize) -> MemoryBlock {
        // Allocate on the heap (optionally reusing an existing block of the same size class)
        #[cfg(feature = "memory_pool_reuse")]
        let alloc_size = if size <= MAX_REUSE_BYTES {
            heap_size_class(size)
        } else {
            size
        };

        #[cfg(not(feature = "memory_pool_reuse"))]
        let alloc_size = size;

        #[cfg(feature = "memory_pool_reuse")]
        {
            let reused = reuse_pop(alloc_size);
            if !reused.is_null() {
                return MemoryBlock {
                    data: MemoryBlockData { heap_ptr: reused },
                    len: size,
                };
            }
        }

        let ptr = unsafe { mi_malloc(alloc_size) as *mut u8 };

        if ptr.is_null() {
            panic!("Allocation failed (likely OOM).");
        }

        MemoryBlock {
            data: MemoryBlockData { heap_ptr: ptr },
            len: size, // len is the logical size; allocation may be larger
        }
    }

    #[inline(always)]
    fn release(&self, ptr: *mut u8) {
        if ptr.is_null() {
            panic!("Invalid operation, releasing null pointer.");
        }

        unsafe {
            mi_free(ptr as *mut _);
        }
    }

    #[cfg(feature = "memory_pool_reuse")]
    #[inline(always)]
    fn recycle_or_free(&self, ptr: *mut u8, allocated_size: usize) {
        if ptr.is_null() {
            panic!("Invalid operation, releasing null pointer.");
        }

        // Park it on this thread's free list; if that class is already full,
        // give it straight back to the allocator so memory stays bounded.
        if reuse_push(ptr, allocated_size) {
            return;
        }

        self.release(ptr);
    }
}

// The union to hold either the inline buffer or the heap pointer.
#[repr(C)] // Ensure a consistent memory layout, critical for unions
union MemoryBlockData {
    inline_data: [u8; INLINE_CAPACITY],
    heap_ptr: *mut u8,
}

// Remove the manual Clone implementation since we're using derive

/// A value's bytes: inline up to `INLINE_CAPACITY`, on the heap beyond it.
///
/// 24 bytes. It was 32: the heap arm also stored its allocation size and a
/// separate `is_heap` flag padded out a word. Both are functions of `len` —
/// a block is on the heap exactly when `len > INLINE_CAPACITY` (`acquire`
/// decides by that and nothing changes `len` afterwards), and the size it is
/// recycled under is `recycle_class_size(len)` — so neither is stored. It is
/// most of what took `Column` from 48 bytes to 32.
pub struct MemoryBlock {
    data: MemoryBlockData,
    len: usize, // Actual length of the data; also decides inline vs heap
}

const _: () = assert!(std::mem::size_of::<MemoryBlock>() == 24);

unsafe impl Send for MemoryBlock {}
unsafe impl Sync for MemoryBlock {}

impl Default for MemoryBlock {
    #[inline(always)]
    fn default() -> Self {
        // A default MemoryBlock will be an empty, inline block
        MemoryBlock {
            data: MemoryBlockData {
                inline_data: [0; INLINE_CAPACITY],
            },
            len: 0,
        }
    }
}

impl fmt::Debug for MemoryBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("MemoryBlock");
        debug_struct.field("len", &self.len);
        debug_struct.field("is_heap", &self.is_heap());

        if self.is_heap() {
            // For heap-allocated, show the pointer
            let ptr = unsafe { self.data.heap_ptr };
            debug_struct.field("ptr", &ptr);
            // Optionally, show a preview of the data (be careful with large sizes)
            let inline_data = &self.into_slice();
            let mut str_buffer = String::new();
            for byte in inline_data.iter() {
                str_buffer.push(*byte as char);
            }
            debug_struct.field("data", &str_buffer);
        } else {
            // For inline, show the inline capacity and the actual data bytes
            debug_struct.field("inline_capacity", &INLINE_CAPACITY);
            let inline_data = unsafe { &self.data.inline_data };
            let mut str_buffer = String::new();
            for byte in inline_data.iter() {
                str_buffer.push(*byte as char);
            }
            // Use self.as_slice() to only show the *used* portion of the inline data
            debug_struct.field("data", &str_buffer);
        }

        debug_struct.finish()
    }
}

impl Drop for MemoryBlock {
    #[inline(always)]
    #[track_caller]
    fn drop(&mut self) {
        if self.is_heap() {
            let ptr = unsafe { self.data.heap_ptr };

            #[cfg(feature = "memory_pool_reuse")]
            {
                MEMORY_POOL.recycle_or_free(ptr, recycle_class_size(self.len));
                return;
            }

            #[cfg(not(feature = "memory_pool_reuse"))]
            {
                MEMORY_POOL.release(ptr);
            }
        } else {
            // If it's inline, we don't need to do anything special
            // The memory will be automatically cleaned up when the block goes out of scope
        }
    }
}

impl Clone for MemoryBlock {
    #[inline(always)]
    fn clone(&self) -> Self {
        // Important: this must be a deep clone.
        // A shallow clone of a heap pointer would double-free on Drop and would also
        // cause aliasing where one block mutation corrupts the other.

        // Inline blocks own no heap memory, so cloning one is just a copy of the
        // fixed-size inline buffer. Copying all INLINE_CAPACITY bytes compiles to
        // a pair of moves instead of the `acquire` + variable-length
        // `copy_from_slice` the general path would emit — and this is the case
        // every numeric column takes.
        if !self.is_heap() {
            return MemoryBlock {
                data: MemoryBlockData {
                    inline_data: unsafe { self.data.inline_data },
                },
                len: self.len,
            };
        }

        let new_block = MEMORY_POOL.acquire(self.len);

        debug_assert!(new_block.is_heap(), "heap clone must allocate on heap");
        let src_ptr = unsafe { self.data.heap_ptr };
        let dst_ptr = unsafe { new_block.data.heap_ptr };
        unsafe {
            copy_ptr_to_ptr(src_ptr, dst_ptr, self.len);
        }

        new_block
    }
}

impl MemoryBlock {
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the bytes live on the heap. Decided by length alone: `acquire`
    /// puts anything longer than `INLINE_CAPACITY` there and nothing else.
    #[inline(always)]
    fn is_heap(&self) -> bool {
        self.len > INLINE_CAPACITY
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *const u8 {
        if self.is_heap() {
            unsafe { self.data.heap_ptr as *const u8 }
        } else {
            unsafe { self.data.inline_data.as_ptr() }
        }
    }

    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        if self.is_heap() {
            unsafe { self.data.heap_ptr }
        } else {
            unsafe { self.data.inline_data.as_mut_ptr() }
        }
    }

    #[inline(always)]
    pub fn prefetch_to_lcache(&self) {
        if self.is_heap() {
            // Prefetch the heap data to the L1 cache
            unsafe { _mm_prefetch::<_MM_HINT_T0>(self.data.heap_ptr as *const i8) };
        } else {
            // Prefetch the inline data to the L1 cache
            unsafe { _mm_prefetch::<_MM_HINT_T0>(self.data.inline_data.as_ptr() as *const i8) };
        }
    }

    // Create a new chunk from a vector with no memory chunk allocated from pool
    #[inline(always)]
    pub fn from_vec(vec: Vec<u8>) -> Self {
        let len = vec.len();
        let memory_chunk = MEMORY_POOL.acquire(len);

        // Handle empty vectors - no copying needed
        if len > 0 {
            if memory_chunk.is_heap() {
                copy_vec_to_ptr(vec.as_slice(), unsafe { memory_chunk.data.heap_ptr });
            } else {
                copy_vec_to_ptr(vec.as_slice(), unsafe {
                    memory_chunk.data.inline_data.as_ptr() as *mut u8
                });
            }
        }

        drop(vec);
        memory_chunk
    }

    #[inline(always)]
    pub fn into_slice(&self) -> &[u8] {
        if self.is_heap() {
            let ptr = unsafe { self.data.heap_ptr };
            debug_assert!(!ptr.is_null(), "heap pointer must not be null");
            unsafe { std::slice::from_raw_parts(ptr as *const u8, self.len) }
        } else {
            unsafe { &self.data.inline_data[..self.len] }
        }
    }

    #[inline(always)]
    pub fn into_slice_mut(&mut self) -> &mut [u8] {
        if self.is_heap() {
            let ptr = unsafe { self.data.heap_ptr };
            debug_assert!(!ptr.is_null(), "heap pointer must not be null");
            unsafe { std::slice::from_raw_parts_mut(ptr, self.len) }
        } else {
            unsafe { &mut self.data.inline_data[..self.len] }
        }
    }

    pub fn deep_clone(&self) -> Self {
        if self.is_heap() {
            // Allocate on the heap
            let new_block = MEMORY_POOL.acquire(self.len);
            unsafe {
                copy_ptr_to_ptr(self.data.heap_ptr, new_block.data.heap_ptr, self.len);
            }
            new_block
        } else {
            // Inline: a straight copy of the fixed-size buffer, same as `clone`.
            MemoryBlock {
                data: MemoryBlockData {
                    inline_data: unsafe { self.data.inline_data },
                },
                len: self.len,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    /// Switches `heap_size_class` back to powers of two, for the A/B bench.
    #[cfg(feature = "memory_pool_reuse")]
    pub(super) static POW2_CLASSES: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    #[cfg(feature = "memory_pool_reuse")]
    #[test]
    fn quarter_classes_are_tight_and_stable() {
        let mut last_class = 0usize;
        for size in 17..=super::MAX_REUSE_BYTES {
            let (class, bytes) = super::quarter_class(size);
            assert!(bytes >= size, "{size} -> {bytes}");
            // Never more than a quarter of the class above the request.
            assert!((bytes - size) * 5 < bytes + 5, "{size} -> {bytes}");
            assert!(class < super::N_CLASSES);
            assert!(class >= last_class, "classes are monotone in size");
            last_class = class;
            // The class of a class size is itself: a block coming back is filed
            // where one of that length is looked for.
            assert_eq!(super::quarter_class(bytes), (class, bytes));
            assert_eq!(super::class_index(bytes), Some(class));
        }
        assert_eq!(super::quarter_class(17), (0, 20));
        assert_eq!(super::quarter_class(33), (4, 40));
        assert_eq!(super::quarter_class(1025), (24, 1280));
        assert_eq!(super::quarter_class(4096), (super::LAST_DEEP_CLASS, 4096));
        assert_eq!(super::quarter_class(super::MAX_REUSE_BYTES).0, super::N_CLASSES - 1);
        // Every class size is distinct.
        let mut sizes: Vec<usize> = (17..=super::MAX_REUSE_BYTES)
            .map(|s| super::quarter_class(s).1)
            .collect();
        sizes.dedup();
        assert_eq!(sizes.len(), super::N_CLASSES);
    }

    /// Bytes the allocator really hands out for 1M text cells, and the RSS a
    /// million live blocks cost, quarter classes against powers of two.
    /// Lengths: 70% short text (17-64 B), 25% medium (65-512), 5% long
    /// (513-4096). `cargo test --release -p rasterizeddb_core --lib --
    /// --ignored text_cell_footprint_bench --nocapture`.
    #[cfg(feature = "memory_pool_reuse")]
    #[test]
    #[ignore]
    fn text_cell_footprint_bench() {
        use std::sync::atomic::Ordering::Relaxed;
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        let mut next = |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        let lens: Vec<usize> = (0..1_000_000)
            .map(|_| match next(100) {
                0..70 => 17 + next(48) as usize,
                70..95 => 65 + next(448) as usize,
                _ => 513 + next(3584) as usize,
            })
            .collect();
        let logical: usize = lens.iter().sum();
        // Declared here: `libmimalloc_sys` only exposes it with its `extended`
        // feature, which this crate does not enable, but the C library always
        // has the symbol.
        unsafe extern "C" {
            fn mi_usable_size(p: *const std::ffi::c_void) -> usize;
        }
        let rss = || {
            let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
            statm.split_whitespace().nth(1).unwrap().parse::<usize>().unwrap() * 4096
        };
        // `POOL_POLICY=pow2` or `=quarter` runs that policy once, so its RSS
        // is measured in a fresh process: the allocator keeps freed pages, so
        // a second policy in the same process reuses the first one's RSS.
        let only = std::env::var("POOL_POLICY").ok();
        let policies: Vec<bool> = match only.as_deref() {
            Some("pow2") => vec![true],
            Some("quarter") => vec![false],
            _ => vec![true, false],
        };
        let rounds = if only.is_some() { 1 } else { 3 };
        for round in 0..rounds {
            for &pow2 in &policies {
                POW2_CLASSES.store(pow2, Relaxed);
                let usable: usize = lens
                    .iter()
                    .map(|&l| {
                        let a = super::heap_size_class(l);
                        unsafe {
                            let p = libmimalloc_sys::mi_malloc(a);
                            let u = mi_usable_size(p as *const std::ffi::c_void);
                            libmimalloc_sys::mi_free(p);
                            u
                        }
                    })
                    .sum();
                let before = rss();
                let t = std::time::Instant::now();
                let blocks: Vec<MemoryBlock> = lens.iter().map(|&l| MEMORY_POOL.acquire(l)).collect();
                let took = t.elapsed();
                // Touch every block so its pages are resident.
                let mut blocks = blocks;
                for b in blocks.iter_mut() {
                    b.into_slice_mut().fill(1);
                }
                let grew = rss().saturating_sub(before);
                let t = std::time::Instant::now();
                drop(blocks);
                let freed = t.elapsed();
                println!(
                    "round {round} {:<7} logical {:>6.1} MB  allocator {:>6.1} MB ({:+.0}%)  RSS +{:>6.1} MB  acquire {:>8.2?} drop {:>8.2?}",
                    if pow2 { "pow2" } else { "quarter" },
                    logical as f64 / 1e6,
                    usable as f64 / 1e6,
                    (usable as f64 / logical as f64 - 1.0) * 100.0,
                    grew as f64 / 1e6,
                    took,
                    freed
                );
            }
        }
        POW2_CLASSES.store(false, Relaxed);
    }

    use std::{
        ptr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
    };

    use super::*;

    #[test]
    fn test_memory_pool_acquire_inline() {
        let pool = MemoryPool::new();
        let block = pool.acquire(10);
        assert!(!block.is_heap());
        assert_eq!(block.len, 10);
    }

    #[test]
    fn test_memory_pool_acquire_heap() {
        let pool = MemoryPool::new();
        let block = pool.acquire(100);
        assert!(block.is_heap());
        assert_eq!(block.len, 100);
    }

    #[test]
    fn test_memory_block_from_vec_inline() {
        let vec = vec![1, 2, 3, 4, 5];
        let block = MemoryBlock::from_vec(vec);
        assert!(!block.is_heap());
        let slice = block.into_slice();
        assert_eq!(slice, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_memory_block_from_vec_heap() {
        let vec = vec![1; 100];
        let block = MemoryBlock::from_vec(vec);
        assert!(block.is_heap());
        let slice = block.into_slice();
        assert_eq!(slice.len(), 100);
        assert!(slice.iter().all(|&x| x == 1));
    }

    #[test]
    fn test_memory_block_clone_inline() {
        let vec = vec![1, 2, 3, 4, 5];
        let block = MemoryBlock::from_vec(vec);
        assert_eq!(block.len, 5);
        assert_eq!(block.into_slice(), &[1, 2, 3, 4, 5]);
        let clone = block.clone();
        assert!(!clone.is_heap());
        let slice = clone.into_slice();
        assert_eq!(slice, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_memory_block_clone_heap() {
        let vec = vec![1; 100];
        let block = MemoryBlock::from_vec(vec);
        let clone = block.clone();
        assert!(clone.is_heap());
        let slice = clone.into_slice();
        assert_eq!(slice.len(), 100);
        assert!(slice.iter().all(|&x| x == 1));
    }

    #[test]
    fn test_memory_block_data_integrity_heap() {
        // Create a pattern of bytes to test
        let mut vec = Vec::with_capacity(100);
        for i in 0..100 {
            vec.push((i % 256) as u8);
        }

        let block = MemoryBlock::from_vec(vec.clone());
        assert!(block.is_heap());

        // Verify data integrity
        let slice = block.into_slice();
        for i in 0..100 {
            assert_eq!(slice[i], vec[i], "Data mismatch at index {}", i);
        }
    }

    #[test]
    fn test_memory_block_data_integrity_inline() {
        // Create a pattern of bytes for inline storage
        let mut vec = Vec::with_capacity(INLINE_CAPACITY);
        for i in 0..INLINE_CAPACITY {
            vec.push((i % 256) as u8);
        }

        let block = MemoryBlock::from_vec(vec.clone());
        assert!(!block.is_heap());

        // Verify data integrity
        let slice = block.into_slice();
        for i in 0..INLINE_CAPACITY {
            assert_eq!(slice[i], vec[i], "Data mismatch at index {}", i);
        }
    }

    #[test]
    fn test_memory_block_into_slice_mut() {
        let vec = vec![1, 2, 3, 4, 5];
        let mut block = MemoryBlock::from_vec(vec);
        let slice_mut = block.into_slice_mut();
        slice_mut[0] = 10;
        assert_eq!(block.into_slice()[0], 10);
    }

    #[test]
    fn test_memory_block_prefetch() {
        let block = MEMORY_POOL.acquire(10);
        // Just ensure it doesn't panic
        block.prefetch_to_lcache();

        let block = MEMORY_POOL.acquire(100);
        block.prefetch_to_lcache();
    }

    #[test]
    #[should_panic(expected = "releasing null pointer")]
    fn test_release_null_ptr_panics() {
        MEMORY_POOL.release(ptr::null_mut());
    }

    #[test]
    fn test_exact_capacity_boundary() {
        let pool = MemoryPool::new();

        // Test exactly at INLINE_CAPACITY
        let block_inline = pool.acquire(INLINE_CAPACITY);
        assert!(!block_inline.is_heap());

        // Test one byte over INLINE_CAPACITY
        let block_heap = pool.acquire(INLINE_CAPACITY + 1);
        assert!(block_heap.is_heap());
    }

    #[test]
    fn test_zero_size_allocation() {
        let block = MEMORY_POOL.acquire(0);
        assert!(!block.is_heap()); // Should use inline storage for zero-size
        assert_eq!(block.len, 0);
    }

    #[test]
    fn test_large_allocations() {
        // Test with a large allocation (1MB)
        let size = 1024 * 1024;
        let mut block = MEMORY_POOL.acquire(size);
        assert!(block.is_heap());
        assert_eq!(block.len, size);

        // Make sure we can write to all of it
        let slice_mut = block.into_slice_mut();
        for i in 0..size {
            slice_mut[i] = (i % 256) as u8;
        }

        // Verify the data
        for i in 0..size {
            assert_eq!(slice_mut[i], (i % 256) as u8);
        }
    }

    #[test]
    fn test_multithreaded_access() {
        // Test that memory_pool can be safely used from multiple threads
        let threads_count = 10;
        let ops_per_thread = 100;

        let barrier = Arc::new(std::sync::Barrier::new(threads_count));
        let errors = Arc::new(Mutex::new(Vec::new()));

        let mut handles = Vec::with_capacity(threads_count);

        for thread_id in 0..threads_count {
            let barrier = Arc::clone(&barrier);
            let errors = Arc::clone(&errors);

            let handle = thread::spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();

                for i in 0..ops_per_thread {
                    let size = (thread_id * i) % 100 + 1; // Vary sizes

                    let mut block = MEMORY_POOL.acquire(size);

                    // Check if allocation was successful
                    if block.is_heap() && size > INLINE_CAPACITY {
                        let slice = block.into_slice_mut();
                        if slice.len() != size {
                            let mut error_log = errors.lock().unwrap();
                            error_log.push(format!(
                                "Thread {} allocation {}: Expected size {}, got {}",
                                thread_id,
                                i,
                                size,
                                slice.len()
                            ));
                        }

                        // Write some data
                        for j in 0..size {
                            slice[j] = ((thread_id + j) % 256) as u8;
                        }

                        // Read it back and verify
                        for j in 0..size {
                            if slice[j] != ((thread_id + j) % 256) as u8 {
                                let mut error_log = errors.lock().unwrap();
                                error_log.push(format!(
                                    "Thread {} allocation {} data mismatch at index {}",
                                    thread_id, i, j
                                ));
                                break;
                            }
                        }
                    } else if !block.is_heap() && size <= INLINE_CAPACITY {
                        // Inline allocation checks
                        let slice = block.into_slice_mut();
                        if slice.len() != size {
                            let mut error_log = errors.lock().unwrap();
                            error_log.push(format!(
                                "Thread {} inline allocation {}: Expected size {}, got {}",
                                thread_id,
                                i,
                                size,
                                slice.len()
                            ));
                        }
                    } else {
                        let mut error_log = errors.lock().unwrap();
                        error_log.push(format!(
                            "Thread {} allocation {}: Wrong allocation type for size {}",
                            thread_id, i, size
                        ));
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for all threads to finish
        for handle in handles {
            handle.join().unwrap();
        }

        // Check if there were any errors
        let errors = errors.lock().unwrap();
        assert!(errors.is_empty(), "Thread errors: {:?}", errors);
    }

    #[test]
    fn test_memory_block_from_vec_empty() {
        let vec = Vec::<u8>::new();
        let block = MemoryBlock::from_vec(vec);
        assert!(!block.is_heap()); // Should use inline for empty vec
        let slice = block.into_slice();
        assert_eq!(slice.len(), 0);
    }

    #[test]
    fn test_drop_behavior() {
        // Create a struct to track if drop is called
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct DropCounter;
        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        // Scope to ensure drop
        {
            // Create some heap allocations
            let _dc = DropCounter;
            let _blocks: Vec<_> = (0..10).map(|_| MEMORY_POOL.acquire(100)).collect();
            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 0);

            // DropCounter should still be alive
            drop(_dc);
            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);

            // All blocks should still be alive
        }
        // Now blocks should be dropped and memory freed

        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_memory_corruption_prevention() {
        // This test specifically checks for the memory corruption bug that was reported
        let original_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut block1 = MemoryBlock::from_vec(original_data.clone());

        // Clone the block - this used to cause use-after-free
        let block2 = block1.clone();

        // Verify both blocks have the same data initially
        assert_eq!(block1.into_slice(), &original_data[..]);
        assert_eq!(block2.into_slice(), &original_data[..]);

        // Modify data through block1
        let slice1 = block1.into_slice_mut();
        slice1[0] = 99;

        // Verify that block2's data is NOT affected (proper isolation)
        assert_eq!(block1.into_slice()[0], 99);
        assert_eq!(block2.into_slice()[0], 1); // Should still be original value

        // Both blocks should maintain their integrity
        assert_eq!(&block1.into_slice()[1..], &original_data[1..]);
        assert_eq!(&block2.into_slice()[1..], &original_data[1..]);
    }

    #[test]
    fn test_aggressive_concurrent_access() {
        // More threads and allocations to stress test concurrency
        let threads_count = 32;
        let ops_per_thread = 500;
        let barrier = Arc::new(std::sync::Barrier::new(threads_count));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::with_capacity(threads_count);
        for thread_id in 0..threads_count {
            let barrier = Arc::clone(&barrier);
            let errors = Arc::clone(&errors);
            let handle = thread::spawn(move || {
                barrier.wait();
                for i in 0..ops_per_thread {
                    let size = (thread_id * i) % 128 + 1;
                    let mut block = MEMORY_POOL.acquire(size);
                    let slice = block.into_slice_mut();
                    if slice.len() != size {
                        let mut error_log = errors.lock().unwrap();
                        error_log.push(format!(
                            "Thread {} allocation {}: Expected size {}, got {}",
                            thread_id,
                            i,
                            size,
                            slice.len()
                        ));
                    }
                    for j in 0..size {
                        slice[j] = ((thread_id + j) % 256) as u8;
                    }
                    for j in 0..size {
                        if slice[j] != ((thread_id + j) % 256) as u8 {
                            let mut error_log = errors.lock().unwrap();
                            error_log.push(format!(
                                "Thread {} allocation {} data mismatch at index {}",
                                thread_id, i, j
                            ));
                            break;
                        }
                    }
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let errors = errors.lock().unwrap();
        assert!(errors.is_empty(), "Aggressive thread errors: {:?}", errors);
    }

    #[test]
    fn test_clone_and_drop_no_double_free() {
        let block = MemoryBlock::from_vec(vec![1, 2, 3, 4, 5]);
        let clone = block.clone();
        drop(block);
        drop(clone);
        // Should not panic
    }

    #[test]
    fn test_zero_size_allocation_edge() {
        let block = MemoryBlock::from_vec(vec![]);
        assert_eq!(block.len, 0);
        let slice = block.into_slice();
        assert_eq!(slice.len(), 0);
    }

    #[test]
    fn test_large_allocation_and_drop() {
        let block = MemoryBlock::from_vec(vec![1; 10_000]);
        assert!(block.is_heap());
        let slice = block.into_slice();
        assert_eq!(slice.len(), 10_000);
        drop(block);
    }

    #[test]
    fn test_concurrent_clone_and_drop_stress() {
        let block = std::sync::Arc::new(MemoryBlock::from_vec(vec![1, 2, 3, 4, 5]));
        let mut handles = vec![];
        for _ in 0..10 {
            let block = block.clone();
            handles.push(std::thread::spawn(move || {
                let clone = block.clone();
                drop(clone);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_deep_clone_inline() {
        let data = vec![10, 20, 30, 40, 50];
        let mut block = MemoryBlock::from_vec(data.clone());
        assert!(!block.is_heap());
        let deep = block.deep_clone();
        assert!(!deep.is_heap());
        assert_eq!(deep.len, block.len);
        assert_eq!(deep.into_slice(), block.into_slice());
        // Mutate original, deep clone should not change
        let slice_mut = block.into_slice_mut();
        slice_mut[0] = 99;
        assert_eq!(block.into_slice()[0], 99);
        assert_eq!(deep.into_slice()[0], 10);
    }

    #[test]
    fn test_deep_clone_heap() {
        let data = vec![7; 128];
        let mut block = MemoryBlock::from_vec(data.clone());
        assert!(block.is_heap());
        let deep = block.deep_clone();
        assert!(deep.is_heap());
        assert_eq!(deep.len, block.len);
        assert_eq!(deep.into_slice(), block.into_slice());
        // Mutate original, deep clone should not change
        let slice_mut = block.into_slice_mut();
        slice_mut[0] = 42;
        assert_eq!(block.into_slice()[0], 42);
        assert_eq!(deep.into_slice()[0], 7);
    }
}
