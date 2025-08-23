use std::{
    arch::x86_64::{_mm_prefetch, _MM_HINT_T0}, fmt, hash::Hash, sync::{atomic::{AtomicU64, Ordering}, Arc}
};

use crate::instructions::{copy_ptr_to_ptr, copy_vec_to_ptr, ref_slice, ref_slice_mut};

pub static MEMORY_POOL: MemoryPool = MemoryPool::new();

use libmimalloc_sys::{mi_malloc, mi_free};

// Define the maximum size for inline allocation
const INLINE_CAPACITY: usize = 64;

pub struct MemoryPool;

unsafe impl Send for MemoryPool {}
unsafe impl Sync for MemoryPool {}

impl MemoryPool {
    #[inline(always)]
    pub const  fn new() -> Self {
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
                hash: 0, // Default hash value
                data: MemoryBlockData { inline_data: data },
                len: size,
                is_heap: false,
                should_drop: true,
                //dropped: Arc::new(AtomicBool::new(false)),
                references: Arc::new(AtomicU64::new(0)),
            }
        } else {
            // Allocate on the heap
            let ptr = unsafe { mi_malloc(size as usize) as *mut u8 };

            if ptr.is_null() {
                panic!("Allocation failed (likely OOM).");
            }

            MemoryBlock {
                hash: 0, // Default hash value
                data: MemoryBlockData { heap_data: (ptr, size) },
                len: size, // For heap, len is the full size
                is_heap: true,
                should_drop: true,
                //dropped: Arc::new(AtomicBool::new(false)),
                references: Arc::new(AtomicU64::new(1)),
            }
        }
    }

    #[inline(always)]
    pub fn acquire_with_hash(&self, size: usize) -> MemoryBlock {
        if size <= INLINE_CAPACITY {
            // Allocate on the stack (within the union's inline_data)
            let data = [0u8; INLINE_CAPACITY];
            // No actual allocation needed, just zeroing the bytes for safety.
            // We can't directly copy bytes into `inline_data` from an external slice here
            // without more `unsafe` unless we take a `&[u8]` argument to `acquire`.
            // For now, we'll just initialize to zeros.
            MemoryBlock {
                hash: fastrand::u64(0..u64::MAX), // Default hash value
                data: MemoryBlockData { inline_data: data },
                len: size,
                is_heap: false,
                should_drop: true,
                //dropped: Arc::new(AtomicBool::new(false)),
                references: Arc::new(AtomicU64::new(0))
            }
        } else {
            // Allocate on the heap
            let ptr = unsafe { mi_malloc(size as usize) as *mut u8 };

            if ptr.is_null() {
                panic!("Allocation failed (likely OOM).");
            }

            MemoryBlock {
                hash: fastrand::u64(0..u64::MAX), // Default hash value
                data: MemoryBlockData { heap_data: (ptr, size) },
                len: size, // For heap, len is the full size
                is_heap: true,
                should_drop: true,
                //dropped: Arc::new(AtomicBool::new(false)),
                references: Arc::new(AtomicU64::new(1)),
            }
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
}

// The union to hold either the inline buffer or the heap pointer/size
#[repr(C)] // Ensure a consistent memory layout, critical for unions
union MemoryBlockData {
    inline_data: [u8; INLINE_CAPACITY],
    heap_data: (*mut u8, usize), // Tuple for pointer and size
}

// Remove the manual Clone implementation since we're using derive

// The main MemoryBlock struct that manages the union
pub struct MemoryBlock {
    hash: u64, // Hash for the block, if needed
    data: MemoryBlockData,
    len: usize, // Actual length of the data
    is_heap: bool, // Discriminant for the union: true if heap, false if inline
    should_drop: bool,
    //dropped: Arc<AtomicBool>,
    references: Arc<AtomicU64>
}

unsafe impl Send for MemoryBlock {}
unsafe impl Sync for MemoryBlock {}

impl Default for MemoryBlock {
     #[inline(always)]
    fn default() -> Self {
        // A default MemoryBlock will be an empty, inline block
        MemoryBlock {
            hash: 0, // Default hash value
            data: MemoryBlockData { inline_data: [0; INLINE_CAPACITY] }, // Initialize with zeros
            len: 0,
            is_heap: false, // It's an inline block
            should_drop: false, // Not needed as it's implied by is_heap,
            //dropped: Arc::new(AtomicBool::new(false)),
            references: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl fmt::Debug for MemoryBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("MemoryBlock");
        debug_struct.field("len", &self.len);
        debug_struct.field("is_heap", &self.is_heap);

        if self.is_heap {
            // For heap-allocated, show the pointer and the full allocated size
            let (ptr, allocated_size) = unsafe { self.data.heap_data };
            debug_struct.field("ptr", &ptr);
            debug_struct.field("allocated_size", &allocated_size);
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
        if self.should_drop && self.is_heap {
            let refs = self.references.fetch_sub(1, Ordering::Release);
            
            if refs == 1 {
                // let was_dropped = self.dropped
                //     .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                //     .expect("MemoryBlock was already dropped");

                // if was_dropped {
                //     panic!("MemoryBlock was already dropped, this should not happen.");
                // }

                MEMORY_POOL.release(unsafe { self.data.heap_data.0 });
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
        let references = Arc::clone(&self.references);

        if self.is_heap {
            references.fetch_add(1, Ordering::Release);

            MemoryBlock {
                hash: self.hash,
                data: MemoryBlockData { 
                    heap_data: (
                    unsafe { 
                        self.data.heap_data.0.clone() 
                    }, 
                    unsafe { 
                        self.data.heap_data.1.clone() 
                    }) 
                },
                len: self.len,
                is_heap: true,
                should_drop: true, // Important: new allocation should be cleaned up
                //dropped: self.dropped.clone(),
                references: references
            }
        } else {
            // For inline data, we can safely copy the data
            MemoryBlock {
                hash: self.hash,
                data: unsafe { MemoryBlockData { inline_data: self.data.inline_data.clone() } }, // This is safe to copy for inline data
                len: self.len,
                is_heap: false,
                should_drop: true, // Inline doesn't need cleanup, but keep consistent
                //dropped: self.dropped.clone(),
                references: references
            }
        }
    }
}

impl MemoryBlock {
    #[inline(always)]
    pub fn prefetch_to_lcache(&self) {
        // let was_dropped = self.dropped.load(Ordering::Relaxed);

        // if was_dropped {
        //     panic!("MemoryBlock was already dropped, this should not happen.");
        // }

        if self.is_heap {
            // if unsafe { self.data.heap_data.0.is_null() } {
            //     panic!("MemoryBlock is heap-allocated but pointer is null.");
            // }
            // Prefetch the heap data to the L1 cache
            unsafe { _mm_prefetch::<_MM_HINT_T0>(self.data.heap_data.0 as *const i8) };
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
            if memory_chunk.is_heap {
                copy_vec_to_ptr(vec.as_slice(), unsafe { memory_chunk.data.heap_data.0 });
            } else {
                copy_vec_to_ptr(vec.as_slice(), unsafe { memory_chunk.data.inline_data.as_ptr() as *mut u8 });
            }
        }
      
        drop(vec);
        memory_chunk
    }

    #[inline(always)]
    pub fn into_slice(&self) -> &'static [u8] {
        // let was_dropped = self.dropped.load(Ordering::Relaxed);

        // if was_dropped {
        //     panic!("MemoryBlock was already dropped, this should not happen.");
        // }

        if self.is_heap {
            // if unsafe { self.data.heap_data.0.is_null() } {
            //     panic!("MemoryBlock is heap-allocated but pointer is null.");
            // }
            // If it's a heap allocation, return the slice from the pointer
            unsafe { ref_slice(self.data.heap_data.0, self.len) }
        } else {
            // If it's inline, return the slice from the inline data
            unsafe { ref_slice(self.data.inline_data.as_ptr() as *mut u8, self.len) }
        }
    }

    #[inline(always)]
    pub fn into_slice_mut(&self) -> &'static mut [u8] {
        // let was_dropped = self.dropped.load(Ordering::Relaxed);

        // if was_dropped {
        //     panic!("MemoryBlock was already dropped, this should not happen.");
        // }

        if self.is_heap {
            // if unsafe { self.data.heap_data.0.is_null() } {
            //     panic!("MemoryBlock is heap-allocated but pointer is null.");
            // }
            // If it's a heap allocation, return the mutable slice from the pointer
            unsafe { ref_slice_mut(self.data.heap_data.0, self.len) }
        } else {
            // If it's inline, return the mutable slice from the inline data
            unsafe { ref_slice_mut(self.data.inline_data.as_ptr() as *mut u8, self.len) }
        }
    }

    #[inline(always)]
    pub fn from_slice_hashed(slice: &[u8]) -> MemoryBlock {
        let len = slice.len();
        let memory_chunk = MEMORY_POOL.acquire_with_hash(len);

        // Handle empty slices - no copying needed
        if len > 0 {
            if memory_chunk.is_heap {
                copy_vec_to_ptr(slice, unsafe { memory_chunk.data.heap_data.0 });
            } else {
                copy_vec_to_ptr(slice, unsafe { memory_chunk.data.inline_data.as_ptr() as *mut u8 });
            }
        }

        memory_chunk
    }

    pub fn deep_clone(&self) -> Self {
        if self.is_heap {
            // Allocate on the heap
            let new_block = MEMORY_POOL.acquire(self.len);
            unsafe {
                copy_ptr_to_ptr(self.data.heap_data.0, new_block.data.heap_data.0, self.len);
            }
            new_block
        } else {
            // Allocate inline
            let mut new_block = MEMORY_POOL.acquire(self.len);
            // Only copy the used portion of inline_data
            let dst = unsafe { &mut new_block.data.inline_data[..self.len] };
            let src = self.into_slice();
            dst.copy_from_slice(src);
            new_block
        }
    }
}

impl Hash for MemoryBlock {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        if self.hash != 0 {
            // Use the hash field directly
            _state.write_u64(self.hash);
        } else {
            panic!("MemoryBlock hash is not set. Ensure to use from_slice_hashed or acquire_with_hash.");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{hash::Hasher, ptr, sync::{atomic::{AtomicUsize, Ordering}, Arc, Mutex}, thread};

    use super::*;

    #[test]
    fn test_memory_pool_acquire_inline() {
        let pool = MemoryPool::new();
        let block = pool.acquire(10);
        assert!(!block.is_heap);
        assert_eq!(block.len, 10);
    }

    #[test]
    fn test_memory_pool_acquire_heap() {
        let pool = MemoryPool::new();
        let block = pool.acquire(100);
        assert!(block.is_heap);
        assert_eq!(block.len, 100);
    }

    #[test]
    fn test_memory_block_from_vec_inline() {
        let vec = vec![1, 2, 3, 4, 5];
        let block = MemoryBlock::from_vec(vec);
        assert!(!block.is_heap);
        let slice = block.into_slice();
        assert_eq!(slice, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_memory_block_from_vec_heap() {
        let vec = vec![1; 100];
        let block = MemoryBlock::from_vec(vec);
        assert!(block.is_heap);
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
        assert!(!clone.is_heap);
        let slice = clone.into_slice();
        assert_eq!(slice, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_memory_block_clone_heap() {
        let vec = vec![1; 100];
        let block = MemoryBlock::from_vec(vec);
        let clone = block.clone();
        assert!(clone.is_heap);
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
        assert!(block.is_heap);
        
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
        assert!(!block.is_heap);
        
        // Verify data integrity
        let slice = block.into_slice();
        for i in 0..INLINE_CAPACITY {
            assert_eq!(slice[i], vec[i], "Data mismatch at index {}", i);
        }
    }

    #[test]
    fn test_memory_block_into_slice_mut() {
        let vec = vec![1, 2, 3, 4, 5];
        let block = MemoryBlock::from_vec(vec);
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
        assert!(!block_inline.is_heap);
        
        // Test one byte over INLINE_CAPACITY
        let block_heap = pool.acquire(INLINE_CAPACITY + 1);
        assert!(block_heap.is_heap);
    }

    #[test]
    fn test_should_drop_flag() {
        let mut block = MEMORY_POOL.acquire(100);
        assert!(block.should_drop);
        
        // Prevent drop from freeing memory
        block.should_drop = false;
        // This should not free the memory or panic
    }

    #[test]
    fn test_zero_size_allocation() {
        let block = MEMORY_POOL.acquire(0);
        assert!(!block.is_heap); // Should use inline storage for zero-size
        assert_eq!(block.len, 0);
    }
    
    #[test]
    fn test_large_allocations() {
        // Test with a large allocation (1MB)
        let size = 1024 * 1024;
        let block = MEMORY_POOL.acquire(size);
        assert!(block.is_heap);
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
                    
                    let block = MEMORY_POOL.acquire(size);
                    
                    // Check if allocation was successful
                    if block.is_heap && size > INLINE_CAPACITY {
                        let slice = block.into_slice_mut();
                        if slice.len() != size {
                            let mut error_log = errors.lock().unwrap();
                            error_log.push(format!(
                                "Thread {} allocation {}: Expected size {}, got {}",
                                thread_id, i, size, slice.len()
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
                    } else if !block.is_heap && size <= INLINE_CAPACITY {
                        // Inline allocation checks
                        let slice = block.into_slice_mut();
                        if slice.len() != size {
                            let mut error_log = errors.lock().unwrap();
                            error_log.push(format!(
                                "Thread {} inline allocation {}: Expected size {}, got {}",
                                thread_id, i, size, slice.len()
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
        assert!(!block.is_heap); // Should use inline for empty vec
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
        let block1 = MemoryBlock::from_vec(original_data.clone());
        
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
    fn test_memory_block_hashing() {
        let pool = MemoryPool::new();
        let data = b"test data";
        
        // Create a memory block with a specific hash
        let block = pool.acquire_with_hash(data.len());
        let slice_mut = block.into_slice_mut();
        slice_mut.copy_from_slice(data);
        
        // Hash the block
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        block.hash(&mut hasher);
        let hash_value_1 = hasher.finish();
        
        // Hash the hash value
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        block.hash.hash(&mut hasher);
        let hash_value_2 = hasher.finish();

        // Check if the hash matches the expected value
        assert_ne!(hash_value_1, 0, "Hash should not be zero");
        assert_eq!(hash_value_2, hash_value_1, "Block hash should match computed hash");
    }

    // #[test]
    // fn test_simulate_stack_overflow() {
    //     // This test will recurse until stack overflow is likely, but catch the panic.
    //     fn recurse(n: usize) {
    //         let _block = MEMORY_POOL.acquire(8); // Small inline allocation
    //         if n > 0 {
    //             recurse(n - 1);
    //         }
    //     }
    //     let result = std::panic::catch_unwind(|| {
    //         // 32_000 is usually enough to overflow stack on default settings
    //         recurse(32_000);
    //     });
    //     assert!(result.is_err(), "Expected stack overflow panic");
    // }

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
                    let block = MEMORY_POOL.acquire(size);
                    let slice = block.into_slice_mut();
                    if slice.len() != size {
                        let mut error_log = errors.lock().unwrap();
                        error_log.push(format!("Thread {} allocation {}: Expected size {}, got {}", thread_id, i, size, slice.len()));
                    }
                    for j in 0..size {
                        slice[j] = ((thread_id + j) % 256) as u8;
                    }
                    for j in 0..size {
                        if slice[j] != ((thread_id + j) % 256) as u8 {
                            let mut error_log = errors.lock().unwrap();
                            error_log.push(format!("Thread {} allocation {} data mismatch at index {}", thread_id, i, j));
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

    // #[test]
    // #[should_panic]
    // fn test_double_free_should_panic() {
    //     let block = MemoryBlock::from_vec(vec![1, 2, 3, 4, 5]);
    //     let mut block2 = block.clone();
    //     unsafe {
    //         std::ptr::drop_in_place(&mut block2);
    //         std::ptr::drop_in_place(&mut block2); // Should panic or crash
    //     }
    // }

    // #[test]
    // #[should_panic]
    // fn test_use_after_drop_should_panic() {
    //     let block = MemoryBlock::from_vec(vec![1, 2, 3, 4, 5]);
    //     let mut block2 = block.clone();
    //     unsafe {
    //         std::ptr::drop_in_place(&mut block2);
    //     }
    //     let _ = block2.into_slice(); // Should panic or crash
    // }

    // #[test]
    // #[should_panic]
    // fn test_invalid_pointer_dereference() {
    //     let mut block = MemoryBlock::default();
    //     unsafe {
    //         let heap_data = &mut block as *mut MemoryBlock as *mut u8;
    //         *heap_data = 0xFF;
    //     }
    //     let _ = block.into_slice(); // Should panic or crash
    // }

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
        assert!(block.is_heap);
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
        let block = MemoryBlock::from_vec(data.clone());
        assert!(!block.is_heap);
        let deep = block.deep_clone();
        assert!(!deep.is_heap);
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
        let block = MemoryBlock::from_vec(data.clone());
        assert!(block.is_heap);
        let deep = block.deep_clone();
        assert!(deep.is_heap);
        assert_eq!(deep.len, block.len);
        assert_eq!(deep.into_slice(), block.into_slice());
        // Mutate original, deep clone should not change
        let slice_mut = block.into_slice_mut();
        slice_mut[0] = 42;
        assert_eq!(block.into_slice()[0], 42);
        assert_eq!(deep.into_slice()[0], 7);
    }
}