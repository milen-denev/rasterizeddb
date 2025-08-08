use std::{
    arch::x86_64::{_mm_prefetch, _MM_HINT_T0}, fmt, hash::Hash
};

use crate::instructions::{copy_vec_to_ptr, ref_slice, ref_slice_mut};

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
                should_drop: true
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
                should_drop: true
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
                should_drop: true
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
                should_drop: true
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

impl Clone for MemoryBlockData {
    #[inline(always)]
    fn clone(&self) -> Self {
        unsafe {
            if self.heap_data.0.is_null() {
                // If the heap pointer is null, we are dealing with inline data
                MemoryBlockData {
                    inline_data: self.inline_data,
                }
            } else {
                // Create a new MemoryBlockData with the copied data
                MemoryBlockData {
                    heap_data: self.heap_data,
                }
            }
        }
    }
}

// The main MemoryBlock struct that manages the union
pub struct MemoryBlock {
    hash: u64, // Hash for the block, if needed
    data: MemoryBlockData,
    len: usize, // Actual length of the data
    is_heap: bool, // Discriminant for the union: true if heap, false if inline
    should_drop: bool
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
            should_drop: false // Not needed as it's implied by is_heap
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
    fn drop(&mut self) {
        if self.should_drop && self.is_heap {
            MEMORY_POOL.release(unsafe { self.data.heap_data.0 });
        } else {
            // If it's inline, we don't need to do anything special
            // The memory will be automatically cleaned up when the block goes out of scope
        }
    }
}

impl Clone for MemoryBlock {
    #[inline(always)]
    fn clone(&self) -> Self {
        if self.is_heap {
            MemoryBlock {
                hash: self.hash,
                data: self.data.clone(),
                len: self.len,
                is_heap: true,
                should_drop: false
            }
        } else {
            MemoryBlock {
                hash: self.hash,
                data: self.data.clone(),
                len: self.len,
                is_heap: false,
                should_drop: false
            }
        }
    }
}

impl MemoryBlock {
    #[inline(always)]
    pub fn prefetch_to_lcache(&self) {
        if self.is_heap {
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

        if memory_chunk.is_heap {
            copy_vec_to_ptr(vec.as_slice(), unsafe { memory_chunk.data.heap_data.0 });
        } else {
            copy_vec_to_ptr(vec.as_slice(), unsafe { memory_chunk.data.inline_data.as_ptr() as *mut u8 });
        }
      
        drop(vec);
        memory_chunk
    }

    #[inline(always)]
    pub fn into_slice(&self) -> &'static [u8] {
        if self.is_heap {
            // If it's a heap allocation, return the slice from the pointer
            unsafe { ref_slice(self.data.heap_data.0, self.len) }
        } else {
            // If it's inline, return the slice from the inline data
            unsafe { ref_slice(self.data.inline_data.as_ptr() as *mut u8, self.len) }
        }
    }

    #[inline(always)]
    pub fn into_slice_mut(&self) -> &'static mut [u8] {
        if self.is_heap {
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

        if memory_chunk.is_heap {
            copy_vec_to_ptr(slice, unsafe { memory_chunk.data.heap_data.0 });
        } else {
            copy_vec_to_ptr(slice, unsafe { memory_chunk.data.inline_data.as_ptr() as *mut u8 });
        }

        memory_chunk
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
}