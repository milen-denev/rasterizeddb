use once_cell::sync::Lazy;
use tokio::task::yield_now;
use std::{
    collections::BTreeMap, mem::{self, ManuallyDrop}, pin::Pin, ptr, sync::{Arc, RwLock}
};
use dashmap::DashMap;
use crate::instructions::{ref_vec, zero_buffer};

const MEMORY_POOL_SIZE: usize = 8_388_608;

pub static MEMORY_POOL: Lazy<Arc<RwLock<MemoryPool>>> = Lazy::new(|| {
    Arc::new(RwLock::new(MemoryPool::new()))
});

pub struct MemoryPool {
    pub buffer: Pin<Box<[u8]>>, // 8MB
    pub allocations: DashMap<usize, u32>, // Tracks active allocations
    pub freed_chunks: BTreeMap<u32, Vec<usize>>, // Sorted free chunks by size
}

impl MemoryPool {
    pub fn new() -> Self {
        Self {
            buffer: {
                let buffer = vec![0_u8; MEMORY_POOL_SIZE].into_boxed_slice();
                Pin::new(buffer)
            },
            allocations: DashMap::new(),
            freed_chunks: BTreeMap::new(),
        }
    }

    pub async fn acquire(&mut self, size: u32) -> Option<Chunk> {
        yield_now().await;

        // First, check if there's a freed chunk we can reuse
        if let Some((&free_size, ptrs)) = self.freed_chunks.range_mut(size..).next() {
            if let Some(ptr) = ptrs.pop() {
                if ptrs.is_empty() {
                    self.freed_chunks.remove(&free_size);
                }
                self.allocations.insert(ptr, size);

                let ptr = ptr as *mut u8;

                return Some(Chunk {
                    ptr,
                    size,
                    pool: Arc::downgrade(&MEMORY_POOL),
                    vec: None
                });
            }
        }

        // Otherwise, allocate from the buffer
        let current_index = self.allocations.iter().map(|e| *e.value()).sum::<u32>();
        if current_index + size > MEMORY_POOL_SIZE as u32 {
            return None;
        }

        let ptr = self.buffer[current_index as usize..(current_index + size) as usize].as_mut_ptr();
        self.allocations.insert(ptr as usize, size);

        Some(Chunk {
            ptr,
            size,
            pool: Arc::downgrade(&MEMORY_POOL),
            vec: None
        })
    }

    fn release(&mut self, ptr: *mut u8) {
        if let Some((_, size)) = self.allocations.remove(&(ptr as usize)) {
            unsafe { zero_buffer(ptr, size as usize) };

            // Track freed chunk for reuse
            self.freed_chunks.entry(size).or_default().push(ptr as usize);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub ptr: *mut u8,
    pub size: u32,
    pool: std::sync::Weak<RwLock<MemoryPool>>,
    // Used to store a vector instead of a raw pointer. Either one or the other will be set.
    pub vec: Option<Vec<u8>>
}

impl Default for Chunk {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            size: 0,
            pool: std::sync::Weak::default(),
            vec: None
        }
    }
}

impl Drop for Chunk {
    #[track_caller]
    fn drop(&mut self) {
        if self.vec.is_none() {
            if let Some(pool) = self.pool.upgrade() {
                if let Ok(mut pool) = pool.write() {
                    pool.release(self.ptr);
                }
            }
        }
    }
}

impl Chunk {
    // Create a new chunk from a vector with no memory chunk allocated from pool
    #[track_caller]
    pub fn from_vec(vec: Vec<u8>) -> Self {

        Chunk {
            ptr: ptr::null_mut(),
            size: 0,
            pool: std::sync::Weak::default(),
            vec: Some(vec)
        }
    }

    #[track_caller]
    pub unsafe fn into_vec(&self) -> ManuallyDrop<Vec<u8>> {
        if let Some(vec) = self.vec.as_ref() {
            let new_vec = vec.clone();
            let manual = ManuallyDrop::new(new_vec);
            manual
        } else {
            ref_vec(self.ptr, self.size as usize)
        }
    }

    pub fn deallocate_vec(&mut self, vec: ManuallyDrop<Vec<u8>>) {
        if self.vec.is_some() {
            self.vec = Some(ManuallyDrop::into_inner(vec));
        } else {
            mem::forget(vec);
        }
    }
}