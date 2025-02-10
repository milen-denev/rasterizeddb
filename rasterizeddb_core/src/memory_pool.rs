use once_cell::sync::Lazy;
use tokio::task::yield_now;
use std::{
    pin::Pin,
    sync::{Arc, RwLock},
    collections::BTreeMap,
};
use dashmap::DashMap;
use crate::instructions::zero_buffer;

pub static MEMORY_POOL: Lazy<Arc<RwLock<MemoryPool>>> = Lazy::new(|| {
    Arc::new(RwLock::new(MemoryPool::new()))
});

pub struct MemoryPool {
    pub buffer: Pin<Box<[u8; 8_388_608]>>, // 8MB
    pub allocations: DashMap<usize, u32>, // Tracks active allocations
    pub freed_chunks: BTreeMap<u32, Vec<usize>>, // Sorted free chunks by size
}

impl MemoryPool {
    pub fn new() -> Self {
        Self {
            buffer: Box::pin([0; 8_388_608]),
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
                });
            }
        }

        // Otherwise, allocate from the buffer
        let current_index = self.allocations.iter().map(|e| *e.value()).sum::<u32>();
        if current_index + size > 8_388_608 {
            return None;
        }

        let ptr = self.buffer[current_index as usize..(current_index + size) as usize].as_mut_ptr();
        self.allocations.insert(ptr as usize, size);

        Some(Chunk {
            ptr,
            size,
            pool: Arc::downgrade(&MEMORY_POOL),
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
}

impl Default for Chunk {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            size: 0,
            pool: std::sync::Weak::default()
        }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            if let Ok(mut pool) = pool.write() {
                pool.release(self.ptr);
            }
        }
    }
}