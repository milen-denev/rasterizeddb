use std::{
    mem::{self, ManuallyDrop}, pin::Pin, ptr, sync::{atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering}, Arc, LazyLock}
};

use crate::instructions::ref_vec;

const MEMORY_POOL_SIZE: usize = 268435456; // 128MB

pub static MEMORY_POOL: LazyLock<Arc<MemoryPool>> = LazyLock::new(|| Arc::new(MemoryPool::new()));

pub struct MemoryPool {
    pub buffer: Arc<Pin<Box<[u8]>>>,
    pub start: usize,
    pub end: usize
}

unsafe impl Send for MemoryPool {}
unsafe impl Sync for MemoryPool {}

static ATOMIC_PTR_ARRAY: LazyLock<Arc<Box<[(AtomicUsize, AtomicU32)]>>> = LazyLock::new(|| {
    Arc::new(Box::new([
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
        (AtomicUsize::new(0), AtomicU32::new(0)),
    ]))
});

static ATOMIC_IN_USE_ARRAY: LazyLock<Arc<Box<[AtomicBool]>>> = LazyLock::new(|| {
    Arc::new(Box::new([
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
        AtomicBool::new(false),
    ]))
});

impl MemoryPool {
    pub fn new() -> Self {
        let buffer = vec![0_u8; MEMORY_POOL_SIZE].into_boxed_slice();
        let pinned_box = Pin::new(buffer);
        let ptr_start = pinned_box.as_ptr() as usize;
        let ptr_end = ptr_start + MEMORY_POOL_SIZE;

        Self {
            buffer: {
                Arc::new(pinned_box)
            },
            start: ptr_start,
            end: ptr_end
        }
    }

    pub fn acquire(&self, size: u32) -> Option<Chunk> {
        let mut current_start = 0;
        let mut slot_index: Option<usize> = None;
    
        // Try to atomically claim one of the available slots.
        for (i, flag) in ATOMIC_IN_USE_ARRAY.iter().enumerate() {
            if flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
                slot_index = Some(i);
    
                // Read the previous size and pointer in an atomic way.
                let previous_size = ATOMIC_PTR_ARRAY[i].1.load(Ordering::SeqCst);
                let previous_start = ATOMIC_PTR_ARRAY[i].0.load(Ordering::SeqCst);
    
                if previous_size != 0 && previous_start != 0 {
                    current_start = previous_start - self.start + previous_size as usize;
                } else {
                    current_start += previous_size as usize; // usually zero on first allocation
                }
                break;
            }
        }
    
        let i = match slot_index {
            Some(idx) => idx,
            None => return None
        };
    
        // Check that we do not exceed the limit.
        if current_start + size as usize > MEMORY_POOL_SIZE {
            // Release our claim if allocation cannot proceed.
            ATOMIC_IN_USE_ARRAY[i].store(false, Ordering::SeqCst);
            return None;
        }
    
        // Set the pointer and size for the slot.
        let ptr = self.buffer[current_start..current_start + size as usize].as_ptr() as *mut u8;
        ATOMIC_PTR_ARRAY[i].0.store(ptr as usize, Ordering::SeqCst);
        ATOMIC_PTR_ARRAY[i].1.store(size, Ordering::SeqCst);
    
        Some(Chunk {
            ptr,
            size,
            pool: Arc::downgrade(&MEMORY_POOL),
            vec: None,
            index: i as i32,
        })
    }

    fn release(&self, index: i32) {
        if index != -1 {
            ATOMIC_IN_USE_ARRAY[index as usize].store(false, Ordering::Release);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub ptr: *mut u8,
    pub size: u32,
    pool: std::sync::Weak<MemoryPool>,
    // Used to store a vector instead of a raw pointer. Either one or the other will be set.
    pub vec: Option<Vec<u8>>,
    pub index: i32
}

unsafe impl Send for Chunk {}
unsafe impl Sync for Chunk {}

impl Default for Chunk {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            size: 0,
            pool: std::sync::Weak::default(),
            vec: None,
            index: -1
        }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        if self.vec.is_none() {
            if let Some(pool) = self.pool.upgrade() {
                pool.release(self.index);
            }
        }
    }
}

impl Chunk {
    // Create a new chunk from a vector with no memory chunk allocated from pool
    pub fn from_vec(vec: Vec<u8>) -> Self {
        Chunk {
            ptr: ptr::null_mut(),
            size: 0,
            pool: std::sync::Weak::default(),
            vec: Some(vec),
            index: -1
        }
    }

    pub unsafe fn into_vec(&self) -> ChunkIntoVecResult {
        if let Some(vec) = self.vec.as_ref() {
            let new_vec = vec.clone();
            ChunkIntoVecResult::Vec(new_vec)
        } else {
            ChunkIntoVecResult::ManualVec(ref_vec(self.ptr, self.size as usize))
        }
    }

    pub fn deallocate_vec(&mut self, chunk_vec_result: ChunkIntoVecResult) {
        match chunk_vec_result {
            ChunkIntoVecResult::Vec(_) => {
            },
            ChunkIntoVecResult::ManualVec(manual) => {
                mem::forget(manual);
            }
        }
    }
}

#[derive(Debug)]
pub enum ChunkIntoVecResult {
    Vec(Vec<u8>),
    ManualVec(ManuallyDrop<Vec<u8>>)
}

impl ChunkIntoVecResult {
    pub fn as_vec(&self) -> &Vec<u8> {
        match self {
            ChunkIntoVecResult::Vec(v) => v,
            ChunkIntoVecResult::ManualVec(v) => v,
        }
    }

    pub fn as_vec_mut(&mut self) -> &mut Vec<u8> {
        match self {
            ChunkIntoVecResult::Vec(v) => v,
            ChunkIntoVecResult::ManualVec(v) => v,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            ChunkIntoVecResult::Vec(v) => v.as_slice(),
            ChunkIntoVecResult::ManualVec(v) => v.as_slice(),
        }
    }
}