use std::{
    arch::x86_64::{_mm_prefetch, _MM_HINT_T0},
    mem::{self, ManuallyDrop},
    pin::Pin,
    ptr
};

use crate::instructions::{ref_vec, ref_vec_no_manual};

pub static MEMORY_POOL: MemoryPool = MemoryPool::new();

pub struct MemoryPool;

unsafe impl Send for MemoryPool {}
unsafe impl Sync for MemoryPool {}

use libmimalloc_sys::{mi_malloc, mi_free};

impl MemoryPool {
    pub const  fn new() -> Self {
        Self
    }

    #[inline(always)]
    pub fn acquire(&self, size: u32) -> Option<MemoryChunk> {
        let ptr = unsafe { mi_malloc(size as usize) as *mut u8 };

        if ptr.is_null() {
            return None;  // Allocation failed (likely OOM) – return None instead of panicking.
        }

        Some(MemoryChunk {
            ptr,
            size,
            //pool: Arc::downgrade(&MEMORY_POOL),
            vec: None
        })
    }

    #[inline(always)]
    fn release(&self, ptr: *mut u8) {
        if ptr.is_null() {
            return;  // No action needed for null pointers (matches C free behavior).
        }

        unsafe {
            mi_free(ptr as *mut _);
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryChunk {
    pub ptr: *mut u8,
    pub size: u32,
    //pool: std::sync::Weak<MemoryPool>,
    // Used to store a vector instead of a raw pointer. Either one or the other will be set.
    pub vec: Option<Pin<Box<Vec<u8>>>>,
}

unsafe impl Send for MemoryChunk {}
unsafe impl Sync for MemoryChunk {}

impl Default for MemoryChunk {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            size: 0,
            //pool: std::sync::Weak::default(),
            vec: None,
        }
    }
}

impl Drop for MemoryChunk {
    fn drop(&mut self) {
        if self.vec.is_none() {
            // if let Some(pool) = self.pool.upgrade() {
            //     pool.release(self.index);
            // }
            MEMORY_POOL.release(self.ptr);
        }
    }
}

impl MemoryChunk {
    pub fn prefetch_to_lcache(&self) {
        if self.vec.is_none() {
            unsafe { _mm_prefetch::<_MM_HINT_T0>(self.ptr as *const i8) };
        }
    }

    // Create a new chunk from a vector with no memory chunk allocated from pool
    pub fn from_vec(vec: Vec<u8>) -> Self {
        MemoryChunk {
            ptr: ptr::null_mut(),
            size: 0,
            //pool: std::sync::Weak::default(),
            vec: Some(Box::pin(vec))
        }
    }

    pub unsafe fn into_vec(&self) -> ChunkIntoVecResult {
        if let Some(vec) = self.vec.as_ref() {
            let new_vec = unsafe { ref_vec_no_manual(vec.as_ptr() as *mut u8, vec.len()) };
            ChunkIntoVecResult::Vec(Box::pin(new_vec))
        } else {
            ChunkIntoVecResult::ManualVec(unsafe { ref_vec(self.ptr, self.size as usize) })
        }
    }

    pub fn deallocate_vec(&mut self, chunk_vec_result: ChunkIntoVecResult) {
        match chunk_vec_result {
            ChunkIntoVecResult::Vec(_) => {}
            ChunkIntoVecResult::ManualVec(manual) => {
                mem::forget(manual);
            }
        }
    }
}

#[derive(Debug)]
pub enum ChunkIntoVecResult {
    Vec(Pin<Box<Vec<u8>>>),
    ManualVec(ManuallyDrop<Vec<u8>>),
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
