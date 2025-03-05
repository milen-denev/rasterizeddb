use std::{
    arch::x86_64::{_mm_prefetch, _MM_HINT_T0},
    mem::{self, ManuallyDrop},
    pin::Pin,
    ptr
};

use crate::instructions::{copy_vec_to_ptr, ref_vec, ref_vec_no_manual};

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
    pub fn acquire(&self, size: u32) -> MemoryChunk {
        let ptr = unsafe { mi_malloc(size as usize) as *mut u8 };

        if ptr.is_null() {
            panic!("Allocation failed (likely OOM)");
        }

        MemoryChunk {
            ptr,
            size
        }
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
    pub size: u32
}

unsafe impl Send for MemoryChunk {}
unsafe impl Sync for MemoryChunk {}

impl Drop for MemoryChunk {
    fn drop(&mut self) {
        MEMORY_POOL.release(self.ptr);
    }
}

impl MemoryChunk {
    pub fn prefetch_to_lcache(&self) {
        unsafe { _mm_prefetch::<_MM_HINT_T0>(self.ptr as *const i8) };
    }

    // Create a new chunk from a vector with no memory chunk allocated from pool
    pub fn from_vec(vec: Vec<u8>) -> Self {
        let len = vec.len() as u32;
        let memory_chunk = MEMORY_POOL.acquire(len);
        copy_vec_to_ptr(vec.as_slice(), memory_chunk.ptr);
        drop(vec);
        memory_chunk
    }

    pub unsafe fn into_vec(&self) -> ChunkIntoVecResult {
        ChunkIntoVecResult::ManualVec(unsafe { ref_vec(self.ptr, self.size as usize) })
    }
}

#[derive(Debug)]
pub enum ChunkIntoVecResult {
    ManualVec(ManuallyDrop<Vec<u8>>),
}

impl ChunkIntoVecResult {
    pub fn as_vec(&self) -> &Vec<u8> {
        match self {
            ChunkIntoVecResult::ManualVec(v) => v,
        }
    }

    pub fn as_vec_mut(&mut self) -> &mut Vec<u8> {
        match self {
            ChunkIntoVecResult::ManualVec(v) => v,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            ChunkIntoVecResult::ManualVec(v) => v.as_slice(),
        }
    }
}