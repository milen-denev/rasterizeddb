use std::{
    arch::x86_64::{_mm_prefetch, _MM_HINT_T0},
    mem::ManuallyDrop
};

use crate::instructions::{copy_vec_to_ptr, ref_vec};

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
    pub fn acquire(&self, size: u32) -> MemoryBlock {
        let ptr = unsafe { mi_malloc(size as usize) as *mut u8 };

        if ptr.is_null() {
            panic!("Allocation failed (likely OOM).");
        }

        MemoryBlock {
            ptr,
            size
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

#[derive(Debug, Clone)]
pub struct MemoryBlock {
    pub ptr: *mut u8,
    pub size: u32
}

unsafe impl Send for MemoryBlock {}
unsafe impl Sync for MemoryBlock {}

impl Drop for MemoryBlock {
    fn drop(&mut self) {
        MEMORY_POOL.release(self.ptr);
    }
}

impl MemoryBlock {
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

    pub unsafe fn into_wrapper(&self) -> MemoryBlockWrapper {
        MemoryBlockWrapper {
            data: unsafe { ref_vec(self.ptr, self.size as usize) },
            ptr: self.ptr,
            size: self.size,
        }
    }
}

#[derive(Debug)]
pub struct  MemoryBlockWrapper {
    pub data: ManuallyDrop<Vec<u8>>,
    pub ptr: *mut u8,
    pub size: u32
}

impl MemoryBlockWrapper {
    pub fn as_vec(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn as_vec_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn into_block(self) -> MemoryBlock {
        MemoryBlock {
            ptr: self.ptr,
            size: self.size
        }
    }
}