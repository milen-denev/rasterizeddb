use std::{
    arch::x86_64::{_mm_prefetch, _MM_HINT_T0}
};

use crate::instructions::{copy_vec_to_ptr, ref_slice, ref_slice_mut};

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
    pub fn acquire(&self, size: usize) -> MemoryBlock {
        let ptr = unsafe { mi_malloc(size as usize) as *mut u8 };

        if ptr.is_null() {
            panic!("Allocation failed (likely OOM).");
        }

        MemoryBlock {
            ptr,
            size,
            should_drop: true
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

#[derive(Debug, Hash)]
pub struct MemoryBlock {
    pub ptr: *mut u8,
    pub size: usize,
    pub(crate) should_drop: bool
}

unsafe impl Send for MemoryBlock {}
unsafe impl Sync for MemoryBlock {}

impl Drop for MemoryBlock {
    fn drop(&mut self) {
        if self.should_drop {
            MEMORY_POOL.release(self.ptr);
        }
    }
}

impl Clone for MemoryBlock {
    fn clone(&self) -> Self {
        MemoryBlock {
            ptr: self.ptr,
            size: self.size,
            should_drop: false
        }
    }
}

impl MemoryBlock {
    pub fn prefetch_to_lcache(&self) {
        unsafe { _mm_prefetch::<_MM_HINT_T0>(self.ptr as *const i8) };
    }

    // Create a new chunk from a vector with no memory chunk allocated from pool
    pub fn from_vec(vec: Vec<u8>) -> Self {
        let len = vec.len();
        let memory_chunk = MEMORY_POOL.acquire(len);
        copy_vec_to_ptr(vec.as_slice(), memory_chunk.ptr);
        drop(vec);
        memory_chunk
    }

    pub fn into_slice(&self) -> &'static [u8] {
        unsafe { ref_slice(self.ptr, self.size) }
    }

    pub fn into_slice_mut(&self) -> &'static mut [u8] {
        unsafe { ref_slice_mut(self.ptr, self.size) }
    }
}