//! The `profile_rw` driver with mimalloc as the global allocator.
//!
//! `libmimalloc-sys` is already a dependency, but only `memory_pool` uses it —
//! every `Vec`, `String` and `Box` in the engine goes to glibc's malloc. This
//! build exists to measure what that costs: same workloads, same binary code,
//! one different allocator.
use std::alloc::{GlobalAlloc, Layout};

use libmimalloc_sys::{mi_free, mi_malloc_aligned, mi_realloc_aligned, mi_zalloc_aligned};

struct Mi;

unsafe impl GlobalAlloc for Mi {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { mi_malloc_aligned(layout.size(), layout.align()) as *mut u8 }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        unsafe { mi_zalloc_aligned(layout.size(), layout.align()) as *mut u8 }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        unsafe { mi_free(ptr as *mut _) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        unsafe { mi_realloc_aligned(ptr as *mut _, new_size, layout.align()) as *mut u8 }
    }
}

#[global_allocator]
static GLOBAL: Mi = Mi;

include!("profile_rw.rs");
