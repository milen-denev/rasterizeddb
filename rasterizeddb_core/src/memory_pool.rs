use once_cell::sync::Lazy;
use std::{
    mem::{self, ManuallyDrop}, pin::Pin, ptr, sync::{atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering}, Arc, LazyLock, RwLock}
};
use crate::instructions::ref_vec;

const MEMORY_POOL_SIZE: usize = 8_388_608; // 8MB

pub static MEMORY_POOL: LazyLock<Arc<MemoryPool>> = LazyLock::new(|| Arc::new(MemoryPool::new()));

pub struct MemoryPool {
    pub buffer: Pin<Box<[u8]>>,
    pub start: usize,
    pub end: usize
}

static ATOMIC_PTR_ARRAY: LazyLock<Box<[(AtomicUsize, AtomicU32)]>> = LazyLock::new(|| {
    Box::new([
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
    ])
});

static ATOMIC_IN_USE_ARRAY: LazyLock<Box<[AtomicBool]>> = LazyLock::new(|| {
    Box::new([
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
    ])
});

impl MemoryPool {
    pub fn new() -> Self {
        let buffer = vec![0_u8; MEMORY_POOL_SIZE].into_boxed_slice();
        let pinned_box = Pin::new(buffer);
        let ptr_start = pinned_box.as_ptr() as usize;
        let ptr_end = ptr_start + MEMORY_POOL_SIZE;

        Self {
            buffer: {
                pinned_box
            },
            start: ptr_start,
            end: ptr_end
        }
    }

    #[inline(always)]
    pub fn acquire(&self, size: u32) -> Option<Chunk> {
        //let caller = std::panic::Location::caller();
        //println!("acquire called: {}, {}", caller.file(), caller.line());

        let mut current_start = 0;
    
        let mut slot: i32 = -1;

        // Find the last allocated slot and compute the next starting address
        for (i, in_use) in ATOMIC_IN_USE_ARRAY.iter().enumerate() {
            if !in_use.load(Ordering::Acquire) {
                let previous_size = ATOMIC_PTR_ARRAY[i].1.load(Ordering::Acquire);
                if previous_size != 0 {
                    if size <= previous_size {
                        let previous_start = ATOMIC_PTR_ARRAY[i].0.load(Ordering::Acquire);

                        if previous_start != 0 {
                            current_start = previous_start - self.start + previous_size as usize;
                        } else {
                            current_start += previous_size as usize;
                        }

                        slot = i as i32;
                        break;
                    }
                } else {
                    let previous_start = ATOMIC_PTR_ARRAY[i].0.load(Ordering::Acquire);
                    
                    if previous_start != 0 {
                        current_start = previous_start - self.start + previous_size as usize;
                    } else {
                        current_start += previous_size as usize;
                    }

                    slot = i as i32;
                    break;
                }
            }
        }

        if slot == -1 {
            return None;
        }

        for (i, in_use) in ATOMIC_IN_USE_ARRAY.iter().enumerate() {
            if slot == i as i32 {
                let ptr = self.buffer[current_start as usize..(current_start + size as usize)].as_ptr() as *mut u8;

                in_use.store(true, Ordering::Release);
                ATOMIC_PTR_ARRAY[i].0.store(ptr as usize, Ordering::Release);
                ATOMIC_PTR_ARRAY[i].1.store(size, Ordering::Release);
                
                return Some(Chunk {
                    ptr,
                    size,
                    pool: Arc::downgrade(&MEMORY_POOL),
                    vec: None
                });
            } 
        }

        return None;
    }

    #[inline(always)]
    fn release(&self, ptr: *mut u8) {
        let index = find_index_by_usize(ptr as usize);
        if let Some(index) = index {
            ATOMIC_IN_USE_ARRAY[index].store(false, Ordering::Release);
        }
    }
}

#[inline(always)]
fn find_index_by_usize(target: usize) -> Option<usize> {
    for (i, (stored_usize, _)) in ATOMIC_PTR_ARRAY.iter().enumerate() {
        if stored_usize.load(Ordering::Acquire) == target {
            return Some(i);
        }
    }
    None
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub ptr: *mut u8,
    pub size: u32,
    pool: std::sync::Weak<MemoryPool>,
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
    
    fn drop(&mut self) {
        if self.vec.is_none() {
            if let Some(pool) = self.pool.upgrade() {
                pool.release(self.ptr);
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
            vec: Some(vec)
        }
    }

    #[inline(always)]
    pub unsafe fn into_vec(&self) -> ManuallyDrop<Vec<u8>> {
        if let Some(vec) = self.vec.as_ref() {
            let new_vec = vec.clone();
            let manual = ManuallyDrop::new(new_vec);
            manual
        } else {
            ref_vec(self.ptr, self.size as usize)
        }
    }

    #[inline(always)]
    pub fn deallocate_vec(&mut self, vec: ManuallyDrop<Vec<u8>>) {
        if self.vec.is_some() {
            self.vec = Some(ManuallyDrop::into_inner(vec));
        } else {
            mem::forget(vec);
        }
    }
}