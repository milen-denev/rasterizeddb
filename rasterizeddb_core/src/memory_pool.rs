use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use std::{pin::Pin, sync::Arc};

pub static MEMORY_POOL: Lazy<Arc<RwLock<MemoryPool>>> = Lazy::new(|| {
    Arc::new(RwLock::new(MemoryPool::new()))
});

pub struct MemoryPool {
    pub current_index: Arc<RwLock<u32>>,
    pub buffer: Pin<Box<[u8; 8_388_608]>> // 8MB
}

impl MemoryPool {
    pub fn new() -> Self {
        Self {
            current_index: Arc::new(RwLock::new(0)),
            buffer: Box::pin([0; 8_388_608])
        }
    }

    pub async fn acquire(&mut self, size: u32) -> Option<*mut u8> {
        let mut current_index = self.current_index.write().await;
        let start_index = *current_index;
        let end_index = start_index + size;

        if end_index > 8_388_608 {
            return None;
        }

        *current_index += size;

        Some(self.buffer[start_index as usize..end_index as usize].as_mut_ptr())
    }

    pub async fn release(&mut self, ptr: *mut u8, size: u32) {
        let mut current_index = self.current_index.write().await;
        let start_index = *current_index;
        let end_index = start_index + size;

        for i in start_index..end_index {
            self.buffer[i as usize] = 0;
        }

        *current_index -= size;
    }
}