use std::io::Cursor;

use super::storage_providers::traits::IOOperationsSync;

#[derive(Debug)]
pub struct RowPrefetchResult {
    pub found_id: u64,
    pub length: u32
}

#[derive(Debug, Clone)]
pub(crate) struct FileChunk {
    pub current_file_position: u64,
    pub chunk_size: u32,
    pub next_row_id: u64
}

impl FileChunk {
    pub async fn read_chunk_sync(&self, io_sync: &mut impl IOOperationsSync) -> Cursor<Vec<u8>> {
        let mut current_file_position = self.current_file_position.clone();

        let buffer = io_sync.read_data(&mut current_file_position, self.chunk_size as u32).await;
    
        if buffer.len() == 0 {
            panic!("Tried to read larger buffer than available.")
        }

        let cursor = Cursor::new(buffer);

        return cursor;
    }

    pub async fn read_chunk_to_end_sync(&self, file_position: u64, io_sync: &mut impl IOOperationsSync) -> Cursor<Vec<u8>> {
        let buffer = io_sync.read_data_to_end(file_position).await;
        
        if buffer.len() == 0 {
            panic!("Error reading the file to buffer.");
        }

        let cursor = Cursor::new(buffer);

        return cursor;
    }
}