use std::io::Cursor;

use smallvec::SmallVec;

use super::{row::Row, storage_providers::traits::StorageIO};

#[derive(Debug, Default)]
pub struct RowPrefetchResult {
    pub found_id: u64,
    pub length: u32,
}

#[derive(Debug, Clone)]
pub struct FileChunk {
    pub current_file_position: u64,
    pub chunk_size: u32,
    pub next_row_id: u64,
}

pub struct CursorVector<'a> {
    pub cursor: Cursor<&'a Vec<u8>>,
    pub vector: &'a Vec<u8>,
}

impl FileChunk {
    pub async fn read_chunk_sync(&self, io_sync: &mut Box<impl StorageIO>) -> Vec<u8> {
        let mut current_file_position = self.current_file_position.clone();

        let buffer = io_sync
            .read_data(&mut current_file_position, self.chunk_size as u32)
            .await;

        if buffer.len() == 0 {
            panic!("Tried to read larger buffer than available.")
        }

        return buffer;
    }

    pub async fn read_chunk_to_end_sync(
        &self,
        file_position: u64,
        io_sync: &mut Box<impl StorageIO>,
    ) -> Vec<u8> {
        let buffer = io_sync.read_data_to_end(file_position).await;

        if buffer.len() == 0 {
            panic!("Error reading the file to buffer.");
        }

        return buffer;
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        self.current_file_position > 0 && self.chunk_size > 0
    }
}

impl<'a> CursorVector<'a> {
    pub fn new(vector: &'a Vec<u8>) -> CursorVector<'a> {
        CursorVector {
            cursor: Cursor::new(vector),
            vector: vector,
        }
    }
}

pub enum ReturnResult {
    Rows(SmallVec<[Row; 32]>),
    HtmlView(String)
}