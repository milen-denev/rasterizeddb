pub struct RowPrefetchResult {
    pub found_id: u64,
    pub length: u32
}

#[derive(Debug)]
pub(crate) struct FileChunk {
    pub current_file_position: u64,
    pub chunk_size: u64,
    pub next_row_id: u64
}