use crate::memory_pool::{MemoryBlock, MEMORY_POOL};

use super::{db_type::DbType, row_v2::row::{ColumnFetchingData, ColumnWritePayload, RowFetch, RowWrite}};

pub fn create_small_string_column() -> MemoryBlock {
    let string_bytes = b"Hello, world!";

    let string_data = MEMORY_POOL.acquire(string_bytes.len());
    let string_slice = string_data.into_slice_mut();

    string_slice[0..].copy_from_slice(string_bytes);

    string_data
}

pub fn create_large_string_column() -> MemoryBlock {
    let large_string = "Data - ".repeat(25);
    let large_string_bytes = large_string.as_bytes();

    let large_string_data = MEMORY_POOL.acquire(large_string_bytes.len());
    let large_string_slice = large_string_data.into_slice_mut();

    large_string_slice[0..].copy_from_slice(large_string_bytes);

    large_string_data
}

pub fn create_i32_column() -> MemoryBlock {
    let i32_bytes = 42_i32.to_le_bytes();
    let i32_data = MEMORY_POOL.acquire(4);
    let i32_slice = i32_data.into_slice_mut();
    i32_slice.copy_from_slice(&i32_bytes);

    i32_data
}

pub fn i32_column(val: i32) -> MemoryBlock {
    let i32_bytes = val.to_le_bytes();
    let i32_data = MEMORY_POOL.acquire(4);
    let i32_slice = i32_data.into_slice_mut();
    i32_slice.copy_from_slice(&i32_bytes);

    i32_data
}

pub fn create_row_write(with_large_string: bool) -> RowWrite {
    let string_data = create_small_string_column();
    let i32_data = create_i32_column();
    if with_large_string {
        let large_string_data = create_large_string_column();

        RowWrite {
            columns_writing_data: smallvec::smallvec![
                ColumnWritePayload {
                    data: string_data,
                    write_order: 0,
                    column_type: DbType::STRING,
                    size: 4 + 8 // String Size + String Pointer
                },
                ColumnWritePayload {
                    data: i32_data,
                    write_order: 1,
                    column_type: DbType::I32,
                    size: 4
                },
                ColumnWritePayload {
                    data: large_string_data,
                    write_order: 2,
                    column_type: DbType::STRING,
                    size: 4 + 8 // String Size + String Pointer
                },
            ],
        }
    } else {
        RowWrite {
            columns_writing_data: smallvec::smallvec![
                ColumnWritePayload {
                    data: string_data,
                    write_order: 0,
                    column_type: DbType::STRING,
                    size: 4 + 8 // String Size + String Pointer
                },
                ColumnWritePayload {
                    data: i32_data,
                    write_order: 1,
                    column_type: DbType::I32,
                    size: 4
                },
            ],
        }
    }
}

pub fn create_row_write_custom_i32(val: i32, with_large_string: bool) -> RowWrite {
    let string_data = create_small_string_column();
    let i32_data = i32_column(val);
    if with_large_string {
        let large_string_data = create_large_string_column();

        RowWrite {
            columns_writing_data: smallvec::smallvec![
                ColumnWritePayload {
                    data: string_data,
                    write_order: 0,
                    column_type: DbType::STRING,
                    size: 4 + 8 // String Size + String Pointer
                },
                ColumnWritePayload {
                    data: i32_data,
                    write_order: 1,
                    column_type: DbType::I32,
                    size: 4
                },
                ColumnWritePayload {
                    data: large_string_data,
                    write_order: 2,
                    column_type: DbType::STRING,
                    size: 4 + 8 // String Size + String Pointer
                },
            ],
        }
    } else {
        RowWrite {
            columns_writing_data: smallvec::smallvec![
                ColumnWritePayload {
                    data: string_data,
                    write_order: 0,
                    column_type: DbType::STRING,
                    size: 4 + 8 // String Size + String Pointer
                },
                ColumnWritePayload {
                    data: i32_data,
                    write_order: 1,
                    column_type: DbType::I32,
                    size: 4
                },
            ],
        }
    }
}

pub fn get_row_fetch_small_string() -> RowFetch {
    RowFetch {
        columns_fetching_data: vec![
            ColumnFetchingData {
                column_offset: 0,
                column_type: DbType::STRING,
                size: 0 // String size is not known yet
            }
        ],
    }
}

pub fn get_row_fetch_large_string() -> RowFetch {
    RowFetch {
        columns_fetching_data: vec![
            ColumnFetchingData {
                column_offset: 8 + 4 + 4,
                column_type: DbType::STRING,
                size: 0 // String size is not known yet
            },
        ],
    }
}

pub fn get_row_fetch_i32() -> RowFetch {
    RowFetch {
        columns_fetching_data: vec![
            ColumnFetchingData {
                column_offset: 8 + 4,
                column_type: DbType::I32,
                size: 4
            },
        ],
    }
}

pub fn get_row_fetch_all() -> RowFetch {
    RowFetch {
         columns_fetching_data: vec![
            ColumnFetchingData {
                column_offset: 0,
                column_type: DbType::STRING,
                size: 0 // String size is not known yet
            },
            ColumnFetchingData {
                column_offset: 8 + 4,
                column_type: DbType::I32,
                size: 4
            },
            ColumnFetchingData {
                column_offset: 8 + 4 + 4,
                column_type: DbType::STRING,
                size: 0 // String size is not known yet
            },
        ],
    }
}
