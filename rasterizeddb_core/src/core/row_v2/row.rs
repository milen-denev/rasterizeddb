use crate::{core::db_type::DbType, memory_pool::MemoryBlock};

pub struct RowFetch {
    pub columns_fetching_data: Vec<ColumnFetchingData>
}

pub struct ColumnFetchingData {
    pub column_offset: u32,
    pub column_type: DbType,
    pub size: u32
}

pub struct RowWrite {
    pub columns_writing_data: Vec<ColumnWritePayload>
}

pub struct ColumnWritePayload {
    pub data: MemoryBlock,
    pub write_order: u32,
    pub column_type: DbType,
    pub size: u32
}

#[derive(Debug, Default)]
pub struct Row {
    pub position: u64,
    pub columns: Vec<Column>,
    
    #[cfg(feature = "enable_long_row")]
    pub length: u64,

    #[cfg(not(feature = "enable_long_row"))]
    pub length: u32,
}

impl Row {
    pub fn clone_from_mut_row(row: &Row) -> Row {
        Row {
            position: row.position,
            columns: row.columns.clone(),
            length: row.length
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub schema_id: u64,
    pub data: MemoryBlock,
    pub column_type: DbType,
}