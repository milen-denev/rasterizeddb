use crate::{core::db_type::DbType, memory_pool::MemoryBlockWrapper};

pub struct RowFetch {
    pub columns_fetching_data: Vec<ColumnFetchingData>
}

pub struct ColumnFetchingData {
    pub column_position: u32,
    pub column_type: DbType,
    pub size: u32
}

pub struct RowWrite {
    pub columns_writing_data: Vec<ColumnWritingData>
}

pub struct ColumnWritingData {
    pub data: MemoryBlockWrapper,
    pub write_order: u32,
    pub column_type: DbType,
    pub size: u32
}

pub struct Row {
    pub position: u64,
    pub columns: Vec<Column>,
    
    #[cfg(feature = "enable_long_row")]
    pub length: u64,

    #[cfg(not(feature = "enable_long_row"))]
    pub length: u32,
}

pub struct Column {
    pub schema_id: u64,
    pub data: MemoryBlockWrapper,
    pub column_type: DbType,
}

