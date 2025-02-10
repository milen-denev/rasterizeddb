use crate::core::db_type::{self, DbType};

pub struct ColumnUnsafe {
    pub db_type: DbType,
    pub length: u32,
    pub buffer: ColumnStoredValue
}

pub enum ColumnStoredValue {
    StaticMemoryPointer(*mut u8),
    ManagedMemoryPointer(Vec<u8>)
}

impl ColumnUnsafe {
    pub fn new(
        db_type: DbType, 
        length: u32, 
        buffer: ColumnStoredValue) -> Self {
        Self {
            db_type,
            length,
            buffer
        }
    }
}