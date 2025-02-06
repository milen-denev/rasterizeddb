use crate::core::db_type::{self, DbType};

pub struct ColumnUnsafe {
    pub db_type: DbType,
    pub lentgh: u32,
    pub buffer: ColumnStoredValue
}

pub enum ColumnStoredValue {
    StaticMemoryPointer(*mut u8),
    ManagedMemoryPointer(Vec<u8>)
}

impl ColumnUnsafe {
    pub fn new(
        db_type: DbType, 
        lentgh: u32, 
        buffer: ColumnStoredValue) -> Self {
        Self {
            db_type,
            lentgh,
            buffer
        }
    }
}