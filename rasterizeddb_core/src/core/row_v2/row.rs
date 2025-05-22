use std::borrow::Cow;

use crate::{core::db_type::DbType, memory_pool::MemoryBlock};

use super::schema::SchemaField;

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
            position: row.position.clone(),
            columns: row.columns.clone(),
            length: row.length.clone()
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub schema_id: u64,
    pub data: MemoryBlock,
    pub column_type: DbType,
}

pub fn column_vec_into_vec<'a>(
    row_mut: &mut Vec<(Cow<'a, str>, MemoryBlock)>,
    columns: &Vec<Column>,
    schema: &'a Vec<SchemaField>
) {
    for column in columns {
        row_mut.push((Cow::Borrowed(schema[column.schema_id as usize].name.as_str()), column.data.clone()));
    }
}