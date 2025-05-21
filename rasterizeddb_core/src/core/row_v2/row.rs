use dashmap::DashMap;

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

pub fn column_vec_into_dashmap(
    columns: &Vec<Column>,
    schema: &Vec<SchemaField>
) -> DashMap<String, MemoryBlock> {
    let column_map = DashMap::new();

    for column in columns {
        column_map.insert(schema[column.schema_id as usize].name.clone(), column.data.clone());
    }

    column_map
}

pub fn update_values_of_dashmap(
    column_map: &DashMap<String, MemoryBlock>,
    columns: &Vec<Column>,
    schema: &Vec<SchemaField>
) {
    for column in columns {
        column_map.alter(&schema[column.schema_id as usize].name, |_, _| {
            column.data.clone()
        });
    }
}