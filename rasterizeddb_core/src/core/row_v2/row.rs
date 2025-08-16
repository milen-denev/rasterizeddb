use std::{borrow::Cow, io::Read};

use byteorder::ReadBytesExt;
use log::{debug, info};
use smallvec::SmallVec;

use crate::{core::db_type::DbType, memory_pool::{MemoryBlock, MEMORY_POOL}};

use super::schema::SchemaField;

#[derive(Debug)]
pub struct RowFetch {
    pub columns_fetching_data: Vec<ColumnFetchingData>
}

#[derive(Debug)]
pub struct ColumnFetchingData {
    pub column_offset: u32,
    pub column_type: DbType,
    pub size: u32
}

pub struct RowWrite {
    pub columns_writing_data: SmallVec<[ColumnWritePayload; 32]>
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
    pub columns: SmallVec<[Column; 20]>,
    
    #[cfg(feature = "enable_long_row")]
    pub length: u64,

    #[cfg(not(feature = "enable_long_row"))]
    pub length: u32,
}

impl Row {
    pub fn clone_row(row: &Row) -> Row {
        let columns = row.columns.iter().map(|col| col.clone()).collect();
        
        Row {
            position: row.position.clone(),
            columns: columns,
            length: row.length.clone()
        }
    }

    pub fn get_column(&self, column_name: &str, schema: &SmallVec<[SchemaField; 20]>) -> Option<MemoryBlock> {
        let result = self.columns.iter().find_map(|col| {
            if schema[col.schema_id as usize].name == *column_name {
                return Some(col.data.clone());
            } else {
                None
            }
        });

        result
    }

    pub fn into_vec(self) -> Vec<u8> {
        let mut vec = Vec::new();
        //vec.extend_from_slice(&self.position.to_le_bytes());

        #[cfg(feature = "enable_long_row")]
        const LEN_EMPTY_BUFFER: [u8; 8] = [0u8; 8];

        #[cfg(not(feature = "enable_long_row"))]
        const LEN_EMPTY_BUFFER: [u8; 4] = [0u8; 4];

        vec.extend_from_slice(&LEN_EMPTY_BUFFER);
        
        let mut size: u64 = 0;

        for column in self.columns {
            let column_data = column.data.into_slice();
            let column_size = column_data.len() as u64;
            size += 8 + 8;
            vec.extend_from_slice(&column_size.to_le_bytes());
            vec.extend_from_slice(&column.schema_id.to_le_bytes());

            vec.extend_from_slice(&column.data.into_slice());
            
            debug!("Writing column with data: {:?}", column.data.into_slice());

            size += column_size;

            vec.extend_from_slice(&[column.column_type.to_byte()]);
            size += 1;
        }

        #[cfg(feature = "enable_long_row")]
        let len_bytes = (size as u64).to_le_bytes();

        #[cfg(not(feature = "enable_long_row"))]
        let len_bytes = (size as u32).to_le_bytes();

        vec[0..len_bytes.len()].copy_from_slice(&len_bytes);

        debug!("Row turned into vector: {:?}", vec);
        vec
    }

    pub fn from_vec(bytes: &[u8]) -> std::io::Result<Self> {
        info!("Reading row from vector");
        debug!("Row bytes: {:?}", bytes);

        let mut cursor = std::io::Cursor::new(bytes);
        //let position = cursor.read_u64::<byteorder::LittleEndian>()?;

        #[cfg(feature = "enable_long_row")]
        let length = cursor.read_u64::<byteorder::LittleEndian>()?;

        #[cfg(not(feature = "enable_long_row"))]
        let length = cursor.read_u32::<byteorder::LittleEndian>()?;
        
        let mut columns = SmallVec::new();
        while cursor.position() < bytes.len() as u64 {
            let column_size = cursor.read_u64::<byteorder::LittleEndian>()?;
            let schema_id = cursor.read_u64::<byteorder::LittleEndian>()?;
            
            let column_data_mb = MEMORY_POOL.acquire(column_size as usize);
            let mut column_data = column_data_mb.into_slice_mut();
            cursor.read_exact(&mut column_data)?;

            let column_type = DbType::from_byte(cursor.read_u8()?);
            columns.push(Column { schema_id, data: column_data_mb, column_type });
        }
        Ok(Row { position: 0, columns, length })
    }

    pub fn from_cursor(cursor: &mut std::io::Cursor<&[u8]>, global_position: &mut u64) -> std::io::Result<Self> {
        info!("Reading row from cursor, position: {}", global_position);

        //let position = cursor.read_u64::<byteorder::LittleEndian>()?;

        #[cfg(feature = "enable_long_row")]
        let length = cursor.read_u64::<byteorder::LittleEndian>()?;

        #[cfg(not(feature = "enable_long_row"))]
        let length = cursor.read_u32::<byteorder::LittleEndian>()?;
        
        let mut columns = SmallVec::new();

        let mut current_position = global_position.clone() + 4;
        let end_position = current_position + length as u64;

        cursor.set_position(current_position);

        loop {
            if current_position == end_position {
                break;
            }
            
            let column_size = cursor.read_u64::<byteorder::LittleEndian>()?;
            let schema_id = cursor.read_u64::<byteorder::LittleEndian>()?;
            
            let column_data_mb = MEMORY_POOL.acquire(column_size as usize);
            let mut column_data = column_data_mb.into_slice_mut();
            cursor.read_exact(&mut column_data)?;

            let column_type = DbType::from_byte(cursor.read_u8()?);
            columns.push(Column { schema_id, data: column_data_mb, column_type });

            current_position = current_position + 8 + 8 + column_size + 1;
        }

        *global_position += current_position as u64;

        Ok(Row { position: 0, columns, length })
    }
}

pub fn rows_into_vec(rows: Vec<Row>) -> Vec<u8> {
    let mut vec = Vec::new();
    for row in rows {
        vec.extend(row.into_vec());
    }
    vec
}

pub fn vec_into_rows(bytes: &[u8]) -> std::io::Result<Vec<Row>> {
    let mut rows = Vec::new();
    let mut cursor = std::io::Cursor::new(bytes);

    let mut global_position = 0;

    let len = bytes.len() as u64;

    while global_position < len {
        info!("Reading row at global position: {}", global_position);
        let row = Row::from_cursor(&mut cursor, &mut global_position)?;
        rows.push(row);
    }

    Ok(rows)
}

#[derive(Debug, Clone)]
pub struct Column {
    pub schema_id: u64,
    pub data: MemoryBlock,
    pub column_type: DbType,
}

pub fn column_vec_into_vec<'a>(
    row_mut: &mut SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>,
    columns: &SmallVec<[Column; 20]>,
    schema: &'a SmallVec<[SchemaField; 20]>
) {
    for column in columns {
        row_mut.push((Cow::Borrowed(schema[column.schema_id as usize].name.as_str()), column.data.clone()));
    }
}

pub enum ReturnResult {
    Rows(Vec<Row>),
    HtmlView(String)
}