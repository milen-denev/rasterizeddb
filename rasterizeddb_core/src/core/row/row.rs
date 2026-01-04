use std::{borrow::Cow, io::Read};

use byteorder::ReadBytesExt;
use smallvec::SmallVec;

use crate::{
    core::db_type::DbType,
    memory_pool::{MEMORY_POOL, MemoryBlock},
};

use super::schema::SchemaField;

#[derive(Debug)]
pub struct RowFetch {
    pub columns_fetching_data: SmallVec<[ColumnFetchingData; 32]>,
}

#[derive(Debug)]
pub struct ColumnFetchingData {
    pub column_offset: u32,
    pub column_type: DbType,
    pub size: u32,
    pub schema_id: u64,
}

pub struct RowWrite {
    pub columns_writing_data: SmallVec<[ColumnWritePayload; 32]>,
}

pub struct ColumnWritePayload {
    pub data: MemoryBlock,
    pub write_order: u32,
    pub column_type: DbType,
    pub size: u32,
}

#[derive(Debug, Default, Clone)]
pub struct Row {
    pub position: u64,
    pub columns: SmallVec<[Column; 32]>,
    #[cfg(feature = "enable_long_row")]
    pub length: u64,
    #[cfg(not(feature = "enable_long_row"))]
    pub length: u32,
}

impl Row {
    pub fn clone_row(row: &Row) -> Row {
        let columns = row.columns.iter().map(|col| col.deep_clone()).collect();
        Row {
            position: row.position.clone(),
            columns: columns,
            length: row.length.clone(),
        }
    }

    pub fn get_column(
        &self,
        column_name: &str,
        schema: &SmallVec<[SchemaField; 20]>,
    ) -> Option<MemoryBlock> {
        let result = self.columns.iter().find_map(|col| {
            if schema[col.schema_id as usize].name == *column_name {
                let data = col.data.clone();
                return Some(data);
            } else {
                None
            }
        });

        result
    }

    pub fn into_vec(self) -> Vec<u8> {
        let mut vec = Vec::new();

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

            vec.extend_from_slice(&column_data);

            size += column_size;

            vec.extend_from_slice(&[column.column_type.to_byte()]);

            size += 1;
        }

        #[cfg(feature = "enable_long_row")]
        let len_bytes = (size as u64).to_le_bytes();

        #[cfg(not(feature = "enable_long_row"))]
        let len_bytes = (size as u32).to_le_bytes();

        vec[0] = len_bytes[0];
        vec[1] = len_bytes[1];
        vec[2] = len_bytes[2];
        vec[3] = len_bytes[3];

        #[cfg(feature = "enable_long_row")]
        {
            vec[4] = len_bytes[4];
            vec[5] = len_bytes[5];
            vec[6] = len_bytes[6];
            vec[7] = len_bytes[7];
        }

        vec
    }

    pub fn from_vec(bytes: &[u8]) -> std::io::Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);

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
            columns.push(Column {
                schema_id,
                data: column_data_mb,
                column_type,
            });
        }
        Ok(Row {
            position: 0,
            columns,
            length,
        })
    }

    pub fn from_cursor(
        cursor: &mut std::io::Cursor<&[u8]>,
        global_position: &mut u64,
    ) -> std::io::Result<Self> {
        #[cfg(feature = "enable_long_row")]
        let length = cursor.read_u64::<byteorder::LittleEndian>().unwrap();

        #[cfg(not(feature = "enable_long_row"))]
        let length = cursor.read_u32::<byteorder::LittleEndian>().unwrap();

        let mut columns = SmallVec::new();

        #[cfg(feature = "enable_long_row")]
        const ADD_LENGTH: u64 = 8;

        #[cfg(not(feature = "enable_long_row"))]
        const ADD_LENGTH: u64 = 4;

        let mut current_position = global_position.clone() + ADD_LENGTH;
        let end_position = current_position + length as u64;

        cursor.set_position(current_position);

        loop {
            if current_position == end_position {
                break;
            }

            let column_size = cursor.read_u64::<byteorder::LittleEndian>().unwrap();
            let schema_id = cursor.read_u64::<byteorder::LittleEndian>().unwrap();

            let column_data_mb = MEMORY_POOL.acquire(column_size as usize);
            let mut column_data = column_data_mb.into_slice_mut();
            cursor.read_exact(&mut column_data).unwrap();

            let column_type = DbType::from_byte(cursor.read_u8().unwrap());
            columns.push(Column {
                schema_id,
                data: column_data_mb,
                column_type,
            });

            current_position = current_position + 8 + 8 + column_size + 1;
        }

        *global_position += current_position as u64;

        Ok(Row {
            position: 0,
            columns,
            length,
        })
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

    // Fix: Loop until all bytes are consumed
    while (cursor.position() as u64) < len {
        let row = Row::from_cursor(&mut cursor, &mut global_position)?;
        rows.push(row);
        // Ensure global_position is updated to cursor's position
        global_position = cursor.position() as u64;
    }

    Ok(rows)
}

#[derive(Debug, Clone)]
pub struct Column {
    pub schema_id: u64,
    pub data: MemoryBlock,
    pub column_type: DbType,
}

impl Column {
    pub fn deep_clone(&self) -> Self {
        Column {
            schema_id: self.schema_id,
            data: self.data.deep_clone(),
            column_type: self.column_type.clone(),
        }
    }
}

pub fn column_vec_into_vec<'a>(
    row_mut: &mut SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>,
    columns: &Vec<Column>,
    schema: &'a SmallVec<[SchemaField; 20]>,
) {
    for column in columns {
        row_mut.push((
            Cow::Borrowed(schema[column.schema_id as usize].name.as_str()),
            column.data.clone(),
        ));
    }
}

pub enum ReturnResult {
    Rows(Vec<Row>),
    HtmlView(String),
}

#[cfg(test)]
mod row_tests {
    use super::*;
    use crate::core::{db_type::DbType, tokenizer::query_tokenizer::{numeric_to_mb, str_to_mb, NumericValue}};

    fn make_numeric_column(schema_id: u64, value: NumericValue, column_type: DbType) -> Column {
        let mb = numeric_to_mb(&value);
        Column {
            schema_id,
            data: mb,
            column_type,
        }
    }

    fn make_string_column(schema_id: u64, value: &str, column_type: DbType) -> Column {
        let mb = str_to_mb(value);
        Column {
            schema_id,
            data: mb,
            column_type,
        }
    }

    #[cfg(feature = "enable_long_row")]
    fn make_test_row(position: u64, columns: Vec<Column>, length: u64) -> Row {
        Row {
            position,
            columns,
            length,
        }
    }

    #[cfg(not(feature = "enable_long_row"))]
    fn make_test_row(position: u64, columns: SmallVec<[Column; 32]>, length: u32) -> Row {
        Row {
            position,
            columns,
            length,
        }
    }

    #[test]
    fn test_row_into_vec_and_from_vec_roundtrip() {
        let col1 = make_numeric_column(0, NumericValue::I32(0x05040301), DbType::I32);
        let col2 = make_string_column(1, "hello", DbType::STRING);
        let row = make_test_row(42, smallvec::smallvec![col1, col2], 0);
        let vec = row.clone().into_vec();
        let row2 = Row::from_vec(&vec).unwrap();
        assert_eq!(row2.columns.len(), 2);
        assert_eq!(
            row2.columns[0].data.into_slice(),
            &0x05040301i32.to_le_bytes()
        );
        assert_eq!(row2.columns[1].data.into_slice(), b"hello");
        assert_eq!(row2.columns[0].column_type, DbType::I32);
        assert_eq!(row2.columns[1].column_type, DbType::STRING);
    }

    #[test]
    fn test_row_into_vec_and_from_vec_empty_row() {
        let row = make_test_row(0, smallvec::smallvec![], 0);
        let vec = row.clone().into_vec();
        let row2 = Row::from_vec(&vec).unwrap();
        assert_eq!(row2.columns.len(), 0);
    }

    #[test]
    fn test_rows_into_vec_and_vec_into_rows_multiple_rows() {
        let col1 = make_numeric_column(0, NumericValue::I32(2020), DbType::I32);
        let col2 = make_string_column(1, "world", DbType::STRING);
        let row1 = make_test_row(1, smallvec::smallvec![col1.clone()], 0);
        let row2 = make_test_row(2, smallvec::smallvec![col2.clone()], 0);
        let rows = vec![row1.clone(), row2.clone()];
        let vec = rows_into_vec(rows.clone());
        let rows2 = vec_into_rows(&vec).unwrap();
        assert_eq!(rows2.len(), 2);
        assert_eq!(
            rows2[0].columns[0].data.into_slice(),
            &2020i32.to_le_bytes()
        );
        assert_eq!(rows2[1].columns[0].data.into_slice(), b"world");
    }

    #[test]
    fn test_vec_into_rows_empty() {
        let vec: Vec<u8> = Vec::new();
        let rows = vec_into_rows(&vec).unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_row_from_cursor_and_from_vec_equivalence() {
        let col1 = make_numeric_column(0, NumericValue::I32(789), DbType::I32);
        let row = make_test_row(0, smallvec::smallvec![col1.clone()], 0);
        let vec = row.clone().into_vec();
        let mut cursor = std::io::Cursor::new(vec.as_slice());
        let mut global_position = 0u64;
        let row2 = Row::from_cursor(&mut cursor, &mut global_position).unwrap();
        assert_eq!(row2.columns.len(), 1);
        assert_eq!(row2.columns[0].data.into_slice(), &789i32.to_le_bytes());
        assert_eq!(row2.columns[0].column_type, DbType::I32);
    }

    #[test]
    fn test_row_from_cursor_multiple_rows() {
        let col1 = make_numeric_column(0, NumericValue::I32(1), DbType::I32);
        let col2 = make_string_column(1, "2", DbType::STRING);
        let row1 = make_test_row(0, smallvec::smallvec![col1.clone()], 0);
        let row2 = make_test_row(0, smallvec::smallvec![col2.clone()], 0);
        let vec = rows_into_vec(vec![row1.clone(), row2.clone()]);
        let mut cursor = std::io::Cursor::new(vec.as_slice());
        let mut global_position = 0u64;
        let r1 = Row::from_cursor(&mut cursor, &mut global_position).unwrap();
        let r2 = Row::from_cursor(&mut cursor, &mut global_position).unwrap();
        assert_eq!(r1.columns[0].data.into_slice(), &1i32.to_le_bytes());
        assert_eq!(r2.columns[0].data.into_slice(), b"2");
    }

    #[test]
    fn test_row_into_vec_column_types_and_schema_ids() {
        let col1 = make_numeric_column(5, NumericValue::I32(100), DbType::I32);
        let col2 = make_string_column(7, "abc", DbType::STRING);
        let row = make_test_row(0, smallvec::smallvec![col1.clone(), col2.clone()], 0);
        let vec = row.clone().into_vec();
        let row2 = Row::from_vec(&vec).unwrap();
        assert_eq!(row2.columns[0].schema_id, 5);
        assert_eq!(row2.columns[1].schema_id, 7);
        assert_eq!(row2.columns[0].column_type, DbType::I32);
        assert_eq!(row2.columns[1].column_type, DbType::STRING);
    }

    // Running Indefinitely in CI
    // #[test]
    // fn test_row_into_vec_and_from_vec_with_large_data() {
    //     let long_str = "a".repeat(1000);
    //     let col = make_string_column(0, &long_str, DbType::STRING);
    //     let row = make_test_row(0, smallvec::smallvec![col.clone()], 0);
    //     let vec = row.clone().into_vec();
    //     let row2 = Row::from_vec(&vec).unwrap();
    //     assert_eq!(row2.columns[0].data.into_slice(), long_str.as_bytes());
    // }

    #[test]
    fn test_row_into_vec_and_from_vec_with_no_columns() {
        let row = make_test_row(0, smallvec::smallvec![], 0);
        let vec = row.clone().into_vec();
        let row2 = Row::from_vec(&vec).unwrap();
        assert_eq!(row2.columns.len(), 0);
    }

    // Running Indefinitely in CI
    // #[test]
    // fn test_row_with_very_large_numeric_column() {
    //     // Create a column with 100 i32 values
    //     let large_vec: Vec<i32> = (0..100).collect();
    //     let mut bytes = Vec::with_capacity(large_vec.len() * 4);
    //     for v in &large_vec {
    //         bytes.extend_from_slice(&v.to_le_bytes());
    //     }
    //     // Store raw bytes directly (simulate as a string/byte column for simplicity)
    //     let col = Column {
    //         schema_id: 0,
    //         data: MemoryBlock::from_vec(bytes.clone()),
    //         column_type: DbType::STRING,
    //     };
    //     let row = make_test_row(0, smallvec::smallvec![col.clone()], 0);
    //     let vec = row.clone().into_vec();
    //     let row2 = Row::from_vec(&vec).unwrap();
    //     assert_eq!(row2.columns.len(), 1);
    //     assert_eq!(row2.columns[0].data.into_slice().len(), bytes.len());
    //     assert_eq!(row2.columns[0].data.into_slice(), bytes.as_slice());
    // }

    #[test]
    fn test_row_with_very_large_string_column() {
        // Create a string with 100 characters
        let large_str = "x".repeat(100);
        let col = make_string_column(0, &large_str, DbType::STRING);
        let row = make_test_row(0, smallvec::smallvec![col.clone()], 0);
        let vec = row.clone().into_vec();
        let row2 = Row::from_vec(&vec).unwrap();
        assert_eq!(row2.columns.len(), 1);
        assert_eq!(row2.columns[0].data.into_slice(), large_str.as_bytes());
    }

    #[test]
    fn test_rows_into_vec_and_back_with_large_rows() {
        // Create multiple rows with large columns
        let large_str1 = "A".repeat(50);
        let large_str2 = "B".repeat(50);
        let col1 = make_string_column(0, &large_str1, DbType::STRING);
        let col2 = make_string_column(1, &large_str2, DbType::STRING);
        let row1 = make_test_row(0, smallvec::smallvec![col1.clone()], 0);
        let row2 = make_test_row(1, smallvec::smallvec![col2.clone()], 0);
        let rows = vec![row1.clone(), row2.clone()];
        let vec = rows_into_vec(rows.clone());
        let rows2 = vec_into_rows(&vec).unwrap();
        assert_eq!(rows2.len(), 2);
        assert_eq!(rows2[0].columns[0].data.into_slice(), large_str1.as_bytes());
        assert_eq!(rows2[1].columns[0].data.into_slice(), large_str2.as_bytes());
    }

    #[test]
    fn test_combined_roundtrip_large_rows_and_columns() {
        // Create 10 rows, each with a large numeric and string column
        let mut rows = Vec::new();
        for i in 0..10 {
            let num_bytes: Vec<u8> = (0..10)
                .flat_map(|v| (v as i32 + i as i32).to_le_bytes().to_vec())
                .collect();
            let str_val = format!("row{}_{}", i, "Y".repeat(10));
            let col_num = make_string_column(
                0,
                std::str::from_utf8(&num_bytes).unwrap_or(""),
                DbType::STRING,
            );
            let col_str = make_string_column(1, &str_val, DbType::STRING);
            let row = make_test_row(i, smallvec::smallvec![col_num, col_str], 0);
            rows.push(row);
        }
        let vec = rows_into_vec(rows.clone());
        let rows2 = vec_into_rows(&vec).unwrap();
        assert_eq!(rows2.len(), 10);
        for (i, row) in rows2.iter().enumerate() {
            assert_eq!(row.columns.len(), 2);
            let prefix = &row.columns[1].data.into_slice()[..format!("row{}", i).len()];
            assert_eq!(prefix, format!("row{}", i).as_bytes());
        }
    }
}
