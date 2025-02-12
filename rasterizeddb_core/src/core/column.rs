use std::{fmt::{Debug, Display}, io::{self, Cursor, Read, Write}, ptr};

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use once_cell::sync::Lazy;

use crate::{instructions::{
    compare_raw_vecs, 
    compare_vecs_ends_with, 
    compare_vecs_eq, 
    compare_vecs_ne, 
    compare_vecs_starts_with, 
    contains_subsequence,
    vec_from_ptr_safe
}, memory_pool::{Chunk, MEMORY_POOL}};

use super::db_type::DbType;

pub(crate) const ZERO_VALUE: Lazy<Column> = Lazy::new(|| {
    Column::new(0 as i128).unwrap()
});

#[derive(PartialEq, Clone)]
pub struct Column {
    pub data_type: DbType,
    pub content: ColumnValue
}

#[derive(Debug)]
pub enum ColumnValue {
    TempHolder((DbType, Vec<u8>)),
    StaticMemoryPointer(Chunk),
    ManagedMemoryPointer(Vec<u8>)
}

impl ColumnValue {
    pub fn len(&self) -> u32 {
        match self {
            ColumnValue::StaticMemoryPointer(chunk) => chunk.size,
            ColumnValue::ManagedMemoryPointer(vec) => vec.len() as u32,
            _ => panic!("Operation is not supported, column is in temporary state.")
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            ColumnValue::StaticMemoryPointer(chunk) => chunk.size == 0,
            ColumnValue::ManagedMemoryPointer(vec) => vec.is_empty(),
            _ => panic!("Operation is not supported, column is in temporary state.")
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            ColumnValue::StaticMemoryPointer(chunk) => {
                unsafe { std::slice::from_raw_parts::<u8>(chunk.ptr, chunk.size as usize) }
            }
            ColumnValue::ManagedMemoryPointer(vec) => vec.as_slice(),
            _ => panic!("Operation is not supported, column is in temporary state.")
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            ColumnValue::StaticMemoryPointer(chunk) => vec_from_ptr_safe(chunk.ptr, chunk.size as usize),
            ColumnValue::ManagedMemoryPointer(vec) => vec.clone(),
            _ => panic!("Operation is not supported, column is in temporary state.")
        }
    }

    pub fn replace(&mut self, new_values: &[u8]) {
        match self {
            ColumnValue::StaticMemoryPointer(chunk) => {
                assert_eq!(new_values.len(), chunk.size as usize, "New values must have the same length");
                unsafe {
                    std::ptr::copy_nonoverlapping(new_values.as_ptr(), chunk.ptr, chunk.size as usize);
                }
            }
            ColumnValue::ManagedMemoryPointer(vec) => {
                assert_eq!(new_values.len(), vec.len(), "New values must have the same length");
                vec.copy_from_slice(new_values);
            },
            _ => panic!("Operation is not supported, column is in temporary state.")
        }
    }
}

impl PartialEq for ColumnValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ColumnValue::StaticMemoryPointer(pointer_a), ColumnValue::StaticMemoryPointer(pointer_b)) => {
                unsafe { compare_raw_vecs(pointer_a.ptr, pointer_b.ptr, pointer_a.size, pointer_b.size) }
            }
            (ColumnValue::ManagedMemoryPointer(a), ColumnValue::ManagedMemoryPointer(b)) => {
                compare_vecs_eq(a, b)
            }
            _ => false
        }
    }
}

impl Clone for ColumnValue {
    fn clone(&self) -> Self {
        match self {
            ColumnValue::StaticMemoryPointer(chunk) => {
                ColumnValue::StaticMemoryPointer(chunk.clone())
            }
            ColumnValue::ManagedMemoryPointer(vec) => {
                ColumnValue::ManagedMemoryPointer(vec.clone())
            },
            _ => panic!("Operation is not supported, column is in temporary state.")
        }
    }
}

impl Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = self.to_str();
        f.debug_struct("").field("", &val).finish()
    }
}

impl Column {
    fn get_type<T>() -> &'static str {
        std::any::type_name::<T>()
    }

    fn to_vec<T>(value: T, value_type: &str) -> io::Result<Vec<u8>> 
    where
        T: AsRef<str> {
        
            match value_type {
                "i8" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let i8_value = str::parse::<i8>(value.as_ref()).unwrap();
                    buffer.write_i8(i8_value).unwrap();
                    Ok(buffer)
                }
                "i16" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let i16_value = str::parse::<i16>(value.as_ref()).unwrap();
                    buffer.write_i16::<LittleEndian>(i16_value).unwrap();
                    Ok(buffer)
                }
                "i32" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let i32_value = str::parse::<i32>(value.as_ref()).unwrap();
                    buffer.write_i32::<LittleEndian>(i32_value).unwrap();
                    Ok(buffer)
                }
                "i64" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let i64_value = str::parse::<i64>(value.as_ref()).unwrap();
                    buffer.write_i64::<LittleEndian>(i64_value).unwrap();
                    Ok(buffer)
                }
                "i128" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let i128_value = str::parse::<i128>(value.as_ref()).unwrap();
                    buffer.write_i128::<LittleEndian>(i128_value).unwrap();
                    Ok(buffer)
                }
                "u8" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let u8_value = str::parse::<u8>(value.as_ref()).unwrap();
                    buffer.write_u8(u8_value).unwrap();
                    Ok(buffer)
                }
                "u16" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let u16_value = str::parse::<u16>(value.as_ref()).unwrap();
                    buffer.write_u16::<LittleEndian>(u16_value).unwrap();
                    Ok(buffer)
                }
                "u32" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let u32_value = str::parse::<u32>(value.as_ref()).unwrap();
                    buffer.write_u32::<LittleEndian>(u32_value).unwrap();
                    Ok(buffer)
                }
                "u64" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let u64_value = str::parse::<u64>(value.as_ref()).unwrap();
                    buffer.write_u64::<LittleEndian>(u64_value).unwrap();
                    Ok(buffer)
                }
                "u128" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let u128_value = str::parse::<u128>(value.as_ref()).unwrap();
                    buffer.write_u128::<LittleEndian>(u128_value).unwrap();
                    Ok(buffer)
                }
                "f32" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let f32_value = str::parse::<f32>(value.as_ref()).unwrap();
                    buffer.write_f32::<LittleEndian>(f32_value).unwrap();
                    Ok(buffer)
                }
                "f64" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let f64_value = str::parse::<f64>(value.as_ref()).unwrap();
                    buffer.write_f64::<LittleEndian>(f64_value).unwrap();
                    Ok(buffer)
                }
                "char" => {
                    let mut buffer: Vec<u8> = [0u8; 4].to_vec();
                    let char_value = str::parse::<char>(value.as_ref()).unwrap();
                    char_value.encode_utf8(&mut buffer);
                    Ok(buffer)
                }
                "alloc::string::String" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let string_value = str::parse::<String>(value.as_ref()).unwrap();
                    buffer.write_all(string_value.as_bytes()).unwrap();
                    Ok(buffer)
                },
                "&alloc::string::String" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let string_value = str::parse::<String>(value.as_ref()).unwrap();
                    buffer.write_all(string_value.as_bytes()).unwrap();
                    Ok(buffer)
                },
                "&str" => {
                    let mut buffer: Vec<u8> = Vec::new();
                    let string_value = str::parse::<String>(value.as_ref()).unwrap();
                    buffer.write_all(string_value.as_bytes()).unwrap();
                    Ok(buffer)
                }
                // "datetime" => {
                //     let mut buffer: Vec<u8> = Vec::new();
                //     buffer.write_all(value.as_bytes()).unwrap(); // Custom handling for datetime strings
                //     Ok(buffer)
                // }
                // "Option<>" => {
                //     let buffer: Vec<u8> = vec![0]; // Representing NULL as a single byte
                //     Ok(buffer)
                // }
                _ => panic!("Unsupported value type: {}", value_type),
            }
    }

    pub fn new<T>(value: T) -> io::Result<Column>
    where
        T: Display  {
        let value_type = Self::get_type::<T>();

        let vec = Self::to_vec(value.to_string(), value_type).unwrap();

        let db_type = {
            if value_type == "i32" {
                DbType::I32
            } else if value_type == "i8" {
                DbType::I8
            } else if value_type == "i16" {
                DbType::I16
            } else if value_type == "i64" {
                DbType::I64
            } else if value_type == "i128" {
                DbType::I128
            } else if value_type == "u8" {
                DbType::U8
            } else if value_type == "u16" {
                DbType::U16
            } else if value_type == "u32" {
                DbType::U32
            } else if value_type == "u64" {
                DbType::U64
            } else if value_type == "u128" {
                DbType::U128
            } else if value_type == "f32" {
                DbType::F32
            } else if value_type == "f64" {
                DbType::F64
            } else if value_type == "char" {
                DbType::CHAR
            } else if value_type == "alloc::string::String" {
                DbType::STRING
            } else if value_type == "&alloc::string::String" {
                DbType::STRING
            } else if value_type == "&str" {
                DbType::STRING
            } else if value_type == "datetime" {
                DbType::DATETIME
            } else if value_type == "bool" {
                DbType::U8
            } else {
                panic!("Wrong datatype provided: {}.", value_type);
            }
        };

        //Create the buffer with db_type included
        let mut buffer: Vec<u8> = if db_type != DbType::STRING {
            Vec::with_capacity(vec.len() + 1) 
        } else {
            Vec::with_capacity(vec.len() + 1 + 4) 
        };

        buffer.write_u8(db_type.to_byte()).unwrap();

        if db_type == DbType::STRING {
            buffer.write_u32::<LittleEndian>(vec.len() as u32).unwrap();
        }

        buffer.extend_from_slice(&vec);

        Ok(Column {
            data_type: db_type,
            content: ColumnValue::ManagedMemoryPointer(buffer)
        })
    }

    // Vector must be dropped before dropping column
    pub unsafe fn into_chunk(self) -> io::Result<Chunk> {
        let chunk = match self.content {
            ColumnValue::StaticMemoryPointer(chunk) => {
                chunk
            },
            ColumnValue::ManagedMemoryPointer(vec) => Chunk::from_vec(vec),
            _ => panic!("Operation is not supported, column is in temporary state.")
        };

        Ok(chunk)
    }

    pub fn len(&self) -> usize {
        if self.content.is_empty() {
            panic!("Trying to calculate length of empty vector");
        }

        self.content.len() as usize + 1
    }

    pub fn create_temp(is_i128: bool, vec: Vec<u8>) -> Self { 
        if is_i128 {
            Column {
                data_type: DbType::TBD,
                content: ColumnValue::TempHolder((DbType::I128, vec))
            }
        } else {
            Column {
                data_type: DbType::TBD,
                content: ColumnValue::TempHolder((DbType::F64, vec))
            }
        }
    }

    pub fn into_regular(&mut self, db_type: DbType) {
        let new_content = match &self.content {
            ColumnValue::TempHolder((is_i128_or_f64, vec)) => {
                if db_type == DbType::NULL {
                    panic!()
                }

                let new_vec = if vec.len() as u32 != db_type.get_size() {
                    if DbType::I128 == *is_i128_or_f64 {
                        Self::convert_i128_vec(vec, db_type.clone())
                    } else if DbType::I128 == *is_i128_or_f64  {
                        Self::convert_f64_vec(vec, db_type.clone())
                    } else {
                        vec.clone()
                    }
                } else {
                    vec.clone()
                };

                if new_vec.len() as u32 != db_type.get_size() {
                    panic!()
                }

                ColumnValue::ManagedMemoryPointer(new_vec)
            } 
            _ => panic!()
        };
        
        self.content =  new_content;
        self.data_type = db_type;
    }

    pub fn from_chunk(data_type: u8, chunk: Chunk) -> Column {    
        if chunk.vec.is_none() {
            Column {
                data_type: DbType::from_byte(data_type),
                content: ColumnValue::StaticMemoryPointer(chunk)
            }
        } else {
            let vec = chunk.vec.as_ref().unwrap();
            Column {
                data_type: DbType::from_byte(data_type),
                content: ColumnValue::ManagedMemoryPointer(vec.clone())
            }
        }
    }

    pub fn from_raw_clone(data_type: u8, buffer: &[u8]) -> Column {    
        let column_value = {
            ColumnValue::ManagedMemoryPointer(buffer.to_vec())  
        };

        Column {
            data_type: DbType::from_byte(data_type),
            content: column_value
        }
    }

    pub fn from_buffer(buffer: &[u8]) -> io::Result<Vec<Column>> {
        let mut cursor = Cursor::new(buffer);

        let mut columns: Vec<Column> = Vec::default();

        while cursor.position() < buffer.len() as u64 {
            let column_type = cursor.read_u8().unwrap();

            let db_type = DbType::from_byte(column_type);

            let mut data_buffer: Vec<u8> = Vec::default();

            if db_type != DbType::STRING {
                let mut temp_buffer = vec![0; db_type.get_size() as usize];
                cursor.read(&mut temp_buffer).unwrap();
                data_buffer.append(&mut temp_buffer.to_vec());
            } else {
                let str_length = cursor.read_u32::<LittleEndian>().unwrap();
                let mut temp_buffer = vec![0; str_length as usize];
                cursor.read(&mut temp_buffer).unwrap();
                data_buffer.append(&mut temp_buffer.to_vec());
            }

            let memory_chunk = Chunk::from_vec(buffer.to_vec());
            let column = Column::from_chunk(column_type, memory_chunk);

            columns.push(column);
        }

        return Ok(columns);
    }

    #[inline(always)]
    pub fn into_downgrade(self) -> Column {
        let data_type = self.data_type.to_byte();
    
        if (1..=10).contains(&data_type) {
            let db_type = DbType::from_byte(data_type);
            // SAFETY: we assume self.into_chunk() always returns Some(chunk)
            let memory_chunk = unsafe { self.into_chunk().unwrap() };
            let data_buffer = unsafe { memory_chunk.into_vec() };
    
            memory_chunk.prefetch_to_lcache();
            
            // Read the value and cast to i128 as needed.
            let value: i128 = match db_type {
                DbType::I8    => data_buffer.as_slice().read_i8().unwrap() as i128,
                DbType::I16   => LittleEndian::read_i16(data_buffer.as_slice()) as i128,
                DbType::I32   => LittleEndian::read_i32(data_buffer.as_slice()) as i128,
                DbType::I64   => LittleEndian::read_i64(data_buffer.as_slice()) as i128,
                DbType::I128  => LittleEndian::read_i128(data_buffer.as_slice()),
                DbType::U8    => data_buffer.as_slice().read_u8().unwrap() as i128,
                DbType::U16   => LittleEndian::read_u16(data_buffer.as_slice()) as i128,
                DbType::U32   => LittleEndian::read_u32(data_buffer.as_slice()) as i128,
                DbType::U64   => LittleEndian::read_u64(data_buffer.as_slice()) as i128,
                DbType::U128  => LittleEndian::read_u128(data_buffer.as_slice()) as i128,
                _             => panic!("Unsupported type"),
            };
    
            // Convert to little-endian bytes (16 bytes for i128).
            let mut bytes = value.to_le_bytes();
            // Acquire a new memory chunk sized for 16 bytes.
            let new_memory_chunk = MEMORY_POOL
                .acquire(16)
                .unwrap_or_else(|| Chunk::from_vec(vec![0; 16]));

            let new_buffer = unsafe { new_memory_chunk.into_vec() };

            // Copy the bytes safely.
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_mut_ptr(), new_buffer.as_vec().as_ptr() as *mut u8, 8);
            }
    
            // Column type for i128 is indicated by 5.
            return Column::from_chunk(5, new_memory_chunk);
        } else if (11..=12).contains(&data_type) {
            let db_type = DbType::from_byte(data_type);
            let memory_chunk = unsafe { self.into_chunk().unwrap() };
            let data_buffer = unsafe { memory_chunk.into_vec() };

            memory_chunk.prefetch_to_lcache();

            // For floating-point, convert to f64.
            let value: f64 = match db_type {
                DbType::F32 => LittleEndian::read_f32(data_buffer.as_slice()) as f64,
                DbType::F64 => LittleEndian::read_f64(data_buffer.as_slice()) as f64,
                _         => panic!("Unsupported type"),
            };
    
            // f64 converts to 8 little-endian bytes.
            let mut bytes = value.to_le_bytes();
            let new_memory_chunk = MEMORY_POOL
                .acquire(8)
                .unwrap_or_else(|| Chunk::from_vec(vec![0; 8]));

            let new_buffer = unsafe { new_memory_chunk.into_vec() };

            unsafe {
                ptr::copy_nonoverlapping(bytes.as_mut_ptr(), new_buffer.as_vec().as_ptr() as *mut u8, 8);
            }
    
            // Column type for f64 is indicated by 12.
            return Column::from_chunk(12, new_memory_chunk);
        }
        // For any other type, return self unchanged.
        self
    }

    /// For debug purposes
    pub fn print(&self) {
        if self.data_type == DbType::I32 {
            let i32_value = LittleEndian::read_i32(self.content.as_slice());
            println!("i32:{}", i32_value);
        } else if self.data_type == DbType::STRING {
            let vec = self.content.as_slice()[4..].to_vec();
            let string_value = String::from_utf8(vec).unwrap();
            println!("string:{}", string_value);
        } else if self.data_type == DbType::CHAR {
            let char_value = char::from_u32(LittleEndian::read_u32(self.content.as_slice())).unwrap();
            println!("char:{}", char_value);
        } else if self.data_type == DbType::I128 {
            let i128_value = LittleEndian::read_i128(self.content.as_slice());
            println!("i128:{}", i128_value);
        }
    }

    /// For debug purposes
    pub fn to_str(&self) -> String {
        if self.data_type == DbType::I16 {
            let i16_value = LittleEndian::read_i16(self.content.as_slice());
            format!("i16:{}", i16_value)
        } else if self.data_type == DbType::I32 {
            let i32_value = LittleEndian::read_i32(self.content.as_slice());
            format!("i32:{}", i32_value)
        } else if self.data_type == DbType::U16 {
            let u16_value = LittleEndian::read_u16(self.content.as_slice());
            format!("u16:{}", u16_value)
        } else if self.data_type == DbType::STRING {
            let vec = self.content.as_slice()[4..].to_vec();
            let string_value = String::from_utf8(vec).unwrap();
            format!("string:{}", string_value)
        } else if self.data_type == DbType::CHAR {
            let char_value = char::from_u32(LittleEndian::read_u32(self.content.as_slice())).unwrap();
            format!("char:{}", char_value)
        } else if self.data_type == DbType::I128 {
            let i128_value = LittleEndian::read_i128(self.content.as_slice());
            format!("i128:{}", i128_value)
        } else {
            String::from("N/A")
        }
    }
  
    #[inline(always)]
    pub fn into_value(&self) -> String {
        match self.data_type {
            DbType::I8 => {
                let value = self.content.as_slice()[1] as i8;
                value.to_string()
            }
            DbType::I16 => {
                let value = LittleEndian::read_i16(self.content.as_slice()[1..].as_ref());
                value.to_string()
            }
            DbType::I32 => {
                let value = LittleEndian::read_i32(self.content.as_slice()[1..].as_ref());
                value.to_string()
            }
            DbType::I64 => {
                let value = LittleEndian::read_i64(self.content.as_slice()[1..].as_ref());
                value.to_string()
            }
            DbType::I128 => {
                let value = LittleEndian::read_i128(self.content.as_slice()[1..].as_ref());
                value.to_string()
            }
            DbType::U8 => {
                let value = self.content.as_slice()[1] as i8;
                value.to_string()
            }
            DbType::U16 => {
                let value = LittleEndian::read_u16(self.content.as_slice()[1..].as_ref());
                value.to_string()
            }
            DbType::U32 => {
                let value = LittleEndian::read_u32(self.content.as_slice()[1..].as_ref());
                value.to_string()
            }
            DbType::U64 => {
                let value = LittleEndian::read_u64(self.content.as_slice()[1..].as_ref());
                value.to_string()
            }
            DbType::U128 => {
                let value = LittleEndian::read_u128(self.content.as_slice()[1..].as_ref());
                value.to_string()
            }
            DbType::F32 => {
                let value = LittleEndian::read_f32(self.content.as_slice()[1..].as_ref());
                value.to_string()
            }
            DbType::F64 => {
                let value = LittleEndian::read_f64(self.content.as_slice()[1..].as_ref());
                value.to_string()
            }
            DbType::CHAR => {
                let value = char::from_u32(LittleEndian::read_u32(self.content.as_slice()[1..].as_ref())).unwrap();
                value.to_string()
            }
            DbType::STRING => {
                let vec = self.content.as_slice()[4..].to_vec();
                let value = String::from_utf8(vec).unwrap();
                value
            }
            _ => todo!(),
        }
    } 

    pub fn is_zero(&self) -> bool {
        self.equals(&ZERO_VALUE)
    }

    pub fn equals(&self, column: &Column) -> bool {
        //println!("vec 1: {:?} | vec 2: {:?}", self.content.as_slice(), column.content.as_slice());
        compare_vecs_eq(self.content.as_slice(), column.content.as_slice())
    }

    pub fn not_equal(&self, column: &Column) -> bool {
        compare_vecs_ne(self.content.as_slice(), column.content.as_slice())
    }

    pub fn contains(&self, column: &Column) -> bool {
        if self.data_type == DbType::STRING {
            contains_subsequence(self.content.as_slice(), column.content.as_slice())
        } else {
            panic!("Contains method cannot be used on columns that are not of type string");
        }
    }

    pub fn starts_with(&self, column: &Column) -> bool {
        if self.data_type == DbType::STRING {
            compare_vecs_starts_with(self.content.as_slice(), column.content.as_slice())
        } else {
            panic!("Starts With method cannot be used on columns that are not of type string");
        }
    }

    pub fn ends_with(&self, column: &Column) -> bool {
        if self.data_type == DbType::STRING {
            compare_vecs_ends_with(self.content.as_slice(), column.content.as_slice())
        } else {
            panic!("Ends With method cannot be used on columns that are not of type string");
        }
    }

    pub fn greater_than(&self, column: &Column) -> bool {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap();
                let i8_2 = column.content.as_slice().read_i8().unwrap();
                i8_1 > i8_2
            },
            DbType::I16 => {
                let i16_1 = self.content.as_slice().read_i16::<LittleEndian>().unwrap();
                let i16_2 = column.content.as_slice().read_i16::<LittleEndian>().unwrap();
                i16_1 > i16_2
            },
            DbType::I32 => {
                let i32_1 = self.content.as_slice().read_i32::<LittleEndian>().unwrap();
                let i32_2 = column.content.as_slice().read_i32::<LittleEndian>().unwrap();
                i32_1 > i32_2
            },
            DbType::I64 => {
                let i64_1 = self.content.as_slice().read_i64::<LittleEndian>().unwrap();
                let i64_2 = column.content.as_slice().read_i64::<LittleEndian>().unwrap();
                i64_1 > i64_2
            },
            DbType::I128 => {
                let i128_1 = self.content.as_slice().read_i128::<LittleEndian>().unwrap();
                let i128_2 = column.content.as_slice().read_i128::<LittleEndian>().unwrap();
                i128_1 > i128_2
            },
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                u8_1 > u8_2
            },
            DbType::U16 => {
                let u16_1 = self.content.as_slice().read_u16::<LittleEndian>().unwrap();
                let u16_2 = column.content.as_slice().read_u16::<LittleEndian>().unwrap();
                u16_1 > u16_2
            },
            DbType::U32 => {
                let u32_1 = self.content.as_slice().read_u32::<LittleEndian>().unwrap();
                let u32_2 = column.content.as_slice().read_u32::<LittleEndian>().unwrap();
                u32_1 > u32_2
            },
            DbType::U64 => {
                let u64_1 = self.content.as_slice().read_u64::<LittleEndian>().unwrap();
                let u64_2 = column.content.as_slice().read_u64::<LittleEndian>().unwrap();
                u64_1 > u64_2
            },
            DbType::U128 => {
                let u128_1 = self.content.as_slice().read_u128::<LittleEndian>().unwrap();
                let u128_2 = column.content.as_slice().read_u128::<LittleEndian>().unwrap();
                u128_1 > u128_2
            },
            DbType::F32 => {
                let f32_1 = self.content.as_slice().read_f32::<LittleEndian>().unwrap();
                let f32_2 = column.content.as_slice().read_f32::<LittleEndian>().unwrap();
                f32_1 > f32_2
            },
            DbType::F64 => {
                let f64_1 = self.content.as_slice().read_f64::<LittleEndian>().unwrap();
                let f64_2 = column.content.as_slice().read_f64::<LittleEndian>().unwrap();
                f64_1 > f64_2
            },
            _ => panic!("Greater than method cannot be used on columns that are not of numeric types"),
        }
    }

    pub fn greater_or_equals(&self, column: &Column) -> bool {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap();
                let i8_2 = column.content.as_slice().read_i8().unwrap();
                i8_1 >= i8_2
            }
            DbType::I16 => {
                let i16_1 = self.content.as_slice().read_i16::<LittleEndian>().unwrap();
                let i16_2 = column.content.as_slice().read_i16::<LittleEndian>().unwrap();
                i16_1 >= i16_2
            }
            DbType::I32 => {
                let i32_1 = self.content.as_slice().read_i32::<LittleEndian>().unwrap();
                let i32_2 = column.content.as_slice().read_i32::<LittleEndian>().unwrap();
                i32_1 >= i32_2
            }
            DbType::I64 => {
                let i64_1 = self.content.as_slice().read_i64::<LittleEndian>().unwrap();
                let i64_2 = column.content.as_slice().read_i64::<LittleEndian>().unwrap();
                i64_1 >= i64_2
            }
            DbType::I128 => {
                let i128_1 = self.content.as_slice().read_i128::<LittleEndian>().unwrap();
                let i128_2 = column.content.as_slice().read_i128::<LittleEndian>().unwrap();
                i128_1 >= i128_2
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                u8_1 >= u8_2
            }
            DbType::U16 => {
                let u16_1 = self.content.as_slice().read_u16::<LittleEndian>().unwrap();
                let u16_2 = column.content.as_slice().read_u16::<LittleEndian>().unwrap();
                u16_1 >= u16_2
            }
            DbType::U32 => {
                let u32_1 = self.content.as_slice().read_u32::<LittleEndian>().unwrap();
                let u32_2 = column.content.as_slice().read_u32::<LittleEndian>().unwrap();
                u32_1 >= u32_2
            }
            DbType::U64 => {
                let u64_1 = self.content.as_slice().read_u64::<LittleEndian>().unwrap();
                let u64_2 = column.content.as_slice().read_u64::<LittleEndian>().unwrap();
                u64_1 >= u64_2
            }
            DbType::U128 => {
                let u128_1 = self.content.as_slice().read_u128::<LittleEndian>().unwrap();
                let u128_2 = column.content.as_slice().read_u128::<LittleEndian>().unwrap();
                u128_1 >= u128_2
            }
            DbType::F32 => {
                let f32_1 = self.content.as_slice().read_f32::<LittleEndian>().unwrap();
                let f32_2 = column.content.as_slice().read_f32::<LittleEndian>().unwrap();
                f32_1 >= f32_2
            }
            DbType::F64 => {
                let f64_1 = self.content.as_slice().read_f64::<LittleEndian>().unwrap();
                let f64_2 = column.content.as_slice().read_f64::<LittleEndian>().unwrap();
                f64_1 >= f64_2
            }
            _ => panic!("Greater or equals method cannot be used on columns that are not of numeric types"),
        }
    }

    pub fn less_than(&self, column: &Column) -> bool {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap();
                let i8_2 = column.content.as_slice().read_i8().unwrap();
                i8_1 < i8_2
            }
            DbType::I16 => {
                let i16_1 = self.content.as_slice().read_i16::<LittleEndian>().unwrap();
                let i16_2 = column.content.as_slice().read_i16::<LittleEndian>().unwrap();
                i16_1 < i16_2
            }
            DbType::I32 => {
                let i32_1 = self.content.as_slice().read_i32::<LittleEndian>().unwrap();
                let i32_2 = column.content.as_slice().read_i32::<LittleEndian>().unwrap();
                i32_1 < i32_2
            }
            DbType::I64 => {
                let i64_1 = self.content.as_slice().read_i64::<LittleEndian>().unwrap();
                let i64_2 = column.content.as_slice().read_i64::<LittleEndian>().unwrap();
                i64_1 < i64_2
            }
            DbType::I128 => {
                let i128_1 = self.content.as_slice().read_i128::<LittleEndian>().unwrap();
                let i128_2 = column.content.as_slice().read_i128::<LittleEndian>().unwrap();
                i128_1 < i128_2
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                u8_1 < u8_2
            }
            DbType::U16 => {
                let u16_1 = self.content.as_slice().read_u16::<LittleEndian>().unwrap();
                let u16_2 = column.content.as_slice().read_u16::<LittleEndian>().unwrap();
                u16_1 < u16_2
            }
            DbType::U32 => {
                let u32_1 = self.content.as_slice().read_u32::<LittleEndian>().unwrap();
                let u32_2 = column.content.as_slice().read_u32::<LittleEndian>().unwrap();
                u32_1 < u32_2
            }
            DbType::U64 => {
                let u64_1 = self.content.as_slice().read_u64::<LittleEndian>().unwrap();
                let u64_2 = column.content.as_slice().read_u64::<LittleEndian>().unwrap();
                u64_1 < u64_2
            }
            DbType::U128 => {
                let u128_1 = self.content.as_slice().read_u128::<LittleEndian>().unwrap();
                let u128_2 = column.content.as_slice().read_u128::<LittleEndian>().unwrap();
                u128_1 < u128_2
            }
            DbType::F32 => {
                let f32_1 = self.content.as_slice().read_f32::<LittleEndian>().unwrap();
                let f32_2 = column.content.as_slice().read_f32::<LittleEndian>().unwrap();
                f32_1 < f32_2
            }
            DbType::F64 => {
                let f64_1 = self.content.as_slice().read_f64::<LittleEndian>().unwrap();
                let f64_2 = column.content.as_slice().read_f64::<LittleEndian>().unwrap();
                f64_1 < f64_2
            }
            _ => panic!("Less than method cannot be used on columns that are not of numeric types"),
        }
    }

    pub fn less_or_equals(&self, column: &Column) -> bool {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap();
                let i8_2 = column.content.as_slice().read_i8().unwrap();
                i8_1 <= i8_2
            }
            DbType::I16 => {
                let i16_1 = self.content.as_slice().read_i16::<LittleEndian>().unwrap();
                let i16_2 = column.content.as_slice().read_i16::<LittleEndian>().unwrap();
                i16_1 <= i16_2
            }
            DbType::I32 => {
                let i32_1 = self.content.as_slice().read_i32::<LittleEndian>().unwrap();
                let i32_2 = column.content.as_slice().read_i32::<LittleEndian>().unwrap();
                i32_1 <= i32_2
            }
            DbType::I64 => {
                let i64_1 = self.content.as_slice().read_i64::<LittleEndian>().unwrap();
                let i64_2 = column.content.as_slice().read_i64::<LittleEndian>().unwrap();
                i64_1 <= i64_2
            }
            DbType::I128 => {
                let i128_1 = self.content.as_slice().read_i128::<LittleEndian>().unwrap();
                let i128_2 = column.content.as_slice().read_i128::<LittleEndian>().unwrap();
                i128_1 <= i128_2
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                u8_1 <= u8_2
            }
            DbType::U16 => {
                let u16_1 = self.content.as_slice().read_u16::<LittleEndian>().unwrap();
                let u16_2 = column.content.as_slice().read_u16::<LittleEndian>().unwrap();
                u16_1 <= u16_2
            }
            DbType::U32 => {
                let u32_1 = self.content.as_slice().read_u32::<LittleEndian>().unwrap();
                let u32_2 = column.content.as_slice().read_u32::<LittleEndian>().unwrap();
                u32_1 <= u32_2
            }
            DbType::U64 => {
                let u64_1 = self.content.as_slice().read_u64::<LittleEndian>().unwrap();
                let u64_2 = column.content.as_slice().read_u64::<LittleEndian>().unwrap();
                u64_1 <= u64_2
            }
            DbType::U128 => {
                let u128_1 = self.content.as_slice().read_u128::<LittleEndian>().unwrap();
                let u128_2 = column.content.as_slice().read_u128::<LittleEndian>().unwrap();
                u128_1 <= u128_2
            }
            DbType::F32 => {
                let f32_1 = self.content.as_slice().read_f32::<LittleEndian>().unwrap();
                let f32_2 = column.content.as_slice().read_f32::<LittleEndian>().unwrap();
                f32_1 <= f32_2
            }
            DbType::F64 => {
                let f64_1 = self.content.as_slice().read_f64::<LittleEndian>().unwrap();
                let f64_2 = column.content.as_slice().read_f64::<LittleEndian>().unwrap();
                f64_1 <= f64_2
            }
            _ => panic!("Less or equals method cannot be used on columns that are not of numeric types"),
        }
    }

    pub fn add(&mut self, column: &Column) {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap();
                let i8_2 = column.content.as_slice().read_i8().unwrap();
                self.content.replace(&(i8_1 + i8_2).to_le_bytes());
            }
            DbType::I16 => {
                let i16_1 = self.content.as_slice().read_i16::<LittleEndian>().unwrap();
                let i16_2 = column.content.as_slice().read_i16::<LittleEndian>().unwrap();
                self.content.replace(&(i16_1 + i16_2).to_le_bytes());
            }
            DbType::I32 => {
                let i32_1 = self.content.as_slice().read_i32::<LittleEndian>().unwrap();
                let i32_2 = column.content.as_slice().read_i32::<LittleEndian>().unwrap();
                self.content.replace(&(i32_1 + i32_2).to_le_bytes());
            }
            DbType::I64 => {
                let i64_1 = self.content.as_slice().read_i64::<LittleEndian>().unwrap();
                let i64_2 = column.content.as_slice().read_i64::<LittleEndian>().unwrap();
                self.content.replace(&(i64_1 + i64_2).to_le_bytes());
            }
            DbType::I128 => {
                let i128_1 = self.content.as_slice().read_i128::<LittleEndian>().unwrap();
                let i128_2 = column.content.as_slice().read_i128::<LittleEndian>().unwrap();
                self.content.replace(&(i128_1 + i128_2).to_le_bytes());
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                self.content.replace(&(u8_1 + u8_2).to_le_bytes());
            }
            DbType::U16 => {
                let u16_1 = self.content.as_slice().read_u16::<LittleEndian>().unwrap();
                let u16_2 = column.content.as_slice().read_u16::<LittleEndian>().unwrap();
                self.content.replace(&(u16_1 + u16_2).to_le_bytes());
            }
            DbType::U32 => {
                let u32_1 = self.content.as_slice().read_u32::<LittleEndian>().unwrap();
                let u32_2 = column.content.as_slice().read_u32::<LittleEndian>().unwrap();
                self.content.replace(&(u32_1 + u32_2).to_le_bytes());
            }
            DbType::U64 => {
                let u64_1 = self.content.as_slice().read_u64::<LittleEndian>().unwrap();
                let u64_2 = column.content.as_slice().read_u64::<LittleEndian>().unwrap();
                self.content.replace(&(u64_1 + u64_2).to_le_bytes());
            }
            DbType::U128 => {
                let u128_1 = self.content.as_slice().read_u128::<LittleEndian>().unwrap();
                let u128_2 = column.content.as_slice().read_u128::<LittleEndian>().unwrap();
                self.content.replace(&(u128_1 + u128_2).to_le_bytes());
            }
            DbType::F32 => {
                let f32_1 = self.content.as_slice().read_f32::<LittleEndian>().unwrap();
                let f32_2 = column.content.as_slice().read_f32::<LittleEndian>().unwrap();
                self.content.replace(&(f32_1 + f32_2).to_le_bytes());
            }
            DbType::F64 => {
                let f64_1 = self.content.as_slice().read_f64::<LittleEndian>().unwrap();
                let f64_2 = column.content.as_slice().read_f64::<LittleEndian>().unwrap();
                self.content.replace(&(f64_1 + f64_2).to_le_bytes());
            }
            DbType::STRING => {
                let str_1 = String::from_utf8(self.content.as_slice()[4..].to_vec()).unwrap();
                let str_2 = String::from_utf8(column.content.as_slice()[4..].to_vec()).unwrap();
                let concatenated = format!("{}{}", str_1, str_2);
                self.content = ColumnValue::ManagedMemoryPointer(concatenated.as_bytes().to_vec());
            }
            _ => panic!("Addition method cannot be used on unsupported data types"),
        }
    }  

    pub fn subtract(&mut self, column: &Column) {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap();
                let i8_2 = column.content.as_slice().read_i8().unwrap();
                self.content.replace(&(i8_1 - i8_2).to_le_bytes());
            }
            DbType::I16 => {
                let i16_1 = self.content.as_slice().read_i16::<LittleEndian>().unwrap();
                let i16_2 = column.content.as_slice().read_i16::<LittleEndian>().unwrap();
                self.content.replace(&(i16_1 - i16_2).to_le_bytes());
            }
            DbType::I32 => {
                let i32_1 = self.content.as_slice().read_i32::<LittleEndian>().unwrap();
                let i32_2 = column.content.as_slice().read_i32::<LittleEndian>().unwrap();
                self.content.replace(&(i32_1 - i32_2).to_le_bytes());
            }
            DbType::I64 => {
                let i64_1 = self.content.as_slice().read_i64::<LittleEndian>().unwrap();
                let i64_2 = column.content.as_slice().read_i64::<LittleEndian>().unwrap();
                self.content.replace(&(i64_1 - i64_2).to_le_bytes());
            }
            DbType::I128 => {
                let i128_1 = self.content.as_slice().read_i128::<LittleEndian>().unwrap();
                let i128_2 = column.content.as_slice().read_i128::<LittleEndian>().unwrap();
                self.content.replace(&(i128_1 - i128_2).to_le_bytes());
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                self.content.replace(&(u8_1 - u8_2).to_le_bytes());
            }
            DbType::U16 => {
                let u16_1 = self.content.as_slice().read_u16::<LittleEndian>().unwrap();
                let u16_2 = column.content.as_slice().read_u16::<LittleEndian>().unwrap();
                self.content.replace(&(u16_1 - u16_2).to_le_bytes());
            }
            DbType::U32 => {
                let u32_1 = self.content.as_slice().read_u32::<LittleEndian>().unwrap();
                let u32_2 = column.content.as_slice().read_u32::<LittleEndian>().unwrap();
                self.content.replace(&(u32_1 - u32_2).to_le_bytes());
            }
            DbType::U64 => {
                let u64_1 = self.content.as_slice().read_u64::<LittleEndian>().unwrap();
                let u64_2 = column.content.as_slice().read_u64::<LittleEndian>().unwrap();
                self.content.replace(&(u64_1 - u64_2).to_le_bytes());
            }
            DbType::U128 => {
                let u128_1 = self.content.as_slice().read_u128::<LittleEndian>().unwrap();
                let u128_2 = column.content.as_slice().read_u128::<LittleEndian>().unwrap();
                self.content.replace(&(u128_1 - u128_2).to_le_bytes());
            }
            DbType::F32 => {
                let f32_1 = self.content.as_slice().read_f32::<LittleEndian>().unwrap();
                let f32_2 = column.content.as_slice().read_f32::<LittleEndian>().unwrap();
                self.content.replace(&(f32_1 - f32_2).to_le_bytes());
            }
            DbType::F64 => {
                let f64_1 = self.content.as_slice().read_f64::<LittleEndian>().unwrap();
                let f64_2 = column.content.as_slice().read_f64::<LittleEndian>().unwrap();
                self.content.replace(&(f64_1 - f64_2).to_le_bytes());
            }
            _ => panic!("Subtraction method cannot be used on unsupported data types"),
        }
    }

    pub fn multiply(&mut self, column: &Column) {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap();
                let i8_2 = column.content.as_slice().read_i8().unwrap();
                self.content.replace(&(i8_1 * i8_2).to_le_bytes());
            }
            DbType::I16 => {
                let i16_1 = self.content.as_slice().read_i16::<LittleEndian>().unwrap();
                let i16_2 = column.content.as_slice().read_i16::<LittleEndian>().unwrap();
                self.content.replace(&(i16_1 * i16_2).to_le_bytes());
            }
            DbType::I32 => {
                let i32_1 = self.content.as_slice().read_i32::<LittleEndian>().unwrap();
                let i32_2 = column.content.as_slice().read_i32::<LittleEndian>().unwrap();
                self.content.replace(&(i32_1 * i32_2).to_le_bytes());
            }
            DbType::I64 => {
                let i64_1 = self.content.as_slice().read_i64::<LittleEndian>().unwrap();
                let i64_2 = column.content.as_slice().read_i64::<LittleEndian>().unwrap();
                self.content.replace(&(i64_1 * i64_2).to_le_bytes());
            }
            DbType::I128 => {
                let i128_1 = self.content.as_slice().read_i128::<LittleEndian>().unwrap();
                let i128_2 = column.content.as_slice().read_i128::<LittleEndian>().unwrap();
                self.content.replace(&(i128_1 * i128_2).to_le_bytes());
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                self.content.replace(&(u8_1 * u8_2).to_le_bytes());
            }
            DbType::U16 => {
                let u16_1 = self.content.as_slice().read_u16::<LittleEndian>().unwrap();
                let u16_2 = column.content.as_slice().read_u16::<LittleEndian>().unwrap();
                self.content.replace(&(u16_1 * u16_2).to_le_bytes());
            }
            DbType::U32 => {
                let u32_1 = self.content.as_slice().read_u32::<LittleEndian>().unwrap();
                let u32_2 = column.content.as_slice().read_u32::<LittleEndian>().unwrap();
                self.content.replace(&(u32_1 * u32_2).to_le_bytes());
            }
            DbType::U64 => {
                let u64_1 = self.content.as_slice().read_u64::<LittleEndian>().unwrap();
                let u64_2 = column.content.as_slice().read_u64::<LittleEndian>().unwrap();
                self.content.replace(&(u64_1 * u64_2).to_le_bytes());
            }
            DbType::U128 => {
                let u128_1 = self.content.as_slice().read_u128::<LittleEndian>().unwrap();
                let u128_2 = column.content.as_slice().read_u128::<LittleEndian>().unwrap();
                self.content.replace(&(u128_1 * u128_2).to_le_bytes());
            }
            DbType::F32 => {
                let f32_1 = self.content.as_slice().read_f32::<LittleEndian>().unwrap();
                let f32_2 = column.content.as_slice().read_f32::<LittleEndian>().unwrap();
                self.content.replace(&(f32_1 * f32_2).to_le_bytes());
            }
            DbType::F64 => {
                let f64_1 = self.content.as_slice().read_f64::<LittleEndian>().unwrap();
                let f64_2 = column.content.as_slice().read_f64::<LittleEndian>().unwrap();
                self.content.replace(&(f64_1 * f64_2).to_le_bytes());
            }
            _ => panic!("Multiplication method cannot be used on unsupported data types"),
        }
    }

    pub fn divide(&mut self, column: &Column) {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap();
                let i8_2 = column.content.as_slice().read_i8().unwrap();
                self.content.replace(&(i8_1 / i8_2).to_le_bytes());
            }
            DbType::I16 => {
                let i16_1 = self.content.as_slice().read_i16::<LittleEndian>().unwrap();
                let i16_2 = column.content.as_slice().read_i16::<LittleEndian>().unwrap();
                self.content.replace(&(i16_1 / i16_2).to_le_bytes());
            }
            DbType::I32 => {
                let i32_1 = self.content.as_slice().read_i32::<LittleEndian>().unwrap();
                let i32_2 = column.content.as_slice().read_i32::<LittleEndian>().unwrap();
                self.content.replace(&(i32_1 / i32_2).to_le_bytes());
            }
            DbType::I64 => {
                let i64_1 = self.content.as_slice().read_i64::<LittleEndian>().unwrap();
                let i64_2 = column.content.as_slice().read_i64::<LittleEndian>().unwrap();
                self.content.replace(&(i64_1 / i64_2).to_le_bytes());
            }
            DbType::I128 => {
                let i128_1 = self.content.as_slice().read_i128::<LittleEndian>().unwrap();
                let i128_2 = column.content.as_slice().read_i128::<LittleEndian>().unwrap();
                self.content.replace(&(i128_1 / i128_2).to_le_bytes());
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                self.content.replace(&(u8_1 / u8_2).to_le_bytes());
            }
            DbType::U16 => {
                let u16_1 = self.content.as_slice().read_u16::<LittleEndian>().unwrap();
                let u16_2 = column.content.as_slice().read_u16::<LittleEndian>().unwrap();
                self.content.replace(&(u16_1 / u16_2).to_le_bytes());
            }
            DbType::U32 => {
                let u32_1 = self.content.as_slice().read_u32::<LittleEndian>().unwrap();
                let u32_2 = column.content.as_slice().read_u32::<LittleEndian>().unwrap();
                self.content.replace(&(u32_1 / u32_2).to_le_bytes());
            }
            DbType::U64 => {
                let u64_1 = self.content.as_slice().read_u64::<LittleEndian>().unwrap();
                let u64_2 = column.content.as_slice().read_u64::<LittleEndian>().unwrap();
                self.content.replace(&(u64_1 / u64_2).to_le_bytes());
            }
            DbType::U128 => {
                let u128_1 = self.content.as_slice().read_u128::<LittleEndian>().unwrap();
                let u128_2 = column.content.as_slice().read_u128::<LittleEndian>().unwrap();
                self.content.replace(&(u128_1 / u128_2).to_le_bytes());
            }
            DbType::F32 => {
                let f32_1 = self.content.as_slice().read_f32::<LittleEndian>().unwrap();
                let f32_2 = column.content.as_slice().read_f32::<LittleEndian>().unwrap();
                self.content.replace(&(f32_1 / f32_2).to_le_bytes());
            }
            DbType::F64 => {
                let f64_1 = self.content.as_slice().read_f64::<LittleEndian>().unwrap();
                let f64_2 = column.content.as_slice().read_f64::<LittleEndian>().unwrap();
                self.content.replace(&(f64_1 / f64_2).to_le_bytes());
            }
            _ => panic!("Division method cannot be used on unsupported data types"),
        }
    }

    fn convert_f64_vec(vec: &Vec<u8>, db_type: DbType) -> Vec<u8> {
        let mut buf = [0u8; 8];
        let len = vec.len().min(8);
        buf[..len].copy_from_slice(&vec[..len]);
        let value = f64::from_le_bytes(buf);
        match db_type {
            DbType::F64 => value.to_le_bytes().to_vec(),
            DbType::F32 => {
                let value_f32 = value as f32;
                value_f32.to_le_bytes().to_vec()
            }
            _ => panic!("Unsupported expected size {} for float conversion", db_type.get_size()),
        }
    }

    fn convert_i128_vec(vec: &Vec<u8>, db_type: DbType) -> Vec<u8> {
        // Prepare a 16-byte buffer (i128 is 16 bytes)
        let mut buf = [0u8; 16];
        // Copy the available bytes (if vec is shorter, assume it was zero–padded)
        let len = vec.len().min(16);
        buf[..len].copy_from_slice(&vec[..len]);
        let value = i128::from_le_bytes(buf);
        match db_type {
            DbType::U128 => {
                // Convert to u128 (lossy if value < 0)
                let value_u128 = value as u128;
                value_u128.to_le_bytes().to_vec() // 16 bytes
            }
            DbType::I128 => {
                value.to_le_bytes().to_vec() // 16 bytes
            }
            DbType::U64 => {
                let value_u64 = value as u64;
                value_u64.to_le_bytes().to_vec() // 8 bytes
            }
            DbType::I64 => {
                let value_i64 = value as i64;
                value_i64.to_le_bytes().to_vec() // 8 bytes
            }
            DbType::U32 => {
                let value_u32 = value as u32;
                value_u32.to_le_bytes().to_vec() // 4 bytes
            }
            DbType::I32 => {
                let value_i32 = value as i32;
                value_i32.to_le_bytes().to_vec() // 4 bytes
            }
            DbType::U16 => {
                let value_u16 = value as u16;
                value_u16.to_le_bytes().to_vec() // 2 bytes
            }
            DbType::I16 => {
                let value_i16 = value as i16;
                value_i16.to_le_bytes().to_vec() // 2 bytes
            }
            DbType::U8 => {
                let value_u8 = value as u8;
                value_u8.to_le_bytes().to_vec() // 1 byte
            }
            DbType::I8 => {
                let value_i8 = value as i8;
                value_i8.to_le_bytes().to_vec() // 1 byte
            }
            _ => panic!("Unsupported expected size {} for integer conversion", db_type.get_size()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum DowngradeType {
    I128,
    F64
}