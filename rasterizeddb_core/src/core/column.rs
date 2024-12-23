use std::{fmt::{Debug, Display}, io::{self, Cursor, Read, Write}};

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::instructions::{compare_vecs_ends_with, compare_vecs_eq, compare_vecs_ne, compare_vecs_starts_with, contains_subsequence};

use super::db_type::DbType;

#[derive(PartialEq, Clone)]
pub struct Column {
    pub data_type: DbType,
    pub content: Vec<u8>
}

impl Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = self.to_str();
        f.debug_struct("").field("", &val).finish()
    }
}

impl Column {
    fn get_type<T>() -> &'static str{
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
            } else if value_type == "f32" {
                DbType::F32
            } else if value_type == "f64" {
                DbType::F64
            } else if value_type == "char" {
                DbType::CHAR
            } else if value_type == "alloc::string::String" {
                DbType::STRING
            } else if value_type == "&str" {
                DbType::STRING
            } else if value_type == "datetime" {
                DbType::DATETIME
            } else {
                panic!("Wrong datatype provided: {}.", value_type);
            }
        };

        Ok(Column {
            data_type: db_type,
            content: vec
        })
    }

    pub fn into_vec(&mut self) -> io::Result<Vec<u8>> {
        let content_len = self.content.len();

        let mut buffer: Vec<u8> = if self.data_type != DbType::STRING {
            Vec::with_capacity(content_len + 1) 
        } else {
            Vec::with_capacity(content_len + 1 + 4) 
        };

        buffer.push(self.data_type.to_byte());

        if self.data_type == DbType::STRING {
            buffer.write_u32::<LittleEndian>(content_len as u32).unwrap();
        }

        buffer.append(&mut self.content);

        Ok(buffer)
    }

    pub fn len(&self) -> usize {
        if self.content.is_empty() {
            panic!("Trying to calculate length of empty vector");
        }

        self.content.len() + 1
    }

    pub fn from_raw(data_type: u8, buffer: Vec<u8>, downgrade_type: Option<DowngradeType>) -> Column {        
        if let Some(downgrade_type) = downgrade_type {
            if downgrade_type == DowngradeType::I128 {
                if data_type >= 1 && data_type <= 10 {
                    let db_type_enum = DbType::from_byte(data_type);
    
                    let mut cursor = Cursor::new(buffer);
                    let mut data_buffer: Vec<u8> = Vec::default();
    
                    let mut temp_buffer = vec![0; db_type_enum.get_size() as usize];
                    cursor.read(&mut temp_buffer).unwrap();
                    data_buffer.append(&mut temp_buffer.to_vec());
                    
                    let value = match db_type_enum {
                        DbType::I8 => data_buffer.as_slice().read_i8().unwrap() as i128,
                        DbType::I16 => LittleEndian::read_i16(&data_buffer) as i128,
                        DbType::I32 => LittleEndian::read_i32(&data_buffer) as i128,
                        DbType::I64 => LittleEndian::read_i64(&data_buffer) as i128,
                        DbType::I128 => LittleEndian::read_i128(&data_buffer),
                        DbType::U8 => data_buffer.as_slice().read_u8().unwrap() as i128,
                        DbType::U16 => LittleEndian::read_u16(&data_buffer) as i128,
                        DbType::U32 => LittleEndian::read_u32(&data_buffer) as i128,
                        DbType::U64 => LittleEndian::read_u64(&data_buffer) as i128,
                        DbType::U128 => LittleEndian::read_u128(&data_buffer) as i128,
                        _ => panic!("Unsupported type"),
                    };
    
                    let mut new_buffer: Vec<u8> = Vec::with_capacity(0);
    
                    new_buffer.write_i128::<LittleEndian>(value).unwrap();
    
                    return Column {
                        data_type: db_type_enum,
                        content: new_buffer
                    };
                }
            } else {
                if data_type >= 11 && data_type <= 12 {
                    let db_type_enum = DbType::from_byte(data_type);
    
                    let mut cursor = Cursor::new(buffer);
                    let mut data_buffer: Vec<u8> = Vec::default();
    
                    let mut temp_buffer = vec![0; db_type_enum.get_size() as usize];
                    cursor.read(&mut temp_buffer).unwrap();
                    data_buffer.append(&mut temp_buffer.to_vec());
                    
                    let value = match db_type_enum {
                        DbType::F32 => LittleEndian::read_f32(&data_buffer) as f64,
                        DbType::F64 => LittleEndian::read_f64(&data_buffer) as f64,
                        _ => panic!("Unsupported type"),
                    };
    
                    let mut new_buffer: Vec<u8> = Vec::with_capacity(0);
    
                    new_buffer.write_f64::<LittleEndian>(value).unwrap();
    
                    return Column {
                        data_type: db_type_enum,
                        content: new_buffer
                    };
                }
            }
        }

        Column {
            data_type: DbType::from_byte(data_type),
            content: buffer
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

            let column = Column::from_raw(column_type, data_buffer, None);

            columns.push(column);
        }

        return Ok(columns);
    }

    /// For debug purposes
    pub fn print(&self) {
        if self.data_type == DbType::I32 {
            let i32_value = LittleEndian::read_i32(&self.content);
            println!("i32:{}", i32_value);
        } else if self.data_type == DbType::STRING {
            let string_value = String::from_utf8(self.content.clone()).unwrap();
            println!("string:{}", string_value);
        } else if self.data_type == DbType::CHAR {
            let char_value = char::from_u32(LittleEndian::read_u32(&self.content)).unwrap();
            println!("char:{}", char_value);
        } else if self.data_type == DbType::I128 {
            let i128_value = LittleEndian::read_i128(&self.content);
            println!("i128:{}", i128_value);
        }
    }

    /// For debug purposes
    pub fn to_str(&self) -> String {
        if self.data_type == DbType::I32 {
            let i32_value = LittleEndian::read_i32(&self.content);
            format!("i32:{}", i32_value)
        } else if self.data_type == DbType::STRING {
            let string_value = String::from_utf8(self.content.clone()).unwrap();
            format!("string:{}", string_value)
        } else if self.data_type == DbType::CHAR {
            let char_value = char::from_u32(LittleEndian::read_u32(&self.content)).unwrap();
            format!("char:{}", char_value)
        } else if self.data_type == DbType::I128 {
            let i128_value = LittleEndian::read_i128(&self.content);
            format!("i128:{}", i128_value)
        } else {
            String::from("N/A")
        }
    }
  
    pub fn into_value(&self) -> String {
        match self.data_type {
            DbType::I8 => {
                let value = self.content[0] as i8;
                value.to_string()
            }
            DbType::I16 => {
                let value = LittleEndian::read_i16(&self.content);
                value.to_string()
            }
            DbType::I32 => {
                let value = LittleEndian::read_i32(&self.content);
                value.to_string()
            }
            DbType::I64 => {
                let value = LittleEndian::read_i64(&self.content);
                value.to_string()
            }
            DbType::I128 => {
                let value = LittleEndian::read_i128(&self.content);
                value.to_string()
            }
            DbType::U8 => {
                let value = self.content[0] as u8;
                value.to_string()
            }
            DbType::U16 => {
                let value = LittleEndian::read_u16(&self.content);
                value.to_string()
            }
            DbType::U32 => {
                let value = LittleEndian::read_u32(&self.content);
                value.to_string()
            }
            DbType::U64 => {
                let value = LittleEndian::read_u64(&self.content);
                value.to_string()
            }
            DbType::U128 => {
                let value = LittleEndian::read_u128(&self.content);
                value.to_string()
            }
            DbType::F32 => {
                let value = LittleEndian::read_f32(&self.content);
                value.to_string()
            }
            DbType::F64 => {
                let value = LittleEndian::read_f64(&self.content);
                value.to_string()
            }
            DbType::CHAR => {
                let value = char::from_u32(LittleEndian::read_u32(&self.content)).unwrap();
                value.to_string()
            }
            DbType::STRING => {
                let value = String::from_utf8(self.content.clone()).unwrap();
                value
            }
            _ => todo!(),
        }
    } 

    pub fn equals(&self, column: &Column) -> bool {
        compare_vecs_eq(&self.content, &column.content)
    }

    pub fn ne(&self, column: &Column) -> bool {
        compare_vecs_ne(&self.content, &column.content)
    }

    pub fn contains(&self, column: &Column) -> bool {
        if self.data_type == DbType::STRING {
            contains_subsequence(&self.content, &column.content)
        } else {
            panic!("Contains method cannot be used on columns that are not of type string");
        }
    }

    pub fn starts_with(&self, column: &Column) -> bool {
        if self.data_type == DbType::STRING {
            compare_vecs_starts_with(&self.content, &column.content)
        } else {
            panic!("Starts With method cannot be used on columns that are not of type string");
        }
    }

    pub fn ends_with(&self, column: &Column) -> bool {
        if self.data_type == DbType::STRING {
            compare_vecs_ends_with(&self.content, &column.content)
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
            }
            DbType::I16 => {
                let i16_1 = Cursor::new(&self.content).read_i16::<LittleEndian>().unwrap();
                let i16_2 = Cursor::new(&column.content).read_i16::<LittleEndian>().unwrap();
                i16_1 > i16_2
            }
            DbType::I32 => {
                let i32_1 = Cursor::new(&self.content).read_i32::<LittleEndian>().unwrap();
                let i32_2 = Cursor::new(&column.content).read_i32::<LittleEndian>().unwrap();
                i32_1 > i32_2
            }
            DbType::I64 => {
                let i64_1 = Cursor::new(&self.content).read_i64::<LittleEndian>().unwrap();
                let i64_2 = Cursor::new(&column.content).read_i64::<LittleEndian>().unwrap();
                i64_1 > i64_2
            }
            DbType::I128 => {
                let i128_1 = Cursor::new(&self.content).read_i128::<LittleEndian>().unwrap();
                let i128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                i128_1 > i128_2
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                u8_1 > u8_2
            }
            DbType::U16 => {
                let u16_1 = Cursor::new(&self.content).read_u16::<LittleEndian>().unwrap();
                let u16_2 = Cursor::new(&column.content).read_u16::<LittleEndian>().unwrap();
                u16_1 > u16_2
            }
            DbType::U32 => {
                let u32_1 = Cursor::new(&self.content).read_u32::<LittleEndian>().unwrap();
                let u32_2 = Cursor::new(&column.content).read_u32::<LittleEndian>().unwrap();
                u32_1 > u32_2
            }
            DbType::U64 => {
                let u64_1 = Cursor::new(&self.content).read_u64::<LittleEndian>().unwrap();
                let u64_2 = Cursor::new(&column.content).read_u64::<LittleEndian>().unwrap();
                u64_1 > u64_2
            }
            DbType::U128 => {
                let u128_1 = Cursor::new(&self.content).read_u128::<LittleEndian>().unwrap();
                let u128_2 = Cursor::new(&column.content).read_u128::<LittleEndian>().unwrap();
                u128_1 > u128_2
            }
            DbType::F32 => {
                let f32_1 = Cursor::new(&self.content).read_f32::<LittleEndian>().unwrap();
                let f32_2 = Cursor::new(&column.content).read_f32::<LittleEndian>().unwrap();
                f32_1 > f32_2
            }
            DbType::F64 => {
                let f64_1 = Cursor::new(&self.content).read_f64::<LittleEndian>().unwrap();
                let f64_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
                f64_1 > f64_2
            }
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
                let i16_1 = Cursor::new(&self.content).read_i16::<LittleEndian>().unwrap();
                let i16_2 = Cursor::new(&column.content).read_i16::<LittleEndian>().unwrap();
                i16_1 >= i16_2
            }
            DbType::I32 => {
                let i32_1 = Cursor::new(&self.content).read_i32::<LittleEndian>().unwrap();
                let i32_2 = Cursor::new(&column.content).read_i32::<LittleEndian>().unwrap();
                i32_1 >= i32_2
            }
            DbType::I64 => {
                let i64_1 = Cursor::new(&self.content).read_i64::<LittleEndian>().unwrap();
                let i64_2 = Cursor::new(&column.content).read_i64::<LittleEndian>().unwrap();
                i64_1 >= i64_2
            }
            DbType::I128 => {
                let i128_1 = Cursor::new(&self.content).read_i128::<LittleEndian>().unwrap();
                let i128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                i128_1 >= i128_2
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                u8_1 >= u8_2
            }
            DbType::U16 => {
                let u16_1 = Cursor::new(&self.content).read_u16::<LittleEndian>().unwrap();
                let u16_2 = Cursor::new(&column.content).read_u16::<LittleEndian>().unwrap();
                u16_1 >= u16_2
            }
            DbType::U32 => {
                let u32_1 = Cursor::new(&self.content).read_u32::<LittleEndian>().unwrap();
                let u32_2 = Cursor::new(&column.content).read_u32::<LittleEndian>().unwrap();
                u32_1 >= u32_2
            }
            DbType::U64 => {
                let u64_1 = Cursor::new(&self.content).read_u64::<LittleEndian>().unwrap();
                let u64_2 = Cursor::new(&column.content).read_u64::<LittleEndian>().unwrap();
                u64_1 >= u64_2
            }
            DbType::U128 => {
                let u128_1 = Cursor::new(&self.content).read_u128::<LittleEndian>().unwrap();
                let u128_2 = Cursor::new(&column.content).read_u128::<LittleEndian>().unwrap();
                u128_1 >= u128_2
            }
            DbType::F32 => {
                let f32_1 = Cursor::new(&self.content).read_f32::<LittleEndian>().unwrap();
                let f32_2 = Cursor::new(&column.content).read_f32::<LittleEndian>().unwrap();
                f32_1 >= f32_2
            }
            DbType::F64 => {
                let f64_1 = Cursor::new(&self.content).read_f64::<LittleEndian>().unwrap();
                let f64_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
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
                let i16_1 = Cursor::new(&self.content).read_i16::<LittleEndian>().unwrap();
                let i16_2 = Cursor::new(&column.content).read_i16::<LittleEndian>().unwrap();
                i16_1 < i16_2
            }
            DbType::I32 => {
                let i32_1 = Cursor::new(&self.content).read_i32::<LittleEndian>().unwrap();
                let i32_2 = Cursor::new(&column.content).read_i32::<LittleEndian>().unwrap();
                i32_1 < i32_2
            }
            DbType::I64 => {
                let i64_1 = Cursor::new(&self.content).read_i64::<LittleEndian>().unwrap();
                let i64_2 = Cursor::new(&column.content).read_i64::<LittleEndian>().unwrap();
                i64_1 < i64_2
            }
            DbType::I128 => {
                let i128_1 = Cursor::new(&self.content).read_i128::<LittleEndian>().unwrap();
                let i128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                i128_1 < i128_2
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                u8_1 < u8_2
            }
            DbType::U16 => {
                let u16_1 = Cursor::new(&self.content).read_u16::<LittleEndian>().unwrap();
                let u16_2 = Cursor::new(&column.content).read_u16::<LittleEndian>().unwrap();
                u16_1 < u16_2
            }
            DbType::U32 => {
                let u32_1 = Cursor::new(&self.content).read_u32::<LittleEndian>().unwrap();
                let u32_2 = Cursor::new(&column.content).read_u32::<LittleEndian>().unwrap();
                u32_1 < u32_2
            }
            DbType::U64 => {
                let u64_1 = Cursor::new(&self.content).read_u64::<LittleEndian>().unwrap();
                let u64_2 = Cursor::new(&column.content).read_u64::<LittleEndian>().unwrap();
                u64_1 < u64_2
            }
            DbType::U128 => {
                let u128_1 = Cursor::new(&self.content).read_u128::<LittleEndian>().unwrap();
                let u128_2 = Cursor::new(&column.content).read_u128::<LittleEndian>().unwrap();
                u128_1 < u128_2
            }
            DbType::F32 => {
                let f32_1 = Cursor::new(&self.content).read_f32::<LittleEndian>().unwrap();
                let f32_2 = Cursor::new(&column.content).read_f32::<LittleEndian>().unwrap();
                f32_1 < f32_2
            }
            DbType::F64 => {
                let f64_1 = Cursor::new(&self.content).read_f64::<LittleEndian>().unwrap();
                let f64_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
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
                let i16_1 = Cursor::new(&self.content).read_i16::<LittleEndian>().unwrap();
                let i16_2 = Cursor::new(&column.content).read_i16::<LittleEndian>().unwrap();
                i16_1 <= i16_2
            }
            DbType::I32 => {
                let i32_1 = Cursor::new(&self.content).read_i32::<LittleEndian>().unwrap();
                let i32_2 = Cursor::new(&column.content).read_i32::<LittleEndian>().unwrap();
                i32_1 <= i32_2
            }
            DbType::I64 => {
                let i64_1 = Cursor::new(&self.content).read_i64::<LittleEndian>().unwrap();
                let i64_2 = Cursor::new(&column.content).read_i64::<LittleEndian>().unwrap();
                i64_1 <= i64_2
            }
            DbType::I128 => {
                let i128_1 = Cursor::new(&self.content).read_i128::<LittleEndian>().unwrap();
                let i128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                i128_1 <= i128_2
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap();
                let u8_2 = column.content.as_slice().read_u8().unwrap();
                u8_1 <= u8_2
            }
            DbType::U16 => {
                let u16_1 = Cursor::new(&self.content).read_u16::<LittleEndian>().unwrap();
                let u16_2 = Cursor::new(&column.content).read_u16::<LittleEndian>().unwrap();
                u16_1 <= u16_2
            }
            DbType::U32 => {
                let u32_1 = Cursor::new(&self.content).read_u32::<LittleEndian>().unwrap();
                let u32_2 = Cursor::new(&column.content).read_u32::<LittleEndian>().unwrap();
                u32_1 <= u32_2
            }
            DbType::U64 => {
                let u64_1 = Cursor::new(&self.content).read_u64::<LittleEndian>().unwrap();
                let u64_2 = Cursor::new(&column.content).read_u64::<LittleEndian>().unwrap();
                u64_1 <= u64_2
            }
            DbType::U128 => {
                let u128_1 = Cursor::new(&self.content).read_u128::<LittleEndian>().unwrap();
                let u128_2 = Cursor::new(&column.content).read_u128::<LittleEndian>().unwrap();
                u128_1 <= u128_2
            }
            DbType::F32 => {
                let f32_1 = Cursor::new(&self.content).read_f32::<LittleEndian>().unwrap();
                let f32_2 = Cursor::new(&column.content).read_f32::<LittleEndian>().unwrap();
                f32_1 <= f32_2
            }
            DbType::F64 => {
                let f64_1 = Cursor::new(&self.content).read_f64::<LittleEndian>().unwrap();
                let f64_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
                f64_1 <= f64_2
            }
            _ => panic!("Less or equals method cannot be used on columns that are not of numeric types"),
        }
    }

    pub fn add(&mut self, column: &Column) {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap() as i128;
                let i8_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i8_1 + i8_2).to_le_bytes().to_vec();
            }
            DbType::I16 => {
                let i16_1 = Cursor::new(&self.content).read_i16::<LittleEndian>().unwrap() as i128;
                let i16_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i16_1 + i16_2).to_le_bytes().to_vec();
            }
            DbType::I32 => {
                let i32_1 = Cursor::new(&self.content).read_i32::<LittleEndian>().unwrap() as i128;
                let i32_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i32_1 + i32_2).to_le_bytes().to_vec();
            }
            DbType::I64 => {
                let i64_1 = Cursor::new(&self.content).read_i64::<LittleEndian>().unwrap() as i128;
                let i64_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i64_1 + i64_2).to_le_bytes().to_vec();
            }
            DbType::I128 => {
                let i128_1 = Cursor::new(&self.content).read_i128::<LittleEndian>().unwrap();
                let i128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i128_1 + i128_2).to_le_bytes().to_vec();
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap() as i128;
                let u8_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u8_1 + u8_2).to_le_bytes().to_vec();
            }
            DbType::U16 => {
                let u16_1 = Cursor::new(&self.content).read_u16::<LittleEndian>().unwrap() as i128;
                let u16_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content =  (u16_1 + u16_2).to_le_bytes().to_vec();
            }
            DbType::U32 => {
                let u32_1 = Cursor::new(&self.content).read_u32::<LittleEndian>().unwrap() as i128;
                let u32_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u32_1 + u32_2).to_le_bytes().to_vec();
            }
            DbType::U64 => {
                let u64_1 = Cursor::new(&self.content).read_u64::<LittleEndian>().unwrap() as i128;
                let u64_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u64_1 + u64_2).to_le_bytes().to_vec();
            }
            DbType::U128 => {
                let u128_1 = Cursor::new(&self.content).read_u128::<LittleEndian>().unwrap() as i128;
                let u128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u128_1 + u128_2).to_le_bytes().to_vec();
            }
            DbType::F32 => {
                let f32_1 = Cursor::new(&self.content).read_f32::<LittleEndian>().unwrap() as f64;
                let f32_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
                self.content = (f32_1 + f32_2).to_le_bytes().to_vec();
            }
            DbType::F64 => {
                let f64_1 = Cursor::new(&self.content).read_f64::<LittleEndian>().unwrap() as f64;
                let f64_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
                self.content = (f64_1 + f64_2).to_le_bytes().to_vec();
            }
            DbType::STRING => {
                let str_1 = String::from_utf8(self.content.clone()).unwrap();
                let str_2 = String::from_utf8(column.content.clone()).unwrap();
                let concatenated = format!("{}{}", str_1, str_2);
                self.content = concatenated.into_bytes();
            }
            _ => panic!("Add operation is not supported for this data type"),
        }
    }   

    pub fn subtract(&mut self, column: &Column) {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap() as i128;
                let i8_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i8_1 - i8_2).to_le_bytes().to_vec();
            }
            DbType::I16 => {
                let i16_1 = Cursor::new(&self.content).read_i16::<LittleEndian>().unwrap() as i128;
                let i16_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i16_1 - i16_2).to_le_bytes().to_vec();
            }
            DbType::I32 => {
                let i32_1 = Cursor::new(&self.content).read_i32::<LittleEndian>().unwrap() as i128;
                let i32_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i32_1 - i32_2).to_le_bytes().to_vec();
            }
            DbType::I64 => {
                let i64_1 = Cursor::new(&self.content).read_i64::<LittleEndian>().unwrap() as i128;
                let i64_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i64_1 - i64_2).to_le_bytes().to_vec();
            }
            DbType::I128 => {
                let i128_1 = Cursor::new(&self.content).read_i128::<LittleEndian>().unwrap();
                let i128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i128_1 - i128_2).to_le_bytes().to_vec();
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap() as i128;
                let u8_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u8_1 - u8_2).to_le_bytes().to_vec();
            }
            DbType::U16 => {
                let u16_1 = Cursor::new(&self.content).read_u16::<LittleEndian>().unwrap() as i128;
                let u16_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content =  (u16_1 - u16_2).to_le_bytes().to_vec();
            }
            DbType::U32 => {
                let u32_1 = Cursor::new(&self.content).read_u32::<LittleEndian>().unwrap() as i128;
                let u32_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u32_1 - u32_2).to_le_bytes().to_vec();
            }
            DbType::U64 => {
                let u64_1 = Cursor::new(&self.content).read_u64::<LittleEndian>().unwrap() as i128;
                let u64_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u64_1 - u64_2).to_le_bytes().to_vec();
            }
            DbType::U128 => {
                let u128_1 = Cursor::new(&self.content).read_u128::<LittleEndian>().unwrap() as i128;
                let u128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u128_1 - u128_2).to_le_bytes().to_vec();
            }
            DbType::F32 => {
                let f32_1 = Cursor::new(&self.content).read_f32::<LittleEndian>().unwrap() as f64;
                let f32_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
                self.content = (f32_1 - f32_2).to_le_bytes().to_vec();
            }
            DbType::F64 => {
                let f64_1 = Cursor::new(&self.content).read_f64::<LittleEndian>().unwrap() as f64;
                let f64_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
                self.content = (f64_1 - f64_2).to_le_bytes().to_vec();
            }
            _ => panic!("Add operation is not supported for this data type"),
        }
    }

    pub fn multiply(&mut self, column: &Column) {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap() as i128;
                let i8_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i8_1 * i8_2).to_le_bytes().to_vec();
            }
            DbType::I16 => {
                let i16_1 = Cursor::new(&self.content).read_i16::<LittleEndian>().unwrap() as i128;
                let i16_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i16_1 * i16_2).to_le_bytes().to_vec();
            }
            DbType::I32 => {
                let i32_1 = Cursor::new(&self.content).read_i32::<LittleEndian>().unwrap() as i128;
                let i32_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i32_1 * i32_2).to_le_bytes().to_vec();
            }
            DbType::I64 => {
                let i64_1 = Cursor::new(&self.content).read_i64::<LittleEndian>().unwrap() as i128;
                let i64_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i64_1 * i64_2).to_le_bytes().to_vec();
            }
            DbType::I128 => {
                let i128_1 = Cursor::new(&self.content).read_i128::<LittleEndian>().unwrap();
                let i128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i128_1 * i128_2).to_le_bytes().to_vec();
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap() as i128;
                let u8_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u8_1 * u8_2).to_le_bytes().to_vec();
            }
            DbType::U16 => {
                let u16_1 = Cursor::new(&self.content).read_u16::<LittleEndian>().unwrap() as i128;
                let u16_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content =  (u16_1 * u16_2).to_le_bytes().to_vec();
            }
            DbType::U32 => {
                let u32_1 = Cursor::new(&self.content).read_u32::<LittleEndian>().unwrap() as i128;
                let u32_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u32_1 * u32_2).to_le_bytes().to_vec();
            }
            DbType::U64 => {
                let u64_1 = Cursor::new(&self.content).read_u64::<LittleEndian>().unwrap() as i128;
                let u64_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u64_1 * u64_2).to_le_bytes().to_vec();
            }
            DbType::U128 => {
                let u128_1 = Cursor::new(&self.content).read_u128::<LittleEndian>().unwrap() as i128;
                let u128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u128_1 * u128_2).to_le_bytes().to_vec();
            }
            DbType::F32 => {
                let f32_1 = Cursor::new(&self.content).read_f32::<LittleEndian>().unwrap() as f64;
                let f32_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
                self.content = (f32_1 * f32_2).to_le_bytes().to_vec();
            }
            DbType::F64 => {
                let f64_1 = Cursor::new(&self.content).read_f64::<LittleEndian>().unwrap() as f64;
                let f64_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
                self.content = (f64_1 * f64_2).to_le_bytes().to_vec();
            }
            _ => panic!("Add operation is not supported for this data type"),
        }
    }

    pub fn divide(&mut self, column: &Column) {
        match self.data_type {
            DbType::I8 => {
                let i8_1 = self.content.as_slice().read_i8().unwrap() as i128;
                let i8_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i8_1 / i8_2).to_le_bytes().to_vec();
            }
            DbType::I16 => {
                let i16_1 = Cursor::new(&self.content).read_i16::<LittleEndian>().unwrap() as i128;
                let i16_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i16_1 / i16_2).to_le_bytes().to_vec();
            }
            DbType::I32 => {
                let i32_1 = Cursor::new(&self.content).read_i32::<LittleEndian>().unwrap() as i128;
                let i32_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i32_1 / i32_2).to_le_bytes().to_vec();
            }
            DbType::I64 => {
                let i64_1 = Cursor::new(&self.content).read_i64::<LittleEndian>().unwrap() as i128;
                let i64_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i64_1 / i64_2).to_le_bytes().to_vec();
            }
            DbType::I128 => {
                let i128_1 = Cursor::new(&self.content).read_i128::<LittleEndian>().unwrap();
                let i128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (i128_1 / i128_2).to_le_bytes().to_vec();
            }
            DbType::U8 => {
                let u8_1 = self.content.as_slice().read_u8().unwrap() as i128;
                let u8_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u8_1 / u8_2).to_le_bytes().to_vec();
            }
            DbType::U16 => {
                let u16_1 = Cursor::new(&self.content).read_u16::<LittleEndian>().unwrap() as i128;
                let u16_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content =  (u16_1 / u16_2).to_le_bytes().to_vec();
            }
            DbType::U32 => {
                let u32_1 = Cursor::new(&self.content).read_u32::<LittleEndian>().unwrap() as i128;
                let u32_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u32_1 / u32_2).to_le_bytes().to_vec();
            }
            DbType::U64 => {
                let u64_1 = Cursor::new(&self.content).read_u64::<LittleEndian>().unwrap() as i128;
                let u64_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u64_1 / u64_2).to_le_bytes().to_vec();
            }
            DbType::U128 => {
                let u128_1 = Cursor::new(&self.content).read_u128::<LittleEndian>().unwrap() as i128;
                let u128_2 = Cursor::new(&column.content).read_i128::<LittleEndian>().unwrap();
                self.content = (u128_1 / u128_2).to_le_bytes().to_vec();
            }
            DbType::F32 => {
                let f32_1 = Cursor::new(&self.content).read_f32::<LittleEndian>().unwrap() as f64;
                let f32_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
                self.content = (f32_1 / f32_2).to_le_bytes().to_vec();
            }
            DbType::F64 => {
                let f64_1 = Cursor::new(&self.content).read_f64::<LittleEndian>().unwrap() as f64;
                let f64_2 = Cursor::new(&column.content).read_f64::<LittleEndian>().unwrap();
                self.content = (f64_1 / f64_2).to_le_bytes().to_vec();
            }
            _ => panic!("Add operation is not supported for this data type"),
        }
    }   
}

#[derive(Debug, PartialEq)]
pub enum DowngradeType {
    I128,
    F64
}