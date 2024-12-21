use std::{io::{Cursor, Read, Seek, SeekFrom, Write}, os::windows::fs::FileExt, path::Path};

use byteorder::{LittleEndian, ReadBytesExt};

use super::traits::{IOOperationsSync, IOResult};

pub struct LocalStorageProvider {
    pub(super) read_file: std::fs::File,
    pub(super) append_file: std::fs::File,
    pub(super) write_file: std::fs::File,
}

impl Clone for LocalStorageProvider {
    fn clone(&self) -> Self {
        Self { 
            read_file: self.read_file.try_clone().unwrap(), 
            append_file: self.append_file.try_clone().unwrap(), 
            write_file: self.write_file.try_clone().unwrap() }
    }
}

unsafe impl Send for LocalStorageProvider {}
unsafe impl Sync for LocalStorageProvider {}

impl LocalStorageProvider {
    pub fn new(location: &str, table_name: &str) -> LocalStorageProvider {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let file_str = format!("{}{}{}", location, delimiter, table_name);
        
        let path = Path::new(&file_str);

        if !path.exists() && !path.is_file() {
            _ = std::fs::File::create(&file_str).unwrap();
        }

        let file_read = std::fs::File::options().read(true).open(&file_str).unwrap();
        let file_append = std::fs::File::options().read(true).append(true).open(&file_str).unwrap();
        let file_write = std::fs::File::options().read(true).write(true).open(&file_str).unwrap();

        LocalStorageProvider {
            read_file: file_read,
            append_file: file_append,
            write_file: file_write
        }
    }
}


impl IOOperationsSync for LocalStorageProvider {
    fn write_data(&mut self,  
        position: u64, 
        buffer: &[u8]) {
        let mut file = &self.write_file;
        file.seek(SeekFrom::Start(position)).unwrap();
        file.write_all(buffer).unwrap();
        file.flush().unwrap();
        file.sync_all().unwrap();
    }

    fn append_data(&mut self,  
        buffer: &[u8]) {
        let mut file = &self.append_file;
        file.write_all(&buffer).unwrap();
        file.flush().unwrap();
        file.sync_all().unwrap();
    }

    fn read_data(&mut self,
        position: &mut u64,  
        length: u32) -> Vec<u8> {
        let mut file = &self.read_file;
        file.seek(SeekFrom::Start(*position)).unwrap();
        let mut buffer: Vec<u8> = vec![0; length as usize];
        file.read_exact(&mut buffer).unwrap();
        *position += length as u64;
        return buffer;
    }
    
    fn read_data_to_end(&mut self,
        position: u64) -> Vec<u8> {
        let mut file = &self.read_file;
        file.seek(SeekFrom::Start(position)).unwrap();
        let mut buffer: Vec<u8> = Vec::default();
        file.read_to_end(&mut buffer).unwrap();
        return buffer;
    }

    fn get_len(&mut self) -> u64 {
        let file = &self.read_file;
        let len = file.metadata().unwrap().len();
        len
    }
    
    fn exists(location: &str, table_name: &str) -> bool {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let file_str = format!("{}{}{}", location, delimiter, table_name);
        
        let path = Path::new(&file_str);

        if path.exists() && path.is_file() {
            return true;
        } else {
            return false;
        }
    }
    
    fn read_data_into_buffer(&mut self,
        position: &mut u64,
        buffer: &mut [u8]) {
        let mut file = &self.read_file;
        file.seek(SeekFrom::Start(*position)).unwrap();
        file.seek_read(buffer, *position).unwrap();
        *position += buffer.len() as u64;
    }
    
    fn seek(&mut self, seek: SeekFrom) {
        self.read_file.seek(seek).unwrap();
    }
    
    fn read_u8(&mut self) -> IOResult<u8> {
        let num_result = self.read_file.read_u8();
        if let Ok(num) = num_result {
            IOResult::Ok(num)
        } else {
            IOResult::Err(num_result.unwrap_err())
        }
    }
    
    fn read_u32(&mut self) -> IOResult<u32> {
        let num_result = self.read_file.read_u32::<LittleEndian>();
        if let Ok(num) = num_result {
            IOResult::Ok(num)
        } else {
            IOResult::Err(num_result.unwrap_err())
        }
    }
    
    fn read_u64(&mut self) -> IOResult<u64> {
        let num_result = self.read_file.read_u64::<LittleEndian>();
        if let Ok(num) = num_result {
            IOResult::Ok(num)
        } else {
            IOResult::Err(num_result.unwrap_err())
        }
    }
    
    fn read_data_to_cursor(&mut self,
        position: &mut u64,  
        length: u32) -> Cursor<Vec<u8>> {
        let mut file = &self.read_file;
        file.seek(SeekFrom::Start(*position)).unwrap();
        let mut buffer: Vec<u8> = vec![0; length as usize];
        let result = file.read_exact(&mut buffer);
        if result.is_err() {
            Cursor::new(Vec::default())
        } else {
            *position += length as u64;
            let mut cursor = Cursor::new(buffer);
            cursor.set_position(0);
            return cursor;
        }
    }
    
    fn write_data_seek(&mut self,  
        seek: SeekFrom, 
        buffer: &[u8]) {
        let mut file = &self.write_file;
        file.seek(seek).unwrap();
        file.write_all(buffer).unwrap();
        file.flush().unwrap();
        file.sync_all().unwrap();
    }
}