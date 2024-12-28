use std::{fs, io::{Cursor, Read, Seek, SeekFrom, Write}, os::windows::fs::FileExt, path::Path};

use super::traits::IOOperationsSync;

pub struct LocalStorageProvider {
    pub(super) read_file: std::fs::File,
    pub(super) append_file: std::fs::File,
    pub(super) write_file: std::fs::File,
    pub(crate) location: String,
    pub(crate) table_name: String
}

impl Clone for LocalStorageProvider {
    fn clone(&self) -> Self {
        Self { 
            read_file: self.read_file.try_clone().unwrap(), 
            append_file: self.append_file.try_clone().unwrap(), 
            write_file: self.write_file.try_clone().unwrap(),
            location: self.location.clone(),
            table_name: self.table_name.clone() }
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

        let location_str = format!("{}", location);

        let location_path = Path::new(&location_str);

        if !location_path.exists() {
            _ = std::fs::create_dir(location_path);
        }

        let file_str = format!("{}{}{}", location, delimiter, table_name);
        
        let file_path = Path::new(&file_str);

        if !file_path.exists() && !file_path.is_file() {
            _ = std::fs::File::create(&file_str).unwrap();
        }

        let file_read = std::fs::File::options().read(true).open(&file_str).unwrap();
        let file_append = std::fs::File::options().read(true).append(true).open(&file_str).unwrap();
        let file_write = std::fs::File::options().read(true).write(true).open(&file_str).unwrap();

        LocalStorageProvider {
            read_file: file_read,
            append_file: file_append,
            write_file: file_write,
            location: location.to_string(),
            table_name: table_name.to_string()
        }
    }

    pub fn close_files(&mut self) {
        _ = self.read_file.sync_all();
        _ = self.append_file.sync_all();
        _ = self.write_file.sync_all();
    }
}


impl IOOperationsSync for LocalStorageProvider {  
    fn write_data_unsync(&mut self,
        position: u64, 
        buffer: &[u8]) {
        let mut file = &self.write_file;
        file.seek(SeekFrom::Start(position)).unwrap();
        file.write_all(buffer).unwrap();
    }
    
    fn verify_data(&mut self,
        position: u64, 
        buffer: &[u8]) -> bool {
        let mut file = &self.write_file;
        file.seek(SeekFrom::Start(position)).unwrap();
        let mut file_buffer = vec![0; buffer.len() as usize];
        file.read_exact(&mut file_buffer).unwrap();
        buffer.eq(&file_buffer)
    }

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
        let read_result = file.read_exact(&mut buffer);
        if read_result.is_err() {
            return Vec::default()
        } else {
            *position += length as u64;
            return buffer;
        }
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
    
    fn verify_data_and_sync(&mut self,
        position: u64, 
        buffer: &[u8]) -> bool {
        let mut file = &self.write_file;
        file.seek(SeekFrom::Start(position)).unwrap();
        let mut file_buffer = vec![0; buffer.len() as usize];
        file.read_exact(&mut file_buffer).unwrap();
        if buffer.eq(&file_buffer) {
            self.write_file.sync_data().unwrap();  
            true
        } else {
            false
        }
    }

    fn append_data_unsync(&mut self,  
        buffer: &[u8]) {
        let mut file = &self.append_file;
        file.write_all(&buffer).unwrap();
    }
    
    fn create_temp(&self) -> Self {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let location_str = format!("{}", self.location);

        let location_path = Path::new(&location_str);

        if !location_path.exists() {
            _ = std::fs::create_dir(location_path);
        }

        let file_str = format!("{}{}{}", self.location, delimiter, "temp.db");
        
        _ = std::fs::File::create(&file_str).unwrap();

        let file_read = std::fs::File::options().read(true).open(&file_str).unwrap();
        let file_append = std::fs::File::options().read(true).append(true).open(&file_str).unwrap();
        let file_write = std::fs::File::options().read(true).write(true).open(&file_str).unwrap();

        Self {
            read_file: file_read,
            append_file: file_append,
            write_file: file_write,
            location: self.location.to_string(),
            table_name: "temp.db".to_string()
        }
    }
    
    fn swap_temp(&mut self, temp_io_sync: &mut Self) {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let temp_file_str = format!("{}{}{}", self.location, delimiter, "temp.db");
        let actual_file_str = format!("{}{}{}", self.location, delimiter, self.table_name);

        // Rename the temp file to the actual file name
        _ = fs::remove_file(&actual_file_str);
        _ = fs::rename(&temp_file_str, &actual_file_str);

        self.close_files();

        // Reopen the files with the new actual file name
        let file_read = std::fs::File::options().read(true).open(&actual_file_str).unwrap();
        let file_append = std::fs::File::options().append(true).open(&actual_file_str).unwrap();
        let file_write = std::fs::File::options().write(true).open(&actual_file_str).unwrap();

        self.read_file = file_read;
        self.append_file = file_append;
        self.write_file = file_write;
    }
}