use std::io::{Cursor, SeekFrom};

use orx_concurrent_vec::ConcurrentVec;

use super::traits::IOOperationsSync;

pub struct MemoryStorageProvider {
    vec: ConcurrentVec<u8>
}

impl Clone for MemoryStorageProvider {
    fn clone(&self) -> Self {
        Self { 
            vec: self.vec.clone()
        }
    }
}

impl MemoryStorageProvider {
    pub fn new() -> MemoryStorageProvider {
        MemoryStorageProvider {
            vec: ConcurrentVec::with_doubling_growth()
        }
    }
}

impl IOOperationsSync for MemoryStorageProvider {
    fn write_data(&mut self,  
        position: u64, 
        buffer: &[u8]) {
        let end = position + buffer.len() as u64;

        let mut x = 0;

        let needed_len = buffer.len();
        let current_len = self.vec.len();

        if current_len < needed_len {
            self.vec.extend(vec![0; needed_len as usize]);
        }

        for i in position..end {
            let u8_value = buffer[x];
            self.vec[i as usize].update(|x| *x = u8_value);
            x += 1;
        }
    }

    fn write_data_seek(&mut self,  
        seek: std::io::SeekFrom, 
        buffer: &[u8]) {
        
        let needed_len = buffer.len();
        let current_len = self.vec.len();

        if current_len < needed_len {
            self.vec.extend(vec![0; needed_len as usize]);
        }

        match seek {
            SeekFrom::Current(_index) => {
                panic!("Not supported")
            },
            SeekFrom::End(_index) => panic!("Not supported"),
            SeekFrom::Start(index) => {
                let mut i = 0;
                for u8_value in buffer {
                    self.vec[(index + i) as usize].update(|x| *x = *u8_value);
                    i += 1;
                }
            }
        }
    }

    async fn read_data(&mut self,
        position: &mut u64,  
        length: u32) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::with_capacity(length as usize);
        let start = *position;
        let end = *position + length as u64;

        for i in start..end {
            let u8_value: u8 = self.vec[i as usize].map(|x| *x);
            buffer.push(u8_value);
            *position += 1;
        }
        
        return buffer;
    }

    async fn read_data_into_buffer(&mut self,
        position: &mut u64,  
        buffer: &mut [u8]) {
        let start = *position;
        let end = *position + buffer.len() as u64 + 1;

        let mut x = 0; 

        for i in start..end {
            let u8_value: u8 = self.vec[i as usize].map(|x| *x);
            buffer[x]  = u8_value;
            x += 1;
            *position += 1;
        }
    }

    async fn read_data_to_cursor(&mut self,
        position: &mut u64,  
        length: u32) -> std::io::Cursor<Vec<u8>> {
        let mut buffer: Vec<u8> = Vec::with_capacity(length as usize);
        let start = *position;
        let end = *position + length as u64;

        for i in start..end {
            let u8_value: u8 = self.vec[i as usize].map(|x| *x);
            buffer.push(u8_value);
            *position += 1;
        }
        
        return Cursor::new(buffer);
    }

    async fn read_data_to_end(&mut self,
        position: u64) -> Vec<u8> {
        let total_len = self.vec.len() as u64;
        let mut buffer: Vec<u8> = Vec::with_capacity((total_len - position) as usize);

        for i in position..total_len {
            let u8_value: u8 = self.vec[i as usize].map(|x| *x);
            buffer.push(u8_value);
        }
        
        return buffer;
    }

    fn append_data(&mut self,  
        buffer: &[u8]) {
        for u8_value in buffer {
            self.vec.extend(vec![*u8_value; 1]);
        }
    }

    async fn get_len(&mut self) -> u64 {
        self.vec.len() as u64
    }

    fn exists(location: &str, table_name: &str) -> bool {
        let _location = location;
        let _table_name = table_name;
        true
    }
    
    fn write_data_unsync(
        &mut self,
        position: u64, 
        buffer: &[u8]) {
        let end = position + buffer.len() as u64;

        let mut x = 0;

        let needed_len = buffer.len();
        let current_len = self.vec.len();

        if current_len < needed_len {
            self.vec.extend(vec![0; needed_len as usize]);
        }

        for i in position..end {
            let u8_value = buffer[x];
            self.vec[i as usize].update(|x| *x = u8_value);
            x += 1;
        }
    }
    
    fn verify_data(&mut self,
        position: u64, 
        buffer: &[u8]) -> bool {
        let _position = position;
        let _buffer = buffer;
        true
    }

    fn verify_data_and_sync(&mut self,
        position: u64, 
        buffer: &[u8]) -> bool {
        let _position = position;
        let _buffer = buffer;
        true
    }
    
    fn append_data_unsync(&mut self,  
        buffer: &[u8]) {
        for u8_value in buffer {
            self.vec.extend(vec![*u8_value; 1]);
        }
    }
    
    async fn create_temp(&self) -> Self {
        Self {
            vec: ConcurrentVec::with_doubling_growth()
        }
    }
    
    async fn swap_temp(&mut self, temp_io_sync: &mut Self) {
        self.vec = temp_io_sync.vec.clone();
        self.vec.clear();
        let iter = temp_io_sync.vec.clone_to_vec();
        self.vec.extend(iter);
        temp_io_sync.vec.clear();
    }
    
    fn get_location(&self) -> Option<String> {
        None
    }
    
    #[allow(refining_impl_trait)]
    async fn create_new(&self, _name: String) -> Self {
        MemoryStorageProvider {
            vec: ConcurrentVec::with_doubling_growth()
        }
    }
}