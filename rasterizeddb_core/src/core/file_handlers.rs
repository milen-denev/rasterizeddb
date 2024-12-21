use std::{future::Future, io::{Read, Seek, SeekFrom, Write}, marker::PhantomData, path::Path, sync::Arc};
use tokio::{fs::File, io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt}};

pub async fn save_data(file: &mut File, buffer: &mut Vec<u8>) {
    file.write_all(buffer).await.unwrap();
    file.flush().await.unwrap();
    file.sync_all().await.unwrap();
}

pub(crate) struct LocalStorageProvider {
    pub(crate) read_file: std::fs::File,
    pub(crate) append_file: std::fs::File,
    pub(crate) write_file: std::fs::File,
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

        if path.exists() && path.is_file() {
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
        position: u64,  
        length: u32) -> Vec<u8> {
        let mut file = &self.read_file;
        file.seek(SeekFrom::Start(position)).unwrap();
        let mut buffer: Vec<u8> = vec![0; length as usize];
        file.read_exact(&mut buffer).unwrap();
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
}

pub(crate) struct LocalStorageProviderAsync {
    pub(crate) read_file: tokio::fs::File,
    pub(crate) append_file: tokio::fs::File,
    pub(crate) write_file: tokio::fs::File
}

unsafe impl Send for LocalStorageProviderAsync {}
unsafe impl Sync for LocalStorageProviderAsync {}

impl LocalStorageProviderAsync {
    pub async fn new(location: &str, table_name: &str) -> LocalStorageProviderAsync {
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
            _ = tokio::fs::File::create(&file_str).await.unwrap();
        }

        let file_read = tokio::fs::File::options().read(true).open(&file_str).await.unwrap();
        let file_append = tokio::fs::File::options().read(true).append(true).open(&file_str).await.unwrap();
        let file_write = tokio::fs::File::options().read(true).write(true).open(&file_str).await.unwrap();

        LocalStorageProviderAsync {
            read_file: file_read,
            append_file: file_append,
            write_file: file_write
        }
    }
}

impl<'a> IOOperationsAsync<'a> for LocalStorageProviderAsync {
    async fn write_data(&'a mut self,  
        position: u64, 
        buffer: &[u8]) {
        let file = &mut self.write_file;
        file.seek(SeekFrom::Start(position)).await.unwrap();
        file.write_all(buffer).await.unwrap();
        file.flush().await.unwrap();
        file.sync_all().await.unwrap();
    }

    async fn append_data(&'a mut self,  
        buffer: &'a [u8]) {
        let file = &mut self.append_file;
        file.write_all(&buffer).await.unwrap();
        file.flush().await.unwrap();
        file.sync_all().await.unwrap();
    }

    async fn append_data_own(&'a mut self,  
        buffer: Box<Vec<u8>>) {
        let file = &mut self.append_file;
        file.write_all(&buffer).await.unwrap();
        file.flush().await.unwrap();
        file.sync_all().await.unwrap();
    }

    async fn read_data(&'a mut self,
        position: u64,  
        length: u32) -> Vec<u8> {
        let file = &mut self.read_file;
        file.seek(SeekFrom::Start(position)).await.unwrap();
        let mut buffer: Vec<u8> = vec![0; length as usize];
        file.read_exact(&mut buffer).await.unwrap();
        return buffer;
    }
    
    async fn get_len(&'a mut self) -> u64 {
        let file = &mut self.read_file;
        let len = file.metadata().await.unwrap().len();
        len
    }

    async fn exists(location: &'a str, table_name: &'a str) -> bool {
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
}

pub trait IOOperationsSync {
    fn write_data(&mut self,  
        position: u64, 
        buffer: &[u8]);

    fn read_data(&mut self,
        position: u64,  
        length: u32) -> Vec<u8>;

    fn read_data_to_end(&mut self,
        position: u64) -> Vec<u8>;

    fn append_data(&mut self,  
        buffer: &[u8]);

    fn get_len(&mut self) -> u64;

    fn exists(location: &str, table_name: &str) -> bool;
}

pub trait IOOperationsAsync<'a> {
    fn write_data(&'a mut self,  
        position: u64, 
        buffer: &[u8]) -> impl Future<Output = ()> + Send;

    fn read_data(&'a mut self,
        position: u64,  
        length: u32) -> impl Future<Output = Vec<u8>> + Send;

    fn append_data(&'a mut self,  
        buffer: &'a [u8]) -> impl Future<Output = ()> + Send;

    fn append_data_own(&'a mut self,  
        buffer: Box<Vec<u8>>) -> impl Future<Output = ()> + Send;

    fn get_len(&'a mut self) -> impl Future<Output = u64> + Send;

    fn exists(location: &'a str, table_name: &'a str) -> impl Future<Output = bool> + Send;
}