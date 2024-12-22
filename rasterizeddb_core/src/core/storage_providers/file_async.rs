use std::{io::SeekFrom, path::Path};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::traits::{IOOperationsAsync, TryCloneAsync};

pub struct LocalStorageProviderAsync {
    pub(crate) read_file: tokio::fs::File,
    pub(crate) append_file: tokio::fs::File,
    pub(crate) write_file: tokio::fs::File
}

unsafe impl Send for LocalStorageProviderAsync {}
unsafe impl Sync for LocalStorageProviderAsync {}

impl TryCloneAsync for LocalStorageProviderAsync {
    async fn try_clone(&self) -> Self {
        LocalStorageProviderAsync {
            read_file: self.read_file.try_clone().await.unwrap(),
            append_file: self.append_file.try_clone().await.unwrap(),
            write_file: self.write_file.try_clone().await.unwrap()
        }
    }
}

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

        if !path.exists() && !path.is_file() {
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

    async fn write_data_own(&'a mut self,
        position: u64,  
        buffer: Box<Vec<u8>>) {
        let file = &mut self.append_file;
        file.seek(SeekFrom::Start(position)).await.unwrap();
        file.write(&buffer).await.unwrap();
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