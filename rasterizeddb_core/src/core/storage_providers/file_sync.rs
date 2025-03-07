use std::{
    fs::{self, remove_file},
    io::{Cursor, Read, Seek, SeekFrom},
    path::Path, sync::Arc
};

use tokio::{io::{AsyncSeekExt, AsyncWriteExt}, sync::RwLock, task::yield_now};

use super::traits::IOOperationsSync;

pub struct LocalStorageProvider {
    pub(super) append_file: Arc<RwLock<tokio::fs::File>>,
    pub(super) write_file: Arc<RwLock<tokio::fs::File>>,
    pub(crate) location: String,
    pub(crate) table_name: String,
    pub(crate) file_str: String
}

unsafe impl Sync for LocalStorageProvider { }
unsafe impl Send for LocalStorageProvider { }

impl Clone for LocalStorageProvider {
    fn clone(&self) -> Self {
        Self {
            append_file: self.append_file.clone(),
            write_file: self.write_file.clone(),
            location: self.location.clone(),
            table_name: self.table_name.clone(),
            file_str: self.file_str.clone(),
        }
    }
}

impl LocalStorageProvider {
    pub async fn new(location: &str, table_name: Option<&str>) -> LocalStorageProvider {
        if let Some(table_name) = table_name {
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

            let file_append = tokio::fs::File::options()
                .read(true)
                .append(true)
                .open(&file_str)
                .await
                .unwrap();

            let file_write = tokio::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .await
                .unwrap();

            LocalStorageProvider {
                append_file: Arc::new(RwLock::new(file_append)),
                write_file: Arc::new(RwLock::new(file_write)),
                location: location.to_string(),
                table_name: table_name.to_string(),
                file_str: file_str,
            }
        } else {
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
    
            let file_str = format!("{}{}{}", location, delimiter, "CONFIG_TABLE.db");
    
            _ = std::fs::File::create(&file_str).unwrap();
    
            let file_append = tokio::fs::File::options()
                .read(true)
                .append(true)
                .open(&file_str)
                .await
                .unwrap();
    
            let file_write = tokio::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .await
                .unwrap();
    
            LocalStorageProvider {
                append_file: Arc::new(RwLock::new(file_append)),
                write_file: Arc::new(RwLock::new(file_write)),
                location: location.to_string(),
                table_name: "temp.db".to_string(),
                file_str: file_str,
            }
        }
 
    }

    pub async fn close_files(&mut self) {
        let file_read = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();

        let file_append = std::fs::File::options()
            .read(true)
            .append(true)
            .open(&self.file_str)
            .unwrap();

        let file_write = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&self.file_str)
            .unwrap();

        _ = file_read.sync_all();
        _ = file_append.sync_all();
        _ = file_write.sync_all();
    }
}

impl IOOperationsSync for LocalStorageProvider {
    async fn write_data_unsync(&mut self, position: u64, buffer: &[u8]) {
        let mut file = self.write_file.write().await;
        file.seek(SeekFrom::Start(position)).await.unwrap();
        file.write_all(buffer).await.unwrap();
    }

    async fn verify_data(&mut self, position: u64, buffer: &[u8]) -> bool {
        yield_now().await;

        let mut file_read = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();

        file_read.seek(SeekFrom::Start(position)).unwrap();
        let mut file_buffer = vec![0; buffer.len() as usize];
        file_read.read_exact(&mut file_buffer).unwrap();
        buffer.eq(&file_buffer)
    }

    async fn write_data(&mut self, position: u64, buffer: &[u8]) {
        let mut file = self.write_file.write().await;
        file.seek(SeekFrom::Start(position)).await.unwrap();
        file.write_all(buffer).await.unwrap();
        file.flush().await.unwrap();
        file.sync_all().await.unwrap();
    }

    async fn append_data(&mut self, buffer: &[u8]) {
        let mut file = self.append_file.write().await;
        file.write_all(&buffer).await.unwrap();
        file.flush().await.unwrap();
        file.sync_all().await.unwrap();
    }

    async fn read_data(&self, position: &mut u64, length: u32) -> Vec<u8> {
        yield_now().await;

        let mut read_file = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();

        read_file.seek(SeekFrom::Start(*position)).unwrap();
        let mut buffer: Vec<u8> = vec![0; length as usize];
        let read_result = read_file.read_exact(&mut buffer);
        if read_result.is_err() {
            return Vec::default();
        } else {
            *position += length as u64;
            return buffer;
        }
    }

    async fn read_data_to_end(&self, position: u64) -> Vec<u8> {
        yield_now().await;
        
        let mut read_file = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();

        read_file.seek(SeekFrom::Start(position)).unwrap();
        let mut buffer: Vec<u8> = Vec::default();
        read_file.read_to_end(&mut buffer).unwrap();
        return buffer;
    }

    async fn get_len(&mut self) -> u64 {
        yield_now().await;

        let read_file = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();
        
        let len = read_file.metadata().unwrap().len();
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

    async fn read_data_into_buffer(&self, position: &mut u64, buffer: &mut [u8]) {
        yield_now().await;
        
        let mut read_file = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();

        read_file.seek(SeekFrom::Start(*position)).unwrap();
        read_file.read(buffer).unwrap();
        *position += buffer.len() as u64;
    }

    async fn read_data_to_cursor(&self, position: &mut u64, length: u32) -> Cursor<Vec<u8>> {
        yield_now().await;
        
        let mut read_file = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();

        read_file.seek(SeekFrom::Start(*position)).unwrap();
        let mut buffer: Vec<u8> = vec![0; length as usize];
        let result = read_file.read_exact(&mut buffer);
        if result.is_err() {
            Cursor::new(Vec::default())
        } else {
            *position += length as u64;
            let mut cursor = Cursor::new(buffer);
            cursor.set_position(0);
            return cursor;
        }
    }

    async fn write_data_seek(&mut self, seek: SeekFrom, buffer: &[u8]) {
        let mut file = self.write_file.write().await;
        file.seek(seek).await.unwrap();
        file.write_all(buffer).await.unwrap();
        file.flush().await.unwrap();
        file.sync_all().await.unwrap();
    }

    async fn verify_data_and_sync(&mut self, position: u64, buffer: &[u8]) -> bool {
        yield_now().await;
        
        let mut file = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&self.file_str)
            .unwrap();

        file.seek(SeekFrom::Start(position)).unwrap();
        let mut file_buffer = vec![0; buffer.len() as usize];
        file.read_exact(&mut file_buffer).unwrap();
        if buffer.eq(&file_buffer) {
            file.sync_data().unwrap();
            true
        } else {
            false
        }
    }

    async fn append_data_unsync(&mut self, buffer: &[u8]) {
        let mut file = self.append_file.write().await;
        file.write_all(&buffer).await.unwrap();
    }

    async fn create_temp(&self) -> Self {
        yield_now().await;
        
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let location_path = Path::new(&self.location);

        if !location_path.exists() {
            _ = std::fs::create_dir(location_path);
        }

        let file_str = format!("{}{}{}", self.location, delimiter, "temp.db");

        _ = std::fs::File::create(&file_str).unwrap();

        let file_append = tokio::fs::File::options()
            .read(true)
            .append(true)
            .open(&file_str)
            .await
            .unwrap();

        let file_write = tokio::fs::File::options()
            .read(true)
            .write(true)
            .open(&file_str)
            .await
            .unwrap();

        Self {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            location: self.location.to_string(),
            table_name: "temp.db".to_string(),
            file_str: file_str,
        }
    }

    async fn swap_temp(&mut self, _temp_io_sync: &mut Self) {
        yield_now().await;
        
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let temp_file_str = format!("{}{}{}", self.location, delimiter, "temp.db");
        let actual_file_str = self.file_str.clone();

        // Rename the temp file to the actual file name
        _ = fs::remove_file(&actual_file_str);
        _ = fs::rename(&temp_file_str, &actual_file_str);

        self.close_files().await;
    }

    fn get_location(&self) -> Option<String> {
        Some(self.location.clone())
    }

    #[allow(refining_impl_trait)]
    async fn create_new(&self, name: String) -> Self {
        let location_path = Path::new(& self.location);

        if !location_path.exists() {
            _ = std::fs::create_dir(location_path);
        }

        let new_table = format!("{}{}", self.location, name);
        let file_path = Path::new(&new_table);

        if !file_path.exists() && !file_path.is_file() {
            _ = std::fs::File::create(&file_path).unwrap();
        }
        
        let file_append = tokio::fs::File::options()
            .read(true)
            .append(true)
            .open(&file_path)
            .await
            .unwrap();

        let file_write = tokio::fs::File::options()
            .read(true)
            .write(true)
            .open(&file_path)
            .await
            .unwrap();

        LocalStorageProvider {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            location: self.location.to_string(),
            table_name: name.to_string(),
            file_str: new_table.clone()
        }
    }

    fn drop_io(&mut self) {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let file_str = format!("{}{}{}", self.location, delimiter, self.table_name);

        let path = Path::new(&file_str);

        remove_file(&path).unwrap();
    }
}
