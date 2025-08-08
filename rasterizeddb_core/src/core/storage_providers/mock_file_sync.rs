use std::{
    fs::{self, remove_file},
    io::{Cursor, Read, Seek, SeekFrom},
    path::{Path, PathBuf}, sync::Arc
};

use crossbeam_queue::SegQueue;
use futures::future::join_all;
use memmap2::{Mmap, MmapOptions};
use temp_testdir::TempDir;
use tokio::{io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt}, sync::RwLock, task::yield_now};

use crate::memory_pool::MemoryBlock;

use super::{traits::StorageIO, CRC};

pub struct MockStorageProvider {
    pub(super) append_file: Arc<RwLock<tokio::fs::File>>,
    pub(super) write_file: Arc<RwLock<tokio::fs::File>>,
    pub(crate) location: String,
    pub(crate) table_name: String,
    pub(crate) file_str: String,
    _temp_dir_hold: TempDir,
    pub(crate) hash: u32,
    appender: SegQueue<MemoryBlock>,
    pub(crate) _memory_map: Mmap,
}

unsafe impl Sync for MockStorageProvider { }
unsafe impl Send for MockStorageProvider { }

impl Clone for MockStorageProvider {
    fn clone(&self) -> Self {
        Self {
            append_file: self.append_file.clone(),
            write_file: self.write_file.clone(),
            location: self.location.clone(),
            table_name: self.table_name.clone(),
            file_str: self.file_str.clone(),
            _temp_dir_hold: TempDir::default(),
            hash: CRC.checksum(format!("{}+++{}", self.location, "temp.db").as_bytes()),
            appender: SegQueue::new(),
            _memory_map: unsafe { MmapOptions::new()
                .map(&std::fs::File::open(&self.file_str).unwrap())
                .unwrap() },
        }
    }
}

impl MockStorageProvider {
    pub async fn new() -> MockStorageProvider {
        let temp = TempDir::default();
        let file_path = PathBuf::from(temp.as_ref().to_str().unwrap());
       
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let location_str = format!("{}", file_path.to_str().unwrap());

        let location_path = Path::new(&location_str);

        _ = std::fs::create_dir(location_path);
        
        let file_str = format!("{}{}{}", location_str, delimiter, "TEMP.db");

        let file_append = tokio::fs::File::options()
            .create(true)
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

        let file_read = std::fs::File::options()
            .read(true)
            .open(&file_str)
            .unwrap();

        MockStorageProvider {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            location: location_str.to_string(),
            table_name: "TEMP.db".to_string(),
            file_str: file_str,
            _temp_dir_hold: temp,
            hash: CRC.checksum(format!("{}+++{}", file_path.to_str().unwrap(), "TEMP.db").as_bytes()),
            appender: SegQueue::new(),
            _memory_map: unsafe { MmapOptions::new()
                    .map(&file_read)
                    .unwrap() },
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

    pub async fn start_append_data_service(&mut self) {
        loop {
            if self.appender.len() > 0 {
                let mut buffer: Vec<u8> = Vec::default();

                while let Some(block) = self.appender.pop() {
                    buffer.extend_from_slice(block.into_slice());
    
                }

                let mut file = self.append_file.write().await;
                file.write_all(&buffer).await.unwrap();
                file.flush().await.unwrap();
                file.sync_all().await.unwrap();
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }
    }
}

impl StorageIO for MockStorageProvider {
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

    async fn append_data(&mut self, buffer: &[u8], _immediate: bool) {
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
        let mut read_file = tokio::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .await
            .unwrap();

        read_file.seek(SeekFrom::Start(position)).await.unwrap();
        let mut buffer: Vec<u8> = Vec::default();
        read_file.read_to_end(&mut buffer).await.unwrap();
        return buffer;
    }

    async fn get_len(&mut self) -> u64 {
        let read_file = tokio::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .await
            .unwrap();
        
        let len = read_file.metadata().await.unwrap().len();
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

    #[allow(unreachable_code)]
    async fn read_data_into_buffer(&self, position: &mut u64, buffer: &mut [u8]) -> Result<(), std::io::Error> {
        // use crate::{core::row_v2::row_pointer::RowPointer, memory_pool::{MemoryBlock, MEMORY_POOL}};
        // fn get_range(position: &u64, max: u64) -> (u64,u64) {
        //     const BLOCK_SIZE: u64 = 1024 * 1024 * 16; // 16MB
        //     let chunk = position / BLOCK_SIZE;
        //     let start = chunk * BLOCK_SIZE;
        //     let mut end = (chunk + 1) * BLOCK_SIZE;

        //     if end > max {
        //         end = max;
        //     }

        //     (start, end)
        // }

        // let range = get_range(position, self.file_len.load(std::sync::atomic::Ordering::Relaxed));

        // if let Some(block) = CACHE_MAP.pin().get(&(range.1, self.hash)) {
        //     let slice = block.into_slice();
        //     let new_position = *position - range.0;
        //     let buffer_len = buffer.len();

        //     if slice.len() > new_position as usize + buffer_len {
        //         // Copy the available data from the cached block
        //         buffer.copy_from_slice(&slice[new_position as usize..new_position as usize + buffer_len]);
        //         *position += buffer_len as u64;
        //         return;
        //     } else {
        //         // This is the case where we need to handle partial read from cache and rest from memory map
        //         let available_len = slice.len() - new_position as usize; // How much we can read from cache
                
        //         // Copy the available part from cache
        //         buffer[..available_len].copy_from_slice(&slice[new_position as usize..]);
                
        //         // Calculate the remaining part position and size
        //         let remaining_size = buffer_len - available_len;
        //         let remaining_position = range.0 + new_position as u64 + available_len as u64;
                
        //         // Copy the remaining part from memory map
        //         buffer[available_len..].copy_from_slice(
        //             &self.memory_map[remaining_position as usize..remaining_position as usize + remaining_size]
        //         );
                
        //         *position += buffer_len as u64;
        //         return;
        //     }
        // } else {
        //     let range = (range.0, range.1, RowPointer::get_timestamp_seconds());
        //     let block = MEMORY_POOL.acquire((range.1 - range.0) as usize);
        //     let slice = block.into_slice_mut();

        //     slice.copy_from_slice(&self.memory_map[range.0 as usize..range.1 as usize]);

        //     CACHE_MAP.pin_owned().insert((range.1, self.hash), block);

        //     let buffer_len = buffer.len() as u64;

        //     *position += buffer_len;

        //     buffer.copy_from_slice(&slice[(*position - range.0) as usize..*position as usize - range.0 as usize + buffer_len as usize]);
        // }

        // return;
    
        let buffer_len = buffer.len();
        //let table_len = self.file_len.load(std::sync::atomic::Ordering::Relaxed);

        let mut read_file = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();

        read_file.seek(SeekFrom::Start(*position)).unwrap();
        read_file.read_exact(buffer).unwrap();

        *position += buffer_len as u64;

        Ok(())
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

        let file_str = format!("{}{}{}", self.location, delimiter, "TEMP_2");

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

        let file_read = std::fs::File::options()
            .read(true)
            .open(&file_str)
            .unwrap();

        Self {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            location: self.location.to_string(),
            table_name: "TEMP_2.db".to_string(),
            file_str: file_str,
            _temp_dir_hold: TempDir::default(),
            hash: CRC.checksum(format!("{}+++{}", self.location, "TEMP_2.db").as_bytes()),
            appender: SegQueue::new(),
            _memory_map: unsafe { MmapOptions::new()
                .map(&file_read)
                .unwrap() },
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
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let location_path = Path::new(& self.location);

        if !location_path.exists() {
            _ = std::fs::create_dir(location_path);
        }

        let new_table = format!("{}{}{}", self.location, delimiter, name);
        let file_path = Path::new(&new_table);

        if !file_path.exists() {
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

        let file_read = std::fs::File::options()
            .read(true)
            .open(&file_path)
            .unwrap();

        MockStorageProvider {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            location: self.location.to_string(),
            table_name: name.to_string(),
            file_str: new_table.clone(),
            _temp_dir_hold: TempDir::default(),
            hash: CRC.checksum(format!("{}+++{}", file_path.to_str().unwrap(), name).as_bytes()),
            appender: SegQueue::new(),
            _memory_map: unsafe { MmapOptions::new()
                .map(&file_read)
                .unwrap() },
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
    
    fn get_hash(&self) -> u32 {
        self.hash
    }

    async fn start_service(&mut self) {
        let services = vec![
            self.start_append_data_service()
        ];

        join_all(services).await;
    } 

    fn get_name(&self) -> String {
        self.table_name.clone()
    }
}

impl Drop for MockStorageProvider {
    fn drop(&mut self) {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let file_str = format!("{}{}", self.location, delimiter);

        let path = Path::new(&file_str);

        if path.exists() && path.is_dir() {
            _ = fs::remove_dir_all(path);
        }
    }
}