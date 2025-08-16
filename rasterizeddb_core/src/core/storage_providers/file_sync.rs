use std::{
    cmp::min, fs::{self, remove_file, OpenOptions}, path::Path, sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc}, usize
};

use std::io::*;

use arc_swap::ArcSwap;
use crossbeam_queue::SegQueue;
use futures::future::join_all;
use tokio::{io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt}, sync::RwLock, task::yield_now};

use memmap2::{Mmap, MmapOptions};

use crate::{memory_pool::{MemoryBlock, MEMORY_POOL}, IMMEDIATE_WRITE, WRITE_BATCH_SIZE, WRITE_SLEEP_DURATION};

use super::{traits::StorageIO, CRC};

#[cfg(unix)]
use super::io_uring_reader::AsyncUringReader;

pub struct LocalStorageProvider {
    pub(super) append_file: Arc<RwLock<tokio::fs::File>>,
    pub(super) write_file: Arc<RwLock<tokio::fs::File>>,
    pub(crate) location: String,
    pub(crate) table_name: String,
    pub(crate) file_str: String,
    pub(crate) file_len: AtomicU64,
    pub(crate) _memory_map: ArcSwap<Mmap>,
    pub(crate) hash: u32,
    _locked: AtomicBool,
    appender: SegQueue<MemoryBlock>,
    #[cfg(unix)]
    io_uring_reader: AsyncUringReader
}

unsafe impl Sync for LocalStorageProvider { }
unsafe impl Send for LocalStorageProvider { }

impl Clone for LocalStorageProvider {
    fn clone(&self) -> Self {
        let map = Arc::new(unsafe { MmapOptions::new()
            .map(&std::fs::File::open(&self.file_str).unwrap())
            .unwrap() });

        Self {
            append_file: self.append_file.clone(),
            write_file: self.write_file.clone(),
            location: self.location.clone(),
            table_name: self.table_name.clone(),
            file_str: self.file_str.clone(),
            file_len: AtomicU64::new(self.file_len.load(std::sync::atomic::Ordering::SeqCst)),
            _memory_map: arc_swap::ArcSwap::new(map),
            hash: CRC.checksum(format!("{}+++{}", self.location, self.table_name).as_bytes()),
            appender: SegQueue::new(),
            _locked: AtomicBool::new(false),
            #[cfg(unix)]
            io_uring_reader: self.io_uring_reader.clone()
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
    
            if !location_path.read_dir().is_err() {
                _ = std::fs::create_dir(location_path);
            }

            let final_location = if location.ends_with(delimiter) {
                location[location.len()-1..].replace(delimiter, "")
            } else {
                format!("{}", location)
            };

            let file_str = format!("{}{}{}", final_location, delimiter, table_name);
    
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

            let file_read_mmap = std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .unwrap();

            let file_len = file_read_mmap.metadata().unwrap().len();

            let map = ArcSwap::new(Arc::new(unsafe { MmapOptions::new()
                .map(&file_read_mmap)
                .unwrap() }));

            LocalStorageProvider {
                append_file: Arc::new(RwLock::new(file_append)),
                write_file: Arc::new(RwLock::new(file_write)),
                location: final_location.to_string(),
                table_name: table_name.to_string(),
                file_str: file_str.clone(),
                file_len: AtomicU64::new(file_len),
                _memory_map: map,
                hash: CRC.checksum(format!("{}+++{}", final_location, table_name).as_bytes()),
                appender: SegQueue::new(),
                _locked: AtomicBool::new(false),
                #[cfg(unix)]
                io_uring_reader: AsyncUringReader::new(&file_str, 1024).unwrap()
            }
        } else {
            let delimiter = if cfg!(unix) {
                "/"
            } else if cfg!(windows) {
                "\\"
            } else {
                panic!("OS not supported");
            };
    
            let location_str = if location.ends_with(delimiter) {
                location[location.len()-1..].replace(delimiter, "")
            } else {
                format!("{}", location)
            };

            let location_path = Path::new(&location_str);
    
            if !location_path.read_dir().is_err() {
                _ = std::fs::create_dir(location_path);
            }
    
            let file_str = format!("{}{}{}", location_str, delimiter, "CONFIG_TABLE.db");
    
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
    
            let file_read_mmap = std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .unwrap();

            let file_len = file_read_mmap.metadata().unwrap().len();

            let map = ArcSwap::new(Arc::new(unsafe { MmapOptions::new()
                .map(&file_read_mmap)
                .unwrap() }));

            LocalStorageProvider {
                append_file: Arc::new(RwLock::new(file_append)),
                write_file: Arc::new(RwLock::new(file_write)),
                location: location_str.to_string(),
                table_name: "temp.db".to_string(),
                file_str: file_str.clone(),
                file_len: AtomicU64::new(file_len),
                _memory_map: map,
                hash: CRC.checksum(format!("{}+++{}", location_str, "CONFIG_TABLE.db").as_bytes()),
                appender: SegQueue::new(),
                _locked: AtomicBool::new(false),
                #[cfg(unix)]
                io_uring_reader: AsyncUringReader::new(&file_str, 1024).unwrap()
            }
        }
 
    }

    pub async fn close_files(&self) {
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

    pub async fn start_append_data_service(&self) {
        if IMMEDIATE_WRITE {
            return;
        }
        
        let mut idle_count = 0;
        let mut buffer: Vec<u8> = Vec::with_capacity(WRITE_BATCH_SIZE);
        
        loop {
            let mut total_size = 0;
            let mut has_data = false;
            
            // Collect data in batches
            while let Some(block) = self.appender.pop() {
                has_data = true;
                let slice = block.into_slice();
                buffer.extend_from_slice(slice);
                total_size += slice.len();
                
                // Break if we've reached our batch size limit
                if total_size >= WRITE_BATCH_SIZE {
                    break;
                }
            }
            
            if has_data {
                idle_count = 0;
                self.write_batch_data(&mut buffer, total_size).await;
            } else {
                idle_count += 1;
                
                // Adaptive sleep - sleep longer when idle for extended periods
                let sleep_duration = if idle_count < 100 {
                    WRITE_SLEEP_DURATION
                } else if idle_count < 1000 {
                    tokio::time::Duration::from_millis(50)
                } else {
                    tokio::time::Duration::from_millis(100)
                };
                
                tokio::time::sleep(sleep_duration).await;
            }
        }
    }

    async fn write_batch_data(&self, buffer: &mut Vec<u8>, total_size: usize) {
        if total_size == 0 {
            return;
        }

        // Write the data
        {
            let mut file = self.append_file.write().await;
            
            file.write_all(buffer).await.unwrap();
            file.flush().await.unwrap();
        } // file handle is dropped here
        
        // Update file length atomically
        self.file_len.fetch_add(total_size as u64, std::sync::atomic::Ordering::Release);
        
        // Update memory map after writing
        self.update_memory_map().await;

        buffer.clear(); // Clear the buffer for the next batch
    }

    async fn update_memory_map(&self) {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_str) 
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Failed to open file for memory mapping: {}", e);
                return;
            }
        };

        loop {
            if self._locked.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                continue;
            } else {
                break;
            }
        }

        // Create new memory map with the current file size
        self._memory_map.store(Arc::new(unsafe {
            MmapOptions::new()
                .map(&file)
                .unwrap()
        }));

        self._locked.store(false, Ordering::Release);
    }

    fn read_from_memory_map_or_file(
        &self, 
        position: u64, 
        buffer: &mut [u8]
    ) -> Result<()> {
        let memory_map = self._memory_map.load();
        let buffer_len = buffer.len();
        
        if memory_map.len() >= position as usize + buffer_len {
            // Read from memory map
            let start_idx = position as usize;
            let end_idx = start_idx + buffer_len;
            
            // Bounds check
            if end_idx <= memory_map.len() {
                buffer.copy_from_slice(&memory_map[start_idx..end_idx]);
                
                return Ok(());
            }
        }
        
        // Fallback to file reading
        self.read_from_file_direct(position, buffer)
    }

    fn read_from_file_direct(
        &self, 
        position: u64, 
        buffer: &mut [u8]
    ) -> Result<()> {
        let mut read_file = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, 
                format!("Failed to open file: {}", e)))?;

        read_file.seek(SeekFrom::Start(position))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, 
                format!("Failed to seek to position {}: {}", position, e)))?;
        
        read_file.read_exact(buffer)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, 
                format!("Failed to read {} bytes at position {}: {}", buffer.len(), position, e)))?;

        Ok(())
    }

    #[allow(dead_code)]
    // This function is used to calculate the range for a given position and size.
    fn range_for_pos(&self, pos: u64, size: u64, max_size: u64) -> (u64, u64) {
        let start = (pos / size) * size;
        let mut end = start + size;
        if end > max_size {
            end = max_size;
        }
        (start, end)
    }
}

impl StorageIO for LocalStorageProvider {
    async fn write_data_unsync(&self, position: u64, buffer: &[u8]) {
        let mut file = self.write_file.write().await;
        file.seek(SeekFrom::Start(position)).await.unwrap();
        file.write_all(buffer).await.unwrap();
    }

    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        let mut file_read = tokio::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .await
            .unwrap();

        file_read.seek(SeekFrom::Start(position)).await.unwrap();
        let mut file_buffer = vec![0; buffer.len() as usize];
        file_read.read_exact(&mut file_buffer).await.unwrap();
        buffer.eq(&file_buffer)
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        let mut file = self.write_file.write().await;
        file.seek(SeekFrom::Start(position)).await.unwrap();
        file.write_all(buffer).await.unwrap();
        file.flush().await.unwrap();
        file.sync_all().await.unwrap();
    }

    async fn append_data(&self, buffer: &[u8], immediate: bool) {
        if immediate {
            let buffer_len = buffer.len();

            let mut file = self.append_file.write().await;
            file.write_all(&buffer).await.unwrap();
            file.flush().await.unwrap();

            self.file_len.fetch_add(buffer_len as u64, std::sync::atomic::Ordering::Release);

            drop(file);

            self.update_memory_map().await;
        } else {
            let block = MEMORY_POOL.acquire(buffer.len());
            let slice = block.into_slice_mut();
            slice.copy_from_slice(buffer);
            self.appender.push(block);
        }
    }

    async fn read_data(&self, position: &mut u64, length: u32) -> Vec<u8> {
        let mut read_file = tokio::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .await
            .unwrap();

        read_file.seek(SeekFrom::Start(*position)).await.unwrap();
        let mut buffer: Vec<u8> = vec![0; length as usize];
        let read_result = read_file.read_exact(&mut buffer).await;
        
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

    async fn get_len(&self) -> u64 {
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

    async fn read_data_into_buffer(&self, position: &mut u64, buffer: &mut [u8]) -> Result<()> {
        let buffer_len = buffer.len();
        
        // Early return for empty buffer
        if buffer_len == 0 {
            return Ok(());
        }

        let file_len = self.file_len.load(Ordering::SeqCst);
            
        // Check if we're trying to read beyond file end
        if *position >= file_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Position beyond file end"
            ));
        }
        
        // Adjust buffer length if reading beyond file end
        let actual_buffer_len = min(buffer_len, (file_len - *position) as usize);
        let actual_buffer = buffer;

        // Cache miss or partial hit - need to read from source
        let read_result = if !self._locked.load(Ordering::Relaxed) {
            self.read_from_memory_map_or_file(*position, actual_buffer)
        } else {
            self.read_from_file_direct(*position, actual_buffer)
        };

        match read_result {
            Ok(_) => {
                *position += actual_buffer_len as u64;
                Ok(())
            }
            Err(e) => Err(e)
        }
    }

    async fn read_data_to_cursor(&self, position: &mut u64, length: u32) -> Cursor<Vec<u8>> {
        let mut read_file = tokio::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .await
            .unwrap();

        read_file.seek(SeekFrom::Start(*position)).await.unwrap();
        let mut buffer: Vec<u8> = vec![0; length as usize];
        let result = read_file.read_exact(&mut buffer).await;
        if result.is_err() {
            Cursor::new(Vec::default())
        } else {
            *position += length as u64;
            let mut cursor = Cursor::new(buffer);
            cursor.set_position(0);
            return cursor;
        }
    }

    async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
        let mut file = self.write_file.write().await;
        file.seek(seek).await.unwrap();
        file.write_all(buffer).await.unwrap();
        file.flush().await.unwrap();
        file.sync_all().await.unwrap();
    }

    async fn verify_data_and_sync(&self, position: u64, buffer: &[u8]) -> bool {
        let mut file = tokio::fs::File::options()
            .read(true)
            .write(true)
            .open(&self.file_str)
            .await
            .unwrap();

        file.seek(SeekFrom::Start(position)).await.unwrap();
        let mut file_buffer = vec![0; buffer.len() as usize];
        file.read_exact(&mut file_buffer).await.unwrap();
        
        if buffer.eq(&file_buffer) {
            file.sync_data().await.unwrap();
            true
        } else {
            false
        }
    }

    async fn append_data_unsync(&self, buffer: &[u8]) {
        let mut file = self.append_file.write().await;
        file.write_all(&buffer).await.unwrap();
        self.file_len.fetch_add(buffer.len() as u64, std::sync::atomic::Ordering::SeqCst);
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

        let final_location = if self.location.ends_with(delimiter) {
            self.location[self.location.len()-1..].replace(delimiter, "")
        } else {
            format!("{}", self.location)
        };

        let file_str = format!("{}{}{}", final_location, delimiter, "temp.db");

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

        let file_read_mmap = std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .unwrap();

        let file_len = file_read_mmap.metadata().unwrap().len();

        Self {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            location: final_location.to_string(),
            table_name: "temp.db".to_string(),
            file_str: file_str.clone(),
            file_len: AtomicU64::new(file_len),
            _memory_map: ArcSwap::new(Arc::new(unsafe { MmapOptions::new()
                .map(&file_read_mmap)
                .unwrap() })),
            hash: CRC.checksum(format!("{}+++{}", final_location, "temp.db").as_bytes()),
            appender: SegQueue::new(),
            _locked: AtomicBool::new(false),
            #[cfg(unix)]
            io_uring_reader: AsyncUringReader::new(&file_str, 1024).unwrap()
        }
    }

    async fn swap_temp(&self, _temp_io_sync: &mut Self) {
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

        let final_location = if self.location.ends_with(delimiter) {
            self.location[self.location.len()-1..].replace(delimiter, "")
        } else {
            format!("{}", self.location)
        };

        let new_table = format!("{}{}{}", final_location, delimiter, name);
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

        let file_read_mmap = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        let file_len = file_read_mmap.metadata().unwrap().len();

        LocalStorageProvider {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            location: final_location.to_string(),
            table_name: name.to_string(),
            file_str: new_table.clone(),
            file_len: AtomicU64::new(file_len),
            _memory_map: ArcSwap::new(Arc::new(unsafe { MmapOptions::new()
                .map(&file_read_mmap)
                .unwrap() })),
            hash: CRC.checksum(format!("{}+++{}", final_location, name).as_bytes()),
            appender: SegQueue::new(),
            _locked: AtomicBool::new(false),
            #[cfg(unix)]
            io_uring_reader: AsyncUringReader::new(&new_table, 1024).unwrap()
        }
    }

    fn drop_io(&self) {
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
    
    async fn start_service(&self) {
        let vec = vec![self.start_append_data_service()];

        join_all(vec).await;
    }

    fn get_name(&self) -> String {
        self.table_name.clone()
    }
}