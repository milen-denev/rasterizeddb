use std::io::{self, Read, Seek, SeekFrom};
use std::cmp::min;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use memmap2::Mmap;
use orx_concurrent_vec::{ConcurrentElement, ConcurrentVec, FixedVec};
use smallvec::SmallVec;

pub struct BufferedCache {
    location: String,
    buffer: ConcurrentVec<u8, FixedVec<ConcurrentElement<u8>>>,
    buffer_start: AtomicU64,
    buffer_len: AtomicUsize,
    current_pos: AtomicU64,
    buffer_size: u64,
}

impl BufferedCache {
    pub fn new(location: String, buffer_size: u64) -> Self {
        let buffer = ConcurrentVec::with_fixed_capacity(buffer_size as usize);
        
        // Pre-populate the buffer with default values
        for _ in 0..buffer_size {
            buffer.push(0u8);
        }
        
        Self {
            location,
            buffer,
            buffer_start: AtomicU64::new(0),
            buffer_len: AtomicUsize::new(0),
            current_pos: AtomicU64::new(0),
            buffer_size,
        }
    }
    
    pub fn read(&self, buf: &mut [u8], mmap: Option<&Mmap>) -> io::Result<usize> {
        let mut bytes_read = 0;
        let mut remaining = buf.len();
        
        while remaining > 0 {
            let current_pos = self.current_pos.load(Ordering::SeqCst);
            
            if !self.is_position_in_buffer(current_pos) {
                self.fill_buffer(current_pos, mmap)?;
            }

            let buffer_len = self.buffer_len.load(Ordering::SeqCst);
            if buffer_len == 0 {
                break;
            }

            let buffer_start = self.buffer_start.load(Ordering::SeqCst);
            
            // Handle race condition where current_pos < buffer_start
            if current_pos < buffer_start {
                self.fill_buffer(current_pos, mmap)?;
                let new_buffer_len = self.buffer_len.load(Ordering::SeqCst);
                if new_buffer_len == 0 {
                    break;
                }
                continue; // Restart with updated buffer
            }
            
            let buffer_offset = current_pos - buffer_start;
            
            // Check bounds
            if buffer_offset >= buffer_len as u64 {
                self.fill_buffer(current_pos, mmap)?;
                let new_buffer_len = self.buffer_len.load(Ordering::SeqCst);
                if new_buffer_len == 0 {
                    break;
                }
                continue; // Restart with updated buffer
            }
            
            let available_in_buffer = buffer_len - buffer_offset as usize;
            let to_read = min(remaining, available_in_buffer);
            
            if to_read == 0 {
                break;
            }
            
            let src_start = buffer_offset;
            let src_end = buffer_offset + to_read as u64;
            let dst_start = bytes_read;
            let dst_end = bytes_read + to_read;

            let mut small_buffer = SmallVec::<[u8; 1024 * 4]>::new();

            // Add bounds check here too
            for i in src_start..src_end {
                if (i as usize) < self.buffer.len() {
                    let u8_value: u8 = self.buffer[i as usize].map(|x| *x);
                    small_buffer.push(u8_value);
                } else {
                    // Buffer was modified, restart
                    break;
                }
            }

            if small_buffer.len() != to_read {
                // Buffer was modified during read, restart
                continue;
            }

            buf[dst_start..dst_end].copy_from_slice(&small_buffer);

            bytes_read += to_read;
            remaining -= to_read;
            self.current_pos.fetch_add(to_read as u64, Ordering::SeqCst);
        }
        
        Ok(bytes_read)
    }
    
    pub fn read_at(&self, pos: u64, buf: &mut [u8], mmap: Option<&Mmap>) -> io::Result<usize> {
        self.current_pos.store(pos, Ordering::SeqCst);
        self.read(buf, mmap)
    }
    
    fn is_position_in_buffer(&self, pos: u64) -> bool {
        let buffer_start = self.buffer_start.load(Ordering::SeqCst);
        let buffer_len = self.buffer_len.load(Ordering::SeqCst);
        
        pos >= buffer_start && pos < buffer_start + buffer_len as u64
    }
    
    fn fill_buffer(&self, pos: u64, mmap: Option<&Mmap>) -> io::Result<()> {
        if let Some(mmap) = mmap {
            let file_len = mmap.len() as u64;
            
            // Handle case where pos is beyond file end
            if pos >= file_len {
                self.buffer_len.store(0, Ordering::SeqCst);
                self.buffer_start.store(pos, Ordering::SeqCst);
                return Ok(());
            }
            
            let max_read = min(self.buffer_size, file_len - pos);
            let bytes_read = &mmap[pos as usize..(pos as usize + max_read as usize)];
            
            // Update existing buffer elements
            for i in 0..bytes_read.len() {
                let u8_value = bytes_read[i];
                self.buffer[i].update(|x| *x = u8_value);
            }

            self.buffer_len.store(bytes_read.len(), Ordering::SeqCst);
            self.buffer_start.store(pos, Ordering::SeqCst);
        } else {
            let mut reader = std::fs::File::open(&self.location)?;
            reader.seek(SeekFrom::Start(pos))?;
            let file_len = reader.metadata()?.len();
            
            // Handle case where pos is beyond file end
            if pos >= file_len {
                self.buffer_len.store(0, Ordering::SeqCst);
                self.buffer_start.store(pos, Ordering::SeqCst);
                return Ok(());
            }
            
            let max_read = min(self.buffer_size, file_len - pos);
            let mut temp_vec = vec![0_u8; max_read as usize];
            let bytes_read = reader.read(&mut temp_vec)?;
            
            // Update existing buffer elements
            for i in 0..bytes_read {
                let u8_value = temp_vec[i];
                self.buffer[i].update(|x| *x = u8_value);
            }
            
            self.buffer_len.store(bytes_read, Ordering::SeqCst);
            self.buffer_start.store(pos, Ordering::SeqCst);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Helper function to create a test file with known content
    // Returns both the path and the temp file to keep it alive
    fn create_test_file(content: &[u8]) -> (String, NamedTempFile) {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content).unwrap();
        temp_file.flush().unwrap();
        
        let path = temp_file.path().to_string_lossy().to_string();
        (path, temp_file)
    }

    // Helper function to create a large test file
    fn create_large_test_file(size: usize) -> (String, NamedTempFile) {
        let content: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        create_test_file(&content)
    }

    #[test]
    fn test_basic_sequential_read() {
        let test_data: Vec<u8> = (0..200).map(|i| (i % 256) as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 64);
        
        // Read first 32 bytes
        let mut buf1 = vec![0u8; 32];
        let n1 = cache.read(&mut buf1, None).unwrap();
        assert_eq!(n1, 32);
        assert_eq!(buf1, test_data[0..32]);
        
        // Read next 32 bytes (should use same buffer)
        let mut buf2 = vec![0u8; 32];
        let n2 = cache.read(&mut buf2, None).unwrap();
        assert_eq!(n2, 32);
        assert_eq!(buf2, test_data[32..64]);
        
        // _temp_file is automatically cleaned up when dropped
    }

    #[test]
    fn test_buffer_boundary_crossing() {
        let test_data: Vec<u8> = (0..200).map(|i| (i % 256) as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 64);
        
        // Read 100 bytes (should cross buffer boundary)
        let mut buf = vec![0u8; 100];
        let n = cache.read(&mut buf, None).unwrap();
        assert_eq!(n, 100);
        assert_eq!(buf, test_data[0..100]);
    }

    #[test]
    fn test_read_at_random_positions() {
        let test_data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 128);
        
        // Test multiple random positions
        let positions = vec![50, 200, 500, 750, 100];
        let read_sizes = vec![30, 50, 25, 40, 60];
        
        for (pos, size) in positions.iter().zip(read_sizes.iter()) {
            let mut buf = vec![0u8; *size];
            let n = cache.read_at(*pos, &mut buf, None).unwrap();
            
            let expected_size = std::cmp::min(*size, test_data.len() - *pos as usize);
            assert_eq!(n, expected_size);
            
            let expected_data = &test_data[*pos as usize..(*pos as usize + expected_size)];
            assert_eq!(&buf[..expected_size], expected_data);
        }
    }

    #[test]
    fn test_read_beyond_file_end() {
        let test_data: Vec<u8> = (0..100).map(|i| i as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 64);
        
        // Try to read beyond file end
        let mut buf = vec![0u8; 200];
        let n = cache.read_at(50, &mut buf, None).unwrap();
        
        // Should only read available bytes
        assert_eq!(n, 50);
        assert_eq!(&buf[..50], &test_data[50..100]);
    }

    #[test]
    fn test_empty_file() {
        let (file_path, _temp_file) = create_test_file(&[]);
        
        let cache = BufferedCache::new(file_path, 64);
        
        let mut buf = vec![0u8; 100];
        let n = cache.read(&mut buf, None).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_single_byte_file() {
        let test_data = vec![42u8];
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 64);
        
        let mut buf = vec![0u8; 10];
        let n = cache.read(&mut buf, None).unwrap();
        assert_eq!(n, 1);
        assert_eq!(buf[0], 42);
    }

    #[test]
    fn test_large_file_with_small_buffer() {
        let (file_path, _temp_file) = create_large_test_file(10000);
        
        let cache = BufferedCache::new(file_path, 100);
        
        // Read in chunks and verify content
        let mut total_read = 0;
        let chunk_size = 50;
        
        for _ in 0..5 {
            let mut buf = vec![0u8; chunk_size];
            let n = cache.read(&mut buf, None).unwrap();
            assert_eq!(n, chunk_size);
            
            // Verify content matches expected pattern
            for j in 0..chunk_size {
                let expected = ((total_read + j) % 256) as u8;
                assert_eq!(buf[j], expected);
            }
            
            total_read += chunk_size;
        }
    }

    #[test]
    fn test_buffer_size_larger_than_file() {
        let test_data: Vec<u8> = (0..50).map(|i| i as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 1000); // Buffer larger than file
        
        let mut buf = vec![0u8; 100];
        let n = cache.read(&mut buf, None).unwrap();
        assert_eq!(n, 50);
        assert_eq!(&buf[..50], &test_data);
    }

    #[test]
    fn test_multiple_small_reads() {
        let test_data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 256);
        
        // Perform many small reads
        let mut total_bytes_read = 0;
        let mut all_data = Vec::new();
        
        for _ in 0..50 {
            let mut buf = vec![0u8; 20];
            let n = cache.read(&mut buf, None).unwrap();
            if n == 0 { break; }
            
            all_data.extend_from_slice(&buf[..n]);
            total_bytes_read += n;
        }
        
        assert_eq!(total_bytes_read, 1000);
        assert_eq!(all_data, test_data);
    }

    // #[test]
    // fn test_concurrent_reads() {
    //     let test_data: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
    //     let (file_path, _temp_file) = create_test_file(&test_data);
        
    //     let cache = Arc::new(BufferedCache::new(file_path, 512));
    //     let mut handles = vec![];
        
    //     // Spawn multiple threads to read different parts
    //     for i in 0..10 {
    //         let cache_clone = Arc::clone(&cache);
    //         let start_pos = i * 500;
            
    //         let handle = thread::spawn(move || {
    //             let mut buf = vec![0u8; 200];
    //             let n = cache_clone.read_at(start_pos, &mut buf).unwrap();
    //             (start_pos, n, buf)
    //         });
            
    //         handles.push(handle);
    //     }
        
    //     // Collect results and verify
    //     for handle in handles {
    //         let (start_pos, bytes_read, buf) = handle.join().unwrap();
    //         let expected_size = std::cmp::min(200, test_data.len() - start_pos as usize);
            
    //         assert_eq!(bytes_read, expected_size);
            
    //         if expected_size > 0 {
    //             let expected_data = &test_data[start_pos as usize..(start_pos as usize + expected_size)];
    //             assert_eq!(&buf[..expected_size], expected_data);
    //         }
    //     }
    // }

    #[test]
    fn test_backward_and_forward_seeks() {
        let test_data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 256);
        
        // Read forward
        let mut buf1 = vec![0u8; 100];
        let n1 = cache.read_at(100, &mut buf1, None).unwrap();
        assert_eq!(n1, 100);
        assert_eq!(buf1, test_data[100..200]);
        
        // Read backward (should trigger buffer refill)
        let mut buf2 = vec![0u8; 50];
        let n2 = cache.read_at(50, &mut buf2, None).unwrap();
        assert_eq!(n2, 50);
        assert_eq!(buf2, test_data[50..100]);
        
        // Read forward again
        let mut buf3 = vec![0u8; 150];
        let n3 = cache.read_at(500, &mut buf3, None).unwrap();
        assert_eq!(n3, 150);
        assert_eq!(buf3, test_data[500..650]);
    }

    #[test]
    fn test_exact_buffer_size_reads() {
        let test_data: Vec<u8> = (0..512).map(|i| (i % 256) as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 256);
        
        // Read exactly buffer size
        let mut buf1 = vec![0u8; 256];
        let n1 = cache.read(&mut buf1, None).unwrap();
        assert_eq!(n1, 256);
        assert_eq!(buf1, test_data[0..256]);
        
        // Read remaining data (exactly buffer size again)
        let mut buf2 = vec![0u8; 256];
        let n2 = cache.read(&mut buf2, None).unwrap();
        assert_eq!(n2, 256);
        assert_eq!(buf2, test_data[256..512]);
    }

    #[test]
    fn test_zero_size_reads() {
        let test_data: Vec<u8> = (0..100).map(|i| i as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 64);
        
        // Zero size read should return 0
        let mut buf = vec![];
        let n = cache.read(&mut buf, None).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_nonexistent_file() {
        let cache = BufferedCache::new("nonexistent_file.dat".to_string(), 64);
        
        let mut buf = vec![0u8; 100];
        let result = cache.read(&mut buf, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_very_large_read_request() {
        let test_data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 128);
        
        // Request much more data than available
        let mut buf = vec![0u8; 5000];
        let n = cache.read(&mut buf, None).unwrap();
        assert_eq!(n, 1000);
        assert_eq!(&buf[..1000], &test_data);
    }

    #[test]
    fn test_alternating_read_patterns() {
        let test_data: Vec<u8> = (0..2000).map(|i| (i % 256) as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 512);
        
        // Alternating between different positions
        let positions = vec![0, 1000, 100, 1500, 200];
        let sizes = vec![50, 75, 100, 25, 80];
        
        for (pos, size) in positions.iter().zip(sizes.iter()) {
            let mut buf = vec![0u8; *size];
            let n = cache.read_at(*pos, &mut buf, None).unwrap();
            
            let expected_size = std::cmp::min(*size, test_data.len() - *pos as usize);
            assert_eq!(n, expected_size);
            
            if expected_size > 0 {
                let expected_data = &test_data[*pos as usize..(*pos as usize + expected_size)];
                assert_eq!(&buf[..expected_size], expected_data);
            }
        }
    }

    #[test]
    fn test_buffer_reuse_efficiency() {
        let test_data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let (file_path, _temp_file) = create_test_file(&test_data);
        
        let cache = BufferedCache::new(file_path, 200);
        
        // Multiple reads within same buffer range should be efficient
        let positions = vec![50, 75, 100, 125, 150]; // All within first buffer
        
        for pos in positions {
            let mut buf = vec![0u8; 25];
            let n = cache.read_at(pos, &mut buf, None).unwrap();
            assert_eq!(n, 25);
            
            let expected = &test_data[pos as usize..(pos as usize + 25)];
            assert_eq!(buf, expected);
        }
    }

    
    fn create_pattern_file(size: usize, pattern: impl Fn(usize) -> u8) -> (String, NamedTempFile) {
        let content: Vec<u8> = (0..size).map(pattern).collect();
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(&content).unwrap();
        temp_file.flush().unwrap();
        
        let path = temp_file.path().to_string_lossy().to_string();
        (path, temp_file)
    }

    #[test]
    fn test_your_original_use_case() {
        // Simulate your original use case: read 0-64, then 64-128
        let (file_path, _temp_file) = create_pattern_file(256, |i| (i % 256) as u8);
        let cache = BufferedCache::new(file_path, 1024);

        // Read from 0 to 64
        let mut buffer1 = vec![0u8; 64];
        cache.read_at(0, &mut buffer1, None).unwrap();
        
        // Verify first 64 bytes
        for i in 0..64 {
            assert_eq!(buffer1[i], i as u8);
        }

        // Read from 64 to 128 (should use same buffer - efficient!)
        let mut buffer2 = vec![0u8; 64];
        let n = cache.read(&mut buffer2, None).unwrap(); // Sequential read from current position
        assert_eq!(n, 64);
        
        // Verify next 64 bytes
        for i in 0..64 {
            assert_eq!(buffer2[i], (64 + i) as u8);
        }
    }

    #[test]
    fn test_log_file_reading_pattern() {
        // Simulate reading a log file line by line
        let log_content = "Line 1: Error occurred\nLine 2: Warning message\nLine 3: Info message\nLine 4: Debug trace\n";
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(log_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();
        
        let file_path = temp_file.path().to_string_lossy().to_string();
        let cache = BufferedCache::new(file_path, 1024);
        
        // Read entire content in small chunks
        let mut result = Vec::new();
        let mut pos = 0u64;
        
        loop {
            let mut chunk = vec![0u8; 20];
            let n = cache.read_at(pos, &mut chunk, None).unwrap();
            if n == 0 { break; }
            
            result.extend_from_slice(&chunk[..n]);
            pos += n as u64;
        }
        
        assert_eq!(String::from_utf8(result).unwrap(), log_content);
    }

    #[test]
    fn test_file_boundary_conditions() {
        // Test with a file that's exactly the buffer size
        let (file_path, _temp_file) = create_pattern_file(1024, |i| (i % 256) as u8);
        let cache = BufferedCache::new(file_path, 1024);

        // Read entire file in one go
        let mut buffer = vec![0u8; 1024];
        let n = cache.read(&mut buffer, None).unwrap();
        assert_eq!(n, 1024);

        // Verify content
        for i in 0..1024 {
            assert_eq!(buffer[i], (i % 256) as u8);
        }

        // Try to read more - should get 0
        let mut extra_buffer = vec![0u8; 100];
        let n2 = cache.read(&mut extra_buffer, None).unwrap();
        assert_eq!(n2, 0);
    }
}