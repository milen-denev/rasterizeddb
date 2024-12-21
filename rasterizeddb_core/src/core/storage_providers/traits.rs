use std::{future::Future, io::{Cursor, SeekFrom}};

pub(crate) type IOError = std::io::Error;
pub(crate) type IOResult<T> = std::result::Result<T, IOError>;

pub trait IOOperationsSync: Clone {
    fn write_data(&mut self,  
        position: u64, 
        buffer: &[u8]);

    fn read_data(&mut self,
        position: &mut u64,  
        length: u32) -> Vec<u8>;

    fn read_data_into_buffer(&mut self,
        position: &mut u64,  
        buffer: &mut [u8]);

    fn read_data_to_cursor(&mut self,
        position: &mut u64,  
        length: u32) -> Cursor<Vec<u8>>;

    fn read_data_to_end(&mut self,
        position: u64) -> Vec<u8>;

    fn append_data(&mut self,  
        buffer: &[u8]);

    fn get_len(&mut self) -> u64;

    fn exists(location: &str, table_name: &str) -> bool;

    fn seek(&mut self, seek: SeekFrom);

    fn read_u8(&mut self) -> IOResult<u8>;

    fn read_u32(&mut self) -> IOResult<u32>;

    fn read_u64(&mut self) -> IOResult<u64>;
}

pub trait IOOperationsAsync<'a>: TryCloneAsync {
    fn write_data(&'a mut self,  
        position: u64, 
        buffer: &[u8]) -> impl Future<Output = ()> + Send;

    fn write_data_own(&'a mut self,  
        position: u64, 
        buffer: Box<Vec<u8>>) -> impl Future<Output = ()> + Send;

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

pub trait TryCloneAsync {
    fn try_clone(&self) -> impl Future<Output = Self> + Send;
}