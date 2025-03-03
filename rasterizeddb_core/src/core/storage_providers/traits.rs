use std::{
    future::Future,
    io::{Cursor, SeekFrom},
};

pub type IOError = std::io::Error;
pub type IOResult<T> = std::result::Result<T, IOError>;

#[allow(async_fn_in_trait)]
pub trait IOOperationsSync: Clone + Sync + Send + 'static {
    fn write_data_unsync(&mut self, position: u64, buffer: &[u8]) -> impl Future<Output = ()> + Send + Sync;

    fn verify_data(&mut self, position: u64, buffer: &[u8]) -> impl Future<Output = bool> + Send + Sync;

    fn verify_data_and_sync(&mut self, position: u64, buffer: &[u8]) -> impl Future<Output = bool> + Send + Sync;

    fn write_data(&mut self, position: u64, buffer: &[u8]) -> impl Future<Output = ()> + Send + Sync;

    fn write_data_seek(&mut self, seek: SeekFrom, buffer: &[u8]) -> impl Future<Output = ()> + Send + Sync;

    fn read_data(
        &self,
        position: &mut u64,
        length: u32,
    ) -> impl Future<Output = Vec<u8>> + Send + Sync;

    fn read_data_into_buffer(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
    ) -> impl Future<Output = ()> + Send + Sync;

    fn read_data_to_cursor(
        &self,
        position: &mut u64,
        length: u32,
    ) -> impl Future<Output = Cursor<Vec<u8>>> + Send + Sync;

    fn read_data_to_end(&self, position: u64) -> impl Future<Output = Vec<u8>> + Send + Sync;

    fn append_data(&mut self, buffer: &[u8]) -> impl Future<Output = ()> + Send + Sync;

    fn append_data_unsync(&mut self, buffer: &[u8]) -> impl Future<Output = ()> + Send + Sync;

    fn get_len(&mut self) -> impl Future<Output = u64> + Send + Sync;

    fn exists(location: &str, table_name: &str) -> bool;

    fn create_temp(&self) -> impl Future<Output = Self> + Send + Sync;

    fn swap_temp(&mut self, temp_io_sync: &mut Self) -> impl Future<Output = ()> + Send + Sync;

    fn get_location(&self) -> Option<String>;

    fn create_new(&self, name: String) -> impl Future<Output = Self> + Send + Sync;

    fn drop_io(&mut self);
}

pub trait IOOperationsAsync<'a>: TryCloneAsync {
    fn write_data(&'a mut self, position: u64, buffer: &[u8]) -> impl Future<Output = ()> + Send;

    fn write_data_own(
        &'a mut self,
        position: u64,
        buffer: Box<Vec<u8>>,
    ) -> impl Future<Output = ()> + Send;

    fn read_data(&'a mut self, position: u64, length: u32) -> impl Future<Output = Vec<u8>> + Send;

    fn append_data(&'a mut self, buffer: &'a [u8]) -> impl Future<Output = ()> + Send;

    fn append_data_own(&'a mut self, buffer: Box<Vec<u8>>) -> impl Future<Output = ()> + Send;

    fn get_len(&'a mut self) -> impl Future<Output = u64> + Send;

    fn exists(location: &'a str, table_name: &'a str) -> impl Future<Output = bool> + Send;
}

pub trait TryCloneAsync {
    fn try_clone(&self) -> impl Future<Output = Self> + Send;
}
