use std::{
    future::Future,
    io::SeekFrom,
};

pub type IOError = std::io::Error;
pub type IOResult<T> = std::result::Result<T, IOError>;

#[allow(async_fn_in_trait)]
pub trait StorageIO: Clone + Sync + Send + 'static {
    fn verify_data(&self, position: u64, buffer: &[u8])
    -> impl Future<Output = bool> + Send + Sync;

    fn write_data(&self, position: u64, buffer: &[u8]) -> impl Future<Output = ()> + Send + Sync;

    fn write_data_seek(
        &self,
        seek: SeekFrom,
        buffer: &[u8],
    ) -> impl Future<Output = ()> + Send + Sync;

    fn read_data_into_buffer(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send + Sync;

    fn read_vectored(
        &self,
        reads: &mut [(u64, &mut [u8])],
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send + Sync;

    fn append_data(&self, buffer: &[u8], immediate: bool)
    -> impl Future<Output = ()> + Send + Sync;

    fn get_len(&self) -> impl Future<Output = u64> + Send + Sync;

    fn exists(location: &str, table_name: &str) -> bool;

    fn create_temp(&self) -> impl Future<Output = Self> + Send + Sync;

    fn swap_temp(&self, temp_io_sync: &mut Self) -> impl Future<Output = ()> + Send + Sync;

    fn get_location(&self) -> Option<String>;

    fn create_new(&self, name: String) -> impl Future<Output = Self> + Send + Sync;

    fn drop_io(&self);

    fn get_hash(&self) -> u32;

    fn start_service(&self) -> impl Future<Output = ()> + Send + Sync;

    fn get_name(&self) -> String;
}
