use std::{
    io::SeekFrom,
    sync::Arc,
};

use temp_testdir::TempDir;

use super::{
    file_sync::LocalStorageProvider,
    traits::StorageIO,
};

/// Test-only storage provider.
///
/// This is intended to be a behavioral replica of `LocalStorageProvider` (from `file_sync.rs`),
/// with the only difference being that it automatically allocates its files under a temporary
/// directory that is deleted once all clones are dropped.
pub struct MockStorageProvider {
    inner: LocalStorageProvider,
    // Keep the temp directory alive for as long as any clone exists.
    _temp_dir_hold: Arc<TempDir>,
}

unsafe impl Sync for MockStorageProvider {}
unsafe impl Send for MockStorageProvider {}

impl Clone for MockStorageProvider {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _temp_dir_hold: self._temp_dir_hold.clone(),
        }
    }
}

impl MockStorageProvider {
    pub async fn new() -> MockStorageProvider {
        let temp = Arc::new(TempDir::default());
        let location = temp
            .as_ref()
            .as_ref()
            .to_str()
            .expect("TempDir path must be valid UTF-8");

        let inner = LocalStorageProvider::new(location, Some("TEMP.db")).await;

        MockStorageProvider {
            inner,
            _temp_dir_hold: temp,
        }
    }

    pub async fn close_files(&self) {
        self.inner.close_files().await;
    }
}

impl StorageIO for MockStorageProvider {
    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        self.inner.verify_data(position, buffer).await
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        self.inner.write_data(position, buffer).await
    }

    async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
        self.inner.write_data_seek(seek, buffer).await
    }

    async fn read_data_into_buffer(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
    ) -> Result<(), std::io::Error> {
        self.inner.read_data_into_buffer(position, buffer).await
    }

    async fn read_vectored(
        &self,
        reads: &mut [(u64, &mut [u8])],
    ) -> Result<(), std::io::Error> {
        self.inner.read_vectored(reads).await
    }

    async fn append_data(&self, buffer: &[u8], immediate: bool) {
        self.inner.append_data(buffer, immediate).await
    }

    async fn get_len(&self) -> u64 {
        self.inner.get_len().await
    }

    fn exists(location: &str, table_name: &str) -> bool {
        LocalStorageProvider::exists(location, table_name)
    }

    async fn create_temp(&self) -> Self {
        let inner = self.inner.create_temp().await;
        Self {
            inner,
            _temp_dir_hold: self._temp_dir_hold.clone(),
        }
    }

    async fn swap_temp(&self, temp_io_sync: &mut Self) {
        self.inner.swap_temp(&mut temp_io_sync.inner).await
    }

    fn get_location(&self) -> Option<String> {
        self.inner.get_location()
    }

    #[allow(refining_impl_trait)]
    async fn create_new(&self, name: String) -> Self {
        let inner = self.inner.create_new(name).await;
        Self {
            inner,
            _temp_dir_hold: self._temp_dir_hold.clone(),
        }
    }

    fn drop_io(&self) {
        self.inner.drop_io();
    }

    fn get_hash(&self) -> u32 {
        self.inner.get_hash()
    }

    async fn start_service(&self) {
        self.inner.start_service().await;
    }

    fn get_name(&self) -> String {
        self.inner.get_name()
    }
}
