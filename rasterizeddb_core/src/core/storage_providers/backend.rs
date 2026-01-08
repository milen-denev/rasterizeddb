use std::io::{self, SeekFrom};

use super::{
    file_sync::LocalStorageProvider,
    memory::MemoryStorageProvider,
    traits::StorageIO,
};

#[derive(Clone)]
pub enum DbStorageProvider {
    File(LocalStorageProvider),
    Memory(MemoryStorageProvider),
}

impl DbStorageProvider {
    pub async fn new_file(location: &str, table_name: Option<&str>) -> Self {
        Self::File(LocalStorageProvider::new(location, table_name).await)
    }

    pub fn new_memory(name: &str) -> Self {
        Self::Memory(MemoryStorageProvider::new(name))
    }
}

impl StorageIO for DbStorageProvider {
    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        match self {
            Self::File(p) => p.verify_data(position, buffer).await,
            Self::Memory(p) => p.verify_data(position, buffer).await,
        }
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        match self {
            Self::File(p) => p.write_data(position, buffer).await,
            Self::Memory(p) => p.write_data(position, buffer).await,
        }
    }

    async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
        match self {
            Self::File(p) => p.write_data_seek(seek, buffer).await,
            Self::Memory(p) => p.write_data_seek(seek, buffer).await,
        }
    }

    async fn read_data_into_buffer(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
    ) -> io::Result<()> {
        match self {
            Self::File(p) => p.read_data_into_buffer(position, buffer).await,
            Self::Memory(p) => p.read_data_into_buffer(position, buffer).await,
        }
    }

    async fn read_vectored(&self, reads: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        match self {
            Self::File(p) => p.read_vectored(reads).await,
            Self::Memory(p) => p.read_vectored(reads).await,
        }
    }

    async fn append_data(&self, buffer: &[u8], immediate: bool) {
        match self {
            Self::File(p) => p.append_data(buffer, immediate).await,
            Self::Memory(p) => p.append_data(buffer, immediate).await,
        }
    }

    async fn get_len(&self) -> u64 {
        match self {
            Self::File(p) => p.get_len().await,
            Self::Memory(p) => p.get_len().await,
        }
    }

    fn exists(location: &str, table_name: &str) -> bool {
        // Exists is only meaningful for file-backed providers.
        LocalStorageProvider::exists(location, table_name)
    }

    async fn create_temp(&self) -> Self {
        match self {
            Self::File(p) => Self::File(p.create_temp().await),
            Self::Memory(p) => Self::Memory(p.create_temp().await),
        }
    }

    async fn swap_temp(&self, temp_io_sync: &mut Self) {
        match (self, temp_io_sync) {
            (Self::File(p), Self::File(tmp)) => p.swap_temp(tmp).await,
            (Self::Memory(p), Self::Memory(tmp)) => p.swap_temp(tmp).await,
            _ => {
                // Backend mismatch; do nothing to avoid panics.
            }
        }
    }

    fn get_location(&self) -> Option<String> {
        match self {
            Self::File(p) => p.get_location(),
            Self::Memory(p) => p.get_location(),
        }
    }

    #[allow(refining_impl_trait)]
    async fn create_new(&self, name: String) -> Self {
        match self {
            Self::File(p) => Self::File(p.create_new(name).await),
            Self::Memory(p) => Self::Memory(p.create_new(name).await),
        }
    }

    fn drop_io(&self) {
        match self {
            Self::File(p) => p.drop_io(),
            Self::Memory(p) => p.drop_io(),
        }
    }

    fn get_hash(&self) -> u32 {
        match self {
            Self::File(p) => p.get_hash(),
            Self::Memory(p) => p.get_hash(),
        }
    }

    async fn start_service(&self) {
        match self {
            Self::File(p) => p.start_service().await,
            Self::Memory(p) => p.start_service().await,
        }
    }

    fn get_name(&self) -> String {
        match self {
            Self::File(p) => p.get_name(),
            Self::Memory(p) => p.get_name(),
        }
    }
}
