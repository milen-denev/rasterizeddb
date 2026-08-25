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
    /// One file kept in several directories at once — see
    /// [`crate::core::storage::mirror`]. Only ever built when the pool has at
    /// least one backup location, so a database that has never been told about
    /// one pays nothing for this variant beyond a match arm.
    #[cfg(feature = "storage_pools")]
    Mirror(crate::core::storage::mirror::MirrorStorageProvider),
}

impl DbStorageProvider {
    pub async fn new_file(location: &str, table_name: Option<&str>) -> Self {
        Self::File(LocalStorageProvider::new(location, table_name).await)
    }

    pub fn new_memory(name: &str) -> Self {
        Self::Memory(MemoryStorageProvider::new(name))
    }

    /// Opens `name` in `location` and in every directory in `backups`.
    ///
    /// Falls back to a plain file provider when `backups` is empty, so callers
    /// can hand it whatever the pool reports without checking first.
    #[cfg(feature = "storage_pools")]
    pub async fn new_mirrored(location: &str, backups: &[String], name: &str) -> Self {
        if backups.is_empty() {
            return Self::new_file(location, Some(name)).await;
        }
        Self::Mirror(
            crate::core::storage::mirror::MirrorStorageProvider::attach(location, backups, name)
                .await,
        )
    }

    /// Whether this provider keeps backup copies, and how many.
    #[cfg(feature = "storage_pools")]
    pub fn backup_count(&self) -> usize {
        match self {
            Self::Mirror(p) => p.backup_count(),
            _ => 0,
        }
    }

    /// The same provider with its backup copies dropped from view.
    ///
    /// For the one operation that must touch a primary's files and *not* the
    /// mirrored ones: releasing a table's old files after it has moved between
    /// two primary locations. A backup directory is flat, so both the source
    /// and the destination name the same file in it — and sweeping through the
    /// source would delete the copy the destination has just written.
    #[cfg(feature = "storage_pools")]
    pub fn without_backups(&self) -> Self {
        match self {
            Self::Mirror(p) => Self::File(p.primary().clone()),
            other => other.clone(),
        }
    }
}

impl StorageIO for DbStorageProvider {
    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        match self {
            Self::File(p) => p.verify_data(position, buffer).await,
            Self::Memory(p) => p.verify_data(position, buffer).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.verify_data(position, buffer).await,
        }
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        match self {
            Self::File(p) => p.write_data(position, buffer).await,
            Self::Memory(p) => p.write_data(position, buffer).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.write_data(position, buffer).await,
        }
    }

    async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
        match self {
            Self::File(p) => p.write_data_seek(seek, buffer).await,
            Self::Memory(p) => p.write_data_seek(seek, buffer).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.write_data_seek(seek, buffer).await,
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
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.read_data_into_buffer(position, buffer).await,
        }
    }

    async fn read_vectored(&self, reads: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        match self {
            Self::File(p) => p.read_vectored(reads).await,
            Self::Memory(p) => p.read_vectored(reads).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.read_vectored(reads).await,
        }
    }

    async fn append_data(&self, buffer: &[u8], immediate: bool) {
        match self {
            Self::File(p) => p.append_data(buffer, immediate).await,
            Self::Memory(p) => p.append_data(buffer, immediate).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.append_data(buffer, immediate).await,
        }
    }

    async fn append_data_returning_offset(&self, buffer: &[u8], immediate: bool) -> u64 {
        match self {
            Self::File(p) => p.append_data_returning_offset(buffer, immediate).await,
            Self::Memory(p) => p.append_data_returning_offset(buffer, immediate).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.append_data_returning_offset(buffer, immediate).await,
        }
    }

    fn begin_txn(&self) -> u64 {
        match self {
            Self::File(p) => p.begin_txn(),
            Self::Memory(p) => p.begin_txn(),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.begin_txn(),
        }
    }

    async fn append_data_txn(&self, txn: u64, buffer: &[u8]) -> (u64, u64) {
        match self {
            Self::File(p) => p.append_data_txn(txn, buffer).await,
            Self::Memory(p) => p.append_data_txn(txn, buffer).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.append_data_txn(txn, buffer).await,
        }
    }

    async fn write_data_txn(&self, txn: u64, position: u64, buffer: &[u8]) -> u64 {
        match self {
            Self::File(p) => p.write_data_txn(txn, position, buffer).await,
            Self::Memory(p) => p.write_data_txn(txn, position, buffer).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.write_data_txn(txn, position, buffer).await,
        }
    }

    async fn commit_durable(&self, token: u64) -> super::traits::IOResult<()> {
        match self {
            Self::File(p) => p.commit_durable(token).await,
            Self::Memory(p) => p.commit_durable(token).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.commit_durable(token).await,
        }
    }

    fn try_borrow_range(
        &self,
        position: u64,
        len: usize,
    ) -> Option<super::traits::BorrowedBytes> {
        match self {
            Self::File(p) => p.try_borrow_range(position, len),
            Self::Memory(p) => p.try_borrow_range(position, len),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.try_borrow_range(position, len),
        }
    }

    async fn truncate(&self, len: u64) -> super::traits::IOResult<()> {
        match self {
            Self::File(p) => p.truncate(len).await,
            Self::Memory(p) => p.truncate(len).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.truncate(len).await,
        }
    }

    async fn get_len(&self) -> u64 {
        match self {
            Self::File(p) => p.get_len().await,
            Self::Memory(p) => p.get_len().await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.get_len().await,
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
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => Self::Mirror(p.create_temp().await),
        }
    }

    async fn swap_temp(&self, temp_io_sync: &mut Self) {
        match (self, temp_io_sync) {
            (Self::File(p), Self::File(tmp)) => p.swap_temp(tmp).await,
            (Self::Memory(p), Self::Memory(tmp)) => p.swap_temp(tmp).await,
            #[cfg(feature = "storage_pools")]
            (Self::Mirror(p), Self::Mirror(tmp)) => p.swap_temp(tmp).await,
            _ => {
                // Backend mismatch; do nothing to avoid panics.
            }
        }
    }

    fn get_location(&self) -> Option<String> {
        match self {
            Self::File(p) => p.get_location(),
            Self::Memory(p) => p.get_location(),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.get_location(),
        }
    }

    #[allow(refining_impl_trait)]
    async fn create_new_at(&self, location: &str, name: String) -> Self {
        match self {
            Self::File(p) => Self::File(p.create_new_at(location, name).await),
            Self::Memory(p) => Self::Memory(p.create_new_at(location, name).await),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => Self::Mirror(p.create_new_at(location, name).await),
        }
    }

    #[allow(refining_impl_trait)]
    async fn create_new(&self, name: String) -> Self {
        match self {
            Self::File(p) => Self::File(p.create_new(name).await),
            Self::Memory(p) => Self::Memory(p.create_new(name).await),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => Self::Mirror(p.create_new(name).await),
        }
    }

    fn drop_io(&self) {
        match self {
            Self::File(p) => p.drop_io(),
            Self::Memory(p) => p.drop_io(),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.drop_io(),
        }
    }

    fn get_hash(&self) -> u32 {
        match self {
            Self::File(p) => p.get_hash(),
            Self::Memory(p) => p.get_hash(),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.get_hash(),
        }
    }

    async fn start_service(&self) {
        match self {
            Self::File(p) => p.start_service().await,
            Self::Memory(p) => p.start_service().await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.start_service().await,
        }
    }

    fn get_name(&self) -> String {
        match self {
            Self::File(p) => p.get_name(),
            Self::Memory(p) => p.get_name(),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.get_name(),
        }
    }
}
