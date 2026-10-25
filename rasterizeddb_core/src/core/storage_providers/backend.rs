use std::io::{self, SeekFrom};

use super::{file_sync::LocalStorageProvider, memory::MemoryStorageProvider, traits::StorageIO};

#[cfg(feature = "kv_storage")]
use super::kv::KvStorageProvider;

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
    /// A database kept in a key/value store rather than in files — see
    /// [`crate::core::storage_providers::kv`]. The whole database shares one
    /// backing store and one page cache, held by the family the provider was
    /// opened from.
    #[cfg(feature = "kv_storage")]
    KeyValue(KvStorageProvider),
}

impl DbStorageProvider {
    pub async fn new_file(location: &str, table_name: Option<&str>) -> Self {
        Self::File(LocalStorageProvider::new(location, table_name).await)
    }

    pub fn new_memory(name: &str) -> Self {
        Self::Memory(MemoryStorageProvider::new(name))
    }

    /// Opens `name` in an already-configured key/value family.
    ///
    /// The family is what carries the backing store, the chunk geometry and the
    /// shared page budget, so every file of one database must come from the
    /// same one.
    #[cfg(feature = "kv_storage")]
    pub async fn new_key_value(
        family: std::sync::Arc<super::kv::KvFamily>,
        name: &str,
    ) -> std::io::Result<Self> {
        Ok(Self::KeyValue(KvStorageProvider::open(family, name).await?))
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

    /// What is holding these bytes, as a console groups tables by.
    ///
    /// One word, and a stable one: it is a value somebody filters and sorts a
    /// table list by, so it is not prose and it does not carry a path. Where
    /// the bytes actually are is [`Self::location`], which is prose and may be
    /// empty.
    ///
    /// | value | what it means |
    /// |---|---|
    /// | `memory` | in this process, and gone when it exits |
    /// | `file` | ordinary files in a directory |
    /// | `mirrored` | files, written to a primary directory and to backups |
    /// | `s3` | objects in an S3-compatible store |
    /// | `kv-directory` | the object layout, in a local directory |
    /// | `kv-memory` | the object layout, in a map |
    ///
    /// The three `kv-*` answers are one storage engine over three backing
    /// stores, and they are kept apart rather than folded into one `object`
    /// because what an operator does about a slow table differs completely
    /// between them — a local directory is a disk to look at, and S3 is a
    /// request count to pay for.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::File(_) => "file",
            Self::Memory(_) => "memory",
            #[cfg(feature = "storage_pools")]
            Self::Mirror(_) => "mirrored",
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.family().store().kind(),
        }
    }

    /// Where those bytes are, in the form an operator would act on: a
    /// directory, a bucket, or nothing at all for a database that has none.
    pub fn location(&self) -> String {
        match self {
            Self::File(p) => p.location.clone(),
            // Not the empty string, and not the process id either: what matters
            // about an in-memory table is that it does not survive, and this is
            // read by someone deciding whether they have a problem.
            Self::Memory(_) => "(in process memory, not persisted)".to_string(),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => {
                let copies = p.backup_count();
                format!(
                    "{} (+{} backup {})",
                    p.primary().location,
                    copies,
                    if copies == 1 { "copy" } else { "copies" }
                )
            }
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => {
                let family = p.family();
                format!("{}/{}", family.store().describe(), family.config().root)
            }
        }
    }

    /// The key/value family behind this provider, if one is.
    ///
    /// The page cache and the request counters live on the family and are
    /// shared by every file of the database, so they are reported once for the
    /// database rather than per table.
    #[cfg(feature = "kv_storage")]
    pub fn kv_family(&self) -> Option<&std::sync::Arc<super::kv::KvFamily>> {
        match self {
            Self::KeyValue(p) => Some(p.family()),
            _ => None,
        }
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

impl DbStorageProvider {
    /// Records a write issued to this file in the census journal
    /// (`index_store::census`, test builds only): every write entry point
    /// below calls it first.
    #[inline(always)]
    fn census_write(&self) {
        #[cfg(test)]
        if let Self::File(p) = self {
            crate::core::index_store::census::write(&p.location, &p.table_name);
        }
    }
}

impl StorageIO for DbStorageProvider {
    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        match self {
            Self::File(p) => p.verify_data(position, buffer).await,
            Self::Memory(p) => p.verify_data(position, buffer).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.verify_data(position, buffer).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.verify_data(position, buffer).await,
        }
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        self.census_write();
        match self {
            Self::File(p) => p.write_data(position, buffer).await,
            Self::Memory(p) => p.write_data(position, buffer).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.write_data(position, buffer).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.write_data(position, buffer).await,
        }
    }

    async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
        self.census_write();
        match self {
            Self::File(p) => p.write_data_seek(seek, buffer).await,
            Self::Memory(p) => p.write_data_seek(seek, buffer).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.write_data_seek(seek, buffer).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.write_data_seek(seek, buffer).await,
        }
    }

    async fn read_data_into_buffer(&self, position: &mut u64, buffer: &mut [u8]) -> io::Result<()> {
        match self {
            Self::File(p) => p.read_data_into_buffer(position, buffer).await,
            Self::Memory(p) => p.read_data_into_buffer(position, buffer).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.read_data_into_buffer(position, buffer).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.read_data_into_buffer(position, buffer).await,
        }
    }

    async fn read_vectored(&self, reads: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        match self {
            Self::File(p) => p.read_vectored(reads).await,
            Self::Memory(p) => p.read_vectored(reads).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.read_vectored(reads).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.read_vectored(reads).await,
        }
    }

    // Forwarded like every other method. Without it this enum took the trait's
    // default `false`, so every row fetch went through the async
    // `read_vectored` chain and the providers' own synchronous mmap and
    // resident-page paths were never reached. Each provider's fast path keeps
    // its own refusal rules (staged temp data, pages not resident), and a
    // `false` still sends the caller to `read_vectored` for the same reads.
    #[inline(always)]
    fn try_read_vectored_sync(&self, reads: &mut [(u64, &mut [u8])]) -> bool {
        match self {
            Self::File(p) => p.try_read_vectored_sync(reads),
            Self::Memory(p) => p.try_read_vectored_sync(reads),
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.try_read_vectored_sync(reads),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.try_read_vectored_sync(reads),
        }
    }

    async fn append_data(&self, buffer: &[u8], immediate: bool) {
        self.census_write();
        match self {
            Self::File(p) => p.append_data(buffer, immediate).await,
            Self::Memory(p) => p.append_data(buffer, immediate).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.append_data(buffer, immediate).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.append_data(buffer, immediate).await,
        }
    }

    async fn append_data_returning_offset(&self, buffer: &[u8], immediate: bool) -> u64 {
        self.census_write();
        match self {
            Self::File(p) => p.append_data_returning_offset(buffer, immediate).await,
            Self::Memory(p) => p.append_data_returning_offset(buffer, immediate).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.append_data_returning_offset(buffer, immediate).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.append_data_returning_offset(buffer, immediate).await,
        }
    }

    fn begin_txn(&self) -> u64 {
        match self {
            Self::File(p) => p.begin_txn(),
            Self::Memory(p) => p.begin_txn(),
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.begin_txn(),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.begin_txn(),
        }
    }

    async fn append_data_txn(&self, txn: u64, buffer: &[u8]) -> (u64, u64) {
        self.census_write();
        match self {
            Self::File(p) => p.append_data_txn(txn, buffer).await,
            Self::Memory(p) => p.append_data_txn(txn, buffer).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.append_data_txn(txn, buffer).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.append_data_txn(txn, buffer).await,
        }
    }

    async fn write_data_txn(&self, txn: u64, position: u64, buffer: &[u8]) -> u64 {
        self.census_write();
        match self {
            Self::File(p) => p.write_data_txn(txn, position, buffer).await,
            Self::Memory(p) => p.write_data_txn(txn, position, buffer).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.write_data_txn(txn, position, buffer).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.write_data_txn(txn, position, buffer).await,
        }
    }

    async fn commit_durable(&self, token: u64) -> super::traits::IOResult<()> {
        match self {
            Self::File(p) => p.commit_durable(token).await,
            Self::Memory(p) => p.commit_durable(token).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.commit_durable(token).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.commit_durable(token).await,
        }
    }

    async fn abort_txn(&self, token: u64) {
        match self {
            Self::File(p) => p.abort_txn(token).await,
            Self::Memory(p) => p.abort_txn(token).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.abort_txn(token).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.abort_txn(token).await,
        }
    }

    fn try_borrow_range(&self, position: u64, len: usize) -> Option<super::traits::BorrowedBytes> {
        match self {
            Self::File(p) => p.try_borrow_range(position, len),
            Self::Memory(p) => p.try_borrow_range(position, len),
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.try_borrow_range(position, len),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.try_borrow_range(position, len),
        }
    }

    async fn truncate(&self, len: u64) -> super::traits::IOResult<()> {
        self.census_write();
        match self {
            Self::File(p) => p.truncate(len).await,
            Self::Memory(p) => p.truncate(len).await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.truncate(len).await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.truncate(len).await,
        }
    }

    async fn get_len(&self) -> u64 {
        match self {
            Self::File(p) => p.get_len().await,
            Self::Memory(p) => p.get_len().await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.get_len().await,
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
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => Self::KeyValue(p.create_temp().await),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => Self::Mirror(p.create_temp().await),
        }
    }

    async fn swap_temp(&self, temp_io_sync: &mut Self) {
        match (self, temp_io_sync) {
            (Self::File(p), Self::File(tmp)) => p.swap_temp(tmp).await,
            (Self::Memory(p), Self::Memory(tmp)) => p.swap_temp(tmp).await,
            #[cfg(feature = "kv_storage")]
            (Self::KeyValue(p), Self::KeyValue(tmp)) => p.swap_temp(tmp).await,
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
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.get_location(),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.get_location(),
        }
    }

    #[allow(refining_impl_trait)]
    async fn create_new_at(&self, location: &str, name: String) -> Self {
        match self {
            Self::File(p) => Self::File(p.create_new_at(location, name).await),
            Self::Memory(p) => Self::Memory(p.create_new_at(location, name).await),
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => Self::KeyValue(p.create_new_at(location, name).await),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => Self::Mirror(p.create_new_at(location, name).await),
        }
    }

    #[allow(refining_impl_trait)]
    async fn create_new(&self, name: String) -> Self {
        match self {
            Self::File(p) => Self::File(p.create_new(name).await),
            Self::Memory(p) => Self::Memory(p.create_new(name).await),
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => Self::KeyValue(p.create_new(name).await),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => Self::Mirror(p.create_new(name).await),
        }
    }

    fn drop_io(&self) {
        match self {
            Self::File(p) => p.drop_io(),
            Self::Memory(p) => p.drop_io(),
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.drop_io(),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.drop_io(),
        }
    }

    fn get_hash(&self) -> u32 {
        match self {
            Self::File(p) => p.get_hash(),
            Self::Memory(p) => p.get_hash(),
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.get_hash(),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.get_hash(),
        }
    }

    async fn start_service(&self) {
        match self {
            Self::File(p) => p.start_service().await,
            Self::Memory(p) => p.start_service().await,
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.start_service().await,
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.start_service().await,
        }
    }

    fn get_name(&self) -> String {
        match self {
            Self::File(p) => p.get_name(),
            Self::Memory(p) => p.get_name(),
            #[cfg(feature = "kv_storage")]
            Self::KeyValue(p) => p.get_name(),
            #[cfg(feature = "storage_pools")]
            Self::Mirror(p) => p.get_name(),
        }
    }

    fn index_store_dir(&self) -> Option<std::path::PathBuf> {
        match self {
            Self::File(p) => p.index_store_dir(),
            // No directory of its own (memory, key/value), or several copies
            // of one (a mirrored pool): excluded from snapshots (P33).
            _ => None,
        }
    }

    fn attach_write_gate(&self, gate: &std::sync::Arc<crate::core::index_store::gate_word::WriteGate>) {
        if let Self::File(p) = self {
            p.attach_write_gate(gate);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A row fetch asks for the synchronous copy first and only awaits
    /// `read_vectored` when that says no. Through this enum the answer was
    /// always no, so the file provider's mmap copy was dead code on every
    /// table. This pins that the backend now reaches it, and that what it
    /// copies is what the async path reads.
    #[tokio::test]
    async fn a_file_backend_serves_resident_reads_synchronously() {
        // Written before the provider opens it, so its map covers every byte:
        // a small append is served from the file rather than remapped (see
        // `REMAP_GROWTH_THRESHOLD`), and the fast path rightly refuses those.
        let dir = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(dir.path().join("sync.db"), &data).unwrap();
        let io = DbStorageProvider::new_file(dir.path().to_str().unwrap(), Some("sync.db")).await;
        assert_eq!(io.get_len().await, data.len() as u64);

        let mut header = [0u8; 12];
        let mut body = [0u8; 150];
        let mut reads = [(40u64, &mut header[..]), (5000u64, &mut body[..])];
        assert!(
            io.try_read_vectored_sync(&mut reads),
            "a resident read on a file backend was refused"
        );
        assert_eq!(&header[..], &data[40..52]);
        assert_eq!(&body[..], &data[5000..5150]);

        let mut header_async = [0u8; 12];
        let mut body_async = [0u8; 150];
        let mut reads = [(40u64, &mut header_async[..]), (5000u64, &mut body_async[..])];
        io.read_vectored(&mut reads).await.unwrap();
        assert_eq!(header, header_async);
        assert_eq!(body, body_async);

        // A read running past the end is still refused, which sends the
        // caller to `read_vectored` and its error rather than a short copy.
        let mut tail = [0u8; 16];
        assert!(!io.try_read_vectored_sync(&mut [(8190u64, &mut tail[..])]));
    }
}
