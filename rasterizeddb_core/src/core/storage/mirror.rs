//! A file that exists in more than one directory at once.
//!
//! Wraps one primary [`LocalStorageProvider`] and any number of backup ones.
//! Reads are served by the primary alone; every write is applied to all of
//! them. That is the whole of "primary + backup": the pool decides which
//! directories are backups, and this makes a table's files land in each of
//! them without anything above the storage layer knowing.
//!
//! # What this is and is not
//!
//! It is a **warm copy**, not a synchronous replica. The primary's write is the
//! one whose durability a commit waits on; a backup's is issued at the same
//! time and flushed by its own directory's log. A crash can therefore leave a
//! backup a few records behind — the window is one group commit — and the copy
//! is repaired at the next open, not continuously.
//!
//! It is also not a replacement for [`crate::replication`], which ships
//! transactions to another *process* and survives this machine. A backup
//! location survives the primary disk, and that is a different failure.
//!
//! # Why offsets stay in step
//!
//! Every provider here receives the same operations in the same order and each
//! file starts empty, so a given record lands at the same offset in all of
//! them. Row pointers, which record absolute offsets, are therefore valid
//! against any copy — which is what makes a backup directory directly usable
//! as a database rather than only as an archive.
//!
//! The one thing that can break that is a crash, and it is checked rather than
//! assumed: [`MirrorStorageProvider::attach`] compares lengths when a file is
//! opened and rewrites any backup that disagrees.

use std::io::{self, SeekFrom};

use log::{debug, error, info};
use rclite::Arc;

use super::super::storage_providers::file_sync::LocalStorageProvider;
use super::super::storage_providers::traits::{BorrowedBytes, IOResult, StorageIO};

/// How much is copied at a time when a backup is rebuilt. A table is not
/// bounded by memory and neither should its resync be.
const RESYNC_CHUNK: usize = 1 << 20;

#[derive(Clone)]
pub struct MirrorStorageProvider {
    /// Held by value, not behind an `Arc`: `swap_temp` needs `&mut` on the
    /// temporary's primary, and it is the same shape the plain file provider
    /// already has.
    primary: LocalStorageProvider,
    /// Never empty: a mirror with no backups is just its primary, and the pool
    /// builds a plain `File` provider in that case. Shared rather than cloned,
    /// because cloning a provider re-maps its file and a mirror would otherwise
    /// pay that once per backup on every clone.
    backups: Arc<Vec<LocalStorageProvider>>,
}

impl std::fmt::Debug for MirrorStorageProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MirrorStorageProvider")
            .field("primary", &self.primary.get_name())
            .field("backups", &self.backups.len())
            .finish()
    }
}

impl MirrorStorageProvider {
    /// Opens `name` in `primary_location` and in every one of `backup_locations`,
    /// bringing any backup that has fallen behind back into step.
    pub async fn attach(primary_location: &str, backup_locations: &[String], name: &str) -> Self {
        let primary = LocalStorageProvider::new(primary_location, Some(name)).await;

        let mut backups = Vec::with_capacity(backup_locations.len());
        for location in backup_locations {
            let backup = LocalStorageProvider::new(location, Some(name)).await;
            resync(&primary, &backup, name).await;
            backups.push(backup);
        }

        Self {
            primary,
            backups: Arc::new(backups),
        }
    }

    /// The provider every read goes to.
    pub fn primary(&self) -> &LocalStorageProvider {
        &self.primary
    }

    pub fn backup_count(&self) -> usize {
        self.backups.len()
    }

    /// Rewrites every backup from the primary.
    ///
    /// Called after an operation that replaces the primary's contents wholesale
    /// rather than appending to them — compaction, which swaps a rebuilt file
    /// in underneath. Copying the difference is not an option there: nothing
    /// about the new file's bytes relates to the old one's.
    async fn rebuild_backups(&self) {
        for backup in self.backups.iter() {
            force_resync(&self.primary, backup).await;
        }
    }
}

/// Brings `backup` into step with `primary` if their lengths disagree.
///
/// Length, not content. A full comparison would read both files end to end
/// every time a table opens, which on a large table costs more than the copy it
/// is trying to avoid. Lengths can only diverge through a crash, since every
/// write reaches both providers in the same order, and a crash that leaves the
/// lengths equal has left the same records in both.
async fn resync(primary: &LocalStorageProvider, backup: &LocalStorageProvider, name: &str) {
    let primary_len = primary.get_len().await;
    let backup_len = backup.get_len().await;
    if primary_len == backup_len {
        return;
    }

    info!(
        "backup copy of {} in {} is {} bytes against the primary's {}; rebuilding it",
        name,
        backup.get_location().unwrap_or_default(),
        backup_len,
        primary_len
    );
    force_resync(primary, backup).await;
}

/// Replaces `backup`'s contents with `primary`'s, whatever they currently are.
async fn force_resync(primary: &LocalStorageProvider, backup: &LocalStorageProvider) {
    let primary_len = primary.get_len().await;

    // Emptied first, then rewritten. Appending the tail would be cheaper and is
    // only correct if the shared prefix is identical — which is exactly what a
    // length mismatch gives no grounds to assume.
    if let Err(e) = backup.truncate(0).await {
        error!(
            "could not empty the backup copy at {}: {}. It is left as it was, and its \
             contents do not match the primary",
            backup.get_name(),
            e
        );
        return;
    }

    let mut position = 0u64;
    let mut buffer = vec![0u8; RESYNC_CHUNK.min(primary_len.max(1) as usize)];
    while position < primary_len {
        let take = ((primary_len - position) as usize).min(buffer.len());
        let mut cursor = position;
        if let Err(e) = primary
            .read_data_into_buffer(&mut cursor, &mut buffer[..take])
            .await
        {
            error!(
                "could not read {} to rebuild its backup copy: {}. The copy is incomplete",
                primary.get_name(),
                e
            );
            return;
        }
        backup.append_data(&buffer[..take], false).await;
        position += take as u64;
    }

    let token = backup.begin_txn();
    if let Err(e) = backup.commit_durable(token).await {
        error!(
            "the rebuilt backup copy at {} is not durable: {}",
            backup.get_name(),
            e
        );
        return;
    }
    debug!(
        "rebuilt backup copy of {} ({} bytes)",
        backup.get_name(),
        primary_len
    );
}

impl StorageIO for MirrorStorageProvider {
    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        self.primary.verify_data(position, buffer).await
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        self.primary.write_data(position, buffer).await;
        for backup in self.backups.iter() {
            backup.write_data(position, buffer).await;
        }
    }

    async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
        self.primary.write_data_seek(seek, buffer).await;
        for backup in self.backups.iter() {
            backup.write_data_seek(seek, buffer).await;
        }
    }

    async fn read_data_into_buffer(&self, position: &mut u64, buffer: &mut [u8]) -> io::Result<()> {
        self.primary.read_data_into_buffer(position, buffer).await
    }

    async fn read_vectored(&self, reads: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        self.primary.read_vectored(reads).await
    }

    #[inline(always)]
    fn try_read_vectored_sync(&self, reads: &mut [(u64, &mut [u8])]) -> bool {
        self.primary.try_read_vectored_sync(reads)
    }

    async fn append_data(&self, buffer: &[u8], immediate: bool) {
        let _ = self.append_data_returning_offset(buffer, immediate).await;
    }

    async fn append_data_returning_offset(&self, buffer: &[u8], immediate: bool) -> u64 {
        let offset = self
            .primary
            .append_data_returning_offset(buffer, immediate)
            .await;
        for backup in self.backups.iter() {
            // Always the immediate path, whatever the caller asked for. The
            // deferred one stages into a per-file temp buffer that its own
            // background task lands later — so a backup would hold bytes in a
            // `.tmp` that the primary already has in its data file, and the two
            // directories would only agree once that task happened to run. A
            // backup directory has to be openable as a database at any moment,
            // which means its *files* have to be current, not its staging area.
            // The cost is bounded: this path defers the `fsync` too, so it is a
            // write, not a flush.
            let mirrored = backup.append_data_returning_offset(buffer, true).await;
            warn_on_drift(backup, offset, mirrored);
        }
        offset
    }

    fn begin_txn(&self) -> u64 {
        // The primary's log owns the transaction. A backup's write is not part
        // of it: the two directories have separate logs, so one token cannot
        // name a transaction in both, and a commit that waited for the backup's
        // log would put a second `fsync` on the latency of every statement.
        self.primary.begin_txn()
    }

    async fn append_data_txn(&self, txn: u64, buffer: &[u8]) -> (u64, u64) {
        let (offset, token) = self.primary.append_data_txn(txn, buffer).await;
        for backup in self.backups.iter() {
            // Into the backup's own data file, under its own log — see
            // `append_data_returning_offset` for why this is not the deferred
            // path. The `fsync` is still deferred, and `sync_durable` walks
            // every pool directory before a statement reports success.
            let mirrored = backup.append_data_returning_offset(buffer, true).await;
            warn_on_drift(backup, offset, mirrored);
        }
        (offset, token)
    }

    async fn write_data_txn(&self, txn: u64, position: u64, buffer: &[u8]) -> u64 {
        let token = self.primary.write_data_txn(txn, position, buffer).await;
        for backup in self.backups.iter() {
            backup.write_data(position, buffer).await;
        }
        token
    }

    async fn commit_durable(&self, token: u64) -> IOResult<()> {
        // The primary's result is the statement's. A backup that fails to
        // commit has fallen behind, which the next open repairs; reporting the
        // statement as failed for it would turn a redundant copy into a second
        // way to lose a write.
        let outcome = self.primary.commit_durable(token).await;
        for backup in self.backups.iter() {
            if let Err(e) = backup.commit_durable(backup.begin_txn()).await {
                error!("backup copy at {} is not durable: {}", backup.get_name(), e);
            }
        }
        outcome
    }

    #[inline(always)]
    fn try_borrow_range(&self, position: u64, len: usize) -> Option<BorrowedBytes> {
        self.primary.try_borrow_range(position, len)
    }

    async fn truncate(&self, len: u64) -> IOResult<()> {
        let outcome = self.primary.truncate(len).await;
        if outcome.is_ok() {
            for backup in self.backups.iter() {
                if let Err(e) = backup.truncate(len).await {
                    error!(
                        "could not truncate the backup copy at {}: {}",
                        backup.get_name(),
                        e
                    );
                }
            }
        }
        outcome
    }

    async fn get_len(&self) -> u64 {
        self.primary.get_len().await
    }

    fn exists(location: &str, table_name: &str) -> bool {
        LocalStorageProvider::exists(location, table_name)
    }

    async fn create_temp(&self) -> Self {
        // The scratch file has one copy. It is consumed by `swap_temp`, which
        // rebuilds the backups from the result — mirroring the scratch as well
        // would double the write cost of a compaction for a file that does not
        // outlive it.
        Self {
            primary: self.primary.create_temp().await,
            backups: Arc::new(Vec::new()),
        }
    }

    async fn swap_temp(&self, temp_io_sync: &mut Self) {
        self.primary.swap_temp(&mut temp_io_sync.primary).await;
        // The primary's contents were replaced wholesale, so nothing about the
        // backups' bytes still corresponds. Copied rather than diffed.
        self.rebuild_backups().await;
    }

    fn get_location(&self) -> Option<String> {
        self.primary.get_location()
    }

    #[allow(refining_impl_trait)]
    async fn create_new(&self, name: String) -> Self {
        let primary = self.primary.create_new(name.clone()).await;
        let mut backups = Vec::with_capacity(self.backups.len());
        for backup in self.backups.iter() {
            let copy = backup.create_new(name.clone()).await;
            resync(&primary, &copy, &name).await;
            backups.push(copy);
        }
        Self {
            primary,
            backups: Arc::new(backups),
        }
    }

    #[allow(refining_impl_trait)]
    async fn create_new_at(&self, location: &str, name: String) -> Self {
        // The primary moves; the backup set does not. A backup location is a
        // property of the database, not of the file being opened.
        let primary = LocalStorageProvider::new(location, Some(&name)).await;
        let mut backups = Vec::with_capacity(self.backups.len());
        for backup in self.backups.iter() {
            let copy = backup.create_new(name.clone()).await;
            resync(&primary, &copy, &name).await;
            backups.push(copy);
        }
        Self {
            primary,
            backups: Arc::new(backups),
        }
    }

    fn drop_io(&self) {
        self.primary.drop_io();
        for backup in self.backups.iter() {
            backup.drop_io();
        }
    }

    fn get_hash(&self) -> u32 {
        self.primary.get_hash()
    }

    async fn start_service(&self) {
        // The backups stage writes through the same temp-file machinery the
        // primary does, so each needs its own flush loop or those bytes sit in
        // a `.tmp` until the next open.
        let primary = self.primary.start_service();
        let backups =
            futures::future::join_all(self.backups.iter().map(|backup| backup.start_service()));
        futures::join!(primary, backups);
    }

    fn get_name(&self) -> String {
        self.primary.get_name()
    }
}

/// A backup that lands a record at a different offset than the primary is no
/// longer a usable copy, and silently carrying on would leave a directory that
/// looks like a database and reads as corruption.
fn warn_on_drift(backup: &LocalStorageProvider, primary_offset: u64, backup_offset: u64) {
    if primary_offset != backup_offset {
        error!(
            "backup copy at {} took offset {} where the primary took {}; \
             this copy is no longer byte-identical and will be rebuilt at the next open",
            backup.get_name(),
            backup_offset,
            primary_offset
        );
    }
}
