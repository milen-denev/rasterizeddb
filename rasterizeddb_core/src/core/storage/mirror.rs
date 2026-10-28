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
    primary: std::sync::Arc<LocalStorageProvider>,
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
            primary: std::sync::Arc::new(primary),
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
    // A copy the last run left behind its primary (`BehindMark`): the same
    // length says nothing about the bytes, so it is rebuilt whatever its length.
    if let Some(mark) = BehindMark::path(backup, name)
        && mark.exists()
    {
        info!("backup copy of {} in {} was left behind its primary; rebuilding it", name, backup.get_location().unwrap_or_default());
        force_resync(primary, backup).await;
        BehindMark::forget(&mark);
        return;
    }
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
            write_backup_or_mark(backup, &self.primary.get_name(), position, buffer).await;
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

    fn defers_in_place_writes(&self) -> bool {
        // Never: whatever the primary does, `write_data_txn` writes every
        // backup at once, outside the transaction. An UPDATE that overwrote a
        // row in place and then failed left the backup holding the new bytes
        // and the primary the old.
        false
    }

    async fn write_data_txn(&self, txn: u64, position: u64, buffer: &[u8]) -> u64 {
        let token = self.primary.write_data_txn(txn, position, buffer).await;
        if token == 0 {
            // The primary wrote it at once (no log): so do the backups.
            for backup in self.backups.iter() {
                write_backup_or_mark(backup, &self.primary.get_name(), position, buffer).await;
            }
        } else {
            pending::note(&self.primary, &self.backups, token, position, buffer.len() as u64);
        }
        token
    }

    /// The primary's transaction, which the primary's log holds. Without
    /// this the trait's no-op ran: an aborted UPDATE's transaction stayed
    /// pending in the log for the life of the process, and every checkpoint
    /// after it declined, so the log grew without bound. A backup holds no
    /// transaction of its own (its writes go straight to its files).
    async fn abort_txn(&self, token: u64) {
        pending::take(&self.primary, token);
        self.primary.abort_txn(token).await
    }

    async fn commit_durable(&self, token: u64) -> IOResult<()> {
        // The primary's result is the statement's. A backup that fails to
        // commit has fallen behind, which the next open repairs; reporting the
        // statement as failed for it would turn a redundant copy into a second
        // way to lose a write.
        // Every file of this directory's transaction, not only this one: the
        // rows file's in-place writes (a compaction, an insert into a
        // reclaimed extent, a whole-row rewrite) are committed through the
        // pointers file, and a copy kept per file was never made for them.
        let copies = pending::take(&self.primary, token);
        // Before the primary commits: a crash between its commit and the
        // copy leaves each backup the same length and different bytes, which
        // no length check sees. The mark makes the next open rebuild it.
        let marks: Vec<BehindMark> = copies.iter().flat_map(|c| c.mark_behind()).collect();
        let outcome = self.primary.commit_durable(token).await;
        if outcome.is_ok() {
            let mut copied = true;
            #[cfg(test)]
            let cut_off = !copies.is_empty() && test_hooks::cut_off(&self.primary);
            #[cfg(not(test))]
            let cut_off = false;
            if cut_off {
                copied = false;
            } else {
                for copy in &copies {
                    copied &= copy.copy().await;
                }
            }
            for mark in marks {
                mark.release(copied);
            }
        } else if !copies.is_empty() {
            // Whatever of the commit reached the primary's file did not reach
            // the copies: the marks stay.
            for mark in marks {
                mark.release(false);
            }
            error!(
                "the primary {} did not commit transaction {token}; its in-place writes were not copied, and \
                 the backups are rebuilt at the next open",
                self.primary.get_name()
            );
        }
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
            primary: std::sync::Arc::new(self.primary.create_temp().await),
            backups: Arc::new(Vec::new()),
        }
    }

    async fn swap_temp(&self, temp_io_sync: &mut Self) {
        let temp = std::sync::Arc::get_mut(&mut temp_io_sync.primary)
            .expect("a temporary's primary is held by the temporary alone");
        self.primary.swap_temp(temp).await;
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
            primary: std::sync::Arc::new(primary),
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
            primary: std::sync::Arc::new(primary),
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

/// Writes `buffer` at `position` in `backup` and commits it; whether it
/// landed. `write_data` only logs a failure, and a copy that went on to
/// release its `BehindMark` left a same-length divergence nothing sees.
async fn write_backup(backup: &LocalStorageProvider, position: u64, buffer: &[u8]) -> bool {
    let txn = backup.begin_txn();
    let token = backup.write_data_txn(txn, position, buffer).await;
    match backup.commit_durable(token).await {
        Ok(()) => true,
        Err(e) => {
            error!(
                "could not write {} bytes at {} to the backup copy {} in {}: {}; it is rebuilt at the next open",
                buffer.len(),
                position,
                backup.get_name(),
                backup.get_location().unwrap_or_default(),
                e
            );
            false
        }
    }
}

/// [`write_backup`] for a write no mark covers yet: a failure leaves one,
/// stuck, so the next open rebuilds the copy.
async fn write_backup_or_mark(backup: &LocalStorageProvider, name: &str, position: u64, buffer: &[u8]) {
    if !write_backup(backup, position, buffer).await
        && let Some(mark) = BehindMark::make(backup, name)
    {
        mark.release(false);
    }
}

/// A durable "this backup copy is behind its primary" mark: a file beside the
/// copy, made (and its directory synced) before the primary commits writes
/// the copy has not had, removed once no commit is between its primary's
/// commit and its copy. `resync` rebuilds a copy with one at the next open,
/// whatever its length.
///
/// Counted: two commits to one file overlap, and the first to finish its
/// copy must not remove the mark the other is still relying on. A copy that
/// failed leaves the mark for the rest of the process (`stuck`), so a later
/// commit's successful copy does not remove it either.
pub(crate) struct BehindMark {
    path: std::path::PathBuf,
    state: std::sync::Arc<parking_lot::Mutex<MarkState>>,
}

#[derive(Default)]
struct MarkState {
    in_flight: usize,
    stuck: bool,
}

static MARKS: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<std::path::PathBuf, std::sync::Arc<parking_lot::Mutex<MarkState>>>>,
> = std::sync::LazyLock::new(Default::default);

impl BehindMark {
    const PREFIX: &'static str = "__mirror_behind.";

    fn path(backup: &LocalStorageProvider, name: &str) -> Option<std::path::PathBuf> {
        backup
            .get_location()
            .map(|dir| std::path::Path::new(&dir).join(format!("{}{}", Self::PREFIX, name)))
    }

    fn make(backup: &LocalStorageProvider, name: &str) -> Option<Self> {
        let path = Self::path(backup, name)?;
        let state = MARKS.lock().entry(path.clone()).or_default().clone();
        let mut held = state.lock();
        // Made while the count is held, so a commit that finds the count
        // already raised also finds the mark already durable.
        if held.in_flight == 0
            && let Err(e) = std::fs::write(&path, b"")
                .and_then(|()| crate::core::wal::sync_parent_dir(&path))
        {
            error!("could not mark the backup copy {} as behind its primary: {}", path.display(), e);
        }
        held.in_flight += 1;
        drop(held);
        Some(Self { path, state })
    }

    /// The commit this mark covered is done; `copied` says whether its
    /// writes reached the copy.
    fn release(self, copied: bool) {
        let mut held = self.state.lock();
        held.in_flight = held.in_flight.saturating_sub(1);
        held.stuck |= !copied;
        if held.in_flight == 0 && !held.stuck {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    /// The copy was rebuilt at open: forgets the mark.
    fn forget(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        MARKS.lock().remove(path);
    }
}

/// In-place writes under a transaction that the backups have not had yet,
/// per (primary directory, transaction), across every file of it: a
/// transaction's writes to one file can be committed or aborted through
/// another's provider. Ranges, not bytes: they are read back from the
/// primary once it commits, so a compaction's whole-file rewrite costs a
/// list of ranges here, not a second copy of the file.
mod pending {
    use super::{BehindMark, LocalStorageProvider};
    use crate::core::storage_providers::traits::StorageIO;
    use std::collections::HashMap;
    use std::sync::Arc;

    pub(super) struct Copy {
        primary: Arc<LocalStorageProvider>,
        backups: rclite::Arc<Vec<LocalStorageProvider>>,
        ranges: Vec<(u64, u64)>,
    }

    const MAX_RANGES: usize = 4096;

    type Key = (String, u64);
    static PENDING: std::sync::LazyLock<parking_lot::Mutex<HashMap<Key, Vec<Copy>>>> =
        std::sync::LazyLock::new(Default::default);

    fn key(primary: &LocalStorageProvider, token: u64) -> Key {
        (primary.get_location().unwrap_or_default(), token)
    }

    pub(super) fn note(
        primary: &Arc<LocalStorageProvider>,
        backups: &rclite::Arc<Vec<LocalStorageProvider>>,
        token: u64,
        position: u64,
        len: u64,
    ) {
        let mut map = PENDING.lock();
        let copies = map.entry(key(primary, token)).or_default();
        let copy = match copies.iter_mut().find(|c| Arc::ptr_eq(&c.primary, primary)) {
            Some(copy) => copy,
            None => {
                copies.push(Copy { primary: primary.clone(), backups: backups.clone(), ranges: Vec::new() });
                copies.last_mut().expect("just pushed")
            }
        };
        // Merged with the last range when they touch: a rewrite's writes
        // arrive in order.
        match copy.ranges.last_mut() {
            Some((start, end)) if position <= *end && position + len >= *start => {
                *start = (*start).min(position);
                *end = (*end).max(position + len);
            }
            _ => copy.ranges.push((position, position + len)),
        }
        // Bounded: past this many scattered writes the list becomes the one
        // span that covers them all, which copies more but holds 16 bytes.
        if copy.ranges.len() > MAX_RANGES {
            let start = copy.ranges.iter().map(|r| r.0).min().unwrap_or(0);
            let end = copy.ranges.iter().map(|r| r.1).max().unwrap_or(0);
            copy.ranges.clear();
            copy.ranges.push((start, end));
        }
    }

    /// Everything noted for this directory's transaction, which is then
    /// forgotten (committed or aborted).
    pub(super) fn take(primary: &LocalStorageProvider, token: u64) -> Vec<Copy> {
        PENDING.lock().remove(&key(primary, token)).unwrap_or_default()
    }

    /// How many of `location`'s transactions have copies waiting, for tests.
    #[cfg(test)]
    pub(crate) fn waiting_in(location: &str) -> usize {
        PENDING.lock().keys().filter(|(l, _)| l == location).count()
    }

    impl Copy {
        pub(super) fn mark_behind(&self) -> Vec<BehindMark> {
            let name = self.primary.get_name();
            self.backups.iter().filter_map(|b| BehindMark::make(b, &name)).collect()
        }

        /// Copies the ranges from the primary, now committed, to every
        /// backup; whether every read and write succeeded.
        pub(super) async fn copy(&self) -> bool {
            let mut copied = true;
            let mut buffer = Vec::new();
            for &(start, end) in &self.ranges {
                let mut at = start;
                while at < end {
                    let take = ((end - at) as usize).min(super::RESYNC_CHUNK);
                    buffer.resize(take, 0);
                    let mut cursor = at;
                    if let Err(e) = self.primary.read_data_into_buffer(&mut cursor, &mut buffer).await {
                        log::error!(
                            "could not read {} to copy a committed write to its backups: {}; they are rebuilt at \
                             the next open",
                            self.primary.get_name(),
                            e
                        );
                        return false;
                    }
                    for backup in self.backups.iter() {
                        copied &= super::write_backup(backup, at, &buffer).await;
                    }
                    at += take as u64;
                }
            }
            copied
        }
    }
}

/// Failures no file can be made to produce on demand.
#[cfg(test)]
pub(crate) mod test_hooks {
    use crate::core::storage_providers::traits::StorageIO;

    /// Directories whose next commit with in-place writes to copy leaves them
    /// uncopied (and the mark in place), as a crash between the primary's
    /// commit and the copy would. Taken by that commit.
    pub(crate) static CUT_OFF_COPY: parking_lot::Mutex<Vec<String>> = parking_lot::Mutex::new(Vec::new());

    pub(super) fn cut_off(primary: &super::LocalStorageProvider) -> bool {
        let location = primary.get_location().unwrap_or_default();
        let mut armed = CUT_OFF_COPY.lock();
        match armed.iter().position(|l| *l == location) {
            Some(i) => {
                armed.swap_remove(i);
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
pub(crate) use pending::waiting_in as pending_copies_in;

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
