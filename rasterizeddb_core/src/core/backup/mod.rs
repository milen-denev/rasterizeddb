//! Snapshot and point-in-time backup to any S3-compatible object store.
//!
//! Behind the off-by-default `s3_backup` feature.
//!
//! # The shape of a backup
//!
//! A *snapshot* is the whole database directory, packed into one archive,
//! compressed, and written as a single object. A *segment bundle* is the
//! write-ahead log records cut since the last one, compressed the same way.
//! Together they answer two different questions: the snapshot says what the
//! database looked like at some point, and the bundles say everything that
//! happened after it.
//!
//! # Why the snapshot may be taken while writes are in flight
//!
//! Stopping the database to copy it would make a backup a maintenance window,
//! and the window would grow with the data. Instead the copy is *fuzzy*: files
//! are read while writers keep appending, so the result is a mix of states that
//! is not, on its own, consistent.
//!
//! What makes it consistent is the log. Records are physical redo — "these
//! bytes belong at this offset in this file" — so replaying them is idempotent
//! and it does not matter whether the copy already caught a given write. The
//! snapshot therefore records the commit sequence at the moment the copy
//! started and again at the moment it finished, and a restore replays the
//! archived log across that whole window. Below `end_lsn` the copy is not yet
//! usable; at or above it, it is exactly the database.
//!
//! This is why the log is archived rather than discarded at a checkpoint, and
//! why a database with the log disabled can be snapshotted but not recovered to
//! a point in time.
//!
//! # Compaction
//!
//! One kind of write is *not* redo-logged: compaction moves rows and then
//! shortens the file, and a truncation has no record that replay could undo. A
//! compaction running underneath a fuzzy copy could therefore produce an
//! archive that no amount of replay repairs. [`compaction_permit`] closes that:
//! a snapshot holds the gate exclusively for its duration and compaction waits.
//!
//! # Staged appends
//!
//! There is a second unlogged path. `StorageIO::append_data` with `immediate`
//! false stages bytes in a `.tmp` sidecar, which a background task later folds
//! into the main file — and neither the staging nor the fold writes a log
//! record. A snapshot captures the sidecars along with everything else, so
//! staged bytes are restored and folded in when the restored database is
//! opened, exactly as a restart of the original would fold them.
//!
//! What that does not survive is a *fold landing in the middle of the copy*:
//! the main file would be captured before it gained the bytes and the sidecar
//! after it lost them, so they would be in neither. No write path in the engine
//! currently stages — every caller passes `immediate` — so this cannot happen
//! today, and rather than leave a trap for the day one does,
//! `check_staged_appends` compares the sidecars before and after the copy and
//! fails the snapshot instead of uploading a lossy one.
//!
//! # What it costs in requests
//!
//! The design target is that request count scales with *time*, not with write
//! volume or object count. A snapshot is one multipart upload — or a single
//! `PUT` when it compresses small enough — plus one manifest write. An archive
//! flush bundles every log segment cut since the last one into one object, so a
//! database writing ten times as fast makes the same number of requests. The
//! catalogue lives in a single `MANIFEST.json`, so planning a restore is one
//! `GET` rather than a paged listing plus a probe per segment. Retention prunes
//! through the batch-delete API, 1000 keys per request.
//!
//! At the defaults — a daily snapshot and a five-minute archive flush — that is
//! roughly 600 requests a day regardless of how busy the database is.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use cacheguard::CacheGuard;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use log::{debug, error, info, warn};
use tokio::sync::{RwLock, Semaphore};

pub mod archive;
pub mod manifest;
#[cfg(test)]
mod pitr_tests;
#[cfg(test)]
mod upload_window_tests;
pub mod restore;
pub mod s3;
/// An in-process S3 for tests; see its module docs.
#[doc(hidden)]
pub mod fake_s3;
pub mod timeutil;

use manifest::{Manifest, RecoveryTarget, SegmentEntry, SnapshotEntry};
use s3::{Credentials, Endpoint, S3Client, S3Config};

/// Directory inside the database where cut log segments wait to be uploaded.
pub const STAGING_DIR: &str = "wal_archive";

/// File holding this database's backup identity.
const INSTANCE_FILE: &str = "backup_instance.id";

/// Stream key, and archive directory, of a storage location outside the
/// backed-up directory: this followed by a hash of the location's path.
///
/// Not a path relative to the directory, so it cannot collide with the stream
/// of a database created inside it — the restore puts such a location's files
/// somewhere under a different name ([`RESTORED_LOCATION_PREFIX`]).
pub const LOCATION_KEY_PREFIX: &str = "~loc-";

/// What a restore into a new directory names the subdirectory a location's
/// files are placed in. They cannot go back where they came from: that is the
/// source's disk, still in use.
pub const RESTORED_LOCATION_PREFIX: &str = "pool-location-";

/// The stream key of the storage location at `path`.
fn location_key(path: &str) -> String {
    format!(
        "{LOCATION_KEY_PREFIX}{:016x}",
        crate::core::wal::file_id_for_name(&crate::core::wal::dir_key(path))
    )
}

/// `rasterized.wal.floors`: see [`crate::core::torn_tail::Floors`].
const FLOORS_FILE: &str = "rasterized.wal.floors";

/// Files never captured in a snapshot.
///
/// The live log is excluded because it is replayed *into* a restore rather than
/// restored, and a stale copy of it in the target directory would be replayed a
/// second time on first open. `temp.db` is compaction scratch, and the instance
/// id belongs to the database that made the backup, not the one restoring it.
const EXCLUDED: &[&str] = &[
    crate::core::wal::DEFAULT_WAL_NAME,
    // The durable floors name the source's files by inode; a restore's are
    // new files, and its first open records their own.
    FLOORS_FILE,
    INSTANCE_FILE,
    "temp.db",
    // Holds copies of what is being uploaded, one subdirectory per log.
    // Backing it up would nest a snapshot inside a snapshot; the walk below
    // dropped its `.wseg` files by suffix but kept a `.partial` copy of a log
    // that happened to be mid-flight.
    STAGING_DIR,
];

/// Whether a file is left out of every snapshot: [`EXCLUDED`], and the
/// index-store files (`.ixs`, `.ixm` and their `.tmp`s; persistent indexes
/// §10). An open trusts a snapshot its marker log proves (B14), and a fuzzy
/// walk could pair a copied `CLEAN` with rows changed after it; a restore
/// rebuilds instead. `rdb.ixe` is kept: it is WAL'd.
fn excluded_file(name: &str) -> bool {
    EXCLUDED.contains(&name)
        || crate::core::index_store::file_gate::INDEX_STORE_SUFFIXES
            .iter()
            .any(|s| name.ends_with(s))
}

/// S3 minimum for any part but the last.
const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// S3 caps a multipart upload at 10 000 parts.
const MAX_PARTS: u64 = 10_000;

/// First wait after the base snapshot fails, before the backoff doubles it.
const BASE_RETRY_START: Duration = Duration::from_secs(60);

/// Slowest the base snapshot is retried at, however long it keeps failing.
const BASE_RETRY_CEILING: Duration = Duration::from_secs(3600);

#[derive(Clone, Debug)]
pub struct BackupConfig {
    pub bucket: String,
    /// Key prefix within the bucket. One database per prefix.
    pub prefix: String,
    pub region: String,
    pub endpoint: Endpoint,
    pub credentials: Credentials,
    pub path_style: bool,

    /// zstd level for snapshots. Higher trades CPU for a smaller object.
    pub snapshot_level: i32,
    /// zstd level for log bundles. Lower by default: they are written far more
    /// often, are already small, and their compression happens on the same
    /// machine that is serving queries.
    pub segment_level: i32,
    /// Compression threads, or 0 to size from the machine.
    pub compression_threads: u32,
    /// Enables long-distance matching, which finds repeats far beyond the
    /// normal window. Database files repeat their record framing across
    /// megabytes, so this is most of the ratio on a large snapshot.
    pub long_range: bool,

    pub part_size: usize,
    pub upload_concurrency: usize,
    pub max_attempts: u32,

    /// Seconds between automatic snapshots, or 0 for manual only.
    pub snapshot_interval_secs: u64,
    /// Seconds between archive flushes, or 0 for manual only.
    pub wal_flush_secs: u64,
    /// Staged bytes that trigger a flush without waiting for the interval.
    pub wal_bundle_bytes: u64,
    /// Snapshots kept. Older ones, and the segments only they needed, are
    /// deleted. 0 keeps everything.
    pub retain_snapshots: usize,
    /// Seconds between rollovers, or 0 to never roll over.
    ///
    /// A rollover takes a fresh full snapshot and, once it has landed, deletes
    /// everything the new one does not need — so the bucket returns to holding
    /// one self-contained generation. It is retention measured in time rather
    /// than in snapshot count, and it is what stops a long-lived database from
    /// accumulating an ever-longer chain of segments that a restore has to
    /// replay.
    ///
    /// The order matters and is not negotiable: the new snapshot must be
    /// complete before anything old is removed. A rollover that deleted first
    /// would leave a window with no restorable backup at all.
    pub rollover_interval_secs: u64,
    /// Staged log bytes after which archiving is suspended. 0 is unlimited.
    ///
    /// The failure this exists for: uploads start failing — expired
    /// credentials, a bucket policy change, the network — and every checkpoint
    /// keeps handing segments to a staging directory nobody is draining. Left
    /// alone that fills the disk, and a full disk stops the database. Losing
    /// point-in-time coverage is bad; taking the database down to preserve it
    /// is worse, so past this ceiling checkpoints go back to discarding the log
    /// and the operator is told loudly.
    pub staging_max_bytes: u64,
    /// Files at least this large are stored as objects of their own, and a
    /// later snapshot whose copy of one hashes the same reuses the object
    /// instead of uploading it again. 0 — the default — takes every snapshot
    /// as one self-contained archive, as before.
    ///
    /// What this trades: a snapshot of a mostly-idle database uploads only
    /// what changed, but its restore then depends on objects earlier
    /// snapshots wrote (retention keeps them while referenced), and files
    /// outside the archive lose long-distance matching against each other.
    pub incremental_min_bytes: u64,
}

impl BackupConfig {
    /// Normalises the prefix to `something/`, or empty for the bucket root.
    pub fn normalized_prefix(&self) -> String {
        let trimmed = self.prefix.trim_matches('/');
        if trimmed.is_empty() {
            String::new()
        } else {
            format!("{trimmed}/")
        }
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}{}", self.normalized_prefix(), suffix)
    }
}

/// Blocks compaction while a snapshot copies files.
static COMPACTION_GATE: OnceLock<RwLock<()>> = OnceLock::new();

fn compaction_gate() -> &'static RwLock<()> {
    COMPACTION_GATE.get_or_init(|| RwLock::new(()))
}

/// Taken by anything that rewrites a file in place with no redo record behind
/// it — compaction, principally.
///
/// Held shared, so any number of tables compact at once; a snapshot takes it
/// exclusively and they wait. Cheap when no backup is configured: an uncontended
/// read lock is one atomic.
pub async fn compaction_permit() -> tokio::sync::RwLockReadGuard<'static, ()> {
    compaction_gate().read().await
}

/// Keeps a log barrier and the deletion it announces on one side of the
/// point a snapshot's copy begins.
///
/// A point-in-time restore empties a restored file at its barrier only when
/// the barrier committed after that point (see
/// [`crate::core::wal::Wal::recover_restoring`]): the copy then may hold the
/// deleted file. That is right only if the deletion is on the same side as
/// the barrier. A barrier just before the point with its file deleted after
/// the copy read it left the deleted file's bytes in a copy the restore took
/// for the next file's.
///
/// Its own gate, not the compaction gate. A snapshot holds that one for its
/// whole upload, minutes over a slow link, and every `DROP`, rename, move
/// and `ALTER` rewrite waited that long, process-wide. A barrier and its
/// deletion both after the point are emptied correctly however long the
/// copy runs, so the snapshot holds this gate only while it cuts the logs
/// and reads where its copy begins.
static DELETION_GATE: OnceLock<RwLock<()>> = OnceLock::new();

fn deletion_gate() -> &'static RwLock<()> {
    DELETION_GATE.get_or_init(|| RwLock::new(()))
}

/// Held shared from a log barrier through the deletion of the files it
/// voided; see [`DELETION_GATE`].
pub async fn deletion_permit() -> tokio::sync::RwLockReadGuard<'static, ()> {
    deletion_gate().read().await
}

/// Keeps a rename and a move from running while a snapshot copies files.
///
/// Both copy a table's files to new ones with no log record behind the
/// copy, then delete the old ones. During a snapshot's upload the old files
/// are ones it listed, and the new ones are not in it: the copy failed on
/// the first missing file, and could not leave it out, because nothing but
/// that file holds its bytes, not the log a restore replays, so the table
/// would be missing from the snapshot and every point-in-time restore after
/// it. A `DROP` is different and does not wait (see [`DELETION_GATE`]).
///
/// A snapshot takes it exclusively first, before the compaction gate, and
/// holds it until it has listed its files again after the copy; a rename or
/// a move holds it shared from start to finish, and takes the deletion gate
/// inside it, so the order is the same on both sides.
static RELOCATION_GATE: OnceLock<RwLock<()>> = OnceLock::new();

fn relocation_gate() -> &'static RwLock<()> {
    RELOCATION_GATE.get_or_init(|| RwLock::new(()))
}

/// Held shared by a rename or a move for its whole run; see
/// [`RELOCATION_GATE`].
pub async fn relocation_permit() -> tokio::sync::RwLockReadGuard<'static, ()> {
    relocation_gate().read().await
}

/// The deletion gate as a snapshot holds it while it fixes where its copy
/// begins, for tests.
#[cfg(test)]
pub(crate) async fn hold_snapshot_gate() -> tokio::sync::RwLockWriteGuard<'static, ()> {
    deletion_gate().write().await
}

/// The compaction gate as a snapshot's copy holds it, for tests.
#[cfg(test)]
pub(crate) async fn hold_compaction_gate() -> tokio::sync::RwLockWriteGuard<'static, ()> {
    compaction_gate().write().await
}

static SERVICE: OnceLock<Arc<BackupService>> = OnceLock::new();

/// The configured backup service, if one was installed at start-up.
pub fn service() -> Option<Arc<BackupService>> {
    SERVICE.get().cloned()
}

/// What a completed snapshot did.
#[derive(Debug, Clone)]
pub struct SnapshotReport {
    pub id: String,
    pub key: String,
    pub file_count: u32,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub elapsed: Duration,
}

impl SnapshotReport {
    pub fn ratio(&self) -> f64 {
        if self.compressed_bytes == 0 {
            return 0.0;
        }
        self.uncompressed_bytes as f64 / self.compressed_bytes as f64
    }
}

/// What an archive flush did.
#[derive(Debug, Clone, Default)]
pub struct ArchiveReport {
    pub bundles: u32,
    pub segments: u32,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
    pub first_lsn: u64,
    pub last_lsn: u64,
}

pub struct BackupService {
    config: BackupConfig,
    client: Arc<S3Client>,
    /// Database directory this service backs up.
    location: String,
    instance_id: String,
    /// The catalogue, and the lock that makes read-modify-write of it atomic
    /// within this process.
    manifest: tokio::sync::Mutex<Manifest>,
    /// One snapshot at a time. A second concurrent one would double the I/O to
    /// describe the same database.
    snapshot_lock: tokio::sync::Mutex<()>,
    /// One archive flush at a time, so two do not claim the same segments.
    archive_lock: tokio::sync::Mutex<()>,
    /// Both flags are padded. They are polled on the archive path and set
    /// almost never, and unpadded they sat next to three `tokio::sync::Mutex`
    /// fields — each of which carries its own atomic state that the locking
    /// itself writes. The flags were being invalidated by lock traffic they
    /// have nothing to do with.
    shutting_down: CacheGuard<AtomicBool>,
    /// Set when the staging directory hit its ceiling and checkpoints were told
    /// to stop handing over segments. Cleared once it drains.
    archiving_suspended: CacheGuard<AtomicBool>,
    /// Every log this service pointed at its staging directory. Suspension
    /// detaches these as well as the logs it can name now: a location whose
    /// pool file stopped parsing cannot be named, and its log kept staging
    /// past the ceiling that exists to protect the disk.
    attached: std::sync::Mutex<Vec<std::sync::Weak<crate::core::wal::Wal>>>,
}

impl BackupService {
    /// Builds the service, reads or creates the catalogue, and points the log's
    /// checkpoints at the staging directory.
    ///
    /// Returns an error rather than degrading: an operator who asked for
    /// backups and silently got none is worse off than one who fails to start.
    pub async fn connect(config: BackupConfig, location: &str) -> io::Result<Arc<Self>> {
        if location.is_empty() {
            return Err(io::Error::other(
                "backup needs a file-backed database; an in-memory one has nothing to snapshot",
            ));
        }

        let client = Arc::new(S3Client::new(S3Config {
            endpoint: config.endpoint.clone(),
            region: config.region.clone(),
            bucket: config.bucket.clone(),
            credentials: config.credentials.clone(),
            path_style: config.path_style,
            max_attempts: config.max_attempts,
        })?);

        let instance_id = read_or_create_instance_id(location)?;

        // One GET tells us whether this prefix is already in use and, if so, by
        // whom. Two servers sharing a prefix would overwrite each other's
        // catalogue and leave both unrestorable.
        let manifest_key = config.key(manifest::MANIFEST_KEY);
        let manifest = match client.get_object(&manifest_key).await {
            Ok(bytes) => {
                let existing = Manifest::decode(&bytes)?;
                if existing.instance_id != instance_id {
                    return Err(io::Error::other(format!(
                        "s3://{}/{} already holds backups for instance {}, but this database is {}. \
                         Point --s3-prefix somewhere else, or restore from that prefix instead.",
                        config.bucket, manifest_key, existing.instance_id, instance_id
                    )));
                }
                info!(
                    "Backup catalogue found at s3://{}/{}: {} snapshots, {} segment bundles",
                    config.bucket,
                    manifest_key,
                    existing.snapshots.len(),
                    existing.segments.len()
                );
                existing
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                info!(
                    "No backup catalogue at s3://{}/{}; starting a new one for instance {}",
                    config.bucket, manifest_key, instance_id
                );
                Manifest::new(instance_id.clone())
            }
            Err(e) => return Err(e),
        };

        let service = Arc::new(Self {
            config,
            client,
            location: location.to_string(),
            instance_id,
            manifest: tokio::sync::Mutex::new(manifest),
            snapshot_lock: tokio::sync::Mutex::new(()),
            archive_lock: tokio::sync::Mutex::new(()),
            shutting_down: AtomicBool::new(false).into(),
            archiving_suspended: AtomicBool::new(false).into(),
            attached: std::sync::Mutex::new(Vec::new()),
        });

        // Sweep first, then hook, and the order is load-bearing rather than
        // stylistic. `DatabaseCatalog::open` installs the log of every existing
        // sub-database *before* the backup service is installed, so a hook on
        // its own would only ever catch databases created later — which is to
        // say it would leave the original bug in place for every database that
        // was already there, while passing a test that creates one afterwards.
        // The sweep covers what exists; the hook covers what follows.
        service.attach_to_logs()?;

        // Databases created from here on announce themselves through the
        // registry rather than through the catalogue, so every path that
        // installs a log is covered. Weak, because hooks live for the process
        // and this service must not be kept alive by one.
        let weak = Arc::downgrade(&service);
        crate::core::wal::on_log_installed(move |dir, wal| {
            if let Some(service) = weak.upgrade() {
                service.attach_new_log(dir, wal);
            }
        });
        service.abandon_orphaned_uploads().await;
        Ok(service)
    }

    /// Aborts multipart uploads left behind by a previous run.
    ///
    /// A process killed mid-snapshot leaves its parts in the bucket, and S3
    /// bills for them until something reaps them. The usual answer is a bucket
    /// lifecycle rule, which is still worth having — but it is one more thing
    /// an operator has to know to set, and by the time this runs the prefix is
    /// already known to belong to *this* database and no upload of ours can be
    /// in flight, so anything listed is unambiguously an orphan.
    ///
    /// Best-effort: a provider that does not implement the call, or credentials
    /// without permission for it, must not stop the database from starting.
    async fn abandon_orphaned_uploads(&self) {
        let prefix = self.config.normalized_prefix();
        let orphans = match self.client.list_multipart_uploads(&prefix).await {
            Ok(orphans) => orphans,
            Err(e) => {
                debug!("Could not list multipart uploads under {prefix}: {e}");
                return;
            }
        };

        for (key, upload_id) in &orphans {
            match self.client.abort_multipart_upload(key, upload_id).await {
                Ok(()) => info!("Abandoned an orphaned multipart upload of {key}"),
                Err(e) => warn!("Could not abandon the orphaned upload of {key}: {e}"),
            }
        }
    }

    /// Every log under this location, keyed by its path relative to it.
    ///
    /// `""` is the database at the location itself. Every database created
    /// with `CREATE DATABASE` is a subdirectory with a log of its own — one
    /// commit sequence, one file-id space, one staging directory — and they are
    /// kept apart all the way to the restore, because a file id is a hash of a
    /// *table name*: two databases each holding `users` name it identically,
    /// and a merged replay would write one's records into the other's file.
    ///
    /// A storage location *outside* this directory that holds tables is a
    /// stream too, keyed by [`location_key`]: the snapshot copies its files, so
    /// its records have somewhere to replay into. Any other log outside it —
    /// another installation's, a backup location's — is not.
    fn log_streams(&self) -> Vec<(String, Arc<crate::core::wal::Wal>)> {
        let (streams, problems) = self.log_streams_checked();
        for problem in &problems {
            error!("Backup of {}: {problem}", self.location);
        }
        streams
    }

    /// [`Self::log_streams`], with what kept a log from being named.
    ///
    /// Degrades per database: a pool file that cannot be parsed costs that
    /// database's locations, never the root's log or anyone else's. Failing
    /// the whole list stopped every cut, every suspension at the staging
    /// ceiling and every resume, for every database, over one file. A snapshot
    /// is still refused while any problem stands — it would leave out tables.
    fn log_streams_checked(&self) -> (Vec<(String, Arc<crate::core::wal::Wal>)>, Vec<String>) {
        let root = crate::core::wal::dir_key(&self.location);
        let (locations, mut problems) = self.pool_locations();
        let mut streams: Vec<(String, Arc<crate::core::wal::Wal>)> = crate::core::wal::registered()
            .into_iter()
            .filter_map(|(dir, wal)| {
                stream_key(&root, &dir)
                    .or_else(|| {
                        locations
                            .iter()
                            .find(|(_, path)| crate::core::wal::dir_key(path) == dir)
                            .map(|(key, _)| key.clone())
                    })
                    .map(|key| (key, wal))
            })
            .collect();
        streams.sort_by(|a, b| a.0.cmp(&b.0));
        // Two logs under one key would share a staging directory and a bundle
        // stream, and a restore would replay one's records into the other's
        // files. Neither is archived.
        let mut clashing: Vec<String> = streams
            .windows(2)
            .filter(|pair| pair[0].0 == pair[1].0)
            .map(|pair| pair[0].0.clone())
            .collect();
        clashing.dedup();
        for key in &clashing {
            problems.push(format!(
                "two write-ahead logs map to the backup stream {key:?}; neither is archived"
            ));
        }
        streams.retain(|(key, _)| !clashing.contains(key));
        (streams, problems)
    }

    /// The storage locations outside this directory that hold tables, as
    /// `(stream key, path)` sorted by key, and what could not be read.
    ///
    /// Read from the pool file of every database here — the one at the
    /// location itself and each created with `CREATE DATABASE`, which has its
    /// own. Snapshots walked only this directory and these locations were left
    /// out of every one of them, silently; a restore came back without the
    /// tables on them. A location inside this directory is not listed: the
    /// walk already copies it and its log is already a stream.
    ///
    /// A location two databases share is listed once. Its whole directory is
    /// copied, flat, so whatever else is kept there comes along: files cannot
    /// be told apart by table without a list of suffixes, and a list is how
    /// sidecars get missed (`RENAMED_SUFFIXES` has been short twice).
    ///
    /// A pool file that cannot be read skips that database's locations only.
    fn pool_locations(&self) -> (Vec<(String, String)>, Vec<String>) {
        let root = Path::new(&self.location);
        let root_key = crate::core::wal::dir_key(&self.location);
        let mut problems: Vec<String> = Vec::new();
        let mut databases = vec![root.to_path_buf()];
        match std::fs::read_dir(root) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                        continue;
                    };
                    if EXCLUDED.contains(&name.as_str())
                        || !entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
                    {
                        continue;
                    }
                    if entry.path().join(crate::core::storage::POOL_FILE).is_file() {
                        databases.push(entry.path());
                    }
                }
            }
            Err(e) => problems.push(format!(
                "{} cannot be listed ({e}), so its created databases' storage locations are unknown",
                self.location
            )),
        }

        let mut out: Vec<(String, String)> = Vec::new();
        for database in databases {
            let paths = match crate::core::storage::data_locations(&database) {
                Ok(paths) => paths,
                Err(e) => {
                    problems.push(e.to_string());
                    continue;
                }
            };
            for path in paths {
                if stream_key(&root_key, &path).is_some() {
                    continue;
                }
                let key = location_key(&path);
                match out.iter().find(|(k, _)| *k == key) {
                    Some((_, seen))
                        if crate::core::wal::dir_key(seen) == crate::core::wal::dir_key(&path) => {}
                    Some((_, seen)) => problems.push(format!(
                        "storage locations {seen} and {path} hash to the same backup key {key}; \
                         this backup cannot tell their files apart"
                    )),
                    None => out.push((key, path)),
                }
            }
        }
        out.sort();
        (out, problems)
    }

    /// [`Self::pool_locations`] for a snapshot, which must not leave any out.
    fn pool_locations_for_snapshot(&self) -> io::Result<Vec<(String, String)>> {
        let (locations, problems) = self.pool_locations();
        if problems.is_empty() {
            Ok(locations)
        } else {
            Err(io::Error::other(format!(
                "a snapshot of {} would leave out tables kept on other disks: {}",
                self.location,
                problems.join("; ")
            )))
        }
    }

    /// Points `wal`'s checkpoints at `key`'s staging directory, and remembers
    /// it for suspension.
    fn archive_log(&self, key: &str, wal: &Arc<crate::core::wal::Wal>) -> io::Result<()> {
        wal.set_archive_dir(Some(self.staging_dir_for(key)))?;
        let mut attached = self.attached.lock().unwrap_or_else(|e| e.into_inner());
        attached.retain(|w| w.strong_count() > 0);
        if !attached
            .iter()
            .any(|w| std::ptr::eq(w.as_ptr(), Arc::as_ptr(wal)))
        {
            attached.push(Arc::downgrade(wal));
        }
        Ok(())
    }

    /// Points every log's checkpoints at its staging directory, so the records
    /// they discard are kept instead.
    ///
    /// Covers the logs that already exist — every sub-database the catalogue
    /// opened before this service was installed. The hook registered alongside
    /// this call covers the ones installed afterwards; neither half is
    /// sufficient on its own.
    fn attach_to_logs(&self) -> io::Result<()> {
        let streams = self.log_streams();
        if !streams.iter().any(|(key, _)| key.is_empty()) {
            warn!(
                "The write-ahead log is disabled for {}, so snapshots will be taken but \
                 point-in-time recovery is not available. Restores will land on a snapshot \
                 boundary and may be internally inconsistent if writes were in flight.",
                self.location
            );
        }
        for (key, wal) in streams {
            self.archive_log(&key, &wal)?;
            if !key.is_empty() {
                debug!(
                    "Archiving the log of database {} under {}",
                    key, self.location
                );
            }
        }
        Ok(())
    }

    /// Starts archiving a log installed after the service was.
    ///
    /// `CREATE DATABASE` installs one while the service is already running, and
    /// nothing re-runs [`Self::attach_to_logs`]. Left unattached, that database
    /// keeps a log nobody stages: its records reach neither the archive nor —
    /// the snapshot excludes live logs at every depth — the copy, and the
    /// backup reports success over what is left.
    fn attach_new_log(&self, dir: &str, wal: &Arc<crate::core::wal::Wal>) {
        let root = crate::core::wal::dir_key(&self.location);
        let key = match stream_key(&root, dir) {
            Some(key) => key,
            // `STORAGE ADD LOCATION` records the location before it opens the
            // log, so the pool files already name it by the time this runs.
            None => {
                let (locations, problems) = self.pool_locations();
                match locations
                    .into_iter()
                    .find(|(_, path)| crate::core::wal::dir_key(path) == dir)
                {
                    Some((key, _)) => key,
                    // Another installation's log, or a backup location's.
                    None if problems.is_empty() => return,
                    None => {
                        error!(
                            "Could not tell whether the log in {dir} belongs to a storage \
                             location of {}: {}. Snapshots will refuse to run until the pool \
                             file can be read.",
                            self.location,
                            problems.join("; ")
                        );
                        return;
                    }
                }
            }
        };
        if self.archiving_suspended.load(Ordering::Acquire) {
            // Staging is over its ceiling and nothing is being staged; the
            // resume path attaches every stream, this one included.
            return;
        }
        match self.archive_log(&key, wal) {
            Ok(()) => info!(
                "Archiving the log of {} under {}",
                if key.is_empty() {
                    "the default database"
                } else {
                    key.as_str()
                },
                self.location
            ),
            Err(e) => error!("Could not archive the log of database {}: {}", key, e),
        }
    }

    /// Starts archiving any storage location's log nothing attached yet, ahead
    /// of its cut.
    ///
    /// The install hook only sees a log the moment it is installed. Whether a
    /// location is a stream can change after that — a backup location turned
    /// primary — and without this its checkpoints would keep discarding records
    /// the archive needs. Attached before the cut, so the window opens on an
    /// archived log.
    ///
    /// Locations only. A log under this directory is a stream from the moment
    /// it exists, so the sweep and the hook already cover it, and one found
    /// detached was detached on purpose: re-attaching it here would hide that
    /// from `verify_log_is_archived`, which is what must notice.
    fn attach_unattached(&self, streams: &[(String, Arc<crate::core::wal::Wal>)]) {
        for (key, wal) in streams {
            if !key.starts_with(LOCATION_KEY_PREFIX)
                || wal.is_archiving()
                || self.archiving_suspended.load(Ordering::Acquire)
            {
                continue;
            }
            if let Err(e) = self.archive_log(key, wal) {
                error!("Could not archive the log of {key}: {e}");
                continue;
            }
            // Suspension may have landed in between; it detaches every
            // stream, and this one must not be the exception.
            if self.archiving_suspended.load(Ordering::Acquire) {
                let _ = wal.set_archive_dir(None);
            }
        }
    }

    pub fn staging_dir(&self) -> PathBuf {
        Path::new(&self.location).join(STAGING_DIR)
    }

    /// Where one log's cut segments wait.
    ///
    /// The root's segments stay directly in the staging directory, where a
    /// build that predates per-database streams left them, so an upgrade finds
    /// and uploads what it already staged.
    fn staging_dir_for(&self, key: &str) -> PathBuf {
        if key.is_empty() {
            self.staging_dir()
        } else {
            self.staging_dir().join(key)
        }
    }

    pub fn config(&self) -> &BackupConfig {
        &self.config
    }

    /// The database directory this service backs up.
    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// A copy of the catalogue, for `SHOW BACKUPS`.
    pub async fn manifest_snapshot(&self) -> Manifest {
        self.manifest.lock().await.clone()
    }

    // ---- snapshot ----------------------------------------------------------

    /// Takes a snapshot and makes it restorable.
    ///
    /// Incremental when `incremental_min_bytes` is set: see
    /// [`BackupConfig::incremental_min_bytes`].
    pub async fn snapshot_now(self: &Arc<Self>) -> io::Result<SnapshotReport> {
        self.take_snapshot(false).await
    }

    /// `full` refuses to reuse anything an earlier snapshot stored, so the
    /// result depends on no older object — what a rollover needs before it
    /// deletes every older generation.
    async fn take_snapshot(self: &Arc<Self>, full: bool) -> io::Result<SnapshotReport> {
        let _one_at_a_time = self.snapshot_lock.lock().await;
        // A snapshot is a copy of a directory, so a table that is not in one is
        // not in the snapshot. That is the intended behaviour — a table on
        // another backend does not get the file-backed features — but it must be
        // said out loud every time rather than discovered at a restore, which is
        // the worst possible moment to learn what a backup does not contain.
        self.warn_about_excluded_tables();
        let started = std::time::Instant::now();
        let started_at = timeutil::now_unix();

        // Exclusive for the whole copy, and first: see `RELOCATION_GATE`.
        let no_relocation = relocation_gate().write().await;
        // Exclusive for the whole copy: compaction is the one writer whose
        // effects replay cannot reconstruct.
        let _no_compaction = compaction_gate().write().await;
        // Exclusive only until the copy's start is fixed below: see
        // `DELETION_GATE`.
        let no_deletion = deletion_gate().write().await;

        // Before the first cut, so each location's log is a stream of this
        // snapshot and its window opens with the copy's.
        let locations = self.pool_locations_for_snapshot()?;
        let streams = self.log_streams();

        // Read before the log is cut, so every bundle this snapshot's window
        // produces lands at or above it. A bundle already archived under a
        // lower sequence holds only records an earlier checkpoint flushed into
        // the data files, which the copy below therefore contains — so the
        // floor may over-keep by a bundle but can never cut one the restore
        // needs. This is what retention and restore use to tell the two apart;
        // `start_lsn` cannot, because the commit sequence restarts with the
        // log. See `manifest::segment_needed_by`.
        let first_segment_sequence = self.next_segment_sequence().await;

        // Flush every data file and cut every log, so the copy starts from a
        // point where the files are as complete as they can be and the window
        // that needs replaying begins here rather than at some earlier
        // checkpoint. Every log, because every database created with `CREATE
        // DATABASE` has one of its own and the copy below excludes live logs at
        // any depth — records left in one would be in neither the copy nor the
        // archive.
        //
        // A failure here fails the snapshot, though every healthy log has been
        // cut by then. A snapshot is one restore point for the whole location,
        // and a database whose log cannot be cut is one whose data files may
        // not hold what its log says was committed: published, the snapshot
        // would restore that database wrong and report nothing amiss. The
        // error names the database, so the others are not left guessing.
        self.checkpoint_all_logs().await?;
        let opened_at: HashMap<String, u64> = streams
            .iter()
            .map(|(key, wal)| (key.clone(), wal.commit_lsn()))
            .collect();
        let start_lsn = opened_at.get("").copied().unwrap_or(0);
        // No snapshot from here on asks about a drop at or before its window.
        for (key, wal) in &streams {
            wal.forget_drops_through(opened_at.get(key).copied().unwrap_or(0));
        }
        let dropped = DroppedDuringCopy::new(&streams, &opened_at);
        // Each window's start is fixed: a barrier from here commits past it,
        // and its deletion follows it.
        drop(no_deletion);

        let files = self.snapshot_file_list(&locations)?;
        if files.is_empty() {
            return Err(io::Error::other(format!(
                "{} holds no database files to snapshot",
                self.location
            )));
        }
        #[cfg(test)]
        upload_hooks::parked_here(&self.location).await;

        let id = format!(
            "snap-{}-{:08x}",
            timeutil::format_basic(started_at),
            fastrand::u32(..)
        );
        let key = self.config.key(&format!("snapshots/{id}.rdba.zst"));

        info!(
            "Snapshot {} starting: {} files from {} at LSN {}",
            id,
            files.len(),
            self.location,
            start_lsn
        );

        let staged_before = self.staged_lengths(&locations)?;
        // Large files first, as objects of their own (or references to ones an
        // earlier snapshot stored); the rest into this snapshot's archive.
        let (archived, objects, objects_uncompressed, objects_compressed) =
            if !full && self.config.incremental_min_bytes > 0 {
                self.store_large_files(&id, &files, &dropped).await?
            } else {
                (files.clone(), Vec::new(), 0, 0)
            };
        let (uncompressed, compressed) = self.upload_snapshot(&key, &archived, &dropped).await?;
        let (uncompressed, compressed) =
            (uncompressed + objects_uncompressed, compressed + objects_compressed);
        // A dropped table's sidecar is gone, not folded: whatever it staged
        // went with the table (see `DroppedDuringCopy`).
        let staged_before: HashMap<String, u64> = staged_before
            .into_iter()
            .filter(|(name, _)| {
                !files
                    .iter()
                    .any(|(listed, path)| listed == name && !path.exists() && dropped.explains(path))
            })
            .collect();
        self.check_staged_appends(&staged_before, &locations)?;

        // A location that started holding tables while the copy ran is in
        // neither the copy nor this snapshot's list, and a restore would read
        // its tables as missing. Rare — it takes `STORAGE ADD LOCATION` or a
        // placement mid-copy — and refused rather than recorded.
        let appeared: Vec<String> = self
            .pool_locations_for_snapshot()?
            .into_iter()
            .filter(|location| !locations.contains(location))
            .map(|(_, path)| path)
            .collect();
        if !appeared.is_empty() {
            return Err(io::Error::other(format!(
                "storage location(s) {} started holding tables while snapshot {id} was being \
                 copied, so their files are not in it. Discarding it; retry the backup.",
                appeared.join(", ")
            )));
        }

        // Cut the logs again. Everything written while the copy ran is now in a
        // staged segment, and each window's `end_lsn` is the point a restore
        // must replay that database through for its copy to become consistent.
        //
        // The list is read again rather than reused: a database created while
        // the copy ran has a log of its own that nothing has cut yet, and
        // leaving it would strand those records. Its window opens at 0, which
        // is the truth — every record it has ever written is in the archive.
        //
        // The ends are read before that cut, together (see `read_cut`): with a
        // location, they are where a restore of this snapshot stops every log,
        // and read one log at a time they could pair a database's outcome with
        // a location log that does not yet hold its rows. The cut then stages
        // everything through them.
        let ending = self.log_streams();
        let ends = read_cut(&ending)?;
        self.checkpoint_all_logs().await?;
        let windows: Vec<manifest::LogWindow> = ends
            .into_iter()
            .map(|(key, end_lsn)| manifest::LogWindow {
                start_lsn: opened_at.get(&key).copied().unwrap_or(0),
                end_lsn,
                database: key,
            })
            .collect();
        let end_lsn = windows
            .iter()
            .find(|w| w.database.is_empty())
            .map(|w| w.end_lsn)
            .unwrap_or(0);

        // A file that appeared during the copy is not in the archive, but the
        // log records that filled it are. Naming it here lets a restore create
        // it empty so replay has somewhere to land.
        let mut names: Vec<String> = files.iter().map(|(name, _)| name.clone()).collect();
        for (name, _) in self.snapshot_file_list(&locations)? {
            if !names.contains(&name) {
                debug!(
                    "{} appeared during snapshot {}; it will be rebuilt from the log",
                    name, id
                );
                names.push(name);
            }
        }

        let entry = SnapshotEntry {
            id: id.clone(),
            sequence: self.manifest.lock().await.next_snapshot_sequence(),
            key: key.clone(),
            started_at,
            finished_at: timeutil::now_unix(),
            start_lsn,
            end_lsn,
            first_segment_sequence: Some(first_segment_sequence),
            windows,
            compressed_bytes: compressed,
            uncompressed_bytes: uncompressed,
            file_count: names.len() as u32,
            compression_level: self.config.snapshot_level,
            files: names,
            objects,
            locations: locations
                .iter()
                .map(|(key, path)| manifest::PoolLocation {
                    key: key.clone(),
                    path: path.clone(),
                })
                .collect(),
        };

        // Released before the archive flush: that is network work and holding
        // compaction off for it buys nothing. A rename or a move after this
        // point is past the window's end and in no file this snapshot names.
        drop(_no_compaction);
        drop(no_relocation);

        // Upload the window's log immediately. Until it is up there the
        // snapshot object exists but cannot be restored from.
        self.archive_wal_now().await?;
        self.verify_log_is_archived(&id, &entry.windows).await?;

        // Recorded only once the log behind it is known to be in S3, so a
        // catalogue entry always names a snapshot that can actually be
        // restored.
        {
            let mut manifest = self.manifest.lock().await;
            // A reused object must still be referenced by a snapshot in the
            // catalogue at the moment this one joins it. A prune running
            // concurrently plans under this same lock: either it saw this
            // entry and keeps the object, or it already dropped the last
            // snapshot naming it and is deleting it — and then this snapshot
            // would restore from an object that is gone. Refused, not
            // repaired: the next snapshot simply uploads the file again.
            let own_prefix = self.config.key(&format!("snapshots/{id}/"));
            let live: std::collections::HashSet<&str> = manifest
                .snapshots
                .iter()
                .flat_map(|s| s.objects.iter().map(|o| o.key.as_str()))
                .collect();
            if let Some(gone) = entry
                .objects
                .iter()
                .find(|o| !o.key.starts_with(&own_prefix) && !live.contains(o.key.as_str()))
            {
                return Err(io::Error::other(format!(
                    "snapshot {id} reused {} for {}, but retention removed it while the                      snapshot ran; the snapshot is abandoned and the next one will upload it",
                    gone.key, gone.name
                )));
            }
            manifest.snapshots.push(entry.clone());
        }

        self.prune().await?;
        self.publish_manifest().await?;

        let report = SnapshotReport {
            id,
            key,
            file_count: entry.file_count,
            uncompressed_bytes: uncompressed,
            compressed_bytes: compressed,
            start_lsn,
            end_lsn,
            elapsed: started.elapsed(),
        };

        info!(
            "Snapshot {} complete: {} files, {} -> {} bytes ({:.1}x) in {:.1}s, LSN {}..{}",
            report.id,
            report.file_count,
            report.uncompressed_bytes,
            report.compressed_bytes,
            report.ratio(),
            report.elapsed.as_secs_f64(),
            report.start_lsn,
            report.end_lsn
        );
        Ok(report)
    }

    /// Total bytes waiting in the staging directory.
    pub fn staged_bytes(&self) -> u64 {
        self.staged_segments()
            .unwrap_or_default()
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
            .sum()
    }

    /// True while checkpoints have been told to stop staging.
    pub fn archiving_is_suspended(&self) -> bool {
        self.archiving_suspended.load(Ordering::Acquire)
    }

    /// Suspends or resumes archiving according to how full staging is.
    ///
    /// Resumption uses half the ceiling rather than the ceiling itself, so a
    /// directory hovering at the limit does not flap between the two states
    /// once per tick.
    pub fn enforce_staging_ceiling(&self) {
        let ceiling = self.config.staging_max_bytes;
        let staged = self.staged_bytes();
        let suspended = self.archiving_suspended.load(Ordering::Acquire);

        let Some(suspend) = staging_action(staged, ceiling, suspended) else {
            return;
        };

        let streams = self.log_streams();
        if suspend {
            // Every log that can be named, and every one this service ever
            // attached: one it can no longer name would otherwise go on
            // filling the disk this exists to protect. Detaching cannot fail.
            let attached: Vec<Arc<crate::core::wal::Wal>> = self
                .attached
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .filter_map(std::sync::Weak::upgrade)
                .collect();
            for wal in streams.iter().map(|(_, wal)| wal).chain(attached.iter()) {
                let _ = wal.set_archive_dir(None);
            }
            self.archiving_suspended.store(true, Ordering::Release);
            error!(
                "Log archiving SUSPENDED: {} bytes are staged in {} and uploads are not keeping \
                 up, which would eventually fill the disk and stop the database. Point-in-time \
                 recovery is broken from this moment until archiving resumes and a new snapshot \
                 is taken. Fix the upload failures above; archiving resumes on its own once \
                 staging drains below {} bytes.",
                staged,
                self.staging_dir().display(),
                ceiling / 2
            );
        } else {
            for (key, wal) in &streams {
                if let Err(e) = self.archive_log(key, wal) {
                    error!("Could not resume log archiving: {e}");
                    return;
                }
            }
            self.archiving_suspended.store(false, Ordering::Release);
            warn!(
                "Log archiving resumed after draining to {} bytes. Point-in-time recovery has a \
                 gap covering the suspension; take a snapshot to establish a new baseline.",
                staged
            );
        }
    }

    /// Length of every staged-append sidecar in the database directory and in
    /// the storage locations a snapshot copies.
    fn staged_lengths(&self, locations: &[(String, String)]) -> io::Result<HashMap<String, u64>> {
        staged_lengths_in(Path::new(&self.location), locations)
    }

    /// Fails the snapshot if a staged-append fold landed while the copy ran.
    ///
    /// A sidecar that shrank was folded into its main file, and the copy may
    /// have taken the main file before the fold and the sidecar after it — in
    /// which case those bytes are in neither, and no log record exists to put
    /// them back. Refusing is the only honest answer: the next attempt will
    /// almost certainly succeed, whereas an uploaded snapshot with a hole in it
    /// is not discovered until someone needs it.
    fn check_staged_appends(
        &self,
        before: &std::collections::HashMap<String, u64>,
        locations: &[(String, String)],
    ) -> io::Result<()> {
        folded_while_copying(before, &self.staged_lengths(locations)?)
    }

    /// Confirms the log covering a snapshot has actually reached S3.
    ///
    /// `Wal::checkpoint` returns `Ok(())` when it *declines* to run — a
    /// transaction with pending in-place writes points into the log, so
    /// truncating underneath it would strand them. That is correct for the log,
    /// but it means a snapshot cannot conclude from a successful call that its
    /// window was cut and staged. Without this check the snapshot would record
    /// an `end_lsn` whose records were never archived, publish a catalogue
    /// entry that looks healthy, and produce a restore that silently stops
    /// short of consistency.
    ///
    /// So the condition is checked directly rather than inferred: the live log
    /// must be empty and the staging directory drained. A pending transaction
    /// is usually brief, so it is retried before giving up.
    async fn verify_log_is_archived(
        self: &Arc<Self>,
        snapshot_id: &str,
        windows: &[manifest::LogWindow],
    ) -> io::Result<()> {
        // Asked of every log under the location. A sub-database whose records
        // are still in its own live log leaves its files in the copy without
        // the log that reconciles them, which is the same failure as the root's
        // and just as silent.
        //
        // The question is whether every record *inside each window* is in the
        // archive — not whether the logs are empty. A checkpoint keeps what
        // was logged during its own sync pass, so under write load a log is
        // never empty, and requiring that discarded every snapshot and every
        // rollover taken while the database was busy.
        let streams = self.log_streams();
        if streams.is_empty() {
            // No log at all: nothing to archive, and the caller has already
            // been warned that this database has no point-in-time recovery.
            return Ok(());
        }
        let window_of = |key: &str| windows.iter().find(|w| w.database == key);

        const ATTEMPTS: u32 = 5;
        let mut short: Vec<String> = Vec::new();
        for attempt in 1..=ATTEMPTS {
            // Read before the upload below lists what is staged: whatever a
            // checkpoint had staged by now is in that listing.
            short = streams
                .iter()
                .filter_map(|(key, wal)| {
                    let w = window_of(key)?;
                    (!wal.staged_covers(w.start_lsn, w.end_lsn)).then(|| {
                        format!(
                            "{} through LSN {}",
                            if key.is_empty() { "<default>" } else { key.as_str() },
                            w.end_lsn
                        )
                    })
                })
                .collect();
            // Uploads everything staged so far; a failure here is the error.
            self.flush_staged_segments().await?;
            if short.is_empty() {
                return Ok(());
            }
            if attempt == ATTEMPTS {
                break;
            }
            debug!(
                "Snapshot {}: not yet staged: {}; retrying ({}/{})",
                snapshot_id,
                short.join(", "),
                attempt,
                ATTEMPTS
            );
            tokio::time::sleep(Duration::from_millis(200 * attempt as u64)).await;
            // A checkpoint can decline (another running, an in-place write
            // pending); cut again.
            self.checkpoint_all_logs().await?;
        }

        // Failing loses a snapshot; publishing would lose the ability to
        // restore one, and only at the moment someone needed it.
        Err(io::Error::other(format!(
            "snapshot {snapshot_id} was copied, but the log covering it could not be archived \
             (not staged: {}){}. Discarding it rather than recording a snapshot that cannot \
             be restored; retry the backup.",
            short.join(", "),
            if streams.iter().any(|(_, wal)| wal.is_poisoned()) {
                " — the log is poisoned by an earlier flush failure, so durability cannot be \
                 promised until the database is restarted"
            } else {
                ""
            }
        )))
    }

    /// The files a snapshot captures, with their lengths at scan time.
    ///
    /// Walks subdirectories. Every database created with `CREATE DATABASE`
    /// lives in one of them, and a listing that only took plain files left all
    /// of them out of every snapshot — silently, since the backup then
    /// succeeded over what remained. A restore came back with the default
    /// database and nothing else, which is the worst way to discover it.
    ///
    /// Reports the tables this snapshot cannot contain.
    ///
    /// Read from `db_storage.cfg` rather than from a live `Database`, so the
    /// backup service keeps having no dependency on the catalogue.
    #[cfg(feature = "storage_pools")]
    fn warn_about_excluded_tables(&self) {
        use crate::core::storage::{StorageManager, TableBackend};
        let pool = StorageManager::open(&self.location);
        let mut excluded: Vec<String> = pool
            .placements()
            .into_iter()
            .filter_map(|(table, spec)| {
                let backend = TableBackend::parse(&spec);
                (!backend.is_file_based()).then(|| format!("{table} ({})", backend.kind()))
            })
            .collect();
        if excluded.is_empty() {
            return;
        }
        excluded.sort_unstable();
        warn!(
            "snapshot of {} does not include {} table(s) that are not file-backed: {}. \
             A restore will not bring them back.",
            self.location,
            excluded.len(),
            excluded.join(", ")
        );
    }

    #[cfg(not(feature = "storage_pools"))]
    fn warn_about_excluded_tables(&self) {}

    /// Names are relative paths with `/` separators, so a restore rebuilds the
    /// same shape. A storage location's files are named under its stream key,
    /// which a restore maps back to a directory (see [`restore`]).
    fn snapshot_file_list(
        &self,
        locations: &[(String, String)],
    ) -> io::Result<Vec<(String, PathBuf)>> {
        fn walk(dir: &Path, prefix: &str, files: &mut Vec<(String, PathBuf)>) -> io::Result<()> {
            for entry in std::fs::read_dir(dir)?.flatten() {
                let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                let Ok(file_type) = entry.file_type() else {
                    continue;
                };

                if file_type.is_dir() {
                    // The archive staging directory holds copies of what is
                    // being uploaded; backing it up would nest a snapshot
                    // inside a snapshot.
                    if EXCLUDED.contains(&name.as_str()) {
                        continue;
                    }
                    let nested = format!("{}{}/", prefix, name);
                    walk(&entry.path(), &nested, files)?;
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                if excluded_file(&name)
                    || name.ends_with(crate::core::wal::SEGMENT_SUFFIX)
                {
                    continue;
                }
                files.push((format!("{}{}", prefix, name), entry.path()));
            }
            Ok(())
        }

        let mut files = Vec::new();
        walk(Path::new(&self.location), "", &mut files)?;
        // Flat: tables sit directly in a location, and a recursive walk would
        // take in whatever else is kept beneath it — another database's
        // location, say, which that database's own entry already covers.
        for (key, path) in locations {
            let entries = std::fs::read_dir(path).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("storage location {path} cannot be read for the snapshot: {e}"),
                )
            })?;
            for entry in entries.flatten() {
                let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                if !entry.file_type().map(|t| t.is_file()).unwrap_or(false)
                    || excluded_file(&name)
                    || name.ends_with(crate::core::wal::SEGMENT_SUFFIX)
                {
                    continue;
                }
                files.push((format!("{key}/{name}"), entry.path()));
            }
        }
        // Deterministic order, so two snapshots of an unchanged database
        // produce byte-identical archives and are trivially comparable.
        files.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(files)
    }

    /// Packs, compresses and uploads the archive, returning
    /// `(uncompressed, compressed)` bytes.
    ///
    /// Compression runs on a blocking thread and upload on the runtime, joined
    /// by a bounded channel: the compressor stalls when the network falls
    /// behind rather than buffering a database in memory.
    async fn upload_snapshot(
        self: &Arc<Self>,
        key: &str,
        files: &[(String, PathBuf)],
        dropped: &DroppedDuringCopy,
    ) -> io::Result<(u64, u64)> {
        let part_size = self.part_size_for(files);
        let (tx, rx) = kanal::bounded::<Vec<u8>>(2);

        let level = self.config.snapshot_level;
        let threads = self.compression_threads();
        let long_range = self.config.long_range;
        let owned: Vec<(String, PathBuf)> = files.to_vec();
        let dropped = dropped.clone();

        let producer = tokio::task::spawn_blocking(move || -> io::Result<u64> {
            let sink = PartSink::new(tx, part_size);
            let mut encoder = new_encoder(sink, level, threads, long_range)?;

            let mut writer = archive::ArchiveWriter::new(&mut encoder, owned.len() as u32)?;
            let mut total = 0u64;
            for (name, path) in &owned {
                match std::fs::File::open(path) {
                    Ok(file) => total += writer.add_open_file(name, file)?,
                    Err(e) => {
                        dropped.leave_out(path, e)?;
                        // The header already counts it. Empty is what a
                        // restore leaves of it at its barrier anyway.
                        writer.add_reader(name, 0, &mut io::empty())?;
                    }
                }
            }
            writer.finish()?;

            // `finish` flushes the zstd frame into the sink; the sink's own
            // `finish` then releases whatever is left as the final part.
            encoder.finish()?.finish()?;
            Ok(total)
        });

        let uploaded = self.upload_parts(key.to_string(), rx.to_async()).await;

        let produced = producer
            .await
            .map_err(|e| io::Error::other(format!("snapshot compressor panicked: {e}")))?;

        // The compressor's error is reported first: an upload that failed
        // because the channel closed is a symptom of it, and the other way
        // round the compressor stops with the same channel error.
        let uncompressed = produced?;
        Ok((uncompressed, uploaded?))
    }

    /// Stores every file of at least `incremental_min_bytes` as an object of
    /// its own, or references the object an earlier snapshot stored for the
    /// same bytes. Returns the files left for the archive, the objects, and
    /// the uncompressed and newly uploaded compressed byte counts.
    ///
    /// Two passes over a file that is not reused: one to hash it for the
    /// lookup, one to compress and upload it, which hashes again. The second
    /// hash is the one recorded, so the catalogue always describes exactly the
    /// bytes in the object even if the file changed between passes. Either
    /// copy is a valid fuzzy copy: both were read after the checkpoint that
    /// opens this snapshot's replay window.
    async fn store_large_files(
        self: &Arc<Self>,
        id: &str,
        files: &[(String, PathBuf)],
        dropped: &DroppedDuringCopy,
    ) -> io::Result<(Vec<(String, PathBuf)>, Vec<manifest::FileObject>, u64, u64)> {
        let reusable = self.manifest.lock().await.reusable_objects();
        let mut archived = Vec::new();
        let mut objects = Vec::new();
        let (mut uncompressed, mut compressed) = (0u64, 0u64);

        for (index, (name, path)) in files.iter().enumerate() {
            // A file left out here is in neither the archive nor an object,
            // so a restore creates it empty, as its barrier leaves it.
            let len = match std::fs::metadata(path) {
                Ok(meta) => meta.len(),
                Err(e) => {
                    dropped.leave_out(path, e)?;
                    continue;
                }
            };
            if len < self.config.incremental_min_bytes {
                archived.push((name.clone(), path.clone()));
                continue;
            }
            let hashed_path = path.clone();
            let hashed = tokio::task::spawn_blocking(move || hash_prefix(&hashed_path, len))
                .await
                .map_err(|e| io::Error::other(format!("snapshot hasher panicked: {e}")))?;
            let sha256 = match hashed {
                Ok(sha256) => sha256,
                Err(e) => {
                    dropped.leave_out(path, e)?;
                    continue;
                }
            };
            uncompressed += len;

            if let Some(key) = reusable.get(&(sha256.clone(), len)) {
                objects.push(manifest::FileObject {
                    name: name.clone(),
                    len,
                    sha256,
                    key: key.clone(),
                });
                continue;
            }

            let key = self.config.key(&format!("snapshots/{id}/{index:06}.zst"));
            let Some((sha256, stored)) = self.upload_file_object(&key, path, len, dropped).await?
            else {
                uncompressed -= len;
                continue;
            };
            compressed += stored;
            objects.push(manifest::FileObject {
                name: name.clone(),
                len,
                sha256,
                key,
            });
        }
        Ok((archived, objects, uncompressed, compressed))
    }

    /// Compresses the first `len` bytes of `path` into `key`, hashing exactly
    /// what it compresses. Returns the hash and the compressed size, or
    /// `None`, with nothing uploaded, for a file a `DROP` took away first
    /// (see [`DroppedDuringCopy`]).
    async fn upload_file_object(
        self: &Arc<Self>,
        key: &str,
        path: &Path,
        len: u64,
        dropped: &DroppedDuringCopy,
    ) -> io::Result<Option<(String, u64)>> {
        let part_size = self.config.part_size.max(5 * 1024 * 1024);
        let (tx, rx) = kanal::bounded::<Vec<u8>>(2);
        let level = self.config.snapshot_level;
        let threads = self.compression_threads();
        let long_range = self.config.long_range;
        let path = path.to_path_buf();
        let dropped = dropped.clone();

        let producer = tokio::task::spawn_blocking(move || -> io::Result<Option<String>> {
            use sha2::Digest;
            // Opened before anything reaches the sink: a file left out sends
            // no part, and the upload below then writes no object.
            let file = match std::fs::File::open(&path) {
                Ok(file) => file,
                Err(e) => {
                    dropped.leave_out(&path, e)?;
                    return Ok(None);
                }
            };
            let sink = PartSink::new(tx, part_size);
            let mut encoder = new_encoder(sink, level, threads, long_range)?;
            encoder.set_pledged_src_size(Some(len))?;
            let mut file = std::io::BufReader::with_capacity(256 * 1024, file);
            let mut digest = sha2::Sha256::new();
            let mut chunk = vec![0u8; 1024 * 1024];
            let mut remaining = len;
            while remaining > 0 {
                let want = chunk.len().min(remaining as usize);
                // Exactly `len`, as the archive does: a file that grows during
                // the copy is captured at the length it had, and the log
                // replay supplies the rest.
                file.read_exact(&mut chunk[..want])?;
                digest.update(&chunk[..want]);
                encoder.write_all(&chunk[..want])?;
                remaining -= want as u64;
            }
            encoder.finish()?.finish()?;
            Ok(Some(to_hex(&digest.finalize())))
        });

        let uploaded = self.upload_parts(key.to_string(), rx.to_async()).await;
        let produced = producer
            .await
            .map_err(|e| io::Error::other(format!("snapshot compressor panicked: {e}")))?;
        let Some(sha256) = produced? else {
            uploaded?;
            return Ok(None);
        };
        Ok(Some((sha256, uploaded?)))
    }

    /// Part size that keeps a snapshot inside S3's 10 000-part ceiling.
    fn part_size_for(&self, files: &[(String, PathBuf)]) -> usize {
        part_size_from_files(self.config.part_size, files)
    }

    fn compression_threads(&self) -> u32 {
        if self.config.compression_threads > 0 {
            return self.config.compression_threads;
        }
        // Half the machine, capped: a backup should not evict the query
        // workload from every core it has.
        let available = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2);
        (available / 2).clamp(1, 8) as u32
    }

    /// Consumes parts and writes them to `key`.
    ///
    /// A body that fits in one part goes out as a plain `PUT` — three requests
    /// saved on every small database, which is most of them.
    async fn upload_parts(
        self: &Arc<Self>,
        key: String,
        rx: kanal::AsyncReceiver<Vec<u8>>,
    ) -> io::Result<u64> {
        let first = rx.recv().await.ok();
        let Some(first) = first else {
            // Nothing produced at all: the compressor failed and its error is
            // the one worth reporting.
            return Ok(0);
        };

        let second = rx.recv().await.ok();
        let Some(second) = second else {
            let total = first.len() as u64;
            self.client.put_object(&key, &first).await?;
            return Ok(total);
        };

        let upload_id = self.client.create_multipart_upload(&key).await?;
        match self
            .run_multipart(&key, &upload_id, first, second, rx)
            .await
        {
            Ok(total) => Ok(total),
            Err(e) => {
                // An abandoned multipart upload is billed until a lifecycle
                // rule reaps it, so it is cleaned up here even though the
                // operation as a whole has already failed.
                if let Err(abort) = self.client.abort_multipart_upload(&key, &upload_id).await {
                    warn!("Could not abort the multipart upload for {key}: {abort}");
                }
                Err(e)
            }
        }
    }

    async fn run_multipart(
        self: &Arc<Self>,
        key: &str,
        upload_id: &str,
        first: Vec<u8>,
        second: Vec<u8>,
        rx: kanal::AsyncReceiver<Vec<u8>>,
    ) -> io::Result<u64> {
        let permits = Arc::new(Semaphore::new(self.config.upload_concurrency.max(1)));
        let mut tasks = Vec::new();
        let mut total = 0u64;
        let mut part_number = 1u32;

        let spawn_part = |body: Vec<u8>, number: u32, total: &mut u64| {
            *total += body.len() as u64;
            let client = self.client.clone();
            let permits = permits.clone();
            let key = key.to_string();
            let upload_id = upload_id.to_string();
            tokio::spawn(async move {
                let _permit = permits.acquire_owned().await;
                client
                    .upload_part(&key, &upload_id, number, &body)
                    .await
                    .map(|etag| (number, etag))
            })
        };

        tasks.push(spawn_part(first, part_number, &mut total));
        part_number += 1;
        tasks.push(spawn_part(second, part_number, &mut total));

        while let Ok(body) = rx.recv().await {
            part_number += 1;
            if part_number as u64 > MAX_PARTS {
                return Err(io::Error::other(
                    "snapshot exceeded S3's 10 000-part limit; raise --s3-part-size-mb",
                ));
            }
            tasks.push(spawn_part(body, part_number, &mut total));
        }

        let mut parts = Vec::with_capacity(tasks.len());
        let mut failure = None;
        for task in tasks {
            match task.await {
                Ok(Ok(part)) => parts.push(part),
                Ok(Err(e)) => {
                    failure.get_or_insert(e);
                }
                Err(e) => {
                    failure.get_or_insert(io::Error::other(format!("part upload panicked: {e}")));
                }
            }
        }
        if let Some(e) = failure {
            return Err(e);
        }

        parts.sort_by_key(|(number, _)| *number);
        self.client
            .complete_multipart_upload(key, upload_id, &parts)
            .await?;
        Ok(total)
    }

    // ---- log archiving -----------------------------------------------------

    /// Cuts the live log and uploads everything staged.
    ///
    /// This is the point-in-time barrier: once it returns, every commit made
    /// before it is recoverable from S3.
    pub async fn archive_wal_now(self: &Arc<Self>) -> io::Result<ArchiveReport> {
        // Every log, not only the root's: a database created with `CREATE
        // DATABASE` keeps its own, and records left in it are recoverable from
        // nowhere. A checkpoint with nothing to do is cheap.
        //
        // The upload runs even when a cut failed. One database's log refusing
        // to be cut — a poisoned log refuses until restart — says nothing about
        // the segments every other database has already staged, and stopping
        // here stopped point-in-time recovery for all of them. The failure is
        // still the result, so the caller learns which database is not covered.
        let cut = self.checkpoint_all_logs().await;
        let uploaded = self.flush_staged_segments().await;
        match (cut, uploaded) {
            (Ok(()), uploaded) => uploaded,
            (Err(cut), Ok(_)) => Err(cut),
            (Err(cut), Err(upload)) => Err(io::Error::other(format!(
                "{cut}; and uploading the segments that were staged failed too: {upload}"
            ))),
        }
    }

    /// Cuts every log under this location, so its records reach staging.
    ///
    /// Every log is attempted even after one fails, and the error names each
    /// that failed. Returning at the first one left every log sorted after it
    /// uncut, so a single poisoned sub-database stopped the archiving of every
    /// healthy database beside it until the process restarted.
    ///
    /// A log that cannot be named — its database's pool file does not parse —
    /// is reported the same way, after every other log has been cut.
    async fn checkpoint_all_logs(self: &Arc<Self>) -> io::Result<()> {
        let (streams, mut failed) = self.log_streams_checked();
        self.attach_unattached(&streams);
        for (key, wal) in streams {
            let w = wal.clone();
            let outcome = tokio::task::spawn_blocking(move || w.checkpoint())
                .await
                .map_err(|e| io::Error::other(e.to_string()))
                .and_then(|r| r);
            if let Err(e) = outcome {
                failed.push(format!(
                    "the log of database {} could not be cut: {e}",
                    if key.is_empty() {
                        "<default>"
                    } else {
                        key.as_str()
                    }
                ));
            }
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(failed.join("; ")))
        }
    }

    /// Uploads staged segments without cutting the live log first.
    ///
    /// Used by the background task, which does not want to force a checkpoint —
    /// and its fsync of every data file — on a schedule of its own.
    ///
    /// Except while a database keeps tables on a storage location: then every
    /// log is cut behind one [`read_cut`] first, and the cut is recorded once
    /// the bundles holding it are. Uploading what a database's log staged on
    /// its own put a transaction's outcome in the archive without the rows its
    /// location's log still held, and a restore came back with a later commit
    /// and without an earlier one.
    async fn flush_staged_segments(self: &Arc<Self>) -> io::Result<ArchiveReport> {
        let _one_at_a_time = self.archive_lock.lock().await;

        let cut = self.cut_together().await;
        let staged = self.staged_by_stream()?;
        if cut.is_none() && staged.iter().all(|(_, paths)| paths.is_empty()) {
            return Ok(ArchiveReport::default());
        }
        let staged_names: Vec<String> = staged.iter().map(|(name, _)| name.clone()).collect();

        let mut report = ArchiveReport::default();

        // Segments are concatenated into bundles up to the configured size. The
        // concatenation replays exactly as the pieces would individually:
        // records are self-framing and stay in log order, and a transaction
        // split across a cut is reunited rather than being dropped for want of
        // its commit marker.
        //
        // Never across logs, though: each database's records name files by an
        // id derived from a table name, so two databases' streams cannot be
        // told apart once concatenated. One bundle belongs to exactly one.
        for (database, paths) in staged {
            for group in group_by_size(&paths, self.config.wal_bundle_bytes)? {
                // Opened first and sized from the handles, so the body is
                // allocated once at its final size rather than regrown per
                // segment. A staged segment is never appended to, so the sum
                // is exact; `read_to_end` still copes if it were not.
                let mut files = Vec::with_capacity(group.len());
                let mut total = 0usize;
                for path in &group {
                    let file = std::fs::File::open(path)?;
                    total += file.metadata()?.len() as usize;
                    files.push(file);
                }
                let mut body = Vec::with_capacity(total);
                for mut file in files {
                    file.read_to_end(&mut body)?;
                }

                let (first_lsn, last_lsn, commits) = crate::core::wal::scan_commit_range(&body);
                let uncompressed = body.len() as u64;

                let sequence = self.next_segment_sequence().await;
                // The root's bundles keep the flat layout an older build wrote, so
                // its catalogue entries and objects line up across the upgrade.
                let key = self.config.key(&if database.is_empty() {
                    format!("wal/{sequence:016x}-{first_lsn:016x}.wseg.zst")
                } else {
                    format!("wal/{database}/{sequence:016x}-{first_lsn:016x}.wseg.zst")
                });

                let level = self.config.segment_level;
                let threads = self.compression_threads();
                let long_range = self.config.long_range;
                let compressed = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
                    compress_bundle(&body, level, threads, long_range)
                })
                .await
                .map_err(|e| io::Error::other(format!("segment compressor panicked: {e}")))??;

                let compressed_bytes = compressed.len() as u64;
                self.client.put_object(&key, &compressed).await?;

                {
                    let mut manifest = self.manifest.lock().await;
                    manifest.record_segment(SegmentEntry {
                        sequence,
                        database: database.clone(),
                        key,
                        archived_at: timeutil::now_unix(),
                        first_lsn,
                        last_lsn,
                        compressed_bytes,
                        uncompressed_bytes: uncompressed,
                        segment_count: group.len() as u32,
                    });
                }

                report.bundles += 1;
                report.segments += group.len() as u32;
                report.uncompressed_bytes += uncompressed;
                report.compressed_bytes += compressed_bytes;
                if first_lsn != 0 && (report.first_lsn == 0 || first_lsn < report.first_lsn) {
                    report.first_lsn = first_lsn;
                }
                report.last_lsn = report.last_lsn.max(last_lsn);

                debug!(
                    "Archived {} segments of {} ({} commits, LSN {}..{}) as {} bytes",
                    group.len(),
                    if database.is_empty() {
                        "the default database"
                    } else {
                        database.as_str()
                    },
                    commits,
                    first_lsn,
                    last_lsn,
                    compressed_bytes
                );

                // Only once the bundle is durably in S3. Deleting first and failing
                // the upload would lose the records outright.
                for path in &group {
                    if let Err(e) = std::fs::remove_file(path) {
                        warn!("Could not remove staged segment {}: {}", path.display(), e);
                    }
                }
            }
        }

        // Only now: every bundle holding records through the cut is recorded.
        let recorded = match cut {
            Some(lsns) => self.record_cut(lsns, &staged_names).await,
            None => false,
        };

        // Checked after every flush, successful or not: this is the only place
        // that observes whether uploads are keeping up with checkpoints.
        self.enforce_staging_ceiling();

        if report.bundles > 0 || recorded {
            self.publish_manifest().await?;
            info!(
                "Archived {} log segments in {} bundle(s), {} -> {} bytes, through LSN {}",
                report.segments,
                report.bundles,
                report.uncompressed_bytes,
                report.compressed_bytes,
                report.last_lsn
            );
        }
        Ok(report)
    }

    /// Reads a [`read_cut`] across every log and cuts them all behind it, when
    /// some database keeps tables on a storage location. `None` when none does,
    /// or when this pass cannot produce a cut — the next pass tries again, and
    /// a restore stops at the newest cut recorded meanwhile.
    async fn cut_together(self: &Arc<Self>) -> Option<Vec<manifest::CutLsn>> {
        if self.archiving_suspended.load(Ordering::Acquire) {
            return None;
        }
        let (streams, problems) = self.log_streams_checked();
        if !streams.iter().any(|(key, _)| key.starts_with(LOCATION_KEY_PREFIX)) {
            return None;
        }
        if !problems.is_empty() {
            // A log nobody can name is in no cut, and its database's outcomes
            // would be cut without it.
            warn!(
                "Backup of {}: no restore point this pass: {}",
                self.location,
                problems.join("; ")
            );
            return None;
        }
        let lsns = match read_cut(&streams) {
            Ok(lsns) => lsns,
            Err(e) => {
                warn!("Backup of {}: no restore point this pass: {e}", self.location);
                return None;
            }
        };
        if let Err(e) = self.checkpoint_all_logs().await {
            warn!("Backup of {}: {e}", self.location);
        }
        // A cut is only a restore point once everything through it is staged:
        // a checkpoint declines while another runs or an in-place write is
        // pending. `staged_covers` also asks whether anything after its first
        // argument was dropped unstaged — a snapshot window's question, so it
        // is passed the largest value and answers only the first.
        if let Some(((key, _), _)) = streams
            .iter()
            .zip(&lsns)
            .find(|((_, wal), (_, lsn))| !wal.staged_covers(u64::MAX, *lsn))
        {
            debug!(
                "Backup of {}: the log of {key:?} is not staged through the cut; no restore \
                 point this pass",
                self.location
            );
            return None;
        }
        Some(
            lsns.into_iter()
                .map(|(database, lsn)| manifest::CutLsn {
                    database,
                    lsn: Some(lsn),
                })
                .collect(),
        )
    }

    /// Records a cut whose bundles are all in the catalogue. Returns whether
    /// the catalogue changed.
    ///
    /// A log the catalogue knows that was not open at the cut wrote nothing
    /// then, so all of it is before the cut. One that was not
    /// open then but is now opened during this pass: what it logged since is
    /// after the cut and what it archived before is not, which one number
    /// cannot say — so no cut this pass.
    async fn record_cut(&self, mut lsns: Vec<manifest::CutLsn>, staged: &[String]) -> bool {
        let open_now: Vec<String> = self.log_streams().into_iter().map(|(key, _)| key).collect();
        let mut manifest = self.manifest.lock().await;
        // Every log a snapshot window names too: a cut that leaves one out
        // cannot serve that snapshot, and its restores would stop at the copy.
        let mut history = manifest.databases_with_segments();
        history.extend(staged.iter().cloned());
        for snapshot in &manifest.snapshots {
            history.extend(snapshot.windows.iter().map(|w| w.database.clone()));
        }
        history.sort();
        history.dedup();
        for database in history {
            if lsns.iter().any(|l| l.database == database) {
                continue;
            }
            if open_now.contains(&database) {
                debug!(
                    "Backup of {}: {database:?} opened during the cut; no restore point this pass",
                    self.location
                );
                return false;
            }
            lsns.push(manifest::CutLsn {
                database,
                lsn: None,
            });
        }
        lsns.sort_by(|a, b| a.database.cmp(&b.database));
        let segment_sequence = manifest.next_segment_sequence();
        manifest.record_cut(manifest::LogCut {
            segment_sequence,
            archived_at: timeutil::now_unix(),
            lsns,
        })
    }

    /// Staged segments, one entry per log, each in log order.
    ///
    /// The root's sit directly in the staging directory; every other log has a
    /// subdirectory named for its database.
    fn staged_by_stream(&self) -> io::Result<Vec<(String, Vec<PathBuf>)>> {
        fn segments_in(dir: &Path) -> io::Result<Vec<PathBuf>> {
            let mut staged = Vec::new();
            let entries = match std::fs::read_dir(dir) {
                Ok(e) => e,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(staged),
                Err(e) => return Err(e),
            };
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                // `.partial` files are still being copied; leaving them for the
                // next flush is what makes staging atomic.
                if name.ends_with(crate::core::wal::SEGMENT_SUFFIX) {
                    staged.push(entry.path());
                }
            }
            // Names are zero-padded hex of a monotonic counter, so sorting them
            // reproduces log order.
            staged.sort();
            Ok(staged)
        }

        let root = self.staging_dir();
        let mut streams = vec![(String::new(), segments_in(&root)?)];

        // Subdirectories are read from disk rather than from `log_streams`, so
        // a database dropped while its last segments were still staged still
        // has them uploaded instead of stranded.
        let entries = match std::fs::read_dir(&root) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(streams),
            Err(e) => return Err(e),
        };
        for entry in entries.flatten() {
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str().map(str::to_string) else {
                continue;
            };
            streams.push((name, segments_in(&entry.path())?));
        }
        streams.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(streams)
    }

    /// Every staged segment under this location, whichever log cut it.
    fn staged_segments(&self) -> io::Result<Vec<PathBuf>> {
        Ok(self
            .staged_by_stream()?
            .into_iter()
            .flat_map(|(_, paths)| paths)
            .collect())
    }

    async fn next_segment_sequence(&self) -> u64 {
        self.manifest.lock().await.next_segment_sequence()
    }

    // ---- catalogue ---------------------------------------------------------

    async fn publish_manifest(&self) -> io::Result<()> {
        let (key, body) = {
            let mut manifest = self.manifest.lock().await;
            manifest.updated_at = timeutil::now_unix();
            // Refreshed rather than written once: a database directory that
            // moves would otherwise leave a restore repointing a storage pool
            // at a path this installation no longer uses.
            manifest.source_location = self.location.clone();
            (self.config.key(manifest::MANIFEST_KEY), manifest.encode())
        };
        self.client.put_object(&key, &body).await?;
        Ok(())
    }

    /// Drops snapshots past the retention count, and the segments that only
    /// they needed.
    ///
    /// A segment is kept while any surviving snapshot might replay it, which
    /// means anything whose records could belong to or follow the oldest
    /// remaining snapshot's copy window.
    pub async fn prune(self: &Arc<Self>) -> io::Result<u32> {
        self.prune_keeping(self.config.retain_snapshots).await
    }

    /// Drops everything past the newest `keep` snapshots.
    ///
    /// Split out from [`Self::prune`] so a rollover can ask for one generation
    /// without the configured retention having to say so — the two are separate
    /// policies over the same mechanism.
    pub async fn prune_keeping(self: &Arc<Self>, keep: usize) -> io::Result<u32> {
        if keep == 0 {
            return Ok(0);
        }

        let doomed: Vec<String> = {
            let mut manifest = self.manifest.lock().await;
            let plan = manifest.plan_retention(keep);
            if plan.doomed.is_empty() {
                return Ok(0);
            }
            manifest.snapshots = plan.snapshots;
            manifest.segments = plan.segments;
            plan.doomed
        };

        // Catalogue first. An object that outlives its entry is orphaned
        // storage; an entry that outlives its object is a restore that fails
        // partway through, which is far worse.
        self.publish_manifest().await?;
        self.client.delete_objects(&doomed).await?;

        info!("Backup retention removed {} objects", doomed.len());
        Ok(doomed.len() as u32)
    }

    /// Takes a fresh full snapshot and then removes every older generation.
    ///
    /// Deliberately in that order. The delete only runs if the snapshot
    /// succeeded, so a failed rollover leaves the previous backup exactly as it
    /// was rather than leaving the bucket empty — the one outcome worse than a
    /// stale backup is no backup.
    pub async fn rollover_now(self: &Arc<Self>) -> io::Result<(SnapshotReport, u32)> {
        // Full, never incremental: the prune below removes every older
        // generation, and a snapshot that reused their objects would lose them.
        let report = self.take_snapshot(true).await?;
        let removed = self.prune_keeping(1).await?;

        // Stamped only once both halves are done, and published on its own
        // because `prune_keeping` writes nothing when it finds nothing to
        // delete. This is the only trace a rollover leaves — the snapshot it
        // took is indistinguishable from any other — and the next one's
        // schedule is measured from it across restarts.
        //
        // Best-effort: the rollover itself succeeded, and failing it here would
        // report a backup that exists as a backup that did not happen. The cost
        // of the stamp not landing is one rollover sooner than asked for.
        {
            self.manifest.lock().await.last_rollover_at = timeutil::now_unix();
        }
        if let Err(e) = self.publish_manifest().await {
            warn!("Rollover finished but its timestamp could not be recorded: {e}");
        }

        info!(
            "Backup rolled over: new snapshot {} kept, {} object(s) from earlier generations removed",
            report.key, removed
        );
        Ok((report, removed))
    }

    // ---- background --------------------------------------------------------

    /// Unix seconds the newest snapshot finished, if the catalogue holds one.
    async fn last_snapshot_at(&self) -> Option<u64> {
        self.manifest
            .lock()
            .await
            .snapshots_newest_first()
            .first()
            .map(|s| s.finished_at)
    }

    /// Unix seconds the newest segment bundle was archived, if there is one.
    async fn last_archive_at(&self) -> Option<u64> {
        self.manifest
            .lock()
            .await
            .segments
            .iter()
            .map(|s| s.archived_at)
            .max()
    }

    /// What the rollover schedule measures from.
    ///
    /// The recorded rollover when there is one. A catalogue written before that
    /// field existed reports 0, and the newest snapshot stands in for it: that
    /// is the last time the prefix was refreshed, so an upgraded server waits
    /// out the remainder of an interval instead of rolling over — and deleting
    /// every earlier generation — the moment it starts.
    async fn last_rollover_at(&self) -> Option<u64> {
        let manifest = self.manifest.lock().await;
        match manifest.last_rollover_at {
            0 => manifest
                .snapshots_newest_first()
                .first()
                .map(|s| s.finished_at),
            recorded => Some(recorded),
        }
    }

    /// Starts the scheduled snapshot, rollover and archive tasks.
    ///
    /// Each one measures its first run from what the catalogue says was last
    /// done, not from the moment this process started — see [`first_delay`] for
    /// what that fixes.
    pub fn spawn_background(self: &Arc<Self>) {
        if self.config.wal_flush_secs > 0 {
            let service = self.clone();
            let interval = Duration::from_secs(service.config.wal_flush_secs);
            tokio::spawn(async move {
                // This interval is a recovery point objective, and a restart is
                // not a reason to widen it. A crash is a reason to narrow it:
                // segments the previous run staged and never uploaded are still
                // sitting there, and starting from the last archive rather than
                // from now sends them at once.
                let mut delay = match service.last_archive_at().await {
                    Some(last) => first_delay(interval, last),
                    None => Duration::ZERO,
                };
                loop {
                    tokio::time::sleep(delay).await;
                    delay = interval;
                    if service.shutting_down.load(Ordering::Acquire) {
                        return;
                    }
                    // Cut the log first, so the interval means "recoverable to
                    // within this long" rather than "whatever happened to have
                    // been checkpointed".
                    if let Err(e) = service.archive_wal_now().await {
                        error!("Scheduled log archive failed: {e}");
                    }
                }
            });
        }

        // The snapshot schedule — and the base snapshot every other object in
        // the prefix rests on, which is why this task is spawned even when
        // snapshots are set to manual.
        //
        // Archiving starts the moment the service attaches to the log, so a
        // prefix with no snapshot in it is one accumulating segments that
        // nothing can be replayed onto: a growing bill for a backup that cannot
        // restore. "Manual" is a choice about the schedule, not a request for
        // that.
        {
            let service = self.clone();
            let interval = Duration::from_secs(self.config.snapshot_interval_secs);
            // Slowest a failing base snapshot is retried at. Capped by the
            // interval too, so a short schedule is never overtaken by its own
            // retries.
            let retry_ceiling = if interval.is_zero() {
                BASE_RETRY_CEILING
            } else {
                interval.min(BASE_RETRY_CEILING)
            };
            tokio::spawn(async move {
                let mut retry = BASE_RETRY_START;
                let mut delay = match service.last_snapshot_at().await {
                    // Nothing to restore onto: due now, whatever the schedule
                    // says.
                    None => Some(Duration::ZERO),
                    // A base is already there and no schedule was asked for.
                    Some(_) if interval.is_zero() => None,
                    Some(last) => Some(first_delay(interval, last)),
                };
                while let Some(wait) = delay {
                    tokio::time::sleep(wait).await;
                    if service.shutting_down.load(Ordering::Acquire) {
                        return;
                    }
                    let failed = match service.snapshot_now().await {
                        Ok(_) => false,
                        Err(e) => {
                            error!("Scheduled snapshot failed: {e}");
                            true
                        }
                    };
                    // A base snapshot that failed leaves the prefix
                    // unrestorable with the log piling up behind it, so it is
                    // retried rather than left until the next interval — a day
                    // away, by default. Backing off doubles the wait, so a
                    // bucket that is simply misconfigured is not re-uploading
                    // the whole database every minute.
                    delay = if failed && service.last_snapshot_at().await.is_none() {
                        let wait = retry;
                        retry = (retry * 2).min(retry_ceiling);
                        Some(wait)
                    } else if interval.is_zero() {
                        None
                    } else {
                        retry = BASE_RETRY_START;
                        Some(interval)
                    };
                }
            });
        }

        if self.config.rollover_interval_secs > 0 {
            let service = self.clone();
            let interval = Duration::from_secs(service.config.rollover_interval_secs);
            tokio::spawn(async move {
                // Nothing recorded means there is nothing to reclaim yet: the
                // snapshot task above is taking the base right now, and rolling
                // over on top of it would mean a second full copy taken only to
                // delete the first.
                let mut delay = match service.last_rollover_at().await {
                    Some(last) => first_delay(interval, last),
                    None => interval,
                };
                loop {
                    tokio::time::sleep(delay).await;
                    delay = interval;
                    if service.shutting_down.load(Ordering::Acquire) {
                        return;
                    }
                    // A failure is logged and the old generation is kept: the
                    // next rollover tries again, and until one succeeds there is
                    // still something to restore from.
                    if let Err(e) = service.rollover_now().await {
                        error!("Scheduled backup rollover failed: {e}");
                    }
                }
            });
        }

        // Staged bytes can cross the bundle threshold well before the interval
        // does on a write-heavy database. Checking often and uploading rarely
        // keeps the staging directory bounded without adding requests.
        if self.config.wal_bundle_bytes > 0 {
            let service = self.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    if service.shutting_down.load(Ordering::Acquire) {
                        return;
                    }
                    let staged = service.staged_bytes();
                    if staged >= service.config.wal_bundle_bytes {
                        if let Err(e) = service.flush_staged_segments().await {
                            error!("Log archive flush failed: {e}");
                        }
                    } else {
                        // A flush would have done this on its way out; without
                        // it a suspended service that has drained would stay
                        // suspended until the next bundle-sized burst.
                        service.enforce_staging_ceiling();
                    }
                }
            });
        }
    }

    /// Final archive flush, so a clean shutdown loses no recoverable window.
    pub async fn shutdown(self: &Arc<Self>) {
        self.shutting_down.store(true, Ordering::Release);
        if let Err(e) = self.archive_wal_now().await {
            error!("Final log archive on shutdown failed: {e}");
        }
    }
}

/// How often [`read_cut`] tries before giving up on a busy server.
const CUT_ATTEMPTS: u32 = 10_000;

/// Every log's commit sequence, all at one instant, as `(stream key, LSN)` in
/// the order of `streams`.
///
/// Without a storage location there is nothing to pair — each database has its
/// own status — and one read of each log is the cut. With one, the rows of a
/// transaction are in the location's log and its outcome in its database's,
/// and two independent reads can pair an outcome with a location log that
/// does not hold its rows yet. So the databases are read first and the
/// locations after: rows are logged before their outcome, so every outcome the
/// first read takes in has its rows in the second. The databases are then read
/// again, and the whole read repeated if any moved: nothing a location logged
/// between the reads can then rest on an outcome the cut leaves out, since
/// whatever acts on an outcome — vacuum freeing a deleted version, say — runs
/// only once that outcome is logged. What a location holds past its read is
/// rows of transactions still in flight at the cut, as after a crash there.
fn read_cut(streams: &[(String, Arc<crate::core::wal::Wal>)]) -> io::Result<Vec<(String, u64)>> {
    let is_location = |key: &str| key.starts_with(LOCATION_KEY_PREFIX);
    if !streams.iter().any(|(key, _)| is_location(key)) {
        return Ok(streams
            .iter()
            .map(|(key, wal)| (key.clone(), wal.commit_lsn()))
            .collect());
    }
    let read = |locations: bool| -> Vec<u64> {
        streams
            .iter()
            .filter(|(key, _)| is_location(key) == locations)
            .map(|(_, wal)| wal.commit_lsn())
            .collect()
    };
    for attempt in 0..CUT_ATTEMPTS {
        let databases = read(false);
        let locations = read(true);
        if read(false) == databases {
            let (mut databases, mut locations) = (databases.into_iter(), locations.into_iter());
            return Ok(streams
                .iter()
                .map(|(key, _)| {
                    let lsn = if is_location(key) {
                        locations.next()
                    } else {
                        databases.next()
                    };
                    (key.clone(), lsn.expect("one reading per log"))
                })
                .collect());
        }
        if attempt % 64 == 63 {
            std::thread::yield_now();
        } else {
            std::hint::spin_loop();
        }
    }
    Err(io::Error::other(format!(
        "could not read the logs at one instant: a database committed during each of \
         {CUT_ATTEMPTS} attempts"
    )))
}

/// See [`BackupService::staged_lengths`]: keyed by the name the snapshot gives
/// the sidecar, a location's under its stream key.
fn staged_lengths_in(
    root: &Path,
    locations: &[(String, String)],
) -> io::Result<HashMap<String, u64>> {
    fn walk(dir: &Path, prefix: &str, lengths: &mut HashMap<String, u64>) -> io::Result<()> {
        for entry in std::fs::read_dir(dir)?.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Ok(file_type) = entry.file_type() else {
                continue;
            };

            // Sub-databases have sidecars of their own, and a fold racing
            // the copy is exactly as undetectable there as at the root.
            if file_type.is_dir() {
                if EXCLUDED.contains(&name) {
                    continue;
                }
                walk(&entry.path(), &format!("{prefix}{name}/"), lengths)?;
                continue;
            }
            if !name.ends_with(".tmp") {
                continue;
            }
            lengths.insert(
                format!("{prefix}{name}"),
                entry.metadata().map(|m| m.len()).unwrap_or(0),
            );
        }
        Ok(())
    }

    let mut lengths = HashMap::new();
    walk(root, "", &mut lengths)?;
    // Flat, as the copy is; see `snapshot_file_list`.
    for (key, path) in locations {
        for entry in std::fs::read_dir(path)?.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.ends_with(".tmp") && entry.file_type().map(|t| t.is_file()).unwrap_or(false)
            {
                lengths.insert(
                    format!("{key}/{name}"),
                    entry.metadata().map(|m| m.len()).unwrap_or(0),
                );
            }
        }
    }
    Ok(lengths)
}

/// See [`BackupService::check_staged_appends`].
fn folded_while_copying(before: &HashMap<String, u64>, after: &HashMap<String, u64>) -> io::Result<()> {
    for (name, was) in before {
        if *was == 0 {
            continue;
        }
        let now = after.get(name).copied().unwrap_or(0);
        if now < *was {
            return Err(io::Error::other(format!(
                "{name} was folded into its main file while the snapshot was being copied,                      so up to {} bytes are in neither the copy nor the log. Discarding this                      snapshot; retry it.",
                was - now
            )));
        }
    }
    Ok(())
}

/// What a snapshot's copy may find gone: a file a `DROP` deleted after the
/// copy began.
///
/// The deletion gate is held only while a snapshot fixes where its copy
/// begins, so a `DROP` during the upload deletes files the snapshot already
/// listed. Such a file needs no copy: its barrier committed past the
/// window's start, and a restore empties the file there whatever the copy
/// held (see [`crate::core::wal::Wal::recover_restoring`]). Anything else
/// gone is not explained and fails the snapshot. A rename or a move is the
/// case that matters: their copies write no log record, so a file one of
/// them took away would vanish from the snapshot and from every restore
/// after it; they wait for the upload instead (see [`RELOCATION_GATE`]).
#[derive(Clone)]
struct DroppedDuringCopy {
    /// Each stream's directory, as [`crate::core::wal::dir_key`] spells it,
    /// with its log and the start of its window: the database directories
    /// here and the storage locations outside them alike.
    windows: Arc<HashMap<String, (Arc<crate::core::wal::Wal>, u64)>>,
}

impl DroppedDuringCopy {
    fn new(
        streams: &[(String, Arc<crate::core::wal::Wal>)],
        opened_at: &HashMap<String, u64>,
    ) -> Self {
        let windows = streams
            .iter()
            .filter_map(|(key, wal)| {
                let dir = wal.path().parent()?.to_str()?;
                let start = opened_at.get(key).copied().unwrap_or(0);
                Some((crate::core::wal::dir_key(dir), (wal.clone(), start)))
            })
            .collect();
        DroppedDuringCopy {
            windows: Arc::new(windows),
        }
    }

    /// Whether a `DROP` barrier for `path` committed past the start of the
    /// window of the directory it is in.
    fn explains(&self, path: &Path) -> bool {
        let (Some(dir), Some(name)) = (
            path.parent().and_then(Path::to_str),
            path.file_name().and_then(|n| n.to_str()),
        ) else {
            return false;
        };
        // A table file's `.tmp` staging sibling has no log record and no
        // barrier of its own; the drop deletes it with the file it stages
        // for, and is that file's barrier.
        let name = name.strip_suffix(".tmp").unwrap_or(name);
        let id = crate::core::wal::file_id_for_name(name);
        match self.windows.get(&crate::core::wal::dir_key(dir)) {
            Some((wal, start)) => wal.dropped_after(id, *start),
            // A database created during the copy: its window opens at 0, as
            // the catalogue records it.
            None => crate::core::wal::wal_for(dir).is_some_and(|wal| wal.dropped_after(id, 0)),
        }
    }

    /// `Ok` when the copy may leave `path` out after reaching it failed with
    /// `error`; the error otherwise, a missing file's naming the file.
    fn leave_out(&self, path: &Path, error: io::Error) -> io::Result<()> {
        if error.kind() != io::ErrorKind::NotFound {
            return Err(error);
        }
        if self.explains(path) {
            info!(
                "{} was dropped while the snapshot copied it; left out, as a restore empties \
                 it at the drop",
                path.display()
            );
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "{} was listed for the snapshot and is gone, and no DROP committed during the \
                 copy explains it: {error}",
                path.display()
            ),
        ))
    }
}

/// A snapshot parked between listing its files and copying them, for tests.
#[cfg(test)]
pub(crate) mod upload_hooks {
    use tokio::sync::oneshot;

    struct Park {
        reached: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    }

    static PARKS: parking_lot::Mutex<Vec<(String, Park)>> = parking_lot::Mutex::new(Vec::new());

    /// Parks the next snapshot of `location` once it has listed its files.
    /// The first receiver fires when it is parked; the sender releases it.
    pub(crate) fn park_next(location: &str) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, parked) = oneshot::channel();
        let (release, released) = oneshot::channel();
        PARKS.lock().push((
            location.to_string(),
            Park {
                reached,
                release: released,
            },
        ));
        (parked, release)
    }

    pub(super) async fn parked_here(location: &str) {
        let park = {
            let mut parks = PARKS.lock();
            parks
                .iter()
                .position(|(at, _)| at == location)
                .map(|i| parks.swap_remove(i).1)
        };
        if let Some(park) = park {
            let _ = park.reached.send(());
            let _ = park.release.await;
        }
    }
}

/// Where `dir` sits relative to a backup location, as a stream key.
///
/// `Some("")` for the location itself, `Some("shop")` for a database created
/// inside it, and `None` for a directory that is not under it — including the
/// sibling trap, where `/var/lib/rasterizeddb2` starts with the same characters
/// as `/var/lib/rasterizeddb` but is a different installation.
fn stream_key(root: &str, dir: &str) -> Option<String> {
    let dir = crate::core::wal::dir_key(dir);
    if dir == root {
        return Some(String::new());
    }
    let rest = dir.strip_prefix(root)?;
    let rest = rest.strip_prefix(['/', '\\'])?;
    if rest.is_empty() {
        return None;
    }
    Some(rest.replace('\\', "/"))
}

/// Installs the service and starts its background tasks.
pub async fn install(config: BackupConfig, location: &str) -> io::Result<Arc<BackupService>> {
    let service = BackupService::connect(config, location).await?;
    service.spawn_background();
    let _ = SERVICE.set(service.clone());
    Ok(service)
}

/// Restores from a prefix without a running service, for the CLI's pre-open
/// path and for cloning into a fresh directory.
pub async fn restore_from(
    config: &BackupConfig,
    target_dir: &Path,
    target: RecoveryTarget,
) -> io::Result<restore::RestoreReport> {
    let client = S3Client::new(S3Config {
        endpoint: config.endpoint.clone(),
        region: config.region.clone(),
        bucket: config.bucket.clone(),
        credentials: config.credentials.clone(),
        path_style: config.path_style,
        max_attempts: config.max_attempts,
    })?;
    restore::run(&client, config, target_dir, target).await
}

// ---- shared plumbing -------------------------------------------------------

/// A zstd encoder configured for this backup.
/// Hex SHA-256 of the first `len` bytes of `path`.
pub(crate) fn hash_prefix(path: &Path, len: u64) -> io::Result<String> {
    use sha2::Digest;
    let mut file = std::io::BufReader::with_capacity(256 * 1024, std::fs::File::open(path)?);
    let mut digest = sha2::Sha256::new();
    let mut chunk = vec![0u8; 1024 * 1024];
    let mut remaining = len;
    while remaining > 0 {
        let want = chunk.len().min(remaining as usize);
        file.read_exact(&mut chunk[..want])?;
        digest.update(&chunk[..want]);
        remaining -= want as u64;
    }
    Ok(to_hex(&digest.finalize()))
}

pub(crate) fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Compresses one log bundle whose size is known up front.
///
/// The size is pledged to the encoder. Without it zstd has to assume an
/// unbounded stream and sets up the whole configured window — 128 MiB with
/// long-distance matching — for a bundle that is usually a few megabytes; with
/// it the window and match tables are sized to the input, and the frame
/// records the content size. A pledge that turned out wrong fails `finish`
/// rather than producing a bad frame, and the size here is `body.len()` itself.
fn compress_bundle(body: &[u8], level: i32, threads: u32, long_range: bool) -> io::Result<Vec<u8>> {
    // Log records compress well; a quarter is a cheap first guess that saves
    // the early regrowths without over-reserving much.
    let mut encoder = new_encoder(Vec::with_capacity(body.len() / 4), level, threads, long_range)?;
    encoder.set_pledged_src_size(Some(body.len() as u64))?;
    encoder.write_all(body)?;
    encoder.finish()
}

fn new_encoder<W: Write>(
    sink: W,
    level: i32,
    threads: u32,
    long_range: bool,
) -> io::Result<zstd::stream::write::Encoder<'static, W>> {
    let mut encoder = zstd::stream::write::Encoder::new(sink, level)?;

    if threads > 1 {
        // Worker threads compress independent blocks, so this is throughput at
        // no cost in ratio.
        encoder.multithread(threads)?;
    }

    if long_range {
        use zstd::zstd_safe::CParameter;
        encoder.set_parameter(CParameter::EnableLongDistanceMatching(true))?;
        // A 128 MiB window. Database files repeat their record framing and
        // schema headers across far more than zstd's default window, and this
        // is where most of the ratio on a large snapshot comes from. The memory
        // is transient and belongs to the compressor alone.
        encoder.set_parameter(CParameter::WindowLog(27))?;
    }

    // Frame checksum, on top of the per-entry CRC. Costs a hash of the
    // compressed stream and catches corruption in transit or at rest.
    encoder.include_checksum(true)?;
    Ok(encoder)
}

/// Whether the staging ceiling calls for a state change.
///
/// `Some(true)` means suspend, `Some(false)` means resume, `None` means leave
/// it alone. Resumption uses half the ceiling rather than the ceiling itself,
/// so a directory hovering at the limit does not flap between the two states
/// once per tick.
/// How long a task that runs every `interval` should wait before its first run,
/// given the unix second its last run finished.
///
/// The background loops used to sleep a whole interval before doing anything,
/// which makes every schedule relative to the *process* rather than to the
/// catalogue: each restart pushes the next run a full interval away, so a
/// server restarted more often than its interval never reaches one at all. That
/// fails silently — nothing errors, the loop is running, and the bucket simply
/// never gains a snapshot. Measuring from the last recorded run makes a restart
/// resume the schedule instead of resetting it.
///
/// A run that is already overdue returns zero: due now. So does a clock stepped
/// backwards, or a catalogue stamped by a machine running ahead of this one —
/// `saturating_sub` reads a last run "in the future" as due rather than as a
/// wait of several hundred years.
fn first_delay(interval: Duration, last_finished_at: u64) -> Duration {
    let elapsed = Duration::from_secs(timeutil::now_unix().saturating_sub(last_finished_at));
    interval.saturating_sub(elapsed)
}

fn staging_action(staged: u64, ceiling: u64, suspended: bool) -> Option<bool> {
    if ceiling == 0 {
        // No ceiling configured. A service already suspended is still allowed
        // to resume, so turning the limit off does not strand it.
        return suspended.then_some(false);
    }
    if !suspended && staged >= ceiling {
        return Some(true);
    }
    if suspended && staged <= ceiling / 2 {
        return Some(false);
    }
    None
}

/// Part size for a set of files, from their total size on disk.
fn part_size_from_files(configured: usize, files: &[(String, PathBuf)]) -> usize {
    let raw: u64 = files
        .iter()
        .filter_map(|(_, p)| std::fs::metadata(p).ok().map(|m| m.len()))
        .sum();
    part_size_for(configured, raw)
}

/// Part size that keeps `raw` bytes inside S3's 10 000-part ceiling.
///
/// Compression only shrinks things, so sizing from the uncompressed total is
/// conservative in the right direction: the upload ends up with fewer parts
/// than this planned for, never more.
fn part_size_for(configured: usize, raw: u64) -> usize {
    let configured = configured.max(MIN_PART_SIZE);
    let needed = raw.div_ceil(MAX_PARTS) as usize;
    configured.max(needed)
}

/// Splits staged segment paths into groups no larger than `max_bytes`.
fn group_by_size(paths: &[PathBuf], max_bytes: u64) -> io::Result<Vec<Vec<PathBuf>>> {
    let cap = max_bytes.max(1);
    let mut groups: Vec<Vec<PathBuf>> = Vec::new();
    let mut current: Vec<PathBuf> = Vec::new();
    let mut current_bytes = 0u64;

    for path in paths {
        let len = std::fs::metadata(path)?.len();
        // A single segment larger than the cap still goes out on its own rather
        // than being split: a bundle has to stay a whole number of segments for
        // the concatenation to replay.
        if !current.is_empty() && current_bytes + len > cap {
            groups.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current.push(path.clone());
        current_bytes += len;
    }
    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

/// Reads this database's backup identity, creating one on first use.
///
/// It exists so a prefix cannot silently accumulate two databases' backups,
/// which would produce a catalogue whose snapshots and segments describe
/// different data and whose restores would interleave them.
fn read_or_create_instance_id(location: &str) -> io::Result<String> {
    let path = Path::new(location).join(INSTANCE_FILE);
    match std::fs::read_to_string(&path) {
        Ok(existing) => {
            let trimmed = existing.trim().to_string();
            if trimmed.is_empty() {
                return Err(io::Error::other(format!(
                    "{} is empty; delete it to have a new instance id generated",
                    path.display()
                )));
            }
            Ok(trimmed)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let id = format!(
                "rdb-{}-{:016x}",
                timeutil::format_basic(timeutil::now_unix()),
                fastrand::u64(..)
            );
            std::fs::write(&path, &id)?;
            crate::core::wal::sync_parent_dir(&path)?;
            Ok(id)
        }
        Err(e) => Err(e),
    }
}

/// A `Write` that cuts its input into upload parts.
///
/// The compressor writes into this from a blocking thread; the bounded channel
/// behind it is what stops a fast disk from queueing the whole database in
/// memory ahead of a slow uplink.
struct PartSink {
    tx: kanal::Sender<Vec<u8>>,
    buf: Vec<u8>,
    part_size: usize,
}

impl PartSink {
    fn new(tx: kanal::Sender<Vec<u8>>, part_size: usize) -> Self {
        Self {
            tx,
            buf: Vec::with_capacity(part_size),
            part_size,
        }
    }

    /// Releases the tail as the final part and signals end of stream.
    ///
    /// The sender is *dropped* rather than closed: `kanal::Sender::close`
    /// clears the queue, so calling it here would discard every part the
    /// uploader had not yet taken — the whole tail of the snapshot.
    fn finish(mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            let part = std::mem::take(&mut self.buf);
            self.send(part)?;
        }
        drop(self);
        Ok(())
    }

    fn send(&self, part: Vec<u8>) -> io::Result<()> {
        self.tx
            .send(part)
            .map_err(|_| io::Error::other("the snapshot uploader stopped accepting parts"))
    }
}

impl Write for PartSink {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        while self.buf.len() >= self.part_size {
            let rest = self.buf.split_off(self.part_size);
            let part = std::mem::replace(&mut self.buf, rest);
            self.send(part)?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod bundle_tests {
    use super::*;

    /// Log-shaped bytes: a fixed frame around a few varying fields.
    fn log_like(len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len + 64);
        let mut i = 0u64;
        while out.len() < len {
            out.extend_from_slice(&0x4C52_5741u32.to_le_bytes());
            out.extend_from_slice(&[1, 0, 0, 0]);
            out.extend_from_slice(&i.to_le_bytes());
            out.extend_from_slice(&(i * 7919 % 1_000_003).to_le_bytes());
            out.extend_from_slice(format!("employee_{i}|dept_{}|", i % 32).as_bytes());
            i += 1;
        }
        out.truncate(len);
        out
    }

    #[test]
    fn a_pledged_bundle_round_trips() {
        for len in [0usize, 1, 4096, 3 << 20] {
            let body = log_like(len);
            for threads in [1, 4] {
                let compressed = compress_bundle(&body, 6, threads, true).unwrap();
                let back = zstd::stream::decode_all(&compressed[..]).unwrap();
                assert_eq!(back, body, "len {len} threads {threads}");
            }
        }
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// bundle_compress_bench --nocapture`. The configured encoder with and
    /// without the size pledged, on bundles of a few sizes.
    #[test]
    #[ignore]
    fn bundle_compress_bench() {
        for len in [256 << 10, 4 << 20, 32 << 20] {
            let body = log_like(len);
            let iters = (64 << 20) / len.max(1) + 1;
            for round in 0..3 {
                let t = std::time::Instant::now();
                let mut old_size = 0;
                for _ in 0..iters {
                    let mut e = new_encoder(Vec::new(), 6, 1, true).unwrap();
                    e.write_all(&body).unwrap();
                    old_size = e.finish().unwrap().len();
                }
                let old = t.elapsed() / iters as u32;
                let t = std::time::Instant::now();
                let mut new_size = 0;
                for _ in 0..iters {
                    new_size = compress_bundle(&body, 6, 1, true).unwrap().len();
                }
                let new = t.elapsed() / iters as u32;
                println!(
                    "{} KiB round {round}: unpledged {old:?} ({old_size} B)  pledged {new:?} ({new_size} B)",
                    len >> 10
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Persistent indexes §10 (B14): no snapshot carries an index-store
    /// file, so a restore never pairs a copied `CLEAN` with rows changed
    /// after it; the WAL'd `rdb.ixe` and every table file are kept.
    /// Mutation: `excluded_file` checks `EXCLUDED` only.
    #[test]
    fn index_store_files_are_left_out_of_snapshots() {
        for name in ["t.ixs", "t.ixm", "t.ixs.tmp", "t.ixm.tmp", "__pt_00000000000000aa_00000001.ixs"] {
            assert!(excluded_file(name), "{name} would be backed up");
        }
        for name in ["rdb.ixe", "t.db", "t_pointers.db", "t_schema.db", "t.db.tmp", "t_ixs.db"] {
            assert!(!excluded_file(name), "{name} would be left out");
        }
    }

    #[test]
    fn a_schedule_resumes_from_the_last_run_rather_than_restarting() {
        let day = Duration::from_secs(86_400);
        let now = timeutil::now_unix();

        // The bug this exists for: a restart used to put the next run a whole
        // interval away, so a server restarted daily never snapshotted.
        assert_eq!(
            first_delay(day, now - 3600),
            day - Duration::from_secs(3600)
        );
        assert_eq!(first_delay(day, now - 86_000), Duration::from_secs(400));
    }

    #[test]
    fn an_overdue_run_is_due_now() {
        let day = Duration::from_secs(86_400);
        let now = timeutil::now_unix();
        assert_eq!(first_delay(day, now - 86_400), Duration::ZERO);
        assert_eq!(first_delay(day, now - 7 * 86_400), Duration::ZERO);
    }

    #[test]
    fn a_last_run_in_the_future_is_due_now_rather_than_centuries_away() {
        // A clock stepped back by NTP, or a catalogue stamped by a machine
        // running ahead of this one. Subtracting into a huge unsigned elapsed
        // would park the task past the heat death of the sun.
        let day = Duration::from_secs(86_400);
        let now = timeutil::now_unix();
        assert_eq!(first_delay(day, now + 500), day);
        assert_eq!(first_delay(day, u64::MAX), day);
    }

    #[test]
    fn a_rollover_stamp_survives_a_catalogue_that_predates_it() {
        // Old manifests have no `last_rollover_at`; serde fills 0, which the
        // schedule reads as "fall back to the newest snapshot" rather than as
        // 1970 — which would roll over, and delete every earlier generation,
        // the moment an upgraded server started.
        let json = br#"{"version":1,"instance_id":"rdb-x","updated_at":7}"#;
        let manifest = manifest::Manifest::decode(json).unwrap();
        assert_eq!(manifest.last_rollover_at, 0);
        assert!(manifest.snapshots.is_empty());
    }

    #[test]
    fn part_sink_cuts_at_the_configured_size() {
        let (tx, rx) = kanal::unbounded::<Vec<u8>>();
        let mut sink = PartSink::new(tx, 10);
        sink.write_all(&[1u8; 25]).unwrap();
        sink.finish().unwrap();

        let parts: Vec<Vec<u8>> = rx.into_iter().collect();
        assert_eq!(
            parts.iter().map(|p| p.len()).collect::<Vec<_>>(),
            vec![10, 10, 5]
        );
    }

    #[test]
    fn groups_segments_under_the_cap() {
        let dir = temp_testdir::TempDir::default();
        let mut paths = Vec::new();
        for (i, len) in [30usize, 30, 30, 90].iter().enumerate() {
            let path = dir.join(format!("{i}.wseg"));
            std::fs::write(&path, vec![0u8; *len]).unwrap();
            paths.push(path);
        }

        let groups = group_by_size(&paths, 100).unwrap();
        // 30+30+30 fits; the 90 starts a new bundle.
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 3);
        assert_eq!(groups[1].len(), 1);
    }

    #[test]
    fn an_oversized_segment_still_forms_its_own_bundle() {
        let dir = temp_testdir::TempDir::default();
        let path = dir.join("0.wseg");
        std::fs::write(&path, vec![0u8; 500]).unwrap();

        let groups = group_by_size(&[path], 100).unwrap();
        assert_eq!(groups.len(), 1);
    }

    #[test]
    fn instance_id_is_stable_across_reads() {
        let dir = temp_testdir::TempDir::default();
        let location = dir.to_str().unwrap();
        let first = read_or_create_instance_id(location).unwrap();
        let second = read_or_create_instance_id(location).unwrap();
        assert_eq!(first, second);
        assert!(first.starts_with("rdb-"));
    }

    #[test]
    fn the_staging_ceiling_suspends_and_resumes_with_hysteresis() {
        // Under the ceiling: nothing to do.
        assert_eq!(staging_action(500, 1000, false), None);
        // At it: suspend, so checkpoints stop filling the disk.
        assert_eq!(staging_action(1000, 1000, false), Some(true));
        // Still high while suspended: stay suspended rather than flapping back
        // the moment one bundle uploads.
        assert_eq!(staging_action(900, 1000, true), None);
        assert_eq!(staging_action(501, 1000, true), None);
        // Drained to half: resume.
        assert_eq!(staging_action(500, 1000, true), Some(false));
        assert_eq!(staging_action(0, 1000, true), Some(false));
    }

    #[test]
    fn no_ceiling_never_suspends_but_still_releases() {
        assert_eq!(staging_action(u64::MAX, 0, false), None);
        // Turning the ceiling off must not strand a service that was already
        // suspended under an earlier setting.
        assert_eq!(staging_action(u64::MAX, 0, true), Some(false));
    }

    #[test]
    fn part_size_grows_to_stay_inside_the_part_limit() {
        // S3 caps a multipart upload at 10 000 parts. At the default 32 MiB
        // that is ~320 GB, and a database past it must get larger parts rather
        // than a failed upload half a day in.
        let dir = temp_testdir::TempDir::default();
        let path = dir.join("rows.db");
        std::fs::write(&path, b"x").unwrap();
        let files = vec![("rows.db".to_string(), path)];

        let config = test_config();
        let part = 32 * 1024 * 1024usize;

        // Well inside the limit: the configured size is used as-is.
        assert_eq!(part_size_for(part, 100 * 1024 * 1024 * 1024), part);

        // Past it: the size grows so the count stays under the ceiling.
        let huge = 900u64 * 1024 * 1024 * 1024;
        let chosen = part_size_for(part, huge);
        assert!(chosen > part, "part size should have grown, got {chosen}");
        assert!(
            huge.div_ceil(chosen as u64) <= MAX_PARTS,
            "{huge} bytes at {chosen} per part is {} parts",
            huge.div_ceil(chosen as u64)
        );

        // Never below S3's 5 MiB floor, whatever was configured.
        assert!(part_size_for(1, 1024) >= MIN_PART_SIZE);

        // And the real entry point agrees with the arithmetic.
        let service_choice = part_size_from_files(config.part_size, &files);
        assert_eq!(service_choice, config.part_size.max(MIN_PART_SIZE));
    }

    #[test]
    fn an_empty_database_still_picks_a_legal_part_size() {
        assert!(part_size_for(32 * 1024 * 1024, 0) >= MIN_PART_SIZE);
    }

    #[test]
    fn prefix_is_normalised() {
        let mut config = test_config();
        config.prefix = "/db/main/".to_string();
        assert_eq!(config.normalized_prefix(), "db/main/");
        assert_eq!(config.key("MANIFEST.json"), "db/main/MANIFEST.json");

        config.prefix = String::new();
        assert_eq!(config.key("MANIFEST.json"), "MANIFEST.json");
    }

    fn test_config() -> BackupConfig {
        BackupConfig {
            bucket: "b".to_string(),
            prefix: String::new(),
            region: "us-east-1".to_string(),
            endpoint: Endpoint::parse("https://s3.us-east-1.amazonaws.com").unwrap(),
            credentials: Credentials {
                access_key: "AK".to_string(),
                secret_key: "SK".to_string(),
                session_token: None,
            },
            path_style: false,
            snapshot_level: 12,
            segment_level: 6,
            compression_threads: 0,
            long_range: true,
            part_size: 32 * 1024 * 1024,
            upload_concurrency: 3,
            max_attempts: 4,
            snapshot_interval_secs: 0,
            wal_flush_secs: 0,
            wal_bundle_bytes: 0,
            retain_snapshots: 7,
            rollover_interval_secs: 0,
            staging_max_bytes: 4 * 1024 * 1024 * 1024,
            incremental_min_bytes: 0,
        }
    }

    #[test]
    fn a_log_in_the_location_itself_is_the_root_stream() {
        assert_eq!(
            stream_key("/var/lib/rdb", "/var/lib/rdb"),
            Some(String::new())
        );
        // The location may or may not carry a trailing separator; the registry
        // key does not, and neither should the answer.
        assert_eq!(
            stream_key("/var/lib/rdb", "/var/lib/rdb/"),
            Some(String::new())
        );
    }

    #[test]
    fn a_created_database_is_a_stream_named_after_its_directory() {
        assert_eq!(
            stream_key("/var/lib/rdb", "/var/lib/rdb/shop"),
            Some("shop".to_string())
        );
        assert_eq!(
            stream_key("/var/lib/rdb", "/var/lib/rdb/shop/"),
            Some("shop".to_string())
        );
    }

    #[test]
    fn a_sibling_directory_is_not_a_stream() {
        // The trap a plain `starts_with` falls into: the replica's directory
        // shares every character of the primary's and is a different
        // installation. Archiving its log here would upload records with
        // nothing in this prefix to replay them into.
        assert_eq!(stream_key("/var/lib/rdb", "/var/lib/rdb2"), None);
        assert_eq!(stream_key("/var/lib/rdb", "/var/lib/other"), None);
        assert_eq!(stream_key("/var/lib/rdb", "/var/lib"), None);
    }

    #[test]
    fn a_storage_location_on_another_disk_is_not_a_relative_stream() {
        // Placement can put a table's files outside the backed-up directory,
        // and those get a log of their own. It is a stream, but under its own
        // key from `pool_locations`, never as a path under the location: that
        // would name a directory inside the restore that the files are not in.
        assert_eq!(stream_key("/var/lib/rdb", "/mnt/fast/rdb"), None);
        let key = location_key("/mnt/fast/rdb");
        assert!(key.starts_with(LOCATION_KEY_PREFIX), "{key}");
        assert_eq!(
            key,
            location_key("/mnt/fast/rdb/"),
            "a trailing separator is the same place"
        );
        assert_ne!(key, location_key("/mnt/fast/rdb2"));
    }

    /// An S3 stand-in on loopback: answers every request with an empty `200`
    /// and records its request line, which is all the archive path reads.
    async fn fake_s3() -> (Endpoint, Arc<std::sync::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            // Bounded: an archive makes a handful of requests, one per
            // connection because every reply closes it.
            for _ in 0..64 {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let log = log.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 8192];
                    let head_end = loop {
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i + 4;
                        }
                        match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let body_len = head
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.trim()
                                .eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    while buf.len() < head_end + body_len {
                        match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    log.lock()
                        .unwrap()
                        .push(head.lines().next().unwrap_or("").to_string());
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                        )
                        .await;
                });
            }
        });
        (
            Endpoint {
                host: "127.0.0.1".to_string(),
                port,
                tls: false,
            },
            seen,
        )
    }

    /// One database's poisoned log must not stop the others being archived.
    ///
    /// A poisoned log refuses every checkpoint until restart. The archive used
    /// to stop at the first log that failed to cut — logs are walked in name
    /// order — so every database sorted after it went uncut, and the segments
    /// already staged by every database went un-uploaded: point-in-time
    /// recovery ended for databases that were working normally.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_poisoned_log_does_not_stop_the_other_databases_being_archived() {
        let root = tempfile::tempdir().unwrap();
        let location = root.path().to_str().unwrap().to_string();
        let mut logs = Vec::new();
        for name in ["a", "b"] {
            let dir = root.path().join(name);
            std::fs::create_dir(&dir).unwrap();
            let wal = Arc::new(
                crate::core::wal::Wal::open(dir.join(crate::core::wal::DEFAULT_WAL_NAME)).unwrap(),
            );
            crate::core::wal::install_for(dir.to_str().unwrap(), wal.clone());
            logs.push((dir, wal));
        }
        let (poisoned, healthy) = (logs[0].1.clone(), logs[1].1.clone());

        let (endpoint, requests) = fake_s3().await;
        let mut config = test_config();
        config.endpoint = endpoint;
        config.path_style = true;
        config.max_attempts = 1;
        let client = Arc::new(
            S3Client::new(S3Config {
                endpoint: config.endpoint.clone(),
                region: config.region.clone(),
                bucket: config.bucket.clone(),
                credentials: config.credentials.clone(),
                path_style: config.path_style,
                max_attempts: config.max_attempts,
            })
            .unwrap(),
        );
        let service = Arc::new(BackupService {
            config,
            client,
            location: location.clone(),
            instance_id: "rdb-test".to_string(),
            manifest: tokio::sync::Mutex::new(Manifest::new("rdb-test".to_string())),
            snapshot_lock: tokio::sync::Mutex::new(()),
            archive_lock: tokio::sync::Mutex::new(()),
            shutting_down: CacheGuard::new(AtomicBool::new(false)),
            archiving_suspended: CacheGuard::new(AtomicBool::new(false)),
            attached: std::sync::Mutex::new(Vec::new()),
        });
        service.attach_to_logs().unwrap();

        // "b" commits normally; "a" — sorted first — has one append that
        // failed and could not be cut back, which poisons it.
        let txn = healthy.begin();
        healthy.log(txn, 7, 0, b"healthy").unwrap();
        healthy.commit_txn(txn).unwrap();
        crate::core::wal::test_hooks::SHORT_WAL_WRITE.with(|h| h.set(Some(3)));
        crate::core::wal::test_hooks::FAIL_WAL_ROLLBACK.with(|f| f.set(true));
        let failed = poisoned.log(poisoned.begin(), 7, 0, b"lost");
        crate::core::wal::test_hooks::SHORT_WAL_WRITE.with(|h| h.set(None));
        crate::core::wal::test_hooks::FAIL_WAL_ROLLBACK.with(|f| f.set(false));
        assert!(failed.is_err());
        assert!(poisoned.is_poisoned());

        let outcome = service.archive_wal_now().await;
        for (dir, _) in &logs {
            crate::core::wal::remove_for(dir.to_str().unwrap());
        }
        let err = outcome.expect_err("a log that cannot be cut must still be reported");
        assert!(err.to_string().contains("database a"), "{err}");

        assert_eq!(
            healthy.bytes_since_checkpoint(),
            0,
            "the healthy database's log was not cut because another database's was poisoned"
        );
        let staged_b = service.staging_dir_for("b");
        let left = std::fs::read_dir(&staged_b)
            .map(|d| d.flatten().count())
            .unwrap_or(0);
        assert_eq!(left, 0, "the healthy database's staged segments were not uploaded");
        let requests = requests.lock().unwrap().clone();
        assert!(
            requests
                .iter()
                .any(|line| line.starts_with("PUT") && line.contains("/wal/b/")),
            "no segment of the healthy database reached the bucket: {:?}",
            requests
        );
    }

    /// A service over `location` that never reaches its bucket.
    fn offline_service(location: &str, config: BackupConfig) -> Arc<BackupService> {
        let client = Arc::new(
            S3Client::new(S3Config {
                endpoint: config.endpoint.clone(),
                region: config.region.clone(),
                bucket: config.bucket.clone(),
                credentials: config.credentials.clone(),
                path_style: config.path_style,
                max_attempts: config.max_attempts,
            })
            .unwrap(),
        );
        Arc::new(BackupService {
            config,
            client,
            location: location.to_string(),
            instance_id: "rdb-test".to_string(),
            manifest: tokio::sync::Mutex::new(Manifest::new("rdb-test".to_string())),
            snapshot_lock: tokio::sync::Mutex::new(()),
            archive_lock: tokio::sync::Mutex::new(()),
            shutting_down: CacheGuard::new(AtomicBool::new(false)),
            archiving_suspended: CacheGuard::new(AtomicBool::new(false)),
            attached: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Unregisters logs a test installed, however it ends.
    struct Installed(Vec<String>);
    impl Drop for Installed {
        fn drop(&mut self) {
            for dir in &self.0 {
                crate::core::wal::remove_for(dir);
            }
        }
    }

    fn install_log(dir: &Path, installed: &mut Installed) -> Arc<crate::core::wal::Wal> {
        let wal = Arc::new(
            crate::core::wal::Wal::open(dir.join(crate::core::wal::DEFAULT_WAL_NAME)).unwrap(),
        );
        crate::core::wal::install_for(dir.to_str().unwrap(), wal.clone());
        installed.0.push(dir.to_str().unwrap().to_string());
        wal
    }

    fn commit(wal: &crate::core::wal::Wal, bytes: &[u8]) {
        let txn = wal.begin();
        wal.log(txn, 7, 0, bytes).unwrap();
        wal.commit_txn(txn).unwrap();
    }

    /// A staged append moving into place inside a storage location while the
    /// copy runs is as torn as one in the database directory, and must refuse
    /// the snapshot the same way.
    #[test]
    fn a_fold_in_a_storage_location_is_seen() {
        let root = tempfile::tempdir().unwrap();
        let disk2 = tempfile::tempdir().unwrap();
        let path = disk2.path().to_str().unwrap().to_string();
        let key = location_key(&path);
        let locations = vec![(key.clone(), path)];
        std::fs::write(disk2.path().join("t.db.tmp"), vec![1u8; 64]).unwrap();
        std::fs::write(disk2.path().join("t.db"), b"main").unwrap();

        let before = staged_lengths_in(root.path(), &locations).unwrap();
        assert_eq!(before.get(&format!("{key}/t.db.tmp")), Some(&64), "{before:?}");
        assert!(!before.contains_key(&format!("{key}/t.db")), "{before:?}");

        // Untouched: nothing to refuse.
        let same = staged_lengths_in(root.path(), &locations).unwrap();
        folded_while_copying(&before, &same).unwrap();

        std::fs::write(disk2.path().join("t.db.tmp"), vec![1u8; 8]).unwrap();
        let shrunk = staged_lengths_in(root.path(), &locations).unwrap();
        let err = folded_while_copying(&before, &shrunk).unwrap_err();
        assert!(err.to_string().contains("t.db.tmp"), "{err}");

        std::fs::remove_file(disk2.path().join("t.db.tmp")).unwrap();
        let gone = staged_lengths_in(root.path(), &locations).unwrap();
        folded_while_copying(&before, &gone).unwrap_err();
    }

    /// One database's unparsable pool file costs that database's locations and
    /// nothing else: the root's log is still cut, a snapshot is refused rather
    /// than taken without those tables, and the staging ceiling still detaches
    /// every log — the one that can no longer be named included.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unparsable_pool_file_costs_only_its_own_locations() {
        let root = tempfile::tempdir().unwrap();
        let disk2 = tempfile::tempdir().unwrap();
        let location = root.path().to_str().unwrap().to_string();
        let shop = root.path().join("shop");
        std::fs::create_dir(&shop).unwrap();
        std::fs::write(
            shop.join(crate::core::storage::POOL_FILE),
            format!(
                "version\t1\nlocation\tprimary\t1\tenabled\t{}\n",
                disk2.path().display()
            ),
        )
        .unwrap();

        let mut installed = Installed(Vec::new());
        let root_log = install_log(root.path(), &mut installed);
        let location_log = install_log(disk2.path(), &mut installed);

        let mut config = test_config();
        config.staging_max_bytes = 1;
        let service = offline_service(&location, config);
        service.attach_to_logs().unwrap();
        assert!(location_log.is_archiving(), "the test proves nothing");

        std::fs::write(shop.join(crate::core::storage::POOL_FILE), "location\tprimary\n").unwrap();
        commit(&root_log, b"root");

        let err = service
            .checkpoint_all_logs()
            .await
            .expect_err("a database whose locations are unknown must be reported");
        assert!(err.to_string().contains("shop"), "{err}");
        assert_eq!(
            root_log.bytes_since_checkpoint(),
            0,
            "the root's log was not cut because another database's pool file is unreadable"
        );
        assert!(service.pool_locations_for_snapshot().is_err());

        // The root's staged segment is over the 1-byte ceiling.
        service.enforce_staging_ceiling();
        assert!(service.archiving_is_suspended());
        assert!(!root_log.is_archiving());
        assert!(
            !location_log.is_archiving(),
            "a log this service attached kept staging past the ceiling"
        );
    }

    /// A cut is each log's commit sequence; with a location it is read in
    /// the order that keeps it consistent, and the reading is per log.
    #[test]
    fn a_cut_names_every_log_at_its_commit_sequence() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let open = |dir: &Path| {
            Arc::new(
                crate::core::wal::Wal::open(dir.join(crate::core::wal::DEFAULT_WAL_NAME)).unwrap(),
            )
        };
        let (root, far) = (open(a.path()), open(b.path()));
        commit(&root, b"one");
        commit(&root, b"two");
        commit(&far, b"three");
        let streams = vec![
            (String::new(), root.clone()),
            ("~loc-00000000000000ff".to_string(), far.clone()),
        ];
        let cut = read_cut(&streams).unwrap();
        assert_eq!(
            cut,
            vec![
                (String::new(), root.commit_lsn()),
                ("~loc-00000000000000ff".to_string(), far.commit_lsn()),
            ]
        );
        assert_ne!(root.commit_lsn(), far.commit_lsn(), "the readings must not be mixed up");
    }
}
