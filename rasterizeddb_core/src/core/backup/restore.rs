//! Rebuilding a database directory from a snapshot and the archived log.
//!
//! # The two halves
//!
//! Unpacking the snapshot gives a directory that is *nearly* the database:
//! every file is present, but each was copied at whatever length it happened to
//! have when its turn came, so the set as a whole corresponds to no single
//! instant. Replaying the archived log over it is what fixes that. The records
//! are physical redo, so applying one is idempotent and applying them in order
//! drives every file to the state it had at the commit the replay stops on.
//!
//! # Why replay runs over one concatenated file
//!
//! Segments could be replayed one at a time, but a transaction whose records
//! were written just before a checkpoint and whose commit marker landed just
//! after would then be split across two of them — and per-segment replay, which
//! discards records belonging to transactions it saw no commit for, would throw
//! it away. Concatenating first makes that transaction whole again, which costs
//! one sequential write of the log's bytes and removes the failure mode.
//!
//! # Where it stops
//!
//! Recovery to a point in time cuts on the *commit sequence*, so a transaction
//! is either wholly applied or wholly absent. Cutting on a wall-clock instant
//! instead resolves to the end of the last archive flush at or before it: log
//! records carry no timestamp, because stamping each one would put a clock read
//! on the write path, so time is only known at the granularity a bundle was
//! uploaded at.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use log::{info, warn};

use super::manifest::{Manifest, RecoveryTarget, SnapshotEntry};
use super::s3::S3Client;
use super::{BackupConfig, archive, manifest, timeutil};

/// Written into a restored directory, so an operator can tell what it is.
const MARKER_FILE: &str = "RESTORED.json";

#[derive(Debug, Clone)]
pub struct RestoreReport {
    pub snapshot_id: String,
    pub target_dir: PathBuf,
    /// Where replay actually stopped, or `None` when it replayed everything.
    pub recovered_to_lsn: Option<u64>,
    pub snapshot_taken_at: u64,
    pub files_restored: u32,
    pub bytes_restored: u64,
    pub segments_replayed: u32,
    pub records_applied: u64,
    pub records_discarded: u64,
    /// Log records naming a file the restored directory does not have. Should
    /// be zero; anything else means the snapshot and the log disagree.
    pub records_unresolved: u64,
}

/// Restores into `target_dir`.
///
/// The directory must not already hold a database. Overwriting one in place
/// while a server has it open would leave the server reading files that were
/// swapped underneath it, so the in-place path is the CLI's, taken before
/// anything is opened.
pub async fn run(
    client: &S3Client,
    config: &BackupConfig,
    target_dir: &Path,
    target: RecoveryTarget,
) -> io::Result<RestoreReport> {
    ensure_usable_target(target_dir)?;

    // Everything is built in a sibling directory and moved into place at the
    // end. Two things follow from that, and both matter more than the extra
    // rename costs:
    //
    // A restore that fails half way leaves nothing behind, so it can simply be
    // run again — without this, the partial directory is non-empty and the
    // retry is refused by the check above, which is a trap rather than a
    // safeguard. And the target directory never exists in a half-built state,
    // so nothing can open it and find a database that is missing the log
    // records it needed.
    //
    // A sibling rather than a temp directory, because the rename has to stay
    // within one filesystem to be atomic and cheap.
    let staging = staging_beside(target_dir);
    if staging.exists() {
        std::fs::remove_dir_all(&staging)?;
    }

    // Checked here, and named, because the failure is otherwise a bare
    // "Permission denied (os error 13)" that points at nothing. The directory
    // being restored *into* may be perfectly writable while its parent is not —
    // which is the normal case for somewhere like `/var/lib` — and the staging
    // directory is a sibling, so the parent is what has to be writable.
    if let Some(parent) = target_dir.parent().filter(|p| !p.as_os_str().is_empty()) {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return Err(io::Error::new(
                e.kind(),
                format!(
                    "cannot prepare {}: the restore is staged in a sibling directory, so {} has \
                     to be writable by the server ({})",
                    target_dir.display(),
                    parent.display(),
                    e
                ),
            ));
        }
        let probe = staging.with_extension("probe");
        match std::fs::create_dir(&probe) {
            Ok(()) => {
                let _ = std::fs::remove_dir(&probe);
            }
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!(
                        "cannot write beside {}: the restore is staged in a sibling directory, \
                         so {} has to be writable by the server ({})",
                        target_dir.display(),
                        parent.display(),
                        e
                    ),
                ));
            }
        }
    }
    let mut cleanup = CleanupOnDrop::arm(&staging);

    let report = build(client, config, &staging, target, target_dir).await?;

    std::fs::create_dir_all(
        target_dir
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?;
    // The target may exist as an empty directory, which `rename` will not
    // replace on any platform.
    let _ = std::fs::remove_dir(target_dir);
    std::fs::rename(&staging, target_dir)?;
    cleanup.disarm();
    crate::core::wal::sync_parent_dir(target_dir)?;

    Ok(report)
}

/// Where the restore is assembled before being moved into place.
fn staging_beside(target_dir: &Path) -> PathBuf {
    let name = target_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("database");
    target_dir.with_file_name(format!(".{name}.restore-partial"))
}

/// Removes a half-built restore unless it was disarmed.
struct CleanupOnDrop {
    path: PathBuf,
    armed: bool,
}

impl CleanupOnDrop {
    fn arm(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CleanupOnDrop {
    fn drop(&mut self) {
        if self.armed && self.path.exists() {
            if let Err(e) = std::fs::remove_dir_all(&self.path) {
                warn!(
                    "Could not remove the half-built restore at {}: {}",
                    self.path.display(),
                    e
                );
            }
        }
    }
}

/// Assembles the restore in `target_dir`, which is the staging directory.
///
/// `final_dir` is only used for messages, so they name the place the operator
/// asked for rather than the scratch directory they never see.
async fn build(
    client: &S3Client,
    config: &BackupConfig,
    target_dir: &Path,
    target: RecoveryTarget,
    final_dir: &Path,
) -> io::Result<RestoreReport> {
    // One GET decides the whole plan: which snapshot, which segments, where to
    // stop. This is what the manifest exists for.
    let manifest_key = config.key(manifest::MANIFEST_KEY);
    let manifest = Manifest::decode(&client.get_object(&manifest_key).await.map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "no backup catalogue at s3://{}/{}; nothing has been backed up to this prefix",
                    client.bucket(),
                    manifest_key
                ),
            )
        } else {
            e
        }
    })?)?;

    let snapshot = manifest.snapshot_for(&target).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            describe_no_snapshot(&manifest, &target),
        )
    })?;
    let ceiling = target.resolve_lsn(&manifest, snapshot);

    info!(
        "Restoring snapshot {} (taken {}, LSN {}..{}) into {}",
        snapshot.id,
        timeutil::format_rfc3339(snapshot.finished_at),
        snapshot.start_lsn,
        snapshot.end_lsn,
        final_dir.display()
    );

    std::fs::create_dir_all(target_dir)?;
    let mut bytes_restored = download_snapshot(client, snapshot, target_dir).await?;
    // Files an incremental snapshot keeps outside its archive — possibly in
    // objects an earlier snapshot wrote. Each is checked against the length
    // and hash the catalogue recorded: a wrong object here would restore a
    // database that opens and answers with the wrong bytes.
    for object in &snapshot.objects {
        bytes_restored += download_object(client, object, target_dir).await?;
    }

    // A file that was created while the snapshot was being copied is named in
    // the catalogue but has no archive entry. Replay needs it to exist before
    // it can write into it.
    for name in &snapshot.files {
        let path = target_dir.join(name);
        if !path.exists() {
            // A name may carry a directory: every database created with
            // `CREATE DATABASE` is a subdirectory of the location, and the
            // snapshot names its files relative to that.
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::File::create(&path)?;
        }
    }

    // The snapshot carries the source's storage pool file, which names
    // absolute paths. Left as it is, every table it places resolves back to the
    // live database's directory and this copy is never read — see
    // `storage::repoint_pool_config`. Rewritten before anything opens the
    // restored directory as a database.
    // Repointed at `final_dir`, not at the staging directory this is being
    // built in: the paths have to be true after the rename that publishes it.
    let source_root = Some(manifest.source_location.as_str()).filter(|s| !s.is_empty());
    match crate::core::storage::repoint_pool_config(target_dir, final_dir, source_root) {
        Ok(dropped) if !dropped.is_empty() => warn!(
            "The source placed {} on storage locations outside its database directory, which no \
             snapshot copies. Those tables are not in this backup and will read as missing: {}",
            if dropped.len() == 1 {
                "a table"
            } else {
                "tables"
            },
            dropped.join(", ")
        ),
        Ok(_) => {}
        // Not fatal: the restored copy is complete either way, and refusing
        // here would turn a placement problem into no restore at all.
        Err(e) => warn!(
            "Could not repoint the storage pool of the restored copy ({e}); if it names another \
             directory, queries against this copy may read that one instead"
        ),
    }

    // Checked before anything is downloaded, so a restore that cannot succeed
    // says so in one request rather than after moving the whole snapshot.
    verify_archive_covers(&manifest, snapshot)?;

    // One replay per database. They are never merged: a file id is a hash of a
    // table name, so two databases both holding `users` name it identically and
    // a merged stream would write one's records into the other's file.
    let mut segments_replayed = 0;
    let mut stats = crate::core::wal::RecoveryStats::default();

    for database in databases_to_replay(&manifest, snapshot) {
        let into = if database.is_empty() {
            target_dir.to_path_buf()
        } else {
            target_dir.join(&database)
        };
        // A database whose every file was created while the copy ran is named
        // in the catalogue but has nothing on disk yet.
        std::fs::create_dir_all(&into)?;

        let (count, replay_path) =
            download_segments(client, &manifest, snapshot, &database, &into).await?;
        segments_replayed += count;

        let window = snapshot.window_for(&database);
        if count > 0 {
            let ceiling = target.resolve_lsn_for(&manifest, snapshot, &database);
            let applied = replay(&replay_path, &into, ceiling)?;
            stats.applied += applied.applied;
            stats.rolled_back += applied.rolled_back;
            stats.unresolved += applied.unresolved;
        } else if window.as_ref().is_some_and(|w| w.end_lsn > w.start_lsn) {
            let w = window.expect("checked above");
            warn!(
                "Snapshot {} was taken while writes were in flight against {} (LSN {}..{}) but \
                 no archived log covers that window. The restored copy of that database may be \
                 internally inconsistent.",
                snapshot.id,
                if database.is_empty() {
                    "the default database"
                } else {
                    database.as_str()
                },
                w.start_lsn,
                w.end_lsn
            );
        }

        // The replay file has done its job, and leaving a log-shaped file in
        // the directory would have the next start-up replay it a second time.
        let _ = std::fs::remove_file(&replay_path);
        let _ = std::fs::remove_file(into.join(crate::core::wal::DEFAULT_WAL_NAME));
    }

    let _ = std::fs::remove_dir_all(target_dir.join(super::STAGING_DIR));
    // The restored copy is a distinct database; letting it inherit the source's
    // identity would let it write into the source's backup prefix.
    let _ = std::fs::remove_file(target_dir.join(super::INSTANCE_FILE));

    let report = RestoreReport {
        snapshot_id: snapshot.id.clone(),
        target_dir: final_dir.to_path_buf(),
        recovered_to_lsn: ceiling,
        snapshot_taken_at: snapshot.finished_at,
        files_restored: snapshot.files.len().max(snapshot.file_count as usize) as u32,
        bytes_restored,
        segments_replayed,
        records_applied: stats.applied,
        records_discarded: stats.rolled_back,
        records_unresolved: stats.unresolved,
    };

    write_marker(target_dir, &report)?;

    if report.records_unresolved > 0 {
        warn!(
            "{} log records named a file the snapshot did not contain and were skipped. \
             The restore is incomplete.",
            report.records_unresolved
        );
    }

    info!(
        "Restore complete: {} files, {} bytes, {} segments replayed, {} records applied, \
         {} discarded as uncommitted, recovered to LSN {}",
        report.files_restored,
        report.bytes_restored,
        report.segments_replayed,
        report.records_applied,
        report.records_discarded,
        report
            .recovered_to_lsn
            .map(|l| l.to_string())
            .unwrap_or_else(|| "end of archive".to_string()),
    );

    Ok(report)
}

/// Refuses a restore whose snapshot has no log to make it consistent.
///
/// A snapshot copied while writes were in flight is a mix of states, and the
/// archived log through `end_lsn` is what reconciles it. If those segments are
/// gone — retention that pruned too far, an object deleted by hand, a
/// truncated catalogue — replay stops short and the result is a database that
/// opens, answers queries, and is quietly wrong.
///
/// That is worth failing for. The operator can still recover by choosing an
/// older snapshot whose window *is* covered.
fn verify_archive_covers(manifest: &Manifest, snapshot: &SnapshotEntry) -> io::Result<()> {
    for database in databases_to_replay(manifest, snapshot) {
        let Some(window) = snapshot.window_for(&database) else {
            continue;
        };
        // Nothing committed against this database while the copy ran, so its
        // files are already consistent and need no replay.
        if window.end_lsn <= window.start_lsn {
            continue;
        }

        // Bundles outside this snapshot's window do not cover it, however high
        // their commit sequence reads: a bundle from before a restart carries
        // sequences from an earlier run of the same counter, and counting it
        // here would pass a snapshot whose own window never reached S3.
        let archived_through = manifest
            .segments_for(&database)
            .into_iter()
            .filter(|s| manifest::segment_needed_by(snapshot, s))
            .map(|s| s.last_lsn)
            .max()
            .unwrap_or(0);

        if archived_through >= window.end_lsn {
            continue;
        }

        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "snapshot {} needs the log of {} through LSN {} to be consistent, but the \
                 catalogue only reaches LSN {}. Restoring it would produce a database that \
                 looks healthy and is not. Choose an older snapshot whose window is still \
                 covered.",
                snapshot.id,
                if database.is_empty() {
                    "the default database"
                } else {
                    database.as_str()
                },
                window.end_lsn,
                archived_through
            ),
        ));
    }
    Ok(())
}

/// Every database this snapshot may need log replayed into.
///
/// The root is always in the list — a catalogue that predates per-database
/// windows describes only that one, and its bundles carry `database: ""`.
fn databases_to_replay(manifest: &Manifest, snapshot: &SnapshotEntry) -> Vec<String> {
    let mut names: Vec<String> = vec![String::new()];
    for window in &snapshot.windows {
        if !names.contains(&window.database) {
            names.push(window.database.clone());
        }
    }
    for name in manifest.databases_with_segments() {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// Refuses a target that already holds something, so a restore cannot merge
/// itself into an unrelated database.
fn ensure_usable_target(target_dir: &Path) -> io::Result<()> {
    match std::fs::read_dir(target_dir) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "{} is not empty. Restore into a new directory, or move the existing \
                         one aside first — merging a restore into live files would leave a \
                         database that is neither the old one nor the new one.",
                        target_dir.display()
                    ),
                ));
            }
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn describe_no_snapshot(manifest: &Manifest, target: &RecoveryTarget) -> String {
    if manifest.snapshots.is_empty() {
        return "the backup catalogue holds no snapshots".to_string();
    }

    let oldest = manifest
        .snapshots_newest_first()
        .last()
        .map(|s| {
            format!(
                "the oldest is {} from {} at LSN {}",
                s.id,
                timeutil::format_rfc3339(s.finished_at),
                s.end_lsn
            )
        })
        .unwrap_or_default();

    match target {
        RecoveryTarget::Snapshot(id) => format!("no snapshot named '{id}' in the catalogue"),
        RecoveryTarget::Lsn(lsn) => format!(
            "no snapshot finished at or before LSN {lsn}, so there is nothing to replay onto; {oldest}"
        ),
        RecoveryTarget::Time(unix) => format!(
            "no snapshot was taken at or before {}; {oldest}",
            timeutil::format_rfc3339(*unix)
        ),
        RecoveryTarget::Latest => "the backup catalogue holds no usable snapshot".to_string(),
    }
}

/// Streams the snapshot object straight into the target directory.
///
/// Download runs on the runtime and decompression on a blocking thread, joined
/// by a bounded channel — the same split the upload path uses, for the same
/// reason: neither side has to buffer an object that may be larger than memory.
async fn download_snapshot(
    client: &S3Client,
    snapshot: &SnapshotEntry,
    target_dir: &Path,
) -> io::Result<u64> {
    let (tx, rx) = kanal::bounded::<Vec<u8>>(4);
    let target = target_dir.to_path_buf();

    let unpacker = tokio::task::spawn_blocking(move || -> io::Result<u64> {
        let mut decoder = zstd::stream::read::Decoder::new(ChannelReader::new(rx))?;
        let entries = archive::unpack(&mut decoder, &target)?;
        Ok(entries.iter().map(|e| e.len).sum())
    });

    let download = client
        .get_object_streaming(&snapshot.key, |chunk| {
            tx.send(chunk.to_vec())
                .map_err(|_| io::Error::other("the snapshot unpacker stopped accepting data"))
        })
        .await;
    // Dropped, never closed: `kanal::Sender::close` clears the queue, which
    // would throw away every chunk the unpacker had not yet consumed.
    drop(tx);

    let unpacked = unpacker
        .await
        .map_err(|e| io::Error::other(format!("snapshot unpacker panicked: {e}")))?;

    // The unpacker's error is the more specific of the two — a truncated
    // download shows up there as a checksum or EOF failure — but a transport
    // error explains why, so it wins when the unpacker only saw the symptom.
    download?;
    unpacked
}

/// Fetches one file object into `target_dir`, verifying its length and hash.
async fn download_object(
    client: &S3Client,
    object: &manifest::FileObject,
    target_dir: &Path,
) -> io::Result<u64> {
    if !archive::is_safe_name(&object.name) {
        return Err(io::Error::other(format!(
            "backup catalogue names an unsafe path: {}",
            object.name
        )));
    }
    let path = target_dir.join(&object.name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let (tx, rx) = kanal::bounded::<Vec<u8>>(4);
    let expected = object.clone();
    let writer = tokio::task::spawn_blocking(move || -> io::Result<u64> {
        use sha2::Digest;
        let mut decoder = zstd::stream::read::Decoder::new(ChannelReader::new(rx))?;
        let mut out = io::BufWriter::with_capacity(256 * 1024, std::fs::File::create(&path)?);
        let mut digest = sha2::Sha256::new();
        let mut chunk = vec![0u8; 1024 * 1024];
        let mut total = 0u64;
        loop {
            let n = decoder.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            digest.update(&chunk[..n]);
            out.write_all(&chunk[..n])?;
            total += n as u64;
        }
        out.flush()?;
        let sha256 = super::to_hex(&digest.finalize());
        if total != expected.len || sha256 != expected.sha256 {
            return Err(io::Error::other(format!(
                "object {} for {} does not match the catalogue: {} bytes with SHA-256 {},                  expected {} bytes with {}",
                expected.key, expected.name, total, sha256, expected.len, expected.sha256
            )));
        }
        Ok(total)
    });

    let download = client
        .get_object_streaming(&object.key, |chunk| {
            tx.send(chunk.to_vec())
                .map_err(|_| io::Error::other("the object writer stopped accepting data"))
        })
        .await;
    // Dropped, never closed: see `download_snapshot`.
    drop(tx);
    let written = writer
        .await
        .map_err(|e| io::Error::other(format!("object writer panicked: {e}")))?;
    download?;
    written
}

/// Fetches every segment bundle the snapshot may need and concatenates them.
///
/// Returns the number of bundles and the path of the file to replay.
async fn download_segments(
    client: &S3Client,
    manifest: &Manifest,
    snapshot: &SnapshotEntry,
    database: &str,
    target_dir: &Path,
) -> io::Result<(u32, PathBuf)> {
    let needed: Vec<&manifest::SegmentEntry> = manifest
        .segments_for(database)
        .into_iter()
        // The same rule retention prunes by, so a restore never downloads a
        // bundle retention considers deletable — and never replays one from
        // before the snapshot, whose records describe file offsets that mean
        // something else now.
        .filter(|s| manifest::segment_needed_by(snapshot, s))
        .collect();

    let replay_path = target_dir.join("restore_replay.wal");
    if needed.is_empty() {
        return Ok((0, replay_path));
    }

    let mut out = io::BufWriter::with_capacity(1024 * 1024, std::fs::File::create(&replay_path)?);

    // Sequential rather than parallel: the bundles have to land in log order,
    // and a restore is bounded by decompression and disk rather than by
    // round-trip latency.
    for segment in &needed {
        let (tx, rx) = kanal::bounded::<Vec<u8>>(4);
        let decompressed = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
            let mut decoder = zstd::stream::read::Decoder::new(ChannelReader::new(rx))?;
            let mut out = Vec::new();
            decoder.read_to_end(&mut out)?;
            Ok(out)
        });

        let download = client
            .get_object_streaming(&segment.key, |chunk| {
                tx.send(chunk.to_vec())
                    .map_err(|_| io::Error::other("the segment decompressor stopped"))
            })
            .await;
        drop(tx);

        let bytes = decompressed
            .await
            .map_err(|e| io::Error::other(format!("segment decompressor panicked: {e}")))?;
        download?;
        out.write_all(&bytes?)?;
    }

    out.flush()?;
    out.into_inner()
        .map_err(|e| io::Error::other(e.to_string()))?
        .sync_data()?;

    Ok((needed.len() as u32, replay_path))
}

/// Applies the concatenated log to the restored files, stopping at `ceiling`.
fn replay(
    replay_path: &Path,
    target_dir: &Path,
    ceiling: Option<u64>,
) -> io::Result<crate::core::wal::RecoveryStats> {
    // File ids are derived from a name within a database directory, never an
    // absolute path, which is exactly what lets a log recorded under one mount
    // point replay under another.
    let mut resolver: HashMap<u64, PathBuf> = HashMap::new();
    for entry in std::fs::read_dir(target_dir)?.flatten() {
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if name == "restore_replay.wal" || name == MARKER_FILE {
            continue;
        }
        resolver.insert(crate::core::wal::file_id_for_name(&name), entry.path());
    }

    crate::core::wal::Wal::recover_upto(replay_path, |id| resolver.get(&id).cloned(), ceiling)
}

fn write_marker(target_dir: &Path, report: &RestoreReport) -> io::Result<()> {
    let marker = serde_json::json!({
        "restored_at": timeutil::format_rfc3339(timeutil::now_unix()),
        "snapshot_id": report.snapshot_id,
        "snapshot_taken_at": timeutil::format_rfc3339(report.snapshot_taken_at),
        "recovered_to_lsn": report.recovered_to_lsn,
        "files_restored": report.files_restored,
        "bytes_restored": report.bytes_restored,
        "segments_replayed": report.segments_replayed,
        "records_applied": report.records_applied,
        "records_discarded": report.records_discarded,
        "records_unresolved": report.records_unresolved,
    });
    std::fs::write(
        target_dir.join(MARKER_FILE),
        serde_json::to_vec_pretty(&marker).expect("the marker is always serialisable"),
    )
}

/// Turns a channel of byte chunks into a `Read`.
///
/// Lets the synchronous zstd decoder consume an asynchronous download without
/// either side buffering the whole object.
struct ChannelReader {
    rx: kanal::Receiver<Vec<u8>>,
    current: Vec<u8>,
    cursor: usize,
}

impl ChannelReader {
    fn new(rx: kanal::Receiver<Vec<u8>>) -> Self {
        Self {
            rx,
            current: Vec::new(),
            cursor: 0,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.cursor >= self.current.len() {
            match self.rx.recv() {
                Ok(chunk) => {
                    self.current = chunk;
                    self.cursor = 0;
                }
                // The sender closed: end of stream, which `read_exact` upstream
                // turns into the right error if it came too early.
                Err(_) => return Ok(0),
            }
        }
        let available = self.current.len() - self.cursor;
        let n = available.min(buf.len());
        buf[..n].copy_from_slice(&self.current[self.cursor..self.cursor + n]);
        self.cursor += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_reader_reassembles_chunks() {
        let (tx, rx) = kanal::unbounded::<Vec<u8>>();
        tx.send(b"hello ".to_vec()).unwrap();
        tx.send(b"world".to_vec()).unwrap();
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        let mut out = String::new();
        reader.read_to_string(&mut out).unwrap();
        assert_eq!(out, "hello world");
    }

    fn entry(start_lsn: u64, end_lsn: u64) -> SnapshotEntry {
        SnapshotEntry {
            id: "snap-test".to_string(),
            sequence: 0,
            key: "snapshots/snap-test.rdba.zst".to_string(),
            started_at: 1_000,
            finished_at: 1_010,
            start_lsn,
            end_lsn,
            first_segment_sequence: None,
            windows: Vec::new(),
            compressed_bytes: 1,
            uncompressed_bytes: 2,
            file_count: 1,
            compression_level: 3,
            files: Vec::new(),
            objects: Vec::new(),
        }
    }

    fn manifest_reaching(last_lsn: u64) -> Manifest {
        let mut m = Manifest::new("test".to_string());
        m.segments.push(super::manifest::SegmentEntry {
            sequence: 0,
            database: String::new(),
            key: "wal/0.zst".to_string(),
            archived_at: 1_005,
            first_lsn: 1,
            last_lsn,
            compressed_bytes: 1,
            uncompressed_bytes: 2,
            segment_count: 1,
        });
        m
    }

    #[test]
    fn a_snapshot_with_no_writes_during_the_copy_needs_no_log() {
        // start == end: nothing committed while the files were being read, so
        // the copy is already consistent.
        let m = Manifest::new("test".to_string());
        verify_archive_covers(&m, &entry(50, 50)).unwrap();
    }

    #[test]
    fn a_covered_window_is_accepted() {
        let m = manifest_reaching(120);
        verify_archive_covers(&m, &entry(50, 120)).unwrap();
        verify_archive_covers(&m, &entry(50, 100)).unwrap();
    }

    #[test]
    fn an_uncovered_window_is_refused_rather_than_half_restored() {
        // The archive stops at 90 but the copy only becomes consistent at 120.
        // Replaying what exists would produce a database that opens, answers
        // queries, and is quietly wrong — so this fails instead.
        let m = manifest_reaching(90);
        let err = verify_archive_covers(&m, &entry(50, 120)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("LSN 120"), "{err}");
        assert!(err.to_string().contains("older snapshot"), "{err}");
    }

    #[test]
    fn a_window_with_no_segments_at_all_is_refused() {
        let m = Manifest::new("test".to_string());
        assert!(verify_archive_covers(&m, &entry(50, 120)).is_err());
    }

    #[test]
    fn refuses_a_non_empty_target() {
        let dir = temp_testdir::TempDir::default();
        let target = dir.join("db");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(target.join("something.db"), b"x").unwrap();

        let err = ensure_usable_target(&target).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn accepts_a_missing_or_empty_target() {
        let dir = temp_testdir::TempDir::default();
        ensure_usable_target(&dir.join("does-not-exist")).unwrap();

        let empty = dir.join("empty");
        std::fs::create_dir_all(&empty).unwrap();
        ensure_usable_target(&empty).unwrap();
    }
}
