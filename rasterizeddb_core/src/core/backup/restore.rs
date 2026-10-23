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

    let (report, in_place_locations) = build(client, config, &staging, target, target_dir).await?;

    // An in-place restore puts each storage location's files back where they
    // came from — another disk, so a copy rather than the rename the root
    // gets. Before that rename: a root that appears is a database that opens,
    // and its tables on these locations must be there when it does. Undone if
    // any fails, so a retry finds them as empty as this one did.
    let mut published = PublishedLocations::default();
    for (staged, destination) in &in_place_locations {
        published.publish(staged, destination)?;
        // Out of the root before it is renamed into place, or the database
        // directory would carry a second copy of every one of these files.
        std::fs::remove_dir_all(staged)?;
    }

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
    published.keep();
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

/// Files copied into storage locations by an in-place restore, removed again
/// unless the restore completes.
#[derive(Default)]
struct PublishedLocations {
    files: Vec<PathBuf>,
    kept: bool,
}

impl PublishedLocations {
    /// Copies every file of `staged` into `destination`, which must be empty.
    fn publish(&mut self, staged: &Path, destination: &Path) -> io::Result<()> {
        // Checked again here, not only before the download: a restore can run
        // for hours, and merging into files that appeared meanwhile would
        // leave a location that is neither the backup nor what was there.
        if !is_empty_or_missing(destination)? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "storage location {} is no longer empty; the restore will not merge into it",
                    destination.display()
                ),
            ));
        }
        std::fs::create_dir_all(destination)?;
        for entry in std::fs::read_dir(staged)?.flatten() {
            if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                continue;
            }
            let to = destination.join(entry.file_name());
            self.files.push(to.clone());
            std::fs::copy(entry.path(), &to)?;
            std::fs::File::open(&to)?.sync_all()?;
        }
        // The entries, not only the bytes: a crash after the root is renamed
        // into place must not leave it naming files the directory lost.
        #[cfg(unix)]
        std::fs::File::open(destination)?.sync_all()?;
        Ok(())
    }

    fn keep(&mut self) {
        self.kept = true;
    }
}

impl Drop for PublishedLocations {
    fn drop(&mut self) {
        if self.kept {
            return;
        }
        for file in &self.files {
            if let Err(e) = std::fs::remove_file(file) {
                if e.kind() != io::ErrorKind::NotFound {
                    warn!(
                        "Could not remove {} after a failed restore: {}",
                        file.display(),
                        e
                    );
                }
            }
        }
    }
}

fn is_empty_or_missing(dir: &Path) -> io::Result<bool> {
    match std::fs::read_dir(dir) {
        Ok(mut entries) => Ok(entries.next().is_none()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e),
    }
}

/// Where each storage location a snapshot carried is restored to.
///
/// In place, its own path: every path in the pool file is still true there,
/// and the location is the disk the operator gave those tables. Anywhere else,
/// a subdirectory of the restored database — the source's location is still
/// the source's, and a clone writing into it would overwrite live tables.
fn location_destinations(
    snapshot: &SnapshotEntry,
    final_dir: &Path,
    in_place: bool,
) -> Vec<(manifest::PoolLocation, PathBuf)> {
    snapshot
        .locations
        .iter()
        .map(|location| {
            let destination = if in_place {
                PathBuf::from(&location.path)
            } else {
                final_dir.join(restored_location_dir(&location.key))
            };
            (location.clone(), destination)
        })
        .collect()
}

/// The subdirectory a restore into a new directory gives a location's files.
fn restored_location_dir(key: &str) -> String {
    format!(
        "{}{}",
        super::RESTORED_LOCATION_PREFIX,
        key.strip_prefix(super::LOCATION_KEY_PREFIX).unwrap_or(key)
    )
}

/// Assembles the restore in `target_dir`, which is the staging directory.
///
/// `final_dir` is only used for messages, so they name the place the operator
/// asked for rather than the scratch directory they never see — and for the
/// paths the restored pool files name, which must be true after the rename.
///
/// Returns, with the report, the storage locations an in-place restore still
/// has to copy into place: `(staged directory, destination)`.
async fn build(
    client: &S3Client,
    config: &BackupConfig,
    target_dir: &Path,
    target: RecoveryTarget,
    final_dir: &Path,
) -> io::Result<(RestoreReport, Vec<(PathBuf, PathBuf)>)> {
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
    // Refused before anything is downloaded or deleted: these become paths a
    // restore writes into, copies out of and removes.
    validate_locations(snapshot)?;

    // With a storage location, every log stops at one cut; see `LogCut`.
    let cut = target.resolve_cut(&manifest, snapshot)?;
    if let Some(cut) = &cut {
        verify_cut_is_archived(&manifest, snapshot, cut)?;
        if manifest.cuts.last().is_some_and(|newest| newest != cut)
            && matches!(target, RecoveryTarget::Latest)
        {
            warn!(
                "The newest restore point in the catalogue cannot serve snapshot {}; restoring \
                 to an earlier one. Commits after it are not in this restore.",
                snapshot.id
            );
        }
    }
    let ceiling_for = |database: &str| match &cut {
        Some(cut) => cut.ceiling_for(database),
        None => target.resolve_lsn_for(&manifest, snapshot, database),
    };
    let ceiling = ceiling_for("");

    info!(
        "Restoring snapshot {} (taken {}, LSN {}..{}) into {}",
        snapshot.id,
        timeutil::format_rfc3339(snapshot.finished_at),
        snapshot.start_lsn,
        snapshot.end_lsn,
        final_dir.display()
    );

    // In place when it lands on the directory the backup came from — the
    // CLI's `--restore-from-s3` — which is the one case a location's files go
    // back to the location itself. That must not merge into whatever is there
    // now, so it is checked before anything is downloaded.
    let source_root = Some(manifest.source_location.as_str()).filter(|s| !s.is_empty());
    let in_place = source_root.is_some_and(|root| {
        crate::core::storage::normalize(root)
            == crate::core::storage::normalize(&final_dir.to_string_lossy())
    });
    let destinations = location_destinations(snapshot, final_dir, in_place);
    if in_place {
        for (location, destination) in &destinations {
            if !is_empty_or_missing(destination)? {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "storage location {} is not empty. An in-place restore puts the tables \
                         snapshot {} kept there back into it, and will not merge them with what \
                         is there now; move it aside first.",
                        location.path, snapshot.id
                    ),
                ));
            }
        }
    }

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
    // Every pool file, the root's and each created database's: each names its
    // own directory by an absolute path.
    let moved: Vec<(String, String)> = destinations
        .iter()
        .map(|(location, destination)| {
            (
                location.path.clone(),
                destination.to_string_lossy().into_owned(),
            )
        })
        .collect();
    let mut dropped: Vec<String> = Vec::new();
    let mut repoint_failed = None;
    for (written_in, final_root, root) in pool_files(target_dir, final_dir, source_root) {
        match crate::core::storage::repoint_restored_pool(
            &written_in,
            &final_root,
            root.as_deref(),
            &moved,
        ) {
            Ok(names) => dropped.extend(names),
            Err(e) => repoint_failed = Some(e),
        }
    }
    if !dropped.is_empty() {
        warn!(
            "The source placed {} on storage locations this snapshot did not copy (it predates \
             copying them, or they are backup locations). Those tables are not in this backup \
             and will read as missing: {}",
            if dropped.len() == 1 {
                "a table"
            } else {
                "tables"
            },
            dropped.join(", ")
        );
    }
    // Not fatal: the restored copy is complete either way, and refusing here
    // would turn a placement problem into no restore at all.
    if let Some(e) = repoint_failed {
        warn!(
            "Could not repoint the storage pool of the restored copy ({e}); if it names another \
             directory, queries against this copy may read that one instead"
        );
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
            // Where this database's copy began: a barrier committed past it
            // deleted a file the copy still holds. A database with no window
            // had every file created during the copy, which began at 0.
            let copied_at = window.as_ref().map_or(0, |w| w.start_lsn);
            let applied = replay(&replay_path, &into, ceiling_for(&database), copied_at)?;
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

    // Each location's files were unpacked and replayed under its stream key;
    // now they go where its pool lines say. A location's log archived without
    // the location being in this snapshot — one added after it — has nothing
    // of its own to restore into, and its records were counted as unresolved.
    let mut in_place_locations = Vec::new();
    for (location, destination) in &destinations {
        let staged = target_dir.join(&location.key);
        std::fs::create_dir_all(&staged)?;
        if in_place {
            in_place_locations.push((staged, destination.clone()));
        } else {
            std::fs::rename(
                &staged,
                target_dir.join(restored_location_dir(&location.key)),
            )?;
        }
    }
    for entry in std::fs::read_dir(target_dir)?.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let carried = destinations.iter().any(|(l, _)| l.key == name && in_place);
        if name.starts_with(super::LOCATION_KEY_PREFIX) && !carried {
            let _ = std::fs::remove_dir_all(entry.path());
        }
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

    Ok((report, in_place_locations))
}

/// Every pool file in a restored tree: `(directory, its final root, its source
/// root)` for each.
///
/// The roots are the whole backup's when the catalogue recorded where it was
/// taken from, so a path anywhere under the source maps to the same place
/// under the restore. Without that record each file falls back to its own
/// first `location` line, as [`crate::core::storage::repoint_pool_config`]
/// always did — which is only right relative to that file's own directory.
fn pool_files(
    target_dir: &Path,
    final_dir: &Path,
    source_root: Option<&str>,
) -> Vec<(PathBuf, PathBuf, Option<String>)> {
    let mut out = vec![(
        target_dir.to_path_buf(),
        final_dir.to_path_buf(),
        source_root.map(str::to_string),
    )];
    let Ok(entries) = std::fs::read_dir(target_dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(super::LOCATION_KEY_PREFIX)
            || !entry.path().join(crate::core::storage::POOL_FILE).is_file()
        {
            continue;
        }
        out.push(match source_root {
            Some(root) => (
                entry.path(),
                final_dir.to_path_buf(),
                Some(root.to_string()),
            ),
            None => (entry.path(), final_dir.join(&name), None),
        });
    }
    out
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

/// Refuses a catalogue whose storage locations are not what a backup writes.
///
/// A location's key names a directory inside the restore that is renamed,
/// copied from and deleted, and its path is where an in-place restore writes.
/// Archive entries and object names are checked for the same reason
/// ([`archive::is_safe_name`]); a corrupt or hostile catalogue must not choose
/// what a restore removes or where it writes.
fn validate_locations(snapshot: &SnapshotEntry) -> io::Result<()> {
    let mut seen: Vec<&str> = Vec::new();
    for location in &snapshot.locations {
        let hex = location.key.strip_prefix(super::LOCATION_KEY_PREFIX);
        let well_formed = hex.is_some_and(|hex| {
            hex.len() == 16 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        });
        let path = Path::new(&location.path);
        let plain_path = path.is_absolute()
            && path.components().all(|c| {
                matches!(c, std::path::Component::RootDir | std::path::Component::Normal(_))
            });
        if !well_formed
            || !plain_path
            || location.key != super::location_key(&location.path)
            || seen.contains(&location.key.as_str())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "the backup catalogue names storage location {:?} at {:?} in snapshot {}, \
                     which is not one a backup writes; refusing to restore from it",
                    location.key, location.path, snapshot.id
                ),
            ));
        }
        seen.push(&location.key);
    }
    Ok(())
}

/// Refuses a cut whose logs the catalogue does not reach.
///
/// Recorded only after its bundles were, but bundles can go — retention, a
/// hand-deleted object — and replaying short of a cut is the half-a-transaction
/// restore cuts exist to prevent. Only the logs the snapshot has a window for:
/// another may have archived nothing since the snapshot, with the cut naming
/// where its log already stood.
fn verify_cut_is_archived(
    manifest: &Manifest,
    snapshot: &SnapshotEntry,
    cut: &manifest::LogCut,
) -> io::Result<()> {
    for window in &snapshot.windows {
        let Some(lsn) = cut.ceiling_for(&window.database) else {
            continue;
        };
        if lsn <= window.end_lsn {
            continue;
        }
        let archived_through = manifest
            .segments_for(&window.database)
            .into_iter()
            .filter(|s| manifest::segment_needed_by(snapshot, s))
            .map(|s| s.last_lsn)
            .max()
            .unwrap_or(0);
        if archived_through < lsn {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "the restore point for snapshot {} needs the log of {} through LSN {lsn}, \
                     but the catalogue only reaches LSN {archived_through}",
                    snapshot.id,
                    if window.database.is_empty() {
                        "the default database"
                    } else {
                        window.database.as_str()
                    }
                ),
            ));
        }
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
///
/// `copied_at` is the commit sequence when the snapshot's copy of this
/// database began: a file whose deletion committed after it is emptied before
/// replay writes the next file of its name (see
/// [`crate::core::wal::Wal::recover_restoring`]). Without that, a table
/// dropped and created again after the snapshot came back holding every old
/// row past the new table's end.
pub(super) fn replay(
    replay_path: &Path,
    target_dir: &Path,
    ceiling: Option<u64>,
    copied_at: u64,
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

    // A file the snapshot copied mid-append can end inside a record. Cut back
    // before the log replays, as an open does, so the records it rewrites are
    // not appended after a fragment. A restored file has no durable floor:
    // what the copy holds past its last whole record is what the log replays.
    crate::core::torn_tail::cut_torn_tails(target_dir, &Default::default())?;

    crate::core::wal::Wal::recover_restoring(
        replay_path,
        |id| resolver.get(&id).cloned(),
        ceiling,
        copied_at,
    )
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
            locations: Vec::new(),
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

    // ---- storage locations --------------------------------------------------

    fn location(key: &str, path: &str) -> manifest::PoolLocation {
        manifest::PoolLocation {
            key: key.to_string(),
            path: path.to_string(),
        }
    }

    /// A catalogue's location key becomes a directory the restore renames and
    /// deletes, and its path a place an in-place restore writes. Neither may be
    /// anything a backup would not have written.
    #[test]
    fn a_location_that_could_name_any_path_is_refused() {
        let good_key = super::super::location_key("/mnt/disk2");
        let mut s = entry(0, 0);
        s.locations = vec![location(&good_key, "/mnt/disk2")];
        validate_locations(&s).expect("what a backup writes");

        let other_key = super::super::location_key("/mnt/elsewhere");
        for (key, path) in [
            ("/tmp/x", "/mnt/disk2"),
            ("../x", "/mnt/disk2"),
            ("~loc-../../../../x", "/mnt/disk2"),
            ("~loc-ABCDEF0123456789", "/mnt/disk2"),
            (other_key.as_str(), "/mnt/disk2"),
            (good_key.as_str(), "relative/disk2"),
            (good_key.as_str(), "/mnt/../disk2"),
        ] {
            s.locations = vec![location(key, path)];
            let err = validate_locations(&s).expect_err(&format!("{key:?} at {path:?}"));
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
        }

        s.locations = vec![location(&good_key, "/mnt/disk2"), location(&good_key, "/mnt/disk2")];
        validate_locations(&s).expect_err("one key twice");
    }

    fn files_in(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .map(|d| {
                d.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    fn staged_location(dir: &Path, name: &str) -> PathBuf {
        let staged = dir.join(name);
        std::fs::create_dir_all(&staged).unwrap();
        std::fs::write(staged.join("t.db"), b"rows").unwrap();
        std::fs::write(staged.join("t_pointers.db"), b"pointers").unwrap();
        staged
    }

    /// A failed in-place restore must take back what it copied into the
    /// operator's location: left behind, a retry is refused as "not empty", or
    /// merges with whatever appears there next.
    #[test]
    fn published_location_files_are_removed_unless_the_restore_completes() {
        let dir = temp_testdir::TempDir::default();
        let staged = staged_location(&dir, "staged");
        let destination = dir.join("disk2");

        {
            let mut published = PublishedLocations::default();
            published.publish(&staged, &destination).unwrap();
            assert_eq!(files_in(&destination), vec!["t.db", "t_pointers.db"]);
        }
        assert!(
            files_in(&destination).is_empty(),
            "a restore that did not complete left files in the location"
        );

        {
            let mut published = PublishedLocations::default();
            published.publish(&staged, &destination).unwrap();
            published.keep();
        }
        assert_eq!(files_in(&destination), vec!["t.db", "t_pointers.db"]);
    }

    /// Checked again at publish time, not only before the download: something
    /// that appeared meanwhile is neither merged with nor removed.
    #[test]
    fn a_location_that_filled_up_meanwhile_is_left_alone() {
        let dir = temp_testdir::TempDir::default();
        let staged = staged_location(&dir, "staged");
        let destination = dir.join("disk2");
        std::fs::create_dir_all(&destination).unwrap();
        std::fs::write(destination.join("theirs.db"), b"not from the backup").unwrap();

        let mut published = PublishedLocations::default();
        let err = published.publish(&staged, &destination).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{err}");
        assert!(err.to_string().contains("no longer empty"), "{err}");
        drop(published);
        assert_eq!(files_in(&destination), vec!["theirs.db"]);
    }

    /// The first location published, the second refused: the first is undone
    /// too, so a retry finds both as they were.
    #[test]
    fn a_later_location_failing_undoes_an_earlier_one() {
        let dir = temp_testdir::TempDir::default();
        let first = staged_location(&dir, "first");
        let second = staged_location(&dir, "second");
        let (to_first, to_second) = (dir.join("disk2"), dir.join("disk3"));
        std::fs::create_dir_all(&to_second).unwrap();
        std::fs::write(to_second.join("theirs.db"), b"x").unwrap();

        {
            let mut published = PublishedLocations::default();
            published.publish(&first, &to_first).unwrap();
            published.publish(&second, &to_second).unwrap_err();
        }
        assert!(files_in(&to_first).is_empty(), "{:?}", files_in(&to_first));
        assert_eq!(files_in(&to_second), vec!["theirs.db"]);
    }
}
