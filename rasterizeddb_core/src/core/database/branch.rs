//! Database branches: a database made as a consistent copy of another, which
//! it then diverges from, and can be refreshed from, swapped with, or merged
//! back into.
//!
//! Behind the `db_branching` feature. Design: `docs/branching.md`.
//!
//! # What a branch is
//!
//! An ordinary database of the catalogue — its own directory, log, roles and
//! tables, reachable by name — plus a small metadata file, [`META_FILE`],
//! naming the database it was copied from. Nothing is shared with the parent
//! after the copy, so writes on either side never reach the other, and the
//! parent can be dropped without touching its branches.
//!
//! # How a live database is copied
//!
//! The parent keeps taking writes while it is copied, so the copy is fuzzy,
//! and the log makes it consistent — the argument a backup snapshot rests on
//! (`docs/s3-backup.md` §2), applied to a local copy:
//!
//! 1. The snapshot's gates are taken ([`CopyGates`]): no compaction, rename or
//!    move — writers with no log record behind them — runs during the copy.
//!    The deletion gate only until the copy's starting point is fixed.
//! 2. Every log the parent writes (its own and each storage location's) is
//!    held against checkpoints ([`crate::core::wal::Wal::hold_checkpoints`]),
//!    so from here it keeps every record since its last truncation.
//! 3. Files are copied — cloned (`FICLONE`) where the filesystem shares
//!    blocks, copied in full otherwise.
//! 4. Each log is copied, whole records only, and replayed over the copy with
//!    the restore's barrier-aware replay. Records are physical redo, so it
//!    does not matter what the copy already caught: afterwards every file is
//!    what the log says, through the last commit the copied log holds.
//!
//! The copy is assembled in a hidden staging directory and renamed into
//! place, so no half-built branch is ever a database.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::*;

/// Present in every branch's directory.
pub const META_FILE: &str = "rdb_branch.meta";

/// Under the catalogue root: the frozen copy of each branch as it was made,
/// which a merge diffs both sides against. Hidden (a leading dot is not a
/// valid database name), so it is never discovered as a database.
pub const BASES_DIR: &str = ".rdb_branch_bases";

/// A branch being built: `<root>/.branching-<name>`.
const STAGING_PREFIX: &str = ".branching-";
/// A branch's old copy while `RESET` swaps the new one in.
const RETIRING_PREFIX: &str = ".retiring-";
/// The parent, set aside while `PROMOTE` swaps names.
const PROMOTING_PREFIX: &str = ".promoting-";
/// A `PROMOTE` in progress, in the root: what start-up finishes.
const PROMOTE_INTENT: &str = ".rdb_promote_intent";
/// A point-in-time branch's restore, before its database is taken out.
const PITR_PREFIX: &str = ".branch-restore-";
/// The parent's log, copied into the branch and replayed there.
const REPLAY_FILE: &str = "branch_replay.wal";
/// The schema changes made on a branch since its base, one JSON string per
/// line, in commit order: what a merge replays on the parent.
pub const DDL_FILE: &str = "rdb_branch.ddl";

/// Space left free on the target filesystem after a full copy: a copy that
/// fits exactly would leave the server unable to write anything at all.
const FREE_SPACE_MARGIN: u64 = 256 * 1024 * 1024;

/// What a branch remembers about how it was made.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BranchMeta {
    /// The database it was copied from.
    pub parent: String,
    /// When the copy was made (Unix seconds).
    pub created_at: u64,
    /// The parent's commit sequence the copy reached.
    pub parent_lsn: u64,
    /// `reflink`, `copy`, `mixed` (some files cloned, some copied), or
    /// `restore` (a past point, from the S3 backup).
    pub method: String,
    /// `now`, or the past point asked for: `lsn N`, `time <rfc3339>`,
    /// `snapshot <id>`.
    pub point: String,
    /// Whether a base was kept, so the branch can be merged.
    pub has_base: bool,
    /// Bytes the copy holds.
    pub bytes: u64,
    /// The last successful `MERGE BRANCH`, if any (Unix seconds).
    #[serde(default)]
    pub merged_at: Option<u64>,
    /// Which base is current: `.rdb_branch_bases/<name>.<generation>`. A
    /// merge writes the next one beside it and switches only once it has
    /// committed.
    #[serde(default)]
    pub base_generation: u64,
    /// Branches it was re-parented through, oldest first: each a branch it
    /// was made from (directly or through another) that was dropped, so its
    /// parent became that branch's parent. Its base is still the copy taken
    /// from the first of them.
    #[serde(default)]
    pub via: Vec<String>,
    /// Names this copy among every branch ever made under its name: what a
    /// merge's renumbered keys are recorded against in the parent, so a
    /// branch made again (or reset) never takes an old one's.
    #[serde(default)]
    pub id: String,
}

impl BranchMeta {
    /// [`BranchMeta::id`], or for metadata written before it existed, the
    /// name and creation time.
    pub fn merge_id(&self, name: &str) -> String {
        if self.id.is_empty() {
            format!("{name}@{}", self.created_at)
        } else {
            self.id.clone()
        }
    }

    pub fn read(dir: &Path) -> Option<BranchMeta> {
        let text = std::fs::read_to_string(dir.join(META_FILE)).ok()?;
        serde_json::from_str(&text).ok()
    }

    pub fn write(&self, dir: &Path) -> io::Result<()> {
        let text = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
        let temp = dir.join(format!("{META_FILE}.tmp"));
        std::fs::write(&temp, text.as_bytes())?;
        if let Ok(f) = std::fs::File::open(&temp) {
            let _ = f.sync_all();
        }
        std::fs::rename(&temp, dir.join(META_FILE))?;
        let _ = crate::core::wal::sync_parent_dir(&dir.join(META_FILE));
        Ok(())
    }
}

/// Where a branch is taken from in the parent's history.
#[derive(Debug, Clone, PartialEq)]
pub enum BranchPoint {
    /// As the parent is now, copied live.
    Now,
    /// Through this commit sequence of the parent's log, from the S3 backup.
    Lsn(u64),
    /// Through the last archive flush at or before this instant, from the S3
    /// backup.
    Time(u64),
    /// This backup snapshot exactly.
    Snapshot(String),
}

impl BranchPoint {
    fn describe(&self) -> String {
        match self {
            BranchPoint::Now => "now".to_string(),
            BranchPoint::Lsn(n) => format!("lsn {n}"),
            BranchPoint::Time(t) => {
                format!("time {}", crate::core::backup::timeutil::format_rfc3339(*t))
            }
            BranchPoint::Snapshot(id) => format!("snapshot {id}"),
        }
    }
}

/// One row of `SHOW BRANCHES`.
#[derive(Debug, Clone)]
pub struct BranchListing {
    pub name: String,
    pub meta: BranchMeta,
    /// Whether the parent still exists under its name.
    pub parent_exists: bool,
    pub sessions: u64,
    /// Bytes the base holds, when there is one.
    pub base_bytes: Option<u64>,
    /// Branches between it and the first ancestor that is not a branch (or
    /// no longer exists): 1 for a branch of an ordinary database.
    pub depth: u64,
    /// Its ancestry, root first, ending with its own name: `prod/dev/x`.
    pub path: String,
    /// Branches made from it.
    pub children: u64,
}

/// What a copy did, for the branch's metadata and the operator.
#[derive(Debug, Default, Clone)]
struct CopyStats {
    files: u64,
    bytes: u64,
    cloned: u64,
    copied: u64,
}

impl CopyStats {
    fn method(&self) -> &'static str {
        match (self.cloned, self.copied) {
            (_, 0) => "reflink",
            (0, _) => "copy",
            _ => "mixed",
        }
    }
}

/// Copies one file, sharing its blocks where the filesystem can.
fn copy_file(from: &Path, to: &Path, stats: &mut CopyStats) -> io::Result<()> {
    let len = std::fs::metadata(from)?.len();
    stats.files += 1;
    stats.bytes += len;
    if len > 0 && try_clone(from, to)? {
        stats.cloned += 1;
        return Ok(());
    }
    std::fs::copy(from, to)?;
    stats.copied += 1;
    Ok(())
}

/// `FICLONE`: `to` shares `from`'s blocks, copied on write. `Ok(false)` when
/// the filesystem cannot (ext4, NTFS, across filesystems), and `to` is then
/// left for an ordinary copy to overwrite.
#[cfg(target_os = "linux")]
fn try_clone(from: &Path, to: &Path) -> io::Result<bool> {
    use std::os::fd::AsRawFd;
    let source = std::fs::File::open(from)?;
    let target = std::fs::File::create(to)?;
    // SAFETY: both descriptors are open for the duration of the call, and
    // FICLONE takes the source descriptor as its argument.
    let r = unsafe { libc::ioctl(target.as_raw_fd(), libc::FICLONE, source.as_raw_fd()) };
    Ok(r == 0)
}

#[cfg(not(target_os = "linux"))]
fn try_clone(_from: &Path, _to: &Path) -> io::Result<bool> {
    Ok(false)
}

/// Bytes free to this process on the filesystem holding `path`.
#[cfg(unix)]
fn free_bytes(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid C string and `st` a valid out-pointer.
    if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
        return None;
    }
    Some(st.f_bavail as u64 * st.f_frsize as u64)
}

#[cfg(not(unix))]
fn free_bytes(_path: &Path) -> Option<u64> {
    None
}

/// Whether a file of a database's directory is left out of its branch.
///
/// What a backup snapshot leaves out (the live log — replayed from a copy
/// instead —, the durable floors, compaction scratch, index-store snapshots,
/// which the branch rebuilds), plus what belongs to this node rather than to
/// the data: replication's log and state, and a branch's own metadata.
fn excluded(name: &str) -> bool {
    crate::core::backup::excluded_from_copy(name)
        || name.starts_with("db_replication.")
        || name == META_FILE
        || name == format!("{META_FILE}.tmp")
        || name == DDL_FILE
        || name.starts_with(".rdb_")
        || name == "RESTORED.json"
        || name == REPLAY_FILE
        // What ntfs-3g (FUSE) keeps of a file removed while open: the parent's
        // garbage, never its data.
        || name.starts_with(".fuse_hidden")
        // A branch never shares its parent's stores: their objects are the
        // parent's, and a table the branch placed there would write among
        // them. Their tables are copied out of them ([`copy_store_tables`]).
        || name == "db_kv.cfg"
        || name == "db_kv.cfg.tmp"
        || name == "db_kv_secrets.cfg"
        || name == "db_kv_secrets.cfg.tmp"
}

/// The files of a database directory to copy: flat, because a database's own
/// files sit directly in it; a subdirectory is another database (the default
/// database is the catalogue root), a storage location (copied as one), or
/// index runs the copy rebuilds.
fn files_of(dir: &Path) -> io::Result<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)?.flatten() {
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if excluded(&name) {
            continue;
        }
        out.push((name, entry.path()));
    }
    out.sort();
    Ok(out)
}

/// Lengths of the staged-append sidecars (`.tmp`) under the given dirs.
///
/// A fold moves a sidecar's bytes into its main file with no log record; one
/// landing mid-copy could leave the bytes in neither copy. No write path
/// stages today, so this only ever fails a copy rather than letting it lose
/// rows silently (the snapshot's `check_staged_appends`).
fn staged_lengths(dirs: &[PathBuf]) -> HashMap<PathBuf, u64> {
    let mut out = HashMap::new();
    for dir in dirs {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let path = e.path();
                if path.extension().is_some_and(|x| x == "tmp")
                    && let Ok(m) = e.metadata()
                {
                    out.insert(path, m.len());
                }
            }
        }
    }
    out
}

/// One directory of the parent the branch copies, and the log behind it.
struct Stream {
    /// The parent's directory.
    source: PathBuf,
    /// Where its files go in the staging directory.
    dest: PathBuf,
    wal: std::sync::Arc<crate::core::wal::Wal>,
    /// Commit sequence when the copy began.
    copied_at: u64,
    /// The names of the files the log knows in `source`: a file created
    /// while the copy ran exists only in the log, and replay creates it.
    named: Vec<String>,
}

/// The name a storage location outside the parent's directory is copied to
/// inside the branch: what a restore elsewhere names it.
fn location_dir_name(path: &str) -> String {
    format!(
        "{}{:016x}",
        crate::core::backup::RESTORED_LOCATION_PREFIX,
        crate::core::wal::file_id_for_name(&crate::core::wal::dir_key(path))
    )
}

/// Copies the live database at `source` into `staging`, which will be
/// published as `final_dir`, consistent as of the parent's commit sequence
/// it returns.
async fn copy_live(
    source: &Path,
    staging: &Path,
    final_dir: &Path,
    space_factor: u64,
) -> io::Result<(u64, CopyStats)> {
    let source_key = crate::core::wal::dir_key(&source.to_string_lossy());
    let Some(root_wal) = crate::core::wal::wal_for(&source_key) else {
        return Err(io::Error::other(
            "branching needs the write-ahead log, and this server runs without it: a copy taken \
             while writes run is made consistent by replaying the log",
        ));
    };

    // The storage locations the parent keeps tables on, and where each goes.
    #[allow(unused_mut)]
    let mut locations: Vec<(PathBuf, PathBuf)> = Vec::new();
    #[allow(unused_mut)]
    let mut moved: Vec<(String, String)> = Vec::new();
    #[cfg(feature = "storage_pools")]
    {
        refuse_uncopyable(source)?;
        let source_norm = crate::core::storage::normalize(&source.to_string_lossy());
        for path in crate::core::storage::data_locations(source)? {
            let path_norm = crate::core::storage::normalize(&path);
            let inside = Path::new(&path_norm)
                .strip_prefix(&source_norm)
                .ok()
                .filter(|rest| !rest.as_os_str().is_empty())
                .map(Path::to_path_buf);
            match inside {
                // Inside the parent's directory: the same place inside the
                // branch, which the pool file's rewrite maps by itself.
                Some(rest) => locations.push((PathBuf::from(&path_norm), staging.join(rest))),
                None => {
                    let name = location_dir_name(&path_norm);
                    locations.push((PathBuf::from(&path_norm), staging.join(&name)));
                    moved.push((
                        path_norm.clone(),
                        final_dir.join(&name).to_string_lossy().into_owned(),
                    ));
                }
            }
        }
    }

    let mut streams = vec![Stream {
        source: source.to_path_buf(),
        dest: staging.to_path_buf(),
        wal: root_wal,
        copied_at: 0,
        named: Vec::new(),
    }];
    let mut unlogged: Vec<(PathBuf, PathBuf)> = Vec::new();
    for (path, dest) in &locations {
        match crate::core::wal::wal_for(&crate::core::wal::dir_key(&path.to_string_lossy())) {
            Some(wal) => streams.push(Stream {
                source: path.clone(),
                dest: dest.clone(),
                wal,
                copied_at: 0,
                named: Vec::new(),
            }),
            // A location whose log was never opened holds nothing written
            // since this process started; its files are copied as they are.
            None => unlogged.push((path.clone(), dest.clone())),
        }
    }

    // Space: probed by cloning the largest file. A filesystem that shares
    // blocks needs next to nothing; anything else needs the whole size. An
    // estimate: the copy itself lists again once the logs are held.
    let mut listing: Vec<(PathBuf, Vec<(String, PathBuf)>)> = Vec::new();
    for s in &streams {
        listing.push((s.dest.clone(), files_of(&s.source)?));
    }
    for (path, dest) in &unlogged {
        listing.push((dest.clone(), files_of(path)?));
    }
    let total: u64 = listing
        .iter()
        .flat_map(|(_, files)| files.iter())
        .filter_map(|(_, p)| std::fs::metadata(p).ok().map(|m| m.len()))
        .sum();
    for (dest, _) in &listing {
        std::fs::create_dir_all(dest)?;
    }
    let shares_blocks = listing
        .iter()
        .flat_map(|(dest, files)| files.iter().map(move |f| (dest, f)))
        .filter_map(|(dest, (name, p))| Some((dest, name, p, std::fs::metadata(p).ok()?.len())))
        .filter(|(.., len)| *len > 0)
        .max_by_key(|(.., len)| *len)
        .map(|(dest, name, p, _)| {
            let probe = dest.join(format!("{name}.clone-probe"));
            let ok = try_clone(p, &probe).unwrap_or(false);
            let _ = std::fs::remove_file(&probe);
            ok
        })
        .unwrap_or(true);
    if !shares_blocks {
        let needed = total.saturating_mul(space_factor) + FREE_SPACE_MARGIN;
        if let Some(free) = free_bytes(staging)
            && free < needed
        {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                format!(
                    "not enough space to branch: this filesystem cannot share blocks, so the copy{} \
                     needs {} and {} is free",
                    if space_factor > 1 {
                        " and its merge base"
                    } else {
                        ""
                    },
                    human(needed),
                    human(free)
                ),
            ));
        }
    }

    // ---- the copy, under the gates and the holds ----
    let mut gates = crate::core::backup::CopyGates::hold().await;
    let wals: Vec<std::sync::Arc<crate::core::wal::Wal>> =
        streams.iter().map(|s| s.wal.clone()).collect();
    let holds = tokio::task::spawn_blocking(move || {
        wals.iter()
            .map(|w| w.hold_checkpoints())
            .collect::<Vec<_>>()
    })
    .await
    .map_err(io::Error::other)?;
    for s in &mut streams {
        s.copied_at = s.wal.commit_lsn();
    }
    // Listed again now the logs are held: a file made before this point and
    // checkpointed away would otherwise be in neither the listing above nor
    // the log. One made after it is in the log from its first byte.
    let mut listing: Vec<(PathBuf, Vec<(String, PathBuf)>)> = Vec::new();
    for s in &streams {
        listing.push((s.dest.clone(), files_of(&s.source)?));
    }
    for (path, dest) in &unlogged {
        listing.push((dest.clone(), files_of(path)?));
    }
    // Each copy's start is fixed: a deletion from here falls after it, and the
    // replay's barrier handling covers it.
    gates.release_deletion();

    let sidecar_dirs: Vec<PathBuf> = streams
        .iter()
        .map(|s| s.source.clone())
        .chain(unlogged.iter().map(|(p, _)| p.clone()))
        .collect();
    let before = staged_lengths(&sidecar_dirs);

    let copy_listing = listing.clone();
    let stream_logs: Vec<(std::sync::Arc<crate::core::wal::Wal>, PathBuf)> = streams
        .iter()
        .map(|s| (s.wal.clone(), s.dest.join(REPLAY_FILE)))
        .collect();
    let (stats, lsns) =
        tokio::task::spawn_blocking(move || -> io::Result<(CopyStats, Vec<u64>)> {
            let mut stats = CopyStats::default();
            for (dest, files) in &copy_listing {
                for (name, path) in files {
                    match copy_file(path, &dest.join(name), &mut stats) {
                        Ok(()) => {}
                        // Dropped while the copy ran: its deletion's barrier is in
                        // the log, and replay voids its records.
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e),
                    }
                }
            }
            // The logs last: every record a copied file may lack is in them.
            let mut lsns = Vec::new();
            for (wal, to) in &stream_logs {
                lsns.push(wal.copy_log_to(to)?);
            }
            Ok((stats, lsns))
        })
        .await
        .map_err(io::Error::other)??;
    // The files each log knows, read after its copy: one made while the copy
    // ran is in no listing, and replay makes it from its records.
    for s in &mut streams {
        let dir = crate::core::wal::dir_key(&s.source.to_string_lossy());
        s.named = s
            .wal
            .registered_files()
            .into_iter()
            .filter(|(_, p)| {
                p.parent().is_some_and(|parent| {
                    crate::core::wal::dir_key(&parent.to_string_lossy()) == dir
                })
            })
            .filter_map(|(_, p)| p.file_name().and_then(|n| n.to_str()).map(str::to_string))
            .collect();
    }

    let after = staged_lengths(&sidecar_dirs);
    if before
        .iter()
        .any(|(p, len)| after.get(p).is_some_and(|now| now < len))
    {
        return Err(io::Error::other(
            "a staged append was folded into its table while the branch was being copied, so the \
             copy may hold its rows in neither file; retry the branch",
        ));
    }
    drop(holds);
    drop(gates);

    // ---- made consistent, offline ----
    let replays: Vec<(PathBuf, u64, Vec<String>)> = streams
        .iter()
        .map(|s| (s.dest.clone(), s.copied_at, s.named.clone()))
        .collect();
    tokio::task::spawn_blocking(move || -> io::Result<()> {
        for (dest, copied_at, named) in replays {
            replay_into(&dest, &dest.join(REPLAY_FILE), copied_at, &named)?;
            std::fs::remove_file(dest.join(REPLAY_FILE))?;
        }
        Ok(())
    })
    .await
    .map_err(io::Error::other)??;

    // After the replay: the log may still name a file a table had before it
    // moved onto a store, and its records must not land on the copy.
    copy_store_tables(source, staging).await?;

    crate::core::database::incompat::fence_restored_directory(staging)?;
    repoint(staging, final_dir, source, &moved)?;

    Ok((lsns.first().copied().unwrap_or(0), stats))
}

/// Refuses a database holding something a branch cannot copy: the partitions
/// of a partitioned table placed on a key/value store (which a backup does
/// not copy either, `backup::cold`). And says what a branch holds
/// differently: in-memory tables come over empty.
#[cfg(feature = "storage_pools")]
fn refuse_uncopyable(source: &Path) -> io::Result<()> {
    use crate::core::backup::cold;
    use crate::core::storage::TableBackend;
    let partitioned = cold::partitioned_on_stores(source);
    if !partitioned.is_empty() {
        let stores: std::collections::BTreeSet<&String> = partitioned.values().flatten().collect();
        return Err(io::Error::other(format!(
            "cannot branch: {} partitioned table(s) keep partitions on key/value store(s) {}, which a branch \
             cannot copy yet; STORAGE MOVE moves them back into a directory first",
            partitioned.len(),
            stores
                .iter()
                .map(|s| format!("'{s}'"))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    let placements = cold::placements(source);
    #[cfg(not(feature = "kv_storage"))]
    {
        let on_stores: Vec<&String> = placements
            .iter()
            .filter(|(_, spec)| matches!(TableBackend::parse(spec), TableBackend::KeyValue(_)))
            .map(|(t, _)| t)
            .collect();
        if !on_stores.is_empty() {
            return Err(io::Error::other(format!(
                "cannot branch: this build has no key/value storage, so the tables on stores cannot be read: {}",
                on_stores.iter().map(|t| t.as_str()).collect::<Vec<_>>().join(", ")
            )));
        }
    }
    let memory: Vec<&String> = placements
        .iter()
        .filter(|(_, spec)| matches!(TableBackend::parse(spec), TableBackend::Memory))
        .map(|(t, _)| t)
        .collect();
    if !memory.is_empty() {
        crate::core::notice::push(
            "WARNING",
            "01000",
            format!(
                "in-memory table(s) {} come over empty: nothing of them is on disk to copy",
                memory.iter().map(|t| t.as_str()).collect::<Vec<_>>().join(", ")
            ),
        );
    }
    Ok(())
}

#[cfg(not(feature = "storage_pools"))]
fn refuse_uncopyable(_source: &Path) -> io::Result<()> {
    Ok(())
}

/// Copies the tables `source` keeps on key/value stores into `staging` as
/// ordinary files, one crash-consistent cut per store (the backup's
/// `kv::export`): on the branch they are tables of its own directory, the
/// pool file's rewrite dropping their placements. Never onto the store
/// itself, where the branch would write among the parent's objects; for the
/// same reason the store definitions are not copied ([`excluded`]).
///
/// Each store is cut at its own moment, after the directories' copy: a
/// transaction that wrote both a store's table and a directory's may be in
/// one copy and not the other.
#[cfg(all(feature = "storage_pools", feature = "kv_storage"))]
async fn copy_store_tables(source: &Path, staging: &Path) -> io::Result<()> {
    use crate::core::backup::cold;
    use crate::core::storage::TableBackend;
    let mut on_store: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for (table, spec) in cold::placements(source) {
        if let TableBackend::KeyValue(store) = TableBackend::parse(&spec) {
            on_store.entry(store).or_default().push(table);
        }
    }
    if on_store.is_empty() {
        return Ok(());
    }
    let stores = crate::core::storage::kv_registry::KvRegistry::open(&source.to_string_lossy());
    let mut copied: Vec<String> = Vec::new();
    for (store, tables) in on_store {
        let scratch = staging.join(format!(".kv-export-{store}"));
        remove_quietly(&scratch);
        let (client, config) = stores
            .connect(&store)
            .map_err(|e| io::Error::other(format!("cannot branch: store '{store}': {e}")))?;
        let owned = |file: &str| cold::owner(&tables, file).is_some();
        let files = crate::core::storage_providers::kv::export::export(client, config, owned, &scratch)
            .await
            .map_err(|e| {
                remove_quietly(&scratch);
                io::Error::other(format!("cannot branch: the tables on store '{store}' could not be copied: {e}"))
            })?;
        for table in &tables {
            if !files.iter().any(|f| cold::owner(&tables, &f.name) == Some(table)) {
                remove_quietly(&scratch);
                return Err(io::Error::other(format!(
                    "cannot branch: store '{store}' holds no files for table {table}"
                )));
            }
        }
        for f in &files {
            std::fs::rename(&f.path, staging.join(&f.name))?;
        }
        remove_quietly(&scratch);
        copied.extend(tables);
    }
    crate::core::notice::push(
        "NOTICE",
        "00000",
        format!(
            "table(s) {} were copied out of their key/value store(s): on the branch they are ordinary tables \
             in its own directory, and the stores are not part of it",
            copied.join(", ")
        ),
    );
    Ok(())
}

#[cfg(not(all(feature = "storage_pools", feature = "kv_storage")))]
async fn copy_store_tables(_source: &Path, _staging: &Path) -> io::Result<()> {
    Ok(())
}

/// Rewrites the pool file in `dir` from `from` to `to`.
#[cfg(feature = "storage_pools")]
fn repoint(dir: &Path, to: &Path, from: &Path, moved: &[(String, String)]) -> io::Result<()> {
    let dropped =
        crate::core::storage::repoint_restored_pool(dir, to, Some(&from.to_string_lossy()), moved)?;
    if !dropped.is_empty() {
        log::warn!(
            "The branch at {} leaves out tables placed on storage it does not copy (backup \
             locations): {}",
            to.display(),
            dropped.join(", ")
        );
    }
    Ok(())
}

#[cfg(not(feature = "storage_pools"))]
fn repoint(_dir: &Path, _to: &Path, _from: &Path, _moved: &[(String, String)]) -> io::Result<()> {
    Ok(())
}

/// Replays a copied log over the copied files in `dir`: the restore's replay
/// (`backup::restore::replay`), for a copy whose log is a local file.
fn replay_into(
    dir: &Path,
    log: &Path,
    copied_at: u64,
    named: &[String],
) -> io::Result<crate::core::wal::RecoveryStats> {
    // File ids are a name within the directory, never a path: what lets the
    // parent's log replay into the branch.
    let mut resolver: HashMap<u64, PathBuf> = HashMap::new();
    for entry in std::fs::read_dir(dir)?.flatten() {
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if name == REPLAY_FILE {
            continue;
        }
        resolver.insert(crate::core::wal::file_id_for_name(&name), entry.path());
    }
    // A file made while the copy ran is only in the log: created by its name,
    // and only when the log covers it from its first byte.
    let starts = if named.is_empty() {
        HashMap::new()
    } else {
        crate::core::wal::file_starts(log)?
    };
    for name in named {
        let plain = !name.is_empty() && !name.contains(['/', '\\']) && name != "." && name != "..";
        let id = crate::core::wal::file_id_for_name(name);
        if plain && starts.get(&id) == Some(&0) {
            resolver.entry(id).or_insert_with(|| dir.join(name));
        }
    }
    // A file copied mid-append can end inside a record: cut back first, as an
    // open does, so the replayed records are not written after a fragment.
    crate::core::torn_tail::cut_torn_tails(dir, &Default::default())?;
    crate::core::wal::Wal::recover_restoring(log, |id| resolver.get(&id).cloned(), None, copied_at)
}

/// Copies a closed directory tree (a built branch, into its base).
fn copy_tree(from: &Path, to: &Path, stats: &mut CopyStats) -> io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)?.flatten() {
        let path = entry.path();
        if entry.file_name().to_string_lossy().starts_with(".fuse_hidden") {
            continue;
        }
        let target = to.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            copy_tree(&path, &target, stats)?;
        } else if kind.is_file() {
            copy_file(&path, &target, stats)?;
        }
    }
    Ok(())
}

fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => dir_bytes(&e.path()),
            Ok(t) if t.is_file() => e.metadata().map(|m| m.len()).unwrap_or(0),
            _ => 0,
        })
        .sum()
}

fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

/// A new [`BranchMeta::id`] for a copy named `name`.
fn fresh_id(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{name}@{nanos:x}.{:x}", std::process::id())
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Waits (up to a few seconds) until this process holds no file under
/// `dir` open: a database just dropped closes its files from its own tasks,
/// shortly after. Removing them while open works on most filesystems, but on
/// a FUSE one (ntfs-3g) leaves `.fuse_hidden` files the removal then trips
/// over.
pub(super) async fn released(dir: &Path) {
    #[cfg(target_os = "linux")]
    {
        let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
        for _ in 0..50 {
            let open = std::fs::read_dir("/proc/self/fd").ok().is_some_and(|entries| {
                entries
                    .flatten()
                    .any(|e| std::fs::read_link(e.path()).is_ok_and(|t| t.starts_with(&dir)))
            });
            if !open {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = dir;
}

/// Removes a directory if it exists, logging rather than failing.
fn remove_quietly(path: &Path) {
    if path.exists()
        && let Err(e) = std::fs::remove_dir_all(path)
    {
        warn!("could not remove {}: {}", path.display(), e);
    }
}

/// Forgets every process-wide record keyed by a database directory, before
/// the directory goes or is replaced: what `DROP DATABASE` does.
fn forget_directory(path: &Path) {
    let key = path.to_string_lossy();
    let _ = crate::core::wal::remove_for(&key);
    crate::core::storage_providers::file_sync::forget_directory_failure(&key);
}

/// Options of `CREATE BRANCH`.
#[derive(Debug, Clone, PartialEq)]
pub struct BranchOptions {
    pub point: BranchPoint,
    /// Keep a base so the branch can be merged (`WITHOUT BASE` clears it).
    pub with_base: bool,
}

impl Default for BranchOptions {
    fn default() -> Self {
        Self {
            point: BranchPoint::Now,
            with_base: true,
        }
    }
}

impl DatabaseCatalog {
    fn root_dir(&self) -> Result<&Path, String> {
        self.root
            .as_deref()
            .map(Path::new)
            .ok_or_else(|| "branching needs a catalogue on disk; this one is in memory".to_string())
    }

    /// The directory of the named database.
    pub fn directory_of(&self, name: &str) -> Option<PathBuf> {
        let root = self.root.as_deref()?;
        Some(if name == self.default_name {
            PathBuf::from(root)
        } else {
            Path::new(root).join(name)
        })
    }

    /// The branch metadata of `name`, or `None` when it is not a branch.
    pub fn branch_meta(&self, name: &str) -> Option<BranchMeta> {
        if !self.databases.contains_key(name) {
            return None;
        }
        BranchMeta::read(&self.directory_of(name)?)
    }

    /// Where generation `generation` of `name`'s merge base is kept.
    pub fn base_path(&self, name: &str, generation: u64) -> Option<PathBuf> {
        Some(
            Path::new(self.root.as_deref()?)
                .join(BASES_DIR)
                .join(format!("{name}.{generation}")),
        )
    }

    /// Where `name`'s current merge base is kept.
    pub fn base_dir(&self, name: &str) -> Option<PathBuf> {
        let meta = self.branch_meta(name)?;
        self.base_path(name, meta.base_generation)
    }

    /// Removes every base generation of `name`, and forgets an open one.
    pub(super) async fn remove_bases(&self, name: &str) {
        if let Some((_, (_, open))) = self.bases.remove(name) {
            drop(open);
        }
        let Some(root) = self.root.as_deref() else {
            return;
        };
        let dir = Path::new(root).join(BASES_DIR);
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return;
        };
        let prefix = format!("{name}.");
        for e in entries.flatten() {
            let entry_name = e.file_name().to_string_lossy().into_owned();
            if entry_name.strip_prefix(&prefix).is_some_and(|rest| {
                !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()) || rest == "building"
            }) {
                forget_directory(&e.path());
                released(&e.path()).await;
                remove_quietly(&e.path());
            }
        }
    }

    /// Every branch, in tree order: each after its parent, siblings by name
    /// (ordered by [`BranchListing::path`]).
    pub fn branches(&self) -> Vec<BranchListing> {
        let mut metas: HashMap<String, BranchMeta> = HashMap::new();
        let mut sessions: HashMap<String, u64> = HashMap::new();
        for entry in self.databases.iter() {
            let Some(dir) = self.directory_of(entry.key()) else {
                continue;
            };
            if let Some(meta) = BranchMeta::read(&dir) {
                sessions.insert(
                    entry.key().clone(),
                    entry.sessions.load(Ordering::Acquire) as u64,
                );
                metas.insert(entry.key().clone(), meta);
            }
        }
        let mut out = Vec::new();
        for (name, meta) in &metas {
            let base_bytes = self
                .base_path(name, meta.base_generation)
                .filter(|d| meta.has_base && d.is_dir())
                .map(|d| dir_bytes(&d));
            let (ancestors, _) = ancestry(name, &metas);
            let mut path: Vec<String> = ancestors.iter().rev().cloned().collect();
            path.push(name.clone());
            out.push(BranchListing {
                name: name.clone(),
                parent_exists: self.databases.contains_key(&meta.parent),
                sessions: sessions.get(name).copied().unwrap_or(0),
                base_bytes,
                depth: ancestors
                    .iter()
                    .filter(|a| metas.contains_key(*a))
                    .count() as u64
                    + 1,
                path: path.join("/"),
                children: metas.values().filter(|m| &m.parent == name).count() as u64,
                meta: meta.clone(),
            });
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        out
    }

    /// `dropped` is going: each branch made from it is re-parented to
    /// `dropped`'s own parent, when `dropped` was a branch, keeping its base
    /// (docs/branching.md §8). Returns `(child, new parent)` per child; a
    /// child of a database that was not a branch keeps naming it, and can no
    /// longer be merged. Run while `dropped`'s directory still exists.
    pub(super) fn reparent_children(&self, dropped: &str) -> Vec<(String, Option<String>)> {
        let Some(dropped_dir) = self.directory_of(dropped) else {
            return Vec::new();
        };
        let grandparent = BranchMeta::read(&dropped_dir).map(|m| m.parent);
        let mut out = Vec::new();
        for entry in self.databases.iter() {
            if entry.key() == dropped {
                continue;
            }
            let Some(dir) = self.directory_of(entry.key()) else {
                continue;
            };
            let Some(mut meta) = BranchMeta::read(&dir) else {
                continue;
            };
            if meta.parent != dropped {
                continue;
            }
            if let Some(grandparent) = &grandparent {
                meta.via.push(dropped.to_string());
                meta.parent = grandparent.clone();
                if let Err(e) = meta.write(&dir) {
                    warn!(
                        "branch \"{}\" could not be re-parented from \"{}\" to \"{}\": {}",
                        entry.key(),
                        dropped,
                        grandparent,
                        e
                    );
                    continue;
                }
                info!(
                    "Branch \"{}\" re-parented from \"{}\" to \"{}\"",
                    entry.key(),
                    dropped,
                    grandparent
                );
            }
            out.push((entry.key().clone(), grandparent.clone()));
        }
        out.sort();
        out
    }

    /// Removes what an interrupted branch operation left in the root: staging
    /// and retiring directories, and bases whose branch is gone. Run at open.
    pub(super) fn sweep_branch_leftovers(&self) {
        let Ok(root) = self.root_dir() else { return };
        if let Some(mut intent) = PromoteIntent::load(root) {
            warn!(
                "finishing PROMOTE BRANCH {} over {}, which was interrupted at step {}",
                intent.branch, intent.parent, intent.step
            );
            if let Err(e) = finish_promote(root, &mut intent) {
                log::error!(
                    "could not finish the interrupted PROMOTE BRANCH {} ({}); {} and {} are left as \
                     they are, with {} describing it",
                    intent.branch,
                    e,
                    intent.parent,
                    intent.branch,
                    root.join(PROMOTE_INTENT).display()
                );
            }
        }
        if let Ok(entries) = std::fs::read_dir(root) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                // A restore stages beside its target as `.<target>.restore-partial`.
                if name.starts_with(STAGING_PREFIX)
                    || name.starts_with(PITR_PREFIX)
                    || name.starts_with(&format!(".{PITR_PREFIX}"))
                {
                    warn!(
                        "removing {}: a branch interrupted while it was being built",
                        e.path().display()
                    );
                    remove_quietly(&e.path());
                } else if let Some(original) = name.strip_prefix(RETIRING_PREFIX) {
                    // A RESET that stopped half way. If the name it was
                    // replacing is gone, the retired copy is the only one:
                    // put it back rather than lose it.
                    let at = root.join(original);
                    if !at.exists() {
                        warn!(
                            "restoring {} from {}: an interrupted branch swap",
                            at.display(),
                            e.path().display()
                        );
                        let _ = std::fs::rename(e.path(), &at);
                    } else {
                        // The new copy is in place: this is what it
                        // replaced (a `RESET` only ever moves the old copy
                        // here and the new one into the name).
                        remove_quietly(&e.path());
                    }
                } else if name.starts_with(PROMOTING_PREFIX) {
                    // Only a PROMOTE's intent says where this belongs; one
                    // with no intent is kept rather than guessed at.
                    warn!(
                        "{} is a database set aside by a PROMOTE BRANCH with no record of where it \
                         belongs; it was kept. Rename it to the name it should have, or remove it.",
                        e.path().display()
                    );
                }
            }
        }
        if let Ok(entries) = std::fs::read_dir(root.join(BASES_DIR)) {
            for e in entries.flatten() {
                let entry_name = e.file_name().to_string_lossy().into_owned();
                // `<branch>.<generation>`: kept only while it is the branch's
                // current base. A merge that stopped part way leaves the next
                // generation, never switched to.
                let current = entry_name
                    .rsplit_once('.')
                    .is_some_and(|(branch, generation)| {
                        BranchMeta::read(&root.join(branch)).is_some_and(|m| {
                            m.has_base && generation == m.base_generation.to_string()
                        })
                    });
                if !current {
                    remove_quietly(&e.path());
                }
            }
        }
    }

    /// `CREATE BRANCH name FROM source`: a new database holding `source` as of
    /// now (copied live) or a past point (restored from the S3 backup).
    pub async fn create_branch(
        &self,
        name: &str,
        source: &str,
        options: &BranchOptions,
    ) -> Result<(Arc<Database>, BranchMeta), String> {
        validate_name(name)?;
        let root = self.root_dir()?.to_path_buf();
        if self.databases.contains_key(name) {
            return Err(format!("database \"{}\" already exists", name));
        }
        if !self.databases.contains_key(source) {
            return Err(format!("database \"{}\" does not exist", source));
        }
        let final_dir = root.join(name);
        if final_dir.exists() {
            return Err(format!(
                "cannot create branch \"{name}\": {} already exists and is not a database this \
                 catalogue serves; move it aside first",
                final_dir.display()
            ));
        }
        let staging = root.join(format!("{STAGING_PREFIX}{name}"));
        // Made with `create_dir`, not `create_dir_all`: it is the claim on the
        // name, so a concurrent branch of the same name fails here.
        std::fs::create_dir(&staging).map_err(|e| {
            if e.kind() == io::ErrorKind::AlreadyExists {
                format!("branch \"{name}\" is already being created")
            } else {
                format!("could not create branch \"{name}\": {e}")
            }
        })?;
        let guard = StagingGuard(staging.clone());

        let meta = self
            .build_branch(name, source, options, &staging, &final_dir, 0)
            .await
            .map_err(|e| format!("could not create branch \"{name}\": {e}"))?;

        if self.databases.contains_key(name) {
            return Err(format!("database \"{}\" already exists", name));
        }
        std::fs::rename(&staging, &final_dir)
            .map_err(|e| format!("could not publish branch \"{name}\": {e}"))?;
        std::mem::forget(guard);
        let _ = crate::core::wal::sync_parent_dir(&final_dir);

        let database =
            match Database::try_new_named(&final_dir.to_string_lossy(), false, name).await {
                Ok(d) => Arc::new(d),
                Err(e) => {
                    forget_directory(&final_dir);
                    remove_quietly(&final_dir);
                    self.remove_bases(name).await;
                    return Err(format!(
                        "branch \"{name}\" was copied but would not open: {e}"
                    ));
                }
            };
        self.databases
            .insert(name.to_string(), Entry::new(database.clone()));
        info!(
            "Created branch \"{}\" of \"{}\" ({}, {}, {})",
            name,
            source,
            meta.point,
            meta.method,
            human(meta.bytes)
        );
        Ok((database, meta))
    }

    /// Builds a branch of `source` into `staging` (published as `final_dir`),
    /// and its base when asked for one.
    async fn build_branch(
        &self,
        name: &str,
        source: &str,
        options: &BranchOptions,
        staging: &Path,
        final_dir: &Path,
        base_generation: u64,
    ) -> io::Result<BranchMeta> {
        let source_dir = self
            .directory_of(source)
            .ok_or_else(|| io::Error::other("the catalogue is in memory"))?;
        let space_factor = if options.with_base { 2 } else { 1 };
        let (lsn, method, bytes) = match &options.point {
            BranchPoint::Now => {
                let (lsn, stats) = copy_live(&source_dir, staging, final_dir, space_factor).await?;
                (lsn, stats.method().to_string(), stats.bytes)
            }
            point => {
                let lsn = self
                    .restore_point(source, point, staging, final_dir)
                    .await?;
                (lsn, "restore".to_string(), dir_bytes(staging))
            }
        };
        let meta = BranchMeta {
            parent: source.to_string(),
            created_at: now_unix(),
            parent_lsn: lsn,
            method,
            point: options.point.describe(),
            has_base: options.with_base,
            bytes,
            merged_at: None,
            base_generation,
            via: Vec::new(),
            id: fresh_id(name),
        };
        if options.with_base {
            self.write_base(name, staging, final_dir, base_generation)
                .await?;
        }
        meta.write(staging)?;
        Ok(meta)
    }

    /// Copies a built (closed) branch directory into base generation
    /// `generation`.
    async fn write_base(
        &self,
        name: &str,
        built: &Path,
        built_for: &Path,
        generation: u64,
    ) -> io::Result<()> {
        let base = self
            .base_path(name, generation)
            .ok_or_else(|| io::Error::other("the catalogue is in memory"))?;
        let building = base.with_file_name(format!("{name}.building"));
        remove_quietly(&building);
        if let Some(parent) = base.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let (from, to) = (built.to_path_buf(), building.clone());
        tokio::task::spawn_blocking(move || {
            let mut stats = CopyStats::default();
            copy_tree(&from, &to, &mut stats)
        })
        .await
        .map_err(io::Error::other)??;
        let _ = std::fs::remove_file(building.join(META_FILE));
        repoint(&building, &base, built_for, &[])?;
        remove_quietly(&base);
        std::fs::rename(&building, &base)?;
        Ok(())
    }

    /// Restores `source` as of `point` from the S3 backup into `staging`.
    #[cfg(feature = "s3_backup")]
    async fn restore_point(
        &self,
        source: &str,
        point: &BranchPoint,
        staging: &Path,
        final_dir: &Path,
    ) -> io::Result<u64> {
        use crate::core::backup::manifest::RecoveryTarget;
        let root = Path::new(self.root.as_deref().unwrap_or(""));
        let config = backup_config_for(root)?;
        let target = match point {
            BranchPoint::Lsn(n) => RecoveryTarget::Lsn(*n),
            BranchPoint::Time(t) => RecoveryTarget::Time(*t),
            BranchPoint::Snapshot(id) => RecoveryTarget::Snapshot(id.clone()),
            BranchPoint::Now => RecoveryTarget::Latest,
        };
        let name = final_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let restored = root.join(format!("{PITR_PREFIX}{name}"));
        remove_quietly(&restored);
        let _cleanup = StagingGuard(restored.clone());
        let report = crate::core::backup::restore_from(&config, &restored, target).await?;

        // The restore holds every database; take out the one asked for.
        let db_in_restore = if source == self.default_name {
            restored.clone()
        } else {
            restored.join(source)
        };
        if !db_in_restore.join(DATABASE_MARKER_FILE).is_file() {
            return Err(io::Error::other(format!(
                "database \"{source}\" is not in the backup at that point"
            )));
        }
        // Its own files, flat (the rest of the restore is other databases).
        for entry in std::fs::read_dir(&db_in_restore)?.flatten() {
            if entry.file_type()?.is_file() {
                std::fs::rename(entry.path(), staging.join(entry.file_name()))?;
            }
        }
        // Its storage locations, wherever the restore put them.
        let mut moved: Vec<(String, String)> = Vec::new();
        #[cfg(feature = "storage_pools")]
        for path in crate::core::storage::data_locations(staging)? {
            let p = PathBuf::from(&path);
            if !p.starts_with(&restored) || !p.is_dir() {
                continue;
            }
            let dir_name = location_dir_name(&path);
            std::fs::rename(&p, staging.join(&dir_name))?;
            moved.push((
                path,
                final_dir.join(&dir_name).to_string_lossy().into_owned(),
            ));
        }
        // The pool file names the database where the restore put it.
        #[cfg(feature = "storage_pools")]
        {
            crate::core::storage::repoint_restored_pool(
                staging,
                final_dir,
                Some(&db_in_restore.to_string_lossy()),
                &moved,
            )?;
        }
        let _ = moved;
        let _ = std::fs::remove_file(staging.join("RESTORED.json"));
        for name in std::fs::read_dir(staging)?
            .flatten()
            .filter_map(|e| e.file_name().to_str().map(str::to_string))
            .filter(|n| excluded(n))
            .collect::<Vec<_>>()
        {
            let _ = std::fs::remove_file(staging.join(name));
        }
        Ok(report.recovered_to_lsn.unwrap_or(0))
    }

    #[cfg(not(feature = "s3_backup"))]
    #[allow(clippy::unused_async)]
    async fn restore_point(
        &self,
        _source: &str,
        _point: &BranchPoint,
        _staging: &Path,
        _final_dir: &Path,
    ) -> io::Result<u64> {
        Err(io::Error::other(
            "a branch at a past point is restored from the S3 backup, which this build does not \
             include (feature s3_backup)",
        ))
    }

    /// Takes `name` out of the catalogue for a swap: refused while anyone is
    /// attached, or when it is the default database or the caller's.
    fn detach(&self, name: &str, current: &str, verb: &str) -> Result<Arc<Database>, String> {
        if name == self.default_name {
            return Err(format!(
                "cannot {verb} the default database \"{name}\"; it is the catalogue root"
            ));
        }
        if name == current {
            return Err(format!(
                "cannot {verb} \"{name}\" from a session connected to it; connect to another database"
            ));
        }
        let entry = self
            .databases
            .get(name)
            .ok_or_else(|| format!("database \"{}\" does not exist", name))?;
        let attached = entry.sessions.load(Ordering::Acquire);
        if attached > 0 {
            return Err(format!(
                "database \"{}\" is being accessed by {} other session(s)",
                name, attached
            ));
        }
        let database = entry.database.clone();
        drop(entry);
        self.databases.remove(name);
        crate::core::rql::sequence::forget_database(database.id);
        Ok(database)
    }

    /// Opens the database at `name`'s directory and serves it again.
    async fn attach(&self, name: &str) -> Result<Arc<Database>, String> {
        let dir = self
            .directory_of(name)
            .ok_or_else(|| "the catalogue is in memory".to_string())?;
        let database =
            Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, name).await?);
        self.databases
            .insert(name.to_string(), Entry::new(database.clone()));
        Ok(database)
    }

    /// `RESET BRANCH name`: copies the parent again, as of now, in place of
    /// the branch. Everything written on the branch is discarded.
    pub async fn reset_branch(&self, name: &str, current: &str) -> Result<BranchMeta, String> {
        let root = self.root_dir()?.to_path_buf();
        let meta = self
            .branch_meta(name)
            .ok_or_else(|| format!("\"{name}\" is not a branch"))?;
        if !self.databases.contains_key(&meta.parent) {
            return Err(format!(
                "cannot reset branch \"{name}\": its parent \"{}\" no longer exists",
                meta.parent
            ));
        }
        // Refused before the copy, not after it.
        {
            let entry = self.databases.get(name).ok_or("branch vanished")?;
            let attached = entry.sessions.load(Ordering::Acquire);
            if name == current || attached > 0 {
                return Err(format!(
                    "cannot reset branch \"{name}\" while sessions are connected to it"
                ));
            }
        }
        let final_dir = root.join(name);
        let staging = root.join(format!("{STAGING_PREFIX}{name}"));
        std::fs::create_dir(&staging)
            .map_err(|e| format!("could not reset branch \"{name}\": {e}"))?;
        let guard = StagingGuard(staging.clone());
        let options = BranchOptions {
            point: BranchPoint::Now,
            with_base: meta.has_base,
        };
        let generation = meta.base_generation + 1;
        let fresh = match self
            .build_branch(
                name,
                &meta.parent,
                &options,
                &staging,
                &final_dir,
                generation,
            )
            .await
        {
            Ok(m) => m,
            Err(e) => {
                if let Some(next) = self.base_path(name, generation) {
                    remove_quietly(&next);
                }
                return Err(format!("could not reset branch \"{name}\": {e}"));
            }
        };

        let old = self.detach(name, current, "reset")?;
        drop(old);
        forget_directory(&final_dir);
        let retiring = root.join(format!("{RETIRING_PREFIX}{name}"));
        remove_quietly(&retiring);
        std::fs::rename(&final_dir, &retiring).map_err(|e| e.to_string())?;
        std::fs::rename(&staging, &final_dir).map_err(|e| e.to_string())?;
        std::mem::forget(guard);
        match self.attach(name).await {
            Ok(_) => {
                released(&retiring).await;
                remove_quietly(&retiring);
                // The old base generation goes with what it described.
                if let Some((_, (_, open))) = self.bases.remove(name) {
                    drop(open);
                }
                if let Some(old) = self.base_path(name, meta.base_generation) {
                    forget_directory(&old);
                    released(&old).await;
                    remove_quietly(&old);
                }
                info!("Reset branch \"{}\" from \"{}\"", name, meta.parent);
                Ok(fresh)
            }
            Err(e) => {
                // Put the old branch back rather than leave the name empty.
                forget_directory(&final_dir);
                remove_quietly(&final_dir);
                let _ = std::fs::rename(&retiring, &final_dir);
                let _ = self.attach(name).await;
                Err(format!(
                    "branch \"{name}\" was copied again but would not open ({e}); it was left as it was"
                ))
            }
        }
    }

    /// `PROMOTE BRANCH name`: swaps the branch and its parent by name. The
    /// parent's old contents stay, as a branch under the old branch's name.
    pub async fn promote_branch(&self, name: &str, current: &str) -> Result<String, String> {
        let root = self.root_dir()?.to_path_buf();
        let meta = self
            .branch_meta(name)
            .ok_or_else(|| format!("\"{name}\" is not a branch"))?;
        let parent = meta.parent.clone();
        if !self.databases.contains_key(&parent) {
            return Err(format!(
                "cannot promote branch \"{name}\": its parent \"{parent}\" no longer exists"
            ));
        }
        if parent == self.default_name {
            return Err(format!(
                "cannot promote over the default database \"{parent}\": it is the catalogue root and \
                 cannot be swapped while the server runs"
            ));
        }
        // Both checked before either is detached.
        for n in [name, parent.as_str()] {
            let entry = self.databases.get(n).ok_or("database vanished")?;
            if n == current || entry.sessions.load(Ordering::Acquire) > 0 {
                return Err(format!(
                    "cannot promote branch \"{name}\" while sessions are connected to \"{n}\""
                ));
            }
        }
        let branch_db = self.detach(name, current, "promote")?;
        let parent_db = match self.detach(&parent, current, "promote") {
            Ok(d) => d,
            Err(e) => {
                drop(branch_db);
                let _ = self.attach(name).await;
                return Err(e);
            }
        };
        drop(branch_db);
        drop(parent_db);

        let branch_dir = root.join(name);
        let parent_dir = root.join(&parent);
        forget_directory(&branch_dir);
        forget_directory(&parent_dir);
        let mut intent = PromoteIntent {
            parent: parent.clone(),
            branch: name.to_string(),
            step: 0,
            parent_meta: BranchMeta::read(&parent_dir),
            // The old parent becomes a branch of the new one, which `DROP
            // BRANCH` removes once it is not wanted.
            branch_meta: BranchMeta {
                parent: parent.clone(),
                created_at: now_unix(),
                parent_lsn: 0,
                method: "promote".to_string(),
                point: "replaced by promote".to_string(),
                has_base: false,
                bytes: dir_bytes(&parent_dir),
                merged_at: None,
                base_generation: 0,
                via: Vec::new(),
                id: fresh_id(name),
            },
        };
        let swapped = intent
            .store(&root)
            .and_then(|_| finish_promote(&root, &mut intent));
        self.remove_bases(name).await;
        let reopened = (self.attach(&parent).await, self.attach(name).await);
        if let Err(e) = swapped {
            return Err(format!(
                "promote of \"{name}\" stopped part way ({e}); check both databases"
            ));
        }
        match reopened {
            (Ok(_), Ok(_)) => {
                info!("Promoted branch \"{}\" over \"{}\"", name, parent);
                Ok(parent)
            }
            (a, b) => Err(format!(
                "promoted, but reopening failed: {}",
                [a.err(), b.err()]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join("; ")
            )),
        }
    }

    /// `DROP BRANCH name`: `DROP DATABASE`, refused for a database that is not
    /// a branch, so a typo cannot drop one.
    pub async fn drop_branch(&self, name: &str, current: &str) -> Result<(), String> {
        if self.branch_meta(name).is_none() {
            return Err(format!(
                "\"{name}\" is not a branch; DROP DATABASE drops an ordinary database"
            ));
        }
        self.drop_database(name, current).await
    }

    /// Copies the branch as it is now into its next base generation, and
    /// opens it: the side of the branch a merge reads, which becomes the base
    /// once the merge commits ([`DatabaseCatalog::adopt_base`]).
    pub async fn next_base(&self, name: &str) -> Result<(u64, Arc<Database>), String> {
        let dir = self
            .directory_of(name)
            .ok_or("the catalogue is in memory")?;
        let meta = BranchMeta::read(&dir).ok_or_else(|| format!("\"{name}\" is not a branch"))?;
        let generation = meta.base_generation + 1;
        let next = self
            .base_path(name, generation)
            .ok_or("the catalogue is in memory")?;
        forget_directory(&next);
        remove_quietly(&next);
        let staging = next.with_file_name(format!("{name}.building"));
        remove_quietly(&staging);
        std::fs::create_dir_all(&staging).map_err(|e| e.to_string())?;
        let guard = StagingGuard(staging.clone());
        copy_live(&dir, &staging, &next, 1)
            .await
            .map_err(|e| format!("could not copy branch \"{name}\" for the merge: {e}"))?;
        std::fs::rename(&staging, &next).map_err(|e| e.to_string())?;
        std::mem::forget(guard);
        let database = Database::try_new_named(
            &next.to_string_lossy(),
            false,
            &format!("{name}__base{generation}"),
        )
        .await
        .map_err(|e| format!("could not open the copy of branch \"{name}\": {e}"))?;
        Ok((generation, Arc::new(database)))
    }

    /// Makes generation `generation` (from [`DatabaseCatalog::next_base`])
    /// the branch's base after a merge committed, and records the merge. The
    /// DDL the merge replayed — the first `ddl_replayed` lines — is dropped
    /// from the branch's log.
    pub async fn adopt_base(
        &self,
        name: &str,
        generation: u64,
        database: Arc<Database>,
        ddl_replayed: usize,
    ) -> Result<(), String> {
        let dir = self
            .directory_of(name)
            .ok_or("the catalogue is in memory")?;
        let mut meta =
            BranchMeta::read(&dir).ok_or_else(|| format!("\"{name}\" is not a branch"))?;
        let old = meta.base_generation;
        meta.base_generation = generation;
        meta.merged_at = Some(now_unix());
        meta.write(&dir).map_err(|e| e.to_string())?;
        trim_ddl(&dir, ddl_replayed).map_err(|e| e.to_string())?;
        if let Some((_, open)) = self.bases.insert(name.to_string(), (generation, database)) {
            drop(open);
        }
        if let Some(old) = self.base_path(name, old) {
            forget_directory(&old);
            released(&old).await;
            remove_quietly(&old);
        }
        Ok(())
    }

    /// Drops a next generation a merge did not commit.
    pub async fn discard_base(&self, name: &str, generation: u64) {
        if let Some(next) = self.base_path(name, generation) {
            forget_directory(&next);
            released(&next).await;
            remove_quietly(&next);
        }
    }

    /// `name`'s current merge base, opened once and kept open: a database
    /// that is opened starts services that are never stopped, so a diff that
    /// opened it every time would leak them.
    pub async fn open_base(&self, name: &str) -> Result<Arc<Database>, String> {
        let meta = self
            .branch_meta(name)
            .ok_or_else(|| format!("\"{name}\" is not a branch"))?;
        if let Some(open) = self.bases.get(name)
            && open.0 == meta.base_generation
        {
            return Ok(open.1.clone());
        }
        let dir = self
            .base_path(name, meta.base_generation)
            .ok_or("the catalogue is in memory")?;
        if !meta.has_base || !dir.join(DATABASE_MARKER_FILE).is_file() {
            return Err(format!(
                "branch \"{name}\" has no merge base (made WITHOUT BASE, promoted, or its base was \
                 lost), so it cannot be merged; RESET BRANCH gives it a new one"
            ));
        }
        let database = Arc::new(
            Database::try_new_named(
                &dir.to_string_lossy(),
                false,
                &format!("{name}__base{}", meta.base_generation),
            )
            .await
            .map_err(|e| format!("could not open the merge base of \"{name}\": {e}"))?,
        );
        self.bases
            .insert(name.to_string(), (meta.base_generation, database.clone()));
        Ok(database)
    }

    /// The schema changes recorded on `name` since its base.
    pub fn branch_ddl(&self, name: &str) -> Vec<String> {
        self.directory_of(name)
            .map(|dir| read_ddl(&dir))
            .unwrap_or_default()
    }
}

/// `name`'s ancestors, nearest first, through the branches in `metas`: each
/// branch's parent, until one that is not a branch (included, it is the
/// tree's root) — and whether the walk stopped at a cycle, which only a
/// damaged metadata file can make.
fn ancestry(name: &str, metas: &HashMap<String, BranchMeta>) -> (Vec<String>, bool) {
    let mut out: Vec<String> = Vec::new();
    let mut at = name;
    while let Some(meta) = metas.get(at) {
        if meta.parent == name || out.contains(&meta.parent) {
            return (out, true);
        }
        out.push(meta.parent.clone());
        at = &meta.parent;
    }
    (out, false)
}

/// Every branch directory under `root` whose metadata names `from` as its
/// parent, other than those in `except`, now names `to`: what a `PROMOTE`
/// does for the promoted branch's own branches, which follow its data to
/// its new name. Idempotent, so a promote finished after a crash runs it
/// again safely.
fn repoint_children(root: &Path, from: &str, to: &str, except: &[&Path]) -> io::Result<()> {
    for entry in std::fs::read_dir(root)?.flatten() {
        let dir = entry.path();
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
            || entry.file_name().to_string_lossy().starts_with('.')
            || except.iter().any(|e| *e == dir.as_path())
        {
            continue;
        }
        if let Some(mut meta) = BranchMeta::read(&dir)
            && meta.parent == from
        {
            meta.parent = to.to_string();
            meta.write(&dir)?;
        }
    }
    Ok(())
}

// ---- the DDL a branch records -------------------------------------------------

/// DDL run inside a block, kept until the block ends: `(directory, block)`.
static PENDING_DDL: std::sync::LazyLock<parking_lot::Mutex<HashMap<(String, u64), Vec<String>>>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

/// Whether `stmt` changes a schema a merge must carry: `CREATE`, `ALTER`,
/// `DROP` and `COMMENT` of the database's own objects. Not of databases,
/// branches or roles (above the database), nor of temporary tables (the
/// session's).
pub fn is_recorded_ddl(stmt: &str) -> bool {
    let mut words = stmt.split_whitespace();
    let (Some(verb), second) = (words.next(), words.next()) else {
        return false;
    };
    let verb = verb.to_ascii_uppercase();
    if !matches!(verb.as_str(), "CREATE" | "ALTER" | "DROP" | "COMMENT") {
        return false;
    }
    let second = second.unwrap_or("").to_ascii_uppercase();
    // A merge's record of renumbered keys is the parent's own, never a
    // schema change to carry further up.
    if stmt.contains(crate::pgwire::KEYMAP_TABLE_NAME) {
        return false;
    }
    !matches!(
        second.as_str(),
        "DATABASE" | "BRANCH" | "ROLE" | "USER" | "TEMP" | "TEMPORARY" | "GROUP"
    )
}

/// Records `stmt`, which ran successfully on the database at `dir`, if that
/// database is a branch. `block` is the open transaction block's id: the
/// statement is then kept until the block ends ([`block_ended`]).
pub fn note_ddl(dir: &str, block: Option<u64>, stmt: &str) {
    if !is_recorded_ddl(stmt) || !Path::new(dir).join(META_FILE).is_file() {
        return;
    }
    match block {
        Some(id) => PENDING_DDL
            .lock()
            .entry((crate::core::wal::dir_key(dir), id))
            .or_default()
            .push(stmt.to_string()),
        None => append_ddl(Path::new(dir), &[stmt.to_string()]),
    }
}

/// A block of the database at `dir` ended: its DDL is recorded if it
/// committed, and forgotten if not.
pub fn block_ended(dir: &str, block: u64, committed: bool) {
    let taken = PENDING_DDL
        .lock()
        .remove(&(crate::core::wal::dir_key(dir), block));
    if let (Some(statements), true) = (taken, committed) {
        append_ddl(Path::new(dir), &statements);
    }
}

fn append_ddl(dir: &Path, statements: &[String]) {
    use std::io::Write;
    let path = dir.join(DDL_FILE);
    let result = (|| -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let mut text = String::new();
        for s in statements {
            text.push_str(&serde_json::to_string(s).map_err(io::Error::other)?);
            text.push('\n');
        }
        file.write_all(text.as_bytes())?;
        file.sync_data()
    })();
    if let Err(e) = result {
        // A merge then finds the schema changed with no record of how, and
        // refuses, rather than merging without the change.
        log::error!(
            "could not record a branch's schema change in {}: {}",
            path.display(),
            e
        );
    }
}

fn read_ddl(dir: &Path) -> Vec<String> {
    std::fs::read_to_string(dir.join(DDL_FILE))
        .map(|text| {
            text.lines()
                .filter(|l| !l.trim().is_empty())
                .filter_map(|l| serde_json::from_str::<String>(l).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Drops the first `n` statements of the log in `dir`.
fn trim_ddl(dir: &Path, n: usize) -> io::Result<()> {
    if n == 0 {
        return Ok(());
    }
    let rest: Vec<String> = read_ddl(dir).into_iter().skip(n).collect();
    let temp = dir.join(format!("{DDL_FILE}.tmp"));
    let mut text = String::new();
    for s in &rest {
        text.push_str(&serde_json::to_string(s).map_err(io::Error::other)?);
        text.push('\n');
    }
    std::fs::write(&temp, text)?;
    std::fs::rename(&temp, dir.join(DDL_FILE))
}

/// Backups the tests point a catalogue root at, in place of the server's
/// configured one (which is process-wide, and a test installing it would
/// reach every other test's databases).
#[cfg(all(test, feature = "s3_backup"))]
pub(crate) static TEST_BACKUPS: std::sync::LazyLock<
    parking_lot::Mutex<HashMap<String, crate::core::backup::BackupConfig>>,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

/// The S3 backup of the catalogue rooted at `root`.
#[cfg(feature = "s3_backup")]
fn backup_config_for(root: &Path) -> io::Result<crate::core::backup::BackupConfig> {
    #[cfg(test)]
    if let Some(config) = TEST_BACKUPS
        .lock()
        .get(&crate::core::wal::dir_key(&root.to_string_lossy()))
    {
        return Ok(config.clone());
    }
    let service = crate::core::backup::service().ok_or_else(|| {
        io::Error::other(
            "a branch at a past point is restored from the S3 backup, and this server has none \
             configured",
        )
    })?;
    if crate::core::storage::normalize(service.location())
        != crate::core::storage::normalize(&root.to_string_lossy())
    {
        return Err(io::Error::other(format!(
            "the S3 backup covers {}, not this catalogue's root {}",
            service.location(),
            root.display()
        )));
    }
    Ok(service.config().clone())
}

/// A `PROMOTE BRANCH` in progress: written before the first rename and
/// advanced after each, so a crash anywhere is finished at the next start
/// rather than left as two databases under each other's names.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PromoteIntent {
    parent: String,
    branch: String,
    /// Renames done: 1 the parent aside, 2 the branch into its name, 3 the
    /// parent into the branch's.
    step: u8,
    /// The parent's own lineage, which the promoted branch takes over.
    parent_meta: Option<BranchMeta>,
    /// What the old parent records, as a branch of the new one.
    branch_meta: BranchMeta,
}

impl PromoteIntent {
    fn store(&self, root: &Path) -> io::Result<()> {
        let text = serde_json::to_string(self).map_err(io::Error::other)?;
        let temp = root.join(format!("{PROMOTE_INTENT}.tmp"));
        std::fs::write(&temp, text)?;
        std::fs::File::open(&temp)?.sync_all()?;
        std::fs::rename(&temp, root.join(PROMOTE_INTENT))?;
        crate::core::wal::sync_parent_dir(&root.join(PROMOTE_INTENT))
    }

    fn load(root: &Path) -> Option<PromoteIntent> {
        serde_json::from_str(&std::fs::read_to_string(root.join(PROMOTE_INTENT)).ok()?).ok()
    }
}

/// Carries a `PROMOTE` from wherever it stopped to the end. Every step is
/// idempotent or guarded by the recorded step and what is on disk, so it can
/// be run again after a crash in any of them.
fn finish_promote(root: &Path, intent: &mut PromoteIntent) -> io::Result<()> {
    let parent_dir = root.join(&intent.parent);
    let branch_dir = root.join(&intent.branch);
    let aside = root.join(format!("{PROMOTING_PREFIX}{}", intent.parent));
    // A rename done but not yet recorded is seen on disk.
    if intent.step == 0 && aside.exists() && !parent_dir.exists() {
        intent.step = 1;
    }
    if intent.step == 1 && !branch_dir.exists() && parent_dir.exists() {
        intent.step = 2;
    }
    if intent.step == 2 && !aside.exists() && branch_dir.exists() {
        intent.step = 3;
    }
    if intent.step == 0 {
        std::fs::rename(&parent_dir, &aside)?;
        intent.step = 1;
        intent.store(root)?;
    }
    if intent.step == 1 {
        std::fs::rename(&branch_dir, &parent_dir)?;
        intent.step = 2;
        intent.store(root)?;
    }
    if intent.step == 2 {
        std::fs::rename(&aside, &branch_dir)?;
        intent.step = 3;
        intent.store(root)?;
    }
    // Each pool file names the directory it was written for; each now names
    // its own.
    repoint_to_itself(&parent_dir)?;
    repoint_to_itself(&branch_dir)?;
    match &intent.parent_meta {
        Some(m) => m.write(&parent_dir)?,
        None => match std::fs::remove_file(parent_dir.join(META_FILE)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => {}
        },
    }
    intent.branch_meta.write(&branch_dir)?;
    // The promoted branch's own branches were copied from what is now the
    // parent's name, and follow it there.
    repoint_children(root, &intent.branch, &intent.parent, &[&parent_dir, &branch_dir])?;
    let _ = std::fs::remove_file(branch_dir.join(DDL_FILE));
    let _ = std::fs::remove_file(parent_dir.join(DDL_FILE));
    std::fs::remove_file(root.join(PROMOTE_INTENT))?;
    crate::core::wal::sync_parent_dir(&root.join(PROMOTE_INTENT))
}

/// Rewrites the pool file in `dir` from the directory it was written for to
/// `dir` itself. Nothing when it already names `dir`.
#[cfg(feature = "storage_pools")]
fn repoint_to_itself(dir: &Path) -> io::Result<()> {
    crate::core::storage::repoint_restored_pool(dir, dir, None, &[]).map(|_| ())
}

#[cfg(not(feature = "storage_pools"))]
fn repoint_to_itself(_dir: &Path) -> io::Result<()> {
    Ok(())
}

/// Removes a staging directory unless it was published.
struct StagingGuard(PathBuf);

impl Drop for StagingGuard {
    fn drop(&mut self) {
        remove_quietly(&self.0);
    }
}

// ---- statements -------------------------------------------------------------

/// Parses the branch statements, given the statement upper-cased and as
/// written (for names, whose quoting decides their case). `None` when the
/// statement is not one.
pub(super) fn parse(trimmed: &str, upper: &str) -> Option<Result<DatabaseStatement, String>> {
    let words: Vec<&str> = upper.split_whitespace().collect();
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    let fold = |token: &str| {
        if token.starts_with('"') {
            token.trim_matches('"').to_string()
        } else {
            token.to_ascii_lowercase()
        }
    };
    let name_at = |i: usize| tokens.get(i).map(|t| fold(t));
    match (words.first().copied()?, words.get(1).copied()?) {
        ("SHOW", "BRANCHES") => {
            if words.len() > 2 {
                return Some(Err("SHOW BRANCHES takes no arguments".to_string()));
            }
            Some(Ok(DatabaseStatement::ListBranches))
        }
        ("SHOW", "BRANCH") => {
            if words.get(2) != Some(&"DIFF") {
                return Some(Err("expected SHOW BRANCH DIFF <name>".to_string()));
            }
            let Some(name) = name_at(3) else {
                return Some(Err("SHOW BRANCH DIFF names no branch".to_string()));
            };
            let into = match words.get(4) {
                None => None,
                Some(&"INTO") => match name_at(5) {
                    Some(n) if words.len() == 6 => Some(n),
                    _ => {
                        return Some(Err("SHOW BRANCH DIFF … INTO names one database".to_string()));
                    }
                },
                Some(other) => {
                    return Some(Err(format!("unexpected {other:?} in SHOW BRANCH DIFF")));
                }
            };
            Some(Ok(DatabaseStatement::DiffBranch { name, into }))
        }
        ("CREATE", "BRANCH") => Some(parse_create(&words, &tokens, &name_at)),
        ("DROP", "BRANCH") => {
            let if_exists = words.get(2) == Some(&"IF") && words.get(3) == Some(&"EXISTS");
            let at = if if_exists { 4 } else { 2 };
            let Some(name) = name_at(at) else {
                return Some(Err("DROP BRANCH names no branch".to_string()));
            };
            if words.len() > at + 1 {
                return Some(Err(format!(
                    "unexpected {:?} after DROP BRANCH",
                    tokens[at + 1]
                )));
            }
            Some(Ok(DatabaseStatement::DropBranch { name, if_exists }))
        }
        ("RESET", "BRANCH") => {
            let Some(name) = name_at(2) else {
                return Some(Err("RESET BRANCH names no branch".to_string()));
            };
            if words.len() > 3 {
                return Some(Err(format!(
                    "unexpected {:?} after RESET BRANCH",
                    tokens[3]
                )));
            }
            Some(Ok(DatabaseStatement::ResetBranch { name }))
        }
        ("PROMOTE", "BRANCH") => {
            let Some(name) = name_at(2) else {
                return Some(Err("PROMOTE BRANCH names no branch".to_string()));
            };
            if words.len() > 3 {
                return Some(Err(format!(
                    "unexpected {:?} after PROMOTE BRANCH",
                    tokens[3]
                )));
            }
            Some(Ok(DatabaseStatement::PromoteBranch { name }))
        }
        // T-SQL's `MERGE target USING …` needs no `INTO`: a table named
        // `branch` merged that way is the engine's MERGE, not this.
        ("MERGE", "BRANCH") if words.contains(&"USING") => None,
        ("MERGE", "BRANCH") => Some(parse_merge(&words, &name_at)),
        _ => None,
    }
}

/// `CREATE BRANCH name [FROM db] [AS OF TIMESTAMP '…' | LSN n | SNAPSHOT '…']
/// [WITHOUT BASE]`.
fn parse_create(
    words: &[&str],
    tokens: &[&str],
    name_at: &dyn Fn(usize) -> Option<String>,
) -> Result<DatabaseStatement, String> {
    let Some(name) = name_at(2) else {
        return Err("CREATE BRANCH names no branch".to_string());
    };
    let mut from = None;
    let mut options = BranchOptions::default();
    let mut i = 3;
    while i < words.len() {
        match words[i] {
            "FROM" => {
                from = Some(name_at(i + 1).ok_or("FROM names no database")?);
                i += 2;
            }
            "WITHOUT" if words.get(i + 1) == Some(&"BASE") => {
                options.with_base = false;
                i += 2;
            }
            "AS" if words.get(i + 1) == Some(&"OF") => {
                let kind = words.get(i + 2).copied().unwrap_or("");
                // The value as written: a quoted timestamp may hold spaces.
                let rest: Vec<&str> = tokens[(i + 3).min(tokens.len())..].to_vec();
                let (value, used) =
                    quoted_or_word(&rest).ok_or_else(|| format!("AS OF {kind} needs a value"))?;
                options.point = match kind {
                    "LSN" => BranchPoint::Lsn(
                        value
                            .parse()
                            .map_err(|_| format!("not a log sequence number: {value:?}"))?,
                    ),
                    "TIMESTAMP" | "TIME" => BranchPoint::Time(
                        crate::core::backup::timeutil::parse_rfc3339(&value)
                            .ok_or_else(|| format!("not a timestamp: {value:?}"))?,
                    ),
                    "SNAPSHOT" => BranchPoint::Snapshot(value),
                    _ => return Err("expected AS OF TIMESTAMP, LSN or SNAPSHOT".to_string()),
                };
                i += 3 + used;
            }
            other => return Err(format!("unexpected {other:?} in CREATE BRANCH")),
        }
    }
    Ok(DatabaseStatement::CreateBranch {
        name,
        from,
        options,
        template: false,
    })
}

/// The first value in `tokens`: a single-quoted string (which may span
/// several whitespace-separated tokens), or one bare word. Returns it and how
/// many tokens it used.
fn quoted_or_word(tokens: &[&str]) -> Option<(String, usize)> {
    let first = tokens.first()?;
    if !first.starts_with('\'') {
        return Some((first.to_string(), 1));
    }
    let mut text = String::new();
    for (n, t) in tokens.iter().enumerate() {
        if n > 0 {
            text.push(' ');
        }
        text.push_str(t);
        if (n > 0 || t.len() > 1) && t.ends_with('\'') {
            return Some((text[1..text.len() - 1].replace("''", "'"), n + 1));
        }
    }
    None
}

/// `MERGE BRANCH name [INTO db] [ON CONFLICT FAIL | BRANCH WINS | PARENT
/// WINS] [ALLOW DROP]`.
fn parse_merge(
    words: &[&str],
    name_at: &dyn Fn(usize) -> Option<String>,
) -> Result<DatabaseStatement, String> {
    let Some(name) = name_at(2) else {
        return Err("MERGE BRANCH names no branch".to_string());
    };
    let mut into = None;
    let mut policy = ConflictPolicy::Fail;
    let mut allow_drop = false;
    let mut i = 3;
    while i < words.len() {
        match (
            words[i],
            words.get(i + 1).copied(),
            words.get(i + 2).copied(),
        ) {
            ("INTO", Some(_), _) => {
                into = name_at(i + 1);
                i += 2;
            }
            ("ON", Some("CONFLICT"), Some("FAIL")) => {
                policy = ConflictPolicy::Fail;
                i += 3;
            }
            ("ON", Some("CONFLICT"), Some("BRANCH")) if words.get(i + 3) == Some(&"WINS") => {
                policy = ConflictPolicy::BranchWins;
                i += 4;
            }
            ("ON", Some("CONFLICT"), Some("PARENT")) if words.get(i + 3) == Some(&"WINS") => {
                policy = ConflictPolicy::ParentWins;
                i += 4;
            }
            ("ALLOW", Some("DROP"), _) => {
                allow_drop = true;
                i += 2;
            }
            (other, ..) => return Err(format!("unexpected {other:?} in MERGE BRANCH")),
        }
    }
    Ok(DatabaseStatement::MergeBranch {
        name,
        into,
        policy,
        allow_drop,
    })
}

/// What a merge does with a row both sides changed differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// Apply nothing and list the conflicts.
    Fail,
    /// The branch's version is written over the parent's.
    BranchWins,
    /// The parent's version is kept.
    ParentWins,
}

impl ConflictPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            ConflictPolicy::Fail => "fail",
            ConflictPolicy::BranchWins => "branch wins",
            ConflictPolicy::ParentWins => "parent wins",
        }
    }
}

/// `CREATE DATABASE name [WITH] TEMPLATE [=] source`: PostgreSQL's spelling
/// of a copy. `template0`/`template1` name PostgreSQL's empty templates,
/// which do not exist here; a plain create is what they mean.
pub(super) fn template_of(
    words_after_name: &[&str],
    tokens_after_name: &[&str],
) -> Option<Result<Option<String>, String>> {
    let mut i = 0;
    if words_after_name.first() == Some(&"WITH") {
        i += 1;
    }
    if words_after_name.get(i) != Some(&"TEMPLATE") {
        return None;
    }
    i += 1;
    if words_after_name.get(i) == Some(&"=") {
        i += 1;
    }
    let Some(token) = tokens_after_name.get(i) else {
        return Some(Err("TEMPLATE names no database".to_string()));
    };
    if words_after_name.len() > i + 1 {
        return Some(Err(format!(
            "CREATE DATABASE takes no options here but TEMPLATE; unexpected {:?}",
            tokens_after_name[i + 1]
        )));
    }
    let name = if token.starts_with('"') {
        token.trim_matches('"').to_string()
    } else {
        token.to_ascii_lowercase()
    };
    if name == "template0" || name == "template1" {
        return Some(Ok(None));
    }
    Some(Ok(Some(name)))
}

#[cfg(test)]
#[path = "branch_tests.rs"]
mod tests;
