//! A partitioned table's files on disk (§3.1, §3.2, §12): their names, their
//! creation, the never-creating existence check, and the map log's file.
//! No `Table` involvement.
//!
//! # Names
//!
//! Every file of the table with uid `u` starts `__pt_<u:016x>_`:
//!
//! | name | what |
//! |---|---|
//! | `__pt_<u>_<pid:08x>.db`, `_pointers.db` | one partition's rows and pointers |
//! | `__pt_<u>_<pid:08x>.ixs`, `.ixm` | its index snapshot and marker log (persistent indexes §1) |
//! | `__pt_<u>_<pid:08x>_flt.db` | its retired filter sidecar (persistent indexes §5, B14): deleted at open, reaped as a leftover |
//! | `__pt_<u>_schema.db`, `__pt_<u>_schema_<SIDECAR>.db` | the shared schema; sidecar names derive from the schema provider's (`schema.rs`) |
//! | `__pt_<u>_map_<gen>.db` | the map log, one live generation |
//! | `__pt_<u>_ids.db` | the shared row-id reservation |
//! | `__pt_<u>_seq.db` | `SERIAL` high-water marks (phase 3) |
//!
//! each with its provider's `.tmp` staging sibling. [`parse_name`] accepts
//! exactly §12's reaper pattern
//! `^__pt_[0-9a-f]{16}_([0-9a-f]{8}(_pointers|_flt)?|schema(_[A-Z]+)?|map_[0-9]+|ids|seq)\.db(\.tmp)?$`
//! and persistent indexes' second one (§10, B14)
//! `^__pt_[0-9a-f]{16}_[0-9a-f]{8}\.(ixs|ixm)(\.tmp)?$`, both partition data
//! files, narrowed in one place: a map generation must be written the way
//! [`map_file`] writes it (no leading zero, fits a `u64`), so two names never
//! parse to one generation. A name it rejects is never deleted by anything
//! built on it, which is the safe side. Outside the pattern on purpose:
//! `__pt_<u>_map_<gen>.db.torn`, the torn tails [`MapLogFile::open`] cut off
//! and keeps; only [`remove_all`] (DROP) deletes it.
//!
//! # Creation
//!
//! `create_new` and `LocalStorageProvider::new` create a missing file
//! silently and fsync its directory once per file and per `.tmp`, discarding
//! the error. [`create_partition_files`] and [`create_parent_files`] instead
//! create every file and its `.tmp` with `create_new(true)`, fsync the
//! directory once, and return any failure; a provider opened afterwards finds
//! all of them present and creates nothing. [`require_existing`] is the
//! check made before any provider is built on a file that must already exist
//! (I9): it opens without ever creating.
//!
//! The IO here is `pub(crate)`: it deletes files by uid, which is no API.

// Only `PartitionedTable` calls the IO, and nothing builds one until
// `Database` does (S4), which removes this.
#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use super::log;
use crate::core::storage_providers::backend::DbStorageProvider;
use crate::core::storage_providers::traits::StorageIO;

/// What every partitioned table's file name starts with. The same string as
/// `database::PARTITION_FILE_PREFIX`, which reserves it from table names.
pub const PREFIX: &str = "__pt_";

/// The suffixes of one partition's files, after its [`data_stem`]. The two
/// a `Table` opens; a partition loads no `_seq.db` (§3.4). `_flt.db` was the
/// third until persistent indexes B14 retired it: an open deletes it.
pub const DATA_SUFFIXES: [&str; 2] = [".db", "_pointers.db"];

/// Files a partition's stem may also have, none of which must exist: the
/// retired filter sidecar and the index-store pair (persistent indexes §1,
/// §5). A materialization deletes them as leftovers with the data files,
/// and the reaper and DROP read them as the partition's.
pub const OPTIONAL_SUFFIXES: [&str; 3] = ["_flt.db", ".ixs", ".ixm"];

/// The staging sibling every file provider keeps beside its file.
const TMP: &str = ".tmp";

/// Appended to a map log's name for the torn tails [`MapLogFile::open`] cut
/// off it. [`parse_name`] and §12's pattern reject the result, so no reaper
/// deletes the evidence that keeps reaping off.
pub const TORN: &str = ".torn";

/// `__pt_<uid:016x>_<pid:08x>`: a partition's physical table name, which its
/// `Table` hangs [`DATA_SUFFIXES`] off.
pub fn data_stem(uid: u64, pid: u32) -> String {
    format!("{PREFIX}{uid:016x}_{pid:08x}")
}

/// `__pt_<uid:016x>_schema`. The provider is opened as `<stem>.db`, and the
/// sidecars derive from that name.
pub fn schema_stem(uid: u64) -> String {
    format!("{PREFIX}{uid:016x}_schema")
}

/// `__pt_<uid:016x>_map_<gen>.db`. Generation 0 throughout phase 2a.
pub fn map_file(uid: u64, generation: u64) -> String {
    format!("{PREFIX}{uid:016x}_map_{generation}.db")
}

/// `__pt_<uid:016x>_ids.db`.
pub fn ids_file(uid: u64) -> String {
    format!("{PREFIX}{uid:016x}_ids.db")
}

/// `__pt_<uid:016x>_seq.db`.
pub fn seq_file(uid: u64) -> String {
    format!("{PREFIX}{uid:016x}_seq.db")
}

/// The two files of partition `pid`, without their `.tmp` siblings: what
/// [`require_existing`] checks before a materialized partition opens.
pub fn data_files(uid: u64, pid: u32) -> [String; 2] {
    let stem = data_stem(uid, pid);
    DATA_SUFFIXES.map(|s| format!("{stem}{s}"))
}

/// The parent files [`create_parent_files`] makes and a table open requires:
/// the schema, the generation-0 map log and the ids file.
pub fn parent_files(uid: u64) -> [String; 3] {
    [format!("{}.db", schema_stem(uid)), map_file(uid, 0), ids_file(uid)]
}

/// What a partition file name says it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartFile {
    Data { uid: u64, pid: u32 },
    Schema { uid: u64 },
    Map { uid: u64, generation: u64 },
    Ids { uid: u64 },
    Seq { uid: u64 },
}

impl PartFile {
    pub fn uid(&self) -> u64 {
        match *self {
            PartFile::Data { uid, .. }
            | PartFile::Schema { uid }
            | PartFile::Map { uid, .. }
            | PartFile::Ids { uid }
            | PartFile::Seq { uid } => uid,
        }
    }
}

/// `s` as a lowercase hex number of exactly `digits` digits.
fn lower_hex(s: &str, digits: usize) -> Option<u64> {
    if s.len() != digits || !s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return None;
    }
    u64::from_str_radix(s, 16).ok()
}

/// The uid a name is under: `__pt_` then 16 lowercase hex digits then `_`.
/// Looser than [`parse_name`] on purpose, for the callers that must treat
/// anything under a uid as taken.
fn uid_prefix(name: &str) -> Option<u64> {
    let rest = name.strip_prefix(PREFIX)?;
    let (hex, tail) = (rest.get(..16)?, rest.get(16..)?);
    if !tail.starts_with('_') {
        return None;
    }
    lower_hex(hex, 16)
}

/// Reads a partition file name, `.tmp` siblings included (module docs). Hand
/// written rather than a regex: the crate has no regex dependency here, and
/// the grammar is five literals.
pub fn parse_name(name: &str) -> Option<PartFile> {
    let uid = uid_prefix(name)?;
    let rest = &name[PREFIX.len() + 17..];
    let rest = rest.strip_suffix(TMP).unwrap_or(rest);
    // A partition's index-store pair (persistent indexes §1): `.ixs`, `.ixm`.
    if let Some(pid) = rest.strip_suffix(".ixs").or_else(|| rest.strip_suffix(".ixm")) {
        return lower_hex(pid, 8).map(|pid| PartFile::Data { uid, pid: pid as u32 });
    }
    let body = rest.strip_suffix(".db")?;
    match body {
        "ids" => return Some(PartFile::Ids { uid }),
        "seq" => return Some(PartFile::Seq { uid }),
        "schema" => return Some(PartFile::Schema { uid }),
        _ => {}
    }
    if let Some(sidecar) = body.strip_prefix("schema_") {
        return (!sidecar.is_empty() && sidecar.bytes().all(|b| b.is_ascii_uppercase()))
            .then_some(PartFile::Schema { uid });
    }
    if let Some(generation) = body.strip_prefix("map_") {
        let canonical = !generation.is_empty()
            && generation.bytes().all(|b| b.is_ascii_digit())
            && (generation == "0" || !generation.starts_with('0'));
        return canonical
            .then(|| generation.parse().ok())
            .flatten()
            .map(|generation| PartFile::Map { uid, generation });
    }
    let pid = body
        .strip_suffix("_pointers")
        .or_else(|| body.strip_suffix("_flt"))
        .unwrap_or(body);
    lower_hex(pid, 8).map(|pid| PartFile::Data { uid, pid: pid as u32 })
}

/// The schema file that owns `name` when it is a partition's pointer file,
/// `__pt_<u>_<pid>_pointers.db`: the table's one shared schema
/// `__pt_<u>_schema.db`, since a partition has no schema of its own. `None`
/// for every other name, its `.tmp` sibling included.
///
/// The torn-tail cutter (`torn_tail::classify`) counts a pointer file only
/// when its schema sits beside it, and looks for `<stem>_schema.db`, which a
/// partition never has; without this its pointers were never cut back after
/// a power cut, and the next insert landed inside the torn record.
pub fn pointers_schema(name: &str) -> Option<String> {
    let stem = name.strip_suffix("_pointers.db")?;
    match parse_name(name)? {
        PartFile::Data { uid, pid } if stem == data_stem(uid, pid) => Some(format!("{}.db", schema_stem(uid))),
        _ => None,
    }
}

/// Every uid with any file in `dir`, by the loose prefix test.
fn uids_in(dir: &Path) -> io::Result<HashSet<u64>> {
    let mut seen = HashSet::new();
    for entry in std::fs::read_dir(dir)? {
        if let Some(uid) = entry?.file_name().to_str().and_then(uid_prefix) {
            seen.insert(uid);
        }
    }
    Ok(seen)
}

/// A fresh uid for a new partitioned table: random, nonzero (`clusters = 0`
/// means a plain table), no file in `dir` under it, and not `taken` (the
/// catalogue's uids, whose root files may be missing). `dir` is the root
/// location, where every live table keeps its parent files; a partition
/// orphaned elsewhere under a reused uid is deleted by materializing.
pub(crate) fn new_uid(dir: &Path, taken: impl Fn(u64) -> bool) -> io::Result<u64> {
    new_uid_from(dir, taken, || {
        let mut b = [0u8; 8];
        rand::fill(&mut b);
        u64::from_le_bytes(b)
    })
}

fn new_uid_from(dir: &Path, taken: impl Fn(u64) -> bool, mut next: impl FnMut() -> u64) -> io::Result<u64> {
    let present = uids_in(dir)?;
    loop {
        let uid = next();
        if uid != 0 && !present.contains(&uid) && !taken(uid) {
            return Ok(uid);
        }
    }
}

/// Flushes `dir`'s entries: what makes a create or delete in it durable.
/// Errors are returned, unlike the constructors' `let _ =`.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let handle = std::fs::File::open(dir)?;
        crate::core::sync_audit::note("partition_dir", dir);
        handle.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}

/// Opens every one of `names` in `dir` read-write without ever creating it,
/// so a missing file is a named error rather than a fresh empty table (I9).
/// Made before any provider is built on them, since providers create.
pub(crate) fn require_existing(dir: &Path, names: &[String]) -> io::Result<()> {
    for name in names {
        let path = dir.join(name);
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| io::Error::new(e.kind(), format!("partition file {} cannot be opened: {e}", path.display())))?;
    }
    Ok(())
}

/// Creates each of `names` and its `.tmp` with `create_new(true)`, then
/// fsyncs `dir` once. An existing file is an error, never reused.
fn create_all(dir: &Path, names: &[String]) -> io::Result<()> {
    for name in names {
        for file in [name.clone(), format!("{name}{TMP}")] {
            let path = dir.join(&file);
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|e| io::Error::new(e.kind(), format!("creating partition file {}: {e}", path.display())))?;
        }
    }
    sync_dir(dir)
}

/// Materializes partition `pid`'s files in `dir` (§3.2): deletes any
/// leftover file of that partition, creates both and their `.tmp`s
/// exclusively, and fsyncs the directory once. Any failure is returned.
///
/// Leftovers are deleted, never reused: files without `MATERIALIZE` may hold
/// rows of a transaction whose outcome was lost, which would read as
/// committed once the forget horizon passes it (§12). Only the six names
/// [`parse_name`] reads as this partition's can exist (`lower_hex` takes one
/// spelling of each number), so those are deleted by name: no directory scan
/// per first write, which is O(every file in the root). Call only for a
/// partition with no `MATERIALIZE` record and no open provider.
///
/// A failure part-way leaves files without `MATERIALIZE`, which the next
/// attempt and the reaper both delete.
pub(crate) fn create_partition_files(dir: &Path, uid: u64, pid: u32) -> io::Result<()> {
    let names = data_files(uid, pid);
    let stem = data_stem(uid, pid);
    let leftovers = OPTIONAL_SUFFIXES.map(|s| format!("{stem}{s}"));
    for name in names.iter().chain(leftovers.iter()) {
        for file in [name.clone(), format!("{name}{TMP}")] {
            let path = dir.join(&file);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(io::Error::new(e.kind(), format!("deleting leftover {}: {e}", path.display())));
                }
            }
        }
    }
    create_all(dir, &names)
}

/// Creates a new table's parent files in the root `dir`: the schema, the
/// generation-0 map log and the ids file, each exclusively, with one
/// directory fsync. Schema sidecars are created by the schema code as it
/// writes them; [`sync_dir`] after that makes their entries durable.
///
/// Does not delete anything first: a new table's uid comes from [`new_uid`],
/// so a file already there means the uid is not new.
pub(crate) fn create_parent_files(dir: &Path, uid: u64) -> io::Result<()> {
    create_all(dir, &parent_files(uid))
}

/// Makes sure the table's `SERIAL` reservations file exists in `dir`
/// (§10, `partition::serial`): created exclusively with its `.tmp`, and the
/// directory fsynced, when it is missing (a table created before phase 3
/// P5a, or a new one). An existing file is kept: it is the reservation.
/// Returns whether it was created.
pub(crate) fn ensure_seq_file(dir: &Path, uid: u64) -> io::Result<bool> {
    let name = seq_file(uid);
    match std::fs::metadata(dir.join(&name)) {
        Ok(_) => Ok(false),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            // A `.tmp` left by an earlier attempt holds nothing staged yet.
            let tmp = dir.join(format!("{name}{TMP}"));
            match std::fs::remove_file(&tmp) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(io::Error::new(e.kind(), format!("deleting leftover {}: {e}", tmp.display()))),
            }
            create_all(dir, &[name]).map(|()| true)
        }
        Err(e) => Err(io::Error::new(e.kind(), format!("checking {name}: {e}"))),
    }
}

/// Creates map log generation `generation` of `uid` in `dir` for a
/// compaction (§3.2 step 2), exclusively with its `.tmp`, and fsyncs the
/// directory once. An existing file is an error, never deleted or reused:
/// the caller picks a number above every generation in the directory
/// ([`next_map_generation`]).
pub(crate) fn create_map_generation(dir: &Path, uid: u64, generation: u64) -> io::Result<()> {
    create_all(dir, &[map_file(uid, generation)])
}

/// One above the highest map log generation of `uid` in `dir`, and above
/// `at_least`: a number no file has, so a compaction never writes over a
/// leftover (one the reaper keeps as unreadable, or an unfinished attempt
/// whose committed appends a crash could replay onto a new file of the same
/// name).
pub(crate) fn next_map_generation(dir: &Path, uid: u64, at_least: u64) -> io::Result<u64> {
    let highest = generations_in(dir, uid)?.into_iter().max().unwrap_or(0);
    Ok(highest.max(at_least).saturating_add(1))
}

/// Deletes map log generation `generation` of `uid` and its `.tmp` from
/// `dir`, then fsyncs it: a compaction's old generation once appends have
/// switched (§3.2 step 4), or its own unfinished one. Missing files are no
/// error.
pub(crate) fn remove_map_generation(dir: &Path, uid: u64, generation: u64) -> io::Result<()> {
    let name = map_file(uid, generation);
    let mut removed = false;
    for file in [name.clone(), format!("{name}{TMP}")] {
        let path = dir.join(&file);
        match std::fs::remove_file(&path) {
            Ok(()) => removed = true,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(io::Error::new(e.kind(), format!("deleting {}: {e}", path.display()))),
        }
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(())
}

/// Deletes every file in `dir` that [`parse_name`] puts under `uid`, `.tmp`s
/// included, then fsyncs `dir`; returns how many went. Partitions' files go
/// first and the map log last, so a crash part-way leaves the parent files
/// that say what the rest were (§12's DROP order). The map log's [`TORN`]
/// evidence goes with the map: once the table is dropped it guards nothing.
/// Providers on these files must be closed first. A file already gone is not
/// an error.
pub(crate) fn remove_all(dir: &Path, uid: u64) -> io::Result<usize> {
    let mut found: Vec<(u8, std::path::PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(file) = name.to_str().and_then(|n| {
            parse_name(n).or_else(|| {
                n.strip_suffix(TORN)
                    .and_then(parse_name)
                    .filter(|f| matches!(f, PartFile::Map { .. }))
            })
        }) else {
            continue;
        };
        if file.uid() != uid {
            continue;
        }
        let rank = match file {
            PartFile::Data { .. } => 0,
            PartFile::Seq { .. } | PartFile::Ids { .. } | PartFile::Schema { .. } => 1,
            PartFile::Map { .. } => 2,
        };
        found.push((rank, entry.path()));
    }
    found.sort();
    let mut removed = 0;
    for (_, path) in &found {
        match std::fs::remove_file(path) {
            Ok(()) => removed += 1,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(io::Error::new(e.kind(), format!("deleting {}: {e}", path.display()))),
        }
    }
    if removed > 0 {
        sync_dir(dir)?;
    }
    Ok(removed)
}

/// What the startup reaper did (§12).
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ReapReport {
    /// The file names deleted.
    pub deleted: Vec<String>,
    /// Why some or all reapable files were kept, when they were: reaping
    /// was off for this start, or a step failed.
    pub kept: Option<String>,
}

/// The map log generations of `uid` in `root`, highest first.
fn generations_in(root: &Path, uid: u64) -> io::Result<Vec<u64>> {
    let mut gens = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let Ok(name) = entry?.file_name().into_string() else {
            continue;
        };
        if let Some(PartFile::Map { uid: u, generation }) = parse_name(&name)
            && u == uid
        {
            gens.push(generation);
        }
    }
    gens.sort_unstable_by(|a, b| b.cmp(a));
    gens.dedup();
    Ok(gens)
}

/// The live generation of `uid`'s map log in `root` (§3.2): the highest
/// whose log replays to a sealed state (a compaction that reached its
/// durable `SEALED`, whose appends then switched to it), else 0. A higher
/// one that does not replay, or replays unsealed, is a compaction that never
/// finished, which nothing was ever appended to after its clone; the reaper
/// deletes it. Reads without building a provider, so nothing is created or
/// cut. 0 when no generation is sealed, even if generation 0 is gone, so
/// the never-creating check that follows names the missing file (I9).
pub(crate) fn live_generation(root: &Path, uid: u64) -> Result<u64, String> {
    live_generation_and_unreadable(root, uid).map(|(g, _)| g)
}

/// [`live_generation`], and the higher generations that do not replay at
/// all. An unfinished compaction can leave one so (pages written out of
/// order before its `SEALED` was synced), but so would a later version's
/// records this one cannot read: the reaper deletes none of them.
fn live_generation_and_unreadable(root: &Path, uid: u64) -> Result<(u64, Vec<u64>), String> {
    let gens = generations_in(root, uid).map_err(|e| format!("{} cannot be listed: {e}", root.display()))?;
    let mut unreadable = Vec::new();
    for &g in &gens {
        if g == 0 {
            return Ok((0, unreadable));
        }
        let name = map_file(uid, g);
        let buf = std::fs::read(root.join(&name)).map_err(|e| format!("its map log {name} cannot be read: {e}"))?;
        match log::replay(&buf) {
            Ok(r) if r.state.sealed() => return Ok((g, unreadable)),
            Ok(_) => ::log::warn!("map log {name} is an unfinished compaction (no SEALED); the generation before it is live"),
            Err(e) => {
                ::log::warn!("map log {name} does not replay ({e}); the generation before it is live");
                unreadable.push(g);
            }
        }
    }
    Ok((0, unreadable))
}

/// A live table's map log as the reaper reads it: its live generation
/// ([`live_generation`]), read without building a provider, so nothing is
/// created or cut. `Err` with why it cannot be relied on to say
/// which partitions are materialized: missing, staged bytes in its `.tmp`,
/// a torn tail or [`TORN`] evidence, a replay error, another table's uid, a
/// `DROPPED` record, or a shape this version cannot open (reshaped, retired
/// partitions, a partition off the root). Any of these turns reaping off.
pub(crate) fn read_live_map(root: &Path, uid: u64) -> Result<LiveMap, String> {
    let (generation, unreadable) = live_generation_and_unreadable(root, uid)?;
    if let Some(g) = unreadable.first() {
        return Err(format!(
            "its map log generation {g} ({}) does not replay; it may be a later version's, so nothing is reaped until \
             it is examined",
            map_file(uid, *g)
        ));
    }
    let name = map_file(uid, generation);
    let path = root.join(&name);
    let buf = std::fs::read(&path).map_err(|e| format!("its map log {} cannot be read: {e}", path.display()))?;
    // Map appends go through the log, never the staging file; bytes there
    // would be records this read misses.
    match std::fs::metadata(root.join(format!("{name}{TMP}"))) {
        Ok(m) if m.len() > 0 => return Err(format!("its map log {name} has {} staged bytes", m.len())),
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("the staging file of its map log {name} cannot be checked: {e}")),
    }
    // An unanswerable check counts as present: the safe side.
    if root.join(format!("{name}{TORN}")).try_exists().unwrap_or(true) {
        return Err(format!("its map log {name} had a torn tail cut into {name}{TORN}, not yet examined"));
    }
    let replay = log::replay(&buf).map_err(|e| format!("its map log {name}: {e}"))?;
    if let Some(t) = &replay.torn {
        return Err(format!("its map log {name} has a torn tail at byte {}: {}", t.offset, t.reason));
    }
    let state = replay.state;
    if state.header().uid != uid {
        return Err(format!("its map log {name} belongs to table {:016x}", state.header().uid));
    }
    if state.dropped() {
        return Err(format!("its map log {name} says it was dropped, but its catalogue row is live"));
    }
    let map = state.map();
    let dense = map.entries().iter().enumerate().all(|(i, &(_, pid))| pid as usize == i);
    if !dense
        || state.next_pid() != map.len() as u64
        || state.reshapes().next().is_some()
        || state.parts().any(|(_, p)| p.retired || p.location.is_some_and(|l| l != 0))
    {
        return Err(format!("its map log {name} describes a reshaped or spread map, which this version cannot read"));
    }
    Ok(LiveMap { generation, state })
}

/// A live table's map, as the reaper reads it: which generation is live,
/// and what it says.
#[derive(Debug)]
pub(crate) struct LiveMap {
    pub generation: u64,
    pub state: log::MapState,
}

/// The names in `root` the reaper deletes (§12), ordered partitions first and
/// map logs last as [`remove_all`] orders them, or why it deletes none.
///
/// A name is deleted only if [`parse_name`] reads it and either its uid is
/// not in `live` (no catalogue row holds it), it is a partition file whose
/// pid the live map never materialized, or it is a map generation of a live
/// uid other than its live one (a compaction's old generation, or one that
/// never finished, §3.2). A live uid's schema, ids, seq and live map are
/// kept. Nothing is deleted if any live map is `Err`.
fn reapable(root: &Path, live: &std::collections::HashMap<u64, Result<LiveMap, String>>) -> io::Result<Result<Vec<String>, String>> {
    let mut failed: Vec<(&u64, &String)> = live.iter().filter_map(|(u, m)| m.as_ref().err().map(|e| (u, e))).collect();
    if !failed.is_empty() {
        failed.sort();
        let (uid, why) = failed[0];
        return Ok(Err(format!(
            "partitioned table {uid:016x}'s map cannot be relied on ({why}){}",
            if failed.len() > 1 { format!(", and {} more", failed.len() - 1) } else { String::new() }
        )));
    }
    let mut doomed: Vec<(u8, String)> = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let Ok(name) = entry?.file_name().into_string() else {
            continue;
        };
        let Some(file) = parse_name(&name) else {
            continue;
        };
        let rank = match file {
            PartFile::Data { .. } => 0,
            PartFile::Seq { .. } | PartFile::Ids { .. } | PartFile::Schema { .. } => 1,
            PartFile::Map { .. } => 2,
        };
        let Some(state) = live.get(&file.uid()) else {
            doomed.push((rank, name));
            continue;
        };
        let live_map = state.as_ref().expect("an Err map returned above");
        match file {
            PartFile::Data { pid, .. } => {
                if live_map.state.part(pid).and_then(|p| p.location).is_none() {
                    doomed.push((rank, name));
                }
            }
            PartFile::Map { generation, .. } if generation != live_map.generation => {
                doomed.push((rank, name));
            }
            _ => {}
        }
    }
    doomed.sort();
    Ok(Ok(doomed.into_iter().map(|(_, n)| n).collect()))
}

/// Runs the startup reaper over `root` (§12): finds what [`reapable`]
/// allows, hands the names to `void` (which voids the logs' records for
/// them; a failure keeps every file, since deleted files with live records
/// would be written back by a replay), deletes them and fsyncs `root`.
/// Never errs: a step that fails is reported in [`ReapReport::kept`] and
/// the files stay, a leak the next start retries.
pub(crate) fn reap(
    root: &Path,
    live: &std::collections::HashMap<u64, Result<LiveMap, String>>,
    void: impl FnOnce(&[String]) -> io::Result<()>,
) -> ReapReport {
    let names = match reapable(root, live) {
        Ok(Ok(names)) => names,
        Ok(Err(why)) => {
            return ReapReport {
                deleted: Vec::new(),
                kept: Some(why),
            };
        }
        Err(e) => {
            return ReapReport {
                deleted: Vec::new(),
                kept: Some(format!("{} cannot be listed: {e}", root.display())),
            };
        }
    };
    if names.is_empty() {
        return ReapReport::default();
    }
    if let Err(e) = void(&names) {
        return ReapReport {
            deleted: Vec::new(),
            kept: Some(format!("the logs' records for {} file(s) could not be voided: {e}", names.len())),
        };
    }
    let mut report = ReapReport::default();
    for name in names {
        match std::fs::remove_file(root.join(&name)) {
            Ok(()) => report.deleted.push(name),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                report.kept.get_or_insert_with(|| format!("deleting {name}: {e}"));
            }
        }
    }
    if !report.deleted.is_empty()
        && let Err(e) = sync_dir(root)
    {
        report.kept.get_or_insert_with(|| format!("syncing {} after deleting: {e}", root.display()));
    }
    report
}

/// The map log's file (§3.2). Appends are serialised by its lock, and each
/// is written ahead of the log and durable when it returns
/// ([`MapLogFile::append_all`]), so a transaction outcome logged after it
/// can never replay without it, and no crash leaves the file torn.
pub(crate) struct MapLogFile {
    /// The live generation's provider: swapped by a compaction
    /// ([`Self::adopt`]), under `lock`, so no append straddles two files.
    io_slot: parking_lot::RwLock<Arc<DbStorageProvider>>,
    /// The live generation (§3.2): 0 until the first compaction.
    generation: std::sync::atomic::AtomicU64,
    /// Records appended since a compaction cloned the state, for it to
    /// append to the new generation before switching (§3.2 step 3).
    capture: parking_lot::Mutex<Option<Vec<log::Record>>>,
    lock: tokio::sync::Mutex<()>,
    /// Set when a failed append could not be cut back out of the file: its
    /// record may replay, so nothing is appended after it in this process.
    broken: OnceLock<String>,
}

fn invalid(e: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// Appends `tail` to the evidence file `kept` and makes it durable, entry
/// included, before the caller cuts the tail out of the log. Appended rather
/// than replaced: an earlier start's evidence, not yet cleared, still counts.
fn keep_torn_tail(kept: &Path, tail: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let mut f = OpenOptions::new().create(true).append(true).open(kept)?;
    f.write_all(tail)?;
    f.sync_all()?;
    sync_dir(kept.parent().unwrap_or(Path::new(".")))
}

impl MapLogFile {
    fn new(io: Arc<DbStorageProvider>) -> Self {
        // The generation its name says (`map_file`); 0 for any other name.
        let generation = match parse_name(&io.get_name()) {
            Some(PartFile::Map { generation, .. }) => generation,
            _ => 0,
        };
        Self {
            io_slot: parking_lot::RwLock::new(io),
            generation: std::sync::atomic::AtomicU64::new(generation),
            capture: parking_lot::Mutex::new(None),
            lock: tokio::sync::Mutex::new(()),
            broken: OnceLock::new(),
        }
    }

    /// The live generation's number.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(std::sync::atomic::Ordering::Acquire)
    }

    /// From here on, every record appended is also kept for
    /// [`Self::end_capture`]. Called under the map state's lock, with the
    /// clone a compaction writes as its `SNAPSHOT`.
    pub(crate) fn begin_capture(&self) {
        *self.capture.lock() = Some(Vec::new());
    }

    /// The records appended since [`Self::begin_capture`], and no more
    /// capture.
    pub(crate) fn end_capture(&self) -> Vec<log::Record> {
        self.capture.lock().take().unwrap_or_default()
    }

    /// Switches appends to `next`, generation `generation`, a compaction's
    /// sealed new log (§3.2 step 3). Under the append lock, so no append is
    /// in flight. Returns the old generation's provider, for the caller to
    /// drop before it deletes the file.
    pub(crate) async fn adopt(&self, next: MapLogFile, generation: u64) -> io::Result<Arc<DbStorageProvider>> {
        let _order = self.lock.lock().await;
        self.check()?;
        next.check()?;
        let new_io = next.io();
        let old = std::mem::replace(&mut *self.io_slot.write(), new_io);
        self.generation.store(generation, std::sync::atomic::Ordering::Release);
        Ok(old)
    }

    /// A new, empty log. Refuses a file that already holds bytes: appending
    /// a second `HEADER` after someone else's records would refuse the table
    /// at its next open.
    pub(crate) async fn create(io: Arc<DbStorageProvider>) -> io::Result<Self> {
        let len = io.get_len().await;
        if len != 0 {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("partition map log {} already holds {len} bytes", io.get_name()),
            ));
        }
        Ok(Self::new(io))
    }

    /// `<log>.torn` beside the log, or `None` for a provider with no
    /// directory.
    fn torn_path(&self) -> Option<std::path::PathBuf> {
        let location = self.io().get_location()?;
        Some(Path::new(&location).join(format!("{}{TORN}", self.io().get_name())))
    }

    /// Opens an existing log and replays it. Run after WAL recovery, which
    /// may complete the tail.
    ///
    /// A torn tail is cut off before anything can be appended after it:
    /// otherwise the next record would sit past damaged bytes, and replay
    /// refuses that shape (`log::replay`), so one append would refuse the
    /// table for good. The cut bytes are first kept in `<log>`[`TORN`],
    /// because the CRC cannot tell a tear from a durable last record damaged
    /// on the media: a lost `MATERIALIZE` would make its partition's files
    /// look reapable. While that file exists, every open reports
    /// [`log::Replay::torn`], so reaping stays off (§12) until an operator
    /// has looked and removed it, not just on the first start after the cut.
    pub(crate) async fn open(io: Arc<DbStorageProvider>) -> io::Result<(Self, log::Replay)> {
        let file = Self::new(io);
        let buf = file.read_all().await?;
        let mut replay = log::replay(&buf).map_err(|e| invalid(format!("{}: {e}", file.io().get_name())))?;
        let kept = file.torn_path();
        if replay.torn.is_some() {
            let name = file.io().get_name();
            let Some(kept) = &kept else {
                return Err(invalid(format!(
                    "partition map log {name} has a torn tail and no directory to keep it in"
                )));
            };
            keep_torn_tail(kept, &buf[replay.valid_len..]).map_err(|e| {
                io::Error::new(e.kind(), format!("keeping the torn tail of {name} in {}: {e}", kept.display()))
            })?;
            file.io().truncate(replay.valid_len as u64).await.map_err(|e| {
                io::Error::new(e.kind(), format!("cutting the torn tail of partition map log {name}: {e}"))
            })?;
        } else if let Some(kept) = &kept
            // An unanswerable check counts as present: the safe side.
            && kept.try_exists().unwrap_or(true)
        {
            replay.torn = Some(log::TornTail {
                offset: replay.valid_len,
                reason: format!(
                    "an earlier open cut a torn tail into {}; reaping waits until it is examined and removed",
                    kept.display()
                ),
                trailing: 0,
            });
        }
        Ok((file, replay))
    }

    /// The live generation's provider.
    pub(crate) fn io(&self) -> Arc<DbStorageProvider> {
        self.io_slot.read().clone()
    }

    fn check(&self) -> io::Result<()> {
        match self.broken.get() {
            Some(why) => Err(io::Error::other(format!(
                "partition map log {} takes no more appends: {why}",
                self.io().get_name()
            ))),
            None => Ok(()),
        }
    }

    /// Appends `rec`: [`Self::append_all`] of one record.
    pub(crate) async fn append(&self, rec: &log::Record) -> io::Result<()> {
        self.append_all(std::slice::from_ref(rec)).await
    }

    /// Appends `recs`, in order, as one WAL transaction, durable when this
    /// returns.
    ///
    /// **Write-ahead, as `torn_tail::SidecarWrites` writes the schema
    /// sidecars:** one in-place write at the log's end
    /// (`StorageIO::write_data_txn`), which the WAL holds back until the
    /// record describing it is durable and only then applies to the file. So
    /// the file never holds a byte the log cannot account for, and no crash
    /// can tear it: a cut before the commit's sync leaves the file as it was,
    /// one after it leaves whatever of the apply reached the disk, which
    /// recovery writes over whole. The logged append this replaced wrote the
    /// file first and the log record after, so a power cut before a sync
    /// could keep the file's new length and drop its pages: a run of zeros
    /// at the end (a torn tail, with compaction and reaping off until an
    /// operator looked), or zeros followed by a record whose page did reach
    /// the disk, which `log::replay` refuses, refusing the table. Every
    /// record kind goes this way, whatever §3.2 lists it under, so the
    /// evidence and refusal logic is left for real media damage.
    ///
    /// The price is a log sync per call (a transaction holding an in-place
    /// write syncs at its commit), so callers batch: a forget round's
    /// `SWEPT`s are one call (`PinBoard::sweep_all`), a `CREATE`'s three
    /// records one, a compaction's opening records one.
    ///
    /// Without a log (a directory with `wal_enabled = false`) an in-place
    /// write lands at once and does not move the provider's length, so the
    /// records are appended directly, flushed, as before: there is no log
    /// for a record to be written ahead of.
    ///
    /// A failure leaves the file as it was, and nothing is aborted here:
    /// `commit_durable` ends a transaction it refuses before attempting the
    /// commit itself, and one whose commit was attempted and failed has
    /// poisoned the log, which refuses every later commit (its record then
    /// replays or not, either of which the state tolerates for every kind:
    /// `DROPPED` and `MATERIALIZE` follow their commit points, `DIRTY` and
    /// `SWEPT` only pin lower or record a proof already made). A retry
    /// therefore appends at the same offset. The provider's claim on where
    /// in-place writes reached is cut back to that offset, or the next
    /// commit would publish the failed record's end as the file's length;
    /// if that cut fails, the log refuses every later append in this
    /// process.
    pub(crate) async fn append_all(&self, recs: &[log::Record]) -> io::Result<()> {
        let mut buf = Vec::new();
        for rec in recs {
            buf.extend_from_slice(&log::encode(rec).map_err(invalid)?);
        }
        if buf.is_empty() {
            return Ok(());
        }
        let _order = self.lock.lock().await;
        self.check()?;
        let io = self.io();
        let txn = io.begin_txn();
        let (at, token) = if io.defers_in_place_writes() {
            let at = io.get_len().await;
            (at, io.write_data_txn(txn, at, &buf).await)
        } else {
            io.append_data_txn(txn, &buf).await
        };
        let Err(e) = io.commit_durable(token).await else {
            if let Some(kept) = self.capture.lock().as_mut() {
                kept.extend(recs.iter().cloned());
            }
            return Ok(());
        };
        if let Err(cut) = io.truncate(at).await {
            let _ = self.broken.set(format!(
                "an append failed ({e}) and the file could not be cut back to byte {at} ({cut})"
            ));
        }
        Err(e)
    }

    /// [`Self::append_all`], then [`Self::sync_pending`]: for a caller that
    /// must also see every earlier group commit of the location durable.
    pub(crate) async fn append_all_durable(&self, recs: &[log::Record]) -> io::Result<()> {
        self.append_all(recs).await?;
        self.sync_pending()
    }

    fn sync_pending(&self) -> io::Result<()> {
        match self.io().get_location() {
            Some(location) => {
                crate::core::wal::with_wal_for(&location, |w| w.map_or(Ok(()), |w| w.sync_pending()))
            }
            None => Ok(()),
        }
    }

    /// [`Self::append`], then the location's group commits forced out too:
    /// for the records §3.2 marks fsync (`HEADER`, `LOCATIONS`, `PLACE`,
    /// `MATERIALIZE`, `DIRTY`, reshape steps, `DROPPED`). The record itself
    /// is durable once [`Self::append`] returns; the sync after it covers a
    /// caller that relies on the log being synced through this point, as
    /// before. With no WAL the append already flushed the file. A failed sync
    /// poisons the log (`Wal::commit_through`), which refuses every later
    /// commit, so nothing lands after this record.
    pub(crate) async fn append_durable(&self, rec: &log::Record) -> io::Result<()> {
        self.append_all_durable(std::slice::from_ref(rec)).await
    }

    /// Reads the whole log and replays it, without changing the file.
    pub(crate) async fn replay(&self) -> io::Result<log::Replay> {
        let buf = self.read_all().await?;
        log::replay(&buf).map_err(|e| invalid(format!("{}: {e}", self.io().get_name())))
    }

    async fn read_all(&self) -> io::Result<Vec<u8>> {
        let _order = self.lock.lock().await;
        let len = self.io().get_len().await;
        let len = usize::try_from(len).map_err(|_| invalid(format!("partition map log of {len} bytes")))?;
        let mut buf = vec![0u8; len];
        let mut at = 0u64;
        if len > 0 {
            self.io().read_data_into_buffer(&mut at, &mut buf).await?;
        }
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::partition::RangeMap;
    use crate::core::partition::key::KeyFamily;
    use crate::core::partition::log::{MapHeader, ROUTE_V1, Record};
    use crate::core::partition::map::SPACE_END;

    const UID: u64 = 0x0123_4567_89ab_cdef;

    fn header() -> Record {
        Record::Header(MapHeader {
            uid: UID,
            route_v: ROUTE_V1,
            key: vec![(0, KeyFamily::Integer)],
            widths: vec![48],
            colocation: 0,
        })
    }

    fn place() -> Record {
        Record::Place {
            hi: SPACE_END,
            entries: RangeMap::initial(8).unwrap().entries().to_vec(),
        }
    }

    fn names_in(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn names_are_the_designed_shapes() {
        assert_eq!(data_stem(UID, 0x2a), "__pt_0123456789abcdef_0000002a");
        assert_eq!(schema_stem(UID), "__pt_0123456789abcdef_schema");
        assert_eq!(map_file(UID, 0), "__pt_0123456789abcdef_map_0.db");
        assert_eq!(map_file(UID, 12), "__pt_0123456789abcdef_map_12.db");
        assert_eq!(ids_file(UID), "__pt_0123456789abcdef_ids.db");
        assert_eq!(seq_file(UID), "__pt_0123456789abcdef_seq.db");
        assert_eq!(
            data_files(1, 0xffff_ffff),
            [
                "__pt_0000000000000001_ffffffff.db",
                "__pt_0000000000000001_ffffffff_pointers.db",
            ]
        );
    }

    #[test]
    fn parse_name_accepts_every_reaper_shape_and_round_trips() {
        let mut names: Vec<(String, PartFile)> = Vec::new();
        for pid in [0u32, 7, 0xdead_beef] {
            for n in data_files(UID, pid) {
                names.push((n, PartFile::Data { uid: UID, pid }));
            }
            // The retired filter sidecar and the index-store pair (B14).
            for s in OPTIONAL_SUFFIXES {
                names.push((format!("{}{s}", data_stem(UID, pid)), PartFile::Data { uid: UID, pid }));
            }
        }
        names.push((format!("{}.db", schema_stem(UID)), PartFile::Schema { uid: UID }));
        for sidecar in ["FIELDS", "FK", "DEF", "UK", "JSON", "TYP", "CHK", "GEN", "PIX", "IDX"] {
            names.push((format!("{}_{sidecar}.db", schema_stem(UID)), PartFile::Schema { uid: UID }));
        }
        for generation in [0u64, 1, 10, u64::MAX] {
            names.push((map_file(UID, generation), PartFile::Map { uid: UID, generation }));
        }
        names.push((ids_file(UID), PartFile::Ids { uid: UID }));
        names.push((seq_file(UID), PartFile::Seq { uid: UID }));
        names.push((ids_file(0), PartFile::Ids { uid: 0 }));
        names.push((ids_file(u64::MAX), PartFile::Ids { uid: u64::MAX }));
        for (name, want) in names {
            assert_eq!(parse_name(&name), Some(want), "{name}");
            assert_eq!(parse_name(&format!("{name}.tmp")), Some(want), "{name}.tmp");
        }
    }

    #[test]
    fn parse_name_rejects_near_misses() {
        let u = "0123456789abcdef";
        for name in [
            "users.db".to_string(),
            "__pt_x".to_string(),
            "__pt_".to_string(),
            format!("__PT_{u}_ids.db"),
            format!("__pt_0123456789ABCDEF_ids.db"),
            format!("__pt_0123456789abcde_ids.db"),
            format!("__pt_0123456789abcdef0_ids.db"),
            format!("__pt_{u}ids.db"),
            format!("__pt_{u}_.db"),
            format!("__pt_{u}.db"),
            format!("__pt_{u}_ids"),
            format!("__pt_{u}_ids.db.tmp.tmp"),
            format!("__pt_{u}_ids.tmp"),
            format!("__pt_{u}_ids.db.bak"),
            format!("x__pt_{u}_ids.db"),
            format!("__pt_{u}_0000002A.db"),
            format!("__pt_{u}_0000002.db"),
            format!("__pt_{u}_000000002.db"),
            format!("__pt_{u}_0000002a_pointer.db"),
            format!("__pt_{u}_0000002a_seq.db"),
            format!("__pt_{u}_0000002a_schema.db"),
            format!("__pt_{u}_0000002a_flt_pointers.db"),
            format!("__pt_{u}_0000002a.ixs.tmp.tmp"),
            format!("__pt_{u}_0000002A.ixs"),
            format!("__pt_{u}_0000002a.ixe"),
            format!("__pt_{u}_0000002a_pointers.ixs"),
            format!("__pt_{u}_ids.ixm"),
            format!("__pt_{u}_schema_.db"),
            format!("__pt_{u}_schema_fields.db"),
            format!("__pt_{u}_schema_FIELDS1.db"),
            format!("__pt_{u}_schemas.db"),
            format!("__pt_{u}_map_.db"),
            format!("__pt_{u}_map_x.db"),
            format!("__pt_{u}_map_01.db"),
            format!("__pt_{u}_map_-1.db"),
            format!("__pt_{u}_map_18446744073709551616.db"),
            format!("__pt_{u}_map_0.db{TORN}"),
            format!("__pt_{u}_IDS.db"),
            format!("__pt_{u}_junk.db"),
        ] {
            assert_eq!(parse_name(&name), None, "{name}");
        }
    }

    #[test]
    fn new_uid_skips_zero_present_and_taken_uids() {
        let dir = tempfile::tempdir().unwrap();
        let present = 0x1111_u64;
        // A junk name under a uid still claims it: the loose prefix test.
        std::fs::write(dir.path().join(format!("__pt_{present:016x}_junk")), b"").unwrap();
        let mut seq = vec![0u64, present, 0x2222, 0x3333].into_iter();
        let uid = new_uid_from(dir.path(), |u| u == 0x2222, || seq.next().unwrap()).unwrap();
        assert_eq!(uid, 0x3333);
        // And the real generator answers.
        assert_ne!(new_uid(dir.path(), |_| false).unwrap(), 0);
    }

    #[test]
    fn create_partition_files_deletes_leftovers_and_creates_fresh_empty_files() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let [rows, pointers] = data_files(UID, 5);
        std::fs::write(d.join(&rows), b"stale rows of a lost transaction").unwrap();
        std::fs::write(d.join(format!("{pointers}.tmp")), b"stale staged pointers").unwrap();
        // A leftover snapshot, log and retired sidecar of the stem go too
        // (B14): never pair a new partition's files with an old `CLEAN`.
        let optional: Vec<String> = OPTIONAL_SUFFIXES.iter().map(|s| format!("{}{s}", data_stem(UID, 5))).collect();
        for o in &optional {
            std::fs::write(d.join(o), b"leftover").unwrap();
            std::fs::write(d.join(format!("{o}.tmp")), b"leftover").unwrap();
        }
        // Neighbours that must survive: another partition, another uid, a
        // near-miss name and a plain table.
        let keep = [
            data_files(UID, 6)[0].clone(),
            data_files(UID + 1, 5)[0].clone(),
            format!("{}_junk.db", data_stem(UID, 5)),
            "users.db".to_string(),
        ];
        for k in &keep {
            std::fs::write(d.join(k), b"keep").unwrap();
        }

        create_partition_files(d, UID, 5).unwrap();

        for name in data_files(UID, 5) {
            for f in [name.clone(), format!("{name}.tmp")] {
                assert_eq!(std::fs::metadata(d.join(&f)).unwrap().len(), 0, "{f} must be fresh and empty");
            }
        }
        for k in &keep {
            assert_eq!(std::fs::read(d.join(k)).unwrap(), b"keep", "{k} is not this partition's");
        }
        for o in &optional {
            assert!(!d.join(o).exists() && !d.join(format!("{o}.tmp")).exists(), "{o} must be gone");
        }
        // A second materialization of the same pid finds the first one's
        // files as leftovers and replaces them too.
        std::fs::write(d.join(&rows), b"x").unwrap();
        create_partition_files(d, UID, 5).unwrap();
        assert_eq!(std::fs::metadata(d.join(&rows)).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn create_partition_files_syncs_the_directory_once_and_providers_create_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let watch = crate::core::sync_audit::watch(d);
        create_partition_files(d, UID, 9).unwrap();
        let counts = watch.counts();
        assert_eq!(counts.get("partition_dir"), Some(&1), "{counts:?}");
        assert_eq!(counts.values().sum::<usize>(), 1, "{counts:?}");

        // Every file and `.tmp` exists, so opening the providers creates no
        // file and syncs no directory.
        let location = d.to_str().unwrap();
        let before = names_in(d);
        for name in data_files(UID, 9) {
            let _p = DbStorageProvider::new_file(location, Some(&name)).await;
        }
        let events = watch.take();
        assert!(
            events.iter().all(|(tag, _)| *tag != "dir" && *tag != "partition_dir"),
            "{events:?}"
        );
        assert_eq!(names_in(d), before);
        assert_eq!(before.len(), 4);
    }

    #[test]
    fn create_partition_files_returns_a_failure() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        // A directory where a file must go cannot be deleted as a leftover
        // or created over.
        let [_, pointers] = data_files(UID, 3);
        std::fs::create_dir(d.join(&pointers)).unwrap();
        let err = create_partition_files(d, UID, 3).unwrap_err();
        assert!(err.to_string().contains(&pointers), "{err}");

        let missing = d.join("no_such_dir");
        assert!(create_partition_files(&missing, UID, 3).is_err());
    }

    #[test]
    fn create_parent_files_is_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        create_parent_files(d, UID).unwrap();
        for name in parent_files(UID) {
            assert!(d.join(&name).is_file(), "{name}");
            assert!(d.join(format!("{name}.tmp")).is_file(), "{name}.tmp");
        }
        let err = create_parent_files(d, UID).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists, "{err}");
    }

    #[test]
    fn require_existing_errors_on_a_missing_file_and_never_creates_it() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let names = data_files(UID, 1).to_vec();
        std::fs::write(d.join(&names[0]), b"").unwrap();

        let err = require_existing(d, &names).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(err.to_string().contains(&names[1]), "{err}");
        assert!(!d.join(&names[1]).exists(), "the check created {}", names[1]);

        std::fs::write(d.join(&names[1]), b"").unwrap();
        require_existing(d, &names).unwrap();
    }

    #[test]
    fn remove_all_takes_only_the_uids_files_and_syncs_once() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let mut mine: Vec<String> = Vec::new();
        for pid in [0, 1] {
            mine.extend(data_files(UID, pid));
        }
        mine.extend(parent_files(UID));
        mine.push(format!("{}_FIELDS.db", schema_stem(UID)));
        mine.push(seq_file(UID));
        mine.push(format!("{}.tmp", map_file(UID, 0)));
        mine.push(format!("{}{TORN}", map_file(UID, 0)));
        let keep = [
            ids_file(UID + 1),
            format!("{}{TORN}", map_file(UID + 1, 0)),
            format!("{}{TORN}", ids_file(UID)),
            format!("__pt_{UID:016x}_junk.db"),
            format!("__pt_{UID:016x}_map_01.db"),
            "users.db".to_string(),
        ];
        for n in mine.iter().chain(keep.iter()) {
            std::fs::write(d.join(n), b"").unwrap();
        }
        let watch = crate::core::sync_audit::watch(d);
        assert_eq!(remove_all(d, UID).unwrap(), mine.len());
        // The deletes are made durable, once.
        assert_eq!(watch.counts().get("partition_dir"), Some(&1), "{:?}", watch.counts());
        let mut left = keep.to_vec();
        left.sort();
        assert_eq!(names_in(d), left);
        assert_eq!(remove_all(d, UID).unwrap(), 0);
    }

    /// §12's DROP order, made visible by a delete that fails part-way: the
    /// map log, the schema and the ids file must still be there to say what
    /// the remaining files were.
    #[test]
    fn remove_all_deletes_partitions_before_the_parent_files() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        create_parent_files(d, UID).unwrap();
        create_partition_files(d, UID, 1).unwrap();
        // `remove_file` fails on a directory, so the partitions' rank stops.
        std::fs::create_dir(d.join(&data_files(UID, 0)[0])).unwrap();

        let err = remove_all(d, UID).unwrap_err();
        assert!(err.to_string().contains(&data_files(UID, 0)[0]), "{err}");
        for name in parent_files(UID) {
            assert!(d.join(&name).is_file(), "{name} went before a partition's file");
        }
    }

    /// The directory fsync's own failure is returned, not discarded like
    /// the constructors' `let _ =`: a directory with write and search but no
    /// read permission creates and deletes files, but cannot be opened to
    /// sync.
    #[cfg(unix)]
    #[test]
    fn a_failed_directory_sync_is_returned() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o300)).unwrap();
        // Root reads regardless of the mode; nothing can fail then.
        if std::fs::read_dir(d).is_ok() {
            std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o700)).unwrap();
            return;
        }
        let parents = create_parent_files(d, UID);
        let partition = create_partition_files(d, UID, 2);
        std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o700)).unwrap();

        assert!(parents.is_err(), "the parent files' directory sync failure was swallowed");
        assert!(partition.is_err(), "the partition's directory sync failure was swallowed");
        // The files themselves were made: only the sync failed.
        assert!(d.join(&data_files(UID, 2)[0]).is_file());
        assert!(d.join(map_file(UID, 0)).is_file());
    }

    async fn map_provider(d: &Path) -> Arc<DbStorageProvider> {
        let location = d.to_str().unwrap();
        create_parent_files(d, UID).unwrap();
        Arc::new(DbStorageProvider::new_file(location, Some(&map_file(UID, 0))).await)
    }

    fn install_wal(d: &Path) -> Arc<crate::core::wal::Wal> {
        let wal = Arc::new(crate::core::wal::Wal::open(d.join(crate::core::wal::DEFAULT_WAL_NAME)).unwrap());
        crate::core::wal::install_for(d.to_str().unwrap(), wal.clone());
        wal
    }

    #[tokio::test]
    async fn append_lands_in_the_main_file_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let _wal = install_wal(d);
        let log = MapLogFile::create(map_provider(d).await).await.unwrap();
        let path = d.join(map_file(UID, 0));
        assert_eq!(log.io().get_name(), map_file(UID, 0));

        log.append(&header()).await.unwrap();
        let after_header = std::fs::metadata(&path).unwrap().len();
        assert_eq!(after_header, log::encode(&header()).unwrap().len() as u64);
        log.append(&place()).await.unwrap();
        let expect = after_header + log::encode(&place()).unwrap().len() as u64;
        assert_eq!(std::fs::metadata(&path).unwrap().len(), expect);
        assert_eq!(std::fs::metadata(d.join(format!("{}.tmp", map_file(UID, 0)))).unwrap().len(), 0);

        let replay = log.replay().await.unwrap();
        assert_eq!(replay.valid_len as u64, expect);
        assert!(replay.torn.is_none());
        assert_eq!(replay.state.map().entries(), RangeMap::initial(8).unwrap().entries());
        crate::core::wal::remove_for(d.to_str().unwrap());
    }

    /// The record is in the WAL, committed: when the file's own bytes are
    /// lost (writeback that never happened), recovery puts them back.
    #[tokio::test]
    async fn an_appended_record_is_recovered_from_the_wal() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let location = d.to_str().unwrap().to_string();
        let wal = install_wal(d);
        let path = d.join(map_file(UID, 0));
        {
            let log = MapLogFile::create(map_provider(d).await).await.unwrap();
            log.append(&header()).await.unwrap();
            log.append_durable(&place()).await.unwrap();
            // Both records went through the log, and append_durable forced it out.
            assert!(wal.commit_lsn() >= 2, "the records took no WAL commit");
            assert!(wal.durable_through() >= wal.commit_lsn(), "the log was not synced");
        }
        let full = std::fs::read(&path).unwrap();
        crate::core::wal::remove_for(&location);
        drop(wal);
        std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(0).unwrap();

        let (_wal, stats) = crate::core::wal::open_and_recover(&location).unwrap();
        crate::core::wal::remove_for(&location);
        assert!(stats.applied >= 2, "{stats:?}");
        assert_eq!(std::fs::read(&path).unwrap(), full);
    }

    #[tokio::test]
    async fn open_cuts_a_torn_tail_so_later_appends_replay() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let path = d.join(map_file(UID, 0));
        let valid = {
            let log = MapLogFile::create(map_provider(d).await).await.unwrap();
            log.append(&header()).await.unwrap();
            log.append(&Record::Locations(vec!["/data/root".to_string()])).await.unwrap();
            log.append(&place()).await.unwrap();
            std::fs::metadata(&path).unwrap().len()
        };
        // Half of a third record, as a crash mid-append leaves it.
        let partial = log::encode(&Record::Stats { pid: 1, rows: 2, bytes: 3 }).unwrap();
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&partial[..partial.len() / 2]).unwrap();
        }

        let open = move || async move {
            let io = Arc::new(DbStorageProvider::new_file(d.to_str().unwrap(), Some(&map_file(UID, 0))).await);
            MapLogFile::open(io).await.unwrap()
        };
        let (log, replay) = open().await;
        assert!(replay.torn.is_some(), "the caller must still learn of the torn tail");
        assert_eq!(replay.valid_len as u64, valid);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid);
        // The cut bytes are kept, not destroyed.
        let kept = d.join(format!("{}{TORN}", map_file(UID, 0)));
        assert_eq!(std::fs::read(&kept).unwrap(), &partial[..partial.len() / 2]);

        log.append(&Record::Materialize { pid: 1, floor: 7, loc: 0 }).await.unwrap();
        let again = log.replay().await.unwrap();
        assert!(again.torn.is_none());
        assert_eq!(again.state.part(1).unwrap().location, Some(0));
        drop(log);

        // A later start still holds reaping back: the evidence outlives the
        // start that cut it.
        let (_log, later) = open().await;
        assert!(later.torn.is_some(), "the next open forgot the torn tail");
        assert_eq!(later.state.part(1).unwrap().location, Some(0));
        drop(_log);
        // Until an operator removes it.
        std::fs::remove_file(&kept).unwrap();
        let (_log, cleared) = open().await;
        assert!(cleared.torn.is_none(), "{:?}", cleared.torn);
    }

    /// A commit that fails after the bytes reached the file (here the commit
    /// marker, as a full disk would) leaves the file as it was: a retried
    /// `MATERIALIZE` is the only one, and the log still replays.
    #[tokio::test]
    async fn a_failed_append_leaves_no_record_behind_and_a_retry_replays() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let _wal = install_wal(d);
        let log = MapLogFile::create(map_provider(d).await).await.unwrap();
        let path = d.join(map_file(UID, 0));
        log.append(&header()).await.unwrap();
        log.append(&Record::Locations(vec!["/data/root".to_string()])).await.unwrap();
        log.append(&place()).await.unwrap();
        let before = std::fs::metadata(&path).unwrap().len();
        let materialize = Record::Materialize { pid: 1, floor: 7, loc: 0 };

        crate::core::wal::test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(true));
        let failed = log.append(&materialize).await;
        crate::core::wal::test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(false));
        assert!(failed.is_err(), "the injected commit failure was swallowed");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), before, "the failed record stayed in the file");

        log.append(&materialize).await.unwrap();
        let replay = log.replay().await.unwrap();
        assert_eq!(replay.state.part(1).unwrap().location, Some(0));
        let one = log::encode(&materialize).unwrap().len() as u64;
        assert_eq!(std::fs::metadata(&path).unwrap().len(), before + one);
        crate::core::wal::remove_for(d.to_str().unwrap());
    }

    /// With a log, an append is durable when it returns, and a failed append
    /// that was longer than the retry leaves no claim on the bytes past the
    /// retry's end. Without the provider's claim cut back, the next
    /// commit published the failed record's end as the length, and the next
    /// append landed past a hole of zeros, which replay refuses.
    ///
    /// (That no crash can tear the file is `power_loss::pins_crash_tests`'
    /// to show: no failure here can tell the two orders apart.)
    ///
    /// Mutation: `append_all` not cutting the provider back after a failure
    /// (the length runs past the file, and the replay is torn).
    #[tokio::test]
    async fn an_append_is_written_ahead_and_a_failed_one_leaves_no_claim() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let wal = install_wal(d);
        let log = MapLogFile::create(map_provider(d).await).await.unwrap();
        let path = d.join(map_file(UID, 0));
        log.append(&header()).await.unwrap();
        let base = std::fs::metadata(&path).unwrap().len();

        // A failed commit marker leaves the file as it was.
        let long = Record::Locations(vec!["/a/much/longer/location/than/the/retry".to_string()]);
        crate::core::wal::test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(true));
        let failed = log.append(&long).await;
        crate::core::wal::test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(false));
        assert!(failed.is_err(), "the injected commit failure was swallowed");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), base, "the failed record reached the file");
        assert_eq!(log.io().get_len().await, base);

        // A shorter retry: the length is its end, not the failed record's.
        let short = Record::Locations(vec!["/r".to_string()]);
        assert!(log::encode(&short).unwrap().len() < log::encode(&long).unwrap().len());
        log.append(&short).await.unwrap();
        let after_short = base + log::encode(&short).unwrap().len() as u64;
        assert_eq!(log.io().get_len().await, after_short, "the failed append's claim outlived it");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), after_short);
        log.append(&place()).await.unwrap();
        let expect = after_short + log::encode(&place()).unwrap().len() as u64;
        assert_eq!(log.io().get_len().await, expect);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), expect);
        assert!(wal.durable_through() >= wal.commit_lsn(), "append_all returned before its log record was durable");
        let replay = log.replay().await.unwrap();
        assert!(replay.torn.is_none(), "{:?}", replay.torn);
        assert_eq!(replay.valid_len as u64, expect);
        assert_eq!(replay.state.map().entries(), RangeMap::initial(8).unwrap().entries());
        crate::core::wal::remove_for(d.to_str().unwrap());
    }

    /// A map-log append whose commit marker failed holds no claim on the log
    /// afterwards. The append's record is held as an in-place overwrite, and
    /// the commit contract forbids an abort after a failed commit, so only
    /// `finish_txn` can release it: before it did, `pending_transactions`
    /// stayed raised and every later checkpoint declined, so the log grew
    /// until restart although the next append succeeded.
    ///
    /// Mutation: `finish_txn` returning the marker's error without
    /// `abort_txn` (pending stays 1, both checkpoints return `Ok(false)`).
    #[tokio::test]
    async fn a_failed_commit_marker_leaves_checkpoints_working() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let wal = install_wal(d);
        let log = MapLogFile::create(map_provider(d).await).await.unwrap();
        log.append(&header()).await.unwrap();
        log.append(&place()).await.unwrap();
        assert_eq!(wal.pending_transactions(), 0);
        assert!(wal.checkpoint().unwrap(), "a checkpoint with nothing in flight declined");

        crate::core::wal::test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(true));
        let failed = log.append(&Record::Locations(vec!["/x".to_string()])).await;
        crate::core::wal::test_hooks::FAIL_COMMIT_MARKER.with(|f| f.set(false));
        assert!(failed.is_err(), "the injected commit failure was swallowed");
        assert!(!wal.is_poisoned(), "a cleanly cut-back marker poisoned the log");
        assert_eq!(wal.pending_transactions(), 0, "the failed commit kept its claim on the log");

        log.append(&Record::Locations(vec!["/r".to_string()])).await.unwrap();
        assert!(wal.checkpoint().unwrap(), "every checkpoint after a failed marker declines");
        assert!(wal.checkpoint().unwrap());
        let replay = log.replay().await.unwrap();
        assert!(replay.torn.is_none(), "{:?}", replay.torn);
        crate::core::wal::remove_for(d.to_str().unwrap());
    }

    /// A failure that stays (a data file in the directory failed its flush)
    /// refuses every retry, and none of them leaves bytes behind.
    #[tokio::test]
    async fn appends_refused_by_a_failed_directory_leave_the_file_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let location = d.to_str().unwrap();
        let _wal = install_wal(d);
        let log = MapLogFile::create(map_provider(d).await).await.unwrap();
        let path = d.join(map_file(UID, 0));
        log.append(&header()).await.unwrap();
        log.append(&Record::Locations(vec!["/data/root".to_string()])).await.unwrap();
        log.append(&place()).await.unwrap();
        let before = std::fs::metadata(&path).unwrap().len();

        crate::core::storage_providers::file_sync::record_directory_failure(location, "test".to_string());
        for _ in 0..2 {
            assert!(log.append(&Record::Materialize { pid: 1, floor: 7, loc: 0 }).await.is_err());
            assert_eq!(std::fs::metadata(&path).unwrap().len(), before);
        }
        crate::core::storage_providers::file_sync::forget_directory_failure(location);

        let replay = log.replay().await.unwrap();
        assert_eq!(replay.valid_len as u64, before);
        assert_eq!(replay.state.part(1).and_then(|p| p.location), None);
        crate::core::wal::remove_for(location);
    }

    /// Once a failed record could not be cut back, nothing more is written.
    #[tokio::test]
    async fn a_broken_log_appends_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let log = MapLogFile::create(map_provider(d).await).await.unwrap();
        let path = d.join(map_file(UID, 0));
        log.append(&header()).await.unwrap();
        log.broken.set("injected".to_string()).unwrap();
        let before = std::fs::metadata(&path).unwrap().len();
        let err = log.append(&place()).await.unwrap_err();
        assert!(err.to_string().contains("injected"), "{err}");
        assert!(log.append_durable(&place()).await.is_err());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), before);
    }

    #[tokio::test]
    async fn create_refuses_a_log_that_holds_records() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let io = map_provider(d).await;
        MapLogFile::create(io.clone()).await.unwrap().append(&header()).await.unwrap();
        let err = MapLogFile::create(io).await.err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }
}
