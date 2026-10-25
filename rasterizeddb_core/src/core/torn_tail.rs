//! A fixed-record file that ends inside a record is cut back to its last
//! whole record before the log replays, and never appended after.
//!
//! # The failure
//!
//! A write is not atomic across a power cut. A record appended to a file is
//! one `write(2)`, but the page cache flushes pages, not writes, and records
//! of most strides here straddle pages (37 does not divide 4096). So a power
//! cut can leave a file ending part-way through a record: the leading bytes
//! of an append that never committed, or of one the log will write again.
//!
//! The local provider appends at the file's length. After such a cut, the
//! next append lands right after the fragment, every reader steps through
//! the file in whole records from offset 0, and the record a reader finds at
//! that slot is the fragment plus the head of the new record. The write was
//! acknowledged and is invisible (`a_lost_reservation_page_never_reissues_an_id`
//! saw an `xmin` of 1099696177152 there). Everything appended after it is
//! shifted the same way.
//!
//! # The repair, and why before replay
//!
//! [`cut_torn_tails`] runs in [`crate::core::wal::open_and_recover`] after the
//! directory's files are registered and before [`crate::core::wal::Wal::recover`],
//! and in a restore before its replay. It cuts each fixed-record file to
//! `header + k * stride`, syncs the cut, and only then lets the log replay.
//! Replay writes committed records at their absolute offsets, so every byte
//! past the cut that a committed record owns comes back. Cutting *after*
//! replay instead would leave the fragment's bytes in place whenever replay
//! wrote a record beyond it: the slot would hold the fragment over zeros, not
//! the zeros a never-committed append leaves.
//!
//! # The durable floor
//!
//! A cut removes less than one record, and under whole-record appends that is
//! never a record the log no longer holds: a checkpoint drops records only
//! after syncing their files, and a synced record is whole. The floor checks
//! that this holds rather than assuming it. Each checkpoint records, per
//! fixed-record file it synced, the whole-record length the file had when its
//! sync was issued ([`Floors`], `<log>.floors` beside the log). A cut that
//! would go below it means the file lost bytes it had synced, and removing
//! them would drop a committed record for good, so the open is refused with
//! the file, its length, the stride and the floor named.
//!
//! Floors are keyed by the file's identity (device, inode, birth time), not
//! its name, so a dropped, recreated, renamed, copied or restored file never
//! inherits another file's floor. A file shrunk in place (`VACUUM`'s pointer
//! compaction, a mirror rebuild) lowers its floor durably before and again
//! after the shrink, through the provider's `truncate`. Each open clamps every
//! floor to its file's length after the cut and forgets files that are gone,
//! so a floor never outlives the run that could not lower it.
//!
//! # Which files
//!
//! Fixed-record files appended at their length, every one header-less:
//!
//! | file | stride |
//! |---|---|
//! | `<t>_pointers.db` | `ROW_POINTER_RECORD_LEN` (37; 108 with `enable_long_row`) |
//! | `<t>_schema_FIELDS.db` | `SCHEMA_FIELD_SIZE` (153) |
//! | `<t>_schema_FK.db` | `FOREIGN_KEY_SIZE` (519) |
//! | `<t>_schema_DEF.db` | `COLUMN_DEFAULT_SIZE` (387) |
//! | `<t>_schema_JSON.db` | `JSON_COLUMN_SIZE` (259) |
//! | `<t>_schema_PIX.db` | `PATH_INDEX_SIZE` (259) |
//! | `<t>_schema_IDX.db` | `DECL_SIZE` (197) |
//! | `<t>_schema_UK.db` | `UNIQUE_KEY_SIZE` (1163) |
//! | `<t>_schema_TYP.db` | `TYPE_MODIFIER_SIZE` (134) |
//! | `<t>_schema_GEN.db` | `GENERATED_SIZE` (643) |
//! | `<t>_schema_CHK.db` | `CHECK_SIZE` (643) |
//! | `rdb.ixe` | 32, after a 16-byte header (the slot-epoch log, one per directory) |
//!
//! The catalogue's own `db_data_pointers.db` and `db_data_schema_FIELDS.db`
//! are the first two for the table `db_data`.
//!
//! A file counts only when `<t>_schema.db` sits beside it, so a table whose
//! *name* ends in one of these suffixes (its row file `x_pointers.db`) is not
//! mistaken for another table's sidecar. Cutting a variable-length file to a
//! multiple of an unrelated stride would destroy committed bytes.
//!
//! A partitioned table's files are in the same directory (`partition::files`):
//! its schema and sidecars are `__pt_<u>_schema.db` and
//! `__pt_<u>_schema_<SIDECAR>.db`, the rule above for the table `__pt_<u>`.
//! Each partition's pointers, `__pt_<u>_<pid>_pointers.db`, have no schema of
//! their own; they count when the table's shared one sits beside them
//! (`partition::files::pointers_schema`). The rest under the prefix (rows,
//! filters, the map log, which cuts its own torn tail at open, the ids and
//! sequence files) are none of these.
//!
//! Left alone, each for a reason:
//! - `<t>.db`, the row data: variable-length records addressed by the
//!   absolute offsets pointer records hold, so a torn tail is bytes no pointer
//!   names; the next append's offset is where it really landed.
//! - the log itself: replay stops at the first record that does not read
//!   whole, and recovery truncates it.
//! - `<t>_schema.db`: written once at offset 0 (header plus incarnation
//!   record), never appended to.
//! - `<t>_seq.db`, `db_transactions.next`, `db_transactions.status`: written
//!   at fixed offsets, not appended; a short status file reads as no outcome.
//! - `<t>_flt.db`: retired (persistent indexes §5, B14), deleted at open and
//!   never written; `.ixs` and `.ixm` are written whole and proven by
//!   checksums and the marker log, never appended to by offset.
//! - `db_replication.state`: cut back to whole 16-byte records at its own
//!   open (`replication::log`).
//! - files written by temporary name and rename (`.cfg`, marks, snapshots).
//! - tables on a key/value store: each commit is one CRC-checked object, a
//!   file's length is the end of its committed records rather than any
//!   physical length, and an append's offset comes from that length, so an
//!   uncommitted tail vanishes whole and nothing can end inside a record.
//! - the memory backend: nothing survives a restart to be torn.

use std::collections::HashMap;
use std::fs::{File, Metadata};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::core::storage_providers::CRC;

/// A file appended in records of one size after an optional header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedRecordFile {
    /// The suffix that names it, for messages.
    pub kind: &'static str,
    pub header: u64,
    pub stride: u64,
}

impl FixedRecordFile {
    /// `len` cut back to the end of its last whole record.
    pub fn whole(&self, len: u64) -> u64 {
        if len <= self.header {
            return len;
        }
        len - (len - self.header) % self.stride
    }
}

/// `rdb.ixe`, the directory's slot-epoch log (docs/persistent-indexes.md
/// §1; `index_store::epoch_log`): a 16-byte header, then 32-byte records
/// appended at its length through the directory's log. One per directory,
/// named by no table (no table file ends `.ixe`), so it needs no schema
/// beside it to be recognised.
pub const EPOCH_LOG: FixedRecordFile = FixedRecordFile {
    kind: crate::core::index_store::epoch_log::EPOCH_LOG_FILE,
    header: crate::core::index_store::epoch_log::HEADER_LEN as u64,
    stride: crate::core::index_store::epoch_log::RECORD_LEN as u64,
};

/// Every fixed-record file kind, by the suffix after the table name. See the
/// module documentation for the ones deliberately absent.
const FIXED_RECORD_SUFFIXES: &[(&str, u64)] = &[
    (
        "_pointers.db",
        crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN as u64,
    ),
    ("_schema_FIELDS.db", crate::core::row::schema::SCHEMA_FIELD_SIZE),
    ("_schema_FK.db", crate::core::rql::foreign_key::FOREIGN_KEY_SIZE),
    ("_schema_DEF.db", crate::core::rql::column_default::COLUMN_DEFAULT_SIZE),
    ("_schema_JSON.db", crate::core::rql::json_map::JSON_COLUMN_SIZE),
    ("_schema_PIX.db", crate::core::rql::path_index::PATH_INDEX_SIZE),
    ("_schema_IDX.db", crate::core::index::decl::DECL_SIZE),
    ("_schema_UK.db", crate::core::rql::unique_key::UNIQUE_KEY_SIZE),
    ("_schema_TYP.db", crate::core::rql::type_modifier::TYPE_MODIFIER_SIZE),
    ("_schema_GEN.db", crate::core::rql::generated::GENERATED_SIZE),
    ("_schema_CHK.db", crate::core::rql::check::CHECK_SIZE),
];

/// What `name`, a file in `dir`, is, if it is a fixed-record file: its
/// suffix names a kind and its table's schema file is beside it, or it is the
/// directory's slot-epoch log.
pub fn classify(dir: &Path, name: &str) -> Option<FixedRecordFile> {
    if name == crate::core::index_store::epoch_log::EPOCH_LOG_FILE {
        return Some(EPOCH_LOG);
    }
    for &(suffix, stride) in FIXED_RECORD_SUFFIXES {
        let Some(table) = name.strip_suffix(suffix) else {
            continue;
        };
        if table.is_empty() || !(dir.join(format!("{table}_schema.db")).is_file() || partition_pointers(dir, name)) {
            return None;
        }
        return Some(FixedRecordFile {
            kind: suffix,
            header: 0,
            stride,
        });
    }
    None
}

/// Whether `name` is a partition's pointer file whose table's shared schema
/// sits beside it ([`crate::core::partition::files::pointers_schema`]), and
/// not the row data of a plain table that an older build let take a name
/// under the partition prefix (`<name less .db>_schema.db` beside it).
fn partition_pointers(dir: &Path, name: &str) -> bool {
    crate::core::partition::files::pointers_schema(name).is_some_and(|schema| {
        dir.join(schema).is_file()
            && !dir
                .join(format!("{}_schema.db", name.trim_end_matches(".db")))
                .is_file()
    })
}

/// [`classify`] for a full path.
pub fn classify_path(path: &Path) -> Option<FixedRecordFile> {
    let name = path.file_name()?.to_str()?;
    classify(path.parent().unwrap_or(Path::new("")), name)
}

/// What [`truncate_to_whole_records`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truncated {
    /// The file ended on a record boundary, or does not exist.
    Whole,
    /// The file was cut from `from` bytes to `to`, and the cut synced.
    Cut { from: u64, to: u64 },
}

impl Truncated {
    /// Bytes the cut removed.
    pub fn bytes(&self) -> u64 {
        match self {
            Truncated::Whole => 0,
            Truncated::Cut { from, to } => from - to,
        }
    }
}

/// Cuts `path` back to `header_len + k * stride`, the end of its last whole
/// record, and syncs the cut before returning.
///
/// Refuses, changing nothing, when the cut would go below `durable_floor`:
/// those bytes were synced and the log that held them is gone, so cutting
/// would drop a committed record. A file shorter than its header, or missing,
/// is left alone.
pub fn truncate_to_whole_records(
    path: &Path,
    header_len: u64,
    stride: u64,
    durable_floor: u64,
) -> io::Result<Truncated> {
    assert!(stride > 0, "a record stride of zero");
    let len = match std::fs::metadata(path) {
        Ok(m) => m.len(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Truncated::Whole),
        Err(e) => return Err(e),
    };
    if len <= header_len {
        return Ok(Truncated::Whole);
    }
    let partial = (len - header_len) % stride;
    if partial == 0 {
        return Ok(Truncated::Whole);
    }
    let to = len - partial;
    if to < durable_floor {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} ends inside a record: it is {len} bytes, its records are {stride} bytes \
                 after a {header_len}-byte header, so its last whole record ends at {to}; \
                 but its durable floor is {durable_floor} bytes, the whole records the last \
                 checkpoint synced before dropping them from the log. Cutting it back would \
                 remove a committed record that nothing else holds, so the database was not \
                 opened. The file lost bytes after they were synced: a storage fault, or \
                 something outside the database shortened it. Restore the file (or the \
                 database) from a backup; deleting the log's `.floors` file opens it with \
                 the record lost",
                path.display()
            ),
        ));
    }
    let file = File::options().write(true).open(path)?;
    file.set_len(to)?;
    crate::core::sync_audit::note("torn_tail.cut", path);
    file.sync_all()?;
    log::info!(
        "{} ended inside a record: cut {partial} bytes ({len} -> {to}, {stride}-byte records) \
         before the log replays",
        path.display()
    );
    Ok(Truncated::Cut { from: len, to })
}

/// Cuts every fixed-record file directly in `dir` back to whole records,
/// refusing as [`truncate_to_whole_records`] does below a file's floor in
/// `floors`. Called before the directory's log replays; see the module
/// documentation. Variable-length files (the row data `<t>.db`, addressed by
/// the absolute offsets its pointers hold) are not touched: a torn tail there
/// is bytes no pointer names.
pub fn cut_torn_tails(dir: &Path, floors: &Floors) -> io::Result<Vec<(PathBuf, Truncated)>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut cut = Vec::new();
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if !meta.is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(kind) = classify(dir, &name) else {
            continue;
        };
        let path = entry.path();
        let floor = floors.floor_of(&meta);
        let done = truncate_to_whole_records(&path, kind.header, kind.stride, floor)?;
        if done != Truncated::Whole {
            cut.push((path, done));
        }
    }
    Ok(cut)
}

/// Who a file is, whatever it is called: a floor follows the file through a
/// rename and is never inherited by the next file of a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileIdentity {
    dev: u64,
    ino: u64,
    /// Birth time in nanoseconds, where the filesystem reports one: an inode
    /// number freed by a `DROP` can be handed to the next file created.
    born: u128,
}

impl FileIdentity {
    pub fn of(meta: &Metadata) -> FileIdentity {
        #[cfg(unix)]
        let (dev, ino) = {
            use std::os::unix::fs::MetadataExt;
            (meta.dev(), meta.ino())
        };
        #[cfg(not(unix))]
        let (dev, ino) = (0, 0);
        let born = meta
            .created()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        FileIdentity { dev, ino, born }
    }
}

const FLOORS_MAGIC: &str = "rdb-floors 1";

/// The durable floor of each fixed-record file a checkpoint synced: the
/// whole-record length it had when its sync was issued. See the module
/// documentation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Floors {
    /// identity -> (file name when recorded, floor)
    entries: HashMap<FileIdentity, (String, u64)>,
}

impl Floors {
    /// Where the floors of the log at `wal_path` are kept.
    pub fn path_for(wal_path: &Path) -> PathBuf {
        let mut name = wal_path.file_name().unwrap_or_default().to_os_string();
        name.push(FLOORS_SUFFIX);
        wal_path.with_file_name(name)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The floor of the file `meta` describes; 0 when none is recorded.
    pub fn floor_of(&self, meta: &Metadata) -> u64 {
        self.entries
            .get(&FileIdentity::of(meta))
            .map_or(0, |(_, floor)| *floor)
    }

    /// Records `floor` for `id`. Whether anything changed.
    pub fn set(&mut self, id: FileIdentity, name: &str, floor: u64) -> bool {
        if name.contains('\n') {
            return false;
        }
        let new = (name.to_string(), floor);
        self.entries.insert(id, new.clone()) != Some(new)
    }

    /// Lowers `id`'s floor to `to` if it is above. Whether anything changed.
    pub fn lower(&mut self, id: FileIdentity, to: u64) -> bool {
        match self.entries.get_mut(&id) {
            Some((_, floor)) if *floor > to => {
                *floor = to;
                true
            }
            _ => false,
        }
    }

    /// Forgets the files no longer in `dir` and clamps each floor to its
    /// file's whole-record length. Run at open after the cut, so a floor a
    /// past run could not lower never outlives it. Whether anything changed.
    pub fn reconcile(&mut self, dir: &Path) -> bool {
        let mut present: HashMap<FileIdentity, u64> = HashMap::new();
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let Ok(meta) = entry.metadata() else { continue };
                if !meta.is_file() {
                    continue;
                }
                let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                if let Some(kind) = classify(dir, &name) {
                    present.insert(FileIdentity::of(&meta), kind.whole(meta.len()));
                }
            }
        }
        let before = self.clone();
        self.entries.retain(|id, (_, floor)| match present.get(id) {
            Some(&len) => {
                *floor = (*floor).min(len);
                true
            }
            None => false,
        });
        *self != before
    }

    /// Reads the floors at `path`. Missing or unreadable is no floors, never
    /// an error: a floor only ever refuses, so losing one costs a check, while
    /// refusing to open over a stray file would cost the database.
    pub fn load(path: &Path) -> Floors {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Floors::default(),
            Err(e) => {
                log::warn!("Could not read {}: {}; no durable floors are checked", path.display(), e);
                return Floors::default();
            }
        };
        match Self::parse(&text) {
            Some(f) => f,
            None => {
                log::warn!(
                    "{} is not a whole floors file; no durable floors are checked",
                    path.display()
                );
                Floors::default()
            }
        }
    }

    fn parse(text: &str) -> Option<Floors> {
        let body_end = text.rfind("end ")?;
        let (body, trailer) = text.split_at(body_end);
        let crc: u32 = trailer.strip_prefix("end ")?.trim().parse().ok()?;
        if CRC.checksum(body.as_bytes()) != crc {
            return None;
        }
        let mut lines = body.lines();
        if lines.next()? != FLOORS_MAGIC {
            return None;
        }
        let mut entries = HashMap::new();
        for line in lines {
            let mut parts = line.splitn(5, ' ');
            let dev = parts.next()?.parse().ok()?;
            let ino = parts.next()?.parse().ok()?;
            let born = parts.next()?.parse().ok()?;
            let floor = parts.next()?.parse().ok()?;
            let name = parts.next()?.to_string();
            entries.insert(FileIdentity { dev, ino, born }, (name, floor));
        }
        Some(Floors { entries })
    }

    fn render(&self) -> String {
        let mut rows: Vec<_> = self.entries.iter().collect();
        rows.sort_by(|a, b| a.1.0.cmp(&b.1.0));
        let mut body = format!("{FLOORS_MAGIC}\n");
        for (id, (name, floor)) in rows {
            body.push_str(&format!("{} {} {} {} {}\n", id.dev, id.ino, id.born, floor, name));
        }
        let crc = CRC.checksum(body.as_bytes());
        body.push_str(&format!("end {crc}\n"));
        body
    }

    /// Writes the floors to `path` durably: a temporary file, synced, renamed
    /// over it, and the directory synced.
    pub fn store(&self, path: &Path) -> io::Result<()> {
        let temp = path.with_extension(FLOORS_PARTIAL_EXTENSION);
        {
            let mut file = File::create(&temp)?;
            file.write_all(self.render().as_bytes())?;
            crate::core::sync_audit::note("wal.floors", path);
            file.sync_all()?;
        }
        std::fs::rename(&temp, path)?;
        crate::core::wal::sync_parent_dir(path)
    }
}

/// Lowers `path`'s floor in `floors`, the floors of the log at `wal_path`,
/// to `len` cut back to whole records, and stores them if that changed it.
pub fn lower_floor_in(floors: &mut Floors, wal_path: &Path, path: &Path, len: u64) -> io::Result<()> {
    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let to = classify_path(path).map_or(len, |kind| kind.whole(len));
    if floors.lower(FileIdentity::of(&meta), to) {
        floors.store(&Floors::path_for(wal_path))?;
    }
    Ok(())
}

/// [`lower_floor_in`] on the floors stored beside `wal_path`, for a directory
/// whose log is not installed. Serialised process-wide: two shrinks must not
/// each store the floors with only their own change.
pub fn lower_floor_on_disk(wal_path: &Path, path: &Path, len: u64) -> io::Result<()> {
    static ON_DISK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    let at = Floors::path_for(wal_path);
    if !at.exists() {
        return Ok(());
    }
    let _serial = ON_DISK.lock();
    let mut floors = Floors::load(&at);
    lower_floor_in(&mut floors, wal_path, path, len)
}

/// Appended to the log's file name to name its floors.
pub const FLOORS_SUFFIX: &str = ".floors";

/// [`Floors::store`]'s temporary file, as `Path::with_extension` takes it.
pub const FLOORS_PARTIAL_EXTENSION: &str = "floors.partial";

#[cfg(test)]
#[path = "torn_tail_tests.rs"]
mod tests;
