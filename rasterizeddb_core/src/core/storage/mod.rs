//! Multiple storage directories for one database, and the policies that decide
//! which of them a table's files are created in.
//!
//! A single-directory database is the degenerate case of this: a pool holding
//! only its own location, no backups, and the `primary` policy. Nothing here
//! changes what such a database does, which is why the whole subsystem sits
//! behind the off-by-default `storage_pools` feature.
//!
//! # The model
//!
//! A pool is an ordered list of [`StorageLocation`]s. The first is always the
//! database's own directory and cannot be removed — it holds the catalogue, the
//! transaction status log, and this pool's own configuration file, so a
//! database with no first location is not a database.
//!
//! Each location carries a [`LocationRole`]:
//!
//! * `PRIMARY` — eligible to hold a table's files. Which primary a new table
//!   lands in is what a [`PlacementPolicy`] decides.
//! * `BACKUP` — receives a mirrored copy of every write. Not eligible for
//!   placement, so a table is never *only* on a backup.
//!
//! The two roles compose rather than being alternatives: "primary + backup" is
//! one primary and one backup, and "balance across four disks, all mirrored to
//! a fifth" is four primaries and one backup. There is no separate mode switch
//! for either.
//!
//! # Policies
//!
//! * [`PlacementPolicy::Primary`] — everything in the first enabled primary.
//!   The behaviour of a database that has never been told about a second
//!   directory.
//! * [`PlacementPolicy::RoundRobin`] — the enabled primary currently holding
//!   the fewest tables, ties broken by declaration order. Implemented by
//!   counting rather than by a rotating cursor so that it survives a restart:
//!   a cursor would reset to zero and pile the next several tables into the
//!   first directory.
//! * [`PlacementPolicy::FreeSpace`] — the enabled primary with the most
//!   available bytes, scaled by the location's weight. This is the one that
//!   reacts to a disk filling up, and it is re-evaluated per `CREATE TABLE`
//!   rather than cached.
//!
//! A location whose free space is below `min_free_bytes` is skipped entirely —
//! unless every location is, in which case the least-bad one is used and a
//! warning is logged. Refusing to create the table would be the other option
//! and is worse: the operator loses a table because a *different* directory is
//! full.
//!
//! # Where a table actually is
//!
//! Placement is recorded, not recomputed. `db_storage.cfg` in the database
//! directory holds the locations, the policy, and one line per placed table.
//! Recomputing on open would be a disaster in slow motion: change the policy,
//! or add a disk, and every existing table would be looked for in a directory
//! its files are not in — reported not as an error but as an empty table.
//!
//! Tables with no line are in the database directory, which is what every table
//! written before this feature existed is, so an existing installation opens
//! unchanged.

#[cfg(all(test, feature = "kv_storage"))]
mod backend_tests;
#[cfg(feature = "kv_storage")]
pub mod kv_registry;
pub mod kv_sql;
pub mod mirror;
#[cfg(test)]
mod placement_tests;
pub mod relocate;
pub mod sql;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use log::{error, info, warn};
use parking_lot::{Mutex, RwLock};

/// The file, in the database directory, that records the pool.
pub const POOL_FILE: &str = "db_storage.cfg";

/// Format marker, so a later layout change is recognisable rather than
/// mis-parsed.
const POOL_FORMAT_VERSION: u32 = 1;

/// Default floor on free space before a location stops being chosen: 256 MiB.
///
/// Not zero. A directory with a few kilobytes left is technically eligible and
/// the table created there fails on its first insert, which is a worse outcome
/// than having been placed elsewhere.
pub const DEFAULT_MIN_FREE_BYTES: u64 = 256 * 1024 * 1024;

/// What a location is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocationRole {
    /// Holds table files. Candidates for placement come from these.
    Primary,
    /// Receives a mirrored copy of every write. Never chosen for placement.
    Backup,
}

impl LocationRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Backup => "backup",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "primary" | "data" => Some(Self::Primary),
            "backup" | "mirror" | "replica" => Some(Self::Backup),
            _ => None,
        }
    }
}

/// How a new table's directory is chosen from the enabled primaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementPolicy {
    /// The first enabled primary, always.
    Primary,
    /// The enabled primary holding the fewest tables.
    RoundRobin,
    /// The enabled primary with the most available bytes, weighted.
    FreeSpace,
}

impl PlacementPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::RoundRobin => "round-robin",
            Self::FreeSpace => "free-space",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "primary" | "first" | "none" => Some(Self::Primary),
            "round-robin" | "roundrobin" | "rr" | "least-tables" => Some(Self::RoundRobin),
            "free-space" | "freespace" | "balance" | "space" | "available" => Some(Self::FreeSpace),
            _ => None,
        }
    }

    /// Every accepted spelling, for the error message a bad one produces.
    pub fn names() -> &'static str {
        "primary, round-robin, free-space"
    }
}

/// One directory in the pool.
#[derive(Debug, Clone)]
pub struct StorageLocation {
    /// Absolute, with any trailing separator trimmed. Used verbatim: a
    /// non-default database does *not* get an implicit subdirectory of its own
    /// under an added location, because guessing one would put files somewhere
    /// the operator did not name.
    pub path: String,
    pub role: LocationRole,
    /// Multiplies this location's score under `free-space`. A location with
    /// weight 2 is chosen while it has half as much free space as one with
    /// weight 1 — the knob for a fast disk that should take more than its share.
    pub weight: u32,
    /// A disabled location keeps every table already placed in it — those files
    /// stay open and readable — and takes no new ones. It is how a disk is
    /// drained before it is removed.
    pub enabled: bool,
}

impl StorageLocation {
    fn root(path: String) -> Self {
        Self {
            path,
            role: LocationRole::Primary,
            weight: 1,
            enabled: true,
        }
    }
}

/// What `SHOW STORAGE LOCATIONS` reports for one location.
#[derive(Debug, Clone)]
pub struct LocationReport {
    pub location: StorageLocation,
    /// True for the database's own directory, which cannot be dropped.
    pub is_root: bool,
    pub tables: u64,
    /// `None` when the filesystem could not be interrogated — a permission
    /// error, or a platform with no implementation here. Reported as unknown
    /// rather than as zero, which would read as "full".
    pub available_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
}

/// What holds a table's files.
///
/// A placement used to be a directory and nothing else. It is now a *backend*,
/// of which a directory is one kind — which is how one database can hold a
/// file-backed table, a scratch table that lives only in memory, and a table
/// kept in an object store, side by side.
///
/// The spec is what `db_storage.cfg` stores. Anything without a recognised
/// scheme is a directory, so every file written before this existed still parses
/// as what it meant, and a Windows path is not mistaken for a scheme.
///
/// **A table that is not file-backed does not get the file-backed features.**
/// It is not in a backup, it cannot be moved between disks, it has no
/// write-ahead log, and the free-space policy does not consider it. Those
/// features are about files, and inventing an equivalent for each backend would
/// mean three half-working versions of each instead of one that works.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableBackend {
    /// Files in a directory. The only backend before this existed.
    Directory(String),
    /// Buffers in this process. Gone when the server stops.
    Memory,
    /// Objects in a named key/value store — see `db_kv.cfg`.
    KeyValue(String),
}

/// Spec prefix for an in-memory table.
pub const MEMORY_SPEC: &str = "memory:";
/// Spec prefix for a key/value table, followed by the store's name.
pub const KV_SPEC_PREFIX: &str = "kv:";

impl TableBackend {
    /// Reads a placement spec. Anything unrecognised is a directory.
    pub fn parse(spec: &str) -> Self {
        let trimmed = spec.trim();
        if trimmed.eq_ignore_ascii_case(MEMORY_SPEC) || trimmed.eq_ignore_ascii_case("memory") {
            return TableBackend::Memory;
        }
        // Case-insensitive on the scheme but not on the store's name, which is
        // an identifier the operator chose.
        if trimmed.len() > KV_SPEC_PREFIX.len()
            && trimmed[..KV_SPEC_PREFIX.len()].eq_ignore_ascii_case(KV_SPEC_PREFIX)
        {
            let name = trimmed[KV_SPEC_PREFIX.len()..].trim();
            if !name.is_empty() {
                return TableBackend::KeyValue(name.to_string());
            }
        }
        TableBackend::Directory(normalize(trimmed))
    }

    /// How this is written to `db_storage.cfg`.
    pub fn as_spec(&self) -> String {
        match self {
            TableBackend::Directory(path) => path.clone(),
            TableBackend::Memory => MEMORY_SPEC.to_string(),
            TableBackend::KeyValue(name) => format!("{KV_SPEC_PREFIX}{name}"),
        }
    }

    /// The directory, for the backend that has one.
    pub fn directory(&self) -> Option<&str> {
        match self {
            TableBackend::Directory(path) => Some(path),
            _ => None,
        }
    }

    /// Whether the file-backed features apply: backup, relocation, the log, and
    /// the free-space policy.
    pub fn is_file_based(&self) -> bool {
        matches!(self, TableBackend::Directory(_))
    }

    /// A short name for error messages.
    pub fn kind(&self) -> &'static str {
        match self {
            TableBackend::Directory(_) => "directory",
            TableBackend::Memory => "in-memory",
            TableBackend::KeyValue(_) => "key/value",
        }
    }
}

/// The mutable half of a pool, behind one lock.
#[derive(Debug)]
struct PoolState {
    policy: PlacementPolicy,
    min_free_bytes: u64,
    locations: Vec<StorageLocation>,
    /// Table name to location path. Absent means the database directory.
    placements: HashMap<String, String>,
}

/// A database's storage pool.
///
/// Always present on a [`crate::core::database::Database`] and inert for an
/// in-memory one, so the placement call sites need no branch of their own —
/// the same shape replication uses.
#[derive(Debug)]
pub struct StorageManager {
    /// The database directory, or `None` when the database is in memory. An
    /// in-memory database has no pool and every method here is a no-op for it.
    root: Option<String>,
    state: RwLock<PoolState>,
    /// Held across the whole of [`StorageManager::persist`] — render, write,
    /// rename — so only one of them is ever in flight.
    ///
    /// Separate from `state`, and not a substitute for it: `persist` only
    /// *reads* the state, so serialising it through the write lock would block
    /// every reader for the duration of a write and an fsync.
    writing: Mutex<()>,
}

impl StorageManager {
    /// A pool that does nothing, for an in-memory database.
    pub fn inert() -> Self {
        Self {
            root: None,
            writing: Mutex::new(()),
            state: RwLock::new(PoolState {
                policy: PlacementPolicy::Primary,
                min_free_bytes: DEFAULT_MIN_FREE_BYTES,
                locations: Vec::new(),
                placements: HashMap::new(),
            }),
        }
    }

    /// Opens the pool for the database at `root`, reading `db_storage.cfg` if
    /// there is one.
    ///
    /// A configuration that cannot be read is logged and treated as absent,
    /// which degrades to a single-directory database — every table then
    /// resolves to the root, and any table that is genuinely elsewhere reads as
    /// missing. That is loud in a way silently inventing placements is not.
    ///
    /// Panics where [`StorageManager::try_open`] errs, as a fatal startup path.
    pub fn open(root: &str) -> Self {
        Self::try_open(root).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`StorageManager::open`], refusing with an error rather than a panic
    /// when a location's log cannot be replayed. A log this call installed for
    /// another location is uninstalled again on that error: nothing will use
    /// it, and a registered log makes the next open skip that recovery.
    pub fn try_open(root: &str) -> Result<Self, String> {
        let root = normalize(root);
        let manager = Self {
            root: Some(root.clone()),
            writing: Mutex::new(()),
            state: RwLock::new(PoolState {
                policy: PlacementPolicy::Primary,
                min_free_bytes: DEFAULT_MIN_FREE_BYTES,
                locations: vec![StorageLocation::root(root.clone())],
                placements: HashMap::new(),
            }),
        };

        let path = Path::new(&root).join(POOL_FILE);
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                warn_if_written_elsewhere(&text, &root, &path);
                let mut state = manager.state.write();
                if let Err(e) = parse_into(&text, &root, &mut state) {
                    error!(
                        "{} is not a storage pool configuration ({}); \
                         treating this database as single-directory",
                        path.display(),
                        e
                    );
                    *state = PoolState {
                        policy: PlacementPolicy::Primary,
                        min_free_bytes: DEFAULT_MIN_FREE_BYTES,
                        locations: vec![StorageLocation::root(root.clone())],
                        placements: HashMap::new(),
                    };
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => error!("could not read {}: {}", path.display(), e),
        }

        let unlogged: Vec<String> = manager
            .all_paths()
            .into_iter()
            .skip(1)
            .filter(|path| crate::core::wal::wal_for(path).is_none())
            .collect();
        if let Err(e) = manager.prepare_locations() {
            for path in &unlogged {
                let _ = crate::core::wal::remove_for(path);
            }
            return Err(e);
        }
        Ok(manager)
    }

    /// Whether this pool can place anything anywhere but the root.
    pub fn is_active(&self) -> bool {
        let state = self.state.read();
        state.locations.len() > 1
    }

    pub fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }

    /// What `path` is for, or `None` when it is not a location of this pool.
    pub fn role_of(&self, path: &str) -> Option<LocationRole> {
        let path = normalize(path);
        self.state
            .read()
            .locations
            .iter()
            .find(|l| l.path == path)
            .map(|l| l.role)
    }

    /// Whether `path` is enabled. `None` when it is not a location at all.
    pub fn is_enabled(&self, path: &str) -> Option<bool> {
        let path = normalize(path);
        self.state
            .read()
            .locations
            .iter()
            .find(|l| l.path == path)
            .map(|l| l.enabled)
    }

    pub fn policy(&self) -> PlacementPolicy {
        self.state.read().policy
    }

    pub fn min_free_bytes(&self) -> u64 {
        self.state.read().min_free_bytes
    }

    /// Creates every location's directory and gives it a write-ahead log.
    ///
    /// The log matters more than it looks. Every file this engine writes takes
    /// its durability from `wal_for(<its directory>)`, so a table in a
    /// directory with no log falls back to an `fsync` per append — correct, and
    /// roughly an order of magnitude slower than the group commit the root
    /// enjoys. Recovery runs here too, for the same reason it runs for the
    /// root: a log on disk means writes that may not have reached their files.
    ///
    /// A location whose log recovery refuses fails the whole call; see
    /// [`recover_location`] for why carrying on without it is not an option.
    fn prepare_locations(&self) -> Result<(), String> {
        let extra: Vec<String> = {
            let state = self.state.read();
            state
                .locations
                .iter()
                .skip(1)
                .map(|location| location.path.clone())
                .collect()
        };

        for path in extra {
            if let Err(e) = std::fs::create_dir_all(&path) {
                error!("storage location {} is unusable: {}", path, e);
                continue;
            }
            recover_location(&path)?;
        }
        Ok(())
    }

    /// Every directory this pool writes into, root first.
    ///
    /// What `Database::sync_durable` walks: a commit that touched a table on a
    /// second disk is not durable until *that* disk's log is flushed, and the
    /// root's log knows nothing about it.
    pub fn all_paths(&self) -> Vec<String> {
        self.state
            .read()
            .locations
            .iter()
            .map(|l| l.path.clone())
            .collect()
    }

    /// The enabled backup locations, which every write is mirrored to.
    pub fn mirrors(&self) -> Vec<String> {
        self.state
            .read()
            .locations
            .iter()
            .filter(|l| l.enabled && l.role == LocationRole::Backup)
            .map(|l| l.path.clone())
            .collect()
    }

    /// Where `table_name`'s files are, or `None` for the database directory.
    ///
    /// Directory placements only. A table on another backend has no directory,
    /// and handing its spec back here would have every caller that treats this
    /// as a path build one out of `kv:main`.
    pub fn location_of(&self, table_name: &str) -> Option<String> {
        self.backend_of(table_name)?.directory().map(str::to_string)
    }

    /// What holds `table_name`, or `None` when nothing is recorded — which
    /// means the database directory.
    pub fn backend_of(&self, table_name: &str) -> Option<TableBackend> {
        self.state
            .read()
            .placements
            .get(table_name)
            .map(|spec| TableBackend::parse(spec))
    }

    /// The recorded spec verbatim, for the paths that carry it around rather
    /// than act on it.
    pub fn spec_of(&self, table_name: &str) -> Option<String> {
        self.state.read().placements.get(table_name).cloned()
    }

    /// Records a backend for a table without consulting the policy.
    ///
    /// The counterpart of [`StorageManager::pin`] for the backends that are not
    /// directories. A key/value store's name is not validated here — the pool
    /// does not own `db_kv.cfg` — so the caller checks it exists first, or the
    /// table opens against a store that is not configured.
    pub fn pin_backend(&self, table_name: &str, backend: &TableBackend) -> Result<(), String> {
        if let TableBackend::Directory(path) = backend {
            return self.pin(table_name, path);
        }
        if self.root.is_none() {
            return Err("this database is in memory".to_string());
        }
        self.state
            .write()
            .placements
            .insert(table_name.to_string(), backend.as_spec());
        self.persist();
        Ok(())
    }

    /// Decides where a table about to be created goes, records it, and returns
    /// the directory.
    ///
    /// Recording happens here rather than after the files are written, because
    /// a crash between the two must leave the *placement* pointing at the
    /// directory the half-written files are in. The other order strands them:
    /// the table would be looked for in a directory it was never created in.
    pub fn place(&self, table_name: &str) -> Option<String> {
        let root = self.root.as_deref()?;

        let chosen = {
            let mut state = self.state.write();
            if let Some(existing) = state.placements.get(table_name) {
                // A table already pinned to another backend has no directory to
                // report, and choosing one for it would overwrite the pin.
                return TableBackend::parse(existing)
                    .directory()
                    .map(str::to_string);
            }
            let chosen = choose(&state, root);
            // Recorded even when it is the database's own directory. The
            // alternative — recording only the exceptions — makes the map an
            // undercount of the root, and `round-robin` reads those counts: the
            // root would look permanently empty and take every table.
            state
                .placements
                .insert(table_name.to_string(), chosen.clone());
            chosen
        };

        self.persist();
        info!("table {} placed in {}", table_name, chosen);
        Some(chosen)
    }

    /// Pins a table to `path` without consulting the policy.
    ///
    /// Only records the intent — it does not move files. Used by `STORAGE MOVE
    /// TABLE`, which moves them first and flips this last, and by
    /// `STORAGE PLACE TABLE`, whose whole point is to be applied before the
    /// table exists.
    pub fn pin(&self, table_name: &str, path: &str) -> Result<(), String> {
        let root = self.root.as_deref().ok_or("this database is in memory")?;
        let path = normalize(path);

        {
            let mut state = self.state.write();
            if !state.locations.iter().any(|l| l.path == path) {
                return Err(format!(
                    "{} is not a storage location of this database",
                    path
                ));
            }
            // The root is recorded like any other location — see `place`.
            let _ = root;
            state.placements.insert(table_name.to_string(), path);
        }

        self.persist();
        Ok(())
    }

    /// Drops a table's placement, after its files are gone.
    pub fn forget(&self, table_name: &str) {
        let removed = self.state.write().placements.remove(table_name).is_some();
        if removed {
            self.persist();
        }
    }

    /// Carries a placement across a rename. A renamed table does not move.
    pub fn rename(&self, old_name: &str, new_name: &str) {
        let moved = {
            let mut state = self.state.write();
            match state.placements.remove(old_name) {
                Some(path) => {
                    state.placements.insert(new_name.to_string(), path);
                    true
                }
                None => false,
            }
        };
        if moved {
            self.persist();
        }
    }

    // -- configuration ----------------------------------------------------

    pub fn add_location(
        &self,
        path: &str,
        role: LocationRole,
        weight: u32,
        enabled: bool,
    ) -> Result<String, String> {
        let root = self.root.as_deref().ok_or("this database is in memory")?;
        let path = normalize(path);

        if path.is_empty() {
            return Err("a storage location needs a path".to_string());
        }
        if !Path::new(&path).is_absolute() {
            return Err(format!(
                "{} is relative; a storage location must be an absolute path, \
                 since the server's working directory is not part of its configuration",
                path
            ));
        }
        if weight == 0 {
            return Err(
                "weight must be at least 1; use DISABLE to take a location out".to_string(),
            );
        }
        if path == root {
            return Err(format!(
                "{} is this database's own directory and is always in the pool",
                path
            ));
        }

        // Nesting is refused rather than merely discouraged: a location inside
        // another means one directory's backup lives on the same filesystem as
        // the data, and a `DROP` sweep of the outer one takes the inner with it.
        {
            let state = self.state.read();
            if let Some(clash) = state.locations.iter().find(|l| l.path == path) {
                return Err(format!(
                    "{} is already a storage location ({})",
                    clash.path,
                    clash.role.as_str()
                ));
            }
            if let Some(clash) = state
                .locations
                .iter()
                .find(|l| contains_path(&l.path, &path) || contains_path(&path, &l.path))
            {
                return Err(format!(
                    "{} overlaps the existing storage location {}",
                    path, clash.path
                ));
            }
        }

        std::fs::create_dir_all(&path)
            .map_err(|e| format!("{} cannot be used as a storage location: {}", path, e))?;

        // Writability is checked now, not at the first `CREATE TABLE`. A
        // location added successfully and unusable later is a support call; one
        // that refuses on the spot names its own problem.
        let probe = Path::new(&path).join(".rdb_write_probe");
        std::fs::write(&probe, b"rdb")
            .map_err(|e| format!("{} is not writable by this server: {}", path, e))?;
        let _ = std::fs::remove_file(&probe);

        // Before the location is recorded: a directory holding a log that
        // cannot be replayed is refused here, not at the next restart.
        recover_location(&path)?;

        self.state.write().locations.push(StorageLocation {
            path: path.clone(),
            role,
            weight,
            enabled,
        });
        self.persist();
        self.prepare_locations().map_err(|e| {
            format!("{path} was added to the pool, but another location is unusable: {e}")
        })?;
        Ok(path)
    }

    pub fn drop_location(&self, path: &str) -> Result<String, String> {
        let root = self.root.as_deref().ok_or("this database is in memory")?;
        let path = normalize(path);

        if path == root {
            return Err("the database's own directory cannot be removed from the pool".to_string());
        }

        let mut state = self.state.write();
        let Some(index) = state.locations.iter().position(|l| l.path == path) else {
            return Err(format!(
                "{} is not a storage location of this database",
                path
            ));
        };

        // A location still holding tables would take its files with it —
        // silently, since nothing reads a directory the pool no longer names.
        let holding: Vec<&String> = state
            .placements
            .iter()
            .filter(|(_, at)| **at == path)
            .map(|(table, _)| table)
            .collect();
        if !holding.is_empty() {
            let mut names: Vec<&str> = holding.iter().map(|s| s.as_str()).collect();
            names.sort_unstable();
            return Err(format!(
                "{} still holds {} table(s): {}. Move them with \
                 STORAGE MOVE TABLE <name> TO '<path>' first, or DISABLE the location \
                 to stop new tables landing there",
                path,
                names.len(),
                names.join(", ")
            ));
        }

        state.locations.remove(index);
        drop(state);
        self.persist();
        Ok(path)
    }

    pub fn set_enabled(&self, path: &str, enabled: bool) -> Result<String, String> {
        self.alter(path, |location| location.enabled = enabled)
    }

    pub fn set_role(&self, path: &str, role: LocationRole) -> Result<String, String> {
        let root = self.root.as_deref().ok_or("this database is in memory")?;
        let normalized = normalize(path);
        if normalized == root && role == LocationRole::Backup {
            return Err(
                "the database's own directory holds the catalogue and cannot be a backup"
                    .to_string(),
            );
        }
        // Demoting a primary that still holds tables would leave them on a
        // location nothing places into — which is fine — but promoting is the
        // dangerous direction and neither is worth blocking. What is worth
        // blocking is turning a location holding tables into a backup: its
        // files would then be mirrored *onto*, over rows nothing else knows
        // about.
        {
            let state = self.state.read();
            if role == LocationRole::Backup
                && state
                    .placements
                    .values()
                    .any(|spec| TableBackend::parse(spec).directory() == Some(normalized.as_str()))
            {
                return Err(format!(
                    "{} still holds tables; a backup location is written by mirroring \
                     and would overwrite them",
                    normalized
                ));
            }
        }
        self.alter(path, |location| location.role = role)
    }

    pub fn set_weight(&self, path: &str, weight: u32) -> Result<String, String> {
        if weight == 0 {
            return Err("weight must be at least 1".to_string());
        }
        self.alter(path, |location| location.weight = weight)
    }

    fn alter(
        &self,
        path: &str,
        change: impl FnOnce(&mut StorageLocation),
    ) -> Result<String, String> {
        let path = normalize(path);
        {
            let mut state = self.state.write();
            let Some(location) = state.locations.iter_mut().find(|l| l.path == path) else {
                return Err(format!(
                    "{} is not a storage location of this database",
                    path
                ));
            };
            change(location);
        }
        self.persist();
        Ok(path)
    }

    pub fn set_policy(&self, policy: PlacementPolicy) {
        self.state.write().policy = policy;
        self.persist();
    }

    pub fn set_min_free_bytes(&self, bytes: u64) {
        self.state.write().min_free_bytes = bytes;
        self.persist();
    }

    // -- reporting --------------------------------------------------------

    pub fn locations(&self) -> Vec<LocationReport> {
        let state = self.state.read();
        let root = self.root.as_deref().unwrap_or("");
        state
            .locations
            .iter()
            .map(|location| {
                // Every table placed since this pool file existed is in the
                // map, the root's included. A table older than the file is not,
                // and resolves to the root — so the root's count here is a
                // floor, which `SHOW STORAGE` corrects against the live table
                // list, the only thing that knows about those.
                let tables = state
                    .placements
                    .values()
                    .filter(|spec| {
                        TableBackend::parse(spec).directory() == Some(location.path.as_str())
                    })
                    .count() as u64;
                LocationReport {
                    location: location.clone(),
                    is_root: location.path == root,
                    tables,
                    available_bytes: available_bytes(&location.path),
                    total_bytes: total_bytes(&location.path),
                }
            })
            .collect()
    }

    /// Every recorded placement, as `(table, location)`, sorted by table.
    pub fn placements(&self) -> Vec<(String, String)> {
        let state = self.state.read();
        let mut out: Vec<(String, String)> = state
            .placements
            .iter()
            .map(|(table, at)| (table.clone(), at.clone()))
            .collect();
        out.sort_unstable();
        out
    }

    /// Where the policy would put a table created now. Used by `STORAGE
    /// REBALANCE DRY RUN` and by the `SHOW STORAGE` summary, neither of which
    /// should have a side effect.
    pub fn would_place(&self) -> Option<String> {
        let root = self.root.as_deref()?;
        let state = self.state.read();
        Some(choose(&state, root))
    }

    // -- persistence ------------------------------------------------------

    /// Writes the pool to `db_storage.cfg`.
    ///
    /// Through a temporary file and a rename, so a crash mid-write leaves the
    /// previous configuration rather than a truncated one. A truncated pool
    /// file is not a lost setting: it is every table on a second disk becoming
    /// unfindable at the next start-up.
    ///
    /// One at a time, and the lock spans the render as well as the write. Every
    /// caller here releases the state lock before calling this, so two
    /// placements landing together — which is what a table-creation storm is —
    /// used to reach the same fixed temporary path concurrently. Both wrote it,
    /// the first rename moved it away, and the second got
    /// `No such file or directory`: one isolated error line, seen in production
    /// on 2026-08-23, with that pool's newer state left only in memory.
    ///
    /// The lost update was the smaller half. `fs::write` truncates before it
    /// writes, so the rename could equally publish the *other* persist's file
    /// half-written — the truncated configuration this method's temporary file
    /// exists to prevent, arriving through the back door.
    ///
    /// Unique temporary names would have stopped the `ENOENT` without fixing
    /// that, and would have let an older render win the last rename. Holding a
    /// lock across render-write-rename makes the file always some persist's
    /// whole output, and always the newest one to have rendered.
    fn persist(&self) {
        let Some(root) = self.root.as_deref() else {
            return;
        };

        let _one_at_a_time = self.writing.lock();
        let text = self.render();
        let final_path = Path::new(root).join(POOL_FILE);
        let temp_path: PathBuf = Path::new(root).join(format!("{}.tmp", POOL_FILE));

        if let Err(e) = std::fs::write(&temp_path, text.as_bytes()) {
            error!("could not write {}: {}", temp_path.display(), e);
            return;
        }
        // The bytes, then the directory entry. Renaming over a file whose
        // contents are still only in the page cache would survive a crash as an
        // empty configuration.
        if let Ok(file) = std::fs::File::open(&temp_path) {
            let _ = file.sync_all();
        }
        if let Err(e) = std::fs::rename(&temp_path, &final_path) {
            error!(
                "could not replace {}: {}. The placements this database is running with are \
                 newer than the ones on disk; tables outside its own directory may be \
                 unfindable if it restarts before the next successful write",
                final_path.display(),
                e
            );
            let _ = std::fs::remove_file(&temp_path);
            return;
        }
        let _ = crate::core::wal::sync_parent_dir(&final_path);
    }

    fn render(&self) -> String {
        let state = self.state.read();

        let mut out = String::new();
        out.push_str(
            "# rasterizeddb storage pool. Rewritten by the server; edit while it is stopped.\n",
        );
        out.push_str(&format!("version\t{}\n", POOL_FORMAT_VERSION));
        out.push_str(&format!("policy\t{}\n", state.policy.as_str()));
        out.push_str(&format!("min_free_bytes\t{}\n", state.min_free_bytes));

        // The root is written too, even though `open` recreates it from the
        // directory it read the file out of. It is what makes the file
        // self-describing: an operator looking at a pool file on a backup disk
        // can tell which database it belongs to.
        for location in state.locations.iter() {
            out.push_str(&format!(
                "location\t{}\t{}\t{}\t{}\n",
                location.role.as_str(),
                location.weight,
                if location.enabled {
                    "enabled"
                } else {
                    "disabled"
                },
                location.path,
            ));
        }

        let mut placements: Vec<(&String, &String)> = state.placements.iter().collect();
        placements.sort_unstable();
        for (table, at) in placements {
            out.push_str(&format!("table\t{}\t{}\n", at, table));
        }

        out
    }
}

/// Rewrites a restored `db_storage.cfg` so it describes the directory it was
/// restored *into*.
///
/// A pool file names absolute paths, and a restore copies it verbatim. Opened
/// from a new directory it therefore still names the source's, so every table
/// the pool has a placement for — with the default single-location pool, every
/// table that has ever been created — resolves back to the live database's
/// files. The restored copy is then never read at all: a query run to verify a
/// restore answers from the source, and a write against the clone lands in the
/// original. Both failures are silent, and the first one makes a backup look
/// good by consulting the thing it was supposed to be a copy of.
///
/// The source root becomes `target`. Any other location is dropped, along with
/// the tables placed on it: those files are on another disk, and a snapshot
/// only ever walks the database directory, so they are not in the backup under
/// any reading. Dropping them makes the restored database resolve those tables
/// to its own root and report them as missing — which is loud, where reading
/// the source's copy is not.
///
/// `written_in` is the directory holding the file — a restore stages its work
/// in a sibling and renames it into place — and `final_root` is where that
/// directory will be once it is published, which is what the paths must name.
///
/// `source_root` is what the catalogue recorded as the database's directory.
/// Without it the first `location` line stands in, which [`Pool::render`]
/// always writes as the root.
///
/// Returns the tables it dropped, for the caller to report.
pub fn repoint_pool_config(
    written_in: &Path,
    final_root: &Path,
    source_root: Option<&str>,
) -> io::Result<Vec<String>> {
    let path = written_in.join(POOL_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    // A restore is built in a staging directory and renamed into place, so the
    // paths written here are the ones it will have *after* that rename.
    let target_path = normalize(&final_root.to_string_lossy());

    let old_root = match source_root {
        Some(root) => normalize(root),
        None => match first_location(&text) {
            Some(root) => root,
            // Nothing to repoint: a file with no locations places nothing.
            None => return Ok(Vec::new()),
        },
    };

    // An in-place restore lands where it came from, and every path in the file
    // is still true. Rewriting would only drop placements this host can serve.
    if old_root == target_path {
        return Ok(Vec::new());
    }

    let mut out = String::new();
    let mut dropped = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() || line.starts_with('#') {
            out.push_str(line);
            out.push('\n');
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        match fields.as_slice() {
            ["location", role, weight, enabled, path] => {
                if normalize(path) == old_root {
                    out.push_str(&format!(
                        "location\t{role}\t{weight}\t{enabled}\t{target_path}\n"
                    ));
                }
                // Any other disk: dropped, and its tables with it below.
            }
            ["table", at, table] => {
                if normalize(at) == old_root {
                    out.push_str(&format!("table\t{target_path}\t{table}\n"));
                } else {
                    dropped.push((*table).to_string());
                }
            }
            _ => {
                out.push_str(line);
                out.push('\n');
            }
        }
    }

    // Through a temporary file and a rename, for the reason `persist` gives.
    let temp = written_in.join(format!("{}.tmp", POOL_FILE));
    std::fs::write(&temp, out.as_bytes())?;
    if let Ok(file) = std::fs::File::open(&temp) {
        let _ = file.sync_all();
    }
    std::fs::rename(&temp, &path)?;
    let _ = crate::core::wal::sync_parent_dir(&path);

    dropped.sort();
    dropped.dedup();
    Ok(dropped)
}

/// Says so when a pool file was written for a different directory.
///
/// [`Pool::render`] always writes the root among the locations, so a file none
/// of whose locations is the directory it was just read from was written for
/// somewhere else and copied here — a mirrored directory opened as a database,
/// an rsync of a data directory, a restore taken by a build that predates
/// [`repoint_pool_config`]. Its placements then send this database's tables
/// back to the original's files: reads answer from there, and writes land
/// there. Both are silent, and the reader has every reason to believe it is
/// looking at the copy.
///
/// Only a warning. Rewriting an operator's configuration because it looks
/// surprising would be the wrong side to err on — a pool whose root is
/// genuinely absent from its own locations is also what an operator gets while
/// hand-editing one — but nothing that reads this file should have to guess
/// afterwards why the numbers came from another directory.
fn warn_if_written_elsewhere(text: &str, root: &str, path: &Path) {
    let mut locations = text
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(
            |line| match line.split('\t').collect::<Vec<_>>().as_slice() {
                ["location", _, _, _, at] => Some(normalize(at)),
                _ => None,
            },
        );

    let mut any = false;
    let mut names_this_one = false;
    for at in &mut locations {
        any = true;
        if at == root {
            names_this_one = true;
            break;
        }
    }
    if !any || names_this_one {
        return;
    }

    warn!(
        "{} was written for a different directory and names none of {}. Tables it places will \
         resolve to the directory it came from, so this database would read and write those \
         files instead of its own. If this is a copy — a mirror opened directly, or a directory \
         moved by hand — remove or repoint the file before using it.",
        path.display(),
        root
    );
}

/// The path on the first `location` line, which is the root the file was
/// written for.
fn first_location(text: &str) -> Option<String> {
    text.lines()
        .filter(|line| !line.starts_with('#'))
        .find_map(
            |line| match line.split('\t').collect::<Vec<_>>().as_slice() {
                ["location", _, _, _, path] => Some(normalize(path)),
                _ => None,
            },
        )
}

/// Applies a rendered pool file to `state`.
///
/// Tab-separated rather than a serialisation format: paths contain spaces and
/// almost never tabs, the file has to stay hand-readable for an operator
/// recovering a database onto different mount points, and this is a dozen lines
/// of parsing against a dependency and a derive.
fn parse_into(text: &str, root: &str, state: &mut PoolState) -> Result<(), String> {
    let mut locations: Vec<StorageLocation> = vec![StorageLocation::root(root.to_string())];
    let mut placements: HashMap<String, String> = HashMap::new();
    let mut policy = PlacementPolicy::Primary;
    let mut min_free_bytes = DEFAULT_MIN_FREE_BYTES;
    let mut version = None;

    for (number, line) in text.lines().enumerate() {
        let line = line.trim_end_matches(['\r', '\n']);
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split('\t');
        let keyword = fields.next().unwrap_or("");
        let at = |what: &str| format!("line {}: {} is malformed", number + 1, what);

        match keyword {
            "version" => {
                let value: u32 = fields
                    .next()
                    .and_then(|v| v.trim().parse().ok())
                    .ok_or_else(|| at("version"))?;
                if value > POOL_FORMAT_VERSION {
                    return Err(format!(
                        "this file is version {}, and this build understands up to {}",
                        value, POOL_FORMAT_VERSION
                    ));
                }
                version = Some(value);
            }
            "policy" => {
                let value = fields.next().ok_or_else(|| at("policy"))?;
                policy = PlacementPolicy::parse(value)
                    .ok_or_else(|| format!("line {}: unknown policy {:?}", number + 1, value))?;
            }
            "min_free_bytes" => {
                min_free_bytes = fields
                    .next()
                    .and_then(|v| v.trim().parse().ok())
                    .ok_or_else(|| at("min_free_bytes"))?;
            }
            "location" => {
                let role = fields.next().ok_or_else(|| at("location"))?;
                let weight = fields.next().ok_or_else(|| at("location"))?;
                let enabled = fields.next().ok_or_else(|| at("location"))?;
                let path = fields.next().ok_or_else(|| at("location"))?;
                let role = LocationRole::parse(role)
                    .ok_or_else(|| format!("line {}: unknown role {:?}", number + 1, role))?;
                let weight: u32 = weight.trim().parse().map_err(|_| at("location weight"))?;
                let path = normalize(path);
                if path == root {
                    // Already present as entry zero; its role and weight are
                    // fixed, so only the enabled flag is worth carrying back.
                    locations[0].enabled = enabled.trim() != "disabled";
                    continue;
                }
                locations.push(StorageLocation {
                    path,
                    role,
                    weight: weight.max(1),
                    enabled: enabled.trim() != "disabled",
                });
            }
            "table" => {
                let at_path = fields.next().ok_or_else(|| at("table"))?;
                // The name is the rest of the line, so a table name containing
                // a tab — which nothing can create, but a corrupted file may
                // hold — does not silently become a shorter name.
                let name = fields.collect::<Vec<_>>().join("\t");
                if name.is_empty() {
                    return Err(at("table"));
                }
                placements.insert(name, TableBackend::parse(at_path).as_spec());
            }
            other => {
                warn!(
                    "{}: ignoring unknown storage pool directive {:?}",
                    POOL_FILE, other
                );
            }
        }
    }

    if version.is_none() {
        return Err("no version line".to_string());
    }

    // A placement naming a directory the pool no longer lists is the one
    // inconsistency that loses data quietly, so it is reported and the table is
    // left pointing where it says. Dropping the placement would make the table
    // resolve to the root, where it would open as empty.
    for (table, spec) in placements.iter() {
        let Some(path) = TableBackend::parse(spec).directory().map(str::to_string) else {
            // Not a directory, so no location is expected to list it.
            continue;
        };
        if !locations.iter().any(|l| l.path == path) {
            warn!(
                "{} places table {} in {}, which is not a storage location; \
                 add it back before using that table",
                POOL_FILE, table, path
            );
        }
    }

    state.policy = policy;
    state.min_free_bytes = min_free_bytes;
    state.locations = locations;
    state.placements = placements;
    Ok(())
}

/// Picks a directory for a new table.
fn choose(state: &PoolState, root: &str) -> String {
    let candidates: Vec<&StorageLocation> = state
        .locations
        .iter()
        .filter(|l| l.enabled && l.role == LocationRole::Primary)
        .collect();

    if candidates.is_empty() {
        // Every primary disabled. The root is still a directory this database
        // owns, and a `CREATE TABLE` that fails because of a placement setting
        // is worse than one that lands somewhere defensible.
        warn!("no enabled primary storage location; placing in {}", root);
        return root.to_string();
    }
    if candidates.len() == 1 {
        return candidates[0].path.clone();
    }

    // Anything below the floor is out of the running. If that empties the list
    // the floor is ignored rather than the statement failed — see the module
    // docs.
    let roomy: Vec<&&StorageLocation> = candidates
        .iter()
        .filter(|l| available_bytes(&l.path).is_none_or(|free| free >= state.min_free_bytes))
        .collect();
    let pool: Vec<&&StorageLocation> = if roomy.is_empty() {
        warn!(
            "every storage location is below the {} byte floor; placing in the emptiest",
            state.min_free_bytes
        );
        candidates.iter().collect()
    } else {
        roomy
    };

    match state.policy {
        PlacementPolicy::Primary => pool[0].path.clone(),
        PlacementPolicy::RoundRobin => {
            // Fewest tables wins; declaration order breaks ties, which makes
            // the choice a function of state on disk rather than of how many
            // tables this process happens to have created.
            // Counts come from the placement map, which records every table
            // placed since the pool file existed — the root's included, which
            // is what makes this policy able to leave the root at all. Tables
            // older than the file have no line and are undercounted against the
            // root; the effect is a bias towards the other locations until they
            // catch up, which is the direction an operator adding a disk wants.
            let counted = |path: &str| state.placements.values().filter(|at| *at == path).count();
            pool.iter()
                .min_by_key(|l| counted(&l.path))
                .map(|l| l.path.clone())
                .unwrap_or_else(|| root.to_string())
        }
        PlacementPolicy::FreeSpace => pool
            .iter()
            .max_by_key(|l| {
                available_bytes(&l.path)
                    .unwrap_or(0)
                    .saturating_mul(l.weight.max(1) as u64)
            })
            .map(|l| l.path.clone())
            .unwrap_or_else(|| root.to_string()),
    }
}

/// Replays `path`'s log and installs it, or says why the location cannot be used.
///
/// A log recovery refuses (a contested file id, one that cannot be read) is
/// left on disk untouched, and carrying on without it is what must not happen:
/// writes would land on the bytes its records describe, and the replay that
/// runs once the cause is cleared would put the older records back over them.
/// The caller refuses instead.
fn recover_location(path: &str) -> Result<(), String> {
    if crate::core::wal::wal_for(path).is_some() {
        return Ok(());
    }
    let (wal, stats) = crate::core::wal::open_and_recover(path).map_err(|e| {
        format!(
            "storage location {path} cannot be used: WAL recovery failed ({e}). Its log \
             was left as it was; the writes it holds are not on disk until it replays. \
             Resolve the cause and retry — deleting the log discards those writes."
        )
    })?;
    if stats.applied > 0 || stats.rolled_back > 0 {
        info!(
            "WAL recovery in {}: applied {}, rolled back {}",
            path, stats.applied, stats.rolled_back
        );
    }
    if crate::wal_enabled() {
        crate::core::wal::install_for(path, wal);
    }
    Ok(())
}

/// Trims trailing separators so that `/mnt/b` and `/mnt/b/` are one location.
///
/// Without this the same directory could be added twice, and the second copy
/// would take placements the first could not find.
pub fn normalize(path: &str) -> String {
    let trimmed = path.trim().trim_matches('"').trim_matches('\'').trim();
    let trimmed = trimmed.trim_end_matches(['/', '\\']);
    if trimmed.is_empty() {
        // Everything was separators: this is the filesystem root, which is a
        // real directory and must not normalise to the empty string.
        return path.trim().chars().take(1).collect();
    }
    trimmed.to_string()
}

/// Whether `outer` contains `inner` as a directory.
///
/// Compared per path component, so `/data/db2` is not "inside" `/data/db`.
fn contains_path(outer: &str, inner: &str) -> bool {
    let outer = Path::new(outer);
    let inner = Path::new(inner);
    inner.starts_with(outer) && inner != outer
}

/// Bytes an unprivileged process can still write to the filesystem holding
/// `path`, or `None` when that cannot be determined.
#[cfg(unix)]
pub fn available_bytes(path: &str) -> Option<u64> {
    statvfs(path).map(|s| (s.f_bavail as u64).saturating_mul(s.f_frsize as u64))
}

/// Size of the filesystem holding `path`.
#[cfg(unix)]
pub fn total_bytes(path: &str) -> Option<u64> {
    statvfs(path).map(|s| (s.f_blocks as u64).saturating_mul(s.f_frsize as u64))
}

#[cfg(unix)]
fn statvfs(path: &str) -> Option<libc::statvfs> {
    let c_path = std::ffi::CString::new(path).ok()?;
    // SAFETY: `statvfs` writes the whole struct on success and reads only the
    // NUL-terminated path; the zeroed value is a valid initial state for a
    // plain-old-data struct of integers.
    let mut out: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut out) };
    (rc == 0).then_some(out)
}

/// No implementation on this platform, so the space-aware policy degrades to
/// round-robin rather than to a wrong answer — `choose` treats `None` as "not
/// known to be full".
#[cfg(not(unix))]
pub fn available_bytes(_path: &str) -> Option<u64> {
    None
}

#[cfg(not(unix))]
pub fn total_bytes(_path: &str) -> Option<u64> {
    None
}

/// Identifies the filesystem `path` sits on, so that two locations on one disk
/// are not counted as two disks.
///
/// Two directories on the same filesystem report the same free space, and
/// adding those figures together claims twice the room the machine has. That is
/// not a cosmetic error: it is the number an operator decides against when
/// asking whether a database will fit.
#[cfg(unix)]
pub fn filesystem_id(path: &str) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).ok().map(|meta| meta.dev())
}

#[cfg(not(unix))]
pub fn filesystem_id(_path: &str) -> Option<u64> {
    None
}

/// Renders a byte count the way an operator reads one.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[0])
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

/// Parses `512`, `512MB`, `2GiB` — what an operator types for a byte floor.
pub fn parse_bytes(text: &str) -> Option<u64> {
    let text = text.trim().trim_matches('\'').trim_matches('"').trim();
    let digits: String = text.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    let value: u64 = digits.parse().ok()?;
    let suffix = text[digits.len()..].trim().to_ascii_lowercase();
    let scale = match suffix.as_str() {
        "" | "b" => 1u64,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        "t" | "tb" | "tib" => 1024u64 * 1024 * 1024 * 1024,
        _ => return None,
    };
    value.checked_mul(scale)
}
