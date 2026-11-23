//! Several databases in one process, addressed by name.
//!
//! A PostgreSQL client names the database it wants in its startup packet — the
//! `/postgres` in `postgres://user@host:5432/postgres`. This is what that name
//! resolves against.
//!
//! # Layout
//!
//! Each database is a whole [`Database`]: its own tables, write-ahead log,
//! transaction manager and row files. Nothing is shared, so a transaction
//! cannot span two of them — which is also PostgreSQL's rule.
//!
//! On disk the **root directory is the default database**, and every other
//! database is a subdirectory of it:
//!
//! ```text
//! /var/lib/rdb/            <- the default database (`postgres`)
//!   db_data_rows.db
//!   db_data_schema.db
//!   analytics/             <- database "analytics"
//!     db_data_rows.db
//!   staging/               <- database "staging"
//! ```
//!
//! The asymmetry is deliberate: a directory written by a single-database build
//! *is* a valid catalogue root holding exactly the default database, so an
//! existing installation keeps working with no migration and no move of the
//! files it already has.
//!
//! Which databases exist is read from the directory itself rather than from a
//! metadata file — a subdirectory is a database if it contains the schema file
//! every database has. A catalogue file could disagree with the directories it
//! lists; this cannot.
//!
//! # Roles are per database
//!
//! Each database carries its own `rdb_roles`, so an account exists in one
//! database and not in another. A database therefore has to be resolved
//! *before* its connection can be authenticated, which is why a name that does
//! not exist is reported as such to a client that has not authenticated yet.
//! Database names are not secret; the alternative is refusing a typo with
//! "password authentication failed".

use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cacheguard::CacheGuard;

use dashmap::DashMap;
use log::{info, warn};
use rclite::Arc;

#[cfg(test)]
use crate::core::database::DEFAULT_DATABASE_NAME;
use crate::core::database::Database;

/// Database branches (`CREATE BRANCH`, `MERGE BRANCH`, …): `docs/branching.md`.
#[cfg(feature = "db_branching")]
#[path = "branch.rs"]
pub mod branch;

/// The file every database has, used to tell a database directory apart from
/// whatever else an operator has left in the root.
const DATABASE_MARKER_FILE: &str = "db_data_schema.db";

/// Longest name accepted, matching PostgreSQL's `NAMEDATALEN - 1`.
const MAX_NAME_LEN: usize = 63;

/// One database, plus how many sessions are currently attached to it.
struct Entry {
    database: Arc<Database>,
    /// Set while a database is being built by an import.
    ///
    /// An import restores the source's keys only once every row is in, so
    /// between the first row and the last the database holds data that nothing
    /// yet constrains. A client writing into that window is not merely early:
    /// its rows are unconstrained, and the keys then cannot be put back. That
    /// is not hypothetical — an application recorded two page hits into a
    /// half-imported table and the source's `UNIQUE (endpoint)` could no longer
    /// be restored, failing the whole import.
    ///
    /// The window used to be closed by accident: a created database had no role
    /// catalogue, so every login failed until the import seeded one at the end.
    /// Seeding at creation fixed a real problem — an import target that could
    /// not be looked at while it loaded — and removed that accident with it, so
    /// the exclusion is now explicit and says why.
    loading: StdArc<CacheGuard<AtomicBool>>,
    /// Live sessions. `DROP DATABASE` consults it, for the same reason
    /// PostgreSQL does: the files cannot go while someone is still reading
    /// them.
    ///
    /// Both of these are `CacheGuard`ed inside their `Arc` rather than outside
    /// it, which is the point: an `Arc<AtomicUsize>` lays the counter out
    /// immediately after the strong and weak reference counts, on the same
    /// line. Every [`SessionGuard`] clone therefore wrote to the line holding
    /// the value the guard exists to maintain. The padding puts the payload on
    /// a line the refcounts cannot reach.
    sessions: StdArc<CacheGuard<AtomicUsize>>,
}

/// The file in a catalogue's root that the process serving it holds an
/// exclusive lock on, for as long as the catalogue is open.
pub const LOCK_FILE: &str = "rasterizeddb.lock";

/// The exclusive lock on a root's [`LOCK_FILE`]: the operating system's
/// (`flock` on unix, `LockFileEx` on Windows), so it ends with the process
/// however it ends, and never outlives it as a stale file would.
///
/// Two engines on one directory each replay and truncate the other's
/// write-ahead log and write the same row files: silent corruption. A second
/// process, or a second catalogue in this one (the lock is per open file, not
/// per process), is refused at open instead, before it reads anything.
#[derive(Debug)]
struct LocationLock {
    _file: std::fs::File,
}

impl LocationLock {
    fn take(root: &str) -> Result<Self, String> {
        use std::io::{Read, Seek, Write};
        std::fs::create_dir_all(root).map_err(|e| format!("creating {root}: {e}"))?;
        let path = std::path::Path::new(root).join(LOCK_FILE);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| format!("opening {}: {e}", path.display()))?;
        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                // Who holds it, when the file can be read (not on Windows,
                // where the lock covers reads too).
                let mut holder = String::new();
                let _ = file.read_to_string(&mut holder);
                let holder = holder.trim();
                return Err(format!(
                    "{root} is already open in another RasterizedDB ({}): two engines on one data                      directory would corrupt it, so this one does not open it. Stop the other, or                      use another directory",
                    if holder.is_empty() {
                        "another process, or an earlier engine in this one".to_string()
                    } else {
                        format!("process {holder}, or an earlier engine in that process")
                    }
                ));
            }
            Err(std::fs::TryLockError::Error(e)) => {
                return Err(format!("locking {}: {e}", path.display()));
            }
        }
        // Best effort, for the refusal above to name the holder.
        let _ = file.set_len(0);
        let _ = file.rewind();
        let _ = write!(file, "{}", std::process::id());
        Ok(LocationLock { _file: file })
    }
}

/// The databases this process serves.
pub struct DatabaseCatalog {
    /// Behind a `std` `Arc` so the automatic compaction can hold it weakly
    /// (`core::auto_compact`) and end with the catalogue.
    databases: StdArc<DashMap<String, Entry, ahash::RandomState>>,
    /// Root directory, or `None` for an in-memory catalogue.
    root: Option<String>,
    /// The database a connection that names none is given, and the one that
    /// lives at the root rather than in a subdirectory.
    default_name: String,
    /// A catalogue of one database that is served through another catalogue
    /// ([`DatabaseCatalog::single_unmanaged`]): `CREATE DATABASE` is refused
    /// rather than answered with an in-memory database that only this
    /// catalogue would ever hold.
    fixed: bool,
    /// Each branch's merge base, once opened: `(generation, database)`.
    #[cfg(feature = "db_branching")]
    bases: DashMap<String, (u64, Arc<Database>), ahash::RandomState>,
    /// Held for the catalogue's life; `None` in memory and for a catalogue
    /// over databases another one opened.
    _lock: Option<LocationLock>,
}

/// Keeps a database attached for as long as a session is using it.
///
/// Holding the [`Arc<Database>`] alone would not do: `DROP DATABASE` asks how
/// many sessions are attached, and an `Arc` count includes the catalogue's own
/// handle and any transient clone.
pub struct SessionGuard {
    database: Arc<Database>,
    sessions: StdArc<CacheGuard<AtomicUsize>>,
}

impl SessionGuard {
    pub fn database(&self) -> &Arc<Database> {
        &self.database
    }
}

/// Holds a database closed to clients while an import builds it.
///
/// Cleared on drop, so however the import ends — finished, failed, or the task
/// dropped — the database becomes reachable rather than staying shut.
pub struct LoadingGuard {
    loading: StdArc<CacheGuard<AtomicBool>>,
}

impl Drop for LoadingGuard {
    fn drop(&mut self) {
        self.loading.store(false, Ordering::Release);
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.sessions.fetch_sub(1, Ordering::AcqRel);
    }
}

impl DatabaseCatalog {
    /// Opens the catalogue rooted at `root`, or an in-memory one when `root` is
    /// `None`.
    ///
    /// The default database is opened either way; on disk, every subdirectory
    /// that looks like a database is opened with it.
    pub async fn open(root: Option<&str>, default_name: &str) -> Result<Arc<Self>, String> {
        validate_name(default_name)?;

        let databases = DashMap::with_hasher(ahash::RandomState::new());

        let in_memory = root.is_none();
        let location = root.unwrap_or("");

        // Before anything in the directory is read: a second engine must not
        // even replay the first one's log.
        let lock = root.map(LocationLock::take).transpose()?;

        // Refusals come back as errors, so the server can say which database
        // would not open and exit, rather than unwinding out of here.
        let default = Database::try_new_named(location, in_memory, default_name).await?;
        databases.insert(default_name.to_string(), Entry::new(Arc::new(default)));

        if let Some(root) = root {
            for name in discover_databases(root, default_name).await {
                let path = std::path::Path::new(root).join(&name);
                let database = Database::try_new_named(&path.to_string_lossy(), false, &name).await?;
                databases.insert(name, Entry::new(Arc::new(database)));
            }
        }

        let databases = StdArc::new(databases);
        crate::core::auto_compact::spawn(StdArc::downgrade(&databases), |map| {
            map.iter().map(|e| e.database.clone()).collect()
        });
        let catalogue = Arc::new(Self {
            databases,
            root: root.map(str::to_string),
            default_name: default_name.to_string(),
            fixed: false,
            #[cfg(feature = "db_branching")]
            bases: DashMap::with_hasher(ahash::RandomState::new()),
            _lock: lock,
        });
        if let Some(root) = root {
            sweep_dropped(std::path::Path::new(root));
        }
        #[cfg(feature = "db_branching")]
        catalogue.sweep_branch_leftovers();

        info!(
            "Database catalogue open with {}: {}",
            match catalogue.databases.len() {
                1 => "1 database".to_string(),
                n => format!("{} databases", n),
            },
            catalogue.names().join(", ")
        );

        Ok(catalogue)
    }

    /// A catalogue serving one already-open database, which every connection
    /// reaches under its own name.
    ///
    /// For embedding a `Database` behind the wire protocol — tests, benchmarks,
    /// and the single-database entry points — without going through a
    /// directory.
    pub fn single(database: Arc<Database>) -> Arc<Self> {
        let name = database.name.clone();
        let databases = DashMap::with_hasher(ahash::RandomState::new());
        databases.insert(name.clone(), Entry::new(database));
        let databases = StdArc::new(databases);
        crate::core::auto_compact::spawn(StdArc::downgrade(&databases), |map| {
            map.iter().map(|e| e.database.clone()).collect()
        });
        Arc::new(Self {
            databases,
            root: None,
            default_name: name,
            fixed: false,
            #[cfg(feature = "db_branching")]
            bases: DashMap::with_hasher(ahash::RandomState::new()),
            _lock: None,
        })
    }

    /// [`DatabaseCatalog::single`] without the automatic compaction task: a
    /// view of one database that is already served (and compacted) through
    /// the catalogue that opened it. What a native-protocol transaction's
    /// session (`pgwire::native_session`) runs its statements against, so a
    /// block opened over native does not start a second compaction loop over
    /// the same tables.
    pub(crate) fn single_unmanaged(database: Arc<Database>) -> Arc<Self> {
        let name = database.name.clone();
        let databases = DashMap::with_hasher(ahash::RandomState::new());
        databases.insert(name.clone(), Entry::new(database));
        Arc::new(Self {
            databases: StdArc::new(databases),
            root: None,
            default_name: name,
            fixed: true,
            #[cfg(feature = "db_branching")]
            bases: DashMap::with_hasher(ahash::RandomState::new()),
            _lock: None,
        })
    }

    /// What `SHOW DATABASES` reports, ordered by name.
    pub fn listing(&self) -> Vec<DatabaseListing> {
        let mut out: Vec<DatabaseListing> = self
            .databases
            .iter()
            .map(|entry| DatabaseListing {
                name: entry.key().clone(),
                location: entry
                    .database
                    .location
                    .clone()
                    .unwrap_or_else(|| "(in memory)".to_string()),
                tables: entry.database.table_count() as u64,
                sessions: entry.sessions.load(Ordering::Acquire) as u64,
                is_default: entry.key() == &self.default_name,
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// The name a connection that names no database is given.
    pub fn default_name(&self) -> &str {
        &self.default_name
    }

    /// Every database, in no particular order.
    pub fn all(&self) -> Vec<Arc<Database>> {
        self.databases.iter().map(|e| e.database.clone()).collect()
    }

    /// Every name, sorted, so output is stable between calls.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.databases.iter().map(|e| e.key().clone()).collect();
        names.sort();
        names
    }

    /// The named database, without attaching a session to it.
    pub fn get(&self, name: &str) -> Option<Arc<Database>> {
        self.databases.get(name).map(|e| e.database.clone())
    }

    /// Attaches a session to the named database.
    ///
    /// The database cannot be dropped while the returned guard is alive.
    pub fn connect(&self, name: &str) -> Option<SessionGuard> {
        let entry = self.databases.get(name)?;
        entry.sessions.fetch_add(1, Ordering::AcqRel);
        Some(SessionGuard {
            database: entry.database.clone(),
            sessions: entry.sessions.clone(),
        })
    }

    /// Whether `name` is mid-import and so must not be connected to.
    pub fn is_loading(&self, name: &str) -> bool {
        self.databases
            .get(name)
            .map(|e| e.loading.load(Ordering::Acquire))
            .unwrap_or(false)
    }

    /// Marks a database as being built, and returns the guard that clears it.
    ///
    /// Cleared on drop however the import ends, so a failure cannot leave a
    /// database permanently unreachable.
    pub fn begin_loading(&self, name: &str) -> Option<LoadingGuard> {
        let entry = self.databases.get(name)?;
        entry.loading.store(true, Ordering::Release);
        Some(LoadingGuard {
            loading: entry.loading.clone(),
        })
    }

    /// Creates a database and opens it.
    ///
    /// The caller is responsible for seeding its role catalogue: roles are per
    /// database, so a new one is unreachable until it has an account, and what
    /// that account should be is the server's configuration rather than the
    /// catalogue's business.
    pub async fn create(&self, name: &str) -> Result<Arc<Database>, String> {
        // While the default database replicates, the CREATE is owed to the
        // peers from before the database exists until its record is durable:
        // a marker in its directory says so, written first. A crash anywhere
        // in between leaves the marker, and the next start records the CREATE
        // (`reconcile_replication`); a crash before it leaves a directory
        // that is not a database, which nothing serves or announces.
        let announced = self.replicating() && self.root.is_some() && !self.fixed;
        if announced {
            validate_name(name)?;
            // Never into a database that is there: its marker would announce
            // it at the next start.
            if self.databases.contains_key(name) {
                return Err(format!("database \"{}\" already exists", name));
            }
            let dir = self.dir_of(name).expect("a root");
            tokio::fs::create_dir_all(&dir)
                .await
                .map_err(|e| format!("could not create database \"{}\": {}", name, e))?;
            write_marker(&dir.join(PENDING_CREATE)).map_err(|e| format!("could not create database \"{}\": {}", name, e))?;
        }
        let database = match self.create_unrecorded(name).await {
            Ok(database) => database,
            Err(e) => {
                if announced {
                    let _ = std::fs::remove_file(self.dir_of(name).expect("a root").join(PENDING_CREATE));
                }
                return Err(e);
            }
        };
        #[cfg(test)]
        if test_hooks::crashes_before_record(name) {
            return Ok(database);
        }
        self.replicate(crate::replication::manager::CatalogOp::Create(name.to_string()), Some(&database))
            .await;
        if announced {
            let _ = std::fs::remove_file(self.dir_of(name).expect("a root").join(PENDING_CREATE));
        }
        Ok(database)
    }

    /// Whether the default database replicates, and so catalogue changes do.
    fn replicating(&self) -> bool {
        self.get(&self.default_name).is_some_and(|d| d.replication.is_active())
    }

    /// Where `name` lives: the root for the default database, a directory
    /// under it for any other. `None` in memory.
    fn dir_of(&self, name: &str) -> Option<std::path::PathBuf> {
        let root = std::path::Path::new(self.root.as_deref()?);
        Some(if name == self.default_name { root.to_path_buf() } else { root.join(name) })
    }

    /// Records the catalogue changes a crash left unrecorded: a database whose
    /// directory still holds its `CREATE` marker, and a drop whose tombstone
    /// is still in the root (finished first if the database is still there).
    /// Run at each start of replication with the catalogue; idempotent on the
    /// peers, so a change recorded twice — the crash fell after the record and
    /// before the marker went — is made once.
    ///
    /// A database with no marker is never announced: one that existed before
    /// replication was on, announced, would be created empty on the peers and
    /// take only the writes from now on, which is worse than its link waiting
    /// visibly for a base copy.
    pub(crate) async fn reconcile_replication(&self) {
        if !self.replicating() {
            return;
        }
        let Some(root) = self.root.clone() else { return };
        for name in self.names() {
            if name == self.default_name {
                continue;
            }
            let marker = self.dir_of(&name).expect("a root").join(PENDING_CREATE);
            if marker.exists() {
                log::warn!("Recording the CREATE DATABASE \"{name}\" a stop left unrecorded");
                // Started too, if the stop came before its replication was:
                // a start with the catalogue starts every database, but this
                // does not lean on having run inside one.
                let unstarted = self.get(&name).filter(|d| !d.replication.is_active());
                self.replicate(crate::replication::manager::CatalogOp::Create(name.clone()), unstarted.as_ref())
                    .await;
                let _ = std::fs::remove_file(&marker);
            }
        }
        let tombstones: Vec<String> = std::fs::read_dir(&root)
            .map(|entries| {
                entries
                    .flatten()
                    .filter_map(|e| e.file_name().to_str()?.strip_prefix(PENDING_DROP).map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        for name in tombstones {
            let tombstone = std::path::Path::new(&root).join(format!("{PENDING_DROP}{name}"));
            if self.get(&name).is_some() {
                if let Err(e) = self.drop_unrecorded(&name, "").await {
                    log::error!("The DROP DATABASE \"{name}\" a stop interrupted could not be finished: {e}");
                    continue;
                }
            }
            log::warn!("Recording the DROP DATABASE \"{name}\" a stop left unrecorded");
            self.replicate(crate::replication::manager::CatalogOp::Drop(name.clone()), None).await;
            let _ = std::fs::remove_file(&tombstone);
        }
    }

    /// When the default database replicates, a catalogue change does too: a
    /// new database starts replicating (its links name it, and the peers'
    /// listeners route them to theirs once it is there), and the default
    /// stream carries the change for the peers to make (`ReplicatedCatalog`).
    ///
    /// The record follows the change. A crash between them leaves the change
    /// on this node alone, as a failed `REPLICATION` capture does; the other
    /// order would have the peers make a change this node may then refuse.
    async fn replicate(&self, op: crate::replication::manager::CatalogOp, created: Option<&Arc<Database>>) {
        let Some(default) = self.get(&self.default_name) else { return };
        if !default.replication.is_active() {
            return;
        }
        if let Some(database) = created {
            let config = default.replication.config();
            if let Err(e) = Database::start_routed_replication(database.clone(), op.name(), &config) {
                log::error!("Database \"{}\" was created but does not replicate: {}", op.name(), e);
            }
        }
        let tx = default.transactions.begin();
        default.replication.note_statement(tx, &op.statement(), None);
        let family = default.commit_transaction(tx);
        default.publish_transaction(family).await;
        // Durable before the caller lets its marker go.
        if let Err(e) = default.sync_own_log() {
            log::error!("The record of {} could not be made durable: {}", op.statement(), e);
        }
    }

    /// [`DatabaseCatalog::create`] without recording it for the peers: what
    /// applying a peer's `CREATE DATABASE` record does.
    pub(crate) async fn create_unrecorded(&self, name: &str) -> Result<Arc<Database>, String> {
        validate_name(name)?;

        if self.fixed {
            return Err(format!(
                "could not create database \"{}\": this listener serves the one database \"{}\"",
                name, self.default_name
            ));
        }
        if self.databases.contains_key(name) {
            return Err(format!("database \"{}\" already exists", name));
        }

        let database = match &self.root {
            Some(root) => {
                let path = std::path::Path::new(root).join(name);
                // An existing directory is adopted rather than refused: it can
                // only be one this catalogue failed to notice, since the name
                // is not in the map.
                tokio::fs::create_dir_all(&path)
                    .await
                    .map_err(|e| format!("could not create database \"{}\": {}", name, e))?;
                // Refused as an error, not a panic: a panic here ends the
                // client's session where it should answer the statement.
                Database::try_new_named(&path.to_string_lossy(), false, name).await?
            }
            None => Database::try_new_named("", true, name).await?,
        };

        let database = Arc::new(database);

        // Between the check above and here another connection may have created
        // the same name. The map decides, so the loser reports the conflict
        // rather than replacing a database someone may already be using.
        if self.databases.contains_key(name) {
            return Err(format!("database \"{}\" already exists", name));
        }
        // The roles of the database this one was created from, so the account
        // that could create it can open it. Roles are per database and resolved
        // *before* a connection is authenticated, so a database with no
        // catalogue refuses every login — a database you could create and then
        // never read. See `auth::copy_role_catalogue`.
        //
        // A failure here is logged rather than failing the creation: the
        // database exists and holds data either way, and reporting the create
        // as failed would be a worse description of what happened than saying
        // its accounts did not come across.
        if let Some(default) = self
            .databases
            .get(&self.default_name)
            .map(|e| e.database.clone())
        {
            if let Err(e) = crate::auth::copy_role_catalogue(&default, &database).await {
                log::error!(
                    "Database \"{}\" was created but its roles could not be carried over from \
                     \"{}\" ({}); no account will be able to connect to it until one is created",
                    name,
                    self.default_name,
                    e
                );
            }
        }

        self.databases
            .insert(name.to_string(), Entry::new(database.clone()));

        info!("Created database \"{}\"", name);
        Ok(database)
    }

    /// Removes a database from the catalogue and deletes its directory.
    ///
    /// Refused while any session is attached, and for the default database,
    /// which is the root directory itself.
    ///
    /// Background maintenance tasks belonging to a dropped database — vacuum,
    /// filter checkpointing — are not stopped; they run until the process
    /// exits, writing to files that have already been unlinked. Their work is
    /// discarded rather than resurrected, but the space is not reclaimed until
    /// then.
    pub async fn drop_database(&self, name: &str, current: &str) -> Result<(), String> {
        // As for a CREATE: a tombstone in the root says the DROP is owed to
        // the peers, from before the database goes until its record is
        // durable (`reconcile_replication`).
        let tombstone = (self.replicating() && name != self.default_name && self.get(name).is_some())
            .then(|| self.dir_of(&self.default_name).map(|root| root.join(format!("{PENDING_DROP}{name}"))))
            .flatten();
        if let Some(tombstone) = &tombstone {
            write_marker(tombstone).map_err(|e| format!("could not drop database \"{}\": {}", name, e))?;
        }
        if let Err(e) = self.drop_unrecorded(name, current).await {
            if let Some(tombstone) = &tombstone {
                let _ = std::fs::remove_file(tombstone);
            }
            return Err(e);
        }
        #[cfg(test)]
        if test_hooks::crashes_before_record(name) {
            return Ok(());
        }
        self.replicate(crate::replication::manager::CatalogOp::Drop(name.to_string()), None).await;
        if let Some(tombstone) = &tombstone {
            let _ = std::fs::remove_file(tombstone);
        }
        Ok(())
    }

    /// [`DatabaseCatalog::drop_database`] without recording it for the
    /// peers: what applying a peer's `DROP DATABASE` record does.
    pub(crate) async fn drop_unrecorded(&self, name: &str, current: &str) -> Result<(), String> {
        if name == self.default_name {
            return Err(format!(
                "cannot drop the default database \"{}\"; it is the catalogue root",
                name
            ));
        }
        if name == current {
            return Err("cannot drop the currently open database".to_string());
        }

        let Some(entry) = self.databases.get(name) else {
            return Err(format!("database \"{}\" does not exist", name));
        };

        let database_id = entry.database.id;
        let attached = entry.sessions.load(Ordering::Acquire);
        if attached > 0 {
            return Err(format!(
                "database \"{}\" is being accessed by {} other session(s)",
                name, attached
            ));
        }
        let database = entry.database.clone();
        drop(entry);

        // Its replication stops first: its links end, and the listener's
        // router stops serving it with the name gone from the map below.
        database.stop_replication();
        drop(database);

        // Out of the map first: a session that resolves the name after this
        // point must not be handed files that are about to be unlinked.
        self.databases.remove(name);
        // Its sequences' counters. A recreated database has a new id and would
        // never read them, so this only releases them.
        crate::core::rql::sequence::forget_database(database_id);

        // Branches made from it move to its parent (docs/branching.md §8).
        #[cfg(feature = "db_branching")]
        {
            let moved = self.reparent_children(name);
            let (reparented, orphaned): (Vec<_>, Vec<_>) =
                moved.into_iter().partition(|(_, to)| to.is_some());
            if let Some((_, Some(to))) = reparented.first() {
                crate::core::notice::push(
                    "NOTICE",
                    "00000",
                    format!(
                        "{} now branch{} from \"{to}\"; {} merge base{} still the copy taken from \"{name}\"",
                        reparented.iter().map(|(c, _)| format!("\"{c}\"")).collect::<Vec<_>>().join(", "),
                        if reparented.len() == 1 { "es" } else { "" },
                        if reparented.len() == 1 { "its" } else { "their" },
                        if reparented.len() == 1 { " is" } else { "s are" },
                    ),
                );
            }
            if !orphaned.is_empty() {
                crate::core::notice::push(
                    "WARNING",
                    "01000",
                    format!(
                        "{} {} made from \"{name}\", which was not a branch, so {} no parent to merge into now",
                        orphaned.iter().map(|(c, _)| format!("\"{c}\"")).collect::<Vec<_>>().join(", "),
                        if orphaned.len() == 1 { "was" } else { "were" },
                        if orphaned.len() == 1 { "it has" } else { "they have" },
                    ),
                );
            }
        }

        if let Some(root) = &self.root {
            let path = std::path::Path::new(root).join(name);

            // Forget the log before the directory goes. It holds physical redo
            // records naming files *inside* this directory, so once they are
            // unlinked it describes nothing — and the registry would otherwise
            // keep it, and its open handle to the now-unlinked log file, for the
            // life of the process. Nothing else removes registry entries.
            let _ = crate::core::wal::remove_for(&path.to_string_lossy());
            // And for the same reason, any data-file flush failure recorded
            // against it: a database recreated under this name reuses the path,
            // and would otherwise refuse every commit for a failure in files
            // that no longer exist.
            crate::core::storage_providers::file_sync::forget_directory_failure(
                &path.to_string_lossy(),
            );

            // Its files close from its own tasks, just after it is let go:
            // removing them while open leaves `.fuse_hidden` files on
            // ntfs-3g, and the directory behind.
            #[cfg(feature = "db_branching")]
            branch::released(&path).await;
            // The database is gone as far as clients are concerned, so one
            // that cannot be removed yet is a disk-space matter, not a failed
            // statement: it is set aside and removed once it can be.
            remove_eventually(&path);
        }

        // A branch's merge bases go with it.
        #[cfg(feature = "db_branching")]
        self.remove_bases(name).await;

        info!("Dropped database \"{}\"", name);
        Ok(())
    }
}

/// The catalogue this process serves, registered by the server's `main` so a
/// `REPLICATION ENABLE` on its default database starts every database's
/// replication, as `--replication` does at start. Unset in tests and
/// embedded use, where replication starts with the catalogue it is given.
static SERVED: std::sync::OnceLock<Arc<DatabaseCatalog>> = std::sync::OnceLock::new();

/// Registers the catalogue this process serves. The first call wins.
pub fn register_served(catalog: Arc<DatabaseCatalog>) {
    let _ = SERVED.set(catalog);
}

/// The served catalogue, when `database` is its default database.
pub(crate) fn served_by_default(database: &Arc<Database>) -> Option<Arc<DatabaseCatalog>> {
    let catalog = SERVED.get()?;
    let default = catalog.get(&catalog.default_name)?;
    Arc::ptr_eq(&default, database).then(|| catalog.clone())
}

/// In a database's directory: its `CREATE` is not yet recorded for the
/// peers. See [`DatabaseCatalog::create`].
const PENDING_CREATE: &str = "db_replication.pending_create";
/// In the root, followed by a name: that database's `DROP` is not yet
/// recorded for the peers. See [`DatabaseCatalog::drop_database`].
const PENDING_DROP: &str = "db_replication.pending_drop.";

/// Writes an empty marker file durably: the file, then its directory entry.
fn write_marker(path: &std::path::Path) -> std::io::Result<()> {
    let file = std::fs::File::create(path)?;
    file.sync_all()?;
    if let Some(dir) = path.parent() {
        crate::core::sync_path::sync_dir(dir)?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::collections::HashSet;
    use std::sync::Mutex;

    static CRASH: Mutex<Option<HashSet<String>>> = Mutex::new(None);

    /// The next CREATE or DROP of `name` stops after the catalogue change and
    /// before its record, as a crash there would.
    pub(crate) fn crash_before_record(name: &str) {
        CRASH.lock().unwrap().get_or_insert_with(HashSet::new).insert(name.to_string());
    }

    pub(super) fn crashes_before_record(name: &str) -> bool {
        CRASH.lock().unwrap().as_mut().is_some_and(|names| names.remove(name))
    }
}

/// Applies the catalogue changes the default stream carries
/// ([`crate::replication::manager::CatalogOp`]) to this node's catalogue.
///
/// Idempotent, as a record offered again after a restart must be: a database
/// that is already there is only made to replicate, and one already gone is
/// done. Neither is recorded again, so nothing travels back.
pub struct ReplicatedCatalog {
    catalog: Arc<DatabaseCatalog>,
}

impl ReplicatedCatalog {
    pub fn new(catalog: Arc<DatabaseCatalog>) -> Self {
        Self { catalog }
    }
}

impl crate::replication::manager::CatalogLink for ReplicatedCatalog {
    fn apply<'a>(
        &'a self,
        op: crate::replication::manager::CatalogOp,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            use crate::replication::manager::CatalogOp;
            match op {
                CatalogOp::Create(name) => {
                    let database = match self.catalog.get(&name) {
                        Some(database) => database,
                        None => self.catalog.create_unrecorded(&name).await?,
                    };
                    let default = self
                        .catalog
                        .get(&self.catalog.default_name)
                        .ok_or_else(|| "this node has no default database".to_string())?;
                    if !database.replication.is_active() {
                        Database::start_routed_replication(database, &name, &default.replication.config())
                            .map_err(|e| format!("database \"{name}\" was created but does not replicate: {e}"))?;
                    }
                    Ok(())
                }
                CatalogOp::Drop(name) => {
                    if self.catalog.get(&name).is_none() {
                        return Ok(());
                    }
                    self.catalog.drop_unrecorded(&name, "").await
                }
            }
        })
    }

    fn replications(&self) -> Vec<(String, std::sync::Arc<crate::replication::Replication>)> {
        self.catalog
            .names()
            .into_iter()
            .filter(|name| *name != self.catalog.default_name)
            .filter_map(|name| {
                let database = self.catalog.get(&name)?;
                Some((name, database.replication.clone()))
            })
            .collect()
    }
}

impl Entry {
    fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            sessions: StdArc::new(AtomicUsize::new(0).into()),
            loading: StdArc::new(AtomicBool::new(false).into()),
        }
    }
}

/// Subdirectories of `root` that hold a database, excluding the default one,
/// which is the root itself.
async fn discover_databases(root: &str, default_name: &str) -> Vec<String> {
    let mut found = Vec::new();

    let mut entries = match tokio::fs::read_dir(root).await {
        Ok(entries) => entries,
        Err(e) => {
            warn!("Could not scan {} for databases: {}", root, e);
            return found;
        }
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        if !entry.file_type().await.map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }

        let name = entry.file_name().to_string_lossy().to_string();

        // A name the catalogue could not serve would be opened and then be
        // unreachable, so skip it and say so rather than pretending.
        if validate_name(&name).is_err() {
            continue;
        }
        if name == default_name {
            warn!(
                "Ignoring {}/{}: the default database is the root directory itself",
                root, name
            );
            continue;
        }
        let marker = entry.path().join(DATABASE_MARKER_FILE);
        if tokio::fs::metadata(&marker).await.is_err() {
            continue;
        }

        found.push(name);
    }

    found.sort();
    found
}

/// Whether `name` can be a database name here.
///
/// Stricter than PostgreSQL, which allows anything inside double quotes: a name
/// is also a directory name, so it is restricted to characters that mean the
/// same thing on every filesystem and cannot escape the root.
/// A directory removed while some of its files were still open, set aside
/// under a name nothing reuses and removed once it can be: `.dropped-<name>-<n>`.
pub(crate) const DROPPED_PREFIX: &str = ".dropped-";

fn try_remove(path: &std::path::Path) -> bool {
    match std::fs::remove_dir_all(path) {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(_) => !path.exists(),
    }
}

/// Removes the directory `path`. One that cannot be removed yet — on a FUSE
/// filesystem (ntfs-3g) a file removed while open stays as `.fuse_hidden…`
/// until it closes, and the directory with it — is renamed aside first, so
/// its name is free at once (a database dropped and made again, a base
/// generation reused), and removed in the background once it can be; what
/// is still there at the next start is removed then ([`sweep_dropped`]).
pub(crate) fn remove_eventually(path: &std::path::Path) {
    if try_remove(path) {
        return;
    }
    let (Some(parent), Some(base)) = (path.parent(), path.file_name()) else {
        return;
    };
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let aside = parent.join(format!(
        "{DROPPED_PREFIX}{}-{nanos:x}",
        base.to_string_lossy().trim_start_matches('.')
    ));
    if let Err(e) = crate::core::sync_path::rename_when_released(path, &aside) {
        warn!(
            "could not remove {} ({e}), nor set it aside; it is left as it is",
            path.display()
        );
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    // What keeps it is usually a file still mapped by a worker thread's
    // read cache (`file_sync`'s per-thread mmap), which lets go the next time
    // that thread reads another file: soon on a busy server, maybe not until
    // a restart on an idle one. So: often at first, then rarely, for a day.
    runtime.spawn(async move {
        for attempt in 0u32..(60 + 24 * 120) {
            let wait = if attempt < 60 { 1 } else { 30 };
            tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
            if try_remove(&aside) {
                return;
            }
        }
        warn!(
            "{} could not be removed; the next start removes it",
            aside.display()
        );
    });
}

/// Removes what [`remove_eventually`] set aside and did not get to.
fn sweep_dropped(root: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for e in entries.flatten() {
        if e.file_name().to_string_lossy().starts_with(DROPPED_PREFIX) && !try_remove(&e.path()) {
            warn!("could not remove {}", e.path().display());
        }
    }
}

pub fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("database name is empty".to_string());
    }
    if name.len() > MAX_NAME_LEN {
        return Err(format!(
            "database name is longer than {} characters",
            MAX_NAME_LEN
        ));
    }
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        return Err("database name may not start with a digit".to_string());
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_'))
    {
        return Err(format!(
            "database name may not contain {:?}; use letters, digits and underscores",
            bad
        ));
    }
    Ok(())
}

/// A parsed `CREATE DATABASE`, `DROP DATABASE` or `SHOW DATABASES` statement.
#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseStatement {
    Create {
        name: String,
        if_not_exists: bool,
    },
    Drop {
        name: String,
        if_exists: bool,
    },
    /// Lists what the catalogue holds. It belongs here rather than with the
    /// rest of the introspection surface because the executor is handed one
    /// database and the catalogue is above it.
    List,
    /// `CREATE BRANCH name [FROM db] [AS OF …] [WITHOUT BASE]`, and
    /// `CREATE DATABASE name TEMPLATE db` (`from` set, default options).
    #[cfg(feature = "db_branching")]
    CreateBranch {
        name: String,
        /// `None`: the session's own database.
        from: Option<String>,
        options: branch::BranchOptions,
        /// Written as `CREATE DATABASE … TEMPLATE`, and so answered as one.
        template: bool,
    },
    #[cfg(feature = "db_branching")]
    DropBranch { name: String, if_exists: bool },
    #[cfg(feature = "db_branching")]
    ResetBranch { name: String },
    #[cfg(feature = "db_branching")]
    PromoteBranch { name: String },
    #[cfg(feature = "db_branching")]
    ListBranches,
    /// `SHOW BRANCH DIFF name [INTO db]`.
    #[cfg(feature = "db_branching")]
    DiffBranch { name: String, into: Option<String> },
    #[cfg(feature = "db_branching")]
    MergeBranch {
        name: String,
        into: Option<String>,
        policy: branch::ConflictPolicy,
        allow_drop: bool,
    },
}

/// One database, as `SHOW DATABASES` reports it.
pub struct DatabaseListing {
    pub name: String,
    pub location: String,
    pub tables: u64,
    pub sessions: u64,
    pub is_default: bool,
}

/// Recognises database management SQL, or `None` when the statement is
/// something else.
///
/// An unquoted name is folded to lower case and a quoted one is taken as
/// written, which is how PostgreSQL treats every identifier.
pub fn parse_database_statement(sql: &str) -> Option<Result<DatabaseStatement, String>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    // Every form names `DATABASE[S]` as its second word. Asked of the text as
    // it is (uppercasing moves no whitespace), so the statements that are not
    // one, which is every other statement the server runs, skip the copy.
    let noun = trimmed.split_whitespace().nth(1)?;
    if noun.eq_ignore_ascii_case("BRANCH") || noun.eq_ignore_ascii_case("BRANCHES") {
        let verb = trimmed.split_whitespace().next()?.to_ascii_uppercase();
        if !matches!(verb.as_str(), "CREATE" | "DROP" | "RESET" | "PROMOTE" | "MERGE" | "SHOW") {
            return None;
        }
        #[cfg(feature = "db_branching")]
        return branch::parse(trimmed, &trimmed.to_ascii_uppercase());
        #[cfg(not(feature = "db_branching"))]
        return Some(Err(
            "database branching is not built into this server (feature db_branching)".to_string(),
        ));
    }
    if !noun.eq_ignore_ascii_case("DATABASE") && !noun.eq_ignore_ascii_case("DATABASES") {
        return None;
    }
    let upper = trimmed.to_ascii_uppercase();

    let mut words = upper.split_whitespace();
    let verb = words.next()?;
    let noun = words.next()?;

    // `SHOW DATABASES`, with the plural PostgreSQL does not have and every
    // other console does. `\l` is not a statement, so this is the only
    // spelling a client can send over the wire.
    if verb == "SHOW" && (noun == "DATABASES" || noun == "DATABASE") {
        if noun == "DATABASE" {
            // `SHOW DATABASE STATS` belongs to the introspection surface.
            return None;
        }
        if words.next().is_some() {
            return Some(Err("SHOW DATABASES takes no arguments".to_string()));
        }
        return Some(Ok(DatabaseStatement::List));
    }

    if noun != "DATABASE" {
        return None;
    }

    // Read the name from the original text: quoting decides its case.
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();

    let fold = |token: &str| {
        if token.starts_with('"') {
            token.trim_matches('"').to_string()
        } else {
            token.to_ascii_lowercase()
        }
    };

    match verb {
        "CREATE" => {
            let if_not_exists = upper.contains("IF NOT EXISTS");
            let index = if if_not_exists { 5 } else { 2 };
            let Some(name) = tokens.get(index).map(|t| fold(t)) else {
                return Some(Err("CREATE DATABASE names no database".to_string()));
            };
            // `TEMPLATE db` is a copy: a branch of `db`. `template0` and
            // `template1` are PostgreSQL's empty templates, a plain create.
            #[cfg(feature = "db_branching")]
            {
                let words: Vec<&str> = upper.split_whitespace().collect();
                if let Some(template) = branch::template_of(
                    words.get(index + 1..).unwrap_or(&[]),
                    tokens.get(index + 1..).unwrap_or(&[]),
                ) {
                    return Some(match template {
                        Err(e) => Err(e),
                        Ok(None) => Ok(DatabaseStatement::Create {
                            name,
                            if_not_exists,
                        }),
                        Ok(Some(from)) if !if_not_exists => Ok(DatabaseStatement::CreateBranch {
                            name,
                            from: Some(from),
                            options: branch::BranchOptions::default(),
                            template: true,
                        }),
                        Ok(Some(_)) => Err(
                            "CREATE DATABASE IF NOT EXISTS … TEMPLATE is not supported; use CREATE BRANCH".to_string(),
                        ),
                    });
                }
            }
            // `WITH OWNER`, `ENCODING` and the rest are silently
            // accepted by nothing here; saying so beats creating a database
            // that ignores half of what was asked for.
            if tokens.len() > index + 1 {
                return Some(Err(format!(
                    "CREATE DATABASE takes no options here; unexpected {:?}",
                    tokens[index + 1]
                )));
            }
            Some(Ok(DatabaseStatement::Create {
                name,
                if_not_exists,
            }))
        }
        "DROP" => {
            let if_exists = upper.contains("IF EXISTS");
            let index = if if_exists { 4 } else { 2 };
            let Some(name) = tokens.get(index).map(|t| fold(t)) else {
                return Some(Err("DROP DATABASE names no database".to_string()));
            };
            Some(Ok(DatabaseStatement::Drop { name, if_exists }))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Opening a database scans its (empty) table catalogue, and the scan path
    /// reads the worker count from a process-wide `OnceLock` that only the real
    /// binary sets. Unset, it panics — so any test that opens a database has to
    /// set it first.
    fn init_globals() {
        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(10_000);
    }

    #[test]
    fn a_name_that_would_escape_the_root_is_refused() {
        for name in ["../etc", "a/b", ".", "..", "a b", "a.b"] {
            assert!(
                validate_name(name).is_err(),
                "{:?} should not be a database name",
                name
            );
        }
    }

    #[test]
    fn ordinary_names_are_accepted() {
        for name in ["postgres", "app_prod", "_internal", "db2"] {
            validate_name(name).unwrap_or_else(|e| panic!("{:?}: {}", name, e));
        }
    }

    #[test]
    fn an_unquoted_name_folds_and_a_quoted_one_does_not() {
        assert_eq!(
            parse_database_statement("CREATE DATABASE Analytics")
                .unwrap()
                .unwrap(),
            DatabaseStatement::Create {
                name: "analytics".to_string(),
                if_not_exists: false
            }
        );
        assert_eq!(
            parse_database_statement("CREATE DATABASE \"Analytics\"")
                .unwrap()
                .unwrap(),
            DatabaseStatement::Create {
                name: "Analytics".to_string(),
                if_not_exists: false
            }
        );
    }

    #[test]
    fn if_exists_shifts_the_name() {
        assert_eq!(
            parse_database_statement("DROP DATABASE IF EXISTS staging")
                .unwrap()
                .unwrap(),
            DatabaseStatement::Drop {
                name: "staging".to_string(),
                if_exists: true
            }
        );
        assert_eq!(
            parse_database_statement("CREATE DATABASE IF NOT EXISTS staging")
                .unwrap()
                .unwrap(),
            DatabaseStatement::Create {
                name: "staging".to_string(),
                if_not_exists: true
            }
        );
    }

    #[test]
    fn unsupported_options_are_reported_rather_than_ignored() {
        assert!(
            parse_database_statement("CREATE DATABASE x WITH OWNER bob")
                .unwrap()
                .is_err()
        );
    }

    #[test]
    fn other_statements_are_not_claimed() {
        for sql in [
            "SELECT 1",
            "CREATE TABLE t (id INT)",
            "DROP TABLE t",
            "CREATE USER bob",
        ] {
            assert!(parse_database_statement(sql).is_none(), "claimed {:?}", sql);
        }
    }

    #[tokio::test]
    async fn an_in_memory_catalogue_creates_and_drops() {
        init_globals();
        let catalogue = DatabaseCatalog::open(None, DEFAULT_DATABASE_NAME)
            .await
            .unwrap();
        assert_eq!(catalogue.names(), vec![DEFAULT_DATABASE_NAME.to_string()]);

        catalogue.create("analytics").await.unwrap();
        assert!(catalogue.get("analytics").is_some());
        assert!(catalogue.create("analytics").await.is_err());

        catalogue
            .drop_database("analytics", DEFAULT_DATABASE_NAME)
            .await
            .unwrap();
        assert!(catalogue.get("analytics").is_none());
    }

    #[tokio::test]
    async fn the_default_database_cannot_be_dropped() {
        init_globals();
        let catalogue = DatabaseCatalog::open(None, DEFAULT_DATABASE_NAME)
            .await
            .unwrap();
        assert!(
            catalogue
                .drop_database(DEFAULT_DATABASE_NAME, "other")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn an_attached_session_holds_a_database_open() {
        init_globals();
        let catalogue = DatabaseCatalog::open(None, DEFAULT_DATABASE_NAME)
            .await
            .unwrap();
        catalogue.create("staging").await.unwrap();

        let guard = catalogue
            .connect("staging")
            .expect("staging should be connectable");
        let err = catalogue
            .drop_database("staging", DEFAULT_DATABASE_NAME)
            .await
            .expect_err("a database with a session attached must not be dropped");
        assert!(err.contains("being accessed"), "{}", err);

        drop(guard);
        catalogue
            .drop_database("staging", DEFAULT_DATABASE_NAME)
            .await
            .expect("dropping should succeed once the session has gone");
    }

    /// A database recreated under a dropped one's name starts healthy.
    ///
    /// A data-file flush failure is recorded against the directory, and the
    /// recreated database reuses that directory's path. Remembered across the
    /// drop, it refused every commit of the new database — for files that had
    /// been unlinked — until the process restarted.
    /// Two engines on one directory would replay and truncate each other's
    /// log: the second open is refused before it reads anything, and the
    /// directory opens again once the first catalogue is gone.
    #[tokio::test]
    async fn a_second_catalogue_on_one_directory_is_refused() {
        init_globals();
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_str().unwrap();
        let first = DatabaseCatalog::open(Some(path), DEFAULT_DATABASE_NAME).await.unwrap();
        let wal = root.path().join(crate::core::wal::DEFAULT_WAL_NAME);
        let wal_before = std::fs::metadata(&wal).map(|m| m.len()).ok();

        let refused = match DatabaseCatalog::open(Some(path), DEFAULT_DATABASE_NAME).await {
            Err(e) => e,
            Ok(_) => panic!("a second catalogue opened the same directory"),
        };
        assert!(refused.contains("already open"), "{refused}");
        assert!(refused.contains(&std::process::id().to_string()) || cfg!(windows), "{refused}");
        assert_eq!(std::fs::metadata(&wal).map(|m| m.len()).ok(), wal_before, "the refused open touched the log");

        // A backup or a branch copies the directory, never the lock.
        #[cfg(feature = "s3_backup")]
        assert!(crate::core::backup::excluded_from_copy(LOCK_FILE));

        drop(first);
        DatabaseCatalog::open(Some(path), DEFAULT_DATABASE_NAME).await.unwrap();
    }

    #[tokio::test]
    async fn a_recreated_database_does_not_inherit_a_dropped_one_s_flush_failure() {
        use crate::core::storage_providers::file_sync::{
            LocalStorageProvider, directory_flush_failure, record_directory_failure,
        };
        use crate::core::storage_providers::traits::StorageIO;

        init_globals();
        let root = tempfile::tempdir().unwrap();
        let catalogue =
            DatabaseCatalog::open(Some(root.path().to_str().unwrap()), DEFAULT_DATABASE_NAME)
                .await
                .unwrap();
        catalogue.create("reused").await.unwrap();
        let dir = root.path().join("reused").to_string_lossy().to_string();

        record_directory_failure(&dir, "injected fdatasync failure".to_string());
        assert!(directory_flush_failure(&dir).is_some());

        catalogue
            .drop_database("reused", DEFAULT_DATABASE_NAME)
            .await
            .unwrap();
        catalogue.create("reused").await.unwrap();

        assert_eq!(directory_flush_failure(&dir), None);
        let provider = LocalStorageProvider::new(&dir, Some("fresh.db")).await;
        let txn = provider.begin_txn();
        let (_, token) = provider.append_data_txn(txn, b"row").await;
        provider
            .commit_durable(token)
            .await
            .expect("a commit in the recreated database was refused for the dropped one's failure");
    }

    /// The same, for a failure the dropped database records *after* the drop.
    ///
    /// The drop does not stop the database's vacuum or filter-checkpoint
    /// tasks, and once its log is out of the registry their writes take the
    /// no-log path and flush by opening the file's path — which the drop has
    /// deleted. That failed open was recorded against the directory after the
    /// drop had forgotten it, so the recreated database refused every commit
    /// for files nobody could read any more.
    #[tokio::test]
    async fn a_late_write_from_a_dropped_database_does_not_poison_its_successor() {
        use crate::core::storage_providers::file_sync::{
            LocalStorageProvider, directory_flush_failure,
        };
        use crate::core::storage_providers::traits::{StorageIO, is_poisoned};

        init_globals();
        let root = tempfile::tempdir().unwrap();
        let catalogue =
            DatabaseCatalog::open(Some(root.path().to_str().unwrap()), DEFAULT_DATABASE_NAME)
                .await
                .unwrap();
        catalogue.create("late").await.unwrap();
        let dir = root.path().join("late").to_string_lossy().to_string();

        // A maintenance task's handle on one of the database's files, opened
        // while it existed and never flushed directly.
        let survivor = LocalStorageProvider::new(&dir, Some("late_vacuum.db")).await;

        catalogue
            .drop_database("late", DEFAULT_DATABASE_NAME)
            .await
            .unwrap();
        assert!(!root.path().join("late").exists(), "the drop removes the directory");

        // Two late writes: the first finds the file gone, the second finds
        // the file already poisoned. Neither may reach the directory.
        for _ in 0..2 {
            let (_, token) = survivor
                .append_data_txn(survivor.begin_txn(), b"late")
                .await;
            // The dead file's own commits stay refused: they are not durable.
            let err = survivor
                .commit_durable(token)
                .await
                .expect_err("a write to an unlinked file was acknowledged as durable");
            assert!(is_poisoned(&err), "{err}");
        }
        assert_eq!(directory_flush_failure(&dir), None);

        catalogue.create("late").await.unwrap();
        assert_eq!(directory_flush_failure(&dir), None);
        let provider = LocalStorageProvider::new(&dir, Some("fresh.db")).await;
        let (_, token) = provider.append_data_txn(provider.begin_txn(), b"row").await;
        provider
            .commit_durable(token)
            .await
            .expect("a commit in the recreated database was refused for the dropped one's late write");
    }
}

#[cfg(test)]
mod role_seeding_tests {
    use super::*;

    /// A database is reachable from the moment it exists.
    ///
    /// `CREATE DATABASE` and `IMPORT` both seed roles through the wire layer,
    /// but the import does it *after the load finishes* — which for a real
    /// database is hours. Until then the target existed, held a growing number
    /// of tables, and refused every login, because roles are per database and
    /// it had none. An operator watching an import could not read what it had
    /// written so far, and the failure reported was "password authentication
    /// failed", which says nothing about the cause.
    ///
    /// Seeding at creation is what makes that window zero.
    #[tokio::test]
    async fn a_created_database_starts_with_the_catalogue_s_roles() {
        let catalog = DatabaseCatalog::open(None, "postgres")
            .await
            .expect("in-memory catalogue");
        let default = catalog.get("postgres").expect("the default exists");

        crate::auth::ensure_role_catalogue(
            &default,
            "operator",
            Some("s3cret"),
            crate::auth::AuthMethod::ScramSha256,
        )
        .await
        .expect("seed the default");

        let made = catalog.create("made_here").await.expect("create");

        let role = crate::auth::lookup_role(&made, "operator")
            .await
            .expect("the account that could create it can open it");
        assert!(role.can_login);
        // Copied as stored: no password was re-derived, and none had to be
        // known to carry it across.
        assert!(role.verifier.is_some(), "the verifier came across");

        // A role the default does not have is still absent, so this is a copy
        // rather than a blanket accept.
        assert!(
            crate::auth::lookup_role(&made, "someone_else")
                .await
                .is_none()
        );
    }

    /// Trust authentication keeps no catalogue, so there is nothing to carry
    /// and creation must not fail trying.
    #[tokio::test]
    async fn a_database_is_created_even_when_there_are_no_roles_to_carry() {
        let catalog = DatabaseCatalog::open(None, "postgres")
            .await
            .expect("in-memory catalogue");
        assert!(catalog.create("no_roles_here").await.is_ok());
    }
}

#[cfg(test)]
mod loading_tests {
    use super::*;

    /// A database being imported must not accept clients.
    ///
    /// An import restores the source's keys only once every row is in, so a
    /// client writing before then writes rows nothing constrains — and the keys
    /// cannot be put back afterwards. An application recorded two page hits
    /// into a half-imported table and the source's `UNIQUE (endpoint)` could no
    /// longer be added, failing an import that had already copied a million
    /// rows.
    #[tokio::test]
    async fn a_database_being_imported_is_closed_to_clients() {
        let catalog = DatabaseCatalog::open(None, "postgres")
            .await
            .expect("in-memory catalogue");
        catalog.create("loading_db").await.expect("create");

        assert!(
            !catalog.is_loading("loading_db"),
            "not loading before it starts"
        );

        let guard = catalog.begin_loading("loading_db").expect("guard");
        assert!(catalog.is_loading("loading_db"));
        // Other databases are unaffected.
        assert!(!catalog.is_loading("postgres"));

        drop(guard);
        assert!(
            !catalog.is_loading("loading_db"),
            "the database has to reopen when the import ends"
        );
    }

    /// However the import ends — including a failure that unwinds — the
    /// database must not be left permanently shut.
    #[tokio::test]
    async fn a_failed_import_still_reopens_the_database() {
        let catalog = DatabaseCatalog::open(None, "postgres")
            .await
            .expect("in-memory catalogue");
        catalog.create("failed_db").await.expect("create");

        let result: Result<(), &str> = {
            let _guard = catalog.begin_loading("failed_db").expect("guard");
            assert!(catalog.is_loading("failed_db"));
            Err("the import failed")
        };
        assert!(result.is_err());
        assert!(
            !catalog.is_loading("failed_db"),
            "a failed import must not leave the database unreachable"
        );
    }
}
