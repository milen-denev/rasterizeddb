//! The key/value stores a database's tables may be placed on.
//!
//! One file, `db_kv.cfg`, in the database directory, naming stores an operator
//! has configured. A table is placed on one with `STORAGE PLACE TABLE t AT
//! 'kv:<name>'`, and everything else about that table is unchanged — it is the
//! same `Table`, over the same `DbStorageProvider`, and only the bytes go
//! somewhere else.
//!
//! # The file is written by the server
//!
//! It used to be hand-written only. `STORAGE ADD STORE`, `ALTER STORE` and
//! `DROP STORE` now rewrite it, the same bargain `db_storage.cfg` makes: a
//! store added at runtime and forgotten at restart would leave every table
//! placed on it unopenable, so the statement has to persist or fail.
//!
//! Every gap below is a **single TAB**. Spaces do not parse, and the failure is
//! quiet: `version 1` written with a space is not recognised as the version
//! line, the whole file is rejected for having none, and every store then
//! simply does not exist. That is why a file that fails to parse is never
//! rewritten — see [`KvRegistry::readable`].
//!
//! ```text
//! # rasterizeddb key/value stores.
//! version	1
//! store	scratch	dir	/mnt/fast/kv	scratch
//! store	archive	s3	https://s3.us-east-1.amazonaws.com	us-east-1	my-bucket	rdb/archive	virtual
//! store	local	s3	http://127.0.0.1:9000	us-east-1	test	rdb/local	path	chunk_size=1048576
//! ```
//!
//! # Credentials live in a second file
//!
//! A store may carry its own, given with `WITH (ACCESS_KEY_ID = ...,
//! SECRET_ACCESS_KEY = ...)`; a store without them falls back to
//! `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`, the same
//! as the backup service. Carrying them per store is what lets two stores in one
//! database live in two accounts.
//!
//! They are **not** written to `db_kv.cfg`. That file is rewritten by the
//! server, quoted in logs and error messages, and readable by anyone who can
//! list the database directory. Credentials go to `db_kv_secrets.cfg` beside it,
//! created `0600`, and nothing renders them back: not [`KvRegistry::describe`],
//! not [`KvRegistry::list`], not an error message. The split is also what keeps
//! `db_kv.cfg` safe to paste into a ticket.
//!
//! A directory copy still takes the secrets with it — a backup of a database
//! directory carries this file like any other. That is the one place the
//! separation does not help, and it is worth knowing before pointing a backup at
//! somewhere less private than the database.
//!
//! # Stores are built once, on demand
//!
//! A store's family owns the page cache and the open files, so two tables on the
//! same store must share one — otherwise each would have its own cache budget
//! and, worse, its own idea of a file's length. The registry hands out one
//! `Arc<KvFamily>` per name, built the first time it is asked for.

// The examples above are separated by literal tabs, and they have to be: the
// file's single failure mode is an operator typing spaces, and an example
// rendered with spaces would be the thing that taught them to.
#![allow(clippy::tabs_in_doc_comments)]

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::{error, info, warn};
use parking_lot::{Mutex, RwLock};

use crate::core::storage_providers::kv::store::{KvStore, LocalDirStore};
use crate::core::storage_providers::kv::{KvConfig, KvFamily};

/// The file, in the database directory, that records the stores.
pub const KV_FILE: &str = "db_kv.cfg";

/// The file, beside it, that records per-store credentials.
///
/// Separate so the store configuration stays readable and shareable, and so the
/// thing that must be `0600` is one small file rather than the whole config.
pub const KV_SECRETS_FILE: &str = "db_kv_secrets.cfg";

const KV_FORMAT_VERSION: u32 = 1;

/// The default key prefix a store's objects live under.
pub const DEFAULT_PREFIX: &str = "rdb";

/// One `KvConfig` field a store may override, for `SHOW STORAGE KEYS` and for
/// rejecting a misspelt one rather than ignoring it.
pub struct Tunable {
    pub name: &'static str,
    /// Whether the value is a byte size, so `4MiB` is accepted for it.
    pub is_size: bool,
    pub help: &'static str,
}

/// Every setting `WITH (...)` and `ALTER STORE ... SET` accept beyond the
/// fields that say where the store is.
///
/// Enumerated rather than validated by trying to apply it: an unknown key that
/// is merely logged is a setting the operator believes they changed.
pub const TUNABLES: &[Tunable] = &[
    Tunable {
        name: "chunk_size",
        is_size: true,
        help: "bytes per base object; fewer requests per scan against more bytes per fold",
    },
    Tunable {
        name: "page_size",
        is_size: true,
        help: "cache and ranged-read granularity; must divide chunk_size",
    },
    Tunable {
        name: "cache_bytes",
        is_size: true,
        help: "resident base data, shared across every table on this store",
    },
    Tunable {
        name: "chunk_fetch_pages",
        is_size: false,
        help: "pages of one object wanted before it is fetched whole",
    },
    Tunable {
        name: "readahead_chunks",
        is_size: false,
        help: "objects pulled in behind a sequential scan",
    },
    Tunable {
        name: "max_deltas",
        is_size: false,
        help: "delta objects tolerated before compaction is forced",
    },
    Tunable {
        name: "compact_chunk_bytes",
        is_size: true,
        help: "overlay in one chunk that makes folding it worthwhile",
    },
    Tunable {
        name: "flush_interval_ms",
        is_size: false,
        help: "exposure window, in ms, for writes made outside a transaction",
    },
    Tunable {
        name: "spill_bytes",
        is_size: true,
        help: "unstored records held before they are written out early",
    },
];

/// The named tunable, if `key` is one.
pub fn tunable(key: &str) -> Option<&'static Tunable> {
    TUNABLES.iter().find(|t| t.name == key)
}

/// How one store is reached, before anything has been opened.
#[derive(Clone, Debug, PartialEq, Eq)]
enum StoreSpec {
    Directory {
        path: String,
        root: String,
    },
    #[cfg(feature = "kv_storage_s3")]
    S3 {
        endpoint: String,
        region: String,
        bucket: String,
        root: String,
        path_style: bool,
    },
    /// Parsed, but this build cannot open it.
    ///
    /// The positional fields are kept verbatim so the entry survives being
    /// rewritten by a server that does not understand it. Dropping them — which
    /// an earlier version did — meant one `STORAGE ADD STORE` on a build
    /// without `kv_storage_s3` rewrote every S3 store in the file as an empty
    /// stub, and the tables on them stopped opening.
    Unsupported {
        kind: String,
        fields: Vec<String>,
    },
}

#[derive(Clone, Debug)]
struct StoreEntry {
    spec: StoreSpec,
    tuning: Vec<(String, u64)>,
}

/// A store as an operator asks for one, before it is a [`StoreSpec`].
///
/// The SQL surface builds this out of `WITH (...)`; the registry turns it into
/// the entry it stores and writes. Separate so the parsing of an operator's
/// words and the shape of the file do not have to move together.
#[derive(Clone, Debug)]
pub enum StoreDefinition {
    Directory {
        path: String,
        prefix: String,
    },
    #[cfg(feature = "kv_storage_s3")]
    S3 {
        endpoint: String,
        region: String,
        bucket: String,
        prefix: String,
        path_style: bool,
    },
}

/// What a store signs its requests with, when it does not use the environment's.
#[derive(Clone, PartialEq, Eq)]
pub struct StoreCredentials {
    pub access_key: String,
    pub secret_key: String,
    /// Set for temporary credentials (STS, IRSA, instance roles).
    pub session_token: Option<String>,
}

/// Redacted, deliberately.
///
/// `Debug` is what a panic message, a log line and half the error paths in this
/// crate reach for. A secret that is only safe as long as nobody formats the
/// struct holding it is not safe.
impl std::fmt::Debug for StoreCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoreCredentials(<redacted>)")
    }
}

impl StoreCredentials {
    fn into_client(self) -> crate::core::backup::s3::Credentials {
        crate::core::backup::s3::Credentials {
            access_key: self.access_key,
            secret_key: self.secret_key,
            session_token: self.session_token,
        }
    }
}

/// One store, as `SHOW STORAGE STORES` reports it.
#[derive(Clone, Debug)]
pub struct StoreInfo {
    pub name: String,
    /// `s3`, `dir`, or whatever a store this build cannot open calls itself.
    pub kind: String,
    /// The endpoint for S3, the directory for a local store.
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    /// Key prefix every object of this database lives under.
    pub prefix: String,
    /// `path`, `virtual`, or empty for a store where it does not apply.
    pub addressing: String,
    /// Overrides, as `key=value` joined by spaces, or empty for the defaults.
    pub settings: String,
    /// Whether this server has actually opened it — a store nothing has read
    /// from yet has never been contacted, so a wrong endpoint is still silent.
    pub opened: bool,
    /// Whether this build can open it at all.
    pub supported: bool,
    /// `store` when it carries its own, `environment` when it falls back to the
    /// process's. Never the credentials themselves.
    pub credentials: &'static str,
}

/// The stores of one database.
pub struct KvRegistry {
    /// The database directory, or `None` for an in-memory database — which has
    /// no file to write and so no stores.
    root: Option<String>,
    /// False when `db_kv.cfg` or `db_kv_secrets.cfg` exists but could not be
    /// read or parsed.
    ///
    /// The registry is inert either way, but the two must not be confused when
    /// something wants to *write* the file: rewriting a file we failed to parse
    /// would replace an operator's stores with the empty set we read. The
    /// secrets file counts for the same reason — writing it back would revoke
    /// every store's credentials but the one being changed.
    readable: bool,
    entries: RwLock<HashMap<String, StoreEntry>>,
    /// Per-store credentials, from `db_kv_secrets.cfg`. A store with no entry
    /// here signs with the environment's.
    secrets: RwLock<HashMap<String, StoreCredentials>>,
    /// Built families, one per name. An async lock because building one is I/O
    /// and two tables opening at once must not each build their own.
    open: tokio::sync::Mutex<HashMap<String, Arc<KvFamily>>>,
    /// Held across render-write-rename, for the reason
    /// [`crate::core::storage::StorageManager::persist`] spells out.
    writing: Mutex<()>,
}

impl std::fmt::Debug for KvRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvRegistry")
            .field("root", &self.root)
            .field("readable", &self.readable)
            .field("stores", &self.names())
            .finish()
    }
}

impl KvRegistry {
    /// A registry with no stores, for a database that has no `db_kv.cfg` and for
    /// an in-memory one.
    pub fn inert() -> Self {
        Self {
            root: None,
            readable: true,
            entries: RwLock::new(HashMap::new()),
            secrets: RwLock::new(HashMap::new()),
            open: tokio::sync::Mutex::new(HashMap::new()),
            writing: Mutex::new(()),
        }
    }

    /// Reads `db_kv.cfg` from `root` if it is there.
    ///
    /// A file that cannot be parsed is reported and treated as absent, which
    /// leaves every `kv:` placement unresolvable — and [`Self::validate`] then
    /// refuses to open the database rather than letting those tables come back
    /// empty.
    pub fn open(root: &str) -> Self {
        let path = Path::new(root).join(KV_FILE);
        let mut registry = Self::inert();
        registry.root = Some(root.to_string());

        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return registry,
            Err(e) => {
                error!("could not read {}: {}", path.display(), e);
                registry.readable = false;
                return registry;
            }
        };
        match parse(&text) {
            Ok(entries) => {
                if !entries.is_empty() {
                    info!(
                        "{} configures {} key/value store(s)",
                        KV_FILE,
                        entries.len()
                    );
                }
                *registry.entries.write() = entries;
            }
            Err(e) => {
                error!(
                    "{} is not a key/value store configuration ({})",
                    path.display(),
                    e
                );
                registry.readable = false;
            }
        }

        let secrets_path = Path::new(root).join(KV_SECRETS_FILE);
        match std::fs::read_to_string(&secrets_path) {
            Ok(text) => match parse_secrets(&text) {
                Ok(secrets) => {
                    if !secrets.is_empty() {
                        // Counted, never named: a store name is safe to log, and
                        // this line is the only evidence an operator gets that
                        // the file was read at all.
                        info!(
                            "{} holds credentials for {} store(s)",
                            KV_SECRETS_FILE,
                            secrets.len()
                        );
                    }
                    *registry.secrets.write() = secrets;
                }
                Err(e) => {
                    error!("{} could not be read ({})", secrets_path.display(), e);
                    registry.readable = false;
                }
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                error!("could not read {}: {}", secrets_path.display(), e);
                registry.readable = false;
            }
        }
        registry
    }

    /// Whether `name` carries its own credentials.
    pub fn has_credentials(&self, name: &str) -> bool {
        self.secrets.read().contains_key(name)
    }

    /// The address fields of `name` that `definition` disagrees with.
    ///
    /// Empty means the store on disk is the one being asked for. What this
    /// exists for is a deployment that issues its `ADD STORE` on every start:
    /// re-issuing the same definition has to be a no-op, and re-issuing a
    /// *different* one has to be told apart from it rather than quietly leaving
    /// the old store in place under the new name's intent.
    pub fn differences(&self, name: &str, definition: &StoreDefinition) -> Vec<&'static str> {
        let entries = self.entries.read();
        let Some(entry) = entries.get(name) else {
            return Vec::new();
        };
        let mut differ = Vec::new();
        let mut compare = |what: &'static str, a: &str, b: &str| {
            if a != b {
                differ.push(what);
            }
        };
        match (&entry.spec, definition) {
            (
                StoreSpec::Directory { path, root },
                StoreDefinition::Directory {
                    path: want_path,
                    prefix,
                },
            ) => {
                compare("PATH", path, want_path);
                compare("PREFIX", root, prefix);
            }
            #[cfg(feature = "kv_storage_s3")]
            (
                StoreSpec::S3 {
                    endpoint,
                    region,
                    bucket,
                    root,
                    path_style,
                },
                StoreDefinition::S3 {
                    endpoint: want_endpoint,
                    region: want_region,
                    bucket: want_bucket,
                    prefix,
                    path_style: want_style,
                },
            ) => {
                compare("ENDPOINT", endpoint, want_endpoint);
                compare("REGION", region, want_region);
                compare("BUCKET", bucket, want_bucket);
                compare("PREFIX", root, prefix);
                if path_style != want_style {
                    differ.push("ADDRESSING");
                }
            }
            // Same name, different kind of store entirely.
            _ => differ.push("the kind of store"),
        }
        differ
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    pub fn has(&self, name: &str) -> bool {
        self.entries.read().contains_key(name)
    }

    /// Configured store names, sorted.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.entries.read().keys().cloned().collect();
        names.sort_unstable();
        names
    }

    /// A one-line description of each store, for `SHOW STORAGE`.
    pub fn describe(&self, name: &str) -> Option<String> {
        let entries = self.entries.read();
        let entry = entries.get(name)?;
        Some(match &entry.spec {
            StoreSpec::Directory { path, root } => format!("dir {path} ({root})"),
            #[cfg(feature = "kv_storage_s3")]
            StoreSpec::S3 {
                endpoint,
                bucket,
                root,
                ..
            } => {
                format!("s3 {endpoint}/{bucket} ({root})")
            }
            StoreSpec::Unsupported { kind, .. } => format!("{kind} (not supported by this build)"),
        })
    }

    /// Every store as a row, sorted by name.
    pub fn list(&self) -> Vec<StoreInfo> {
        // The set of open names is read without awaiting: `SHOW` is a
        // synchronous path, and a name that opens between this and the render
        // is reported as closed one statement early rather than blocking.
        let opened: Vec<String> = match self.open.try_lock() {
            Ok(open) => open.keys().cloned().collect(),
            Err(_) => Vec::new(),
        };
        let entries = self.entries.read();
        let secrets = self.secrets.read();
        let mut out: Vec<StoreInfo> = entries
            .iter()
            .map(|(name, entry)| {
                let settings = entry
                    .tuning
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                let mut info = StoreInfo {
                    name: name.clone(),
                    kind: String::new(),
                    endpoint: String::new(),
                    region: String::new(),
                    bucket: String::new(),
                    prefix: String::new(),
                    addressing: String::new(),
                    settings,
                    opened: opened.iter().any(|open| open == name),
                    supported: true,
                    credentials: if secrets.contains_key(name) {
                        "store"
                    } else {
                        "environment"
                    },
                };
                match &entry.spec {
                    StoreSpec::Directory { path, root } => {
                        info.kind = "dir".to_string();
                        info.endpoint = path.clone();
                        info.prefix = root.clone();
                    }
                    #[cfg(feature = "kv_storage_s3")]
                    StoreSpec::S3 {
                        endpoint,
                        region,
                        bucket,
                        root,
                        path_style,
                    } => {
                        info.kind = "s3".to_string();
                        info.endpoint = endpoint.clone();
                        info.region = region.clone();
                        info.bucket = bucket.clone();
                        info.prefix = root.clone();
                        info.addressing = if *path_style { "path" } else { "virtual" }.to_string();
                    }
                    StoreSpec::Unsupported { kind, fields } => {
                        info.kind = kind.clone();
                        info.endpoint = fields.join(" ");
                        info.supported = false;
                    }
                }
                info
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Whether this server has built a family for `name` yet.
    ///
    /// A store nothing has opened can have its address changed freely; one that
    /// is open is already serving reads from where it used to point.
    pub fn is_open(&self, name: &str) -> bool {
        match self.open.try_lock() {
            Ok(open) => open.contains_key(name),
            // Someone is building a family right now. Treating that as open is
            // the conservative reading: the alternative lets an address change
            // race the build it would invalidate.
            Err(_) => true,
        }
    }

    /// The family for `name`, building it the first time.
    pub async fn family(&self, name: &str) -> io::Result<Arc<KvFamily>> {
        let mut open = self.open.lock().await;
        if let Some(existing) = open.get(name) {
            return Ok(existing.clone());
        }
        let (store, config) = self.connect(name)?;
        let family = KvFamily::open(store, config).await?;
        info!(
            "key/value store '{name}' opened ({})",
            family.store().describe()
        );
        // So a backup copying this store can pause it: see
        // `kv::export`.
        crate::core::storage_providers::kv::export::register_live(&family);
        open.insert(name.to_string(), family.clone());
        Ok(family)
    }

    /// A client for store `name` and the configuration its family opens
    /// with, without opening it. A backup opens its own read-only family from
    /// these.
    pub fn connect(&self, name: &str) -> io::Result<(Arc<dyn KvStore>, KvConfig)> {
        let entry = self.entries.read().get(name).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no key/value store named '{name}' in {KV_FILE}"),
            )
        })?;
        let credentials = self.secrets.read().get(name).cloned();

        let (store, root): (Arc<dyn KvStore>, String) = match &entry.spec {
            StoreSpec::Directory { path, root } => {
                (Arc::new(LocalDirStore::new(path)?), root.clone())
            }
            #[cfg(feature = "kv_storage_s3")]
            StoreSpec::S3 {
                endpoint,
                region,
                bucket,
                root,
                path_style,
            } => {
                let store = crate::core::storage_providers::kv::S3KvStore::from_parts_with(
                    endpoint,
                    region,
                    bucket,
                    *path_style,
                    credentials.map(StoreCredentials::into_client),
                )?;
                (Arc::new(store), root.clone())
            }
            StoreSpec::Unsupported { kind, .. } => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "key/value store '{name}' is of kind '{kind}', which this build was \
                         not compiled with"
                    ),
                ));
            }
        };

        let mut config = KvConfig::new(root);
        for (key, value) in &entry.tuning {
            apply_tuning(&mut config, key, *value);
        }
        Ok((store, config))
    }

    /// Refuses the database if a table is placed on a store that is not here.
    ///
    /// Checked at open, so the alternative — a table quietly resolving to the
    /// database directory and reading as empty — is never reached. That is the
    /// same reasoning the pool applies to a placement naming a directory it no
    /// longer lists, except that a missing store leaves nothing to point at.
    pub fn validate(&self, placements: &[(String, String)]) -> io::Result<()> {
        let mut missing: Vec<String> = Vec::new();
        for (table, spec) in placements {
            if let super::TableBackend::KeyValue(name) = super::TableBackend::parse(spec)
                && !self.has(&name)
            {
                missing.push(format!("{table} -> kv:{name}"));
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "{} does not configure the key/value store(s) these tables are placed on: {}. \
                 Restore it, or move the tables, before opening this database",
                KV_FILE,
                missing.join(", ")
            ),
        ))
    }

    // -- mutation ---------------------------------------------------------

    /// Defines a new store and writes the file.
    ///
    /// Rejected if the name is taken: a redefinition through `ADD` would move
    /// every table already on that name to different bytes without saying so.
    pub fn add_store(
        &self,
        name: &str,
        definition: StoreDefinition,
        tuning: Vec<(String, u64)>,
        credentials: Option<StoreCredentials>,
    ) -> Result<(), String> {
        let name = check_name(name)?;
        self.writable()?;
        let spec = definition.into_spec()?;
        check_tuning(spec_prefix(&spec), &tuning)?;

        {
            let mut entries = self.entries.write();
            if entries.contains_key(&name) {
                return Err(format!(
                    "a key/value store named '{name}' already exists. \
                     STORAGE ALTER STORE {name} SET <key> = <value> changes one"
                ));
            }
            entries.insert(name.clone(), StoreEntry { spec, tuning });
        }
        if let Some(credentials) = credentials {
            self.secrets.write().insert(name.clone(), credentials);
        }

        // Written before the statement reports success, and rolled back if it
        // cannot be: a store that exists only in this process is one every
        // table created on it stops finding at the next restart.
        //
        // The secrets go first. Dying between the two leaves a credential for a
        // store that does not exist, which is inert; the other order leaves a
        // store whose credentials are only in this process, which works until it
        // restarts and then fails to authenticate.
        if let Err(e) = self.persist_secrets().and_then(|()| self.persist()) {
            self.entries.write().remove(&name);
            self.secrets.write().remove(&name);
            let _ = self.persist_secrets();
            return Err(e);
        }
        info!("key/value store '{}' added to {}", name, KV_FILE);
        Ok(())
    }

    /// Replaces a store's credentials, or clears them so it falls back to the
    /// environment.
    ///
    /// Separate from [`Self::alter_store`] because rotating a key is not a
    /// change of address: it does not move a byte, so it is allowed while
    /// tables are placed on the store and while it is open.
    pub fn set_credentials(
        &self,
        name: &str,
        credentials: Option<StoreCredentials>,
    ) -> Result<(), String> {
        self.writable()?;
        if !self.has(name) {
            return Err(format!("no key/value store named '{name}'"));
        }
        let previous = {
            let mut secrets = self.secrets.write();
            match credentials {
                Some(credentials) => secrets.insert(name.to_string(), credentials),
                None => secrets.remove(name),
            }
        };
        if let Err(e) = self.persist_secrets() {
            let mut secrets = self.secrets.write();
            match previous {
                Some(previous) => secrets.insert(name.to_string(), previous),
                None => secrets.remove(name),
            };
            return Err(e);
        }
        Ok(())
    }

    /// Applies `changes` to an existing store and writes the file.
    ///
    /// `address_block` is the caller's answer to "is anything relying on this
    /// store pointing where it points": `None` to allow the fields that say
    /// where the store is to change, or `Some(reason)` to refuse them and say
    /// why. The registry does not know the placement map, and a store's address
    /// moving out from under a table is how that table starts reading an empty
    /// prefix — but the reason it is in use is the caller's to name.
    ///
    /// Returns the keys that changed, in the order given.
    pub fn alter_store(
        &self,
        name: &str,
        changes: &[(String, String)],
        address_block: Option<&str>,
    ) -> Result<Vec<String>, String> {
        self.writable()?;
        let previous = {
            let entries = self.entries.read();
            entries
                .get(name)
                .cloned()
                .ok_or_else(|| format!("no key/value store named '{name}'"))?
        };

        let mut entry = previous.clone();
        let mut applied = Vec::new();

        for (key, value) in changes {
            let key = key.to_ascii_lowercase();
            if let Some(field) = tunable(&key) {
                let parsed = parse_setting(field, value)?;
                entry.tuning.retain(|(existing, _)| *existing != key);
                entry.tuning.push((key.clone(), parsed));
                applied.push(key);
                continue;
            }
            if let Some(reason) = address_block {
                return Err(format!(
                    "{key} says where store '{name}' points, and changing it does not move \
                     a byte: the old prefix keeps them and anything reading the store finds \
                     an empty one. {reason}. The settings that only tune it can change at \
                     any time: {}",
                    TUNABLES
                        .iter()
                        .map(|t| t.name)
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            apply_address(&mut entry.spec, &key, value, name)?;
            applied.push(key);
        }

        if applied.is_empty() {
            return Err(format!(
                "nothing to change. A store takes {} and the fields that say where it is",
                TUNABLES
                    .iter()
                    .map(|t| t.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        entry.tuning.sort_by(|a, b| a.0.cmp(&b.0));
        // Checked against the entry as it *would be*, not against the change
        // alone: `PAGE_SIZE` and `CHUNK_SIZE` are only valid as a pair, so
        // altering one has to be judged beside whatever the other already is.
        check_tuning(spec_prefix(&entry.spec), &entry.tuning)?;
        self.entries.write().insert(name.to_string(), entry);
        if let Err(e) = self.persist() {
            self.entries.write().insert(name.to_string(), previous);
            return Err(e);
        }
        Ok(applied)
    }

    /// Removes a store and writes the file.
    ///
    /// The caller checks first that no table is placed on it — see
    /// [`Self::validate`] for what a placement pointing at a store that is gone
    /// does to the next start-up.
    pub fn drop_store(&self, name: &str) -> Result<(), String> {
        self.writable()?;
        let previous = self
            .entries
            .write()
            .remove(name)
            .ok_or_else(|| format!("no key/value store named '{name}'"))?;

        let previous_secret = self.secrets.write().remove(name);
        if let Err(e) = self.persist_secrets().and_then(|()| self.persist()) {
            self.entries.write().insert(name.to_string(), previous);
            if let Some(secret) = previous_secret {
                self.secrets.write().insert(name.to_string(), secret);
            }
            let _ = self.persist_secrets();
            return Err(e);
        }
        // The family, if one was built, is deliberately left open. Anything
        // still holding a table on it keeps working until it is closed; taking
        // the store away underneath a live read would turn a configuration
        // change into an I/O error in an unrelated statement.
        info!("key/value store '{}' removed from {}", name, KV_FILE);
        Ok(())
    }

    /// Whether this registry can be written at all, and why not if it cannot.
    fn writable(&self) -> Result<(), String> {
        if self.root.is_none() {
            return Err("this database is in memory and has no key/value stores".to_string());
        }
        if !self.readable {
            return Err(format!(
                "{} exists but could not be read, so this server does not know what it \
                 contains; writing it would replace whatever is in it with nothing. Fix or \
                 remove the file while the server is stopped",
                KV_FILE
            ));
        }
        Ok(())
    }

    /// Writes the stores to `db_kv.cfg`.
    ///
    /// Through a temporary file and a rename, and holding a lock across
    /// render-write-rename, for the reasons set out on
    /// [`crate::core::storage::StorageManager::persist`]. Unlike that one this
    /// reports failure to its caller, because every caller is a statement an
    /// operator is waiting on: a silent failure here is a store they believe
    /// they created.
    fn persist(&self) -> Result<(), String> {
        let Some(root) = self.root.as_deref() else {
            return Err("this database is in memory".to_string());
        };

        let _one_at_a_time = self.writing.lock();
        let text = self.render();
        let final_path = Path::new(root).join(KV_FILE);
        let temp_path: PathBuf = Path::new(root).join(format!("{}.tmp", KV_FILE));

        std::fs::write(&temp_path, text.as_bytes())
            .map_err(|e| format!("could not write {}: {}", temp_path.display(), e))?;
        // The bytes, then the directory entry. Renaming over a file whose
        // contents are still only in the page cache would survive a crash as an
        // empty configuration — which is every store gone.
        if let Ok(file) = std::fs::File::open(&temp_path) {
            let _ = file.sync_all();
        }
        if let Err(e) = std::fs::rename(&temp_path, &final_path) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(format!("could not replace {}: {}", final_path.display(), e));
        }
        let _ = crate::core::wal::sync_parent_dir(&final_path);
        Ok(())
    }

    /// Writes the credentials to `db_kv_secrets.cfg`, readable only by this
    /// user.
    ///
    /// The mode is set on the temporary file *before* the rename, so the file
    /// is never world-readable even for the instant between creating it and
    /// tightening it. Setting it afterwards would leave exactly the window this
    /// exists to close.
    ///
    /// An empty set removes the file rather than leaving an empty one: "there
    /// are no per-store credentials" and "there is a file that grants nothing"
    /// should not look different on disk.
    fn persist_secrets(&self) -> Result<(), String> {
        let Some(root) = self.root.as_deref() else {
            return Err("this database is in memory".to_string());
        };

        let _one_at_a_time = self.writing.lock();
        let final_path = Path::new(root).join(KV_SECRETS_FILE);

        let text = {
            let secrets = self.secrets.read();
            if secrets.is_empty() {
                drop(secrets);
                match std::fs::remove_file(&final_path) {
                    Ok(()) => return Ok(()),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
                    Err(e) => {
                        return Err(format!("could not remove {}: {}", final_path.display(), e));
                    }
                }
            }
            let mut names: Vec<&String> = secrets.keys().collect();
            names.sort_unstable();
            let mut out = String::new();
            out.push_str(
                "# rasterizeddb key/value store credentials. Rewritten by the server.\n\
                 # Keep this file to yourself: it is mode 0600 and holds secrets verbatim.\n",
            );
            out.push_str(&format!("version\t{}\n", KV_FORMAT_VERSION));
            for name in names {
                let credentials = &secrets[name];
                // A `-` for an absent session token rather than an empty field:
                // the parser drops empty fields, so an empty one would shift
                // every field after it.
                out.push_str(&format!(
                    "credentials\t{}\t{}\t{}\t{}\n",
                    name,
                    credentials.access_key,
                    credentials.secret_key,
                    credentials.session_token.as_deref().unwrap_or("-"),
                ));
            }
            out
        };

        let temp_path: PathBuf = Path::new(root).join(format!("{}.tmp", KV_SECRETS_FILE));
        write_private(&temp_path, text.as_bytes())
            .map_err(|e| format!("could not write {}: {}", temp_path.display(), e))?;
        if let Ok(file) = std::fs::File::open(&temp_path) {
            let _ = file.sync_all();
        }
        if let Err(e) = std::fs::rename(&temp_path, &final_path) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(format!("could not replace {}: {}", final_path.display(), e));
        }
        let _ = crate::core::wal::sync_parent_dir(&final_path);
        Ok(())
    }

    fn render(&self) -> String {
        let entries = self.entries.read();
        let mut names: Vec<&String> = entries.keys().collect();
        names.sort_unstable();

        let mut out = String::new();
        out.push_str(
            "# rasterizeddb key/value stores. Rewritten by the server; edit while it is stopped.\n\
             # Fields are separated by a single TAB. No credentials are in this file: a store\n\
             # signs with its entry in db_kv_secrets.cfg, or with the server's AWS_* variables.\n",
        );
        out.push_str(&format!("version\t{}\n", KV_FORMAT_VERSION));

        for name in names {
            let entry = &entries[name];
            let mut fields: Vec<String> = Vec::new();
            match &entry.spec {
                StoreSpec::Directory { path, root } => {
                    fields.push("dir".to_string());
                    fields.push(path.clone());
                    fields.push(root.clone());
                }
                #[cfg(feature = "kv_storage_s3")]
                StoreSpec::S3 {
                    endpoint,
                    region,
                    bucket,
                    root,
                    path_style,
                } => {
                    fields.push("s3".to_string());
                    fields.push(endpoint.clone());
                    fields.push(region.clone());
                    fields.push(bucket.clone());
                    fields.push(root.clone());
                    fields.push(if *path_style { "path" } else { "virtual" }.to_string());
                }
                StoreSpec::Unsupported { kind, fields: kept } => {
                    fields.push(kind.clone());
                    fields.extend(kept.iter().cloned());
                }
            }
            for (key, value) in &entry.tuning {
                fields.push(format!("{key}={value}"));
            }
            out.push_str(&format!("store\t{}\t{}\n", name, fields.join("\t")));
        }
        out
    }
}

impl StoreDefinition {
    fn into_spec(self) -> Result<StoreSpec, String> {
        match self {
            StoreDefinition::Directory { path, prefix } => {
                check_field("path", &path)?;
                check_field("prefix", &prefix)?;
                Ok(StoreSpec::Directory { path, root: prefix })
            }
            #[cfg(feature = "kv_storage_s3")]
            StoreDefinition::S3 {
                endpoint,
                region,
                bucket,
                prefix,
                path_style,
            } => {
                check_field("endpoint", &endpoint)?;
                check_field("region", &region)?;
                check_field("bucket", &bucket)?;
                check_field("prefix", &prefix)?;
                Ok(StoreSpec::S3 {
                    endpoint,
                    region,
                    bucket,
                    root: prefix,
                    path_style,
                })
            }
        }
    }
}

/// Rewrites one address field of an existing store.
fn apply_address(spec: &mut StoreSpec, key: &str, value: &str, name: &str) -> Result<(), String> {
    check_field(key, value)?;
    match spec {
        StoreSpec::Directory { path, root } => match key {
            "path" | "directory" => *path = value.to_string(),
            "prefix" | "root" => *root = value.to_string(),
            other => {
                return Err(format!(
                    "store '{name}' is a directory store: it has PATH and PREFIX, not {other}"
                ));
            }
        },
        #[cfg(feature = "kv_storage_s3")]
        StoreSpec::S3 {
            endpoint,
            region,
            bucket,
            root,
            path_style,
        } => match key {
            "endpoint" => *endpoint = value.to_string(),
            "region" => *region = value.to_string(),
            "bucket" => *bucket = value.to_string(),
            "prefix" | "root" => *root = value.to_string(),
            "addressing" | "style" => *path_style = parse_addressing(value)?,
            other => {
                return Err(format!(
                    "store '{name}' is an S3 store: it has ENDPOINT, REGION, BUCKET, PREFIX \
                     and ADDRESSING, not {other}"
                ));
            }
        },
        StoreSpec::Unsupported { kind, .. } => {
            return Err(format!(
                "store '{name}' is of kind '{kind}', which this build cannot open, so its \
                 address cannot be checked or changed here"
            ));
        }
    }
    Ok(())
}

/// `path` or `virtual`, which is how a bucket name that is not DNS-safe is
/// reached.
pub fn parse_addressing(value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "path" | "path-style" | "path_style" => Ok(true),
        "virtual" | "vhost" | "virtual-hosted" | "dns" => Ok(false),
        other => Err(format!(
            "unknown ADDRESSING {other:?}: expected 'virtual' (the default) or 'path' \
             (MinIO, and any bucket whose name is not DNS-safe)"
        )),
    }
}

/// Reads a tunable's value, accepting `4MiB` where the field is a size.
pub fn parse_setting(field: &Tunable, value: &str) -> Result<u64, String> {
    let value = value.trim();
    if field.is_size {
        return super::parse_bytes(value).ok_or_else(|| {
            format!(
                "{} is a size: write it as 4MiB, 512KB or a plain byte count, not {:?}",
                field.name, value
            )
        });
    }
    value
        .parse()
        .map_err(|_| format!("{} is a whole number, not {:?}", field.name, value))
}

/// The key prefix a spec stores under, which `KvConfig::validate` also checks.
fn spec_prefix(spec: &StoreSpec) -> &str {
    match spec {
        StoreSpec::Directory { root, .. } => root,
        #[cfg(feature = "kv_storage_s3")]
        StoreSpec::S3 { root, .. } => root,
        // Nothing here can open it anyway; give the check a valid prefix so it
        // reports the tuning rather than a prefix the operator never typed.
        StoreSpec::Unsupported { .. } => DEFAULT_PREFIX,
    }
}

/// Whether a set of overrides produces a configuration that can open a file.
///
/// Called by the statement that defines or alters a store, so the refusal
/// arrives while the operator is still looking at the values they typed.
///
/// This check existed already, on [`KvConfig::validate`], but only ran when a
/// family was *opened* — which is long after `STORAGE ADD STORE` has reported
/// success. `CHUNK_SIZE = 4096000` (a decimal 4 MB, not a multiple of the
/// 65,536-byte page) therefore produced a store that was listed by
/// `SHOW STORAGE STORES`, accepted `STORAGE PLACE TABLE`, and appeared to take
/// writes and reads — while never opening. The only symptoms were `opened = f`
/// and one line in the server log, and it cost a day of someone's deployment.
///
/// The message names the nearest values that would work, because "not a
/// multiple of the page size" is arithmetic the operator should not have to do
/// while debugging a deployment.
pub fn check_tuning(prefix: &str, tuning: &[(String, u64)]) -> Result<(), String> {
    let mut config = KvConfig::new(prefix.to_string());
    for (key, value) in tuning {
        apply_tuning(&mut config, key, *value);
    }
    if let Err(e) = config.validate() {
        let mut message = e.to_string();
        if config.page_size > 0 && config.chunk_size % config.page_size != 0 {
            let below = (config.chunk_size / config.page_size) * config.page_size;
            let above = below + config.page_size;
            message.push_str(&format!(
                ". The nearest usable CHUNK_SIZE values are {} and {} ({} is a common                  intended one — a binary 4 MiB rather than a decimal 4 MB)",
                below, above, 4 * 1024 * 1024
            ));
        }
        return Err(message);
    }
    Ok(())
}

/// A store name has to survive the file it is written to and the spec it is
/// named in.
fn check_name(name: &str) -> Result<String, String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("a store needs a name, as STORAGE ADD STORE archive S3 WITH (...)".to_string());
    }
    // A tab would split the line and the rest of the definition would be read
    // as different fields; whitespace and `=` break the placement spec and the
    // settings syntax the same way. Refused rather than escaped: the name is
    // typed into `kv:<name>` by hand.
    if name
        .chars()
        .any(|c| c.is_whitespace() || c == '=' || c == ':')
    {
        return Err(format!(
            "{name:?} is not a usable store name: no spaces, tabs, '=' or ':'"
        ));
    }
    Ok(name.to_string())
}

/// The same, for a field of a definition.
fn check_field(what: &str, value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{} cannot be empty", what.to_ascii_uppercase()));
    }
    if value.contains('\t') || value.contains('\n') {
        return Err(format!(
            "{} cannot contain a tab or a newline",
            what.to_ascii_uppercase()
        ));
    }
    // Settings are told apart from positional fields by the `=` in them, so a
    // positional field containing one would be read back as a setting and the
    // store would lose an address field it appeared to have.
    if value.contains('=') {
        return Err(format!(
            "{} cannot contain '=': the store file tells settings from addresses by it",
            what.to_ascii_uppercase()
        ));
    }
    Ok(())
}

fn apply_tuning(config: &mut KvConfig, key: &str, value: u64) {
    match key {
        "chunk_size" => config.chunk_size = value,
        "page_size" => config.page_size = value,
        "cache_bytes" => config.cache_bytes = value,
        "chunk_fetch_pages" => config.chunk_fetch_pages = value,
        "readahead_chunks" => config.readahead_chunks = value,
        "max_deltas" => config.max_deltas = value as usize,
        "compact_chunk_bytes" => config.compact_chunk_bytes = value,
        "flush_interval_ms" => config.flush_interval_ms = value,
        "spill_bytes" => config.spill_bytes = value,
        other => error!("{}: ignoring unknown store setting {:?}", KV_FILE, other),
    }
}

/// Creates `path` with owner-only permissions and writes `body` to it.
///
/// On Unix the mode is part of the `open`, so there is no instant at which the
/// file exists with a wider one. Elsewhere the file inherits whatever the
/// platform gives it and this says so once — refusing to run would be worse
/// than a warning for a platform where the equivalent is an ACL nobody set.
fn write_private(path: &Path, body: &[u8]) -> io::Result<()> {
    use std::io::Write;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    #[cfg(not(unix))]
    {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            warn!(
                "{} holds credentials and this platform has no mode to restrict it to; \
                 check that the database directory is not readable by other users",
                KV_SECRETS_FILE
            );
        });
    }
    // An existing file keeps the mode it was created with, so a file written
    // before this code — or by an operator's editor — is tightened too.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = file.set_permissions(std::fs::Permissions::from_mode(0o600));
    }
    file.write_all(body)?;
    Ok(())
}

fn parse_secrets(text: &str) -> Result<HashMap<String, StoreCredentials>, String> {
    let mut out: HashMap<String, StoreCredentials> = HashMap::new();
    let mut version = None;

    for (number, line) in text.lines().enumerate() {
        let line = line.trim_end_matches(['\r', '\n']);
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split('\t').filter(|f| !f.trim().is_empty());
        let keyword = fields.next().unwrap_or("");
        // Deliberately without the offending text: this parser's input is
        // secrets, and an error message quoting the line would put one in the
        // log. The line number is enough to find it.
        let at = || format!("line {} is malformed", number + 1);

        match keyword {
            "version" => {
                let value: u32 = fields
                    .next()
                    .and_then(|v| v.trim().parse().ok())
                    .ok_or_else(at)?;
                if value > KV_FORMAT_VERSION {
                    return Err(format!(
                        "this file is version {}, and this build understands up to {}",
                        value, KV_FORMAT_VERSION
                    ));
                }
                version = Some(value);
            }
            "credentials" => {
                let name = fields.next().ok_or_else(at)?.trim().to_string();
                let access_key = fields.next().ok_or_else(at)?.trim().to_string();
                let secret_key = fields.next().ok_or_else(at)?.trim().to_string();
                let session_token = match fields.next().map(str::trim) {
                    Some("-") | None => None,
                    Some(token) => Some(token.to_string()),
                };
                if name.is_empty() || access_key.is_empty() || secret_key.is_empty() {
                    return Err(at());
                }
                if out
                    .insert(
                        name,
                        StoreCredentials {
                            access_key,
                            secret_key,
                            session_token,
                        },
                    )
                    .is_some()
                {
                    return Err(format!(
                        "line {}: a store is given credentials twice",
                        number + 1
                    ));
                }
            }
            _ => {
                // Not named, for the same reason the errors are not: an unknown
                // directive here is still a line out of a file of secrets.
                warn!("{}: ignoring an unknown directive", KV_SECRETS_FILE);
            }
        }
    }

    if version.is_none() {
        return Err("no version line".to_string());
    }
    Ok(out)
}

fn parse(text: &str) -> Result<HashMap<String, StoreEntry>, String> {
    let mut entries: HashMap<String, StoreEntry> = HashMap::new();
    let mut version = None;

    for (number, line) in text.lines().enumerate() {
        let line = line.trim_end_matches(['\r', '\n']);
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split('\t').filter(|f| !f.trim().is_empty());
        let keyword = fields.next().unwrap_or("");
        let at = |what: &str| format!("line {}: {} is malformed", number + 1, what);

        match keyword {
            "version" => {
                let value: u32 = fields
                    .next()
                    .and_then(|v| v.trim().parse().ok())
                    .ok_or_else(|| at("version"))?;
                if value > KV_FORMAT_VERSION {
                    return Err(format!(
                        "this file is version {}, and this build understands up to {}",
                        value, KV_FORMAT_VERSION
                    ));
                }
                version = Some(value);
            }
            "store" => {
                let name = fields.next().ok_or_else(|| at("store"))?.trim().to_string();
                let kind = fields
                    .next()
                    .ok_or_else(|| at("store"))?
                    .trim()
                    .to_ascii_lowercase();
                // Settings are `key=value` and positional fields never contain
                // an `=`, so the two are told apart by shape rather than by
                // position — a store written without an optional positional
                // field would otherwise read its first setting as that field.
                let rest: Vec<&str> = fields.collect();
                let (positional, settings): (Vec<&str>, Vec<&str>) =
                    rest.iter().partition(|f| !f.contains('='));
                let spec = parse_store(&kind, &positional, &at)?;
                let tuning = parse_tuning(&settings, &at)?;
                if entries
                    .insert(name.clone(), StoreEntry { spec, tuning })
                    .is_some()
                {
                    return Err(format!(
                        "line {}: store {:?} is defined twice",
                        number + 1,
                        name
                    ));
                }
            }
            other => {
                warn!("{}: ignoring unknown directive {:?}", KV_FILE, other);
            }
        }
    }

    if version.is_none() {
        return Err("no version line".to_string());
    }
    Ok(entries)
}

fn parse_store(
    kind: &str,
    rest: &[&str],
    at: &dyn Fn(&str) -> String,
) -> Result<StoreSpec, String> {
    match kind {
        "dir" => {
            let path = rest
                .first()
                .ok_or_else(|| at("store dir"))?
                .trim()
                .to_string();
            let root = rest
                .get(1)
                .map(|r| r.trim())
                .unwrap_or(DEFAULT_PREFIX)
                .to_string();
            if path.is_empty() {
                return Err(at("store dir"));
            }
            Ok(StoreSpec::Directory { path, root })
        }
        #[cfg(feature = "kv_storage_s3")]
        "s3" => {
            let endpoint = rest
                .first()
                .ok_or_else(|| at("store s3"))?
                .trim()
                .to_string();
            let region = rest
                .get(1)
                .ok_or_else(|| at("store s3"))?
                .trim()
                .to_string();
            let bucket = rest
                .get(2)
                .ok_or_else(|| at("store s3"))?
                .trim()
                .to_string();
            let root = rest
                .get(3)
                .map(|r| r.trim())
                .unwrap_or(DEFAULT_PREFIX)
                .to_string();
            // MinIO and any bucket whose name is not DNS-safe need path style.
            let path_style = rest
                .get(4)
                .is_some_and(|s| s.trim().eq_ignore_ascii_case("path"));
            if endpoint.is_empty() || bucket.is_empty() {
                return Err(at("store s3"));
            }
            Ok(StoreSpec::S3 {
                endpoint,
                region,
                bucket,
                root,
                path_style,
            })
        }
        other => Ok(StoreSpec::Unsupported {
            kind: other.to_string(),
            fields: rest.iter().map(|f| f.trim().to_string()).collect(),
        }),
    }
}

fn parse_tuning(rest: &[&str], at: &dyn Fn(&str) -> String) -> Result<Vec<(String, u64)>, String> {
    let mut out = Vec::new();
    for field in rest {
        let field = field.trim();
        if field.is_empty() {
            continue;
        }
        let (key, value) = field.split_once('=').ok_or_else(|| at("store setting"))?;
        let value: u64 = value.trim().parse().map_err(|_| at("store setting"))?;
        out.push((key.trim().to_string(), value));
    }
    Ok(out)
}
