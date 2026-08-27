//! The key/value stores a database's tables may be placed on.
//!
//! One file, `db_kv.cfg`, in the database directory, naming stores an operator
//! has configured. A table is placed on one with `STORAGE PLACE TABLE t AT
//! 'kv:<name>'`, and everything else about that table is unchanged — it is the
//! same `Table`, over the same `DbStorageProvider`, and only the bytes go
//! somewhere else.
//!
//! ```text
//! # rasterizeddb key/value stores.
//! version 1
//! store   scratch  dir  /mnt/fast/kv          scratch
//! store   archive  s3   https://s3.us-east-1.amazonaws.com  us-east-1  my-bucket  rdb/archive  virtual
//! store   local    s3   http://127.0.0.1:9000               us-east-1  test       rdb/local    path  chunk_size=1048576
//! ```
//!
//! Fields are tab-separated, as in `db_storage.cfg`.
//!
//! # Credentials are not in here
//!
//! They come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
//! `AWS_SESSION_TOKEN`, the same as the backup service. A configuration file
//! that is rewritten by the server, copied into backups and read by anyone who
//! can list the database directory is the wrong place for a secret.
//!
//! # Stores are built once, on demand
//!
//! A store's family owns the page cache and the open files, so two tables on the
//! same store must share one — otherwise each would have its own cache budget
//! and, worse, its own idea of a file's length. The registry hands out one
//! `Arc<KvFamily>` per name, built the first time it is asked for.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

use log::{error, info};

use crate::core::storage_providers::kv::store::{KvStore, LocalDirStore};
use crate::core::storage_providers::kv::{KvConfig, KvFamily};

/// The file, in the database directory, that records the stores.
pub const KV_FILE: &str = "db_kv.cfg";

const KV_FORMAT_VERSION: u32 = 1;

/// How one store is reached, before anything has been opened.
#[derive(Clone, Debug)]
enum StoreSpec {
    Directory { path: String, root: String },
    #[cfg(feature = "kv_storage_s3")]
    S3 { endpoint: String, region: String, bucket: String, root: String, path_style: bool },
    /// Parsed, but this build cannot open it.
    Unsupported { kind: String },
}

#[derive(Clone, Debug)]
struct StoreEntry {
    spec: StoreSpec,
    tuning: Vec<(String, u64)>,
}

/// The stores of one database.
pub struct KvRegistry {
    entries: HashMap<String, StoreEntry>,
    /// Built families, one per name. An async lock because building one is I/O
    /// and two tables opening at once must not each build their own.
    open: tokio::sync::Mutex<HashMap<String, Arc<KvFamily>>>,
}

impl KvRegistry {
    /// A registry with no stores, for a database that has no `db_kv.cfg` and for
    /// an in-memory one.
    pub fn inert() -> Self {
        Self { entries: HashMap::new(), open: tokio::sync::Mutex::new(HashMap::new()) }
    }

    /// Reads `db_kv.cfg` from `root` if it is there.
    ///
    /// A file that cannot be parsed is reported and treated as absent, which
    /// leaves every `kv:` placement unresolvable — and [`Self::validate`] then
    /// refuses to open the database rather than letting those tables come back
    /// empty.
    pub fn open(root: &str) -> Self {
        let path = Path::new(root).join(KV_FILE);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Self::inert(),
            Err(e) => {
                error!("could not read {}: {}", path.display(), e);
                return Self::inert();
            }
        };
        match parse(&text) {
            Ok(entries) => {
                if !entries.is_empty() {
                    info!("{} configures {} key/value store(s)", KV_FILE, entries.len());
                }
                Self { entries, open: tokio::sync::Mutex::new(HashMap::new()) }
            }
            Err(e) => {
                error!("{} is not a key/value store configuration ({})", path.display(), e);
                Self::inert()
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn has(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Configured store names, sorted.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.entries.keys().cloned().collect();
        names.sort_unstable();
        names
    }

    /// A one-line description of each store, for `SHOW STORAGE`.
    pub fn describe(&self, name: &str) -> Option<String> {
        let entry = self.entries.get(name)?;
        Some(match &entry.spec {
            StoreSpec::Directory { path, root } => format!("dir {path} ({root})"),
            #[cfg(feature = "kv_storage_s3")]
            StoreSpec::S3 { endpoint, bucket, root, .. } => {
                format!("s3 {endpoint}/{bucket} ({root})")
            }
            StoreSpec::Unsupported { kind } => format!("{kind} (not supported by this build)"),
        })
    }

    /// The family for `name`, building it the first time.
    pub async fn family(&self, name: &str) -> io::Result<Arc<KvFamily>> {
        let mut open = self.open.lock().await;
        if let Some(existing) = open.get(name) {
            return Ok(existing.clone());
        }
        let entry = self.entries.get(name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no key/value store named '{name}' in {KV_FILE}"),
            )
        })?;

        let (store, root): (Arc<dyn KvStore>, String) = match &entry.spec {
            StoreSpec::Directory { path, root } => {
                (Arc::new(LocalDirStore::new(path)?), root.clone())
            }
            #[cfg(feature = "kv_storage_s3")]
            StoreSpec::S3 { endpoint, region, bucket, root, path_style } => {
                let store =
                    crate::core::storage_providers::kv::S3KvStore::from_parts(
                        endpoint, region, bucket, *path_style,
                    )?;
                (Arc::new(store), root.clone())
            }
            StoreSpec::Unsupported { kind } => {
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
        let family = KvFamily::open(store, config).await?;
        info!("key/value store '{name}' opened ({})", family.store().describe());
        open.insert(name.to_string(), family.clone());
        Ok(family)
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
                let value: u32 =
                    fields.next().and_then(|v| v.trim().parse().ok()).ok_or_else(|| at("version"))?;
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
                let kind = fields.next().ok_or_else(|| at("store"))?.trim().to_ascii_lowercase();
                // Settings are `key=value` and positional fields never contain
                // an `=`, so the two are told apart by shape rather than by
                // position — a store written without an optional positional
                // field would otherwise read its first setting as that field.
                let rest: Vec<&str> = fields.collect();
                let (positional, settings): (Vec<&str>, Vec<&str>) =
                    rest.iter().partition(|f| !f.contains('='));
                let spec = parse_store(&kind, &positional, &at)?;
                let tuning = parse_tuning(&settings, &at)?;
                if entries.insert(name.clone(), StoreEntry { spec, tuning }).is_some() {
                    return Err(format!("line {}: store {:?} is defined twice", number + 1, name));
                }
            }
            other => {
                log::warn!("{}: ignoring unknown directive {:?}", KV_FILE, other);
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
            let path = rest.first().ok_or_else(|| at("store dir"))?.trim().to_string();
            let root = rest.get(1).map(|r| r.trim()).unwrap_or("rdb").to_string();
            if path.is_empty() {
                return Err(at("store dir"));
            }
            Ok(StoreSpec::Directory { path, root })
        }
        #[cfg(feature = "kv_storage_s3")]
        "s3" => {
            let endpoint = rest.first().ok_or_else(|| at("store s3"))?.trim().to_string();
            let region = rest.get(1).ok_or_else(|| at("store s3"))?.trim().to_string();
            let bucket = rest.get(2).ok_or_else(|| at("store s3"))?.trim().to_string();
            let root = rest.get(3).map(|r| r.trim()).unwrap_or("rdb").to_string();
            // MinIO and any bucket whose name is not DNS-safe need path style.
            let path_style = rest.get(4).is_some_and(|s| s.trim().eq_ignore_ascii_case("path"));
            if endpoint.is_empty() || bucket.is_empty() {
                return Err(at("store s3"));
            }
            Ok(StoreSpec::S3 { endpoint, region, bucket, root, path_style })
        }
        other => Ok(StoreSpec::Unsupported { kind: other.to_string() }),
    }
}

fn parse_tuning(
    rest: &[&str],
    at: &dyn Fn(&str) -> String,
) -> Result<Vec<(String, u64)>, String> {
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
