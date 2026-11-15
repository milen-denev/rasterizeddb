//! Partitions on a key/value store (docs/kv-partitions.md §1).
//!
//! A partitioned table placed with `STORAGE PLACE TABLE t AT 'kv:<store>'`
//! before its `CREATE` keeps its partitions' row and pointer files on the
//! store; its parent files (schema, map log, ids, seq) stay in the root. The
//! map interns the location as the spec `kv:<store>` beside the directories,
//! and `MATERIALIZE { loc }` points at it like at any other location.
//!
//! Everything a directory answers with `std::fs` is answered here from the
//! store's family by file name:
//!
//! - **exists**: from the names the family recovered from the store and the
//!   files opened since ([`PartStore::open_existing`]), never by opening: a
//!   key/value file that was never written opens as an empty one, which is
//!   exactly the silent loss I9 refuses (a materialized partition whose file
//!   is gone must refuse the table, not read as rowless);
//! - **create**: leftovers of the partition dropped first (their sweep is
//!   waited out by the open), then each file opened fresh and made to stand
//!   with an object of its own before `MATERIALIZE` is appended, so a crash
//!   after the record never leaves a materialized partition whose file the
//!   next start cannot find;
//! - **length**: the open file's length, an atomic;
//! - **delete**: the family's drop, whose sweep is recorded on local disk
//!   before it starts and resumed by the next start if a stop cuts it;
//! - **list**: the family's standing names, for the drop and the reaper.
//!
//! The trait keeps this module free of the `kv_storage` feature: a build
//! without it has no store to resolve a location to, and a table whose map
//! names one is refused with that reason.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc as StdArc;

use super::files;
use crate::core::storage_providers::backend::DbStorageProvider;

/// A boxed future, for the object-safe [`PartStore`].
pub(crate) type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The scheme a store location is spelled with, as a table placement spells
/// it (`storage::KV_SPEC_PREFIX`).
pub const STORE_SCHEME: &str = "kv:";

/// The store a location string names, as `kv:<store>`, or `None` for a
/// directory. Case-insensitive on the scheme, as `TableBackend::parse` is,
/// and never on the store's name. Interned directories are absolute paths,
/// so none parses as a store.
pub fn store_spec(location: &str) -> Option<String> {
    let trimmed = location.trim();
    if trimmed.len() <= STORE_SCHEME.len() || !trimmed[..STORE_SCHEME.len()].eq_ignore_ascii_case(STORE_SCHEME) {
        return None;
    }
    let name = trimmed[STORE_SCHEME.len()..].trim();
    (!name.is_empty()).then(|| format!("{STORE_SCHEME}{name}"))
}

/// The store's name in a spec [`store_spec`] returned.
pub fn store_name(spec: &str) -> &str {
    spec.get(STORE_SCHEME.len()..).unwrap_or(spec)
}

/// A key/value store partitions can be placed on, as the partition code
/// needs it. See the module documentation for what each method answers.
pub(crate) trait PartStore: Send + Sync {
    /// `kv:<store>`: what the map interns.
    fn spec(&self) -> &str;
    /// `name` opened if it stands in the store, `None` if it does not. Never
    /// creates it.
    fn open_existing<'a>(&'a self, name: &'a str) -> BoxFut<'a, io::Result<Option<DbStorageProvider>>>;
    /// `name` created empty and made to stand (an object of its own is
    /// stored before this returns). An error if it stands already: the
    /// caller drops leftovers first.
    fn create_fresh<'a>(&'a self, name: &'a str) -> BoxFut<'a, io::Result<DbStorageProvider>>;
    /// Drops `name`, if it stands. Its objects are swept in the background;
    /// a reopen of the name waits the sweep out.
    fn remove<'a>(&'a self, name: &'a str) -> BoxFut<'a, ()>;
    /// Whether `name` stands in the store, answered from the names the
    /// store knows, never by opening it.
    fn stands<'a>(&'a self, name: &'a str) -> BoxFut<'a, bool>;
    /// Every name that stands in the store now, sorted.
    fn names(&self) -> BoxFut<'_, Vec<String>>;
    /// Starts `file`'s background flush and compaction on the runtime the
    /// store was resolved on, once per file: an `External` partition gets no
    /// provider services of its own, and without compaction a key/value
    /// file's overlay only grows.
    fn start_service(&self, file: &DbStorageProvider);
}

/// One partition's two data files on a store, open: what its slot keeps
/// once it is materialized there, so its lengths are read without awaiting
/// and the files are never looked up by opening.
#[derive(Clone)]
pub(crate) struct StoreFiles {
    pub store: StdArc<dyn PartStore>,
    pub rows: DbStorageProvider,
    pub pointers: DbStorageProvider,
}

impl StoreFiles {
    /// (rows, pointers, index store) lengths. A store has no index-store
    /// files (persistent indexes are directory files), and no `.tmp`
    /// staging siblings.
    pub fn lens(&self) -> (u64, u64, u64) {
        (self.rows.len_now().unwrap_or(0), self.pointers.len_now().unwrap_or(0), 0)
    }

    pub fn spec(&self) -> &str {
        self.store.spec()
    }
}

/// Partition `pid` of `uid`'s two data files on `store`, which must stand
/// (I9): a missing one is a named error, never an empty file.
pub(crate) async fn require_existing(store: &StdArc<dyn PartStore>, uid: u64, pid: u32) -> io::Result<StoreFiles> {
    let [rows, pointers] = files::data_files(uid, pid);
    let open = |name: String| async move {
        store.open_existing(&name).await?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "partition file {name} is missing from key/value store '{}': its partition is materialized \
                     there, and opening it as an empty file would lose its rows",
                    store_name(store.spec())
                ),
            )
        })
    };
    let rows = open(rows).await?;
    let pointers = open(pointers).await?;
    Ok(StoreFiles { store: store.clone(), rows, pointers })
}

/// Every name partition `pid` of `uid` may have on a store: its data files
/// and the optional ones, with their staging siblings. A store never writes
/// the optional ones (no index store, no `.tmp`), but a leftover under any
/// of them would be read by a later version that does.
fn partition_names(uid: u64, pid: u32) -> Vec<String> {
    let stem = files::data_stem(uid, pid);
    let mut names: Vec<String> = files::data_files(uid, pid).to_vec();
    names.extend(files::OPTIONAL_SUFFIXES.iter().map(|s| format!("{stem}{s}")));
    let tmps: Vec<String> = names.iter().map(|n| format!("{n}.tmp")).collect();
    names.extend(tmps);
    // The data files' index-snapshot sidecars (§2). Dropped with their file,
    // so normally gone already; one a lost sweep record left behind would
    // otherwise describe the file made next under its name.
    let sidecars: Vec<String> = files::data_files(uid, pid)
        .iter()
        .map(|n| format!("{n}{}", crate::core::storage_providers::traits::KV_SIDECAR_SUFFIX))
        .collect();
    names.extend(sidecars);
    names
}

/// [`files::create_partition_files`] on a store (§3.2): any leftover of
/// partition `pid` dropped, then both data files created empty and made to
/// stand. Leftovers are dropped, never reused, for the reason the directory
/// version gives: files without `MATERIALIZE` may hold rows of a transaction
/// whose outcome was lost. A failure part-way leaves files without
/// `MATERIALIZE`, which the next attempt and the reaper drop.
pub(crate) async fn create_partition_files(store: &StdArc<dyn PartStore>, uid: u64, pid: u32) -> io::Result<StoreFiles> {
    // By name, never a listing of the whole store per first write.
    for name in partition_names(uid, pid) {
        if store.stands(&name).await {
            store.remove(&name).await;
        }
    }
    let [rows, pointers] = files::data_files(uid, pid);
    let rows = store.create_fresh(&rows).await?;
    let pointers = store.create_fresh(&pointers).await?;
    Ok(StoreFiles { store: store.clone(), rows, pointers })
}

/// The names standing on `store` that [`files::parse_name`] puts under
/// `uid`: what a DROP of the table drops there.
pub(crate) async fn uid_names(store: &dyn PartStore, uid: u64) -> Vec<String> {
    store
        .names()
        .await
        .into_iter()
        .filter(|n| files::parse_name(n).is_some_and(|f| f.uid() == uid))
        .collect()
}

/// The key/value driver's family as a [`PartStore`].
#[cfg(feature = "kv_storage")]
pub(crate) struct KvPartStore {
    spec: String,
    family: std::sync::Arc<crate::core::storage_providers::kv::KvFamily>,
    /// The runtime it was resolved on: the server's, which outlives a
    /// statement's own (a test's short-lived one), so a file's service is
    /// not cut off with it.
    runtime: Option<tokio::runtime::Handle>,
}

#[cfg(feature = "kv_storage")]
impl KvPartStore {
    pub(crate) fn new(store: &str, family: std::sync::Arc<crate::core::storage_providers::kv::KvFamily>) -> Self {
        KvPartStore {
            spec: format!("{STORE_SCHEME}{store}"),
            family,
            runtime: tokio::runtime::Handle::try_current().ok(),
        }
    }
}

#[cfg(feature = "kv_storage")]
impl PartStore for KvPartStore {
    fn spec(&self) -> &str {
        &self.spec
    }

    fn open_existing<'a>(&'a self, name: &'a str) -> BoxFut<'a, io::Result<Option<DbStorageProvider>>> {
        use crate::core::storage_providers::kv::KvStorageProvider;
        Box::pin(async move {
            Ok(KvStorageProvider::open_existing(self.family.clone(), name)
                .await
                .map_err(|e| io::Error::new(e.kind(), format!("partition file {name} on {}: {e}", self.spec)))?
                .map(DbStorageProvider::KeyValue))
        })
    }

    fn create_fresh<'a>(&'a self, name: &'a str) -> BoxFut<'a, io::Result<DbStorageProvider>> {
        use crate::core::storage_providers::kv::KvStorageProvider;
        Box::pin(async move {
            if self.family.stands(name).await {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("creating partition file {name} on {}: it exists", self.spec),
                ));
            }
            // Waits out the sweep of a dropped leftover of the name, then
            // builds the file afresh.
            let file = KvStorageProvider::open(self.family.clone(), name)
                .await
                .map_err(|e| io::Error::new(e.kind(), format!("creating partition file {name} on {}: {e}", self.spec)))?;
            if file.len_now() != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("creating partition file {name} on {}: it holds {} bytes", self.spec, file.len_now()),
                ));
            }
            file.persist_existence()
                .await
                .map_err(|e| io::Error::new(e.kind(), format!("creating partition file {name} on {}: {e}", self.spec)))?;
            Ok(DbStorageProvider::KeyValue(file))
        })
    }

    fn remove<'a>(&'a self, name: &'a str) -> BoxFut<'a, ()> {
        use crate::core::storage_providers::kv::KvStorageProvider;
        Box::pin(async move {
            // `drop_name` needs a handle on the family; any file of it is one.
            // Opened only if it stands, so a gone name is not made again.
            if let Ok(Some(file)) = KvStorageProvider::open_existing(self.family.clone(), name).await {
                file.drop_name(name).await;
            }
        })
    }

    fn stands<'a>(&'a self, name: &'a str) -> BoxFut<'a, bool> {
        Box::pin(async move { self.family.stands(name).await })
    }

    fn names(&self) -> BoxFut<'_, Vec<String>> {
        Box::pin(async move { self.family.standing_names().await })
    }

    fn start_service(&self, file: &DbStorageProvider) {
        use crate::core::storage_providers::traits::StorageIO;
        let file = file.clone();
        let started = async move { file.start_service().await };
        match &self.runtime {
            Some(rt) => {
                rt.spawn(started);
            }
            None => {
                if let Ok(rt) = tokio::runtime::Handle::try_current() {
                    rt.spawn(started);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_store_location_is_told_from_a_directory() {
        assert_eq!(store_spec("kv:logs").as_deref(), Some("kv:logs"));
        assert_eq!(store_spec("KV: logs ").as_deref(), Some("kv:logs"));
        assert_eq!(store_spec("kv:"), None);
        assert_eq!(store_spec("/srv/db/kv:logs"), None);
        assert_eq!(store_spec("/srv/db"), None);
        assert_eq!(store_name("kv:logs"), "logs");
    }
}
