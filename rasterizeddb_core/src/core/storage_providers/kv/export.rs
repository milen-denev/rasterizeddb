//! A crash-consistent copy of a family's files, for a backup.
//!
//! A table on a store has no write-ahead log, so a backup cannot copy its
//! files fuzzily and repair them by replay the way it does a directory's. It
//! needs one moment at which every file of the store is exactly what a restart
//! would recover — and a restart already knows how to find that moment: list
//! the objects, keep the records some batch declared committed. So the copy is
//! a second, read-only family opened over the same store
//! ([`KvFamily::open_snapshot`]), with the live family paused around it:
//!
//! 1. **Maintenance paused** for the whole copy. Compaction rewrites base
//!    chunks in place and deletes retired deltas, a truncation deletes chunks,
//!    adopting a scratch generation deletes the old one, a dropped file sweeps
//!    its prefix. Any of those under the reader would hand it an object newer
//!    than its listing, or none at all.
//! 2. **Untransacted writes flushed**: schema headers and the id sequence are
//!    in the store only once the background service has written them.
//! 3. **Commits held** only while the reader lists the objects. A commit is
//!    several objects, the declaring one last; a listing that passed the
//!    first before it was stored and then found the declaration would keep a
//!    transaction with half its bytes. Writers go on appending meanwhile — an
//!    undeclared batch is inert, and per file a listing always sees a prefix
//!    of the sequence, since sequences are allocated in store order. What was
//!    listed is then fetched with commits running again: a listed object
//!    never changes while maintenance is paused.
//!
//! The two gates live here, one pair per store and prefix ([`gates`]), and
//! every family serving that prefix takes them: one opened after a copy began
//! is paused as surely as one open before it.
//!
//! Commits wait for the listing alone, not for the copy; a statement that
//! truncates or a compaction waits for the copy. Meanwhile writes are held in
//! the live overlay, which only compaction empties, so a long copy under a
//! heavy write load costs memory for its length.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Weak};

use parking_lot::Mutex;

use super::provider::identity_of;
use super::{KvConfig, KvFamily, KvStorageProvider, KvStore};
use crate::core::storage_providers::traits::StorageIO;

/// Every family a server has open for its tables, so a copy can find the one
/// writing to the prefix it is about to read. Weak: registering keeps nothing
/// alive.
static LIVE: LazyLock<Mutex<Vec<(String, Weak<KvFamily>)>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// Registers a family that serves tables. See [`LIVE`].
pub fn register_live(family: &Arc<KvFamily>) {
    let identity = family.identity();
    let mut live = LIVE.lock();
    live.retain(|(_, weak)| weak.strong_count() > 0);
    live.push((identity, Arc::downgrade(family)));
}

/// The open families over `identity`. One, if the deployment keeps to one
/// writer per prefix; every one of them is paused if it does not.
fn live_families(identity: &str) -> Vec<Arc<KvFamily>> {
    LIVE.lock()
        .iter()
        .filter(|(id, _)| id == identity)
        .filter_map(|(_, weak)| weak.upgrade())
        .collect()
}

type Gate = Arc<tokio::sync::RwLock<()>>;

/// The `(maintenance, declaring)` gates of the families over each prefix.
static GATES: LazyLock<Mutex<HashMap<String, (Gate, Gate)>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// The gates every family over `identity` takes: see the module comment.
pub(super) fn gates(identity: &str) -> (Gate, Gate) {
    GATES.lock().entry(identity.to_string()).or_default().clone()
}

/// One file the copy wrote.
#[derive(Debug, Clone)]
pub struct ExportedFile {
    /// The file's name in the store, which is its name in a directory.
    pub name: String,
    pub path: PathBuf,
    pub bytes: u64,
}

/// The read-only copy keeps its own cache small: it reads every page once.
const COPY_CACHE_BYTES: u64 = 64 << 20;

/// Copies every file of the family in `store` under `config.root` that `keep`
/// accepts into `dir`, as one crash-consistent cut. See the module comment.
/// Refused before a byte is written when `dir`'s disk cannot hold the copy:
/// it is the database's own disk, and the copy is whole tables.
pub async fn export(
    store: Arc<dyn KvStore>,
    mut config: KvConfig,
    keep: impl Fn(&str) -> bool,
    dir: &Path,
) -> io::Result<Vec<ExportedFile>> {
    let identity = identity_of(store.as_ref(), &config.root);
    let (maintenance, declaring) = gates(&identity);
    let paused = maintenance.write_owned().await;
    for family in live_families(&identity) {
        family.flush_all().await?;
    }
    config.cache_bytes = config.cache_bytes.min(COPY_CACHE_BYTES);
    let reader = KvFamily::open_snapshot(store, config, Some(declaring.write_owned().await)).await?;

    let mut files = Vec::new();
    for name in reader.file_names().into_iter().filter(|name| keep(name)) {
        // A store's names are a table's file names. One that could climb out
        // of `dir` is not, and is not copied.
        if name.is_empty() || name.contains('/') || name.contains('\\') || name == "." || name == ".." {
            return Err(io::Error::other(format!("the store holds a file named {name:?}, which is not a table's")));
        }
        let file = KvStorageProvider::open(reader.clone(), &name).await?;
        let len = file.get_len().await;
        files.push((name, file, len));
    }
    tokio::fs::create_dir_all(dir).await?;
    let needed: u64 = files.iter().map(|(_, _, len)| len).sum();
    if let Some(free) = crate::core::storage::available_bytes(&dir.to_string_lossy())
        && needed.saturating_add(needed / 10) > free
    {
        return Err(io::Error::other(format!(
            "copying {needed} bytes needs more room than {} has ({free} bytes free)",
            dir.display()
        )));
    }

    let mut out = Vec::with_capacity(files.len());
    for (name, file, len) in files {
        let path = dir.join(&name);
        copy_out(&file, len, &path).await?;
        out.push(ExportedFile { name, path, bytes: len });
    }
    drop(paused);
    Ok(out)
}

async fn copy_out(file: &KvStorageProvider, len: u64, path: &Path) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;
    const CHUNK: usize = 1 << 20;
    let mut out = tokio::io::BufWriter::new(tokio::fs::File::create(path).await?);
    let mut buffer = vec![0u8; CHUNK.min(len as usize).max(1)];
    let mut position = 0u64;
    while position < len {
        let take = ((len - position) as usize).min(buffer.len());
        let mut cursor = position;
        file.read_data_into_buffer(&mut cursor, &mut buffer[..take]).await?;
        out.write_all(&buffer[..take]).await?;
        position += take as u64;
    }
    out.flush().await?;
    out.into_inner().sync_all().await
}
