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
//! 3. **Commits held** only while the reader lists and fetches the deltas. A
//!    commit is several objects, the declaring one last; a listing that passed
//!    the first before it was stored and then found the declaration would keep
//!    a transaction with half its bytes. Writers go on appending meanwhile —
//!    an undeclared batch is inert, and per file a listing always sees a
//!    prefix of the sequence, since sequences are allocated in store order.
//!
//! Commits wait for the listing alone, not for the copy; a statement that
//! truncates or a compaction waits for the copy.

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
pub async fn export(
    store: Arc<dyn KvStore>,
    mut config: KvConfig,
    keep: impl Fn(&str) -> bool,
    dir: &Path,
) -> io::Result<Vec<ExportedFile>> {
    let live = live_families(&identity_of(store.as_ref(), &config.root));
    let mut paused = Vec::with_capacity(live.len());
    for family in &live {
        paused.push(family.pause_maintenance().await);
    }
    for family in &live {
        family.flush_all().await?;
    }
    config.cache_bytes = config.cache_bytes.min(COPY_CACHE_BYTES);
    let reader = {
        let mut held = Vec::with_capacity(live.len());
        for family in &live {
            held.push(family.hold_commits().await);
        }
        KvFamily::open_snapshot(store, config).await?
    };

    std::fs::create_dir_all(dir)?;
    let mut out = Vec::new();
    for name in reader.file_names().into_iter().filter(|name| keep(name)) {
        // A store's names are a table's file names. One that could climb out
        // of `dir` is not, and is not copied.
        if name.is_empty() || name.contains('/') || name.contains('\\') || name == "." || name == ".." {
            return Err(io::Error::other(format!("the store holds a file named {name:?}, which is not a table's")));
        }
        let file = KvStorageProvider::open(reader.clone(), &name).await?;
        let len = file.get_len().await;
        let path = dir.join(&name);
        copy_out(&file, len, &path).await?;
        out.push(ExportedFile { name, path, bytes: len });
    }
    drop(paused);
    Ok(out)
}

async fn copy_out(file: &KvStorageProvider, len: u64, path: &Path) -> io::Result<()> {
    use std::io::Write;
    const CHUNK: usize = 1 << 20;
    let mut out = std::io::BufWriter::new(std::fs::File::create(path)?);
    let mut buffer = vec![0u8; CHUNK.min(len as usize).max(1)];
    let mut position = 0u64;
    while position < len {
        let take = ((len - position) as usize).min(buffer.len());
        let mut cursor = position;
        file.read_data_into_buffer(&mut cursor, &mut buffer[..take]).await?;
        out.write_all(&buffer[..take])?;
        position += take as u64;
    }
    out.into_inner().map_err(|e| e.into_error())?.sync_all()
}
