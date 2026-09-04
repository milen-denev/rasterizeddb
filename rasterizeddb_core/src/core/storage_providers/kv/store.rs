//! The key/value substrate a [`super::KvStorageProvider`] is built on.
//!
//! Deliberately the smallest interface an object store can satisfy: whole-object
//! GET, ranged GET, whole-object PUT, batch DELETE, and a prefix listing. There
//! is no rename, no append, no partial write and no compare-and-swap, because S3
//! has none of those and a trait that assumed them would only be implementable
//! by something that is not an object store.
//!
//! Everything that makes the provider usable — chunking, caching, coalescing,
//! the delta log — is built *above* this trait, once, so a new backing store is
//! five methods rather than a storage engine.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

/// A boxed future, because [`KvStore`] has to be object-safe.
///
/// The provider holds one `Arc<dyn KvStore>` for a whole database rather than
/// being generic over the store: `DbStorageProvider` is an enum of concrete
/// types, so a generic parameter here would have to be named in that enum and
/// would fix the backend at compile time. The box costs an allocation per
/// request against something whose fast path is a network round trip.
/// `Sync` as well as `Send`, because [`super::super::traits::StorageIO`] demands
/// it of every future it returns and this one ends up inside them.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 'a>>;

/// One object as a listing reports it.
#[derive(Clone, Debug)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
}

/// A flat, immutable-object key/value store.
///
/// # Contract
///
/// - `put` is atomic: a reader sees either the whole previous value or the whole
///   new one, never a mix. The provider's crash safety rests entirely on this,
///   and every S3-compatible implementation gives it.
/// - `get` and `get_range` return `Ok(None)` for a missing key, not an error.
///   A sparse file's unwritten chunks are simply absent, and treating that as a
///   failure would make every hole an outage.
/// - `get_range` may return fewer bytes than asked for when the object is
///   shorter than `offset + len`. It must never return more.
/// - `delete` on a missing key succeeds.
pub trait KvStore: Send + Sync + 'static {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Vec<u8>>>>;

    fn get_range<'a>(
        &'a self,
        key: &'a str,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, io::Result<Option<Vec<u8>>>>;

    fn put<'a>(&'a self, key: &'a str, body: Vec<u8>) -> BoxFuture<'a, io::Result<()>>;

    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, io::Result<()>>;

    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<ObjectInfo>>>;

    /// How many requests this store can usefully have in flight at once.
    ///
    /// Read coalescing fans out to this width. For an object store it is a
    /// throughput knob — one connection to S3 is latency-bound long before it is
    /// bandwidth-bound — and for a local directory it is closer to a queue depth.
    fn concurrency(&self) -> usize {
        16
    }

    /// Short human-readable identity, for log lines and errors.
    fn describe(&self) -> String;
}

// ---------------------------------------------------------------------------
// In-memory store
// ---------------------------------------------------------------------------

/// A store backed by a map, for tests and for an in-process database whose
/// layout should still be the object one.
pub struct MemoryKvStore {
    objects: dashmap::DashMap<String, Arc<Vec<u8>>>,
}

impl Default for MemoryKvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryKvStore {
    pub fn new() -> Self {
        Self {
            objects: dashmap::DashMap::new(),
        }
    }

    /// Number of objects held. Tests assert on this to pin request-shape
    /// regressions — a change that turns one PUT per commit into three shows up
    /// here and nowhere else.
    pub fn object_count(&self) -> usize {
        self.objects.len()
    }

    pub fn total_bytes(&self) -> u64 {
        self.objects.iter().map(|e| e.value().len() as u64).sum()
    }
}

impl KvStore for MemoryKvStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Vec<u8>>>> {
        Box::pin(async move { Ok(self.objects.get(key).map(|v| v.value().as_ref().clone())) })
    }

    fn get_range<'a>(
        &'a self,
        key: &'a str,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, io::Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let Some(entry) = self.objects.get(key) else {
                return Ok(None);
            };
            let bytes = entry.value();
            let start = (offset as usize).min(bytes.len());
            let end = start.saturating_add(len).min(bytes.len());
            Ok(Some(bytes[start..end].to_vec()))
        })
    }

    fn put<'a>(&'a self, key: &'a str, body: Vec<u8>) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            self.objects.insert(key.to_string(), Arc::new(body));
            Ok(())
        })
    }

    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            for key in keys {
                self.objects.remove(&key);
            }
            Ok(())
        })
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<ObjectInfo>>> {
        Box::pin(async move {
            let mut out: Vec<ObjectInfo> = self
                .objects
                .iter()
                .filter(|e| e.key().starts_with(prefix))
                .map(|e| ObjectInfo {
                    key: e.key().clone(),
                    size: e.value().len() as u64,
                })
                .collect();
            out.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(out)
        })
    }

    fn describe(&self) -> String {
        "memory".to_string()
    }
}

// ---------------------------------------------------------------------------
// Local directory store
// ---------------------------------------------------------------------------

/// A store that keeps each object as a file under one directory.
///
/// Its purpose is testing and local development: the whole provider — chunk
/// mapping, delta log, compaction, recovery — runs against it with no network
/// and no MinIO, so the parts that are hard to get right are exercised by
/// ordinary `cargo test`. It is also a usable backend in its own right for a
/// database on a filesystem that is cheaper to write whole files to than to
/// update in place.
pub struct LocalDirStore {
    root: std::path::PathBuf,
}

impl LocalDirStore {
    pub fn new(root: impl Into<std::path::PathBuf>) -> io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Maps a key to a path, refusing anything that could climb out of `root`.
    ///
    /// The provider only ever generates keys it composed itself, but this store
    /// is also the one a test can hand an arbitrary key, and a traversal here
    /// writes outside the database directory.
    fn path_for(&self, key: &str) -> io::Result<std::path::PathBuf> {
        let mut path = self.root.clone();
        for segment in key.split('/') {
            if segment.is_empty() || segment == "." || segment == ".." {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsafe key segment in '{key}'"),
                ));
            }
            path.push(segment);
        }
        Ok(path)
    }

    fn walk(dir: &std::path::Path, base: &std::path::Path, out: &mut Vec<ObjectInfo>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(t) if t.is_dir() => Self::walk(&path, base, out),
                Ok(t) if t.is_file() => {
                    if let Ok(rel) = path.strip_prefix(base) {
                        let key = rel.to_string_lossy().replace('\\', "/");
                        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                        out.push(ObjectInfo { key, size });
                    }
                }
                _ => {}
            }
        }
    }
}

impl KvStore for LocalDirStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let path = self.path_for(key)?;
            tokio::task::spawn_blocking(move || match std::fs::read(&path) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            })
            .await
            .map_err(io::Error::other)?
        })
    }

    fn get_range<'a>(
        &'a self,
        key: &'a str,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, io::Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let path = self.path_for(key)?;
            tokio::task::spawn_blocking(move || {
                use std::io::{Read, Seek, SeekFrom};
                let mut file = match std::fs::File::open(&path) {
                    Ok(f) => f,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(e),
                };
                file.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; len];
                let mut filled = 0usize;
                while filled < len {
                    match file.read(&mut buf[filled..]) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                        Err(e) => return Err(e),
                    }
                }
                buf.truncate(filled);
                Ok(Some(buf))
            })
            .await
            .map_err(io::Error::other)?
        })
    }

    fn put<'a>(&'a self, key: &'a str, body: Vec<u8>) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            let path = self.path_for(key)?;
            tokio::task::spawn_blocking(move || {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                // Written to a sibling and renamed, because the whole design
                // rests on a put being all-or-nothing and a plain write is not.
                let temp = path.with_extension("tmp");
                std::fs::write(&temp, &body)?;
                std::fs::rename(&temp, &path)
            })
            .await
            .map_err(io::Error::other)?
        })
    }

    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            let paths = keys
                .iter()
                .map(|k| self.path_for(k))
                .collect::<io::Result<Vec<_>>>()?;
            tokio::task::spawn_blocking(move || {
                for path in paths {
                    match std::fs::remove_file(&path) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e),
                    }
                }
                Ok(())
            })
            .await
            .map_err(io::Error::other)?
        })
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<ObjectInfo>>> {
        Box::pin(async move {
            let root = self.root.clone();
            let prefix = prefix.to_string();
            tokio::task::spawn_blocking(move || {
                let mut out = Vec::new();
                Self::walk(&root, &root, &mut out);
                out.retain(|o| o.key.starts_with(&prefix));
                out.sort_by(|a, b| a.key.cmp(&b.key));
                Ok(out)
            })
            .await
            .map_err(io::Error::other)?
        })
    }

    fn concurrency(&self) -> usize {
        8
    }

    fn describe(&self) -> String {
        format!("dir:{}", self.root.display())
    }
}
