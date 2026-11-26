//! What every core shares, read-mostly (§4.9 G2): the map, the template
//! cache's second level, the role keys, the cancel registry, the counter
//! board and the hooks.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use rasterizeddb_router::rshard::{AuthKeys, RouteTemplate};
use rasterizeddb_router::shard_map::ShardMap;

use crate::config::Config;
use crate::counters::Board;
use crate::hooks::Hooks;

/// The cluster map. A core holds its own `Arc` and compares
/// [`MapHolder::epoch`] (one relaxed load) before each statement; only a
/// change takes the lock.
pub struct MapHolder {
    map: RwLock<Arc<ShardMap>>,
    epoch: AtomicU64,
    /// Where the highest epoch seen is persisted (R6: the gateway's only
    /// durable state), next to the map file.
    epoch_file: Option<String>,
}

impl MapHolder {
    pub fn new(map: ShardMap, epoch_file: Option<String>) -> Result<MapHolder, String> {
        if let Some(f) = &epoch_file
            && let Ok(text) = std::fs::read_to_string(f)
            && let Ok(seen) = text.trim().parse::<u64>()
            && seen > map.epoch()
        {
            return Err(format!(
                "the map is at epoch {}, but this gateway has already routed with epoch {seen} ({f}); \
                 epochs never go down: fetch a current map (map_from) or remove {f} deliberately",
                map.epoch()
            ));
        }
        let h = MapHolder { epoch: AtomicU64::new(map.epoch()), map: RwLock::new(Arc::new(map)), epoch_file };
        h.persist();
        Ok(h)
    }

    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn get(&self) -> Arc<ShardMap> {
        self.map.read().map(|m| m.clone()).unwrap_or_else(|e| e.into_inner().clone())
    }

    /// Installs `m` when it is of this cluster and newer. `Ok(true)` when it
    /// replaced the map.
    pub fn install(&self, m: ShardMap) -> Result<bool, String> {
        let mut cur = self.map.write().unwrap_or_else(|e| e.into_inner());
        if m.cluster_id() != cur.cluster_id() {
            return Err(format!(
                "a shard sent a map of cluster {:032x}, but this gateway serves {:032x}",
                m.cluster_id(),
                cur.cluster_id()
            ));
        }
        if m.epoch() <= cur.epoch() {
            if m.epoch() == cur.epoch() && m.digest() != cur.digest() {
                return Err(format!(
                    "two maps at epoch {} with different digests: refusing both until an operator publishes a newer one",
                    m.epoch()
                ));
            }
            return Ok(false);
        }
        let e = m.epoch();
        *cur = Arc::new(m);
        self.epoch.store(e, Ordering::Release);
        drop(cur);
        self.persist();
        Ok(true)
    }

    fn persist(&self) {
        if let Some(f) = &self.epoch_file {
            let tmp = format!("{f}.tmp");
            if std::fs::write(&tmp, format!("{}\n", self.epoch())).is_ok() {
                let _ = std::fs::rename(&tmp, f);
            }
        }
    }
}

/// One cached route template.
#[derive(Clone, Debug)]
pub struct TemplateEntry {
    pub template: RouteTemplate,
    pub schema_epoch: u64,
}

/// The template cache's shared second level: filled on a per-core miss,
/// read on the next core's miss.
pub struct SharedTemplates {
    /// `database → (shapes, parse texts)`.
    inner: Mutex<HashMap<String, (HashMap<String, TemplateEntry>, HashMap<String, TemplateEntry>)>>,
    count: AtomicUsize,
    cap: usize,
}

impl SharedTemplates {
    pub fn new(cap: usize) -> SharedTemplates {
        SharedTemplates { inner: Mutex::new(HashMap::new()), count: AtomicUsize::new(0), cap }
    }

    pub fn get(&self, database: &str, parse: bool, text: &str, min_epoch: u64) -> Option<TemplateEntry> {
        let g = self.inner.lock().ok()?;
        let (shapes, parses) = g.get(database)?;
        let e = if parse { parses.get(text) } else { shapes.get(text) }?;
        (e.schema_epoch >= min_epoch).then(|| e.clone())
    }

    pub fn put(&self, database: &str, parse: bool, text: &str, e: TemplateEntry) {
        let Ok(mut g) = self.inner.lock() else { return };
        if self.count.load(Ordering::Relaxed) >= self.cap {
            // Full: start over rather than track recency on a shared map.
            g.clear();
            self.count.store(0, Ordering::Relaxed);
        }
        let (shapes, parses) = g.entry(database.to_string()).or_default();
        let m = if parse { parses } else { shapes };
        if m.insert(text.to_string(), e).is_none() {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn remove(&self, database: &str, parse: bool, text: &str) {
        let Ok(mut g) = self.inner.lock() else { return };
        if let Some((shapes, parses)) = g.get_mut(database) {
            let m = if parse { parses } else { shapes };
            if m.remove(text).is_some() {
                self.count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

/// Role keys fetched from the home shard (G9), per `(database, role)`.
pub struct AuthCache {
    inner: Mutex<HashMap<(String, String), (AuthKeys, Instant)>>,
    ttl: Duration,
}

impl AuthCache {
    pub fn new(ttl: Duration) -> AuthCache {
        AuthCache { inner: Mutex::new(HashMap::new()), ttl }
    }

    pub fn get(&self, database: &str, role: &str) -> Option<AuthKeys> {
        let g = self.inner.lock().ok()?;
        let (k, at) = g.get(&(database.to_string(), role.to_string()))?;
        (at.elapsed() < self.ttl).then(|| k.clone())
    }

    pub fn put(&self, database: &str, role: &str, k: AuthKeys) {
        if let Ok(mut g) = self.inner.lock() {
            // A key of an older role epoch never replaces a newer one.
            let key = (database.to_string(), role.to_string());
            if let Some((old, _)) = g.get(&key)
                && old.role_epoch > k.role_epoch
            {
                return;
            }
            if g.len() > 100_000 {
                g.clear();
            }
            g.insert(key, (k, Instant::now()));
        }
    }

    pub fn forget(&self, database: &str, role: &str) {
        if let Ok(mut g) = self.inner.lock() {
            g.remove(&(database.to_string(), role.to_string()));
        }
    }
}

/// The virtual sessions a client session has open, by shard, for its
/// CancelRequest.
pub type CancelTargets = Arc<Mutex<Vec<(u32, u64)>>>;

/// The gateway's own BackendKeyData `(pid, secret)` → the backends to cancel.
#[derive(Default)]
pub struct CancelRegistry {
    inner: Mutex<HashMap<(i32, i32), CancelTargets>>,
}

impl CancelRegistry {
    pub fn register(&self, key: (i32, i32)) -> CancelTargets {
        let t: CancelTargets = Arc::new(Mutex::new(Vec::new()));
        if let Ok(mut g) = self.inner.lock() {
            g.insert(key, t.clone());
        }
        t
    }

    pub fn remove(&self, key: (i32, i32)) {
        if let Ok(mut g) = self.inner.lock() {
            g.remove(&key);
        }
    }

    pub fn targets(&self, key: (i32, i32)) -> Vec<(u32, u64)> {
        let t = self.inner.lock().ok().and_then(|g| g.get(&key).cloned());
        t.and_then(|t| t.lock().ok().map(|v| v.clone())).unwrap_or_default()
    }
}

/// Client-facing TLS, when configured.
#[cfg(feature = "tls")]
pub struct ClientTls {
    pub acceptor: tokio_rustls::TlsAcceptor,
    /// `tls-server-end-point`: SHA-256 of the leaf certificate (G9).
    pub end_point: [u8; 32],
}

pub struct Shared {
    pub cfg: Config,
    pub map: MapHolder,
    pub templates: SharedTemplates,
    pub auth: AuthCache,
    pub cancels: CancelRegistry,
    pub board: Board,
    pub hooks: Hooks,
    /// The highest cluster schema epoch any shard reported: templates of an
    /// older one are dropped on lookup.
    pub schema_epoch: AtomicU64,
    pub draining: AtomicBool,
    pub sessions: AtomicUsize,
    /// Transactions open under a global id (`rdb.global_txn`): the only
    /// ones the deadlock detector can follow, so it asks the shards
    /// nothing while there are none.
    pub tagged_txns: AtomicUsize,
    /// Keys the fake SCRAM salts of unknown roles.
    pub secret: [u8; 32],
    #[cfg(feature = "tls")]
    pub tls: Option<ClientTls>,
}

impl Shared {
    /// The home shard: configured, or the lowest id in the map.
    pub fn home(&self, map: &ShardMap) -> u32 {
        self.cfg.home_shard.unwrap_or_else(|| map.shards().first().map_or(0, |s| s.id))
    }

    pub fn note_schema_epoch(&self, e: u64) {
        self.schema_epoch.fetch_max(e, Ordering::AcqRel);
    }
}
