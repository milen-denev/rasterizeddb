//! Client-side sharding (feature `sharding`): one [`Router`] over N
//! servers, each holding the tenants whose shard key values hash into its
//! ranges of the [`ShardMap`].
//!
//! The router never adds a hop: [`Router::for_key`] hands out the shard's
//! own [`Client`], so a transaction, the ORM, `admin()` and `kv()` work as
//! they do against one server. A model declares its shard column
//! (`#[rdb(shard_by = "store_id")]`, or its leading `partition_by` column),
//! and [`ShardedModelExt`] routes each ORM call by it; a query routes by the
//! shard key its filter pins ([`Router::select`]), or runs on every shard
//! and merges ([`Select::all_shards`](crate::orm::Select::all_shards)).
//!
//! The hash is route_v1 from `rasterizeddb_router`, the very code the
//! engine partitions with, so a key's shard here is the shard the engine
//! (Track B) would place it on. See `docs/sharding-client.md` for the rules
//! a sharded application follows.
//!
//! **Identity.** Every shard carries a `rdb_shard_identity` row (written by
//! `rdb-shard init`). [`Router::connect`] reads it on each shard and refuses
//! a server that names another cluster or shard ([`Error::IdentityMismatch`])
//! or a newer map ([`Error::MapStale`]): a wrong server behind an address is
//! a connect error, never a misrouted write.

mod cache;
mod identity;
mod join;
mod model;
mod routed;

pub use cache::{ShardedCached, ShardedKvRepo};
pub use identity::{IDENTITY_TABLE, Identity};
pub use model::{ShardedInsertMany, ShardedModelExt};
pub use rasterizeddb_router::shard_map::BLOCKS;
pub use rasterizeddb_router::{ShardInfo, ShardMap};
pub use routed::{RoutedSelect, RoutedUpdate};

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::Duration;

use rasterizeddb_router::KeyValue;
use rasterizeddb_router::key::shard_block;

use crate::orm::{Model, ShardFamily, ShardKey, Sharding};
use crate::{Client, Error, Options, Result};

/// A shard's id in the map.
pub type ShardId = u32;

/// How a [`Router`] watches its shards and what it checks at connect.
#[derive(Debug, Clone)]
pub struct RouterOptions {
    /// The shard that holds global tables, sequences and global KV keys.
    /// `None`: the lowest shard id in the map.
    pub home: Option<ShardId>,
    /// Refuse a shard without a matching `rdb_shard_identity` row. On by
    /// default; off only for a deployment that has not run `rdb-shard init`.
    pub require_identity: bool,
    /// How often each shard is probed with `SELECT 1`. A shard that fails is
    /// marked down, and probed again at a doubling interval up to
    /// `health_max_backoff`, until it answers.
    pub health_interval: Duration,
    /// The longest wait between probes of a down shard.
    pub health_max_backoff: Duration,
    /// How long a probe may take before it counts as a failure.
    pub health_timeout: Duration,
    /// Room for shards a reloaded map may add, beyond the map's own.
    pub spare_shards: usize,
}

impl Default for RouterOptions {
    fn default() -> Self {
        RouterOptions {
            home: None,
            require_identity: true,
            health_interval: Duration::from_secs(2),
            health_max_backoff: Duration::from_secs(30),
            health_timeout: Duration::from_secs(2),
            spare_shards: 64,
        }
    }
}

/// One shard's connection and health.
struct Shard {
    id: ShardId,
    addr: String,
    client: Client,
    down: AtomicBool,
    last_error: Mutex<String>,
}

/// The map in force, and where each of its shards sits in the slots.
struct Table {
    map: Arc<ShardMap>,
    slot_of: HashMap<ShardId, usize>,
    home: ShardId,
}

struct Inner {
    /// Append-only: a slot, once filled, keeps its shard for the router's
    /// life, so `&Client` can be handed out without a lock.
    slots: Box<[OnceLock<Shard>]>,
    used: AtomicUsize,
    table: RwLock<Arc<Table>>,
    /// Serialises reloads.
    reload: tokio::sync::Mutex<()>,
    options: Options,
    router: RouterOptions,
    closed: AtomicBool,
}

/// N servers as one: routes by shard key, fans out, and watches each shard.
/// See the [module](self).
pub struct Router {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let t = self.table();
        f.debug_struct("Router")
            .field("cluster", &format_args!("{:032x}", t.map.cluster_id()))
            .field("epoch", &t.map.epoch())
            .field("shards", &t.map.shards().len())
            .field("home", &t.home)
            .finish()
    }
}

impl Drop for Router {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::Relaxed);
    }
}

/// What a router's single-shard transaction is pinned to: the ORM checks
/// each row against it before sending ([`Error::CrossShard`]).
#[doc(hidden)]
#[derive(Clone)]
pub struct ShardScope {
    shard: ShardId,
    home: ShardId,
    map: Arc<ShardMap>,
}

impl std::fmt::Debug for ShardScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardScope").field("shard", &self.shard).finish_non_exhaustive()
    }
}

impl ShardScope {
    /// Refuses `row` unless it lives on this scope's shard.
    pub(crate) fn check<M: Model>(&self, row: &M) -> Result<()> {
        let shard = shard_of_row(&self.map, self.home, row)?;
        if shard == self.shard {
            Ok(())
        } else {
            Err(Error::CrossShard(format!(
                "a {} row of shard {shard} was written in a transaction on shard {}",
                M::TABLE,
                self.shard
            )))
        }
    }
}

/// route_v1's level-0 block of `key` in a column of `family`: the unit a
/// shard map assigns. The server answers the same for
/// `rdb_route_block(type, value)`.
pub fn block_of_key(key: &ShardKey<'_>, family: ShardFamily) -> Result<u64> {
    block_for(key, family)
}

/// route_v1's level-0 block of `key` in a column of `family`.
pub(crate) fn block_for(key: &ShardKey<'_>, family: ShardFamily) -> Result<u64> {
    let k = key.coerce(family).ok_or_else(|| {
        Error::Param(format!("the shard key {key:?} has no {family:?} form"))
    })?;
    let (value, fam) = match &k {
        ShardKey::Int(v) => (KeyValue::Int(*v), rasterizeddb_router::KeyFamily::Integer),
        ShardKey::Uuid(b) => (KeyValue::Uuid(*b), rasterizeddb_router::KeyFamily::Uuid),
        ShardKey::Text(t) => (KeyValue::Text(t), rasterizeddb_router::KeyFamily::Text),
    };
    shard_block(value, fam).map_err(Error::Param)
}

/// The shard a row of `M` lives on.
pub(crate) fn shard_of_row<M: Model>(map: &ShardMap, home: ShardId, row: &M) -> Result<ShardId> {
    match M::SHARD {
        Sharding::Global => Ok(home),
        Sharding::By { column, family } => {
            let key = row.shard_key().ok_or_else(|| {
                Error::Param(format!("{} is sharded by {column} but gave no key", M::TABLE))
            })?;
            Ok(map.shard_of_block(block_for(&key, family)?))
        }
        Sharding::Unsharded => Err(unsharded::<M>()),
        Sharding::Unroutable(why) => Err(Error::Unsupported(format!("{}: {why}", M::TABLE))),
    }
}

pub(crate) fn unsharded<M: Model>() -> Error {
    Error::Unsupported(format!(
        "{} is marked shard = \"unsharded\": a router does not route it",
        M::TABLE
    ))
}

impl Router {
    /// Connects to every shard of `map` with `options` (each shard gets
    /// its own [`Client`]), checks each shard's identity row, and starts the
    /// health checks. With [`RouterOptions::default`].
    pub async fn connect(map: ShardMap, options: Options) -> Result<Router> {
        Router::connect_with(map, options, RouterOptions::default()).await
    }

    /// [`connect`](Self::connect) with router options.
    ///
    /// `options.migrations` is not run per shard: with
    /// [`AutoMigrate::Apply`](crate::orm::AutoMigrate) or `Verify` the router
    /// runs it once over every shard, home first ([`Router::migrate`]).
    pub async fn connect_with(map: ShardMap, mut options: Options, router: RouterOptions) -> Result<Router> {
        let migrations = options.migrations.take();
        let home = match router.home {
            Some(h) if map.shard(h).is_none() => {
                return Err(Error::Param(format!("home shard {h} is not in the map")));
            }
            Some(h) => h,
            None => map.shards()[0].id,
        };
        let capacity = map.shards().len() + router.spare_shards;
        let slots: Box<[OnceLock<Shard>]> = (0..capacity).map(|_| OnceLock::new()).collect();
        let inner = Arc::new(Inner {
            slots,
            used: AtomicUsize::new(0),
            table: RwLock::new(Arc::new(Table {
                map: Arc::new(map.clone()),
                slot_of: HashMap::new(),
                home,
            })),
            reload: tokio::sync::Mutex::new(()),
            options,
            router,
            closed: AtomicBool::new(false),
        });
        let mut slot_of = HashMap::new();
        for info in map.shards() {
            let slot = inner.add_shard(info).await?;
            slot_of.insert(info.id, slot);
        }
        for info in map.shards() {
            let shard = inner.slots[slot_of[&info.id]].get().expect("filled");
            identity::check(&shard.client, &map, info.id, inner.router.require_identity).await?;
        }
        *inner.table.write().expect("router table") = Arc::new(Table {
            map: Arc::new(map),
            slot_of,
            home,
        });
        let r = Router { inner };
        r.start_health();
        if let Some(migrator) = migrations {
            use crate::orm::AutoMigrate;
            match r.inner.options.auto_migrate {
                AutoMigrate::Off => {}
                AutoMigrate::Apply => {
                    migrator.apply_sharded(&r).await?;
                }
                AutoMigrate::Verify => {
                    for (id, client) in r.all() {
                        migrator.verify(client).await.map_err(|e| on_shard(id, e))?;
                    }
                }
            }
        }
        Ok(r)
    }

    fn table(&self) -> Arc<Table> {
        self.inner.table.read().expect("router table").clone()
    }

    /// The map in force.
    pub fn map(&self) -> Arc<ShardMap> {
        self.table().map.clone()
    }

    /// The home shard's id.
    pub fn home_id(&self) -> ShardId {
        self.table().home
    }

    /// The shard `key` lives on, hashed in the key's own family (pass the
    /// Rust type the shard column has: `i64` for `BIGINT`, `&str` for text).
    pub fn shard_of<'k>(&self, key: impl Into<ShardKey<'k>>) -> Result<ShardId> {
        let key = key.into();
        let family = key.family();
        Ok(self.table().map.shard_of_block(block_for(&key, family)?))
    }

    /// The shard of `key` ([`shard_of`](Self::shard_of)): for tenant-scoped
    /// work, `begin()`, `kv()` and the ORM on one tenant.
    pub fn for_key<'k>(&self, key: impl Into<ShardKey<'k>>) -> Result<&Client> {
        let id = self.shard_of(key)?;
        self.for_shard(id)
    }

    /// The shard owning routing point `point`.
    pub fn for_point(&self, point: u64) -> Result<&Client> {
        let id = self.table().map.shard_of_point(point);
        self.for_shard(id)
    }

    /// Shard `id`'s client; [`Error::ShardDown`] while its health check
    /// fails.
    pub fn for_shard(&self, id: ShardId) -> Result<&Client> {
        let shard = self.shard(id)?;
        if shard.down.load(Ordering::Relaxed) {
            return Err(Error::ShardDown {
                shard: id,
                message: shard.last_error.lock().expect("shard error").clone(),
            });
        }
        Ok(&shard.client)
    }

    fn shard(&self, id: ShardId) -> Result<&Shard> {
        let slot = *self.table().slot_of.get(&id).ok_or_else(|| {
            Error::Param(format!("shard {id} is not in the router's map"))
        })?;
        Ok(self.inner.slots[slot].get().expect("a mapped slot is filled"))
    }

    /// Whether shard `id` answers its health check.
    pub fn is_up(&self, id: ShardId) -> bool {
        self.shard(id).is_ok_and(|s| !s.down.load(Ordering::Relaxed))
    }

    /// The home shard: global tables, sequences, global KV keys.
    pub fn home(&self) -> Result<&Client> {
        self.for_shard(self.home_id())
    }

    /// Every shard of the map, by id, up or not.
    pub fn all(&self) -> Vec<(ShardId, &Client)> {
        let t = self.table();
        let mut out: Vec<(ShardId, &Client)> = t
            .slot_of
            .iter()
            .map(|(&id, &slot)| (id, &self.inner.slots[slot].get().expect("filled").client))
            .collect();
        out.sort_by_key(|(id, _)| *id);
        out
    }

    /// The shard of a row of `M` (its shard key; `home()` for a global
    /// model).
    pub fn shard_for<M: Model>(&self, row: &M) -> Result<ShardId> {
        let t = self.table();
        shard_of_row(&t.map, t.home, row)
    }

    /// The client of a row's shard.
    pub fn for_model<M: Model>(&self, row: &M) -> Result<&Client> {
        let id = self.shard_for(row)?;
        self.for_shard(id)
    }

    /// Runs `f` on every shard at once; each shard's answer, in shard order.
    /// A shard that is down answers [`Error::ShardDown`] without `f`
    /// running on it.
    pub async fn fan_out<'a, T, F, Fut>(&'a self, f: F) -> Vec<(ShardId, Result<T>)>
    where
        F: Fn(ShardId, &'a Client) -> Fut,
        Fut: Future<Output = Result<T>> + Send + 'a,
        T: Send + 'a,
    {
        let ids: Vec<ShardId> = self.all().into_iter().map(|(id, _)| id).collect();
        self.fan_out_to(&ids, f).await
    }

    /// [`fan_out`](Self::fan_out) over the shards `ids`.
    pub(crate) async fn fan_out_to<'a, T, F, Fut>(&'a self, ids: &[ShardId], f: F) -> Vec<(ShardId, Result<T>)>
    where
        F: Fn(ShardId, &'a Client) -> Fut,
        Fut: Future<Output = Result<T>> + Send + 'a,
        T: Send + 'a,
    {
        let mut futures: Vec<join::Pending<'a, (ShardId, Result<T>)>> = Vec::new();
        for &id in ids {
            match self.for_shard(id) {
                Ok(client) => {
                    let fut = f(id, client);
                    futures.push(Box::pin(async move { (id, fut.await) }));
                }
                Err(e) => futures.push(Box::pin(async move { (id, Err(e)) })),
            }
        }
        join::all(futures).await
    }

    /// Replaces the map with `map`: same cluster, a higher epoch. Shards new
    /// to the map are connected and their identity checked first; shards it
    /// drops stay connected but receive nothing. Any failure leaves the old
    /// map in force.
    pub async fn reload(&self, map: ShardMap) -> Result<()> {
        let _serial = self.inner.reload.lock().await;
        let old = self.table();
        if map.cluster_id() != old.map.cluster_id() {
            return Err(Error::IdentityMismatch(format!(
                "the new map is of cluster {:032x}, the router's of {:032x}",
                map.cluster_id(),
                old.map.cluster_id()
            )));
        }
        if map.epoch() <= old.map.epoch() {
            return Err(Error::MapStale(format!(
                "the new map's epoch {} is not above the router's {}",
                map.epoch(),
                old.map.epoch()
            )));
        }
        let home = if map.shard(old.home).is_some() { old.home } else { map.shards()[0].id };
        let mut slot_of = HashMap::new();
        for info in map.shards() {
            let slot = match old.slot_of.get(&info.id) {
                Some(&slot) => {
                    let s = self.inner.slots[slot].get().expect("filled");
                    if s.addr != info.addr {
                        return Err(Error::Unsupported(format!(
                            "shard {} moved from {} to {}: connect a new router for a changed address",
                            info.id, s.addr, info.addr
                        )));
                    }
                    slot
                }
                None => self.inner.add_shard(info).await?,
            };
            slot_of.insert(info.id, slot);
        }
        for info in map.shards() {
            let shard = self.inner.slots[slot_of[&info.id]].get().expect("filled");
            identity::check(&shard.client, &map, info.id, self.inner.router.require_identity).await?;
        }
        *self.inner.table.write().expect("router table") = Arc::new(Table {
            map: Arc::new(map),
            slot_of,
            home,
        });
        Ok(())
    }

    /// A transaction on the shard of `key`. ORM writes inside it are
    /// checked against that shard before they are sent
    /// ([`Error::CrossShard`]).
    pub async fn begin_for<'k>(&self, key: impl Into<ShardKey<'k>>) -> Result<crate::Transaction> {
        let id = self.shard_of(key)?;
        let tx = self.for_shard(id)?.begin().await?;
        tx.pin_shard(self.scope(id));
        Ok(tx)
    }

    /// [`Client::transaction`] on the shard of `key`, with the same
    /// single-shard check as [`begin_for`](Self::begin_for).
    pub async fn transaction_for<'k, T, F>(&self, key: impl Into<ShardKey<'k>>, mut body: F) -> Result<T>
    where
        T: Send,
        F: for<'t> FnMut(&'t crate::Transaction) -> crate::TransactionFuture<'t, T> + Send,
    {
        let id = self.shard_of(key)?;
        let scope = self.scope(id);
        self.for_shard(id)?
            .transaction(move |tx| {
                tx.pin_shard(scope.clone());
                body(tx)
            })
            .await
    }

    fn scope(&self, shard: ShardId) -> ShardScope {
        let t = self.table();
        ShardScope {
            shard,
            home: t.home,
            map: t.map.clone(),
        }
    }

    /// The key/value cache on the shard of `key`: keys that carry the
    /// tenant route with it.
    pub fn kv_for<'k>(&self, key: impl Into<ShardKey<'k>>) -> Result<crate::kv::Kv<'_>> {
        Ok(self.for_key(key)?.kv())
    }

    /// Runs `migrator` on the home shard, then on every other shard in id
    /// order: [`Migrator::apply_sharded`](crate::orm::Migrator::apply_sharded).
    pub async fn migrate(&self, migrator: &crate::orm::Migrator) -> Result<Vec<(ShardId, crate::orm::MigrationReport)>> {
        migrator.apply_sharded(self).await
    }

    /// The shard ids in migration order: home first, then the rest by id.
    pub(crate) fn migration_order(&self) -> Vec<ShardId> {
        let home = self.home_id();
        let mut ids: Vec<ShardId> = self.all().into_iter().map(|(id, _)| id).filter(|&id| id != home).collect();
        ids.insert(0, home);
        ids
    }

    fn start_health(&self) {
        let weak: Weak<Inner> = Arc::downgrade(&self.inner);
        let interval = self.inner.router.health_interval;
        let max = self.inner.router.health_max_backoff;
        let timeout = self.inner.router.health_timeout;
        tokio::spawn(async move {
            // Per slot: the wait before its next probe, and when that is.
            let mut next: HashMap<usize, (Duration, tokio::time::Instant)> = HashMap::new();
            loop {
                tokio::time::sleep(interval).await;
                let Some(inner) = weak.upgrade() else { return };
                if inner.closed.load(Ordering::Relaxed) {
                    return;
                }
                let now = tokio::time::Instant::now();
                let used = inner.used.load(Ordering::Acquire);
                for slot in 0..used {
                    let Some(shard) = inner.slots[slot].get() else { continue };
                    let (wait, at) = *next.get(&slot).unwrap_or(&(interval, now));
                    if at > now {
                        continue;
                    }
                    let probe = tokio::time::timeout(timeout, shard.client.run("SELECT 1")).await;
                    let failure = match probe {
                        Ok(Ok(_)) => None,
                        Ok(Err(e)) => Some(e.to_string()),
                        Err(_) => Some(format!("no answer to SELECT 1 within {timeout:?}")),
                    };
                    match failure {
                        None => {
                            shard.down.store(false, Ordering::Relaxed);
                            next.insert(slot, (interval, now + interval));
                        }
                        Some(why) => {
                            *shard.last_error.lock().expect("shard error") =
                                format!("{} ({}): {why}", shard.id, shard.addr);
                            shard.down.store(true, Ordering::Relaxed);
                            let wait = (wait * 2).min(max);
                            next.insert(slot, (wait, now + wait));
                        }
                    }
                }
            }
        });
    }
}

impl Inner {
    /// Connects a client to `info`'s primary (then its standbys in order)
    /// and puts it in the next free slot.
    async fn add_shard(&self, info: &ShardInfo) -> Result<usize> {
        if info.addr.is_empty() {
            return Err(Error::Param(format!("shard {} has no address in the map", info.id)));
        }
        let mut last = None;
        let mut client = None;
        let mut addr = info.addr.clone();
        for a in std::iter::once(&info.addr).chain(&info.standbys) {
            match Client::connect(a, self.options.clone()).await {
                Ok(c) => {
                    client = Some(c);
                    addr = a.clone();
                    break;
                }
                Err(e) => last = Some(e),
            }
        }
        let Some(client) = client else {
            return Err(Error::ShardDown {
                shard: info.id,
                message: last.map(|e| e.to_string()).unwrap_or_default(),
            });
        };
        let slot = self.used.fetch_add(1, Ordering::AcqRel);
        let Some(cell) = self.slots.get(slot) else {
            return Err(Error::Unsupported(format!(
                "the router has room for {} shards (RouterOptions::spare_shards): connect a new router",
                self.slots.len()
            )));
        };
        let _ = cell.set(Shard {
            id: info.id,
            addr,
            client,
            down: AtomicBool::new(false),
            last_error: Mutex::new(String::new()),
        });
        Ok(slot)
    }
}

/// `e`, said as shard `id`'s.
pub(crate) fn on_shard(id: ShardId, e: Error) -> Error {
    match e {
        Error::Server(m) => Error::Server(format!("shard {id}: {m}")),
        Error::Unsupported(m) => Error::Unsupported(format!("shard {id}: {m}")),
        Error::Conflict(m) => Error::Conflict(format!("shard {id}: {m}")),
        other => other,
    }
}

/// The answers of a write sent to several shards: their values when every
/// shard applied, else [`Error::FanOutPartial`] naming who did.
pub(crate) fn all_or_partial<T>(answers: Vec<(ShardId, Result<T>)>) -> Result<Vec<(ShardId, T)>> {
    let mut applied = Vec::new();
    let mut failed = Vec::new();
    let mut values = Vec::new();
    for (id, r) in answers {
        match r {
            Ok(v) => {
                applied.push(id);
                values.push((id, v));
            }
            Err(e) => failed.push((id, e.to_string())),
        }
    }
    if failed.is_empty() {
        Ok(values)
    } else {
        Err(Error::FanOutPartial { applied, failed })
    }
}

/// The answers of a read sent to several shards: every value, or the first
/// error (said as its shard's).
pub(crate) fn all_or_first_error<T>(answers: Vec<(ShardId, Result<T>)>) -> Result<Vec<(ShardId, T)>> {
    answers
        .into_iter()
        .map(|(id, r)| r.map(|v| (id, v)).map_err(|e| on_shard(id, e)))
        .collect()
}
