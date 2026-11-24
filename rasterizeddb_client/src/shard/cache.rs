//! The ORM's read-through cache and KV repository over a router: each
//! entry lives on its row's own shard, so an invalidation reaches the server
//! the write it follows went to. A global model's entries live on the home
//! shard.

use std::time::Duration;

use super::{Router, ShardId, on_shard};
use crate::orm::{CACHE_PREFIX, Model};
use crate::to_sql::ToSql;
use crate::{Client, Result};

impl Router {
    /// Model `M`'s rows through each shard's key/value cache, kept for
    /// `ttl`: [`Cached`](crate::orm::Cached), routed.
    pub fn cached<M: Model>(&self, ttl: Duration) -> ShardedCached<'_, M> {
        ShardedCached {
            router: self,
            ttl,
            prefix: CACHE_PREFIX.to_string(),
            _model: std::marker::PhantomData,
        }
    }

    /// Model `M`'s rows kept in each shard's key/value store:
    /// [`KvRepo`](crate::kv::KvRepo), routed.
    pub fn kv_repo<M: Model>(&self) -> ShardedKvRepo<'_, M> {
        ShardedKvRepo {
            router: self,
            prefix: None,
            _model: std::marker::PhantomData,
        }
    }
}

/// [`Cached`](crate::orm::Cached) over a [`Router`]: from
/// [`Router::cached`].
pub struct ShardedCached<'r, M> {
    router: &'r Router,
    ttl: Duration,
    prefix: String,
    _model: std::marker::PhantomData<fn() -> M>,
}

impl<'r, M: Model> ShardedCached<'r, M> {
    /// Entries under `prefix` instead of [`CACHE_PREFIX`].
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    fn on(&self, id: ShardId) -> Result<crate::orm::Cached<'r, M>> {
        let client: &'r Client = self.router.for_shard(id)?;
        Ok(client.cached::<M>(self.ttl).with_prefix(&self.prefix))
    }

    /// [`Cached::find`](crate::orm::Cached::find) on the key's shard.
    pub async fn find(&self, key: impl ToSql) -> Result<Option<M>> {
        self.find_by_key(&[&key]).await
    }

    /// [`Cached::find_by_key`](crate::orm::Cached::find_by_key) on the key's
    /// shard.
    pub async fn find_by_key(&self, key: &[&dyn ToSql]) -> Result<Option<M>> {
        let id = self.router.shard_for_key::<M>(key)?;
        self.on(id)?.find_by_key(key).await.map_err(|e| on_shard(id, e))
    }

    /// Removes `m`'s entry on its shard.
    pub async fn invalidate(&self, m: &M) -> Result<bool> {
        let id = self.router.shard_for(m)?;
        self.on(id)?.invalidate(m).await.map_err(|e| on_shard(id, e))
    }

    /// Inserts on the row's shard, then drops its entry there.
    pub async fn insert(&self, row: &M) -> Result<M> {
        let id = self.router.shard_for(row)?;
        self.on(id)?.insert(row).await.map_err(|e| on_shard(id, e))
    }

    /// Updates on the row's shard, then drops its entry there.
    pub async fn update(&self, row: &M) -> Result<M> {
        let id = self.router.shard_for(row)?;
        self.on(id)?.update(row).await.map_err(|e| on_shard(id, e))
    }

    /// Upserts on the row's shard, then drops its entry there.
    pub async fn upsert(&self, row: &M) -> Result<M> {
        let id = self.router.shard_for(row)?;
        self.on(id)?.upsert(row).await.map_err(|e| on_shard(id, e))
    }

    /// Deletes on the row's shard, then drops its entry there.
    pub async fn delete(&self, row: &M) -> Result<bool> {
        let id = self.router.shard_for(row)?;
        self.on(id)?.delete(row).await.map_err(|e| on_shard(id, e))
    }

    /// Force-deletes on the row's shard, then drops its entry there.
    pub async fn force_delete(&self, row: &M) -> Result<bool> {
        let id = self.router.shard_for(row)?;
        self.on(id)?.force_delete(row).await.map_err(|e| on_shard(id, e))
    }

    /// Restores on the row's shard, then drops its entry there.
    pub async fn restore(&self, row: &M) -> Result<M> {
        let id = self.router.shard_for(row)?;
        self.on(id)?.restore(row).await.map_err(|e| on_shard(id, e))
    }
}

/// [`KvRepo`](crate::kv::KvRepo) over a [`Router`]: from
/// [`Router::kv_repo`].
pub struct ShardedKvRepo<'r, M> {
    router: &'r Router,
    prefix: Option<String>,
    _model: std::marker::PhantomData<fn() -> M>,
}

impl<'r, M: Model> ShardedKvRepo<'r, M> {
    /// Keys under `prefix` instead of the repository's default.
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = Some(prefix.to_string());
        self
    }

    fn on(&self, id: ShardId) -> Result<crate::kv::KvRepo<'r, M>> {
        let client: &'r Client = self.router.for_shard(id)?;
        let repo = client.kv().repo::<M>();
        Ok(match &self.prefix {
            Some(p) => repo.with_prefix(p),
            None => repo,
        })
    }

    /// Stores `m` on its shard.
    pub async fn save(&self, m: &M) -> Result<()> {
        let id = self.router.shard_for(m)?;
        self.on(id)?.save(m).await.map_err(|e| on_shard(id, e))
    }

    /// Stores `m` on its shard for `ttl`.
    pub async fn save_with_ttl(&self, m: &M, ttl: Duration) -> Result<()> {
        let id = self.router.shard_for(m)?;
        self.on(id)?.save_with_ttl(m, ttl).await.map_err(|e| on_shard(id, e))
    }

    /// The row stored under primary key `key`, from the key's shard.
    pub async fn get(&self, key: &[&dyn ToSql]) -> Result<Option<M>> {
        let id = self.router.shard_for_key::<M>(key)?;
        self.on(id)?.get(key).await.map_err(|e| on_shard(id, e))
    }

    /// Removes `m` from its shard.
    pub async fn delete(&self, m: &M) -> Result<bool> {
        let id = self.router.shard_for(m)?;
        self.on(id)?.delete(m).await.map_err(|e| on_shard(id, e))
    }

    /// Removes the row stored under `key` from the key's shard.
    pub async fn delete_key(&self, key: &[&dyn ToSql]) -> Result<bool> {
        let id = self.router.shard_for_key::<M>(key)?;
        self.on(id)?.delete_key(key).await.map_err(|e| on_shard(id, e))
    }

    /// Whether `key` is stored on its shard.
    pub async fn exists(&self, key: &[&dyn ToSql]) -> Result<bool> {
        let id = self.router.shard_for_key::<M>(key)?;
        self.on(id)?.exists(key).await.map_err(|e| on_shard(id, e))
    }

    /// Every stored row: a fan-out over every shard, concatenated.
    pub async fn all(&self) -> Result<Vec<M>> {
        // One shard after another: the repository's own scan is not `Send`.
        let mut out = Vec::new();
        for (id, _) in self.router.all() {
            out.extend(self.on(id)?.all().await.map_err(|e| on_shard(id, e))?);
        }
        Ok(out)
    }
}
