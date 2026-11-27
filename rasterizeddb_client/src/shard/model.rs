//! The ORM's row calls on a router: the same names as
//! [`ModelExt`](crate::orm::ModelExt), each sent to the row's shard.

use std::collections::BTreeMap;
use std::future::{Future, IntoFuture};

use super::{Router, ShardId, all_or_partial, block_for, on_shard, unsharded};
use crate::orm::{BoxFuture, Model, ModelExt, ShardKey, Sharding};
use crate::to_sql::ToSql;
use crate::{Error, Result};

impl Router {
    /// The shard holding the row of `M` whose primary key is `key` (in key
    /// column order). The derive puts the shard column in every sharded
    /// primary key, so the key routes.
    pub fn shard_for_key<M: Model>(&self, key: &[&dyn ToSql]) -> Result<ShardId> {
        match M::SHARD {
            Sharding::Global => Ok(self.home_id()),
            Sharding::Unsharded => Err(unsharded::<M>()),
            Sharding::Unroutable(why) => Err(Error::Unsupported(format!("{}: {why}", M::TABLE))),
            Sharding::By { column, family } => {
                let at = M::COLUMNS
                    .iter()
                    .filter(|c| c.primary_key)
                    .position(|c| c.name == column)
                    .ok_or_else(|| {
                        Error::Unsupported(format!(
                            "{}'s primary key does not contain its shard column {column}",
                            M::TABLE
                        ))
                    })?;
                let value = key.get(at).ok_or_else(|| {
                    Error::Param(format!("{}'s key has its shard column {column} at {at}; {} values given", M::TABLE, key.len()))
                })?;
                let k = ShardKey::from_param(&value.to_param()).ok_or_else(|| {
                    Error::Param(format!("the {column} value does not route: give an integer, text or a UUID"))
                })?;
                Ok(self.map().shard_of_block(block_for(&k, family)?))
            }
        }
    }

    /// The shards in id order a model's table is on: every shard (global
    /// tables too, kept empty off the home shard).
    fn every_shard(&self) -> Vec<ShardId> {
        self.all().into_iter().map(|(id, _)| id).collect()
    }
}

/// [`ModelExt`]'s row calls, routed: implemented for [`Router`]. A call
/// site changes only its receiver (`client` to `router`).
pub trait ShardedModelExt {
    /// `CREATE TABLE` on every shard (home first): every shard has every
    /// model's table, so schemas stay identical.
    fn create_table<M: Model>(&self) -> impl Future<Output = Result<()>> + Send;
    /// `DROP TABLE` on every shard.
    fn drop_table<M: Model>(&self) -> impl Future<Output = Result<()>> + Send;
    /// [`ModelExt::insert`] on the row's shard.
    fn insert<M: Model>(&self, row: &M) -> impl Future<Output = Result<M>> + Send;
    /// Rows grouped by shard, one transaction per shard: atomic per shard
    /// only. Rows of several shards need
    /// [`split_across_shards`](ShardedInsertMany::split_across_shards), else
    /// [`Error::CrossShard`].
    fn insert_many<'a, M: Model + Send + Sync>(&'a self, rows: &'a [M]) -> ShardedInsertMany<'a, M>;
    /// [`ModelExt::upsert`] on the row's shard.
    fn upsert<'a, M: Model + 'a>(&'a self, row: &'a M) -> impl Future<Output = Result<M>> + Send + 'a;
    /// [`ModelExt::update`] on the row's shard.
    fn update<M: Model>(&self, row: &M) -> impl Future<Output = Result<M>> + Send;
    /// [`ModelExt::update_many`] when every row is on one shard;
    /// [`Error::CrossShard`] otherwise (one statement is atomic only on one
    /// server).
    fn update_many<M: Model>(&self, rows: &[M]) -> impl Future<Output = Result<Vec<M>>> + Send;
    /// [`ModelExt::delete`] on the row's shard.
    fn delete<M: Model>(&self, row: &M) -> impl Future<Output = Result<bool>> + Send;
    /// [`ModelExt::force_delete`] on the row's shard.
    fn force_delete<M: Model>(&self, row: &M) -> impl Future<Output = Result<bool>> + Send;
    /// [`ModelExt::restore`] on the row's shard.
    fn restore<M: Model>(&self, row: &M) -> impl Future<Output = Result<M>> + Send;
    /// [`ModelExt::find`] on the key's shard (the key contains the shard
    /// column).
    fn find<M: Model>(&self, key: impl ToSql) -> impl Future<Output = Result<Option<M>>> + Send;
    /// [`ModelExt::find_by_key`] on the key's shard.
    fn find_by_key<M: Model>(&self, key: &[&dyn ToSql]) -> impl Future<Output = Result<Option<M>>> + Send;
    /// Every row of the table: the home shard's for a global model, every
    /// shard's concatenated for a sharded one.
    fn all<M: Model + Send>(&self) -> impl Future<Output = Result<Vec<M>>> + Send;
}

/// A row call's future on its shard, or the routing error.
fn on_row<T, Fut>(target: Result<(ShardId, Fut)>) -> impl Future<Output = Result<T>> + Send
where
    Fut: Future<Output = Result<T>> + Send,
{
    async move {
        let (id, fut) = target?;
        fut.await.map_err(|e| on_shard(id, e))
    }
}

impl Router {
    /// The id and client of a shard.
    fn routed(&self, shard: Result<ShardId>) -> Result<(ShardId, &crate::Client)> {
        let id = shard?;
        Ok((id, self.for_shard(id)?))
    }
}

impl ShardedModelExt for Router {
    fn create_table<M: Model>(&self) -> impl Future<Output = Result<()>> + Send {
        async move {
            for id in self.migration_order() {
                self.for_shard(id)?.create_table::<M>().await.map_err(|e| on_shard(id, e))?;
            }
            Ok(())
        }
    }

    fn drop_table<M: Model>(&self) -> impl Future<Output = Result<()>> + Send {
        async move {
            let ids = self.every_shard();
            let answers = self.fan_out_to(&ids, |_, c| c.drop_table::<M>()).await;
            all_or_partial(answers).map(drop)
        }
    }

    fn insert<M: Model>(&self, row: &M) -> impl Future<Output = Result<M>> + Send {
        on_row(self.routed(self.shard_for(row)).map(|(id, c)| (id, c.insert(row))))
    }

    fn insert_many<'a, M: Model + Send + Sync>(&'a self, rows: &'a [M]) -> ShardedInsertMany<'a, M> {
        ShardedInsertMany {
            router: self,
            rows,
            split: false,
        }
    }

    fn upsert<'a, M: Model + 'a>(&'a self, row: &'a M) -> impl Future<Output = Result<M>> + Send + 'a {
        on_row(self.routed(self.shard_for(row)).map(|(id, c)| (id, c.upsert(row).into_future())))
    }

    fn update<M: Model>(&self, row: &M) -> impl Future<Output = Result<M>> + Send {
        on_row(self.routed(self.shard_for(row)).map(|(id, c)| (id, c.update(row))))
    }

    fn update_many<M: Model>(&self, rows: &[M]) -> impl Future<Output = Result<Vec<M>>> + Send {
        let shard = one_shard(self, rows);
        on_row(self.routed(shard).map(|(id, c)| (id, c.update_many(rows))))
    }

    fn delete<M: Model>(&self, row: &M) -> impl Future<Output = Result<bool>> + Send {
        on_row(self.routed(self.shard_for(row)).map(|(id, c)| (id, c.delete(row))))
    }

    fn force_delete<M: Model>(&self, row: &M) -> impl Future<Output = Result<bool>> + Send {
        on_row(self.routed(self.shard_for(row)).map(|(id, c)| (id, c.force_delete(row))))
    }

    fn restore<M: Model>(&self, row: &M) -> impl Future<Output = Result<M>> + Send {
        on_row(self.routed(self.shard_for(row)).map(|(id, c)| (id, c.restore(row))))
    }

    fn find<M: Model>(&self, key: impl ToSql) -> impl Future<Output = Result<Option<M>>> + Send {
        let shard = self.shard_for_key::<M>(&[&key]);
        on_row(self.routed(shard).map(move |(id, c)| (id, c.find::<M>(key))))
    }

    fn find_by_key<M: Model>(&self, key: &[&dyn ToSql]) -> impl Future<Output = Result<Option<M>>> + Send {
        on_row(self.routed(self.shard_for_key::<M>(key)).map(|(id, c)| (id, c.find_by_key::<M>(key))))
    }

    fn all<M: Model + Send>(&self) -> impl Future<Output = Result<Vec<M>>> + Send {
        async move {
            match M::SHARD {
                Sharding::Global => self.home()?.all::<M>().await,
                Sharding::Unsharded => Err(unsharded::<M>()),
                Sharding::Unroutable(why) => Err(Error::Unsupported(format!("{}: {why}", M::TABLE))),
                Sharding::By { .. } => {
                    let ids = self.every_shard();
                    let answers = self.fan_out_to(&ids, |_, c| c.all::<M>()).await;
                    Ok(super::all_or_first_error(answers)?.into_iter().flat_map(|(_, rows)| rows).collect())
                }
            }
        }
    }
}

/// The one shard every row is on; [`Error::CrossShard`] for several.
fn one_shard<M: Model>(router: &Router, rows: &[M]) -> Result<ShardId> {
    let mut shards = rows.iter().map(|r| router.shard_for(r));
    let first = match shards.next() {
        None => return Ok(router.home_id()),
        Some(s) => s?,
    };
    for s in shards {
        let s = s?;
        if s != first {
            return Err(Error::CrossShard(format!(
                "these {} rows are on shards {first} and {s}: update each shard's rows on their own",
                M::TABLE
            )));
        }
    }
    Ok(first)
}

/// Rows inserted where each lives: from
/// [`ShardedModelExt::insert_many`]. Await it.
pub struct ShardedInsertMany<'a, M> {
    router: &'a Router,
    rows: &'a [M],
    split: bool,
}

impl<'a, M: Model + Send + Sync> ShardedInsertMany<'a, M> {
    /// Allows rows of several shards: each shard's rows commit in their own
    /// transaction, so a failure leaves the others applied
    /// ([`Error::FanOutPartial`]).
    pub fn split_across_shards(mut self) -> Self {
        self.split = true;
        self
    }

    /// The rows as stored, in the order given.
    pub async fn run(self) -> Result<Vec<M>> {
        let mut groups: BTreeMap<ShardId, Vec<usize>> = BTreeMap::new();
        for (i, row) in self.rows.iter().enumerate() {
            groups.entry(self.router.shard_for(row)?).or_default().push(i);
        }
        if groups.len() > 1 && !self.split {
            return Err(Error::CrossShard(format!(
                "these {} rows are on shards {:?}: say split_across_shards() to insert each shard's rows on its own",
                M::TABLE,
                groups.keys().collect::<Vec<_>>()
            )));
        }
        let ids: Vec<ShardId> = groups.keys().copied().collect();
        let rows = self.rows;
        let groups = &groups;
        let answers = self
            .router
            .fan_out_to(&ids, |id, c| async move {
                let picked: Vec<&M> = groups[&id].iter().map(|&i| &rows[i]).collect();
                let tx = c.begin().await?;
                let mut out = Vec::with_capacity(picked.len());
                for chunk in picked.chunks(crate::orm::INSERT_CHUNK_ROWS) {
                    for r in chunk {
                        r.__rdb_before(crate::orm::Write::Insert)?;
                    }
                    let sql = crate::orm::insert_sql(chunk)?;
                    let stored = crate::Executor::run(&tx, &sql).await?;
                    out.extend(crate::orm::decode_models::<M>(&stored)?);
                }
                tx.commit().await?;
                Ok(out)
            })
            .await;
        let stored = all_or_partial(answers)?;
        // Back in the order given.
        let mut out: Vec<Option<M>> = (0..self.rows.len()).map(|_| None).collect();
        for (id, rows) in stored {
            for (i, m) in groups[&id].iter().zip(rows) {
                out[*i] = Some(m);
            }
        }
        out.into_iter()
            .map(|m| m.ok_or_else(|| Error::RowCount { expected: "a stored row per row given", got: 0 }))
            .collect()
    }
}

impl<'a, M: Model + Send + Sync + 'a> IntoFuture for ShardedInsertMany<'a, M> {
    type Output = Result<Vec<M>>;
    type IntoFuture = BoxFuture<'a, Result<Vec<M>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.run())
    }
}
