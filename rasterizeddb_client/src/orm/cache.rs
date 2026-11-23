//! A read-through cache of model rows in the server's key/value store:
//! [`Client::cached`].

use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;

use super::{Model, ModelExt};
use crate::client::Client;
use crate::kv::{Kv, KvRepo, rows_from_text};
use crate::to_sql::ToSql;
use crate::{Error, Result};

/// The key prefix cached rows live under: `rdb_orm:<table>:<primary key>`.
pub const CACHE_PREFIX: &str = "rdb_orm";

/// Model `M`'s rows, read through the server's key/value cache: from
/// [`Client::cached`] or [`ModelQuery::cached`](super::ModelQuery::cached).
///
/// [`find`](Self::find) and [`find_by_key`](Self::find_by_key) answer from
/// the cache when the row is there, and otherwise read the table and store
/// the row for `ttl`. The writes here ([`insert`](Self::insert),
/// [`update`](Self::update), [`upsert`](Self::upsert),
/// [`delete`](Self::delete), …) run the ORM's statement and then remove the
/// row's cache entry, so the next read sees the change.
///
/// **What it does not see.** A write that does not go through this handle
/// (another process, plain SQL, a bulk `query().update()`, the pgwire
/// listener) leaves the cached row as it was until its TTL runs out: the TTL
/// bounds how stale a read can be. So does a read racing a write here: a
/// reader that read the table just before the write and stores its row just
/// after the write removed the entry puts the old row back, until the TTL.
/// Choose the TTL as the staleness you accept. A row that does not exist is
/// not cached.
///
/// Each entry is one `SET … PX <ttl>`, so an entry is never left without
/// its TTL. Entries are keyed by the model's table and primary key, and
/// tagged with its columns: a struct whose columns changed reads an older
/// entry as a miss.
///
/// The cache answers outside any transaction, so it takes a [`Client`],
/// not any executor. It needs a server with the key/value cache (the
/// `KV` statements, in the default build); against one without it, every
/// call fails with [`Error::Unsupported`] saying so.
pub struct Cached<'c, M> {
    client: &'c Client,
    ttl: Duration,
    prefix: String,
    _model: PhantomData<fn() -> M>,
}

impl<M> Clone for Cached<'_, M> {
    fn clone(&self) -> Self {
        Cached {
            client: self.client,
            ttl: self.ttl,
            prefix: self.prefix.clone(),
            _model: PhantomData,
        }
    }
}

impl<M> fmt::Debug for Cached<'_, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cached")
            .field("ttl", &self.ttl)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Model `M`'s rows read through the key/value cache, each kept for
    /// `ttl`: see [`Cached`].
    pub fn cached<M: Model>(&self, ttl: Duration) -> Cached<'_, M> {
        Cached::new(self, ttl)
    }
}

/// A KV failure, said as the cache's.
fn kv_error(e: Error) -> Error {
    match e {
        Error::Server(m) => Error::Unsupported(format!(
            "the read-through cache needs the server's key/value cache (KV statements), \
             which answered: {m}"
        )),
        other => other,
    }
}

/// A write committed, and removing its cache entry failed.
fn stale_error(e: Error) -> Error {
    let detail = kv_error(e).to_string();
    Error::Unsupported(format!(
        "the write committed, but removing the row's cache entry failed ({detail}): the cache \
         serves the old row until its TTL runs out"
    ))
}

/// FNV-1a over the model's column names and types: an entry written for
/// other columns is a miss.
fn columns_tag<M: Model>() -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for column in M::COLUMNS {
        for b in column
            .name
            .bytes()
            .chain([0])
            .chain(column.sql_type.bytes())
            .chain([0])
        {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash
}

impl<'c, M: Model> Cached<'c, M> {
    fn new(client: &'c Client, ttl: Duration) -> Self {
        Cached {
            client,
            ttl,
            prefix: format!("{CACHE_PREFIX}:{}", M::TABLE),
            _model: PhantomData,
        }
    }

    /// Another key prefix than `rdb_orm:<table>`.
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    /// How long a row is kept.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    fn repo(&self) -> KvRepo<'c, M> {
        Kv::new(self.client).repo::<M>().with_prefix(&self.prefix)
    }

    /// The cache key of the row whose primary key is `key`.
    pub fn key_for(&self, key: &[&dyn ToSql]) -> Result<Vec<u8>> {
        self.repo().key_for(key)
    }

    fn encode(m: &M) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(&columns_tag::<M>().to_le_bytes());
        let mut cell = Vec::new();
        for i in 0..M::COLUMNS.len() {
            cell.clear();
            if m.write_text(i, &mut cell)? {
                let len = u32::try_from(cell.len())
                    .map_err(|_| Error::Param("a column is too large to cache".to_string()))?;
                out.push(1);
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(&cell);
            } else {
                out.push(0);
            }
        }
        Ok(out)
    }

    /// The row an entry holds; `None` for an entry of other columns, or one
    /// that does not parse (a miss, then overwritten).
    fn decode(bytes: &[u8]) -> Result<Option<M>> {
        let Some((tag, mut rest)) = bytes.split_first_chunk::<8>() else {
            return Ok(None);
        };
        if u64::from_le_bytes(*tag) != columns_tag::<M>() {
            return Ok(None);
        }
        let mut cells: Vec<Option<&[u8]>> = Vec::with_capacity(M::COLUMNS.len());
        for _ in 0..M::COLUMNS.len() {
            match rest.split_first() {
                Some((0, tail)) => {
                    cells.push(None);
                    rest = tail;
                }
                Some((1, tail)) => {
                    let Some((len, tail)) = tail.split_first_chunk::<4>() else {
                        return Ok(None);
                    };
                    let len = u32::from_le_bytes(*len) as usize;
                    if tail.len() < len {
                        return Ok(None);
                    }
                    cells.push(Some(&tail[..len]));
                    rest = &tail[len..];
                }
                _ => return Ok(None),
            }
        }
        if !rest.is_empty() {
            return Ok(None);
        }
        let rows = rows_from_text(&cells)?;
        let mut m: M = rows.get(0).expect("one row").decode()?;
        m.__rdb_after_load();
        Ok(Some(m))
    }

    /// The row whose (single-column) primary key is `key`: from the cache,
    /// or read from the table and cached.
    pub async fn find(&self, key: impl ToSql) -> Result<Option<M>> {
        self.find_by_key(&[&key]).await
    }

    /// The row whose composite primary key is `key`, in key column order.
    pub async fn find_by_key(&self, key: &[&dyn ToSql]) -> Result<Option<M>> {
        let cache_key = self.key_for(key)?;
        let kv = self.client.kv();
        if let Some(bytes) = kv.get(&cache_key).await.map_err(kv_error)?
            && let Some(m) = Self::decode(&bytes)?
        {
            return Ok(Some(m));
        }
        let Some(m) = self.client.find_by_key::<M>(key).await? else {
            return Ok(None);
        };
        kv.set(&cache_key, &Self::encode(&m)?, Some(self.ttl))
            .await
            .map_err(kv_error)?;
        Ok(Some(m))
    }

    /// Removes the row's entry: whether there was one.
    pub async fn invalidate(&self, m: &M) -> Result<bool> {
        let key = self.repo().key_of_model(m)?;
        Ok(self.client.kv().del(&[&key]).await.map_err(kv_error)? > 0)
    }

    /// Removes the entry of the row whose primary key is `key`.
    pub async fn invalidate_key(&self, key: &[&dyn ToSql]) -> Result<bool> {
        let key = self.key_for(key)?;
        Ok(self.client.kv().del(&[&key]).await.map_err(kv_error)? > 0)
    }

    async fn forget(&self, m: &M) -> Result<()> {
        self.invalidate(m).await.map(drop).map_err(stale_error)
    }

    /// [`ModelExt::insert`], then the row's entry removed (a row deleted
    /// and inserted again under its key must not read as the old one).
    pub async fn insert(&self, row: &M) -> Result<M> {
        let stored = self.client.insert(row).await?;
        self.forget(&stored).await?;
        Ok(stored)
    }

    /// [`ModelExt::update`], then the row's entry removed.
    pub async fn update(&self, row: &M) -> Result<M> {
        let stored = self.client.update(row).await?;
        self.forget(&stored).await?;
        Ok(stored)
    }

    /// [`ModelExt::upsert`], then the row's entry removed.
    pub async fn upsert(&self, row: &M) -> Result<M> {
        let stored = self.client.upsert(row).await?;
        self.forget(&stored).await?;
        Ok(stored)
    }

    /// [`ModelExt::delete`], then the row's entry removed.
    pub async fn delete(&self, row: &M) -> Result<bool> {
        let deleted = self.client.delete(row).await?;
        self.forget(row).await?;
        Ok(deleted)
    }

    /// [`ModelExt::force_delete`], then the row's entry removed.
    pub async fn force_delete(&self, row: &M) -> Result<bool> {
        let deleted = self.client.force_delete(row).await?;
        self.forget(row).await?;
        Ok(deleted)
    }

    /// [`ModelExt::restore`], then the row's entry removed.
    pub async fn restore(&self, row: &M) -> Result<M> {
        let stored = self.client.restore(row).await?;
        self.forget(&stored).await?;
        Ok(stored)
    }
}

#[cfg(all(test, feature = "derive"))]
mod tests {
    use super::*;
    use crate::{Numeric, Timestamp};

    #[derive(crate::Model, Debug, Clone, PartialEq)]
    #[rdb(table = "things")]
    struct Thing {
        #[rdb(primary_key)]
        id: i64,
        name: Option<String>,
        price: Numeric,
        tags: Vec<String>,
        at: Timestamp,
        ok: bool,
    }

    #[test]
    fn entries_round_trip_and_reject_what_they_cannot_read() {
        let thing = Thing {
            id: -4,
            name: None,
            price: Numeric::new("-1.50").unwrap(),
            tags: vec!["a,b".into(), "\"q\"".into(), String::new()],
            at: Timestamp {
                micros: 1_700_000_000_123_456,
            },
            ok: true,
        };
        let bytes = Cached::<Thing>::encode(&thing).unwrap();
        assert_eq!(
            Cached::<Thing>::decode(&bytes).unwrap(),
            Some(thing.clone())
        );
        // Another struct's entry (other columns), a cut one, junk: misses.
        let mut other = bytes.clone();
        other[0] ^= 1;
        assert_eq!(Cached::<Thing>::decode(&other).unwrap(), None);
        assert_eq!(
            Cached::<Thing>::decode(&bytes[..bytes.len() - 1]).unwrap(),
            None
        );
        let mut longer = bytes.clone();
        longer.push(0);
        assert_eq!(Cached::<Thing>::decode(&longer).unwrap(), None);
        assert_eq!(Cached::<Thing>::decode(b"junk").unwrap(), None);
    }
}
