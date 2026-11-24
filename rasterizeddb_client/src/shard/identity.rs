//! The shard identity row (sharding-plan A3): which cluster and shard a
//! server is, and the map it last took part in.
//!
//! An ordinary table, so backups, restores and replicas carry it, which is
//! correct: a restored shard is still that shard.

use crate::{Client, Error, Executor, Result};
use rasterizeddb_router::ShardMap;

/// The table every shard holds one row in.
pub const IDENTITY_TABLE: &str = "rdb_shard_identity";

/// A shard's identity row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Identity {
    /// The cluster it belongs to.
    pub cluster_id: u128,
    /// Its id in the map.
    pub shard_id: u32,
    /// The epoch of the last map it took part in (written by `rdb-shard init`
    /// and by each move).
    pub map_epoch: u64,
    /// That map's digest.
    pub map_digest: u64,
}

impl Identity {
    /// The identity of shard `shard_id` under `map`.
    pub fn of(map: &ShardMap, shard_id: u32) -> Identity {
        Identity {
            cluster_id: map.cluster_id(),
            shard_id,
            map_epoch: map.epoch(),
            map_digest: map.digest(),
        }
    }

    /// Reads the server's identity row; `None` when it has no table or no
    /// row.
    pub async fn read(client: &Client) -> Result<Option<Identity>> {
        let rows = match client
            .run("SELECT cluster_id, shard_id, map_epoch, map_digest FROM rdb_shard_identity")
            .await
        {
            Ok(rows) => rows,
            Err(e) if e.sqlstate() == Some("42P01") || e.to_string().contains("does not exist") => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let Some(row) = rows.get(0) else {
            return Ok(None);
        };
        if rows.len() > 1 {
            return Err(Error::IdentityMismatch(format!(
                "{IDENTITY_TABLE} has {} rows; a shard has exactly one",
                rows.len()
            )));
        }
        let cluster: String = row.get(0)?;
        let cluster_id = u128::from_str_radix(cluster.trim(), 16).map_err(|_| {
            Error::IdentityMismatch(format!("{IDENTITY_TABLE}.cluster_id {cluster:?} is not 32 hex digits"))
        })?;
        let shard_id: i64 = row.get(1)?;
        let map_epoch: i64 = row.get(2)?;
        let digest: Vec<u8> = row.get(3)?;
        let map_digest = u64::from_le_bytes(digest.as_slice().try_into().map_err(|_| {
            Error::IdentityMismatch(format!("{IDENTITY_TABLE}.map_digest is {} bytes, not 8", digest.len()))
        })?);
        Ok(Some(Identity {
            cluster_id,
            shard_id: u32::try_from(shard_id)
                .map_err(|_| Error::IdentityMismatch(format!("{IDENTITY_TABLE}.shard_id {shard_id} is out of range")))?,
            map_epoch: map_epoch as u64,
            map_digest,
        }))
    }

    /// Creates the table if need be and makes this the server's one row,
    /// in one transaction.
    pub async fn write(&self, client: &Client) -> Result<()> {
        client
            .run(
                "CREATE TABLE IF NOT EXISTS rdb_shard_identity \
                 (cluster_id TEXT NOT NULL, shard_id BIGINT NOT NULL, map_epoch BIGINT NOT NULL, map_digest BYTEA NOT NULL)",
            )
            .await?;
        let tx = client.begin().await?;
        tx.run("DELETE FROM rdb_shard_identity").await?;
        let digest = self.map_digest.to_le_bytes();
        tx.query(
            "INSERT INTO rdb_shard_identity (cluster_id, shard_id, map_epoch, map_digest) VALUES ($1, $2, $3, $4)",
            &[
                crate::Param::Text(&format!("{:032x}", self.cluster_id)),
                crate::Param::I64(self.shard_id as i64),
                crate::Param::I64(self.map_epoch as i64),
                crate::Param::Bytes(&digest),
            ],
        )
        .await?;
        tx.commit().await
    }
}

/// Refuses a server that is not shard `shard_id` of `map`'s cluster, or
/// that took part in a newer map.
pub(crate) async fn check(client: &Client, map: &ShardMap, shard_id: u32, required: bool) -> Result<()> {
    let Some(id) = Identity::read(client).await? else {
        if required {
            return Err(Error::IdentityMismatch(format!(
                "shard {shard_id} has no {IDENTITY_TABLE} row: run `rdb-shard init`, or turn off \
                 RouterOptions::require_identity"
            )));
        }
        return Ok(());
    };
    if id.cluster_id != map.cluster_id() || id.shard_id != shard_id {
        return Err(Error::IdentityMismatch(format!(
            "the server at shard {shard_id}'s address is shard {} of cluster {:032x}, not shard {shard_id} of {:032x}",
            id.shard_id,
            id.cluster_id,
            map.cluster_id()
        )));
    }
    if id.map_epoch > map.epoch() {
        return Err(Error::MapStale(format!(
            "shard {shard_id} has seen map epoch {}, newer than the router's {}: reload the map",
            id.map_epoch,
            map.epoch()
        )));
    }
    if id.map_epoch == map.epoch() && id.map_digest != map.digest() {
        return Err(Error::IdentityMismatch(format!(
            "shard {shard_id} took part in another map at epoch {} (digest {:016x}, the router's {:016x})",
            id.map_epoch,
            id.map_digest,
            map.digest()
        )));
    }
    Ok(())
}
