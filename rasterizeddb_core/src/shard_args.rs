//! Flags that start a node as a shard (sharding-plan.md B1a.1).
//!
//! Present in every build, so a build without the `sharding` feature refuses
//! `--role` with the reason instead of ignoring it and serving every range.

#[derive(clap::Args, Debug, Clone, Default)]
pub struct ShardArgs {
    /// `shard`: serve only the level-0 ranges the cluster map gives
    /// `--shard-id`, refusing reads and writes anywhere else (RDB02). The
    /// gateway is its own binary, `rasterizeddb_gateway`, not a role.
    #[arg(long = "role", value_name = "ROLE")]
    pub role: Option<String>,

    /// The cluster map, text or binary form (`rdb-shard init` writes one).
    #[arg(long = "cluster-map", value_name = "FILE")]
    pub cluster_map: Option<std::path::PathBuf>,

    /// This shard's id in the cluster map.
    #[arg(long = "shard-id", value_name = "N")]
    pub shard_id: Option<u32>,

    /// When given, the cluster map must name this cluster (32 hex digits).
    #[arg(long = "cluster-id", value_name = "HEX")]
    pub cluster_id: Option<String>,
}

impl ShardArgs {
    /// True when any shard flag was given.
    fn given(&self) -> bool {
        self.role.is_some() || self.cluster_map.is_some() || self.shard_id.is_some() || self.cluster_id.is_some()
    }

    /// Starts this node as a shard when `--role shard` was given; `Ok(None)`
    /// when no shard flag was. `location` is the data directory (`None` when
    /// the database is in memory, which a shard cannot be).
    #[cfg(feature = "sharding")]
    pub fn start(
        &self,
        location: Option<&str>,
    ) -> Result<Option<rasterizeddb_core::core::shard::start::ShardStarted>, String> {
        use rasterizeddb_core::core::shard::start::{ShardConfig, start};
        if !self.given() {
            return Ok(None);
        }
        match self.role.as_deref() {
            Some("shard") => {}
            Some(other) => return Err(format!("--role {other}: the only role is `shard`")),
            None => return Err("--cluster-map, --shard-id and --cluster-id need --role shard".to_string()),
        }
        let map_file = self.cluster_map.clone().ok_or("--role shard needs --cluster-map")?;
        let shard_id = self.shard_id.ok_or("--role shard needs --shard-id")?;
        let data_dir = location.ok_or("--role shard needs a data directory; it cannot run in memory")?;
        let cluster_id = match &self.cluster_id {
            None => None,
            Some(h) => Some(
                u128::from_str_radix(h.trim_start_matches("0x"), 16)
                    .map_err(|_| format!("--cluster-id {h}: not a hex cluster id"))?,
            ),
        };
        start(&ShardConfig {
            data_dir: data_dir.into(),
            map_file,
            shard_id,
            cluster_id,
        })
        .map(Some)
    }

    /// Without the feature: any shard flag is refused.
    #[cfg(not(feature = "sharding"))]
    pub fn start(&self, _location: Option<&str>) -> Result<Option<()>, String> {
        if self.given() {
            return Err("--role, --cluster-map, --shard-id and --cluster-id: this server was built without the sharding feature".to_string());
        }
        Ok(None)
    }
}
