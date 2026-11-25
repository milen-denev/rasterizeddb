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

    /// Where the gateways reach this shard (rshard over rastcp/2, mTLS
    /// only), `ADDR:PORT`. Closed unless given.
    #[arg(long = "shard-listen", value_name = "ADDR:PORT")]
    pub shard_listen: Option<String>,

    /// The certificate chain the rshard listener presents (PEM).
    #[arg(long = "shard-cert", value_name = "PEM")]
    pub shard_cert: Option<String>,

    /// Its private key (PEM).
    #[arg(long = "shard-key", value_name = "PEM")]
    pub shard_key: Option<String>,

    /// A CA every gateway's certificate is signed by.
    #[arg(long = "gateway-ca", value_name = "PEM")]
    pub gateway_ca: Option<String>,

    /// The authority's Ed25519 public key (64 hex digits): maps must carry
    /// its signature (`<map>.sig`, `SHARD MAP INSTALL … SIGNATURE`).
    #[arg(long = "authority-key", value_name = "HEX")]
    pub authority_key: Option<String>,

    /// The CA the other shards' certificates are signed by, for coordinating
    /// keyless aggregates across shards; `--gateway-ca` when not given.
    #[arg(long = "peer-ca", value_name = "PEM")]
    pub peer_ca: Option<String>,
    /// Compress this shard's links to the other shards (a coordinator's
    /// parts): `off` (the default), `zstd` or `zstd:LEVEL`. Gateways ask for
    /// compression on their own links (`shard_compression`).
    #[arg(long = "shard-peer-compression", value_name = "off|zstd[:LEVEL]", default_value = "off")]
    pub shard_peer_compression: String,

    /// This replica's id in its shard's Raft group (sharding-plan.md B3;
    /// a build with `sharding_raft`). Unique within the group, non-zero.
    #[arg(long = "raft-id", value_name = "N")]
    pub raft_id: Option<u64>,

    /// Where the group's other replicas reach this one, `ADDR:PORT`.
    #[arg(long = "raft-listen", value_name = "ADDR:PORT")]
    pub raft_listen: Option<String>,

    /// Another replica of the group, `ID=ADDR:PORT`; repeatable. The group's
    /// first configuration is this replica and every peer named, all voters.
    #[arg(long = "raft-peer", value_name = "ID=ADDR")]
    pub raft_peer: Vec<String>,

    /// A gateway certificate's SHA-256 fingerprint; repeatable.
    #[arg(long = "gateway-pin", value_name = "SHA256")]
    pub gateway_pin: Vec<String>,
}

impl ShardArgs {
    /// True when any shard flag was given.
    fn given(&self) -> bool {
        self.role.is_some()
            || self.cluster_map.is_some()
            || self.shard_id.is_some()
            || self.cluster_id.is_some()
            || self.shard_listen.is_some()
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
        let authority_key = self
            .authority_key
            .as_deref()
            .map(rasterizeddb_core::core::shard::signing::parse_key)
            .transpose()
            .map_err(|e| format!("--authority-key: {e}"))?;
        start(&ShardConfig {
            data_dir: data_dir.into(),
            map_file,
            shard_id,
            cluster_id,
            authority_key,
        })
        .map(Some)
    }

    /// The rshard listener's configuration when `--shard-listen` was given.
    #[cfg(feature = "sharding")]
    pub fn listener(&self) -> Result<Option<rasterizeddb_core::core::shard::rshard::RshardConfig>, String> {
        let Some(listen) = &self.shard_listen else {
            return Ok(None);
        };
        if self.role.as_deref() != Some("shard") {
            return Err("--shard-listen needs --role shard".to_string());
        }
        let (addr, port) = listen
            .rsplit_once(':')
            .and_then(|(a, p)| p.parse::<u16>().ok().map(|p| (a.trim_matches(['[', ']']).to_string(), p)))
            .ok_or_else(|| format!("--shard-listen {listen}: write ADDR:PORT"))?;
        Ok(Some(rasterizeddb_core::core::shard::rshard::RshardConfig {
            addr,
            port,
            cert: self.shard_cert.clone().ok_or("--shard-listen needs --shard-cert")?,
            key: self.shard_key.clone().ok_or("--shard-listen needs --shard-key")?,
            gateway_ca: self.gateway_ca.clone(),
            gateway_pins: self.gateway_pin.clone(),
            peer_ca: self.peer_ca.clone(),
            peer_compression: rasterizeddb_core::core::shard::rshard::parse_compression(&self.shard_peer_compression)
                .map_err(|e| format!("--shard-peer-compression: {e}"))?,
        }))
    }

    /// The Raft group's configuration when `--raft-id` was given.
    #[cfg(feature = "sharding_raft")]
    pub fn raft(&self) -> Result<Option<(u64, rasterizeddb_core::core::shard::raft_net::RaftNet)>, String> {
        let Some(id) = self.raft_id else {
            if self.raft_listen.is_some() || !self.raft_peer.is_empty() {
                return Err("--raft-listen and --raft-peer need --raft-id".to_string());
            }
            return Ok(None);
        };
        if id == 0 {
            return Err("--raft-id must not be 0".to_string());
        }
        if self.role.as_deref() != Some("shard") {
            return Err("--raft-id needs --role shard".to_string());
        }
        let listen = self.raft_listen.as_deref().ok_or("--raft-id needs --raft-listen")?;
        let (addr, port) = listen
            .rsplit_once(':')
            .and_then(|(a, p)| p.parse::<u16>().ok().map(|p| (a.trim_matches(['[', ']']).to_string(), p)))
            .ok_or_else(|| format!("--raft-listen {listen}: write ADDR:PORT"))?;
        let mut peers = std::collections::BTreeMap::new();
        for p in &self.raft_peer {
            let (pid, paddr) = p.split_once('=').ok_or_else(|| format!("--raft-peer {p}: write ID=ADDR:PORT"))?;
            let pid: u64 = pid.parse().map_err(|_| format!("--raft-peer {p}: {pid:?} is not an id"))?;
            if pid == id || pid == 0 || peers.insert(pid, paddr.to_string()).is_some() {
                return Err(format!("--raft-peer {p}: ids are unique, non-zero and not this replica's"));
            }
        }
        Ok(Some((
            id,
            rasterizeddb_core::core::shard::raft_net::RaftNet {
                listen_addr: addr,
                listen_port: port,
                peers,
                cert: self.shard_cert.clone().ok_or("--raft-id needs --shard-cert")?,
                key: self.shard_key.clone().ok_or("--raft-id needs --shard-key")?,
                ca: self.peer_ca.clone().or_else(|| self.gateway_ca.clone()).ok_or("--raft-id needs --peer-ca")?,
            },
        )))
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
