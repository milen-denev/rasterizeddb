//! Flags for the Redis-compatible key/value store.

use rasterizeddb_core::core::kv_cache::{self, EvictionPolicy};
use rasterizeddb_core::core::kv_cache::resp::{RespAuth, RespConfig};

#[derive(clap::Args, Debug, Clone)]
pub struct KvArgs {
    /// Port for the Redis protocol. Closed unless given; `KV …` statements on
    /// the PostgreSQL port work either way.
    #[arg(long = "kv-port", value_name = "PORT")]
    pub kv_port: Option<u16>,

    /// Address the Redis protocol listens on.
    #[arg(long = "kv-addr", default_value = "127.0.0.1")]
    pub kv_addr: String,

    /// How Redis clients authenticate: `password` checks `AUTH` against the
    /// database's roles (the default), `trust` asks nothing and is refused on
    /// anything but a loopback address.
    #[arg(long = "kv-auth", default_value = "password")]
    pub kv_auth: String,

    /// The role `AUTH <password>` (no user name) is checked as. Defaults to
    /// `--pg-user`.
    #[arg(long = "kv-user", value_name = "ROLE")]
    pub kv_user: Option<String>,

    /// Bytes the store may hold, as Redis's `maxmemory`: `0` (unbounded, the
    /// default), `512mb`, `4gb`.
    #[arg(long = "kv-max-memory", value_name = "SIZE")]
    pub kv_max_memory: Option<String>,

    /// What goes when the store is full, as Redis's `maxmemory-policy`:
    /// noeviction (the default), allkeys-lru, volatile-lru, allkeys-random,
    /// volatile-random, volatile-ttl.
    #[arg(long = "kv-eviction", value_name = "POLICY")]
    pub kv_eviction: Option<String>,

    /// Save the store to `db_kv_cache.snap` periodically and on a clean
    /// shutdown. Off by default: it is a cache.
    #[arg(long = "kv-persist", default_value_t = false)]
    pub kv_persist: bool,

    /// I/O threads for the Redis protocol; one per CPU by default.
    #[arg(long = "kv-threads", value_name = "N")]
    pub kv_threads: Option<usize>,

    /// Seconds between periodic saves when `--kv-persist` is on.
    #[arg(long = "kv-save-interval-secs", value_name = "N")]
    pub kv_save_interval_secs: Option<u64>,
}

impl KvArgs {
    /// Applies the store's settings. They are process-wide, like the other
    /// runtime knobs, and `CONFIG SET` / `SET CONFIG kv.*` change them later.
    pub fn apply(&self) -> Result<(), String> {
        if let Some(size) = &self.kv_max_memory {
            kv_cache::set_max_memory(kv_cache::parse_bytes(size).map_err(|e| format!("--kv-max-memory: {e}"))?);
        }
        if let Some(policy) = &self.kv_eviction {
            kv_cache::set_eviction_policy(EvictionPolicy::parse(policy).map_err(|e| format!("--kv-eviction: {e}"))?);
        }
        kv_cache::set_persist(self.kv_persist);
        if let Some(secs) = self.kv_save_interval_secs {
            kv_cache::set_save_interval_secs(secs).map_err(|e| format!("--kv-save-interval-secs: {e}"))?;
        }
        Ok(())
    }

    /// The listener's settings, or `None` when no port was given.
    pub fn listener(&self, pg_user: &str) -> Result<Option<RespConfig>, String> {
        let Some(port) = self.kv_port else {
            return Ok(None);
        };
        let auth = match self.kv_auth.trim().to_ascii_lowercase().as_str() {
            "password" | "roles" | "scram-sha-256" => RespAuth::Roles {
                default_user: self.kv_user.clone().unwrap_or_else(|| pg_user.to_string()),
            },
            "trust" => RespAuth::Trust,
            other => return Err(format!("--kv-auth {other:?}: expected password or trust")),
        };
        Ok(Some(RespConfig {
            addr: self.kv_addr.clone(),
            port,
            auth,
            threads: self.kv_threads,
        }))
    }
}
