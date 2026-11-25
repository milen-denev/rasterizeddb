//! The gateway's configuration: a `key = value` file, then `--key=value`
//! flags over it (docs/gateway.md lists every key).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

/// What a statement with no route key does (B1b, D8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unrouted {
    Refuse,
    FanOut,
}

/// What a write with no route key does (B1b). Never a gateway-wide default:
/// only a session's `SET rdb.unrouted_writes` turns it on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnroutedWrites {
    Refuse,
    EachShard,
}

/// Per-user shards (G11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UserRoutingMode {
    Off,
    Prefer,
    Require,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    /// SCRAM-SHA-256 (and -PLUS over TLS), verified by the gateway.
    Scram,
    /// No authentication: loopback benches only.
    Trust,
}

/// How the gateway trusts shard certificates.
#[derive(Clone, Debug)]
pub enum ShardTrust {
    AcceptAny,
    RootCa(String),
    Pinned(Vec<String>),
}

/// G0's floor bench.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Relay {
    Off,
    /// Byte-for-byte TCP relay to one address: no parsing at all.
    Tcp(String),
}

#[derive(Clone, Debug)]
pub struct Config {
    pub listen: Vec<SocketAddr>,
    pub workers: usize,
    pub map_file: Option<String>,
    pub map_from: Option<String>,
    pub home_shard: Option<u32>,
    pub gateway_name: String,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub shard_trust: ShardTrust,
    pub shard_cert: Option<String>,
    pub shard_key: Option<String>,
    pub auth: AuthMode,
    pub unrouted: Unrouted,
    pub user_routing: UserRoutingMode,
    /// Every session pinned to this shard (G0's mux floor: no routing).
    pub fixed_shard: Option<u32>,
    pub relay: Relay,
    pub max_sessions: usize,
    pub max_in_flight: usize,
    pub max_message_bytes: usize,
    pub max_batch_bytes: usize,
    pub gather_max_bytes: usize,
    /// The most a join's global tables may hold to be copied to every shard
    /// (`RouteTemplate::Broadcast`) rather than its sharded table gathered
    /// onto the home shard; 0 never copies.
    pub broadcast_max_bytes: usize,
    /// Whether a read no one shard can answer is gathered onto the home
    /// shard (B4.5) rather than refused.
    pub gather: bool,
    pub template_cache_per_core: usize,
    pub template_cache_shared: usize,
    pub auth_cache_secs: u64,
    pub admin: Option<SocketAddr>,
    /// The configuration API (`api`): where it listens, the file holding
    /// its bearer token, and the directory of the local store it keeps the
    /// configuration in.
    pub api_listen: Option<SocketAddr>,
    pub api_token_file: Option<std::path::PathBuf>,
    pub config_dir: Option<std::path::PathBuf>,
    /// Rebalancing from a client (`crate::rebalance`): the `rdb-shard`
    /// binary, the roles that may plan and run it, its login on the shards
    /// (the password in the environment variable named), each shard's native
    /// address when the map names rshard listeners, more flags for it, and
    /// the planner's tolerance and moves per run.
    pub rebalance_tool: Option<String>,
    pub rebalance_roles: Vec<String>,
    pub rebalance_user: String,
    pub rebalance_password_env: String,
    pub rebalance_native: Vec<String>,
    pub rebalance_extra: Vec<String>,
    pub rebalance_tolerance: f64,
    pub rebalance_max_moves: usize,
    pub native_listen: Option<SocketAddr>,
    /// The Redis protocol, routed by key (`kv`): where it listens, and each
    /// shard's Redis address.
    pub kv_listen: Option<SocketAddr>,
    pub kv_shards: BTreeMap<u32, String>,
    /// Two-phase commit for a transaction that writes on several shards
    /// (sharding-plan.md B5). Off: a transaction stays on its first shard.
    pub two_phase: bool,
    /// The lock timeout every part of a multi-shard transaction runs under
    /// (ms), so a deadlock across shards, which no shard's own detector
    /// sees whole, ends in an error instead of a hang (B6).
    pub two_phase_lock_timeout_ms: u64,
    /// How often the deadlock detector reads every shard's waits between
    /// two-phase transactions (B4.2); 0 turns it off.
    pub deadlock_check_ms: u64,
    pub native_role: String,
    pub native_database: String,
    pub drain: Duration,
    pub shard_timeout: Option<Duration>,
    pub server_version: String,
    pub retries: u32,
    /// zstd level for the links to the shards (`shard_compression = zstd` or
    /// `zstd:LEVEL`); `None`: uncompressed, the default.
    pub shard_compression: Option<i32>,
    /// Frames shorter than this go uncompressed.
    pub shard_compression_min_bytes: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen: vec!["0.0.0.0:6432".parse().unwrap()],
            workers: 0,
            map_file: None,
            map_from: None,
            home_shard: None,
            gateway_name: "rasterizeddb_gateway".to_string(),
            tls_cert: None,
            tls_key: None,
            shard_trust: ShardTrust::AcceptAny,
            shard_cert: None,
            shard_key: None,
            auth: AuthMode::Scram,
            unrouted: Unrouted::Refuse,
            user_routing: UserRoutingMode::Off,
            fixed_shard: None,
            relay: Relay::Off,
            max_sessions: 100_000,
            max_in_flight: 4096,
            max_message_bytes: 64 << 20,
            max_batch_bytes: 16 << 20,
            gather_max_bytes: 256 << 20,
            broadcast_max_bytes: 16 << 20,
            template_cache_per_core: 4096,
            template_cache_shared: 65_536,
            auth_cache_secs: 60,
            admin: None,
            api_listen: None,
            api_token_file: None,
            config_dir: None,
            rebalance_tool: None,
            rebalance_roles: Vec::new(),
            rebalance_user: String::new(),
            rebalance_password_env: "RDB_PASSWORD".to_string(),
            rebalance_native: Vec::new(),
            rebalance_extra: Vec::new(),
            rebalance_tolerance: 0.10,
            rebalance_max_moves: 4,
            native_listen: None,
            kv_listen: None,
            kv_shards: BTreeMap::new(),
            two_phase: false,
            two_phase_lock_timeout_ms: 5000,
            deadlock_check_ms: 1000,
            gather: true,
            native_role: String::new(),
            native_database: String::new(),
            drain: Duration::from_secs(30),
            shard_timeout: None,
            server_version: "17.0".to_string(),
            retries: 3,
            shard_compression: None,
            shard_compression_min_bytes: 1024,
        }
    }
}

const USAGE: &str = "usage: rasterizeddb_gateway [--config <file>] [--<key>=<value> ...]\n\
see docs/gateway.md for the keys";

impl Config {
    /// The configuration from the command line: `--config <file>` (or
    /// `--config=<file>`) is read first, then every other `--key=value`
    /// overrides it.
    pub fn from_args(args: impl Iterator<Item = String>) -> Result<Config, String> {
        let args: Vec<String> = args.collect();
        let mut pairs: Vec<(String, String)> = Vec::new();
        let mut file: Option<String> = None;
        let mut i = 0;
        while i < args.len() {
            let a = &args[i];
            if a == "--help" || a == "-h" {
                return Err(USAGE.to_string());
            }
            let Some(flag) = a.strip_prefix("--") else {
                return Err(format!("unexpected argument {a:?}\n{USAGE}"));
            };
            let (k, v) = match flag.split_once('=') {
                Some((k, v)) => (k.to_string(), v.to_string()),
                None => {
                    i += 1;
                    let v = args.get(i).ok_or_else(|| format!("--{flag} needs a value\n{USAGE}"))?;
                    (flag.to_string(), v.clone())
                }
            };
            let k = k.replace('-', "_");
            if k == "config" {
                file = Some(v);
            } else {
                pairs.push((k, v));
            }
            i += 1;
        }
        let mut entries: BTreeMap<String, String> = BTreeMap::new();
        if let Some(path) = &file {
            let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
            for (n, line) in text.lines().enumerate() {
                let line = line.split('#').next().unwrap_or("").trim();
                if line.is_empty() {
                    continue;
                }
                let (k, v) = line
                    .split_once('=')
                    .ok_or_else(|| format!("{path}:{}: expected key = value", n + 1))?;
                entries.insert(k.trim().replace('-', "_"), v.trim().to_string());
            }
        }
        for (k, v) in pairs {
            entries.insert(k, v);
        }
        let mut c = Config::default();
        for (k, v) in &entries {
            c.set(k, v).map_err(|e| format!("{k}: {e}"))?;
        }
        c.validate()?;
        Ok(c)
    }

    /// Sets `k` to `v` as a config file line does (the API's settings use
    /// it too, so a value it stores is one the gateway reads).
    pub fn set(&mut self, k: &str, v: &str) -> Result<(), String> {
        let num = |v: &str| v.parse::<u64>().map_err(|_| format!("{v:?} is not a number"));
        let opt = |v: &str| (!v.is_empty() && v != "off" && v != "none").then(|| v.to_string());
        // A comma- or space-separated list.
        let list = |v: &str| v.split([',', ' ']).map(str::trim).filter(|x| !x.is_empty()).map(str::to_string).collect::<Vec<String>>();
        match k {
            "listen" => {
                self.listen = v
                    .split(',')
                    .map(|a| a.trim().parse::<SocketAddr>().map_err(|_| format!("{a:?} is not host:port")))
                    .collect::<Result<_, _>>()?;
            }
            "workers" => self.workers = num(v)? as usize,
            "map" | "map_file" => self.map_file = opt(v),
            "map_from" => self.map_from = opt(v),
            "home_shard" => self.home_shard = Some(num(v)? as u32),
            "gateway_name" => self.gateway_name = v.to_string(),
            "tls_cert" => self.tls_cert = opt(v),
            "tls_key" => self.tls_key = opt(v),
            "shard_cert" => self.shard_cert = opt(v),
            "shard_key" => self.shard_key = opt(v),
            "shard_trust" => {
                self.shard_trust = if v == "accept_any" {
                    ShardTrust::AcceptAny
                } else if let Some(f) = v.strip_prefix("ca:") {
                    ShardTrust::RootCa(f.to_string())
                } else if let Some(fps) = v.strip_prefix("pinned:") {
                    ShardTrust::Pinned(fps.split(',').map(|s| s.trim().to_string()).collect())
                } else {
                    return Err("expected accept_any, ca:<file> or pinned:<fp>[,<fp>…]".into());
                }
            }
            "auth" => {
                self.auth = match v {
                    "scram" => AuthMode::Scram,
                    "trust" => AuthMode::Trust,
                    _ => return Err("expected scram or trust".into()),
                }
            }
            "unrouted" => {
                self.unrouted = match v {
                    "refuse" => Unrouted::Refuse,
                    "fan_out" => Unrouted::FanOut,
                    _ => return Err("expected refuse or fan_out".into()),
                }
            }
            "unrouted_writes" => {
                if v != "refuse" {
                    return Err("each_shard is never a gateway-wide default (sharding-plan.md B1b); set it per session with SET rdb.unrouted_writes".into());
                }
            }
            "user_routing" => {
                self.user_routing = match v {
                    "off" => UserRoutingMode::Off,
                    "prefer" => UserRoutingMode::Prefer,
                    "require" => UserRoutingMode::Require,
                    _ => return Err("expected off, prefer or require".into()),
                }
            }
            "fixed_shard" => self.fixed_shard = opt(v).map(|s| s.parse::<u32>()).transpose().map_err(|_| "not a shard id")?,
            "relay" => {
                self.relay = match v.strip_prefix("tcp:") {
                    Some(a) => Relay::Tcp(a.to_string()),
                    None if v == "off" => Relay::Off,
                    None => return Err("expected off or tcp:<host:port>".into()),
                }
            }
            "max_sessions" => self.max_sessions = num(v)? as usize,
            "max_in_flight" => self.max_in_flight = num(v)?.max(1) as usize,
            "max_message_bytes" => self.max_message_bytes = num(v)? as usize,
            "max_batch_bytes" => self.max_batch_bytes = num(v)? as usize,
            "gather_max_bytes" => self.gather_max_bytes = num(v)? as usize,
            "broadcast_max_bytes" => self.broadcast_max_bytes = num(v)? as usize,
            "template_cache_per_core" => self.template_cache_per_core = num(v)?.max(16) as usize,
            "template_cache_shared" => self.template_cache_shared = num(v)?.max(16) as usize,
            "auth_cache_secs" => self.auth_cache_secs = num(v)?,
            "admin" => self.admin = opt(v).map(|a| a.parse()).transpose().map_err(|_| "not host:port")?,
            "native_listen" => self.native_listen = opt(v).map(|a| a.parse()).transpose().map_err(|_| "not host:port")?,
            "two_phase_lock_timeout_ms" => self.two_phase_lock_timeout_ms = num(v)?,
            "deadlock_check_ms" => self.deadlock_check_ms = num(v)?,
            "gather" => self.gather = matches!(v, "on" | "true" | "1"),
            "two_phase" => {
                self.two_phase = match v.to_ascii_lowercase().as_str() {
                    "on" | "true" | "1" => true,
                    "off" | "false" | "0" => false,
                    _ => return Err("on or off".into()),
                }
            }
            "kv_listen" => self.kv_listen = opt(v).map(|a| a.parse()).transpose().map_err(|_| "not host:port")?,
            "kv_shards" => {
                for item in v.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    let (id, addr) = item.split_once('=').ok_or("write ID=ADDR:PORT[,ID=ADDR:PORT…]")?;
                    let id: u32 = id.trim().parse().map_err(|_| "not a shard id")?;
                    self.kv_shards.insert(id, addr.trim().to_string());
                }
            }
            "native_role" => self.native_role = v.to_string(),
            "native_database" => self.native_database = v.to_string(),
            "drain_secs" => self.drain = Duration::from_secs(num(v)?),
            "shard_timeout_ms" => {
                let ms = num(v)?;
                self.shard_timeout = (ms > 0).then(|| Duration::from_millis(ms));
            }
            "server_version" => self.server_version = v.to_string(),
            "retries" => self.retries = num(v)?.min(10) as u32,
            "shard_compression" => {
                self.shard_compression = match v.trim().to_ascii_lowercase().as_str() {
                    "off" | "none" | "" => None,
                    "zstd" => Some(3),
                    other => match other.strip_prefix("zstd:").map(|l| l.parse::<i32>()) {
                        Some(Ok(l)) if (-7..=22).contains(&l) && l != 0 => Some(l),
                        _ => return Err("off, zstd or zstd:LEVEL (-7 to 22, not 0)".into()),
                    },
                };
                if self.shard_compression.is_some() && !cfg!(feature = "shard_compression") {
                    return Err("this gateway was built without the shard_compression feature".into());
                }
            }
            "shard_compression_min_bytes" => self.shard_compression_min_bytes = num(v)?.clamp(64, u32::MAX as u64) as u32,
            "api_listen" => self.api_listen = opt(v).map(|a| a.parse()).transpose().map_err(|_| "not host:port")?,
            "api_token_file" => self.api_token_file = opt(v).map(std::path::PathBuf::from),
            "config_dir" => self.config_dir = opt(v).map(std::path::PathBuf::from),
            "rebalance_tool" => self.rebalance_tool = opt(v),
            "rebalance_roles" => self.rebalance_roles = list(v),
            "rebalance_user" => self.rebalance_user = v.trim().to_string(),
            "rebalance_password_env" => self.rebalance_password_env = v.trim().to_string(),
            "rebalance_native" => {
                let specs = list(v);
                if let Some(bad) = specs.iter().find(|s| s.split_once('=').is_none_or(|(id, addr)| id.parse::<u32>().is_err() || addr.is_empty())) {
                    return Err(format!("{bad:?}: write ID=HOST:PORT"));
                }
                self.rebalance_native = specs;
            }
            "rebalance_extra" => self.rebalance_extra = v.split_whitespace().map(str::to_string).collect(),
            "rebalance_tolerance" => {
                self.rebalance_tolerance = v.trim().parse::<f64>().ok().filter(|t| *t > 0.0 && *t < 1.0).ok_or("a fraction between 0 and 1")?
            }
            "rebalance_max_moves" => self.rebalance_max_moves = num(v)?.max(1) as usize,
            _ => return Err("unknown key".into()),
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), String> {
        if self.relay == Relay::Off && self.map_file.is_none() && self.map_from.is_none() {
            return Err("one of map = <file> or map_from = <shard host:port> is needed".into());
        }
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err("tls_cert and tls_key go together".into());
        }
        if self.shard_cert.is_some() != self.shard_key.is_some() {
            return Err("shard_cert and shard_key go together".into());
        }
        #[cfg(not(feature = "tls"))]
        if self.tls_cert.is_some() {
            return Err("tls_cert: this gateway was built without the tls feature".into());
        }
        #[cfg(not(feature = "native"))]
        if self.native_listen.is_some() {
            return Err("native_listen: this gateway was built without the native feature".into());
        }
        if self.native_listen.is_some() && (self.auth != AuthMode::Trust || self.native_role.is_empty()) {
            return Err("native_listen needs auth = trust and a native_role in this version (native SCRAM is not in the gateway yet)".into());
        }
        if self.max_message_bytes < 1024 || self.max_batch_bytes < self.max_message_bytes.min(1 << 20) {
            return Err("max_message_bytes must be at least 1024 and max_batch_bytes at least 1 MiB".into());
        }
        Ok(())
    }

    /// Worker threads: the configured count, or one per available core.
    pub fn worker_count(&self) -> usize {
        if self.workers > 0 {
            return self.workers;
        }
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
    }
}
