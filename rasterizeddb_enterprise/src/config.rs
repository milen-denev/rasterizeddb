//! The enterprise configuration file.
//!
//! ```text
//! tenant_mode strict
//! operator_database rasterizeddb
//! limit acme connections 50
//! limit acme statement_timeout_ms 60000
//! limit acme max_bytes 21474836480
//! shard_labels 3 region=eu-west tier=dedicated
//! policy eu_dedicated 0x0 0x800000000000 require region=eu-west exclusive
//! pinned_role acme alice
//! tenants_file /var/lib/rasterizeddb/rdb_tenants
//! ```
//!
//! `#` starts a comment. A `policy` names a range of level-0 blocks, the
//! labels a shard must carry to hold it, and `exclusive` when no other
//! policy's range may share its shards. `pinned_role` lists the
//! per-user-shard roles this shard hosts.

use std::collections::BTreeMap;

use rasterizeddb_core::core::policy_hooks::TenantLimits;

#[derive(Clone, Debug, Default)]
pub struct EnterpriseConfig {
    pub tenant_mode_strict: bool,
    pub operator_database: String,
    pub limits: BTreeMap<String, TenantLimits>,
    pub shard_labels: BTreeMap<u32, Vec<String>>,
    pub policies: Vec<Policy>,
    pub pinned_roles: Vec<(String, String)>,
    /// Where `CREATE TENANT` keeps the tenant directory.
    pub tenants_file: Option<std::path::PathBuf>,
    /// T9: the hosts the server may connect out to (`host`, `host:port`,
    /// `*.suffix`), under strict tenant mode. Unset, nothing is held back.
    pub outbound_hosts: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
pub struct Policy {
    pub name: String,
    pub lo: u64,
    pub hi: u64,
    /// `key=value` labels a shard must carry.
    pub require: Vec<String>,
    pub exclusive: bool,
}

fn block(s: &str) -> Result<u64, String> {
    match s.strip_prefix("0x") {
        Some(h) => u64::from_str_radix(h, 16),
        None => s.parse(),
    }
    .map_err(|_| format!("{s:?} is not a block number"))
}

impl EnterpriseConfig {
    pub fn parse(text: &str) -> Result<EnterpriseConfig, String> {
        let mut c = EnterpriseConfig { operator_database: "rasterizeddb".to_string(), ..Default::default() };
        for (n, raw) in text.lines().enumerate() {
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let bad = |what: &str| format!("line {}: {what}: {line:?}", n + 1);
            let w: Vec<&str> = line.split_whitespace().collect();
            match w.as_slice() {
                ["tenant_mode", "strict"] => c.tenant_mode_strict = true,
                ["tenant_mode", "off"] => c.tenant_mode_strict = false,
                ["operator_database", db] => c.operator_database = db.to_string(),
                ["limit", db, key, value] => {
                    let v: u64 = value.parse().map_err(|_| bad("not a number"))?;
                    let l = c.limits.entry(db.to_string()).or_default();
                    match *key {
                        "connections" => l.max_connections = Some(u32::try_from(v).map_err(|_| bad("too large"))?),
                        "max_bytes" => l.max_bytes = Some(v),
                        "statement_timeout_ms" => l.statement_timeout_ms = Some(v),
                        _ => return Err(bad("unknown limit")),
                    }
                }
                ["shard_labels", shard, labels @ ..] => {
                    let id: u32 = shard.parse().map_err(|_| bad("not a shard id"))?;
                    if labels.iter().any(|l| !l.contains('=')) {
                        return Err(bad("a label is key=value"));
                    }
                    c.shard_labels.insert(id, labels.iter().map(|l| l.to_string()).collect());
                }
                ["policy", name, lo, hi, "require", rest @ ..] => {
                    let exclusive = rest.last() == Some(&"exclusive");
                    let require: Vec<String> =
                        rest.iter().filter(|l| **l != "exclusive").map(|l| l.to_string()).collect();
                    if require.iter().any(|l| !l.contains('=')) {
                        return Err(bad("a label is key=value"));
                    }
                    let (lo, hi) = (block(lo)?, block(hi)?);
                    if lo >= hi {
                        return Err(bad("an empty range"));
                    }
                    c.policies.push(Policy { name: name.to_string(), lo, hi, require, exclusive });
                }
                ["pinned_role", db, role] => c.pinned_roles.push((db.to_string(), role.to_string())),
                ["tenants_file", path] => c.tenants_file = Some(path.into()),
                ["outbound_hosts", hosts @ ..] => {
                    c.outbound_hosts.get_or_insert_with(Vec::new).extend(hosts.iter().map(|h| h.to_ascii_lowercase()));
                }
                _ => return Err(bad("unknown setting")),
            }
        }
        Ok(c)
    }
}
