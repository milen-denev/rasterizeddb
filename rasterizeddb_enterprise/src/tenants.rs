//! The tenant directory and its statements (sharding-plan.md §4.10,
//! provisioning):
//!
//! ```sql
//! CREATE TENANT acme OWNER acme_admin PASSWORD '…'
//!        [LIMITS (connections = 50, storage = '20GB', statement_timeout = '60s')];
//! ALTER TENANT acme SET LIMITS (…);
//! DROP TENANT acme;
//! SHOW TENANTS;
//! ```
//!
//! `CREATE TENANT` creates the database with only its administrator in it:
//! the role is made in the operator's database, the database created with it
//! as its OWNER (which copies only the creator and the owner, roles plan
//! RD1), and the role then dropped from the operator's database, so the
//! tenant's administrator can log in to its own database alone. The
//! directory (one `name owner connections max_bytes statement_timeout_ms`
//! line per tenant) is rewritten atomically on every change, and its limits
//! take effect at once.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::RwLock;

use rasterizeddb_core::core::policy_hooks::TenantLimits;

#[derive(Clone, Debug)]
pub struct Tenant {
    pub owner: String,
    pub limits: TenantLimits,
}

pub struct Directory {
    path: Option<PathBuf>,
    tenants: RwLock<BTreeMap<String, Tenant>>,
}

fn opt(v: Option<u64>) -> String {
    v.map_or("-".to_string(), |v| v.to_string())
}

fn parse_opt(s: &str) -> Result<Option<u64>, String> {
    if s == "-" { Ok(None) } else { s.parse().map(Some).map_err(|_| format!("{s:?} is not a number")) }
}

impl Directory {
    pub fn open(path: Option<PathBuf>) -> Result<Directory, String> {
        let mut tenants = BTreeMap::new();
        if let Some(p) = &path {
            match std::fs::read_to_string(p) {
                Ok(text) => {
                    for line in text.lines().filter(|l| !l.trim().is_empty()) {
                        let w: Vec<&str> = line.split_whitespace().collect();
                        let [name, owner, conns, bytes, timeout] = w.as_slice() else {
                            return Err(format!("{}: a malformed line {line:?}", p.display()));
                        };
                        let limits = TenantLimits {
                            max_connections: parse_opt(conns)?.map(|v| v as u32),
                            max_bytes: parse_opt(bytes)?,
                            statement_timeout_ms: parse_opt(timeout)?,
                        };
                        tenants.insert(name.to_string(), Tenant { owner: owner.to_string(), limits });
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(format!("{}: {e}", p.display())),
            }
        }
        Ok(Directory { path, tenants: RwLock::new(tenants) })
    }

    pub fn limits(&self, database: &str) -> Option<TenantLimits> {
        self.tenants.read().unwrap().get(database).map(|t| t.limits.clone())
    }

    fn save(&self, tenants: &BTreeMap<String, Tenant>) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Err("tenants_file is not set in the enterprise configuration".to_string());
        };
        let mut text = String::new();
        for (name, t) in tenants {
            text.push_str(&format!(
                "{name} {} {} {} {}\n",
                t.owner,
                opt(t.limits.max_connections.map(u64::from)),
                opt(t.limits.max_bytes),
                opt(t.limits.statement_timeout_ms)
            ));
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, text).map_err(|e| format!("{}: {e}", tmp.display()))?;
        std::fs::File::open(&tmp).and_then(|f| f.sync_all()).map_err(|e| format!("{}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, path).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// The statements for `sql`, when it is a tenant statement.
    pub fn rewrite(&self, sql: &str) -> Option<Result<Vec<String>, String>> {
        let t = sql.trim().trim_end_matches(';').trim();
        let up = t.to_ascii_uppercase();
        let words: Vec<&str> = t.split_whitespace().collect();
        let upw: Vec<&str> = up.split_whitespace().collect();
        match upw.as_slice() {
            ["CREATE", "TENANT", ..] => Some(self.create(t, &words, &upw)),
            ["ALTER", "TENANT", _, "SET", "LIMITS", ..] => Some(self.alter(t, &words)),
            ["DROP", "TENANT", ..] => Some(self.drop(&words, &upw)),
            ["SHOW", "TENANTS"] => Some(Ok(vec![self.show()])),
            ["MOVE", "TENANT", ..] => Some(Err(
                "MOVE TENANT needs a whole database's copy between shards, which is not built yet".to_string(),
            )),
            _ => None,
        }
    }

    fn create(&self, t: &str, words: &[&str], up: &[&str]) -> Result<Vec<String>, String> {
        let usage = "the statement is `CREATE TENANT name OWNER role PASSWORD '…' [LIMITS (…)]`";
        if up.get(3) != Some(&"OWNER") || up.get(5) != Some(&"PASSWORD") {
            return Err(usage.to_string());
        }
        let name = ident(words.get(2).ok_or(usage)?)?;
        let owner = ident(words.get(4).ok_or(usage)?)?;
        let at = t.find('\'').ok_or(usage)?;
        let end = literal_end(t, at).ok_or(usage)?;
        let password = &t[at..end];
        let rest = t[end..].trim();
        let limits = if rest.is_empty() {
            TenantLimits::default()
        } else if rest.len() > 6 && rest[..6].eq_ignore_ascii_case("LIMITS") {
            parse_limits(&rest[6..])?
        } else {
            return Err(usage.to_string());
        };
        let mut tenants = self.tenants.write().unwrap();
        if tenants.contains_key(&name) {
            return Err(format!("tenant \"{name}\" already exists"));
        }
        tenants.insert(name.clone(), Tenant { owner: owner.clone(), limits });
        if let Err(e) = self.save(&tenants) {
            tenants.remove(&name);
            return Err(e);
        }
        Ok(vec![
            format!("CREATE ROLE \"{owner}\" LOGIN PASSWORD {password}"),
            format!("CREATE DATABASE \"{name}\" OWNER \"{owner}\""),
            format!("DROP ROLE \"{owner}\""),
        ])
    }

    fn alter(&self, t: &str, words: &[&str]) -> Result<Vec<String>, String> {
        let name = ident(words[2])?;
        let at = t.to_ascii_uppercase().find("LIMITS").unwrap() + 6;
        let limits = parse_limits(&t[at..])?;
        let mut tenants = self.tenants.write().unwrap();
        let Some(entry) = tenants.get_mut(&name) else {
            return Err(format!("tenant \"{name}\" does not exist"));
        };
        let before = entry.limits.clone();
        entry.limits = limits;
        if let Err(e) = self.save(&tenants) {
            tenants.get_mut(&name).unwrap().limits = before;
            return Err(e);
        }
        Ok(vec![format!("SELECT 'tenant {name}''s limits are set' AS result")])
    }

    fn drop(&self, words: &[&str], up: &[&str]) -> Result<Vec<String>, String> {
        let if_exists = up.get(2) == Some(&"IF") && up.get(3) == Some(&"EXISTS");
        let name = ident(words.get(if if_exists { 4 } else { 2 }).ok_or("DROP TENANT names no tenant")?)?;
        let mut tenants = self.tenants.write().unwrap();
        if tenants.remove(&name).is_none() {
            return if if_exists {
                Ok(vec![format!("SELECT 'tenant {name} does not exist, skipping' AS result")])
            } else {
                Err(format!("tenant \"{name}\" does not exist"))
            };
        }
        self.save(&tenants)?;
        Ok(vec![format!("DROP DATABASE \"{name}\"")])
    }

    fn show(&self) -> String {
        let tenants = self.tenants.read().unwrap();
        if tenants.is_empty() {
            return "SELECT CAST(NULL AS TEXT) AS tenant, CAST(NULL AS TEXT) AS owner, CAST(NULL AS BIGINT) AS connections, \
                    CAST(NULL AS BIGINT) AS max_bytes, CAST(NULL AS BIGINT) AS statement_timeout_ms WHERE 1 = 0"
                .to_string();
        }
        let n = |v: Option<u64>| v.map_or("CAST(NULL AS BIGINT)".to_string(), |v| v.to_string());
        tenants
            .iter()
            .map(|(name, t)| {
                format!(
                    "SELECT '{}' AS tenant, '{}' AS owner, {} AS connections, {} AS max_bytes, {} AS statement_timeout_ms",
                    name.replace('\'', "''"),
                    t.owner.replace('\'', "''"),
                    n(t.limits.max_connections.map(u64::from)),
                    n(t.limits.max_bytes),
                    n(t.limits.statement_timeout_ms)
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    }
}

/// A plain or double-quoted identifier, as a bare name.
fn ident(w: &str) -> Result<String, String> {
    let name = if w.starts_with('"') && w.ends_with('"') && w.len() > 1 {
        w[1..w.len() - 1].replace("\"\"", "\"")
    } else {
        w.to_ascii_lowercase()
    };
    if name.is_empty() || name.len() > 63 || name.chars().any(|c| c.is_control() || c == '"' || c.is_whitespace()) {
        return Err(format!("{w:?} is not a name a tenant or its owner may have"));
    }
    Ok(name)
}

/// The end (exclusive) of the string literal opening at `at`.
fn literal_end(t: &str, at: usize) -> Option<usize> {
    let b = t.as_bytes();
    let mut i = at + 1;
    while i < b.len() {
        if b[i] == b'\'' {
            if b.get(i + 1) == Some(&b'\'') {
                i += 2;
                continue;
            }
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

/// `(connections = 50, storage = '20GB', statement_timeout = '60s')`.
fn parse_limits(s: &str) -> Result<TenantLimits, String> {
    let s = s.trim();
    let inner = s.strip_prefix('(').and_then(|s| s.strip_suffix(')')).ok_or("LIMITS takes (key = value, …)")?;
    let mut l = TenantLimits::default();
    for part in inner.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (k, v) = part.split_once('=').ok_or_else(|| format!("{part:?}: write key = value"))?;
        let v = v.trim().trim_matches('\'');
        match k.trim().to_ascii_lowercase().as_str() {
            "connections" => l.max_connections = Some(v.parse().map_err(|_| format!("connections = {v:?}"))?),
            "storage" => l.max_bytes = Some(size(v)?),
            "statement_timeout" => l.statement_timeout_ms = Some(duration_ms(v)?),
            other => return Err(format!("unknown limit {other:?} (connections, storage, statement_timeout)")),
        }
    }
    Ok(l)
}

fn size(v: &str) -> Result<u64, String> {
    let up = v.to_ascii_uppercase();
    let (num, mul) = [("TB", 1u64 << 40), ("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10), ("B", 1)]
        .iter()
        .find_map(|(suf, m)| up.strip_suffix(suf).map(|n| (n.trim().to_string(), *m)))
        .unwrap_or((up.clone(), 1));
    num.parse::<u64>().map(|n| n * mul).map_err(|_| format!("storage = {v:?}"))
}

fn duration_ms(v: &str) -> Result<u64, String> {
    let v = v.trim().to_ascii_lowercase();
    let (num, mul) = if let Some(n) = v.strip_suffix("ms") {
        (n, 1)
    } else if let Some(n) = v.strip_suffix('s') {
        (n, 1000)
    } else if let Some(n) = v.strip_suffix("min") {
        (n, 60_000)
    } else {
        (v.as_str(), 1)
    };
    num.trim().parse::<u64>().map(|n| n * mul).map_err(|_| format!("statement_timeout = {v:?}"))
}
