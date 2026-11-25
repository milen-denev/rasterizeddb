//! Placement policies over shard labels (sharding-plan.md §4.10,
//! "Enterprise segregation"), and the per-user-shard placement list.
//!
//! A policy names a range of level-0 blocks and the labels a shard must
//! carry to hold any of it; `exclusive` keeps every other policy's range off
//! the shards that hold it. Every map edit, move and rebalance asks
//! [`LabelPlacement::may_place`]; the balancer never moves a policy's range
//! (`pinned`).

use rasterizeddb_core::core::policy_hooks::PlacementPolicy;

use crate::EnterpriseConfig;
use crate::config::Policy;

pub struct LabelPlacement {
    labels: std::collections::BTreeMap<u32, Vec<String>>,
    policies: Vec<Policy>,
    /// From the configuration file: fixed for the run.
    pinned_roles: Vec<(String, String)>,
    /// From `pinned_roles_file`: added and dropped at run time.
    hosted: std::sync::RwLock<Vec<(String, String)>>,
    hosted_file: Option<std::path::PathBuf>,
}

impl LabelPlacement {
    pub fn new(cfg: &EnterpriseConfig) -> Self {
        LabelPlacement {
            labels: cfg.shard_labels.clone(),
            policies: cfg.policies.clone(),
            pinned_roles: cfg.pinned_roles.clone(),
            hosted: std::sync::RwLock::new(Vec::new()),
            hosted_file: None,
        }
    }

    /// [`Self::new`], with the run-time list read from `pinned_roles_file`
    /// (a missing file is an empty list).
    pub fn open(cfg: &EnterpriseConfig) -> Result<Self, String> {
        let mut p = Self::new(cfg);
        if let Some(path) = &cfg.pinned_roles_file {
            let list = match std::fs::read_to_string(path) {
                Ok(text) => parse_hosted(&text).map_err(|e| format!("{}: {e}", path.display()))?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(e) => return Err(format!("{}: {e}", path.display())),
            };
            p.hosted = std::sync::RwLock::new(list);
            p.hosted_file = Some(path.clone());
        }
        Ok(p)
    }
}

/// `database role` a line; `#` comments.
fn parse_hosted(text: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        match line.split_whitespace().collect::<Vec<_>>().as_slice() {
            [db, role] => out.push((db.to_string(), role.to_string())),
            _ => return Err(format!("line {}: expected `database role`", n + 1)),
        }
    }
    Ok(out)
}

/// Written whole and renamed into place, then synced, so a crash leaves the
/// old list or the new one.
fn write_hosted(path: &std::path::Path, list: &[(String, String)]) -> Result<(), String> {
    use std::io::Write;
    let mut text = String::from("# per-user-shard roles this shard hosts (SHARD HOST ROLE); database role\n");
    for (db, role) in list {
        text.push_str(&format!("{db} {role}\n"));
    }
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::File::create(&tmp).map_err(|e| format!("{}: {e}", tmp.display()))?;
    f.write_all(text.as_bytes()).and_then(|()| f.sync_all()).map_err(|e| format!("{}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("{}: {e}", path.display()))?;
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        let _ = std::fs::File::open(dir).and_then(|d| d.sync_all());
    }
    Ok(())
}

impl PlacementPolicy for LabelPlacement {
    fn may_place(&self, lo: u64, hi: u64, shard: u32, labels: &[String]) -> Result<(), String> {
        // The configured labels win over the caller's: a forged map cannot
        // relabel a shard.
        let carried: &[String] = self.labels.get(&shard).map(Vec::as_slice).unwrap_or(labels);
        let mut overlapping = self.policies.iter().filter(|p| lo < p.hi && p.lo < hi).peekable();
        let had_policy = overlapping.peek().is_some();
        for p in overlapping {
            if let Some(missing) = p.require.iter().find(|r| !carried.contains(r)) {
                return Err(format!(
                    "placement policy {} requires {missing} for blocks [{:#x}, {:#x}); shard {shard} does not carry it",
                    p.name, p.lo, p.hi
                ));
            }
        }
        // An exclusive policy's shards hold nothing else.
        for p in self.policies.iter().filter(|p| p.exclusive) {
            let theirs = p.require.iter().all(|r| carried.contains(r));
            let outside = !(p.lo <= lo && hi <= p.hi);
            if theirs && outside && !had_policy {
                return Err(format!(
                    "shard {shard} is reserved to placement policy {} (exclusive); blocks [{lo:#x}, {hi:#x}) are not its",
                    p.name
                ));
            }
        }
        Ok(())
    }

    fn pinned(&self) -> Vec<(u64, u64)> {
        self.policies.iter().map(|p| (p.lo, p.hi)).collect()
    }

    fn hosts_pinned_role(&self, database: &str, role: &str) -> bool {
        self.pinned_roles.iter().any(|(d, r)| d == database && r == role)
            || self.hosted.read().unwrap_or_else(|e| e.into_inner()).iter().any(|(d, r)| d == database && r == role)
    }

    fn host_role(&self, database: &str, role: &str, on: bool) -> Result<(), String> {
        let Some(path) = &self.hosted_file else {
            return Err("this shard keeps no run-time list of hosted roles: set pinned_roles_file in its enterprise configuration".to_string());
        };
        if database.contains(char::is_whitespace) || role.contains(char::is_whitespace) || database.contains('#') || role.contains('#') {
            return Err("a hosted database or role name has no spaces or #".to_string());
        }
        let mut list = self.hosted.write().unwrap_or_else(|e| e.into_inner());
        let had = list.iter().any(|(d, r)| d == database && r == role);
        if on == had {
            return Ok(());
        }
        let mut next = list.clone();
        if on {
            next.push((database.to_string(), role.to_string()));
        } else {
            next.retain(|(d, r)| !(d == database && r == role));
        }
        // On disk first: the list in memory never admits what a restart
        // would not.
        write_hosted(path, &next)?;
        *list = next;
        Ok(())
    }
}

#[cfg(test)]
mod hosted_tests {
    use super::*;

    fn cfg(file: &std::path::Path) -> EnterpriseConfig {
        EnterpriseConfig::parse(&format!("pinned_role acme fixed\npinned_roles_file {}\n", file.display())).unwrap()
    }

    #[test]
    fn hosted_roles_are_kept_across_a_restart() {
        // A directory of its own, removed at the end (no tempfile here).
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("rdb-hosted-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("hosted");
        let p = LabelPlacement::open(&cfg(&file)).unwrap();
        assert!(p.hosts_pinned_role("acme", "fixed"), "the configuration's own list");
        assert!(!p.hosts_pinned_role("acme", "app"));
        p.host_role("acme", "app", true).unwrap();
        p.host_role("acme", "app", true).unwrap();
        p.host_role("beta", "svc", true).unwrap();
        assert!(p.hosts_pinned_role("acme", "app") && p.hosts_pinned_role("beta", "svc"));
        p.host_role("beta", "svc", false).unwrap();
        assert!(!p.hosts_pinned_role("beta", "svc"));
        // A restart reads what was kept, once each.
        let again = LabelPlacement::open(&cfg(&file)).unwrap();
        assert!(again.hosts_pinned_role("acme", "app"));
        assert!(!again.hosts_pinned_role("beta", "svc"));
        let text = std::fs::read_to_string(&file).unwrap();
        assert_eq!(text.lines().filter(|l| l.trim() == "acme app").count(), 1, "{text}");
        assert!(p.host_role("a b", "r", true).is_err(), "a name the file cannot hold");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn without_a_file_nothing_is_hosted_at_run_time() {
        let p = LabelPlacement::open(&EnterpriseConfig::parse("pinned_role acme fixed\n").unwrap()).unwrap();
        assert!(p.host_role("acme", "app", true).is_err());
        assert!(!p.hosts_pinned_role("acme", "app"));
    }
}
