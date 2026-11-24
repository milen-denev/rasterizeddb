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
    pinned_roles: Vec<(String, String)>,
}

impl LabelPlacement {
    pub fn new(cfg: &EnterpriseConfig) -> Self {
        LabelPlacement {
            labels: cfg.shard_labels.clone(),
            policies: cfg.policies.clone(),
            pinned_roles: cfg.pinned_roles.clone(),
        }
    }
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
    }
}
