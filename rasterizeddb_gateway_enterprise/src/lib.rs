//! RasterizedDB gateway enterprise features (licensed: see LICENSE): per-user
//! shards (sharding-plan.md G11), the second sharding path. A login is
//! mapped to one shard and its whole session is locked there; the GPL
//! gateway enforces the lock (`rasterizeddb_gateway::hooks`).

use std::collections::BTreeMap;
use std::sync::RwLock;

use rasterizeddb_gateway::hooks::{Pin, UserRouting};

/// The routes file: one `database role shard` per line (`#` comments), or
/// `database role refuse` to keep a login out. Reread with [`Routes::reload`];
/// a change applies at each role's next login.
pub struct Routes {
    path: std::path::PathBuf,
    table: RwLock<(u64, BTreeMap<(String, String), Pin>)>,
    /// `require`: an unmapped login is refused.
    require: bool,
}

impl Routes {
    pub fn open(path: &std::path::Path, require: bool) -> Result<Routes, String> {
        let r = Routes { path: path.to_path_buf(), table: RwLock::new((0, BTreeMap::new())), require };
        r.reload()?;
        Ok(r)
    }

    /// Reads the file again; the epoch rises when its contents changed.
    pub fn reload(&self) -> Result<(), String> {
        let text = std::fs::read_to_string(&self.path).map_err(|e| format!("{}: {e}", self.path.display()))?;
        let mut table = BTreeMap::new();
        for (n, raw) in text.lines().enumerate() {
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let w: Vec<&str> = line.split_whitespace().collect();
            let [db, role, target] = w.as_slice() else {
                return Err(format!("{} line {}: write `database role shard`", self.path.display(), n + 1));
            };
            let pin = if *target == "refuse" {
                Pin::Refuse(format!("role \"{role}\" may not log in to \"{db}\" through this gateway"))
            } else {
                Pin::Shard(target.parse().map_err(|_| format!("{} line {}: {target:?} is not a shard id", self.path.display(), n + 1))?)
            };
            table.insert((db.to_string(), role.to_string()), pin);
        }
        let mut cur = self.table.write().unwrap();
        if cur.1 != table {
            cur.0 += 1;
            cur.1 = table;
        }
        Ok(())
    }
}

impl UserRouting for Routes {
    fn pin(&self, database: &str, role: &str) -> Pin {
        let t = self.table.read().unwrap();
        match t.1.get(&(database.to_string(), role.to_string())) {
            Some(p) => p.clone(),
            None if self.require => Pin::Refuse(format!("role \"{role}\" has no shard for \"{database}\"")),
            None => Pin::None,
        }
    }

    fn epoch(&self) -> u64 {
        self.table.read().unwrap().0
    }
}
