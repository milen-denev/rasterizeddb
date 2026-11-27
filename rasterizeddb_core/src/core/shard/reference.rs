//! Reference tables (sharding-plan.md B5): a global table that every shard
//! holds whole, so a keyed statement may join it on whatever shard its key
//! routes to. A write to one goes to every shard (`classify`: a fan-out
//! write, answered by any shard's count), under the session's
//! `rdb.unrouted_writes = each_shard`.
//!
//! Which tables are reference tables is each shard's own record, made by
//! `SHARD REFERENCE TABLE t` (and undone by `SHARD UNREFERENCE TABLE t`),
//! which `rdb-shard ddl` sends to every shard as it sends DDL. Kept in
//! `shard_reference_tables` beside the shard's state, one `database<TAB>table`
//! per line, read once and kept in memory: classification asks it on every
//! statement and must not wait.
//!
//! A global table that is *not* one lives on the home shard alone: a
//! statement mixing it with a sharded table is refused (`classify`), since
//! the shard its key routes to has none of its rows.

use std::collections::HashSet;
use std::sync::RwLock;

const FILE: &str = "shard_reference_tables";

static SET: RwLock<Option<HashSet<(String, String)>>> = RwLock::new(None);

fn path() -> Option<std::path::PathBuf> {
    super::ownership::data_dir().map(|d| d.join(FILE))
}

fn load() -> HashSet<(String, String)> {
    let Some(path) = path() else { return HashSet::new() };
    let Ok(text) = std::fs::read_to_string(&path) else { return HashSet::new() };
    text.lines()
        .filter_map(|l| l.split_once('\t'))
        .map(|(d, t)| (d.to_string(), t.to_string()))
        .collect()
}

/// Whether `table` of `database` is a reference table on this shard.
pub fn is_reference(database: &str, table: &str) -> bool {
    {
        let guard = SET.read().unwrap_or_else(|e| e.into_inner());
        if let Some(set) = guard.as_ref() {
            return set.iter().any(|(d, t)| d == database && t == table);
        }
    }
    let mut guard = SET.write().unwrap_or_else(|e| e.into_inner());
    let set = guard.get_or_insert_with(load);
    set.iter().any(|(d, t)| d == database && t == table)
}

/// Marks `table` of `database` a reference table (`on`) or not, and keeps
/// it: written to a temporary file and renamed over the record.
pub fn set(database: &str, table: &str, on: bool) -> Result<(), String> {
    let path = path().ok_or("this server keeps no shard state (no data directory)")?;
    let mut guard = SET.write().unwrap_or_else(|e| e.into_inner());
    let set = guard.get_or_insert_with(load);
    let key = (database.to_string(), table.to_string());
    if on {
        set.insert(key);
    } else {
        set.remove(&key);
    }
    let mut lines: Vec<String> = set.iter().map(|(d, t)| format!("{d}\t{t}")).collect();
    lines.sort();
    let tmp = path.with_extension("tmp");
    let write = || -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(lines.join("\n").as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        if let Some(dir) = path.parent() {
            let _ = std::fs::File::open(dir).and_then(|d| d.sync_all());
        }
        Ok(())
    };
    write().map_err(|e| format!("the reference table record could not be written: {e}"))
}
