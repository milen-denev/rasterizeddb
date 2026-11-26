//! Prepared transactions (sharding-plan.md B5): PostgreSQL's
//! `PREPARE TRANSACTION 'gid'`, `COMMIT PREPARED 'gid'` and
//! `ROLLBACK PREPARED 'gid'`, the shard's half of a two-phase commit.
//!
//! Preparing makes the block's rows durable, records its outcome `0b11`
//! (prepared, durable; `TransactionManager::prepare`) and its global id in
//! `<data>/rdb_prepared`, replaced atomically. The transaction stays
//! running and invisible, across restarts too, until a `COMMIT PREPARED` or
//! `ROLLBACK PREPARED` names it, from this session or any other. A block
//! that changed the schema is not prepared: its DDL copies and deferred
//! drops are a commit-time step this does not carry.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::core::mvcc::TxId;

use super::Database;

const FILE: &str = "rdb_prepared";

/// Held over each read-and-replace of a prepared-transactions file. Two
/// sessions preparing (or finishing) at once would otherwise each write the
/// file without the other's line, and a transaction whose line is lost is
/// prepared for good: no `COMMIT PREPARED` finds it. One lock for every
/// database: it is held for a small file's write, never for the rows' sync.
static FILE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The prepared transactions being finished, as `file path` + NUL + `gid`:
/// a second `COMMIT PREPARED` (or `ROLLBACK PREPARED`) of one, sent while
/// the first is still at work, is refused, not run beside it.
static FINISHING: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// A claim in [`FINISHING`], given up when dropped.
struct Finishing(String);

impl Finishing {
    fn claim(key: String) -> Option<Finishing> {
        let mut all = FINISHING.lock().unwrap_or_else(|e| e.into_inner());
        if all.contains(&key) {
            return None;
        }
        all.push(key.clone());
        Some(Finishing(key))
    }
}

impl Drop for Finishing {
    fn drop(&mut self) {
        FINISHING.lock().unwrap_or_else(|e| e.into_inner()).retain(|k| *k != self.0);
    }
}

impl Database {
    fn prepared_path(&self) -> Result<PathBuf, String> {
        let dir = self.location.as_deref().ok_or("a database in memory cannot prepare transactions")?;
        Ok(std::path::Path::new(dir).join(FILE))
    }

    /// The prepared transactions by global id.
    pub fn prepared(&self) -> Result<BTreeMap<String, TxId>, String> {
        let path = self.prepared_path()?;
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(e) => return Err(format!("{}: {e}", path.display())),
        };
        let mut out = BTreeMap::new();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let (gid, tx) = line.rsplit_once(' ').ok_or_else(|| format!("{}: a malformed line", path.display()))?;
            let tx: TxId = tx.parse().map_err(|_| format!("{}: a malformed id", path.display()))?;
            out.insert(gid.to_string(), tx);
        }
        Ok(out)
    }

    fn save_prepared(&self, all: &BTreeMap<String, TxId>) -> Result<(), String> {
        use std::io::Write;
        let path = self.prepared_path()?;
        let tmp = path.with_extension("tmp");
        let mut text = String::new();
        for (gid, tx) in all {
            text.push_str(&format!("{gid} {tx}\n"));
        }
        let io = |e: std::io::Error| format!("{}: {e}", path.display());
        let mut f = std::fs::File::create(&tmp).map_err(io)?;
        f.write_all(text.as_bytes()).map_err(io)?;
        f.sync_all().map_err(io)?;
        drop(f);
        std::fs::rename(&tmp, &path).map_err(io)?;
        if let Some(dir) = path.parent() {
            crate::core::sync_path::sync_dir(dir).map_err(io)?;
        }
        Ok(())
    }

    /// Prepares block transaction `tx` under global id `gid` (module docs).
    pub fn prepare_transaction(&self, tx: TxId, gid: &str) -> Result<(), String> {
        if gid.is_empty() || gid.len() > 200 || gid.chars().any(|c| c.is_control() || c == '\n') {
            return Err("42601: a transaction identifier is 1 to 200 printable characters".to_string());
        }
        let in_use = || format!("42710: transaction identifier \"{gid}\" is already in use");
        {
            let _file = FILE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            if self.prepared()?.contains_key(gid) {
                return Err(in_use());
            }
        }
        // The rows first, on every location, then the outcome.
        self.transactions.check_ids_reserved(tx).map_err(|e| e.to_string())?;
        self.sync_other_locations().map_err(|e| e.to_string())?;
        self.sync_own_location().map_err(|e| e.to_string())?;
        self.transactions.prepare(tx).map_err(|e| format!("could not record the transaction prepared: {e}"))?;
        // Read again under the lock: another session may have prepared, or
        // finished, since the check above. On an error here the caller
        // aborts the transaction, as it does when the file cannot be written.
        let _file = FILE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut all = self.prepared()?;
        if all.contains_key(gid) {
            return Err(in_use());
        }
        all.insert(gid.to_string(), tx);
        self.save_prepared(&all)
    }

    /// `COMMIT PREPARED 'gid'` (`commit`) or `ROLLBACK PREPARED 'gid'`.
    pub fn finish_prepared(&self, gid: &str, commit: bool) -> Result<(), String> {
        let missing = || format!("42704: prepared transaction with identifier \"{gid}\" does not exist");
        let _claim = Finishing::claim(format!("{}\0{gid}", self.prepared_path()?.display()))
            .ok_or_else(|| format!("55000: prepared transaction \"{gid}\" is being finished by another session"))?;
        let tx = {
            let _file = FILE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            *self.prepared()?.get(gid).ok_or_else(missing)?
        };
        if commit {
            let family = self.commit_written_transaction(tx).map_err(|e| e.to_string())?;
            drop(family);
        } else {
            self.abort_transaction(tx);
        }
        self.transactions.flush_deferred().map_err(|e| e.to_string())?;
        self.sync_own_location().map_err(|e| e.to_string())?;
        self.row_locks.release_rows(tx);
        // The line is taken out of the file as it is now, not as it was
        // before the syncs above: other sessions wrote it meanwhile.
        let _file = FILE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut all = self.prepared()?;
        all.remove(gid);
        self.save_prepared(&all)
    }
}
