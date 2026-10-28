//! Sweeps of dropped files that have not finished, kept on local disk.
//!
//! A dropped file's objects are deleted by a task (`KvFamily::sweep`) that
//! retries until the store lets it, holding the name meanwhile. A server that
//! stopped before it finished left the objects behind with nothing naming
//! them, and a table created later under the same name read them as its own —
//! or replayed them at its next restart. So the name is written here before the
//! sweep starts and taken out once it is done, and a family that opens resumes
//! every sweep this file still lists, before any table can open the name.
//!
//! One file per database, beside `db_kv.cfg`: `store<TAB>name` a line. It is
//! left out of every backup snapshot — a restored copy resuming a sweep would
//! delete from the store the live server still uses.

use std::collections::BTreeSet;
use std::io;
use std::path::PathBuf;

use parking_lot::Mutex;

/// The file's name in a database directory.
pub const PENDING_FILE: &str = "db_kv_erase.pending";

/// The pending sweeps of one database. See the module comment.
pub struct PendingErases {
    path: PathBuf,
    /// Held across read-modify-write, so two drops at once cannot each write
    /// the file without the other's line.
    writing: Mutex<()>,
}

impl PendingErases {
    pub fn new(path: PathBuf) -> Self {
        Self { path, writing: Mutex::new(()) }
    }

    /// The names of `store` still to sweep.
    pub fn names(&self, store: &str) -> Vec<String> {
        let _one = self.writing.lock();
        self.read().into_iter().filter(|(s, _)| s == store).map(|(_, name)| name).collect()
    }

    /// Records that `name` of `store` is being swept. Durable before it returns.
    pub fn add(&self, store: &str, name: &str) -> io::Result<()> {
        let _one = self.writing.lock();
        let mut all = self.read();
        if all.insert((store.to_string(), name.to_string())) {
            self.write(&all)?;
        }
        Ok(())
    }

    /// Records that the sweep of `name` of `store` is done.
    pub fn remove(&self, store: &str, name: &str) -> io::Result<()> {
        let _one = self.writing.lock();
        let mut all = self.read();
        if all.remove(&(store.to_string(), name.to_string())) {
            self.write(&all)?;
        }
        Ok(())
    }

    fn read(&self) -> BTreeSet<(String, String)> {
        let Ok(text) = std::fs::read_to_string(&self.path) else {
            return BTreeSet::new();
        };
        text.lines()
            .filter_map(|line| line.split_once('\t'))
            .map(|(store, name)| (store.to_string(), name.to_string()))
            .collect()
    }

    /// Through a temporary file and a rename, so a crash leaves the old list
    /// or the new one, never half of either.
    fn write(&self, all: &BTreeSet<(String, String)>) -> io::Result<()> {
        if all.is_empty() {
            return match std::fs::remove_file(&self.path) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
                _ => Ok(()),
            };
        }
        let mut text = String::new();
        for (store, name) in all {
            text.push_str(store);
            text.push('\t');
            text.push_str(name);
            text.push('\n');
        }
        let temp = self.path.with_extension("pending.tmp");
        std::fs::write(&temp, text.as_bytes())?;
        std::fs::File::open(&temp)?.sync_all()?;
        std::fs::rename(&temp, &self.path)?;
        if let Some(dir) = self.path.parent()
            && let Ok(dir) = std::fs::File::open(dir)
        {
            let _ = dir.sync_all();
        }
        Ok(())
    }
}
