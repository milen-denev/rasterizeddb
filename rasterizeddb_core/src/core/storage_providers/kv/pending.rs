//! Sweeps of dropped files that have not finished, kept on local disk.
//!
//! A dropped file's objects are deleted by a task (`KvFamily::sweep`) that
//! retries until the store lets it, holding the name meanwhile. A server that
//! stopped before it finished left the objects behind with nothing naming them,
//! and a table created later under the same name read them as its own — or
//! replayed them at its next restart. So the name is written here before the
//! sweep starts and taken out once it is done, and a family that opens resumes
//! every sweep this file still lists, before any table can open the name.
//!
//! The invariant everything rests on: **a name is listed here only while no
//! table can hold it.** The sweep keeps the name's gate closed until its line
//! is gone, retrying the removal like the erase, so a line that outlived its
//! sweep can never meet a table that took the name again — whose objects the
//! next start's resumed sweep would otherwise erase. Hence the strictness
//! below: a read that fails is an error, not an empty list (an empty list
//! would let `remove` report success with the line still there), and removing
//! the last line is made durable like any other write.
//!
//! One file per database, beside `db_kv.cfg`: `store<TAB>identity<TAB>name` a
//! line. The identity — the store's address and prefix — is part of the key:
//! a store redefined at another address under the same name must not have
//! the old address's sweeps resumed against it. Left out of every backup
//! snapshot — a restored copy resuming a sweep would delete from the store the
//! live server still uses.

use std::collections::BTreeSet;
use std::io;
use std::path::PathBuf;

use log::warn;
use parking_lot::Mutex;

/// The file's name in a database directory.
pub const PENDING_FILE: &str = "db_kv_erase.pending";

type Entry = (String, String, String);

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

    /// The names of `store` at `identity` still to sweep. Lines of `store` at
    /// any other identity are for objects at an address this store no longer
    /// has, and are dropped with a warning rather than resumed here.
    pub fn take_for(&self, store: &str, identity: &str) -> io::Result<Vec<String>> {
        let _one = self.writing.lock();
        let mut all = self.read()?;
        let stale: Vec<Entry> = all.iter().filter(|(s, i, _)| s == store && i != identity).cloned().collect();
        if !stale.is_empty() {
            for (_, at, name) in &stale {
                warn!("kv: the sweep of {name} on store '{store}' was for {at}, which it no longer is; not resumed");
                all.remove(&(store.to_string(), at.clone(), name.clone()));
            }
            self.write(&all)?;
        }
        Ok(all.into_iter().filter(|(s, i, _)| s == store && i == identity).map(|(_, _, n)| n).collect())
    }

    /// The names of `store` at `identity` still to sweep, read-only.
    pub fn names(&self, store: &str, identity: &str) -> io::Result<Vec<String>> {
        let _one = self.writing.lock();
        Ok(self.read()?.into_iter().filter(|(s, i, _)| s == store && i == identity).map(|(_, _, n)| n).collect())
    }

    /// Records that `name` of `store` at `identity` is being swept. Durable
    /// before it returns.
    pub fn add(&self, store: &str, identity: &str, name: &str) -> io::Result<()> {
        for field in [store, identity, name] {
            if field.contains(['\t', '\n', '\r']) {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("{field:?} cannot be recorded on one line")));
            }
        }
        let _one = self.writing.lock();
        let mut all = self.read()?;
        if all.insert((store.to_string(), identity.to_string(), name.to_string())) {
            self.write(&all)?;
        }
        Ok(())
    }

    /// Records that the sweep of `name` of `store` at `identity` is done.
    /// Durable before it returns.
    pub fn remove(&self, store: &str, identity: &str, name: &str) -> io::Result<()> {
        let _one = self.writing.lock();
        let mut all = self.read()?;
        if all.remove(&(store.to_string(), identity.to_string(), name.to_string())) {
            self.write(&all)?;
        }
        Ok(())
    }

    fn read(&self) -> io::Result<BTreeSet<Entry>> {
        let text = match std::fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
            Err(e) => return Err(e),
        };
        let mut out = BTreeSet::new();
        for line in text.lines().filter(|l| !l.is_empty()) {
            let mut fields = line.split('\t');
            match (fields.next(), fields.next(), fields.next(), fields.next()) {
                (Some(store), Some(identity), Some(name), None) => {
                    out.insert((store.to_string(), identity.to_string(), name.to_string()));
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{} has a line it cannot read: {line:?}", self.path.display()),
                    ));
                }
            }
        }
        Ok(out)
    }

    /// Through a temporary file and a rename, so a crash leaves the old list
    /// or the new one, never half of either; the directory is synced either
    /// way, so a removal cannot come back after a power loss.
    fn write(&self, all: &BTreeSet<Entry>) -> io::Result<()> {
        if all.is_empty() {
            match std::fs::remove_file(&self.path) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
        } else {
            let mut text = String::new();
            for (store, identity, name) in all {
                text.push_str(store);
                text.push('\t');
                text.push_str(identity);
                text.push('\t');
                text.push_str(name);
                text.push('\n');
            }
            let temp = self.path.with_extension("pending.tmp");
            std::fs::write(&temp, text.as_bytes())?;
            std::fs::File::open(&temp)?.sync_all()?;
            std::fs::rename(&temp, &self.path)?;
        }
        if let Some(dir) = self.path.parent() {
            std::fs::File::open(dir)?.sync_all()?;
        }
        Ok(())
    }
}
