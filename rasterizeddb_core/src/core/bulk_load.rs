//! Unlogged, unsynced appends for a load that can be thrown away and redone.
//!
//! A durable append costs the row itself, a log record for it, and a commit
//! marker — and a statement's commit then pays a device flush. That is the
//! right price for a client's write, which the client has been told succeeded.
//! It is the wrong price for an import: nothing has been promised about a
//! database that is still being built, and a database half-built when the
//! process dies is dropped and rebuilt from the source either way. Paying for
//! durability row by row there buys nothing that the one sync at the end does
//! not.
//!
//! So while a directory is registered here, `append_data_txn` writes the bytes
//! and stops: no log record, no flush. The loader takes one sync over every
//! file when it is done, then lets the directory go. A crash before that point
//! leaves the directory as debris — which is exactly what it was going to be.
//!
//! Scoped to a directory, never to the process: an import builds into its own
//! database, and every other database on the server keeps its guarantees.

use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use cacheguard::CacheGuard;

/// How many directories are registered. Read on every append, so the common
/// case — none — costs one relaxed load and no lock.
static ACTIVE_COUNT: CacheGuard<AtomicUsize> = CacheGuard::new(AtomicUsize::new(0));

static ACTIVE: Mutex<Option<HashSet<String>>> = Mutex::new(None);

/// Marks `location` as being bulk-loaded for as long as the guard lives.
pub struct BulkLoad {
    key: String,
}

/// Starts a bulk load into `location`.
pub fn begin(location: &str) -> BulkLoad {
    let key = crate::core::wal::dir_key(location);
    let mut active = ACTIVE.lock().unwrap_or_else(|e| e.into_inner());
    let set = active.get_or_insert_with(HashSet::new);
    if set.insert(key.clone()) {
        ACTIVE_COUNT.fetch_add(1, Ordering::Release);
    }
    BulkLoad { key }
}

impl Drop for BulkLoad {
    fn drop(&mut self) {
        let mut active = ACTIVE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(set) = active.as_mut() {
            if set.remove(&self.key) {
                ACTIVE_COUNT.fetch_sub(1, Ordering::Release);
            }
        }
    }
}

/// Whether appends under `location` may skip the log and the flush.
#[inline]
pub fn is_active(location: &str) -> bool {
    if ACTIVE_COUNT.load(Ordering::Acquire) == 0 {
        return false;
    }
    let active = ACTIVE.lock().unwrap_or_else(|e| e.into_inner());
    active
        .as_ref()
        .is_some_and(|set| set.contains(&crate::core::wal::dir_key(location)))
}

/// Makes everything appended under `location` while it was bulk-loaded
/// durable: one `fdatasync` per file the directory's log knows of.
///
/// The log's registry is the list because every data file registers with it
/// when opened, whether or not any record was ever written for it — which is
/// exactly the case here, since the point of the mode is that none were.
/// Without a log for the directory there is no registry, and the caller is
/// told so rather than handed a success that synced nothing.
pub fn sync_all(location: &str) -> std::io::Result<()> {
    let Some(wal) = crate::core::wal::wal_for(location) else {
        return Err(std::io::Error::other(format!(
            "no write-ahead log is registered for {}, so its files cannot be enumerated for \
             the final sync",
            location
        )));
    };
    let mut first_error = None;
    for (_, path) in wal.registered_files() {
        match std::fs::File::options().read(true).write(true).open(&path) {
            Ok(f) => {
                if let Err(e) = f.sync_data() {
                    first_error.get_or_insert(e);
                }
            }
            // Dropped since it registered — a table that was rewritten, say.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                first_error.get_or_insert(e);
            }
        }
    }
    match first_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_directory_is_active_only_while_its_guard_lives() {
        let dir = "/tmp/rdb-bulk-load-test-a";
        assert!(!is_active(dir));
        {
            let _guard = begin(dir);
            assert!(is_active(dir));
            assert!(is_active("/tmp/rdb-bulk-load-test-a/"), "keyed like the log: trailing separator ignored");
            assert!(!is_active("/tmp/rdb-bulk-load-test-b"), "scoped to the directory");
        }
        assert!(!is_active(dir));
    }
}
