//! Counts durable syncs by call site, for tests that need to know which code
//! path flushed which file.
//!
//! A checkpoint used to fsync every registered table file, and on a database
//! of 2,042 tables that was the whole cost of `VACUUM FULL`; which pass came
//! from where could only be guessed at from totals. Each sync site that can
//! touch many files calls [`note`] with a tag naming it. Off unless a test
//! [`watch`]es a directory, in which case only syncs of paths under that
//! directory are recorded, so tests running in parallel do not see each
//! other's. The cost when off is one relaxed atomic load.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

static WATCHERS: AtomicUsize = AtomicUsize::new(0);
static WATCHED: Mutex<Vec<(PathBuf, Vec<(&'static str, PathBuf)>)>> = Mutex::new(Vec::new());

/// One noted sync with what a crash image needs to know about it: the file's
/// length when the sync was issued, which every byte of is durable once it
/// returns, and the thread that issued it.
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct SyncEvent {
    pub tag: &'static str,
    pub path: PathBuf,
    pub len: u64,
    pub thread: std::thread::ThreadId,
}

/// Per watched directory, in order. Test-only: the `metadata` call is a
/// system call per sync, which production does not pay.
#[cfg(test)]
static EVENTS: Mutex<Vec<(PathBuf, Vec<SyncEvent>)>> = Mutex::new(Vec::new());

/// Records a sync of `path` from the site named `tag`.
#[inline]
pub fn note(tag: &'static str, path: &Path) {
    if WATCHERS.load(Ordering::Relaxed) == 0 {
        return;
    }
    let mut watched = WATCHED.lock();
    for (dir, seen) in watched.iter_mut() {
        if path.starts_with(dir) {
            seen.push((tag, path.to_path_buf()));
        }
    }
    drop(watched);
    #[cfg(test)]
    {
        let mut events = EVENTS.lock();
        if events.iter().any(|(dir, _)| path.starts_with(dir)) {
            // Taken before the caller's sync, so it is a lower bound on what
            // that sync makes durable: a crash image cut here never claims
            // more than the disk could hold.
            let len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            let thread = std::thread::current().id();
            for (dir, seen) in events.iter_mut() {
                if path.starts_with(dir) {
                    seen.push(SyncEvent {
                        tag,
                        path: path.to_path_buf(),
                        len,
                        thread,
                    });
                }
            }
        }
    }
}

/// Starts recording syncs of paths under `dir`; the guard stops it and hands
/// back what was seen.
pub fn watch(dir: &Path) -> Watch {
    WATCHED.lock().push((dir.to_path_buf(), Vec::new()));
    #[cfg(test)]
    EVENTS.lock().push((dir.to_path_buf(), Vec::new()));
    WATCHERS.fetch_add(1, Ordering::AcqRel);
    Watch {
        dir: dir.to_path_buf(),
    }
}

pub struct Watch {
    dir: PathBuf,
}

impl Watch {
    /// Everything recorded so far, and starts afresh.
    pub fn take(&self) -> Vec<(&'static str, PathBuf)> {
        let mut watched = WATCHED.lock();
        watched
            .iter_mut()
            .find(|(d, _)| *d == self.dir)
            .map(|(_, seen)| std::mem::take(seen))
            .unwrap_or_default()
    }

    /// Every sync recorded so far with its length and thread, in order, and
    /// starts afresh. Independent of [`Watch::take`].
    #[cfg(test)]
    pub fn take_events(&self) -> Vec<SyncEvent> {
        let mut events = EVENTS.lock();
        events
            .iter_mut()
            .find(|(d, _)| *d == self.dir)
            .map(|(_, seen)| std::mem::take(seen))
            .unwrap_or_default()
    }

    /// Syncs recorded so far, counted per tag, and starts afresh.
    pub fn counts(&self) -> std::collections::BTreeMap<&'static str, usize> {
        let mut out = std::collections::BTreeMap::new();
        for (tag, _) in self.take() {
            *out.entry(tag).or_insert(0) += 1;
        }
        out
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        let mut watched = WATCHED.lock();
        if let Some(i) = watched.iter().position(|(d, _)| *d == self.dir) {
            watched.remove(i);
        }
        drop(watched);
        #[cfg(test)]
        {
            let mut events = EVENTS.lock();
            if let Some(i) = events.iter().position(|(d, _)| *d == self.dir) {
                events.remove(i);
            }
        }
        WATCHERS.fetch_sub(1, Ordering::AcqRel);
    }
}
