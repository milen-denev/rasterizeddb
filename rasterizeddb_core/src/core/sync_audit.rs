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
}

/// Starts recording syncs of paths under `dir`; the guard stops it and hands
/// back what was seen.
pub fn watch(dir: &Path) -> Watch {
    WATCHED.lock().push((dir.to_path_buf(), Vec::new()));
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
        WATCHERS.fetch_sub(1, Ordering::AcqRel);
    }
}
