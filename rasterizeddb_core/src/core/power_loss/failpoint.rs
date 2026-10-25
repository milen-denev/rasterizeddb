//! Named failpoints.
//!
//! A site calls [`failpoint!`]`(name, path)` (an `io::Result<()>`), and a
//! test arms that name for paths under one directory, so tests running in
//! parallel never trip each other's. An armed failpoint fires on exactly its
//! `nth` hit and runs its action: cut power (a closure that freezes a
//! [`super::image::Recorder`]), park, or fail the site with an error.
//!
//! Every sync `sync_audit` notes is also a site, named `sync:<tag>` (see
//! [`super::SYNC_TAGS`]), hit just before the sync is issued. Those can only
//! continue: `note` has no way to fail its caller.
//!
//! Names are checked when armed, so a typo is a panic rather than a test
//! that silently never fires: the v1 names of design §11, `sync:<tag>`, and
//! `test.*` for this module's own tests.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;

/// What a site does after its action ran.
pub enum Outcome {
    Continue,
    Fail(io::Error),
}

/// One hit of an armed failpoint.
pub struct Hit<'a> {
    pub name: &'a str,
    pub path: &'a Path,
    pub nth: u64,
}

pub type Action = StdArc<dyn Fn(&Hit<'_>) -> Outcome + Send + Sync>;

/// The v1 failpoints of design §11, by name. Only
/// `keep_tail_after_rename_fails` has a site today (`Wal::keep_tail_locked`,
/// right after the rename); the others have none until the paths they sit
/// in exist, and arming one now is allowed so harness scenarios can be
/// written against the table.
pub const V1_NAMES: &[&str] = &[
    "snapshot_before_fsync",
    "snapshot_after_fsync",
    "snapshot_after_rename",
    "snapshot_after_dir_fsync",
    "clean_append_before_fsync",
    "clean_after_fsync",
    "clean_fsync_fails",
    "ticketless_write_publishing",
    "held_writer_during_cancelled_seal",
    "ticketless_write_parked_in_provider",
    "barrier_root_sync_fails",
    "barrier_dir_sync_fails",
    "apply_held_fails_then_shutdown",
    "keep_tail_after_rename_fails",
    "lazy_open_fails_after_marker",
    "marker_compaction_after_rename",
    "marker_rot_after_clean",
    "dirty_append_before_fsync",
    "dirty_after_fsync",
    "dirty_enospc_after_first_write",
    "first_write_after_snapshot",
    "seal_between_commit_and_failed_sync",
    "ixm_read_eio_at_open",
    "unknown_rewrite_after_first_write",
    "flt_evidence_before_ixs_unlink",
    "unknown_flt_delete_order",
    "vacuum_batch_after_compaction",
    "backfill_batch_after_compaction",
    "staged_tmp_at_snapshot",
    "invalidate_after_first_compaction_batch",
    "mid_vacuum_full_phase2",
];

/// Whether `name` may be armed.
pub fn is_known(name: &str) -> bool {
    if let Some(tag) = name.strip_prefix("sync:") {
        return super::SYNC_TAGS.iter().any(|(t, _)| *t == tag);
    }
    name.starts_with("test.") || V1_NAMES.contains(&name)
}

struct Armed {
    id: u64,
    name: String,
    scope: PathBuf,
    nth: u64,
    hits: u64,
    fired: StdArc<AtomicU64>,
    action: Action,
}

static ARMED: AtomicUsize = AtomicUsize::new(0);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static REGISTRY: Mutex<Vec<Armed>> = Mutex::new(Vec::new());

/// Disarms on drop.
pub struct Guard {
    id: u64,
    fired: StdArc<AtomicU64>,
}

impl Guard {
    /// How many times the action ran (0 or 1).
    pub fn fired(&self) -> u64 {
        self.fired.load(Ordering::Acquire)
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let mut reg = REGISTRY.lock();
        if let Some(i) = reg.iter().position(|a| a.id == self.id) {
            reg.remove(i);
            ARMED.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Arms `name` for paths under `scope`, to run `action` on its `nth` hit
/// (counting from 1). Panics on a name [`is_known`] refuses.
pub fn arm(name: &str, scope: &Path, nth: u64, action: Action) -> Guard {
    assert!(is_known(name), "no failpoint is named {name:?}");
    assert!(nth >= 1, "hits count from 1");
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let fired = StdArc::new(AtomicU64::new(0));
    REGISTRY.lock().push(Armed {
        id,
        name: name.to_string(),
        scope: scope.to_path_buf(),
        nth,
        hits: 0,
        fired: fired.clone(),
        action,
    });
    ARMED.fetch_add(1, Ordering::AcqRel);
    Guard { id, fired }
}

/// A site: runs the action of every failpoint armed under `name` for a
/// directory holding `path` whose `nth` hit this is. One relaxed load when
/// nothing is armed.
pub fn hit(name: &str, path: &Path) -> io::Result<()> {
    if ARMED.load(Ordering::Relaxed) == 0 {
        return Ok(());
    }
    let due: Vec<(Action, u64, StdArc<AtomicU64>)> = {
        let mut reg = REGISTRY.lock();
        reg.iter_mut()
            .filter(|a| a.name == name && path.starts_with(&a.scope))
            .filter_map(|a| {
                a.hits += 1;
                (a.hits == a.nth).then(|| (a.action.clone(), a.hits, a.fired.clone()))
            })
            .collect()
    };
    // Run outside the registry lock: an action may take a long time (a cut
    // copies a directory) or park until another thread arms or hits.
    let mut result = Ok(());
    for (action, nth, fired) in due {
        let outcome = action(&Hit { name, path, nth });
        fired.fetch_add(1, Ordering::AcqRel);
        if let Outcome::Fail(e) = outcome
            && result.is_ok()
        {
            result = Err(e);
        }
    }
    result
}

/// The `sync:<tag>` site of a noted sync. Its action cannot fail the sync.
pub fn on_sync(event: &crate::core::sync_audit::SyncEvent) {
    if ARMED.load(Ordering::Relaxed) == 0 {
        return;
    }
    let _ = hit(&format!("sync:{}", event.tag), &event.path);
}

/// `failpoint!(name, path)`: an `io::Result<()>` for the site to `?`.
macro_rules! failpoint {
    ($name:expr, $path:expr) => {
        $crate::core::power_loss::failpoint::hit($name, $path)
    };
}
pub(crate) use failpoint;
