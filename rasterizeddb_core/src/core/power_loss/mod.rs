//! Power-loss harness: crash images, named failpoints, and the index twin
//! suites run through a clean restart and through crash images.
//!
//! Persistent indexes design §11 (phase 0). A killed process keeps the page
//! cache, so `SIGKILL` cannot lose an unsynced write, keep a later write
//! while losing an earlier one, or undo a rename. Nothing here kills
//! anything. A [`image::Recorder`] watches one directory through
//! `sync_audit` and, at a cut, freezes what a power loss there could leave:
//!
//! - **each file as of its last completed sync**: at every sync the harness
//!   copies the file as it was when the sync was issued, and uses that copy
//!   once the sync is known to have returned
//!   ([`crate::core::sync_audit::completion`]). A sync still in flight at
//!   the cut makes nothing durable;
//! - **unsynced pages kept or dropped, per 4 KiB page, seeded**: a page that
//!   differs from the synced copy is kept (current bytes) or dropped (synced
//!   bytes, or zeros past the synced length), and the length is chosen
//!   between the synced one and the current one. Logs are files like any
//!   other, so a kept page after a dropped one is a torn tail;
//! - **directory operations a hooked path announced**
//!   ([`crate::core::sync_audit::dir_op`]) and no completed directory sync
//!   has covered are kept or reverted, newest first, as a prefix: a journal
//!   keeps operations in order.
//!
//! Each seed then writes one image, which the twin suites open as a fresh
//! database. Seed kinds (keep probability `p` for unsynced pages and
//! unsynced directory operations), cycling: 0: `p = 0`; 1: `p = 1`; 2:
//! `p = 1` with every log cut at its synced length; 3: `p = 0.5`; 4: `p`
//! random. `RDB_POWER_LOSS_SEEDS` sets how many seeds each cut is imaged
//! with (default 5, one of each kind).
//!
//! What the model trusts and does not see:
//! - a sync is what `sync_audit::note` names. A site that syncs without
//!   noting (`mvcc/status.rs`'s sidecar, `bulk_load.rs`) is invisible, so a
//!   file **created** during a scenario that no recorded sync or hooked
//!   operation touched is taken whole, as an image read from the directory
//!   would show it. A file that existed at the baseline, or that some
//!   recorded sync touched, follows the page model.
//! - a directory operation nobody announces is kept. Only the log's
//!   keep-tail checkpoint announces its create and rename today; the v1
//!   publish, marker and unlink paths will (§11).
//!
//! The cut copies files while background threads (the log writer's round,
//! a checkpoint on a blocking thread) may still run, so a cut during which
//! another thread records a sync is discarded as disturbed rather than
//! imaged: it might pair a later write with an earlier durable state.

pub(crate) mod failpoint;
pub(crate) mod image;

#[cfg(test)]
mod image_tests;
#[cfg(test)]
mod twin_tests;

use std::path::{Path, PathBuf};

/// Called by `sync_audit::note` for every sync of a watched path, after it is
/// recorded and before the caller syncs.
pub(crate) fn on_sync(event: &crate::core::sync_audit::SyncEvent) {
    image::on_sync(event);
    failpoint::on_sync(event);
}

/// Called by `sync_audit::dir_op` before a hooked directory operation.
pub(crate) fn on_dir_op(op: crate::core::sync_audit::DirOp<'_>, seq: u64) {
    image::on_dir_op(op, seq);
}

/// Sites that sync a temporary file and rename it over `path` note the final
/// path. The file they actually sync, where it can be named.
pub(crate) fn temp_synced_for(tag: &str, path: &Path) -> Option<PathBuf> {
    match tag {
        // `wal::write_lsn_mark` and `wal::write_txn_mark`, through `write_mark`.
        "wal.lsn_mark" => Some(path.with_extension("lsn.partial")),
        "wal.txn_mark" => Some(path.with_extension("txn.partial")),
        // `IdReservation`'s unlogged write: `<name>.partial`.
        "mvcc.id_reservation" => {
            let mut name = path.file_name()?.to_os_string();
            name.push(".partial");
            Some(path.with_file_name(name))
        }
        // `Wal::keep_tail_locked`.
        "wal.keep_tail" => Some(path.with_extension("ckpt")),
        // `torn_tail::Floors::store`.
        "wal.floors" => Some(path.with_extension(crate::core::torn_tail::FLOORS_PARTIAL_EXTENSION)),
        // `database::rewrite::write_intent`.
        "rewrite.intent" => Some(path.with_extension("partial")),
        _ => None,
    }
}

/// The directory a directory sync flushed: `dir` notes a file in it
/// (`wal::sync_parent_dir`), `partition_dir` the directory itself
/// (`partition::files::sync_dir`). `None` for every other tag.
pub(crate) fn dir_synced_by<'a>(tag: &str, path: &'a Path) -> Option<&'a Path> {
    match tag {
        "dir" => path.parent(),
        "partition_dir" => Some(path),
        _ => None,
    }
}

/// Every tag `sync_audit::note` is called with, and whether it flushes
/// file bytes (a directory sync or an unlink notice does not). A source test
/// keeps this complete: a tag missing here would be a sync the model
/// misreads.
pub(crate) const SYNC_TAGS: &[(&str, bool)] = &[
    ("dir", false),
    ("unlink", false),
    ("file.data", true),
    ("wal.log", true),
    ("wal.lsn_mark", true),
    ("wal.txn_mark", true),
    ("wal.truncate", true),
    ("wal.stage_segment", true),
    ("wal.checkpoint.data_file", true),
    ("wal.keep_tail", true),
    ("wal.recover.data_file", true),
    // `database::incompat::write`: the temporary file itself, then a rename
    // and a directory sync.
    ("incompat_features", true),
    // `IdReservation` without a log: a temporary file, a rename and a
    // directory sync.
    ("mvcc.id_reservation", true),
    // `torn_tail::truncate_to_whole_records`: the cut of a torn record, at
    // open, before replay.
    ("torn_tail.cut", true),
    // `torn_tail::Floors::store`: a temporary file, a rename and a directory
    // sync.
    ("wal.floors", true),
    // `partition::files::sync_dir`: a directory sync that notes the
    // directory itself (see `dir_synced_by`).
    ("partition_dir", false),
    // `database::rewrite::write_intent`: a temporary file, a rename over the
    // intent and a directory sync.
    ("rewrite.intent", true),
];
