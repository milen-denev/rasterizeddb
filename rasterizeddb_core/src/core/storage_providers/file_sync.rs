use std::{
    cmp::min,
    fs::{self, remove_file},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    usize,
};

use std::cell::RefCell;

use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};

use arc_swap::ArcSwap;
use log::error;
use opool::{Pool, PoolAllocator};
use parking_lot::{Mutex, RwLock};
use rclite::Arc as RcArc;
use std::sync::OnceLock;
use tokio::task::yield_now;

use memmap2::{Mmap, MmapOptions};
use smallvec::SmallVec;

// Removed chunk cache; no longer need helpers::Chunk

use super::{CRC, traits::StorageIO};
use crate::core::index_store::gated_io::{Failure, GatedIo, Site};

/// Registers a write of this provider's file on its file set's word
/// (`index_store::gated_io`, B12) and evaluates to the registration, to hold
/// until the write returns. The fast path is synchronous
/// (`GatedIo::admit_now`); only release's slow path is awaited, boxed, so a
/// write's future carries one pointer for it. `$fail` runs, with the error
/// bound to `$e`, when that slow path failed and nothing may be issued.
macro_rules! gated {
    ($self:ident, $site:expr, |$e:ident| $fail:expr) => {{
        let registration = match $self.commit.gated.admit_now($site, &$self.location, &$self.table_name) {
            Ok(registration) => registration,
            Err(slow) => match slow.await {
                Ok(registration) => registration,
                Err($e) => $fail,
            },
        };
        // Tests only: a write parked here, registered and not yet issued, is
        // what a seal's count must cover (RV15).
        #[cfg(test)]
        crate::core::index_store::gated_io::park::at(&$self.table_name).await;
        $self.commit.gated.note_write(&$self.location, &$self.table_name);
        registration
    }};
}

pub enum FastPathResult {
    Done,              // fully satisfied
    EofPartial(usize), // partial bytes copied (usize) then EOF
    EofNone,           // position at/after EOF before copying
}

/// Lets one flush make several concurrent appends durable.
///
/// Every immediate append used to issue its own `fsync`, so N clients inserting
/// at once paid N device flushes even though a single flush persists everything
/// written before it. This tracks which appends are already covered so waiters
/// can share one.
#[derive(Default)]
struct CommitState {
    /// The write gate of the file set this file belongs to, once its table
    /// is assembled (`StorageIO::attach_write_gate`) or inherited from the
    /// provider this one was opened from (a schema sidecar): every write
    /// entry point registers on it first (`index_store::gated_io`, B12), and
    /// the background landing of staged bytes takes a count on it for each
    /// landing, or waits for the next round (docs/persistent-indexes.md §2:
    /// `.tmp` landing passes the gate). Shared across clones, like the rest
    /// of this.
    gated: crate::core::index_store::gated_io::GatedIo,
    /// Appends whose `write` has completed. Bumped under the append lock, so it
    /// can never run ahead of the bytes it stands for.
    ///
    /// Not padded. Splitting these two onto separate cache lines was tried and
    /// measured *worse* — `insert_shape_100_rows` went from -2.0% to +2.3%
    /// against the same baseline (p = 0.00 both ways). The waiters that poll
    /// `durable` are already serialised behind `gate`, so the false sharing the
    /// padding was meant to remove was not there to remove, and the cost of
    /// filling a second line on every append was.
    issued: AtomicU64,
    /// Highest `issued` value known to be on stable storage.
    durable: AtomicU64,
    /// Serializes the flush itself.
    gate: Mutex<()>,
    /// Flush handle, opened on first use. Deliberately *not* `append_file`: a
    /// flush must not hold the append lock, or the next appender would block on
    /// it and there would be no group left to commit.
    handle: OnceLock<std::fs::File>,
    /// The first flush failure, after which nothing here is durable again.
    ///
    /// Permanent for the reason the log's poisoning is: Linux drops the dirty
    /// pages an `fdatasync` reported as failed, so a later flush that succeeds
    /// does not cover them. The failure used to be discarded and the sequence
    /// published as durable anyway, which on the no-log path is the only
    /// durability mechanism reporting a success it did not have.
    poisoned: OnceLock<Poison>,
    /// Makes the next flush fail as a failed `fdatasync` would. Tests only:
    /// there is no portable way to make a real file's flush fail on demand.
    #[cfg(test)]
    fail_next_flush: AtomicBool,
    /// When this file's handles came to be: the log refuses their records once
    /// the file has been deleted since (see `Wal::forget_files`).
    born: crate::core::wal::Incarnation,
}

/// Why a file stopped being durable.
struct Poison {
    why: String,
    /// The file was no longer at its path when the first flush tried to open
    /// it: unlinked by `DROP TABLE` or `DROP DATABASE` while a writer still
    /// held its handle. Such a failure poisons the file, so its own commits
    /// are refused, but it is not recorded against the directory — the
    /// unlinked bytes are unreachable whether or not they were flushed, so
    /// no commit of a sibling can name lost data. Recording it did harm: a
    /// dropped database's maintenance tasks keep writing after the drop has
    /// forgotten its failures, and the record they left refused every commit
    /// of a database recreated under the same name.
    ///
    /// Inside the `OnceLock` with the reason, not beside it, so whoever sees
    /// the poison sees whether it was this kind.
    file_gone: bool,
}

/// Database directories in which some data file has failed a flush.
///
/// A flush failure poisons the file it happened on, but that file is often
/// not the one whose commit decides whether a statement succeeded. An INSERT
/// writes its row bytes to one file and commits through the *pointers* file,
/// so a rows file that could not be flushed went unnoticed: the pointer became
/// durable and named row bytes that were not. The failure is therefore also
/// recorded against the directory, and every commit in that directory checks
/// it — the same scope the log's own poisoning has.
static FAILED_DIRECTORIES: OnceLock<Mutex<std::collections::HashMap<String, String>>> =
    OnceLock::new();

/// Set once anything is in [`FAILED_DIRECTORIES`], so a commit in a healthy
/// process pays one load rather than a lock.
static ANY_DIRECTORY_FAILED: AtomicBool = AtomicBool::new(false);

pub(crate) fn record_directory_failure(location: &str, why: String) {
    let mut failed = FAILED_DIRECTORIES
        .get_or_init(|| Mutex::new(std::collections::HashMap::new()))
        .lock();
    failed
        .entry(crate::core::wal::dir_key(location))
        .or_insert(why);
    ANY_DIRECTORY_FAILED.store(true, Ordering::Release);
}

/// Forgets a recorded flush failure for `location`, whose files are going away.
///
/// For `DROP DATABASE` only. The failure describes files in that directory,
/// and once they are unlinked it describes nothing — but `CREATE DATABASE`
/// reuses the same path, and a remembered failure would refuse every commit
/// of the new, healthy database until the process restarted. The log registry
/// forgets a dropped database's poisoned log for the same reason, and the two
/// must agree.
pub fn forget_directory_failure(location: &str) {
    let Some(failed) = FAILED_DIRECTORIES.get() else {
        return;
    };
    let mut failed = failed.lock();
    failed.remove(&crate::core::wal::dir_key(location));
    // Under the lock that `record_directory_failure` raises it under, so a
    // failure recorded concurrently elsewhere cannot be hidden by this.
    ANY_DIRECTORY_FAILED.store(!failed.is_empty(), Ordering::Release);
}

/// The first flush failure of any data file in `location`, if there was one.
///
/// Permanent for the life of the process, like the poisoning it reports,
/// unless the database is dropped (see [`forget_directory_failure`]).
pub fn directory_flush_failure(location: &str) -> Option<String> {
    if !ANY_DIRECTORY_FAILED.load(Ordering::Acquire) {
        return None;
    }
    FAILED_DIRECTORIES
        .get()?
        .lock()
        .get(&crate::core::wal::dir_key(location))
        .cloned()
}

impl CommitState {
    /// Records a completed append. Call with the append lock held.
    #[inline]
    fn issue(&self) -> u64 {
        self.issued.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Returns once every append up to `seq` is on stable storage, or the
    /// error that means it never will be.
    fn flush_through(&self, seq: u64, path: &str) -> io::Result<()> {
        if let Some(poison) = self.poisoned.get() {
            return Err(Self::poisoned_error(&poison.why));
        }
        if self.durable.load(Ordering::Acquire) >= seq {
            return Ok(()); // an earlier flush already covered us
        }

        let _gate = self.gate.lock();

        if self.durable.load(Ordering::Acquire) >= seq {
            return Ok(()); // covered while we waited for the gate
        }

        // Read the counter *before* flushing. The flush also persists every
        // append issued after ours, so publishing the highest sequence written
        // so far — not just our own — is what collapses a burst of concurrent
        // appends into a single device flush.
        let covered = self.issued.load(Ordering::Acquire);

        if self.handle.get().is_none() {
            match std::fs::File::options().read(true).write(true).open(path) {
                Ok(file) => {
                    let _ = self.handle.set(file);
                }
                Err(e) => {
                    let file_gone = e.kind() == io::ErrorKind::NotFound;
                    return Err(self.poison_with(
                        format!("open {} for flush: {}", path, e),
                        file_gone,
                    ));
                }
            }
        }
        let file = self.handle.get().expect("set just above");

        // `sync_data` (fdatasync) rather than `sync_all` (fsync): an append only
        // needs its bytes and the new file size durable to be readable after a
        // crash. Forcing out mtime/ctime costs an extra journal transaction and
        // buys nothing here.
        crate::core::sync_audit::note("file.data", Path::new(path));
        #[cfg(test)]
        let flushed = if self.fail_next_flush.swap(false, Ordering::AcqRel) {
            Err(io::Error::other("injected fdatasync failure"))
        } else {
            file.sync_data()
        };
        #[cfg(not(test))]
        let flushed = file.sync_data();
        if let Err(e) = flushed {
            return Err(self.poison(format!("fdatasync {}: {}", path, e)));
        }

        self.durable.fetch_max(covered, Ordering::AcqRel);
        Ok(())
    }

    /// Records the first failure and returns it as the permanent kind.
    fn poison(&self, why: String) -> io::Error {
        self.poison_with(why, false)
    }

    /// [`Self::poison`], saying whether the cause was the file being gone.
    fn poison_with(&self, why: String, file_gone: bool) -> io::Error {
        error!("flush failed; file is no longer durable: {}", why);
        let poison = self.poisoned.get_or_init(|| Poison { why, file_gone });
        Self::poisoned_error(&poison.why)
    }

    /// Whether this file's failure belongs to its directory too: every
    /// failure except a file that had already been unlinked (see
    /// [`Poison::file_gone`]).
    fn poison_concerns_directory(&self) -> bool {
        self.poisoned.get().is_some_and(|p| !p.file_gone)
    }

    fn poisoned_error(why: &str) -> io::Error {
        io::Error::other(format!("{}: {}", super::traits::POISONED_LOG, why))
    }

    /// The failure that stopped this file being durable, if one happened.
    fn poison_error(&self) -> Option<io::Error> {
        self.poisoned.get().map(|p| Self::poisoned_error(&p.why))
    }
}

impl LocalStorageProvider {
    /// [`StorageIO::commit_durable`], with whether staged appends are landed
    /// too. An immediate append commits only its own bytes and passes
    /// `false`: landing staged ones there would move them ahead of later
    /// immediate appends.
    /// Writes `buffer` at `position`, first keeping the bytes it replaces and
    /// putting them back if the write fails. On failure returns the write's
    /// error and whether the restore worked.
    ///
    /// Only the overwritten range is copied, clipped to the file's current
    /// length — bytes past the end have nothing to restore.
    fn overwrite_with_undo(
        file: &mut std::fs::File,
        position: u64,
        buffer: &[u8],
    ) -> Result<(), (io::Error, io::Result<()>)> {
        #[cfg(test)]
        let skip_undo = failed_appends::NO_UNDO.get();
        #[cfg(not(test))]
        let skip_undo = false;

        // Positioned reads: one `pread` that stops at end of file gives exactly
        // the bytes that exist to be overwritten, with no length query and no
        // seek. Measured: three syscalls became one.
        let mut undo = Vec::new();
        if !skip_undo {
            undo.resize(buffer.len(), 0);
            let mut got = 0usize;
            while got < undo.len() {
                #[cfg(unix)]
                let n = std::os::unix::fs::FileExt::read_at(
                    &*file,
                    &mut undo[got..],
                    position + got as u64,
                );
                #[cfg(not(unix))]
                let n = file
                    .seek(SeekFrom::Start(position + got as u64))
                    .and_then(|_| file.read(&mut undo[got..]));
                match n {
                    Ok(0) => break,
                    Ok(n) => got += n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => return Err((e, Ok(()))),
                }
            }
            undo.truncate(got);
        }

        #[cfg(test)]
        let written = match failed_appends::SHORT_INPLACE.with(|h| h.take()) {
            Some(keep) => file
                .seek(SeekFrom::Start(position))
                .and_then(|_| file.write_all(&buffer[..keep.min(buffer.len())]))
                .and_then(|()| Err(io::Error::other("injected short in-place write"))),
            None => file
                .seek(SeekFrom::Start(position))
                .and_then(|_| file.write_all(buffer))
                .and_then(|()| file.flush()),
        };
        #[cfg(not(test))]
        let written = file
            .seek(SeekFrom::Start(position))
            .and_then(|_| file.write_all(buffer))
            .and_then(|()| file.flush());

        written.map_err(|e| {
            let restored = if undo.is_empty() {
                Ok(())
            } else {
                file.seek(SeekFrom::Start(position))
                    .and_then(|_| file.write_all(&undo))
                    .and_then(|()| file.flush())
            };
            (e, restored)
        })
    }

    async fn commit_durable_with(&self, token: u64, land_staged: bool) -> io::Result<()> {
        // A refusal before the commit is attempted leaves `token` neither
        // committed nor aborted, and nothing else ends it: a failed
        // `commit_durable` must not be followed by `abort_txn` (it may have
        // committed), so callers never do. Its in-place payloads, in every
        // file of the directory's transaction, then kept `pending_count`
        // raised, and every checkpoint declined until a restart. Released
        // here, where it is known the commit was never attempted.
        //
        // Never committed later: a caller's `commit_durable` is its last word
        // on a transaction (one that fails is not retried with the same
        // token, and `abort_txn` after it is refused by contract), and
        // nothing else commits a token. A later commit of it would log a
        // commit marker for payloads already released, and its in-place
        // writes would replay from the log while never being applied here.
        match self.refuse_commit(token, land_staged).await {
            Ok(()) => {}
            Err(e) => {
                // Its held payloads and recorded failures with it.
                StorageIO::abort_txn(self, token).await;
                return Err(e);
            }
        }
        self.commit_attempted(token).await
    }

    /// The checks [`Self::commit_durable_with`] makes before committing
    /// `token`, and the landing of staged bytes: an `Err` means the commit was
    /// never attempted.
    async fn refuse_commit(&self, token: u64, land_staged: bool) -> io::Result<()> {
        // Checked for every token, including the no-log path's `0`: a flush
        // that failed after an append is this file's failure to report, and
        // this is the one call every writer makes before reporting success.
        if let Some(e) = self.commit.poison_error() {
            return Err(e);
        }
        // An append in this transaction failed and was rolled back. Taken
        // before the refusals below, which would otherwise leave it recorded.
        let failed = failed_appends::take(&self.location, &self.file_str, token);
        // A sibling file in the same directory failing its flush is this
        // commit's failure too: the pointer committed here may name bytes in
        // that file which never reached the disk.
        if let Some(why) = directory_flush_failure(&self.location) {
            return Err(io::Error::other(format!(
                "{}: a data file in {} could not be flushed ({})",
                super::traits::POISONED_LOG,
                self.location,
                why
            )));
        }
        // A poisoned log refuses every commit, and a non-zero token is refused
        // by the log itself below. Asked here for the rest, because `0` does
        // not mean "nothing to do with the log": it is also what a bulk load
        // or an append that fell back to a direct flush hands back. A poisoned
        // log is never dropped again, so a restart replays every committed
        // record still in it — over whatever was written directly since.
        // Acknowledging such a write is a revert waiting for the next restart.
        // A failed write is reported as the permanent kind too, since retrying
        // it on this log cannot succeed.
        if (token == 0 || failed.is_some())
            && let Some(why) = self.wal_poison_reason()
        {
            return Err(io::Error::other(format!(
                "{}: the log of {} is poisoned ({})",
                super::traits::POISONED_LOG,
                self.location,
                why
            )));
        }
        if let Some(e) = failed {
            return Err(e);
        }
        // Staged appends (`append_data(.., false)`) sit in the `.tmp` sidecar,
        // which nothing syncs: a copy that staged a table and then asked for
        // durability here used to get `Ok` with every byte still unsynced, and
        // a crash after the catalogue named the file left it empty or short.
        // They are landed in the main file and synced, and the directory entry
        // with them, before this returns. Not from inside an immediate append
        // (`append_data_returning_offset`), which commits only its own bytes
        // and must not move staged ones ahead of later immediate appends.
        if land_staged {
            // A staged append that failed: its bytes are in neither the file
            // nor the mirror, so landing the rest would call a short copy
            // durable.
            if let Some(e) = failed_appends::take_staged(&self.file_str) {
                return Err(e);
            }
            if self.temp_has_data.load(Ordering::Acquire) {
                // A landing writes the main file: registered like any write.
                let _registered = gated!(self, Site::Land, |e| return Err(e));
                self.land_staged_durably().await?;
            }
        }
        Ok(())
    }

    /// [`Self::commit_durable_with`] past its refusals: the log's commit of
    /// `token`, which settles it whatever it returns.
    async fn commit_attempted(&self, token: u64) -> io::Result<()> {
        if token == 0 {
            return Ok(());
        }
        // Deferred: this is a row or pointer write, and the statement that
        // asked for it takes one sync for all of them before reporting success.
        // The log is borrowed rather than cloned out: see `wal::with_wal_for`.
        let committed = crate::core::wal::with_wal_for(&self.location, |w| {
            w.map(|w| {
                w.commit_txn_deferred(token)
                    // Whether this commit carried the log past its checkpoint
                    // threshold, so the rare path below is the only one that
                    // needs the log as an owned `Arc`.
                    .map(|()| w.bytes_since_checkpoint() >= crate::wal_checkpoint_bytes())
                    .map_err(|e| (e, w.is_poisoned()))
            })
        });
        let wants_checkpoint = match committed {
            None => return Ok(()),
            Some(Ok(wants)) => wants,
            Some(Err((e, poisoned))) => {
                // Logged as well as returned. The error goes to the caller so it
                // can stop reporting success; the log line stays because it is
                // what surfaced this from a live server, and an operator watching
                // logs should not have to also be reading the client's errors.
                error!("WAL commit failed: {}", e);
                // Poisoning is permanent by design, so it has to arrive at the
                // caller as something other than "try again".
                if poisoned {
                    return Err(io::Error::other(format!(
                        "{}: {}",
                        super::traits::POISONED_LOG,
                        e
                    )));
                }
                return Err(e);
            }
        };

        // The apply inside `commit_txn` writes to the data file *by path*, with
        // its own handle — this provider is bypassed entirely, so its cached
        // `file_len` and its mmap never learn that their own file grew. Without
        // this refresh a provider cannot read back an in-place write it just
        // made: the bytes are on disk and durable, and its next read still EOFs.
        //
        // `fetch_max` rather than `store`, because appends maintain `file_len`
        // themselves and stage through a temp file — the on-disk length can
        // legitimately lag the logical one, and a length must never go
        // backwards.

        // The apply inside `commit_txn` wrote by path, bypassing this provider,
        // so its cached length and mmap have not seen the bytes. Raise them to
        // what the in-place writes required. Without this a provider cannot
        // read back an in-place write it just made: the bytes are durably on
        // disk and its own next read still EOFs.
        //
        // `fetch_max`, never `store` — a length must not go backwards past a
        // concurrent append.
        let need = self.inplace_end.load(Ordering::Acquire);
        if need > 0 {
            // The MAP is refreshed before the LENGTH that describes those
            // bytes is published, never after. The other order left a window
            // in which `get_len` already reported the longer file while the
            // map still covered less of it — and a read past the end of the
            // mapped region does not fail, it copies **zero bytes** (see the
            // `pos >= mmap_len` guard in the mmap read path). A scan turns the
            // length into a slot count, so in that window it walked slots the
            // map could not serve, read them as empty, and skipped the rows
            // living there: a reader could miss a row that exists while
            // another connection was merely updating it in place, and a writer
            // that missed the current version could then collide with the
            // older one and report a serialization conflict against a
            // transaction that had already finished.
            //
            // Mapping first is safe and costs nothing: `update_memory_map`
            // maps the file at its real on-disk size, and these bytes are
            // already written and synced by the time this runs — so the map
            // covers `need` before anything is told to look there.
            if need > self.file_len.load(Ordering::Acquire) {
                self.update_memory_map();
            }
            self.file_len.fetch_max(need, Ordering::AcqRel);
        }

        if wants_checkpoint && let Some(w) = crate::core::wal::wal_for(&self.location) {
            // Off the caller's thread: a checkpoint fsyncs every data file, and
            // making whichever writer happened to cross the threshold wait for
            // all of that would show up as an unpredictable latency spike.
            // `Wal::checkpoint` already ensures only one runs at a time.
            tokio::task::spawn_blocking(move || {
                if let Err(e) = w.checkpoint() {
                    error!("WAL checkpoint failed: {}", e);
                }
            });
        }
        Ok(())
    }
}

/// Writes that failed and were rolled back, or were skipped, until a commit
/// reports them.
///
/// Process-wide, because the trait's appends and writes return no error and
/// the provider struct is shared with other work: this keeps the failure on
/// the one path every writer takes before reporting success. Empty — one
/// atomic load — unless something failed.
///
/// **Keyed by the transaction, not the file** (slice B12). A transaction's
/// writes span files: an `INSERT` appends its row to the rows file and its
/// pointer to the pointers file, and commits through the pointers. Keyed by
/// file, a failed row append was recorded where no commit ever looked: the
/// pointer committed, naming bytes that were cut back off, and the next
/// append landed on them. A transaction id is its directory log's (`Wal::
/// begin`), so a failure under one is keyed by the directory and the id, and
/// any file's commit of that transaction in the directory takes it.
/// `GatedIo` records a skipped write the same way, which is what lets a gate
/// failure on the rows fail the statement. A write with no log (`txn` 0)
/// stays keyed by its file, as it was.
pub(crate) mod failed_appends {
    use std::collections::HashMap;
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use parking_lot::Mutex;

    static COUNT: AtomicUsize = AtomicUsize::new(0);
    static FAILED: Mutex<Option<HashMap<Key, io::Error>>> = Mutex::new(None);

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    enum Key {
        /// A file's own: a log-less write (`txn` 0), or a staged append.
        File(String, u64),
        /// A directory's: a transaction of its log, or the gate key.
        Dir(String, u64),
    }

    /// The key for a failed staged append (`append_data(.., false)`), which
    /// belongs to no transaction: reported by the next `commit_durable` of
    /// the same file that lands staged bytes. No transaction id reaches it —
    /// they count up from 1.
    pub(super) const STAGED: u64 = u64::MAX;

    /// The key for a gate failure on a write with no transaction (B12:
    /// `write_data_seek`, a staged append, `swap_temp`): per directory,
    /// reported by the next `commit_durable` of any file in it.
    const GATE: u64 = u64::MAX - 1;

    fn txn_key(location: &str, file: &str, txn: u64) -> Key {
        if txn == 0 {
            Key::File(file.to_string(), 0)
        } else {
            Key::Dir(crate::core::wal::dir_key(location), txn)
        }
    }

    fn insert(key: Key, error: io::Error) {
        let mut map = FAILED.lock();
        let map = map.get_or_insert_with(HashMap::new);
        if map.insert(key, error).is_none() {
            COUNT.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn remove(map: &mut HashMap<Key, io::Error>, key: &Key) -> Option<io::Error> {
        let taken = map.remove(key);
        if taken.is_some() {
            COUNT.fetch_sub(1, Ordering::AcqRel);
        }
        taken
    }

    fn reported(e: io::Error) -> io::Error {
        io::Error::new(e.kind(), format!("append failed and was rolled back: {e}"))
    }

    /// Records a failed write of `file` in `location` under `txn`: the commit
    /// of `txn` through any file of the directory reports it (`txn` 0: the
    /// next commit of `file` with token 0).
    pub(crate) fn record(location: &str, file: &str, txn: u64, error: io::Error) {
        insert(txn_key(location, file, txn), error);
    }

    /// Records a failed staged append of `file`.
    pub(super) fn record_staged(file: &str, error: io::Error) {
        insert(Key::File(file.to_string(), STAGED), error);
    }

    /// Records a gate failure on a write of `file` with no transaction in
    /// `location`: the next commit of any file in the directory reports it.
    /// That commit may be another table's statement, so the error names the
    /// file whose write was skipped, and says so: otherwise an operator reads
    /// the failure as the committing table's.
    pub(crate) fn record_gate(location: &str, file: &str, error: io::Error) {
        let named = io::Error::new(
            error.kind(),
            format!(
                "a write to {file} outside any transaction was skipped (reported by the next commit in its \
                 directory, whichever table that is): {error}"
            ),
        );
        insert(Key::Dir(crate::core::wal::dir_key(location), GATE), named);
    }

    /// The failure a commit of `token` through `file` in `location` must
    /// report: one recorded under its transaction, by any file of the
    /// directory, or else the directory's gate failure.
    pub(super) fn take(location: &str, file: &str, token: u64) -> Option<io::Error> {
        if COUNT.load(Ordering::Acquire) == 0 {
            return None;
        }
        let mut guard = FAILED.lock();
        let map = guard.as_mut()?;
        let taken = remove(map, &txn_key(location, file, token))
            .or_else(|| remove(map, &Key::Dir(crate::core::wal::dir_key(location), GATE)));
        taken.map(reported)
    }

    /// A failed staged append of `file`, for the commit that lands staged
    /// bytes.
    pub(super) fn take_staged(file: &str) -> Option<io::Error> {
        if COUNT.load(Ordering::Acquire) == 0 {
            return None;
        }
        let taken = remove(FAILED.lock().as_mut()?, &Key::File(file.to_string(), STAGED));
        taken.map(reported)
    }

    /// Test-only: how many failures are recorded for `location`'s directory
    /// or for files in it.
    #[cfg(test)]
    pub(crate) fn pending_in(location: &str) -> usize {
        let dir = crate::core::wal::dir_key(location);
        FAILED.lock().as_ref().map_or(0, |m| {
            m.keys()
                .filter(|k| match k {
                    Key::Dir(d, _) => *d == dir,
                    Key::File(f, _) => std::path::Path::new(f).parent() == Some(std::path::Path::new(&dir)),
                })
                .count()
        })
    }

    /// The directory-wide gate record names the file whose write was
    /// skipped, so a commit of another table that reports it does not read as
    /// that table's failure.
    #[test]
    fn a_gate_failure_reported_by_another_file_names_the_skipped_one() {
        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().to_string_lossy().into_owned();
        record_gate(&location, "orders_pointers.db", io::Error::other("DIRTY could not be made durable"));
        let reported = take(&location, "customers_pointers.db", 0).expect("the next commit in the directory reports it");
        let text = reported.to_string();
        assert!(text.contains("orders_pointers.db"), "{text}");
        assert!(text.contains("DIRTY could not be made durable"), "{text}");
        assert!(take(&location, "customers_pointers.db", 0).is_none(), "reported twice");
    }

    /// Forgets what `token`'s transaction recorded: it is being aborted, so
    /// nothing will commit it, and a record left behind would sit in the map
    /// for good (every later commit then paying the lock).
    pub(super) fn discard(location: &str, token: u64) {
        if token == 0 || COUNT.load(Ordering::Acquire) == 0 {
            return;
        }
        if let Some(map) = FAILED.lock().as_mut() {
            let _ = remove(map, &Key::Dir(crate::core::wal::dir_key(location), token));
        }
    }

    #[cfg(test)]
    thread_local! {
        /// Makes the next append on this thread write only this many bytes
        /// and then fail.
        pub(crate) static SHORT_APPEND: std::cell::Cell<Option<usize>> =
            const { std::cell::Cell::new(None) };

        /// Makes the next log-less in-place write on this thread write only
        /// this many bytes and then fail.
        pub(crate) static SHORT_INPLACE: std::cell::Cell<Option<usize>> =
            const { std::cell::Cell::new(None) };

        /// Skips the undo image, for measuring what it costs.
        pub(crate) static NO_UNDO: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    /// Directories whose next in-place write fails as a refused write
    /// does: nothing written, the failure taken by the commit. One entry per
    /// failure, keyed by directory so it reaches a write on any thread.
    #[cfg(test)]
    pub(crate) static FAIL_INPLACE_IN: parking_lot::Mutex<Vec<String>> = parking_lot::Mutex::new(Vec::new());

    /// File names whose next append fails as a refused write does: nothing
    /// written, the failure taken by the commit. One entry per failure,
    /// matched against the end of the file's path.
    #[cfg(test)]
    pub(crate) static FAIL_APPEND_TO: parking_lot::Mutex<Vec<String>> = parking_lot::Mutex::new(Vec::new());

    #[cfg(test)]
    pub(super) fn injected_append_failure(file: &str) -> bool {
        let mut armed = FAIL_APPEND_TO.lock();
        match armed.iter().position(|name| file.ends_with(&format!("/{name}"))) {
            Some(i) => {
                armed.swap_remove(i);
                true
            }
            None => false,
        }
    }

    #[cfg(test)]
    pub(super) fn injected_inplace_failure(location: &str) -> bool {
        let mut armed = FAIL_INPLACE_IN.lock();
        match armed.iter().position(|l| l == location) {
            Some(i) => {
                armed.swap_remove(i);
                true
            }
            None => false,
        }
    }
}

/// What one round of the background landing did.
enum LandRound {
    /// Nothing was staged.
    Idle,
    /// The gate word refused the landing's hold this round; the bytes stay
    /// staged.
    Refused,
    /// Staged bytes were landed (or another landing held the block).
    Landed,
}

/// The atomics here are packed, not each in its own `CacheGuard`.
///
/// Padded, the eight of them were 1 KB of the struct's 1280 bytes, and a table
/// holds five of these, so 5 KB per open table went to padding. A read touches
/// several of them in a row (`append_blocked`, `file_len`, `temp_file_len`),
/// which is the shape the padding measurement said loses (see the memory note
/// on cache padding). Packed it is 240 bytes; `examples/storage_padding_bench`
/// measured no difference in read throughput either way, with or without a
/// writer bumping the staged-length counters — the read path's cost is the
/// shared `append_file` lock, not these.
pub struct LocalStorageProvider {
    pub(super) append_file: Arc<RwLock<std::fs::File>>,
    pub(super) write_file: Arc<RwLock<std::fs::File>>,
    // Dedicated read-only handles so read paths avoid lock contention.
    pub(super) read_file: RcArc<std::fs::File>,
    pub(crate) location: String,
    pub(crate) table_name: String,
    pub(crate) file_str: String,
    // temp staging file used for append_data()
    pub(crate) temp_file_str: String,
    pub(super) temp_file: Arc<RwLock<std::fs::File>>,
    pub(super) temp_read_file: RcArc<std::fs::File>,
    // In-memory mirror of staged temp bytes (fast path for reads).
    pub(super) temp_mem: RwLock<Vec<u8>>,
    pub(super) temp_mem_len: AtomicU64,
    // length of staged temp file bytes, maintained atomically (no metadata in read paths)
    pub(super) temp_file_len: AtomicU64,
    /// Logical file length — and, for `append_data_txn`, the **allocator**:
    /// each append takes its offset from `file_len.fetch_add(len)`.
    ///
    /// It therefore *leads* the on-disk length by construction. **Never refresh
    /// it from `metadata()`.** Doing so is not a correction but a race against
    /// the allocator: an append that has written its bytes but not yet claimed
    /// its offset leaves the file momentarily longer than `file_len`, so a
    /// metadata-derived `fetch_max` raises it and the append's own `fetch_add`
    /// then double-counts — the length ends one record too long and that append
    /// takes an offset that overlaps its predecessor.
    ///
    /// That is a real bug that has been in this file, not a hypothetical. It
    /// reproduced at 2 runs in 12 in release, isolated, which is exactly the
    /// rate that gets written off as flake. The trap is that the *staged*
    /// append path (`append_data(.., immediate = false)`, writing to the `.tmp`
    /// sidecar) genuinely does let the on-disk length lag, so the two append
    /// paths look interchangeable at the call site and disagree about whether
    /// the disk is authoritative. It is not, here.
    ///
    /// To make an in-place write's extension visible, use [`Self::inplace_end`],
    /// which derives the target from the write rather than from the file.
    pub file_len: AtomicU64,
    /// Highest end offset (`position + len`) an in-place write in flight needs
    /// the file to reach, raised at commit once the bytes are actually applied.
    ///
    /// The apply in `commit_txn` writes by path and bypasses this provider, so
    /// `file_len` would otherwise never learn that an in-place write extended
    /// the file. Deriving the target from the *write* rather than from
    /// `metadata()` is what keeps it safe: `append_data_txn` treats `file_len`
    /// as the allocator — it hands out each append's offset from `fetch_add` —
    /// so a value read back off disk can race an append that has written its
    /// bytes but not yet claimed its offset, and double-count. A write's own
    /// `position + len` cannot outrun the allocator, because a provider that
    /// appends only ever overwrites *within* the length it already handed out.
    pub(super) inplace_end: AtomicU64,
    // Memory map published with ArcSwap and owned by Arc to allow thread-local caches to own a handle safely.
    pub(crate) _memory_map: Arc<ArcSwap<Mmap>>,
    // Monotonic generation counter; bumped whenever the mmap is refreshed.
    // Used to cheaply invalidate per-thread cached mmap handles.
    pub(crate) mmap_gen: AtomicU64,
    /// Identifies this provider's mapping in the per-thread cache, for the life
    /// of the process. See [`next_mmap_key`].
    pub(crate) mmap_key: u64,
    pub(crate) hash: u32,
    _locked: AtomicBool,
    // true when there is data in temp_file waiting to be flushed
    temp_has_data: AtomicBool,
    // used to temporarily block appends during flush
    append_blocked: AtomicBool,
    /// Shared across clones so all handles to a file commit as one group.
    commit: Arc<CommitState>,
}

unsafe impl Sync for LocalStorageProvider {}
unsafe impl Send for LocalStorageProvider {}

impl Clone for LocalStorageProvider {
    fn clone(&self) -> Self {
        let map = Arc::new(unsafe {
            MmapOptions::new()
                .map(&std::fs::File::open(&self.file_str).unwrap())
                .unwrap()
        });

        let read_file = RcArc::new(
            std::fs::File::options()
                .read(true)
                .open(&self.file_str)
                .unwrap(),
        );
        let temp_read_file = RcArc::new(
            std::fs::File::options()
                .read(true)
                .open(&self.temp_file_str)
                .unwrap(),
        );

        Self {
            append_file: self.append_file.clone(),
            write_file: self.write_file.clone(),
            read_file,
            location: self.location.clone(),
            table_name: self.table_name.clone(),
            file_str: self.file_str.clone(),
            temp_file_str: self.temp_file_str.clone(),
            temp_file: self.temp_file.clone(),
            temp_read_file,
            temp_mem: RwLock::new(self.temp_mem.read().clone()),
            temp_mem_len: AtomicU64::new(self.temp_mem_len.load(Ordering::Acquire)).into(),
            temp_file_len: AtomicU64::new(self.temp_file_len.load(Ordering::Acquire)).into(),
            file_len: AtomicU64::new(self.file_len.load(std::sync::atomic::Ordering::SeqCst))
                .into(),
            inplace_end: AtomicU64::new(self.inplace_end.load(std::sync::atomic::Ordering::SeqCst))
                .into(),
            _memory_map: Arc::new(arc_swap::ArcSwap::new(map)),
            mmap_gen: AtomicU64::new(1).into(),
            mmap_key: next_mmap_key(),
            hash: CRC.checksum(format!("{}+++{}", self.location, self.table_name).as_bytes()),
            _locked: AtomicBool::new(false).into(),
            temp_has_data: AtomicBool::new(self.temp_has_data.load(Ordering::Relaxed)).into(),
            append_blocked: AtomicBool::new(false).into(),
            commit: self.commit.clone(),
        }
    }
}

struct ByteVecAllocator;

impl PoolAllocator<Vec<u8>> for ByteVecAllocator {
    #[inline]
    fn allocate(&self) -> Vec<u8> {
        Vec::with_capacity(1024 * 1024)
    }

    #[inline]
    fn reset(&self, obj: &mut Vec<u8>) {
        obj.clear();
        // A pooled buffer that once staged a large append would otherwise keep
        // that capacity for the life of the process — up to the pool's 64
        // slots of it. Back to the size `allocate` hands out.
        if obj.capacity() > STAGING_KEEP_CAPACITY {
            obj.shrink_to(1024 * 1024);
        }
    }
}

/// Capacity a staging buffer may keep once emptied. Above it, the buffer is
/// shrunk: one bulk load staging hundreds of megabytes must not pin them for
/// every later small append.
const STAGING_KEEP_CAPACITY: usize = 4 * 1024 * 1024;

static BYTE_VEC_POOL: OnceLock<Pool<ByteVecAllocator, Vec<u8>>> = OnceLock::new();

#[inline]
fn byte_vec_pool() -> &'static Pool<ByteVecAllocator, Vec<u8>> {
    BYTE_VEC_POOL.get_or_init(|| Pool::new(64, ByteVecAllocator))
}

// Remapping a growing file can be expensive. We remap lazily only when the mmap coverage
// falls behind the logical file length by a meaningful amount.
const REMAP_GROWTH_THRESHOLD: u64 = 8 * 1024 * 1024;

thread_local! {
    // (provider_key, generation, cached mmap)
    // IMPORTANT: this must be keyed per provider; otherwise different providers would
    // incorrectly share a cached mmap within the same thread.
    static TLS_MMAP_CACHE: RefCell<(u64, u64, Option<Arc<Mmap>>)> = RefCell::new((0, 0, None));
}

/// Hands each provider a key that is never reused.
///
/// The per-thread cache above used to key on `Arc::as_ptr(&self._memory_map)`,
/// which is an address — and an address is only unique among the objects alive
/// at one moment. Drop a provider and build another and the allocator hands
/// back the same block, so the new provider looked up the old one's entry; with
/// `mmap_gen` at 1 on both, as it is on every freshly-opened file, the
/// generation did not tell them apart either and the read was served from the
/// *previous file's* mapping.
///
/// What that looks like from outside is a file whose length is right and whose
/// bytes belong to a different file. `STORAGE MOVE TABLE` was the clearest
/// case, because it opens a table's files one after another and drops each
/// before opening the next: a moved table arrived with its CHECK-constraint
/// sidecar full of the field sidecar's bytes and the constraint silently
/// stopped being enforced. Anything that opens files in sequence on one thread
/// was exposed.
///
/// A counter costs the same to read as the pointer did and cannot repeat.
fn next_mmap_key() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

impl LocalStorageProvider {
    pub async fn new(location: &str, table_name: Option<&str>) -> LocalStorageProvider {
        if let Some(table_name) = table_name {
            let delimiter = if cfg!(unix) {
                "/"
            } else if cfg!(windows) {
                "\\"
            } else {
                panic!("OS not supported");
            };

            let location_str = format!("{}", location);

            let location_path = Path::new(&location_str);

            if !location_path.exists() {
                _ = std::fs::create_dir_all(location_path);
            }

            // Trim any trailing separators. The previous form sliced off the
            // last character and stripped it, which turned "/data/db/" into an
            // empty string — so files were created at the filesystem root and
            // the constructor panicked.
            let final_location = location.trim_end_matches(delimiter).to_string();

            let file_str = format!("{}{}{}", final_location, delimiter, table_name);

            let file_path = Path::new(&file_str);

            if !file_path.exists() && !file_path.is_file() {
                _ = std::fs::File::create(&file_str).unwrap();
                let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&file_str));
                // Creating a file writes a directory entry, which is only
                // durable once the directory itself is flushed. Without this a
                // crash can lose the file even though its contents were synced.
                let _ = crate::core::wal::sync_parent_dir(file_path);
            }

            let file_append = std::fs::File::options()
                .read(true)
                .append(true)
                .open(&file_str)
                .unwrap();

            let file_write = std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .unwrap();

            let file_read_mmap = std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .unwrap();

            let read_file =
                RcArc::new(std::fs::File::options().read(true).open(&file_str).unwrap());

            // Ensure temp staging file exists next to main file
            let temp_file_str = format!("{}.tmp", file_str);

            if !Path::new(&temp_file_str).exists() {
                _ = std::fs::File::create(&temp_file_str).unwrap();
                let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&temp_file_str));
            }

            let temp_file = std::fs::File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(&temp_file_str)
                .unwrap();

            let temp_read_file = RcArc::new(
                std::fs::File::options()
                    .read(true)
                    .open(&temp_file_str)
                    .unwrap(),
            );

            #[cfg(not(test))]
            let temp_len = std::fs::metadata(&temp_file_str)
                .map(|m| m.len())
                .unwrap_or(0);

            #[cfg(test)]
            let temp_len = {
                // If we're operating under the system temp directory (typical for tests),
                // ensure temp file is clean; keep main file intact to avoid nuking freshly written data.
                let sys_tmp = std::env::temp_dir().to_string_lossy().to_string();

                // Determine temp length; under system temp dir, truncate temp as well.
                let temp_len = if final_location.starts_with(&sys_tmp) {
                    let _ = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&temp_file_str)
                        .and_then(|f| {
                            f.set_len(0)?;
                            Ok(())
                        });
                    0u64
                } else {
                    std::fs::metadata(&temp_file_str)
                        .map(|m| m.len())
                        .unwrap_or(0)
                };

                temp_len
            };

            let map = Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            })));

            let file_len = file_read_mmap.metadata().unwrap().len();

            let local_storage_provider = LocalStorageProvider {
                append_file: Arc::new(RwLock::new(file_append)),
                write_file: Arc::new(RwLock::new(file_write)),
                read_file,
                location: final_location.to_string(),
                table_name: table_name.to_string(),
                file_str: file_str.clone(),
                temp_file_str,
                temp_file: Arc::new(RwLock::new(temp_file)),
                temp_read_file,
                temp_mem: RwLock::new(Vec::new()),
                temp_mem_len: AtomicU64::new(0).into(),
                file_len: AtomicU64::new(file_len).into(),
                inplace_end: AtomicU64::new(0).into(),
                _memory_map: map,
                mmap_gen: AtomicU64::new(1).into(),
                mmap_key: next_mmap_key(),
                hash: CRC.checksum(format!("{}+++{}", final_location, table_name).as_bytes()),
                _locked: AtomicBool::new(false).into(),
                temp_file_len: AtomicU64::new(temp_len).into(),
                temp_has_data: AtomicBool::new(false).into(),
                append_blocked: AtomicBool::new(false).into(),
                commit: Default::default(),
            };

            // If there are staged bytes in temp file, flush them into the main file.
            // This must run even when the main file is empty; otherwise bytes can be stranded
            // in the temp file across restarts and readers will observe an empty main file.
            if temp_len > 0 {
                let _ = local_storage_provider.flush_temp_to_main().await;
            }

            local_storage_provider.register_with_wal();
            local_storage_provider
        } else {
            let delimiter = if cfg!(unix) {
                "/"
            } else if cfg!(windows) {
                "\\"
            } else {
                panic!("OS not supported");
            };

            // Same trailing-separator fix as above.
            let location_str = location.trim_end_matches(delimiter).to_string();

            let location_path = Path::new(&location_str);

            if !location_path.exists() {
                _ = std::fs::create_dir_all(location_path);
            }

            let file_str = format!("{}{}{}", location_str, delimiter, "CONFIG_TABLE.db");

            _ = std::fs::File::create(&file_str).unwrap();
            let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&file_str));

            let file_append = std::fs::File::options()
                .read(true)
                .append(true)
                .open(&file_str)
                .unwrap();

            let file_write = std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .unwrap();

            let file_read_mmap = std::fs::File::options()
                .read(true)
                .write(true)
                .open(&file_str)
                .unwrap();

            let read_file =
                RcArc::new(std::fs::File::options().read(true).open(&file_str).unwrap());

            // Ensure temp staging file exists next to main file
            let temp_file_str = format!("{}.tmp", file_str);
            if !Path::new(&temp_file_str).exists() {
                _ = std::fs::File::create(&temp_file_str).unwrap();
                let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&temp_file_str));
            }
            let temp_file = std::fs::File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(&temp_file_str)
                .unwrap();

            let temp_read_file = RcArc::new(
                std::fs::File::options()
                    .read(true)
                    .open(&temp_file_str)
                    .unwrap(),
            );
            #[cfg(not(test))]
            let temp_len = std::fs::metadata(&temp_file_str)
                .map(|m| m.len())
                .unwrap_or(0);

            #[cfg(test)]
            let temp_len = {
                // If we're operating under the system temp directory (typical for tests),
                // ensure temp file is clean; keep main file intact to avoid nuking freshly written data.
                let sys_tmp = std::env::temp_dir().to_string_lossy().to_string();

                // Determine temp length; under system temp dir, truncate temp as well.
                let temp_len = if location_str.starts_with(&sys_tmp) {
                    let _ = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&temp_file_str)
                        .and_then(|f| {
                            f.set_len(0)?;
                            Ok(())
                        });
                    0u64
                } else {
                    std::fs::metadata(&temp_file_str)
                        .map(|m| m.len())
                        .unwrap_or(0)
                };

                temp_len
            };

            let map = Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            })));

            let file_len = file_read_mmap.metadata().unwrap().len();

            let local_storage_provider = LocalStorageProvider {
                append_file: Arc::new(RwLock::new(file_append)),
                write_file: Arc::new(RwLock::new(file_write)),
                read_file,
                location: location_str.to_string(),
                // The name the log keys this file by: it must be the file this
                // provider writes, or its records would carry `temp.db`'s id.
                table_name: "CONFIG_TABLE.db".to_string(),
                file_str: file_str.clone(),
                temp_file_str,
                temp_file: Arc::new(RwLock::new(temp_file)),
                temp_read_file,
                temp_mem: RwLock::new(Vec::new()),
                temp_mem_len: AtomicU64::new(0).into(),
                file_len: AtomicU64::new(file_len).into(),
                inplace_end: AtomicU64::new(0).into(),
                _memory_map: map,
                mmap_gen: AtomicU64::new(1).into(),
                mmap_key: next_mmap_key(),
                hash: CRC.checksum(format!("{}+++{}", location_str, "CONFIG_TABLE.db").as_bytes()),
                _locked: AtomicBool::new(false).into(),
                temp_file_len: AtomicU64::new(temp_len).into(),
                temp_has_data: AtomicBool::new(false).into(),
                append_blocked: AtomicBool::new(false).into(),
                commit: Default::default(),
            };

            if temp_len > 0 {
                let _ = local_storage_provider.flush_temp_to_main().await;
            }

            local_storage_provider.register_with_wal();
            local_storage_provider
        }
    }

    pub async fn close_files(&self) {
        let file_read = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .unwrap();

        let file_append = std::fs::File::options()
            .read(true)
            .append(true)
            .open(&self.file_str)
            .unwrap();

        let file_write = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&self.file_str)
            .unwrap();

        _ = file_read.sync_all();
        _ = file_append.sync_all();
        _ = file_write.sync_all();
    }

    /// One round of the background landing (`start_service`'s loop, or the
    /// shared scheduler's `land_background`): lands staged bytes if there
    /// are any.
    async fn land_round(&self) -> LandRound {
        if !self.temp_has_data.load(Ordering::Acquire) {
            return LandRound::Idle;
        }
        // A landing is a write of the file set: it holds a count on the gate
        // word while it runs, taken by the ticket's fast path. When that
        // would need the slow path (the log ends `CLEAN`, a seal is
        // publishing), the bytes stay staged, readable from the mirror, until
        // the next round.
        let _landing = match self.commit.gated.word().map(|w| w.try_acquire()) {
            Some(Err(_)) => return LandRound::Refused,
            Some(Ok(hold)) => Some(hold),
            None => None,
        };
        let res = self.flush_temp_to_main().await; // ignore errors for now
        if res.is_err() {
            error!("Error flushing temp to main: {:?}", res.err());
        }
        LandRound::Landed
    }

    /// Moves every staged byte into the main file and makes it durable,
    /// waiting out a flush another task is already running.
    async fn land_staged_durably(&self) -> io::Result<()> {
        loop {
            if self.temp_file_len.load(Ordering::Acquire) == 0
                && !self.append_blocked.load(Ordering::Acquire)
            {
                break;
            }
            if self.append_blocked.load(Ordering::Acquire) {
                yield_now().await;
                continue;
            }
            // `Ok(0)` when another flush took the block first; loop and wait.
            self.flush_temp_to_main().await?;
        }
        // The flush synced the file's data; the entry naming a newly created
        // file is only durable once its directory is.
        crate::core::wal::sync_parent_dir(std::path::Path::new(&self.file_str))?;
        Ok(())
    }

    /// Writes staged bytes onto the end of the main file, under its lock.
    ///
    /// A write that fails part-way is cut back off. The main file is opened
    /// for append, so bytes left past `file_len` would put the next append's
    /// bytes after them while it reports `file_len` as their offset. The
    /// staged copy is untouched either way, so the flush can be retried.
    fn land_into_main(&self, main: &mut std::fs::File, bytes: &[u8]) -> io::Result<()> {
        let at = self.file_len.load(Ordering::Acquire);
        if let Err(e) = main.write_all(bytes).and_then(|()| main.flush()) {
            if let Err(cut) = main.set_len(at) {
                let _ = self.commit.poison(format!(
                    "landing staged bytes in {} failed ({}) and could not be cut back: {}",
                    self.file_str, e, cut
                ));
            }
            return Err(e);
        }
        Ok(())
    }

    // Move any staged bytes from temp file into the main file atomically with respect to appends.
    async fn flush_temp_to_main(&self) -> io::Result<usize> {
        // Try to acquire the append block; if already blocked, another flush is running.
        if self
            .append_blocked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Ok(0);
        }

        // Always clear the block at the end
        struct Reset<'a>(&'a AtomicBool);

        impl<'a> Drop for Reset<'a> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }

        let _reset = Reset(&self.append_blocked);

        // Read the staged length; it is NOT cleared here. Clearing it up
        // front made the logical length — `file_len + temp_file_len`, which is
        // what `get_len` reports and what a scan turns into a slot count —
        // DIP for the whole duration of the write and its fsync below: the
        // staged rows had stopped counting while the main file had not yet
        // grown. A scan in that window saw fewer slots than exist and simply
        // did not reach the newest version of a row, so a reader could miss a
        // row that was merely being updated, and a writer could land on the
        // older version and report a serialization conflict against a
        // transaction that had already finished. Appends are blocked for the
        // whole of this function, so nothing can add to the staged length
        // while it is being published.
        let len = self.temp_file_len.load(Ordering::Acquire) as usize;

        if len == 0 {
            // nothing to do
            self.temp_has_data.store(false, Ordering::Release);
            return Ok(0);
        }
        self.commit.gated.note_write(&self.location, &self.table_name);

        // The sync's outcome, reported only after the bytes are published: they
        // are in the main file either way, and leaving them unpublished would
        // have the next flush write them a second time.
        let mut synced: io::Result<()> = Ok(());

        // Prefer writing from the in-memory staging buffer (if it fully covers the staged range).
        let mut wrote_from_mem = false;
        if self.temp_mem_len.load(Ordering::Acquire) == len as u64 {
            let mem = self.temp_mem.write();
            if mem.len() == len {
                let mut main = self.append_file.write();
                self.land_into_main(&mut main, &mem)?;
                crate::core::sync_audit::note("file.data", Path::new(&self.file_str));
                synced = main.sync_all();
                // The mirror is deliberately NOT cleared yet. Until the length
                // is published below, a read of these offsets is still served
                // from the staged copy; dropping it here would leave the bytes
                // reachable from neither place.
                wrote_from_mem = true;
            }
        }

        if !wrote_from_mem {
            let mut buf = byte_vec_pool().get();
            let cap = buf.capacity();
            if cap < len {
                buf.reserve(len - cap);
            }
            buf.resize(len, 0u8);

            // Read all staged bytes from the temp file.
            // NOTE: we keep using the locked temp handle to avoid cursor races with staging writes.
            {
                let mut temp = self.temp_file.write();
                temp.seek(SeekFrom::Start(0))?;
                temp.read_exact(&mut buf[..])?;
            }

            let mut main = self.append_file.write();
            self.land_into_main(&mut main, &buf[..])?;
            crate::core::sync_audit::note("file.data", Path::new(&self.file_str));
            synced = main.sync_all();

            // As above: the mirror is dropped after the length is published,
            // not before.
        }

        // The map has to cover the bytes before the length that describes them
        // is published, or a scan is told about slots the map cannot serve.
        self.update_memory_map();

        // Publish. The two counters move together under the append lock, which
        // `get_len` also takes, so no reader can observe the instant between
        // them — one order would dip and the other would overshoot, and both
        // are wrong for a scan that turns the number into a slot count.
        {
            let _publish = self.append_file.write();
            self.file_len.fetch_add(len as u64, Ordering::Release);
            self.temp_file_len.fetch_sub(len as u64, Ordering::Release);
        }

        // Only now is the staged copy redundant: these offsets are served from
        // the main file, and the length says so.
        {
            let mut mem = self.temp_mem.write();
            mem.clear();
            if mem.capacity() > STAGING_KEEP_CAPACITY {
                mem.shrink_to(STAGING_KEEP_CAPACITY / 4);
            }
        }
        self.temp_mem_len.store(0, Ordering::Release);
        {
            let mut temp = self.temp_file.write();
            temp.seek(SeekFrom::Start(0))?;
            temp.set_len(0)?;
            let _ = temp.flush();
        }

        // Clear pending flag
        self.temp_has_data.store(false, Ordering::Release);
        synced?;
        Ok(len)
    }

    fn update_memory_map(&self) {
        // Reuse the dedicated read-only handle; avoids repeated open/close overhead.
        let file = &*self.read_file;

        // Acquire a simple spin lock to serialize remapping
        while self
            ._locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }

        // Create new memory map with the current file size
        self._memory_map
            .store(Arc::new(unsafe { MmapOptions::new().map(file).unwrap() }));

        // Bump generation to invalidate thread-local caches.
        // Acquire/Release pairing ensures that if a thread observes a new generation,
        // it also observes the newly stored mmap.
        self.mmap_gen.fetch_add(1, Ordering::Release);

        self._locked.store(false, Ordering::Release);
    }

    #[inline(always)]
    fn with_cached_mmap<R>(&self, f: impl FnOnce(&Mmap) -> R) -> R {
        let provider_key = self.mmap_key;
        let generation = self.mmap_gen.load(Ordering::Acquire);
        TLS_MMAP_CACHE.with(|cell| {
            let mut state = cell.borrow_mut();
            if state.2.is_none() || state.0 != provider_key || state.1 != generation {
                // Clone the Arc<Mmap> behind ArcSwap's guard and keep it in TLS.
                state.0 = provider_key;
                state.1 = generation;
                state.2 = Some(Arc::clone(&*self._memory_map.load()));
            }
            let mmap_arc = state.2.as_ref().unwrap();
            f(&*mmap_arc)
        })
    }

    /// This file's id within its database directory, as the log records it.
    #[inline]
    fn wal_file_id(&self) -> u64 {
        crate::core::wal::file_id_for_name(&self.table_name)
    }

    /// Makes a write durable without a log: flush this file, group-committed.
    ///
    /// The failure is recorded rather than returned — the append paths that
    /// call this cannot report one — and surfaces from `commit_durable`, which
    /// every statement runs before it reports success.
    ///
    /// Recorded against the directory as well as the file — see
    /// [`FAILED_DIRECTORIES`] for why the file alone is not enough — unless
    /// the file had been unlinked, which loses nothing a sibling could name.
    #[inline]
    fn flush_self(&self, seq: u64) {
        if let Err(e) = self.commit.flush_through(seq, &self.file_str)
            && self.commit.poison_concerns_directory()
        {
            record_directory_failure(&self.location, e.to_string());
        }
    }

    /// Makes this file's next flush fail. Tests only.
    #[cfg(test)]
    pub(crate) fn fail_next_flush_for_tests(&self) {
        self.commit.fail_next_flush.store(true, Ordering::Release);
    }

    /// The reason this directory's log is poisoned, if it has one and it is.
    fn wal_poison_reason(&self) -> Option<String> {
        crate::core::wal::with_wal_for(&self.location, |w| {
            w.filter(|w| w.is_poisoned())
                .map(|w| w.poison_reason().unwrap_or_else(|| "reason not recorded".into()))
        })
    }

    /// Publishes this file so a replay can find it by path.
    ///
    /// Fails only when another file's name has the same log id. The log then
    /// refuses every record under that id, for both files: appends fall back
    /// to flushing the file directly, each saying so, and in-place writes
    /// fail. Said here as well, because the constructors cannot return it.
    #[inline]
    fn register_with_wal(&self) {
        crate::core::wal::with_wal_for(&self.location, |w| {
            if let Some(w) = w
                && let Err(e) = w.register_file(self.wal_file_id(), &self.file_str)
            {
                error!("{} is not covered by the WAL: {}", self.file_str, e);
            }
        });
    }

    #[inline(always)]
    fn load_file_len(&self) -> u64 {
        // Writers publish length with Release; use Acquire to avoid seeing stale lengths
        // that can produce spurious UnexpectedEof under concurrency.
        self.file_len.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn is_locked(&self) -> bool {
        self._locked.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn temp_mem_copy_region(&self, temp_off: u64, dst: &mut [u8]) -> usize {
        if dst.is_empty() {
            return 0;
        }

        let mem_len = self.temp_mem_len.load(Ordering::Acquire);
        if mem_len == 0 {
            return 0;
        }
        if temp_off >= mem_len {
            return 0;
        }

        let mem = self.temp_mem.read();
        let start = temp_off as usize;
        if start >= mem.len() {
            return 0;
        }
        let avail = mem.len() - start;
        let to_copy = dst.len().min(avail);
        dst[..to_copy].copy_from_slice(&mem[start..start + to_copy]);
        to_copy
    }

    // Single large mmap copy if possible. Returns bytes copied.
    #[inline(always)]
    fn mmap_copy_region(&self, pos: u64, buf: &mut [u8], file_len: u64) -> usize {
        self.with_cached_mmap(|mmap| {
            let mmap_len = mmap.len() as u64;
            if pos >= mmap_len {
                return 0;
            }
            // effective end limited by both mmap coverage and logical file length
            let max_span = (mmap_len - pos).min(file_len - pos);
            if max_span == 0 {
                return 0;
            }
            let to_copy = min(buf.len(), max_span as usize);
            let start = pos as usize;
            let end = start + to_copy;
            // bounds guaranteed by max_span
            buf[..to_copy].copy_from_slice(&mmap[start..end]);
            to_copy
        })
    }

    // Synchronous fast path attempt: returns bytes copied from mmap.
    fn read_from_mmap(&self, position: u64, buffer: &mut [u8], file_len: u64) -> usize {
        if buffer.is_empty() {
            return 0;
        }
        if position >= file_len {
            return 0;
        }

        // (1) Single mmap region copy first (most common path)
        self.mmap_copy_region(position, buffer, file_len)
    }

    #[inline(always)]
    async fn read_data_into_buffer_internal(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
    ) -> io::Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        // Avoid `await` in the hot read path; if a flush is in progress, do a
        // short spin. Flushes are expected to be rare relative to reads.
        while self.append_blocked.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }

        // BOTH lengths after the spin, and both under the publish lock.
        // Reading the main length *before* waiting for the flush and the
        // staged length *after* it meant a flush landing in between was
        // counted in neither: the main length was the one from before it
        // published and the staged length had already been cleared. The
        // logical end then came out short, and this returned `UnexpectedEof`
        // for a position that is well inside the file — which a scan reads as
        // the end of the table, so it stops early and the rows past that point
        // simply do not appear. A reader could miss a row that exists while
        // another connection was updating it.
        let (main_len, temp_len) = {
            let _publish = self.append_file.read();
            (
                self.load_file_len(),
                self.temp_file_len.load(Ordering::Acquire),
            )
        };
        let file_len = main_len + temp_len; // logical length

        // Immediate EOF if the requested position is beyond the logical end.
        // This ensures callers get UnexpectedEof and position remains unchanged.
        if *position >= file_len {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "Position beyond file end",
            ));
        }

        let total_len = buffer.len();
        let mut overall_copied = 0usize;

        // Phase 1: read from main file only (mmap + pread), clamped to main_len.
        if *position < main_len {
            let avail = main_len - *position;
            let phase1_target = min(avail as usize, total_len - overall_copied);
            let start_copied = overall_copied;

            // 1. Try mmap
            let mut mmap_copied = self.read_from_mmap(
                *position,
                &mut buffer[overall_copied..overall_copied + phase1_target],
                main_len,
            );
            overall_copied += mmap_copied;
            *position += mmap_copied as u64;

            // 1b. If mmap is stale (file extended past current mmap coverage),
            // refresh it once and retry. This avoids the very expensive
            // spawn_blocking pread fallback below — a single remap covers
            // millions of subsequent reads, while spawn_blocking incurs tokio
            // scheduler + parking_lot mutex overhead on every call.
            if overall_copied < start_copied + phase1_target {
                let mmap_len_now = self._memory_map.load().len() as u64;
                if main_len > mmap_len_now {
                    self.update_memory_map();
                    let still_needed = (start_copied + phase1_target) - overall_copied;
                    let extra = self.read_from_mmap(
                        *position,
                        &mut buffer[overall_copied..overall_copied + still_needed],
                        main_len,
                    );
                    overall_copied += extra;
                    *position += extra as u64;
                    mmap_copied += extra;
                }
            }
            let _ = mmap_copied;

            // 2. Fallback to synchronous pread (no spawn_blocking). pread is a
            // single syscall that returns immediately for in-cache pages; the
            // overhead of spawn_blocking (tokio blocking-pool mutex + condvar)
            // dwarfs the syscall itself. Briefly blocking the worker is fine
            // here because the only reason we get here is a true cache miss
            // beyond mmap coverage that the remap above couldn't fix.
            while overall_copied < start_copied + phase1_target {
                let needed = (start_copied + phase1_target) - overall_copied;
                let dst = &mut buffer[overall_copied..overall_copied + needed];

                #[cfg(unix)]
                let n = {
                    use std::os::unix::fs::FileExt;
                    self.read_file.read_at(dst, *position)?
                };
                #[cfg(windows)]
                let n = {
                    use std::os::windows::fs::FileExt;
                    self.read_file.seek_read(dst, *position)?
                };

                if n == 0 {
                    return Err(io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "Unexpected EOF in main file",
                    ));
                }

                overall_copied += n;
                *position += n as u64;
            }
        }

        // Phase 2: if logical length extends into temp, read remaining bytes from temp.
        if overall_copied < total_len {
            if *position >= main_len {
                let temp_pos = *position - main_len;
                let to_read = min((file_len - *position) as usize, total_len - overall_copied);

                // 1. Try memory mirror
                let copied = self.temp_mem_copy_region(
                    temp_pos,
                    &mut buffer[overall_copied..overall_copied + to_read],
                );
                overall_copied += copied;
                *position += copied as u64;

                // 2. Fallback to synchronous pread (no spawn_blocking).
                if copied < to_read {
                    let needed = to_read - copied;
                    let current_temp_pos = *position - main_len;
                    let dst = &mut buffer[overall_copied..overall_copied + needed];

                    #[cfg(unix)]
                    let n = {
                        use std::os::unix::fs::FileExt;
                        self.temp_read_file.read_at(dst, current_temp_pos)?
                    };
                    #[cfg(windows)]
                    let n = {
                        use std::os::windows::fs::FileExt;
                        self.temp_read_file.seek_read(dst, current_temp_pos)?
                    };

                    overall_copied += n;
                    *position += n as u64;
                }
            }
        }

        if overall_copied == total_len {
            Ok(())
        } else {
            Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "Requested {} bytes, got {} (reached logical EOF)",
                    total_len, overall_copied
                ),
            ))
        }
    }
}

impl StorageIO for LocalStorageProvider {
    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        // Bytes that cannot be read back are not verified; this used to panic.
        let mut file_buffer = vec![0; buffer.len() as usize];
        let read = std::fs::File::options()
            .read(true)
            .open(&self.file_str)
            .and_then(|mut f| {
                f.seek(SeekFrom::Start(position))?;
                f.read_exact(&mut file_buffer)
            });
        if let Err(e) = read {
            error!("verify of {} at {} could not read: {}", self.file_str, position, e);
            return false;
        }
        buffer.eq(&file_buffer)
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        let txn = self.begin_txn();
        let token = self.write_data_txn(txn, position, buffer).await;
        // `write_data` has no way to report this; callers that need to know use
        // the transactional form and commit it themselves.
        if let Err(e) = self.commit_durable(token).await {
            error!("in-place write to {} is not durable: {}", self.file_str, e);
        }
    }

    fn begin_txn(&self) -> u64 {
        // Borrowed, not cloned: see `wal::with_wal_for`.
        crate::core::wal::with_wal_for(&self.location, |w| w.map_or(0, |w| w.begin()))
    }

    fn defers_in_place_writes(&self) -> bool {
        // Logged, the write is applied by `commit_durable`; without a log it
        // goes straight to the file (see `write_data_txn`).
        crate::core::wal::wal_for(&self.location).is_some()
    }

    async fn write_data_txn(&self, txn: u64, position: u64, buffer: &[u8]) -> u64 {
        // Registered on the file set's word until the write returns (B12).
        // A gate failure (release only) skips the write and fails `txn`'s
        // commit.
        let _registered = gated!(self, Site::WriteInPlace, |e| {
            GatedIo::record_failure(&self.location, &self.table_name, Failure::Txn(txn), e);
            return txn;
        });
        #[cfg(test)]
        if failed_appends::injected_inplace_failure(&self.location) {
            failed_appends::record(&self.location, &self.file_str, txn, io::Error::other("injected in-place write failure"));
            return txn;
        }
        // Recorded now, applied to `file_len` at commit — see `inplace_end`.
        // Raising the length here instead would publish a length whose bytes
        // are not in the file yet, since the apply is deferred to commit.
        self.inplace_end
            .fetch_max(position + buffer.len() as u64, Ordering::AcqRel);
        let logged = crate::core::wal::with_wal_for(&self.location, |w| {
            let w = w?;
            let mut outcome =
                w.log_in_place_from(txn, self.wal_file_id(), position, buffer, self.commit.born);
            if outcome.is_err() {
                // The one failure that is not the log's: a file it has not
                // been told about yet. Registered and tried once more — unless
                // the id is another file's, which no retry changes.
                outcome = w
                    .register_file(self.wal_file_id(), &self.file_str)
                    .and_then(|()| {
                        w.log_in_place_from(txn, self.wal_file_id(), position, buffer, self.commit.born)
                    });
            }
            // The data file is deliberately untouched here; `commit_durable`
            // applies it once the record is durable. Overwriting first would
            // destroy the old bytes with no undo record to restore them.
            Some(outcome)
        });
        match logged {
            Some(Ok(_)) => return txn,
            Some(Err(e)) => {
                // A log exists and would not take the record. Writing the bytes
                // directly — what this used to do — overwrites them with no
                // record to finish or undo the write, outside the transaction
                // the caller thinks it is in. The transaction fails instead,
                // with the file untouched.
                error!("WAL in-place log for {} failed: {}", self.file_str, e);
                failed_appends::record(&self.location, &self.file_str, txn, e);
                return txn;
            }
            None => {}
        }

        // No log. The bytes being replaced are kept first — only the range
        // this write covers, so the cost is one read of what is written — and
        // put back if the write fails part-way, so a failed overwrite leaves
        // the old row rather than a mix of two.
        let written = {
            let mut file = self.write_file.write();
            Self::overwrite_with_undo(&mut file, position, buffer)
        };
        if let Err((e, restored)) = written {
            if let Err(restore) = restored {
                // Neither the new bytes nor the old ones: nothing here can be
                // trusted to be durable again.
                let _ = self.commit.poison(format!(
                    "in-place write to {} failed ({}) and the old bytes could not be put back: {}",
                    self.file_str, e, restore
                ));
            }
            // `commit_durable` takes the failure under the token returned —
            // `txn`, which is 0 when there is no log.
            error!("in-place write to {} failed: {}", self.file_str, e);
            failed_appends::record(&self.location, &self.file_str, txn, e);
            return txn;
        }
        self.flush_self(self.commit.issue());
        0
    }

    async fn append_data_txn(&self, txn: u64, buffer: &[u8]) -> (u64, u64) {
        // As `write_data_txn`: a skipped append reports the allocator's end,
        // which nothing may use, since `txn`'s commit fails.
        let _registered = gated!(self, Site::Append, |e| {
            GatedIo::record_failure(&self.location, &self.table_name, Failure::Txn(txn), e);
            return (self.file_len.load(Ordering::Acquire), txn);
        });
        #[cfg(test)]
        if failed_appends::injected_append_failure(&self.file_str) {
            failed_appends::record(&self.location, &self.file_str, txn, io::Error::other("injected append failure"));
            return (self.file_len.load(Ordering::Acquire), txn);
        }
        let mut file = self.append_file.write();
        // `file_len` is the append allocator, and this is where these bytes go.
        let at = self.file_len.load(Ordering::Acquire);
        #[cfg(test)]
        let written = match failed_appends::SHORT_APPEND.with(|h| h.take()) {
            Some(keep) => file
                .write_all(&buffer[..keep.min(buffer.len())])
                .and_then(|()| Err(io::Error::other("injected short append"))),
            None => file.write_all(buffer).and_then(|()| file.flush()),
        };
        #[cfg(not(test))]
        let written = file.write_all(buffer).and_then(|()| file.flush());
        if let Err(e) = written {
            // A write can fail part-way — a full disk, EIO — with some of the
            // bytes already in the file. This used to `unwrap()`: the panic
            // left the partial bytes behind and `file_len` short of them, so
            // the next append's offset no longer described where its bytes
            // landed. Cut back to the allocator's end instead, leave `file_len`
            // where it was, and fail the transaction at `commit_durable`.
            if let Err(cut) = file.set_len(at) {
                let _ = self.commit.poison(format!(
                    "append to {} failed ({}) and could not be cut back: {}",
                    self.file_str, e, cut
                ));
            }
            drop(file);
            error!("append to {} failed: {}", self.file_str, e);
            failed_appends::record(&self.location, &self.file_str, txn, e);
            return (at, txn);
        }
        let offset = self
            .file_len
            .fetch_add(buffer.len() as u64, Ordering::Release);
        let seq = self.commit.issue();
        let new_len = offset + buffer.len() as u64;

        // Release the append lock before any flush. Holding it across the flush
        // would serialize appends behind the device and leave nothing for the
        // shared commit to batch.
        drop(file);

        // A directory being bulk-loaded takes neither a log record nor a flush:
        // the loader syncs every file once at the end, and a load that dies
        // before then is discarded whole. See `core::bulk_load`.
        let token = if crate::core::bulk_load::is_active(&self.location) {
            0
        } else {
            crate::core::wal::with_wal_for(&self.location, |w| match w {
                Some(w) => match w.log_from(txn, self.wal_file_id(), offset, buffer, self.commit.born) {
                    Ok(_) => txn,
                    // The `0` this falls back to is refused by
                    // `commit_durable` when the failure poisoned the log.
                    Err(e) => {
                        error!(
                            "WAL append failed ({}); flushing {} directly",
                            e, self.file_str
                        );
                        self.flush_self(seq);
                        0
                    }
                },
                None => {
                    self.flush_self(seq);
                    0
                }
            })
        };

        let mmap_len = self._memory_map.load().len() as u64;
        if new_len > mmap_len + REMAP_GROWTH_THRESHOLD {
            self.update_memory_map();
        }

        (offset, token)
    }

    async fn commit_durable(&self, token: u64) -> io::Result<()> {
        self.commit_durable_with(token, true).await
    }

    async fn abort_txn(&self, token: u64) {
        if token == 0 {
            return;
        }
        // Nothing will commit it now; its failures go with it.
        failed_appends::discard(&self.location, token);
        crate::core::wal::with_wal_for(&self.location, |w| {
            if let Some(w) = w {
                w.abort_txn(token);
            }
        });
    }

    async fn truncate(&self, len: u64) -> io::Result<()> {
        let _registered = gated!(self, Site::Truncate, |e| return Err(e));
        // Staged appends live past the current end, so they are exactly the
        // bytes a truncation would drop. Land them first and let the caller see
        // a length that accounts for them, rather than silently discarding
        // writes someone has already been told succeeded.
        let _ = self.flush_temp_to_main().await;

        let current = self.load_file_len();
        if len > current {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("truncate to {} would grow a {}-byte file", len, current),
            ));
        }
        if len == current {
            return Ok(());
        }

        // Appends must not land past the new end while we shorten the file.
        while self
            .append_blocked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            yield_now().await;
        }
        struct Unblock<'a>(&'a AtomicBool);
        impl Drop for Unblock<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _unblock = Unblock(&self.append_blocked);

        // A fixed-record file's durable floor must never be above what it
        // holds, or a torn tail after this would refuse the next open: lowered
        // durably before the cut, and again after it, past any checkpoint that
        // sampled the old length in between (see `crate::core::torn_tail`).
        crate::core::wal::lower_floor_for(&self.location, Path::new(&self.file_str), len)?;
        {
            let file = self.write_file.write();
            file.set_len(len)?;
            crate::core::sync_audit::note("file.data", Path::new(&self.file_str));
            file.sync_all()?;
        }
        let _ = crate::core::wal::sync_parent_dir(Path::new(&self.file_str));
        if let Err(e) =
            crate::core::wal::lower_floor_for(&self.location, Path::new(&self.file_str), len)
        {
            log::error!("{}: could not lower its durable floor after a cut: {}", self.file_str, e);
        }

        self.file_len.store(len, Ordering::Release);

        // `inplace_end` is a high-water claim about where in-place writes have
        // reached, and `commit_durable` raises `file_len` to it. Truncation
        // makes any claim past the new end false, so leaving it would have the
        // *next* in-place commit restore the pre-truncation length: the provider
        // then reports a file longer than it is, and `RowPointerIterator` walks
        // records that do not exist. Measured — truncate to 1024 with a mark at
        // 4096, and the next commit reported 4096 against 1024 bytes on disk.
        //
        // `fetch_min` rather than `store`, for the same reason the other
        // direction uses `fetch_max`: a length claim must only ever move the way
        // the operation making it can justify.
        self.inplace_end.fetch_min(len, Ordering::AcqRel);
        // The old mapping still spans the discarded range; reading through it
        // after the file shrank is a SIGBUS, not a short read.
        self.update_memory_map();

        Ok(())
    }

    async fn append_data(&self, buffer: &[u8], immediate: bool) {
        let _ = self.append_data_returning_offset(buffer, immediate).await;
    }

    async fn append_data_returning_offset(&self, buffer: &[u8], immediate: bool) -> u64 {
        if immediate {
            let txn = self.begin_txn();
            let (offset, token) = self.append_data_txn(txn, buffer).await;
            if let Err(e) = self.commit_durable_with(token, false).await {
                error!("append to {} is not durable: {}", self.file_str, e);
            }
            return offset;
        }

        // A staged write is a write of the file set too (its `.tmp`). A gate
        // failure is reported by the next commit in the directory.
        let _registered = gated!(self, Site::Stage, |e| {
            GatedIo::record_failure(&self.location, &self.table_name, Failure::Untokened, e);
            return self.file_len.load(Ordering::Acquire) + self.temp_file_len.load(Ordering::Acquire);
        });

        // If a flush is ongoing, wait briefly until it completes to keep ordering simple.
        while self.append_blocked.load(Ordering::Acquire) {
            yield_now().await;
        }

        // Stage the file write, the in-memory mirror and the published length in
        // one critical section. Taking these separately let two concurrent
        // stagers land in one order in the temp file and the opposite order in
        // `temp_mem`, leaving the mirror disagreeing with the file it mirrors.
        //
        // Lock order is `temp_mem` then `temp_file`, matching the flush path
        // (which nests `temp_mem` -> `append_file` and never takes `temp_file`
        // while holding `temp_mem`).
        let mut mem = self.temp_mem.write();
        let mut tf = self.temp_file.write();

        // Placed by the staged length, not by seeking to the file's end: the
        // length is what reads, the flush and `temp_mem` all go by, and after
        // a failed write the file's end is not. Only appends move it, and they
        // hold both locks here.
        let staged_at = self.temp_file_len.load(Ordering::Acquire);
        #[cfg(test)]
        let written = match failed_appends::SHORT_APPEND.with(|h| h.take()) {
            Some(keep) => tf
                .seek(SeekFrom::Start(staged_at))
                .and_then(|_| tf.write_all(&buffer[..keep.min(buffer.len())]))
                .and_then(|()| Err(io::Error::other("injected short staged append"))),
            None => tf
                .seek(SeekFrom::Start(staged_at))
                .and_then(|_| tf.write_all(buffer)),
        };
        #[cfg(not(test))]
        let written = tf
            .seek(SeekFrom::Start(staged_at))
            .and_then(|_| tf.write_all(buffer));
        if let Err(e) = written {
            // A full disk or a file-size limit (EFBIG) can stop the write
            // part-way. This used to `unwrap()`, and the panic left the partial
            // bytes in the `.tmp` file while `temp_mem` and the staged length
            // did not count them — so the next append, which sought to the
            // file's end, landed after them in the file and at the right offset
            // in the mirror, and the two stopped agreeing. Nothing is counted
            // and nothing mirrored; the partial bytes are cut back off, and the
            // failure is reported by the `commit_durable` that lands staged
            // bytes, which every caller makes before calling the copy durable.
            if let Err(cut) = tf.set_len(staged_at) {
                let _ = self.commit.poison(format!(
                    "staged append to {} failed ({}) and could not be cut back: {}",
                    self.temp_file_str, e, cut
                ));
            }
            drop(tf);
            drop(mem);
            error!("staged append to {} failed: {}", self.temp_file_str, e);
            failed_appends::record_staged(&self.file_str, e);
            return self.file_len.load(Ordering::Acquire) + staged_at;
        }
        let _ = tf.flush();

        // Mirror staged bytes in memory for faster read-back.
        mem.extend_from_slice(buffer);

        let staged_before = self
            .temp_file_len
            .fetch_add(buffer.len() as u64, Ordering::Release);
        self.temp_mem_len
            .fetch_add(buffer.len() as u64, Ordering::Release);

        // Mark that we have staged data
        self.temp_has_data.store(true, Ordering::Release);

        drop(tf);
        drop(mem);

        // Logical offset = persisted main file + staged bytes ahead of us.
        self.file_len.load(Ordering::Acquire) + staged_before
    }

    fn try_borrow_range(&self, position: u64, len: usize) -> Option<super::traits::BorrowedBytes> {
        if len == 0 {
            return None;
        }

        // Staged writes live outside the mapping, so part of the range might
        // not be visible through it. Decline and let the caller copy.
        if self.temp_has_data.load(Ordering::Relaxed) {
            return None;
        }

        let main_len = self.load_file_len();
        let end = position.checked_add(len as u64)?;
        if end > main_len {
            return None;
        }

        // Hold our own handle to the mapping. If a concurrent append re-maps
        // the file, this Arc keeps the region we handed out alive.
        let map: std::sync::Arc<Mmap> = std::sync::Arc::clone(&*self._memory_map.load());
        if end > map.len() as u64 {
            return None;
        }

        let ptr = unsafe { map.as_ptr().add(position as usize) };
        // SAFETY: the range was just bounds-checked against the mapping, and
        // `map` keeps it mapped for as long as the guard lives.
        Some(unsafe { super::traits::BorrowedBytes::new(map, ptr, len) })
    }

    async fn get_len(&self) -> u64 {
        // Logical length = main persisted on disk + staged temp
        // Re-read filesystem metadata to avoid stale length on freshly opened handles.
        // Both counters under the publish lock: `flush_temp_to_main` moves a
        // run of bytes from the staged file to the main one, and the sum is
        // only meaningful either side of that move, never inside it.
        let _publish = self.append_file.read();
        let main = self.load_file_len();
        let temp = self.temp_file_len.load(Ordering::Acquire);
        main + temp
    }

    fn exists(location: &str, table_name: &str) -> bool {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let file_str = format!("{}{}{}", location, delimiter, table_name);

        let path = Path::new(&file_str);

        if path.exists() && path.is_file() {
            return true;
        } else {
            return false;
        }
    }

    async fn read_data_into_buffer(&self, position: &mut u64, buffer: &mut [u8]) -> io::Result<()> {
        self.read_data_into_buffer_internal(position, buffer).await
    }

    async fn read_vectored(&self, reads: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        // Pending entries are rare (only when mmap can't satisfy the read fully),
        // so a SmallVec avoids a per-call heap allocation in the common case.
        let mut pending: SmallVec<[(usize, u64, usize); 16]> = SmallVec::new();
        let main_len = self.load_file_len();

        // Hoist the TLS borrow + Arc<Mmap> resolution out of the per-entry loop.
        // Previously each entry called `mmap_copy_region` -> `with_cached_mmap`,
        // which performed a TLS RefCell borrow and an `ArcSwap::load` per entry.
        self.with_cached_mmap(|mmap| {
            let mmap_bytes: &[u8] = &mmap[..];
            let mmap_len = mmap_bytes.len() as u64;

            for (i, (pos, buf)) in reads.iter_mut().enumerate() {
                if buf.is_empty() {
                    continue;
                }

                if *pos < main_len && *pos < mmap_len {
                    let max_span = (mmap_len - *pos).min(main_len - *pos);
                    let to_copy = min(buf.len(), max_span as usize);
                    if to_copy > 0 {
                        let start = *pos as usize;
                        buf[..to_copy].copy_from_slice(&mmap_bytes[start..start + to_copy]);
                    }
                    if to_copy == buf.len() {
                        continue;
                    }
                    pending.push((i, *pos + to_copy as u64, to_copy));
                } else {
                    pending.push((i, *pos, 0));
                }
            }
        });

        if pending.is_empty() {
            return Ok(());
        }

        // Process pending reads (temp file or mmap-miss fallbacks)
        for (idx, pos, offset) in pending {
            let (_, buf) = &mut reads[idx];
            let mut p = pos;
            self.read_data_into_buffer_internal(&mut p, &mut buf[offset..])
                .await?;
        }
        Ok(())
    }

    /// Synchronous fast path: copies every read directly out of the mmap if
    /// possible. Returns true only when every entry was fully satisfied — the
    /// caller can then skip the async `read_vectored` entirely (no future
    /// poll, no waker setup, no SmallVec for pending).
    #[inline]
    fn try_read_vectored_sync(&self, reads: &mut [(u64, &mut [u8])]) -> bool {
        let main_len = self.load_file_len();
        // Hot path: do not even attempt this if there is staged temp data —
        // some reads might land there and we would have to fall back anyway.
        if self.temp_has_data.load(Ordering::Relaxed) {
            return false;
        }

        // Single TLS+ArcSwap resolution, then plain memcpy per entry.
        self.with_cached_mmap(|mmap| {
            let mmap_bytes: &[u8] = &mmap[..];
            let mmap_len = mmap_bytes.len() as u64;

            for (pos, buf) in reads.iter_mut() {
                if buf.is_empty() {
                    continue;
                }
                let p = *pos;
                let blen = buf.len();
                // Need every byte to come from mmap (and within main_len).
                if p >= main_len || p >= mmap_len {
                    return false;
                }
                let avail = (mmap_len - p).min(main_len - p);
                if (blen as u64) > avail {
                    return false;
                }
                let start = p as usize;
                buf.copy_from_slice(&mmap_bytes[start..start + blen]);
            }
            true
        })
    }

    async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
        let _registered = gated!(self, Site::WriteSeek, |e| {
            GatedIo::record_failure(&self.location, &self.table_name, Failure::Untokened, e);
            return;
        });
        let mut file = self.write_file.write();
        let written = file
            .seek(seek)
            .and_then(|_| file.write_all(buffer))
            .and_then(|()| file.flush())
            .and_then(|()| {
                crate::core::sync_audit::note("file.data", Path::new(&self.file_str));
                file.sync_all()
            });
        if let Err(e) = written {
            // Used to `unwrap()`. The trait gives this no way to return the
            // error and there is no old copy to restore, so the file is
            // poisoned: every later `commit_durable` on it fails rather than
            // reporting success over bytes that may be half-written.
            drop(file);
            let _ = self.commit.poison(format!(
                "write to {} at {:?} failed: {}",
                self.file_str, seek, e
            ));
        }
    }

    async fn create_temp(&self) -> Self {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let location_path = Path::new(&self.location);

        if !location_path.exists() {
            _ = std::fs::create_dir_all(location_path);
        }

        let final_location = self
            .location
            .trim_end_matches(|c| c == '\\' || c == '/')
            .to_string();

        let file_str = format!("{}{}{}", final_location, delimiter, "temp.db");

        _ = std::fs::File::create(&file_str).unwrap();
        let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&file_str));

        let file_append = std::fs::File::options()
            .read(true)
            .append(true)
            .open(&file_str)
            .unwrap();

        let file_write = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&file_str)
            .unwrap();

        let file_read_mmap = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&file_str)
            .unwrap();

        let read_file = RcArc::new(std::fs::File::options().read(true).open(&file_str).unwrap());

        // Do NOT truncate here. We may be opening an existing companion file (e.g. FIELDS.db)
        // from tests or runtime, and truncation would destroy previously appended data.
        let file_len = file_read_mmap.metadata().unwrap().len();

        let temp_file_str = format!("{}.tmp", file_str);
        if !Path::new(&temp_file_str).exists() {
            _ = std::fs::File::create(&temp_file_str).unwrap();
            let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&temp_file_str));
        }
        let temp_file = std::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(&temp_file_str)
            .unwrap();

        let temp_read_file = RcArc::new(
            std::fs::File::options()
                .read(true)
                .open(&temp_file_str)
                .unwrap(),
        );

        // Preserve any existing staged temp bytes; tests may rely on persistence across openings.
        let mut staged_len = std::fs::metadata(&temp_file_str)
            .map(|m| m.len())
            .unwrap_or(0);

        if file_len == 0 && staged_len > 0 {
            let _ = std::fs::OpenOptions::new()
                .write(true)
                .open(&temp_file_str)
                .and_then(|f| {
                    f.set_len(0)?;
                    Ok(())
                });
            staged_len = 0;
        }

        let local_storage_provider = Self {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            read_file,
            location: final_location.to_string(),
            table_name: "temp.db".to_string(),
            file_str: file_str.clone(),
            temp_file_str,
            temp_file: Arc::new(RwLock::new(temp_file)),
            temp_read_file,
            temp_mem: RwLock::new(Vec::new()),
            temp_mem_len: AtomicU64::new(0).into(),
            temp_file_len: AtomicU64::new(staged_len).into(),
            file_len: AtomicU64::new(file_len).into(),
            inplace_end: AtomicU64::new(0).into(),
            _memory_map: Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            }))),
            mmap_gen: AtomicU64::new(1).into(),
            mmap_key: next_mmap_key(),
            hash: CRC.checksum(format!("{}+++{}", final_location, "temp.db").as_bytes()),
            _locked: AtomicBool::new(false).into(),
            temp_has_data: AtomicBool::new(false).into(),
            append_blocked: AtomicBool::new(false).into(),
            commit: Default::default(),
        };

        if staged_len > 0 {
            let _ = local_storage_provider.flush_temp_to_main().await;
        }

        local_storage_provider.register_with_wal();
        local_storage_provider
    }

    async fn swap_temp(&self, _temp_io_sync: &mut Self) {
        // Replaces this file's bytes wholesale: a write of the file set.
        let _registered = gated!(self, Site::SwapTemp, |e| {
            GatedIo::record_failure(&self.location, &self.table_name, Failure::Untokened, e);
            return;
        });
        yield_now().await;

        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let temp_file_str = format!("{}{}{}", self.location, delimiter, "temp.db");
        let actual_file_str = self.file_str.clone();

        // Rename the temp file to the actual file name
        _ = fs::remove_file(&actual_file_str);
        _ = fs::rename(&temp_file_str, &actual_file_str);

        self.close_files().await;
    }

    fn get_location(&self) -> Option<String> {
        Some(self.location.clone())
    }

    #[allow(refining_impl_trait)]
    async fn create_new_at(&self, location: &str, name: String) -> Self {
        // `new` and `create_new` build the same thing from the same inputs; the
        // only difference is which directory the name is resolved against, and
        // `new` takes that as an argument. Duplicating the body here to avoid
        // one `Option` would be a second copy of the opening sequence to keep
        // in step.
        Self::new(location, Some(&name)).await
    }

    #[allow(refining_impl_trait)]
    async fn create_new(&self, name: String) -> Self {
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let location_path = Path::new(&self.location);

        if !location_path.exists() {
            _ = std::fs::create_dir_all(location_path);
        }

        let final_location = self
            .location
            .trim_end_matches(|c| c == '\\' || c == '/')
            .to_string();

        let new_table = format!("{}{}{}", final_location, delimiter, name);
        let file_path = Path::new(&new_table);

        if !file_path.exists() {
            _ = std::fs::File::create(&file_path).unwrap();
            let _ = crate::core::wal::sync_parent_dir(file_path);
        }

        let file_append = std::fs::File::options()
            .read(true)
            .append(true)
            .open(&file_path)
            .unwrap();

        let file_write = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        let file_read_mmap = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        let read_file = RcArc::new(
            std::fs::File::options()
                .read(true)
                .open(&file_path)
                .unwrap(),
        );

        let temp_file_str = format!("{}.tmp", new_table);

        if !Path::new(&temp_file_str).exists() {
            _ = std::fs::File::create(&temp_file_str).unwrap();
            let _ = crate::core::wal::sync_parent_dir(std::path::Path::new(&temp_file_str));
        }

        let temp_file = std::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(&temp_file_str)
            .unwrap();

        let temp_read_file = RcArc::new(
            std::fs::File::options()
                .read(true)
                .open(&temp_file_str)
                .unwrap(),
        );

        let staged_len = std::fs::metadata(&temp_file_str)
            .map(|m| m.len())
            .unwrap_or(0);

        let file_len = file_read_mmap.metadata().unwrap().len();

        let local_storage_provider = Self {
            append_file: Arc::new(RwLock::new(file_append)),
            write_file: Arc::new(RwLock::new(file_write)),
            read_file,
            location: final_location.to_string(),
            table_name: name.to_string(),
            file_str: new_table.clone(),
            temp_file_str,
            temp_file: Arc::new(RwLock::new(temp_file)),
            temp_read_file,
            temp_mem: RwLock::new(Vec::new()),
            temp_mem_len: AtomicU64::new(0).into(),
            temp_file_len: AtomicU64::new(staged_len).into(),
            file_len: AtomicU64::new(file_len).into(),
            inplace_end: AtomicU64::new(0).into(),
            _memory_map: Arc::new(ArcSwap::new(Arc::new(unsafe {
                MmapOptions::new().map(&file_read_mmap).unwrap()
            }))),
            mmap_gen: AtomicU64::new(1).into(),
            mmap_key: next_mmap_key(),
            hash: CRC.checksum(format!("{}+++{}", final_location, name).as_bytes()),
            _locked: AtomicBool::new(false).into(),
            temp_has_data: AtomicBool::new(false).into(),
            append_blocked: AtomicBool::new(false).into(),
            commit: Default::default(),
        };

        if staged_len > 0 {
            let _ = crate::core::index_store::gated_io::ungated(
                crate::core::index_store::gated_io::Ungated::OpenLanding,
                local_storage_provider.flush_temp_to_main(),
            )
            .await;
        }
        // A file named after this one (`<t>_schema.db`'s `_schema_*`
        // sidecars) is the same file set's: it registers on the same word.
        local_storage_provider
            .commit
            .gated
            .inherit_from(&self.commit.gated, &self.table_name, &name);

        local_storage_provider.register_with_wal();
        local_storage_provider
    }

    fn drop_io(&self) {
        // Best-effort cleanup. These files are temporary staging artifacts and
        // may already be gone (or still open on Windows). Do not panic.
        let delimiter = if cfg!(unix) {
            "/"
        } else if cfg!(windows) {
            "\\"
        } else {
            panic!("OS not supported");
        };

        let file_str = format!("{}{}{}", self.location, delimiter, self.table_name);
        // Not a sync: noted so a crash image can tell which deletions a later
        // directory sync made durable.
        crate::core::sync_audit::note("unlink", Path::new(&file_str));
        let _ = remove_file(Path::new(&file_str));

        // Also remove the companion temp staging file created by this provider.
        crate::core::sync_audit::note("unlink", Path::new(&self.temp_file_str));
        let _ = remove_file(Path::new(&self.temp_file_str));
    }

    fn get_hash(&self) -> u32 {
        self.hash
    }

    async fn start_service(&self) {
        // Run both the legacy batch service (if used elsewhere) and the temp flush service.
        async fn flush_loop(p: &LocalStorageProvider) {
            loop {
                // Fast path: only flush when we know there's staged data
                match p.land_round().await {
                    // Landed: look again at once, as before.
                    LandRound::Landed => {}
                    // Nothing staged, or the gate refused this round: avoid
                    // hot spinning.
                    LandRound::Idle | LandRound::Refused => {
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        }

        tokio::join!(flush_loop(self));
    }

    async fn land_background(&self) -> bool {
        match self.land_round().await {
            LandRound::Idle => false,
            LandRound::Refused => true,
            LandRound::Landed => self.temp_has_data.load(Ordering::Acquire),
        }
    }

    fn get_name(&self) -> String {
        self.table_name.clone()
    }

    fn index_store_dir(&self) -> Option<std::path::PathBuf> {
        Some(std::path::PathBuf::from(&self.location))
    }

    fn attach_write_gate(
        &self,
        gate: &std::sync::Arc<crate::core::index_store::file_gate::FileGate>,
        policy: crate::core::index_store::gated_io::Policy,
    ) {
        self.commit.gated.attach(gate, policy);
    }

    async fn land_staged(&self) -> io::Result<()> {
        self.land_staged_durably().await
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::OpenOptions,
        io::{Seek, SeekFrom, Write},
        sync::atomic::Ordering,
    };

    use rand::{Rng, SeedableRng, rngs::StdRng};

    use crate::core::storage_providers::{file_sync::LocalStorageProvider, traits::StorageIO};

    #[tokio::test]
    async fn test_basic_read_full_within_mmap() {
        let file_size = 16 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        let mut pos = 0u64;
        let mut buf = vec![0u8; 3000];
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap();

        assert_eq!(pos, 3000);
        assert_eq!(&buf, &data[0..3000]);
    }

    #[tokio::test]
    async fn test_read_cross_multiple_chunks_inside_mmap() {
        // Make buffer span several chunk boundaries but stay inside initial mmap.
        let file_size = 64 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        let start = 1500u64; // not aligned
        let mut pos = start;
        let mut buf = vec![0u8; 10_000]; // covers multiple chunks
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap();

        assert_eq!(pos, start + 10_000);
        assert_eq!(&buf, &data[start as usize..start as usize + 10_000]);
    }

    #[tokio::test]
    async fn test_read_beyond_initial_mmap_uses_chunk_path() {
        // Create a file, mmap only the initial part, then append more bytes
        // and update file_len without remapping to force the chunk fallback.
        let initial = 8 * 1024;
        let extra = 6 * 1024;
        let (provider, _initial_data, extra_data) = create_provider(initial, extra).await;

        // Position exactly at end of original mmap -> mmap path misses.
        let mut pos = initial as u64;
        let mut buf = vec![0u8; 3000]; // entirely inside the "extra" region
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap();

        assert_eq!(pos, (initial + 3000) as u64);
        assert_eq!(&buf, &extra_data[..3000]);

        // Second read continuing inside extra area (should hit cached chunk).
        let mut buf2 = vec![0u8; 2000];
        provider
            .read_data_into_buffer(&mut pos, &mut buf2)
            .await
            .unwrap();
        assert_eq!(pos, (initial + 5000) as u64);
        assert_eq!(&buf2, &extra_data[3000..5000]);
    }

    #[tokio::test]
    async fn test_partial_eof_error_after_partial_copy() {
        let file_size = 10 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        // Start 100 bytes before EOF, ask for 500 bytes -> expect UnexpectedEof after copying 100.
        let start = (file_size - 100) as u64;
        let mut pos = start;
        let mut buf = vec![0u8; 500];
        let err = provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        // Position advanced by bytes successfully copied (100)
        assert_eq!(pos, file_size as u64);

        assert_eq!(&buf[..100], &data[file_size - 100..file_size]);
    }

    #[tokio::test]
    async fn test_position_beyond_file_end_immediate_eof() {
        let file_size = 8192;
        let (provider, _data, _extra) = create_provider(file_size, 0).await;

        let mut pos = (file_size + 10) as u64;
        let mut buf = vec![0u8; 128];
        let err = provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(pos, (file_size + 10) as u64); // unchanged
    }

    #[tokio::test]
    async fn test_large_read_entire_file() {
        let file_size = 32 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        let mut pos = 0u64;
        let mut buf = vec![0u8; file_size];
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap();

        assert_eq!(pos, file_size as u64);
        assert_eq!(&buf, &data[..]);
    }

    #[tokio::test]
    async fn test_multiple_sequential_reads_advancing_position() {
        let file_size = 16 * 1024;
        let (provider, data, _extra) = create_provider(file_size, 0).await;

        let mut pos = 0u64;

        for step in [500usize, 1500, 4096, 320].iter() {
            let mut buf = vec![0u8; *step];
            provider
                .read_data_into_buffer(&mut pos, &mut buf)
                .await
                .unwrap();
            assert_eq!(&buf, &data[(pos as usize - *step)..(pos as usize)]);
        }
    }

    /* ---------- Helper to create a provider with optional "extra" appended data ---------- */

    /// Cold-cache against warm scan time.
    ///
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored cold_scan_readahead --nocapture`
    ///
    /// Written to measure an `MADV_WILLNEED` one chunk ahead of each 32 KiB+
    /// sequential mmap read. Measured 2026-10-08 on NVMe/ext4, 1.36 M rows
    /// (131 MB evicted, eviction confirmed with `fincore`): pointer-only
    /// COUNT(*) 42.7-44.9 ms without vs 44.7-45.7 ms with; a row-reading count
    /// 36.9-38.6 vs 37.8-43.1 ms; warm 24-26 ms. No win — the kernel's mmap
    /// read-around already keeps up — so the hint was not landed. A slower
    /// device (network block storage, spinning disk) is where to re-measure.
    ///
    /// On ext4 under /var/tmp, never tmpfs (where there is no I/O to overlap).
    /// Each round closes the database, `fdatasync`s and evicts every file with
    /// `POSIX_FADV_DONTNEED` — per file, so other processes' caches are left
    /// alone — reopens it and scans. Delete the directory when done.
    #[test]
    #[ignore]
    fn cold_scan_readahead() {
        use crate::core::rql::executor;
        use crate::core::rql::lexer_s1::recognize_query_purpose;
        use std::os::fd::AsRawFd;

        let root = std::env::var("RDB_COLD_ROOT")
            .unwrap_or_else(|_| "/var/tmp/rasterizeddb-storage-bench".to_string());
        let rows: usize = std::env::var("RDB_COLD_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2_000_000);
        let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
        let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);
        let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.get_or_init(|| false);

        fn on_runtime<F: std::future::Future<Output = ()> + Send + 'static>(f: F) {
            std::thread::Builder::new()
                .stack_size(64 << 20)
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(8)
                        .thread_stack_size(32 << 20)
                        .enable_all()
                        .build()
                        .unwrap();
                    rt.block_on(f);
                    rt.shutdown_timeout(std::time::Duration::from_secs(5));
                })
                .unwrap()
                .join()
                .unwrap();
        }
        async fn exec(db: &rclite::Arc<crate::core::database::Database>, sql: &str) -> Vec<u8> {
            executor::execute(recognize_query_purpose(sql).unwrap(), db.clone()).await
        }
        fn evict(dir: &std::path::Path) -> u64 {
            let mut bytes = 0;
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    bytes += evict(&path);
                    continue;
                }
                let file = std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
                let _ = file.sync_all();
                unsafe {
                    libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
                }
                bytes += file.metadata().unwrap().len();
            }
            bytes
        }

        let marker = std::path::Path::new(&root).join(format!("loaded-{}", rows));
        if !marker.exists() {
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(&root).unwrap();
            let root2 = root.clone();
            on_runtime(async move {
                let catalog = crate::core::database::catalog::DatabaseCatalog::open(
                    Some(&root2),
                    "coldbench",
                )
                .await
                .unwrap();
                let db = catalog.get("coldbench").unwrap();
                let r = exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, name TEXT)").await;
                assert_eq!(r[0], 0, "{}", String::from_utf8_lossy(&r[1..]));
                let t = std::time::Instant::now();
                for chunk in 0..rows / 5_000 {
                    let values: Vec<String> = (0..5_000)
                        .map(|i| {
                            let id = chunk * 5_000 + i;
                            format!("({}, {}, 'name-{:032}')", id, id % 1000, id)
                        })
                        .collect();
                    let r = exec(
                        &db,
                        &format!("INSERT INTO t (id, a, name) VALUES {}", values.join(", ")),
                    )
                    .await;
                    assert_ne!(r[0], 3, "{}", String::from_utf8_lossy(&r[1..]));
                }
                println!("loaded {} rows in {:?}", rows, t.elapsed());
            });
            std::fs::write(&marker, b"").unwrap();
        }

        for round in 0..3 {
            {
                let evicted = evict(std::path::Path::new(&root));
                let root2 = root.clone();
                on_runtime(async move {
                    let catalog = crate::core::database::catalog::DatabaseCatalog::open(
                        Some(&root2),
                        "coldbench",
                    )
                    .await
                    .unwrap();
                    let db = catalog.get("coldbench").unwrap();
                    let mut line = format!("round {} ({} MB evicted):", round, evicted >> 20);
                    for sql in [
                        "SELECT COUNT(*) FROM t",
                        "SELECT COUNT(*) FROM t WHERE a > -1",
                        "SELECT COUNT(*) FROM t WHERE a > -1",
                    ] {
                        let t = std::time::Instant::now();
                        let r = exec(&db, sql).await;
                        assert_eq!(r[0], 2, "{}", String::from_utf8_lossy(&r[1..]));
                        line.push_str(&format!("  {:?}", t.elapsed()));
                    }
                    println!("{}  (pointer-only cold, rows cold, rows warm)", line);
                });
            }
        }
    }

    /// A large staged append must not leave its capacity behind once flushed,
    /// in the staging mirror or in the pooled buffer.
    /// A write that fails part-way must leave the file and the offset
    /// allocator as they were and fail its transaction, not panic with the
    /// partial bytes left behind and `file_len` short of them.
    #[tokio::test]
    async fn a_failed_append_rolls_back_and_fails_its_commit() {
        let dir = tempfile::tempdir().unwrap();
        let p = LocalStorageProvider::new(dir.path().to_str().unwrap(), Some("t.db")).await;
        p.append_data(&[1u8; 4096], true).await;
        assert_eq!(p.get_len().await, 4096);

        let txn = p.begin_txn();
        super::failed_appends::SHORT_APPEND.with(|h| h.set(Some(1000)));
        let (_, token) = p.append_data_txn(txn, &[2u8; 4096]).await;
        super::failed_appends::SHORT_APPEND.with(|h| h.set(None));
        assert!(p.commit_durable(token).await.is_err(), "the failed append committed");
        assert_eq!(p.get_len().await, 4096);
        assert_eq!(std::fs::metadata(dir.path().join("t.db")).unwrap().len(), 4096);

        let off = p.append_data_returning_offset(&[3u8; 4096], true).await;
        assert_eq!(off, 4096);
        let mut buf = vec![0u8; 4096];
        let mut pos = off;
        p.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();
        assert_eq!(buf, vec![3u8; 4096], "the offset handed out does not hold its bytes");
        // And the failure was reported once, not to every later commit.
        assert!(p.commit_durable(p.begin_txn()).await.is_ok());
    }

    /// An in-place write with no log that fails part-way must leave the old
    /// bytes, not a mix of old and new, and fail its statement.
    #[tokio::test]
    async fn a_failed_logless_overwrite_puts_the_old_bytes_back() {
        for undo in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let p = LocalStorageProvider::new(dir.path().to_str().unwrap(), Some("ip.db")).await;
            p.append_data(&[1u8; 4096], true).await;
            super::failed_appends::NO_UNDO.set(!undo);
            // One inside the file, one running past its end.
            for (at, len) in [(1000u64, 500usize), (4000, 500)] {
                let txn = p.begin_txn();
                super::failed_appends::SHORT_INPLACE.with(|h| h.set(Some(100)));
                let token = p.write_data_txn(txn, at, &vec![9u8; len]).await;
                assert!(p.commit_durable(token).await.is_err(), "the failed overwrite committed");
                let on_disk = std::fs::read(dir.path().join("ip.db")).unwrap();
                let intact = on_disk[at as usize..(at as usize + 100).min(4096)]
                    .iter()
                    .all(|b| *b == 1);
                assert_eq!(intact, undo, "at {at}: undo {undo}, bytes {:?}", &on_disk[at as usize..at as usize + 4]);
            }
            super::failed_appends::NO_UNDO.set(false);
            // The file is still writable, and a successful overwrite lands.
            let token = p.write_data_txn(p.begin_txn(), 10, b"ok").await;
            p.commit_durable(token).await.unwrap();
            assert_eq!(&std::fs::read(dir.path().join("ip.db")).unwrap()[10..12], b"ok");
        }
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// logless_overwrite_undo_bench --nocapture`. The undo image's cost on a
    /// stream of 64-byte in-place overwrites (a pointer record's size class).
    #[tokio::test]
    #[ignore]
    async fn logless_overwrite_undo_bench() {
        let dir = tempfile::tempdir().unwrap();
        let p = LocalStorageProvider::new(dir.path().to_str().unwrap(), Some("bench.db")).await;
        p.append_data(&vec![1u8; 1 << 20], true).await;
        const N: u64 = 100_000;
        for round in 0..3 {
            for undo in [false, true] {
                super::failed_appends::NO_UNDO.set(!undo);
                let t = std::time::Instant::now();
                for i in 0..N {
                    let mut file = p.write_file.write();
                    super::LocalStorageProvider::overwrite_with_undo(
                        &mut file,
                        (i * 64) % ((1 << 20) - 64),
                        &[7u8; 64],
                    )
                    .unwrap();
                }
                println!(
                    "round {round} undo {undo}: {:.0} ns per 64-byte overwrite",
                    t.elapsed().as_nanos() as f64 / N as f64
                );
            }
        }
        super::failed_appends::NO_UNDO.set(false);
    }

    /// `copy_table_files`, `copy_between` and `force_resync` stage a copy and
    /// then ask `commit_durable` for durability. That must land and sync the
    /// staged bytes, which used to stay in the unsynced `.tmp` sidecar.
    #[tokio::test]
    async fn commit_durable_lands_staged_appends() {
        let dir = tempfile::tempdir().unwrap();
        let p = LocalStorageProvider::new(dir.path().to_str().unwrap(), Some("copy.db")).await;
        for i in 0..8u8 {
            p.append_data(&[i; 4096], false).await;
        }
        p.commit_durable(p.begin_txn()).await.unwrap();
        assert_eq!(p.get_len().await, 8 * 4096);
        assert_eq!(
            std::fs::metadata(dir.path().join("copy.db")).unwrap().len(),
            8 * 4096,
            "commit_durable returned with the copy still in the .tmp sidecar"
        );
        let mut buf = vec![0u8; 4096];
        let mut pos = 7 * 4096;
        p.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();
        assert_eq!(buf, vec![7u8; 4096]);
    }

    /// A staged append that fails part-way (a full disk, EFBIG) must not
    /// panic, must leave the `.tmp` file, the in-memory mirror and the staged
    /// length agreeing, and must fail the `commit_durable` that lands the copy.
    /// It used to `unwrap()` with the partial bytes left in the file, and the
    /// next append — placed at the file's end — then sat at a different
    /// offset in the file than in the mirror.
    #[tokio::test]
    async fn a_failed_staged_append_rolls_back_and_fails_the_landing_commit() {
        let dir = tempfile::tempdir().unwrap();
        let p = LocalStorageProvider::new(dir.path().to_str().unwrap(), Some("copy.db")).await;
        let tmp = dir.path().join("copy.db.tmp");
        let agree = |p: &LocalStorageProvider| {
            let staged = p.temp_file_len.load(Ordering::Acquire);
            assert_eq!(std::fs::metadata(&tmp).unwrap().len(), staged, ".tmp file vs staged length");
            assert_eq!(p.temp_mem.read().len() as u64, staged, "mirror vs staged length");
            assert_eq!(p.temp_mem_len.load(Ordering::Acquire), staged, "mirror length vs staged length");
        };

        p.append_data(&[1u8; 4096], false).await;
        super::failed_appends::SHORT_APPEND.with(|h| h.set(Some(1000)));
        let failed_at = p.append_data_returning_offset(&[2u8; 4096], false).await;
        super::failed_appends::SHORT_APPEND.with(|h| h.set(None));
        assert_eq!(failed_at, 4096);
        assert_eq!(p.get_len().await, 4096, "the failed bytes were counted");
        agree(&p);

        let off = p.append_data_returning_offset(&[3u8; 4096], false).await;
        assert_eq!(off, 4096, "the next append is placed where the failed one would have been");
        agree(&p);
        // Read back from the mirror, and from the file the flush reads.
        let mut buf = vec![0u8; 4096];
        let mut pos = off;
        p.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();
        assert_eq!(buf, vec![3u8; 4096]);
        let on_disk = std::fs::read(&tmp).unwrap();
        assert_eq!(&on_disk[4096..], &[3u8; 4096][..], "the file holds other bytes than the mirror");

        assert!(
            p.commit_durable(p.begin_txn()).await.is_err(),
            "a copy missing a failed staged append was called durable"
        );
        // Reported once; the bytes that did stage land intact.
        p.commit_durable(p.begin_txn()).await.unwrap();
        assert_eq!(p.get_len().await, 2 * 4096);
        let main = std::fs::read(dir.path().join("copy.db")).unwrap();
        assert_eq!(&main[..4096], &[1u8; 4096][..]);
        assert_eq!(&main[4096..], &[3u8; 4096][..]);
        assert_eq!(std::fs::metadata(&tmp).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn a_large_staged_append_gives_its_buffers_back() {
        use super::{ByteVecAllocator, STAGING_KEEP_CAPACITY};
        use opool::PoolAllocator;

        let dir = tempfile::tempdir().unwrap();
        let provider =
            LocalStorageProvider::new(dir.path().to_str().unwrap(), Some("staging.db")).await;
        let big: Vec<u8> = (0..(STAGING_KEEP_CAPACITY * 3 / 2)).map(|i| i as u8).collect();
        provider.append_data(&big, false).await;
        assert!(provider.temp_mem.read().capacity() >= big.len());
        provider.flush_temp_to_main().await.unwrap();
        assert!(
            provider.temp_mem.read().capacity() <= STAGING_KEEP_CAPACITY,
            "staging mirror kept {} bytes of capacity",
            provider.temp_mem.read().capacity()
        );
        // And the bytes are where they should be.
        let mut pos = 0u64;
        let mut back = vec![0u8; big.len()];
        provider.read_data_into_buffer(&mut pos, &mut back).await.unwrap();
        assert!(back == big);

        let mut pooled = Vec::with_capacity(STAGING_KEEP_CAPACITY * 2);
        pooled.extend_from_slice(&big);
        ByteVecAllocator.reset(&mut pooled);
        assert!(pooled.is_empty());
        assert!(pooled.capacity() <= STAGING_KEEP_CAPACITY, "{}", pooled.capacity());
        let mut small = Vec::with_capacity(1024 * 1024);
        ByteVecAllocator.reset(&mut small);
        assert!(small.capacity() >= 1024 * 1024, "an ordinary buffer keeps its size");
    }

    async fn create_provider(
        initial_size: usize,
        extra_size: usize,
    ) -> (LocalStorageProvider, Vec<u8>, Vec<u8>) {
        let mut rng = StdRng::seed_from_u64(0xDEADBEEF);

        // Create temp file
        let mut path = std::env::temp_dir();
        path.push(unique_name("lsp_test"));

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .expect("open temp file");

        // Write initial data
        let mut initial_data = vec![0u8; initial_size];
        rng.fill_bytes(&mut initial_data);
        file.write_all(&initial_data).unwrap();
        file.flush().unwrap();

        let provider = LocalStorageProvider::new(
            path.parent().unwrap().to_str().unwrap(),
            Some(path.file_name().unwrap().to_str().unwrap()),
        )
        .await;

        // Append extra data AFTER provider creation (so mmap does not cover it).
        let mut extra_data = Vec::new();
        if extra_size > 0 {
            extra_data = vec![0u8; extra_size];
            rng.fill_bytes(&mut extra_data);
            // Extend underlying file
            file.seek(SeekFrom::End(0)).unwrap();
            file.write_all(&extra_data).unwrap();
            file.flush().unwrap();

            let new_len = (initial_size + extra_size) as u64;
            provider.file_len.store(new_len, Ordering::Release);
        }

        (provider, initial_data, extra_data)
    }

    fn unique_name(prefix: &str) -> String {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}_{nanos}_{}_{}", std::process::id(), count)
    }

    #[tokio::test]
    async fn test_cross_boundary_read_main_then_temp() {
        let init = 2048usize;
        let (provider, initial_data, _extra) = create_provider(init, 0).await;

        // Stage 1024 bytes
        let mut rng = StdRng::seed_from_u64(0x1234);
        let mut staged = vec![0u8; 1024];
        rng.fill_bytes(&mut staged);
        StorageIO::append_data(&provider, &staged, false).await;

        // Start 100 bytes before end of main, read 200 -> 100 main + 100 temp
        let mut pos = (init - 100) as u64;
        let mut buf = vec![0u8; 200];
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap();
        assert_eq!(pos, (init + 100) as u64);
        assert_eq!(&buf[..100], &initial_data[init - 100..init]);
        assert_eq!(&buf[100..], &staged[..100]);
    }

    #[tokio::test]
    async fn test_flush_then_read_full() {
        let init = 1024usize;
        let (provider, _initial_data, _extra) = create_provider(init, 0).await;

        let mut rng = StdRng::seed_from_u64(0xBEEF);
        let mut staged = vec![0u8; 2000];
        rng.fill_bytes(&mut staged);
        StorageIO::append_data(&provider, &staged, false).await;

        // Flush staged data to main
        let flushed = provider.flush_temp_to_main().await.unwrap();
        assert_eq!(flushed, staged.len());
        assert_eq!(provider.temp_file_len.load(Ordering::Acquire), 0);

        // Read back the flushed range from main
        let mut pos = init as u64;
        let mut out = vec![0u8; staged.len()];
        provider
            .read_data_into_buffer(&mut pos, &mut out)
            .await
            .unwrap();
        assert_eq!(pos, (init + staged.len()) as u64);
        assert_eq!(out, staged);
    }

    #[tokio::test]
    async fn test_multiple_appends_then_read_back() {
        let init = 4096usize;
        let (provider, _initial_data, _extra) = create_provider(init, 0).await;

        // Prepare many chunks simulating multiple appended rows
        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut expected: Vec<u8> = Vec::new();

        for _ in 0..20 {
            let size = 64 + (rng.next_u32() as usize % 1536); // 64..1600 bytes
            let mut buf = vec![0u8; size];
            rng.fill_bytes(&mut buf);
            expected.extend_from_slice(&buf);
            chunks.push(buf);
        }

        // Append sequentially
        for ch in &chunks {
            StorageIO::append_data(&provider, ch, false).await;
        }

        // Verify logical len
        let total = expected.len();
        let len = provider.get_len().await as usize;
        assert_eq!(len, init + total);

        // Single full read of all staged bytes
        let mut pos = init as u64;
        let mut out = vec![0u8; total];
        provider
            .read_data_into_buffer(&mut pos, &mut out)
            .await
            .unwrap();
        assert_eq!(pos, (init + total) as u64);
        assert_eq!(out, expected);

        // Also test segmented reads across boundaries
        let mut pos2 = init as u64;
        let mut consumed = 0usize;
        while consumed < total {
            let to_read = (700).min(total - consumed);
            let mut buf = vec![0u8; to_read];
            let before = pos2;
            let _ = provider.read_data_into_buffer(&mut pos2, &mut buf).await; // may return EOF on boundary; position still advances
            let delta = (pos2 - before) as usize;
            assert!(delta <= to_read);
            assert_eq!(&buf[..delta], &expected[consumed..consumed + delta]);
            consumed += delta;
        }
        assert_eq!(pos2, (init + total) as u64);
    }

    #[tokio::test]
    async fn an_in_place_write_is_visible_through_the_provider_that_made_it() {
        // With a log installed, `write_data_txn` defers the bytes and
        // `commit_txn` applies them by opening the target *by path* — bypassing
        // the provider entirely. The provider's cached `file_len` and its mmap
        // therefore never learn that their own file grew.
        //
        // The failure shape is nasty: the write succeeds, is durable, and is on
        // disk, while the writer's own next read reports EOF. Nothing hits it
        // today by luck rather than design — the filter writes its checkpoint
        // and never re-reads it in-process — but that is one refactor from
        // being wrong, so it is pinned here.
        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().to_str().unwrap().to_string();

        let wal = std::sync::Arc::new(
            crate::core::wal::Wal::open(&dir.path().join("rastdb.wal")).unwrap(),
        );
        crate::core::wal::install_for(&location, wal);

        let provider = LocalStorageProvider::new(&location, Some("inplace_vis.db")).await;
        assert_eq!(provider.get_len().await, 0, "fresh file should be empty");

        let payload = vec![7u8; 512];
        provider.write_data(0, &payload).await;

        let on_disk = std::fs::metadata(dir.path().join("inplace_vis.db"))
            .map(|m| m.len())
            .unwrap_or(0);
        assert_eq!(on_disk, 512, "the bytes must reach disk");

        assert_eq!(
            provider.get_len().await,
            512,
            "the provider that issued the write must see its own file grow"
        );

        let mut buf = vec![0u8; 512];
        let mut pos = 0u64;
        provider
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .expect("the writer must be able to read back what it just wrote");
        assert_eq!(buf, payload);
    }

    /// Once the log is poisoned, no write may be acknowledged — whatever token
    /// it hands to `commit_durable`.
    ///
    /// The poisoned log is never dropped again, so a restart replays every
    /// committed record in it. A write that fell back to the data file, or a
    /// direct write that returned `0`, and reported success was therefore
    /// reverted at the next restart by the older record for the same offset:
    /// an acknowledged UPDATE or DELETE silently undone.
    #[tokio::test]
    async fn a_write_after_the_log_is_poisoned_is_refused_not_reverted_later() {
        use crate::core::wal::test_hooks;

        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().to_str().unwrap().to_string();
        let wal = std::sync::Arc::new(
            crate::core::wal::Wal::open(dir.path().join("rastdb.wal")).unwrap(),
        );
        crate::core::wal::install_for(&location, wal.clone());

        let provider = LocalStorageProvider::new(&location, Some("revert.db")).await;

        // T1: committed and applied, and still in the log.
        let txn = provider.begin_txn();
        let token = provider.write_data_txn(txn, 0, b"committed").await;
        provider.commit_durable(token).await.unwrap();

        // T2: same offset, and its log append fails partway and cannot be cut
        // back, which poisons the log.
        test_hooks::SHORT_WAL_WRITE.with(|h| h.set(Some(3)));
        test_hooks::FAIL_WAL_ROLLBACK.with(|f| f.set(true));
        let txn = provider.begin_txn();
        let token = provider.write_data_txn(txn, 0, b"refused!!").await;
        test_hooks::SHORT_WAL_WRITE.with(|h| h.set(None));
        test_hooks::FAIL_WAL_ROLLBACK.with(|f| f.set(false));
        let outcome = provider.commit_durable(token).await;
        assert!(wal.is_poisoned());
        let err = outcome.expect_err(
            "an in-place write that could not be logged was acknowledged over a poisoned log",
        );
        assert!(crate::core::storage_providers::traits::is_poisoned(&err), "{err}");
        // Nor may the refused bytes be in the file: a failed statement must not
        // be visible to every later read in this process.
        assert_eq!(std::fs::read(dir.path().join("revert.db")).unwrap(), b"committed");

        // An append whose record the poisoned log will not take falls back to
        // a direct flush, and must be refused rather than acknowledged.
        test_hooks::SHORT_WAL_WRITE.with(|h| h.set(Some(3)));
        let txn = provider.begin_txn();
        let (_, token) = provider.append_data_txn(txn, b"appended").await;
        test_hooks::SHORT_WAL_WRITE.with(|h| h.set(None));
        let err = provider.commit_durable(token).await.expect_err(
            "an append that could not be logged was acknowledged over a poisoned log",
        );
        assert!(crate::core::storage_providers::traits::is_poisoned(&err), "{err}");

        // And `0`, which a bulk load or any other direct write hands back,
        // is no way around it.
        let err = provider
            .commit_durable(0)
            .await
            .expect_err("a token of 0 was acknowledged over a poisoned log");
        assert!(crate::core::storage_providers::traits::is_poisoned(&err), "{err}");

        crate::core::wal::remove_for(&location);
    }

    /// A data-file flush failure is the whole directory's: a commit through
    /// another file there may name the bytes that were lost.
    #[tokio::test]
    async fn a_flush_failure_refuses_commits_on_every_file_in_its_directory() {
        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().to_str().unwrap().to_string();
        assert!(crate::core::wal::wal_for(&location).is_none());
        let rows = LocalStorageProvider::new(&location, Some("rows.db")).await;
        let pointers = LocalStorageProvider::new(&location, Some("pointers.db")).await;

        rows.fail_next_flush_for_tests();
        let (_, token) = rows.append_data_txn(rows.begin_txn(), b"row bytes").await;
        assert!(rows.commit_durable(token).await.is_err());

        let (_, token) = pointers.append_data_txn(pointers.begin_txn(), b"pointer").await;
        let err = pointers
            .commit_durable(token)
            .await
            .expect_err("a pointer committed beside row bytes that never reached the disk");
        assert!(crate::core::storage_providers::traits::is_poisoned(&err), "{err}");
        use super::{directory_flush_failure, forget_directory_failure};
        assert!(directory_flush_failure(&location).is_some());

        forget_directory_failure(&location);
        assert_eq!(directory_flush_failure(&location), None);
    }

    /// A file unlinked under a writer — `DROP TABLE` with a task still
    /// holding its handle — fails its own commits but not its siblings'.
    ///
    /// Its bytes are unreachable whether or not they were flushed, so no
    /// pointer elsewhere can name lost data. Charging the directory with it
    /// refused every commit in a healthy database.
    #[tokio::test]
    async fn an_unlinked_file_s_failed_flush_is_not_the_directory_s() {
        use super::directory_flush_failure;
        use crate::core::storage_providers::traits::is_poisoned;

        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().to_str().unwrap().to_string();
        assert!(crate::core::wal::wal_for(&location).is_none());
        let dropped = LocalStorageProvider::new(&location, Some("dropped.db")).await;
        let sibling = LocalStorageProvider::new(&location, Some("sibling.db")).await;

        std::fs::remove_file(dir.path().join("dropped.db")).unwrap();
        let (_, token) = dropped.append_data_txn(dropped.begin_txn(), b"late").await;
        let err = dropped
            .commit_durable(token)
            .await
            .expect_err("a write to an unlinked file was acknowledged as durable");
        assert!(is_poisoned(&err), "{err}");
        assert_eq!(directory_flush_failure(&location), None);

        let (_, token) = sibling.append_data_txn(sibling.begin_txn(), b"row").await;
        sibling
            .commit_durable(token)
            .await
            .expect("a sibling's commit was refused for an unlinked file's flush");

        // Only a missing file is excused. Any other failure to open it may
        // leave bytes a sibling names unflushed, and stays the directory's.
        let blocked = LocalStorageProvider::new(&location, Some("blocked.db")).await;
        std::fs::remove_file(dir.path().join("blocked.db")).unwrap();
        std::fs::create_dir(dir.path().join("blocked.db")).unwrap();
        let (_, token) = blocked.append_data_txn(blocked.begin_txn(), b"row").await;
        assert!(blocked.commit_durable(token).await.is_err());
        assert!(
            directory_flush_failure(&location).is_some(),
            "a flush that failed with the file still there was not charged to its directory"
        );
        super::forget_directory_failure(&location);
    }

    #[tokio::test]
    async fn test_concurrent_appends_and_reads() {
        use std::sync::Arc;
        use tokio::time::{Duration, sleep};

        let init = 2048usize;
        let (provider, _initial_data, _extra) = create_provider(init, 0).await;
        let provider = Arc::new(provider);
        let writer = provider.clone();
        let reader = provider.clone();

        // Precompute chunks and expected data
        let mut rng = StdRng::seed_from_u64(0x1337);
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut expected: Vec<u8> = Vec::new();
        for _ in 0..40 {
            let size = 128 + (rng.next_u32() as usize % 512);
            let mut buf = vec![0u8; size];
            rng.fill_bytes(&mut buf);
            expected.extend_from_slice(&buf);
            chunks.push(buf);
        }
        let expected = Arc::new(expected);
        let chunks = Arc::new(chunks);

        // Writer task: append chunks with tiny delays
        let w_expected = expected.clone();
        let w_chunks = chunks.clone();
        let writer_task = tokio::spawn(async move {
            for ch in w_chunks.iter() {
                StorageIO::append_data(&*writer, ch, false).await;
                // small jitter
                sleep(Duration::from_millis(1)).await;
            }
            // ensure final len is as expected
            let len = (*writer).get_len().await as usize;
            assert_eq!(len, init + w_expected.len());
        });

        // Reader task: keep reading as data becomes available
        let r_expected = expected.clone();
        let reader_task = tokio::spawn(async move {
            let mut pos = init as u64;
            let mut consumed = 0usize;
            let total = r_expected.len();

            while consumed < total {
                let current_len = reader.get_len().await;
                if pos >= current_len {
                    // nothing new yet
                    sleep(Duration::from_millis(1)).await;
                    continue;
                }
                let avail = (current_len - pos) as usize;
                let to_read = (700).min(avail);
                let mut buf = vec![0u8; to_read];
                let before = pos;
                let _ = reader.read_data_into_buffer(&mut pos, &mut buf).await; // may Err on boundary; position still advances
                let delta = (pos - before) as usize;
                assert!(delta <= to_read);
                assert_eq!(&buf[..delta], &r_expected[consumed..consumed + delta]);
                consumed += delta;
            }

            // Final assertions
            assert_eq!(pos, (init + total) as u64);
        });

        let _ = tokio::join!(writer_task, reader_task);

        // Final length check
        let final_len = provider.get_len().await as usize;
        assert_eq!(final_len, init + expected.len());
    }

    #[tokio::test]
    async fn test_append_immediate_then_read_back() {
        use rand::{Rng, SeedableRng, rngs::StdRng};

        let init = 4096usize;
        let (provider, initial_data, _extra) = create_provider(init, 0).await;

        // Prepare data to append immediately (persisted directly to main file)
        let mut rng = StdRng::seed_from_u64(0xA11CE);
        let mut appended = vec![0u8; 5000];
        rng.fill_bytes(&mut appended);

        // Append with immediate=true
        StorageIO::append_data(&provider, &appended, true).await;

        // Length should reflect persisted bytes only (no temp staging)
        let len = provider.get_len().await as usize;
        assert_eq!(len, init + appended.len());

        // Verify file contents at the appended position
        let mut pos = init as u64;
        let mut out = vec![0u8; appended.len()];
        provider
            .read_data_into_buffer(&mut pos, &mut out)
            .await
            .unwrap();
        assert_eq!(pos, (init + appended.len()) as u64);
        assert_eq!(out, appended);

        // Spot-check: earlier bytes (from initial) are unchanged
        let mut pos2 = (init - 128) as u64;
        let mut head = vec![0u8; 128];
        provider
            .read_data_into_buffer(&mut pos2, &mut head)
            .await
            .unwrap();
        assert_eq!(head, initial_data[init - 128..init]);

        // verify_data API should also pass for the appended range
        let ok = StorageIO::verify_data(&provider, init as u64, &appended).await;
        assert!(ok);
    }

    #[tokio::test]
    async fn test_append_immediate_then_staged_then_read_across_both() {
        use rand::{Rng, SeedableRng, rngs::StdRng};

        let init = 2048usize;
        let (provider, _initial_data, _extra) = create_provider(init, 0).await;

        let mut rng = StdRng::seed_from_u64(0xBADA55);
        let mut a_immediate = vec![0u8; 1500];
        rng.fill_bytes(&mut a_immediate);
        let mut b_staged = vec![0u8; 1700];
        rng.fill_bytes(&mut b_staged);

        // First append persisted immediately
        StorageIO::append_data(&provider, &a_immediate, true).await;
        // Then stage more data (not flushed yet)
        StorageIO::append_data(&provider, &b_staged, false).await;

        // Logical length should include both
        let len = provider.get_len().await as usize;
        assert_eq!(len, init + a_immediate.len() + b_staged.len());

        // Read a buffer spanning the end of immediate part into staged part
        let mut pos = (init + a_immediate.len() - 200) as u64; // start 200 bytes before boundary
        let mut out = vec![0u8; 600]; // 200 from A + 400 from B
        provider
            .read_data_into_buffer(&mut pos, &mut out)
            .await
            .unwrap();
        assert_eq!(pos, (init + a_immediate.len() - 200 + 600) as u64);
        assert_eq!(&out[..200], &a_immediate[a_immediate.len() - 200..]);
        assert_eq!(&out[200..600], &b_staged[..400]);

        // Full read of the whole appended logical tail should also succeed
        let mut pos2 = init as u64;
        let mut all = vec![0u8; a_immediate.len() + b_staged.len()];
        provider
            .read_data_into_buffer(&mut pos2, &mut all)
            .await
            .unwrap();
        assert_eq!(pos2, (init + all.len()) as u64);
        assert_eq!(all, [a_immediate.as_slice(), b_staged.as_slice()].concat());
    }

    #[tokio::test]
    async fn test_many_small_appends_mixed_then_piecewise_reads() {
        use rand::{Rng, SeedableRng, rngs::StdRng};

        let init = 1024usize;
        let (provider, _initial_data, _extra) = create_provider(init, 0).await;

        let mut rng = StdRng::seed_from_u64(0xFACEFEED);
        let mut expected_main: Vec<u8> = Vec::new();
        let mut expected_temp: Vec<u8> = Vec::new();
        // Alternate between immediate and staged appends, variable sizes
        for i in 0..30 {
            let size = 50 + (rng.next_u32() as usize % 700); // 50..749 bytes
            let mut buf = vec![0u8; size];
            rng.fill_bytes(&mut buf);
            let immediate = i % 2 == 0; // even -> immediate, odd -> staged
            StorageIO::append_data(&provider, &buf, immediate).await;
            if immediate {
                expected_main.extend_from_slice(&buf);
            } else {
                expected_temp.extend_from_slice(&buf);
            }
        }

        // Verify logical length matches
        let len = provider.get_len().await as usize;
        assert_eq!(len, init + expected_main.len() + expected_temp.len());

        // Piecewise reads of random sizes, ensuring correctness across boundaries
        let mut pos = init as u64;
        let mut consumed = 0usize;
        let combined: Vec<u8> = {
            let mut v = expected_main.clone();
            v.extend_from_slice(&expected_temp);
            v
        };
        while consumed < combined.len() {
            let remaining = combined.len() - consumed;
            let step = 1 + (rng.next_u32() as usize % 500); // 1..500 bytes
            let to_read = step.min(remaining);
            let mut buf = vec![0u8; to_read];
            let before = pos;
            let _ = provider.read_data_into_buffer(&mut pos, &mut buf).await; // may EOF at exact logical end
            let got = (pos - before) as usize;
            assert_eq!(&buf[..got], &combined[consumed..consumed + got]);
            consumed += got;
        }
        assert_eq!(pos, (init + combined.len()) as u64);
    }

    #[tokio::test]
    async fn test_service_flush_from_temp_to_main_with_length_checks() {
        use rand::{Rng, SeedableRng, rngs::StdRng};
        use std::sync::Arc;
        use tokio::time::{Duration, sleep};

        // Initial main file content
        let init = 4096usize;
        let (provider_raw, initial_data, _extra) = create_provider(init, 0).await;
        let provider = Arc::new(provider_raw);

        // Prepare staged bytes (append to temp only)
        let mut rng = StdRng::seed_from_u64(0xFEEDFACE);
        let mut staged = vec![0u8; 3000];
        rng.fill_bytes(&mut staged);

        // Append to TMP (no immediate write)
        StorageIO::append_data(&*provider, &staged, false).await;

        // Length checks BEFORE flush
        let main_len_before = provider.file_len.load(Ordering::Acquire) as usize;
        let temp_len_before = provider.temp_file_len.load(Ordering::Acquire) as usize;
        let total_len_before = provider.get_len().await as usize;
        assert_eq!(main_len_before, init);
        assert_eq!(temp_len_before, staged.len());
        assert_eq!(total_len_before, init + staged.len());

        // Read across boundary BEFORE flush: last 100 of main + first 200 of temp
        let mut pos = (init - 100) as u64;
        let mut out = vec![0u8; 300];
        provider
            .read_data_into_buffer(&mut pos, &mut out)
            .await
            .unwrap();
        assert_eq!(pos, (init + 200) as u64);
        assert_eq!(&out[..100], &initial_data[init - 100..init]);
        assert_eq!(&out[100..], &staged[..200]);

        // Read within temp BEFORE flush
        let mut pos2 = init as u64;
        let mut temp_slice = vec![0u8; 512];
        provider
            .read_data_into_buffer(&mut pos2, &mut temp_slice)
            .await
            .unwrap();
        assert_eq!(&temp_slice, &staged[..512]);

        // Start background service which includes the flush loop
        let p = provider.clone();

        tokio::spawn(async move {
            tokio::join!(p.start_service());
        });

        // Wait for flush to complete (temp_len -> 0 and main_len increased)
        let expected_main_after = init + staged.len();
        let mut waited = 0u64;

        loop {
            let ml = provider.file_len.load(Ordering::Acquire) as usize;
            let tl = provider.temp_file_len.load(Ordering::Acquire) as usize;
            if ml == expected_main_after && tl == 0 {
                break;
            }
            if waited > 5_000 {
                // 5s timeout
                panic!("Timed out waiting for background flush: main_len={ml}, temp_len={tl}");
            }
            sleep(Duration::from_millis(10)).await;
            waited += 10;
        }

        // Length checks AFTER flush
        let main_len_after = provider.file_len.load(Ordering::Acquire) as usize;
        let temp_len_after = provider.temp_file_len.load(Ordering::Acquire) as usize;
        let total_len_after = provider.get_len().await as usize;
        assert_eq!(main_len_after, init + staged.len());
        assert_eq!(temp_len_after, 0);
        assert_eq!(total_len_after, init + staged.len());

        // Read appended region AFTER flush (should be from main now)
        let mut pos3 = init as u64;
        let mut out2 = vec![0u8; staged.len()];
        provider
            .read_data_into_buffer(&mut pos3, &mut out2)
            .await
            .unwrap();
        assert_eq!(pos3, (init + staged.len()) as u64);
        assert_eq!(out2, staged);
    }

    #[tokio::test]
    async fn test_read_vectored() {
        let init = 4096usize;
        let (provider, data, _extra) = create_provider(init, 0).await;

        let mut buf1 = vec![0u8; 100];
        let mut buf2 = vec![0u8; 200];
        let mut buf3 = vec![0u8; 50];

        // We need to construct the slice of tuples.
        // Since &mut [u8] is not Copy, we can't easily put them in a vec and then take a slice of mutable references?
        // Wait, `read_vectored` takes `&mut [(u64, &mut [u8])]`.
        // We can create a mutable array or vec of tuples.

        let mut reads: Vec<(u64, &mut [u8])> =
            vec![(0, &mut buf1), (1000, &mut buf2), (4000, &mut buf3)];

        provider.read_vectored(&mut reads).await.unwrap();

        assert_eq!(&buf1[..], &data[0..100]);
        assert_eq!(&buf2[..], &data[1000..1200]);
        assert_eq!(&buf3[..], &data[4000..4050]);
    }

    /// A provider's per-thread cache key is never one a dropped provider used.
    ///
    /// This is the invariant, and it is the one that broke. The cache was keyed
    /// on `Arc::as_ptr(&self._memory_map)` — an address, which is unique only
    /// among the objects alive at one moment. Drop a provider, open another,
    /// and the allocator hands back the same block; with `mmap_gen` at 1 on
    /// both, as it is on every freshly-opened file, nothing else told them
    /// apart and the second file's reads were served from the first file's
    /// mapping. The read reported success and returned another file's bytes at
    /// this file's length.
    ///
    /// Asserted on the key rather than on the bytes because the bytes only go
    /// wrong when the allocator actually reuses the block, and whether it does
    /// is not something a test controls — see
    /// [`a_reopened_file_does_not_read_the_previous_one`], which exercises the
    /// same path and did *not* reproduce it in-process even though a server
    /// reproduced it every time.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_providers_cache_key_is_never_reissued() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_string_lossy().to_string();
        std::fs::write(dir.path().join("first.db"), b"A").unwrap();
        std::fs::write(dir.path().join("second.db"), b"B").unwrap();

        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            for name in ["first.db", "second.db"] {
                let io = LocalStorageProvider::new(&root, Some(name)).await;
                assert!(
                    seen.insert(io.mmap_key),
                    "{name} was given a cache key a dropped provider already used, so a \
                     read through it can be answered from that provider's mapping"
                );
                // A clone maps the file again rather than sharing the handle,
                // so it is a second mapping and needs a key of its own — two
                // containers that can be remapped independently must not share
                // one cache entry.
                assert!(
                    seen.insert(io.clone().mmap_key),
                    "a clone reused a key, but it holds a mapping of its own"
                );
            }
        }
    }

    /// The same hazard from the outside: two files read one after the other,
    /// each provider dropped before the next is opened, must each return their
    /// own bytes.
    ///
    /// Kept even though it does not reproduce the bug in-process. It is the
    /// shape `copy_between` uses — siblings off one parent provider, opened and
    /// dropped in turn — and a future change that breaks the read path in a way
    /// the key invariant does not cover fails here.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reopened_file_does_not_read_the_previous_one() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_string_lossy().to_string();

        // Different lengths, so a wrong answer is a wrong *prefix* rather than
        // only wrong bytes — the way the real corruption showed up.
        let first = vec![b'A'; 306];
        let second = vec![b'B'; 643];
        std::fs::write(dir.path().join("first.db"), &first).unwrap();
        std::fs::write(dir.path().join("second.db"), &second).unwrap();

        // Opened as a table's files are: siblings off one parent provider, each
        // dropped at the end of its turn before the next is opened. That is the
        // shape `copy_between` uses, and the shape that reproduced.
        let parent = LocalStorageProvider::new(&root, Some("parent.db")).await;
        for pass in 0..64 {
            for (name, expected) in [("first.db", &first), ("second.db", &second)] {
                let io = parent.create_new(name.to_string()).await;
                let mut buffer = vec![0u8; expected.len()];
                io.read_data_into_buffer(&mut 0, &mut buffer).await.unwrap();
                assert_eq!(
                    &buffer, expected,
                    "pass {pass}: {name} returned another file's bytes at its own length"
                );
            }
        }
    }
}
