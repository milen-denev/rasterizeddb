//! The durable record of how each transaction ended.
//!
//! Version stamps on disk are only meaningful next to the outcome of the
//! transaction that wrote them: a row stamped `xmin = 41` is either real data or
//! the debris of a transaction that aborted, and nothing in the row itself says
//! which. This is where that is written down, so the answer survives a restart.
//!
//! Two bits per transaction, packed four to a byte and indexed by id, so the
//! whole thing is an array lookup rather than a search. PostgreSQL's `pg_xact`
//! keeps the same shape for the same reason.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use std::sync::atomic::{AtomicUsize, Ordering};

use cacheguard::CacheGuard;

use parking_lot::Mutex;

use super::transaction::{TxId, TxOutcome};
use crate::core::wal::Wal;

/// Bit patterns. `InProgress` is zero so a freshly extended file reads as
/// "started but unfinished", which is the safe reading of a torn write: such a
/// transaction is treated as aborted by anything that outlives it.
const BITS_IN_PROGRESS: u8 = 0b00;
const BITS_COMMITTED: u8 = 0b01;
const BITS_ABORTED: u8 = 0b10;

const ENTRIES_PER_BYTE: u64 = 4;

/// Grow the file in whole pages rather than a byte at a time.
const GROWTH_CHUNK: usize = 4096;

pub struct StatusLog {
    inner: Mutex<Inner>,
    /// Width of the unflushed byte range, readable without taking the lock.
    ///
    /// A read-only transaction skips the durable flush entirely, so on a
    /// read-mostly server nothing would ever clear `dirty` and the range would
    /// grow with the id counter — until the next write had to push hundreds of
    /// megabytes of it out in one go. Callers watch this and fold the range
    /// away with a `flush_deferred` long before it gets there; see
    /// [`StatusLog::dirty_bytes`].
    ///
    /// Padded: it is `fetch_add`ed on every status write while its unpadded
    /// neighbour is the `Mutex` guarding the log itself, so the counter update
    /// and the lock word were contending for one line from different threads.
    dirty_bytes: CacheGuard<AtomicUsize>,
}

impl std::fmt::Debug for StatusLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("StatusLog")
            .field("path", &inner.path)
            .field("highest", &inner.highest)
            .field("logged", &inner.wal.is_some())
            .finish()
    }
}

struct Inner {
    /// The packed bits. Held in memory in full: at two bits each, a billion
    /// transactions is 250 MB, and the working set in practice is a few pages.
    bits: Vec<u8>,
    /// `None` for an in-memory log, which is what tests and `--in-memory` use.
    file: Option<File>,
    path: Option<PathBuf>,
    /// Highest id ever recorded, so ids resume above it after a restart.
    highest: TxId,
    /// The log this file's writes go through, and its id there.
    ///
    /// A commit record has to be exactly as durable as the data it validates,
    /// and by the same mechanism. Written to its own file it is not: a crash can
    /// leave rows recovered from the log whose transaction has no recorded
    /// outcome, so they come back as debris and the write is lost even though
    /// the log had it. Routing this through the same log makes replay restore
    /// both together.
    wal: Option<(std::sync::Arc<Wal>, u64)>,
    /// Byte range touched since the last flush, as `[low, high)`.
    ///
    /// A flush happens per committing transaction, so writing the whole bitmap
    /// each time would make the cost of a commit grow with the number of
    /// transactions the database has ever run.
    dirty: Option<(usize, usize)>,
    /// Ranges that are in the log but not yet copied into the file.
    ///
    /// The copy may only happen once the log record describing it is durable.
    /// In the other order the file can reach the disk carrying an outcome the
    /// log does not have, and a crash there leaves a transaction reading as
    /// committed while the rows it committed are missing — a write reported as
    /// successful that silently is not there.
    ///
    /// Applied in sequence order, so a flusher that was overtaken can never put
    /// an older byte back over a newer one and lose the outcome in it.
    staged: Vec<Staged>,
}

/// A range of the bitmap that has been logged but not yet copied to the file.
struct Staged {
    /// Sequence of the commit marker covering these bytes.
    seq: u64,
    /// Where the range starts, in both the file and the bitmap.
    low: usize,
    bytes: Vec<u8>,
}

impl StatusLog {
    /// A log backed by a file, read into memory if it already exists.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().read(true).write(true).create(true).open(&path)?;

        let mut bits = Vec::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut bits)?;

        // The highest id that has anything but "in progress" recorded. Scanned
        // once at open; a transaction still marked in-progress from a previous
        // run ended by crashing, which is an abort.
        let mut highest = 0u64;
        for (index, byte) in bits.iter().enumerate() {
            if *byte != 0 {
                for slot in 0..ENTRIES_PER_BYTE {
                    if (byte >> (slot * 2)) & 0b11 != BITS_IN_PROGRESS {
                        highest = index as u64 * ENTRIES_PER_BYTE + slot;
                    }
                }
            }
        }

        Ok(StatusLog {
            inner: Mutex::new(Inner {
                bits,
                file: Some(file),
                path: Some(path),
                highest,
                wal: None,
                dirty: None,
                staged: Vec::new(),
            }),
            dirty_bytes: AtomicUsize::new(0).into(),
        })
    }

    /// A log that is never written anywhere.
    ///
    /// Correct for a database with no files: nothing outlives the process, so
    /// there is nothing for the record to be consulted by.
    pub fn in_memory() -> Self {
        StatusLog {
            inner: Mutex::new(Inner {
                bits: Vec::new(),
                file: None,
                path: None,
                highest: 0,
                wal: None,
                dirty: None,
                staged: Vec::new(),
            }),
            dirty_bytes: AtomicUsize::new(0).into(),
        }
    }

    /// Records how `tx` ended.
    pub fn record(&self, tx: TxId, outcome: TxOutcome) {
        let bits = match outcome {
            TxOutcome::InProgress => BITS_IN_PROGRESS,
            TxOutcome::Committed => BITS_COMMITTED,
            TxOutcome::Aborted => BITS_ABORTED,
        };

        let mut inner = self.inner.lock();
        let index = (tx / ENTRIES_PER_BYTE) as usize;
        if index >= inner.bits.len() {
            let target = (index + 1).next_multiple_of(GROWTH_CHUNK);
            inner.bits.resize(target, 0);
        }

        let shift = (tx % ENTRIES_PER_BYTE) * 2;
        let byte = &mut inner.bits[index];
        *byte = (*byte & !(0b11 << shift)) | (bits << shift);

        if tx > inner.highest {
            inner.highest = tx;
        }
        let dirty = match inner.dirty {
            Some((low, high)) => (low.min(index), high.max(index + 1)),
            None => (index, index + 1),
        };
        inner.dirty = Some(dirty);
        // Published for `dirty_bytes`, which a read-only statement consults
        // without taking this lock. Written under it, so the two never disagree.
        self.dirty_bytes.store(dirty.1 - dirty.0, Ordering::Relaxed);
    }

    /// How `tx` ended.
    ///
    /// An id the log has never heard of reads as aborted rather than in
    /// progress: either it was never assigned, or it was assigned by a run that
    /// crashed before recording anything. Both mean its writes must not be seen.
    pub fn outcome(&self, tx: TxId) -> TxOutcome {
        let inner = self.inner.lock();
        let index = (tx / ENTRIES_PER_BYTE) as usize;
        let Some(byte) = inner.bits.get(index) else {
            return TxOutcome::Aborted;
        };
        let shift = (tx % ENTRIES_PER_BYTE) * 2;
        match (byte >> shift) & 0b11 {
            BITS_COMMITTED => TxOutcome::Committed,
            BITS_ABORTED => TxOutcome::Aborted,
            // In progress. To a caller that is not the transaction itself this
            // is indistinguishable from aborted and must be treated as such.
            _ => TxOutcome::InProgress,
        }
    }

    /// The highest id this log has a record of.
    pub fn highest_recorded(&self) -> TxId {
        self.inner.lock().highest
    }

    /// Writes the record to disk and fsyncs it.
    ///
    /// Called on commit, because a commit that is not durable is not a commit:
    /// the rows would come back after a restart with their transaction recorded
    /// as unfinished, and be read as debris.
    ///
    /// The `fdatasync` is taken with no lock held. Holding this one across it
    /// put every committing transaction in the process behind whichever one was
    /// syncing, one at a time, which defeated the log's own group commit:
    /// concurrent flushers there collapse into a single sync, and this lock was
    /// what stopped them from ever being concurrent. Eight clients doing
    /// read-only statements cost 126 syncs where 16 would have done.
    pub fn flush(&self) -> io::Result<()> {
        let Some((wal, seq)) = self.stage()? else {
            return Ok(());
        };
        wal.commit_through(seq)?;
        self.apply_durable(&wal)
    }

    /// Writes the record, leaving its `fdatasync` to the caller.
    ///
    /// The sync is owed to [`Wal::sync_pending`], so a statement about to take
    /// one for its own rows pays for its outcome record with the same one —
    /// the two were separate syncs of the same file, back to back, and cost an
    /// `INSERT` twice what it needed to. Nothing may report success before that
    /// sync has run; [`StatusLog::flush`] is the variant whose contract is that
    /// it is durable when it returns.
    ///
    /// The caller takes its sync and then calls [`StatusLog::apply_staged`].
    pub fn flush_deferred(&self) -> io::Result<()> {
        self.stage()?;
        Ok(())
    }

    /// Copies over any staged range the log has since made durable.
    ///
    /// Call after the sync that [`StatusLog::flush_deferred`] left owed.
    pub fn apply_staged(&self) -> io::Result<()> {
        let wal = self.inner.lock().wal.as_ref().map(|(w, _)| w.clone());
        match wal {
            Some(wal) => self.apply_durable(&wal),
            None => Ok(()),
        }
    }

    /// Width of the range waiting to be written, without taking the lock.
    ///
    /// A read-only transaction skips the flush entirely, so on a read-mostly
    /// server nothing clears `dirty` and the range grows with the id counter —
    /// until the next write has to push the whole accumulation out in one go.
    /// Callers watch this and fold it away with a `flush_deferred`, which costs
    /// no sync, long before it gets that far.
    pub fn dirty_bytes(&self) -> usize {
        self.dirty_bytes.load(Ordering::Relaxed)
    }

    /// Puts the dirty bytes in the log and queues them for the file.
    ///
    /// Returns the log and the sequence the sync is owed on, or `None` when
    /// there was nothing to write or nowhere to write it.
    fn stage(&self) -> io::Result<Option<(std::sync::Arc<Wal>, u64)>> {
        let mut inner = self.inner.lock();
        let Some((low, high)) = inner.dirty else {
            return Ok(None);
        };
        if inner.file.is_none() {
            inner.dirty = None;
            self.dirty_bytes.store(0, Ordering::Relaxed);
            return Ok(None);
        }

        // With no log the file is the only record, so it is written and synced
        // here and there is nothing to stage.
        let Some((wal, file_id)) = inner.wal.clone() else {
            let Inner { bits, file, .. } = &mut *inner;
            let file = file.as_mut().expect("checked above");
            file.seek(SeekFrom::Start(low as u64))?;
            file.write_all(&bits[low..high])?;
            file.sync_data()?;
            inner.dirty = None;
            self.dirty_bytes.store(0, Ordering::Relaxed);
            return Ok(None);
        };

        let seq = wal.log_and_commit_deferred(file_id, low as u64, &inner.bits[low..high])?;

        // The file is written too, even when logging. A checkpoint does not
        // replay records into files — it fsyncs the files their writers already
        // wrote and then discards the log. A participant that wrote only to the
        // log would have its records thrown away at the first checkpoint, which
        // is exactly how `outcomes_survive_the_log_being_checkpointed` failed:
        // every outcome older than the last checkpoint came back as unfinished,
        // so committed rows read as debris.
        //
        // It is queued rather than written here, because it may not reach the
        // disk before the record that justifies it. The file is not fsynced, so
        // the kernel is free to write its page back at any time; if that landed
        // before the log's own sync and the machine then died, the file would
        // name a transaction as committed whose rows never became durable.
        let bytes = inner.bits[low..high].to_vec();
        inner.staged.push(Staged { seq, low, bytes });
        inner.dirty = None;
        self.dirty_bytes.store(0, Ordering::Relaxed);
        Ok(Some((wal, seq)))
    }

    /// Copies staged ranges into the file, oldest first, stopping at the first
    /// one the log has not made durable yet.
    fn apply_durable(&self, wal: &Wal) -> io::Result<()> {
        let durable = wal.durable_through();
        let mut inner = self.inner.lock();
        if inner.staged.is_empty() {
            return Ok(());
        }

        // Sequence order. Two ranges can share a byte — four transactions share
        // every byte — so writing an older snapshot after a newer one would put
        // the byte back the way it was and lose the outcome recorded in it.
        inner.staged.sort_by_key(|s| s.seq);
        let ready = inner.staged.iter().take_while(|s| s.seq <= durable).count();
        if ready == 0 {
            return Ok(());
        }

        let batch: Vec<Staged> = inner.staged.drain(..ready).collect();
        let mut rest = batch.into_iter();
        while let Some(s) = rest.next() {
            let file = inner.file.as_mut().expect("staged only when there is a file");
            let written = file
                .seek(SeekFrom::Start(s.low as u64))
                .and_then(|_| file.write_all(&s.bytes));
            if let Err(e) = written {
                // Put back what did not go out. Dropping it would leave the
                // file missing outcomes that only the log still has, and the
                // next checkpoint would discard the log over the top of them.
                inner.staged.push(s);
                inner.staged.extend(rest);
                inner.staged.sort_by_key(|s| s.seq);
                return Err(e);
            }
        }
        Ok(())
    }

    pub fn path(&self) -> Option<PathBuf> {
        self.inner.lock().path.clone()
    }

    /// Routes this log's writes through `wal`, so an outcome and the rows it
    /// validates are recovered together.
    ///
    /// Registering the file is what lets replay write back into it. Without the
    /// registration the log would hold records addressed to a file it does not
    /// know how to open, and would drop them.
    pub fn attach_wal(&self, wal: std::sync::Arc<Wal>) {
        let mut inner = self.inner.lock();
        let Some(path) = inner.path.clone() else {
            return;
        };
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            return;
        };
        let file_id = crate::core::wal::file_id_for_name(name);
        wal.register_file(file_id, path);
        inner.wal = Some((wal, file_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_outcome_survives_being_written_and_read_back() {
        let log = StatusLog::in_memory();
        log.record(7, TxOutcome::Committed);
        log.record(8, TxOutcome::Aborted);
        log.record(9, TxOutcome::InProgress);

        assert_eq!(log.outcome(7), TxOutcome::Committed);
        assert_eq!(log.outcome(8), TxOutcome::Aborted);
        assert_eq!(log.outcome(9), TxOutcome::InProgress);
    }

    /// Four entries share a byte, so writing one must not disturb its
    /// neighbours.
    #[test]
    fn neighbouring_ids_do_not_overwrite_each_other() {
        let log = StatusLog::in_memory();
        for id in 4..12 {
            log.record(id, if id % 2 == 0 { TxOutcome::Committed } else { TxOutcome::Aborted });
        }
        for id in 4..12 {
            let expected =
                if id % 2 == 0 { TxOutcome::Committed } else { TxOutcome::Aborted };
            assert_eq!(log.outcome(id), expected, "id {}", id);
        }
    }

    /// An outcome can be rewritten — a transaction is recorded as in-progress
    /// when it starts and again when it ends.
    #[test]
    fn an_outcome_can_be_replaced_in_place() {
        let log = StatusLog::in_memory();
        log.record(5, TxOutcome::InProgress);
        log.record(6, TxOutcome::Committed);
        log.record(5, TxOutcome::Committed);

        assert_eq!(log.outcome(5), TxOutcome::Committed);
        assert_eq!(log.outcome(6), TxOutcome::Committed, "the neighbour is untouched");
    }

    /// An id from a run that crashed before recording anything must not be read
    /// as live data.
    #[test]
    fn an_unknown_id_reads_as_aborted() {
        let log = StatusLog::in_memory();
        assert_eq!(log.outcome(99_999), TxOutcome::Aborted);
    }

    #[test]
    fn outcomes_survive_a_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");

        {
            let log = StatusLog::open(&path).expect("open");
            log.record(2, TxOutcome::Committed);
            log.record(3, TxOutcome::Aborted);
            log.record(40, TxOutcome::Committed);
            log.flush().expect("flush");
        }

        let log = StatusLog::open(&path).expect("reopen");
        assert_eq!(log.outcome(2), TxOutcome::Committed);
        assert_eq!(log.outcome(3), TxOutcome::Aborted);
        assert_eq!(log.outcome(40), TxOutcome::Committed);
        assert_eq!(log.highest_recorded(), 40, "ids must resume above what was used");
    }

    /// The unflushed span is what a read-only statement watches to decide when
    /// the accumulation is worth folding away.
    #[test]
    fn the_dirty_span_grows_with_records_and_clears_on_flush() {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = StatusLog::open(dir.path().join("status")).expect("open");
        assert_eq!(log.dirty_bytes(), 0, "nothing recorded yet");

        log.record(4, TxOutcome::Committed);
        let one = log.dirty_bytes();
        assert!(one > 0, "a record left nothing dirty");

        // Four ids to a byte, so this is a byte apiece and the span widens.
        log.record(4_000, TxOutcome::Committed);
        assert!(log.dirty_bytes() > one, "the span did not widen: {}", log.dirty_bytes());

        log.flush().expect("flush");
        assert_eq!(log.dirty_bytes(), 0, "the flush did not clear the span");
    }

    /// The file copy may not reach the disk before the log record that
    /// justifies it.
    ///
    /// Written the other way round, a crash in between leaves the file naming a
    /// transaction as committed whose rows never became durable — a write
    /// reported as successful that silently is not there. So a deferred flush
    /// stages the bytes and only `apply_staged` puts them in the file, once the
    /// log holding them has been synced.
    #[test]
    fn a_deferred_flush_reaches_the_file_only_after_the_log_is_durable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");
        let log = StatusLog::open(&path).expect("open");
        let wal = std::sync::Arc::new(Wal::open(dir.path().join("test.wal")).expect("wal"));
        log.attach_wal(wal.clone());

        // id 7 lives in byte 1, in the top two bits of it.
        log.record(7, TxOutcome::Committed);
        log.flush_deferred().expect("deferred flush");

        let staged = std::fs::read(&path).expect("read");
        assert!(
            staged.get(1).copied().unwrap_or(0) == 0,
            "the bytes reached the file before the log was durable: {:?}",
            staged
        );
        // Applying now must do nothing: the record is not durable yet.
        log.apply_staged().expect("apply with nothing durable");
        let still = std::fs::read(&path).expect("read");
        assert!(still.get(1).copied().unwrap_or(0) == 0, "applied before the sync");

        // The sync the caller owed, then the copy.
        wal.sync_pending().expect("sync");
        log.apply_staged().expect("apply");

        let applied = std::fs::read(&path).expect("read");
        let expected = BITS_COMMITTED << ((7 % ENTRIES_PER_BYTE) * 2);
        assert_eq!(
            applied.get(1).copied().unwrap_or(0),
            expected,
            "the outcome never reached the file"
        );
    }

    /// `flush` is the durable variant: the bytes are in the file by the time it
    /// returns, with no second call needed.
    #[test]
    fn a_plain_flush_is_durable_on_return_even_with_a_log_attached() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");
        let log = StatusLog::open(&path).expect("open");
        let wal = std::sync::Arc::new(Wal::open(dir.path().join("test.wal")).expect("wal"));
        log.attach_wal(wal);

        log.record(7, TxOutcome::Committed);
        log.flush().expect("flush");

        let bytes = std::fs::read(&path).expect("read");
        let expected = BITS_COMMITTED << ((7 % ENTRIES_PER_BYTE) * 2);
        assert_eq!(bytes.get(1).copied().unwrap_or(0), expected, "flush was not durable");
    }

    /// A staged range left over from a deferred flush is not lost when a later
    /// flush comes along — both end up in the file.
    #[test]
    fn a_deferred_range_is_still_applied_by_a_later_flush() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");
        let log = StatusLog::open(&path).expect("open");
        let wal = std::sync::Arc::new(Wal::open(dir.path().join("test.wal")).expect("wal"));
        log.attach_wal(wal);

        log.record(7, TxOutcome::Committed);
        log.flush_deferred().expect("deferred");
        log.record(4_000, TxOutcome::Aborted);
        log.flush().expect("flush");

        let bytes = std::fs::read(&path).expect("read");
        assert_eq!(
            bytes.get(1).copied().unwrap_or(0),
            BITS_COMMITTED << ((7 % ENTRIES_PER_BYTE) * 2),
            "the deferred range was dropped"
        );
        let far = (4_000 / ENTRIES_PER_BYTE) as usize;
        assert_eq!(
            bytes.get(far).copied().unwrap_or(0),
            BITS_ABORTED << ((4_000 % ENTRIES_PER_BYTE) * 2),
            "the later range never landed"
        );
    }

    /// A transaction still marked in-progress at reopen ended by crashing.
    #[test]
    fn a_transaction_interrupted_by_a_crash_is_not_committed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("status");

        {
            let log = StatusLog::open(&path).expect("open");
            log.record(2, TxOutcome::Committed);
            log.record(3, TxOutcome::InProgress);
            log.flush().expect("flush");
        }

        let log = StatusLog::open(&path).expect("reopen");
        assert_ne!(log.outcome(3), TxOutcome::Committed);
    }
}
