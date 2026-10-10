//! The history a node keeps so a peer that fell behind can catch up, and the
//! record of how far it has itself caught up.
//!
//! Two pieces of state, with opposite jobs:
//!
//! - [`ReplicationLog`] — what *this* node committed, kept so a peer that was
//!   down or slow can ask for what it missed. Bounded by a retention budget: a
//!   peer that falls further behind than the budget is told to resynchronise
//!   rather than being served a gap.
//! - [`AppliedState`] — how far this node has applied *each other* origin's
//!   stream, so a restart does not replay what it already took, and so a
//!   handshake can tell a peer exactly what to resend.
//!
//! ## What durability means here
//!
//! Neither file is a source of truth for user data — the WAL and the
//! transaction status log are, and they are unchanged. These two are
//! positioning metadata, and the failure they have to survive is a crash
//! between applying a remote transaction and recording that it was applied.
//!
//! That window is resolved in the direction of **at-least-once**: the applied
//! position is recorded after the transaction commits, so a crash inside the
//! window replays one transaction on restart rather than dropping it. Replaying
//! it re-runs statements that already landed, which the conflict policy then
//! sees — a duplicate key, usually. Losing it instead would be silent
//! divergence, which is worse and undetectable. See
//! [`super::config::ConflictPolicy`].
//!
//! The log itself is buffered rather than flushed per commit, for the same
//! reason a page cache exists: a commit's own durability is already paid for by
//! the WAL, and adding a second `fsync` to the commit path to protect a
//! catch-up cache would be a poor trade. A crash that loses the buffered tail
//! costs a peer its incremental catch-up, and it resynchronises instead.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use cacheguard::CacheGuard;
use parking_lot::Mutex;

use crate::core::storage_providers::CRC;

use super::config::NodeId;
use super::hlc::{Hlc, NO_HLC};
use super::protocol::{Reader, TxnRecord, VectorEntry};

/// Starts every log record. A file that does not begin with one is not this
/// file, and a record that does not is where the log was torn.
const RECORD_MAGIC: u32 = 0x5252_4c47; // "RRLG"

/// `magic`, `len`, `crc`.
const RECORD_HEADER: usize = 12;

pub const LOG_FILE: &str = "db_replication.log";
pub const STATE_FILE: &str = "db_replication.state";

/// Where one record sits in the file.
#[derive(Clone, Copy, Debug)]
struct RecordLoc {
    offset: u64,
    len: u32,
}

/// One origin's sequence -> location map.
///
/// Sequences are dense in practice — one origin numbers its transactions
/// 1, 2, 3, … — so the common shape is a `VecDeque` of 12-byte slots indexed
/// from `base`, against roughly 48 bytes an entry for the `BTreeMap` it
/// replaced (half-full leaves under sequential insertion).
///
/// Not *strictly* dense, so holes are allowed: two commits take sequences 5
/// and 6 and may append 6 first, a failed append leaves its number unused, and
/// a torn tail or a compaction can drop one. A hole is a slot whose offset is
/// `HOLE`. A jump wide enough that holes would cost more than the map did
/// switches that origin to the map, so no sequence pattern can blow up memory.
#[derive(Debug)]
enum SeqIndex {
    Dense {
        base: u64,
        slots: std::collections::VecDeque<Slot>,
    },
    Sparse(BTreeMap<u64, RecordLoc>),
}

/// A packed `RecordLoc`: 12 bytes, where the plain struct pads to 16.
#[derive(Clone, Copy, Debug)]
#[repr(C, packed(4))]
struct Slot {
    offset: u64,
    len: u32,
}

const HOLE: u64 = u64::MAX;

/// The most holes one insert may add before the origin goes sparse (or the
/// slot count, if larger: holes never more than double the memory).
const MAX_HOLE_RUN: u64 = 4096;

impl Default for SeqIndex {
    fn default() -> Self {
        SeqIndex::Dense {
            base: 0,
            slots: std::collections::VecDeque::new(),
        }
    }
}

impl SeqIndex {
    fn insert(&mut self, seq: u64, loc: RecordLoc) {
        let slot = Slot {
            offset: loc.offset,
            len: loc.len,
        };
        match self {
            SeqIndex::Sparse(map) => {
                map.insert(seq, loc);
            }
            SeqIndex::Dense { base, slots } => {
                if slots.is_empty() {
                    *base = seq;
                    slots.push_back(slot);
                    return;
                }
                let end = *base + slots.len() as u64;
                let holes = if seq >= end {
                    seq - end
                } else if seq < *base {
                    *base - seq - 1
                } else {
                    0
                };
                if holes > MAX_HOLE_RUN.max(slots.len() as u64) {
                    let map: BTreeMap<u64, RecordLoc> = self.iter().collect();
                    *self = SeqIndex::Sparse(map);
                    self.insert(seq, loc);
                    return;
                }
                if seq >= end {
                    for _ in 0..holes {
                        slots.push_back(Slot { offset: HOLE, len: 0 });
                    }
                    slots.push_back(slot);
                } else if seq < *base {
                    for _ in 0..holes {
                        slots.push_front(Slot { offset: HOLE, len: 0 });
                    }
                    slots.push_front(slot);
                    *base = seq;
                } else {
                    slots[(seq - *base) as usize] = slot;
                }
            }
        }
    }

    /// The lowest sequence held. A dense index never has a hole at either end:
    /// holes are only ever added between an existing slot and a new one.
    fn first(&self) -> Option<u64> {
        match self {
            SeqIndex::Sparse(map) => map.keys().next().copied(),
            SeqIndex::Dense { base, slots } => (!slots.is_empty()).then_some(*base),
        }
    }

    fn last(&self) -> Option<u64> {
        match self {
            SeqIndex::Sparse(map) => map.keys().next_back().copied(),
            SeqIndex::Dense { base, slots } => {
                (!slots.is_empty()).then(|| *base + slots.len() as u64 - 1)
            }
        }
    }

    /// Every held `(seq, loc)` with `seq >= from`, ascending.
    fn iter_from(&self, from: u64) -> Box<dyn Iterator<Item = (u64, RecordLoc)> + '_> {
        match self {
            SeqIndex::Sparse(map) => Box::new(map.range(from..).map(|(s, l)| (*s, *l))),
            SeqIndex::Dense { base, slots } => {
                let base = *base;
                let skip = from.saturating_sub(base).min(slots.len() as u64) as usize;
                Box::new(
                    slots
                        .iter()
                        .enumerate()
                        .skip(skip)
                        .filter(|(_, slot)| slot.offset != HOLE)
                        .map(move |(i, slot)| {
                            (
                                base + i as u64,
                                RecordLoc {
                                    offset: slot.offset,
                                    len: slot.len,
                                },
                            )
                        }),
                )
            }
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (u64, RecordLoc)> + '_> {
        self.iter_from(0)
    }
}

/// Records kept in memory to answer a catch-up without touching the disk.
///
/// A peer that is behind is almost always behind by one batch — it reconnected,
/// or a send timed out — so the newest few thousand records answer nearly every
/// request. Older ones are read back from the file.
const HOT_RECORDS: usize = 4096;

struct LogFile {
    writer: BufWriter<File>,
    path: PathBuf,
    /// Bytes written, which is also the offset the next record goes to.
    end: u64,
    /// Offset of the oldest record still in the file. Non-zero after a
    /// compaction has dropped a prefix.
    start: u64,
}

/// This node's own committed history, as far back as the retention budget goes.
pub struct ReplicationLog {
    /// `None` for an in-memory database, which keeps only the hot window: there
    /// is nothing to be durable *for*, since the data itself is not.
    file: Mutex<Option<LogFile>>,
    /// Per origin, sequence to file location. A `BTreeMap` because every read
    /// is "everything after N", which is exactly a range scan.
    index: Mutex<BTreeMap<NodeId, SeqIndex>>,
    /// The newest records, ready to serve without a read.
    hot: Mutex<std::collections::VecDeque<TxnRecord>>,
    retention_bytes: u64,
    /// Padded off the two `Mutex` fields above it. Every append does a
    /// `fetch_max` here while holding neither lock, so without the guard the
    /// update invalidated the line carrying the mutex words that the next
    /// appender was about to acquire.
    last_hlc: CacheGuard<std::sync::atomic::AtomicU64>,
}

impl std::fmt::Debug for ReplicationLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationLog")
            .field("retention_bytes", &self.retention_bytes)
            .finish()
    }
}

/// `fdatasync`s a file through a duplicate descriptor, with no lock held.
fn sync_unlocked(handle: &File) -> io::Result<()> {
    #[cfg(test)]
    {
        let delay = test_hooks::SYNC_DELAY.with(|d| d.get());
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
    }
    handle.sync_data()
}

#[cfg(test)]
mod test_hooks {
    thread_local! {
        /// Makes a sync on this thread slow enough for a test to observe
        /// whether anything else waits for it.
        pub static SYNC_DELAY: std::cell::Cell<std::time::Duration> =
            const { std::cell::Cell::new(std::time::Duration::ZERO) };

        /// Runs inside `AppliedState::rewrite` between copying the positions
        /// and swapping the file, to put a `record` exactly in that window.
        pub static BEFORE_STATE_SWAP: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
            const { std::cell::RefCell::new(None) };
    }
}

/// Writes one framed record and returns how many bytes it took.
fn write_record(writer: &mut impl Write, payload: &[u8]) -> io::Result<u64> {
    let crc = CRC.checksum(payload);
    writer.write_all(&RECORD_MAGIC.to_le_bytes())?;
    writer.write_all(&(payload.len() as u32).to_le_bytes())?;
    writer.write_all(&crc.to_le_bytes())?;
    writer.write_all(payload)?;
    Ok((RECORD_HEADER + payload.len()) as u64)
}

impl ReplicationLog {
    /// A log with no file behind it.
    pub fn in_memory(retention_bytes: u64) -> ReplicationLog {
        ReplicationLog {
            file: Mutex::new(None),
            index: Mutex::new(BTreeMap::new()),
            hot: Mutex::new(std::collections::VecDeque::with_capacity(HOT_RECORDS)),
            retention_bytes,
            last_hlc: std::sync::atomic::AtomicU64::new(NO_HLC).into(),
        }
    }

    /// Opens the log in `dir`, rebuilding its index from what is already there.
    ///
    /// A torn tail — the last record half-written when the process died — is
    /// truncated rather than rejected: it is the expected shape of a crash, and
    /// everything before it is still good.
    pub fn open(dir: &Path, retention_bytes: u64) -> io::Result<ReplicationLog> {
        let path = dir.join(LOG_FILE);
        let mut handle = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let mut index: BTreeMap<NodeId, SeqIndex> = BTreeMap::new();
        let mut hot = std::collections::VecDeque::with_capacity(HOT_RECORDS);
        let mut last_hlc = NO_HLC;

        let mut bytes = Vec::new();
        handle.read_to_end(&mut bytes)?;

        let mut at = 0usize;
        let good_end = loop {
            if at + RECORD_HEADER > bytes.len() {
                break at;
            }
            let magic = u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4 bytes"));
            if magic != RECORD_MAGIC {
                log::warn!(
                    "Replication log {} has an unrecognised record at byte {}; truncating there",
                    path.display(),
                    at
                );
                break at;
            }
            let len =
                u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("4 bytes")) as usize;
            let crc = u32::from_le_bytes(bytes[at + 8..at + 12].try_into().expect("4 bytes"));

            let payload_at = at + RECORD_HEADER;
            let Some(payload) = bytes.get(payload_at..payload_at + len) else {
                // A record whose length runs past the end is the torn tail.
                break at;
            };
            if CRC.checksum(payload) != crc {
                log::warn!(
                    "Replication log {} fails its checksum at byte {}; truncating there",
                    path.display(),
                    at
                );
                break at;
            }

            match TxnRecord::decode(&mut Reader::new(payload)) {
                Ok(record) => {
                    index.entry(record.origin).or_default().insert(
                        record.seq,
                        RecordLoc {
                            offset: at as u64,
                            len: len as u32,
                        },
                    );
                    last_hlc = last_hlc.max(record.commit_hlc);
                    if hot.len() == HOT_RECORDS {
                        hot.pop_front();
                    }
                    hot.push_back(record);
                }
                Err(e) => {
                    log::warn!(
                        "Replication log {} holds an undecodable record at byte {} ({}); truncating there",
                        path.display(),
                        at,
                        e
                    );
                    break at;
                }
            }

            at = payload_at + len;
        };

        if (good_end as u64) < bytes.len() as u64 {
            handle.set_len(good_end as u64)?;
        }
        handle.seek(SeekFrom::Start(good_end as u64))?;

        log::info!(
            "Replication log {} opened: {} bytes, {} origin(s)",
            path.display(),
            good_end,
            index.len()
        );

        Ok(ReplicationLog {
            file: Mutex::new(Some(LogFile {
                writer: BufWriter::with_capacity(64 * 1024, handle),
                path,
                end: good_end as u64,
                start: 0,
            })),
            index: Mutex::new(index),
            hot: Mutex::new(hot),
            retention_bytes,
            last_hlc: std::sync::atomic::AtomicU64::new(last_hlc).into(),
        })
    }

    /// The highest commit timestamp this log has seen, so a restarting node can
    /// seed its clock above what it already published.
    pub fn last_hlc(&self) -> Hlc {
        self.last_hlc.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Adds a record. Cheap: an encode and a buffered write, no `fsync`.
    pub fn append(&self, record: &TxnRecord) -> io::Result<()> {
        let mut payload = Vec::with_capacity(record.encoded_size_hint());
        record.encode(&mut payload);

        {
            let mut file = self.file.lock();
            if let Some(log) = file.as_mut() {
                let offset = log.end;
                let written = write_record(&mut log.writer, &payload)?;
                log.end += written;

                self.index.lock().entry(record.origin).or_default().insert(
                    record.seq,
                    RecordLoc {
                        offset,
                        len: payload.len() as u32,
                    },
                );
            }
        }

        self.last_hlc
            .fetch_max(record.commit_hlc, std::sync::atomic::Ordering::AcqRel);

        {
            let mut hot = self.hot.lock();
            if hot.len() == HOT_RECORDS {
                hot.pop_front();
            }
            hot.push_back(record.clone());
        }

        self.compact_if_needed()?;
        Ok(())
    }

    /// Makes everything appended so far durable. Called by the periodic
    /// flusher and at shutdown.
    ///
    /// The buffer is handed to the kernel under the `file` lock, and the
    /// `fdatasync` runs on a duplicate of the descriptor *after* the lock is
    /// released. Every local commit appends under that lock, so syncing while
    /// holding it stalled all committers for one fsync per flusher tick.
    ///
    /// Durability is unchanged: every byte appended before this call was
    /// written to the inode before the lock was dropped, and the duplicate
    /// syncs that same inode. If a compaction swaps the file in between, it
    /// copied those bytes into its replacement and synced the replacement
    /// before renaming it into place, so they are durable either way.
    pub fn flush(&self) -> io::Result<()> {
        let handle = {
            let mut file = self.file.lock();
            match file.as_mut() {
                Some(log) => {
                    log.writer.flush()?;
                    log.writer.get_ref().try_clone()?
                }
                None => return Ok(()),
            }
        };
        sync_unlocked(&handle)
    }

    /// Bytes the log currently occupies.
    pub fn bytes(&self) -> u64 {
        self.file
            .lock()
            .as_ref()
            .map(|log| log.end - log.start)
            .unwrap_or(0)
    }

    /// The newest sequence held for `origin`, or 0.
    ///
    /// Falls back to the in-memory window when there is no file: without one
    /// the index is never populated, and answering 0 there would tell a caller
    /// the log holds nothing when it holds the last few thousand records.
    pub fn highest(&self, origin: NodeId) -> u64 {
        let indexed = self
            .index
            .lock()
            .get(&origin)
            .and_then(SeqIndex::last)
            .unwrap_or(0);
        if indexed != 0 {
            return indexed;
        }
        self.hot
            .lock()
            .iter()
            .rev()
            .find(|record| record.origin == origin)
            .map(|record| record.seq)
            .unwrap_or(0)
    }

    /// The oldest sequence still held for `origin`, or 0 when none is.
    ///
    /// A peer asking for something below this cannot be served incrementally:
    /// retention has dropped it, and the answer is a resynchronisation.
    pub fn earliest(&self, origin: NodeId) -> u64 {
        let indexed = self
            .index
            .lock()
            .get(&origin)
            .and_then(SeqIndex::first)
            .unwrap_or(0);
        if indexed != 0 {
            return indexed;
        }
        // Same reasoning as `highest`: with no file, the window *is* the log,
        // and its oldest record is the real retention boundary. Reporting 0
        // would claim everything is still available and then quietly serve a
        // short answer, which is the one outcome a catch-up must never produce.
        self.hot
            .lock()
            .iter()
            .find(|record| record.origin == origin)
            .map(|record| record.seq)
            .unwrap_or(0)
    }

    /// Whether `after_seq` can still be caught up from here.
    pub fn can_serve(&self, origin: NodeId, after_seq: u64) -> bool {
        let earliest = self.earliest(origin);
        // Nothing held for this origin at all: there is nothing to be missing.
        earliest == 0 || after_seq + 1 >= earliest
    }

    /// Everything held for `origin` after `after_seq`, in order.
    ///
    /// Returns the records and whether more remain past the limits. Serves from
    /// the hot window without touching the disk whenever the request is covered
    /// by it, which is the usual case.
    pub fn read_after(
        &self,
        origin: NodeId,
        after_seq: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> io::Result<(Vec<TxnRecord>, bool)> {
        if max_records == 0 {
            return Ok((Vec::new(), self.highest(origin) > after_seq));
        }

        if let Some(served) = self.read_hot(origin, after_seq, max_records, max_bytes) {
            return Ok(served);
        }

        let locations: Vec<RecordLoc> = {
            let index = self.index.lock();
            match index.get(&origin) {
                // `after_seq + 1..` rather than an exclusive bound on `after_seq`
                // so that `after_seq == u64::MAX` cannot overflow into an empty
                // range that silently reports "nothing to send".
                Some(per_origin) => per_origin
                    .iter_from(after_seq.saturating_add(1))
                    .map(|(_, loc)| loc)
                    .take(max_records + 1)
                    .collect(),
                None => Vec::new(),
            }
        };

        if locations.is_empty() {
            return Ok((Vec::new(), false));
        }

        let mut records = Vec::with_capacity(locations.len().min(max_records));
        let mut bytes = 0usize;
        let mut more = false;

        let mut file = self.file.lock();
        let Some(log) = file.as_mut() else {
            // In-memory: whatever the hot window could not serve is gone.
            return Ok((Vec::new(), false));
        };
        // The record being read may still be sitting in the write buffer.
        log.writer.flush()?;
        let handle = log.writer.get_mut();

        let mut payload = Vec::new();
        for loc in locations {
            if records.len() == max_records || (bytes >= max_bytes && !records.is_empty()) {
                more = true;
                break;
            }

            handle.seek(SeekFrom::Start(loc.offset + RECORD_HEADER as u64))?;
            payload.clear();
            payload.resize(loc.len as usize, 0);
            handle.read_exact(&mut payload)?;

            match TxnRecord::decode(&mut Reader::new(&payload)) {
                Ok(record) => {
                    bytes += loc.len as usize;
                    records.push(record);
                }
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "replication log record at {} is unreadable: {}",
                            loc.offset, e
                        ),
                    ));
                }
            }
        }

        Ok((records, more))
    }

    /// Serves a request entirely from the in-memory window, or declines.
    ///
    /// Declining is the point: if the window does not reach back to
    /// `after_seq`, the caller must go to the file, and a partial answer from
    /// here would look like a complete one with a gap in it.
    fn read_hot(
        &self,
        origin: NodeId,
        after_seq: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> Option<(Vec<TxnRecord>, bool)> {
        let hot = self.hot.lock();

        let oldest_for_origin = hot.iter().find(|r| r.origin == origin)?.seq;
        if oldest_for_origin > after_seq + 1 {
            return None;
        }

        let mut records = Vec::new();
        let mut bytes = 0usize;
        let mut more = false;
        for record in hot
            .iter()
            .filter(|r| r.origin == origin && r.seq > after_seq)
        {
            if records.len() == max_records || (bytes >= max_bytes && !records.is_empty()) {
                more = true;
                break;
            }
            bytes += record.encoded_size_hint();
            records.push(record.clone());
        }

        Some((records, more))
    }

    /// Drops the oldest half of the file once it outgrows its budget.
    ///
    /// Rewriting rather than truncating in place: the records are variable
    /// length, so there is no cheaper way to drop a prefix, and halving means
    /// the rewrite happens once per half-budget of traffic rather than once per
    /// record.
    fn compact_if_needed(&self) -> io::Result<()> {
        if self.retention_bytes == 0 {
            return Ok(());
        }
        {
            let file = self.file.lock();
            match file.as_ref() {
                Some(log) if log.end - log.start > self.retention_bytes => {}
                _ => return Ok(()),
            }
        }

        let mut file = self.file.lock();
        let Some(log) = file.as_mut() else {
            return Ok(());
        };
        if log.end - log.start <= self.retention_bytes {
            // Someone else compacted while this call waited for the lock.
            return Ok(());
        }

        log.writer.flush()?;

        // Keep everything from the point where the newest half begins.
        let keep_from = log.end - self.retention_bytes / 2;
        let mut index = self.index.lock();

        let mut kept: Vec<(NodeId, u64, RecordLoc)> = Vec::new();
        for (origin, per_origin) in index.iter() {
            for (seq, loc) in per_origin.iter() {
                if loc.offset >= keep_from {
                    kept.push((*origin, seq, loc));
                }
            }
        }
        kept.sort_unstable_by_key(|(_, _, loc)| loc.offset);

        let temp_path = log.path.with_extension("compacting");
        let mut source = OpenOptions::new().read(true).open(&log.path)?;
        let mut target = BufWriter::with_capacity(64 * 1024, File::create(&temp_path)?);

        let mut rebuilt: BTreeMap<NodeId, SeqIndex> = BTreeMap::new();
        let mut end = 0u64;
        let mut payload = Vec::new();
        for (origin, seq, loc) in kept {
            source.seek(SeekFrom::Start(loc.offset + RECORD_HEADER as u64))?;
            payload.clear();
            payload.resize(loc.len as usize, 0);
            source.read_exact(&mut payload)?;

            let offset = end;
            end += write_record(&mut target, &payload)?;
            rebuilt.entry(origin).or_default().insert(
                seq,
                RecordLoc {
                    offset,
                    len: loc.len,
                },
            );
        }

        target.flush()?;
        target.get_ref().sync_data()?;
        drop(target);

        // Rename over the original, so a crash mid-compaction leaves either the
        // old log or the new one and never a half of each.
        std::fs::rename(&temp_path, &log.path)?;

        let mut handle = OpenOptions::new().read(true).write(true).open(&log.path)?;
        handle.seek(SeekFrom::Start(end))?;

        log::info!(
            "Replication log compacted: {} bytes -> {} bytes",
            log.end - log.start,
            end
        );

        log.writer = BufWriter::with_capacity(64 * 1024, handle);
        log.end = end;
        log.start = 0;
        *index = rebuilt;

        Ok(())
    }
}

/// How far this node has applied each other origin's stream.
pub struct AppliedState {
    positions: Mutex<BTreeMap<NodeId, u64>>,
    file: Mutex<Option<BufWriter<File>>>,
    path: Option<PathBuf>,
    /// Records appended since the file was last rewritten, so it does not grow
    /// without bound when one origin is advanced millions of times.
    /// Padded for the same reason: incremented per applied record, and its
    /// unpadded neighbour is the writer `Mutex`.
    appends: CacheGuard<std::sync::atomic::AtomicU64>,
    /// One rewrite at a time. Several `record` calls can cross the threshold
    /// together, and two rewrites sharing the one temp path would interleave
    /// their writes into it and rename a file missing positions.
    rewriting: Mutex<()>,
}

/// Rewrite the state file after this many appends. At 16 bytes each that caps
/// it at around 256 KiB between rewrites.
const STATE_REWRITE_AFTER: u64 = 16_384;

impl std::fmt::Debug for AppliedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppliedState")
            .field("positions", &*self.positions.lock())
            .finish()
    }
}

impl AppliedState {
    pub fn in_memory() -> AppliedState {
        AppliedState {
            positions: Mutex::new(BTreeMap::new()),
            file: Mutex::new(None),
            path: None,
            appends: std::sync::atomic::AtomicU64::new(0).into(),
            rewriting: Mutex::new(()),
        }
    }

    /// Opens the state file in `dir`, replaying it to recover each position.
    ///
    /// The file is a sequence of `(origin, seq)` pairs and the recovered
    /// position is the maximum per origin, so a torn tail costs at most the
    /// most recent advance — which replays one transaction, not a stream.
    pub fn open(dir: &Path) -> io::Result<AppliedState> {
        let path = dir.join(STATE_FILE);
        let mut handle = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let mut bytes = Vec::new();
        handle.read_to_end(&mut bytes)?;

        let mut positions: BTreeMap<NodeId, u64> = BTreeMap::new();
        let whole = bytes.len() - bytes.len() % 16;
        for chunk in bytes[..whole].chunks_exact(16) {
            let origin = u64::from_le_bytes(chunk[..8].try_into().expect("8 bytes"));
            let seq = u64::from_le_bytes(chunk[8..].try_into().expect("8 bytes"));
            let entry = positions.entry(origin).or_insert(0);
            *entry = (*entry).max(seq);
        }

        if whole != bytes.len() {
            log::warn!(
                "Replication state {} ended mid-record; dropping the last {} byte(s)",
                path.display(),
                bytes.len() - whole
            );
            handle.set_len(whole as u64)?;
        }
        handle.seek(SeekFrom::Start(whole as u64))?;

        Ok(AppliedState {
            positions: Mutex::new(positions),
            file: Mutex::new(Some(BufWriter::with_capacity(4096, handle))),
            path: Some(path),
            appends: std::sync::atomic::AtomicU64::new(0).into(),
            rewriting: Mutex::new(()),
        })
    }

    /// The highest sequence applied from `origin`, or 0.
    pub fn high_water(&self, origin: NodeId) -> u64 {
        self.positions.lock().get(&origin).copied().unwrap_or(0)
    }

    /// Whether a record has already been applied.
    #[inline]
    pub fn already_applied(&self, origin: NodeId, seq: u64) -> bool {
        seq <= self.high_water(origin)
    }

    /// Advances `origin` to `seq`, if that is forwards.
    ///
    /// Returns whether anything moved, so a duplicate costs no I/O.
    pub fn record(&self, origin: NodeId, seq: u64) -> io::Result<bool> {
        {
            let mut positions = self.positions.lock();
            let entry = positions.entry(origin).or_insert(0);
            if seq <= *entry {
                return Ok(false);
            }
            *entry = seq;
        }

        let mut file = self.file.lock();
        if let Some(writer) = file.as_mut() {
            writer.write_all(&origin.to_le_bytes())?;
            writer.write_all(&seq.to_le_bytes())?;
        }
        drop(file);

        if self
            .appends
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
            >= STATE_REWRITE_AFTER
        {
            self.rewrite()?;
        }

        Ok(true)
    }

    /// Forgets what has been applied from `origin`, so its stream is taken
    /// again from the beginning. What `REPLICATION RESYNC` does.
    pub fn reset(&self, origin: NodeId) -> io::Result<()> {
        self.positions.lock().remove(&origin);
        self.rewrite()
    }

    /// The whole vector, for a handshake or an acknowledgement.
    pub fn vector(&self) -> Vec<VectorEntry> {
        self.positions
            .lock()
            .iter()
            .map(|(origin, seq)| VectorEntry {
                origin: *origin,
                seq: *seq,
            })
            .collect()
    }

    /// Same shape as [`ReplicationLog::flush`]: write out under the lock,
    /// sync a duplicate descriptor outside it, so `record` is never parked
    /// behind an fsync. A `rewrite` that swaps the file in between synced its
    /// replacement before renaming it, so nothing written before this call is
    /// left unsynced.
    pub fn flush(&self) -> io::Result<()> {
        let handle = {
            let mut file = self.file.lock();
            match file.as_mut() {
                Some(writer) => {
                    writer.flush()?;
                    writer.get_ref().try_clone()?
                }
                None => return Ok(()),
            }
        };
        sync_unlocked(&handle)
    }

    /// Replaces the file with one record per origin.
    ///
    /// The positions are copied, written to a temp file and synced without
    /// the `file` lock, so `record` is not parked behind the sync. A `record`
    /// that lands in that window advances `positions` and appends to the *old*
    /// file — which the rename then discards. So the swap re-reads the
    /// positions under the `file` lock and appends whatever moved since the
    /// copy to the new file before installing it. `record` updates `positions`
    /// before it takes the `file` lock, so every advance is either in that
    /// second read or written to the new file after the lock is released;
    /// recovery takes the maximum per origin, so seeing one twice is harmless.
    ///
    /// Losing one did not merely cost a position: on restart this node asked
    /// for that origin's transaction again and **applied it twice** — the
    /// table stamps that would mark it superseded are not persisted, so a
    /// replayed `INSERT` into a table without a unique key, or a `SET n = n +
    /// 1`, landed a second time. On a clean restart too, since nothing
    /// rewrote the file from memory at shutdown.
    fn rewrite(&self) -> io::Result<()> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        let _one_at_a_time = self.rewriting.lock();

        let snapshot: BTreeMap<NodeId, u64> = self.positions.lock().clone();

        let temp_path = path.with_extension("rewriting");
        {
            let mut target = BufWriter::with_capacity(4096, File::create(&temp_path)?);
            for (origin, seq) in &snapshot {
                target.write_all(&origin.to_le_bytes())?;
                target.write_all(&seq.to_le_bytes())?;
            }
            target.flush()?;
            target.get_ref().sync_data()?;
        }

        #[cfg(test)]
        test_hooks::BEFORE_STATE_SWAP.with(|hook| {
            if let Some(f) = hook.borrow_mut().as_mut() {
                f();
            }
        });

        let mut file = self.file.lock();
        // Whatever advanced after the copy above went to the old file only.
        let moved: Vec<(NodeId, u64)> = self
            .positions
            .lock()
            .iter()
            .filter(|(origin, seq)| snapshot.get(origin) != Some(seq))
            .map(|(o, s)| (*o, *s))
            .collect();
        let mut handle = OpenOptions::new().read(true).write(true).open(&temp_path)?;
        handle.seek(SeekFrom::End(0))?;
        if !moved.is_empty() {
            let mut extra = Vec::with_capacity(moved.len() * 16);
            for (origin, seq) in &moved {
                extra.extend_from_slice(&origin.to_le_bytes());
                extra.extend_from_slice(&seq.to_le_bytes());
            }
            handle.write_all(&extra)?;
            // Synced here, under the lock, only in this rare case: a `flush`
            // may already have made these durable in the old file, and the
            // rename must not take that back.
            handle.sync_data()?;
        }
        std::fs::rename(&temp_path, path)?;
        *file = Some(BufWriter::with_capacity(4096, handle));
        self.appends.store(0, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(origin: NodeId, seq: u64) -> TxnRecord {
        TxnRecord {
            origin,
            seq,
            commit_hlc: seq * 1000,
            flags: 0,
            statements: vec![format!("INSERT INTO t (id) VALUES ({})", seq)],
            tables: vec!["t".to_string()],
        }
    }

    #[test]
    fn an_appended_record_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();

        for seq in 1..=10 {
            log.append(&record(1, seq)).unwrap();
        }

        let (records, more) = log.read_after(1, 0, 100, 1 << 20).unwrap();
        assert_eq!(records.len(), 10);
        assert!(!more);
        assert_eq!(records[0].seq, 1);
        assert_eq!(records[9].seq, 10);
    }

    #[test]
    fn a_read_starts_after_the_sequence_it_is_given() {
        let dir = tempfile::tempdir().unwrap();
        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        for seq in 1..=10 {
            log.append(&record(1, seq)).unwrap();
        }

        let (records, _) = log.read_after(1, 7, 100, 1 << 20).unwrap();
        assert_eq!(
            records.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![8, 9, 10]
        );
    }

    #[test]
    fn a_read_respects_its_record_limit_and_says_there_is_more() {
        let dir = tempfile::tempdir().unwrap();
        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        for seq in 1..=10 {
            log.append(&record(1, seq)).unwrap();
        }

        let (records, more) = log.read_after(1, 0, 3, 1 << 20).unwrap();
        assert_eq!(records.len(), 3);
        assert!(more, "seven records were left behind");
    }

    #[test]
    fn origins_do_not_bleed_into_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        for seq in 1..=5 {
            log.append(&record(1, seq)).unwrap();
            log.append(&record(2, seq * 100)).unwrap();
        }

        let (records, _) = log.read_after(2, 0, 100, 1 << 20).unwrap();
        assert!(records.iter().all(|r| r.origin == 2));
        assert_eq!(records.len(), 5);
    }

    #[test]
    fn a_reopened_log_still_knows_what_it_held() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
            for seq in 1..=10 {
                log.append(&record(1, seq)).unwrap();
            }
            log.flush().unwrap();
        }

        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(log.highest(1), 10);
        assert_eq!(log.last_hlc(), 10_000);

        let (records, _) = log.read_after(1, 5, 100, 1 << 20).unwrap();
        assert_eq!(records.len(), 5);
    }

    /// A commit appends under the same lock the flusher takes. The fsync must
    /// not be inside it, or every committer waits out one sync per tick.
    #[test]
    fn appends_are_not_blocked_by_a_slow_flush() {
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(ReplicationLog::open(dir.path(), 1 << 20).unwrap());
        let state = Arc::new(AppliedState::open(dir.path()).unwrap());
        for seq in 1..=10 {
            log.append(&record(1, seq)).unwrap();
        }
        state.record(7, 1).unwrap();

        const SYNC: Duration = Duration::from_millis(600);
        let syncing = Arc::new(std::sync::Barrier::new(2));
        let flusher = {
            let (log, state, syncing) = (log.clone(), state.clone(), syncing.clone());
            std::thread::spawn(move || {
                test_hooks::SYNC_DELAY.with(|d| d.set(SYNC));
                syncing.wait();
                let started = Instant::now();
                log.flush().unwrap();
                state.flush().unwrap();
                started.elapsed()
            })
        };
        syncing.wait();
        // Give the flusher time to be inside its (slow) sync.
        std::thread::sleep(Duration::from_millis(100));

        let started = Instant::now();
        log.append(&record(1, 11)).unwrap();
        let log_append = started.elapsed();
        // Past the log's sync and into the state's.
        std::thread::sleep(SYNC);
        let started = Instant::now();
        state.record(7, 2).unwrap();
        let state_record = started.elapsed();

        let flush_took = flusher.join().unwrap();
        assert!(
            flush_took >= SYNC * 2,
            "the injected delay did not apply ({flush_took:?}); the test proves nothing"
        );
        assert!(
            log_append < Duration::from_millis(250),
            "an append waited {log_append:?} for the flusher's fsync"
        );
        assert!(
            state_record < Duration::from_millis(250),
            "an applied-state record waited {state_record:?} for the flusher's fsync"
        );

        // And what the flush covered is on disk: a reopen finds it all.
        log.flush().unwrap();
        state.flush().unwrap();
        drop((log, state));
        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(log.highest(1), 11);
        let state = AppliedState::open(dir.path()).unwrap();
        assert_eq!(state.high_water(7), 2);
    }

    /// The dense index must answer exactly as the `BTreeMap` it replaced, for
    /// any insert order, holes and jumps included.
    #[test]
    fn seq_index_matches_a_map() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        for round in 0..2_000 {
            let mut dense = SeqIndex::default();
            let mut map: BTreeMap<u64, RecordLoc> = BTreeMap::new();
            let mut seq = 1 + next(1_000);
            for i in 0..next(300) {
                // Mostly the next number, sometimes out of order, a small gap,
                // a rewrite of one held, or (rarely) a huge jump.
                seq = match next(20) {
                    0 => seq.saturating_sub(next(5)),
                    1 => seq + 2 + next(10),
                    2 if round % 7 == 0 => seq + 100_000 + next(1 << 30),
                    _ => seq + 1,
                };
                let loc = RecordLoc { offset: i * 100 + next(50), len: next(1 << 20) as u32 };
                dense.insert(seq, loc);
                map.insert(seq, loc);
            }
            assert_eq!(dense.first(), map.keys().next().copied());
            assert_eq!(dense.last(), map.keys().next_back().copied());
            let probe = |from: u64| -> Vec<(u64, u64, u32)> {
                dense.iter_from(from).map(|(s, l)| (s, l.offset, l.len)).collect()
            };
            let expect = |from: u64| -> Vec<(u64, u64, u32)> {
                map.range(from..).map(|(s, l)| (*s, l.offset, l.len)).collect()
            };
            for from in [0, 1, seq / 2, seq, seq + 1, u64::MAX] {
                assert_eq!(probe(from), expect(from), "round {round} from {from}");
            }
        }
        assert_eq!(std::mem::size_of::<Slot>(), 12);
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// seq_index_memory --nocapture`. Resident growth for 4M dense entries.
    #[test]
    #[ignore]
    fn seq_index_memory() {
        fn rss_kb() -> u64 {
            let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
            statm.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap() * 4
        }
        const N: u64 = 4_000_000;
        // Dense first and kept alive, so the map's pages cannot be the ones
        // reused for it.
        let before = rss_kb();
        let mut dense = SeqIndex::default();
        for i in 1..=N {
            dense.insert(i, RecordLoc { offset: i * 64, len: 40 });
        }
        let dense_kb = rss_kb() - before;
        let before = rss_kb();
        let mut map: BTreeMap<u64, RecordLoc> = BTreeMap::new();
        for i in 1..=N {
            map.insert(i, RecordLoc { offset: i * 64, len: 40 });
        }
        let map_kb = rss_kb() - before;
        std::hint::black_box((&dense, &map));
        println!(
            "{N} entries: BTreeMap {:.1} B/entry, SeqIndex {:.1} B/entry",
            map_kb as f64 * 1024.0 / N as f64,
            dense_kb as f64 * 1024.0 / N as f64
        );
    }

    /// A position advanced while the state file is being rewritten must
    /// survive the swap. It used to go to the old file only and vanish with
    /// the rename, and the restarted node then applied that origin's
    /// transaction a second time.
    #[test]
    fn a_record_racing_a_rewrite_survives_it() {
        use std::sync::Arc;
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(AppliedState::open(dir.path()).unwrap());
        state.record(1, 5).unwrap();
        state.record(2, 3).unwrap();

        let racer = state.clone();
        test_hooks::BEFORE_STATE_SWAP.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                // Lands between the copy and the swap: an advance of an origin
                // the copy holds, and one it has never seen.
                racer.record(2, 4).unwrap();
                racer.record(9, 1).unwrap();
            }));
        });
        // `reset` rewrites; the reset origin must stay gone.
        state.reset(1).unwrap();
        test_hooks::BEFORE_STATE_SWAP.with(|hook| *hook.borrow_mut() = None);

        // After the swap, appends go to the new file.
        state.record(2, 6).unwrap();
        state.flush().unwrap();
        drop(state);

        let reopened = AppliedState::open(dir.path()).unwrap();
        assert_eq!(reopened.high_water(9), 1, "an advance during the rewrite was lost");
        assert_eq!(reopened.high_water(2), 6);
        assert_eq!(reopened.high_water(1), 0, "the reset origin came back");
    }

    /// The shape a crash leaves: a record that got half-written.
    #[test]
    fn a_torn_tail_is_truncated_and_the_rest_survives() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
            for seq in 1..=10 {
                log.append(&record(1, seq)).unwrap();
            }
            log.flush().unwrap();
        }

        let path = dir.path().join(LOG_FILE);
        let len = std::fs::metadata(&path).unwrap().len();
        // Chop the last record in half.
        let handle = OpenOptions::new().write(true).open(&path).unwrap();
        handle.set_len(len - 8).unwrap();
        drop(handle);

        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(log.highest(1), 9, "the intact records must survive");

        // And the log is writable again from the truncation point.
        log.append(&record(1, 10)).unwrap();
        assert_eq!(log.highest(1), 10);
    }

    #[test]
    fn a_corrupted_record_stops_recovery_rather_than_being_trusted() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
            for seq in 1..=5 {
                log.append(&record(1, seq)).unwrap();
            }
            log.flush().unwrap();
        }

        // Flip a byte inside the first record's payload.
        let path = dir.path().join(LOG_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        let target = RECORD_HEADER + 4;
        bytes[target] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(
            log.highest(1),
            0,
            "nothing after a bad checksum may be trusted"
        );
    }

    #[test]
    fn compaction_drops_old_records_and_keeps_recent_ones() {
        let dir = tempfile::tempdir().unwrap();
        // A budget small enough that a few hundred records overflow it.
        let log = ReplicationLog::open(dir.path(), 4096).unwrap();

        for seq in 1..=500 {
            log.append(&record(1, seq)).unwrap();
        }

        assert!(
            log.bytes() <= 4096 + 512,
            "retention should bound the file: {}",
            log.bytes()
        );
        assert_eq!(log.highest(1), 500, "the newest record must survive");
        assert!(
            log.earliest(1) > 1,
            "the oldest ones should have been dropped"
        );
    }

    #[test]
    fn a_peer_asking_for_dropped_history_is_told_it_cannot_be_served() {
        let dir = tempfile::tempdir().unwrap();
        let log = ReplicationLog::open(dir.path(), 4096).unwrap();
        for seq in 1..=500 {
            log.append(&record(1, seq)).unwrap();
        }

        assert!(!log.can_serve(1, 0), "sequence 1 is long gone");
        assert!(log.can_serve(1, 499), "the tail is still there");
    }

    #[test]
    fn an_empty_origin_can_always_be_served() {
        let dir = tempfile::tempdir().unwrap();
        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        assert!(
            log.can_serve(42, 0),
            "nothing is held, so nothing is missing"
        );
    }

    #[test]
    fn an_in_memory_log_serves_from_its_window_and_writes_nothing() {
        let log = ReplicationLog::in_memory(1 << 20);
        for seq in 1..=10 {
            log.append(&record(1, seq)).unwrap();
        }

        let (records, _) = log.read_after(1, 0, 100, 1 << 20).unwrap();
        assert_eq!(records.len(), 10);
        assert_eq!(log.bytes(), 0);
    }

    /// An in-memory log is bounded by its window, and has to say so. Claiming
    /// it can serve history it has already dropped is the one answer that turns
    /// a catch-up into silent data loss.
    #[test]
    fn an_in_memory_log_reports_the_window_as_its_retention_boundary() {
        let log = ReplicationLog::in_memory(1 << 20);
        for seq in 1..=(HOT_RECORDS as u64 + 500) {
            log.append(&record(1, seq)).unwrap();
        }

        assert_eq!(log.highest(1), HOT_RECORDS as u64 + 500);
        assert!(
            log.earliest(1) > 1,
            "the window has dropped the oldest records, so it must not claim to hold them"
        );
        assert!(
            !log.can_serve(1, 0),
            "sequence 1 is gone; the peer must be told to resync"
        );
        assert!(
            log.can_serve(1, HOT_RECORDS as u64 + 499),
            "the tail is still there"
        );
    }

    #[test]
    fn applied_positions_only_move_forwards() {
        let state = AppliedState::in_memory();
        assert!(state.record(1, 5).unwrap());
        assert!(
            !state.record(1, 3).unwrap(),
            "an older position is not an advance"
        );
        assert_eq!(state.high_water(1), 5);
        assert!(state.already_applied(1, 4));
        assert!(!state.already_applied(1, 6));
    }

    #[test]
    fn applied_positions_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let state = AppliedState::open(dir.path()).unwrap();
            state.record(1, 17).unwrap();
            state.record(2, 4).unwrap();
            state.flush().unwrap();
        }

        let state = AppliedState::open(dir.path()).unwrap();
        assert_eq!(state.high_water(1), 17);
        assert_eq!(state.high_water(2), 4);
    }

    #[test]
    fn a_resync_forgets_one_origin_and_leaves_the_others() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppliedState::open(dir.path()).unwrap();
        state.record(1, 17).unwrap();
        state.record(2, 4).unwrap();

        state.reset(1).unwrap();
        assert_eq!(state.high_water(1), 0);
        assert_eq!(state.high_water(2), 4);

        // And the reset is what a reopen sees, not the pre-reset value.
        state.flush().unwrap();
        let reopened = AppliedState::open(dir.path()).unwrap();
        assert_eq!(reopened.high_water(1), 0);
        assert_eq!(reopened.high_water(2), 4);
    }

    #[test]
    fn a_state_file_torn_mid_record_recovers_what_was_whole() {
        let dir = tempfile::tempdir().unwrap();
        {
            let state = AppliedState::open(dir.path()).unwrap();
            state.record(1, 17).unwrap();
            state.flush().unwrap();
        }

        let path = dir.path().join(STATE_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&[0u8; 5]);
        std::fs::write(&path, &bytes).unwrap();

        let state = AppliedState::open(dir.path()).unwrap();
        assert_eq!(state.high_water(1), 17);
    }

    #[test]
    fn the_state_file_is_rewritten_rather_than_growing_without_bound() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppliedState::open(dir.path()).unwrap();

        for seq in 1..=(STATE_REWRITE_AFTER + 100) {
            state.record(1, seq).unwrap();
        }
        state.flush().unwrap();

        let size = std::fs::metadata(dir.path().join(STATE_FILE))
            .unwrap()
            .len();
        assert!(
            size < STATE_REWRITE_AFTER * 16,
            "the file should have been rewritten: {}",
            size
        );
        assert_eq!(state.high_water(1), STATE_REWRITE_AFTER + 100);
    }

    #[test]
    fn the_vector_reports_every_origin() {
        let state = AppliedState::in_memory();
        state.record(1, 3).unwrap();
        state.record(9, 100).unwrap();

        let mut vector = state.vector();
        vector.sort_unstable_by_key(|e| e.origin);
        assert_eq!(vector.len(), 2);
        assert_eq!(vector[0], VectorEntry { origin: 1, seq: 3 });
        assert_eq!(
            vector[1],
            VectorEntry {
                origin: 9,
                seq: 100
            }
        );
    }
}
