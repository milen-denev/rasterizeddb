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
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cacheguard::CacheGuard;
use parking_lot::Mutex;

use crate::core::storage_providers::CRC;
use crate::core::wal::Wal;

use super::config::NodeId;
use super::hlc::{Hlc, NO_HLC};
use super::protocol::{Reader, TxnRecord, VectorEntry};

/// Starts every log record. A file that does not begin with one is not this
/// file, and a record that does not is where the log was torn.
const RECORD_MAGIC: u32 = 0x5252_4c47; // "RRLG"

/// Starts a record that is real only if a transaction committed: its payload
/// begins with that transaction's id.
const RECORD_MAGIC_TX: u32 = 0x5252_4c54; // "RRLT"

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

/// The file's durability: the database's own write-ahead log, when it has one.
///
/// A record appended here is written to the file and then described to that
/// log, without a sync of its own. The commit that follows syncs the log for
/// its rows and its outcome, and that one sync covers this record too: they
/// are records in one log, this one first, and replay stops at the first it
/// cannot read whole. So a committed transaction's record is on disk whenever
/// its outcome is, and it costs the commit no second `fdatasync`.
#[derive(Clone)]
struct Logged {
    wal: Arc<Wal>,
    file_id: u64,
}

/// One file of the log.
struct Segment {
    handle: File,
    path: PathBuf,
    /// Bytes written, which is also the offset the next record goes to.
    len: u64,
    logged: Option<Logged>,
    /// The newest sequence of each origin written to this file.
    newest: BTreeMap<NodeId, u64>,
}

struct LogFile {
    dir: PathBuf,
    /// The log's files by generation: `db_replication.log`, then
    /// `db_replication.log.1`, `.2`, …. Appends go to the last; retention
    /// removes whole files from the front.
    ///
    /// Never rewritten. The write-ahead log replays "these bytes at this
    /// offset of the file named so", so a file is restorable from that log
    /// exactly when everything in it was appended through it. A compaction
    /// that copied the newest half into a fresh file wrote that half behind
    /// the log's back, and a restore that replayed the log got the file's
    /// name, a run of zeros, and the appends that came after.
    segments: BTreeMap<u64, Segment>,
    /// The write-ahead log new segments are registered with.
    wal: Option<Arc<Wal>>,
    /// This node's own records that are only real if their transaction
    /// committed: sequence to transaction id. Cleared by [`ReplicationLog::confirm`]
    /// or [`ReplicationLog::void`]; what is left at open is what a crash
    /// interrupted.
    conditional: BTreeMap<u64, u64>,
}

impl LogFile {
    fn active(&mut self) -> &mut Segment {
        self.segments.values_mut().next_back().expect("a log has at least one file")
    }

    fn bytes(&self) -> u64 {
        self.segments.values().map(|segment| segment.len).sum()
    }
}

/// How many times its budget the log may reach while a connected peer still
/// needs its oldest file.
const KEEP_FOR_PEERS_FACTOR: u64 = 4;

/// A location's file and the offset inside it, in one word: the generation
/// above bit 40, so a segment may be a terabyte and there may be sixteen
/// million of them.
const GENERATION_SHIFT: u32 = 40;

fn pack(generation: u64, offset: u64) -> u64 {
    (generation << GENERATION_SHIFT) | offset
}

fn unpack(packed: u64) -> (u64, u64) {
    (packed >> GENERATION_SHIFT, packed & ((1 << GENERATION_SHIFT) - 1))
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
    /// Records voided in memory whose replacement could not be written. They
    /// read back as void for the life of the process; the next open voids
    /// them for good.
    voided: Mutex<std::collections::HashSet<(NodeId, u64)>>,
    /// Conditional records found at open, until [`ReplicationLog::take_undecided`].
    pending_at_open: Mutex<BTreeMap<(NodeId, u64), u64>>,
    /// Settable while open: the storage is opened at server start, before
    /// the configuration that says how much to keep has been applied.
    retention_bytes: std::sync::atomic::AtomicU64,
    /// The origin whose stream a connected peer is still being sent, and how
    /// far the slowest such peer has got: see [`ReplicationLog::keep_for_peers`].
    kept_origin: std::sync::atomic::AtomicU64,
    kept_after: std::sync::atomic::AtomicU64,
    /// Padded off the two `Mutex` fields above it. Every append does a
    /// `fetch_max` here while holding neither lock, so without the guard the
    /// update invalidated the line carrying the mutex words that the next
    /// appender was about to acquire.
    last_hlc: CacheGuard<std::sync::atomic::AtomicU64>,
}

impl std::fmt::Debug for ReplicationLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationLog")
            .field("retention_bytes", &self.retention())
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
    }
}

/// One record framed for the file: `magic`, `len`, `crc`, then the payload,
/// which for a conditional record starts with its transaction id.
fn frame(body: &[u8], local_tx: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(RECORD_HEADER + 8 + body.len());
    if local_tx == 0 {
        out.extend_from_slice(&RECORD_MAGIC.to_le_bytes());
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(&CRC.checksum(body).to_le_bytes());
    } else {
        let tx = local_tx.to_le_bytes();
        let mut digest = CRC.digest();
        digest.update(&tx);
        digest.update(body);
        out.extend_from_slice(&RECORD_MAGIC_TX.to_le_bytes());
        out.extend_from_slice(&((body.len() + 8) as u32).to_le_bytes());
        out.extend_from_slice(&digest.finalize().to_le_bytes());
        out.extend_from_slice(&tx);
    }
    out.extend_from_slice(body);
    out
}

/// Bytes a framed record carries ahead of its body.
fn lead(local_tx: u64) -> u64 {
    RECORD_HEADER as u64 + if local_tx == 0 { 0 } else { 8 }
}

/// The file of generation `n` in `dir`.
fn generation_path(dir: &Path, generation: u64) -> PathBuf {
    if generation == 0 {
        dir.join(LOG_FILE)
    } else {
        dir.join(format!("{LOG_FILE}.{generation}"))
    }
}

/// The generation a file name in the directory belongs to, if it is a log.
fn generation_of(name: &str) -> Option<u64> {
    if name == LOG_FILE {
        return Some(0);
    }
    let rest = name.strip_prefix(LOG_FILE)?.strip_prefix('.')?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse().ok()
}

/// The temporary an earlier build's compaction copied into.
const COMPACTING_NAME: &str = "db_replication.log.compacting";

/// What a scan of the log's files found.
#[derive(Default)]
struct Scanned {
    index: BTreeMap<NodeId, SeqIndex>,
    hot: std::collections::VecDeque<TxnRecord>,
    conditional: BTreeMap<(NodeId, u64), u64>,
    last_hlc: Hlc,
}

/// Reads one file's records into `found`, and returns where its last whole
/// record ends.
fn scan(
    path: &Path,
    generation: u64,
    bytes: &[u8],
    found: &mut Scanned,
    newest: &mut BTreeMap<NodeId, u64>,
) -> u64 {
    let mut at = 0usize;
    let good_end = loop {
        if at + RECORD_HEADER > bytes.len() {
            break at;
        }
        let magic = u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4 bytes"));
        if magic != RECORD_MAGIC && magic != RECORD_MAGIC_TX {
            log::error!(
                "Replication log {} has an unrecognised record at byte {}; everything from there is dropped",
                path.display(),
                at
            );
            break at;
        }
        let len = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(bytes[at + 8..at + 12].try_into().expect("4 bytes"));

        let payload_at = at + RECORD_HEADER;
        let Some(payload) = bytes.get(payload_at..payload_at + len) else {
            // A record whose length runs past the end is the torn tail.
            break at;
        };
        if CRC.checksum(payload) != crc {
            if payload_at + len == bytes.len() {
                log::warn!(
                    "Replication log {} ends in a record that was never whole (byte {}); cutting it off",
                    path.display(),
                    at
                );
            } else {
                log::error!(
                    "Replication log {} fails its checksum at byte {} with {} byte(s) after it; \
                     everything from there is dropped",
                    path.display(),
                    at,
                    bytes.len() - payload_at - len
                );
            }
            break at;
        }
        let (local_tx, body_at, body) = if magic == RECORD_MAGIC_TX {
            if len < 8 {
                break at;
            }
            (
                u64::from_le_bytes(payload[..8].try_into().expect("8 bytes")),
                payload_at + 8,
                &payload[8..],
            )
        } else {
            (0, payload_at, payload)
        };

        match TxnRecord::decode(&mut Reader::new(body)) {
            Ok(record) => {
                let held = newest.entry(record.origin).or_insert(0);
                *held = (*held).max(record.seq);
                found.index.entry(record.origin).or_default().insert(
                    record.seq,
                    RecordLoc {
                        offset: pack(generation, body_at as u64),
                        len: body.len() as u32,
                    },
                );
                // A later record under a sequence replaces the earlier one:
                // that is how a transaction that never committed is voided.
                if local_tx != 0 {
                    found.conditional.insert((record.origin, record.seq), local_tx);
                } else {
                    found.conditional.remove(&(record.origin, record.seq));
                }
                found.last_hlc = found.last_hlc.max(record.commit_hlc);
                if let Some(held) = found
                    .hot
                    .iter_mut()
                    .rev()
                    .find(|r: &&mut TxnRecord| r.origin == record.origin && r.seq == record.seq)
                {
                    *held = record;
                } else {
                    if found.hot.len() == HOT_RECORDS {
                        found.hot.pop_front();
                    }
                    found.hot.push_back(record);
                }
            }
            Err(e) => {
                log::error!(
                    "Replication log {} holds an undecodable record at byte {} ({}); everything from there is dropped",
                    path.display(),
                    at,
                    e
                );
                break at;
            }
        }

        at = payload_at + len;
    };
    good_end as u64
}

impl ReplicationLog {
    /// A log with no file behind it.
    pub fn in_memory(retention_bytes: u64) -> ReplicationLog {
        ReplicationLog {
            file: Mutex::new(None),
            index: Mutex::new(BTreeMap::new()),
            hot: Mutex::new(std::collections::VecDeque::with_capacity(HOT_RECORDS)),
            voided: Mutex::new(std::collections::HashSet::new()),
            pending_at_open: Mutex::new(BTreeMap::new()),
            retention_bytes: std::sync::atomic::AtomicU64::new(retention_bytes),
            kept_origin: std::sync::atomic::AtomicU64::new(0),
            kept_after: std::sync::atomic::AtomicU64::new(u64::MAX),
            last_hlc: std::sync::atomic::AtomicU64::new(NO_HLC).into(),
        }
    }

    /// Opens the log in `dir` with no write-ahead log behind it: a record is
    /// durable when [`ReplicationLog::sync`] says so.
    pub fn open(dir: &Path, retention_bytes: u64) -> io::Result<ReplicationLog> {
        Self::open_logged(dir, retention_bytes, None)
    }

    /// Opens the log in `dir`, rebuilding its index from what is already there.
    ///
    /// `wal` is the database's own log, already replayed: every record a
    /// durable commit depends on is back in the file by now. A torn tail — the
    /// last record half-written when the machine died, and never described to
    /// that log — is cut off: it belongs to a transaction that did not commit.
    pub fn open_logged(
        dir: &Path,
        retention_bytes: u64,
        wal: Option<Arc<Wal>>,
    ) -> io::Result<ReplicationLog> {
        let mut generations: Vec<u64> = Vec::new();
        for entry in std::fs::read_dir(dir)?.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if name == COMPACTING_NAME {
                // Left by a build that compacted by copying.
                let _ = std::fs::remove_file(entry.path());
            } else if let Some(generation) = generation_of(&name) {
                generations.push(generation);
            }
        }
        generations.sort_unstable();
        if generations.is_empty() {
            generations.push(0);
        }

        let mut found = Scanned {
            hot: std::collections::VecDeque::with_capacity(HOT_RECORDS),
            ..Default::default()
        };
        let mut segments: BTreeMap<u64, Segment> = BTreeMap::new();
        let newest = *generations.last().expect("at least generation 0");
        let mut total = 0u64;
        for generation in generations {
            let path = generation_path(dir, generation);
            let mut handle = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)?;
            let mut bytes = Vec::new();
            handle.read_to_end(&mut bytes)?;
            let mut newest_here = BTreeMap::new();
            let good_end = scan(&path, generation, &bytes, &mut found, &mut newest_here);
            // Only the file appends go to is cut at its last whole record:
            // the next append lands there. An older one keeps its bytes.
            if generation == newest && good_end < bytes.len() as u64 {
                handle.set_len(good_end)?;
                handle.sync_data()?;
            }
            let logged = wal.as_ref().and_then(|wal| Self::attach(wal, &path));
            total += good_end;
            segments.insert(
                generation,
                Segment {
                    handle,
                    path,
                    len: if generation == newest { good_end } else { bytes.len() as u64 },
                    logged,
                    newest: newest_here,
                },
            );
        }

        log::info!(
            "Replication log in {} opened: {} bytes in {} file(s), {} origin(s){}",
            dir.display(),
            total,
            segments.len(),
            found.index.len(),
            if segments.values().next_back().is_some_and(|s| s.logged.is_some()) {
                ""
            } else {
                ", not write-ahead logged"
            }
        );

        let log = ReplicationLog {
            file: Mutex::new(Some(LogFile {
                dir: dir.to_path_buf(),
                segments,
                wal,
                // Keyed by sequence alone: only this node's own records are
                // ever conditional, and one node has one stream.
                conditional: found.conditional.iter().map(|((_, seq), tx)| (*seq, *tx)).collect(),
            })),
            index: Mutex::new(found.index),
            hot: Mutex::new(found.hot),
            voided: Mutex::new(std::collections::HashSet::new()),
            pending_at_open: Mutex::new(found.conditional),
            retention_bytes: std::sync::atomic::AtomicU64::new(retention_bytes),
            kept_origin: std::sync::atomic::AtomicU64::new(0),
            kept_after: std::sync::atomic::AtomicU64::new(u64::MAX),
            last_hlc: std::sync::atomic::AtomicU64::new(found.last_hlc).into(),
        };
        Ok(log)
    }

    /// Registers `path` with the write-ahead log, or says why it is not.
    fn attach(wal: &Arc<Wal>, path: &Path) -> Option<Logged> {
        let name = path.file_name()?.to_str()?;
        let file_id = crate::core::wal::file_id_for_name(name);
        match wal.register_file(file_id, path.to_path_buf()) {
            Ok(()) => Some(Logged {
                wal: wal.clone(),
                file_id,
            }),
            Err(e) => {
                log::error!(
                    "Replication log {} could not be registered with the write-ahead log ({}); \
                     it is synced on its own instead",
                    path.display(),
                    e
                );
                None
            }
        }
    }

    /// Voids the log's records for a file that is going away, then removes
    /// it. Left behind on any failure: the next open tries again.
    fn retire(wal: Option<&Arc<Wal>>, path: &Path) {
        if let Some(wal) = wal {
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                return;
            };
            if let Err(e) = wal.forget_dropped_files(&[crate::core::wal::file_id_for_name(name)]) {
                log::warn!(
                    "Could not void the log records of {} ({}); keeping the file for now",
                    path.display(),
                    e
                );
                return;
            }
        }
        #[cfg(any(test, feature = "index_store_failpoints"))]
        crate::core::sync_audit::dir_op(crate::core::sync_audit::DirOp::Unlink(path));
        if let Err(e) = std::fs::remove_file(path) {
            if e.kind() != io::ErrorKind::NotFound {
                log::warn!("Could not remove {}: {}", path.display(), e);
            }
        }
    }

    /// Whether appends are covered by the database's write-ahead log. When
    /// they are not, a caller that needs a record durable calls
    /// [`ReplicationLog::sync`].
    pub fn is_logged(&self) -> bool {
        self.file
            .lock()
            .as_ref()
            .is_some_and(|log| log.segments.values().next_back().is_some_and(|s| s.logged.is_some()))
    }

    /// The records a crash left undecided: each is real only if its
    /// transaction committed. Taken once, by whoever can ask.
    pub fn take_undecided(&self) -> Vec<(NodeId, u64, u64)> {
        std::mem::take(&mut *self.pending_at_open.lock())
            .into_iter()
            .map(|((origin, seq), tx)| (origin, seq, tx))
            .collect()
    }

    /// The highest commit timestamp this log has seen, so a restarting node can
    /// seed its clock above what it already published.
    pub fn last_hlc(&self) -> Hlc {
        self.last_hlc.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Adds a record that stands on its own: a relayed one, or one whose
    /// transaction has nothing durable to commit.
    pub fn append(&self, record: &TxnRecord) -> io::Result<()> {
        self.append_for(record, 0)
    }

    /// Adds a record. One positional write and one write-ahead log record, no
    /// `fsync`.
    ///
    /// `local_tx`, when not zero, is the transaction the record belongs to:
    /// the record is real only if that transaction committed. It is written
    /// before the commit, so that no committed transaction can be without its
    /// record; [`ReplicationLog::confirm`] or [`ReplicationLog::void`] settles
    /// it afterwards, and the next open settles what a crash left.
    pub fn append_for(&self, record: &TxnRecord, local_tx: u64) -> io::Result<()> {
        let mut body = Vec::with_capacity(record.encoded_size_hint());
        record.encode(&mut body);
        let framed = frame(&body, local_tx);

        let mut retired: Vec<PathBuf> = Vec::new();
        let wal = {
            let mut file = self.file.lock();
            if let Some(log) = file.as_mut() {
                if self.retention() != 0 && log.active().len >= self.segment_bytes() {
                    self.rotate(log, &mut retired)?;
                }
                let generation = *log.segments.keys().next_back().expect("a log has a file");
                let segment = log.active();
                let offset = segment.len;
                std::os::unix::fs::FileExt::write_all_at(&segment.handle, &framed, offset)?;
                // The file first, then the log: a checkpoint syncs the files
                // its records name and drops the log, so every record must
                // already be in its file by the time it is logged.
                if let Some(logged) = &segment.logged {
                    if let Err(e) = logged.wal.log_and_commit_deferred(logged.file_id, offset, &framed) {
                        // Not described to the log, so not durable and not
                        // counted: take the bytes back out of the file.
                        let _ = segment.handle.set_len(offset);
                        return Err(e);
                    }
                }
                segment.len += framed.len() as u64;
                let held = segment.newest.entry(record.origin).or_insert(0);
                *held = (*held).max(record.seq);
                if local_tx != 0 {
                    log.conditional.insert(record.seq, local_tx);
                } else {
                    log.conditional.remove(&record.seq);
                }

                self.index.lock().entry(record.origin).or_default().insert(
                    record.seq,
                    RecordLoc {
                        offset: pack(generation, offset + lead(local_tx)),
                        len: body.len() as u32,
                    },
                );
            }
            file.as_ref().and_then(|log| log.wal.clone())
        };
        // Outside the lock: voiding a file's records is a log sync.
        for path in retired {
            Self::retire(wal.as_ref(), &path);
        }

        self.last_hlc
            .fetch_max(record.commit_hlc, std::sync::atomic::Ordering::AcqRel);
        self.remember(record.clone());
        Ok(())
    }

    /// Says how far the slowest *connected* peer has been sent `origin`'s
    /// stream, or that there is none (`u64::MAX`).
    ///
    /// The retention budget is for a peer that is away: how much history to
    /// hold in case it returns. A peer that is here and being sent the
    /// stream is another matter. Under a burst the origin, with many writers,
    /// outruns a peer that applies one transaction at a time, and a budget
    /// that let go of files such a peer had not been sent yet turned a few
    /// seconds of lag into a node that had to be reseeded. Those files are
    /// kept, up to [`KEEP_FOR_PEERS_FACTOR`] times the budget, past which a
    /// peer that cannot keep up is not allowed to fill the disk.
    pub fn keep_for_peers(&self, origin: NodeId, slowest: u64) {
        self.kept_origin.store(origin, std::sync::atomic::Ordering::Release);
        self.kept_after.store(slowest, std::sync::atomic::Ordering::Release);
    }

    /// The retention budget in force.
    pub fn retention(&self) -> u64 {
        self.retention_bytes.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Changes the retention budget. It takes effect at the next file.
    pub fn set_retention(&self, bytes: u64) {
        self.retention_bytes.store(bytes, std::sync::atomic::Ordering::Release);
    }

    /// How large one file of the log grows before the next is started: an
    /// eighth of the budget, so retention lets go of history an eighth at a
    /// time.
    fn segment_bytes(&self) -> u64 {
        (self.retention() / 8).max(1)
    }

    /// Starts the next file, and takes the oldest ones out of the log while
    /// it is over its budget. Their paths go into `retired`, for the caller
    /// to void and remove once the lock is let go.
    fn rotate(&self, log: &mut LogFile, retired: &mut Vec<PathBuf>) -> io::Result<()> {
        let generation = log.segments.keys().next_back().copied().unwrap_or(0) + 1;
        let path = generation_path(&log.dir, generation);
        // Announced, so a crash image treats the new file as a power cut
        // would: its name and its pages are there only as far as a sync, or
        // a replay of the log, puts them there.
        #[cfg(any(test, feature = "index_store_failpoints"))]
        crate::core::sync_audit::dir_op(crate::core::sync_audit::DirOp::Create(&path));
        let handle = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;
        // The name has to outlive a power cut before anything is logged
        // under it: a replay finds a file by its name.
        crate::core::wal::sync_parent_dir(&path)?;
        let logged = log.wal.as_ref().and_then(|wal| Self::attach(wal, &path));
        log.segments.insert(
            generation,
            Segment {
                handle,
                path,
                len: 0,
                logged,
                newest: BTreeMap::new(),
            },
        );

        let kept_origin = self.kept_origin.load(std::sync::atomic::Ordering::Acquire);
        let kept_after = self.kept_after.load(std::sync::atomic::Ordering::Acquire);
        let mut dropped: Vec<u64> = Vec::new();
        let retention = self.retention();
        while log.bytes() > retention && log.segments.len() > 1 {
            // A file a connected peer has not been sent all of yet is kept
            // past the budget, up to a hard limit: see `keep_for_peers`.
            let needed = log
                .segments
                .values()
                .next()
                .and_then(|oldest| oldest.newest.get(&kept_origin))
                .is_some_and(|newest| *newest > kept_after);
            if needed && log.bytes() <= retention.saturating_mul(KEEP_FOR_PEERS_FACTOR) {
                break;
            }
            let (oldest, segment) = log.segments.pop_first().expect("more than one file");
            dropped.push(oldest);
            retired.push(segment.path);
        }
        if !dropped.is_empty() {
            let mut index = self.index.lock();
            let mut kept: BTreeMap<NodeId, SeqIndex> = BTreeMap::new();
            for (origin, per_origin) in index.iter() {
                for (seq, loc) in per_origin.iter() {
                    if !dropped.contains(&unpack(loc.offset).0) {
                        kept.entry(*origin).or_default().insert(seq, loc);
                    }
                }
            }
            *index = kept;
            log::info!(
                "Replication log: {} file(s) past the retention budget let go, {} bytes held",
                dropped.len(),
                log.bytes()
            );
        }
        Ok(())
    }

    /// Puts `record` in the hot window, over any record it replaces.
    fn remember(&self, record: TxnRecord) {
        let mut hot = self.hot.lock();
        if let Some(held) = hot
            .iter_mut()
            .rev()
            .find(|r| r.origin == record.origin && r.seq == record.seq)
        {
            *held = record;
            return;
        }
        if hot.len() == HOT_RECORDS {
            hot.pop_front();
        }
        hot.push_back(record);
    }

    /// The record's transaction committed: nothing about it is conditional
    /// any more.
    pub fn confirm(&self, seq: u64) {
        if let Some(log) = self.file.lock().as_mut() {
            log.conditional.remove(&seq);
        }
    }

    /// The transaction behind `origin:seq` did not commit. Its place in the
    /// stream is kept, by a record that carries nothing: a peer applies it by
    /// moving past it, so the stream stays without holes.
    ///
    /// Voided in memory whatever happens to the write: an error means only
    /// that the void is not on disk yet, and the next open, which finds the
    /// transaction uncommitted, writes it then.
    pub fn void(&self, origin: NodeId, seq: u64, commit_hlc: Hlc) -> io::Result<()> {
        let void = TxnRecord {
            origin,
            seq,
            commit_hlc,
            flags: super::protocol::FLAG_VOID,
            statements: Vec::new(),
            tables: Vec::new(),
        };
        match self.append_for(&void, 0) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.voided.lock().insert((origin, seq));
                self.remember(void);
                Err(e)
            }
        }
    }

    /// Makes everything appended so far durable on its own. Called by the
    /// periodic flusher and at shutdown, and by a commit when no write-ahead
    /// log covers the file.
    ///
    /// The `fdatasync` runs on a duplicate of the descriptor *after* the lock
    /// is released. Every local commit appends under that lock, so syncing
    /// while holding it stalled all committers for one fsync per flusher tick.
    pub fn flush(&self) -> io::Result<()> {
        let handle = {
            let file = self.file.lock();
            match file.as_ref().and_then(|log| log.segments.values().next_back()) {
                Some(segment) => segment.handle.try_clone()?,
                None => return Ok(()),
            }
        };
        sync_unlocked(&handle)
    }

    /// [`ReplicationLog::flush`], under the name a commit calls it by.
    pub fn sync(&self) -> io::Result<()> {
        self.flush()
    }

    /// Bytes the log currently occupies.
    pub fn bytes(&self) -> u64 {
        self.file.lock().as_ref().map(LogFile::bytes).unwrap_or(0)
    }

    /// Every origin the log holds something for.
    pub fn origins(&self) -> Vec<NodeId> {
        let mut origins: Vec<NodeId> = self.index.lock().keys().copied().collect();
        if origins.is_empty() {
            for record in self.hot.lock().iter() {
                if !origins.contains(&record.origin) {
                    origins.push(record.origin);
                }
            }
        }
        origins
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
            .filter(|record| record.origin == origin)
            .map(|record| record.seq)
            .max()
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
            .filter(|record| record.origin == origin)
            .map(|record| record.seq)
            .min()
            .unwrap_or(0)
    }

    /// Whether `after_seq` can still be caught up from here.
    pub fn can_serve(&self, origin: NodeId, after_seq: u64) -> bool {
        let earliest = self.earliest(origin);
        // Nothing held for this origin at all: there is nothing to be missing.
        earliest == 0 || after_seq + 1 >= earliest
    }

    /// Everything held for `origin` after `after_seq`, in order.
    pub fn read_after(
        &self,
        origin: NodeId,
        after_seq: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> io::Result<(Vec<TxnRecord>, bool)> {
        self.read_range(origin, after_seq, u64::MAX, max_records, max_bytes)
    }

    /// Everything held for `origin` after `after_seq` and up to `upto`, in
    /// sequence order.
    ///
    /// Returns the records and whether more remain past the limits. Serves from
    /// the hot window without touching the disk whenever the request is covered
    /// by it, which is the usual case.
    pub fn read_range(
        &self,
        origin: NodeId,
        after_seq: u64,
        upto: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> io::Result<(Vec<TxnRecord>, bool)> {
        if max_records == 0 {
            return Ok((Vec::new(), self.highest(origin).min(upto) > after_seq));
        }
        if upto <= after_seq {
            return Ok((Vec::new(), false));
        }

        if let Some(served) = self.read_hot(origin, after_seq, upto, max_records, max_bytes) {
            return Ok(served);
        }

        let locations: Vec<(u64, RecordLoc)> = {
            let index = self.index.lock();
            match index.get(&origin) {
                // `after_seq + 1..` rather than an exclusive bound on `after_seq`
                // so that `after_seq == u64::MAX` cannot overflow into an empty
                // range that silently reports "nothing to send".
                Some(per_origin) => per_origin
                    .iter_from(after_seq.saturating_add(1))
                    .take_while(|(seq, _)| *seq <= upto)
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

        // Positional reads on duplicates, with the lock let go: a handle is
        // the appender's, and a `seek` on it once moved where the next append
        // went, so every record appended after a catch-up from the file
        // overwrote an older one.
        let mut handles: Vec<(u64, File)> = Vec::new();
        {
            let file = self.file.lock();
            let Some(log) = file.as_ref() else {
                // In-memory: whatever the hot window could not serve is gone.
                return Ok((Vec::new(), false));
            };
            for (_, loc) in &locations {
                let generation = unpack(loc.offset).0;
                if handles.iter().any(|(held, _)| *held == generation) {
                    continue;
                }
                match log.segments.get(&generation) {
                    Some(segment) => handles.push((generation, segment.handle.try_clone()?)),
                    // Retention let the file go between the index read and
                    // here. The caller comes round again and finds out.
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "the replication log let go of a file during the read",
                        ));
                    }
                }
            }
        }

        let mut payload = Vec::new();
        for (seq, loc) in locations {
            if records.len() == max_records || (bytes >= max_bytes && !records.is_empty()) {
                more = true;
                break;
            }

            let (generation, offset) = unpack(loc.offset);
            let handle = &handles.iter().find(|(held, _)| *held == generation).expect("cloned above").1;
            payload.clear();
            payload.resize(loc.len as usize, 0);
            std::os::unix::fs::FileExt::read_exact_at(handle, &mut payload, offset)?;

            match TxnRecord::decode(&mut Reader::new(&payload)) {
                Ok(record) if record.origin == origin && record.seq == seq => {
                    bytes += loc.len as usize;
                    records.push(record);
                }
                Ok(other) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "replication log holds {}:{} where {}:{} was indexed",
                            other.origin, other.seq, origin, seq
                        ),
                    ));
                }
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "replication log record {}:{} at {} of file {} is unreadable: {}",
                            origin, seq, offset, generation, e
                        ),
                    ));
                }
            }
        }

        let voided = self.voided.lock();
        if !voided.is_empty() {
            for record in &mut records {
                if voided.contains(&(record.origin, record.seq)) {
                    record.flags = super::protocol::FLAG_VOID;
                    record.statements.clear();
                    record.tables.clear();
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
        upto: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> Option<(Vec<TxnRecord>, bool)> {
        let hot = self.hot.lock();

        let oldest_for_origin = hot.iter().filter(|r| r.origin == origin).map(|r| r.seq).min()?;
        if oldest_for_origin > after_seq + 1 {
            return None;
        }

        let mut wanted: Vec<&TxnRecord> = hot
            .iter()
            .filter(|r| r.origin == origin && r.seq > after_seq && r.seq <= upto)
            .collect();
        // A relayed stream can reach the window out of order; this node's own
        // is appended in sequence order, and sorting it costs a scan.
        wanted.sort_by_key(|r| r.seq);

        let mut records = Vec::new();
        let mut bytes = 0usize;
        let mut more = false;
        for record in wanted {
            if records.len() == max_records || (bytes >= max_bytes && !records.is_empty()) {
                more = true;
                break;
            }
            bytes += record.encoded_size_hint();
            records.push(record.clone());
        }

        Some((records, more))
    }

}

/// Starts a slot file. Never the start of an origin's id and a sequence, which
/// is how the file before this one began.
const STATE_MAGIC: [u8; 16] = *b"RDB-REPL-POS-3\0\0";
/// The header and every slot are this long, so no slot straddles a sector:
/// an overwrite of one is all there or not at all.
const STATE_SLOT: u64 = 64;

pub const INTENTS_FILE: &str = "db_replication.intents";
const INTENT_LEN: u64 = 48;
/// Past this the intents file is emptied the next time nothing is waiting on
/// it.
const INTENTS_TRIM_AT: u64 = 1 << 20;

/// One origin's slot: how far its stream is applied *and durable*.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Position {
    origin: NodeId,
    seq: u64,
    /// Which run of intents builds on this slot. Raised whenever the slot is
    /// set by anything but the intents themselves settling, so an intent left
    /// from before can never be read against a position it was not written
    /// for.
    epoch: u64,
}

impl Position {
    fn encode(&self) -> [u8; STATE_SLOT as usize] {
        let mut out = [0u8; STATE_SLOT as usize];
        out[..8].copy_from_slice(&self.origin.to_le_bytes());
        out[8..16].copy_from_slice(&self.seq.to_le_bytes());
        out[16..24].copy_from_slice(&self.epoch.to_le_bytes());
        // Never zero for a written slot, so an all-zero slot is an empty one.
        out[32] = 1;
        let crc = CRC.checksum(&out[..33]);
        out[36..40].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// `Ok(None)` for a slot nothing was written to.
    fn decode(bytes: &[u8]) -> Result<Option<Position>, ()> {
        if bytes.iter().all(|b| *b == 0) {
            return Ok(None);
        }
        let crc = u32::from_le_bytes(bytes[36..40].try_into().expect("4 bytes"));
        if bytes[32] != 1 || CRC.checksum(&bytes[..33]) != crc {
            return Err(());
        }
        let word = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes"));
        Ok(Some(Position {
            origin: word(0),
            seq: word(8),
            epoch: word(16),
        }))
    }
}

/// "`origin` moves from `prev` to `seq`, if transaction `tx` committed."
/// A `tx` of zero is a step no transaction stands behind (a record that wrote
/// no row): it holds if the steps before it do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Intent {
    origin: NodeId,
    epoch: u64,
    prev: u64,
    seq: u64,
    tx: u64,
}

impl Intent {
    fn encode(&self) -> [u8; INTENT_LEN as usize] {
        let mut out = [0u8; INTENT_LEN as usize];
        out[..8].copy_from_slice(&self.origin.to_le_bytes());
        out[8..16].copy_from_slice(&self.epoch.to_le_bytes());
        out[16..24].copy_from_slice(&self.prev.to_le_bytes());
        out[24..32].copy_from_slice(&self.seq.to_le_bytes());
        out[32..40].copy_from_slice(&self.tx.to_le_bytes());
        out[40] = 1;
        let crc = CRC.checksum(&out[..41]);
        out[44..48].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// `None` for anything that is not a whole intent: an empty slot, a torn
    /// one, the leftovers of a file emptied since.
    fn decode(bytes: &[u8]) -> Option<Intent> {
        let crc = u32::from_le_bytes(bytes[44..48].try_into().expect("4 bytes"));
        if bytes[40] != 1 || CRC.checksum(&bytes[..41]) != crc {
            return None;
        }
        let word = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes"));
        Some(Intent {
            origin: word(0),
            epoch: word(8),
            prev: word(16),
            seq: word(24),
            tx: word(32),
        })
    }
}

struct StateFile {
    handle: File,
    /// Which slot each origin has. Slots are handed out once and overwritten
    /// in place from then on: the file never grows past one slot an origin,
    /// and is never rewritten.
    slots: BTreeMap<NodeId, u64>,
    next_slot: u64,
    epochs: BTreeMap<NodeId, u64>,
    logged: Option<Logged>,
    intents: File,
    intents_end: u64,
    intents_logged: Option<Logged>,
    /// Origins with intents written and not yet settled into their slot.
    unsettled: std::collections::BTreeSet<NodeId>,
}

impl StateFile {
    fn epoch(&self, origin: NodeId) -> u64 {
        self.epochs.get(&origin).copied().unwrap_or(0)
    }

    fn write_slot(&mut self, origin: NodeId, seq: u64, epoch: u64) -> io::Result<()> {
        let slot = match self.slots.get(&origin) {
            Some(slot) => *slot,
            None => {
                let slot = self.next_slot;
                self.next_slot += 1;
                self.slots.insert(origin, slot);
                slot
            }
        };
        let offset = STATE_SLOT + slot * STATE_SLOT;
        let bytes = Position { origin, seq, epoch }.encode();
        std::os::unix::fs::FileExt::write_all_at(&self.handle, &bytes, offset)?;
        if let Some(logged) = &self.logged {
            logged.wal.log_and_commit_deferred(logged.file_id, offset, &bytes)?;
        }
        self.epochs.insert(origin, epoch);
        Ok(())
    }

    fn write_intent(&mut self, intent: Intent) -> io::Result<()> {
        let bytes = intent.encode();
        let offset = self.intents_end;
        std::os::unix::fs::FileExt::write_all_at(&self.intents, &bytes, offset)?;
        match &self.intents_logged {
            Some(logged) => {
                logged.wal.log_and_commit_deferred(logged.file_id, offset, &bytes)?;
            }
            // Nothing else will carry it to disk ahead of the commit it
            // speaks for.
            None => self.intents.sync_data()?,
        }
        self.intents_end += INTENT_LEN;
        self.unsettled.insert(intent.origin);
        Ok(())
    }

    /// Empties the intents file once it has grown and nothing waits on it.
    ///
    /// What it held is behind every slot by then, and stays dead if a replay
    /// of the log's older records puts some of it back: an intent counts only
    /// from the exact position, and under the exact epoch, it was written
    /// for, and every slot has moved past those. A newer intent written over
    /// the same offset is later in the log, so the replay ends with it.
    fn trim_intents(&mut self, at: u64) {
        if self.intents_end < at || !self.unsettled.is_empty() {
            return;
        }
        if self.intents.set_len(0).is_ok() {
            self.intents_end = 0;
        }
    }
}

/// How far this node has applied each other origin's stream.
///
/// ## Exactly once
///
/// Applying a record and noting that it was applied are two writes, and a
/// crash between them is the whole problem: noted first, the transaction is
/// lost; noted after, as it was, it is applied again on restart, and a
/// replayed `INSERT` into a table without a unique key, or a `SET n = n + 1`,
/// lands twice.
///
/// So the note is made conditional and written first. Before an applying
/// transaction commits, an *intent* goes into the database's write-ahead log:
/// "this origin moves from `prev` to `seq`, if transaction `tx` committed".
/// The commit's outcome is logged after it, in the same log, and one sync
/// covers both. After any crash the log holds a prefix: an outcome is never
/// there without its intent. At open each origin's position is its slot, moved
/// along its intents for as long as each one's transaction committed, and it
/// stops at exactly the last transaction whose rows are there.
///
/// Several records may share one sync (see `server::Group`); each has its own
/// intent, so a cut part-way through such a batch still stops at the right
/// record. Once the sync has returned the slot itself is moved, and the
/// intents behind it count for nothing.
pub struct AppliedState {
    positions: Mutex<BTreeMap<NodeId, u64>>,
    file: Mutex<Option<StateFile>>,
}

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
        }
    }

    /// Opens the state in `dir` with no write-ahead log and nobody to ask
    /// about transactions: an intent whose transaction would have to be asked
    /// about counts as not applied.
    pub fn open(dir: &Path) -> io::Result<AppliedState> {
        Self::open_logged(dir, None, &|_| false)
    }

    /// Opens the state in `dir` and settles what a crash left.
    ///
    /// `committed` answers for a transaction id of this database. Every
    /// intent still standing is decided by it, once, here: before anything
    /// can forget how that transaction ended.
    pub fn open_logged(
        dir: &Path,
        wal: Option<Arc<Wal>>,
        committed: &dyn Fn(u64) -> bool,
    ) -> io::Result<AppliedState> {
        let path = dir.join(STATE_FILE);
        let mut handle = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;

        let mut bytes = Vec::new();
        handle.read_to_end(&mut bytes)?;

        let mut found: Vec<(u64, Position)> = Vec::new();
        if bytes.is_empty() {
            let mut header = [0u8; STATE_SLOT as usize];
            header[..16].copy_from_slice(&STATE_MAGIC);
            std::os::unix::fs::FileExt::write_all_at(&handle, &header, 0)?;
            handle.sync_data()?;
            crate::core::wal::sync_parent_dir(&path)?;
        } else if bytes.len() >= 16 && bytes[..16] == STATE_MAGIC {
            let slots = bytes.len().saturating_sub(STATE_SLOT as usize) / STATE_SLOT as usize;
            for slot in 0..slots {
                let at = STATE_SLOT as usize + slot * STATE_SLOT as usize;
                match Position::decode(&bytes[at..at + STATE_SLOT as usize]) {
                    Ok(Some(position)) => found.push((slot as u64, position)),
                    Ok(None) => {}
                    Err(()) => {
                        // Guessing low applies transactions twice and guessing
                        // high skips them. Neither is this code's to choose.
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "replication state {} is damaged at slot {}: how far a peer's stream \
                                 has been applied is not known. Reseed this node, or remove the file \
                                 and resynchronise each peer from a known sequence",
                                path.display(),
                                slot
                            ),
                        ));
                    }
                }
            }
        } else {
            // The file before this one: `(origin, seq)` pairs, the maximum
            // per origin being the position. Written out as slots, whole,
            // under a temporary name.
            let mut legacy: BTreeMap<NodeId, u64> = BTreeMap::new();
            let whole = bytes.len() - bytes.len() % 16;
            for chunk in bytes[..whole].chunks_exact(16) {
                let origin = u64::from_le_bytes(chunk[..8].try_into().expect("8 bytes"));
                let seq = u64::from_le_bytes(chunk[8..].try_into().expect("8 bytes"));
                let entry = legacy.entry(origin).or_insert(0);
                *entry = (*entry).max(seq);
            }
            let temp = path.with_extension("converting");
            let mut out = vec![0u8; STATE_SLOT as usize];
            out[..16].copy_from_slice(&STATE_MAGIC);
            for (slot, (origin, seq)) in legacy.iter().enumerate() {
                let position = Position {
                    origin: *origin,
                    seq: *seq,
                    epoch: 0,
                };
                out.extend_from_slice(&position.encode());
                found.push((slot as u64, position));
            }
            {
                let mut file = File::create(&temp)?;
                file.write_all(&out)?;
                file.sync_data()?;
            }
            std::fs::rename(&temp, &path)?;
            crate::core::wal::sync_parent_dir(&path)?;
            handle = OpenOptions::new().read(true).write(true).open(&path)?;
            log::info!(
                "Replication state {} converted to slots: {} origin(s)",
                path.display(),
                legacy.len()
            );
        }

        // One slot an origin. Two can only come of a slot's first write being
        // lost with its page and a second slot handed out since; the later
        // one, by epoch and then by sequence, is the origin's.
        found.sort_by_key(|(_, p)| (p.origin, p.epoch, p.seq));
        let mut latest: Vec<(u64, Position)> = Vec::new();
        for entry in found {
            match latest.last_mut() {
                Some(last) if last.1.origin == entry.1.origin => *last = entry,
                _ => latest.push(entry),
            }
        }
        let highest_slot = bytes.len().saturating_sub(STATE_SLOT as usize) as u64 / STATE_SLOT;
        let found = latest;

        let intents_path = dir.join(INTENTS_FILE);
        let mut intents = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&intents_path)?;
        let mut intent_bytes = Vec::new();
        intents.read_to_end(&mut intent_bytes)?;
        let standing: Vec<Intent> = intent_bytes
            .chunks_exact(INTENT_LEN as usize)
            .filter_map(Intent::decode)
            .collect();

        let logged = wal.clone().and_then(|wal| ReplicationLog::attach(&wal, &path));
        let intents_logged = wal.and_then(|wal| ReplicationLog::attach(&wal, &intents_path));
        let mut file = StateFile {
            handle,
            slots: found.iter().map(|(slot, p)| (p.origin, *slot)).collect(),
            // Past every slot the file has room for, used or not.
            next_slot: found.iter().map(|(slot, _)| slot + 1).max().unwrap_or(0).max(highest_slot),
            epochs: found.iter().map(|(_, p)| (p.origin, p.epoch)).collect(),
            logged,
            intents,
            intents_end: 0,
            intents_logged,
            unsettled: std::collections::BTreeSet::new(),
        };

        // Each origin: its slot, then along its intents while each step's
        // transaction committed.
        let mut positions: BTreeMap<NodeId, u64> = BTreeMap::new();
        let mut origins: Vec<(NodeId, u64, u64)> =
            found.iter().map(|(_, p)| (p.origin, p.seq, p.epoch)).collect();
        for intent in &standing {
            if !origins.iter().any(|(origin, _, _)| *origin == intent.origin) {
                // An origin's very first records: no slot was written yet.
                origins.push((intent.origin, 0, 0));
            }
        }
        for (origin, slot_seq, epoch) in origins {
            let mut position = slot_seq;
            let mut moved = false;
            let mut stopped = None;
            loop {
                let steps: Vec<&Intent> = standing
                    .iter()
                    .filter(|i| i.origin == origin && i.epoch == epoch && i.prev == position && i.seq > position)
                    .collect();
                if steps.is_empty() {
                    break;
                }
                // A record tried twice has two intents from one position; at
                // most one of their transactions committed.
                match steps.iter().find(|i| i.tx == 0 || committed(i.tx)) {
                    Some(step) => {
                        position = step.seq;
                        moved = true;
                    }
                    None => {
                        stopped = Some(steps[0].seq);
                        break;
                    }
                }
            }
            if let Some(seq) = stopped {
                log::warn!(
                    "Origin {}'s transaction {} was being applied when this node stopped and did \
                     not commit; it will be applied again",
                    origin,
                    seq
                );
            }
            // Settled into the slot, under a new epoch: nothing left in the
            // intents file can be read against it again.
            if moved || stopped.is_some() || standing.iter().any(|i| i.origin == origin) {
                file.write_slot(origin, position, epoch + 1)?;
            }
            if position != 0 {
                positions.insert(origin, position);
            }
        }
        // Every origin with an intent standing has a new epoch now, so the
        // file starts again from empty.
        file.intents_end = intent_bytes.len() as u64;
        file.trim_intents(0);

        Ok(AppliedState {
            positions: Mutex::new(positions),
            file: Mutex::new(Some(file)),
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

    /// Advances `origin` to `seq`, if that is forwards, on no transaction's
    /// account: for a record that was passed over. Not while intents for the
    /// origin are waiting on a sync — settle them first.
    ///
    /// Returns whether anything moved, so a duplicate costs no I/O.
    pub fn record(&self, origin: NodeId, seq: u64) -> io::Result<bool> {
        let mut file = self.file.lock();
        {
            let mut positions = self.positions.lock();
            let entry = positions.entry(origin).or_insert(0);
            if seq <= *entry {
                return Ok(false);
            }
            *entry = seq;
        }
        if let Some(file) = file.as_mut() {
            debug_assert!(!file.unsettled.contains(&origin), "a bare advance over waiting intents");
            let epoch = file.epoch(origin);
            file.write_slot(origin, seq, epoch)?;
        }
        Ok(true)
    }

    /// Writes "`origin` moves to `seq`, if `tx` commits", ahead of that
    /// commit. A `tx` of zero is a step with no transaction behind it.
    /// Returns the position it moves from.
    ///
    /// The position readers see does not move: [`AppliedState::advance`]
    /// moves it once the transaction has committed,
    /// [`AppliedState::settle`] makes it the slot's once that is durable, and
    /// [`AppliedState::rollback`] takes it back if it never is.
    pub fn begin(&self, origin: NodeId, seq: u64, tx: u64) -> io::Result<u64> {
        let mut file = self.file.lock();
        let prev = self.high_water(origin);
        if let Some(file) = file.as_mut() {
            let epoch = file.epoch(origin);
            file.write_intent(Intent {
                origin,
                epoch,
                prev,
                seq,
                tx,
            })?;
        }
        Ok(prev)
    }

    /// The transaction [`AppliedState::begin`] named has committed.
    pub fn advance(&self, origin: NodeId, seq: u64) {
        let mut positions = self.positions.lock();
        let entry = positions.entry(origin).or_insert(0);
        *entry = (*entry).max(seq);
    }

    /// Everything begun for `origin` is committed and durable: its slot moves
    /// to where the intents brought it.
    pub fn settle(&self, origin: NodeId) -> io::Result<()> {
        let mut file = self.file.lock();
        let position = self.high_water(origin);
        if let Some(file) = file.as_mut() {
            if !file.unsettled.remove(&origin) {
                return Ok(());
            }
            let epoch = file.epoch(origin);
            file.write_slot(origin, position, epoch)?;
            file.trim_intents(INTENTS_TRIM_AT);
        }
        Ok(())
    }

    /// What was begun for `origin` did not become durable: the position is
    /// `prev` again.
    ///
    /// Under a new epoch, so the intents written for it are dead. They could
    /// not be left for the next open to decide: an aborted transaction's id
    /// is eventually forgotten, and reads as committed from then on.
    pub fn rollback(&self, origin: NodeId, prev: u64) -> io::Result<()> {
        let mut file = self.file.lock();
        {
            let mut positions = self.positions.lock();
            if prev == 0 {
                positions.remove(&origin);
            } else {
                positions.insert(origin, prev);
            }
        }
        if let Some(file) = file.as_mut() {
            file.unsettled.remove(&origin);
            let epoch = file.epoch(origin) + 1;
            file.write_slot(origin, prev, epoch)?;
            file.trim_intents(INTENTS_TRIM_AT);
        }
        Ok(())
    }

    /// Takes the position readers see back to `prev`, and writes nothing.
    ///
    /// For when what was begun may or may not be durable and this process
    /// cannot tell (a sync failed): the intents stay as they are for the next
    /// start to decide, and until then nothing past `prev` is claimed.
    pub fn retreat(&self, origin: NodeId, prev: u64) {
        let mut positions = self.positions.lock();
        if prev == 0 {
            positions.remove(&origin);
        } else {
            positions.insert(origin, prev);
        }
    }

    /// Puts `origin` at `seq` whichever way that moves it. What `REPLICATION
    /// RESYNC … FROM` does; zero takes the stream again from the beginning.
    pub fn set(&self, origin: NodeId, seq: u64) -> io::Result<()> {
        self.rollback(origin, seq)?;
        self.flush()
    }

    /// Forgets what has been applied from `origin`.
    pub fn reset(&self, origin: NodeId) -> io::Result<()> {
        self.set(origin, 0)
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

    /// Syncs both files on duplicate descriptors, outside the lock, so an
    /// apply is never parked behind an fsync.
    pub fn flush(&self) -> io::Result<()> {
        let (slots, intents) = {
            let file = self.file.lock();
            match file.as_ref() {
                Some(file) => (file.handle.try_clone()?, file.intents.try_clone()?),
                None => return Ok(()),
            }
        };
        sync_unlocked(&intents)?;
        sync_unlocked(&slots)
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

    /// A catch-up older than the in-memory window reads the file. It read
    /// through the writer's own handle, which moved the write position: every
    /// append after it landed in the middle of the log, over older records.
    /// Found on a live pair: a burst of writes overflowed the peer queue, the
    /// peer was served from the file, and the link stayed down for good on
    /// "replication log record at … is unreadable".
    #[test]
    fn a_catch_up_from_the_file_does_not_move_where_appends_go() {
        let dir = tempfile::tempdir().unwrap();
        let log = ReplicationLog::open(dir.path(), 1 << 30).unwrap();
        let first = HOT_RECORDS as u64 + 1000;
        for seq in 1..=first {
            log.append(&record(1, seq)).unwrap();
        }
        // Older than the hot window: served from the file.
        let (early, _) = log.read_after(1, 0, 10, 1 << 20).unwrap();
        assert_eq!(early.iter().map(|r| r.seq).collect::<Vec<_>>(), (1..=10).collect::<Vec<_>>());
        let total = first + 500;
        for seq in first + 1..=total {
            log.append(&record(1, seq)).unwrap();
        }

        let (all, more) = log.read_after(1, 0, usize::MAX - 1, usize::MAX).unwrap();
        assert!(!more);
        assert_eq!(all.len() as u64, total, "every record reads back");
        for (i, r) in all.iter().enumerate() {
            assert_eq!(r.seq, i as u64 + 1);
            assert_eq!(r.statements, record(1, r.seq).statements, "record {} holds another's bytes", r.seq);
        }

        log.flush().unwrap();
        drop(log);
        let reopened = ReplicationLog::open(dir.path(), 1 << 30).unwrap();
        assert_eq!(reopened.highest(1), total, "a reopen keeps every record");
        assert_eq!(reopened.earliest(1), 1);
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
            log.bytes() <= 4096 + 1024,
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

    /// The slot is written ahead of the commit it depends on. After a crash
    /// it counts when that transaction committed, and is the position before
    /// it when it did not.
    #[test]
    fn a_position_written_ahead_counts_only_if_its_transaction_committed() {
        for committed in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            {
                let state = AppliedState::open(dir.path()).unwrap();
                state.record(1, 4).unwrap();
                assert_eq!(state.begin(1, 5, 900).unwrap(), 4);
                // A reader sees the old position until the commit.
                assert_eq!(state.high_water(1), 4);
                state.flush().unwrap();
            }
            let state = AppliedState::open_logged(dir.path(), None, &|tx| {
                assert_eq!(tx, 900);
                committed
            })
            .unwrap();
            assert_eq!(state.high_water(1), if committed { 5 } else { 4 });

            // Settled for good: a later open asks nobody.
            state.flush().unwrap();
            drop(state);
            let state =
                AppliedState::open_logged(dir.path(), None, &|_| panic!("already settled")).unwrap();
            assert_eq!(state.high_water(1), if committed { 5 } else { 4 });
        }
    }

    /// Several records share one sync. A cut part-way through leaves the
    /// position at the last one whose transaction committed, wherever that
    /// is: never at a later one, and never back at the start.
    #[test]
    fn a_batch_cut_part_way_stops_at_its_last_committed_step() {
        for (durable, expected) in [(0u64, 4u64), (1, 5), (2, 7), (3, 8)] {
            let dir = tempfile::tempdir().unwrap();
            {
                let state = AppliedState::open(dir.path()).unwrap();
                state.record(1, 4).unwrap();
                // 5 and 6 each by a transaction, 7 by none, 8 by one.
                for (seq, tx) in [(5, 901), (6, 902), (7, 0), (8, 903)] {
                    state.begin(1, seq, tx).unwrap();
                    state.advance(1, seq);
                }
                assert_eq!(state.high_water(1), 8);
                state.flush().unwrap();
            }
            // Outcomes are logged in order, so the durable ones are a prefix.
            let state = AppliedState::open_logged(dir.path(), None, &|tx| tx - 900 <= durable).unwrap();
            let expected = match durable {
                // 902 committed carries the step no transaction stands behind.
                2 => 7,
                _ => expected,
            };
            assert_eq!(state.high_water(1), expected, "{durable} outcome(s) durable");
        }
    }

    /// Once the sync has returned the slot holds the position, and the
    /// intents behind it are never asked about again.
    #[test]
    fn settled_intents_count_for_nothing() {
        let dir = tempfile::tempdir().unwrap();
        {
            let state = AppliedState::open(dir.path()).unwrap();
            for (seq, tx) in [(1, 901), (2, 902)] {
                state.begin(1, seq, tx).unwrap();
                state.advance(1, seq);
            }
            state.settle(1).unwrap();
            state.flush().unwrap();
        }
        let state = AppliedState::open_logged(dir.path(), None, &|_| panic!("already in the slot")).unwrap();
        assert_eq!(state.high_water(1), 2);
    }

    #[test]
    fn a_position_taken_back_is_the_one_before() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppliedState::open(dir.path()).unwrap();
        state.record(1, 4).unwrap();
        let before = state.begin(1, 5, 900).unwrap();
        state.advance(1, 5);
        assert_eq!(state.high_water(1), 5);
        state.rollback(1, before).unwrap();
        assert_eq!(state.high_water(1), 4);
        state.flush().unwrap();
        drop(state);

        let state = AppliedState::open_logged(dir.path(), None, &|_| panic!("nothing to ask")).unwrap();
        assert_eq!(state.high_water(1), 4);
    }

    /// One slot an origin, overwritten in place: the file does not grow with
    /// the stream, so it never needs rewriting.
    #[test]
    fn the_state_file_holds_one_slot_an_origin_however_far_it_advances() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppliedState::open(dir.path()).unwrap();
        for seq in 1..=20_000 {
            state.record(1, seq).unwrap();
            if seq % 5_000 == 0 {
                state.record(2, seq).unwrap();
            }
        }
        state.flush().unwrap();
        let size = std::fs::metadata(dir.path().join(STATE_FILE)).unwrap().len();
        assert_eq!(size, STATE_SLOT * 3, "a header and two slots");
        drop(state);

        let state = AppliedState::open(dir.path()).unwrap();
        assert_eq!(state.high_water(1), 20_000);
        assert_eq!(state.high_water(2), 20_000);
    }

    /// The file an earlier build wrote: `(origin, seq)` pairs, the maximum
    /// counting.
    #[test]
    fn a_state_file_of_pairs_is_converted() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = Vec::new();
        for (origin, seq) in [(1u64, 3u64), (2, 9), (1, 7), (1, 5)] {
            bytes.extend_from_slice(&origin.to_le_bytes());
            bytes.extend_from_slice(&seq.to_le_bytes());
        }
        // And a torn pair at its end, which that format cut off.
        bytes.extend_from_slice(&[1, 2, 3]);
        std::fs::write(dir.path().join(STATE_FILE), &bytes).unwrap();

        let state = AppliedState::open(dir.path()).unwrap();
        assert_eq!(state.high_water(1), 7);
        assert_eq!(state.high_water(2), 9);
        state.record(1, 8).unwrap();
        state.flush().unwrap();
        drop(state);

        let state = AppliedState::open(dir.path()).unwrap();
        assert_eq!(state.high_water(1), 8);
        assert_eq!(state.high_water(2), 9);
    }

    /// A slot that does not check out is not a position anyone should guess.
    #[test]
    fn a_damaged_slot_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let state = AppliedState::open(dir.path()).unwrap();
            state.record(1, 4).unwrap();
            state.flush().unwrap();
        }
        let path = dir.path().join(STATE_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[STATE_SLOT as usize + 8] ^= 0x40;
        std::fs::write(&path, &bytes).unwrap();

        let error = AppliedState::open(dir.path()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    /// A record written ahead of a commit that never happened is found at
    /// open, with the transaction to ask about.
    #[test]
    fn a_reopened_log_reports_the_records_a_crash_left_undecided() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
            log.append_for(&record(1, 1), 70).unwrap();
            log.confirm(1);
            log.append_for(&record(1, 2), 71).unwrap();
            log.append(&record(1, 3)).unwrap();
            log.flush().unwrap();
        }
        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        // Confirming is in memory only: the file still names both
        // transactions, and both are asked about.
        let mut undecided = log.take_undecided();
        undecided.sort();
        assert_eq!(undecided, vec![(1, 1, 70), (1, 2, 71)]);
        assert!(log.take_undecided().is_empty(), "taken once");
    }

    #[test]
    fn a_voided_record_keeps_its_place_and_carries_nothing() {
        let dir = tempfile::tempdir().unwrap();
        {
            let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
            for seq in 1..=3 {
                log.append_for(&record(1, seq), 70 + seq).unwrap();
            }
            log.void(1, 2, 5).unwrap();
            let (records, _) = log.read_after(1, 0, 10, 1 << 20).unwrap();
            assert_eq!(records.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1, 2, 3]);
            assert!(records[1].is_void() && records[1].statements.is_empty());
            assert!(!records[0].is_void() && !records[2].is_void());
            log.flush().unwrap();
        }
        // And from the file: the void is the record a reopen finds.
        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        let (records, _) = log.read_after(1, 0, 10, 1 << 20).unwrap();
        assert_eq!(records.len(), 3);
        assert!(records[1].is_void());
        let mut undecided = log.take_undecided();
        undecided.sort();
        assert_eq!(undecided, vec![(1, 1, 71), (1, 3, 73)], "the voided one is decided");
    }

    #[test]
    fn a_read_stops_at_the_sequence_it_is_capped_at() {
        let dir = tempfile::tempdir().unwrap();
        let log = ReplicationLog::open(dir.path(), 1 << 20).unwrap();
        for seq in 1..=10 {
            log.append(&record(1, seq)).unwrap();
        }
        let (records, more) = log.read_range(1, 2, 6, 100, 1 << 20).unwrap();
        assert_eq!(records.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![3, 4, 5, 6]);
        assert!(!more);
    }


    /// The log is files that are only ever appended to. Past its budget the
    /// oldest go whole; a reopen reads the rest in order and finds every
    /// record that was kept, with the transaction it named.
    #[test]
    fn the_log_grows_by_files_and_lets_the_oldest_go() {
        let dir = tempfile::tempdir().unwrap();
        let files = |dir: &Path| -> Vec<u64> {
            let mut generations: Vec<u64> = std::fs::read_dir(dir)
                .unwrap()
                .flatten()
                .filter_map(|e| e.file_name().to_str().and_then(generation_of))
                .collect();
            generations.sort_unstable();
            generations
        };
        {
            let log = ReplicationLog::open(dir.path(), 4096).unwrap();
            for seq in 1..=400 {
                log.append_for(&record(1, seq), if seq % 2 == 0 { 1000 + seq } else { 0 }).unwrap();
            }
            log.flush().unwrap();
            assert_eq!(log.highest(1), 400);
            // Read across files while they are open.
            let earliest = log.earliest(1);
            let (records, _) = log.read_after(1, earliest - 1, 1000, 1 << 30).unwrap();
            assert_eq!(records.len() as u64, 400 - earliest + 1);
        }
        let held = files(dir.path());
        assert!(held.len() > 1, "several files at a time: {held:?}");
        assert!(held[0] > 0, "the first file was let go: {held:?}");
        assert!(held.windows(2).all(|w| w[1] == w[0] + 1), "no file missing in between: {held:?}");

        let log = ReplicationLog::open(dir.path(), 4096).unwrap();
        assert_eq!(log.highest(1), 400);
        let earliest = log.earliest(1);
        assert!(earliest > 1, "the oldest records were dropped");
        let mut from = earliest - 1;
        loop {
            let (records, more) = log.read_after(1, from, 64, 1 << 20).unwrap();
            for r in &records {
                assert_eq!(r.seq, from + 1);
                assert_eq!(r.statements, record(1, r.seq).statements);
                from = r.seq;
            }
            if !more {
                break;
            }
        }
        assert_eq!(from, 400);
        let undecided = log.take_undecided();
        assert!(!undecided.is_empty());
        assert!(undecided.iter().all(|(_, seq, tx)| *tx == 1000 + *seq && seq % 2 == 0 && *seq >= earliest));

        // And it goes on appending where it stopped, in the newest file.
        log.append(&record(1, 401)).unwrap();
        let (records, _) = log.read_after(1, 399, 10, 1 << 20).unwrap();
        assert_eq!(records.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![400, 401]);
    }

    /// A file of the log is restorable from the write-ahead log only if
    /// everything in it was appended: nothing may ever be written into one
    /// except at its end.
    #[test]
    fn a_file_of_the_log_is_never_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let log = ReplicationLog::open(dir.path(), 8192).unwrap();
        let mut seen: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for seq in 1..=300 {
            log.append(&record(1, seq)).unwrap();
            if seq % 7 == 0 {
                log.void(1, seq - 1, 5).unwrap();
            }
            for entry in std::fs::read_dir(dir.path()).unwrap().flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if generation_of(&name).is_none() {
                    continue;
                }
                let now = std::fs::read(entry.path()).unwrap();
                if let Some(before) = seen.get(&name) {
                    assert!(now.starts_with(before), "{name} changed below its end at sequence {seq}");
                }
                seen.insert(name, now);
            }
        }
    }

    /// A connected peer that has not been sent a file yet keeps it past the
    /// budget; once it has, or once the log reaches its hard limit, the
    /// budget applies again.
    #[test]
    fn a_file_a_connected_peer_still_needs_outlives_the_budget_up_to_a_limit() {
        let dir = tempfile::tempdir().unwrap();
        let log = ReplicationLog::open(dir.path(), 4096).unwrap();

        // A peer at sequence 10 is being sent the stream.
        log.keep_for_peers(1, 10);
        for seq in 1..=120 {
            log.append(&record(1, seq)).unwrap();
        }
        assert!(log.bytes() > 4096, "held past the budget for the peer: {}", log.bytes());
        assert!(log.earliest(1) <= 11, "what the peer needs next is still here");
        let (records, _) = log.read_after(1, 10, 1000, 1 << 30).unwrap();
        assert_eq!(records.len(), 110);

        // It catches up: the budget applies at the next file.
        log.keep_for_peers(1, 120);
        for seq in 121..=140 {
            log.append(&record(1, seq)).unwrap();
        }
        assert!(log.bytes() <= 4096 + 1024, "back within the budget: {}", log.bytes());

        // A peer that never moves does not hold the log for ever.
        log.keep_for_peers(1, 0);
        for seq in 141..=1400 {
            log.append(&record(1, seq)).unwrap();
        }
        assert!(
            log.bytes() <= 4096 * KEEP_FOR_PEERS_FACTOR + 1024,
            "the hard limit: {}",
            log.bytes()
        );
        assert!(log.earliest(1) > 1);
    }
}
