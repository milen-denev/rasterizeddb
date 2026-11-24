//! A shard's Raft storage (sharding-plan.md B3): what `rasterizeddb_raft`'s
//! `Ready` says to persist, durable before any message that depends on it
//! leaves.
//!
//! Three files under `<data>/raft/`:
//! - `raft_log`: framed records (`core::framed_log`), appended and synced:
//!   - `ENTRY` (1): one entry, in the core's codec;
//!   - `TRUNCATE` (2): `from: u64`, drop entries from there on;
//!   - `RESET` (3): `index: u64, term: u64`, the log is now a snapshot's
//!     position with no entries.
//!   A torn tail is cut at open; damage followed by an intact record refuses
//!   the log, as every framed log here does.
//! - `raft_hard_state`: the term, the vote and the commit index, replaced
//!   atomically (written beside, synced, renamed, the directory synced).
//! - `raft_snapshot`: the newest snapshot's meta and data, replaced the same
//!   way.
//!
//! [`RaftStore::compact`] rewrites the log from a snapshot's position, so it
//! does not grow without bound.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use rasterizeddb_raft::codec::{Reader, Writer, decode_hard_state, encode_hard_state};
use rasterizeddb_raft::{Entry, HardState, Index, RaftLog, Ready, SnapshotMeta, Term};

use crate::core::framed_log::{self, Format};

const LOG: &str = "raft_log";
const HARD: &str = "raft_hard_state";
const SNAPSHOT: &str = "raft_snapshot";

const ENTRY: u8 = 1;
const TRUNCATE: u8 = 2;
const RESET: u8 = 3;
const LAYOUT: u8 = 1;

const FORMAT: Format = Format {
    name: "raft log",
    max_record_len: 1 << 30,
    layout: LAYOUT,
    known_kind: |k| matches!(k, ENTRY | TRUNCATE | RESET),
};

pub struct RaftStore {
    dir: PathBuf,
    log: File,
}

/// What a store holds at open.
pub struct Restored {
    pub hard: HardState,
    pub log: RaftLog,
    pub snapshot: Option<(SnapshotMeta, Vec<u8>)>,
}

fn io(path: &Path) -> impl Fn(std::io::Error) -> String + '_ {
    move |e| format!("{}: {e}", path.display())
}

fn record(kind: u8, body: impl FnOnce(&mut Vec<u8>)) -> Result<Vec<u8>, String> {
    let mut buf = framed_log::begin(kind, LAYOUT);
    body(&mut buf);
    framed_log::finish(&FORMAT, buf).map_err(|n| format!("a raft log record of {n} bytes is too long"))
}

/// Writes `bytes` to `dir/name` atomically and durably.
fn replace(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), String> {
    let path = dir.join(name);
    let tmp = dir.join(format!("{name}.tmp"));
    let mut f = File::create(&tmp).map_err(io(&tmp))?;
    f.write_all(bytes).map_err(io(&tmp))?;
    f.sync_all().map_err(io(&tmp))?;
    drop(f);
    std::fs::rename(&tmp, &path).map_err(io(&path))?;
    crate::core::sync_path::sync_dir(dir).map_err(io(dir))?;
    Ok(())
}

impl RaftStore {
    /// Opens (or creates) the store under `dir` and replays it.
    pub fn open(dir: &Path) -> Result<(RaftStore, Restored), String> {
        std::fs::create_dir_all(dir).map_err(io(dir))?;
        let hard = match std::fs::read(dir.join(HARD)) {
            Ok(b) => decode_hard_state(&b).map_err(|e| format!("{}: {e}", dir.join(HARD).display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HardState::default(),
            Err(e) => return Err(format!("{}: {e}", dir.join(HARD).display())),
        };
        let snapshot = match std::fs::read(dir.join(SNAPSHOT)) {
            Ok(b) => {
                let meta = Reader::new(&b).snapshot_meta()?;
                let data = b[meta_len(&meta)..].to_vec();
                Some((meta, data))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(format!("{}: {e}", dir.join(SNAPSHOT).display())),
        };
        let path = dir.join(LOG);
        let mut file = OpenOptions::new().read(true).append(true).create(true).open(&path).map_err(io(&path))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(io(&path))?;
        let (mut s_index, mut s_term) = snapshot.as_ref().map_or((0, 0), |(m, _)| (m.index, m.term));
        let mut entries: Vec<Entry> = Vec::new();
        let end = framed_log::scan(&FORMAT, &bytes, |_, kind, payload| {
            let body = payload.get(1..).ok_or("a record with no layout byte")?;
            let mut r = Reader::new(body);
            match kind {
                ENTRY => {
                    let e = r.entry()?;
                    r.done()?;
                    let want = s_index + entries.len() as u64 + 1;
                    if e.index != want {
                        return Err(format!("entry {} where {want} was due", e.index));
                    }
                    entries.push(e);
                }
                TRUNCATE => {
                    let from = u64::from_le_bytes(body.get(..8).ok_or("short TRUNCATE")?.try_into().unwrap());
                    if from <= s_index {
                        return Err(format!("TRUNCATE from {from}, at or below the snapshot {s_index}"));
                    }
                    entries.truncate((from - s_index - 1) as usize);
                }
                RESET => {
                    let b = body.get(..16).ok_or("short RESET")?;
                    s_index = u64::from_le_bytes(b[..8].try_into().unwrap());
                    s_term = u64::from_le_bytes(b[8..].try_into().unwrap());
                    entries.clear();
                }
                k => return Err(format!("unknown record kind {k}")),
            }
            Ok(())
        })?;
        if let Some(torn) = &end.torn {
            log::warn!("{}: torn tail at byte {} ({}); cut", path.display(), torn.offset, torn.reason);
            file.set_len(end.valid_len as u64).map_err(io(&path))?;
            file.sync_all().map_err(io(&path))?;
        }
        // A snapshot newer than the log's reset point covers what it says.
        if let Some((m, _)) = &snapshot
            && m.index > s_index
        {
            entries.retain(|e| e.index > m.index);
            if entries.first().is_some_and(|e| e.index != m.index + 1) {
                entries.clear();
            }
            s_index = m.index;
            s_term = m.term;
        }
        let commit = hard.commit.max(s_index).min(s_index + entries.len() as u64);
        let log = RaftLog::restore(s_index, s_term, entries, commit)?;
        Ok((RaftStore { dir: dir.to_path_buf(), log: file }, Restored { hard, log, snapshot }))
    }

    /// Makes `ready`'s durable work durable (the core's contract, step 1).
    pub fn persist(&mut self, ready: &Ready) -> Result<(), String> {
        self.persist_parts(ready.snapshot.as_ref(), ready.snapshot_keeps_log, ready.truncate_from, &ready.entries, ready.hard_state.as_ref())
    }

    /// [`Self::persist`] over the parts a `Ready` carries for storage, so a
    /// caller moves only those to a blocking thread.
    pub fn persist_parts(
        &mut self,
        snapshot: Option<&(SnapshotMeta, Vec<u8>)>,
        snapshot_keeps_log: bool,
        truncate_from: Option<Index>,
        entries: &[Entry],
        hard_state: Option<&HardState>,
    ) -> Result<(), String> {
        let ready = PersistView { snapshot, truncate_from, entries, hard_state };
        let path = self.dir.join(LOG);
        let mut buf = Vec::new();
        if let Some((meta, data)) = ready.snapshot {
            let mut w = Writer::default();
            w.snapshot_meta(meta);
            let mut bytes = w.0;
            bytes.extend_from_slice(data);
            replace(&self.dir, SNAPSHOT, &bytes)?;
            // A log that agreed with the snapshot keeps its entries after
            // it: the restore drops those at or below the snapshot's index
            // (`open`). Only a log replaced whole is reset.
        }
        if let Some((meta, _)) = ready.snapshot.filter(|_| !snapshot_keeps_log) {
            buf.extend(record(RESET, |b| {
                b.extend_from_slice(&meta.index.to_le_bytes());
                b.extend_from_slice(&meta.term.to_le_bytes());
            })?);
        }
        if let Some(from) = ready.truncate_from {
            buf.extend(record(TRUNCATE, |b| b.extend_from_slice(&from.to_le_bytes()))?);
        }
        for e in ready.entries {
            buf.extend(record(ENTRY, |b| {
                let mut w = Writer::default();
                w.entry(e);
                b.extend_from_slice(&w.0);
            })?);
        }
        if !buf.is_empty() {
            self.log.write_all(&buf).map_err(io(&path))?;
            self.log.sync_data().map_err(io(&path))?;
        }
        if let Some(h) = ready.hard_state {
            replace(&self.dir, HARD, &encode_hard_state(h))?;
        }
        Ok(())
    }

    /// Rewrites the log as of a snapshot at `(index, term)`, keeping
    /// `entries` (those after it): written beside, synced, renamed over.
    pub fn compact(&mut self, index: Index, term: Term, entries: &[Entry]) -> Result<(), String> {
        let mut buf = record(RESET, |b| {
            b.extend_from_slice(&index.to_le_bytes());
            b.extend_from_slice(&term.to_le_bytes());
        })?;
        for e in entries {
            buf.extend(record(ENTRY, |b| {
                let mut w = Writer::default();
                w.entry(e);
                b.extend_from_slice(&w.0);
            })?);
        }
        replace(&self.dir, LOG, &buf)?;
        let path = self.dir.join(LOG);
        self.log = OpenOptions::new().append(true).open(&path).map_err(io(&path))?;
        Ok(())
    }
}

/// The storage half of a `Ready`, borrowed.
struct PersistView<'a> {
    snapshot: Option<&'a (SnapshotMeta, Vec<u8>)>,
    truncate_from: Option<Index>,
    entries: &'a [Entry],
    hard_state: Option<&'a HardState>,
}

/// The encoded length of `meta`, to find where the snapshot's data starts.
fn meta_len(meta: &SnapshotMeta) -> usize {
    let mut w = Writer::default();
    w.snapshot_meta(meta);
    w.0.len()
}
