//! The open's proof (docs/persistent-indexes.md §4, slice B14): whether a
//! file set's snapshot may be trusted, checked without writing anything,
//! and the FILTER section it hands the load when it may.
//!
//! # The checks, in order
//!
//! A snapshot is used only if **every** check passes. Each failure is a
//! [`Reject`], logged at info by the caller and counted here
//! ([`rejected_total`]), and the table rebuilds exactly as before B14.
//!
//! | # | Check | [`Reject`] |
//! |---|---|---|
//! | 1 | the file set has a directory (P33) | `Excluded` |
//! | 2 | no `_flt.db` evidence beside `.ixm`, and no legacy pair (§5) | `FltEvidence` |
//! | 3 | the word is `Clean` (the log ends `CLEAN`, nothing wrote since) | `NotClean` |
//! | 4 | the slot epoch is the recovered `rdb.ixe` record's | `EpochUnproven` |
//! | 5 | `.ixm` reads, as this incarnation's log | `NoLog`, `LogUnreadable`, `ForeignLog` |
//! | 6 | its last good record is `CLEAN` with no byte after it (B9's `proven_clean`) | `LogNotClean`, `TrailingBytes` |
//! | 7 | the `CLEAN`'s identity equals the files and the binary, field by field: incarnation, slot epoch, slot count, pointer and row file lengths, the version tuple (`FILTER_DERIVE_VERSION` and `persist::FORMAT_VER` among it), shape hash, filter configuration hash | `Identity(field)` |
//! | 8 | `.ixs` opens, and its length is the `CLEAN`'s | `NoSnapshot`, `SnapshotUnreadable`, `SnapshotLength` |
//! | 9 | read once as a stream (B17: never held whole), its header crc32c, every section's crc32c and the footer xxh64 verify | `SnapshotCorrupt`, `SnapshotUnreadable` |
//! | 10 | its footer is the `CLEAN`'s, and its header's identity is the `CLEAN`'s | `Footer`, `SnapshotIdentity(field)` |
//! | 11 | it holds a FILTER section exactly when the table has a filter | `NoFilterSection`, `UnexpectedFilterSection` |
//!
//! The load itself (`TableFilters::load_sealed`) then checks the section's
//! own superblock and column list; a rejection there is `FilterLoad`. The
//! INDEX sections' entries go to the index load ([`super::load`], B17),
//! which reads each again against its crc32c; a failure there rebuilds the
//! indexes and leaves the filter loaded.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use super::file_gate::{FileGate, FltAtOpen};
use super::gate_word::GateState;
use super::identity::{Identity, IdentityField};
use super::marker;
use super::snapshot::{self, SectionEntry, SectionKind};

/// Why an open did not trust its snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reject {
    Excluded,
    FltEvidence,
    NotClean(GateState),
    EpochUnproven,
    NoLog,
    LogUnreadable(String),
    ForeignLog,
    LogNotClean,
    TrailingBytes(u64),
    Identity(IdentityField),
    NoSnapshot,
    SnapshotUnreadable(String),
    SnapshotLength { clean: u64, file: u64 },
    SnapshotCorrupt(String),
    Footer,
    SnapshotIdentity(IdentityField),
    NoFilterSection,
    UnexpectedFilterSection,
    FilterLoad(String),
}

impl Reject {
    /// The counter's index, one per variant (an identity field counts under
    /// its variant).
    fn code(&self) -> usize {
        match self {
            Reject::Excluded => 0,
            Reject::FltEvidence => 1,
            Reject::NotClean(_) => 2,
            Reject::EpochUnproven => 3,
            Reject::NoLog => 4,
            Reject::LogUnreadable(_) => 5,
            Reject::ForeignLog => 6,
            Reject::LogNotClean => 7,
            Reject::TrailingBytes(_) => 8,
            Reject::Identity(_) => 9,
            Reject::NoSnapshot => 10,
            Reject::SnapshotUnreadable(_) => 11,
            Reject::SnapshotLength { .. } => 12,
            Reject::SnapshotCorrupt(_) => 13,
            Reject::Footer => 14,
            Reject::SnapshotIdentity(_) => 15,
            Reject::NoFilterSection => 16,
            Reject::UnexpectedFilterSection => 17,
            Reject::FilterLoad(_) => 18,
        }
    }

    /// A short, stable name, for the counter's label and the log.
    pub fn name(&self) -> &'static str {
        NAMES[self.code()]
    }
}

const NAMES: [&str; 19] = [
    "excluded",
    "flt_evidence",
    "not_clean",
    "epoch_unproven",
    "no_log",
    "log_unreadable",
    "foreign_log",
    "log_not_clean",
    "trailing_bytes",
    "identity",
    "no_snapshot",
    "snapshot_unreadable",
    "snapshot_length",
    "snapshot_corrupt",
    "footer",
    "snapshot_identity",
    "no_filter_section",
    "unexpected_filter_section",
    "filter_load",
];

static REJECTED: [AtomicU64; 19] = [const { AtomicU64::new(0) }; 19];
static LOADED: AtomicU64 = AtomicU64::new(0);

/// Opens that rejected their snapshot for `reason` (by [`Reject::name`]).
pub fn rejected_total(reason: &str) -> u64 {
    NAMES
        .iter()
        .position(|n| *n == reason)
        .map_or(0, |i| REJECTED[i].load(Ordering::Relaxed))
}

/// `rdb_index_snapshot_loaded_total`: opens that loaded a snapshot's
/// FILTER section instead of rebuilding.
pub fn loaded_total() -> u64 {
    LOADED.load(Ordering::Relaxed)
}

/// Counts and logs a rejection. `Excluded` and `NoLog` are the normal case
/// of a table that never had a snapshot and are counted but logged at
/// debug only.
pub fn note_rejected(stem: &str, why: &Reject) {
    REJECTED[why.code()].fetch_add(1, Ordering::Relaxed);
    match why {
        Reject::Excluded | Reject::NoLog => log::debug!("{stem}: no index snapshot to load ({})", why.name()),
        _ => log::info!("{stem}: the index snapshot is not used ({}: {why:?}); rebuilding", why.name()),
    }
}

/// Counts a load.
pub fn note_loaded() {
    LOADED.fetch_add(1, Ordering::Relaxed);
}

/// What a proven snapshot hands the load.
#[derive(Debug)]
pub struct Proven {
    /// The FILTER section's bytes, verified; `None` when the table has no
    /// filter (and the snapshot, correctly, no section).
    pub filter: Option<Vec<u8>>,
    /// The INDEX sections' entries, checked with the rest of the file; the
    /// index load reads each again against its crc32c (B17).
    pub index: Vec<SectionEntry>,
    /// Whether the snapshot holds a GIN section (none is written in phase
    /// 2: a table with a gin index rebuilds its indexes).
    pub has_gin: bool,
    /// The snapshot's path, for that read.
    pub path: std::path::PathBuf,
}

/// Runs the checks of the module documentation against `expected`, the
/// identity the files and this binary have now (the row layer builds it at
/// open, as the seal built it at capture). Reads `.ixm` and `.ixs`; writes
/// nothing. `has_filter` is whether the table will have a filter.
pub fn prove(gate: &FileGate, flt: &FltAtOpen, expected: &Identity, has_filter: bool) -> Result<Proven, Reject> {
    super::file_gate::blocking(|| prove_now(gate, flt, expected, has_filter))
}

fn prove_now(gate: &FileGate, flt: &FltAtOpen, expected: &Identity, has_filter: bool) -> Result<Proven, Reject> {
    let (Some(ixm), Some(ixs)) = (gate.ixm_path(), gate.ixs_path()) else {
        return Err(Reject::Excluded);
    };
    if flt.refuses_snapshot() {
        return Err(Reject::FltEvidence);
    }
    let state = gate.state();
    if state != GateState::Clean {
        return Err(Reject::NotClean(state));
    }
    if !gate.epoch_proven() {
        return Err(Reject::EpochUnproven);
    }
    let log = match std::fs::read(&ixm) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Reject::NoLog),
        Err(e) => return Err(Reject::LogUnreadable(e.to_string())),
    };
    let scan = marker::scan(&log).map_err(|e| Reject::LogUnreadable(e.to_string()))?;
    if scan.incarnation != expected.incarnation || scan.incarnation != gate.incarnation() {
        return Err(Reject::ForeignLog);
    }
    let clean = match scan.proven_clean() {
        Some(c) => *c,
        None if scan.state() == marker::LogState::Clean => return Err(Reject::TrailingBytes(scan.trailing())),
        None => return Err(Reject::LogNotClean),
    };
    if let Some(field) = clean.identity.first_mismatch(expected) {
        return Err(Reject::Identity(field));
    }
    // Streamed once, front to back, never held whole (§4 "The load"): only
    // the FILTER section is kept. The INDEX sections are read again, one at
    // a time and each against its crc32c, by the index load (B17).
    let mut file = match std::fs::File::open(&ixs) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Reject::NoSnapshot),
        Err(e) => return Err(Reject::SnapshotUnreadable(e.to_string())),
    };
    let file_len = file.metadata().map_err(|e| Reject::SnapshotUnreadable(e.to_string()))?.len();
    if file_len != clean.snapshot_len {
        return Err(Reject::SnapshotLength {
            clean: clean.snapshot_len,
            file: file_len,
        });
    }
    let mut reader = io::BufReader::with_capacity(1 << 16, &mut file);
    let snap = snapshot::verify_stream(&mut reader, file_len, |e| e.kind == SectionKind::Filter && e.ordinal == 0)
        .map_err(|e| match e {
            snapshot::StreamError::Format(f) => Reject::SnapshotCorrupt(f.to_string()),
            snapshot::StreamError::Io(e) => Reject::SnapshotUnreadable(e.to_string()),
        })?;
    if snap.footer != clean.footer {
        return Err(Reject::Footer);
    }
    if let Some(field) = snap.header.identity.first_mismatch(&clean.identity) {
        return Err(Reject::SnapshotIdentity(field));
    }
    let section = snap.kept.into_iter().next().map(|(_, b)| b);
    let index: Vec<SectionEntry> =
        snap.header.sections.iter().filter(|e| e.kind == SectionKind::Index).copied().collect();
    let has_gin = snap.header.sections.iter().any(|e| e.kind == SectionKind::Gin);
    let proven = |filter| Proven {
        filter,
        index,
        has_gin,
        path: ixs.clone(),
    };
    match (section, has_filter) {
        (Some(b), true) => Ok(proven(Some(b))),
        (None, false) => Ok(proven(None)),
        (None, true) => Err(Reject::NoFilterSection),
        (Some(_), false) => Err(Reject::UnexpectedFilterSection),
    }
}

#[cfg(test)]
#[path = "open_proof_tests.rs"]
mod tests;
