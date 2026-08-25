use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use cacheguard::CacheGuard;

pub(crate) const SERVER_PORT: u16 = 61170;

pub static MAX_PERMITS_THREADS: OnceLock<usize> = OnceLock::new(); // 16 by default

/// Worker threads for a query, defaulting when nothing has set it.
///
/// Read through this rather than unwrapping the `OnceLock`. The binary sets it
/// from a CLI flag, so every code path that builds a database *without* going
/// through `main` — every test module, embedded use, and anything reached from a
/// new entry point — panicked on the bare unwrap instead of running. Three test
/// modules discovered that trap independently before it was removed.
pub fn max_permits_threads() -> usize {
    MAX_PERMITS_THREADS.get().copied().unwrap_or(16)
}

// Number of row pointers to fetch at once in next_row_pointers
pub static BATCH_SIZE: OnceLock<usize> = OnceLock::new(); // 10K by default

/// Rows read per batch, defaulting when nothing has set it. See
/// [`max_permits_threads`] for why this is an accessor.
pub fn batch_size() -> usize {
    BATCH_SIZE.get().copied().unwrap_or(10_000)
}


/// Filter tuning lives in `core::filter::settings`, which is runtime-mutable
/// so `SET FILTER ...` and the `--filter-*` CLI flags reach the same state.
/// A `OnceLock` could not serve both: it is set-once, and these are not.
pub use crate::core::filter::settings::settings as filter_settings;

// ---------------------------------------------------------------------------
// WAL and vacuum knobs
//
/// Route durable writes through the write-ahead log (`core::wal`).
/// Default: **enabled**.
///
/// With the log on, an append is made durable by one sequential record plus a
/// shared flush of a single file, instead of each data file being flushed on its
/// own. Turning it off restores per-file flushing; it does not skip recovery,
/// which always runs when a log file is present so that disabling the feature
/// cannot strand un-checkpointed writes.
pub static WAL_ENABLED: OnceLock<bool> = OnceLock::new();

/// Log bytes after which a checkpoint is taken: every data file is flushed and
/// the log is discarded. Larger means fewer checkpoints but a longer replay
/// after a crash. Default: 64 MiB.
pub static WAL_CHECKPOINT_BYTES: OnceLock<u64> = OnceLock::new();

#[inline]
pub fn wal_enabled() -> bool {
    *WAL_ENABLED.get().unwrap_or(&true)
}

#[inline]
pub fn wal_checkpoint_bytes() -> u64 {
    *WAL_CHECKPOINT_BYTES.get().unwrap_or(&(64 * 1024 * 1024))
}

/// Whether a commit waits for the log to reach the disk before reporting
/// success. PostgreSQL's `synchronous_commit`, and the same trade.
/// Default: **on**.
///
/// **On**: nothing is acknowledged until its records are durable, so a crash
/// loses nothing that was reported as committed. That costs one `fdatasync` per
/// writing statement, which on a drive without a power-loss-protected cache is
/// most of what the statement takes — measured here at 4.8 ms of a 5.2 ms
/// `INSERT`.
///
/// **Off**: the records are written to the log, but the sync is left to
/// [`crate::core::log_writer`], so a commit returns once the bytes are in the
/// page cache.
///
/// What that risks is narrower than it sounds, and worth being exact about
/// because it is the whole basis for choosing it. The bytes are in the kernel's
/// page cache, not this process's memory, so **the server process dying loses
/// nothing** — `kill -9` and a panic are both survivable, and this was measured
/// rather than assumed: 200 asynchronously committed rows came back in full
/// after a `SIGKILL` with the writer delay set to a minute. Only an **operating
/// system crash or a power loss** can lose them, and then only the last
/// [`wal_writer_delay_ms`] of them. PostgreSQL draws the same line for the same
/// reason.
///
/// What it does **not** do is risk the database. Records are appended in order
/// and replay stops at the first one it cannot read whole, so what is lost is
/// whole transactions off the end of the log — never half of one, and never a
/// transaction whose outcome survived its rows. This is a durability knob, not
/// a consistency one, and that distinction is the whole reason it is safe to
/// offer.
///
/// An `AtomicBool` rather than a `OnceLock` because it is settable per session
/// (`SET synchronous_commit = off`) and per server at runtime (`SET CONFIG
/// wal.synchronous_commit = off`), not only at start-up.
pub static SYNCHRONOUS_COMMIT: CacheGuard<AtomicBool> = CacheGuard::new(AtomicBool::new(true));

/// How long the log writer waits between flushes, in milliseconds.
///
/// This is the bound on what an asynchronous commit can lose. PostgreSQL calls
/// it `wal_writer_delay` and defaults it to 200 ms; so does this.
/// Padded alongside [`SYNCHRONOUS_COMMIT`]. Two process-wide knobs that every
/// commit reads and a `SET` writes once in a blue moon are precisely the values
/// that should never be evicted from a core's L1, and consecutive statics land
/// on one line by default.
pub static WAL_WRITER_DELAY_MS: CacheGuard<AtomicU64> = CacheGuard::new(AtomicU64::new(200));

/// The server-wide default. A pgwire session may override it for itself.
#[inline]
pub fn synchronous_commit() -> bool {
    SYNCHRONOUS_COMMIT.load(AtomicOrdering::Relaxed)
}

#[inline]
pub fn set_synchronous_commit(on: bool) {
    SYNCHRONOUS_COMMIT.store(on, AtomicOrdering::Relaxed);
}

/// Never zero: a zero delay would spin the writer on a busy loop of syncs.
#[inline]
pub fn wal_writer_delay_ms() -> u64 {
    WAL_WRITER_DELAY_MS.load(AtomicOrdering::Relaxed).max(1)
}

#[inline]
pub fn set_wal_writer_delay_ms(ms: u64) {
    WAL_WRITER_DELAY_MS.store(ms.max(1), AtomicOrdering::Relaxed);
}

/// Parses what `synchronous_commit` accepts.
///
/// PostgreSQL's `local`, `remote_write` and `remote_apply` are about waiting for
/// standbys, which this engine expresses through `REPLICATION SET mode`. They
/// are accepted and read as **on**, because every one of them waits for the
/// local log — answering "off" to any of them would be quietly weaker than what
/// was asked for.
pub fn parse_synchronous_commit(value: &str) -> Option<bool> {
    match value.trim().trim_matches(['\'', '"']).to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" | "t" | "local" | "remote_write" | "remote_apply" => Some(true),
        "off" | "false" | "no" | "0" | "f" => Some(false),
        _ => None,
    }
}

/// Reclaim the space that deleted and relocated rows leave behind in the rows
/// file (`core::vacuum`). Default: **enabled**.
///
/// A tombstone frees no bytes on its own, so a table that deletes and reinserts
/// the same working set grows without bound. With this on, a background scan
/// works out which byte ranges no live pointer refers to and the write path
/// allocates from them before extending the file.
///
/// Turning it off restores append-only behaviour and also skips the per-query
/// epoch registration the reclamation safety argument depends on.
pub static VACUUM_ENABLED: OnceLock<bool> = OnceLock::new();

/// Store a `JSON`/`JSONB` column by taking the document apart into a table of
/// its own, rather than storing it as bytes. Default: **off**.
///
/// Shredding was the original mapping and it is kept because it makes a
/// document's fields ordinary typed columns, which the filter index and the
/// comparison evaluator can see into without any JSON-aware code. What it
/// cannot do is hold a document whose *keys are data*: the table grows a column
/// per distinct key, and since adding a column to a populated table rewrites
/// every row of it, loading such a column is quadratic. One real import reached
/// 217 shadow tables against 154 real ones and stalled.
///
/// So the default is now [`crate::core::jsonb`] — one row write, whatever the
/// document. Turn this on for a workload whose documents have a small, stable
/// set of keys and that leans on querying them.
///
/// **It decides only what a newly created column becomes.** The choice is
/// recorded in the schema — a shredded column is a `UUID` with a sidecar entry,
/// a stored one is [`crate::core::db_type::DbType::JSONB`] — so changing this
/// never reinterprets a column that already exists, and the two kinds coexist
/// in one database.
pub static JSON_SHADOW_TABLES: OnceLock<bool> = OnceLock::new();

/// How often the per-table reclamation service scans. Default: 30 seconds.
///
/// The sweep costs ~4.5 ns a row (`benches/vacuum_benchmarks.rs`), so this is
/// not chosen to limit scan cost — a ten-million-row table is ~45 ms — and a
/// table with no `DELETE` or `UPDATE` since its last scan skips the sweep
/// entirely.
pub static VACUUM_INTERVAL_SECS: OnceLock<u64> = OnceLock::new();

/// Dead bytes below which a scan does not bother publishing a free list.
/// Default: 8 MiB. Either this or [`VACUUM_MIN_DEAD_RATIO`] being met is enough.
pub static VACUUM_MIN_DEAD_BYTES: OnceLock<u64> = OnceLock::new();

/// Fraction of the heap that has to be dead before a scan publishes, for tables
/// too small to reach [`VACUUM_MIN_DEAD_BYTES`]. Default: 0.20.
pub static VACUUM_MIN_DEAD_RATIO: OnceLock<f64> = OnceLock::new();

/// How long a reclamation run waits for queries older than its scan to finish
/// before giving up and leaving the batch for the next run. Default: 30s.
///
/// Raising it trades reclamation latency for a better chance of publishing on a
/// table with long-running queries; lowering it keeps the service responsive on
/// one where a query is always in flight. Correctness does not depend on the
/// value — a run that times out publishes nothing, and holds its findings for
/// the next run rather than discarding them.
pub static VACUUM_DRAIN_TIMEOUT_SECS: OnceLock<u64> = OnceLock::new();

#[inline]
pub fn vacuum_enabled() -> bool {
    *VACUUM_ENABLED.get().unwrap_or(&true)
}

/// Whether a new JSON column is shredded into a table of its own rather than
/// stored as bytes. See [`JSON_SHADOW_TABLES`].
#[inline]
pub fn json_shadow_tables() -> bool {
    *JSON_SHADOW_TABLES.get().unwrap_or(&false)
}

#[inline]
pub fn vacuum_interval_secs() -> u64 {
    *VACUUM_INTERVAL_SECS.get().unwrap_or(&30)
}

#[inline]
pub fn vacuum_min_dead_bytes() -> u64 {
    *VACUUM_MIN_DEAD_BYTES.get().unwrap_or(&(8 * 1024 * 1024))
}

#[inline]
pub fn vacuum_min_dead_ratio() -> f64 {
    *VACUUM_MIN_DEAD_RATIO.get().unwrap_or(&0.20)
}

#[inline]
pub fn vacuum_drain_timeout_secs() -> u64 {
    *VACUUM_DRAIN_TIMEOUT_SECS.get().unwrap_or(&30)
}

pub mod core;

pub mod auth;
pub mod cache;
pub mod client;
pub mod configuration;
pub mod instructions;
pub mod memory_pool;
pub mod native_auth;
pub mod pg_params;
pub mod pgwire;

/// Multi-primary replication between instances, over `rastcp`.
pub mod replication;
pub mod shutdown;
pub mod simds;

#[cfg(test)]
mod ext_cov_tests;

#[cfg(test)]
mod synchronous_commit_tests {
    use super::*;

    #[test]
    fn the_boolean_spellings_are_accepted() {
        for on in ["on", "ON", "true", "yes", "1", "t", " on "] {
            assert_eq!(parse_synchronous_commit(on), Some(true), "{:?}", on);
        }
        for off in ["off", "OFF", "false", "no", "0", "f", "'off'"] {
            assert_eq!(parse_synchronous_commit(off), Some(false), "{:?}", off);
        }
    }

    /// PostgreSQL's replication levels all wait for the local log, so reading
    /// them as anything but `on` would be quietly weaker than what was asked
    /// for. They are about standbys, which this engine expresses through
    /// `REPLICATION SET mode`.
    #[test]
    fn the_replication_levels_read_as_on() {
        for level in ["local", "remote_write", "remote_apply"] {
            assert_eq!(parse_synchronous_commit(level), Some(true), "{}", level);
        }
    }

    #[test]
    fn nonsense_is_refused_rather_than_guessed() {
        for bad in ["maybe", "", "onn", "2"] {
            assert_eq!(parse_synchronous_commit(bad), None, "{:?}", bad);
        }
    }

    /// The delay is the bound on what an asynchronous commit can lose, so a
    /// zero would mean the writer spins instead of meaning "never wait".
    #[test]
    fn the_writer_delay_is_never_zero() {
        let restore = wal_writer_delay_ms();
        set_wal_writer_delay_ms(0);
        assert_eq!(wal_writer_delay_ms(), 1);
        set_wal_writer_delay_ms(restore);
    }
}
