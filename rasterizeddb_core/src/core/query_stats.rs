//! What ran recently, what it cost, and how much of the table it had to read.
//!
//! ```text
//! SHOW QUERIES                 -- the last N statements, newest first
//! SHOW QUERY STATS             -- one row per distinct statement shape
//! RESET QUERY STATS            -- empty both
//! SELECT * FROM pg_stat_statements   -- the same aggregate, PostgreSQL's shape
//! ```
//!
//! # Why this is in memory and not a table
//!
//! The obvious implementation is a table the executor inserts into. It is the
//! wrong one here: a statement would cost a second statement, the log would be
//! the busiest relation in the database, and the write path would be the thing
//! being measured *and* the thing doing the measuring. The question this
//! answers — "what has this server been doing for the last few minutes, and
//! which statement is the slow one" — only ever wants the recent past, so the
//! recent past is all that is kept: a fixed ring of records and a bounded map
//! of per-shape totals, both of which are lost on restart and neither of which
//! touches storage.
//!
//! # What `rows_scanned` counts
//!
//! Row pointers the scan handed to its workers — the candidate rows it had to
//! look at to answer, before the predicate was evaluated on any of them. It is
//! counted in [`crate::core::processor::concurrent_processor`] at the one point
//! every scan dispatches from, which has three consequences worth knowing:
//!
//! * Rows the filter index pruned are **not** counted. The candidate bitmap is
//!   applied inside the pointer iterator, so a scan that skipped nine tenths of
//!   a table reports a tenth of it — which is exactly the number that shows the
//!   index is working.
//! * A query answered from the query cache reports **zero**. It read no
//!   pointers, which is the point of the cache.
//! * A join counts every relation it scanned, because each one dispatches
//!   through the same place.
//!
//! Against `rows`, which is what the client received, the ratio is the reason a
//! statement is slow *when the statement is scan-bound*: ten million scanned to
//! return three is a missing index, and ten million scanned to return ten
//! million is a query asking for the whole table.
//!
//! # What `rows_scanned` cannot see, and what `materialised` does
//!
//! `rows_scanned` is a scan-cost proxy, and not every expensive statement is
//! expensive because of a scan. A CTE and a derived table are each built as a
//! real relation before the outer statement runs — a `CREATE TABLE __cte_…`, a
//! multi-row `INSERT` and a `DROP TABLE` through the ordinary executor — so a
//! statement wrapping a trivial query in `WITH … AS` copies the whole subquery
//! result into a table before it reads anything. No scan happened, so
//! `rows_scanned` reports that statement honestly and uselessly: the column
//! that exists to say *why* says "it did no work" for what may be the most
//! expensive shape on the server. A correct number that means nothing is
//! harder to distrust than a wrong one.
//!
//! `materialised` and `materialised_us` are that second cost centre, counted
//! at each of the two places a relation is actually built. They are kept apart
//! from `rows_scanned` rather than summed into one "cost", because they are
//! different kinds of number: `rows_scanned` means something *against* `rows`
//! as a ratio, while a materialisation count means something on its own and
//! has no denominator. A statement can be expensive for either reason or both,
//! and one column would hide the mix.
//!
//! # Why this counter exists, which is no longer why it hurts
//!
//! Those relations used to be durable. Five files created and five deleted per
//! materialisation, every one an fsync, measured at a flat ~130 ms against the
//! deployment on 192.168.1.7 — independent of row count, column count and
//! whether the outer query aggregated. `SELECT COUNT(*) FROM t` at 0.08 ms
//! became 126 ms wrapped in a derived table. Since a statement-scoped relation
//! has no reader that could outlive it, they are memory-backed now
//! ([`crate::core::database::temp_seed`]) and that cost is gone.
//!
//! The counter is not therefore pointless: materialising is still a full copy
//! of a subquery's result into a relation, still invisible to `rows_scanned`,
//! and still the reason a statement with a trivial plan can be the slow one. It
//! is also how anyone confirms the durability fix is still in place rather than
//! taking it on trust — which matters more than usual here, because **no test
//! could have caught the original problem**. The same materialisation measures
//! 0.97 ms on tmpfs against 200-695 ms on ext4, and `tempfile::tempdir()` puts
//! every test on tmpfs.
//!
//! # How a statement's counters find it
//!
//! Through a [`tokio::task_local`]. The alternative — threading a counter from
//! the wire protocol down through the executor, the join planner and the scan
//! — is a parameter on a dozen signatures that exist for other reasons. A
//! statement runs to completion in the task that read it off the socket, and
//! the scan's dispatch loop is *awaited* by that task rather than spawned from
//! it, so the counters are reachable from where they are incremented without
//! any of those signatures changing. Worker tasks never touch them: they are
//! spawned, so they could not see the task-local anyway, and a per-row atomic
//! shared by sixteen workers is a contended cache line in the hottest loop in
//! the engine. One add per batch of ten thousand is not measurable.
//!
//! # Passwords
//!
//! `CREATE ROLE … PASSWORD '…'` and `IMPORT … PASSWORD '…'` carry a secret in
//! their text. Anything containing the word is truncated at it before being
//! stored, because this surface is readable by any session that can run
//! `SHOW QUERIES` and a password that reached the ring would outlive the
//! statement that set it.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

/// Statements kept in the ring by default.
///
/// Enough to cover the burst a page load produces and to still hold the slow
/// statement an operator is looking for; small enough that the whole surface is
/// a few hundred kilobytes at the truncation limit below.
const DEFAULT_RECENT: usize = 256;

/// Distinct statement shapes the aggregate will hold.
///
/// A workload whose literals defeat normalisation — an `IN` list of a different
/// length every time — would otherwise grow this map without bound. At the cap
/// the least recently seen shape is dropped, which costs a scan of the map on
/// the statements that introduce a new shape while it is full.
const MAX_DISTINCT: usize = 5_000;

/// Characters of a statement kept. A generated `INSERT` can be megabytes, and
/// no one reads past the first screen of one in a console.
const MAX_SQL_CHARS: usize = 2_048;

/// Master switch. On by default: the cost is one mutex acquisition per
/// statement and one atomic add per batch of rows scanned, and a server whose
/// statements cannot be seen is a server that cannot be diagnosed.
static ENABLED: AtomicBool = AtomicBool::new(true);

static RECENT_CAPACITY: AtomicUsize = AtomicUsize::new(DEFAULT_RECENT);

#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

pub fn capacity() -> usize {
    RECENT_CAPACITY.load(Ordering::Relaxed)
}

/// Resizes the ring, dropping the oldest records when it shrinks.
pub fn set_capacity(n: usize) {
    let n = n.min(100_000);
    RECENT_CAPACITY.store(n, Ordering::Relaxed);
    let mut log = log().lock();
    while log.recent.len() > n {
        log.recent.pop_back();
    }
}

/// One statement, as it was answered.
#[derive(Clone, Debug)]
pub struct Record {
    /// When it started, in microseconds since the Unix epoch.
    pub started_unix_us: i64,
    pub duration_us: u64,
    /// Rows the client received, plus rows a write reported as affected —
    /// which is what PostgreSQL's `pg_stat_statements.rows` counts too.
    pub rows: u64,
    /// See the module docs: candidate rows the scan read, not rows returned.
    pub rows_scanned: u64,
    /// Relations this statement had to build before it could answer: one per
    /// CTE and per derived table. See [`note_materialised`].
    pub materialised: u64,
    /// Microseconds spent building them.
    pub materialised_us: u64,
    pub database: String,
    pub username: String,
    /// The failure this statement reported, if it failed.
    pub error: Option<String>,
    /// Hash of the normalised text, so a record can be tied to its aggregate.
    pub queryid: u64,
    /// The statement as sent, truncated and with any password removed.
    pub sql: String,
}

/// Every execution of one statement shape, folded together.
#[derive(Clone, Debug)]
pub struct Aggregate {
    pub queryid: u64,
    /// The normalised text: literals replaced by `$n`, whitespace collapsed.
    pub query: String,
    pub calls: u64,
    pub errors: u64,
    pub total_us: u64,
    pub min_us: u64,
    pub max_us: u64,
    /// Sum of the squared durations, which is what a standard deviation needs
    /// and what keeping the durations themselves would cost too much to give.
    pub sum_sq_us: f64,
    pub rows: u64,
    pub rows_scanned: u64,
    /// Relations built across every execution of this shape, and the time it
    /// took. Totals, like `rows` and `rows_scanned` beside them — divide by
    /// `calls` for the per-execution figure.
    pub materialised: u64,
    pub materialised_us: u64,
    pub last_seen_unix_us: i64,
}

impl Aggregate {
    pub fn mean_us(&self) -> f64 {
        if self.calls == 0 {
            return 0.0;
        }
        self.total_us as f64 / self.calls as f64
    }

    /// Population standard deviation of the durations, in microseconds.
    ///
    /// From the running sum of squares rather than from the samples, so the
    /// aggregate stays a fixed size per shape. The `max(0.0)` guards the one
    /// way that formula misbehaves: catastrophic cancellation on a shape whose
    /// durations are large and nearly equal can leave the variance a very
    /// small negative number, and `sqrt` of that is `NaN` on the wire.
    pub fn stddev_us(&self) -> f64 {
        if self.calls == 0 {
            return 0.0;
        }
        let mean = self.mean_us();
        let variance = (self.sum_sq_us / self.calls as f64) - mean * mean;
        variance.max(0.0).sqrt()
    }
}

struct Log {
    /// Newest first, so `SHOW QUERIES` reads off the front and a shrink drops
    /// from the back.
    recent: VecDeque<Record>,
    aggregate: HashMap<u64, Aggregate>,
}

static LOG: OnceLock<Mutex<Log>> = OnceLock::new();

fn log() -> &'static Mutex<Log> {
    LOG.get_or_init(|| {
        Mutex::new(Log {
            recent: VecDeque::with_capacity(DEFAULT_RECENT),
            aggregate: HashMap::new(),
        })
    })
}

/// The counters one statement accumulates while it runs.
#[derive(Default)]
pub struct Probe {
    rows: AtomicU64,
    rows_scanned: AtomicU64,
    materialised: AtomicU64,
    materialised_us: AtomicU64,
}

tokio::task_local! {
    /// The statement running in this task, if it is being measured.
    static PROBE: Arc<Probe>;
}

/// A statement being timed. Handed back to [`finish`], which is what writes the
/// record; dropping it instead records nothing, which is what a statement that
/// never reached its end should leave behind.
pub struct Recording {
    probe: Arc<Probe>,
    started: Instant,
    started_unix_us: i64,
}

impl Recording {
    pub fn probe(&self) -> Arc<Probe> {
        self.probe.clone()
    }
}

/// Starts timing a statement, or returns `None` when this surface is off.
pub fn begin() -> Option<Recording> {
    if !enabled() {
        return None;
    }
    Some(Recording {
        probe: Arc::new(Probe::default()),
        started: Instant::now(),
        started_unix_us: now_unix_us(),
    })
}

/// Runs `fut` with `probe` as the statement's counters.
pub async fn observe<F: Future>(probe: Arc<Probe>, fut: F) -> F::Output {
    PROBE.scope(probe, fut).await
}

/// Adds to what the running statement returned or affected.
///
/// A no-op outside a [`observe`] scope, which is every internal statement the
/// engine runs for itself — the temporary relations a catalogue view is built
/// from would otherwise appear in the log as user statements.
#[inline]
pub fn note_rows(n: u64) {
    let _ = PROBE.try_with(|p| p.rows.fetch_add(n, Ordering::Relaxed));
}

/// Adds to the candidate rows the running statement's scans have read.
#[inline]
pub fn note_rows_scanned(n: u64) {
    let _ = PROBE.try_with(|p| p.rows_scanned.fetch_add(n, Ordering::Relaxed));
}

/// Records one relation built for the running statement, and what it cost.
///
/// A CTE or a derived table is not a plan node here: it is built as a real
/// relation before the outer statement runs — a `CREATE TABLE`, a multi-row
/// `INSERT` and a `DROP TABLE` through the ordinary executor. That work is
/// invisible to `rows_scanned`, which counts only what a *scan* read, so a
/// statement can spend almost all of its time here and report having read
/// almost nothing. This is the counter that says so.
///
/// Called once per relation *that was built*, with the microseconds it took. A
/// materialisation that failed is not counted: it produced nothing to have been
/// slow for, and the statement carrying it is recorded as the failure it was —
/// so on a failed statement this figure is a floor rather than a total.
///
/// A no-op outside a measured statement, like the counters above it, so the
/// temporary relations the engine builds for its own catalogue views cost
/// nothing here.
#[inline]
pub fn note_materialised(micros: u64) {
    let _ = PROBE.try_with(|p| {
        p.materialised.fetch_add(1, Ordering::Relaxed);
        p.materialised_us.fetch_add(micros, Ordering::Relaxed);
    });
}

/// Records a finished statement.
pub fn finish(
    recording: Recording,
    sql: &str,
    database: &str,
    username: &str,
    error: Option<&str>,
) {
    // A console asking the server about itself is not what this log is for.
    // Dropped here rather than before the statement ran, because that is the
    // one place the text is known and the cost of having timed it is an
    // `Instant` and an `Arc`. See [`is_introspection`].
    //
    // One that *failed* is kept. "What did the server refuse, and why" is half
    // of what this surface is for, and a refused `SHOW COLUMNS FROM …` is the
    // thing somebody is looking for when a console has gone blank. Keeping
    // them costs nothing against the flooding this filter exists to stop: a
    // failing introspection statement is rare by construction, and a console
    // that produced them at the rate it polls would be a console reporting an
    // error on every refresh.
    if error.is_none() && is_introspection(sql) {
        return;
    }

    let duration_us = recording
        .started
        .elapsed()
        .as_micros()
        .min(u64::MAX as u128) as u64;
    let rows = recording.probe.rows.load(Ordering::Relaxed);
    let rows_scanned = recording.probe.rows_scanned.load(Ordering::Relaxed);
    let materialised = recording.probe.materialised.load(Ordering::Relaxed);
    let materialised_us = recording.probe.materialised_us.load(Ordering::Relaxed);

    let text = truncate(redact(sql));
    let normalized = normalize(&text);
    let queryid = hash_query(&normalized);

    let record = Record {
        started_unix_us: recording.started_unix_us,
        duration_us,
        rows,
        rows_scanned,
        materialised,
        materialised_us,
        database: database.to_string(),
        username: username.to_string(),
        error: error.map(|e| truncate(e.to_string())),
        queryid,
        sql: text,
    };

    let capacity = capacity();
    let mut log = log().lock();

    if capacity > 0 {
        log.recent.push_front(record);
        while log.recent.len() > capacity {
            log.recent.pop_back();
        }
    }

    // A new shape into a full map costs a scan for the coldest entry. Only
    // `entry` would be cheaper, and only until the map grew without bound.
    if !log.aggregate.contains_key(&queryid) && log.aggregate.len() >= MAX_DISTINCT {
        if let Some(coldest) = log
            .aggregate
            .values()
            .min_by_key(|a| a.last_seen_unix_us)
            .map(|a| a.queryid)
        {
            log.aggregate.remove(&coldest);
        }
    }

    let entry = log.aggregate.entry(queryid).or_insert_with(|| Aggregate {
        queryid,
        query: normalized,
        calls: 0,
        errors: 0,
        total_us: 0,
        min_us: u64::MAX,
        max_us: 0,
        sum_sq_us: 0.0,
        rows: 0,
        rows_scanned: 0,
        materialised: 0,
        materialised_us: 0,
        last_seen_unix_us: 0,
    });
    entry.calls += 1;
    entry.errors += u64::from(error.is_some());
    entry.total_us += duration_us;
    entry.min_us = entry.min_us.min(duration_us);
    entry.max_us = entry.max_us.max(duration_us);
    let d = duration_us as f64;
    entry.sum_sq_us += d * d;
    entry.rows += rows;
    entry.rows_scanned += rows_scanned;
    entry.materialised += materialised;
    entry.materialised_us += materialised_us;
    entry.last_seen_unix_us = recording.started_unix_us;
}

/// The most recent statements, newest first.
pub fn recent(limit: usize) -> Vec<Record> {
    log().lock().recent.iter().take(limit).cloned().collect()
}

/// How much is being kept: statements in the ring, and distinct shapes.
///
/// Separate from [`recent`] and [`aggregated`] because the database statistics
/// want the two sizes and nothing else, and cloning several hundred records to
/// call `.len()` on them is a strange way to answer that.
pub fn counts() -> (usize, usize) {
    let log = log().lock();
    (log.recent.len(), log.aggregate.len())
}

/// Every statement shape, slowest by total time first — which is the order the
/// question "what should I fix" is asked in.
pub fn aggregated() -> Vec<Aggregate> {
    let mut out: Vec<Aggregate> = log().lock().aggregate.values().cloned().collect();
    out.sort_by(|a, b| {
        b.total_us
            .cmp(&a.total_us)
            .then_with(|| a.queryid.cmp(&b.queryid))
    });
    out
}

/// Empties both, and reports what was dropped.
pub fn reset() -> (usize, usize) {
    let mut log = log().lock();
    let counts = (log.recent.len(), log.aggregate.len());
    log.recent.clear();
    log.aggregate.clear();
    // A workload that filled the map to its cap should not keep that memory
    // after being asked to forget it.
    log.aggregate.shrink_to_fit();
    counts
}

fn now_unix_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros().min(i64::MAX as u128) as i64)
        // A clock behind the epoch is a misconfigured machine, not a state to
        // propagate into every timestamp downstream.
        .unwrap_or(0)
}

/// Cuts a statement at the first `PASSWORD`, whatever the statement is.
///
/// Deliberately cruder than parsing: the two grammars that carry a secret today
/// are role management and the PostgreSQL import, and a third one added later
/// would be redacted by this without anyone having to remember to come back
/// here. What is left is enough to see *that* a role was created.
fn redact(sql: &str) -> String {
    crate::core::redact::statement(sql)
}

#[inline]
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn truncate(mut text: String) -> String {
    if text.chars().count() <= MAX_SQL_CHARS {
        return text;
    }
    let cut = text
        .char_indices()
        .nth(MAX_SQL_CHARS)
        .map(|(i, _)| i)
        .unwrap_or(text.len());
    text.truncate(cut);
    text.push('…');
    text
}

/// Replaces the parts of a statement that are data with `$n`, so every
/// execution of one shape folds into one aggregate.
///
/// String literals and numbers become placeholders; quoted identifiers, bare
/// identifiers and keywords are kept as written; runs of whitespace collapse to
/// one space and comments to nothing. This is what makes `WHERE id = 1` and
/// `WHERE id = 2` one entry rather than two, and it is the same normalisation
/// `pg_stat_statements` describes — minus its ability to see through the
/// parser, so `select 1` and `SELECT 1` remain two shapes here where
/// PostgreSQL would also keep them apart, and `IN (1,2)` and `IN (1,2,3)` stay
/// apart where recent PostgreSQL folds them together.
pub fn normalize(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    let mut placeholder = 0usize;
    // Leading whitespace is dropped by starting as though space had just been
    // written.
    let mut pending_space = false;

    // Only ASCII bytes are ever tested, and no ASCII byte occurs inside a
    // multi-byte UTF-8 sequence, so every index taken here is a char boundary.
    while i < bytes.len() {
        let b = bytes[i];

        if b.is_ascii_whitespace() {
            pending_space = !out.is_empty();
            i += 1;
            continue;
        }

        // `-- …` to end of line, and `/* … */`. The wire protocol strips these
        // before a statement is executed, but the native protocol does not, and
        // a comment must not be what tells two identical statements apart.
        if b == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            pending_space = !out.is_empty();
            continue;
        }
        if b == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            while i < bytes.len() && !(bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/')) {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            pending_space = !out.is_empty();
            continue;
        }

        if pending_space {
            out.push(' ');
            pending_space = false;
        }

        match b {
            // A string literal, `''` escapes included. What it holds is data.
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if bytes.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                placeholder += 1;
                out.push('$');
                out.push_str(&placeholder.to_string());
            }
            // A quoted identifier is a name. Kept, quotes and all.
            b'"' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        if bytes.get(i + 1) == Some(&b'"') {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            // An identifier or keyword, digits inside it included: `col1` is a
            // name, not a name and a literal.
            _ if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                while i < bytes.len() && (is_ident_byte(bytes[i]) || bytes[i] == b'$') {
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            // A parameter the client already wrote as one. Left as it is: the
            // statement's own numbering is more use than a renumbering of it.
            b'$' if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                let start = i;
                i += 1;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                out.push_str(&sql[start..i]);
            }
            // A number, in any of the spellings a literal takes.
            _ if b.is_ascii_digit() => {
                while i < bytes.len()
                    && (bytes[i].is_ascii_digit()
                        || bytes[i] == b'.'
                        || bytes[i] == b'e'
                        || bytes[i] == b'E'
                        || (i > 0
                            && (bytes[i] == b'+' || bytes[i] == b'-')
                            && matches!(bytes.get(i - 1), Some(b'e') | Some(b'E'))))
                {
                    i += 1;
                }
                placeholder += 1;
                out.push('$');
                out.push_str(&placeholder.to_string());
            }
            // Punctuation, an operator, or the first byte of a multi-byte
            // character: copied whole.
            _ => {
                let width = utf8_width(b);
                let end = (i + width).min(bytes.len());
                out.push_str(&sql[i..end]);
                i = end;
            }
        }
    }

    out
}

#[inline]
fn utf8_width(first: u8) -> usize {
    match first {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

/// FNV-1a over the normalised text.
///
/// Hand-rolled rather than `ahash`, which is randomly seeded: a `queryid` has
/// to name the same statement across restarts and across the nodes of a
/// cluster, or a dashboard keyed on it starts a new series every time the
/// server comes back.
pub fn hash_query(normalized: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in normalized.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

/// Whether a statement describes the server rather than using it.
///
/// These are not recorded when they succeed — not in `SHOW QUERIES`, not in
/// `SHOW QUERY STATS`, not in `pg_stat_statements`. One that *failed* is kept,
/// because what the server refused is half of what that surface answers. A
/// console refreshes its table list, its column lists and its statistics every
/// few seconds, and every one of those refreshes is a statement. Recorded, they are the overwhelming majority of what this
/// surface holds: the last-statements ring fills with `SHOW COLUMNS FROM …`
/// before a single query the user typed reaches the bottom of it, and the
/// costliest-shape list ranks the console's own polling above the application's
/// slowest query. The log answers "what has this server been doing for the
/// application", and a client asking the server about itself is not that.
///
/// Three families count:
///
/// * **Introspection and settings statements** — `SHOW …`, `SET …`, `RESET …`,
///   `DISCARD …`. Everything the `SHOW`/`SET CONFIG` surface owns, plus the
///   session variables a driver sets on connect.
/// * **Catalogue queries** — anything naming `pg_catalog.…`,
///   `information_schema.…`, or a bare `pg_…` relation. PostgreSQL reserves
///   the `pg_` prefix for system use, so a bare `pg_` identifier is a
///   catalogue reference and not somebody's table.
/// * **Internal relations** — the `__…` tables: a JSON column's shadow table,
///   the temporary relation a CTE or a catalogue view materialises into.
///
/// Names inside string literals do not count. `SELECT … WHERE relname =
/// 'pg_class'` is a query *about* the catalogue name, not one against it, and
/// on a table the application owns.
pub fn is_introspection(sql: &str) -> bool {
    let statement = strip_leading_noise(sql);

    let head_len = statement
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(statement.len());
    let head = &statement[..head_len];
    if ["show", "set", "reset", "discard"]
        .iter()
        .any(|k| head.eq_ignore_ascii_case(k))
    {
        return true;
    }

    names_internal_relation(statement)
}

/// The statement itself, with leading whitespace and leading comments removed.
///
/// The native protocol does not strip comments, and a leading `-- refresh`
/// would otherwise hide the `SHOW` behind it.
fn strip_leading_noise(sql: &str) -> &str {
    let mut rest = sql.trim_start();
    loop {
        if let Some(after) = rest.strip_prefix("--") {
            rest = match after.find('\n') {
                Some(end) => after[end + 1..].trim_start(),
                None => return "",
            };
            continue;
        }
        if let Some(after) = rest.strip_prefix("/*") {
            rest = match after.find("*/") {
                Some(end) => after[end + 2..].trim_start(),
                None => return "",
            };
            continue;
        }
        return rest;
    }
}

/// Whether the statement names a catalogue relation or an internal one, outside
/// of any string literal.
fn names_internal_relation(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        let b = bytes[i];

        // A single-quoted literal is data, skipped whole — doubled quotes
        // included, so `'it''s pg_class'` cannot end early and leave the rest
        // of it being read as identifiers.
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // A double-quoted *identifier*, which is a name and not data:
        // `SELECT * FROM "__json_users"` names the shadow table as surely as
        // the unquoted spelling does.
        if b == b'"' {
            let start = i + 1;
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            let word = &sql[start..i];
            // Past the closing quote, or past the end when there is none.
            i += 1;
            if is_internal_name(word) {
                return true;
            }
            continue;
        }

        if !(b.is_ascii_alphabetic() || b == b'_') {
            i += 1;
            continue;
        }

        // One identifier, qualifier included: `pg_catalog.pg_class` is one
        // name, and splitting it would leave `pg_catalog` to be matched on its
        // own.
        let start = i;
        while i < bytes.len() {
            let c = bytes[i];
            if c.is_ascii_alphanumeric() || c == b'_' || c == b'.' {
                i += 1;
            } else {
                break;
            }
        }
        if is_internal_name(&sql[start..i]) {
            return true;
        }
    }

    false
}

/// Whether one name belongs to the catalogue or to the engine's own plumbing.
fn is_internal_name(word: &str) -> bool {
    if word.starts_with("__") {
        return true;
    }
    let lower = word.to_ascii_lowercase();
    lower.starts_with("pg_") || lower.starts_with("information_schema.")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The log is process-wide, so the tests that read it back cannot run at
    /// the same time as each other. Taken by every test that calls `reset`.
    static SERIAL: Mutex<()> = Mutex::new(());

    // --- what the log refuses to hold

    /// The console's own polling. Every one of these arrives a few times a
    /// second while a console is open, and recorded they are the only thing
    /// the log ends up holding.
    #[test]
    fn a_console_asking_about_the_server_is_not_recorded() {
        for sql in [
            "SHOW DATABASE STATS",
            "show stats",
            "SHOW COLUMNS FROM \"utrn_ffe8aa15c0b04023ae305f9d4922e243\"",
            "SHOW TABLES",
            "SHOW CONFIG",
            "SET CONFIG filter.enabled = on",
            "SET client_encoding TO 'UTF8'",
            "RESET QUERY STATS",
            "DISCARD ALL",
            "SELECT * FROM pg_catalog.pg_class",
            "select relname from pg_class",
            "SELECT * FROM information_schema.columns WHERE table_name = 'users'",
            "SELECT * FROM pg_stat_statements ORDER BY total_exec_time DESC",
            "SELECT * FROM __json_users",
            "SELECT * FROM \"__json_users\"",
            "  -- refresh\n  SHOW TABLES",
            "/* console */ SHOW DATABASE STATS",
        ] {
            assert!(is_introspection(sql), "still recorded: {sql}");
        }
    }

    /// The application's statements, which are the whole point of the log. A
    /// filter that swallowed one of these would be worse than no filter.
    ///
    /// This is not a test of what [`is_introspection`] does today; it is the
    /// guard on what it can *become*. Every plausible future edit to that
    /// function widens it — one more prefix, one more keyword — and a rule
    /// widened one step too far drops the application's own statements while
    /// leaving the log looking perfectly healthy. There is no symptom to
    /// notice: `SHOW QUERIES` still returns rows, and they are simply the
    /// wrong ones. Widen the filter and this list has to widen with it, never
    /// shrink to accommodate it.
    #[test]
    fn an_application_statement_is_still_recorded() {
        for sql in [
            "SELECT * FROM users WHERE id = 1",
            "INSERT INTO vynchat_message (body) VALUES ('hi')",
            "UPDATE users SET name = 'bob' WHERE id = 1",
            "DELETE FROM users WHERE id = 1",
            "SELECT COUNT(*) FROM \"users\"",
            "BEGIN",
            "COMMIT",
            "CREATE TABLE t (id INT)",
            // Names the catalogue only inside a literal: a query about the
            // name, run against a table the application owns.
            "SELECT * FROM audit WHERE relation = 'pg_class'",
            "SELECT * FROM audit WHERE note = 'it''s __json_users'",
            // `SET` only counts as the statement, not as a clause.
            "UPDATE t SET show = 1 WHERE id = 2",
        ] {
            assert!(!is_introspection(sql), "wrongly dropped: {sql}");
        }
    }

    // --- what materialisation costs

    /// The counter has to see each relation exactly once, and a plain statement
    /// has to leave it at zero.
    ///
    /// The double-counting trap this guards: a CTE body may itself contain a
    /// derived table, which is materialised through `materialize_query` and
    /// counts itself. Timing the whole `WITH` iteration instead of just the
    /// relation it builds would count that nested one twice — and the symptom
    /// would be a `materialised_ms` slightly larger than the statement's own
    /// duration, which reads as a rounding artefact rather than as a bug.
    #[tokio::test(flavor = "multi_thread")]
    async fn each_built_relation_is_counted_exactly_once() {
        use crate::core::database::Database;
        use crate::core::mvcc::transaction::FROZEN_TX;

        let _ = crate::MAX_PERMITS_THREADS.get_or_init(|| 8);
        let _ = crate::BATCH_SIZE.get_or_init(|| 1024 * 8);

        // `rclite::Arc`, not the `std::sync::Arc` this module uses for its own
        // probe: the engine's handles are the small one, and the two are
        // distinct types with the same name.
        let dir = tempfile::tempdir().expect("tempdir");
        let database: rclite::Arc<Database> =
            rclite::Arc::new(Database::new(dir.path().to_str().expect("a UTF-8 path")).await);

        let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(
            "CREATE TABLE t (id INT, amount INT)",
        )
        .expect("recognised");
        crate::core::rql::executor::execute(purpose, database.clone()).await;

        // One CTE: one relation.
        let probe = Arc::new(Probe::default());
        let db = database.clone();
        observe(probe.clone(), async move {
            crate::core::rql::cte::materialize(
                "WITH s AS (SELECT id FROM t) SELECT id FROM s",
                &db,
                FROZEN_TX,
            )
            .await
            .expect("the CTE materialises");
        })
        .await;
        assert_eq!(
            probe.materialised.load(Ordering::Relaxed),
            1,
            "one CTE, one relation"
        );

        // Two derived tables: two relations, counted where they are built and
        // not once more at the entry point that walks them.
        let probe = Arc::new(Probe::default());
        let db = database.clone();
        observe(probe.clone(), async move {
            crate::core::rql::cte::materialize_derived(
                "SELECT a.id FROM (SELECT id FROM t) a JOIN (SELECT id FROM t) b ON a.id = b.id",
                &db,
                FROZEN_TX,
            )
            .await
            .expect("the derived tables materialise");
        })
        .await;
        assert_eq!(
            probe.materialised.load(Ordering::Relaxed),
            2,
            "two derived tables"
        );

        // A statement that builds nothing leaves the counter alone, so a zero
        // here means "built nothing" rather than "not measured".
        let probe = Arc::new(Probe::default());
        let db = database.clone();
        observe(probe.clone(), async move {
            let purpose = crate::core::rql::lexer_s1::recognize_query_purpose("SELECT id FROM t")
                .expect("recognised");
            crate::core::rql::executor::execute(purpose, db).await;
        })
        .await;
        assert_eq!(probe.materialised.load(Ordering::Relaxed), 0);
        assert_eq!(probe.materialised_us.load(Ordering::Relaxed), 0);
    }

    /// The counters have to survive into both halves of the log, or the columns
    /// read zero however well they were counted.
    #[test]
    fn what_was_materialised_reaches_the_record_and_the_aggregate() {
        let _guard = SERIAL.lock();
        reset();
        set_enabled(true);

        let recording = begin().expect("recording is on");
        recording.probe.materialised.fetch_add(2, Ordering::Relaxed);
        recording
            .probe
            .materialised_us
            .fetch_add(4_000, Ordering::Relaxed);
        finish(
            recording,
            "WITH s AS (SELECT 1) SELECT * FROM s",
            "db",
            "milen",
            None,
        );

        let recent = recent(10);
        assert_eq!(recent[0].materialised, 2);
        assert_eq!(recent[0].materialised_us, 4_000);

        let aggregate = aggregated();
        assert_eq!(aggregate[0].materialised, 2);
        assert_eq!(aggregate[0].materialised_us, 4_000);
        reset();
    }

    /// A refused `SHOW` is the one somebody is looking for when a console has
    /// gone blank, and it cannot flood the log: an introspection statement that
    /// failed is rare by construction.
    #[test]
    fn an_introspection_statement_that_failed_is_kept() {
        let _guard = SERIAL.lock();
        reset();
        set_enabled(true);

        let recording = begin().expect("recording is on");
        finish(
            recording,
            "SHOW TABLE STATS gone",
            "db",
            "milen",
            Some("relation does not exist"),
        );

        let recent = recent(10);
        assert_eq!(
            recent.len(),
            1,
            "the refusal was dropped with the successes"
        );
        assert_eq!(recent[0].sql, "SHOW TABLE STATS gone");
        assert!(
            recent[0].error.is_some(),
            "the reason is what makes it worth keeping"
        );
        reset();
    }

    /// A dropped statement leaves nothing behind — not a record, not a shape,
    /// not a call on an existing shape.
    #[test]
    fn a_dropped_statement_reaches_neither_half_of_the_log() {
        let _guard = SERIAL.lock();
        reset();
        set_enabled(true);

        let recording = begin().expect("recording is on");
        finish(recording, "SHOW DATABASE STATS", "db", "milen", None);
        let recording = begin().expect("recording is on");
        finish(recording, "SELECT * FROM users", "db", "milen", None);

        let (recorded, shapes) = counts();
        assert_eq!(recorded, 1, "the SHOW was logged");
        assert_eq!(shapes, 1, "the SHOW made a shape");
        assert_eq!(recent(10).len(), 1);
        assert!(
            aggregated().iter().all(|a| !a.query.contains("SHOW")),
            "a SHOW reached the aggregate"
        );
        reset();
    }

    #[test]
    fn literals_become_placeholders() {
        assert_eq!(
            normalize("SELECT * FROM t WHERE id = 42 AND name = 'bob'"),
            "SELECT * FROM t WHERE id = $1 AND name = $2"
        );
        // Two executions of one shape hash the same; a different shape does not.
        let a = hash_query(&normalize("SELECT * FROM t WHERE id = 1"));
        let b = hash_query(&normalize("SELECT * FROM t WHERE id = 2"));
        let c = hash_query(&normalize("SELECT * FROM t WHERE id > 1"));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn names_are_not_literals() {
        // A digit inside an identifier is part of the name.
        assert_eq!(
            normalize("SELECT col1, \"weird name\" FROM t2"),
            "SELECT col1, \"weird name\" FROM t2"
        );
        // An escaped quote does not end the literal early.
        assert_eq!(normalize("SELECT 'o''brien'"), "SELECT $1");
    }

    #[test]
    fn whitespace_and_comments_do_not_make_a_new_shape() {
        let a = normalize("SELECT   a\n  FROM t -- why\n");
        let b = normalize("SELECT a FROM t");
        assert_eq!(a, b);
        assert_eq!(normalize("SELECT /* hi */ a FROM t"), "SELECT a FROM t");
    }

    #[test]
    fn multibyte_text_survives() {
        // The literal is replaced and the identifier is not mangled.
        assert_eq!(
            normalize("SELECT ключ FROM t WHERE v = 'ünïcode'"),
            "SELECT ключ FROM t WHERE v = $1"
        );
    }

    /// The other grammar that puts a credential in statement text. A store's
    /// S3 keys are written to a second file at mode 0600 precisely so they stay
    /// out of the ordinary configuration; recording the statement that set them
    /// would put them straight back into a surface any session can read.
    #[test]
    fn store_credentials_never_reach_the_log() {
        let statement = "STORAGE ADD STORE vault S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
                         BUCKET = 'b', ACCESS_KEY_ID = 'AKIAEXAMPLE', \
                         SECRET_ACCESS_KEY = 'sh-hunter2', SESSION_TOKEN = 'tok-abc')";
        let kept = redact(statement);
        for secret in ["AKIAEXAMPLE", "sh-hunter2", "tok-abc"] {
            assert!(
                !kept.contains(secret),
                "{secret} survived redaction: {kept}"
            );
        }
        // What is left still identifies the statement, which is the whole
        // reason to truncate rather than to drop the record.
        assert!(kept.starts_with("STORAGE ADD STORE vault S3"), "{kept}");
    }

    /// Every spelling the store grammar accepts, each on its own.
    #[test]
    fn each_credential_spelling_is_caught() {
        for option in [
            "ACCESS_KEY_ID",
            "ACCESS_KEY",
            "SECRET_ACCESS_KEY",
            "SECRET_KEY",
            "SECRET",
            "SESSION_TOKEN",
            "TOKEN",
        ] {
            let statement = format!("STORAGE ADD STORE s S3 WITH ({} = 'leaked')", option);
            assert!(
                !redact(&statement).contains("leaked"),
                "{option} did not redact: {}",
                redact(&statement)
            );
        }
    }

    #[test]
    fn passwords_never_reach_the_log() {
        assert_eq!(
            redact("CREATE USER alice PASSWORD 's3cret'"),
            "CREATE USER alice PASSWORD <redacted>"
        );
        assert_eq!(
            redact("IMPORT FROM POSTGRES HOST 'h' USER 'u' PASSWORD 'p' INTO t"),
            "IMPORT FROM POSTGRES HOST 'h' USER 'u' PASSWORD <redacted>"
        );
        // A column that merely contains the word is not a secret.
        assert_eq!(
            redact("SELECT password_hash FROM users"),
            "SELECT password_hash FROM users"
        );
    }

    #[tokio::test]
    async fn a_statement_records_what_it_did() {
        let _serial = SERIAL.lock();
        reset();
        set_capacity(8);
        let recording = begin().expect("enabled by default");
        let probe = recording.probe();
        observe(probe, async {
            note_rows_scanned(1_000);
            note_rows(3);
        })
        .await;
        finish(
            recording,
            "SELECT * FROM t WHERE id = 7",
            "db",
            "alice",
            None,
        );

        let recent = recent(10);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].rows, 3);
        assert_eq!(recent[0].rows_scanned, 1_000);
        assert_eq!(recent[0].username, "alice");
        assert!(recent[0].error.is_none());

        let agg = aggregated();
        assert_eq!(agg.len(), 1);
        assert_eq!(agg[0].calls, 1);
        assert_eq!(agg[0].query, "SELECT * FROM t WHERE id = $1");
        assert_eq!(agg[0].queryid, recent[0].queryid);
        reset();
    }

    #[tokio::test]
    async fn counters_outside_a_scope_go_nowhere() {
        // Every statement the engine runs for itself — the temporary tables a
        // catalogue view is built from, above all — runs outside a scope, and
        // must not be charged to whatever statement is running.
        note_rows(5);
        note_rows_scanned(5);
    }

    #[test]
    fn the_ring_and_the_map_are_bounded() {
        let _serial = SERIAL.lock();
        reset();
        set_capacity(4);
        for i in 0..20 {
            let r = begin().unwrap();
            finish(r, &format!("SELECT {}", i), "db", "u", None);
        }
        assert_eq!(recent(100).len(), 4);
        // Every one of those normalises to the same shape.
        assert_eq!(aggregated().len(), 1);
        assert_eq!(aggregated()[0].calls, 20);
        reset();
        set_capacity(DEFAULT_RECENT);
    }

    #[test]
    fn a_failure_is_recorded_as_one() {
        let _serial = SERIAL.lock();
        reset();
        let r = begin().unwrap();
        finish(r, "SELECT * FROM nope", "db", "u", Some("table not found"));
        let recent = recent(1);
        assert_eq!(recent[0].error.as_deref(), Some("table not found"));
        assert_eq!(aggregated()[0].errors, 1);
        reset();
    }

    #[test]
    fn statistics_are_computable_from_what_is_kept() {
        let mut agg = Aggregate {
            queryid: 1,
            query: String::new(),
            calls: 0,
            errors: 0,
            total_us: 0,
            min_us: u64::MAX,
            max_us: 0,
            sum_sq_us: 0.0,
            rows: 0,
            rows_scanned: 0,
            materialised: 0,
            materialised_us: 0,
            last_seen_unix_us: 0,
        };
        for d in [10u64, 20, 30] {
            agg.calls += 1;
            agg.total_us += d;
            agg.min_us = agg.min_us.min(d);
            agg.max_us = agg.max_us.max(d);
            agg.sum_sq_us += (d * d) as f64;
        }
        assert_eq!(agg.mean_us(), 20.0);
        // Population standard deviation of 10/20/30 is sqrt(200/3).
        assert!((agg.stddev_us() - (200.0f64 / 3.0).sqrt()).abs() < 1e-9);
    }
}
