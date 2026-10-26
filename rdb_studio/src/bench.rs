//! Running one statement over and over, and saying how fast it went.
//!
//! # What is measured
//!
//! End to end, as a client sees it: the clock starts before the statement is
//! handed to the driver and stops when the last row of the answer has arrived.
//! That includes the network and the driver, which is the point — a server
//! that answers in 40 µs behind a link that adds 300 µs is a 340 µs database
//! to everything that uses it.
//!
//! # Why its own connections
//!
//! Each worker opens a fresh connection from the profile the console is using
//! rather than sharing the console's own. One connection can only have one
//! statement in flight, so sharing it would measure a single client however
//! many workers were asked for — and it would freeze every other tab behind
//! the benchmark for as long as it ran.
//!
//! # Why a histogram rather than every sample
//!
//! A statement that answers in 20 µs, on 32 connections for five minutes, is
//! hundreds of millions of samples. Keeping each one is gigabytes. A
//! log-linear histogram with 128 buckets per power of two keeps any latency to
//! within 0.8 % in a fixed 58 KiB per worker, and merges by addition. Minimum,
//! maximum and mean are kept exactly, beside it.
//!
//! # Warm-up
//!
//! The first statements on a fresh connection pay for things the steady state
//! does not: a cold plan cache, pages not yet in memory, TCP slow start. Those
//! are run and thrown away. The statement is also run once before anything
//! else, alone, so a typing mistake is one error message rather than a
//! benchmark of the error path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use iced::futures::{SinkExt, Stream, channel::mpsc};
use tokio::sync::watch;
use tokio_postgres::{Client, SimpleQueryMessage};

use crate::conn;
use crate::model::Connection;

/// The most workers one run may open. A desktop console opening thousands of
/// connections is a mistake rather than a benchmark, and the server's own
/// connection limit is usually well under it.
pub const MAX_CONNECTIONS: usize = 256;

/// The longest measured window, and the longest warm-up.
pub const MAX_DURATION: Duration = Duration::from_secs(3600);
pub const MAX_WARMUP: Duration = Duration::from_secs(600);

/// How often the page hears how the run is going.
const PROGRESS_EVERY: Duration = Duration::from_millis(250);

/// How the statement is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    /// Text in, text out, parsed every time — what this console itself uses,
    /// and what `psql` sends.
    #[default]
    Simple,
    /// Parsed once per connection, then executed by name: what an application
    /// using a driver with prepared statements sends.
    Prepared,
}

impl Protocol {
    pub const ALL: [Protocol; 2] = [Protocol::Simple, Protocol::Prepared];

    pub fn label(&self) -> &'static str {
        match self {
            Protocol::Simple => "simple",
            Protocol::Prepared => "prepared",
        }
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

/// Everything one run needs.
#[derive(Debug, Clone)]
pub struct Plan {
    pub sql: String,
    pub connections: usize,
    pub duration: Duration,
    pub warmup: Duration,
    pub protocol: Protocol,
    pub profile: Connection,
}

/// Where a run is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Connecting,
    WarmingUp,
    Measuring,
    /// The window has closed and the statements already sent are finishing.
    Draining,
}

impl Phase {
    pub fn label(&self) -> &'static str {
        match self {
            Phase::Connecting => "connecting",
            Phase::WarmingUp => "warming up",
            Phase::Measuring => "measuring",
            Phase::Draining => "finishing in-flight statements",
        }
    }
}

/// How a run is going, while it goes.
#[derive(Debug, Clone, PartialEq)]
pub struct Progress {
    pub phase: Phase,
    /// Time spent in the measured window so far.
    pub elapsed: Duration,
    pub completed: u64,
    pub errors: u64,
    /// Over the last progress interval, not the whole run, so a stall shows.
    pub current_qps: f64,
    pub mean: Option<Duration>,
}

/// What a run produced, or why it did not.
#[derive(Debug, Clone)]
pub enum Event {
    Progress(Progress),
    /// Boxed: a report is several hundred bytes, and every progress event
    /// would otherwise be that size too.
    Finished(Result<Box<Report>, String>),
}

/// The answer.
#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    pub sql: String,
    pub connections: usize,
    pub protocol: Protocol,
    pub warmup: Duration,
    /// The measured window as it actually was: from its start to the moment
    /// the last worker finished. Longer than the window asked for when a
    /// statement was still running at its end, shorter when stopped.
    pub elapsed: Duration,
    /// Whether STOP ended the run before its window closed.
    pub stopped: bool,
    pub completed: u64,
    pub errors: u64,
    pub first_error: Option<String>,
    pub rows: u64,
    pub latency: Summary,
    /// Statements finished in each whole second of the window. The trailing
    /// part-second is left out, since it would read as a collapse.
    pub per_second: Vec<u64>,
    /// The latency distribution in display bins: lower bound, upper bound,
    /// count, in nanoseconds.
    pub distribution: Vec<(u64, u64, u64)>,
    pub connect_mean: Duration,
    pub connect_max: Duration,
    /// Statements the least and the most productive connection finished.
    /// Far apart means the server is not serving its clients evenly.
    pub per_connection: (u64, u64),
}

impl Report {
    pub fn qps(&self) -> f64 {
        rate(self.completed, self.elapsed)
    }

    pub fn rows_per_second(&self) -> f64 {
        rate(self.rows, self.elapsed)
    }

    pub fn rows_per_query(&self) -> f64 {
        if self.completed == 0 { 0.0 } else { self.rows as f64 / self.completed as f64 }
    }

    pub fn error_rate(&self) -> f64 {
        let total = self.completed + self.errors;
        if total == 0 { 0.0 } else { self.errors as f64 / total as f64 }
    }

    /// The slowest, fastest and spread of the per-second throughput.
    pub fn throughput_spread(&self) -> Option<(u64, u64, f64)> {
        let low = *self.per_second.iter().min()?;
        let high = *self.per_second.iter().max()?;
        let n = self.per_second.len() as f64;
        let mean = self.per_second.iter().sum::<u64>() as f64 / n;
        let variance = self
            .per_second
            .iter()
            .map(|&v| (v as f64 - mean).powi(2))
            .sum::<f64>()
            / n;
        let cv = if mean > 0.0 { variance.sqrt() / mean } else { 0.0 };
        Some((low, high, cv))
    }

    /// The report as text, for pasting into an issue or a chat.
    pub fn to_text(&self) -> String {
        let l = &self.latency;
        let mut out = String::new();
        out.push_str(&format!("statement:   {}\n", self.sql.trim()));
        out.push_str(&format!(
            "setup:       {} connection{}, {} protocol, {} warm-up, {} measured{}\n",
            self.connections,
            if self.connections == 1 { "" } else { "s" },
            self.protocol,
            duration(self.warmup),
            duration(self.elapsed),
            if self.stopped { " (stopped early)" } else { "" },
        ));
        out.push_str(&format!(
            "throughput:  {:.1} queries/s, {:.1} rows/s, {} queries, {} errors\n",
            self.qps(),
            self.rows_per_second(),
            self.completed,
            self.errors,
        ));
        out.push_str(&format!(
            "latency:     min {}  mean {}  median {}  p90 {}  p95 {}  p99 {}  p99.9 {}  max {}  stddev {}\n",
            duration(l.min),
            duration(l.mean),
            duration(l.p50),
            duration(l.p90),
            duration(l.p95),
            duration(l.p99),
            duration(l.p999),
            duration(l.max),
            duration(l.stddev),
        ));
        if let Some((low, high, cv)) = self.throughput_spread() {
            out.push_str(&format!(
                "per second:  {} to {} queries, variation {:.1}%\n",
                low,
                high,
                cv * 100.0
            ));
        }
        if let Some(error) = &self.first_error {
            out.push_str(&format!("first error: {}\n", error));
        }
        out
    }
}

/// The latency figures.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Summary {
    pub min: Duration,
    pub mean: Duration,
    pub p50: Duration,
    pub p90: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub max: Duration,
    pub stddev: Duration,
}

fn rate(count: u64, over: Duration) -> f64 {
    let seconds = over.as_secs_f64();
    if seconds <= 0.0 { 0.0 } else { count as f64 / seconds }
}

/// A latency the way a person reads one: three significant figures, in the
/// unit that keeps the number small.
pub fn duration(d: Duration) -> String {
    let ns = d.as_nanos() as f64;
    let (value, unit) = if ns < 1_000.0 {
        (ns, "ns")
    } else if ns < 1_000_000.0 {
        (ns / 1_000.0, "µs")
    } else if ns < 1_000_000_000.0 {
        (ns / 1_000_000.0, "ms")
    } else {
        (ns / 1_000_000_000.0, "s")
    };
    if value >= 100.0 {
        format!("{:.0} {}", value, unit)
    } else if value >= 10.0 {
        format!("{:.1} {}", value, unit)
    } else {
        format!("{:.2} {}", value, unit)
    }
}

// ---------------------------------------------------------------------------
// The histogram
// ---------------------------------------------------------------------------

/// Bits of a value kept below its leading one: 128 buckets per power of two.
const SUB_BITS: u32 = 7;
const SUB_COUNT: usize = 1 << SUB_BITS;
/// Enough buckets for any `u64`.
const BUCKETS: usize = (64 - SUB_BITS as usize + 1) * SUB_COUNT;

/// Latencies in nanoseconds, to within 1/128 of each one.
#[derive(Debug, Clone, PartialEq)]
pub struct Histogram {
    counts: Vec<u64>,
    count: u64,
    min: u64,
    max: u64,
    sum: u128,
    /// For the standard deviation. A float, since the squares of a minute's
    /// worth of nanosecond latencies overflow anything else.
    sum_squares: f64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            counts: vec![0; BUCKETS],
            count: 0,
            min: u64::MAX,
            max: 0,
            sum: 0,
            sum_squares: 0.0,
        }
    }
}

fn bucket_of(value: u64) -> usize {
    if value < SUB_COUNT as u64 {
        return value as usize;
    }
    let top = 63 - value.leading_zeros();
    let shift = top - SUB_BITS;
    let sub = (value >> shift) as usize & (SUB_COUNT - 1);
    ((shift as usize + 1) << SUB_BITS) + sub
}

/// The smallest value that falls in a bucket, and the largest.
fn bucket_bounds(index: usize) -> (u64, u64) {
    if index < SUB_COUNT {
        return (index as u64, index as u64);
    }
    let shift = (index >> SUB_BITS) - 1;
    let sub = (index & (SUB_COUNT - 1)) as u64;
    let low = (SUB_COUNT as u64 + sub) << shift;
    (low, low + ((1u64 << shift) - 1))
}

impl Histogram {
    pub fn record(&mut self, nanos: u64) {
        self.counts[bucket_of(nanos)] += 1;
        self.count += 1;
        self.min = self.min.min(nanos);
        self.max = self.max.max(nanos);
        self.sum += nanos as u128;
        self.sum_squares += (nanos as f64) * (nanos as f64);
    }

    pub fn merge(&mut self, other: &Histogram) {
        for (mine, theirs) in self.counts.iter_mut().zip(&other.counts) {
            *mine += theirs;
        }
        self.count += other.count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.sum_squares += other.sum_squares;
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    /// The value at a quantile, `0.0..=1.0`: the middle of the bucket the
    /// ranked sample falls in, never outside what was actually seen.
    pub fn quantile(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        if q <= 0.0 {
            return self.min;
        }
        if q >= 1.0 {
            return self.max;
        }
        let rank = ((q * self.count as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (index, &count) in self.counts.iter().enumerate() {
            seen += count;
            if seen >= rank {
                let (low, high) = bucket_bounds(index);
                return (low + (high - low) / 2).clamp(self.min, self.max);
            }
        }
        self.max
    }

    pub fn summary(&self) -> Summary {
        if self.count == 0 {
            return Summary::default();
        }
        let n = self.count as f64;
        let mean = self.sum as f64 / n;
        let variance = (self.sum_squares / n - mean * mean).max(0.0);
        let ns = |v: u64| Duration::from_nanos(v);
        Summary {
            min: ns(self.min),
            mean: Duration::from_nanos(mean.round() as u64),
            p50: ns(self.quantile(0.50)),
            p90: ns(self.quantile(0.90)),
            p95: ns(self.quantile(0.95)),
            p99: ns(self.quantile(0.99)),
            p999: ns(self.quantile(0.999)),
            max: ns(self.max),
            stddev: Duration::from_nanos(variance.sqrt().round() as u64),
        }
    }

    /// The distribution regrouped into `bins` bins evenly spaced on a log
    /// scale from the minimum to the maximum.
    ///
    /// Log rather than linear because latencies have a long tail: on a linear
    /// axis one 200 ms outlier squeezes every ordinary 1 ms answer into the
    /// first bar.
    pub fn bins(&self, bins: usize) -> Vec<(u64, u64, u64)> {
        if self.count == 0 || bins == 0 {
            return Vec::new();
        }
        let low = self.min.max(1) as f64;
        let high = (self.max.max(1) as f64).max(low * 1.0001);
        let step = (high / low).ln() / bins as f64;
        let edge = |i: usize| (low * (step * i as f64).exp()).round() as u64;

        let mut out: Vec<(u64, u64, u64)> =
            (0..bins).map(|i| (edge(i), edge(i + 1), 0)).collect();
        for (index, &count) in self.counts.iter().enumerate() {
            if count == 0 {
                continue;
            }
            let (b_low, b_high) = bucket_bounds(index);
            let middle = ((b_low + (b_high - b_low) / 2).clamp(self.min, self.max)).max(1) as f64;
            let at = (((middle / low).ln() / step).floor().max(0.0) as usize).min(bins - 1);
            out[at].2 += count;
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Running one
// ---------------------------------------------------------------------------

/// What each worker counts during the measured window, for the progress line.
#[derive(Default)]
struct Live {
    completed: AtomicU64,
    errors: AtomicU64,
    latency_ns: AtomicU64,
}

/// What a worker hands back.
#[derive(Default)]
struct Tally {
    latency: Histogram,
    per_second: Vec<u64>,
    rows: u64,
    errors: u64,
    first_error: Option<String>,
    finished: Option<Instant>,
}

/// Aborts workers that are still running when the run itself is dropped —
/// the window closed, or the console quit — so nothing keeps hammering a
/// server that nobody is measuring any more.
struct Workers(Vec<tokio::task::JoinHandle<Tally>>);

impl Drop for Workers {
    fn drop(&mut self) {
        for handle in &self.0 {
            handle.abort();
        }
    }
}

/// One way of sending the statement, fixed for a connection.
enum Statement {
    Simple(String),
    Prepared(tokio_postgres::Statement),
}

impl Statement {
    async fn prepare(client: &Client, sql: &str, protocol: Protocol) -> Result<Self, String> {
        match protocol {
            Protocol::Simple => Ok(Statement::Simple(sql.to_string())),
            Protocol::Prepared => client
                .prepare(sql)
                .await
                .map(Statement::Prepared)
                .map_err(|e| conn::sql_error(&e)),
        }
    }

    /// Runs it and counts the rows that came back.
    async fn run(&self, client: &Client) -> Result<u64, String> {
        match self {
            Statement::Simple(sql) => client
                .simple_query(sql)
                .await
                .map(|messages| {
                    messages
                        .iter()
                        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
                        .count() as u64
                })
                .map_err(|e| conn::sql_error(&e)),
            Statement::Prepared(statement) => client
                .query(statement, &[])
                .await
                .map(|rows| rows.len() as u64)
                .map_err(|e| conn::sql_error(&e)),
        }
    }
}

/// Runs a plan, reporting progress as it goes and the report at the end.
///
/// `stop` ends it early: statements in flight are abandoned rather than
/// waited for, and the report covers what finished before.
pub fn run(plan: Plan, stop: watch::Receiver<bool>) -> impl Stream<Item = Event> {
    iced::stream::channel(16, async move |mut output: mpsc::Sender<Event>| {
        let result = execute(plan, stop, &mut output).await;
        let _ = output.send(Event::Finished(result.map(Box::new))).await;
    })
}

async fn execute(
    plan: Plan,
    stop: watch::Receiver<bool>,
    output: &mut mpsc::Sender<Event>,
) -> Result<Report, String> {
    if plan.sql.trim().is_empty() {
        return Err("there is no statement to run".to_string());
    }
    let connections = plan.connections.clamp(1, MAX_CONNECTIONS);

    let _ = output
        .send(Event::Progress(Progress {
            phase: Phase::Connecting,
            elapsed: Duration::ZERO,
            completed: 0,
            errors: 0,
            current_qps: 0.0,
            mean: None,
        }))
        .await;

    // Every connection is opened before any statement runs, so the first
    // workers are not measured against a server still accepting the rest.
    let opening = (0..connections).map(|_| {
        let profile = plan.profile.clone();
        async move {
            let started = Instant::now();
            let session = conn::connect(profile).await?;
            Ok::<_, String>((session, started.elapsed()))
        }
    });
    let opened = iced::futures::future::join_all(opening).await;
    let mut sessions = Vec::with_capacity(connections);
    let mut connect_times = Vec::with_capacity(connections);
    for result in opened {
        let (session, took) = result.map_err(|e| format!("could not open a connection: {}", e))?;
        sessions.push(session);
        connect_times.push(took);
    }

    // The statement once, alone: a mistake is one clear error, not a run of
    // them.
    let first = Statement::prepare(sessions[0].client(), &plan.sql, plan.protocol).await?;
    first.run(sessions[0].client()).await?;

    let now = Instant::now();
    let measure_from = now + plan.warmup;
    let measure_until = measure_from + plan.duration;
    let live = Arc::new(Live::default());

    let mut workers = Workers(Vec::with_capacity(connections));
    for session in sessions {
        let sql = plan.sql.clone();
        let protocol = plan.protocol;
        let live = live.clone();
        let mut stop = stop.clone();
        workers.0.push(tokio::spawn(async move {
            let mut tally = Tally::default();
            let client = session.client();
            let statement = match Statement::prepare(client, &sql, protocol).await {
                Ok(statement) => statement,
                Err(e) => {
                    tally.errors += 1;
                    tally.first_error = Some(e);
                    tally.finished = Some(Instant::now());
                    return tally;
                }
            };
            loop {
                if *stop.borrow() || client.is_closed() {
                    break;
                }
                let started = Instant::now();
                if started >= measure_until {
                    break;
                }
                let result = tokio::select! {
                    result = statement.run(client) => result,
                    _ = stop.wait_for(|stopped| *stopped) => break,
                };
                let ended = Instant::now();
                if started < measure_from {
                    continue;
                }
                match result {
                    Ok(rows) => {
                        let took = (ended - started).as_nanos() as u64;
                        tally.latency.record(took);
                        tally.rows += rows;
                        let second = (ended - measure_from).as_secs() as usize;
                        if tally.per_second.len() <= second {
                            tally.per_second.resize(second + 1, 0);
                        }
                        tally.per_second[second] += 1;
                        live.completed.fetch_add(1, Ordering::Relaxed);
                        live.latency_ns.fetch_add(took, Ordering::Relaxed);
                    }
                    Err(e) => {
                        tally.errors += 1;
                        tally.first_error.get_or_insert(e);
                        live.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            tally.finished = Some(Instant::now());
            tally
        }));
    }

    // Progress, until every worker is done.
    let mut ticker = tokio::time::interval(PROGRESS_EVERY);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last = (Instant::now(), 0u64);
    while !workers.0.iter().all(|w| w.is_finished()) {
        ticker.tick().await;
        let now = Instant::now();
        let completed = live.completed.load(Ordering::Relaxed);
        let phase = if now < measure_from {
            Phase::WarmingUp
        } else if now < measure_until && !*stop.borrow() {
            Phase::Measuring
        } else {
            Phase::Draining
        };
        let window = (now - last.0).as_secs_f64();
        let current_qps = if window > 0.0 { (completed - last.1) as f64 / window } else { 0.0 };
        last = (now, completed);
        let mean = (completed > 0).then(|| {
            Duration::from_nanos(live.latency_ns.load(Ordering::Relaxed) / completed)
        });
        let _ = output
            .send(Event::Progress(Progress {
                phase,
                elapsed: now.saturating_duration_since(measure_from).min(plan.duration),
                completed,
                errors: live.errors.load(Ordering::Relaxed),
                current_qps,
                mean,
            }))
            .await;
    }

    let stopped = *stop.borrow() && Instant::now() < measure_until;
    let mut tallies = Vec::with_capacity(connections);
    for handle in std::mem::take(&mut workers.0) {
        tallies.push(handle.await.map_err(|e| format!("a worker failed: {}", e))?);
    }
    Ok(assemble(&plan, connections, measure_from, stopped, &connect_times, tallies))
}

/// Folds the workers' tallies into the report.
fn assemble(
    plan: &Plan,
    connections: usize,
    measure_from: Instant,
    stopped: bool,
    connect_times: &[Duration],
    tallies: Vec<Tally>,
) -> Report {
    let mut latency = Histogram::default();
    let mut per_second: Vec<u64> = Vec::new();
    let mut rows = 0;
    let mut errors = 0;
    let mut first_error = None;
    let mut last_finish = measure_from;
    let mut least = u64::MAX;
    let mut most = 0;

    for tally in tallies {
        latency.merge(&tally.latency);
        if per_second.len() < tally.per_second.len() {
            per_second.resize(tally.per_second.len(), 0);
        }
        for (total, count) in per_second.iter_mut().zip(&tally.per_second) {
            *total += count;
        }
        rows += tally.rows;
        errors += tally.errors;
        if first_error.is_none() {
            first_error = tally.first_error;
        }
        if let Some(finished) = tally.finished {
            last_finish = last_finish.max(finished);
        }
        least = least.min(tally.latency.count());
        most = most.max(tally.latency.count());
    }

    let elapsed = last_finish.saturating_duration_since(measure_from);
    // Only whole seconds: the last one is cut short by the window closing.
    per_second.truncate(elapsed.as_secs() as usize);

    let connect_mean = if connect_times.is_empty() {
        Duration::ZERO
    } else {
        connect_times.iter().sum::<Duration>() / connect_times.len() as u32
    };

    Report {
        sql: plan.sql.clone(),
        connections,
        protocol: plan.protocol,
        warmup: plan.warmup,
        elapsed,
        stopped,
        completed: latency.count(),
        errors,
        first_error,
        rows,
        latency: latency.summary(),
        per_second,
        distribution: latency.bins(40),
        connect_mean,
        connect_max: connect_times.iter().copied().max().unwrap_or_default(),
        per_connection: (if least == u64::MAX { 0 } else { least }, most),
    }
}

/// Whether a statement looks like it changes data, which a benchmark will do
/// thousands of times over. Read off the first keyword, after comments.
pub fn modifies_data(sql: &str) -> bool {
    let mut rest = sql.trim_start();
    loop {
        if let Some(after) = rest.strip_prefix("--") {
            rest = after.split_once('\n').map(|(_, r)| r).unwrap_or("").trim_start();
        } else if let Some(after) = rest.strip_prefix("/*") {
            rest = after.split_once("*/").map(|(_, r)| r).unwrap_or("").trim_start();
        } else {
            break;
        }
    }
    let word: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_uppercase();
    !matches!(word.as_str(), "SELECT" | "WITH" | "SHOW" | "VALUES" | "EXPLAIN" | "TABLE" | "")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every bucket's bounds must contain the values that land in it, and the
    /// buckets must tile the number line with no gap and no overlap.
    #[test]
    fn buckets_tile_the_number_line() {
        for value in (0..5_000u64).chain([u64::MAX / 3, u64::MAX - 1, u64::MAX]) {
            let (low, high) = bucket_bounds(bucket_of(value));
            assert!(low <= value && value <= high, "{} outside {}..={}", value, low, high);
        }
        for index in 0..BUCKETS - 1 {
            let (_, high) = bucket_bounds(index);
            let (next, _) = bucket_bounds(index + 1);
            assert_eq!(high + 1, next, "gap or overlap after bucket {}", index);
        }
        assert!(bucket_of(u64::MAX) < BUCKETS);
    }

    /// Within the 1/128 the bucket width promises.
    #[test]
    fn quantiles_are_accurate() {
        let mut h = Histogram::default();
        for v in 1..=100_000u64 {
            h.record(v * 1_000);
        }
        for (q, expected) in [(0.5, 50_000_000.0), (0.9, 90_000_000.0), (0.99, 99_000_000.0)] {
            let got = h.quantile(q) as f64;
            assert!((got - expected).abs() / expected < 1.0 / 128.0, "q{} = {}", q, got);
        }
        assert_eq!(h.quantile(0.0), 1_000);
        assert_eq!(h.quantile(1.0), 100_000_000);
        let s = h.summary();
        assert_eq!(s.min, Duration::from_micros(1));
        assert_eq!(s.max, Duration::from_millis(100));
        assert_eq!(s.mean, Duration::from_nanos(50_000_500));
    }

    /// One sample is every percentile, exactly — not a bucket midpoint that
    /// reads as a latency nobody saw.
    #[test]
    fn a_single_sample_is_every_percentile() {
        let mut h = Histogram::default();
        h.record(123_456);
        let s = h.summary();
        for value in [s.min, s.p50, s.p90, s.p99, s.p999, s.max, s.mean] {
            assert_eq!(value, Duration::from_nanos(123_456));
        }
        assert_eq!(s.stddev, Duration::ZERO);
    }

    #[test]
    fn merging_is_adding() {
        let mut a = Histogram::default();
        let mut b = Histogram::default();
        let mut both = Histogram::default();
        for v in [10u64, 500, 90_000] {
            a.record(v);
            both.record(v);
        }
        for v in [7u64, 1_000_000] {
            b.record(v);
            both.record(v);
        }
        a.merge(&b);
        assert_eq!(a, both);
    }

    #[test]
    fn an_empty_histogram_reports_zeroes() {
        let h = Histogram::default();
        assert_eq!(h.summary(), Summary::default());
        assert!(h.bins(10).is_empty());
    }

    /// The display bins lose no sample and span what was seen.
    #[test]
    fn bins_keep_every_sample() {
        let mut h = Histogram::default();
        for v in [1_000u64, 1_100, 2_000, 50_000, 3_000_000] {
            h.record(v);
        }
        let bins = h.bins(40);
        assert_eq!(bins.len(), 40);
        assert_eq!(bins.iter().map(|b| b.2).sum::<u64>(), 5);
        assert_eq!(bins[0].0, 1_000);
        assert!(bins[39].1 >= 2_990_000);
        assert!(bins[39].2 >= 1, "the maximum must land in the last bin");
    }

    #[test]
    fn writes_are_noticed() {
        assert!(!modifies_data("SELECT 1"));
        assert!(!modifies_data("  -- a comment\n  select * from t"));
        assert!(!modifies_data("/* hi */ WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(modifies_data("INSERT INTO t VALUES (1)"));
        assert!(modifies_data("update t set a = 1"));
        assert!(modifies_data("-- select\nDELETE FROM t"));
    }

    #[test]
    fn durations_read_in_their_own_unit() {
        assert_eq!(duration(Duration::from_nanos(850)), "850 ns");
        assert_eq!(duration(Duration::from_nanos(41_300)), "41.3 µs");
        assert_eq!(duration(Duration::from_micros(1_500)), "1.50 ms");
        assert_eq!(duration(Duration::from_secs(2)), "2.00 s");
    }

    fn report(per_second: Vec<u64>) -> Report {
        Report {
            sql: "SELECT 1".into(),
            connections: 1,
            protocol: Protocol::Simple,
            warmup: Duration::ZERO,
            elapsed: Duration::from_secs(2),
            stopped: false,
            completed: 200,
            errors: 0,
            first_error: None,
            rows: 400,
            latency: Summary::default(),
            per_second,
            distribution: Vec::new(),
            connect_mean: Duration::ZERO,
            connect_max: Duration::ZERO,
            per_connection: (200, 200),
        }
    }

    #[test]
    fn rates_divide_by_the_real_window() {
        let r = report(vec![100, 100]);
        assert_eq!(r.qps(), 100.0);
        assert_eq!(r.rows_per_second(), 200.0);
        assert_eq!(r.rows_per_query(), 2.0);
        assert_eq!(r.throughput_spread(), Some((100, 100, 0.0)));
        assert!(report(Vec::new()).throughput_spread().is_none());
    }
}
