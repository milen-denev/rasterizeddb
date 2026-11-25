//! Per-client statistics (feature `client_stats`): the gateway's observer
//! hook (`rasterizeddb_gateway::hooks::Observer`) fed into an aggregate per
//! client, where a client is a database, a role, the client's address (its
//! IP: ports change with every connection) and its `application_name`.
//!
//! Each client keeps its sessions, statements and errors, a latency
//! histogram (p50/p95/p99 and the maximum), the bytes it sent and was sent,
//! and the bytes its statements exchanged with the shards: as framed, and
//! as sent on the wire after compression (`shard_compression`).
//!
//! - `SHOW CLIENT STATS` answers the aggregate since the start (or the last
//!   `RESET CLIENT STATS`), busiest first;
//! - `SHOW CLIENT STATS HISTORY [minutes]` answers the intervals an embedded
//!   RasterizedDB kept (feature `client_stats_embedded`, [`History`]);
//! - the metrics endpoint gets a line per client and measure.
//!
//! All three name other clients, so only the roles the operator lists
//! (`--client-stats-roles`) may ask. The number of clients tracked is
//! bounded (`--client-stats-max-clients`): past it, new clients are counted
//! together as `(other)`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rasterizeddb_gateway::hooks::{Answer, ClientInfo, Observer, StatementStats};

/// Latency buckets: four per power of two of microseconds (log-linear).
const BUCKETS: usize = 252;

/// The bucket of `us` microseconds.
fn bucket(us: u64) -> usize {
    if us < 4 {
        return us as usize;
    }
    let msb = 63 - us.leading_zeros() as usize;
    let sub = ((us >> (msb - 2)) & 3) as usize;
    ((msb - 1) * 4 + sub).min(BUCKETS - 1)
}

/// The exclusive upper edge of bucket `b`, in microseconds.
fn bucket_upper(b: usize) -> u64 {
    if b < 4 {
        return b as u64 + 1;
    }
    let msb = b / 4 + 1;
    let sub = (b % 4) as u64;
    (5 + sub).checked_shl((msb - 2) as u32).unwrap_or(u64::MAX)
}

/// One client.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key {
    pub database: String,
    pub role: String,
    pub client: String,
    pub application: String,
}

impl Key {
    fn of(c: &ClientInfo) -> Key {
        // The IP alone: a client's port changes with every connection.
        let client = match c.address.rsplit_once(':') {
            Some((ip, port)) if port.bytes().all(|b| b.is_ascii_digit()) => ip.trim_matches(['[', ']']).to_string(),
            _ => c.address.clone(),
        };
        Key { database: c.database.clone(), role: c.role.clone(), client, application: c.application.clone() }
    }

    fn other() -> Key {
        Key { database: "*".into(), role: "*".into(), client: "(other)".into(), application: String::new() }
    }
}

/// One client's measures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Agg {
    pub sessions: u64,
    pub open: i64,
    pub statements: u64,
    pub errors: u64,
    pub latency: Box<[u64; BUCKETS]>,
    pub latency_sum_us: u64,
    pub latency_max_us: u64,
    pub client_in: u64,
    pub client_out: u64,
    pub shard_calls: u64,
    pub shard_out_plain: u64,
    pub shard_out_wire: u64,
    pub shard_in_plain: u64,
    pub shard_in_wire: u64,
    /// Unix seconds of the last statement or session event.
    pub last_seen: u64,
}

impl Default for Agg {
    fn default() -> Self {
        Agg {
            sessions: 0,
            open: 0,
            statements: 0,
            errors: 0,
            latency: Box::new([0; BUCKETS]),
            latency_sum_us: 0,
            latency_max_us: 0,
            client_in: 0,
            client_out: 0,
            shard_calls: 0,
            shard_out_plain: 0,
            shard_out_wire: 0,
            shard_in_plain: 0,
            shard_in_wire: 0,
            last_seen: 0,
        }
    }
}

impl Agg {
    /// The latency at quantile `q` (0..1) in microseconds: the upper edge of
    /// its bucket, at most the maximum seen.
    pub fn quantile_us(&self, q: f64) -> u64 {
        let total: u64 = self.latency.iter().sum();
        if total == 0 {
            return 0;
        }
        let want = ((total as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (b, n) in self.latency.iter().enumerate() {
            seen += n;
            if seen >= want {
                return bucket_upper(b).min(self.latency_max_us.max(1));
            }
        }
        self.latency_max_us
    }

    /// What happened since `before` (an earlier copy of this one).
    fn since(&self, before: &Agg) -> Agg {
        let mut latency = Box::new([0u64; BUCKETS]);
        for (i, l) in latency.iter_mut().enumerate() {
            *l = self.latency[i].saturating_sub(before.latency[i]);
        }
        Agg {
            sessions: self.sessions - before.sessions.min(self.sessions),
            open: self.open,
            statements: self.statements - before.statements.min(self.statements),
            errors: self.errors - before.errors.min(self.errors),
            latency,
            latency_sum_us: self.latency_sum_us.saturating_sub(before.latency_sum_us),
            // The interval's maximum is not kept; the bucket edge stands in.
            latency_max_us: self.latency_max_us,
            client_in: self.client_in.saturating_sub(before.client_in),
            client_out: self.client_out.saturating_sub(before.client_out),
            shard_calls: self.shard_calls.saturating_sub(before.shard_calls),
            shard_out_plain: self.shard_out_plain.saturating_sub(before.shard_out_plain),
            shard_out_wire: self.shard_out_wire.saturating_sub(before.shard_out_wire),
            shard_in_plain: self.shard_in_plain.saturating_sub(before.shard_in_plain),
            shard_in_wire: self.shard_in_wire.saturating_sub(before.shard_in_wire),
            last_seen: self.last_seen,
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// How the statistics are kept.
#[derive(Clone, Debug)]
pub struct Settings {
    /// Clients tracked apart; the rest are counted as `(other)`.
    pub max_clients: usize,
    /// The roles that may read them (`SHOW CLIENT STATS…`, `RESET CLIENT
    /// STATS`).
    pub roles: Vec<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Settings { max_clients: 4096, roles: Vec::new() }
    }
}

/// The aggregate, sharded by key so the gateway's cores rarely meet on a
/// lock.
pub struct ClientStats {
    shards: Vec<Mutex<HashMap<Key, Agg>>>,
    tracked: AtomicUsize,
    settings: Settings,
    #[cfg(feature = "client_stats_embedded")]
    history: Option<History>,
}

const SHARDS: usize = 16;

impl ClientStats {
    pub fn new(settings: Settings) -> ClientStats {
        ClientStats {
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            tracked: AtomicUsize::new(0),
            settings,
            #[cfg(feature = "client_stats_embedded")]
            history: None,
        }
    }

    /// Keeps intervals in `history` from now on.
    #[cfg(feature = "client_stats_embedded")]
    pub fn with_history(mut self, history: History) -> ClientStats {
        self.history = Some(history);
        self
    }

    fn shard_of(key: &Key) -> usize {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % SHARDS
    }

    /// Applies `f` to `client`'s aggregate (or `(other)`'s, past the bound).
    fn with(&self, client: &ClientInfo, f: impl FnOnce(&mut Agg)) {
        let mut key = Key::of(client);
        let mut shard = self.shards[Self::shard_of(&key)].lock().unwrap_or_else(|e| e.into_inner());
        if !shard.contains_key(&key) {
            if self.tracked.load(Ordering::Relaxed) >= self.settings.max_clients {
                drop(shard);
                key = Key::other();
                shard = self.shards[Self::shard_of(&key)].lock().unwrap_or_else(|e| e.into_inner());
            }
            if !shard.contains_key(&key) {
                self.tracked.fetch_add(1, Ordering::Relaxed);
            }
        }
        let agg = shard.entry(key).or_default();
        f(agg);
        agg.last_seen = now_secs();
    }

    /// Every client's aggregate, busiest first.
    pub fn snapshot(&self) -> Vec<(Key, Agg)> {
        let mut all: Vec<(Key, Agg)> = Vec::new();
        for s in &self.shards {
            let s = s.lock().unwrap_or_else(|e| e.into_inner());
            all.extend(s.iter().map(|(k, a)| (k.clone(), a.clone())));
        }
        all.sort_by(|a, b| b.1.statements.cmp(&a.1.statements).then_with(|| a.0.cmp(&b.0)));
        all
    }

    /// Forgets every client but those with sessions open (their counts
    /// start again).
    pub fn reset(&self) {
        let mut kept = 0;
        for s in &self.shards {
            let mut s = s.lock().unwrap_or_else(|e| e.into_inner());
            s.retain(|_, a| a.open > 0);
            for a in s.values_mut() {
                let open = a.open;
                *a = Agg { open, last_seen: a.last_seen, ..Agg::default() };
            }
            kept += s.len();
        }
        self.tracked.store(kept, Ordering::Relaxed);
    }

    fn may_read(&self, client: &ClientInfo) -> Result<(), String> {
        if self.settings.roles.iter().any(|r| *r == client.role) {
            return Ok(());
        }
        Err(format!(
            "42501: client statistics name other clients: only the roles the gateway's --client-stats-roles lists may read them, and \"{}\" is not one",
            client.role
        ))
    }
}

/// `SHOW CLIENT STATS`' columns.
pub const COLUMNS: [&str; 21] = [
    "database",
    "role",
    "client",
    "application",
    "sessions_open",
    "sessions",
    "statements",
    "errors",
    "avg_ms",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "client_bytes_in",
    "client_bytes_out",
    "shard_calls",
    "shard_bytes_out",
    "shard_bytes_out_wire",
    "shard_bytes_in",
    "shard_bytes_in_wire",
    "last_seen",
];

fn ms(us: u64) -> String {
    format!("{:.3}", us as f64 / 1000.0)
}

fn row(k: &Key, a: &Agg) -> Vec<Option<String>> {
    let avg = if a.statements == 0 { 0 } else { a.latency_sum_us / a.statements };
    vec![
        Some(k.database.clone()),
        Some(k.role.clone()),
        Some(k.client.clone()),
        Some(k.application.clone()),
        Some(a.open.to_string()),
        Some(a.sessions.to_string()),
        Some(a.statements.to_string()),
        Some(a.errors.to_string()),
        Some(ms(avg)),
        Some(ms(a.quantile_us(0.50))),
        Some(ms(a.quantile_us(0.95))),
        Some(ms(a.quantile_us(0.99))),
        Some(ms(a.latency_max_us)),
        Some(a.client_in.to_string()),
        Some(a.client_out.to_string()),
        Some(a.shard_calls.to_string()),
        Some(a.shard_out_plain.to_string()),
        Some(a.shard_out_wire.to_string()),
        Some(a.shard_in_plain.to_string()),
        Some(a.shard_in_wire.to_string()),
        Some(a.last_seen.to_string()),
    ]
}

/// The statement `sql` names, when it is one of ours.
#[derive(Debug, PartialEq, Eq)]
enum Ours {
    Show,
    History(u64),
    Reset,
}

fn ours(sql: &str) -> Option<Result<Ours, String>> {
    let t = sql.trim().trim_end_matches(';').trim();
    let up = t.to_ascii_uppercase();
    let words: Vec<&str> = up.split_whitespace().collect();
    match words.as_slice() {
        ["SHOW", "CLIENT", "STATS"] => Some(Ok(Ours::Show)),
        ["SHOW", "CLIENT", "STATS", "HISTORY"] => Some(Ok(Ours::History(60))),
        ["SHOW", "CLIENT", "STATS", "HISTORY", n] => {
            Some(n.parse::<u64>().map(Ours::History).map_err(|_| "42601: SHOW CLIENT STATS HISTORY takes a number of minutes".to_string()))
        }
        ["RESET", "CLIENT", "STATS"] => Some(Ok(Ours::Reset)),
        ["SHOW", "CLIENT", ..] | ["RESET", "CLIENT", ..] => Some(Err(
            "42601: the statements are SHOW CLIENT STATS, SHOW CLIENT STATS HISTORY [minutes] and RESET CLIENT STATS".to_string(),
        )),
        _ => None,
    }
}

impl Observer for ClientStats {
    fn session_opened(&self, client: &ClientInfo) {
        self.with(client, |a| {
            a.sessions += 1;
            a.open += 1;
        });
    }

    fn statement(&self, client: &ClientInfo, s: &StatementStats) {
        self.with(client, |a| {
            a.statements += 1;
            if s.sqlstate.is_some() {
                a.errors += 1;
            }
            a.latency[bucket(s.latency_us)] += 1;
            a.latency_sum_us += s.latency_us;
            a.latency_max_us = a.latency_max_us.max(s.latency_us);
            a.client_in += s.client_bytes_in;
            a.client_out += s.client_bytes_out;
            a.shard_calls += s.shard_calls as u64;
            a.shard_out_plain += s.shard_bytes_out_plain;
            a.shard_out_wire += s.shard_bytes_out_wire;
            a.shard_in_plain += s.shard_bytes_in_plain;
            a.shard_in_wire += s.shard_bytes_in_wire;
        });
    }

    fn session_closed(&self, client: &ClientInfo) {
        self.with(client, |a| a.open -= 1);
    }

    fn answer<'a>(&'a self, client: &'a ClientInfo, sql: &'a str) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = Answer> + Send + 'a>>> {
        let which = ours(sql)?;
        Some(Box::pin(async move {
            let which = which?;
            self.may_read(client)?;
            match which {
                Ours::Show => {
                    let rows = self.snapshot().iter().map(|(k, a)| row(k, a)).collect();
                    Ok((COLUMNS.iter().map(|c| c.to_string()).collect(), rows))
                }
                Ours::Reset => {
                    self.reset();
                    Ok((vec!["reset".to_string()], vec![vec![Some("ok".to_string())]]))
                }
                #[cfg(feature = "client_stats_embedded")]
                Ours::History(minutes) => match &self.history {
                    Some(h) => h.read(minutes).await,
                    None => Err("55000: no history is kept: start the gateway with --client-stats-dir".to_string()),
                },
                #[cfg(not(feature = "client_stats_embedded"))]
                Ours::History(_) => Err("0A000: this gateway was built without client_stats_embedded, which keeps the history".to_string()),
            }
        }))
    }

    fn metrics(&self) -> String {
        let mut out = String::new();
        let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n");
        for (k, a) in self.snapshot() {
            let labels = format!(
                "database=\"{}\",role=\"{}\",client=\"{}\",application=\"{}\"",
                esc(&k.database),
                esc(&k.role),
                esc(&k.client),
                esc(&k.application)
            );
            for (name, v) in [
                ("rdb_gateway_client_statements_total", a.statements as f64),
                ("rdb_gateway_client_errors_total", a.errors as f64),
                ("rdb_gateway_client_sessions_open", a.open as f64),
                ("rdb_gateway_client_latency_p50_seconds", a.quantile_us(0.50) as f64 / 1e6),
                ("rdb_gateway_client_latency_p99_seconds", a.quantile_us(0.99) as f64 / 1e6),
                ("rdb_gateway_client_bytes_in_total", a.client_in as f64),
                ("rdb_gateway_client_bytes_out_total", a.client_out as f64),
                ("rdb_gateway_client_shard_bytes_plain_total", (a.shard_out_plain + a.shard_in_plain) as f64),
                ("rdb_gateway_client_shard_bytes_wire_total", (a.shard_out_wire + a.shard_in_wire) as f64),
            ] {
                out.push_str(&format!("{name}{{{labels}}} {v}\n"));
            }
        }
        out
    }
}

/// Intervals of the aggregate kept in an embedded RasterizedDB (feature
/// `client_stats_embedded`): a thread of its own runs the engine, writes
/// each interval's deltas every `flush`, drops what is older than
/// `retention`, and answers `SHOW CLIENT STATS HISTORY` over a channel.
#[cfg(feature = "client_stats_embedded")]
pub struct History {
    ask: tokio::sync::mpsc::UnboundedSender<(u64, tokio::sync::oneshot::Sender<Answer>)>,
}

#[cfg(feature = "client_stats_embedded")]
pub const HISTORY_COLUMNS: [&str; 17] = [
    "at",
    "database",
    "role",
    "client",
    "application",
    "statements",
    "errors",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "client_bytes_in",
    "client_bytes_out",
    "shard_calls",
    "shard_bytes_out",
    "shard_bytes_out_wire",
    "shard_bytes_in",
    "shard_bytes_in_wire",
];

#[cfg(feature = "client_stats_embedded")]
impl History {
    /// Starts the history thread on `dir`, reading `stats` every `flush`.
    /// The engine's start failing (a CPU without AVX2, a directory in use)
    /// is returned, and the gateway runs without history.
    pub fn start(
        dir: std::path::PathBuf,
        flush: std::time::Duration,
        retention: std::time::Duration,
        stats: std::sync::Weak<ClientStats>,
    ) -> Result<History, String> {
        let (ask_tx, mut ask_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, tokio::sync::oneshot::Sender<Answer>)>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();
        std::thread::Builder::new()
            .name("client-stats-history".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e.to_string()));
                        return;
                    }
                };
                rt.block_on(async move {
                    // Low-memory mode, always: it keeps a few rows a minute.
                    let db = match rasterizeddb_embedded::Builder::open(&dir).low_memory().start().await {
                        Ok(db) => db,
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("{}: {e}", dir.display())));
                            return;
                        }
                    };
                    let client = match db.connect().await {
                        Ok(c) => c,
                        Err(e) => {
                            let _ = ready_tx.send(Err(e.to_string()));
                            return;
                        }
                    };
                    let create = "CREATE TABLE IF NOT EXISTS client_stats (at BIGINT NOT NULL, database TEXT NOT NULL, role TEXT NOT NULL, \
                                  client TEXT NOT NULL, application TEXT NOT NULL, statements BIGINT, errors BIGINT, p50_us BIGINT, \
                                  p95_us BIGINT, p99_us BIGINT, client_bytes_in BIGINT, client_bytes_out BIGINT, shard_calls BIGINT, \
                                  shard_bytes_out BIGINT, shard_bytes_out_wire BIGINT, shard_bytes_in BIGINT, shard_bytes_in_wire BIGINT)";
                    if let Err(e) = client.run(create).await {
                        let _ = ready_tx.send(Err(e.to_string()));
                        return;
                    }
                    let _ = ready_tx.send(Ok(()));
                    let mut last: HashMap<Key, Agg> = HashMap::new();
                    let mut tick = tokio::time::interval(flush);
                    tick.tick().await;
                    loop {
                        tokio::select! {
                            _ = tick.tick() => {
                                let Some(stats) = stats.upgrade() else { break };
                                if let Err(e) = flush_interval(&client, &stats, &mut last, retention).await {
                                    log::warn!("client statistics history: {e}");
                                }
                            }
                            asked = ask_rx.recv() => {
                                let Some((minutes, reply)) = asked else { break };
                                let _ = reply.send(read_history(&client, minutes).await);
                            }
                        }
                    }
                    let _ = db.shutdown().await;
                });
            })
            .map_err(|e| e.to_string())?;
        ready_rx.recv().map_err(|_| "the history thread ended".to_string())??;
        Ok(History { ask: ask_tx })
    }

    async fn read(&self, minutes: u64) -> Answer {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.ask.send((minutes, tx)).map_err(|_| "XX000: the history thread has ended".to_string())?;
        rx.await.map_err(|_| "XX000: the history thread has ended".to_string())?
    }
}

#[cfg(feature = "client_stats_embedded")]
fn sql_text(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Writes every client's interval (what changed since `last`) and drops rows
/// past `retention`.
#[cfg(feature = "client_stats_embedded")]
async fn flush_interval(
    client: &rasterizeddb_client::Client,
    stats: &ClientStats,
    last: &mut HashMap<Key, Agg>,
    retention: std::time::Duration,
) -> Result<(), String> {
    let at = now_secs();
    let mut values = Vec::new();
    for (k, a) in stats.snapshot() {
        let d = match last.get(&k) {
            Some(before) => a.since(before),
            None => a.clone(),
        };
        last.insert(k.clone(), a);
        if d.statements == 0 && d.sessions == 0 {
            continue;
        }
        values.push(format!(
            "({at}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
            sql_text(&k.database),
            sql_text(&k.role),
            sql_text(&k.client),
            sql_text(&k.application),
            d.statements,
            d.errors,
            d.quantile_us(0.50),
            d.quantile_us(0.95),
            d.quantile_us(0.99),
            d.client_in,
            d.client_out,
            d.shard_calls,
            d.shard_out_plain,
            d.shard_out_wire,
            d.shard_in_plain,
            d.shard_in_wire,
        ));
    }
    for chunk in values.chunks(200) {
        let sql = format!(
            "INSERT INTO client_stats (at, database, role, client, application, statements, errors, p50_us, p95_us, p99_us, \
             client_bytes_in, client_bytes_out, shard_calls, shard_bytes_out, shard_bytes_out_wire, shard_bytes_in, shard_bytes_in_wire) VALUES {}",
            chunk.join(", ")
        );
        client.run(&sql).await.map_err(|e| e.to_string())?;
    }
    let cutoff = at.saturating_sub(retention.as_secs());
    client.run(&format!("DELETE FROM client_stats WHERE at < {cutoff}")).await.map_err(|e| e.to_string())?;
    Ok(())
}

/// The intervals of the last `minutes`, newest first.
#[cfg(feature = "client_stats_embedded")]
async fn read_history(client: &rasterizeddb_client::Client, minutes: u64) -> Answer {
    let since = now_secs().saturating_sub(minutes.saturating_mul(60));
    let rows = client
        .run(&format!(
            "SELECT at, database, role, client, application, statements, errors, p50_us, p95_us, p99_us, client_bytes_in, \
             client_bytes_out, shard_calls, shard_bytes_out, shard_bytes_out_wire, shard_bytes_in, shard_bytes_in_wire \
             FROM client_stats WHERE at >= {since} ORDER BY at DESC, statements DESC"
        ))
        .await
        .map_err(|e| format!("XX000: {e}"))?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows.iter() {
        let int = |i: usize| r.get::<Option<i64>>(i).ok().flatten().map(|v| v.to_string());
        let text = |i: usize| r.get::<Option<String>>(i).ok().flatten();
        let ms_of = |i: usize| r.get::<Option<i64>>(i).ok().flatten().map(|v| ms(v.max(0) as u64));
        out.push(vec![
            int(0),
            text(1),
            text(2),
            text(3),
            text(4),
            int(5),
            int(6),
            ms_of(7),
            ms_of(8),
            ms_of(9),
            int(10),
            int(11),
            int(12),
            int(13),
            int(14),
            int(15),
            int(16),
        ]);
    }
    Ok((HISTORY_COLUMNS.iter().map(|c| c.to_string()).collect(), out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(role: &str, port: u16) -> ClientInfo {
        ClientInfo {
            session: 1,
            database: "app".into(),
            role: role.into(),
            address: format!("10.0.0.7:{port}"),
            application: "api".into(),
        }
    }

    fn stmt(us: u64, error: bool) -> StatementStats {
        StatementStats {
            latency_us: us,
            sqlstate: error.then(|| "23505".to_string()),
            client_bytes_in: 100,
            client_bytes_out: 1000,
            shard_calls: 2,
            shard_bytes_out_plain: 300,
            shard_bytes_out_wire: 120,
            shard_bytes_in_plain: 5000,
            shard_bytes_in_wire: 900,
        }
    }

    #[test]
    fn buckets_cover_every_latency_in_order() {
        let mut prev = 0;
        for us in [0u64, 1, 3, 4, 5, 7, 8, 9, 15, 16, 1000, 1 << 20, u64::MAX / 2] {
            let b = bucket(us);
            assert!(b >= prev, "{us}");
            assert!(bucket_upper(b) > us || b == BUCKETS - 1, "{us} in {b}");
            prev = b;
        }
    }

    #[test]
    fn a_client_is_its_ip_and_its_traffic_adds_up() {
        let s = ClientStats::new(Settings { max_clients: 10, roles: vec!["ops".into()] });
        for port in [5000, 5001] {
            let c = client("api_user", port);
            s.session_opened(&c);
            for i in 0..100 {
                s.statement(&c, &stmt(1000 + i * 10, i == 0));
            }
        }
        s.session_closed(&client("api_user", 5000));
        let snap = s.snapshot();
        assert_eq!(snap.len(), 1);
        let (k, a) = &snap[0];
        assert_eq!(k.client, "10.0.0.7");
        assert_eq!((a.sessions, a.open, a.statements, a.errors), (2, 1, 200, 2));
        assert_eq!((a.shard_out_plain, a.shard_out_wire, a.shard_in_wire), (60_000, 24_000, 180_000));
        let p50 = a.quantile_us(0.5);
        assert!((1000..=1700).contains(&p50), "{p50}");
        assert!(a.quantile_us(0.99) <= a.latency_max_us);
    }

    #[test]
    fn past_the_bound_clients_are_counted_together() {
        let s = ClientStats::new(Settings { max_clients: 2, roles: vec![] });
        for role in ["a", "b", "c", "d"] {
            s.statement(&client(role, 1), &stmt(10, false));
        }
        let snap = s.snapshot();
        assert_eq!(snap.len(), 3);
        let other = snap.iter().find(|(k, _)| k.client == "(other)").unwrap();
        assert_eq!(other.1.statements, 2);
    }

    #[test]
    fn only_listed_roles_read_them() {
        let s = ClientStats::new(Settings { max_clients: 10, roles: vec!["ops".into()] });
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let denied = rt.block_on(s.answer(&client("api_user", 1), "SHOW CLIENT STATS").unwrap());
        assert!(denied.unwrap_err().starts_with("42501"));
        s.statement(&client("api_user", 1), &stmt(10, false));
        let (cols, rows) = rt.block_on(s.answer(&client("ops", 1), "show client stats;").unwrap()).unwrap();
        assert_eq!(cols.len(), COLUMNS.len());
        assert_eq!(rows.len(), 1);
        assert!(s.answer(&client("ops", 1), "SELECT 1").is_none());
        assert!(rt.block_on(s.answer(&client("ops", 1), "SHOW CLIENT nonsense").unwrap()).unwrap_err().starts_with("42601"));
        rt.block_on(s.answer(&client("ops", 1), "RESET CLIENT STATS").unwrap()).unwrap();
        assert!(s.snapshot().is_empty());
    }

    #[test]
    fn an_interval_is_the_difference() {
        let mut a = Agg::default();
        a.statements = 10;
        a.latency[bucket(100)] = 10;
        a.shard_out_wire = 500;
        let mut b = a.clone();
        b.statements = 15;
        b.latency[bucket(100)] = 15;
        b.shard_out_wire = 800;
        let d = b.since(&a);
        assert_eq!((d.statements, d.shard_out_wire, d.latency[bucket(100)]), (5, 300, 5));
    }
}
