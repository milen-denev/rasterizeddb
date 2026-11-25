//! A client session through the gateway (B1a.7, §4.9 G4–G7).
//!
//! One virtual session per shard, opened lazily on that shard's mux
//! connection. The client's bytes go to the shard unchanged; the replies
//! come back unchanged, except for the replies to messages the gateway
//! itself prepended (a deferred `BEGIN`, a lazy re-Parse), which it strips.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::rc::Rc;

use rasterizeddb_router::rshard::{self, Merge, Refusal, Request, Response, RouteTemplate, Source};
use rasterizeddb_router::shard_map::ShardMap;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::config::{Unrouted, UnroutedWrites};
use crate::core::{CallError, Core};
use crate::pg::{self, MsgReader};
use crate::route::{self, OwnedKey};
use crate::state::CancelTargets;

/// Where a statement goes.
pub(crate) enum Target {
    Shard(u32),
    FanOut { merge: Merge, writes: bool },
    Refuse { sqlstate: &'static str, message: String },
    /// Gather these sharded tables onto the home shard and run there (B4.5).
    Gather { tables: Vec<String> },
}

/// What a run on a shard came to.
pub(crate) enum Outcome {
    /// Answered (rows or an error) and written to the client.
    Done { error: bool },
    /// Refused before effect: decide the target again and resend.
    Retry,
    /// The client connection must end (a transaction's shard was lost).
    Close,
}

/// A transaction as the gateway tracks it.
pub(crate) struct Txn {
    /// ReadyForQuery's status: `I`, `T` or `E`.
    pub status: u8,
    /// The shard the transaction runs on, once its first statement routed.
    pub shard: Option<u32>,
    /// `BEGIN` and `SET LOCAL` messages recorded before the first routed
    /// statement, prepended to it.
    pub pending: Vec<u8>,
    pub pending_queries: usize,
    /// Under two-phase commit: every shard the transaction has run on, in
    /// order, and the opening messages (BEGIN, SET LOCAL) each new one gets.
    pub shards: Vec<u32>,
    pub opening: Vec<u8>,
    pub opening_queries: usize,
}

/// A virtual session on one shard.
pub(crate) struct VSession {
    pub id: u64,
    /// The link's connection the session was opened on
    /// ([`crate::core::Link::connection`]).
    pub connection: u64,
    /// Statement name → the generation parsed there.
    pub parsed: HashMap<String, u64>,
}

/// A prepared statement as the client defined it.
pub(crate) struct Stmt {
    pub text: String,
    pub parse: Vec<u8>,
    pub generation: u64,
}

pub struct Login {
    pub database: String,
    pub role: String,
    /// Startup parameters other than `user` and `database`, passed to every
    /// virtual session.
    pub params: Vec<(String, String)>,
    /// G11: locked to this shard.
    pub pinned: Option<u32>,
    pub cancel: CancelTargets,
}

/// Messages the gateway strips from a reply, in order.
struct ReplyFilter {
    /// Prepended simple queries: everything through each one's
    /// ReadyForQuery, errors excepted.
    skip_rfq: usize,
    /// Prepended extended messages' replies: ParseComplete (`1`) and
    /// CloseComplete (`3`).
    skip: VecDeque<u8>,
}

impl ReplyFilter {
    fn none() -> ReplyFilter {
        ReplyFilter { skip_rfq: 0, skip: VecDeque::new() }
    }

    fn apply(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        if self.skip_rfq == 0 && self.skip.is_empty() {
            out.extend_from_slice(bytes);
            return;
        }
        for (t, m) in pg::messages(bytes) {
            if self.skip_rfq > 0 {
                match t {
                    b'Z' => self.skip_rfq -= 1,
                    b'E' => out.extend_from_slice(m),
                    _ => {}
                }
                continue;
            }
            if let Some(&f) = self.skip.front() {
                if t == f {
                    self.skip.pop_front();
                    continue;
                }
                if t == b'E' {
                    self.skip.clear();
                }
            }
            out.extend_from_slice(m);
        }
    }
}

pub struct Session {
    pub(crate) core: Rc<Core>,
    pub(crate) login: Login,
    pub(crate) route_key: Option<OwnedKey>,
    pub(crate) local_key: Option<OwnedKey>,
    pub(crate) unrouted: Unrouted,
    pub(crate) unrouted_writes: UnroutedWrites,
    /// Fan-out reads take one snapshot across shards (B6.1).
    pub(crate) consistent_fanout: bool,
    /// `SET rdb.split_bulk_writes = on`: without two-phase commit, a COPY
    /// into a sharded table is split by shard, each shard's part committed
    /// alone (§8.5).
    pub(crate) split_bulk_writes: bool,
    /// A COPY being split by shard (`copysplit`).
    pub(crate) copy_split: Option<crate::copysplit::Split>,
    pub(crate) vs: HashMap<u32, VSession>,
    pub(crate) txn: Txn,
    /// Session-level `SET`s, as raw Query messages, replayed to every
    /// virtual session that opens later.
    pub(crate) session_sets: Vec<u8>,
    pub(crate) statements: HashMap<String, Stmt>,
    pub(crate) generation: u64,
    /// The shard a session's last routed statement went to: Describe and
    /// Parse-only batches go there (or home).
    pub(crate) last_shard: Option<u32>,
    pub(crate) copy_shard: Option<u32>,
    /// After an error in a batch that ended in Flush: drop messages until
    /// Sync, then ReadyForQuery.
    pub(crate) discard_until_sync: bool,
    /// The channels this session listens on, once it has (B4.3).
    pub(crate) listener: Option<crate::notify::Listener>,
    /// Set by `decide_extended` when the batch's statement fans out: how it
    /// merges, and whether it writes.
    pub(crate) pending_fanout: Option<(Merge, bool)>,
    /// Set by `decide_extended` when the batch's statement gathers.
    pub(crate) pending_gather: Option<Vec<String>>,
    /// Who this session is, for an observer (`hooks::Observer`).
    pub client: crate::hooks::ClientInfo,
    /// Bytes written to the client so far, and the first error's SQLSTATE
    /// in the statement being answered (taken only while observed).
    out_total: u64,
    stmt_error: Option<String>,
    pub(crate) out: Vec<u8>,
}

/// What the gateway does with a simple query itself.
enum Local {
    ShowGateway,
    ShowRouteKey,
    ShowSetting(&'static str),
    SetRouteKey { local: bool, value: Option<String> },
    SetUnrouted(String),
    SetUnroutedWrites(String),
    /// `SET rdb.consistent_fanout = on|off` (B6.1).
    SetConsistentFanout(String),
    SetSplitBulkWrites(String),
    Begin,
    End { commit: bool },
    SetLocal,
    SessionSet,
    Discard,
    Refused(&'static str),
    Routed,
    /// `LISTEN ch`, answered by the gateway's hub (B4.3).
    Listen(String),
    /// `UNLISTEN ch`, or `UNLISTEN *` (`None`).
    Unlisten(Option<String>),
}

/// A channel name as PostgreSQL reads it: quoted kept, bare lower-cased.
fn channel_of(raw: &str) -> Option<String> {
    let t = raw.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        return Some(t[1..t.len() - 1].replace("\"\"", "\""));
    }
    (!t.is_empty() && t.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80) && !t.as_bytes()[0].is_ascii_digit())
        .then(|| t.to_ascii_lowercase())
}

fn local_of(text: &str) -> Local {
    let t = text.trim().trim_end_matches(';').trim();
    // One statement only: anything with another `;` is forwarded whole.
    if t.contains(';') {
        return Local::Routed;
    }
    let lower = t.to_ascii_lowercase();
    let words: Vec<&str> = lower.split_whitespace().collect();
    match words.as_slice() {
        ["show", "gateway"] => return Local::ShowGateway,
        ["show", "rdb.route_key"] => return Local::ShowRouteKey,
        ["show", "rdb.unrouted"] => return Local::ShowSetting("unrouted"),
        ["show", "rdb.unrouted_writes"] => return Local::ShowSetting("unrouted_writes"),
        ["begin", ..] | ["start", "transaction", ..] => return Local::Begin,
        ["commit", ..] | ["end", ..] => return Local::End { commit: true },
        ["rollback"] | ["rollback", "work" | "transaction"] | ["abort", ..] => return Local::End { commit: false },
        ["discard", "all"] | ["reset", "all"] => return Local::Discard,
        // NOTIFY runs where its transaction runs, and is sent at commit
        // there; the hub carries it to every listener (B4.3).
        ["listen", _] => {
            return t[6..].trim().split_whitespace().next().and_then(channel_of).map_or(Local::Refused("LISTEN without a channel"), Local::Listen);
        }
        ["unlisten", "*"] => return Local::Unlisten(None),
        ["unlisten", _] => {
            return t[8..].trim().split_whitespace().next().and_then(channel_of).map_or(Local::Refused("UNLISTEN without a channel"), |c| Local::Unlisten(Some(c)));
        }
        _ => {}
    }
    if let ["reset", name] = words.as_slice() {
        return match *name {
            "rdb.route_key" => Local::SetRouteKey { local: false, value: None },
            _ => Local::SessionSet,
        };
    }
    if words.first() == Some(&"set") {
        let mut rest = &t[3..];
        let mut local = false;
        let r = rest.trim_start();
        if r.len() >= 6 && r[..6].eq_ignore_ascii_case("local ") {
            local = true;
            rest = &r[6..];
        } else if r.len() >= 8 && r[..8].eq_ignore_ascii_case("session ") {
            rest = &r[8..];
        }
        let rest = rest.trim_start();
        let name_end = rest.find(|c: char| c.is_whitespace() || c == '=').unwrap_or(rest.len());
        let name = rest[..name_end].to_ascii_lowercase();
        let after = rest[name_end..].trim_start();
        let value = after
            .strip_prefix('=')
            .or_else(|| (after.len() >= 3 && after[..3].eq_ignore_ascii_case("to ")).then(|| &after[3..]))
            .map(|v| v.trim().to_string());
        match name.as_str() {
            "rdb.route_key" => {
                let value = value.filter(|v| !v.eq_ignore_ascii_case("default"));
                return Local::SetRouteKey { local, value };
            }
            "rdb.unrouted" => return Local::SetUnrouted(value.unwrap_or_default()),
            "rdb.unrouted_writes" => return Local::SetUnroutedWrites(value.unwrap_or_default()),
            "rdb.consistent_fanout" => return Local::SetConsistentFanout(value.unwrap_or_default()),
            "rdb.split_bulk_writes" => return Local::SetSplitBulkWrites(value.unwrap_or_default()),
            // `SET TRANSACTION …` belongs to the transaction.
            "transaction" | "constraints" => return if local { Local::SetLocal } else { Local::Routed },
            _ => return if local { Local::SetLocal } else { Local::SessionSet },
        }
    }
    Local::Routed
}

impl Session {
    pub fn new(core: Rc<Core>, login: Login) -> Session {
        let unrouted = core.shared.cfg.unrouted;
        Session {
            core,
            login,
            route_key: None,
            local_key: None,
            unrouted,
            unrouted_writes: UnroutedWrites::Refuse,
            consistent_fanout: false,
            split_bulk_writes: false,
            copy_split: None,
            vs: HashMap::new(),
            txn: Txn {
                status: b'I',
                shard: None,
                pending: Vec::new(),
                pending_queries: 0,
                shards: Vec::new(),
                opening: Vec::new(),
                opening_queries: 0,
            },
            session_sets: Vec::new(),
            statements: HashMap::new(),
            generation: 0,
            last_shard: None,
            copy_shard: None,
            discard_until_sync: false,
            listener: None,
            pending_fanout: None,
            pending_gather: None,
            client: crate::hooks::ClientInfo {
                session: 0,
                database: String::new(),
                role: String::new(),
                address: String::new(),
                application: String::new(),
            },
            out_total: 0,
            stmt_error: None,
            out: Vec::with_capacity(8 << 10),
        }
    }

    pub(crate) async fn flush<S: AsyncWrite + Unpin>(&mut self, conn: &mut S) -> io::Result<()> {
        if !self.out.is_empty() {
            self.core.counters.borrow_mut().bytes_out += self.out.len() as u64;
            self.out_total += self.out.len() as u64;
            if self.stmt_error.is_none()
                && self.core.shared.hooks.observer.is_some()
                && let Some((_, m)) = pg::messages(&self.out).find(|(t, _)| *t == b'E')
            {
                self.stmt_error = Some(pg::error_sqlstate(m).unwrap_or("XX000").to_string());
            }
            pg::write_all(conn, &self.out).await?;
            self.out.clear();
            if self.out.capacity() > 256 << 10 {
                self.out.shrink_to(8 << 10);
            }
        }
        Ok(())
    }

    pub(crate) fn error_ready(&mut self, sqlstate: &str, message: &str) {
        // A shard forgot one of this session's virtual sessions: outside a
        // transaction every one is reopened on next use, so the error is not
        // repeated on each statement that reaches that shard.
        if sqlstate.as_bytes() == rshard::SESSION_GONE && self.txn.status == b'I' {
            self.vs.clear();
        }
        pg::error(&mut self.out, "ERROR", sqlstate, message);
        let status = if self.txn.status == b'I' { b'I' } else { b'E' };
        if self.txn.status != b'I' {
            self.txn.status = b'E';
        }
        pg::ready(&mut self.out, status);
    }

    /// Serves the session until the client terminates it or the gateway
    /// drains (§4.9 R10: an idle session is closed with `57P01`).
    pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        conn: &mut S,
        reader: &mut MsgReader,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> io::Result<()> {
        let mut batch: Vec<u8> = self.core.buffer();
        let observer = self.core.shared.hooks.observer.clone();
        if let Some(o) = &observer {
            o.session_opened(&self.client);
        }
        // When the batch being collected began, for its latency.
        let mut batch_started: Option<std::time::Instant> = None;
        let result = loop {
            reader.trim();
            let idle = self.txn.status == b'I' && batch.is_empty();
            let listening = self.listener.is_some();
            let next = tokio::select! {
                // A notification while the session waits (B4.3).
                note = async {
                    match self.listener.as_mut() {
                        Some(l) => l.rx.recv().await,
                        None => std::future::pending().await,
                    }
                }, if idle && listening => {
                    if let Some(note) = note {
                        crate::notify::message(&mut self.out, &note);
                        self.flush(conn).await?;
                    }
                    continue;
                }
                r = reader.next(conn) => r.map(|(t, m)| (t, m.to_vec())),
                _ = shutdown.changed(), if self.txn.status == b'I' && batch.is_empty() => {
                    pg::error(&mut self.out, "FATAL", "57P01", "terminating connection: the gateway is shutting down");
                    let _ = self.flush(conn).await;
                    break Ok(());
                }
            };
            let (t, msg) = match next {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break Ok(()),
                Err(e) => {
                    pg::error(&mut self.out, "FATAL", "08P01", &e.to_string());
                    let _ = self.flush(conn).await;
                    break Err(e);
                }
            };
            self.core.counters.borrow_mut().bytes_in += msg.len() as u64;
            if self.discard_until_sync {
                if t == b'S' {
                    self.discard_until_sync = false;
                    let status = self.txn.status;
                    pg::ready(&mut self.out, status);
                    self.flush(conn).await?;
                }
                continue;
            }
            let flow = match t {
                b'Q' => {
                    self.core.counters.borrow_mut().statements += 1;
                    let r = match &observer {
                        None => {
                            let r = self.query(conn, &msg).await;
                            self.flush(conn).await?;
                            r
                        }
                        Some(o) => {
                            let start = (std::time::Instant::now(), self.out_total);
                            self.stmt_error = None;
                            let meter = std::rc::Rc::new(crate::meter::Meter::default());
                            let r = crate::meter::scoped(meter.clone(), self.query(conn, &msg)).await;
                            self.flush(conn).await?;
                            o.statement(&self.client, &self.statement_stats(start, msg.len() as u64, &meter));
                            r
                        }
                    };
                    r?
                }
                b'P' | b'B' | b'D' | b'E' | b'C' => {
                    if batch.is_empty() {
                        batch_started = Some(std::time::Instant::now());
                    }
                    batch.extend_from_slice(&msg);
                    if batch.len() > self.core.shared.cfg.max_batch_bytes {
                        pg::error(&mut self.out, "FATAL", "54000", "pipelined batch without Sync exceeds max_batch_bytes");
                        let _ = self.flush(conn).await;
                        break Ok(());
                    }
                    Outcome::Done { error: false }
                }
                b'S' | b'H' => {
                    batch.extend_from_slice(&msg);
                    self.core.counters.borrow_mut().statements += 1;
                    let b = std::mem::take(&mut batch);
                    let r = match &observer {
                        None => {
                            let r = self.extended(conn, &b, t == b'S').await;
                            self.flush(conn).await?;
                            r
                        }
                        Some(o) => {
                            let start = (batch_started.take().unwrap_or_else(std::time::Instant::now), self.out_total);
                            self.stmt_error = None;
                            let meter = std::rc::Rc::new(crate::meter::Meter::default());
                            let r = crate::meter::scoped(meter.clone(), self.extended(conn, &b, t == b'S')).await;
                            self.flush(conn).await?;
                            o.statement(&self.client, &self.statement_stats(start, b.len() as u64, &meter));
                            r
                        }
                    };
                    batch = b;
                    batch.clear();
                    r?
                }
                b'd' | b'c' | b'f' => {
                    let r = self.copy_data(conn, t, &msg).await;
                    self.flush(conn).await?;
                    r?
                }
                b'X' => break Ok(()),
                b'F' => {
                    self.error_ready("0A000", "the function-call protocol is not supported through the gateway");
                    self.flush(conn).await?;
                    Outcome::Done { error: true }
                }
                other => {
                    pg::error(&mut self.out, "FATAL", "08P01", &format!("unexpected message type {:?}", other as char));
                    let _ = self.flush(conn).await;
                    break Ok(());
                }
            };
            if let Outcome::Close = flow {
                break Ok(());
            }
            // A retry that found no loop to take it: answered, never left
            // for the client to wait on.
            if let Outcome::Retry = flow {
                self.error_ready("57P03", "the statement was refused while the cluster's map changes; try again");
                self.flush(conn).await?;
            }
            if self.core.shared.draining.load(std::sync::atomic::Ordering::Relaxed) && self.txn.status == b'I' && batch.is_empty() {
                pg::error(&mut self.out, "FATAL", "57P01", "terminating connection: the gateway is shutting down");
                let _ = self.flush(conn).await;
                break Ok(());
            }
        };
        self.core.give_back(batch);
        self.close_all().await;
        if let Some(o) = &observer {
            o.session_closed(&self.client);
        }
        result
    }

    /// One observed statement's measures: from `start` (when it began, and
    /// the bytes written to the client by then), `bytes_in` read, `meter`.
    fn statement_stats(&self, start: (std::time::Instant, u64), bytes_in: u64, meter: &crate::meter::Meter) -> crate::hooks::StatementStats {
        crate::hooks::StatementStats {
            latency_us: start.0.elapsed().as_micros() as u64,
            sqlstate: self.stmt_error.clone(),
            client_bytes_in: bytes_in,
            client_bytes_out: self.out_total - start.1,
            shard_calls: meter.calls.get(),
            shard_bytes_out_plain: meter.out_plain.get(),
            shard_bytes_out_wire: meter.out_wire.get(),
            shard_bytes_in_plain: meter.in_plain.get(),
            shard_bytes_in_wire: meter.in_wire.get(),
        }
    }

    /// Closes every virtual session (best effort: a shard also closes a
    /// session whose connection is lost).
    pub(crate) async fn close_all(&mut self) {
        for (shard, vs) in self.vs.drain() {
            if let Ok(link) = self.core.link(shard) {
                let _ = link.call(&self.core, &Request::CloseSession { session: vs.id }.encode()).await;
            }
        }
    }

    // ------------------------------------------------------------ simple

    async fn query<S: AsyncWrite + Unpin>(&mut self, conn: &mut S, msg: &[u8]) -> io::Result<Outcome> {
        let Some(text) = pg::parse_query(msg) else {
            self.error_ready("08P01", "malformed Query message");
            return Ok(Outcome::Done { error: true });
        };
        let text = text.to_string();
        // A statement an observer answers itself (`SHOW CLIENT STATS`).
        if let Some(o) = self.core.shared.hooks.observer.clone() {
            let client = self.client.clone();
            if let Some(answer) = o.answer(&client, &text) {
                match answer.await {
                    Ok((columns, rows)) => {
                        let names: Vec<&str> = columns.iter().map(String::as_str).collect();
                        pg::row_description(&mut self.out, &names);
                        for row in &rows {
                            let cells: Vec<Option<&str>> = row.iter().map(|c| c.as_deref()).collect();
                            pg::data_row(&mut self.out, &cells);
                        }
                        pg::command_complete(&mut self.out, &format!("SHOW {}", rows.len()));
                        let status = self.txn.status;
                        pg::ready(&mut self.out, status);
                        return Ok(Outcome::Done { error: false });
                    }
                    Err(e) => {
                        let (code, msg) = match e.split_once(": ") {
                            Some((c, m)) if c.len() == 5 => (c.to_string(), m.to_string()),
                            _ => ("XX000".to_string(), e),
                        };
                        self.error_ready(&code, &msg);
                        return Ok(Outcome::Done { error: true });
                    }
                }
            }
        }
        // A SHOW about tables and storage, answered for every shard.
        if let Some(outcome) = self.cluster_show(conn, &text).await? {
            return Ok(outcome);
        }
        match local_of(&text) {
            Local::ShowGateway => {
                let rows = crate::admin::show_gateway(&self.core.shared);
                pg::row_description(&mut self.out, &["name", "value"]);
                for (k, v) in &rows {
                    pg::data_row(&mut self.out, &[Some(k), Some(v)]);
                }
                pg::command_complete(&mut self.out, &format!("SHOW {}", rows.len()));
                pg::ready(&mut self.out, self.txn.status);
                return Ok(Outcome::Done { error: false });
            }
            Local::ShowRouteKey => {
                let v = self.local_key.as_ref().or(self.route_key.as_ref()).map(|k| k.show()).unwrap_or_default();
                self.show_one("rdb.route_key", &v);
                return Ok(Outcome::Done { error: false });
            }
            Local::ShowSetting(which) => {
                let v = match which {
                    "unrouted" => match self.unrouted {
                        Unrouted::Refuse => "refuse",
                        Unrouted::FanOut => "fan_out",
                    },
                    _ => match self.unrouted_writes {
                        UnroutedWrites::Refuse => "refuse",
                        UnroutedWrites::EachShard => "each_shard",
                    },
                };
                self.show_one(&format!("rdb.{which}"), v);
                return Ok(Outcome::Done { error: false });
            }
            Local::SetRouteKey { local, value } => {
                if self.login.pinned.is_some() {
                    self.error_ready("42501", "this role is locked to its shard: rdb.route_key cannot be set");
                    return Ok(Outcome::Done { error: true });
                }
                let key = match value.map(|v| OwnedKey::parse_setting(&v)).transpose() {
                    Ok(k) => k,
                    Err(e) => {
                        self.error_ready("22023", &e);
                        return Ok(Outcome::Done { error: true });
                    }
                };
                if local {
                    if self.txn.status == b'I' {
                        pg::notice(&mut self.out, "SET LOCAL can only be used in transaction blocks");
                    } else if let (Some(s), Some(k)) = (self.txn.shard, &key)
                        && self.core.map().shard_of_block(k.block()) != s
                    {
                        self.core.counters.borrow_mut().refused_cross_shard += 1;
                        self.error_ready("0A000", "this transaction runs on another shard: a transaction cannot span shards");
                        return Ok(Outcome::Done { error: true });
                    } else {
                        self.local_key = key;
                    }
                } else {
                    self.route_key = key;
                }
                pg::command_complete(&mut self.out, if value_is_reset(&text) { "RESET" } else { "SET" });
                pg::ready(&mut self.out, self.txn.status);
                return Ok(Outcome::Done { error: false });
            }
            Local::SetUnrouted(v) => {
                let v = v.trim_matches('\'').to_ascii_lowercase();
                self.unrouted = match v.as_str() {
                    "refuse" | "default" => Unrouted::Refuse,
                    "fan_out" if self.login.pinned.is_none() => Unrouted::FanOut,
                    "fan_out" => {
                        self.error_ready("42501", "this role is locked to its shard: it never fans out");
                        return Ok(Outcome::Done { error: true });
                    }
                    _ => {
                        self.error_ready("22023", "rdb.unrouted is refuse or fan_out");
                        return Ok(Outcome::Done { error: true });
                    }
                };
                pg::command_complete(&mut self.out, "SET");
                pg::ready(&mut self.out, self.txn.status);
                return Ok(Outcome::Done { error: false });
            }
            Local::SetSplitBulkWrites(v) => {
                self.split_bulk_writes = match v.trim_matches('\'').to_ascii_lowercase().as_str() {
                    "on" | "true" | "1" => true,
                    "off" | "false" | "0" | "default" | "" => false,
                    _ => {
                        self.error_ready("22023", "rdb.split_bulk_writes is on or off");
                        return Ok(Outcome::Done { error: true });
                    }
                };
                pg::command_complete(&mut self.out, "SET");
                pg::ready(&mut self.out, self.txn.status);
                return Ok(Outcome::Done { error: false });
            }
            Local::SetConsistentFanout(v) => {
                self.consistent_fanout = match v.trim_matches('\'').to_ascii_lowercase().as_str() {
                    "on" | "true" | "1" => true,
                    "off" | "false" | "0" | "default" | "" => false,
                    _ => {
                        self.error_ready("22023", "rdb.consistent_fanout is on or off");
                        return Ok(Outcome::Done { error: true });
                    }
                };
                pg::command_complete(&mut self.out, "SET");
                pg::ready(&mut self.out, self.txn.status);
                return Ok(Outcome::Done { error: false });
            }
            Local::SetUnroutedWrites(v) => {
                let v = v.trim_matches('\'').to_ascii_lowercase();
                self.unrouted_writes = match v.as_str() {
                    "refuse" | "default" => UnroutedWrites::Refuse,
                    "each_shard" if self.login.pinned.is_none() => UnroutedWrites::EachShard,
                    _ => {
                        self.error_ready("22023", "rdb.unrouted_writes is refuse or each_shard (not for a role locked to its shard)");
                        return Ok(Outcome::Done { error: true });
                    }
                };
                pg::command_complete(&mut self.out, "SET");
                pg::ready(&mut self.out, self.txn.status);
                return Ok(Outcome::Done { error: false });
            }
            Local::Begin if self.txn.status == b'I' && self.target_known().is_none() => {
                // Recorded: the first routed statement opens the backend
                // transaction (B1a.7).
                self.txn.pending.clear();
                self.txn.pending.extend_from_slice(msg);
                self.txn.pending_queries = 1;
                self.txn.opening.clear();
                self.txn.opening.extend_from_slice(msg);
                self.txn.opening_queries = 1;
                // Under two-phase commit every part carries the transaction's
                // one id, so the deadlock detector can follow a cycle of waits
                // across shards (B4.2).
                if self.core.shared.cfg.two_phase {
                    let sql = format!("SET LOCAL rdb.global_txn = '{}'", next_global_txn(&self.core.shared.cfg.gateway_name));
                    let mut q = Vec::with_capacity(sql.len() + 6);
                    q.push(b'Q');
                    q.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
                    q.extend_from_slice(sql.as_bytes());
                    q.push(0);
                    self.txn.pending.extend_from_slice(&q);
                    self.txn.pending_queries += 1;
                    self.txn.opening.extend_from_slice(&q);
                    self.txn.opening_queries += 1;
                }
                self.txn.status = b'T';
                pg::command_complete(&mut self.out, "BEGIN");
                pg::ready(&mut self.out, b'T');
                return Ok(Outcome::Done { error: false });
            }
            Local::End { commit } if self.txn.status != b'I' && self.txn.shard.is_none() => {
                let tag = if commit && self.txn.status == b'T' { "COMMIT" } else { "ROLLBACK" };
                self.end_txn();
                pg::command_complete(&mut self.out, tag);
                pg::ready(&mut self.out, b'I');
                return Ok(Outcome::Done { error: false });
            }
            Local::End { commit } if self.core.shared.cfg.two_phase && self.txn.shards.len() > 1 => {
                return Ok(self.end_two_phase(commit && self.txn.status == b'T').await);
            }
            Local::End { .. } if self.core.shared.cfg.two_phase && self.txn.shards.len() == 1 => {
                let shard = self.txn.shards[0];
                return self.run_on(conn, shard, msg, Vec::new(), None).await;
            }
            Local::SetLocal if self.txn.status == b'T' && self.txn.shard.is_none() => {
                self.txn.pending.extend_from_slice(msg);
                self.txn.pending_queries += 1;
                self.txn.opening.extend_from_slice(msg);
                self.txn.opening_queries += 1;
                pg::command_complete(&mut self.out, "SET");
                pg::ready(&mut self.out, b'T');
                return Ok(Outcome::Done { error: false });
            }
            Local::Discard if self.txn.status == b'I' && self.listener.is_some() => {
                // DISCARD ALL is UNLISTEN * too.
                self.listener = None;
                return self.everywhere(conn, msg, true).await;
            }
            Local::SessionSet | Local::Discard if self.txn.status == b'I' => {
                return self.everywhere(conn, msg, matches!(local_of(&text), Local::Discard)).await;
            }
            Local::Refused(what) => {
                self.error_ready("0A000", &format!("{what} is not supported through the gateway; connect to a shard directly"));
                return Ok(Outcome::Done { error: true });
            }
            Local::Listen(channel) => {
                let database = self.login.database.clone();
                self.listener.get_or_insert_with(|| crate::notify::Listener::new(&database)).listen(&channel);
                pg::command_complete(&mut self.out, "LISTEN");
                let status = self.txn.status;
                pg::ready(&mut self.out, status);
                return Ok(Outcome::Done { error: false });
            }
            Local::Unlisten(channel) => {
                if let Some(l) = self.listener.as_mut() {
                    match channel {
                        Some(c) => l.unlisten(&c),
                        None => l.unlisten_all(),
                    }
                }
                pg::command_complete(&mut self.out, "UNLISTEN");
                let status = self.txn.status;
                pg::ready(&mut self.out, status);
                return Ok(Outcome::Done { error: false });
            }
            _ => {}
        }
        self.route_simple(conn, msg, &text).await
    }

    fn show_one(&mut self, name: &str, v: &str) {
        pg::row_description(&mut self.out, &[name]);
        pg::data_row(&mut self.out, &[Some(v)]);
        pg::command_complete(&mut self.out, "SHOW");
        pg::ready(&mut self.out, self.txn.status);
    }

    pub(crate) fn end_txn(&mut self) {
        self.txn.status = b'I';
        self.txn.shard = None;
        self.txn.pending.clear();
        self.txn.pending_queries = 0;
        self.txn.shards.clear();
        self.txn.opening.clear();
        self.txn.opening_queries = 0;
        self.local_key = None;
    }

    /// The shard a statement goes to without looking at it: pinned, the
    /// transaction's, an explicit key's, or the configured fixed shard.
    fn target_known(&self) -> Option<u32> {
        if let Some(p) = self.login.pinned {
            return Some(p);
        }
        // A transaction's statements route on their own; without two-phase
        // commit, `within_block` then keeps its writes on its first shard.
        let map = self.core.map();
        if let Some(k) = self.local_key.as_ref().or(self.route_key.as_ref()) {
            return Some(map.shard_of_block(k.block()));
        }
        self.core.shared.cfg.fixed_shard
    }

    /// A session-level SET (or DISCARD ALL): run on the home shard and on
    /// every open virtual session, recorded for those that open later.
    async fn everywhere<S: AsyncWrite + Unpin>(&mut self, conn: &mut S, msg: &[u8], discard: bool) -> io::Result<Outcome> {
        let map = self.core.map();
        let first = self.login.pinned.or(self.core.shared.cfg.fixed_shard).unwrap_or_else(|| self.core.shared.home(&map));
        let outcome = self.run_on(conn, first, msg, Vec::new(), None).await?;
        let failed = matches!(outcome, Outcome::Done { error: true });
        if !failed && matches!(outcome, Outcome::Done { .. }) {
            if discard {
                self.session_sets.clear();
            } else {
                self.session_sets.extend_from_slice(msg);
            }
            let others: Vec<u32> = self.vs.keys().copied().filter(|&s| s != first).collect();
            for s in others {
                if let Err(e) = self.send_discarding(s, msg).await {
                    log::warn!("a session SET did not reach shard {s}: {e}");
                    // Its virtual session no longer matches the client's:
                    // close it, and the next one opens with the replay.
                    if let Some(vs) = self.vs.remove(&s)
                        && let Ok(link) = self.core.link(s)
                    {
                        let _ = link.call(&self.core, &Request::CloseSession { session: vs.id }.encode()).await;
                    }
                }
            }
        }
        Ok(outcome)
    }

    async fn route_simple<S: AsyncWrite + Unpin>(&mut self, conn: &mut S, msg: &[u8], text: &str) -> io::Result<Outcome> {
        let retries = self.core.shared.cfg.retries;
        // A COPY into a reference table routes as the home shard's (its
        // statement names no key) but its rows belong on every shard.
        if text.trim_start().get(..5).is_some_and(|w| w.eq_ignore_ascii_case("COPY "))
            && let Some(outcome) = self.copy_split_start(conn, text).await?
        {
            return Ok(outcome);
        }
        for attempt in 0..=retries {
            self.core.refresh_if_stale().await;
            let map = self.core.map();
            let (target, tkey) = self.decide_simple(&map, text).await;
            let outcome = match target {
                Target::Refuse { sqlstate, message } => {
                    // A multi-row INSERT is split by the shard each row
                    // goes to; so is a COPY's every row.
                    if sqlstate == "0A000"
                        && let Some(outcome) = self.split_insert(conn, msg, text).await?
                    {
                        return Ok(outcome);
                    }
                    if sqlstate == "0A000"
                        && let Some(outcome) = self.copy_split_start(conn, text).await?
                    {
                        return Ok(outcome);
                    }
                    if sqlstate == "0A000" {
                        self.core.counters.borrow_mut().refused_no_key += 1;
                    }
                    self.error_ready(sqlstate, &message);
                    return Ok(Outcome::Done { error: true });
                }
                // Through the retry loop below, as a routed statement: a part
                // refused before effect (a moving range, a shard a map epoch
                // behind) comes back as Retry, which must never reach the
                // client unanswered.
                Target::FanOut { merge, writes } => self.fan_out(conn, msg, merge, writes).await?,
                Target::Gather { tables } => self.gather_run(conn, text, &tables).await?,
                Target::Shard(s) => {
                    self.core.counters.borrow_mut().routed += 1;
                    self.run_on(conn, s, msg, Vec::new(), tkey.as_ref().map(|t| (Source::Shape, t.as_str()))).await?
                }
            };
            match outcome {
                Outcome::Retry if attempt < retries => {
                    self.core.counters.borrow_mut().retries += 1;
                    continue;
                }
                Outcome::Retry => {
                    self.error_ready("57P03", "the statement was refused for a moving range too many times; try again");
                    return Ok(Outcome::Done { error: true });
                }
                o => return Ok(o),
            }
        }
        unreachable!("the loop returns on its last attempt")
    }

    /// The target of a simple query, and the template text it was decided
    /// by (for refutation).
    pub(crate) async fn decide_simple(&mut self, map: &ShardMap, text: &str) -> (Target, Option<String>) {
        if let Some(s) = self.target_known() {
            return (Target::Shard(s), None);
        }
        let home = self.core.shared.home(map);
        let (shape, slots) = match rasterizeddb_router::literals::shape(text) {
            Some(sh) => (sh.text, sh.slots),
            None => (text.to_string(), Vec::new()),
        };
        let template = match route::template(&self.core, &self.login.database, Source::Shape, &shape, home).await {
            Ok(t) => t,
            Err(e) => {
                return (
                    Target::Refuse { sqlstate: "08006", message: format!("the statement could not be classified: {e}") },
                    None,
                );
            }
        };
        let target = self.within_block_target(self.decided(map, template, &slots, home), text);
        (target, Some(shape))
    }

    /// Without two-phase commit a block runs on its first shard. A plain
    /// read whose key or tables are elsewhere runs there, outside the block,
    /// as READ COMMITTED allows (sharding.md §3.2): a table that lives only
    /// on the home shard read on the block's shard would read as empty. A
    /// statement that names no table (SAVEPOINT, `SELECT 1`) stays with the
    /// block; a write or a locking read reaching another shard is refused.
    fn within_block(&self, shard: u32, text: &str) -> Result<u32, (&'static str, String)> {
        if self.txn.status == b'I' || self.core.shared.cfg.two_phase || self.login.pinned.is_some() {
            return Ok(shard);
        }
        let Some(own) = self.txn.shard else { return Ok(shard) };
        if shard == own || !names_a_table(text) {
            return Ok(own);
        }
        if is_plain_read(text) {
            return Ok(shard);
        }
        Err((
            "0A000",
            format!(
                "this transaction writes on shard {own}, and this statement writes (or locks rows) on shard {shard}: a transaction over two shards needs the gateway's two_phase = on"
            ),
        ))
    }

    fn within_block_target(&self, target: Target, text: &str) -> Target {
        match target {
            Target::Shard(s) => match self.within_block(s, text) {
                Ok(s) => Target::Shard(s),
                Err((sqlstate, message)) => Target::Refuse { sqlstate, message },
            },
            other => other,
        }
    }

    /// The target a template gives, before the block's rule.
    fn decided(&self, map: &ShardMap, template: RouteTemplate, slots: &[rasterizeddb_router::literals::Literal], home: u32) -> Target {
        match template {
            RouteTemplate::Slot { index, family } => match slots.get(index as usize).and_then(|l| route::literal_block(l, family)) {
                Some(b) => Target::Shard(map.shard_of_block(b)),
                None => needs_key(),
            },
            RouteTemplate::Param { .. } => needs_key(),
            RouteTemplate::Fixed { block } => Target::Shard(map.shard_of_block(block)),
            RouteTemplate::Home => Target::Shard(home),
            RouteTemplate::FanOut { merge, writes } => self.unrouted_target(merge, writes),
            RouteTemplate::NeedsKey => needs_key(),
            RouteTemplate::Gather { tables } => {
                if self.core.shared.cfg.gather && self.login.pinned.is_none() {
                    Target::Gather { tables }
                } else {
                    needs_key()
                }
            }
        }
    }

    /// B1b: a statement whose shape can fan out, decided by `unrouted`.
    fn unrouted_target(&self, merge: Merge, writes: bool) -> Target {
        if !cfg!(feature = "fanout") {
            return needs_key();
        }
        if self.login.pinned.is_some() {
            return needs_key();
        }
        let two_phase = self.core.shared.cfg.two_phase;
        // Inside a transaction a fan-out joins every shard to it, which only
        // two-phase commit can end; an aggregate the home shard coordinates
        // joins only the home shard (`fan_out` refuses it once the
        // transaction ran elsewhere).
        if self.txn.status != b'I' && !two_phase && merge != Merge::Coordinator {
            return Target::Refuse {
                sqlstate: "0A000",
                message: "a statement without a route key cannot fan out inside a transaction block unless the gateway runs two_phase = on".into(),
            };
        }
        // A reference table's write is atomic under two-phase commit
        // (B5.1), so it needs no each_shard opt-in; nor does a sharded
        // table's, which changes every shard's rows or none there, once
        // fan-out is allowed at all.
        if writes && two_phase && (merge == Merge::Any || matches!(self.unrouted, Unrouted::FanOut)) {
            return Target::FanOut { merge, writes: true };
        }
        if writes {
            return match self.unrouted_writes {
                UnroutedWrites::EachShard => Target::FanOut { merge, writes: true },
                UnroutedWrites::Refuse => needs_key(),
            };
        }
        match self.unrouted {
            Unrouted::FanOut => Target::FanOut { merge, writes: false },
            Unrouted::Refuse => needs_key(),
        }
    }

    // ------------------------------------------------------------ shards

    /// The virtual session on `shard`, opened (and the session SETs
    /// replayed) on first use.
    pub(crate) async fn ensure_vs(&mut self, shard: u32) -> Result<u64, (String, String)> {
        let link = self.core.link(shard).map_err(|e| ("08006".to_string(), e))?;
        if let Some(v) = self.vs.get(&shard) {
            // Opened on a connection the link has since replaced: the shard
            // dropped it with that connection. Outside a transaction a new
            // one stands in; inside one the transaction's work is gone, and
            // the shard's "session is gone" says so.
            if v.connection == link.connection.get() || self.txn.status != b'I' {
                return Ok(v.id);
            }
            self.vs.remove(&shard);
        }
        let id = self.core.session_id();
        let req = Request::OpenSession {
            session: id,
            database: self.login.database.clone(),
            role: self.login.role.clone(),
            params: self.login.params.clone(),
            gateway: self.core.shared.cfg.gateway_name.clone(),
            pinned: self.login.pinned.is_some(),
        }
        .encode();
        match link.call(&self.core, &req).await {
            Ok(Response::SessionOpened { backend_key, .. }) => {
                self.vs.insert(shard, VSession { id, connection: link.connection.get(), parsed: HashMap::new() });
                if let Ok(mut t) = self.login.cancel.lock() {
                    t.push((shard, backend_key));
                }
                self.core.counters.borrow_mut().virtual_sessions += 1;
                if !self.session_sets.is_empty() {
                    let sets = self.session_sets.clone();
                    if let Err(e) = self.send_discarding(shard, &sets).await {
                        log::warn!("session SETs could not be replayed on shard {shard}: {e}");
                    }
                }
                Ok(id)
            }
            Ok(Response::Refused(r)) => Err((r.sqlstate_str().to_string(), r.message)),
            Ok(other) => Err(("XX000".into(), format!("unexpected reply kind {:#04x} to OpenSession", other.kind()))),
            Err(CallError::Down(e)) => {
                self.core.counters.borrow_mut().down_08006 += 1;
                Err(("08006".into(), e))
            }
            Err(CallError::Lost(e)) | Err(CallError::Bad(e)) => Err(("08006".into(), e)),
        }
    }

    /// Each `(shard, messages)` sent at once on this session's virtual
    /// session there: each shard's first error (`Ok(Some)`), none
    /// (`Ok(None)`), or why it could not be asked (`Err`), in the order given.
    #[allow(clippy::type_complexity)]
    pub(crate) async fn send_each(&mut self, work: &[(u32, Vec<u8>)]) -> Vec<(u32, Result<Option<(String, String)>, String>)> {
        enum Asked {
            Sent(tokio::task::JoinHandle<Result<Vec<u8>, (String, String)>>),
            Not(String),
        }
        let epoch = self.core.map().epoch();
        let mut asked = Vec::with_capacity(work.len());
        for (s, msgs) in work {
            match self.ensure_vs(*s).await {
                Ok(id) => {
                    let mut req = Vec::new();
                    Request::encode_pg(&mut req, id, epoch, msgs);
                    let call = crate::fanout::collect_pg(self.core.clone(), *s, id, req);
                    asked.push((*s, Asked::Sent(tokio::task::spawn_local(crate::meter::carry(call)))));
                }
                Err((c, m)) => asked.push((*s, Asked::Not(format!("{c}: {m}")))),
            }
        }
        let mut out = Vec::with_capacity(asked.len());
        for (s, a) in asked {
            let r = match a {
                Asked::Not(e) => Err(e),
                Asked::Sent(h) => match h.await {
                    Ok(Ok(bytes)) => Ok(pg::messages(&bytes)
                        .find(|(t, _)| *t == b'E')
                        .map(|(_, m)| (pg::error_sqlstate(m).unwrap_or("XX000").to_string(), pg::error_message(m).unwrap_or_default()))),
                    Ok(Err((c, m))) => Err(format!("{c}: {m}")),
                    Err(_) => Err("the call panicked".to_string()),
                },
            };
            out.push((s, r));
        }
        out
    }

    /// [`Self::send_each`] with one simple query per shard.
    #[allow(clippy::type_complexity)]
    pub(crate) async fn quiet_each(&mut self, work: &[(u32, String)]) -> Vec<(u32, Result<Option<(String, String)>, String>)> {
        let msgs: Vec<(u32, Vec<u8>)> = work
            .iter()
            .map(|(s, sql)| {
                let mut q = vec![b'Q'];
                q.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
                q.extend_from_slice(sql.as_bytes());
                q.push(0);
                (*s, q)
            })
            .collect();
        self.send_each(&msgs).await
    }


    /// Sends messages on `shard`'s virtual session and drops the reply.
    async fn send_discarding(&mut self, shard: u32, msgs: &[u8]) -> Result<(), String> {
        let id = self.vs.get(&shard).map(|v| v.id).ok_or("no virtual session")?;
        let link = self.core.link(shard)?;
        let mut req = Vec::new();
        Request::encode_pg(&mut req, id, self.core.map().epoch(), msgs);
        let mut resp = link.call(&self.core, &req).await.map_err(|e| format!("{e:?}"))?;
        loop {
            match resp {
                Response::PgReply { more: true, .. } => {
                    resp = link
                        .call(&self.core, &Request::Continue { session: id }.encode())
                        .await
                        .map_err(|e| format!("{e:?}"))?;
                }
                Response::PgReply { bytes, .. } => {
                    return match pg::messages(&bytes).find(|(t, _)| *t == b'E') {
                        Some((_, m)) => Err(format!("error {}", pg::error_sqlstate(m).unwrap_or("?"))),
                        None => Ok(()),
                    };
                }
                Response::Refused(r) => return Err(format!("{} {}", r.sqlstate_str(), r.message)),
                other => return Err(format!("unexpected reply kind {:#04x}", other.kind())),
            }
        }
    }

    /// Runs `msgs` on `shard` and relays the reply to the client.
    ///
    /// `needs`: statement names the batch uses without parsing them first,
    /// re-Parsed lazily here when this shard's virtual session does not
    /// have the current generation (B1a.7). `tkey`: the template the
    /// target came from, dropped if the shard refutes it.
    pub(crate) async fn run_on<S: AsyncWrite + Unpin>(
        &mut self,
        conn: &mut S,
        shard: u32,
        msgs: &[u8],
        needs: Vec<(String, bool)>,
        tkey: Option<(Source, &str)>,
    ) -> io::Result<Outcome> {
        let vs_id = match self.ensure_vs(shard).await {
            Ok(id) => id,
            Err((code, msg)) => {
                if code.as_bytes() == rshard::MOVED || code.as_bytes() == rshard::NOT_OWNED {
                    let _ = self.core.fetch_map(shard).await;
                }
                self.error_ready(&code, &format!("shard {shard}: {msg}"));
                return Ok(Outcome::Done { error: true });
            }
        };
        let link = match self.core.link(shard) {
            Ok(l) => l,
            Err(e) => {
                self.error_ready("08006", &e);
                return Ok(Outcome::Done { error: true });
            }
        };

        // The payload: a pending BEGIN, lazy re-Parses, then the client's.
        let mut payload = self.core.buffer();
        let mut filter = ReplyFilter::none();
        let first_of_txn = self.txn.status != b'I' && self.txn.shard.is_none();
        if first_of_txn && !self.txn.pending.is_empty() {
            payload.extend_from_slice(&self.txn.pending);
            filter.skip_rfq = self.txn.pending_queries;
        } else if self.core.shared.cfg.two_phase
            && self.txn.status != b'I'
            && !self.txn.shards.contains(&shard)
            && !self.txn.opening.is_empty()
        {
            // A shard the transaction reaches for the first time opens its
            // own part of it, under the multi-shard lock timeout: a cycle of
            // waits across shards is seen whole by no shard's detector.
            payload.extend_from_slice(&self.txn.opening);
            filter.skip_rfq = self.txn.opening_queries;
            let ms = self.core.shared.cfg.two_phase_lock_timeout_ms;
            if ms > 0 {
                let sql = format!("SET LOCAL lock_timeout = {ms}");
                payload.push(b'Q');
                payload.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
                payload.extend_from_slice(sql.as_bytes());
                payload.push(0);
                filter.skip_rfq += 1;
            }
        }
        let mut parsed_now: Vec<(String, u64)> = Vec::new();
        if let Some(vs) = self.vs.get(&shard) {
            for (name, client_parses_it) in &needs {
                if *client_parses_it {
                    // The client's own Parse of a name this backend still
                    // holds from before a Close the gateway did not send
                    // there: close it first.
                    if !name.is_empty() && vs.parsed.contains_key(name) && !self.statements.contains_key(name) {
                        push_close(&mut payload, name);
                        filter.skip.push_back(b'3');
                    }
                    continue;
                }
                let Some(stmt) = self.statements.get(name) else { continue };
                if vs.parsed.get(name) == Some(&stmt.generation) {
                    continue;
                }
                if !name.is_empty() && vs.parsed.contains_key(name) {
                    push_close(&mut payload, name);
                    filter.skip.push_back(b'3');
                }
                payload.extend_from_slice(&stmt.parse);
                filter.skip.push_back(b'1');
                parsed_now.push((name.clone(), stmt.generation));
            }
        }
        payload.extend_from_slice(msgs);
        let mut req = self.core.buffer();
        Request::encode_pg(&mut req, vs_id, self.core.map().epoch(), &payload);
        self.core.give_back(payload);
        let first = link.call(&self.core, &req).await;
        self.core.give_back(req);

        let mut resp = match first {
            Ok(r) => r,
            Err(CallError::Down(e)) => {
                self.core.counters.borrow_mut().down_08006 += 1;
                self.vs.remove(&shard);
                self.error_ready("08006", &e);
                return Ok(Outcome::Done { error: true });
            }
            Err(CallError::Lost(e)) | Err(CallError::Bad(e)) => {
                self.core.counters.borrow_mut().lost_08007 += 1;
                self.vs.remove(&shard);
                if self.txn.shard == Some(shard) {
                    pg::error(&mut self.out, "FATAL", "08007", &format!("the connection to shard {shard} was lost inside a transaction; its outcome is unknown: {e}"));
                    return Ok(Outcome::Close);
                }
                self.error_ready("08007", &format!("the connection to shard {shard} was lost after the statement was sent; its outcome is unknown: {e}"));
                return Ok(Outcome::Done { error: true });
            }
        };

        // A statement refused because its range is fenced for a move's last
        // round wrote nothing (the fence refuses before the write): outside
        // a transaction it is retried after the round, as the error asks.
        if let Response::PgReply { more: false, bytes, .. } = &resp
            && self.txn.status == b'I'
            && self.txn.shard.is_none()
            && self.login.pinned.is_none()
            && fenced_for_a_move(bytes)
        {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            return Ok(Outcome::Retry);
        }
        let mut error = false;
        let mut last_type = 0u8;
        let mut refuted = false;
        // A read run outside the block, on another shard (`within_block`):
        // its ReadyForQuery says the block's status, and it changes none of
        // the block's bookkeeping.
        let outside = self.txn.status != b'I' && !self.core.shared.cfg.two_phase && self.txn.shard.is_some_and(|t| t != shard);
        loop {
            match resp {
                Response::PgReply { more, status, bytes, .. } => {
                    for (t, m) in pg::messages(&bytes) {
                        last_type = t;
                        if t == b'E' {
                            error = true;
                            if pg::error_sqlstate(m).is_some_and(|c| c.as_bytes() == rshard::NOT_OWNED) {
                                refuted = true;
                            }
                        }
                    }
                    let before = self.out.len();
                    filter.apply(&bytes, &mut self.out);
                    if outside && !more {
                        let block_status = if error { b'E' } else { self.txn.status };
                        let mut at = before;
                        while at + 5 <= self.out.len() {
                            let len = i32::from_be_bytes(self.out[at + 1..at + 5].try_into().unwrap()) as usize;
                            if self.out[at] == b'Z' && len == 5 {
                                self.out[at + 5] = block_status;
                            }
                            at += 1 + len;
                        }
                    }
                    self.flush(conn).await?;
                    if more {
                        resp = match link.call(&self.core, &Request::Continue { session: vs_id }.encode()).await {
                            Ok(r) => r,
                            Err(e) => {
                                self.core.counters.borrow_mut().lost_08007 += 1;
                                self.vs.remove(&shard);
                                pg::error(&mut self.out, "FATAL", "08007", &format!("shard {shard} was lost mid-reply: {e:?}"));
                                return Ok(Outcome::Close);
                            }
                        };
                        continue;
                    }
                    // Done: the bookkeeping that follows the reply.
                    if let Some(vs) = self.vs.get_mut(&shard) {
                        for (n, g) in parsed_now {
                            vs.parsed.insert(n, g);
                        }
                    }
                    self.copy_shard = (last_type == b'G').then_some(shard);
                    if outside {
                        if error {
                            self.txn.status = b'E';
                        }
                        self.last_shard = Some(shard);
                        return Ok(Outcome::Done { error });
                    }
                    match status {
                        b'T' | b'E' => {
                            self.txn.shard.get_or_insert(shard);
                            if !self.txn.shards.contains(&shard) {
                                self.txn.shards.push(shard);
                            }
                            self.txn.pending.clear();
                            self.txn.pending_queries = 0;
                            self.txn.status = status;
                        }
                        b'I' => self.end_txn(),
                        _ => {}
                    }
                    self.last_shard = Some(shard);
                    if refuted && let Some((src, text)) = tkey {
                        route::refute(&self.core, &self.login.database, src, text);
                    }
                    return Ok(Outcome::Done { error });
                }
                Response::Refused(r) => return Ok(self.refused(shard, r, tkey).await),
                other => {
                    self.error_ready("XX000", &format!("shard {shard} answered with reply kind {:#04x}", other.kind()));
                    return Ok(Outcome::Done { error: true });
                }
            }
        }
    }

    /// Runs `sql` as one simple query on this session's virtual session on
    /// `shard`, for the gateway's own bookkeeping (nothing reaches the
    /// client): the final ReadyForQuery status, and the first error.
    pub(crate) async fn quiet_query(&mut self, shard: u32, sql: &str) -> Result<(u8, Option<(String, String)>), String> {
        self.tagged_query(shard, sql).await.map(|(status, error, _)| (status, error))
    }

    /// The first column of every row `sql` returns on `shard`.
    pub(crate) async fn first_column(&mut self, shard: u32, sql: &str) -> Result<Vec<Option<String>>, String> {
        let vs_id = self.ensure_vs(shard).await.map_err(|(c, m)| format!("{c}: {m}"))?;
        let link = self.core.link(shard)?;
        let mut q = vec![b'Q'];
        q.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
        q.extend_from_slice(sql.as_bytes());
        q.push(0);
        let mut req = Vec::new();
        Request::encode_pg(&mut req, vs_id, self.core.map().epoch(), &q);
        let mut resp = link.call(&self.core, &req).await.map_err(|e| format!("{e:?}"))?;
        let mut out = Vec::new();
        loop {
            match resp {
                Response::PgReply { more, bytes, .. } => {
                    for (t, m) in pg::messages(&bytes) {
                        match t {
                            b'D' => out.push(pg::data_row_first(m).flatten().map(|v| String::from_utf8_lossy(v).into_owned())),
                            b'E' => return Err(pg::error_message(m).unwrap_or_default()),
                            _ => {}
                        }
                    }
                    if more {
                        resp = link.call(&self.core, &Request::Continue { session: vs_id }.encode()).await.map_err(|e| format!("{e:?}"))?;
                        continue;
                    }
                    return Ok(out);
                }
                Response::Refused(r) => return Err(format!("{}: {}", String::from_utf8_lossy(&r.sqlstate), r.message)),
                other => return Err(format!("reply kind {:#04x}", other.kind())),
            }
        }
    }

    /// [`Self::quiet_query`], with the statement's command tag.
    async fn tagged_query(&mut self, shard: u32, sql: &str) -> Result<(u8, Option<(String, String)>, Option<String>), String> {
        let vs_id = self.ensure_vs(shard).await.map_err(|(c, m)| format!("{c}: {m}"))?;
        let link = self.core.link(shard)?;
        let mut q = vec![b'Q'];
        q.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
        q.extend_from_slice(sql.as_bytes());
        q.push(0);
        let mut req = Vec::new();
        Request::encode_pg(&mut req, vs_id, self.core.map().epoch(), &q);
        let mut resp = link.call(&self.core, &req).await.map_err(|e| format!("{e:?}"))?;
        let mut error = None;
        let mut tag = None;
        loop {
            match resp {
                Response::PgReply { more, status, bytes, .. } => {
                    for (t, m) in pg::messages(&bytes) {
                        if t == b'E' && error.is_none() {
                            let code = pg::error_sqlstate(m).unwrap_or("XX000").to_string();
                            error = Some((code, pg::error_message(m).unwrap_or_default()));
                        }
                        if t == b'C' {
                            tag = pg::cstr(pg::body(m)).map(|(c, _)| c.to_string());
                        }
                    }
                    if more {
                        resp = link.call(&self.core, &Request::Continue { session: vs_id }.encode()).await.map_err(|e| format!("{e:?}"))?;
                        continue;
                    }
                    return Ok((status, error, tag));
                }
                Response::Refused(r) => return Err(format!("{}: {}", String::from_utf8_lossy(&r.sqlstate), r.message)),
                other => return Err(format!("reply kind {:#04x}", other.kind())),
            }
        }
    }

    /// Ends a transaction that ran on several shards (sharding-plan.md B5):
    /// a rollback on each; or a two-phase commit. Every shard prepares
    /// under one global id; any failure rolls every part back; once all are
    /// prepared, the decision is recorded on the anchor (home) shard, and
    /// only then is each part committed. A part whose COMMIT PREPARED fails
    /// after the decision is left prepared for `rdb-shard resolve`, which
    /// reads the anchor's decision: the transaction is committed.
    async fn end_two_phase(&mut self, commit: bool) -> Outcome {
        self.end_two_phase_tagged(commit, None).await
    }

    /// [`Self::end_two_phase`], answered with `tag` instead of COMMIT: a
    /// statement the gateway ran as its own two-phase transaction.
    pub(crate) async fn end_two_phase_tagged(&mut self, commit: bool, tag: Option<String>) -> Outcome {
        let shards = self.txn.shards.clone();
        if !commit {
            let work: Vec<(u32, String)> = shards.iter().map(|s| (*s, "ROLLBACK".to_string())).collect();
            let _ = self.quiet_each(&work).await;
            self.end_txn();
            pg::command_complete(&mut self.out, "ROLLBACK");
            pg::ready(&mut self.out, b'I');
            return Outcome::Done { error: false };
        }
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let gid = format!(
            "rdb2pc:{}:{nanos:x}:{}",
            self.core.shared.cfg.gateway_name.replace(['\'', ':'], "_"),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        // Every part prepares at once: one round trip, not one per shard.
        let mut prepared = Vec::new();
        let mut failure: Option<(String, String)> = None;
        let work: Vec<(u32, String)> = shards.iter().map(|s| (*s, format!("PREPARE TRANSACTION '{gid}'"))).collect();
        for (s, r) in self.quiet_each(&work).await {
            match r {
                Ok(None) => prepared.push(s),
                Ok(Some(e)) => {
                    failure.get_or_insert((e.0, format!("shard {s} could not prepare: {}", e.1)));
                }
                Err(e) => {
                    failure.get_or_insert(("08006".into(), format!("shard {s} could not prepare: {e}")));
                }
            }
        }
        let home = self.core.shared.home(&self.core.map());
        // Marked before the decision exists, cleared once every part is
        // told: reads across shards wait for it (B5.2).
        crate::fanout::committing::begin(&gid);
        if failure.is_none() {
            let decided = match self.core.link(home) {
                Ok(link) => link.call(&self.core, &Request::Decide { gid: gid.clone(), commit: true }.encode()).await,
                Err(e) => Err(CallError::Down(e)),
            };
            if !matches!(decided, Ok(Response::Done)) {
                failure = Some(("40001".into(), format!("the commit decision could not be recorded on shard {home}: {decided:?}")));
                // Recorded or not, an abort may still be recorded first.
                if let Ok(link) = self.core.link(home) {
                    let _ = link.call(&self.core, &Request::Decide { gid: gid.clone(), commit: false }.encode()).await;
                }
            }
        }
        if let Some((code, msg)) = failure {
            crate::fanout::committing::end(&gid);
            let work: Vec<(u32, String)> = shards
                .iter()
                .map(|s| (*s, if prepared.contains(s) { format!("ROLLBACK PREPARED '{gid}'") } else { "ROLLBACK".to_string() }))
                .collect();
            let _ = self.quiet_each(&work).await;
            self.end_txn();
            self.error_ready(&code, &msg);
            return Outcome::Done { error: true };
        }
        // The decision is recorded: every part commits at once.
        let work: Vec<(u32, String)> = shards.iter().map(|s| (*s, format!("COMMIT PREPARED '{gid}'"))).collect();
        for (s, r) in self.quiet_each(&work).await {
            if !matches!(r, Ok(None)) {
                log::warn!("transaction {gid} is decided commit but shard {s} did not finish it ({r:?}); rdb-shard resolve completes it");
            }
        }
        crate::fanout::committing::end(&gid);
        self.end_txn();
        pg::command_complete(&mut self.out, tag.as_deref().unwrap_or("COMMIT"));
        pg::ready(&mut self.out, b'I');
        Outcome::Done { error: false }
    }

    /// A shard's refusal: retried when nothing took effect and the
    /// statement may move (autocommit, or a transaction's first statement),
    /// else reported (R2).
    async fn refused(&mut self, shard: u32, r: Refusal, tkey: Option<(Source, &str)>) -> Outcome {
        let movable = self.txn.shard.is_none() && self.login.pinned.is_none();
        if r.sqlstate == rshard::SESSION_GONE {
            self.vs.remove(&shard);
            if movable && r.before_effect {
                return Outcome::Retry;
            }
        }
        if r.before_effect && (r.sqlstate == rshard::NOT_OWNED || r.sqlstate == rshard::MOVED) {
            if r.sqlstate == rshard::NOT_OWNED
                && let Some((src, text)) = tkey
            {
                route::refute(&self.core, &self.login.database, src, text);
            }
            let _ = self.core.fetch_map(shard).await;
            if movable {
                return Outcome::Retry;
            }
        }
        if r.before_effect && r.sqlstate == rshard::FENCED && movable {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            return Outcome::Retry;
        }
        self.core.counters.borrow_mut().refusals_other += 1;
        self.error_ready(r.sqlstate_str(), &format!("shard {shard}: {}", r.message));
        Outcome::Done { error: true }
    }

    // ------------------------------------------------------------ extended

    /// One extended-protocol batch, ending in Sync (or Flush).
    async fn extended<S: AsyncWrite + Unpin>(&mut self, conn: &mut S, batch: &[u8], sync: bool) -> io::Result<Outcome> {
        let retries = self.core.shared.cfg.retries;
        for attempt in 0..=retries {
            self.core.refresh_if_stale().await;
            let map = self.core.map();
            self.pending_fanout = None;
            self.pending_gather = None;
            let (target, needs, tkey) = match self.decide_extended(&map, batch).await {
                Ok(v) => v,
                Err((code, msg)) => {
                    // A join with a table on other shards, gathered.
                    if let Some(tables) = self.pending_gather.take()
                        && sync
                        && let Some(outcome) = self.gather_extended(conn, batch, &tables).await?
                    {
                        return Ok(outcome);
                    }
                    // A statement without a route key, alone in its batch:
                    // fanned out as a simple query would be.
                    if let Some((merge, writes)) = self.pending_fanout.take()
                        && sync
                        && let Some(outcome) = self.fan_out_extended(conn, batch, merge, writes).await?
                    {
                        return Ok(outcome);
                    }
                    // A prepared multi-row INSERT is split by row.
                    if code == "0A000"
                        && sync
                        && let Some(outcome) = self.split_extended(conn, batch).await?
                    {
                        return Ok(outcome);
                    }
                    if code == "0A000" {
                        self.core.counters.borrow_mut().refused_no_key += 1;
                    }
                    pg::error(&mut self.out, "ERROR", code, &msg);
                    if self.txn.status != b'I' {
                        self.txn.status = b'E';
                    }
                    if sync {
                        let s = self.txn.status;
                        pg::ready(&mut self.out, s);
                    } else {
                        self.discard_until_sync = true;
                    }
                    return Ok(Outcome::Done { error: true });
                }
            };
            self.core.counters.borrow_mut().routed += 1;
            let outcome = self.run_on(conn, target, batch, needs, tkey.as_ref().map(|t| (Source::Parse, t.as_str()))).await?;
            if let Outcome::Retry = outcome {
                if attempt < retries {
                    self.core.counters.borrow_mut().retries += 1;
                    continue;
                }
                self.error_ready("57P03", "the statement was refused for a moving range too many times; try again");
                return Ok(Outcome::Done { error: true });
            }
            self.after_extended(batch, target);
            return Ok(outcome);
        }
        unreachable!("the loop returns on its last attempt")
    }

    /// The batch's shard, the statement names it uses without parsing them
    /// (with whether the batch itself parses them later), and the template
    /// text of the Bind that decided it.
    async fn decide_extended(
        &mut self,
        map: &ShardMap,
        batch: &[u8],
    ) -> Result<(u32, Vec<(String, bool)>, Option<String>), (&'static str, String)> {
        // Statement texts as of each point in the batch.
        let mut in_batch: HashMap<String, String> = HashMap::new();
        let mut needs: Vec<(String, bool)> = Vec::new();
        let mut routes: Vec<(u32, String)> = Vec::new();
        let known = self.target_known();
        let home = self.core.shared.home(map);
        for (t, m) in pg::messages(batch) {
            match t {
                b'P' => {
                    let (name, text) = pg::parse_parse(m).ok_or(("08P01", "malformed Parse message".to_string()))?;
                    needs.push((name.to_string(), true));
                    in_batch.insert(name.to_string(), text.to_string());
                }
                b'B' => {
                    let bind = pg::parse_bind(m).ok_or(("08P01", "malformed Bind message".to_string()))?;
                    let name = bind.statement.to_string();
                    let text = match in_batch.get(&name) {
                        Some(t) => t.clone(),
                        None => {
                            needs.push((name.clone(), false));
                            match self.statements.get(&name) {
                                Some(s) => s.text.clone(),
                                None => return Err(("26000", format!("prepared statement \"{name}\" does not exist"))),
                            }
                        }
                    };
                    if known.is_some() {
                        continue;
                    }
                    let template = route::template(&self.core, &self.login.database, Source::Parse, &text, home)
                        .await
                        .map_err(|e| ("08006", format!("the statement could not be classified: {e}")))?;
                    let shard = match template {
                        RouteTemplate::Param { index, family } => {
                            let block = bind.param(index as usize).and_then(|(bin, v)| route::param_block(bin, v, family));
                            match block {
                                Some(b) => map.shard_of_block(b),
                                None => return Err(no_key()),
                            }
                        }
                        RouteTemplate::Fixed { block } => map.shard_of_block(block),
                        RouteTemplate::Home => home,
                        RouteTemplate::Gather { tables } if self.core.shared.cfg.gather && self.login.pinned.is_none() => {
                            self.pending_gather = Some(tables);
                            return Err(("0A000", "a statement that gathers runs alone in its batch: Parse, Bind, Describe, Execute, Sync".into()));
                        }
                        RouteTemplate::FanOut { merge, writes } => {
                            return match self.unrouted_target(merge, writes) {
                                Target::FanOut { merge, writes } => {
                                    self.pending_fanout = Some((merge, writes));
                                    Err(("0A000", "a statement without a route key fans out only alone in its batch: Parse, Bind, Describe, Execute, Sync".into()))
                                }
                                Target::Refuse { sqlstate, message } => Err((sqlstate, message)),
                                _ => Err(no_key()),
                            };
                        }
                        _ => return Err(no_key()),
                    };
                    routes.push((shard, text));
                }
                b'D' | b'C' => {
                    let b = pg::body(m);
                    if b.first() == Some(&b'S')
                        && let Some((name, _)) = pg::cstr(&b[1..])
                        && !in_batch.contains_key(name)
                        && t == b'D'
                    {
                        needs.push((name.to_string(), false));
                    }
                }
                _ => {}
            }
        }
        if let Some(s) = known {
            return Ok((s, needs, None));
        }
        // The block's rule, statement by statement (`within_block`).
        let routes: Vec<(u32, String)> = routes
            .into_iter()
            .map(|(s, text)| self.within_block(s, &text).map(|s| (s, text)))
            .collect::<Result<_, _>>()?;
        let Some((first, text)) = routes.first().cloned() else {
            // A batch that only parses or describes: the block's shard, or
            // where the last statement went.
            let block = if self.txn.status != b'I' && !self.core.shared.cfg.two_phase { self.txn.shard } else { None };
            return Ok((block.or(self.last_shard).unwrap_or(home), needs, None));
        };
        if routes.iter().any(|(s, _)| *s != first) {
            self.core.counters.borrow_mut().refused_cross_shard += 1;
            return Err(("0A000", "the statements of one batch reach different shards; send a Sync between them".into()));
        }
        Ok((first, needs, Some(text)))
    }

    /// The gateway's record of the client's statements after a batch ran.
    fn after_extended(&mut self, batch: &[u8], shard: u32) {
        for (t, m) in pg::messages(batch) {
            match t {
                b'P' => {
                    if let Some((name, text)) = pg::parse_parse(m) {
                        self.generation += 1;
                        let g = self.generation;
                        self.statements.insert(name.to_string(), Stmt { text: text.to_string(), parse: m.to_vec(), generation: g });
                        if let Some(vs) = self.vs.get_mut(&shard) {
                            vs.parsed.insert(name.to_string(), g);
                        }
                    }
                }
                b'C' => {
                    if let Some((b'S', name)) = pg::parse_close(m) {
                        self.statements.remove(name);
                        if let Some(vs) = self.vs.get_mut(&shard) {
                            vs.parsed.remove(name);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // ------------------------------------------------------------ copy

    async fn copy_data<S: AsyncWrite + Unpin>(&mut self, conn: &mut S, t: u8, msg: &[u8]) -> io::Result<Outcome> {
        if self.copy_split.is_some() {
            return self.copy_split_data(t, msg).await;
        }
        let Some(shard) = self.copy_shard else {
            // PostgreSQL ignores CopyData outside COPY, and so does this.
            return Ok(Outcome::Done { error: false });
        };
        // Every CopyData is sent as it comes: rastcp frames it, and the
        // shard's reply to each is empty until CopyDone or CopyFail.
        let outcome = self.run_on(conn, shard, msg, Vec::new(), None).await?;
        if t == b'd' && matches!(outcome, Outcome::Done { error: false }) {
            self.copy_shard = Some(shard);
        }
        Ok(outcome)
    }
}

/// A new global transaction id, `gateway-pid-n`: unique across gateways,
/// numbered in order so the deadlock detector can tell the youngest.
/// A reply that is only a `57P03` "fenced for a move" error (and its
/// ReadyForQuery): the statement was refused before it did anything.
fn fenced_for_a_move(reply: &[u8]) -> bool {
    let mut error = false;
    for (t, m) in pg::messages(reply) {
        match t {
            // The shard's message leads with its code; the code field may
            // carry it or not, depending on the path the error took.
            b'E' if pg::error_message(m).is_some_and(|e| e.contains("fenced for a move; retry after it")) => error = true,
            b'Z' | b'N' | b'S' => {}
            _ => return false,
        }
    }
    error
}

/// Whether `text` names a table (reads or writes one): a word FROM, INTO,
/// UPDATE, JOIN or TABLE outside quotes. `SAVEPOINT a`, `SELECT 1` and
/// `SELECT now()` do not.
fn names_a_table(text: &str) -> bool {
    let up = text.to_ascii_uppercase();
    up.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).any(|w| matches!(w, "FROM" | "INTO" | "UPDATE" | "JOIN" | "TABLE"))
}

/// A read that takes no lock and changes nothing: SELECT, WITH, TABLE,
/// VALUES or SHOW, with no FOR UPDATE/SHARE and no data-changing word.
fn is_plain_read(text: &str) -> bool {
    let up = text.trim_start().to_ascii_uppercase();
    let words: Vec<&str> = up.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).filter(|w| !w.is_empty()).collect();
    let Some(first) = words.first() else { return false };
    if !matches!(*first, "SELECT" | "WITH" | "TABLE" | "VALUES" | "SHOW") {
        return false;
    }
    let locks = words.windows(2).any(|w| w[0] == "FOR" && matches!(w[1], "UPDATE" | "SHARE" | "NO" | "KEY"));
    let changes = words.iter().any(|w| matches!(*w, "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "TRUNCATE"));
    !locks && !changes
}

pub(crate) fn next_global_txn(gateway: &str) -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let name: String = gateway.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '_').take(40).collect();
    format!("{name}-{}-{n}", std::process::id())
}

fn value_is_reset(text: &str) -> bool {
    text.trim_start().get(..5).is_some_and(|w| w.eq_ignore_ascii_case("reset"))
}

fn needs_key() -> Target {
    Target::Refuse { sqlstate: "0A000", message: "statement needs a route key in cluster mode".into() }
}

fn no_key() -> (&'static str, String) {
    ("0A000", "statement needs a route key in cluster mode".into())
}

fn push_close(out: &mut Vec<u8>, name: &str) {
    let mut b = vec![b'S'];
    b.extend_from_slice(name.as_bytes());
    b.push(0);
    pg::put(out, b'C', &b);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_and_table_statements_are_told_apart() {
        for read in ["SELECT * FROM stores", "with x as (select 1) select * from x", "TABLE t", "SHOW x"] {
            assert!(is_plain_read(read), "{read}");
        }
        for not in ["SELECT * FROM t FOR UPDATE", "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d", "INSERT INTO t VALUES (1)", "SAVEPOINT a"] {
            assert!(!is_plain_read(not), "{not}");
        }
        assert!(names_a_table("SELECT * FROM t") && names_a_table("INSERT INTO t VALUES (1)") && names_a_table("update t set a = 1"));
        assert!(!names_a_table("SAVEPOINT a") && !names_a_table("SELECT 1") && !names_a_table("SELECT now()"));
    }

    #[test]
    fn gateway_local_statements_are_told_apart() {
        assert!(matches!(local_of("BEGIN"), Local::Begin));
        assert!(matches!(local_of("commit;"), Local::End { commit: true }));
        assert!(matches!(local_of("ROLLBACK"), Local::End { commit: false }));
        // ROLLBACK TO is a savepoint's, not the block's end.
        assert!(matches!(local_of("ROLLBACK TO SAVEPOINT s"), Local::Routed));
        assert!(matches!(local_of("SET LOCAL rdb.route_key = 42"), Local::SetRouteKey { local: true, value: Some(ref v) } if v == "42"));
        assert!(matches!(local_of("SET rdb.route_key TO DEFAULT"), Local::SetRouteKey { local: false, value: None }));
        assert!(matches!(local_of("SET search_path = x"), Local::SessionSet));
        assert!(matches!(local_of("SET LOCAL statement_timeout = 5"), Local::SetLocal));
        assert!(matches!(local_of("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"), Local::Routed));
        assert!(matches!(local_of("LISTEN Ch"), Local::Listen(ref c) if c == "ch"));
        assert!(matches!(local_of("LISTEN \"Ch\""), Local::Listen(ref c) if c == "Ch"));
        assert!(matches!(local_of("UNLISTEN *"), Local::Unlisten(None)));
        assert!(matches!(local_of("NOTIFY ch, 'x'"), Local::Routed), "NOTIFY runs on a shard");
        // Two statements are forwarded whole, never half answered here.
        assert!(matches!(local_of("BEGIN; SELECT 1"), Local::Routed));
        assert!(matches!(local_of("SELECT 1"), Local::Routed));
    }
}
