//! Statements without a route key, sent to every shard (B1b tier 1, B1c).
//!
//! Only merges that need no value decoding happen here: concatenation,
//! `COUNT` sums (one int8 per part), any-row, and top-N by sort keys the
//! shards computed. Everything with engine semantics (aggregates, GROUP
//! BY) is [`Merge::Coordinator`]: forwarded to the home shard, which
//! scatters and merges with the engine's own code.
//!
//! Outside a transaction, reads run each shard at its own snapshot. Under
//! two-phase commit a fan-out also runs inside a transaction (every shard
//! joined to it first), and a write runs on every shard as one two-phase
//! transaction: the client's, or the gateway's own in autocommit. Without
//! it, writes (`unrouted_writes = each_shard`) run one shard after another,
//! each its own transaction, and a failure names the shards that already
//! applied.

use std::io;
use std::rc::Rc;

use rasterizeddb_router::rshard::{Merge, PartReply, Request, Response};
use tokio::io::AsyncWrite;

use crate::core::{CallError, Core};
use crate::pg;
use crate::session::{Outcome, Session};

impl Session {
    pub(crate) async fn fan_out<S: AsyncWrite + Unpin>(
        &mut self,
        conn: &mut S,
        msg: &[u8],
        merge: Merge,
        writes: bool,
    ) -> io::Result<Outcome> {
        self.core.counters.borrow_mut().fan_outs += 1;
        let map = self.core.map();
        if self.txn.status == b'E' {
            self.error_ready("25P02", "current transaction is aborted, commands ignored until end of transaction block");
            return Ok(Outcome::Done { error: true });
        }
        let in_txn = self.txn.status != b'I';
        if let Merge::Coordinator = merge {
            let home = self.core.shared.home(&map);
            if in_txn {
                // The home shard coordinates with the others outside this
                // transaction: right only while it has run nowhere else, so
                // no other shard holds a change of its own it would miss.
                if self.txn.shards.iter().chain(self.txn.shard.iter()).any(|&s| s != home) {
                    // A top-N or a DISTINCT merges here instead, over parts
                    // run inside the transaction on every shard.
                    if let Some(text) = pg::query_text(msg) {
                        if let Some((part_sql, limit, offset)) = gateway_top_n(&text) {
                            let part = query_message(&part_sql);
                            return self.in_txn_merge(&part, Merge::TopN { limit, offset }, false).await;
                        }
                        if is_plain_distinct(&text) {
                            return self.in_txn_merge(msg, Merge::Concat, true).await;
                        }
                    }
                    self.error_ready(
                        "0A000",
                        "an aggregate over every shard cannot run in a transaction that already ran on a shard other than the home shard; run it before the transaction's first keyed statement, or outside it",
                    );
                    return Ok(Outcome::Done { error: true });
                }
                // Joined first: the shard coordinates only a statement that
                // comes alone, not behind a deferred BEGIN.
                if let Err((code, m)) = self.join_txn(&[home]).await {
                    self.error_ready(&code, &m);
                    return Ok(Outcome::Done { error: true });
                }
            }
            return self.run_on(conn, home, msg, Vec::new(), None).await;
        }
        let shards: Vec<u32> = map.shards().iter().map(|s| s.id).collect();
        if writes {
            if self.core.shared.cfg.two_phase {
                return self.fan_write(conn, msg, merge, &shards).await;
            }
            return self.each_shard(conn, msg, &shards).await;
        }
        if in_txn {
            if let Err((code, m)) = self.join_txn(&shards).await {
                self.error_ready(&code, &m);
                return Ok(Outcome::Done { error: true });
            }
            let parts = match self.run_parts(&shards, &merge, msg).await {
                Ok(p) => p,
                Err((code, m)) => {
                    self.error_ready(&code, &m);
                    return Ok(Outcome::Done { error: true });
                }
            };
            merge_parts(&mut self.out, &merge, &parts);
            let status = self.txn.status;
            pg::ready(&mut self.out, status);
            return Ok(Outcome::Done { error: false });
        }
        // A commit decided and mid-flight would show here half done (B5.2).
        committing::settle(std::time::Duration::from_secs(5)).await;
        // One snapshot across the shards (B6.1): every multi-shard commit
        // wholly in or wholly out.
        let consistent = self.consistent_fanout;
        if consistent && let Err((code, m)) = self.consistent_snapshots(&shards).await {
            for &s in &shards {
                let _ = self.quiet_query(s, "ROLLBACK").await;
            }
            self.error_ready(&code, &m);
            return Ok(Outcome::Done { error: true });
        }
        // Every part's virtual session first, then the parts at once.
        let mut sessions = Vec::with_capacity(shards.len());
        for &s in &shards {
            match self.ensure_vs(s).await {
                Ok(id) => sessions.push((s, id)),
                Err((code, m)) => {
                    self.error_ready(&code, &format!("shard {s}: {m}"));
                    return Ok(Outcome::Done { error: true });
                }
            }
        }
        let cap = self.core.shared.cfg.gather_max_bytes;
        let epoch = map.epoch();
        let mut handles = Vec::with_capacity(sessions.len());
        for (s, id) in sessions {
            let core = self.core.clone();
            let req = Request::ExecPart { session: id, map_epoch: epoch, merge: merge.clone(), bytes: msg.to_vec() }.encode();
            handles.push((s, tokio::task::spawn_local(gather(core, s, id, req, cap))));
        }
        let mut parts: Vec<(u32, PartReply)> = Vec::with_capacity(handles.len());
        let mut failure: Option<(String, String)> = None;
        for (s, h) in handles {
            match h.await {
                Ok(Ok(p)) => parts.push((s, p)),
                Ok(Err(e)) => {
                    failure.get_or_insert(e);
                }
                Err(_) => {
                    failure.get_or_insert(("XX000".into(), format!("the part on shard {s} panicked")));
                }
            }
        }
        if let Some((code, m)) = failure {
            self.error_ready(&code, &m);
            return Ok(Outcome::Done { error: true });
        }
        // A part's own error (in its tail) answers for the statement.
        if let Err((code, m)) = part_error(&parts) {
            if consistent {
                for &s in &shards {
                    let _ = self.quiet_query(s, "ROLLBACK").await;
                }
            }
            self.error_ready(&code, &m);
            return Ok(Outcome::Done { error: true });
        }
        if consistent {
            for &s in &shards {
                let _ = self.quiet_query(s, "COMMIT").await;
            }
        }
        merge_parts(&mut self.out, &merge, &parts);
        pg::ready(&mut self.out, b'I');
        Ok(Outcome::Done { error: false })
    }

    /// Opens a REPEATABLE READ block on every shard at one instant of the
    /// cluster (B6.1): commit decisions are held on the anchor, the
    /// commits already decided are waited out (no shard still holds one
    /// prepared), every part's snapshot is taken, and the hold is released.
    async fn consistent_snapshots(&mut self, shards: &[u32]) -> Result<(), (String, String)> {
        let home = self.core.shared.home(&self.core.map());
        let link = self.core.link(home).map_err(|e| ("08006".to_string(), e))?;
        let core = self.core.clone();
        let call = |req: Request| {
            let link = link.clone();
            let core = core.clone();
            async move { link.call(&core, &req.encode()).await }
        };
        if !matches!(call(Request::HoldCommits { ms: 3_000 }).await, Ok(Response::Done)) {
            return Err(("08006".into(), format!("the anchor shard {home} did not hold commits for a consistent read")));
        }
        let result = async {
            // Decided commits still prepared somewhere: wait them out.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            // Without two-phase commit no transaction spans shards.
            while self.core.shared.cfg.two_phase {
                let mut in_flight = false;
                for &s in shards {
                    let gids = self.first_column(s, "SHOW PREPARED TRANSACTIONS").await.map_err(|e| ("08006".to_string(), e))?;
                    for gid in gids.into_iter().flatten() {
                        if matches!(call(Request::Decision { gid }).await, Ok(Response::Decided { commit: Some(true) })) {
                            in_flight = true;
                        }
                    }
                }
                if !in_flight {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    return Err(("40001".to_string(), "a commit stayed half finished across shards; retry the read".to_string()));
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            for &s in shards {
                for sql in ["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", "SELECT 1"] {
                    match self.quiet_query(s, sql).await {
                        Ok((_, None)) => {}
                        Ok((_, Some(e))) => return Err(e),
                        Err(e) => return Err(("08006".to_string(), format!("shard {s}: {e}"))),
                    }
                }
            }
            Ok(())
        }
        .await;
        let _ = call(Request::ReleaseCommits).await;
        result
    }

    /// A read no one shard can answer (B4.5): each sharded table in
    /// `tables` read from every shard as fan-out parts, loaded into a
    /// temporary table on the home shard (which holds the unsharded ones),
    /// and the statement run there over those. Bounded by
    /// `gather_max_bytes` across every table; the temporaries are dropped
    /// after.
    pub(crate) async fn gather_run<S: AsyncWrite + Unpin>(&mut self, conn: &mut S, text: &str, tables: &[String]) -> io::Result<Outcome> {
        self.core.counters.borrow_mut().fan_outs += 1;
        committing::settle(std::time::Duration::from_secs(5)).await;
        let map = self.core.map();
        let home = self.core.shared.home(&map);
        let shards: Vec<u32> = map.shards().iter().map(|s| s.id).collect();
        let cap = self.core.shared.cfg.gather_max_bytes;
        let mut used = 0usize;
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut renames: Vec<(String, String)> = Vec::new();
        let fail = |me: &mut Self, code: &str, m: String| {
            me.error_ready(code, &m);
            Outcome::Done { error: true }
        };
        for table in tables {
            let temp = format!("rdb_gather_{n}_{}", table.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '_').collect::<String>());
            let sql = format!("SELECT * FROM {}", quote_ident(table));
            let mut q = vec![b'Q'];
            q.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
            q.extend_from_slice(sql.as_bytes());
            q.push(0);
            let parts = match self.collect_parts(&shards, &q, cap.saturating_sub(used)).await {
                Ok(p) => p,
                Err((code, m)) => {
                    self.drop_temps(home, &renames).await;
                    return Ok(fail(self, code.as_str(), m));
                }
            };
            used += parts.iter().map(|(_, p)| p.rows.iter().map(|(_, r)| r.len()).sum::<usize>()).sum::<usize>();
            let Some(columns) = parts.first().and_then(|(_, p)| row_columns(&p.head)) else {
                self.drop_temps(home, &renames).await;
                return Ok(fail(self, "XX000", format!("gathering {table}: no row description")));
            };
            let defs: Vec<String> = columns.iter().map(|(name, oid)| format!("{} {}", quote_ident(name), type_name(*oid))).collect();
            let create = format!("CREATE TEMP TABLE {} ({})", quote_ident(&temp), defs.join(", "));
            match self.quiet_query(home, &create).await {
                Ok((_, None)) => {}
                Ok((_, Some((code, m)))) => {
                    self.drop_temps(home, &renames).await;
                    return Ok(fail(self, code.as_str(), format!("gathering {table} on shard {home}: {m}")));
                }
                Err(e) => {
                    self.drop_temps(home, &renames).await;
                    return Ok(fail(self, "08006", format!("gathering {table} on shard {home}: {e}")));
                }
            }
            renames.push((table.clone(), temp.clone()));
            // The rows, a few hundred to a statement.
            let mut batch: Vec<String> = Vec::new();
            let all_rows: Vec<&Vec<u8>> = parts.iter().flat_map(|(_, p)| p.rows.iter().map(|(_, r)| r)).collect();
            for (i, row) in all_rows.iter().enumerate() {
                let cells = row_cells(row);
                let vals: Vec<String> = cells
                    .iter()
                    .zip(&columns)
                    .map(|(c, (_, oid))| match c {
                        None => "NULL".to_string(),
                        Some(v) => format!("'{}'::{}", String::from_utf8_lossy(v).replace('\'', "''"), type_name(*oid)),
                    })
                    .collect();
                batch.push(format!("({})", vals.join(", ")));
                if batch.len() == 256 || i + 1 == all_rows.len() {
                    let insert = format!("INSERT INTO {} VALUES {}", quote_ident(&temp), batch.join(", "));
                    batch.clear();
                    match self.quiet_query(home, &insert).await {
                        Ok((_, None)) => {}
                        Ok((_, Some((code, m)))) => {
                            self.drop_temps(home, &renames).await;
                            return Ok(fail(self, code.as_str(), format!("gathering {table}: {m}")));
                        }
                        Err(e) => {
                            self.drop_temps(home, &renames).await;
                            return Ok(fail(self, "08006", format!("gathering {table}: {e}")));
                        }
                    }
                }
            }
        }
        let rewritten = rename_tables(text, &renames);
        let mut q = vec![b'Q'];
        q.extend_from_slice(&((rewritten.len() + 5) as i32).to_be_bytes());
        q.extend_from_slice(rewritten.as_bytes());
        q.push(0);
        let outcome = self.run_on(conn, home, &q, Vec::new(), None).await;
        self.drop_temps(home, &renames).await;
        outcome
    }

    async fn drop_temps(&mut self, home: u32, renames: &[(String, String)]) {
        for (_, temp) in renames {
            let _ = self.quiet_query(home, &format!("DROP TABLE IF EXISTS {}", quote_ident(temp))).await;
        }
    }

    /// Every shard's part of `msg` (a plain read, concatenated), within
    /// `cap` bytes in all.
    async fn collect_parts(&mut self, shards: &[u32], msg: &[u8], cap: usize) -> Result<Vec<(u32, PartReply)>, (String, String)> {
        let epoch = self.core.map().epoch();
        let mut sessions = Vec::with_capacity(shards.len());
        for &s in shards {
            let id = self.ensure_vs(s).await.map_err(|(c, m)| (c, format!("shard {s}: {m}")))?;
            sessions.push((s, id));
        }
        let mut handles = Vec::with_capacity(sessions.len());
        for (s, id) in sessions {
            let req = Request::ExecPart { session: id, map_epoch: epoch, merge: Merge::Concat, bytes: msg.to_vec() }.encode();
            handles.push((s, tokio::task::spawn_local(gather(self.core.clone(), s, id, req, cap))));
        }
        let mut parts = Vec::with_capacity(handles.len());
        for (s, h) in handles {
            let p = h.await.map_err(|_| ("XX000".to_string(), format!("the part on shard {s} panicked")))??;
            if let Some((_, m)) = pg::messages(&p.tail).find(|(t, _)| *t == b'E') {
                let code = pg::error_sqlstate(m).unwrap_or("XX000").to_string();
                return Err((code, format!("shard {s}: {}", pg::error_message(m).unwrap_or_default())));
            }
            parts.push((s, p));
        }
        Ok(parts)
    }

    /// `unrouted_writes = each_shard`: the write on every shard in turn,
    /// each in its own transaction; counts summed.
    async fn each_shard<S: AsyncWrite + Unpin>(&mut self, conn: &mut S, msg: &[u8], shards: &[u32]) -> io::Result<Outcome> {
        let _ = conn;
        let mut applied: Vec<u32> = Vec::new();
        let mut total = 0u64;
        let mut tag_head = String::new();
        for &s in shards {
            let id = match self.ensure_vs(s).await {
                Ok(id) => id,
                Err((code, m)) => return Ok(self.partial(&code, &format!("shard {s}: {m}"), &applied)),
            };
            let link = match self.core.link(s) {
                Ok(l) => l,
                Err(e) => return Ok(self.partial("08006", &e, &applied)),
            };
            // A part, so the shard writes the rows it owns and skips the
            // ranges it does not.
            let req = Request::ExecPart { session: id, map_epoch: self.core.map().epoch(), merge: Merge::Concat, bytes: msg.to_vec() }.encode();
            let mut bytes = Vec::new();
            let mut resp = link.call(&self.core, &req).await;
            loop {
                match resp {
                    Ok(Response::PartReply(p)) => {
                        bytes.extend_from_slice(&p.tail);
                        if p.more {
                            resp = link.call(&self.core, &Request::Continue { session: id }.encode()).await;
                            continue;
                        }
                        break;
                    }
                    Ok(Response::Refused(r)) => return Ok(self.partial(r.sqlstate_str(), &r.message, &applied)),
                    Ok(other) => return Ok(self.partial("XX000", &format!("reply kind {:#04x}", other.kind()), &applied)),
                    Err(CallError::Down(e)) => return Ok(self.partial("08006", &e, &applied)),
                    Err(CallError::Lost(e)) | Err(CallError::Bad(e)) => {
                        return Ok(self.partial("08007", &format!("shard {s}'s outcome is unknown: {e}"), &applied));
                    }
                }
            }
            if let Some((_, m)) = pg::messages(&bytes).find(|(t, _)| *t == b'E') {
                let code = pg::error_sqlstate(m).unwrap_or("XX000").to_string();
                return Ok(self.partial(&code, &format!("shard {s}: {}", pg::error_message(m).unwrap_or_default()), &applied));
            }
            if let Some((_, m)) = pg::messages(&bytes).filter(|(t, _)| *t == b'C').last()
                && let Some((head, n)) = pg::tag_count(m)
            {
                tag_head = head;
                total += n;
            }
            applied.push(s);
        }
        pg::command_complete(&mut self.out, &format!("{} {total}", if tag_head.is_empty() { "UPDATE" } else { &tag_head }));
        pg::ready(&mut self.out, b'I');
        Ok(Outcome::Done { error: false })
    }

    /// Joins every one of `shards` the session's open transaction has not
    /// reached yet: each gets the transaction's opening messages (its BEGIN,
    /// the global id it is tagged with, its SET LOCALs) and, when the
    /// transaction then spans shards, the multi-shard lock timeout.
    pub(crate) async fn join_txn(&mut self, shards: &[u32]) -> Result<(), (String, String)> {
        let mut opening = self.txn.opening.clone();
        if opening.is_empty() {
            opening = self.txn.pending.clone();
        }
        if opening.is_empty() {
            push_query(&mut opening, "BEGIN");
        }
        let ms = self.core.shared.cfg.two_phase_lock_timeout_ms;
        let spans = shards.len() > 1 || self.txn.shards.iter().chain(self.txn.shard.iter()).any(|s| !shards.contains(s));
        if spans && ms > 0 && self.core.shared.cfg.two_phase {
            push_query(&mut opening, &format!("SET LOCAL lock_timeout = {ms}"));
        }
        for &s in shards {
            if self.txn.shards.contains(&s) || self.txn.shard == Some(s) {
                continue;
            }
            self.ensure_vs(s).await.map_err(|(c, m)| (c, format!("shard {s}: {m}")))?;
            self.send_checked(s, &opening).await.map_err(|(c, m)| (c, format!("shard {s}: {m}")))?;
            self.txn.shards.push(s);
            self.txn.shard.get_or_insert(s);
        }
        self.txn.pending.clear();
        self.txn.pending_queries = 0;
        Ok(())
    }

    /// Runs `msg` as a part on each of `shards` at once (inside the
    /// session's transaction there, if one is open), every part's reply in
    /// shard order. A part's own error fails the whole.
    async fn run_parts(&mut self, shards: &[u32], merge: &Merge, msg: &[u8]) -> Result<Vec<(u32, PartReply)>, (String, String)> {
        let mut sessions = Vec::with_capacity(shards.len());
        for &s in shards {
            let id = self.ensure_vs(s).await.map_err(|(c, m)| (c, format!("shard {s}: {m}")))?;
            sessions.push((s, id));
        }
        let cap = self.core.shared.cfg.gather_max_bytes;
        let epoch = self.core.map().epoch();
        let mut handles = Vec::with_capacity(sessions.len());
        for (s, id) in sessions {
            let req = Request::ExecPart { session: id, map_epoch: epoch, merge: merge.clone(), bytes: msg.to_vec() }.encode();
            handles.push((s, tokio::task::spawn_local(gather(self.core.clone(), s, id, req, cap))));
        }
        let mut parts = Vec::with_capacity(handles.len());
        let mut failure = None;
        for (s, h) in handles {
            match h.await {
                Ok(Ok(p)) => parts.push((s, p)),
                Ok(Err(e)) => {
                    failure.get_or_insert(e);
                }
                Err(_) => {
                    failure.get_or_insert(("XX000".into(), format!("the part on shard {s} panicked")));
                }
            }
        }
        if let Some(e) = failure {
            return Err(e);
        }
        part_error(&parts)?;
        Ok(parts)
    }

    /// A write without a route key under two-phase commit: run on every
    /// shard inside one transaction (the client's, or one of the gateway's
    /// own that commits it in two phases), so it changes every shard's rows
    /// or none. Answered with the shards' counts summed (one shard's for a
    /// reference table, whose every shard holds the same rows).
    async fn fan_write<S: AsyncWrite + Unpin>(&mut self, conn: &mut S, msg: &[u8], merge: Merge, shards: &[u32]) -> io::Result<Outcome> {
        let _ = conn;
        let implicit = self.begin_implicit();
        let ran = match self.join_txn(shards).await {
            Ok(()) => self.run_parts(shards, &merge, msg).await,
            Err(e) => Err(e),
        };
        let parts = match ran {
            Ok(p) => p,
            Err((code, m)) => return Ok(self.write_failed(implicit, &code, &m).await),
        };
        let mut out = Vec::new();
        merge_parts(&mut out, &merge, &parts);
        Ok(self.write_done(implicit, out).await)
    }

    /// Under two-phase commit, a write outside a block runs in a
    /// transaction of the gateway's own (tagged like a client's): opened
    /// here, `true` returned, ended by [`Self::write_done`] or
    /// [`Self::write_failed`].
    fn begin_implicit(&mut self) -> bool {
        if self.txn.status != b'I' {
            return false;
        }
        self.txn.opening.clear();
        push_query(&mut self.txn.opening, "BEGIN");
        push_query(
            &mut self.txn.opening,
            &format!("SET LOCAL rdb.global_txn = '{}'", crate::session::next_global_txn(&self.core.shared.cfg.gateway_name)),
        );
        self.txn.opening_queries = 2;
        self.txn.status = b'T';
        true
    }

    /// A multi-shard write failed: the gateway's own transaction rolled
    /// back on every shard it reached; a client's left failed.
    async fn write_failed(&mut self, implicit: bool, code: &str, m: &str) -> Outcome {
        if implicit {
            for s in self.txn.shards.clone() {
                let _ = self.quiet_query(s, "ROLLBACK").await;
            }
            self.end_txn();
        }
        self.error_ready(code, m);
        Outcome::Done { error: true }
    }

    /// A multi-shard write's merged reply `out` (rows, then its tag): in a
    /// client's block, relayed; in the gateway's own transaction, the rows
    /// relayed and the tag the two-phase commit's answer.
    async fn write_done(&mut self, implicit: bool, out: Vec<u8>) -> Outcome {
        if !implicit {
            self.out.extend_from_slice(&out);
            let status = self.txn.status;
            pg::ready(&mut self.out, status);
            return Outcome::Done { error: false };
        }
        // Rows (RETURNING) go out now; the count is the commit's answer.
        let mut tag = None;
        for (t, m) in pg::messages(&out) {
            if t == b'C' {
                tag = pg::cstr(pg::body(m)).map(|(c, _)| c.to_string());
            } else {
                self.out.extend_from_slice(m);
            }
        }
        self.end_two_phase_tagged(true, tag).await
    }

    /// A multi-row INSERT whose rows go to several shards (module docs of
    /// [`crate::split`]): `None` when it is not one (a row with no route
    /// key of its own), for the caller to answer as it would have.
    pub(crate) async fn split_insert<S: AsyncWrite + Unpin>(&mut self, conn: &mut S, msg: &[u8], text: &str) -> io::Result<Option<Outcome>> {
        let Some(rows) = crate::split::split(text) else {
            return Ok(None);
        };
        let map = self.core.map();
        let mut groups: std::collections::BTreeMap<u32, Vec<usize>> = std::collections::BTreeMap::new();
        for i in 0..rows.rows.len() {
            match self.decide_simple(&map, &rows.statement(&[i])).await.0 {
                crate::session::Target::Shard(s) => groups.entry(s).or_default().push(i),
                _ => return Ok(None),
            }
        }
        if groups.len() == 1 {
            let s = *groups.keys().next().expect("one group");
            return self.run_on(conn, s, msg, Vec::new(), None).await.map(Some);
        }
        if !self.core.shared.cfg.two_phase {
            self.error_ready(
                "0A000",
                "this INSERT's rows go to several shards, which needs two_phase = on in the gateway; or send one INSERT per shard",
            );
            return Ok(Some(Outcome::Done { error: true }));
        }
        if self.txn.status == b'E' {
            self.error_ready("25P02", "current transaction is aborted, commands ignored until end of transaction block");
            return Ok(Some(Outcome::Done { error: true }));
        }
        self.core.counters.borrow_mut().fan_outs += 1;
        let shards: Vec<u32> = groups.keys().copied().collect();
        let implicit = self.begin_implicit();
        if let Err((code, m)) = self.join_txn(&shards).await {
            return Ok(Some(self.write_failed(implicit, &code, &m).await));
        }
        // Every shard's INSERT at once, each a plain statement there (not
        // a part: a row the shard does not own is refused, never skipped).
        let epoch = map.epoch();
        let mut handles = Vec::with_capacity(groups.len());
        for (s, idx) in &groups {
            let id = match self.ensure_vs(*s).await {
                Ok(id) => id,
                Err((code, m)) => return Ok(Some(self.write_failed(implicit, &code, &format!("shard {s}: {m}")).await)),
            };
            let mut req = Vec::new();
            Request::encode_pg(&mut req, id, epoch, &query_message(&rows.statement(idx)));
            handles.push((*s, tokio::task::spawn_local(collect_pg(self.core.clone(), *s, id, req))));
        }
        let mut replies = Vec::with_capacity(handles.len());
        let mut failure = None;
        for (s, h) in handles {
            match h.await {
                Ok(Ok(bytes)) => {
                    if failure.is_none()
                        && let Some((_, m)) = pg::messages(&bytes).find(|(t, _)| *t == b'E')
                    {
                        failure = Some((pg::error_sqlstate(m).unwrap_or("XX000").to_string(), format!("shard {s}: {}", pg::error_message(m).unwrap_or_default())));
                    }
                    replies.push(bytes);
                }
                Ok(Err(e)) => {
                    failure.get_or_insert(e);
                }
                Err(_) => {
                    failure.get_or_insert(("XX000".into(), format!("the part on shard {s} panicked")));
                }
            }
        }
        if let Some((code, m)) = failure {
            return Ok(Some(self.write_failed(implicit, &code, &m).await));
        }
        // One RowDescription (RETURNING), every row, the counts summed.
        let mut out = Vec::new();
        let mut described = false;
        let mut total = 0u64;
        let mut head = "INSERT 0".to_string();
        for bytes in &replies {
            for (t, m) in pg::messages(bytes) {
                match t {
                    b'T' if !described => {
                        out.extend_from_slice(m);
                        described = true;
                    }
                    b'D' => out.extend_from_slice(m),
                    b'C' => {
                        if let Some((h, n)) = pg::tag_count(m) {
                            head = h;
                            total += n;
                        }
                    }
                    _ => {}
                }
            }
        }
        pg::command_complete(&mut out, &format!("{head} {total}"));
        Ok(Some(self.write_done(implicit, out).await))
    }

    /// A read merged here from parts run inside the session's transaction
    /// on every shard (every shard joined to it first); `distinct` drops
    /// rows another part already returned.
    async fn in_txn_merge(&mut self, msg: &[u8], merge: Merge, distinct: bool) -> io::Result<Outcome> {
        let shards: Vec<u32> = self.core.map().shards().iter().map(|s| s.id).collect();
        let joined = self.join_txn(&shards).await;
        let ran = match joined {
            Ok(()) => self.run_parts(&shards, &merge, msg).await,
            Err(e) => Err(e),
        };
        let mut parts = match ran {
            Ok(p) => p,
            Err((code, m)) => {
                self.error_ready(&code, &m);
                return Ok(Outcome::Done { error: true });
            }
        };
        if distinct {
            let mut seen = std::collections::HashSet::new();
            for (_, p) in parts.iter_mut() {
                p.rows.retain(|(_, row)| seen.insert(row.clone()));
            }
        }
        merge_parts(&mut self.out, &merge, &parts);
        let status = self.txn.status;
        pg::ready(&mut self.out, status);
        Ok(Outcome::Done { error: false })
    }

    fn partial(&mut self, code: &str, message: &str, applied: &[u32]) -> Outcome {
        let msg = if applied.is_empty() {
            message.to_string()
        } else {
            format!("{message}; the write already applied on shards {applied:?} and is not undone there")
        };
        self.error_ready(code, &msg);
        Outcome::Done { error: true }
    }
}

/// The first part whose reply holds an error, as that error, the shard
/// named.
fn part_error(parts: &[(u32, PartReply)]) -> Result<(), (String, String)> {
    for (s, p) in parts {
        if let Some((_, m)) = pg::messages(&p.tail).find(|(t, _)| *t == b'E') {
            let code = pg::error_sqlstate(m).unwrap_or("XX000").to_string();
            return Err((code, format!("shard {s}: {}", pg::error_message(m).unwrap_or_default())));
        }
    }
    Ok(())
}

/// A simple Query message for `sql`.
fn query_message(sql: &str) -> Vec<u8> {
    let mut m = Vec::with_capacity(sql.len() + 6);
    push_query(&mut m, sql);
    m
}

/// The statement's trailing words, upper-cased, after its last closing
/// parenthesis or quote: only there can a top-level LIMIT or OFFSET be told
/// apart without a parser.
fn tail_words(t: &str) -> Option<(usize, Vec<(String, usize)>)> {
    let start = t.rfind([')', '\'', '"']).map_or(0, |i| i + 1);
    let mut words = Vec::new();
    let mut at = start;
    for w in t[start..].split_whitespace() {
        let p = t[at..].find(w)? + at;
        words.push((w.to_ascii_uppercase(), p));
        at = p + w.len();
    }
    Some((start, words))
}

/// A top-N the gateway merges itself (inside a transaction, where the home
/// shard cannot coordinate): the part each shard runs (`LIMIT limit +
/// offset`, no OFFSET), `limit` and `offset`. `None` unless the statement
/// has a top-level ORDER BY and ends in literal LIMIT/OFFSET clauses (or
/// none: every row, in order). The shards check the keys are output
/// columns.
fn gateway_top_n(sql: &str) -> Option<(String, u64, u64)> {
    let t = sql.trim().trim_end_matches(';').trim_end();
    if !t.to_ascii_uppercase().contains("ORDER BY") {
        return None;
    }
    // This statement's ORDER BY, not a subquery's: after the last ')'.
    let upper = t.to_ascii_uppercase();
    if upper.rfind("ORDER BY")? < t.rfind(')').unwrap_or(0) {
        return None;
    }
    let (_, words) = tail_words(t)?;
    let n = words.len();
    let num = |i: usize| words.get(i).and_then(|(w, _)| w.parse::<u64>().ok());
    let (mut limit, mut offset, mut cut) = (u64::MAX, 0u64, t.len());
    let mut i = n;
    // Up to two trailing clauses, LIMIT n and OFFSET m, in either order.
    for _ in 0..2 {
        if i >= 2 {
            match (words[i - 2].0.as_str(), num(i - 1)) {
                ("LIMIT", Some(_)) if limit != u64::MAX => return None,
                ("OFFSET", Some(_)) if cut != t.len() && limit == u64::MAX => return None,
                ("LIMIT", Some(v)) => limit = v,
                ("OFFSET", Some(v)) => offset = v,
                ("LIMIT" | "OFFSET", None) => return None,
                _ => break,
            }
            cut = words[i - 2].1;
            i -= 2;
        }
    }
    // Nothing else after the ORDER BY: no parameter LIMIT, no FETCH, no
    // locking clause, no second LIMIT.
    let order_at = upper.rfind("ORDER BY")?;
    if upper[order_at..cut].split(|c: char| !c.is_ascii_alphanumeric() && c != '_').any(|w| matches!(w, "LIMIT" | "OFFSET" | "FETCH" | "FOR")) {
        return None;
    }
    let head = t[..cut].trim_end();
    let part = if limit == u64::MAX { head.to_string() } else { format!("{head} LIMIT {}", limit.saturating_add(offset)) };
    Some((part, limit, offset))
}

/// `SELECT DISTINCT …` (not DISTINCT ON) with no ORDER BY, LIMIT or OFFSET.
fn is_plain_distinct(sql: &str) -> bool {
    let up = sql.trim_start().to_ascii_uppercase();
    let Some(rest) = up.strip_prefix("SELECT") else { return false };
    let rest = rest.trim_start();
    rest.starts_with("DISTINCT") && !rest.starts_with("DISTINCT ON") && !up.contains(" ORDER BY ") && !up.contains(" LIMIT ") && !up.contains(" OFFSET ")
}

/// Appends a simple Query message for `sql`.
fn push_query(out: &mut Vec<u8>, sql: &str) {
    out.push(b'Q');
    out.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
    out.extend_from_slice(sql.as_bytes());
    out.push(0);
}

/// One plain batch's whole reply on `shard`, every chunk fetched.
async fn collect_pg(core: Rc<Core>, shard: u32, id: u64, req: Vec<u8>) -> Result<Vec<u8>, (String, String)> {
    let link = core.link(shard).map_err(|e| ("08006".to_string(), e))?;
    let mut resp = link.call(&core, &req).await;
    let mut out = Vec::new();
    loop {
        match resp {
            Ok(Response::PgReply { more, bytes, .. }) => {
                out.extend_from_slice(&bytes);
                if !more {
                    return Ok(out);
                }
                resp = link.call(&core, &Request::Continue { session: id }.encode()).await;
            }
            Ok(Response::Refused(r)) => return Err((r.sqlstate_str().to_string(), format!("shard {shard}: {}", r.message))),
            Ok(other) => return Err(("XX000".into(), format!("shard {shard} answered with kind {:#04x}", other.kind()))),
            Err(CallError::Down(e)) => return Err(("08006".into(), e)),
            Err(CallError::Lost(e)) | Err(CallError::Bad(e)) => return Err(("08007".into(), format!("shard {shard}'s outcome is unknown: {e}"))),
        }
    }
}

/// One shard's part, every chunk fetched, within `cap` bytes.
async fn gather(core: Rc<Core>, shard: u32, id: u64, req: Vec<u8>, cap: usize) -> Result<PartReply, (String, String)> {
    let link = core.link(shard).map_err(|e| ("08006".to_string(), e))?;
    let mut resp = link.call(&core, &req).await;
    let mut whole: Option<PartReply> = None;
    let mut size = 0usize;
    loop {
        match resp {
            Ok(Response::PartReply(p)) => {
                size += p.head.len() + p.tail.len() + p.rows.iter().map(|(k, r)| k.len() + r.len()).sum::<usize>();
                if size > cap {
                    return Err(("54000".into(), format!("the fan-out's result passed gather_max_bytes ({cap}) on shard {shard}")));
                }
                let more = p.more;
                match &mut whole {
                    None => whole = Some(p),
                    Some(w) => {
                        w.rows.extend(p.rows);
                        w.tail = p.tail;
                        w.more = p.more;
                        w.status = p.status;
                    }
                }
                if more {
                    resp = link.call(&core, &Request::Continue { session: id }.encode()).await;
                    continue;
                }
                return Ok(whole.expect("set above"));
            }
            Ok(Response::Refused(r)) => return Err((r.sqlstate_str().to_string(), format!("shard {shard}: {}", r.message))),
            Ok(other) => return Err(("XX000".into(), format!("shard {shard} answered ExecPart with kind {:#04x}", other.kind()))),
            Err(CallError::Down(e)) => return Err(("08006".into(), e)),
            Err(CallError::Lost(e)) | Err(CallError::Bad(e)) => return Err(("08006".into(), e)),
        }
    }
}

/// Writes the merged result (without ReadyForQuery).
fn merge_parts(out: &mut Vec<u8>, merge: &Merge, parts: &[(u32, PartReply)]) {
    let Some((_, first)) = parts.first() else {
        pg::command_complete(out, "SELECT 0");
        return;
    };
    // The RowDescription (and anything before rows) once, from the first.
    out.extend_from_slice(&first.head);
    let tag = parts
        .iter()
        .find_map(|(_, p)| pg::messages(&p.tail).find(|(t, _)| *t == b'C').and_then(|(_, m)| pg::tag_count(m)).map(|(h, _)| h))
        .unwrap_or_else(|| "SELECT".to_string());
    let n = match merge {
        Merge::Concat => {
            let mut n = 0u64;
            for (_, p) in parts {
                for (_, row) in &p.rows {
                    out.extend_from_slice(row);
                    n += 1;
                }
            }
            n
        }
        Merge::CountSum => {
            let binary = pg::messages(&first.head)
                .find(|(t, _)| *t == b'T')
                .and_then(|(_, m)| pg::row_description_first_format(m))
                == Some(1);
            let mut sum: i64 = 0;
            for (_, p) in parts {
                for (_, row) in &p.rows {
                    if let Some(Some(v)) = pg::data_row_first(row) {
                        let x = if binary {
                            v.try_into().map(i64::from_be_bytes).unwrap_or(0)
                        } else {
                            std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()).unwrap_or(0)
                        };
                        sum = sum.saturating_add(x);
                    }
                }
            }
            if binary {
                let mut b = 1i16.to_be_bytes().to_vec();
                b.extend_from_slice(&8i32.to_be_bytes());
                b.extend_from_slice(&sum.to_be_bytes());
                pg::put(out, b'D', &b);
            } else {
                pg::data_row(out, &[Some(&sum.to_string())]);
            }
            1
        }
        Merge::Any => {
            let chosen = parts.iter().find(|(_, p)| !p.rows.is_empty()).map(|(_, p)| p).unwrap_or(first);
            for (_, row) in &chosen.rows {
                out.extend_from_slice(row);
            }
            chosen.rows.len() as u64
        }
        Merge::TopN { limit, offset } => {
            // k-way merge by the shards' sort keys, stable in shard order.
            let mut idx = vec![0usize; parts.len()];
            let mut emitted = 0u64;
            let mut skipped = 0u64;
            loop {
                let mut best: Option<usize> = None;
                for (i, (_, p)) in parts.iter().enumerate() {
                    if let Some((k, _)) = p.rows.get(idx[i])
                        && best.is_none_or(|b| k < &parts[b].1.rows[idx[b]].0)
                    {
                        best = Some(i);
                    }
                }
                let Some(b) = best else { break };
                let row = &parts[b].1.rows[idx[b]].1;
                idx[b] += 1;
                if skipped < *offset {
                    skipped += 1;
                    continue;
                }
                if emitted >= *limit {
                    break;
                }
                out.extend_from_slice(row);
                emitted += 1;
            }
            emitted
        }
        Merge::Coordinator => 0,
    };
    // A statement that returns no rows (a write without RETURNING) counts
    // by the shards' own tags: summed when each changed its own rows, one
    // shard's when every shard holds the same rows (a reference table).
    // Counted from rows, every fan-out write said `UPDATE 0`.
    let returns_rows = pg::messages(&first.head).any(|(t, _)| t == b'T');
    let n = if returns_rows {
        n
    } else {
        let counts = parts.iter().filter_map(|(_, p)| {
            pg::messages(&p.tail).find(|(t, _)| *t == b'C').and_then(|(_, m)| pg::tag_count(m)).map(|(_, c)| c)
        });
        match merge {
            Merge::Any => counts.max().unwrap_or(0),
            _ => counts.sum(),
        }
    };
    pg::command_complete(out, &format!("{tag} {n}"));
}

#[cfg(test)]
mod tests {
    #[test]
    fn gateway_top_n_reads_only_trailing_literal_clauses() {
        use super::gateway_top_n as g;
        assert_eq!(g("SELECT id FROM t ORDER BY id DESC LIMIT 3"), Some(("SELECT id FROM t ORDER BY id DESC LIMIT 3".into(), 3, 0)));
        assert_eq!(g("SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 2;"), Some(("SELECT id FROM t ORDER BY id LIMIT 5".into(), 3, 2)));
        assert_eq!(g("SELECT id FROM t ORDER BY id OFFSET 2 LIMIT 3"), Some(("SELECT id FROM t ORDER BY id LIMIT 5".into(), 3, 2)));
        assert_eq!(g("SELECT id FROM t ORDER BY id"), Some(("SELECT id FROM t ORDER BY id".into(), u64::MAX, 0)));
        assert_eq!(g("SELECT id FROM t ORDER BY id OFFSET 4"), Some(("SELECT id FROM t ORDER BY id".into(), u64::MAX, 4)));
        // Not literal, not this statement's, or not a plain tail.
        assert_eq!(g("SELECT id FROM t ORDER BY id LIMIT $1"), None);
        assert_eq!(g("SELECT * FROM (SELECT id FROM t ORDER BY id LIMIT 3) s"), None);
        assert_eq!(g("SELECT id FROM t"), None);
        assert_eq!(g("SELECT id FROM t ORDER BY id FOR UPDATE"), None);
        assert_eq!(g("SELECT id FROM t ORDER BY id LIMIT 2 LIMIT 3"), None);
    }

    use super::*;

    fn part(head: Vec<u8>, rows: Vec<(Vec<u8>, Vec<u8>)>, tag: &str) -> PartReply {
        let mut tail = Vec::new();
        pg::command_complete(&mut tail, tag);
        PartReply { session: 1, map_epoch: 1, more: false, status: b'I', head, rows, tail }
    }

    fn row(v: &str) -> Vec<u8> {
        let mut out = Vec::new();
        pg::data_row(&mut out, &[Some(v)]);
        out
    }

    fn head() -> Vec<u8> {
        let mut out = Vec::new();
        pg::row_description(&mut out, &["c"]);
        out
    }

    /// The merged rows' first cells and the command tag.
    fn merged(merge: Merge, parts: &[(u32, PartReply)]) -> (Vec<String>, String) {
        let mut out = Vec::new();
        merge_parts(&mut out, &merge, parts);
        let mut cells = Vec::new();
        let mut tag = String::new();
        for (t, m) in pg::messages(&out) {
            match t {
                b'D' => cells.push(String::from_utf8(pg::data_row_first(m).unwrap().unwrap().to_vec()).unwrap()),
                b'C' => tag = pg::cstr(pg::body(m)).unwrap().0.to_string(),
                _ => {}
            }
        }
        (cells, tag)
    }

    #[test]
    fn fanout_concat_and_count_sum() {
        let parts = vec![
            (0, part(head(), vec![(vec![], row("a")), (vec![], row("b"))], "SELECT 2")),
            (1, part(head(), vec![(vec![], row("c"))], "SELECT 1")),
        ];
        assert_eq!(merged(Merge::Concat, &parts), (vec!["a".into(), "b".into(), "c".into()], "SELECT 3".into()));
        let counts = vec![
            (0, part(head(), vec![(vec![], row("40"))], "SELECT 1")),
            (1, part(head(), vec![(vec![], row("2"))], "SELECT 1")),
        ];
        assert_eq!(merged(Merge::CountSum, &counts), (vec!["42".into()], "SELECT 1".into()));
    }

    #[test]
    fn fanout_top_n_merges_by_key_then_offset_and_limit() {
        let p = |rows: &[(&str, &str)]| rows.iter().map(|(k, v)| (k.as_bytes().to_vec(), row(v))).collect::<Vec<_>>();
        let parts = vec![
            (0, part(head(), p(&[("a", "1"), ("d", "4"), ("e", "5")]), "SELECT 3")),
            (1, part(head(), p(&[("b", "2"), ("c", "3"), ("f", "6")]), "SELECT 3")),
        ];
        let (cells, tag) = merged(Merge::TopN { limit: 3, offset: 1 }, &parts);
        assert_eq!(cells, vec!["2", "3", "4"]);
        assert_eq!(tag, "SELECT 3");
    }

    #[test]
    fn fanout_writes_count_by_the_shards_tags() {
        let parts = vec![(0, part(Vec::new(), vec![], "UPDATE 3")), (1, part(Vec::new(), vec![], "UPDATE 4"))];
        assert_eq!(merged(Merge::Concat, &parts).1, "UPDATE 7", "each shard changed its own rows");
        // A reference table: every shard changed the same rows.
        let same = vec![(0, part(Vec::new(), vec![], "UPDATE 5")), (1, part(Vec::new(), vec![], "UPDATE 5"))];
        assert_eq!(merged(Merge::Any, &same).1, "UPDATE 5");
        let inserts = vec![(0, part(Vec::new(), vec![], "INSERT 0 2")), (1, part(Vec::new(), vec![], "INSERT 0 2"))];
        assert_eq!(merged(Merge::Any, &inserts).1, "INSERT 0 2");
    }
}

/// An identifier, double-quoted.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The columns a RowDescription names: (name, type oid).
fn row_columns(head: &[u8]) -> Option<Vec<(String, u32)>> {
    let (_, m) = pg::messages(head).find(|(t, _)| *t == b'T')?;
    let b = pg::body(m);
    let n = u16::from_be_bytes(b.get(0..2)?.try_into().ok()?) as usize;
    let mut at = 2;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let end = at + b.get(at..)?.iter().position(|&c| c == 0)?;
        let name = String::from_utf8_lossy(&b[at..end]).into_owned();
        at = end + 1;
        let oid = u32::from_be_bytes(b.get(at + 6..at + 10)?.try_into().ok()?);
        at += 18;
        out.push((name, oid));
    }
    Some(out)
}

/// A DataRow's cells, text format.
fn row_cells(row: &[u8]) -> Vec<Option<Vec<u8>>> {
    let b = pg::body(row);
    let Some(n) = b.get(0..2).map(|x| u16::from_be_bytes([x[0], x[1]]) as usize) else { return Vec::new() };
    let mut at = 2;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let Some(len) = b.get(at..at + 4).map(|x| i32::from_be_bytes([x[0], x[1], x[2], x[3]])) else { break };
        at += 4;
        if len < 0 {
            out.push(None);
        } else {
            let end = at + len as usize;
            out.push(b.get(at..end).map(<[u8]>::to_vec));
            at = end;
        }
    }
    out
}

/// The SQL type for a column of type `oid`, for the gathered temporary.
fn type_name(oid: u32) -> &'static str {
    match oid {
        16 => "boolean",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        700 => "real",
        701 => "double precision",
        1700 => "numeric",
        17 => "bytea",
        1082 => "date",
        1114 => "timestamp",
        1184 => "timestamptz",
        2950 => "uuid",
        3802 => "jsonb",
        114 => "json",
        _ => "text",
    }
}

/// `text` with every table in `renames` written as its temporary, outside
/// string literals. A dotted name is read part by part:
/// - `t` is a relation;
/// - `public.t` is a relation (the schema dropped: temporaries are not in it);
/// - `t.c` is a column of `t` (the qualifier renamed);
/// - `s.t.c` likewise.
fn rename_tables(text: &str, renames: &[(String, String)]) -> String {
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len() + 64);
    let mut i = 0;
    let temp_of = |part: &str| {
        let name = if part.starts_with('"') { part.trim_matches('"').to_string() } else { part.to_ascii_lowercase() };
        renames.iter().find(|(t, _)| *t == name).map(|(_, temp)| quote_ident(temp))
    };
    while i < b.len() {
        let c = b[i];
        if c == b'\'' {
            let end = b[i + 1..].iter().position(|&x| x == b'\'').map_or(b.len(), |p| i + 2 + p);
            out.push_str(&text[i..end.min(b.len())]);
            i = end;
            continue;
        }
        let ident_start = (c.is_ascii_alphabetic() || c == b'_' || c == b'"') && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_' || b[i - 1] == b'$'));
        if !ident_start {
            out.push(c as char);
            i += 1;
            continue;
        }
        // The dotted chain's parts.
        let mut parts: Vec<&str> = Vec::new();
        let mut j = i;
        loop {
            let s = j;
            if j < b.len() && b[j] == b'"' {
                j += 1 + b[j + 1..].iter().position(|&x| x == b'"').map_or(b.len() - j - 1, |p| p + 1);
            } else {
                while j < b.len() && (b[j].is_ascii_alphanumeric() || b[j] == b'_') {
                    j += 1;
                }
            }
            parts.push(&text[s..j.min(text.len())]);
            if j < b.len() && b[j] == b'.' && b.get(j + 1).is_some_and(|n| n.is_ascii_alphabetic() || *n == b'_' || *n == b'"') {
                j += 1;
                continue;
            }
            break;
        }
        let is_public = |p: &str| p.eq_ignore_ascii_case("public") || p == "\"public\"";
        let rendered: Vec<String> = match parts.len() {
            1 => vec![temp_of(parts[0]).unwrap_or_else(|| parts[0].to_string())],
            2 if is_public(parts[0]) => match temp_of(parts[1]) {
                Some(t) => vec![t],
                None => parts.iter().map(|p| p.to_string()).collect(),
            },
            2 => vec![temp_of(parts[0]).unwrap_or_else(|| parts[0].to_string()), parts[1].to_string()],
            _ if is_public(parts[0]) => match temp_of(parts[1]) {
                Some(t) => std::iter::once(t).chain(parts[2..].iter().map(|p| p.to_string())).collect(),
                None => parts.iter().map(|p| p.to_string()).collect(),
            },
            _ => parts.iter().map(|p| p.to_string()).collect(),
        };
        out.push_str(&rendered.join("."));
        i = j;
    }
    out
}

#[cfg(test)]
mod gather_tests {
    use super::*;

    #[test]
    fn gather_renames_relations_not_their_columns_or_strings() {
        let r = vec![("orders".to_string(), "rdb_gather_1_orders".to_string())];
        assert_eq!(
            rename_tables("SELECT o.id FROM orders o JOIN public.orders p ON true WHERE note = 'orders'", &r),
            "SELECT o.id FROM \"rdb_gather_1_orders\" o JOIN \"rdb_gather_1_orders\" p ON true WHERE note = 'orders'"
        );
        // `orders.id`: the qualifier of a column is the table too.
        assert_eq!(
            rename_tables("SELECT orders.id, public.orders.v FROM orders", &r),
            "SELECT \"rdb_gather_1_orders\".id, \"rdb_gather_1_orders\".v FROM \"rdb_gather_1_orders\""
        );
        assert_eq!(rename_tables("SELECT x.orders FROM t x", &r), "SELECT x.orders FROM t x", "a column named like the table");
    }
}

/// Two-phase commits this gateway has decided and not yet finished on
/// every shard (B5.2): a read across shards waits for them, so it never
/// sees one part of a committed transaction without the rest.
pub(crate) mod committing {
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    static SET: Mutex<Option<HashSet<String>>> = Mutex::new(None);

    pub(crate) fn begin(gid: &str) {
        SET.lock().unwrap_or_else(|e| e.into_inner()).get_or_insert_with(HashSet::new).insert(gid.to_string());
    }

    pub(crate) fn end(gid: &str) {
        if let Some(s) = SET.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            s.remove(gid);
        }
    }

    fn busy() -> bool {
        SET.lock().unwrap_or_else(|e| e.into_inner()).as_ref().is_some_and(|s| !s.is_empty())
    }

    /// Waits until no decided commit is mid-flight, at most `bound`;
    /// `false` when it gave up (a stuck part is `rdb-shard resolve`'s).
    pub(crate) async fn settle(bound: Duration) -> bool {
        let until = Instant::now() + bound;
        while busy() {
            if Instant::now() >= until {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        true
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn b52_a_read_waits_for_a_commit_mid_flight() {
            begin("g1");
            let start = Instant::now();
            let waiter = tokio::spawn(settle(Duration::from_secs(5)));
            tokio::time::sleep(Duration::from_millis(30)).await;
            end("g1");
            assert!(waiter.await.unwrap(), "settled once the commit finished");
            assert!(start.elapsed() >= Duration::from_millis(25));
            begin("stuck");
            assert!(!settle(Duration::from_millis(20)).await, "bounded");
            end("stuck");
        }
    }
}
