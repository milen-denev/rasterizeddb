//! Statements without a route key, sent to every shard (B1b tier 1, B1c).
//!
//! Only merges that need no value decoding happen here: concatenation,
//! `COUNT` sums (one int8 per part), any-row, and top-N by sort keys the
//! shards computed. Everything with engine semantics (aggregates, GROUP
//! BY) is [`Merge::Coordinator`]: forwarded to the home shard, which
//! scatters and merges with the engine's own code.
//!
//! Reads run in autocommit only, each shard at its own snapshot; writes
//! (`unrouted_writes = each_shard`) run one shard after another, each its
//! own transaction, and a failure names the shards that already applied.

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
        if let Merge::Coordinator = merge {
            let home = self.core.shared.home(&map);
            return self.run_on(conn, home, msg, Vec::new(), None).await;
        }
        let shards: Vec<u32> = map.shards().iter().map(|s| s.id).collect();
        if writes {
            return self.each_shard(conn, msg, &shards).await;
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
        for (s, p) in &parts {
            if let Some((_, m)) = pg::messages(&p.tail).find(|(t, _)| *t == b'E') {
                let code = pg::error_sqlstate(m).unwrap_or("XX000").to_string();
                self.error_ready(&code, &format!("shard {s} failed its part of the statement"));
                return Ok(Outcome::Done { error: true });
            }
        }
        merge_parts(&mut self.out, &merge, &parts);
        pg::ready(&mut self.out, b'I');
        Ok(Outcome::Done { error: false })
    }

    /// A read no one shard can answer (B4.5): each sharded table in
    /// `tables` read from every shard as fan-out parts, loaded into a
    /// temporary table on the home shard (which holds the unsharded ones),
    /// and the statement run there over those. Bounded by
    /// `gather_max_bytes` across every table; the temporaries are dropped
    /// after.
    pub(crate) async fn gather_run<S: AsyncWrite + Unpin>(&mut self, conn: &mut S, text: &str, tables: &[String]) -> io::Result<Outcome> {
        self.core.counters.borrow_mut().fan_outs += 1;
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
            let mut req = Vec::new();
            Request::encode_pg(&mut req, id, self.core.map().epoch(), msg);
            let mut bytes = Vec::new();
            let mut resp = link.call(&self.core, &req).await;
            loop {
                match resp {
                    Ok(Response::PgReply { more, bytes: b, .. }) => {
                        bytes.extend_from_slice(&b);
                        if more {
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
                return Ok(self.partial(&code, &format!("shard {s} failed"), &applied));
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
