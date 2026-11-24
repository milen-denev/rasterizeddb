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
    pg::command_complete(out, &format!("{tag} {n}"));
}
