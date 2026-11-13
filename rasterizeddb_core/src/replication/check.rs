//! Comparing what two nodes hold, row for row.
//!
//! Sequence numbers say how much of a stream a node has applied. They do not
//! say the rows are the same, and the one time a pair here diverged, by
//! 44,670 transactions, both nodes reported the peer up with a lag of zero.
//! This is the check that does not take the stream's word for it: each node
//! reduces every table to a row count and a checksum over its rows' values,
//! and the two are compared. Values, not stored bytes: see [`value_hash`].
//!
//! ## Only when in step
//!
//! Two nodes taking writes are never guaranteed to be at the same point, and
//! a table read while a transaction is in flight differs for an honest
//! reason. So each node reports where its streams stood before and after it
//! read its tables, and the comparison counts only when nothing moved on
//! either side and each has applied all the other has released. Otherwise it
//! is tried again a few times, and then reported as not comparable rather
//! than as a difference.

use std::time::Duration;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::rql::lexer_s1::recognize_query_purpose;

use super::peer::PeerHandle;
use super::protocol::{self, FrameKind, NodeDigest, Reader, TableDigest};

/// A table with more rows than this is counted and not read: its rows are
/// held in memory while they are hashed.
pub const MAX_ROWS_READ: u64 = 2_000_000;

/// How often an out-of-step comparison is tried again, and how long between.
const TRIES: usize = 8;
const PAUSE: Duration = Duration::from_millis(250);

/// Whether `name` is a table a client made, as opposed to one the engine
/// keeps for itself on each node.
fn comparable(name: &str) -> bool {
    !(name.starts_with(crate::core::rql::cte::TEMP_TABLE_PREFIX)
        || crate::core::rql::json_map::is_shadow_table(name)
        // Each node's own: its first role is made at its first start.
        || name == crate::auth::ROLE_TABLE)
}

async fn query(database: &Arc<Database>, sql: &str) -> Result<Vec<crate::core::row::row::Row>, String> {
    let purpose = recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse `{sql}`"))?;
    let response = crate::core::rql::executor::execute(purpose, database.clone()).await;
    match response.first() {
        Some(&2) => crate::core::row::row::vec_into_rows(&response[1..]).map_err(|e| e.to_string()),
        Some(&3) => Err(String::from_utf8_lossy(&response[1..]).to_string()),
        _ => Ok(Vec::new()),
    }
}

/// FNV-1a over `bytes`, continued from `state`. Written out so that two
/// nodes on different builds still agree.
fn fnv(mut state: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        state ^= *byte as u64;
        state = state.wrapping_mul(0x0000_0100_0000_01b3);
    }
    state
}

/// A table's columns as a client sees them, in order, each with whether it
/// is a `character(n)`: the one text type whose trailing spaces are padding.
/// `None` when the table is not there.
fn live_columns(database: &Arc<Database>, name: &str) -> Option<Vec<(String, bool)>> {
    use crate::core::rql::type_modifier::{is_legacy_bpchar, is_marked_bpchar};
    let relation = database.relation(name)?;
    let schema = relation.schema();
    Some(
        crate::core::rql::nullable::visible_fields(&schema.fields)
            .into_iter()
            .map(|field| {
                let padded = field.db_type == crate::core::db_type::DbType::CHAR
                    || is_marked_bpchar(&field.name, &schema.type_modifiers)
                    || is_legacy_bpchar(&field.name, &schema.type_modifiers);
                (field.name, padded)
            })
            .collect(),
    )
}

/// One value, as something two nodes agree on when they hold the same value.
///
/// Not its stored bytes. The same row can be laid out differently on two
/// nodes that both hold it correctly: the bytes under a `NULL` are whatever
/// the last writer left; a `CHAR(n)` is stored padded or not depending on the
/// path that wrote it. So a `NULL` is hashed as a `NULL` whatever lies under
/// it, a value is hashed as the engine writes it out as a literal, and a
/// `character(n)` is compared without its trailing spaces.
///
/// Only a `character(n)`. In `text` and `varchar` a trailing space is part of
/// the value, and trimming it there would make `'a'` on one node and `'a '`
/// on the other hash alike: a divergence reported as agreement, which is the
/// one answer a check like this must never give. A false alarm costs a look;
/// that costs the data.
fn value_hash(mut state: u64, column: &crate::core::row::row::Column, padded: bool) -> u64 {
    // A query's result marks a `NULL` cell by its type; the bytes under it
    // are not looked at.
    if column.column_type == crate::core::db_type::DbType::NULL {
        return fnv(state, &[0]);
    }
    state = fnv(state, &[1]);
    let bytes = column.data.into_slice();
    let text_like = matches!(
        column.column_type,
        crate::core::db_type::DbType::STRING | crate::core::db_type::DbType::CHAR
    );
    if text_like {
        // NULs are storage fill for a fixed-width string, never content.
        let mut end = bytes.len();
        while end > 0 && (bytes[end - 1] == 0 || (padded && bytes[end - 1] == b' ')) {
            end -= 1;
        }
        state = fnv(state, &(end as u32).to_le_bytes());
        return fnv(state, &bytes[..end]);
    }
    match crate::core::rql::subquery::subquery_literal(&column.column_type, bytes, true) {
        Some(literal) => {
            state = fnv(state, &(literal.len() as u32).to_le_bytes());
            fnv(state, literal.as_bytes())
        }
        None => {
            state = fnv(state, &(bytes.len() as u32).to_le_bytes());
            fnv(state, bytes)
        }
    }
}

/// One row's hash, over the columns it was asked for, in order.
fn row_hash(row: &crate::core::row::row::Row, columns: &[(String, bool)]) -> u64 {
    let mut state = 0xcbf2_9ce4_8422_2325u64;
    for (at, value) in row.columns.iter().enumerate() {
        let padded = columns.get(at).is_some_and(|(_, padded)| *padded);
        state = fnv(state, &(at as u32).to_le_bytes());
        state = value_hash(state, value, padded);
    }
    // Spread before the rows are summed: FNV's low bits are weak, and a sum
    // of weak hashes cancels more easily than it should.
    state ^= state >> 33;
    state = state.wrapping_mul(0xff51_afd7_ed55_8ccd);
    state ^ (state >> 33)
}

async fn table_digest(database: &Arc<Database>, name: &str) -> TableDigest {
    let unread = |rows: u64, note: String| TableDigest {
        name: name.to_string(),
        rows,
        checksum: 0,
        note,
    };
    let Some(columns) = live_columns(database, name) else {
        return unread(0, "no such table".to_string());
    };
    let count = match query(database, &format!("SELECT COUNT(*) FROM {name}")).await {
        Ok(rows) => rows
            .first()
            .and_then(|row| row.columns.first())
            .map(|column| {
                let bytes = column.data.into_slice();
                let mut word = [0u8; 8];
                let n = bytes.len().min(8);
                word[..n].copy_from_slice(&bytes[..n]);
                u64::from_le_bytes(word)
            })
            .unwrap_or(0),
        Err(e) => return unread(0, format!("could not be counted: {e}")),
    };
    if count > MAX_ROWS_READ {
        return unread(count, format!("more than {MAX_ROWS_READ} rows: counted, contents not read"));
    }
    // Each live column by name. Not `*`, and not the row as stored: a
    // dropped column stays in the layout, and two nodes that altered a table
    // at different points relative to their rows lay the same row out
    // differently.
    let list: Vec<&str> = columns.iter().map(|(column, _)| column.as_str()).collect();
    let expected: usize = list.len();
    match query(database, &format!("SELECT {} FROM {name}", list.join(", "))).await {
        Ok(rows) => {
            if let Some(odd) = rows.iter().find(|row| row.columns.len() != expected) {
                return unread(
                    count,
                    format!(
                        "its rows came back with {} columns where {} were asked for; not compared",
                        odd.columns.len(),
                        expected
                    ),
                );
            }
            // A sum, so the order the rows came in does not matter, and a
            // row held twice does not cancel itself the way it would in an
            // exclusive-or.
            let checksum = rows
                .iter()
                .fold(0u64, |sum, row| sum.wrapping_add(row_hash(row, &columns)));
            TableDigest {
                name: name.to_string(),
                rows: rows.len() as u64,
                checksum,
                note: String::new(),
            }
        }
        Err(e) => unread(count, format!("could not be read: {e}")),
    }
}

/// This node's tables, and where its streams stood around reading them.
/// `only` names the tables wanted; empty means every table a client made.
pub async fn digest(database: &Arc<Database>, only: &[String]) -> NodeDigest {
    let replication = &database.replication;
    let released_before = replication.released_through();
    let applied_before = replication.applied().vector();

    let mut names: Vec<String> = if only.is_empty() {
        database.relation_names().into_iter().filter(|name| comparable(name)).collect()
    } else {
        only.to_vec()
    };
    names.sort();
    names.dedup();

    let mut tables = Vec::with_capacity(names.len());
    for name in &names {
        tables.push(table_digest(database, name).await);
    }

    NodeDigest {
        node_id: replication.node_id(),
        released: (released_before, replication.released_through()),
        unreleased: replication.unreleased() as u64,
        applied_before,
        applied_after: replication.applied().vector(),
        tables,
    }
}

/// Answers a peer's `Digest`.
pub async fn answer(database: &Arc<Database>, payload: &[u8]) -> Vec<u8> {
    let only = match protocol::decode_digest_request(&mut Reader::new(payload)) {
        Ok(only) => only,
        Err(e) => return protocol::error_frame(&format!("unreadable DIGEST: {e}")),
    };
    let mut out = Vec::new();
    digest(database, &only).await.encode(&mut out);
    protocol::plain_frame(FrameKind::DigestAck, 0, &out)
}

/// Whether the two digests describe the same moment: nothing moved on
/// either node while it read, and each has applied all the other released.
fn in_step(here: &NodeDigest, there: &NodeDigest) -> bool {
    match (here.own(), there.own()) {
        (Some(ours), Some(theirs)) => {
            there.applied(here.node_id) == Some(ours) && here.applied(there.node_id) == Some(theirs)
        }
        _ => false,
    }
}

/// What one comparison found.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Verdict {
    pub in_step: bool,
    pub compared: usize,
    /// Tables that differ, with both sides' figures.
    pub different: Vec<String>,
    /// Tables only one node has.
    pub missing: Vec<String>,
    /// Tables whose contents were not read on one side or the other.
    pub unread: Vec<String>,
}

fn compare(here: &NodeDigest, there: &NodeDigest, peer: &str) -> Verdict {
    let mut verdict = Verdict {
        in_step: in_step(here, there),
        ..Default::default()
    };
    for ours in &here.tables {
        let Some(theirs) = there.tables.iter().find(|t| t.name == ours.name) else {
            verdict.missing.push(format!("{}: here, not on {peer}", ours.name));
            continue;
        };
        verdict.compared += 1;
        if ours.rows != theirs.rows {
            verdict.different.push(format!(
                "{}: {} rows here, {} on {peer}",
                ours.name, ours.rows, theirs.rows
            ));
        } else if !ours.note.is_empty() || !theirs.note.is_empty() {
            verdict.unread.push(format!(
                "{}: {} rows on both; {}",
                ours.name,
                ours.rows,
                if ours.note.is_empty() { &theirs.note } else { &ours.note }
            ));
        } else if ours.checksum != theirs.checksum {
            verdict.different.push(format!(
                "{}: {} rows on both, and they are not the same rows (checksum {:016x} here, {:016x} on {peer})",
                ours.name, ours.rows, ours.checksum, theirs.checksum
            ));
        }
    }
    for theirs in &there.tables {
        if !here.tables.iter().any(|t| t.name == theirs.name) {
            verdict.missing.push(format!("{}: on {peer}, not here", theirs.name));
        }
    }
    verdict
}

/// Compares this node with `peer`, table by table. Returns the verdict and
/// the lines a `REPLICATION CHECK` shows for it.
pub async fn check(
    database: &Arc<Database>,
    peer: &std::sync::Arc<PeerHandle>,
    only: &[String],
) -> Result<(Verdict, Vec<String>), String> {
    let replication = database.replication.clone();
    let mut request = Vec::new();
    protocol::encode_digest_request(only, &mut request);

    let mut last = None;
    for attempt in 0..TRIES {
        if attempt > 0 {
            tokio::time::sleep(PAUSE).await;
        }
        // At the same time, so the two reads overlap as far as they can.
        let (here, reply) = tokio::join!(
            digest(database, only),
            super::peer::ask(&replication, peer, FrameKind::Digest, &request)
        );
        let (kind, body) = reply?;
        if kind != FrameKind::DigestAck {
            return Err(format!("{} answered a DIGEST with {}", peer.name, kind));
        }
        let there = NodeDigest::decode(&mut Reader::new(&body))
            .map_err(|e| format!("{} sent an unreadable DIGEST-ACK: {}", peer.name, e))?;
        let verdict = compare(&here, &there, &peer.name);
        let settled = verdict.in_step;
        last = Some((here, there, verdict));
        if settled {
            break;
        }
    }
    let (here, there, verdict) = last.expect("at least one try");

    let mut lines = Vec::new();
    if verdict.in_step {
        lines.push(format!(
            "node {} and {} (node {}) are in step: each has applied all the other has committed \
             ({} from here, {} from there)",
            here.node_id, peer.name, there.node_id, here.released.1, there.released.1
        ));
    } else {
        lines.push(format!(
            "node {} and {} (node {}) were NOT in step in {} tries: writes are in flight or a \
             stream is behind (here released {}, {} has applied {}; there released {}, applied here {}). \
             What follows may differ for that reason alone; run it again when the nodes are quiet",
            here.node_id,
            peer.name,
            there.node_id,
            TRIES,
            here.released.1,
            peer.name,
            there.applied(here.node_id).map(|s| s.to_string()).unwrap_or_else(|| "a moving position".to_string()),
            there.released.1,
            here.applied(there.node_id).map(|s| s.to_string()).unwrap_or_else(|| "a moving position".to_string()),
        ));
    }
    let clean = verdict.different.is_empty() && verdict.missing.is_empty();
    lines.push(format!(
        "{} table(s) compared: {}",
        verdict.compared,
        if clean && verdict.unread.is_empty() {
            "the same rows on both".to_string()
        } else if clean {
            format!("no difference found, {} not fully read", verdict.unread.len())
        } else {
            format!(
                "{} DIFFERENT, {} on one node only",
                verdict.different.len(),
                verdict.missing.len()
            )
        }
    ));
    for line in &verdict.different {
        lines.push(format!("DIFFERENT  {line}"));
    }
    for line in &verdict.missing {
        lines.push(format!("MISSING    {line}"));
    }
    for line in &verdict.unread {
        lines.push(format!("not read   {line}"));
    }
    Ok((verdict, lines))
}
