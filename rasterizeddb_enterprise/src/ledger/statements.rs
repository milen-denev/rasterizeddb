//! The ledger statements (docs/ledger-and-vault-plan.md §3.1, §3.5):
//!
//! ```sql
//! VERIFY LEDGER [table]
//! SHOW LEDGER [table]
//! ALTER LEDGER TABLE t SET RETENTION <n> <unit>
//! ALTER LEDGER TABLE t SET NO DROP UNTIL <n> <unit> IDLE
//! ALTER LEDGER TABLE t SET LEGAL HOLD '<name>'
//! ALTER LEDGER TABLE t RELEASE LEGAL HOLD '<name>'
//! ALTER LEDGER TABLE t ADD ANCHOR '<sink>'
//! ACKNOWLEDGE LEDGER ROLLBACK '<reason>'
//! ```
//!
//! Core reads the tables (and, for `VERIFY`, every visible row) and checks
//! who may run an `ALTER` or `ACKNOWLEDGE`; this decides what it means.

use std::collections::{HashMap, HashSet};

use rasterizeddb_core::core::ledger_hooks::{LedgerReply, LedgerRequest};
use rasterizeddb_core::core::row::schema::SpecialTable;

use super::store::{self, Item, OP_DELETE, OP_EVENT, OP_INSERT, OP_UPDATE};
use super::{LedgerGuard, anchor, clauses, clock, policy};

/// Words, with quoted strings kept whole: `'…'` stays one word starting
/// with `'`, its closing quote dropped and `''` unescaped.
fn words(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = sql.trim().trim_end_matches(';').chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else if c == '\'' {
            chars.next();
            let mut v = String::from("'");
            while let Some(ch) = chars.next() {
                if ch == '\'' {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                        v.push('\'');
                        continue;
                    }
                    break;
                }
                v.push(ch);
            }
            out.push(v);
        } else {
            let mut w = String::new();
            while let Some(&ch) = chars.peek() {
                if ch.is_whitespace() || ch == '\'' {
                    break;
                }
                w.push(ch);
                chars.next();
            }
            out.push(w);
        }
    }
    out
}

fn quoted(w: Option<&String>) -> Result<String, String> {
    match w.and_then(|w| w.strip_prefix('\'')) {
        Some(v) if !v.is_empty() && !v.chars().any(|c| c.is_whitespace() || c.is_control()) => Ok(v.to_string()),
        _ => Err("42601: expected a quoted name with no spaces".to_string()),
    }
}

pub fn run(g: &LedgerGuard, store_key: &str, req: LedgerRequest<'_>) -> Result<LedgerReply, String> {
    let w = words(req.sql);
    let up: Vec<String> = w.iter().map(|x| x.to_ascii_uppercase()).collect();
    let at = |i: usize| up.get(i).map(String::as_str).unwrap_or("");
    match at(0) {
        "VERIFY" => verify(g, store_key, &req),
        #[cfg(feature = "vault")]
        "VAULT" => crate::vault::run(g, store_key, &req),
        #[cfg(not(feature = "vault"))]
        "VAULT" => Err("0A000: this build has no secret vaults (feature `vault`)".to_string()),
        "SHOW" => show(g, store_key, &req),
        "ALTER" => alter(g, store_key, &req, &w, &up),
        "ACKNOWLEDGE" => {
            if at(2) != "ROLLBACK" {
                return Err("42601: expected ACKNOWLEDGE LEDGER ROLLBACK '<reason>'".to_string());
            }
            let reason = w.get(3).and_then(|r| r.strip_prefix('\'')).unwrap_or("").to_string();
            if reason.trim().is_empty() {
                return Err("42601: ACKNOWLEDGE LEDGER ROLLBACK needs a reason".to_string());
            }
            let lines = anchor::acknowledge_rollback(store_key, &reason, req.role)?;
            Ok(LedgerReply { lines, records: Vec::new(), inserts: Vec::new() })
        }
        _ => Err("42601: not a ledger statement".to_string()),
    }
}

fn verify(g: &LedgerGuard, store_key: &str, req: &LedgerRequest<'_>) -> Result<LedgerReply, String> {
    let chain = store::chain(store_key)?;
    let read = chain.lock().reread()?;
    let mut lines = Vec::new();
    match &read.broken {
        Some((seq, why)) => lines.push(format!("TAMPERED  chain: {why} (first bad entry {seq})")),
        None => lines.push(format!(
            "OK        chain: {} entries, head {}",
            read.entries.len(),
            read.entries.last().map_or_else(|| "(empty)".to_string(), |e| store::hex(&e.hash))
        )),
    }
    // Every verified table's own sinks: they reached the store's list only
    // as rows were written, so after a restart VERIFY checked none of them
    // until a new row committed, and a chain cut back while the server was
    // down read as "no anchors".
    for (table, _) in &req.tables {
        if let Ok(policy) = g.policy(table) {
            anchor::add_sinks(store_key, &policy.anchors);
        }
    }
    lines.extend(anchor::verify_lines(store_key, &read));
    for (table, rows) in &req.tables {
        let policy = g.policy(table)?;
        // Every row the chain says was inserted, and whether it was deleted.
        let mut chained: HashMap<i64, (u32, store::Hash, bool)> = HashMap::new();
        for e in &read.entries {
            // An entry whose transaction aborted after it was written: its
            // rows never committed.
            if req.aborted.is_some_and(|aborted| aborted(e.txid)) {
                continue;
            }
            for i in e.items.iter().filter(|i| i.ledger == policy.id) {
                match i.op {
                    OP_INSERT => {
                        chained.insert(i.row_ts, (i.width, i.leaf, false));
                    }
                    OP_UPDATE => {
                        chained.insert(i.row_ts, (i.width, i.leaf, false));
                    }
                    OP_DELETE => {
                        chained.entry(i.row_ts).or_insert((0, [0; 32], true)).2 = true;
                    }
                    _ => {}
                }
            }
        }
        let mut problems: Vec<String> = Vec::new();
        let mut matched = 0u64;
        let mut seen = HashSet::new();
        for (ts, cells) in rows {
            // A stamp names one row: a second row with it is a copy made past
            // the guard, which matched the chain twice and read as OK.
            if !seen.insert(*ts) {
                problems.push(format!("row stamped {ts} appears more than once: a copy written past the guard"));
                continue;
            }
            let refs: Vec<(u32, &[u8])> = cells.iter().map(|(o, b)| (*o, b.as_slice())).collect();
            match chained.get(ts) {
                None => problems.push(format!("row stamped {ts} is not in the chain: written past the guard")),
                Some((_, _, true)) => problems.push(format!("row stamped {ts} was deleted in the chain but is present")),
                Some((width, leaf, false)) => {
                    if store::leaf_upto(policy.id, *ts, *width, &refs) == *leaf {
                        matched += 1;
                    } else {
                        problems.push(format!("row stamped {ts} differs from what was committed"));
                    }
                }
            }
        }
        for (ts, (_, _, deleted)) in &chained {
            if !deleted && !seen.contains(ts) {
                problems.push(format!("row stamped {ts} is missing, and the chain records no deletion of it"));
            }
        }
        if problems.is_empty() {
            lines.push(format!("OK        {}: {matched} row(s) match the chain", table.name));
        } else {
            lines.push(format!("TAMPERED  {}: {} problem(s), {matched} row(s) match", table.name, problems.len()));
            problems.sort();
            for p in problems.iter().take(50) {
                lines.push(format!("          {p}"));
            }
            if problems.len() > 50 {
                lines.push(format!("          … and {} more", problems.len() - 50));
            }
        }
    }
    Ok(LedgerReply { lines, records: Vec::new(), inserts: Vec::new() })
}

/// A provider URI as shown: never a PIN or a secret parameter.
pub fn redact_provider(k: &str) -> String {
    k.split(';')
        .map(|part| {
            let lower = part.to_ascii_lowercase();
            if lower.contains("pin") || lower.contains("secret") || lower.contains("password") {
                format!("{}=…", part.split('=').next().unwrap_or(""))
            } else {
                part.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(";")
}

/// `SHOW LEDGER RECEIPT <table> <row stamp>`: the proof that a row's leaf is
/// in the chain, up to the newest head every sink has anchored (or the head,
/// unanchored). Lines:
///
/// ```text
/// receipt rdb-v1
/// row <ledger id hex> <stamp> <leaf hex>
/// entry <seq> <prev hex> <payload hex>
/// after <payload digest hex>        (one per later entry)
/// head <seq> <hash hex> <anchored|unanchored>
/// ```
///
/// `rasterizeddb_client_enterprise::verify_receipt` checks one against a head
/// the caller got elsewhere (an anchor it read itself).
fn receipt(g: &LedgerGuard, store_key: &str, req: &LedgerRequest<'_>, ts: i64) -> Result<LedgerReply, String> {
    let Some((table, _)) = req.tables.first() else {
        return Err("42P01: SHOW LEDGER RECEIPT names no ledger table".to_string());
    };
    let policy = g.policy(table)?;
    let chain = store::chain(store_key)?;
    let read = chain.lock().reread()?;
    if let Some((seq, why)) = &read.broken {
        return Err(format!("XX001: the chain is broken at entry {seq}: {why}"));
    }
    let Some((at, item)) = read
        .entries
        .iter()
        .enumerate()
        .find_map(|(i, e)| e.items.iter().find(|it| it.op == OP_INSERT && it.ledger == policy.id && it.row_ts == ts).map(|it| (i, it.clone())))
    else {
        return Err(format!("42704: no row of \"{}\" stamped {ts} is in the chain", table.name));
    };
    let entry = &read.entries[at];
    let anchored = anchor::anchored_seq(store_key).unwrap_or(0);
    let end = if anchored as usize > at { (anchored as usize).min(read.entries.len()) } else { read.entries.len() };
    let mut lines = vec![
        "receipt rdb-v1".to_string(),
        format!("row {:032x} {ts} {}", policy.id, store::hex(&item.leaf)),
        format!("entry {} {} {}", entry.seq, store::hex(&entry.prev), store::hex(&entry.payload)),
    ];
    for e in &read.entries[at + 1..end] {
        lines.push(format!("after {}", store::hex(&store::payload_digest(&e.payload))));
    }
    let head = &read.entries[end - 1];
    lines.push(format!(
        "head {} {} {}",
        head.seq,
        store::hex(&head.hash),
        if anchored >= head.seq { "anchored" } else { "unanchored" }
    ));
    Ok(LedgerReply { lines, records: Vec::new(), inserts: Vec::new() })
}

/// A cell as text, by its column's type: integers and floats as numbers,
/// text as quoted text, anything else as hex.
fn cell_text(ty: Option<&rasterizeddb_core::core::db_type::DbType>, b: &[u8]) -> String {
    use rasterizeddb_core::core::db_type::DbType as T;
    let int = |n: usize| -> Option<i128> {
        let s = b.get(..n)?;
        let mut w = [0u8; 16];
        w[..n].copy_from_slice(s);
        // Sign-extend.
        if s[n - 1] & 0x80 != 0 {
            for x in &mut w[n..] {
                *x = 0xff;
            }
        }
        Some(i128::from_le_bytes(w))
    };
    let uint = |n: usize| -> Option<u128> {
        let mut w = [0u8; 16];
        w[..n].copy_from_slice(b.get(..n)?);
        Some(u128::from_le_bytes(w))
    };
    let shown = match ty {
        Some(T::I8) => int(1).map(|v| v.to_string()),
        Some(T::I16) => int(2).map(|v| v.to_string()),
        Some(T::I32) => int(4).map(|v| v.to_string()),
        Some(T::I64) => int(8).map(|v| v.to_string()),
        Some(T::I128) => int(16).map(|v| v.to_string()),
        Some(T::U8) => uint(1).map(|v| v.to_string()),
        Some(T::U16) => uint(2).map(|v| v.to_string()),
        Some(T::U32) => uint(4).map(|v| v.to_string()),
        Some(T::U64) => uint(8).map(|v| v.to_string()),
        Some(T::U128) => uint(16).map(|v| v.to_string()),
        Some(T::F64) => b.get(..8).map(|x| f64::from_le_bytes(x.try_into().unwrap()).to_string()),
        Some(T::F32) => b.get(..4).map(|x| f32::from_le_bytes(x.try_into().unwrap()).to_string()),
        Some(T::BOOL) => b.first().map(|x| (*x != 0).to_string()),
        Some(T::STRING | T::CHAR) => {
            let end = b.iter().rposition(|x| *x != 0).map_or(0, |i| i + 1);
            Some(format!("'{}'", String::from_utf8_lossy(&b[..end])))
        }
        _ => None,
    };
    shown.unwrap_or_else(|| format!("0x{}", store::hex(b)))
}

/// `SHOW LEDGER HISTORY <table> <stamp>`: every version of one row still on
/// disk, oldest first, each with whether the chain holds its digest.
fn history(g: &LedgerGuard, store_key: &str, req: &LedgerRequest<'_>) -> Result<LedgerReply, String> {
    let Some((table, _)) = req.tables.first() else {
        return Err("42P01: SHOW LEDGER HISTORY names no ledger table".to_string());
    };
    let policy = g.policy(table)?;
    let ts: i64 = req.sql.split_whitespace().nth(4).and_then(|x| x.trim_end_matches(';').parse().ok()).ok_or("42601: SHOW LEDGER HISTORY <table> <row stamp>")?;
    let chain = store::chain(store_key)?;
    let read = chain.lock().reread()?;
    let chained: Vec<(u32, store::Hash)> = read
        .entries
        .iter()
        .flat_map(|e| e.items.iter())
        .filter(|i| i.ledger == policy.id && i.row_ts == ts && (i.op == OP_INSERT || i.op == OP_UPDATE))
        .map(|i| (i.width, i.leaf))
        .collect();
    let names: std::collections::HashMap<u32, (&str, &rasterizeddb_core::core::db_type::DbType)> =
        table.fields.iter().map(|f| (f.write_order as u32, (f.name.as_str(), &f.db_type))).collect();
    let mut lines = Vec::new();
    for (n, (xmin, xmax, cells)) in req.versions.iter().enumerate() {
        let refs: Vec<(u32, &[u8])> = cells.iter().map(|(o, b)| (*o, b.as_slice())).collect();
        let ok = chained.iter().any(|(w, leaf)| store::leaf_upto(policy.id, ts, *w, &refs) == *leaf);
        let values: Vec<String> = cells
            .iter()
            .filter(|(o, _)| names.get(o).is_some_and(|(n, _)| !n.starts_with("__")))
            .map(|(o, b)| {
                let (n, ty) = names.get(o).map_or(("?", None), |(n, t)| (*n, Some(*t)));
                format!("{n}={}", cell_text(ty, b))
            })
            .collect();
        lines.push(format!(
            "v{} {} xmin {xmin}{} {}",
            n + 1,
            if ok { "chained  " } else { "UNCHAINED" },
            if *xmax == 0 { String::new() } else { format!(" xmax {xmax}") },
            values.join(" ")
        ));
    }
    if lines.is_empty() {
        lines.push(format!("no version of the row stamped {ts} is on disk"));
    }
    if !policy.versioned {
        lines.push("note: an append-only table's row has one version; VERSIONED tables keep every one".to_string());
    }
    Ok(LedgerReply { lines, records: Vec::new(), inserts: Vec::new() })
}

fn show(g: &LedgerGuard, store_key: &str, req: &LedgerRequest<'_>) -> Result<LedgerReply, String> {
    let w: Vec<String> = req.sql.split_whitespace().map(|x| x.trim_end_matches(';').to_string()).collect();
    if w.get(2).is_some_and(|x| x.eq_ignore_ascii_case("HISTORY")) {
        return history(g, store_key, req);
    }
    if w.get(2).is_some_and(|x| x.eq_ignore_ascii_case("RECEIPT")) {
        let ts: i64 = w.get(4).and_then(|x| x.parse().ok()).ok_or("42601: SHOW LEDGER RECEIPT <table> <row stamp>")?;
        return receipt(g, store_key, req, ts);
    }
    let chain = store::chain(store_key)?;
    let (seq, head, path) = {
        let c = chain.lock();
        (c.seq, c.head, c.path.clone())
    };
    let mut lines = vec![format!(
        "chain: {} entries, head {}, at {}",
        seq,
        store::hex(&head),
        path.map_or_else(|| "memory".to_string(), |p| p.display().to_string())
    )];
    if store::same_device(store_key) == Some(true) {
        lines.push("WARNING: the chain is on the same device as the data".to_string());
    }
    lines.extend(anchor::show_lines(store_key));
    #[cfg(feature = "vault")]
    lines.extend(crate::columns::show_lines(store_key));
    for (table, _) in &req.tables {
        let p = g.policy(table)?;
        lines.push(format!(
            "{}: {} id {:032x}, retention {} s, drop after {} s idle, hash {}{}{}{}{}",
            table.name,
            if p.kind == SpecialTable::Vault { "vault" } else { "ledger" },
            p.id,
            p.retention_us / 1_000_000,
            p.drop_idle_us / 1_000_000,
            p.hash.name(),
            if p.versioned { ", versioned" } else { "" },
            if p.anchors.is_empty() { String::new() } else { format!(", anchors {}", p.anchors.join(" ")) },
            if p.holds.is_empty() {
                String::new()
            } else {
                format!(", holds {}", p.holds.keys().cloned().collect::<Vec<_>>().join(" "))
            },
            p.key_provider.as_ref().map_or_else(String::new, |k| format!(", key provider {}", redact_provider(k))),
        ));
    }
    Ok(LedgerReply { lines, records: Vec::new(), inserts: Vec::new() })
}

fn alter(
    g: &LedgerGuard,
    store_key: &str,
    req: &LedgerRequest<'_>,
    w: &[String],
    up: &[String],
) -> Result<LedgerReply, String> {
    let Some((table, _)) = req.tables.first() else {
        return Err("42P01: ALTER LEDGER TABLE names no ledger table".to_string());
    };
    let current = g.policy(table)?;
    let rest: Vec<&str> = up.iter().skip(4).map(String::as_str).collect();
    let tail = |from: usize| w.get(from..).map(|s| s.join(" ")).unwrap_or_default();
    let now = clock::now_micros();
    let (payload, event) = match rest.as_slice() {
        ["SET", "RETENTION", ..] => {
            let p = clauses::parse(SpecialTable::Ledger, &format!("NO DELETE UNTIL {} AFTER INSERT", tail(6)))
                .map_err(|e| format!("42601: {e}"))?;
            if p.retention_us < current.retention_us {
                return Err(format!(
                    "55000: a ledger retention is only ever raised: \"{}\" keeps rows {} s",
                    table.name,
                    current.retention_us / 1_000_000
                ));
            }
            (format!("v1\nretention_us {}\n", p.retention_us), format!("retention {} s", p.retention_us / 1_000_000))
        }
        ["SET", "NO", "DROP", "UNTIL", ..] => {
            let p = clauses::parse(
                SpecialTable::Ledger,
                &format!("NO DELETE UNTIL 0 SECONDS AFTER INSERT NO DROP UNTIL {}", tail(8)),
            )
            .map_err(|e| format!("42601: {e}"))?;
            if p.drop_idle_us < current.drop_idle_us {
                return Err("55000: a drop-idle time is only ever raised".to_string());
            }
            (format!("v1\ndrop_idle_us {}\n", p.drop_idle_us), format!("drop idle {} s", p.drop_idle_us / 1_000_000))
        }
        ["SET", "LEGAL", "HOLD", _] => {
            let name = quoted(w.get(7))?;
            (format!("v1\nhold {name} set {now}\n"), format!("legal hold {name} placed"))
        }
        ["RELEASE", "LEGAL", "HOLD", _] => {
            let name = quoted(w.get(7))?;
            if !current.holds.contains_key(&name) {
                return Err(format!("42704: \"{}\" has no legal hold \"{name}\"", table.name));
            }
            (format!("v1\nhold {name} release {now}\n"), format!("legal hold {name} released"))
        }
        ["ADD", "ANCHOR", _] => {
            let sink = quoted(w.get(6))?;
            clauses::parse(SpecialTable::Ledger, &format!("NO DELETE UNTIL 0 SECONDS AFTER INSERT ANCHOR TO '{sink}'"))
                .map_err(|e| format!("42601: {e}"))?;
            (format!("v1\nanchor {sink}\n"), format!("anchor {sink} added"))
        }
        _ => {
            return Err("42601: ALTER LEDGER TABLE takes SET RETENTION, SET NO DROP UNTIL … IDLE, SET LEGAL HOLD, \
                        RELEASE LEGAL HOLD or ADD ANCHOR"
                .to_string());
        }
    };
    let record = policy::record(&payload);
    // Checked as the next open will fold it.
    let mut all = table.ledger_policy.clone();
    all.extend_from_slice(&g.overlay.read().get(&current.id).cloned().unwrap_or_default());
    all.extend_from_slice(&record);
    policy::fold(&all, table.special).map_err(|e| format!("55000: {e}"))?;
    // The change joins the chain, so it is as tamper-evident as the rows.
    let text = format!("{}: {event} by {}", table.name, if req.role.is_empty() { "?" } else { req.role });
    let item = Item { op: OP_EVENT, ledger: current.id, row_ts: now, width: 0, leaf: store::event_leaf(&text) };
    let chain = store::chain(store_key)?;
    let hash = chain.lock().append(0, &[item])?;
    anchor::note_append(store_key, &chain, hash);
    g.overlay.write().entry(current.id).or_default().extend_from_slice(&record);
    Ok(LedgerReply { lines: vec![text], records: vec![(table.name.clone(), record)], inserts: Vec::new() })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn words_keep_quoted_values_whole() {
        assert_eq!(
            words("ALTER LEDGER TABLE t SET LEGAL HOLD 'case 1''s';"),
            vec!["ALTER", "LEDGER", "TABLE", "t", "SET", "LEGAL", "HOLD", "'case 1's"]
        );
    }

    #[test]
    fn a_provider_shows_without_its_pin() {
        assert_eq!(
            redact_provider("pkcs11:module=/x.so;token=rdb;pin-source=/etc/pin;key=k"),
            "pkcs11:module=/x.so;token=rdb;pin-source=…;key=k"
        );
    }
}
