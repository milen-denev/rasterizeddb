//! Top-N across shards for a statement sent with the extended protocol
//! (`Parse`, `Bind`, `Describe`, `Execute`, `Sync`), as drivers send every
//! statement (sharding-plan.md B1c).
//!
//! A simple query's top-N keys each row by its key columns' text. A driver
//! asks for its results in binary as often as not, so here the statement each
//! shard runs is rewritten instead: every `ORDER BY` key is appended to the
//! select list as one more column (`__rdb_k1`, …), and those columns alone
//! are asked for as text. The sort keys come from them; the client's own
//! columns keep the formats it asked for, and the appended ones are cut from
//! every row and from the description before anything is sent on.
//!
//! `LIMIT $n` and `OFFSET $m` take their bound values (each shard runs `LIMIT
//! limit + offset` and no `OFFSET`), and the parameters left are numbered
//! again from `$1`, so the Bind each shard gets carries exactly what its
//! statement uses.

use super::topn::{self, Key, KeyRef, TopN};
use crate::core::sql_scan::{skip_comment, skip_quoted};

/// One extended-protocol statement, as a client's batch carries it.
#[derive(Clone, Debug, PartialEq)]
pub struct Statement {
    pub sql: String,
    /// The Parse message's parameter type OIDs.
    pub types: Vec<u32>,
    /// Each bound parameter: its format code and value (`None`: NULL).
    pub params: Vec<(i16, Option<Vec<u8>>)>,
    /// The Bind's result format codes, as sent.
    pub results: Vec<i16>,
    /// Whether the batch asked for the portal's description.
    pub describe: bool,
    /// A simple Query run this way (its reply has no Parse or Bind
    /// acknowledgement, and always its description).
    pub simple: bool,
}

impl Statement {
    /// A simple Query's statement: no parameters, text results.
    pub fn simple(sql: &str) -> Statement {
        Statement { sql: sql.trim().trim_end_matches(';').to_string(), types: Vec::new(), params: Vec::new(), results: Vec::new(), describe: true, simple: true }
    }
}

/// The statement of a batch that is exactly `Parse Bind [Describe] Execute
/// Sync` (the gateway's normalised form), or `None`.
pub fn statement(bytes: &[u8]) -> Option<Statement> {
    let mut msgs = Vec::new();
    let mut i = 0;
    while i + 5 <= bytes.len() {
        let len = i32::from_be_bytes(bytes[i + 1..i + 5].try_into().ok()?) as usize;
        msgs.push((bytes[i], bytes.get(i + 5..i + 1 + len)?));
        i += 1 + len;
    }
    let tags: Vec<u8> = msgs.iter().map(|(t, _)| *t).collect();
    let describe = match tags.as_slice() {
        [b'P', b'B', b'E', b'S'] => false,
        [b'P', b'B', b'D', b'E', b'S'] => msgs[2].1.first() == Some(&b'P'),
        _ => return None,
    };
    let (sql, types) = parse_message(msgs[0].1)?;
    let (params, results) = bind_message(msgs[1].1)?;
    Some(Statement { sql, types, params, results, describe, simple: false })
}

fn cstr(b: &[u8]) -> Option<(&[u8], &[u8])> {
    let end = b.iter().position(|&c| c == 0)?;
    Some((&b[..end], &b[end + 1..]))
}

fn i16_at(b: &[u8], at: usize) -> Option<i16> {
    Some(i16::from_be_bytes(b.get(at..at + 2)?.try_into().ok()?))
}

fn parse_message(body: &[u8]) -> Option<(String, Vec<u32>)> {
    let (_, rest) = cstr(body)?;
    let (q, rest) = cstr(rest)?;
    let n = i16_at(rest, 0)? as usize;
    let types = (0..n)
        .map(|k| rest.get(2 + 4 * k..6 + 4 * k).map(|x| u32::from_be_bytes(x.try_into().unwrap())))
        .collect::<Option<Vec<_>>>()?;
    Some((String::from_utf8(q.to_vec()).ok()?, types))
}

#[allow(clippy::type_complexity)]
fn bind_message(body: &[u8]) -> Option<(Vec<(i16, Option<Vec<u8>>)>, Vec<i16>)> {
    let (_, rest) = cstr(body)?;
    let (_, rest) = cstr(rest)?;
    let nf = i16_at(rest, 0)? as usize;
    let formats = (0..nf).map(|k| i16_at(rest, 2 + 2 * k)).collect::<Option<Vec<_>>>()?;
    let mut at = 2 + 2 * nf;
    let np = i16_at(rest, at)? as usize;
    at += 2;
    let mut params = Vec::with_capacity(np);
    for k in 0..np {
        let len = i32::from_be_bytes(rest.get(at..at + 4)?.try_into().ok()?);
        at += 4;
        let value = if len < 0 {
            None
        } else {
            let v = rest.get(at..at + len as usize)?.to_vec();
            at += len as usize;
            Some(v)
        };
        let f = match nf {
            0 => 0,
            1 => formats[0],
            _ => *formats.get(k)?,
        };
        params.push((f, value));
    }
    let nr = i16_at(rest, at)? as usize;
    let results = (0..nr).map(|k| i16_at(rest, at + 2 + 2 * k)).collect::<Option<Vec<_>>>()?;
    Some((params, results))
}

fn put(out: &mut Vec<u8>, tag: u8, body: &[u8]) {
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
}

/// `Parse` (unnamed) for `sql` and `types`.
pub fn parse_msg(out: &mut Vec<u8>, sql: &str, types: &[u32]) {
    let mut b = Vec::with_capacity(sql.len() + 4 + 4 * types.len());
    b.push(0);
    b.extend_from_slice(sql.as_bytes());
    b.push(0);
    b.extend_from_slice(&(types.len() as i16).to_be_bytes());
    for t in types {
        b.extend_from_slice(&t.to_be_bytes());
    }
    put(out, b'P', &b);
}

/// `Bind` (unnamed portal and statement), every format code written out.
pub fn bind_msg(out: &mut Vec<u8>, params: &[(i16, Option<Vec<u8>>)], results: &[i16]) {
    let mut b = vec![0, 0];
    b.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for (f, _) in params {
        b.extend_from_slice(&f.to_be_bytes());
    }
    b.extend_from_slice(&(params.len() as i16).to_be_bytes());
    for (_, v) in params {
        match v {
            Some(v) => {
                b.extend_from_slice(&(v.len() as i32).to_be_bytes());
                b.extend_from_slice(v);
            }
            None => b.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    b.extend_from_slice(&(results.len() as i16).to_be_bytes());
    for r in results {
        b.extend_from_slice(&r.to_be_bytes());
    }
    put(out, b'B', &b);
}

/// `Describe` of the unnamed statement (`b'S'`) or portal (`b'P'`).
pub fn describe_msg(out: &mut Vec<u8>, kind: u8) {
    put(out, b'D', &[kind, 0]);
}

pub fn execute_sync(out: &mut Vec<u8>) {
    put(out, b'E', &[0, 0, 0, 0, 0]);
    put(out, b'S', &[]);
}

/// The top-N each shard runs for an extended statement.
#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    /// The rewritten statement: the keys appended to its select list,
    /// `LIMIT limit + offset`, parameters renumbered.
    pub sql: String,
    pub types: Vec<u32>,
    pub params: Vec<(i16, Option<Vec<u8>>)>,
    /// Keys over the appended columns, by their names.
    pub keys: Vec<Key>,
    /// How many columns were appended (the last ones of every row).
    pub extras: usize,
    pub limit: u64,
    pub offset: u64,
}

/// The name of the `i`-th appended key column.
pub fn extra_name(i: usize) -> String {
    format!("__rdb_k{}", i + 1)
}

/// The plan for `st`, a top-N statement; `None` when it is not one (no
/// top-level `ORDER BY`); `Some(Err)` for one that cannot be merged (a key
/// that is not an output column, a parameter `LIMIT` that is not a whole
/// number).
pub fn plan(st: &Statement) -> Option<Result<Plan, String>> {
    // LIMIT $n / OFFSET $m take their bound values.
    let (sql, _) = match bind_limits(&st.sql, &st.params) {
        Ok(v) => v?,
        Err(e) => return Some(Err(e)),
    };
    let topn: TopN = match topn::parse(&sql)? {
        Ok(t) => t,
        Err(e) => return Some(Err(e)),
    };
    Some(plan_from(st, topn))
}

fn plan_from(st: &Statement, topn: TopN) -> Result<Plan, String> {
    let (with_keys, keys) = append_keys(&topn.part_sql, &topn.keys)?;
    let (sql, orig) = renumber(&with_keys).ok_or("0A000: a parameter number past 65535")?;
    let types = orig.iter().map(|&n| st.types.get(n - 1).copied().unwrap_or(0)).collect();
    let params = orig
        .iter()
        .map(|&n| st.params.get(n - 1).cloned().ok_or_else(|| format!("08P01: the statement uses ${n}, which the Bind does not carry")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Plan { sql, types, params, extras: keys.len(), keys, limit: topn.limit, offset: topn.offset })
}

/// The result format codes for the rewritten statement's `total` columns:
/// the client's for its own, text for the appended `extras`.
pub fn result_formats(client: &[i16], total: usize, extras: usize) -> Vec<i16> {
    let own = total.saturating_sub(extras);
    let mut out: Vec<i16> = (0..own)
        .map(|i| match client.len() {
            0 => 0,
            1 => client[0],
            _ => client.get(i).copied().unwrap_or(0),
        })
        .collect();
    out.extend(std::iter::repeat_n(0, extras));
    out
}

/// Whether the client asked for anything but text (so the column count is
/// needed to write the format codes out).
pub fn needs_count(client: &[i16]) -> bool {
    client.iter().any(|&f| f != 0)
}

/// `LIMIT $n` and `OFFSET $m` after the top-level `ORDER BY` replaced by
/// their bound values; `Ok(None)` when the statement has no top-level
/// `ORDER BY`.
fn bind_limits(sql: &str, params: &[(i16, Option<Vec<u8>>)]) -> Result<Option<(String, Vec<usize>)>, String> {
    let words = topn::top_level_words(sql);
    let Some(order) = words.iter().position(|(w, _)| w == "ORDER") else {
        return Ok(None);
    };
    let mut out = sql.to_string();
    let mut used = Vec::new();
    // From the end, so earlier offsets stay right.
    for (w, at) in words[order..].iter().rev() {
        if w != "LIMIT" && w != "OFFSET" {
            continue;
        }
        let after = at + w.len();
        let rest = &sql[after..];
        let lead = rest.len() - rest.trim_start().len();
        let tok = &rest[lead..];
        let Some(digits) = tok.strip_prefix('$') else { continue };
        let len = digits.bytes().take_while(u8::is_ascii_digit).count();
        let n: usize = digits[..len].parse().map_err(|_| format!("0A000: {w} ${} is not a parameter", &digits[..len]))?;
        let (fmt, value) = params.get(n.wrapping_sub(1)).ok_or_else(|| format!("08P01: {w} ${n} is not bound"))?;
        let v = match (fmt, value.as_deref()) {
            (_, None) => return Err(format!("0A000: {w} NULL is not merged across shards")),
            (0, Some(t)) => std::str::from_utf8(t).ok().and_then(|t| t.trim().parse::<u64>().ok()),
            (_, Some(b)) if b.len() == 8 => i64::try_from(u64::from_be_bytes(b.try_into().unwrap())).ok().and_then(|v| u64::try_from(v).ok()),
            (_, Some(b)) if b.len() == 4 => u64::try_from(i32::from_be_bytes(b.try_into().unwrap())).ok(),
            (_, Some(b)) if b.len() == 2 => u64::try_from(i16::from_be_bytes(b.try_into().unwrap())).ok(),
            _ => None,
        }
        .ok_or_else(|| format!("22023: {w} ${n} is not a whole number of rows"))?;
        let start = after + lead;
        out.replace_range(start..start + 1 + len, &v.to_string());
        used.push(n);
    }
    Ok(Some((out, used)))
}

/// Splits `list` (a select list) at its top-level commas.
fn items(list: &str) -> Vec<&str> {
    let b = list.as_bytes();
    let (mut depth, mut i, mut start) = (0i32, 0usize, 0usize);
    let mut out = Vec::new();
    while i < b.len() {
        if let Some(end) = skip_comment(b, i).or_else(|| skip_quoted(b, i)) {
            i = end.max(i + 1);
            continue;
        }
        match b[i] {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth -= 1,
            b',' if depth == 0 => {
                out.push(list[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(list[start..].trim());
    out
}

/// An item's expression and its alias (`expr AS a`, `expr a`), if any.
fn split_alias(item: &str) -> (&str, Option<String>) {
    let words = topn::top_level_words(item);
    if let Some((_, at)) = words.iter().rev().find(|(w, _)| w == "AS") {
        let alias = item[at + 2..].trim();
        return (item[..*at].trim(), Some(ident_name(alias)));
    }
    // A trailing bare identifier after an expression is an alias.
    let t = item.trim_end();
    let tail_start = if t.ends_with('"') {
        t[..t.len() - 1].rfind('"')
    } else {
        t.bytes().rposition(|b| !topn::is_ident_byte(b)).map(|p| p + 1)
    };
    if let Some(s) = tail_start
        && s > 0
        && t.as_bytes()[s - 1].is_ascii_whitespace()
    {
        let head = t[..s].trim_end();
        let last = head.as_bytes().last().copied().unwrap_or(b'+');
        let tail = &t[s..];
        let keyword = ["DESC", "ASC", "NULL", "TRUE", "FALSE", "END"].iter().any(|k| tail.eq_ignore_ascii_case(k));
        if !keyword && (topn::is_ident_byte(last) || matches!(last, b')' | b'\'' | b'"' | b']')) {
            return (head, Some(ident_name(tail)));
        }
    }
    (t, None)
}

/// An identifier as a name: unquoted and lower-cased, or quoted and kept.
fn ident_name(s: &str) -> String {
    let s = s.trim();
    match s.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        Some(q) => q.replace("\"\"", "\""),
        None => s.to_ascii_lowercase(),
    }
}

/// The statement with each key appended to its select list (`, expr AS
/// __rdb_kN`), and the keys over those columns.
fn append_keys(sql: &str, keys: &[Key]) -> Result<(String, Vec<Key>), String> {
    let words = topn::top_level_words(sql);
    let select = words.iter().find(|(w, _)| w == "SELECT").ok_or("0A000: a top-N without SELECT")?.1;
    let list_start = select + "SELECT".len();
    let from = words
        .iter()
        .filter(|(_, at)| *at > select)
        .find(|(w, _)| matches!(w.as_str(), "FROM" | "WHERE" | "GROUP" | "HAVING" | "WINDOW" | "ORDER" | "LIMIT"))
        .map(|(_, at)| *at)
        .ok_or("0A000: a top-N whose select list has no end")?;
    let list = &sql[list_start..from];
    let its = items(list);
    let parsed: Vec<(&str, Option<String>)> = its.iter().map(|i| split_alias(i)).collect();
    let mut appended = String::new();
    let mut out_keys = Vec::with_capacity(keys.len());
    for (i, k) in keys.iter().enumerate() {
        let expr: String = match &k.column {
            KeyRef::Ordinal(n) => {
                let (e, _) = parsed.get(n.wrapping_sub(1)).ok_or_else(|| format!("ORDER BY position {n} is not in select list"))?;
                if *e == "*" || e.ends_with(".*") {
                    return Err(format!("0A000: ORDER BY position {n} names a * column, which a prepared merge across shards does not resolve"));
                }
                e.to_string()
            }
            KeyRef::Name(name) => {
                if let Some((e, _)) = parsed.iter().find(|(_, a)| a.as_deref() == Some(name.as_str())) {
                    e.to_string()
                } else if let Some((e, _)) = parsed.iter().find(|(e, a)| {
                    a.is_none() && ident_name(e.rsplit('.').next().unwrap_or(e)) == *name && e.bytes().all(|b| topn::is_ident_byte(b) || b == b'.' || b == b'"')
                }) {
                    e.to_string()
                } else {
                    // A column of the table the list does not show (as
                    // PostgreSQL allows): fetched beside it. One that does
                    // not exist is the shard's own error.
                    format!("\"{}\"", name.replace('"', "\"\""))
                }
            }
        };
        appended.push_str(&format!(", {expr} AS {}", extra_name(i)));
        out_keys.push(Key { column: KeyRef::Name(extra_name(i)), ..k.clone() });
    }
    let mut out = String::with_capacity(sql.len() + appended.len());
    out.push_str(sql[..from].trim_end());
    out.push_str(&appended);
    out.push(' ');
    out.push_str(&sql[from..]);
    Ok((out, out_keys))
}

/// `sql` with its parameters numbered `$1…` in order of first use, and the
/// original number of each (see `rasterizeddb_gateway::split::renumber`).
fn renumber(sql: &str) -> Option<(String, Vec<usize>)> {
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut orig: Vec<usize> = Vec::new();
    let (mut i, mut copied) = (0usize, 0usize);
    while i < b.len() {
        if let Some(end) = skip_comment(b, i).or_else(|| skip_quoted(b, i)) {
            i = end.max(i + 1);
            continue;
        }
        if b[i] == b'$' && (i == 0 || !topn::is_ident_byte(b[i - 1])) && b.get(i + 1).is_some_and(u8::is_ascii_digit) {
            let mut j = i + 1;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            let n: usize = sql[i + 1..j].parse().ok().filter(|&n| (1..=65535).contains(&n))?;
            let k = orig.iter().position(|&o| o == n).unwrap_or_else(|| {
                orig.push(n);
                orig.len() - 1
            });
            out.push_str(&sql[copied..i]);
            out.push('$');
            out.push_str(&(k + 1).to_string());
            copied = j;
            i = j;
            continue;
        }
        i += 1;
    }
    out.push_str(&sql[copied..]);
    Some((out, orig))
}

/// A RowDescription body with only its first `keep` fields.
pub fn strip_description(body: &[u8], keep: usize) -> Option<Vec<u8>> {
    let n = i16_at(body, 0)? as usize;
    let mut at = 2;
    for _ in 0..keep.min(n) {
        at += body[at..].iter().position(|&b| b == 0)? + 1 + 18;
    }
    let mut out = (keep.min(n) as i16).to_be_bytes().to_vec();
    out.extend_from_slice(body.get(2..at)?);
    Some(out)
}

/// A DataRow body with only its first `keep` cells.
pub fn strip_row(body: &[u8], keep: usize) -> Option<Vec<u8>> {
    let n = i16_at(body, 0)? as usize;
    let mut at = 2;
    for _ in 0..keep.min(n) {
        let len = i32::from_be_bytes(body.get(at..at + 4)?.try_into().ok()?);
        at += 4 + len.max(0) as usize;
    }
    let mut out = (keep.min(n) as i16).to_be_bytes().to_vec();
    out.extend_from_slice(body.get(2..at)?);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st(sql: &str, params: Vec<(i16, Option<Vec<u8>>)>) -> Statement {
        Statement { sql: sql.into(), types: vec![23; params.len()], params, results: vec![1], describe: true, simple: false }
    }

    #[test]
    fn keys_are_appended_and_limits_bound() {
        let p = plan(&st(
            "SELECT id, v * 2 AS dbl FROM t WHERE store = $1 ORDER BY dbl DESC, id LIMIT $2 OFFSET $3",
            vec![(0, Some(b"4".to_vec())), (0, Some(b"10".to_vec())), (1, Some(5i64.to_be_bytes().to_vec()))],
        ))
        .unwrap()
        .unwrap();
        assert_eq!(p.sql, "SELECT id, v * 2 AS dbl, v * 2 AS __rdb_k1, id AS __rdb_k2 FROM t WHERE store = $1 ORDER BY dbl DESC, id LIMIT 15");
        assert_eq!((p.limit, p.offset, p.extras), (10, 5, 2));
        assert_eq!(p.params, vec![(0, Some(b"4".to_vec()))]);
        assert_eq!(p.types, vec![23]);
        assert!(p.keys[0].desc && !p.keys[1].desc);
    }

    #[test]
    fn a_star_list_keys_by_column_name_and_parameters_renumber() {
        let p = plan(&st("SELECT * FROM t WHERE a > $2 AND b < $1 ORDER BY id LIMIT 3", vec![(0, Some(b"1".to_vec())), (0, Some(b"2".to_vec()))]))
            .unwrap()
            .unwrap();
        assert_eq!(p.sql, "SELECT *, \"id\" AS __rdb_k1 FROM t WHERE a > $1 AND b < $2 ORDER BY id LIMIT 3");
        assert_eq!(p.params, vec![(0, Some(b"2".to_vec())), (0, Some(b"1".to_vec()))]);
    }

    #[test]
    fn bare_aliases_and_ordinals_resolve() {
        let p = plan(&st("SELECT name n, count c FROM t ORDER BY 2, n LIMIT 1", vec![])).unwrap().unwrap();
        assert_eq!(p.sql, "SELECT name n, count c, count AS __rdb_k1, name AS __rdb_k2 FROM t ORDER BY 2, n LIMIT 1");
        // A key the list does not show is fetched beside it.
        assert_eq!(plan(&st("SELECT a FROM t ORDER BY b LIMIT 1", vec![])).unwrap().unwrap().sql, "SELECT a, \"b\" AS __rdb_k1 FROM t ORDER BY b LIMIT 1");
        assert!(plan(&st("SELECT a FROM t", vec![])).is_none());
        assert!(plan(&st("SELECT a FROM t ORDER BY a LIMIT $1", vec![(0, None)])).unwrap().is_err());
    }

    #[test]
    fn formats_put_the_keys_in_text() {
        assert_eq!(result_formats(&[1], 4, 2), vec![1, 1, 0, 0]);
        assert_eq!(result_formats(&[], 3, 1), vec![0, 0, 0]);
        assert_eq!(result_formats(&[1, 0], 3, 1), vec![1, 0, 0]);
    }

    #[test]
    fn descriptions_and_rows_lose_the_appended_columns() {
        let mut t = 2i16.to_be_bytes().to_vec();
        for name in ["a", "__rdb_k1"] {
            t.extend_from_slice(name.as_bytes());
            t.push(0);
            t.extend_from_slice(&[0; 18]);
        }
        let stripped = strip_description(&t, 1).unwrap();
        assert_eq!(&stripped[..2], &1i16.to_be_bytes());
        assert_eq!(stripped.len(), 2 + 2 + 18);
        let mut d = 2i16.to_be_bytes().to_vec();
        d.extend_from_slice(&3i32.to_be_bytes());
        d.extend_from_slice(b"abc");
        d.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(strip_row(&d, 1).unwrap(), [&1i16.to_be_bytes()[..], &3i32.to_be_bytes(), b"abc"].concat());
    }
}
