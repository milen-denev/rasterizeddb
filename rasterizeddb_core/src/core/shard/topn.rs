//! Top-N across shards (sharding-plan.md B1c): `SELECT … ORDER BY … LIMIT k
//! [OFFSET o]` over a sharded table with no key, merged on the coordinator.
//!
//! Every shard runs the statement with `LIMIT k + o` and no `OFFSET`; each
//! row gets a **sort key** whose plain byte order is the statement's
//! `ORDER BY` order (each key's NULL placement and direction folded in), so
//! the merge compares bytes and decodes nothing twice. The coordinator
//! merges every shard's rows by key, skips `o`, keeps `k`.
//!
//! Keys must name output columns, by name or ordinal: an expression or an
//! alias not in the select list is refused (`0A000`), never approximated.
//! Values are the text a simple query returns; a key's encoding follows its
//! column's type (integers and floats by value, NUMERIC by value at any
//! scale, booleans, and everything else by its text's bytes, the C
//! collation PostgreSQL's `ORDER BY … COLLATE "C"` gives).

/// One `ORDER BY` key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Key {
    /// An output column's name (lower-cased unless quoted), or a 1-based
    /// ordinal.
    pub column: KeyRef,
    pub desc: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyRef {
    Name(String),
    Ordinal(usize),
}

/// A top-N statement, understood.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopN {
    pub keys: Vec<Key>,
    pub limit: u64,
    pub offset: u64,
    /// The statement each shard runs: the original up to its `LIMIT`, with
    /// `LIMIT limit + offset` instead.
    pub part_sql: String,
}

pub(crate) fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Top-level keyword offsets (outside quotes, comments, parentheses).
pub(crate) fn top_level_words(t: &str) -> Vec<(String, usize)> {
    use crate::core::sql_scan::{skip_comment, skip_quoted};
    let b = t.as_bytes();
    let mut out = Vec::new();
    let (mut depth, mut i) = (0i32, 0usize);
    while i < b.len() {
        if let Some(end) = skip_comment(b, i) {
            i = end.max(i + 1);
            continue;
        }
        if let Some(end) = skip_quoted(b, i) {
            i = end.max(i + 1);
            continue;
        }
        match b[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            c if c.is_ascii_alphabetic() && (i == 0 || !is_ident_byte(b[i - 1])) => {
                let s = i;
                while i < b.len() && is_ident_byte(b[i]) {
                    i += 1;
                }
                if depth == 0 {
                    out.push((t[s..i].to_ascii_uppercase(), s));
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    out
}

/// The statement's top-N, or `None` when it is not one this handles (no
/// `ORDER BY`, no literal `LIMIT`, a key that is not an output column…).
pub fn parse(sql: &str) -> Option<Result<TopN, String>> {
    let t = sql.trim().trim_end_matches(';').trim();
    let words = top_level_words(t);
    let order = words.iter().position(|(w, _)| w == "ORDER")?;
    if words.get(order + 1).map(|(w, _)| w.as_str()) != Some("BY") {
        return None;
    }
    if words.iter().any(|(w, _)| matches!(w.as_str(), "FOR" | "FETCH" | "UNION" | "INTERSECT" | "EXCEPT")) {
        return Some(Err("0A000: this top-N shape is not merged across shards".to_string()));
    }
    let by_end = words[order + 1].1 + 2;
    let Some(limit_at) = words.iter().position(|(w, _)| w == "LIMIT") else {
        // ORDER BY with no LIMIT: every row, merged by the same keys (an
        // OFFSET, if any, applied after the merge).
        let offset_at = words.iter().position(|(w, _)| w == "OFFSET").filter(|&o| o > order);
        let (keys_end, offset) = match offset_at {
            Some(o) => match t[words[o].1 + 6..].trim().parse::<u64>() {
                Ok(v) => (words[o].1, v),
                Err(_) => return Some(Err("0A000: an ORDER BY merged across shards needs a literal OFFSET".to_string())),
            },
            None => (t.len(), 0),
        };
        let keys = match parse_keys(&t[by_end..keys_end]) {
            Ok(k) => k,
            Err(e) => return Some(Err(e)),
        };
        return Some(Ok(TopN { keys, limit: u64::MAX, offset, part_sql: t[..keys_end].trim_end().to_string() }));
    };
    if limit_at < order {
        return None;
    }
    let keys_text = &t[by_end..words[limit_at].1];
    let after_limit = &t[words[limit_at].1 + 5..];
    let (limit_text, offset_text) = match words.iter().position(|(w, _)| w == "OFFSET") {
        Some(o) if o > limit_at => (&t[words[limit_at].1 + 5..words[o].1], Some(&t[words[o].1 + 6..])),
        Some(_) => return Some(Err("0A000: OFFSET before LIMIT is not merged across shards".to_string())),
        None => (after_limit, None),
    };
    let num = |s: &str| s.trim().parse::<u64>().ok();
    let Some(limit) = num(limit_text) else {
        return Some(Err("0A000: a top-N merged across shards needs a literal LIMIT".to_string()));
    };
    let offset = match offset_text {
        Some(o) => match num(o) {
            Some(v) => v,
            None => return Some(Err("0A000: a top-N merged across shards needs a literal OFFSET".to_string())),
        },
        None => 0,
    };
    let keys = match parse_keys(keys_text) {
        Ok(k) => k,
        Err(e) => return Some(Err(e)),
    };
    let part_sql = format!("{} LIMIT {}", t[..words[limit_at].1].trim_end(), limit.saturating_add(offset));
    Some(Ok(TopN { keys, limit, offset, part_sql }))
}

/// The ORDER BY keys of `keys_text` (the text between `BY` and what ends
/// the list).
fn parse_keys(keys_text: &str) -> Result<Vec<Key>, String> {
    let mut keys = Vec::new();
    for item in keys_text.split(',') {
        let w: Vec<&str> = item.split_whitespace().collect();
        if w.is_empty() {
            return Err("an empty ORDER BY key".to_string());
        }
        let col = w[0];
        let column = if let Ok(n) = col.parse::<usize>() {
            if n == 0 {
                return Err("ORDER BY position 0 is not in select list".to_string());
            }
            KeyRef::Ordinal(n)
        } else if col.bytes().all(|b| is_ident_byte(b) || b == b'.') {
            KeyRef::Name(col.rsplit('.').next().unwrap_or(col).to_ascii_lowercase())
        } else if col.starts_with('"') && col.ends_with('"') && col.len() > 1 {
            KeyRef::Name(col[1..col.len() - 1].to_string())
        } else {
            return Err(format!("0A000: ORDER BY {col} is an expression, which a merge across shards does not order by"));
        };
        let rest: Vec<String> = w[1..].iter().map(|s| s.to_ascii_uppercase()).collect();
        let desc = rest.iter().any(|s| s == "DESC");
        // PostgreSQL: NULLs sort as larger than every value by default.
        let nulls_first = match rest.iter().position(|s| s == "NULLS") {
            Some(p) => rest.get(p + 1).map(|s| s == "FIRST").unwrap_or(false),
            None => desc,
        };
        if rest.iter().any(|s| !matches!(s.as_str(), "ASC" | "DESC" | "NULLS" | "FIRST" | "LAST")) {
            return Err(format!("0A000: ORDER BY {} has a clause a merge across shards does not handle", item.trim()));
        }
        keys.push(Key { column, desc, nulls_first });
    }
    Ok(keys)
}

/// Whether `sql` is a top-N a merge across shards handles, a parameter
/// `LIMIT $n` / `OFFSET $m` taken as some number (a prepared statement's
/// shape: its values come with each Bind, `topn_ext`).
pub fn is_top_n_shape(sql: &str) -> bool {
    let t = sql.trim().trim_end_matches(';').trim();
    let words = top_level_words(t);
    let Some(order) = words.iter().position(|(w, _)| w == "ORDER") else {
        return false;
    };
    let mut out = t.to_string();
    for (w, at) in words[order..].iter().rev() {
        if w != "LIMIT" && w != "OFFSET" {
            continue;
        }
        let after = at + w.len();
        let rest = &t[after..];
        let lead = rest.len() - rest.trim_start().len();
        if let Some(d) = rest[lead..].strip_prefix('$') {
            let len = d.bytes().take_while(u8::is_ascii_digit).count();
            if len > 0 {
                out.replace_range(after + lead..after + lead + 1 + len, "1");
            }
        }
    }
    matches!(parse(&out), Some(Ok(_)))
}

/// The columns a RowDescription names, with their type OIDs.
pub fn fields_of(row_description_body: &[u8]) -> Option<Vec<(String, u32)>> {
    let n = u16::from_be_bytes(row_description_body.get(..2)?.try_into().ok()?) as usize;
    let mut at = 2;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let end = at + row_description_body[at..].iter().position(|&b| b == 0)?;
        let name = String::from_utf8_lossy(&row_description_body[at..end]).to_string();
        at = end + 1;
        let oid = u32::from_be_bytes(row_description_body.get(at + 6..at + 10)?.try_into().ok()?);
        at += 18;
        out.push((name, oid));
    }
    Some(out)
}

/// A DataRow's cells (`None` for NULL).
pub fn cells_of(data_row_body: &[u8]) -> Option<Vec<Option<&[u8]>>> {
    let n = u16::from_be_bytes(data_row_body.get(..2)?.try_into().ok()?) as usize;
    let mut at = 2;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let len = i32::from_be_bytes(data_row_body.get(at..at + 4)?.try_into().ok()?);
        at += 4;
        if len < 0 {
            out.push(None);
        } else {
            let l = len as usize;
            out.push(Some(data_row_body.get(at..at + l)?));
            at += l;
        }
    }
    Some(out)
}

/// The key's column indexes in `fields`.
pub fn resolve(keys: &[Key], fields: &[(String, u32)]) -> Result<Vec<usize>, String> {
    keys.iter()
        .map(|k| match &k.column {
            KeyRef::Ordinal(n) if *n <= fields.len() => Ok(n - 1),
            KeyRef::Ordinal(n) => Err(format!("ORDER BY position {n} is not in select list")),
            KeyRef::Name(name) => {
                let mut hits = fields.iter().enumerate().filter(|(_, (f, _))| f.eq_ignore_ascii_case(name));
                match (hits.next(), hits.next()) {
                    (Some((i, _)), None) => Ok(i),
                    (Some(_), Some(_)) => Err(format!("ORDER BY \"{name}\" is ambiguous")),
                    (None, _) => Err(format!("0A000: ORDER BY {name} is not an output column, which a merge across shards needs")),
                }
            }
        })
        .collect()
}

/// An order-preserving encoding of a value's text, by its type OID.
fn encode_value(oid: u32, text: &[u8], out: &mut Vec<u8>) -> Result<(), String> {
    let s = std::str::from_utf8(text).map_err(|_| "a non-UTF-8 sort value".to_string())?;
    match oid {
        // int2, int4, int8, oid
        21 | 23 | 20 | 26 => {
            let v: i128 = s.trim().parse().map_err(|_| format!("sort value {s:?} is not an integer"))?;
            out.extend_from_slice(&((v as u128) ^ (1u128 << 127)).to_be_bytes());
        }
        // float4, float8: total order, -0 = 0 as PostgreSQL compares them.
        700 | 701 => {
            let v: f64 = match s.trim() {
                "NaN" => f64::NAN,
                "Infinity" => f64::INFINITY,
                "-Infinity" => f64::NEG_INFINITY,
                x => x.parse().map_err(|_| format!("sort value {s:?} is not a number"))?,
            };
            let v = if v == 0.0 { 0.0 } else { v };
            // PostgreSQL sorts NaN above every number.
            let bits = if v.is_nan() { u64::MAX } else { v.to_bits() };
            let key = if v.is_nan() { bits } else if bits >> 63 == 1 { !bits } else { bits | (1 << 63) };
            out.extend_from_slice(&key.to_be_bytes());
        }
        // numeric: sign, then exponent and digits in a comparable form.
        1700 => encode_numeric(s.trim(), out)?,
        // bool: f < t
        16 => out.push(u8::from(s.starts_with('t'))),
        // Everything else by its bytes, escaped so a prefix sorts first.
        _ => {
            for &b in text {
                if b == 0 {
                    out.extend_from_slice(&[0, 0xFF]);
                } else {
                    out.push(b);
                }
            }
            out.extend_from_slice(&[0, 0]);
        }
    }
    Ok(())
}

/// NUMERIC text (`-12.340`, `NaN`) as `class ‖ (exponent ‖ digits)` that
/// sorts by value: class 0 negative, 1 zero, 2 positive, 3 NaN; for a
/// negative value the rest is inverted.
fn encode_numeric(s: &str, out: &mut Vec<u8>) -> Result<(), String> {
    if s.eq_ignore_ascii_case("nan") {
        out.push(3);
        return Ok(());
    }
    let (neg, body) = match s.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        return Err(format!("sort value {s:?} is not a numeric"));
    }
    let digits: String = format!("{int}{frac}");
    let lead = digits.bytes().position(|b| b != b'0');
    let Some(lead) = lead else {
        out.push(1);
        return Ok(());
    };
    let sig = digits[lead..].trim_end_matches('0');
    // The decimal exponent of the first significant digit.
    let exp = int.len() as i64 - lead as i64;
    let mut rest = Vec::with_capacity(sig.len() + 9);
    rest.extend_from_slice(&((exp as u64) ^ (1 << 63)).to_be_bytes());
    rest.extend(sig.bytes());
    rest.push(0);
    if neg {
        out.push(0);
        out.extend(rest.iter().map(|b| !b));
    } else {
        out.push(2);
        out.extend(rest);
    }
    Ok(())
}

/// One row's sort key (module docs).
pub fn sort_key(keys: &[Key], index: &[usize], fields: &[(String, u32)], cells: &[Option<&[u8]>]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(32);
    for (k, &i) in keys.iter().zip(index) {
        let mut part = Vec::with_capacity(16);
        match cells.get(i).copied().flatten() {
            // NULL first: 0 before every value's 1; last: 2 after.
            None => part.push(if k.nulls_first { 0 } else { 2 }),
            Some(v) => {
                part.push(1);
                encode_value(fields[i].1, v, &mut part)?;
            }
        }
        if k.desc {
            // Invert the value but keep the NULL placement byte's meaning.
            let null_byte = part[0];
            let flipped = match null_byte {
                1 => 1,
                b => b,
            };
            out.push(flipped);
            out.extend(part[1..].iter().map(|b| !b));
            // A terminator, inverted, so a shorter value sorts after a
            // longer one it prefixes under DESC.
            out.push(0xFF);
        } else {
            out.extend(part);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(oid: u32, value: Option<&str>, desc: bool, nulls_first: bool) -> Vec<u8> {
        let keys = [Key { column: KeyRef::Ordinal(1), desc, nulls_first }];
        let fields = [("c".to_string(), oid)];
        sort_key(&keys, &[0], &fields, &[value.map(str::as_bytes)]).unwrap()
    }

    /// `values` sorted by their keys, as the gateway merges them.
    fn sorted(oid: u32, values: &[Option<&str>], desc: bool, nulls_first: bool) -> Vec<Option<String>> {
        let mut v: Vec<(Vec<u8>, Option<String>)> =
            values.iter().map(|x| (key(oid, *x, desc, nulls_first), x.map(str::to_string))).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v.into_iter().map(|(_, x)| x).collect()
    }

    fn some(v: &[&str]) -> Vec<Option<String>> {
        v.iter().map(|s| Some(s.to_string())).collect()
    }

    #[test]
    fn r2r3_topn_integers_and_floats_sort_numerically() {
        assert_eq!(sorted(23, &[Some("10"), Some("-3"), Some("2"), Some("-20")], false, false), some(&["-20", "-3", "2", "10"]));
        assert_eq!(sorted(20, &[Some("10"), Some("-3"), Some("2")], true, false), some(&["10", "2", "-3"]));
        assert_eq!(
            sorted(701, &[Some("NaN"), Some("-Infinity"), Some("1.5"), Some("-0"), Some("Infinity"), Some("-2.25")], false, false),
            some(&["-Infinity", "-2.25", "-0", "1.5", "Infinity", "NaN"])
        );
        assert_eq!(key(701, Some("-0"), false, false), key(701, Some("0"), false, false), "-0 = 0, as PostgreSQL compares them");
    }

    #[test]
    fn r2r3_topn_numeric_sorts_by_value_not_text() {
        let values = ["10", "-10.5", "0.001", "1.5", "-2", "0", "100.25", "-0.5", "9.99"];
        let expected = ["-10.5", "-2", "-0.5", "0", "0.001", "1.5", "9.99", "10", "100.25"];
        let input: Vec<Option<&str>> = values.iter().map(|v| Some(*v)).collect();
        assert_eq!(sorted(1700, &input, false, false), some(&expected));
        let mut reversed = expected.to_vec();
        reversed.reverse();
        assert_eq!(sorted(1700, &input, true, false), some(&reversed));
        assert_eq!(key(1700, Some("1.50"), false, false), key(1700, Some("1.5"), false, false), "equal values, equal keys");
    }

    #[test]
    fn r2r3_topn_text_nulls_and_prefixes() {
        assert_eq!(
            sorted(25, &[Some("ab"), None, Some("a"), Some("abc"), Some("b")], false, false),
            vec![Some("a".into()), Some("ab".into()), Some("abc".into()), Some("b".into()), None]
        );
        assert_eq!(
            sorted(25, &[Some("ab"), None, Some("a"), Some("abc")], true, true),
            vec![None, Some("abc".into()), Some("ab".into()), Some("a".into())]
        );
    }

    #[test]
    fn r2r3_topn_parse_asks_each_shard_for_limit_plus_offset() {
        let t = parse("SELECT a, b FROM t ORDER BY a DESC, 2 LIMIT 5 OFFSET 2").unwrap().unwrap();
        assert_eq!((t.limit, t.offset), (5, 2));
        assert_eq!(t.keys.len(), 2);
        assert!(t.keys[0].desc && t.keys[0].nulls_first, "DESC is NULLS FIRST by default");
        assert_eq!(t.keys[1].column, KeyRef::Ordinal(2));
        assert!(t.part_sql.contains("LIMIT 7"), "{}", t.part_sql);
        assert!(parse("SELECT a FROM t LIMIT 5").is_none(), "no ORDER BY: not a top-N");
    }
}
