//! Answers the gateway's `Classify` (sharding-plan.md B1a.6): how a statement
//! shape routes, so the gateway can route every later statement of that
//! shape by hashing one value, with no parser of its own.
//!
//! **The engine decides, by probing.** For a single-table statement over a
//! partitioned table, the shape's `WHERE` is routed with the engine's own
//! `route_where` twice per placeholder, once with each of two probe values
//! of the key's family in that placeholder (every other placeholder holding
//! the first probe). When each probe routes to exactly the block that value
//! hashes to, that placeholder is the key: `Param`/`Slot`. When every probe
//! routes to the same single block, the shape is `Fixed` there. When the
//! route reaches every partition, the shape has no key: `FanOut` where the
//! gateway can combine the parts exactly, `NeedsKey` where it cannot. So the
//! template agrees with what the shard itself enforces, operators and
//! coercions included, and a wrong answer costs an `RDB02`, never rows.
//!
//! **Fail closed.** A shape this cannot analyse (a join, a subquery, a CTE,
//! a multi-row or `INSERT … SELECT` insert) that mentions a partitioned
//! table is `NeedsKey`; DDL is `NeedsKey` too, since cluster DDL runs from
//! the operator tool (B1a.10), never through the gateway. A shape that
//! touches only plain tables is `Home`.

use rasterizeddb_router::KeyFamily;
use rasterizeddb_router::key::{KeyValue, shard_block};
use rasterizeddb_router::rshard::{Merge, RouteTemplate, Source};

use crate::core::database::Database;
use crate::core::database::relation::Relation;
use crate::core::partition::route::Route;
use crate::core::sql_scan::{skip_comment, skip_quoted};

/// The template for `text` (module docs).
pub fn classify(database: &Database, source: Source, text: &str) -> RouteTemplate {
    match classify_inner(database, source, text) {
        RouteTemplate::Slot { index, family } if source == Source::Parse => RouteTemplate::Param { index, family },
        // A read refused for want of one key is gathered instead (B4.5):
        // every sharded table it names, fetched from every shard onto the
        // home shard. A write or a locking read stays refused.
        RouteTemplate::NeedsKey if gatherable(text) => {
            let mut tables: Vec<String> = Vec::new();
            for w in words(text) {
                if matches!(database.relation(&w), Some(Relation::Partitioned(_))) && !tables.contains(&w) {
                    tables.push(w);
                }
            }
            if tables.is_empty() { RouteTemplate::NeedsKey } else { RouteTemplate::Gather { tables } }
        }
        t => t,
    }
}

/// Whether `text` is a read that may be gathered: a SELECT, WITH, TABLE
/// or VALUES that locks nothing and writes nothing.
fn gatherable(text: &str) -> bool {
    let t = text.trim_start();
    let head = first_word(t);
    if !matches!(head.as_str(), "SELECT" | "WITH" | "TABLE" | "VALUES") {
        return false;
    }
    let up = t.to_ascii_uppercase();
    let words: Vec<&str> = up.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).filter(|w| !w.is_empty()).collect();
    !words.windows(2).any(|p| p[0] == "FOR" && matches!(p[1], "UPDATE" | "SHARE" | "NO" | "KEY"))
        && !words.iter().any(|w| matches!(*w, "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "NEXTVAL" | "SETVAL"))
}

fn classify_inner(database: &Database, source: Source, text: &str) -> RouteTemplate {
    let t = text.trim().trim_end_matches(';').trim();
    let first = first_word(t);
    // A global table lives on the home shard alone (unless it is a
    // reference table, which every shard holds): with a sharded table in
    // the same statement, the shard the key picks has none of its rows,
    // and a join there would answer short without a word.
    if matches!(first.as_str(), "SELECT" | "UPDATE" | "DELETE" | "INSERT" | "WITH" | "MERGE" | "TABLE" | "VALUES")
        && mentions_partitioned(database, t)
        && mentions_global(database, t)
    {
        return RouteTemplate::NeedsKey;
    }
    match first.as_str() {
        "SELECT" => select(database, source, t),
        "UPDATE" | "DELETE" => write(database, source, t, &first),
        "INSERT" => insert(database, source, t),
        "WITH" | "MERGE" | "COPY" | "DO" | "CALL" | "EXPLAIN" | "TABLE" | "VALUES" => {
            complex(database, t)
        }
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "COMMENT" | "GRANT" | "REVOKE" | "REINDEX" | "VACUUM"
        | "CLUSTER" | "IMPORT" | "REFRESH" => RouteTemplate::NeedsKey,
        // Session and transaction control, SHOW, LISTEN…: the gateway keeps
        // them for the session; a shard answers them as the home shard.
        _ => RouteTemplate::Home,
    }
}

fn first_word(t: &str) -> String {
    t.split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}

/// A shape this does not probe (a join, a subquery, a CTE…): routed by a
/// placeholder compared with the shard key column of every partitioned
/// table it mentions, when they share one key family (co-located tables, a
/// tenant id); `NeedsKey` when it mentions partitioned tables otherwise;
/// `Home` when it mentions none. A wrong guess is safe: the shard refuses a
/// read that reaches another shard's rows (RDB02), never answers from its
/// own alone.
fn complex(database: &Database, t: &str) -> RouteTemplate {
    let mut family = None;
    let mut key_names: Vec<String> = Vec::new();
    let mut any = false;
    for w in words(t) {
        let Some(Relation::Partitioned(pt)) = database.relation(&w) else {
            continue;
        };
        any = true;
        let Ok(spec) = pt.spec() else {
            return RouteTemplate::NeedsKey;
        };
        let level0 = &spec.levels()[0];
        match family {
            None => family = Some(level0.family),
            Some(f) if f != level0.family => return RouteTemplate::NeedsKey,
            Some(_) => {}
        }
        let schema = Relation::Partitioned(pt.clone()).schema().clone();
        if let Some(f) = schema.fields.iter().find(|f| !f.is_deleted && f.write_order == level0.write_order as u64) {
            let n = f.name.to_ascii_lowercase();
            if !key_names.contains(&n) {
                key_names.push(n);
            }
        }
    }
    if !any {
        return RouteTemplate::Home;
    }
    let Some(family) = family else {
        return RouteTemplate::NeedsKey;
    };
    match keyed_placeholder(t, &key_names) {
        // Shape or Parse is not known here; the caller's source decides.
        Some(k) => RouteTemplate::Slot { index: k - 1, family },
        None => RouteTemplate::NeedsKey,
    }
}

/// The first placeholder `$k` compared with `=` to a column named in
/// `names` (qualified or not), on either side.
fn keyed_placeholder(t: &str, names: &[String]) -> Option<u16> {
    let b = t.as_bytes();
    let is_ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'.' || c == b'"' || c == b'$';
    let name_of = |x: &str| x.rsplit('.').next().unwrap_or(x).trim_matches('"').to_ascii_lowercase();
    // `$k`, `$k::type`: the placeholder's number.
    let ph = |x: &str| {
        let x = x.split("::").next().unwrap_or(x);
        x.strip_prefix('$').and_then(|d| d.parse::<u16>().ok()).filter(|&k| k >= 1)
    };
    // The operand that ends at `end` (going left) and the one that starts
    // at `start` (going right), whitespace of any kind skipped.
    let left_of = |end: usize| {
        let mut l = end;
        while l > 0 && b[l - 1].is_ascii_whitespace() {
            l -= 1;
        }
        let le = l;
        while l > 0 && (is_ident(b[l - 1]) || b[l - 1] == b':') {
            l -= 1;
        }
        &t[l..le]
    };
    let right_of = |start: usize| {
        let mut r = start;
        while r < b.len() && b[r].is_ascii_whitespace() {
            r += 1;
        }
        let rs = r;
        while r < b.len() && (is_ident(b[r]) || b[r] == b':') {
            r += 1;
        }
        &t[rs..r]
    };
    let mut i = 0;
    while i < b.len() {
        if let Some(end) = skip_quoted(b, i)
            && b[i] != b'"'
        {
            i = end.max(i + 1);
            continue;
        }
        // `col = $k` and `$k = col`.
        if b[i] == b'=' && b.get(i + 1) != Some(&b'=') && i > 0 && !matches!(b[i - 1], b'<' | b'>' | b'!') {
            let (left, right) = (left_of(i), right_of(i + 1));
            if let Some(k) = ph(right)
                && names.contains(&name_of(left))
            {
                return Some(k);
            }
            if let Some(k) = ph(left)
                && names.contains(&name_of(right))
            {
                return Some(k);
            }
        }
        // `col IN ($k)`: one value, one shard.
        if (b[i] == b'I' || b[i] == b'i')
            && b.get(i + 1).is_some_and(|c| c.eq_ignore_ascii_case(&b'N'))
            && i > 0
            && b[i - 1].is_ascii_whitespace()
            && b.get(i + 2).is_some_and(|c| c.is_ascii_whitespace() || *c == b'(')
        {
            let col = left_of(i);
            let rest = t[i + 2..].trim_start();
            if let Some(inner) = rest.strip_prefix('(')
                && let Some(close) = inner.find(')')
                && !inner[..close].contains(',')
                && let Some(k) = ph(inner[..close].trim())
                && names.contains(&name_of(col))
            {
                return Some(k);
            }
        }
        i += 1;
    }
    None
}

/// Whether `t` names a global table (a plain, unsharded one) that is not
/// a reference table.
fn mentions_global(database: &Database, t: &str) -> bool {
    words(t).into_iter().any(|w| {
        matches!(database.relation(&w), Some(Relation::Plain(_)))
            && !w.starts_with("__")
            && w != crate::auth::ROLE_TABLE
            && !super::reference::is_reference(&database.name, &w)
    })
}

/// Whether `table` is a reference table: a write to it goes to every shard.
fn writes_reference(database: &Database, table: &str) -> bool {
    matches!(database.relation(table), Some(Relation::Plain(_))) && super::reference::is_reference(&database.name, table)
}

/// Whether any identifier outside quotes names a partitioned relation.
fn mentions_partitioned(database: &Database, t: &str) -> bool {
    words(t).into_iter().any(|w| matches!(database.relation(&w), Some(Relation::Partitioned(_))))
}

/// The unquoted identifiers of `t`, in order, lower-cased as the catalogue
/// keys them; quoted identifiers keep their case.
fn words(t: &str) -> Vec<String> {
    let b = t.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if let Some(end) = skip_comment(b, i) {
            i = end.max(i + 1);
            continue;
        }
        if b[i] == b'"' {
            if let Some(end) = skip_quoted(b, i) {
                if end > i + 1 {
                    out.push(t[i + 1..end.saturating_sub(1).max(i + 1)].replace("\"\"", "\""));
                }
                i = end.max(i + 1);
                continue;
            }
        }
        if let Some(end) = skip_quoted(b, i) {
            i = end.max(i + 1);
            continue;
        }
        if b[i].is_ascii_alphabetic() || b[i] == b'_' {
            let s = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'.') {
                i += 1;
            }
            let w = &t[s..i];
            out.push(w.rsplit('.').next().unwrap_or(w).to_ascii_lowercase());
            continue;
        }
        i += 1;
    }
    out
}

/// Top-level (depth-0, outside quotes and comments) positions of the
/// keywords in `t`, and whether `t` holds a subquery or a CTE anywhere.
struct Clauses {
    /// `(keyword upper-cased, byte offset)` at depth 0, in order.
    at: Vec<(String, usize)>,
    nested_select: bool,
}

fn clauses(t: &str) -> Clauses {
    let b = t.as_bytes();
    let mut at = Vec::new();
    let mut nested_select = false;
    let mut depth = 0i32;
    let mut i = 0;
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
            c if c.is_ascii_alphabetic() && (i == 0 || !crate::core::sql_scan::is_identifier_byte(b[i - 1])) => {
                let s = i;
                while i < b.len() && crate::core::sql_scan::is_identifier_byte(b[i]) {
                    i += 1;
                }
                let w = t[s..i].to_ascii_uppercase();
                if depth > 0 && (w == "SELECT" || w == "WITH") {
                    nested_select = true;
                }
                if depth == 0 {
                    at.push((w, s));
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    Clauses { at, nested_select }
}

/// Keywords that end a `FROM` list or a `WHERE`.
const ENDS: &[&str] = &[
    "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "WINDOW", "UNION", "INTERSECT", "EXCEPT", "FOR",
    "RETURNING", "FETCH", "ON", "SET", "USING", "VALUES",
];

/// The text from keyword `from` (exclusive) to the next ending keyword.
fn between<'a>(t: &'a str, c: &Clauses, from: &str) -> Option<&'a str> {
    let (idx, &(_, pos)) = c.at.iter().enumerate().find(|(_, (w, _))| w == from)?;
    let start = pos + from.len();
    let end = c.at[idx + 1..]
        .iter()
        .find(|(w, _)| ENDS.contains(&w.as_str()))
        .map_or(t.len(), |&(_, p)| p);
    Some(t[start..end].trim())
}

/// A single plain table reference (`t`, `t alias`, `t AS alias`, `s.t`):
/// its name, or `None` for a list, a join or anything else.
fn single_table(from: &str) -> Option<String> {
    if from.contains(',') || from.contains('(') {
        return None;
    }
    let parts: Vec<&str> = from.split_whitespace().collect();
    let name = match parts.as_slice() {
        [t] | [t, _] => t,
        [t, a, _] if a.eq_ignore_ascii_case("AS") => t,
        _ => return None,
    };
    let joined = parts.iter().any(|p| {
        matches!(p.to_ascii_uppercase().as_str(), "JOIN" | "NATURAL" | "CROSS" | "LATERAL" | "INNER" | "LEFT" | "RIGHT" | "FULL")
    });
    if joined {
        return None;
    }
    let name = name.rsplit('.').next().unwrap_or(name);
    Some(if name.starts_with('"') {
        name.trim_matches('"').replace("\"\"", "\"")
    } else {
        name.to_ascii_lowercase()
    })
}

fn select(database: &Database, source: Source, t: &str) -> RouteTemplate {
    let c = clauses(t);
    if c.nested_select {
        return complex(database, t);
    }
    let Some(table) = between(t, &c, "FROM").and_then(single_table) else {
        return complex(database, t);
    };
    let Some(Relation::Partitioned(pt)) = database.relation(&table) else {
        return if mentions_partitioned(database, t) { RouteTemplate::NeedsKey } else { RouteTemplate::Home };
    };
    let filter = between(t, &c, "WHERE").unwrap_or("");
    match probe(&pt, source, filter) {
        Probed::Key(tpl) => tpl,
        Probed::NoKey => fan_out_read(t, &c),
        Probed::Unknown => RouteTemplate::NeedsKey,
    }
}

/// A keyless read the gateway can combine exactly, or `NeedsKey`: rows with
/// no ordering, aggregate or DISTINCT concatenate; a lone `count(*)` sums;
/// anything else needs the coordinator shard, which is refused until it
/// lands, rather than combined into something that looks right.
fn fan_out_read(t: &str, c: &Clauses) -> RouteTemplate {
    let has = |k: &str| c.at.iter().any(|(w, _)| w == k);
    if has("FOR") {
        return RouteTemplate::NeedsKey;
    }
    let list = between(t, c, "SELECT").unwrap_or("");
    let list_upper = list.to_ascii_uppercase();
    let compact: String = list_upper.chars().filter(|c| !c.is_whitespace()).collect();
    if (compact == "COUNT(*)" || compact == "COUNT(1)") && !has("GROUP") && !has("HAVING") && !has("ORDER") {
        return RouteTemplate::FanOut { merge: Merge::CountSum, writes: false };
    }
    let aggregate = ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "ARRAY_AGG(", "STRING_AGG(", "BOOL_", "EVERY(", "JSON_AGG("]
        .iter()
        .any(|a| compact.contains(a));
    // Aggregates and groups merge on a coordinator shard with the engine's
    // own fold (B1c); DISTINCT, windows and a bare ORDER BY … LIMIT are not
    // a fold, and wait for B4.
    let windowed = compact.contains(")OVER(") || compact.contains(")OVER ");
    if (aggregate || has("GROUP")) && !windowed && !list_upper.trim_start().starts_with("DISTINCT") {
        return RouteTemplate::FanOut { merge: Merge::Coordinator, writes: false };
    }
    // SELECT DISTINCT with no ordering or limit: every shard's distinct rows,
    // deduplicated across shards on the coordinator (B4).
    if list_upper.trim_start().starts_with("DISTINCT ")
        && !list_upper.trim_start().starts_with("DISTINCT ON")
        && !aggregate
        && !has("GROUP")
        && !has("HAVING")
        && !has("ORDER")
        && !has("LIMIT")
        && !has("OFFSET")
    {
        return RouteTemplate::FanOut { merge: Merge::Coordinator, writes: false };
    }
    // ORDER BY (… LIMIT) over output columns: merged on the coordinator by
    // sort keys (B1c top-N; with no LIMIT, every row in order).
    if !aggregate && !has("GROUP") && !has("HAVING") && !list_upper.trim_start().starts_with("DISTINCT")
        && has("ORDER")
        && matches!(super::topn::parse(t), Some(Ok(_)))
    {
        return RouteTemplate::FanOut { merge: Merge::Coordinator, writes: false };
    }
    if aggregate || has("GROUP") || has("HAVING") || has("ORDER") || has("OFFSET") || list_upper.trim_start().starts_with("DISTINCT")
    {
        return RouteTemplate::NeedsKey;
    }
    RouteTemplate::FanOut { merge: Merge::Concat, writes: false }
}

fn write(database: &Database, source: Source, t: &str, verb: &str) -> RouteTemplate {
    let c = clauses(t);
    if c.nested_select || c.at.iter().any(|(w, _)| w == "USING" || w == "FROM" && verb == "UPDATE") {
        return complex(database, t);
    }
    let target = if verb == "DELETE" { between(t, &c, "FROM") } else { between(t, &c, "UPDATE") };
    let Some(table) = target.and_then(single_table) else {
        return complex(database, t);
    };
    if writes_reference(database, &table) {
        // Every shard holds it whole: each changes its copy, and any one's
        // count is the statement's.
        return RouteTemplate::FanOut { merge: Merge::Any, writes: true };
    }
    let Some(Relation::Partitioned(pt)) = database.relation(&table) else {
        return if mentions_partitioned(database, t) { RouteTemplate::NeedsKey } else { RouteTemplate::Home };
    };
    let filter = between(t, &c, "WHERE").unwrap_or("");
    match probe(&pt, source, filter) {
        Probed::Key(tpl) => tpl,
        Probed::NoKey => RouteTemplate::FanOut { merge: Merge::Concat, writes: true },
        Probed::Unknown => RouteTemplate::NeedsKey,
    }
}

fn insert(database: &Database, source: Source, t: &str) -> RouteTemplate {
    let c = clauses(t);
    if c.nested_select {
        return complex(database, t);
    }
    // INSERT INTO t (cols) VALUES (…)
    let Some(&(_, into)) = c.at.iter().find(|(w, _)| w == "INTO") else {
        return complex(database, t);
    };
    let Some(&(_, values)) = c.at.iter().find(|(w, _)| w == "VALUES") else {
        return complex(database, t);
    };
    let head = t[into + 4..values].trim();
    let (name, cols) = match head.split_once('(') {
        Some((n, rest)) => (n.trim(), Some(rest.trim_end().trim_end_matches(')'))),
        None => (head, None),
    };
    let Some(table) = single_table(name) else {
        return complex(database, t);
    };
    if writes_reference(database, &table) {
        return RouteTemplate::FanOut { merge: Merge::Any, writes: true };
    }
    let relation = database.relation(&table);
    let Some(Relation::Partitioned(pt)) = relation.clone() else {
        return RouteTemplate::Home;
    };
    let Some(relation) = relation else {
        return RouteTemplate::NeedsKey;
    };
    let Ok(spec) = pt.spec() else {
        return RouteTemplate::NeedsKey;
    };
    let level0 = &spec.levels()[0];
    let schema = relation.schema();
    let Some(key_name) = schema
        .fields
        .iter()
        .find(|f| !f.is_deleted && f.write_order == level0.write_order as u64)
        .map(|f| f.name.clone())
    else {
        return RouteTemplate::NeedsKey;
    };
    // The key's position among the values.
    let position = match cols {
        Some(cols) => split_top(cols)
            .iter()
            .position(|c| c.trim().trim_matches('"').eq_ignore_ascii_case(&key_name)),
        None => schema
            .fields
            .iter()
            .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
            .position(|f| f.name.eq_ignore_ascii_case(&key_name)),
    };
    let Some(position) = position else {
        return RouteTemplate::NeedsKey;
    };
    // One row only: several rows may be several shards.
    let rest = t[values + 6..].trim();
    let Some(row_end) = matching_paren(rest) else {
        return RouteTemplate::NeedsKey;
    };
    let after = rest[row_end + 1..].trim_start();
    if after.starts_with(',') {
        return RouteTemplate::NeedsKey;
    }
    let items = split_top(&rest[1..row_end]);
    let Some(item) = items.get(position).map(|s| s.trim()) else {
        return RouteTemplate::NeedsKey;
    };
    match placeholder(item) {
        Some(k) => keyed(source, k, level0.family),
        None => RouteTemplate::NeedsKey,
    }
}

/// `$k` (1-based) when `item` is exactly a placeholder.
fn placeholder(item: &str) -> Option<u16> {
    item.strip_prefix('$').and_then(|d| d.parse::<u16>().ok()).filter(|&k| k >= 1)
}

fn keyed(source: Source, k: u16, family: KeyFamily) -> RouteTemplate {
    match source {
        Source::Shape => RouteTemplate::Slot { index: k - 1, family },
        Source::Parse => RouteTemplate::Param { index: k - 1, family },
    }
}

/// The end of the parenthesised group `s` opens with.
fn matching_paren(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    if b.first() != Some(&b'(') {
        return None;
    }
    let mut depth = 0i32;
    let mut i = 0;
    while i < b.len() {
        if let Some(end) = skip_quoted(b, i) {
            i = end.max(i + 1);
            continue;
        }
        match b[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// `s` split at depth-0 commas, outside quotes.
fn split_top(s: &str) -> Vec<&str> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let (mut depth, mut start, mut i) = (0i32, 0, 0);
    while i < b.len() {
        if let Some(end) = skip_quoted(b, i) {
            i = end.max(i + 1);
            continue;
        }
        match b[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b',' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(&s[start..]);
    out
}

enum Probed {
    Key(RouteTemplate),
    NoKey,
    Unknown,
}

/// Two probe values of `family`, as SQL literals and as keys.
fn probes(family: KeyFamily) -> [(String, KeyValue<'static>); 2] {
    match family {
        KeyFamily::Integer => [
            ("1000003".to_string(), KeyValue::Int(1_000_003)),
            ("2000029".to_string(), KeyValue::Int(2_000_029)),
        ],
        KeyFamily::Text => [
            ("'rdb probe a'".to_string(), KeyValue::Text("rdb probe a")),
            ("'rdb probe b'".to_string(), KeyValue::Text("rdb probe b")),
        ],
        KeyFamily::Uuid => [
            ("'0b5e7a51-0000-4000-8000-00000000000a'".to_string(), KeyValue::Uuid(uuid_bytes(0x0a))),
            ("'0b5e7a51-0000-4000-8000-00000000000b'".to_string(), KeyValue::Uuid(uuid_bytes(0x0b))),
        ],
    }
}

fn uuid_bytes(last: u8) -> [u8; 16] {
    let mut b = [0x0b, 0x5e, 0x7a, 0x51, 0, 0, 0x40, 0, 0x80, 0, 0, 0, 0, 0, 0, 0];
    b[15] = last;
    b
}

/// The placeholder numbers `$k` in `filter`, outside quotes.
fn placeholders(filter: &str) -> Vec<u16> {
    let b = filter.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'$' && b.get(i + 1).is_some_and(u8::is_ascii_digit) {
            let s = i + 1;
            let mut e = s;
            while e < b.len() && b[e].is_ascii_digit() {
                e += 1;
            }
            if let Ok(k) = filter[s..e].parse::<u16>() {
                if k >= 1 && !out.contains(&k) {
                    out.push(k);
                }
            }
            i = e;
            continue;
        }
        if let Some(end) = skip_quoted(b, i) {
            i = end.max(i + 1);
            continue;
        }
        i += 1;
    }
    out
}

/// `filter` with every `$k` replaced: `$which` by `value`, the others by
/// `rest`. Longest numbers first, so `$1` never eats part of `$10`.
fn substitute(filter: &str, ks: &[u16], which: Option<u16>, value: &str, rest: &str) -> String {
    let mut sorted = ks.to_vec();
    sorted.sort_unstable_by(|a, b| b.cmp(a));
    let mut out = filter.to_string();
    for k in sorted {
        let with = if Some(k) == which { value } else { rest };
        out = out.replace(&format!("${k}"), with);
    }
    out
}

/// The single level-0 block a route reaches, if it reaches one.
fn single_block(route: &Route) -> Option<u64> {
    match route {
        Route::Points(ranges) => {
            let first = ranges.first()?;
            let last = ranges.last()?;
            let lo = first.lo >> 16;
            let hi = ((last.hi - 1) >> 16) as u64;
            (lo == hi).then_some(lo)
        }
        _ => None,
    }
}

fn probe(pt: &crate::core::partition::table::PartitionedTable, source: Source, filter: &str) -> Probed {
    let Ok(spec) = pt.spec() else {
        return Probed::Unknown;
    };
    let family = spec.levels()[0].family;
    let [(a_sql, a_key), (b_sql, b_key)] = probes(family);
    let (Ok(block_a), Ok(block_b)) = (shard_block(a_key, family), shard_block(b_key, family)) else {
        return Probed::Unknown;
    };
    let ks = placeholders(filter);
    let route = |text: &str| pt.route_where(text).ok();
    for &k in &ks {
        let (Some(ra), Some(rb)) = (
            route(&substitute(filter, &ks, Some(k), &a_sql, &a_sql)),
            route(&substitute(filter, &ks, Some(k), &b_sql, &a_sql)),
        ) else {
            return Probed::Unknown;
        };
        if single_block(&ra) == Some(block_a) && single_block(&rb) == Some(block_b) {
            return Probed::Key(keyed(source, k, family));
        }
    }
    // No placeholder is the key: a fixed block, every block, or a zero route.
    let Some(r) = route(&substitute(filter, &ks, None, &a_sql, &a_sql)) else {
        return Probed::Unknown;
    };
    match (&r, single_block(&r)) {
        (Route::Zero, _) => Probed::Key(RouteTemplate::Home),
        (_, Some(block)) => {
            // Fixed only if it does not move with the placeholders at all.
            let other = route(&substitute(filter, &ks, None, &b_sql, &b_sql)).and_then(|r| single_block(&r));
            if other == Some(block) { Probed::Key(RouteTemplate::Fixed { block }) } else { Probed::Unknown }
        }
        (Route::All, _) => Probed::NoKey,
        _ => Probed::Unknown,
    }
}

#[cfg(test)]
mod b48_tests {
    use super::keyed_placeholder;

    #[test]
    fn b48_a_key_compared_anywhere_routes_the_statement() {
        let keys = vec!["tenant_id".to_string()];
        let k = |t: &str| keyed_placeholder(t, &keys);
        assert_eq!(k("SELECT * FROM a JOIN b ON a.tenant_id = b.tenant_id WHERE a.tenant_id = $2"), Some(2));
        assert_eq!(k("SELECT * FROM a WHERE $1 = a.tenant_id"), Some(1), "placeholder first");
        assert_eq!(k("SELECT * FROM a WHERE a.tenant_id\n\t=\n$3"), Some(3), "any whitespace");
        assert_eq!(k("SELECT * FROM a WHERE a.tenant_id = $1::bigint"), Some(1), "a cast");
        assert_eq!(k("SELECT * FROM a WHERE a.tenant_id IN ($4)"), Some(4), "one-value IN");
        assert_eq!(k("SELECT * FROM a WHERE a.tenant_id IN ($1, $2)"), None, "two values may be two shards");
        assert_eq!(k("SELECT * FROM a WHERE a.other = $1"), None);
        assert_eq!(k("SELECT * FROM a WHERE a.tenant_id >= $1"), None, "a range is not one key");
    }
}

#[cfg(test)]
mod b45_tests {
    use super::gatherable;

    #[test]
    fn b45_only_plain_reads_are_gathered() {
        assert!(gatherable("SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer"));
        assert!(gatherable("WITH r AS (SELECT * FROM t) SELECT row_number() OVER (ORDER BY id) FROM r"));
        assert!(!gatherable("SELECT * FROM t FOR UPDATE"), "a locking read");
        assert!(!gatherable("SELECT * FROM t FOR NO KEY UPDATE"));
        assert!(!gatherable("WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d"), "a write in a CTE");
        assert!(!gatherable("SELECT nextval('s'), * FROM t"), "a sequence call writes");
        assert!(!gatherable("UPDATE t SET a = 1"));
    }
}
