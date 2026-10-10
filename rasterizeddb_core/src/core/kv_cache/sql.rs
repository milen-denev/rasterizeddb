//! `KV <command> …` from SQL: the same commands as the Redis port, answered as
//! rows.
//!
//! ```sql
//! KV SET session:42 '{"user":7}' EX 3600;   -- one row: OK
//! KV GET session:42;                          -- one row, or none
//! KV INCR hits;                               -- one bigint row
//! KV HGETALL user:7;                          -- (field, value) rows
//! KV ZRANGE board 0 9 REV WITHSCORES;         -- (member, score) rows
//! KV SCAN 0 MATCH 'session:*' COUNT 100;      -- (cursor, key) rows
//! ```
//!
//! Arguments are words or single-quoted strings (`''` for a quote inside).
//! The statement runs against database 0. Commands that need a connection of
//! their own (`MULTI`, `SUBSCRIBE`, `SELECT`, `AUTH`, …) are refused with a
//! pointer to the Redis port. A blocking pop answers at once, as if its
//! timeout had passed. The result's columns depend only on the command and
//! its options, never on the data, so the extended protocol can describe a
//! statement before it runs.
//!
//! Values are text. One that is not valid UTF-8 is refused with an error
//! rather than sent mangled: a binary value is read over the Redis port.

use super::commands::{self, CONN, Cmd, SERVER, Spec, eq};
use super::reply::{Collect, Item, Reply};
use super::store::Direct;
use crate::core::db_type::DbType;
use crate::core::row::row::{Column, Row, RowColumns, schema_id_of};
use crate::memory_pool::MEMORY_POOL;

/// Whether `sql` is a `KV …` statement.
pub fn is_kv_statement(sql: &str) -> bool {
    let t = sql.trim_start();
    t.len() > 3 && t[..2].eq_ignore_ascii_case("KV") && t.as_bytes()[2].is_ascii_whitespace()
}

/// The arguments after `KV`.
pub fn tokenize(sql: &str) -> Result<Vec<Vec<u8>>, String> {
    let t = sql.trim();
    let t = t.strip_suffix(';').unwrap_or(t).trim_end();
    let body = &t.as_bytes()[2..];
    let mut args = Vec::new();
    let mut i = 0;
    while i < body.len() {
        while i < body.len() && body[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= body.len() {
            break;
        }
        if body[i] == b'\'' {
            let mut value = Vec::new();
            i += 1;
            loop {
                match body.get(i) {
                    None => return Err("unterminated quoted string in KV statement".into()),
                    Some(b'\'') if body.get(i + 1) == Some(&b'\'') => {
                        value.push(b'\'');
                        i += 2;
                    }
                    Some(b'\'') => {
                        i += 1;
                        break;
                    }
                    Some(&c) => {
                        value.push(c);
                        i += 1;
                    }
                }
            }
            if body.get(i).is_some_and(|c| !c.is_ascii_whitespace()) {
                return Err("a quoted KV argument must be followed by a space".into());
            }
            args.push(value);
        } else {
            let start = i;
            while i < body.len() && !body[i].is_ascii_whitespace() {
                if body[i] == b'\'' {
                    return Err("a quote inside a KV word; quote the whole argument".into());
                }
                i += 1;
            }
            args.push(body[start..i].to_vec());
        }
    }
    if args.is_empty() {
        return Err("KV names no command".into());
    }
    Ok(args)
}

/// How a command's reply becomes rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// One `value` column of this type; an array reply is a row per element.
    Value(Kind),
    /// Two text columns from a map or flat pairs.
    Pairs(&'static str, &'static str),
    /// `(member, score)` from flat member/score pairs.
    MemberScore,
    /// `ZRANK … WITHSCORE`.
    RankScore,
    /// `[cursor, [items]]`: the cursor and then `width` columns per item.
    Scan(ScanKind),
    /// `INFO` text as `(metric, value)`.
    Info,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Text,
    Int,
    Float,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanKind {
    Keys,
    Fields,
    FieldValues,
    Members,
    MemberScores,
}

fn has(args: &[Vec<u8>], word: &[u8]) -> bool {
    args.iter().skip(1).any(|a| eq(a, word))
}

fn shape(spec: &Spec, args: &[Vec<u8>]) -> Shape {
    use Cmd::*;
    match spec.cmd {
        SetNx | Incr | Decr | IncrBy | DecrBy | Append | StrLen | SetRange | SetBit | GetBit | BitCount | Del
        | Unlink | Exists | Touch | Expire | PExpire | ExpireAt | PExpireAt | Ttl | PTtl | ExpireTime
        | PExpireTime | Persist | RenameNx | Copy | DbSize | HSet | HSetNx | HDel | HExists | HLen
        | HIncrBy | HStrLen | LPush | RPush | LPushX | RPushX | LLen | LRem | LInsert | LPos | SAdd | SRem
        | SIsMember | SMIsMember | SCard | SInterStore | SUnionStore | SDiffStore | SInterCard | SMove
        | ZRem | ZCard | ZCount | ZLexCount | ZRemRangeByRank | ZRemRangeByScore | ZRemRangeByLex
        | ZUnionStore | ZInterStore | ZDiffStore | Publish | LastSave | MSetNx | Move => Shape::Value(Kind::Int),
        ZAdd if has(args, b"INCR") => Shape::Value(Kind::Float),
        ZAdd => Shape::Value(Kind::Int),
        ZScore | ZMScore | ZIncrBy => Shape::Value(Kind::Float),
        ZRank | ZRevRank if has(args, b"WITHSCORE") => Shape::RankScore,
        ZRank | ZRevRank => Shape::Value(Kind::Int),
        ZRange | ZRevRange | ZRangeByScore | ZRevRangeByScore | ZUnion | ZInter | ZDiff
            if has(args, b"WITHSCORES") =>
        {
            Shape::MemberScore
        }
        ZPopMin | ZPopMax => Shape::MemberScore,
        HGetAll => Shape::Pairs("field", "value"),
        HRandField if has(args, b"WITHVALUES") => Shape::Pairs("field", "value"),
        Config if args.get(1).is_some_and(|a| eq(a, b"GET")) => Shape::Pairs("name", "value"),
        BLPop | BRPop => Shape::Pairs("key", "value"),
        Scan => Shape::Scan(ScanKind::Keys),
        HScan if has(args, b"NOVALUES") => Shape::Scan(ScanKind::Fields),
        HScan => Shape::Scan(ScanKind::FieldValues),
        SScan => Shape::Scan(ScanKind::Members),
        ZScan => Shape::Scan(ScanKind::MemberScores),
        Info => Shape::Info,
        _ => Shape::Value(Kind::Text),
    }
}

fn kind_type(kind: Kind) -> DbType {
    match kind {
        Kind::Text => DbType::STRING,
        Kind::Int => DbType::I64,
        Kind::Float => DbType::F64,
    }
}

/// Refuses what cannot run from SQL; otherwise the command's spec.
fn plan(args: &[Vec<u8>]) -> Result<(&'static Spec, Shape), String> {
    let spec = commands::lookup(&args[0])
        .ok_or_else(|| format!("unknown KV command '{}'", String::from_utf8_lossy(&args[0])))?;
    if !spec.arity_ok(args.len()) {
        return Err(format!("wrong number of arguments for KV {}", spec.name.to_ascii_uppercase()));
    }
    let needs_connection = spec.is(CONN) && spec.cmd != Cmd::Publish && spec.cmd != Cmd::PubSubCmd;
    if needs_connection {
        return Err(format!(
            "KV {} needs a connection of its own; use the Redis protocol port (--kv-port)",
            spec.name.to_ascii_uppercase()
        ));
    }
    if matches!(spec.cmd, Cmd::SwapDb | Cmd::Move) {
        return Err(format!(
            "KV {} works across numbered databases, which SQL does not select; use the Redis port",
            spec.name.to_ascii_uppercase()
        ));
    }
    Ok((spec, shape(spec, args)))
}

/// The columns a `KV` statement returns, for `Describe`.
pub fn result_fields(sql: &str) -> Vec<(String, DbType)> {
    let fallback = vec![("value".to_string(), DbType::STRING)];
    let Ok(args) = tokenize(sql) else {
        return fallback;
    };
    let Ok((_, shape)) = plan(&args) else {
        return fallback;
    };
    let text = |n: &str| (n.to_string(), DbType::STRING);
    match shape {
        Shape::Value(kind) => vec![("value".to_string(), kind_type(kind))],
        Shape::Pairs(a, b) => vec![text(a), text(b)],
        Shape::MemberScore => vec![text("member"), ("score".to_string(), DbType::F64)],
        Shape::RankScore => vec![("rank".to_string(), DbType::I64), ("score".to_string(), DbType::F64)],
        Shape::Scan(kind) => match kind {
            ScanKind::Keys => vec![text("cursor"), text("key")],
            ScanKind::Fields => vec![text("cursor"), text("field")],
            ScanKind::FieldValues => vec![text("cursor"), text("field"), text("value")],
            ScanKind::Members => vec![text("cursor"), text("member")],
            ScanKind::MemberScores => vec![text("cursor"), text("member"), ("score".to_string(), DbType::F64)],
        },
        Shape::Info => vec![text("metric"), text("value")],
    }
}

/// Runs a `KV` statement and encodes its rows (or its error) the way the
/// executor's other surfaces do.
pub async fn execute(sql: &str, handle: &super::KvCacheHandle) -> Vec<u8> {
    match run(sql, handle).await {
        Ok(rows) => encode_rows(rows),
        Err(message) => failure(&message),
    }
}

async fn run(sql: &str, handle: &super::KvCacheHandle) -> Result<Vec<Vec<Cell>>, String> {
    let args = tokenize(sql)?;
    let (spec, shape) = plan(&args)?;
    let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
    let mut out = Collect::default();
    match spec.cmd {
        Cmd::Save => {
            let handle = handle.clone();
            match tokio::task::spawn_blocking(move || handle.save()).await {
                Ok(Ok(keys)) => out.simple(&format!("OK ({keys} keys)")),
                Ok(Err(e)) => return Err(e.to_string()),
                Err(e) => return Err(format!("save panicked: {e}")),
            }
        }
        Cmd::BgSave => {
            let handle = handle.clone();
            if handle.inner().saving.swap(true, std::sync::atomic::Ordering::AcqRel) {
                return Err("a background save is already in progress".into());
            }
            tokio::task::spawn_blocking(move || {
                if let Err(e) = handle.save() {
                    log::error!("kv cache: BGSAVE failed: {e}");
                }
                handle.inner().saving.store(false, std::sync::atomic::Ordering::Release);
            });
            out.simple("Background saving started");
        }
        Cmd::Publish => {
            let n = handle.inner().pubsub.publish(&args[1], &args[2]);
            out.int(n as i64);
        }
        Cmd::PubSubCmd => {
            let pubsub = &handle.inner().pubsub;
            if eq(&args[1], b"CHANNELS") {
                let list = pubsub.channels(args.get(2).map(|a| a.as_slice()));
                out.array(list.len());
                for c in list {
                    out.bulk(&c);
                }
            } else if eq(&args[1], b"NUMPAT") {
                out.int(pubsub.numpat() as i64);
            } else {
                return Err("KV PUBSUB supports CHANNELS and NUMPAT".into());
            }
        }
        _ if spec.is(SERVER) => super::server::run(spec, handle, 0, &refs, &mut out),
        _ => {
            let store = handle.db(0);
            let mut access = Direct::new(store);
            commands::run(spec, &mut access, &refs, &mut out);
        }
    }
    rows_of(out.take(), shape)
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Text(String),
    Int(i64),
    Float(f64),
    Null,
}

fn text(bytes: &[u8]) -> Result<String, String> {
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        "a value is not valid UTF-8 text, so it cannot be returned as text; read binary values over \
         the Redis protocol port (--kv-port)"
            .to_string()
    })
}

fn cell(item: &Item, kind: Kind) -> Result<Cell, String> {
    Ok(match item {
        Item::Null | Item::NullArray => Cell::Null,
        Item::Int(n) => match kind {
            Kind::Float => Cell::Float(*n as f64),
            Kind::Text => Cell::Text(n.to_string()),
            Kind::Int => Cell::Int(*n),
        },
        Item::Double(f) => match kind {
            Kind::Text => Cell::Text(String::from_utf8_lossy(&super::reply::double_string(*f)).into()),
            _ => Cell::Float(*f),
        },
        Item::Bool(b) => match kind {
            Kind::Text => Cell::Text(b.to_string()),
            _ => Cell::Int(i64::from(*b)),
        },
        Item::Simple(s) => Cell::Text(s.clone()),
        Item::Bulk(b) => match kind {
            Kind::Text => Cell::Text(text(b)?),
            Kind::Int => Cell::Int(commands::parse_int(b).ok_or("expected an integer reply")?),
            Kind::Float => Cell::Float(commands::parse_float(b).ok_or("expected a number reply")?),
        },
        Item::Error(e) => return Err(e.clone()),
        Item::Array(_) | Item::Map(_) => Cell::Text(render(item)),
    })
}

/// A nested reply as one line of text, for the rare command (`COMMAND`) whose
/// elements are themselves arrays.
fn render(item: &Item) -> String {
    match item {
        Item::Null | Item::NullArray => "(nil)".into(),
        Item::Int(n) => n.to_string(),
        Item::Double(f) => String::from_utf8_lossy(&super::reply::double_string(*f)).into(),
        Item::Bool(b) => b.to_string(),
        Item::Simple(s) | Item::Error(s) => s.clone(),
        Item::Bulk(b) => String::from_utf8_lossy(b).into(),
        Item::Array(items) => format!("[{}]", items.iter().map(render).collect::<Vec<_>>().join(", ")),
        Item::Map(pairs) => format!(
            "{{{}}}",
            pairs
                .iter()
                .map(|(k, v)| format!("{}: {}", render(k), render(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Flat `[a, b, a, b, …]` or a map, as pairs.
fn pairs_of(item: Item) -> Vec<(Item, Item)> {
    match item {
        Item::Map(pairs) => pairs,
        Item::Array(items) => {
            let mut it = items.into_iter();
            let mut out = Vec::new();
            while let (Some(a), Some(b)) = (it.next(), it.next()) {
                out.push((a, b));
            }
            out
        }
        _ => Vec::new(),
    }
}

fn rows_of(reply: Item, shape: Shape) -> Result<Vec<Vec<Cell>>, String> {
    if let Item::Error(e) = &reply {
        return Err(e.clone());
    }
    Ok(match shape {
        Shape::Value(kind) => match reply {
            Item::Null | Item::NullArray => Vec::new(),
            Item::Array(items) => items.iter().map(|i| cell(i, kind).map(|c| vec![c])).collect::<Result<_, _>>()?,
            Item::Map(pairs) => pairs
                .iter()
                .map(|(k, v)| Ok(vec![Cell::Text(format!("{}: {}", render(k), render(v)))]))
                .collect::<Result<_, String>>()?,
            other => vec![vec![cell(&other, kind)?]],
        },
        Shape::Pairs(..) => pairs_of(reply)
            .iter()
            .map(|(a, b)| Ok(vec![cell(a, Kind::Text)?, cell(b, Kind::Text)?]))
            .collect::<Result<_, String>>()?,
        Shape::MemberScore => pairs_of(reply)
            .iter()
            .map(|(m, s)| Ok(vec![cell(m, Kind::Text)?, cell(s, Kind::Float)?]))
            .collect::<Result<_, String>>()?,
        Shape::RankScore => match reply {
            Item::Array(items) if items.len() == 2 => {
                vec![vec![cell(&items[0], Kind::Int)?, cell(&items[1], Kind::Float)?]]
            }
            _ => Vec::new(),
        },
        Shape::Scan(kind) => {
            let Item::Array(mut parts) = reply else {
                return Ok(Vec::new());
            };
            if parts.len() != 2 {
                return Ok(Vec::new());
            }
            let items = parts.pop().expect("two");
            let cursor = match parts.pop().expect("two") {
                Item::Bulk(b) => text(&b)?,
                other => render(&other),
            };
            let items = match items {
                Item::Array(items) => items,
                _ => Vec::new(),
            };
            let width = match kind {
                ScanKind::Keys | ScanKind::Fields | ScanKind::Members => 1,
                ScanKind::FieldValues | ScanKind::MemberScores => 2,
            };
            let mut rows = Vec::new();
            for chunk in items.chunks(width) {
                let mut row = vec![Cell::Text(cursor.clone())];
                row.push(cell(&chunk[0], Kind::Text)?);
                if width == 2 {
                    row.push(cell(
                        &chunk[1],
                        if kind == ScanKind::MemberScores { Kind::Float } else { Kind::Text },
                    )?);
                }
                rows.push(row);
            }
            if rows.is_empty() {
                // The cursor is the answer a client needs even when this
                // page held nothing.
                let mut row = vec![Cell::Text(cursor), Cell::Null];
                if width == 2 {
                    row.push(Cell::Null);
                }
                rows.push(row);
            }
            rows
        }
        Shape::Info => {
            let body = match reply {
                Item::Bulk(b) => String::from_utf8_lossy(&b).to_string(),
                other => render(&other),
            };
            body.lines()
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| vec![Cell::Text(k.to_string()), Cell::Text(v.trim_end().to_string())])
                .collect()
        }
    })
}

fn encode_rows(rows: Vec<Vec<Cell>>) -> Vec<u8> {
    let mut out = vec![2u8];
    for cells in rows {
        let mut columns: RowColumns = smallvec::SmallVec::new();
        for (index, c) in cells.iter().enumerate() {
            let (bytes, column_type): (Vec<u8>, DbType) = match c {
                Cell::Text(t) => (t.as_bytes().to_vec(), DbType::STRING),
                Cell::Int(n) => (n.to_le_bytes().to_vec(), DbType::I64),
                Cell::Float(f) => (f.to_le_bytes().to_vec(), DbType::F64),
                Cell::Null => (Vec::new(), DbType::NULL),
            };
            let mut block = MEMORY_POOL.acquire(bytes.len());
            block.into_slice_mut().copy_from_slice(&bytes);
            columns.push(Column {
                schema_id: schema_id_of(index as u64),
                data: block,
                column_type,
            });
        }
        let length = columns.iter().map(|c| c.data.len()).sum::<usize>();
        let row = Row {
            position: 0,
            columns,
            #[cfg(feature = "enable_long_row")]
            length: length as u64,
            #[cfg(not(feature = "enable_long_row"))]
            length: length as u32,
        };
        row.extend_serialized(&mut out);
    }
    out
}

fn failure(message: &str) -> Vec<u8> {
    let mut out = vec![3u8];
    out.extend_from_slice(message.as_bytes());
    out
}

#[cfg(test)]
pub(crate) fn rows_for_test(reply: Item, args: &[&str]) -> Result<Vec<Vec<Cell>>, String> {
    let args: Vec<Vec<u8>> = args.iter().map(|a| a.as_bytes().to_vec()).collect();
    let (_, shape) = plan(&args)?;
    rows_of(reply, shape)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenizes_words_and_quoted_strings() {
        assert!(is_kv_statement("kv get x"));
        assert!(is_kv_statement("  KV\tSET a b"));
        assert!(!is_kv_statement("KVX get"));
        assert!(!is_kv_statement("SELECT 1"));
        let args = tokenize("KV SET 'user:1' '{\"a\": ''x''}' EX 10;").unwrap();
        assert_eq!(
            args,
            vec![b"SET".to_vec(), b"user:1".to_vec(), br#"{"a": 'x'}"#.to_vec(), b"EX".to_vec(), b"10".to_vec()]
        );
        assert!(tokenize("KV SET 'open").is_err());
        assert!(tokenize("KV SET ab'c d").is_err());
    }

    #[test]
    fn describe_matches_shape() {
        assert_eq!(result_fields("KV GET k"), vec![("value".to_string(), DbType::STRING)]);
        assert_eq!(result_fields("KV INCR k"), vec![("value".to_string(), DbType::I64)]);
        assert_eq!(
            result_fields("KV ZRANGE z 0 -1 WITHSCORES"),
            vec![("member".to_string(), DbType::STRING), ("score".to_string(), DbType::F64)]
        );
        assert_eq!(result_fields("KV HGETALL h").len(), 2);
        assert_eq!(result_fields("KV SCAN 0").len(), 2);
    }

    #[test]
    fn connection_commands_are_refused() {
        for sql in ["KV MULTI", "KV SUBSCRIBE c", "KV SELECT 1", "KV AUTH x"] {
            let args = tokenize(sql).unwrap();
            assert!(plan(&args).is_err(), "{sql}");
        }
    }
}
