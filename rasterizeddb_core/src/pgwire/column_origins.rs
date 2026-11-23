//! sqlx's column-origin query, answered from the tables.
//!
//! Since a `RowDescription` names each column's table and number
//! (`catalog_model::column_origins`), sqlx 0.9 resolves them on every
//! describe with
//!
//! ```sql
//! SELECT attrelid table_oid, attrelid::regclass::text table_name,
//!        array_agg(ROW(attnum, attname)) AS columns
//!   FROM (VALUES (16384, 1), …) lookup(table_oid, attribute_num)
//!  INNER JOIN pg_catalog.pg_attribute ON lookup.table_oid = attrelid AND lookup.attribute_num = attnum
//!  GROUP BY attrelid
//! ```
//!
//! and reads `table_oid` as an `oid` and `columns` as an array of records
//! (`Vec<(i16, Arc<str>)>`). The engine has no record values, so the query
//! failed ("function 'row' is not supported yet") and with it every
//! `sqlx::query!` describe. It is answered here as PostgreSQL answers it:
//! `oid`, `text`, `record[]`.

use super::*;

/// The `(table oid, attnum)` pairs of sqlx's query, `None` for any other
/// statement.
pub(super) fn sqlx_origin_pairs(stmt: &str) -> Option<Vec<(i64, i64)>> {
    let lower = stmt.to_ascii_lowercase();
    if !lower.contains("lookup(table_oid, attribute_num)")
        || !lower.contains("pg_attribute")
        || !lower.contains("attnum, attname)")
        || !lower.contains("array_agg(")
    {
        return None;
    }
    let values_at = lower.find("(values")? + "(values".len();
    let end = lower.find(") lookup(table_oid, attribute_num)")?;
    if end <= values_at {
        return None;
    }
    let mut pairs = Vec::new();
    for tuple in crate::core::sql_scan::split_top_level(&stmt[values_at..end], b',') {
        let inner = tuple.trim().strip_prefix('(')?.strip_suffix(')')?;
        let (oid, attnum) = inner.split_once(',')?;
        pairs.push((oid.trim().parse().ok()?, attnum.trim().parse().ok()?));
    }
    Some(pairs)
}

/// A name as `regclass` prints it: bare when it needs no quotes.
fn regclass_text(name: &str) -> String {
    let plain = name.bytes().next().is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if plain { name.to_string() } else { crate::core::rql::views::quote_ident(name) }
}

/// `{"(1,id)","(2,name)"}`: an array of `(attnum, attname)` records in
/// PostgreSQL's text form, each field and element quoted as it needs.
fn record_array(columns: &[(i64, String)]) -> String {
    let field = |s: &str| {
        if s.is_empty() || s.bytes().any(|b| matches!(b, b',' | b'(' | b')' | b'"' | b'\\') || b.is_ascii_whitespace()) {
            format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\"\""))
        } else {
            s.to_string()
        }
    };
    let elements: Vec<String> = columns
        .iter()
        .map(|(attnum, name)| {
            let record = format!("({attnum},{})", field(name));
            format!("\"{}\"", record.replace('\\', "\\\\").replace('"', "\\\""))
        })
        .collect();
    format!("{{{}}}", elements.join(","))
}

pub(super) async fn answer<S>(
    database: &Arc<Database>,
    stream: &mut S,
    pairs: &[(i64, i64)],
    omit_description: bool,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let rows = crate::core::rql::catalog_model::origin_names(database, pairs);
    if !omit_description {
        // name, table oid, column number, type oid, type length, modifier, format
        let fields: [(&str, i32, i16); 3] = [("table_oid", 26, 4), ("table_name", 25, -1), ("columns", 2287, -1)];
        let mut payload = Vec::new();
        payload.extend_from_slice(&(fields.len() as i16).to_be_bytes());
        for (name, oid, len) in fields {
            payload.extend_from_slice(name.as_bytes());
            payload.push(0);
            payload.extend_from_slice(&0i32.to_be_bytes());
            payload.extend_from_slice(&0i16.to_be_bytes());
            payload.extend_from_slice(&oid.to_be_bytes());
            payload.extend_from_slice(&len.to_be_bytes());
            payload.extend_from_slice(&(-1i32).to_be_bytes());
            payload.extend_from_slice(&0i16.to_be_bytes());
        }
        send_message(stream, b'T', &payload).await?;
    }
    for (oid, name, columns) in &rows {
        let oid = oid.to_string();
        let name = regclass_text(name);
        let columns = record_array(columns);
        send_data_row(stream, &[Some(oid.as_bytes()), Some(name.as_bytes()), Some(columns.as_bytes())]).await?;
    }
    send_command_complete(stream, &format!("SELECT {}", rows.len())).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlx_s_query_is_recognised() {
        let sql = "SELECT\nattrelid table_oid,\nattrelid::regclass::text table_name,\narray_agg(ROW(attnum, attname)) AS columns\n\
                   FROM (VALUES (16384, 1), (16384, 2), (17000, 3)) lookup(table_oid, attribute_num)\n\
                   INNER JOIN pg_catalog.pg_attribute ON lookup.table_oid = attrelid AND lookup.attribute_num = attnum\nGROUP BY attrelid";
        assert_eq!(sqlx_origin_pairs(sql), Some(vec![(16384, 1), (16384, 2), (17000, 3)]));
        assert_eq!(sqlx_origin_pairs("SELECT attnum, attname FROM pg_attribute"), None);
    }

    #[test]
    fn records_are_written_as_postgresql_writes_them() {
        assert_eq!(record_array(&[(1, "id".into()), (2, "a b".into())]), r#"{"(1,id)","(2,\"a b\")"}"#);
        assert_eq!(record_array(&[(3, "q\"x".into())]), r#"{"(3,\"q\"\"x\")"}"#);
        assert_eq!(regclass_text("users"), "users");
        assert_eq!(regclass_text("Users"), "\"Users\"");
    }
}
