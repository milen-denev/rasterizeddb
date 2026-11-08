//! Relations a `FROM` list computes rather than reads: `(VALUES (…), (…))
//! alias(cols)`, `generate_series(start, stop [, step])` and `unnest(array
//! [, …])`, each optionally `WITH ORDINALITY`.
//!
//! Each was "relation '…' does not exist", though `generate_series` is how
//! applications fill a calendar and test fixtures make rows, and `unnest`
//! how an ORM passes a list. Their arguments are constants, so the rows are
//! computed once and the item materialised into a statement temporary, as a
//! CTE or a derived table is (`cte::materialize_derived`); the describe path
//! takes the columns alone (`columns_of`). A call reading another relation's
//! column (`generate_series(1, t.n)`) is refused here with a message
//! `lateral::is_outer_reference_refusal` recognises, and is then run as the
//! implicit `LATERAL` item it is (`core::rql::lateral`). A set-returning call
//! inside an expression is refused (0A000).

use crate::core::db_type::DbType;
use crate::core::rql::expr::{ArithOp, Value};
use crate::core::rql::pg_numeric::PgNum;

/// The most rows one item may compute. A statement temporary is held in
/// memory, so a series past this is refused rather than allowed to exhaust it.
pub const MAX_ROWS: usize = 1_000_000;

/// One computed `FROM` item found in a statement.
#[derive(Debug, Clone)]
pub struct Item {
    /// The byte range of the item in the statement, alias and column list
    /// included.
    pub start: usize,
    pub end: usize,
    pub kind: Kind,
    /// The alias the statement gives it, or the function's name.
    pub alias: String,
    /// The column names as the statement gives them.
    pub columns: Vec<String>,
    pub ordinality: bool,
}

#[derive(Debug, Clone)]
pub enum Kind {
    /// Each row's values as written.
    Values(Vec<Vec<String>>),
    GenerateSeries(Vec<String>),
    Unnest(Vec<String>),
}

/// A computed relation: its column names and types, and its rows as SQL
/// literals (`NULL` for a NULL).
#[derive(Debug, Clone)]
pub struct Computed {
    pub columns: Vec<(String, DbType, String)>,
    pub rows: Vec<Vec<String>>,
}

const CLAUSE_ENDS: &[&str] = &["WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "FOR", "WINDOW", "RETURNING", "ON"];

/// The first computed item of the statement's top-level `FROM` list (or a
/// `JOIN`'s right side), `None` when it has none.
pub fn next_item(sql: &str) -> Option<Item> {
    use crate::core::rql::join::find_keyword;
    let mut search = 0usize;
    while let Some(from) = find_keyword(sql, "FROM", search) {
        let list_start = from + 4;
        let list_end = CLAUSE_ENDS
            .iter()
            .filter_map(|k| find_keyword(sql, k, list_start))
            .filter(|p| *p > list_start)
            .min()
            .unwrap_or(sql.len());
        // Item starts: after FROM, after a top-level comma, after JOIN.
        let mut starts = vec![list_start];
        let bytes = sql.as_bytes();
        let (mut depth, mut i) = (0i32, list_start);
        while i < list_end {
            if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
                i = end.max(i + 1);
                continue;
            }
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                b',' if depth == 0 => starts.push(i + 1),
                _ => {}
            }
            i += 1;
        }
        let mut join_from = list_start;
        while let Some(at) = find_keyword(sql, "JOIN", join_from).filter(|p| *p < list_end) {
            starts.push(at + 4);
            join_from = at + 4;
        }
        starts.sort_unstable();
        for start in starts {
            let lead = sql[start..].len() - sql[start..].trim_start().len();
            if let Some(item) = item_at(sql, start + lead, list_end) {
                return Some(item);
            }
        }
        search = list_start;
    }
    None
}

fn item_at(sql: &str, at: usize, limit: usize) -> Option<Item> {
    let rest = &sql[at..];
    let upper: String = rest.chars().take(20).collect::<String>().to_ascii_uppercase();
    let (kind_name, open) = if upper.starts_with("GENERATE_SERIES") {
        ("GENERATE_SERIES", at + "GENERATE_SERIES".len())
    } else if upper.starts_with("UNNEST") {
        ("UNNEST", at + "UNNEST".len())
    } else if rest.starts_with('(') && rest[1..].trim_start().to_ascii_uppercase().starts_with("VALUES") {
        ("VALUES", at)
    } else {
        return None;
    };
    let open = open + (sql[open..].len() - sql[open..].trim_start().len());
    if sql.as_bytes().get(open) != Some(&b'(') {
        return None;
    }
    let close = crate::core::sql_scan::matching_close(sql, open)?;
    let inside = &sql[open + 1..close];
    let kind = match kind_name {
        "VALUES" => {
            let body = inside.trim_start();
            let body = body[6..].trim();
            let mut rows = Vec::new();
            for tuple in crate::core::sql_scan::split_top_level(body, b',') {
                let t = tuple.trim();
                let inner = t.strip_prefix('(').and_then(|x| x.strip_suffix(')'))?;
                rows.push(crate::core::sql_scan::split_top_level(inner, b',').iter().map(|v| v.trim().to_string()).collect());
            }
            Kind::Values(rows)
        }
        name => {
            let args: Vec<String> = if inside.trim().is_empty() {
                Vec::new()
            } else {
                crate::core::sql_scan::split_top_level(inside, b',').iter().map(|a| a.trim().to_string()).collect()
            };
            if name == "UNNEST" { Kind::Unnest(args) } else { Kind::GenerateSeries(args) }
        }
    };
    // The tail: [WITH ORDINALITY] [AS] [alias] [(columns)].
    let mut cursor = close + 1;
    let word_at = |c: usize| -> (usize, String) {
        let s = &sql[c..limit.max(c)];
        let lead = s.len() - s.trim_start().len();
        let w: String = s[lead..].chars().take_while(|ch| ch.is_alphanumeric() || *ch == '_' || *ch == '"').collect();
        (c + lead, w)
    };
    let mut ordinality = false;
    let (p, w) = word_at(cursor);
    if w.eq_ignore_ascii_case("WITH") {
        let (q, w2) = word_at(p + 4);
        if w2.eq_ignore_ascii_case("ORDINALITY") {
            ordinality = true;
            cursor = q + w2.len();
        }
    }
    let (p, w) = word_at(cursor);
    let mut alias = None;
    if w.eq_ignore_ascii_case("AS") {
        let (q, name) = word_at(p + 2);
        alias = Some(name.trim_matches('"').to_string());
        cursor = q + name.len();
    } else if !w.is_empty() && !is_keyword(&w) {
        alias = Some(w.trim_matches('"').to_string());
        cursor = p + w.len();
    }
    let mut columns = Vec::new();
    let s = &sql[cursor..limit.max(cursor)];
    let lead = s.len() - s.trim_start().len();
    if alias.is_some() && s[lead..].starts_with('(') {
        let open = cursor + lead;
        let close = crate::core::sql_scan::matching_close(sql, open)?;
        columns = sql[open + 1..close].split(',').map(|c| c.trim().trim_matches('"').to_string()).collect();
        cursor = close + 1;
    }
    let default = match kind_name {
        "GENERATE_SERIES" => "generate_series",
        "UNNEST" => "unnest",
        _ => "",
    };
    let alias = match alias {
        Some(a) if !a.is_empty() => a,
        _ if kind_name == "VALUES" => return None,
        _ => default.to_string(),
    };
    Some(Item { start: at, end: cursor, kind, alias, columns, ordinality })
}

fn is_keyword(word: &str) -> bool {
    [
        "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "FOR", "WINDOW", "JOIN",
        "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL", "ON", "USING", "RETURNING", "WITH",
    ]
    .iter()
    .any(|k| k.eq_ignore_ascii_case(word))
}

/// A constant argument's value. One reading a column has no value here: a
/// lateral reference.
fn constant(text: &str, what: &str) -> Result<Value, String> {
    let expr = crate::core::rql::expr::parse(text, &[]).map_err(|_| {
        format!("{what} over another relation's column ('{text}', a LATERAL reference) is not supported yet")
    })?;
    let value = crate::core::rql::expr::eval_aggregate_argument(&expr, &crate::core::row::row::Row::default(), &[])?;
    match crate::core::rql::expr::display_scale(&expr) {
        Some(scale) => Ok(crate::core::rql::pg_functions::shown_at(value, scale)),
        None => Ok(value),
    }
}

fn literal_of(value: &Value) -> Result<String, String> {
    match value {
        Value::Null => Ok("NULL".to_string()),
        Value::Data { bytes, db_type: DbType::DECIMAL } => {
            Ok(crate::core::rql::expr::decimal_value_as_pgnum(bytes)?.to_text())
        }
        Value::Data { bytes, db_type } => crate::core::rql::subquery::value_to_literal(db_type, bytes)
            .ok_or_else(|| format!("a {} value could not be materialised", crate::core::value_codec::pg_input_type_name(db_type))),
    }
}

/// The SQL type a column of `db_type` is declared with in the temporary.
fn sql_type(db_type: &DbType, scale: Option<u32>, timestamp: bool) -> String {
    match db_type {
        DbType::I32 => "INTEGER".into(),
        DbType::I64 => "BIGINT".into(),
        DbType::DECIMAL => match scale {
            Some(s) => format!("NUMERIC(38,{s})"),
            None => "NUMERIC".into(),
        },
        DbType::F64 => "DOUBLE PRECISION".into(),
        DbType::BOOL => "BOOLEAN".into(),
        DbType::DATE => "DATE".into(),
        DbType::DATETIME if timestamp => "TIMESTAMP".into(),
        DbType::DATETIME => "TIMESTAMPTZ".into(),
        DbType::INTERVAL => "INTERVAL".into(),
        DbType::UUID => "UUID".into(),
        DbType::JSONB => "JSONB".into(),
        _ => "TEXT".into(),
    }
}

/// The item's rows, computed from its constant arguments.
pub fn compute(item: &Item) -> Result<Computed, String> {
    let mut computed = match &item.kind {
        Kind::GenerateSeries(args) => generate_series(args, &item.alias)?,
        Kind::Unnest(args) => unnest(args)?,
        Kind::Values(rows) => values(rows)?,
    };
    if item.ordinality {
        computed.columns.push(("ordinality".into(), DbType::I64, "BIGINT".into()));
        for (i, row) in computed.rows.iter_mut().enumerate() {
            row.push((i + 1).to_string());
        }
    }
    // A column list renames the columns, in order (fewer names leave the
    // rest as they were).
    if item.columns.len() > computed.columns.len() {
        return Err(format!(
            "table \"{}\" has {} columns available but {} columns specified",
            item.alias,
            computed.columns.len(),
            item.columns.len()
        ));
    }
    for (column, name) in computed.columns.iter_mut().zip(&item.columns) {
        column.0 = name.clone();
    }
    // A single-column function named by its alias alone takes the alias's
    // name, as PostgreSQL names it.
    if item.columns.is_empty() && matches!(item.kind, Kind::GenerateSeries(_) | Kind::Unnest(_)) && computed.columns.len() - usize::from(item.ordinality) == 1 {
        computed.columns[0].0 = item.alias.clone();
    }
    let names: Vec<&str> = computed.columns.iter().map(|c| c.0.as_str()).collect();
    for (i, n) in names.iter().enumerate() {
        if names[..i].iter().any(|m| m.eq_ignore_ascii_case(n)) {
            return Err(format!("the computed relation '{}' names column \"{n}\" twice; give it a column list", item.alias));
        }
    }
    Ok(computed)
}

fn generate_series(args: &[String], alias: &str) -> Result<Computed, String> {
    if !(2..=3).contains(&args.len()) {
        return Err(format!("function generate_series({} arguments) does not exist", args.len()));
    }
    let values: Vec<Value> = args.iter().map(|a| constant(a, "generate_series")).collect::<Result<_, _>>()?;
    if values.iter().any(Value::is_null) {
        return Ok(Computed { columns: vec![(alias.to_string(), DbType::I32, "INTEGER".into())], rows: Vec::new() });
    }
    let ty = |v: &Value| v.db_type().cloned().unwrap_or(DbType::NULL);
    let integer = |t: &DbType| matches!(t, DbType::I16 | DbType::I32 | DbType::I64);
    let (start_t, stop_t) = (ty(&values[0]), ty(&values[1]));
    let too_many = || format!("generate_series producing more than {MAX_ROWS} rows is not supported");
    // Integers.
    if integer(&start_t) && integer(&stop_t) && values.get(2).is_none_or(|s| integer(&ty(s))) {
        let int = |v: &Value| crate::core::rql::expr::as_i64(v).ok_or_else(|| "an integer could not be read".to_string());
        let (start, stop) = (int(&values[0])?, int(&values[1])?);
        let step = match values.get(2) {
            Some(s) => int(s)?,
            None => 1,
        };
        if step == 0 {
            return Err("step size cannot equal zero".to_string());
        }
        let wide = [&start_t, &stop_t].into_iter().chain(values.get(2).map(|_| &start_t)).any(|t| *t == DbType::I64)
            || values.iter().any(|v| ty(v) == DbType::I64);
        let mut rows = Vec::new();
        let mut v = start;
        while (step > 0 && v <= stop) || (step < 0 && v >= stop) {
            if rows.len() == MAX_ROWS {
                return Err(too_many());
            }
            rows.push(vec![v.to_string()]);
            match v.checked_add(step) {
                Some(next) => v = next,
                None => break,
            }
        }
        let (t, s) = if wide { (DbType::I64, "BIGINT") } else { (DbType::I32, "INTEGER") };
        return Ok(Computed { columns: vec![(alias.to_string(), t, s.into())], rows });
    }
    // NUMERIC (an integer mixed in counts as one).
    let numeric = |t: &DbType| *t == DbType::DECIMAL || integer(t);
    if values.iter().all(|v| numeric(&ty(v))) {
        let num = |v: &Value| -> Result<PgNum, String> {
            match v {
                Value::Data { bytes, db_type: DbType::DECIMAL } => crate::core::rql::expr::decimal_value_as_pgnum(bytes),
                other => Ok(PgNum::int(crate::core::rql::expr::as_i64(other).ok_or("an integer could not be read")? as i128)),
            }
        };
        let (start, stop) = (num(&values[0])?, num(&values[1])?);
        let step = match values.get(2) {
            Some(s) => num(s)?,
            None => PgNum::int(1),
        };
        if step.m == 0 {
            return Err("step size cannot equal zero".to_string());
        }
        let scale = start.s.max(stop.s).max(step.s);
        let up = step.m > 0;
        let mut rows = Vec::new();
        let mut v = start;
        loop {
            let ord = v.cmp_value(stop);
            if (up && ord == std::cmp::Ordering::Greater) || (!up && ord == std::cmp::Ordering::Less) {
                break;
            }
            if rows.len() == MAX_ROWS {
                return Err(too_many());
            }
            rows.push(vec![v.round_to(scale).ok_or("numeric out of range")?.to_text()]);
            v = v.add(step).ok_or("numeric out of range")?;
        }
        return Ok(Computed { columns: vec![(alias.to_string(), DbType::DECIMAL, format!("NUMERIC(38,{scale})"))], rows });
    }
    // Timestamps (a date is its midnight) by an interval.
    let instant = |t: &DbType| matches!(t, DbType::DATETIME | DbType::TIMESTAMP | DbType::DATE);
    if instant(&start_t) && instant(&stop_t) && values.get(2).is_some_and(|s| ty(s) == DbType::INTERVAL) {
        let step = &values[2];
        let iv = match step {
            Value::Data { bytes, .. } => crate::core::pg_types::decode_interval(bytes).ok_or("an interval could not be read")?,
            Value::Null => unreachable!("NULL answered above"),
        };
        if iv.months == 0 && iv.days == 0 && iv.micros == 0 {
            return Err("step size cannot equal zero".to_string());
        }
        let forward = iv.months > 0 || (iv.months == 0 && (iv.days > 0 || (iv.days == 0 && iv.micros > 0)));
        let midnight = |v: &Value| -> Result<Value, String> {
            match v {
                Value::Data { bytes, db_type: DbType::DATE } => {
                    let days = i32::from_be_bytes(bytes.as_slice().try_into().map_err(|_| "a date could not be read")?);
                    let us = (days as i64 + crate::core::pg_types::PG_EPOCH_DAYS as i64) * 86_400_000_000;
                    Ok(Value::Data { bytes: us.to_le_bytes().to_vec(), db_type: DbType::DATETIME })
                }
                other => Ok(other.clone()),
            }
        };
        let (mut v, stop) = (midnight(&values[0])?, midnight(&values[1])?);
        // `timestamp` written as such (or a date, which PostgreSQL widens to
        // `timestamptz`): what the column is declared as.
        let without_zone = !args[0].to_ascii_lowercase().contains("tz") && start_t != DbType::DATE && !args[0].to_ascii_lowercase().contains("with time zone");
        let mut rows = Vec::new();
        loop {
            let ord = crate::core::rql::expr::compare(&v, &stop).ok_or("timestamps could not be compared")?;
            if (forward && ord == std::cmp::Ordering::Greater) || (!forward && ord == std::cmp::Ordering::Less) {
                break;
            }
            if rows.len() == MAX_ROWS {
                return Err(too_many());
            }
            let Value::Data { bytes, .. } = &v else { break };
            let us = i64::from_le_bytes(bytes.as_slice().try_into().map_err(|_| "a timestamp could not be read")?);
            let text = crate::core::value_codec::format_timestamp_us(us);
            rows.push(vec![format!("'{text}{}'", if without_zone { "" } else { "+00" })]);
            v = crate::core::rql::expr::arith(&v, ArithOp::Add, step)?;
        }
        let (t, s) = if without_zone { (DbType::DATETIME, "TIMESTAMP") } else { (DbType::DATETIME, "TIMESTAMPTZ") };
        return Ok(Computed { columns: vec![(alias.to_string(), t, s.into())], rows });
    }
    Err(format!(
        "function generate_series({}) does not exist",
        values.iter().map(|v| crate::core::value_codec::pg_input_type_name(&ty(v))).collect::<Vec<_>>().join(", ")
    ))
}

/// An array argument's elements as literals, and the element SQL type.
fn array_argument(text: &str) -> Result<(Vec<Option<String>>, DbType, String), String> {
    let trimmed = text.trim();
    // `ARRAY[a, b, …]`: each element a constant.
    if trimmed.len() > 6 && trimmed[..5].eq_ignore_ascii_case("ARRAY") && trimmed[5..].trim_start().starts_with('[') {
        let open = trimmed.find('[').expect("checked");
        let close = trimmed.rfind(']').ok_or("an ARRAY constructor is not closed")?;
        let inner = &trimmed[open + 1..close];
        let mut elements = Vec::new();
        let mut types: Vec<DbType> = Vec::new();
        let mut scale = 0u32;
        for e in crate::core::sql_scan::split_top_level(inner, b',') {
            let e = e.trim();
            if e.is_empty() {
                continue;
            }
            let v = constant(e, "unnest")?;
            if let Some(t) = v.db_type() {
                types.push(t.clone());
            }
            if let Value::Data { bytes, db_type: DbType::DECIMAL } = &v {
                scale = scale.max(crate::core::rql::expr::decimal_value_as_pgnum(bytes)?.s);
            }
            elements.push(match &v {
                Value::Null => None,
                _ => Some(literal_of(&v)?),
            });
        }
        let ty = unify(&types);
        let sql = sql_type(&ty, Some(scale), true);
        return Ok((elements, ty, sql));
    }
    // An array value: `'{1,2}'::int[]`, a constant expression.
    let (body, element_type) = match trimmed.rfind("::") {
        Some(at) => (&trimmed[..at], trimmed[at + 2..].trim().to_ascii_lowercase()),
        None => (trimmed, "text[]".to_string()),
    };
    let element_type = element_type.trim_end_matches("[]").to_string();
    let (ty, sql) = match element_type.as_str() {
        "int" | "int4" | "integer" => (DbType::I32, "INTEGER"),
        "int8" | "bigint" => (DbType::I64, "BIGINT"),
        "numeric" | "decimal" => (DbType::DECIMAL, "NUMERIC"),
        "float8" | "double precision" => (DbType::F64, "DOUBLE PRECISION"),
        "bool" | "boolean" => (DbType::BOOL, "BOOLEAN"),
        "date" => (DbType::DATE, "DATE"),
        "uuid" => (DbType::UUID, "UUID"),
        _ => (DbType::STRING, "TEXT"),
    };
    let value = constant(body, "unnest")?;
    let text = match &value {
        Value::Null => return Ok((Vec::new(), ty, sql.into())),
        Value::Data { bytes, db_type: DbType::ARRAY } => crate::core::value_codec::format_array(bytes).unwrap_or_default(),
        other => crate::core::rql::expr::as_text(other),
    };
    let elements = crate::core::rql::pg_functions::array_elements_of(&text)?
        .into_iter()
        .map(|e| e.map(|v| if matches!(ty, DbType::I32 | DbType::I64 | DbType::DECIMAL | DbType::F64 | DbType::BOOL) { v } else { crate::core::value_codec::quote_sql(&v) }))
        .collect();
    Ok((elements, ty, sql.into()))
}

/// The type PostgreSQL resolves a set of constants to: integers widen to
/// `bigint` and then `numeric`, a numeric among integers is `numeric`,
/// anything else alone keeps its type, text otherwise.
fn unify(types: &[DbType]) -> DbType {
    let distinct: Vec<&DbType> = types.iter().filter(|t| **t != DbType::NULL).collect();
    if distinct.is_empty() {
        return DbType::STRING;
    }
    if distinct.iter().all(|t| matches!(t, DbType::I16 | DbType::I32)) {
        return DbType::I32;
    }
    if distinct.iter().all(|t| matches!(t, DbType::I16 | DbType::I32 | DbType::I64)) {
        return DbType::I64;
    }
    if distinct.iter().all(|t| matches!(t, DbType::I16 | DbType::I32 | DbType::I64 | DbType::DECIMAL)) {
        return DbType::DECIMAL;
    }
    if distinct.iter().all(|t| matches!(t, DbType::I16 | DbType::I32 | DbType::I64 | DbType::DECIMAL | DbType::F32 | DbType::F64)) {
        return DbType::F64;
    }
    if distinct.iter().all(|t| **t == *distinct[0]) {
        return distinct[0].clone();
    }
    DbType::STRING
}

fn unnest(args: &[String]) -> Result<Computed, String> {
    if args.is_empty() {
        return Err("function unnest() does not exist".to_string());
    }
    let arrays: Vec<(Vec<Option<String>>, DbType, String)> = args.iter().map(|a| array_argument(a)).collect::<Result<_, _>>()?;
    let len = arrays.iter().map(|a| a.0.len()).max().unwrap_or(0);
    if len > MAX_ROWS {
        return Err(format!("unnest producing more than {MAX_ROWS} rows is not supported"));
    }
    let columns = arrays.iter().map(|(_, t, s)| ("unnest".to_string(), t.clone(), s.clone())).collect();
    let rows = (0..len)
        .map(|i| arrays.iter().map(|a| a.0.get(i).cloned().flatten().unwrap_or_else(|| "NULL".into())).collect())
        .collect();
    Ok(Computed { columns, rows })
}

fn values(rows: &[Vec<String>]) -> Result<Computed, String> {
    let width = rows.first().map_or(0, Vec::len);
    if rows.iter().any(|r| r.len() != width) {
        return Err("VALUES lists must all be the same length".to_string());
    }
    let evaluated: Vec<Vec<Value>> = rows
        .iter()
        .map(|r| r.iter().map(|v| constant(v, "VALUES")).collect::<Result<Vec<_>, _>>())
        .collect::<Result<_, _>>()?;
    let mut columns = Vec::new();
    for c in 0..width {
        let types: Vec<DbType> = evaluated.iter().filter_map(|r| r[c].db_type().cloned()).collect();
        let ty = unify(&types);
        let mut scale = 0u32;
        for r in &evaluated {
            if let Value::Data { bytes, db_type: DbType::DECIMAL } = &r[c] {
                scale = scale.max(crate::core::rql::expr::decimal_value_as_pgnum(bytes)?.s);
            }
        }
        let timestamp = true;
        columns.push((format!("column{}", c + 1), ty.clone(), sql_type(&ty, Some(scale), timestamp)));
    }
    let rows = evaluated.iter().map(|r| r.iter().map(literal_of).collect::<Result<Vec<_>, _>>()).collect::<Result<_, _>>()?;
    Ok(Computed { columns, rows })
}

/// The item's columns, for a describe: computed as the rows would be (the
/// arguments are constants, so this has no side effect).
pub fn columns_of(item: &Item) -> Result<Vec<(String, DbType)>, String> {
    Ok(compute(item)?.columns.into_iter().map(|(n, t, _)| (n, t)).collect())
}

/// `SELECT generate_series(…) [AS name]` or `SELECT unnest(…) [AS name]`,
/// alone in a select list with no `FROM`: the `FROM` form it means.
/// `None` for anything else.
pub fn select_list_form(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let body = trimmed.get(..7).filter(|w| w.eq_ignore_ascii_case("SELECT "))?;
    let _ = body;
    let list = trimmed[7..].trim();
    if crate::core::rql::join::find_keyword(list, "FROM", 0).is_some() {
        return None;
    }
    let upper = list.to_ascii_uppercase();
    let name = if upper.starts_with("GENERATE_SERIES") {
        "generate_series"
    } else if upper.starts_with("UNNEST") {
        "unnest"
    } else {
        return None;
    };
    let open = list.find('(')?;
    if !list[name.len()..open].trim().is_empty() {
        return None;
    }
    let close = crate::core::sql_scan::matching_close(list, open)?;
    let call = &list[..=close];
    let rest = list[close + 1..].trim();
    let label = if rest.is_empty() {
        name.to_string()
    } else {
        let (as_kw, alias) = rest.split_once(char::is_whitespace).unwrap_or(("", rest));
        let alias = if as_kw.eq_ignore_ascii_case("AS") { alias.trim() } else { rest };
        if alias.contains(char::is_whitespace) || alias.is_empty() {
            return None;
        }
        alias.trim_matches('"').to_string()
    };
    Some(format!("SELECT {label} FROM {call} AS {label}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn items_are_found_with_their_alias_and_columns() {
        let sql = "SELECT t.id, g FROM t, generate_series(1, 2) g ORDER BY 1";
        let item = next_item(sql).unwrap();
        assert_eq!((item.alias.as_str(), &sql[item.start..item.end]), ("g", "generate_series(1, 2) g"));
        let sql = "SELECT * FROM unnest(ARRAY[1, 2], ARRAY['x', 'y']) AS z(a, b) WHERE a > 1";
        let item = next_item(sql).unwrap();
        assert_eq!((item.alias.as_str(), item.columns.clone()), ("z", vec!["a".to_string(), "b".to_string()]));
        let sql = "SELECT * FROM generate_series(1, 3) WITH ORDINALITY";
        let item = next_item(sql).unwrap();
        assert!(item.ordinality && item.alias == "generate_series");
        let sql = "SELECT * FROM t CROSS JOIN (VALUES (10), (20)) v(x)";
        let item = next_item(sql).unwrap();
        assert_eq!((item.alias.as_str(), item.columns.clone()), ("v", vec!["x".to_string()]));
        assert!(next_item("SELECT generate_series(1, 3)").is_none());
        assert!(next_item("SELECT a FROM t WHERE b IN (1, 2)").is_none());
    }

    #[test]
    fn a_series_is_computed_as_postgresql_does() {
        let rows = |sql: &str| compute(&next_item(sql).unwrap()).unwrap().rows.into_iter().map(|r| r.join("|")).collect::<Vec<_>>();
        assert_eq!(rows("SELECT * FROM generate_series(5, 1, -2) g"), ["5", "3", "1"]);
        assert!(rows("SELECT * FROM generate_series(1, 0) g").is_empty());
        assert_eq!(rows("SELECT * FROM generate_series(1.5, 3, 0.5) g"), ["1.5", "2.0", "2.5", "3.0"]);
        assert_eq!(rows("SELECT * FROM generate_series(1, 3) WITH ORDINALITY AS g"), ["1|1", "2|2", "3|3"]);
        assert!(compute(&next_item("SELECT * FROM generate_series(1, 5, 0) g").unwrap()).is_err());
        assert!(compute(&next_item("SELECT * FROM generate_series(1, t.n) g").unwrap()).unwrap_err().contains("LATERAL"));
    }
}
