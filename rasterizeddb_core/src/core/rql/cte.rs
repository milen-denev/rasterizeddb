//! Common table expressions: `WITH name AS (SELECT …) <statement>`.
//!
//! Each CTE is **materialised** into a temporary table and the outer statement
//! rewritten to read from it. That composes machinery that already works —
//! `CREATE TABLE`, multi-row `INSERT`, `DROP TABLE` — instead of adding a
//! second way to produce rows, and it gives PostgreSQL's pre-12 semantics: a
//! CTE is an optimisation fence, evaluated once however many times it is
//! referenced. That is a position worth documenting rather than an accident.
//!
//! `WITH RECURSIVE` is **refused by keyword**, before anything else looks at
//! the statement. A recursive CTE run as a non-recursive one does not fail: it
//! computes the base case, stops, and returns a shorter answer to a well-formed
//! query. Silently wrong results are worse than an unsupported feature.

use rclite::Arc;

use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::row::schema::SchemaField;

/// The columns of a relation that exists only while one statement is
/// described: a CTE or a derived table, resolved from its own query.
pub(crate) type ScratchFields = smallvec::SmallVec<[SchemaField; 20]>;

/// One `name AS ( body )` definition.
#[derive(Debug)]
pub struct CteDef {
    pub name: String,
    pub body: String,
}

/// A statement whose CTEs have been materialised.
#[derive(Debug)]
pub struct Materialized {
    /// The statement with every CTE reference pointing at its temporary table.
    pub rewritten: String,
    /// Tables to drop once the statement is done, however it ends.
    pub temps: Vec<String>,
}

/// True when `sql` starts with the `WITH` keyword as a whole word.
fn starts_with_with(sql: &str) -> bool {
    let t = sql.trim_start();
    t.len() >= 4
        && t[..4].eq_ignore_ascii_case("WITH")
        && t[4..]
            .chars()
            .next()
            .is_some_and(|c| c.is_whitespace() || c == '(')
}

/// Index just past the parenthesis matching the one at `open`, ignoring
/// parentheses inside string literals.
fn matching_paren(s: &str, open: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                b'\'' | b'"' => quote = Some(c),
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
        i += 1;
    }
    None
}

/// Splits `WITH a AS (…), b AS (…) <rest>` into its definitions and the rest.
///
/// `Ok(None)` when the statement has no `WITH` at all. `Err` when it has one
/// this cannot honour — including `WITH RECURSIVE`, which is refused rather
/// than approximated.
pub fn split_with(sql: &str) -> Result<Option<(Vec<CteDef>, String)>, String> {
    if !starts_with_with(sql) {
        return Ok(None);
    }
    let trimmed = sql.trim_start();
    let after_with = trimmed[4..].trim_start();

    // Refused by keyword, deliberately before any other check: run as a
    // non-recursive CTE this would return the base case and no error.
    if after_with.len() >= 9
        && after_with[..9].eq_ignore_ascii_case("RECURSIVE")
        && after_with[9..]
            .chars()
            .next()
            .is_some_and(|c| c.is_whitespace())
    {
        return Err(
            "WITH RECURSIVE is not supported yet; refusing rather than returning only the \
             base case"
                .to_string(),
        );
    }

    let mut rest = after_with;
    let mut defs: Vec<CteDef> = Vec::new();

    loop {
        // `name [(cols)] AS ( body )`
        let name_end = rest
            .find(|c: char| c.is_whitespace() || c == '(')
            .ok_or_else(|| "a WITH clause names no query".to_string())?;
        let name = rest[..name_end].trim().trim_matches('"').to_string();
        if name.is_empty() {
            return Err("a WITH clause names no query".to_string());
        }
        let after_name = rest[name_end..].trim_start();
        if after_name.starts_with('(') {
            return Err("a WITH clause that renames its columns is not supported yet".to_string());
        }
        if after_name.len() < 2 || !after_name[..2].eq_ignore_ascii_case("AS") {
            return Err(format!("expected AS after the WITH name '{}'", name));
        }
        let after_as = after_name[2..].trim_start();
        if !after_as.starts_with('(') {
            return Err(format!("expected ( after AS in the WITH clause '{}'", name));
        }
        let open = after_as.as_ptr() as usize - rest.as_ptr() as usize;
        let close = matching_paren(rest, open)
            .ok_or_else(|| format!("unterminated WITH body for '{}'", name))?;
        let body = rest[open + 1..close].trim().to_string();
        if body.is_empty() {
            return Err(format!("the WITH clause '{}' has an empty body", name));
        }
        defs.push(CteDef { name, body });

        let after_body = rest[close + 1..].trim_start();
        if let Some(next) = after_body.strip_prefix(',') {
            rest = next.trim_start();
            continue;
        }
        return Ok(Some((defs, after_body.to_string())));
    }
}

/// The output name of one select-list item, following PostgreSQL: an explicit
/// alias wins, otherwise a column keeps its own name.
///
/// `None` when the item has no name this can determine — refused rather than
/// invented, since the outer query refers to these by name.
fn item_label(item: &str) -> Option<String> {
    let item = item.trim();
    if item.is_empty() {
        return None;
    }
    match split_alias(item) {
        (_, Some(alias)) => Some(alias.to_string()),
        (expression, None) => bare_column(expression),
    }
}

/// One select-list item as its expression and its ` AS name` alias, if it has
/// one at the top level.
fn split_alias(item: &str) -> (&str, Option<&str>) {
    let upper = item.as_bytes().to_ascii_uppercase();
    let mut depth = 0usize;
    let mut i = 0usize;
    while i + 4 <= upper.len() {
        match upper[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 && upper[i..].starts_with(b" AS ") {
            let name = item[i + 4..].trim().trim_matches('"');
            if !name.is_empty() {
                return (&item[..i], Some(name));
            }
        }
        i += 1;
    }
    (item, None)
}

/// The column an expression names when it is nothing but a bare or qualified
/// column reference: `d.id` is `id`.
fn bare_column(expression: &str) -> Option<String> {
    let bare = expression.trim().trim_matches('"');
    if bare.contains('(') || bare.contains(' ') || bare == "*" {
        return None;
    }
    let last = bare.rsplit('.').next()?.trim_matches('"');
    if last.is_empty() || !last.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }
    Some(last.to_string())
}

/// The select-list text of a `SELECT … FROM …`, for naming the columns.
fn select_list_of(body: &str) -> Option<&str> {
    let upper = body.to_ascii_uppercase();
    if !upper.trim_start().starts_with("SELECT") {
        return None;
    }
    let start = upper.find("SELECT")? + "SELECT".len();
    // Whole-word, not the literal `" FROM "`: a CTE body written across lines
    // puts `FROM` at the start of one, preceded by a newline rather than a
    // space, and the literal search then found no `FROM` at all — so the body
    // was reported as "must be a SELECT" when it plainly was one. Every ORM
    // and every Rust raw-string query formats across lines. `find_keyword`
    // already respects quoting and nesting, which is what this used to
    // hand-roll.
    let at = crate::core::rql::join::find_keyword(body, "FROM", start)?;
    Some(&body[start..at])
}

/// The constant literals a `RETURNING` clause lists, when every item is one.
///
/// `RETURNING 1` publishes one row of that constant per row written — the count
/// is the meaning, and no column of the table is involved. Resolving `1` against
/// the schema fails, so the clause is dropped and the constants are rebuilt
/// from the statement's row count instead.
fn returning_constants(body: &str) -> Option<Vec<String>> {
    let (_, tail) = crate::core::rql::lexer_ir::returning_split(body)?;
    let items = crate::core::rql::lexer_ct::split_definitions(tail);
    if items.is_empty() {
        return None;
    }
    let mut literals = Vec::with_capacity(items.len());
    for item in &items {
        let item = item.trim();
        let is_literal = item.starts_with('\'')
            || item
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_digit() || c == '-')
            || item.eq_ignore_ascii_case("NULL");
        if !is_literal {
            return None;
        }
        literals.push(item.to_string());
    }
    Some(literals)
}

/// One column of a body made of named constants; see [`constant_body`].
pub(crate) struct ConstantColumn {
    pub label: String,
    pub db_type: DbType,
    /// What the materialised relation declares the column as.
    pub sql_type: String,
    /// The literal as written.
    pub value: String,
}

/// A body with no `FROM` whose items are all named literals —
/// `SELECT 50 AS a, 'x' AS b` — as its columns and their one row of values.
/// `None` for anything else, which then runs as a query.
///
/// Such a body names no relation, so the query path cannot run it and
/// refused it as "cannot parse subquery". It is one row of constants, which
/// is exactly what a `VALUES` tuple is. Each type is the one PostgreSQL gives
/// the literal: an integer that fits is `int4`, then `int8`, then `numeric`,
/// a decimal is `numeric`, and a string is `text`. Shared with the describe
/// path, so the client is told the types the relation is built with.
pub(crate) fn constant_body(body: &str) -> Option<Vec<ConstantColumn>> {
    let trimmed = body.trim().trim_end_matches(';').trim_end();
    if trimmed.len() < "SELECT".len() || !trimmed[.."SELECT".len()].eq_ignore_ascii_case("SELECT") {
        return None;
    }
    let list = &trimmed["SELECT".len()..];
    if !list.starts_with(char::is_whitespace) {
        return None;
    }
    for keyword in ["FROM", "WHERE", "UNION", "GROUP", "ORDER", "LIMIT", "OFFSET"] {
        if crate::core::rql::join::find_keyword(list, keyword, 0).is_some() {
            return None;
        }
    }
    let mut columns = Vec::new();
    for item in crate::core::rql::lexer_ct::split_definitions(list) {
        let (expression, alias) = split_alias(item.trim());
        let label = alias?.to_string();
        let value = expression.trim();
        let (db_type, sql_type) = if value.eq_ignore_ascii_case("NULL") {
            (DbType::STRING, "TEXT".to_string())
        } else if value.eq_ignore_ascii_case("TRUE") || value.eq_ignore_ascii_case("FALSE") {
            (DbType::BOOL, "BOOLEAN".to_string())
        } else if value.len() >= 2
            && value.starts_with('\'')
            && value.ends_with('\'')
            && !value[1..value.len() - 1].replace("''", "").contains('\'')
        {
            (DbType::STRING, "TEXT".to_string())
        } else if let Ok(n) = value.parse::<i64>() {
            if i32::try_from(n).is_ok() {
                (DbType::I32, "INTEGER".to_string())
            } else {
                (DbType::I64, "BIGINT".to_string())
            }
        } else if value.parse::<f64>().is_ok()
            && value.chars().all(|c| c.is_ascii_digit() || matches!(c, '-' | '+' | '.'))
        {
            // A numeric literal keeps the places it was written with, as
            // PostgreSQL's does: `1.5` prints as 1.5, not at the engine's nine.
            let places = value.split_once('.').map_or(0, |(_, places)| places.len());
            if places > crate::core::rql::type_modifier::MAX_DECIMAL_SCALE as usize {
                return None;
            }
            (DbType::DECIMAL, format!("NUMERIC(38,{})", places))
        } else {
            return None;
        };
        columns.push(ConstantColumn {
            label,
            db_type,
            sql_type,
            value: value.to_string(),
        });
    }
    if columns.is_empty() {
        return None;
    }
    Some(columns)
}

/// The output names of a `SELECT` that is about to be materialised.
///
/// Each item's own name, as PostgreSQL gives it. A list holding a `*` has
/// no name per item, so its names are taken from the query's description,
/// which expands the star the way the outer statement's describe will:
/// `WITH x AS (SELECT * FROM t)` was refused as "a column this cannot name".
async fn select_labels(query: &str, database: &Arc<Database>) -> Option<Vec<String>> {
    let list = select_list_of(query)?;
    let items = crate::core::rql::lexer_ct::split_definitions(list);
    if let Some(labels) = items
        .iter()
        .map(|item| item_label(item))
        .collect::<Option<Vec<String>>>()
    {
        return Some(labels);
    }
    let starred = items.iter().any(|item| {
        let item = item.trim();
        item == "*" || item.ends_with(".*")
    });
    if !starred {
        return None;
    }
    match Box::pin(crate::pgwire::describe_query(database, query)).await {
        Ok(Some((fields, true))) if !fields.is_empty() => {
            Some(fields.into_iter().map(|(name, _)| name).collect())
        }
        _ => None,
    }
}

/// The column names a data-modifying CTE exposes, taken from its `RETURNING`
/// clause.
///
/// `WITH x AS (INSERT … RETURNING id)` publishes `id` to the statement that
/// follows, exactly as a SELECT publishes its select list.
fn returning_labels(body: &str) -> Option<Vec<String>> {
    let (_, tail) = crate::core::rql::lexer_ir::returning_split(body)?;
    crate::core::rql::lexer_ct::split_definitions(tail)
        .iter()
        .map(|item| item_label(item))
        .collect::<Option<Vec<String>>>()
        .filter(|labels| !labels.is_empty())
}

/// The SQL type name that reads back as `db_type`, for the temporary table.
///
/// Every type is named as itself. A narrower integer or a `REAL` used to be
/// widened, so the relation advertised `int4` or `float8` where the inner
/// query's description said `int2` or `float4`, and a `REAL` of 0.1 read back
/// as 0.10000000149011612.
fn sql_type_of(db_type: &DbType) -> Option<&'static str> {
    Some(match db_type {
        DbType::I8 => "TINYINT",
        DbType::I16 => "SMALLINT",
        DbType::I32 => "INTEGER",
        DbType::I64 => "BIGINT",
        DbType::U8 => "UTINYINT",
        DbType::U16 => "USMALLINT",
        DbType::U32 => "UINT",
        // Their own widths: as `BIGINT` a `UBIGINT` above `i64::MAX` read
        // back negative, a 16-byte integer was cut to eight, and the column
        // advertised `int8` where the base table's said `numeric`.
        DbType::U64 => "UBIGINT",
        DbType::I128 => "HUGEINT",
        DbType::U128 => "UHUGEINT",
        DbType::F32 => "REAL",
        DbType::F64 => "DOUBLE",
        DbType::DECIMAL => "NUMERIC",
        DbType::BOOL => "BOOLEAN",
        DbType::UUID => "UUID",
        // `TIMESTAMP` alone declares a `timestamp` (without time zone),
        // whose literals drop their zone; a `DATETIME` cell is a
        // `timestamptz`.
        DbType::DATETIME => "TIMESTAMPTZ",
        DbType::TIMESTAMP => "TIMESTAMP",
        DbType::STRING => "TEXT",
        DbType::CHAR => "CHAR",
        // The types stored in PostgreSQL's own layout. Without them a CTE
        // over a date, a time, an interval, a MAC address or any geometry was
        // refused as "a type that cannot be materialised".
        DbType::DATE => "DATE",
        DbType::TIME => "TIME",
        DbType::TIMETZ => "TIMETZ",
        DbType::INTERVAL => "INTERVAL",
        DbType::MACADDR => "MACADDR",
        DbType::MACADDR8 => "MACADDR8",
        DbType::BIT => "BIT",
        DbType::VARBIT => "VARBIT",
        DbType::POINT => "POINT",
        DbType::LINE => "LINE",
        DbType::LSEG => "LSEG",
        DbType::BOX => "BOX",
        DbType::PATH => "PATH",
        DbType::POLYGON => "POLYGON",
        DbType::CIRCLE => "CIRCLE",
        DbType::INT4RANGE => "INT4RANGE",
        DbType::INT8RANGE => "INT8RANGE",
        DbType::NUMRANGE => "NUMRANGE",
        DbType::TSRANGE => "TSRANGE",
        DbType::TSTZRANGE => "TSTZRANGE",
        DbType::DATERANGE => "DATERANGE",
        // Every remaining stored type, named as `parse_column_type` accepts it.
        // A materialised subquery has to be able to hold whatever a select list
        // can produce: a `SELECT COUNT(*) FROM (SELECT …) AS sub_query` — which
        // is how a paginated count is written — carries every column of the
        // inner query through a real table, so a type missing here fails the
        // whole statement rather than just that column.
        DbType::JSONB | DbType::JSON => "JSONB",
        DbType::INET => "INET",
        DbType::BYTEA => "BYTEA",
        DbType::ARRAY => "ARRAY",
        // `character(n)` keeps its type through the relation: compared
        // without trailing spaces, and shown padded (`relation_columns`
        // declares it with its length).
        DbType::BPCHAR(_) => "BPCHAR",
        // NULL is not a type, it is the absence of a value in the row that was
        // sampled. The caller resolves it by looking at further rows.
        DbType::NULL => return None,
        _ => return None,
    })
}

/// One column of a materialised relation: what it is declared as, and the
/// type its cells are written back as when a scan reports them physically.
struct RelationColumn {
    sql_type: String,
    declared: Option<DbType>,
    /// The scale this column's `DECIMAL` cells arrive at, when known.
    scale: Option<u32>,
}

/// The column types of the relation `query` is materialised into.
///
/// Taken from the query's description, which is what the client is told the
/// columns are and what pgwire renders their cells by — the same source the
/// outer statement's describe resolves a CTE through, so the relation and its
/// description cannot disagree. A scanned row cannot answer this: it reports
/// the *physical* type, which is TEXT for a document, an array, a range, a bit
/// string and a path, so a relation typed from the rows held a document's
/// encoded bytes in a TEXT column. They came back non-NULL and unreadable, and
/// `@>` refused the column as a string.
///
/// A sampled cell still wins where it disagrees physically with the
/// description, because its bytes are what has to be written. The rows decide
/// alone only when the query cannot be described, as before.
async fn relation_columns(
    query: &str,
    labels: &[String],
    rows: &[crate::core::row::row::Row],
    scales: Option<&[u32]>,
    database: &Arc<Database>,
) -> Result<Vec<RelationColumn>, usize> {
    let described: Vec<DbType> = match Box::pin(crate::pgwire::describe_query(database, query)).await {
        Ok(Some((fields, _))) if fields.len() == labels.len() => {
            fields.into_iter().map(|(_, ty)| ty).collect()
        }
        _ => Vec::new(),
    };
    let sampled_wide = declared_wide_types(query, labels, database);
    let engine = crate::core::value_codec::DECIMAL_SCALE;
    // A bare `character varying` column keeps that type in the relation, so
    // it compares blank-padded with a `character(n)` one there as it does in
    // its table; declared TEXT, `WHERE a = b` over a CTE compared exactly.
    let varying: Vec<Option<u32>> = {
        use crate::core::rql::type_modifier::TextTypmod;
        declared_fields_among(query, labels, database, &[])
            .into_iter()
            .map(|f| match f.as_ref().and_then(|f| (f.db_type == DbType::STRING).then(|| f.text_typmod()).flatten()) {
                Some(TextTypmod::Varchar(n)) => Some(n),
                _ => None,
            })
            .collect()
    };

    let mut columns = Vec::with_capacity(labels.len());
    for index in 0..labels.len() {
        // The first row that has a value here: a NULL is the absence of a
        // value, not a type.
        let sampled = rows
            .iter()
            .filter_map(|row| row.columns.get(index))
            .map(|c| c.column_type.clone())
            .find(|t| *t != DbType::NULL);
        let described = described
            .get(index)
            .filter(|t| sql_type_of(t).is_some())
            .cloned();
        let declared = match (described, sampled) {
            (Some(d), Some(s)) if s != d && s != d.physical() => Some(s),
            (Some(d), _) => Some(d),
            // Undescribable: the sampled type, with TEXT second-guessed from
            // the schema for the bare-column case `declared_wide_types` reads.
            (None, Some(DbType::STRING)) | (None, None) => sampled_wide
                .get(index)
                .cloned()
                .flatten()
                .or(Some(DbType::STRING)),
            (None, Some(s)) => Some(s),
        };
        let scale = scales.and_then(|s| s.get(index)).copied();
        let sql_type = match &declared {
            // A decimal is declared at the scale its cells were stored at, so
            // it prints the places it was written with rather than nine.
            Some(DbType::DECIMAL) => match self_scaled_width(rows, index, scale).or(scale) {
                Some(scale) if scale != engine => format!("NUMERIC(38,{})", scale),
                _ => "NUMERIC".to_string(),
            },
            Some(DbType::BPCHAR(n)) if *n > 0 => format!("CHAR({n})"),
            Some(DbType::STRING) if varying.get(index).copied().flatten().is_some() => {
                match varying[index] {
                    Some(0) | None => "VARCHAR".to_string(),
                    Some(n) => format!("VARCHAR({n})"),
                }
            }
            Some(t) => sql_type_of(t).ok_or(index)?.to_string(),
            None => "TEXT".to_string(),
        };
        columns.push(RelationColumn {
            sql_type,
            declared,
            scale,
        });
    }
    Ok(columns)
}

/// The scale a `DECIMAL` column has to be declared at to hold every cell of
/// it exactly, when any of those cells carries a scale of its own.
///
/// An `AVG` picks its scale per value, as PostgreSQL's numeric division does:
/// twenty places for an average below 1, twelve from 10^4. The item's column
/// scale is a fixed sixteen, so declaring the relation at it rounded
/// `0.33333333333333333333` to sixteen places and padded `30000.5` to sixteen.
/// The widest scale any cell carries loses nothing. A column holds one scale,
/// so where the values differ the narrower ones print trailing zeros.
///
/// Narrowed to the most places a column may declare, which an average below
/// 10^-12 exceeds, and so the widest integer part still fits the 38 digits a
/// column holds, which only an average far below 1 sharing a column with a
/// large one reaches. `None` when no cell carries its own scale, and the
/// column's is then the item's.
fn self_scaled_width(
    rows: &[crate::core::row::row::Row],
    index: usize,
    column_scale: Option<u32>,
) -> Option<u32> {
    let arrival = column_scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE);
    let mut self_scaled = false;
    let mut widest = 0u32;
    let mut integer_digits = 0u32;
    for row in rows {
        let Some(cell) = row.columns.get(index) else { continue };
        if cell.column_type != DbType::DECIMAL {
            continue;
        }
        let Some((v, own)) =
            crate::core::value_codec::split_decimal_cell(cell.data.into_slice())
        else {
            continue;
        };
        self_scaled |= own.is_some();
        let at = own.unwrap_or(arrival);
        widest = widest.max(at);
        let whole = 10u128.checked_pow(at).map_or(0, |f| v.unsigned_abs() / f);
        let digits = if whole == 0 { 0 } else { whole.ilog10() + 1 };
        integer_digits = integer_digits.max(digits);
    }
    let most = u32::from(crate::core::rql::type_modifier::MAX_DECIMAL_SCALE);
    self_scaled.then(|| widest.min(most).min(38u32.saturating_sub(integer_digits)))
}

/// The literal one cell is written into a materialised relation as.
///
/// A cell a scan reports as `STRING` is read as the column's declared type
/// when that type is stored as a string: the bytes are a document, an array
/// or a range, and quoting them as text wrote their encoding back.
fn cell_literal(column: &RelationColumn, cell: &crate::core::row::row::Column) -> Option<String> {
    let as_type = match &column.declared {
        Some(declared)
            if cell.column_type == DbType::STRING && declared.physical() == DbType::STRING =>
        {
            declared.clone()
        }
        _ => cell.column_type.clone(),
    };
    // A decimal is written at the scale it arrived at. Its cells are not
    // brought to the engine scale first (see `run_query_rows_scaled`), so a
    // cell with no scale of its own is at its column's.
    if as_type == DbType::DECIMAL {
        if let (Some((v, None)), Some(scale)) = (
            crate::core::value_codec::split_decimal_cell(cell.data.into_slice()),
            column.scale,
        ) {
            return Some(crate::core::value_codec::format_decimal_scaled(v, scale));
        }
    }
    crate::core::rql::subquery::value_to_literal(&as_type, cell.data.into_slice())
}

/// Refuses a relation that would name one column twice.
///
/// A join's `*`, or `a.id, b.id`, names a shared column twice. The relation
/// is a table and its fill names its columns, and the INSERT maps each name
/// to the first field called that: the second value was never written, or a
/// nullable pair's companion was declared twice and the CREATE failed.
/// Refused rather than renamed, since a renamed column would reach the client
/// under the new name and resolve a reference PostgreSQL calls ambiguous.
///
/// Compared exactly, as the INSERT matches: `id` and `ID` are two columns of
/// the relation and each is written, so refusing them would only turn a right
/// answer into an error.
fn refuse_repeated_labels<S: AsRef<str>>(labels: &[S], subject: &str) -> Result<(), String> {
    for (index, label) in labels.iter().enumerate() {
        let label = label.as_ref();
        if labels[..index].iter().any(|earlier| earlier.as_ref() == label) {
            return Err(format!(
                "{} names the column '{}' twice; alias one of them with AS",
                subject, label
            ));
        }
    }
    Ok(())
}

/// Creates `temp` with one column per label, typed by [`relation_columns`],
/// and writes `rows` into it. `subject` names what is being materialised in
/// an error: "the WITH clause 'x'" or "the subquery for 's'".
///
/// `created` is set as soon as the table exists, so a caller can drop it
/// when the fill then fails: DDL is not transactional here.
#[allow(clippy::too_many_arguments)]
async fn build_relation(
    query: &str,
    temp: &str,
    labels: &[String],
    rows: &[crate::core::row::row::Row],
    scales: Option<&[u32]>,
    subject: &str,
    database: &Arc<Database>,
    tx: crate::core::mvcc::transaction::TxId,
    created: &mut bool,
) -> Result<(), String> {
    refuse_repeated_labels(labels, subject)?;
    // A scan fetches the columns its `WHERE` reads as well as the selected
    // ones, so a row may carry more than the select list names. The visible
    // columns come first, which is what `visible_columns` trims to elsewhere.
    if let Some(row) = rows.first() {
        if row.columns.len() < labels.len() {
            return Err(format!(
                "{} returned {} columns but names {}",
                subject,
                row.columns.len(),
                labels.len()
            ));
        }
    }
    let columns = relation_columns(query, labels, rows, scales, database)
        .await
        .map_err(|index| {
            format!(
                "{} returned a type that cannot be materialised in column '{}'",
                subject,
                labels.get(index).map(String::as_str).unwrap_or("?")
            )
        })?;

    let definitions: Vec<String> = labels
        .iter()
        .zip(columns.iter())
        .map(|(name, column)| format!("{} {}", name, column.sql_type))
        .collect();
    create_temporary(
        &format!("CREATE TABLE {} ({})", temp, definitions.join(", ")),
        database,
        tx,
    )
    .await
    .map_err(|e| format!("could not materialise {}: {}", subject, e))?;
    *created = true;

    if rows.is_empty() {
        return Ok(());
    }
    let mut tuples: Vec<String> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut parts: Vec<String> = Vec::with_capacity(labels.len());
        for (index, (cell, column)) in row.columns.iter().zip(columns.iter()).enumerate() {
            parts.push(cell_literal(column, cell).ok_or_else(|| {
                // Naming the type and the column is the difference between a
                // five-minute fix and a hunt.
                format!(
                    "{} returned a {:?} value in column '{}' that cannot be stored",
                    subject,
                    column.declared.as_ref().unwrap_or(&cell.column_type),
                    labels.get(index).map(String::as_str).unwrap_or("?"),
                )
            })?);
        }
        tuples.push(format!("({})", parts.join(", ")));
    }
    crate::core::rql::executor::run_statement(
        &format!(
            "INSERT INTO {} ({}) VALUES {}",
            temp,
            labels.join(", "),
            tuples.join(", ")
        ),
        database,
        tx,
    )
    .await
    .map_err(|e| format!("could not fill {}: {}", subject, e))?;
    Ok(())
}

/// The prefix every materialised CTE and derived table's name carries.
///
/// Public because it is the one thing that identifies a relation as
/// statement-scoped from the outside. `Database::seed_in` reads it to give such
/// a table memory-backed storage instead of files, and `pool_place` reads it to
/// record no placement for one: a relation that is created and dropped inside a
/// single statement can never be read after that statement ends, so nothing it
/// writes has to reach a disk.
pub const TEMP_TABLE_PREFIX: &str = "__cte_";

tokio::task_local! {
    /// Set while this module creates one of its own temporaries, and while a
    /// replica applies a peer's statements (the peer's temporaries arrive as
    /// its `CREATE TABLE __cte_…`): the only creates [`reserved_name_refusal`]
    /// lets take the prefix.
    static CREATING_TEMPORARY: ();
}

/// Runs `create`, a `CREATE TABLE` of a temporary this module named.
async fn create_temporary(create: &str, database: &Arc<Database>, tx: crate::core::mvcc::transaction::TxId) -> Result<(), String> {
    CREATING_TEMPORARY
        .scope((), crate::core::rql::executor::run_statement(create, database, tx))
        .await
}

/// Runs `work` with the prefix open, for the replication apply loop: a
/// statement a peer accepted, its temporaries included, must apply here.
pub(crate) async fn allowing_temporaries<F: std::future::Future>(work: F) -> F::Output {
    CREATING_TEMPORARY.scope((), work).await
}

/// Why a table may not be created under, or renamed to, `name`: the
/// [`TEMP_TABLE_PREFIX`] is the statement temporaries', and a user table
/// taking it was treated as one. Memory-backed storage (its rows lost at
/// every restart), no table locks, and reaped at startup: a plain `CREATE
/// TABLE __cte_orders` lost everything it held. Matched ignoring ASCII case,
/// so a later case folding of identifiers cannot let one through. `None`
/// for every other name, and inside [`create_temporary`] or
/// [`allowing_temporaries`].
///
/// The message is the one `pgwire::sqlstate_for` maps to 42939
/// (`reserved_name`), PostgreSQL's code for a name a reserved prefix keeps
/// out (`pg_` for schemas and roles), rather than 42602 (`invalid_name`),
/// which says the name itself is malformed.
pub fn reserved_name_refusal(name: &str) -> Option<String> {
    let reserved = name
        .get(..TEMP_TABLE_PREFIX.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(TEMP_TABLE_PREFIX));
    if !reserved || CREATING_TEMPORARY.try_with(|_| ()).is_ok() {
        return None;
    }
    Some(format!(
        "table name {} is reserved: the prefix {} is reserved for the temporary tables a statement \
         materialises",
        crate::core::value_codec::double_quoted(name),
        crate::core::value_codec::double_quoted(TEMP_TABLE_PREFIX)
    ))
}

/// A temporary table name that cannot collide with a user's table or with
/// another execution — including another session running the *same* statement.
///
/// The `__cte_` prefix keeps it out of the user's namespace, which is also what
/// makes a CTE that shadows a real table safe: references are rewritten to this
/// name, so the real table is untouched.
///
/// The name mixes in per-execution entropy as well as the statement text. It
/// used to hash the text alone, which made two sessions running one statement
/// agree on a name: both created the same table, and whichever finished first
/// dropped it out from under the other — "Table not found", then "could not
/// drop the temporary table … does not exist". A paginated count run twice at
/// once is enough to hit it. The name is derived once per execution and carried
/// through the rewrite and the drop, so it does not need to be reproducible.
fn temp_name(cte: &str, statement: &str, index: usize) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in statement.bytes().chain(cte.bytes()) {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash ^= fastrand::u64(..);
    let safe: String = cte
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .take(24)
        .collect();
    format!("{}{}_{}_{:x}", TEMP_TABLE_PREFIX, safe, index, hash)
}

/// Replaces references to a CTE *as a relation*, leaving same-named columns
/// alone.
///
/// A CTE may share its name with a column — `WITH domain_id AS (…) INSERT INTO
/// users (…, domain_id, …) SELECT …, domain_id.id, … FROM domain_id` is real
/// migration SQL. Replacing every occurrence rewrote the **column list** too,
/// shifting the insert's columns and surfacing as "column 'Zapier Integration'
/// not found in schema" — the select list read against the wrong columns.
///
/// A name is a relation reference when it follows `FROM`/`JOIN`, or when it
/// qualifies something (`domain_id.id`). A name *preceded* by a dot is a column
/// of something else and is left as it is.
fn replace_relation_references(sql: &str, from: &str, to: &str) -> String {
    // Bytes, not `char`s: pushing a byte of a multi-byte character as a
    // `char` re-encoded it on its own, so `t = 'é'` in a statement over a CTE
    // compared against `'Ã©'` and matched nothing.
    let mut out: Vec<u8> = Vec::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let upper = sql.to_ascii_uppercase();
    let mut i = 0usize;
    let mut quote: Option<u8> = None;
    // The last word seen outside quotes, to spot `FROM x` / `JOIN x`.
    let mut previous_word = String::new();
    // Whether we are inside a `FROM` clause, where a comma separates relations.
    //
    // `FROM a, b` is an implicit cross join, so `b` is a relation position just
    // as `a` is — but a comma alone cannot mean that, or an unqualified select
    // item sharing a CTE's name would be rewritten too. The clause keywords
    // below are what end the list.
    let mut in_from = false;
    let mut relation_position = false;

    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = quote {
            out.push(c);
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if c == b'\'' || c == b'"' {
            quote = Some(c);
            out.push(c);
            i += 1;
            continue;
        }

        if c.is_ascii_alphanumeric() || c == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word = &sql[start..i];
            let preceded_by_dot = start > 0 && bytes[start - 1] == b'.';
            let qualifies = bytes.get(i) == Some(&b'.');
            // `USING` names relations too — `DELETE … USING victims v` — but
            // only outside parentheses: in `JOIN b USING (id)` the word after
            // it is a column, and the paren is what tells the two apart.
            let preceded_by_paren = start > 0 && bytes[start - 1] == b'(';
            let after_relation_keyword = matches!(previous_word.as_str(), "FROM" | "JOIN")
                || (previous_word.as_str() == "USING" && !preceded_by_paren);

            if word.eq_ignore_ascii_case(from)
                && !preceded_by_dot
                && (qualifies || after_relation_keyword || relation_position)
            {
                out.extend_from_slice(to.as_bytes());
            } else {
                out.extend_from_slice(word.as_bytes());
            }
            previous_word = upper[start..i].to_string();

            // A word consumes the relation position a comma opened.
            relation_position = false;
            match previous_word.as_str() {
                "FROM" | "USING" => in_from = true,
                "WHERE" | "GROUP" | "HAVING" | "ORDER" | "LIMIT" | "OFFSET" | "UNION"
                | "INTERSECT" | "EXCEPT" | "RETURNING" | "SET" | "VALUES" => in_from = false,
                _ => {}
            }
            continue;
        }

        if c == b',' && in_from {
            relation_position = true;
        }
        out.push(c);
        i += 1;
    }
    // Every byte is copied in order and only whole ASCII words are replaced,
    // by text, so the result is the input's characters.
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

/// Runs each CTE, stores it in a temporary table, and rewrites the statement to
/// read from those tables.
///
/// `Ok(None)` when there is no `WITH` clause. The caller **must** drop
/// [`Materialized::temps`] on every exit path: DDL is not transactional here,
/// so a failed outer statement leaves the temporary tables behind to collide
/// with the next run.
/// Whether a statement opens with a `WITH` clause.
///
/// Cheap enough to ask before deciding whether a statement needs a transaction
/// of its own, which is the one thing a caller must know before parsing.
pub fn has_with_clause(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    trimmed.len() >= 4
        && trimmed[..4].eq_ignore_ascii_case("WITH")
        && trimmed[4..].starts_with(|c: char| c.is_whitespace() || c == '(')
}

/// `INSERT INTO t [(cols)] WITH a AS (…) SELECT …` rewritten as
/// `WITH a AS (…) INSERT INTO t [(cols)] SELECT …`, and `None` for any other
/// statement.
///
/// The two mean the same thing in PostgreSQL: a non-recursive CTE is visible
/// to the query it is attached to, and here that query is the whole row
/// source. Only the leading form was recognised, so the trailing one reached
/// the insert path with its `WITH` intact and ran the CTE body as if it were
/// the row source — "cannot parse subquery", or "a subquery used as a value
/// returned 2 rows". Moving the clause to the front hands it to the
/// materialisation every other CTE goes through, `ON CONFLICT` and
/// `RETURNING` included, since they stay where they were.
pub fn hoist_insert_with(sql: &str) -> Option<String> {
    let (head, source) = crate::core::rql::lexer_ir::insert_select_source(sql)?;
    if !starts_with_with(source) {
        return None;
    }
    let (defs, rest) = split_with(source).ok()??;
    let clauses: Vec<String> = defs
        .iter()
        .map(|def| format!("{} AS ({})", def.name, def.body))
        .collect();
    Some(format!("WITH {} {} {}", clauses.join(", "), head, rest))
}

pub async fn materialize(
    sql: &str,
    database: &Arc<Database>,
    // The statement's transaction. A data-modifying CTE runs inside it, so its
    // writes stand or fall with the statement rather than committing on their
    // own — `FROZEN_TX` means there is none and each body autocommits.
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<Option<Materialized>, String> {
    let mut temps: Vec<String> = Vec::new();
    // A statement with several CTEs can fail on a later one having already
    // materialised the earlier ones. The caller only drops what it is handed,
    // and an `Err` hands it nothing — so the tables created so far are dropped
    // here before the error propagates. Without this, one failing statement
    // leaks a full table per CTE that had already succeeded.
    match materialize_inner(sql, database, &mut temps, tx).await {
        Ok(result) => Ok(result),
        Err(e) => {
            drop_temps(database, &temps, tx).await;
            Err(e)
        }
    }
}

async fn materialize_inner(
    sql: &str,
    database: &Arc<Database>,
    temps: &mut Vec<String>,
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<Option<Materialized>, String> {
    let Some((defs, rest)) = split_with(sql)? else {
        return Ok(None);
    };
    if rest.trim().is_empty() {
        return Err("a WITH clause with no statement after it".to_string());
    }
    crate::owner_probe!(crate::core::mvcc::owner::probe::Site::Cte, tx, database);

    let mut rewritten = rest;

    // Which temporary table each CTE published, by name.
    //
    // Deliberately not `temps` positionally zipped against `defs`: `temps` also
    // holds the tables a *derived table inside a body* needed, and a CTE that
    // publishes nothing pushes none at all — so position `i` is not CTE `i`.
    // It was, until a body containing `FROM (SELECT …) x` added an entry ahead
    // of its own, and every later CTE then read the derived table instead of
    // the CTE it named. The rows were real and the query looked fine.
    let mut published: Vec<(String, String)> = Vec::new();

    for (index, def) in defs.iter().enumerate() {
        // Earlier CTEs are visible to later ones, so rewrite the body too.
        let mut body = def.body.clone();
        for (name, temp) in &published {
            body = replace_relation_references(&body, name, temp);
        }

        // A CTE body may modify data — `WITH x AS (INSERT … RETURNING id)` —
        // in which case it runs for its effect and publishes the RETURNING
        // rows. PostgreSQL runs such a CTE exactly once, which is what
        // materialising in order already gives.
        let modifies = !matches!(
            body.trim_start()
                .get(..6)
                .map(|w| w.to_ascii_uppercase())
                .as_deref(),
            Some("SELECT")
        );

        // `RETURNING <constants>` names no column of the table, so the clause is
        // dropped and the constants rebuilt from the row count.
        if modifies {
            if let Some(literals) = returning_constants(&body) {
                let (head, _) = crate::core::rql::lexer_ir::returning_split(&body)
                    .ok_or_else(|| format!("the WITH clause '{}' lost its RETURNING", def.name))?;
                let affected =
                    crate::core::rql::executor::run_statement_affected(head, database, tx)
                        .await
                        .map_err(|e| format!("the WITH clause '{}' failed: {}", def.name, e))?;

                let labels: Vec<String> = (0..literals.len())
                    .map(|i| format!("col{}", i + 1))
                    .collect();
                let temp = temp_name(&def.name, sql, index);
                let columns: Vec<String> =
                    labels.iter().map(|name| format!("{} TEXT", name)).collect();
                create_temporary(
                    &format!("CREATE TABLE {} ({})", temp, columns.join(", ")),
                    database,
                    tx,
                )
                .await
                .map_err(|e| {
                    format!(
                        "could not materialise the WITH clause '{}': {}",
                        def.name, e
                    )
                })?;
                temps.push(temp.clone());

                if affected > 0 {
                    let tuple = format!("({})", literals.join(", "));
                    let tuples: Vec<String> = (0..affected).map(|_| tuple.clone()).collect();
                    crate::core::rql::executor::run_statement(
                        &format!(
                            "INSERT INTO {} ({}) VALUES {}",
                            temp,
                            labels.join(", "),
                            tuples.join(", ")
                        ),
                        database,
                        tx,
                    )
                    .await
                    .map_err(|e| format!("could not fill the WITH clause '{}': {}", def.name, e))?;
                }

                rewritten = replace_relation_references(&rewritten, &def.name, &temp);
                // Registered by name so a later CTE referencing this one finds
                // it, exactly as the ordinary path does.
                published.push((def.name.clone(), temp));
                continue;
            }
        }

        // A body of named constants is one `VALUES` row, with no query to run.
        if !modifies {
            if let Some(constants) = constant_body(&body) {
                let temp = temp_name(&def.name, sql, index);
                let columns: Vec<String> = constants
                    .iter()
                    .map(|c| format!("{} {}", c.label, c.sql_type))
                    .collect();
                let labels: Vec<&str> = constants.iter().map(|c| c.label.as_str()).collect();
                refuse_repeated_labels(&labels, &format!("the WITH clause '{}'", def.name))?;
                let values: Vec<&str> = constants.iter().map(|c| c.value.as_str()).collect();
                create_temporary(
                    &format!("CREATE TABLE {} ({})", temp, columns.join(", ")),
                    database,
                    tx,
                )
                .await
                .map_err(|e| {
                    format!("could not materialise the WITH clause '{}': {}", def.name, e)
                })?;
                temps.push(temp.clone());
                crate::core::rql::executor::run_statement(
                    &format!(
                        "INSERT INTO {} ({}) VALUES ({})",
                        temp,
                        labels.join(", "),
                        values.join(", ")
                    ),
                    database,
                    tx,
                )
                .await
                .map_err(|e| format!("could not fill the WITH clause '{}': {}", def.name, e))?;
                rewritten = replace_relation_references(&rewritten, &def.name, &temp);
                published.push((def.name.clone(), temp));
                continue;
            }
        }

        // A CTE body may itself select from a derived table. Those are
        // materialised in the statement path, which runs *after* this — so a
        // body reaching the scan with `FROM (SELECT …) x` still in it was
        // reported as `relation '(' does not exist`. Rewriting it here, before
        // the body runs, is the only point at which it can be caught.
        let derived = materialize_derived(&body, database, tx).await?;
        let body_owned;
        let body = match &derived {
            Some(d) => {
                body_owned = d.rewritten.clone();
                temps.extend(d.temps.iter().cloned());
                body_owned.as_str()
            }
            None => body.as_str(),
        };

        // A later CTE or the statement itself can still fail after this body
        // wrote, and an in-place overwrite would outlive the abort
        // (`TransactionManager::forbid_in_place`).
        if modifies {
            database.transactions.forbid_in_place(tx);
        }
        // A data-modifying CTE with no `RETURNING` is legal and runs purely for
        // its effect. It publishes no columns, so it cannot be referenced and
        // needs no temporary table — running it is the whole job. Decided
        // before the body runs: it was checked after the run below, which had
        // already executed it, so the body ran twice (`n = n + 1` added 2).
        if modifies && crate::core::rql::lexer_ir::returning_split(body).is_none() {
            crate::core::rql::executor::run_statement(body, database, tx)
                .await
                .map_err(|e| format!("the WITH clause '{}' failed: {}", def.name, e))?;
            continue;
        }
        let (rows, scales) = if modifies {
            // Returned cells are read straight from storage, at the scales
            // the client's copy of them is rendered by.
            let scales = crate::core::rql::lexer_s1::recognize_query_purpose(body)
                .and_then(|purpose| crate::pgwire::purpose_returning_scales(database, &purpose));
            crate::core::rql::executor::run_returning_rows(body, database, tx)
                .await
                .map(|rows| (rows, scales))
        } else {
            crate::core::rql::executor::run_query_rows_scaled(body, database, tx).await
        }
        .map_err(|e| format!("the WITH clause '{}' failed: {}", def.name, e))?;

        let labels: Vec<String> = if modifies {
            returning_labels(&body).ok_or_else(|| {
                format!(
                    "the WITH clause '{}' modifies data but has no RETURNING clause to name \
                     its columns",
                    def.name
                )
            })?
        } else {
            if select_list_of(body).is_none() {
                return Err(format!("the WITH clause '{}' must be a SELECT", def.name));
            }
            select_labels(body, database).await.ok_or_else(|| {
                format!(
                    "the WITH clause '{}' has a column this cannot name; alias it with AS",
                    def.name
                )
            })?
        };

        let temp = temp_name(&def.name, sql, index);

        // Timed from here rather than from the top of the loop: the body has
        // already run, and a body containing its own derived table has already
        // materialised that through `materialize_query`, which counts itself.
        // Starting the clock earlier would count the nested relation twice.
        #[cfg(feature = "query_stats")]
        let materialising = std::time::Instant::now();

        // Typed by the body's description rather than by its rows, which also
        // types an empty result: a TEXT placeholder there made `@>` on an
        // empty CTE's document column an error instead of no rows.
        let mut created = false;
        let built = build_relation(
            body,
            &temp,
            &labels,
            &rows,
            scales.as_deref(),
            &format!("the WITH clause '{}'", def.name),
            database,
            tx,
            &mut created,
        )
        .await;
        // Recorded whether or not the fill succeeded, so a failure part-way
        // still gets cleaned up.
        if created {
            temps.push(temp.clone());
        }
        built?;

        // One relation built. A CTE that failed part-way is not counted — the
        // `?` above carries the error out — which is the right answer: nothing
        // usable was produced, and the statement is recorded as the failure it
        // was.
        #[cfg(feature = "query_stats")]
        crate::core::query_stats::note_materialised(
            materialising.elapsed().as_micros().min(u64::MAX as u128) as u64,
        );

        rewritten = replace_relation_references(&rewritten, &def.name, &temp);
        published.push((def.name.clone(), temp));
    }

    Ok(Some(Materialized {
        rewritten,
        temps: std::mem::take(temps),
    }))
}

/// Materialises one query into a temporary table and returns its name.
///
/// The same mechanism a CTE uses, exposed for `UPDATE … FROM (subquery) alias`,
/// where the derived table has to become a real relation before the join can
/// read it. The caller owns the result and must drop it — see [`drop_temps`].
/// The declared type of each output column, for the columns whose real type a
/// scan cannot report.
///
/// `DbType::physical()` maps `JSONB`, `BYTEA` and `ARRAY` all to `STRING`, and
/// a scan types its cells physically — so sampling the returned rows says
/// `TEXT` for all three. Materialising a derived table on that sample created
/// the temp relation's column as `TEXT`, and the outer select then described
/// the column from the temp relation (text) while the DESCRIBE path resolved
/// the inner query properly and said `jsonb`. The document went out as raw
/// stored bytes under a `jsonb` description: "unsupported JSONB format version
/// 7", 7 being the encoded object's tag. `paginate()` wraps selects in derived
/// tables, so this is an ordinary shape rather than an exotic one.
///
/// Only those three types are recovered, and only when the inner query selects
/// bare columns from one real relation. Anything else keeps the sampled type —
/// in particular a rebuilt (shredded) document, whose declared type is the
/// `UUID` it is stored as and whose sampled `JSON` is the correct answer.
fn declared_wide_types(
    query: &str,
    labels: &[String],
    database: &Arc<Database>,
) -> Vec<Option<DbType>> {
    declared_types(query, labels, database)
        .into_iter()
        .map(|t| t.filter(|t| matches!(t, DbType::JSONB | DbType::BYTEA | DbType::ARRAY)))
        .collect()
}

/// The declared type of each of `labels`, read as a column of the one real
/// relation `query` selects from, and `None` for a label that is not one.
fn declared_types(
    query: &str,
    labels: &[String],
    database: &Arc<Database>,
) -> Vec<Option<DbType>> {
    declared_types_among(query, labels, database, &[])
}

/// [`declared_types`], where the relation may also be one of `relations`:
/// a CTE or a derived table a describe resolved from its own query. Those
/// come first, as the statement reads them once it is materialised: a CTE
/// is renamed to its temporary relation before the statement runs, so it
/// stands in for a table of the same name.
fn declared_types_among(
    query: &str,
    labels: &[String],
    database: &Arc<Database>,
    relations: &[(String, ScratchFields)],
) -> Vec<Option<DbType>> {
    declared_fields_among(query, labels, database, relations)
        .into_iter()
        // `literal_type`: a `timestamp` column is one.
        .map(|f| f.map(|f| f.literal_type()))
        .collect()
}

/// Whether each output column of `query`, by position, compares blank-padded
/// with a `character(n)` value: a bare `character(n)` or `character
/// varying` column, which PostgreSQL resolves to `bpchar` against one. Any
/// other item — a text column, an expression, a column this cannot read —
/// is text, which compares with `character(n)` exactly, trailing spaces
/// kept (`a IN (SELECT t …)` does not take a `t` of `'ab  '` for `'ab'`).
pub(crate) fn blank_padded_columns(query: &str, database: &Arc<Database>) -> Vec<bool> {
    use crate::core::rql::type_modifier::TextTypmod;
    let Some(list) = select_list_of(query) else {
        return Vec::new();
    };
    let list = list.trim_start();
    let list = match list.get(.."DISTINCT ".len()) {
        Some(head) if head.eq_ignore_ascii_case("DISTINCT ") => &list["DISTINCT ".len()..],
        _ => list,
    };
    let labels: Vec<String> = crate::core::rql::lexer_ct::split_definitions(list)
        .iter()
        .map(|item| item.trim())
        .take_while(|item| !(*item == "*" || item.ends_with(".*")))
        .map(|item| bare_column(split_alias(item).0).unwrap_or_default())
        .collect();
    declared_fields_among(query, &labels, database, &[])
        .into_iter()
        .map(|f| {
            f.is_some_and(|f| {
                f.db_type == DbType::STRING
                    && matches!(f.text_typmod(), Some(TextTypmod::Bpchar(_) | TextTypmod::Varchar(_)))
            })
        })
        .collect()
}

/// [`declared_types_among`]'s fields.
fn declared_fields_among(
    query: &str,
    labels: &[String],
    database: &Arc<Database>,
    relations: &[(String, ScratchFields)],
) -> Vec<Option<SchemaField>> {
    let none = vec![None; labels.len()];
    // A join has more than one relation behind its columns; this resolves only
    // the single-relation case rather than guessing which one a label meant.
    if crate::core::rql::join::statement_has_join(query) {
        return none;
    }
    let Some(at) = crate::core::rql::join::find_keyword(query, "FROM", 0) else {
        return none;
    };
    let rest = query[at + "FROM".len()..].trim_start();
    let name = rest
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches(|c: char| c == ';' || c == ',')
        .trim_matches('"');
    if name.is_empty() || name.contains('(') {
        return none;
    }
    let types_in = |fields: &[SchemaField]| -> Vec<Option<SchemaField>> {
        labels
            .iter()
            .map(|label| {
                fields
                    .iter()
                    .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(label))
                    .cloned()
            })
            .collect()
    };
    if let Some((_, fields)) = relations.iter().find(|(alias, _)| alias.eq_ignore_ascii_case(name)) {
        return types_in(fields);
    }
    let Some(table) = database.schema_view(name) else {
        return none;
    };
    types_in(&table.schema().fields)
}

/// Which output columns of `query`, by position, are stored documents, for
/// the callers that compare its rows by value: `UNION`'s duplicate check and
/// `IN (subquery)`. A scan reports a stored document as text, so without this
/// `{"a":1}` and `{"a":1.0}` were two rows to `UNION`, and an `IN` list was
/// built from the stored bytes as if they were text.
///
/// The same resolution as [`declared_wide_types`]: bare columns of one
/// relation. Anything it cannot resolve is `false` and compares as before.
///
/// Each item is looked up by the column it reads, not by its output name. An
/// alias is only a name: `doc->>'tag' AS doc` and `tag AS doc` are text, and
/// looking `doc` up made them documents, so `IN` read text as encoded bytes
/// and refused it, and `UNION` keyed a blob by its first byte and merged rows.
/// `doc AS d`, the other way round, is the document column.
pub(crate) fn document_columns(query: &str, database: &Arc<Database>) -> Vec<bool> {
    declared_item_types(query, database)
        .into_iter()
        .map(|t| t == Some(DbType::JSONB))
        .collect()
}

/// The declared type of each output column of `query`, by position, when the
/// item is a bare column of the one relation it selects from; `None` for
/// anything else. Looked up by the column read, not the output name, for the
/// reason [`document_columns`] gives.
///
/// What a `UNION` widens its integer columns by: PostgreSQL gives a column
/// that is `int` in one branch and `bigint` in another the type `bigint`, and
/// the rows and the description sent to the client have to agree on it.
pub(crate) fn declared_item_types(query: &str, database: &Arc<Database>) -> Vec<Option<DbType>> {
    declared_item_types_among(query, database, &[])
}

/// [`declared_item_types`], where the relation read may also be one of
/// `relations`: the CTEs and derived tables a describe resolved, which are
/// not in `database.tables` until the statement is materialised to run.
pub(crate) fn declared_item_types_among(
    query: &str,
    database: &Arc<Database>,
    relations: &[(String, ScratchFields)],
) -> Vec<Option<DbType>> {
    let Some(list) = select_list_of(query) else {
        return Vec::new();
    };
    let list = list.trim_start();
    let list = match list.get(.."DISTINCT ".len()) {
        Some(head) if head.eq_ignore_ascii_case("DISTINCT ") => &list["DISTINCT ".len()..],
        _ => list,
    };
    // A `*` is several columns, so no item after it is at the position its
    // place in the list says, as `output_names` notes. `SELECT *, n` was typed
    // `[?, int]`, and a `UNION` widened the column after the `*` by `n`'s
    // type: a text column described as `bigint`. The items before the first
    // `*` are where they say, and are kept.
    let labels: Vec<String> = crate::core::rql::lexer_ct::split_definitions(list)
        .iter()
        .map(|item| item.trim())
        .take_while(|item| !(*item == "*" || item.ends_with(".*")))
        .map(|item| bare_column(split_alias(item).0).unwrap_or_default())
        .collect();
    declared_types_among(query, &labels, database, relations)
}

/// The output name of each item in a `SELECT`'s list: its alias, or the
/// column it reads, and `None` for an item this cannot name (an expression,
/// or a `*` that stands for several columns).
///
/// What a `UNION`'s trailing `ORDER BY` resolves a name against: PostgreSQL
/// names the combined columns after the first branch.
pub(crate) fn output_names(query: &str) -> Vec<Option<String>> {
    let Some(list) = select_list_of(query) else {
        return Vec::new();
    };
    let list = list.trim_start();
    let list = match list.get(.."DISTINCT ".len()) {
        Some(head) if head.eq_ignore_ascii_case("DISTINCT ") => &list["DISTINCT ".len()..],
        _ => list,
    };
    let items = crate::core::rql::lexer_ct::split_definitions(list);
    // A `*` is several columns, so no item after it is at the position its
    // place in the list says. Naming none is better than naming the wrong one.
    if items.iter().any(|item| {
        let item = item.trim();
        item == "*" || item.ends_with(".*")
    }) {
        return Vec::new();
    }
    items.iter().map(|item| item_label(item)).collect()
}

pub async fn materialize_query(
    query: &str,
    alias: &str,
    statement: &str,
    database: &Arc<Database>,
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<String, String> {
    // An explicit `return`, matching the same shape in `pgwire`: two cfg'd
    // blocks in tail position depend on cfg-stripping to decide which one is
    // the tail expression, and saying so outright costs nothing.
    #[cfg(not(feature = "query_stats"))]
    {
        return materialize_query_inner(query, alias, statement, database, tx).await;
    }

    // A wrapper rather than a timer threaded through the body, because the body
    // returns early through `?` in a dozen places and a count that a failure
    // path could skip is a count nobody can subtract from a duration.
    #[cfg(feature = "query_stats")]
    {
        let started = std::time::Instant::now();
        let result = materialize_query_inner(query, alias, statement, database, tx).await;
        // Only a relation that was actually built, which is the same rule the
        // `WITH` path follows: a materialisation that failed produced nothing
        // to have been slow *for*, and the statement carrying it is recorded as
        // the failure it was.
        if result.is_ok() {
            crate::core::query_stats::note_materialised(
                started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );
        }
        result
    }
}

async fn materialize_query_inner(
    query: &str,
    alias: &str,
    statement: &str,
    database: &Arc<Database>,
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<String, String> {
    // A derived table may hold derived tables of its own, to any depth. Those
    // are materialised first, so the query that runs here reads relations
    // only. Without this the inner query reached the scan with `FROM (SELECT`
    // still in it and failed as "Table not found". Each level drops what it
    // built once its own relation is filled: the rows have been copied out.
    let nested = Box::pin(materialize_derived(query, database, tx)).await?;
    let result = match &nested {
        Some(inner) => {
            build_derived(&inner.rewritten, alias, statement, database, tx).await
        }
        None => build_derived(query, alias, statement, database, tx).await,
    };
    if let Some(inner) = &nested {
        drop_temps(database, &inner.temps, tx).await;
    }
    result
}

/// [`materialize_query`] for a query that reads relations only.
async fn build_derived(
    query: &str,
    alias: &str,
    statement: &str,
    database: &Arc<Database>,
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<String, String> {
    let (rows, scales) = crate::core::rql::executor::run_query_rows_scaled(query, database, tx)
        .await
        .map_err(|e| format!("the subquery for '{}' failed: {}", alias, e))?;

    if select_list_of(query).is_none() {
        return Err(format!("the subquery for '{}' must be a SELECT", alias));
    }
    let labels = select_labels(query, database).await.ok_or_else(|| {
        format!(
            "the subquery for '{}' has a column this cannot name; alias it with AS",
            alias
        )
    })?;

    let temp = temp_name(alias, statement, 0);
    let mut created = false;
    let built = build_relation(
        query,
        &temp,
        &labels,
        &rows,
        scales.as_deref(),
        &format!("the subquery for '{}'", alias),
        database,
        tx,
        &mut created,
    )
    .await;
    if let Err(e) = built {
        // The caller is handed a name only on success, so a table created
        // before the fill failed is dropped here.
        if created {
            drop_temps(database, std::slice::from_ref(&temp), tx).await;
        }
        return Err(e);
    }
    Ok(temp)
}

/// Drops the temporary tables a statement materialised. Best effort: a failure
/// here must not mask the statement's own result.
pub async fn drop_temps(
    database: &Arc<Database>,
    temps: &[String],
    tx: crate::core::mvcc::transaction::TxId,
) {
    for temp in temps {
        crate::owner_probe!(crate::core::mvcc::owner::probe::Site::DropTemps, tx, database);
        let sql = format!("DROP TABLE {}", temp);
        if let Err(e) = crate::core::rql::executor::run_statement(&sql, database, tx).await {
            log::warn!("could not drop the temporary table {}: {}", temp, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_is_refused_by_keyword() {
        // The one that must never be approximated: run as non-recursive it
        // returns the base case and no error.
        let err = split_with("WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t").unwrap_err();
        assert!(err.contains("RECURSIVE"), "{}", err);
        // A CTE merely *named* something starting with "recursive" is fine.
        let ok =
            split_with("WITH recursive_totals AS (SELECT a FROM b) SELECT a FROM recursive_totals");
        assert!(ok.is_ok(), "{:?}", ok.err());
    }

    #[test]
    fn a_statement_without_with_is_left_alone() {
        assert!(split_with("SELECT 1 FROM t").unwrap().is_none());
        // `withdrawals` must not read as the keyword.
        assert!(split_with("SELECT x FROM withdrawals").unwrap().is_none());
    }

    #[test]
    fn splits_one_and_several_definitions() {
        let (defs, rest) = split_with("WITH a AS (SELECT id FROM t) SELECT id FROM a")
            .unwrap()
            .unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].name, "a");
        assert_eq!(defs[0].body, "SELECT id FROM t");
        assert_eq!(rest, "SELECT id FROM a");

        let (defs, rest) = split_with(
            "WITH a AS (SELECT id FROM t), b AS (SELECT id FROM u) INSERT INTO v (id) SELECT id FROM b",
        )
        .unwrap()
        .unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[1].name, "b");
        assert!(rest.starts_with("INSERT INTO v"));
    }

    #[test]
    fn a_nested_paren_in_the_body_does_not_end_it() {
        let (defs, _) = split_with(
            "WITH a AS (SELECT id FROM t WHERE x IN (SELECT y FROM u)) SELECT id FROM a",
        )
        .unwrap()
        .unwrap();
        assert_eq!(defs.len(), 1);
        assert!(defs[0].body.ends_with("(SELECT y FROM u))") || defs[0].body.contains("IN ("));
    }

    #[test]
    fn labels_follow_postgres_naming() {
        assert_eq!(item_label("d.id").as_deref(), Some("id"));
        assert_eq!(item_label("id").as_deref(), Some("id"));
        assert_eq!(item_label("\"id\"").as_deref(), Some("id"));
        assert_eq!(item_label("count(*) AS n").as_deref(), Some("n"));
        // Unnameable rather than invented: the outer query refers to these by
        // name, so guessing would bind the wrong column.
        assert_eq!(item_label("count(*)"), None);
        assert_eq!(item_label("*"), None);
    }

    #[test]
    fn only_relation_references_are_replaced() {
        let f = replace_relation_references;
        assert_eq!(f("SELECT a FROM cte", "cte", "__t"), "SELECT a FROM __t");
        assert_eq!(
            f("SELECT a FROM x JOIN cte ON 1", "cte", "__t"),
            "SELECT a FROM x JOIN __t ON 1"
        );
        // Qualifying something is a relation reference too.
        assert_eq!(
            f("SELECT cte.id FROM cte", "cte", "__t"),
            "SELECT __t.id FROM __t"
        );
        // A longer identifier merely starting with the name is untouched.
        assert_eq!(
            f("SELECT a FROM cte_extra", "cte", "__t"),
            "SELECT a FROM cte_extra"
        );
        // Inside a literal it is data.
        assert_eq!(
            f("SELECT 'cte' FROM cte", "cte", "__t"),
            "SELECT 'cte' FROM __t"
        );
    }

    #[test]
    fn a_column_sharing_the_ctes_name_is_left_alone() {
        // Real migration SQL: a CTE named `domain_id` beside a column of the
        // same name. Rewriting the column list shifted the insert's columns and
        // surfaced as a bogus "column not found" about a string literal.
        let sql = "INSERT INTO users (id, domain_id, name) \
                   SELECT domain_id.id, 'x' FROM domain_id";
        let out = replace_relation_references(sql, "domain_id", "__cte_d");
        assert!(
            out.contains("(id, domain_id, name)"),
            "column list changed: {}",
            out
        );
        assert!(
            out.contains("__cte_d.id"),
            "qualified ref not rewritten: {}",
            out
        );
        assert!(
            out.contains("FROM __cte_d"),
            "relation not rewritten: {}",
            out
        );
    }

    #[test]
    fn a_column_qualified_by_another_relation_is_left_alone() {
        // `users.domain_id` is a column of `users`, not the CTE.
        let out = replace_relation_references(
            "SELECT users.domain_id FROM domain_id",
            "domain_id",
            "__cte_d",
        );
        assert!(out.contains("users.domain_id"), "{}", out);
        assert!(out.contains("FROM __cte_d"), "{}", out);
    }

    /// The statement temporaries' prefix is refused for any other table,
    /// whatever its case, and open only inside this module's own creates
    /// and a replica's apply.
    ///
    /// Mutation: `reserved_name_refusal` ignoring `CREATING_TEMPORARY` (the
    /// scoped calls are refused too).
    #[tokio::test]
    async fn the_temporary_prefix_is_reserved_outside_this_module() {
        for name in ["__cte_orders", "__CTE_x", "__Cte_", &temp_name("c", "WITH c AS (SELECT 1) SELECT 1", 0)] {
            let refused = reserved_name_refusal(name).unwrap_or_else(|| panic!("{name} was accepted"));
            assert_eq!(crate::pgwire::sqlstate_for(&refused), "42939", "{refused}");
            assert_eq!(
                crate::pgwire::sqlstate_for(&format!("could not rename t to {name}: {refused}")),
                "42939"
            );
            assert_eq!(allowing_temporaries(async { reserved_name_refusal(name) }).await, None, "{name}");
        }
        for name in ["orders", "_cte_x", "__ct", "my__cte_x", "__cat_tables_1f"] {
            assert_eq!(reserved_name_refusal(name), None, "{name}");
        }
    }

    #[test]
    fn temp_names_are_prefixed_and_never_shared() {
        let a = temp_name("roles", "WITH roles AS (SELECT 1) SELECT 1", 0);
        let b = temp_name("roles", "WITH roles AS (SELECT 2) SELECT 2", 0);
        assert!(a.starts_with("__cte_roles_0_"));
        assert_ne!(a, b);

        // The same statement twice must *also* differ. Two sessions running one
        // statement used to agree on a name, so one dropped the table the other
        // was reading — which is how a paginated count run concurrently failed
        // with "Table not found".
        let one = "WITH roles AS (SELECT 1) SELECT 1";
        assert_ne!(temp_name("roles", one, 0), temp_name("roles", one, 0));
    }

    #[test]
    fn a_with_clause_after_the_insert_target_is_moved_to_the_front() {
        assert_eq!(
            hoist_insert_with(
                "INSERT INTO t (a, b) WITH w AS (SELECT n FROM d) SELECT n, 'x' FROM w \
                 ON CONFLICT (a) DO NOTHING RETURNING a"
            )
            .as_deref(),
            Some(
                "WITH w AS (SELECT n FROM d) INSERT INTO t (a, b) SELECT n, 'x' FROM w \
                 ON CONFLICT (a) DO NOTHING RETURNING a"
            )
        );
        // No column list, and several definitions.
        assert_eq!(
            hoist_insert_with("INSERT INTO t WITH v AS (SELECT 1 AS a), w AS (SELECT a FROM v) SELECT a FROM w")
                .as_deref(),
            Some("WITH v AS (SELECT 1 AS a), w AS (SELECT a FROM v) INSERT INTO t SELECT a FROM w")
        );
        // Everything else is left as written.
        assert!(hoist_insert_with("INSERT INTO t (a) SELECT a FROM w").is_none());
        assert!(hoist_insert_with("INSERT INTO t (a) VALUES (1)").is_none());
        assert!(hoist_insert_with("WITH w AS (SELECT 1 AS a) INSERT INTO t SELECT a FROM w").is_none());
        assert!(hoist_insert_with("INSERT INTO withdrawals SELECT a FROM w").is_none());
    }

    #[test]
    fn a_body_of_named_constants_is_one_typed_row() {
        let columns =
            constant_body("SELECT 50 AS a, 'it''s' AS b, NULL AS c, true AS d, 1.50 AS e, 3000000000 AS f")
                .expect("constant");
        let labels: Vec<&str> = columns.iter().map(|c| c.label.as_str()).collect();
        let types: Vec<&str> = columns.iter().map(|c| c.sql_type.as_str()).collect();
        assert_eq!(labels, ["a", "b", "c", "d", "e", "f"]);
        assert_eq!(types, ["INTEGER", "TEXT", "TEXT", "BOOLEAN", "NUMERIC(38,2)", "BIGINT"]);
        assert_eq!(columns[1].value, "'it''s'");
        assert_eq!(columns[4].db_type, DbType::DECIMAL);
        // A column, a relation or an unnamed item is a query, not a constant row.
        assert!(constant_body("SELECT n AS a").is_none());
        assert!(constant_body("SELECT 1 AS a FROM t").is_none());
        assert!(constant_body("SELECT 1").is_none());
        assert!(constant_body("SELECT 'a' || 'b' AS c").is_none());
    }

    #[test]
    fn a_with_clause_that_renames_its_columns_is_refused() {
        let err = split_with("WITH a (x, y) AS (SELECT 1, 2) SELECT x FROM a").unwrap_err();
        assert!(err.contains("renames its columns"), "{}", err);
    }
}

/// Materialises every derived table — `FROM (SELECT …) alias` — into a
/// temporary table and points the statement at it.
///
/// The same treatment a CTE gets, for the same reason: the scan reads one
/// relation at a time, so a subquery standing where a relation should be has to
/// become one. Without this the FROM parser took `(SELECT` for a relation name
/// and reported `relation '(SELECT' does not exist`.
///
/// Returns `None` when the statement has no derived table.
pub async fn materialize_derived(
    sql: &str,
    database: &Arc<Database>,
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<Option<Materialized>, String> {
    let mut temps: Vec<String> = Vec::new();
    match materialize_derived_inner(sql, database, &mut temps, tx).await {
        Ok(result) => Ok(result),
        Err(e) => {
            // Whatever was created before the failure is dropped here: the
            // caller is handed nothing by an `Err` and so cannot clean up.
            drop_temps(database, &temps, tx).await;
            Err(e)
        }
    }
}

async fn materialize_derived_inner(
    sql: &str,
    database: &Arc<Database>,
    temps: &mut Vec<String>,
    tx: crate::core::mvcc::transaction::TxId,
) -> Result<Option<Materialized>, String> {
    let mut current = sql.to_string();
    let mut found_any = false;

    // Bounded: each pass replaces one derived table with a name, so the count
    // strictly decreases. The cap is a backstop against a rewrite that fails to
    // shrink rather than a real limit on how many a statement may have.
    for index in 0..16usize {
        let Some((start, end, inner, alias)) = next_derived_table(&current) else {
            break;
        };
        found_any = true;
        crate::owner_probe!(crate::core::mvcc::owner::probe::Site::Derived, tx, database);

        let temp = materialize_query(&inner, &alias, &current, database, tx).await?;
        temps.push(temp.clone());
        current = format!("{}{} {}{}", &current[..start], temp, alias, &current[end..]);
        let _ = index;
    }

    if !found_any {
        return Ok(None);
    }
    Ok(Some(Materialized {
        rewritten: current,
        temps: std::mem::take(temps),
    }))
}

/// Finds the first `FROM (SELECT …) alias` or `JOIN (SELECT …) alias`.
///
/// Returns the byte range covering `(…) alias`, the subquery text, and the
/// alias. A parenthesised *expression* is not a derived table, so the group has
/// to begin with `SELECT` to qualify.
/// The next derived table in , as .
///
/// Exposed so the describe path can resolve a derived table's columns without
/// materialising it — describe must have no side effects, and the alias's
/// schema is decided by the inner query's select list alone.
pub fn next_derived_table(sql: &str) -> Option<(usize, usize, String, String)> {
    for keyword in ["FROM", "JOIN"] {
        let mut from = 0usize;
        while let Some(at) = crate::core::rql::join::find_keyword(sql, keyword, from) {
            let after_keyword = at + keyword.len();
            let rest = &sql[after_keyword..];
            let lead = rest.len() - rest.trim_start().len();
            let open = after_keyword + lead;

            if sql.as_bytes().get(open) != Some(&b'(') {
                from = after_keyword;
                continue;
            }
            let inner_start = open + 1;
            let Some(close) = matching_paren(sql, open) else {
                from = after_keyword;
                continue;
            };
            let inner = sql[inner_start..close].trim();
            if !inner.to_ascii_uppercase().starts_with("SELECT")
                && !inner.to_ascii_uppercase().starts_with("WITH")
            {
                from = after_keyword;
                continue;
            }

            // `) alias` or `) AS alias`.
            let tail = &sql[close + 1..];
            let tail_trimmed = tail.trim_start();
            let lead_alias = tail.len() - tail_trimmed.len();
            let mut words = tail_trimmed.split_whitespace();
            let first = words.next().unwrap_or("");
            let (alias, alias_len) = if first.eq_ignore_ascii_case("AS") {
                let name = words.next().unwrap_or("");
                (name, first.len() + 1 + name.len())
            } else {
                (first, first.len())
            };
            let alias = alias.trim_matches('"');
            if alias.is_empty()
                || !alias
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            {
                // PostgreSQL requires one, and guessing a name would make the
                // rewritten statement refer to something the caller never wrote.
                return None;
            }

            return Some((
                open,
                close + 1 + lead_alias + alias_len,
                inner.to_string(),
                alias.to_string(),
            ));
        }
    }
    None
}

#[cfg(test)]
mod derived_tests {
    use super::*;

    #[test]
    fn a_derived_table_is_found_with_its_alias() {
        let (start, end, inner, alias) =
            next_derived_table("SELECT id FROM (SELECT id FROM t WHERE a = 1) ranked WHERE x")
                .expect("found");
        assert_eq!(inner, "SELECT id FROM t WHERE a = 1");
        assert_eq!(alias, "ranked");
        let sql = "SELECT id FROM (SELECT id FROM t WHERE a = 1) ranked WHERE x";
        assert_eq!(&sql[start..end], "(SELECT id FROM t WHERE a = 1) ranked");
    }

    #[test]
    fn the_as_spelling_is_accepted() {
        let (_, _, _, alias) = next_derived_table("SELECT * FROM (SELECT 1) AS r").expect("found");
        assert_eq!(alias, "r");
    }

    #[test]
    fn a_join_against_a_derived_table_is_found() {
        let (_, _, inner, alias) =
            next_derived_table("SELECT * FROM a JOIN (SELECT id FROM b) c ON a.id = c.id")
                .expect("found");
        assert_eq!(inner, "SELECT id FROM b");
        assert_eq!(alias, "c");
    }

    #[test]
    fn a_parenthesised_expression_is_not_a_derived_table() {
        // Only a group that begins with SELECT is one; `WHERE (a OR b)` and a
        // plain relation list must be left alone.
        assert!(next_derived_table("SELECT * FROM t WHERE (a = 1 OR b = 2)").is_none());
        assert!(next_derived_table("SELECT * FROM t").is_none());
    }

    #[test]
    fn a_derived_table_without_an_alias_is_not_rewritten() {
        // PostgreSQL requires one. Inventing a name would point the rewritten
        // statement at something nobody wrote.
        assert!(next_derived_table("SELECT * FROM (SELECT 1)").is_none());
    }
}
