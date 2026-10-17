//! `GENERATED ALWAYS AS (expression) STORED` columns.
//!
//! A generated column is not written by the client. Its value is computed from
//! the other columns of the same row, at insert and again at every update that
//! touches one of its inputs, and stored like an ordinary column — which is
//! what `STORED` means and why reads pay nothing for it.
//!
//! # Why this is not a `DEFAULT`
//!
//! A default and a generation expression look alike and behave differently in
//! the two ways that matter:
//!
//! * A default applies only when the column is **omitted**. A generation
//!   expression applies always, and PostgreSQL *refuses* an `INSERT` that
//!   supplies a value for the column at all.
//! * A default is evaluated with nothing in scope. A generation expression
//!   reads the row it belongs to, so it can only be evaluated once the rest of
//!   the row is known.
//!
//! Folding the two together would have meant a generated column silently
//! keeping a stale value after an update to its inputs — the failure this
//! engine is most prone to, where nothing errors and the number is simply
//! wrong. The declaration therefore gets a sidecar of its own.
//!
//! The clause was previously *parsed and discarded*: `CREATE TABLE` succeeded,
//! the column existed, and every row's value for it was NULL. `SUM(total)` over
//! such a table returned nothing and reported no error.

use std::io;

use rclite::Arc;

use crate::core::storage_providers::traits::StorageIO;

/// Bytes one persisted record occupies: the column name, the expression, and a
/// tombstone. Fixed-stride, so the file reads as an array — the same shape
/// every other sidecar here uses.
pub const GENERATED_SIZE: u64 = 129 + 513 + 1;

/// Longest generation expression that can be stored.
///
/// Wider than a default's: a default is usually a constant, where a generation
/// expression names several columns and the arithmetic between them — the one
/// in the schema this was written for is 62 characters, and a wider table's
/// would be longer still.
pub const MAX_EXPRESSION_LEN: usize = 512;

/// A column's generation expression, as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedColumn {
    pub column: String,
    pub expression: String,
    pub is_deleted: bool,
}

impl GeneratedColumn {
    pub fn into_vec(&self) -> Vec<u8> {
        let mut out = vec![0u8; GENERATED_SIZE as usize];

        let filtered: String = self
            .column
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let name = filtered.as_bytes();
        let name_len = name.len().min(128);
        out[..name_len].copy_from_slice(&name[..name_len]);

        let expression = self.expression.as_bytes();
        let expression_len = expression.len().min(MAX_EXPRESSION_LEN);
        out[129..129 + expression_len].copy_from_slice(&expression[..expression_len]);

        out[129 + 513] = u8::from(self.is_deleted);
        out
    }

    pub fn from_vec(raw: &[u8]) -> Result<Self, String> {
        if raw.len() != GENERATED_SIZE as usize {
            return Err(format!(
                "invalid generated-column record: expected {} bytes, got {}",
                GENERATED_SIZE,
                raw.len()
            ));
        }

        let read = |at: usize, len: usize| -> Result<String, String> {
            let end = raw[at..at + len]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(len - 1);
            std::str::from_utf8(&raw[at..at + end])
                .map(str::to_string)
                .map_err(|e| format!("invalid UTF-8 in generated-column record: {}", e))
        };

        Ok(GeneratedColumn {
            column: read(0, 129)?,
            expression: read(129, 513)?,
            is_deleted: raw[129 + 513] != 0,
        })
    }
}

fn sidecar_name<S: StorageIO>(schema_io: &Arc<S>) -> String {
    format!("{}_{}", schema_io.get_name().replace(".db", ""), "GEN.db")
}

/// Reads a table's generated columns.
pub async fn load<S: StorageIO>(schema_io: Arc<S>) -> Vec<GeneratedColumn> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;

    let mut out: Vec<GeneratedColumn> = Vec::new();
    if len < GENERATED_SIZE {
        return out;
    }

    let mut buffer = vec![0u8; GENERATED_SIZE as usize];
    for i in 0..(len / GENERATED_SIZE) {
        let mut position = i * GENERATED_SIZE;
        if io
            .read_data_into_buffer(&mut position, &mut buffer)
            .await
            .is_err()
        {
            break;
        }
        match GeneratedColumn::from_vec(&buffer) {
            // The newest record for a column wins, so a redefinition is an
            // append rather than a rewrite.
            Ok(entry) => {
                out.retain(|e| !e.column.eq_ignore_ascii_case(&entry.column));
                if !entry.is_deleted {
                    out.push(entry);
                }
            }
            Err(e) => log::error!("Skipping unreadable generated-column record: {}", e),
        }
    }

    out
}

/// Appends a record, superseding any earlier one for the same column.
pub async fn append<S: StorageIO>(schema_io: Arc<S>, entry: &GeneratedColumn) -> io::Result<()> {
    if entry.expression.len() > MAX_EXPRESSION_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "a generation expression may be at most {} characters",
                MAX_EXPRESSION_LEN
            ),
        ));
    }
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    io.append_data(&entry.into_vec(), true).await;
    Ok(())
}

/// The generation expression declared for `column`, if any.
pub fn expression_for<'a>(column: &str, generated: &'a [GeneratedColumn]) -> Option<&'a str> {
    generated
        .iter()
        .find(|g| g.column.eq_ignore_ascii_case(column) && !g.is_deleted)
        .map(|g| g.expression.as_str())
}

/// The literal an input column will actually hold once stored, for a
/// generation expression to read.
///
/// PostgreSQL computes a generated column after the row's other values have
/// been coerced to their columns' types, so the expression sees what is
/// stored. This read the literal the client wrote: `p NUMERIC(12,2)` given
/// 20.169 stored 20.17 while `g = p * 15` was computed from 20.169 — 302.54,
/// not 302.55 — and `WHERE g <> p * 15` found the row.
///
/// Only numbers are brought to their column here, a NUMERIC to its declared
/// scale (rounded) and an integer to a whole number (rounded); every other
/// type is stored as written, so its literal already is the stored value.
/// Text that does not evaluate is left for the insert to refuse in its own
/// words.
pub fn as_stored(literal: &str, field: &crate::core::row::schema::SchemaField) -> String {
    use crate::core::db_type::DbType;
    use crate::core::rql::expr::{self, Value};
    let numeric = matches!(
        field.db_type,
        DbType::I8
            | DbType::I16
            | DbType::I32
            | DbType::I64
            | DbType::U8
            | DbType::U16
            | DbType::U32
            | DbType::U64
            | DbType::DECIMAL
    );
    let text = literal.trim();
    if !numeric || text.eq_ignore_ascii_case("NULL") {
        return literal.to_string();
    }
    let unquoted = text.strip_prefix('\'').and_then(|t| t.strip_suffix('\'')).unwrap_or(text);
    // A plain number bound for a NUMERIC(p, s) is read the way the insert
    // reads it, rounded once at `s` from the digits as written. Evaluated, a
    // constant past nine places is a float, which keeps only the digits a
    // double does.
    if let (DbType::DECIMAL, Some(scale)) = (&field.db_type, field.scale) {
        if let Some(stored) = crate::core::value_codec::parse_decimal_rounded(unquoted, scale) {
            return crate::core::value_codec::format_decimal_scaled(stored, scale);
        }
    }
    let empty = crate::core::row::row::Row::default();
    let Ok(Value::Data { bytes, db_type }) = expr::parse(unquoted, &[])
        .and_then(|e| expr::eval(&e, &empty, &[]))
        .and_then(|v| expr::from_engine_scale(v, field))
    else {
        return literal.to_string();
    };
    match db_type {
        // `from_engine_scale` leaves it at the column's own scale, which the
        // engine-scale renderer would read a power of ten wrong.
        DbType::DECIMAL => match <[u8; 16]>::try_from(bytes.as_slice()) {
            Ok(raw) => crate::core::value_codec::format_decimal_scaled(
                i128::from_le_bytes(raw),
                field.scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE),
            ),
            Err(_) => literal.to_string(),
        },
        other => crate::core::rql::subquery::value_to_literal(&other, &bytes)
            .unwrap_or_else(|| literal.to_string()),
    }
}

/// Reads a `GENERATED ALWAYS AS (…) STORED` clause out of a column definition,
/// returning the definition with the clause removed and the expression.
///
/// `GENERATED … AS IDENTITY` is a different construct — a sequence, not a
/// computed value — and is left in place for the identity handling rather than
/// mistaken for this one.
pub fn split_clause(definition: &str) -> (String, Option<String>) {
    let upper = definition.to_ascii_uppercase();
    let Some(at) = find_keyword(&upper, "GENERATED") else {
        return (definition.to_string(), None);
    };
    let tail = &definition[at..];
    let tail_upper = &upper[at..];
    if tail_upper.contains("IDENTITY") {
        return (definition.to_string(), None);
    }
    // The expression is the parenthesised group after `AS`.
    let Some(open) = tail.find('(') else {
        return (definition.to_string(), None);
    };
    let mut depth = 0usize;
    let mut close = None;
    for (offset, c) in tail[open..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(open + offset);
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(close) = close else {
        return (definition.to_string(), None);
    };

    let expression = tail[open + 1..close].trim().to_string();
    // Everything through the closing parenthesis goes, and so does a trailing
    // `STORED` or `VIRTUAL`.
    let mut rest = tail[close + 1..].trim_start();
    for word in ["STORED", "VIRTUAL"] {
        if rest.len() >= word.len() && rest[..word.len()].eq_ignore_ascii_case(word) {
            rest = rest[word.len()..].trim_start();
            break;
        }
    }
    let remainder = format!("{} {}", definition[..at].trim_end(), rest);
    (remainder.trim().to_string(), Some(expression))
}

/// Position of `keyword` as a whole word.
fn find_keyword(upper: &str, keyword: &str) -> Option<usize> {
    fn is_word_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'_'
    }
    let bytes = upper.as_bytes();
    let mut from = 0usize;
    while let Some(offset) = upper[from..].find(keyword) {
        let at = from + offset;
        let before_ok = at == 0 || !is_word_byte(bytes[at - 1]);
        let after = at + keyword.len();
        let after_ok = after >= bytes.len() || !is_word_byte(bytes[after]);
        if before_ok && after_ok {
            return Some(at);
        }
        from = at + keyword.len();
    }
    None
}

/// Whether a statement is a `CREATE TABLE` that declares a generated column,
/// whose expressions must reach [`validate`] as written.
///
/// Matched on the words `GENERATED ALWAYS AS` rather than on `GENERATED`
/// alone, so a column that is merely named `generated` does not turn off the
/// subquery handling of a `CREATE TABLE … AS SELECT`.
pub fn declares_generated(statement: &str) -> bool {
    let upper = statement.to_ascii_uppercase();
    let words: Vec<&str> = upper.split_whitespace().collect();
    words.first() == Some(&"CREATE")
        && words.iter().skip(1).take(3).any(|w| *w == "TABLE")
        && words
            .windows(3)
            .any(|w| w[0] == "GENERATED" && w[1] == "ALWAYS" && w[2].starts_with("AS"))
}

/// Checks the generation expressions of a `CREATE TABLE`, with PostgreSQL's
/// refusals.
///
/// Each of these used to be accepted. A reference to another generated column
/// read that column's value before it had been computed, so it saw NULL; a
/// volatile function stored a different answer for the same inputs on every
/// write; and a subquery reached the evaluator as text it cannot run, so every
/// insert failed. Refused here, the table is never created, which is
/// PostgreSQL's behaviour and leaves nothing half-usable behind.
///
/// `columns` is every column the table declares, so a column that happens to
/// share a name with a clock keyword is read as the column.
pub fn validate(generated: &[GeneratedColumn], columns: &[&str]) -> Result<(), String> {
    // Not immutable: each gives a different answer on a later evaluation, so
    // a stored value could not be recomputed from the row it belongs to.
    const VOLATILE_CALLS: &[&str] = &[
        "now",
        "random",
        "random_normal",
        "setseed",
        "clock_timestamp",
        "statement_timestamp",
        "transaction_timestamp",
        "timeofday",
        "gen_random_uuid",
        "uuid_generate_v1",
        "uuid_generate_v4",
        "uuidv4",
        "uuidv7",
        "nextval",
        "currval",
        "lastval",
        "setval",
        "txid_current",
        "pg_current_xact_id",
        "current_setting",
        "pg_backend_pid",
    ];
    // The same, spelled without parentheses.
    const VOLATILE_WORDS: &[&str] = &[
        "current_timestamp",
        "current_date",
        "current_time",
        "localtime",
        "localtimestamp",
        "current_user",
        "session_user",
        "current_role",
        "current_catalog",
        "current_schema",
    ];

    for g in generated.iter().filter(|g| !g.is_deleted) {
        let words = words_of(&g.expression);
        if words.iter().any(|(w, _)| w.eq_ignore_ascii_case("SELECT")) {
            return Err("cannot use subquery in column generation expression".to_string());
        }
        // PostgreSQL computes every generated column from the row's other
        // columns, never from one another, and that includes the column itself.
        if let Some((name, _)) = words.iter().find(|(w, called)| {
            !called
                && generated
                    .iter()
                    .any(|other| !other.is_deleted && other.column.eq_ignore_ascii_case(w))
        }) {
            return Err(format!(
                "cannot use generated column \"{}\" in column generation expression",
                name
            ));
        }
        let volatile = words.iter().any(|(w, called)| {
            if *called {
                VOLATILE_CALLS.iter().any(|f| f.eq_ignore_ascii_case(w))
            } else {
                VOLATILE_WORDS.iter().any(|k| k.eq_ignore_ascii_case(w))
                    && !columns.iter().any(|c| c.eq_ignore_ascii_case(w))
            }
        });
        if volatile {
            return Err("generation expression is not immutable".to_string());
        }
    }
    Ok(())
}

/// The identifiers of an expression outside its string literals, each with
/// whether a `(` follows it, which is what makes it a function call.
fn words_of(expression: &str) -> Vec<(String, bool)> {
    let bytes = expression.as_bytes();
    let mut out: Vec<(String, bool)> = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if bytes.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            // A quoted identifier is still a name, just one that keeps its case.
            b'"' => {
                let start = i + 1;
                let end = expression[start..]
                    .find('"')
                    .map(|at| start + at)
                    .unwrap_or(bytes.len());
                out.push((expression[start..end].to_string(), false));
                i = end + 1;
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let called = expression[i..].trim_start().starts_with('(');
                out.push((expression[start..i].to_string(), called));
            }
            // A number's digits are not a name, and neither is a letter that
            // follows them, as in `1e5`.
            b if b.is_ascii_digit() => {
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }
    out
}

/// Parses a generation expression, reading it as a condition when it is one.
///
/// `expr::parse` reads scalar expressions only, so `q > 5` for a `BOOL` column
/// failed on every insert with "'(7) > 5' is neither a column nor a value",
/// although `CREATE TABLE` had accepted it. A condition is therefore tried when
/// the scalar parse fails, and anything the scalar parser already reads keeps
/// exactly the meaning it had.
pub fn parse_expression(
    text: &str,
    schema: &[crate::core::row::schema::SchemaField],
) -> Result<crate::core::rql::expr::Expr, String> {
    let scalar_error = match crate::core::rql::expr::parse(text, schema) {
        Ok(expr) => return Ok(expr),
        Err(e) => e,
    };
    match parse_predicate(text, schema) {
        Ok(predicate) => Ok(predicate.into_expr()),
        // The condition's reason when the text reads as one, since the scalar
        // parser's complaint is about a comparison it was never meant to read.
        Err(reason) if reads_as_condition(text) => Err(reason),
        Err(_) => Err(scalar_error),
    }
}

fn reads_as_condition(text: &str) -> bool {
    split_top_level(text, "AND").is_some()
        || split_top_level(text, "OR").is_some()
        || starts_with_word(text, "NOT")
        || text.contains(['<', '>', '='])
        || find_keyword(&text.to_ascii_uppercase(), "IS").is_some()
}

/// A condition in SQL's three-valued logic.
///
/// `expr::Cond` is two-valued: a comparison with NULL is false, which is right
/// for `CASE WHEN` and `WHERE`, where unknown and false both mean "no". A
/// stored boolean has to keep the difference, since `NULL > 5` stores NULL
/// and `NOT (NULL = 1)` is NULL, not true. So each condition is carried as
/// the pair "it is true" and "it is false", both two-valued, and the value is
/// TRUE, FALSE, or, when neither holds, NULL.
enum Predicate {
    Leaf(crate::core::rql::expr::Condition),
    /// A boolean value with no comparison of its own, such as a column.
    Term(crate::core::rql::expr::Expr),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    /// The two-valued condition that holds exactly when this one is true.
    fn holds(&self) -> crate::core::rql::expr::Cond {
        use crate::core::rql::expr::{Cmp, Cond, Condition};
        match self {
            Predicate::Leaf(c) => Cond::Cmp(c.clone()),
            Predicate::Term(e) => Cond::Cmp(Condition {
                left: Box::new(e.clone()),
                cmp: Cmp::IsTrue,
                right: None,
            }),
            Predicate::And(parts) => Cond::And(parts.iter().map(Predicate::holds).collect()),
            Predicate::Or(parts) => Cond::Or(parts.iter().map(Predicate::holds).collect()),
            Predicate::Not(inner) => inner.fails(),
        }
    }

    /// The two-valued condition that holds exactly when this one is false.
    ///
    /// Negation is pushed down to the comparisons rather than applied to
    /// `holds`: "not true" includes NULL, and "false" must not.
    fn fails(&self) -> crate::core::rql::expr::Cond {
        use crate::core::rql::expr::{Cmp, Cond, Condition};
        match self {
            Predicate::Leaf(c) => {
                let cmp = match c.cmp {
                    Cmp::Eq => Cmp::Ne,
                    Cmp::Ne => Cmp::Eq,
                    Cmp::Lt => Cmp::Ge,
                    Cmp::Ge => Cmp::Lt,
                    Cmp::Le => Cmp::Gt,
                    Cmp::Gt => Cmp::Le,
                    Cmp::IsNull => Cmp::IsNotNull,
                    Cmp::IsNotNull => Cmp::IsNull,
                    Cmp::IsTrue => Cmp::IsNotTrue,
                    Cmp::IsNotTrue => Cmp::IsTrue,
                    Cmp::IsFalse => Cmp::IsNotFalse,
                    Cmp::IsNotFalse => Cmp::IsFalse,
                };
                Cond::Cmp(Condition {
                    left: c.left.clone(),
                    cmp,
                    right: c.right.clone(),
                })
            }
            Predicate::Term(e) => Cond::Cmp(Condition {
                left: Box::new(e.clone()),
                cmp: Cmp::IsFalse,
                right: None,
            }),
            Predicate::And(parts) => Cond::Or(parts.iter().map(Predicate::fails).collect()),
            Predicate::Or(parts) => Cond::And(parts.iter().map(Predicate::fails).collect()),
            Predicate::Not(inner) => inner.holds(),
        }
    }

    /// `CASE WHEN <true> THEN TRUE WHEN <false> THEN FALSE END`, whose missing
    /// `ELSE` is the NULL a condition over NULL yields.
    fn into_expr(self) -> crate::core::rql::expr::Expr {
        use crate::core::db_type::DbType;
        use crate::core::rql::expr::Expr;
        let boolean = |b: bool| Expr::Literal {
            bytes: vec![u8::from(b)],
            db_type: DbType::BOOL,
        };
        Expr::Case {
            whens: vec![(self.holds(), boolean(true)), (self.fails(), boolean(false))],
            otherwise: None,
        }
    }
}

/// `OR` binds loosest, then `AND`, then `NOT`, and a comparison tightest, as
/// in SQL.
fn parse_predicate(
    text: &str,
    schema: &[crate::core::row::schema::SchemaField],
) -> Result<Predicate, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("empty condition".to_string());
    }
    if text.starts_with('(') {
        if let Some(close) = crate::core::rql::expr::matching_paren(text, 0) {
            if close == text.len() - 1 {
                return parse_predicate(&text[1..close], schema);
            }
        }
    }
    for (keyword, join) in [
        ("OR", Predicate::Or as fn(Vec<Predicate>) -> Predicate),
        ("AND", Predicate::And as fn(Vec<Predicate>) -> Predicate),
    ] {
        if let Some(parts) = split_top_level(text, keyword) {
            let mut out = Vec::with_capacity(parts.len());
            for part in parts {
                out.push(parse_predicate(part, schema)?);
            }
            return Ok(join(out));
        }
    }
    if starts_with_word(text, "NOT") {
        return Ok(Predicate::Not(Box::new(parse_predicate(&text[3..], schema)?)));
    }
    match crate::core::rql::expr::parse_condition(text, schema) {
        Ok(condition) => Ok(Predicate::Leaf(condition)),
        Err(reason) => {
            if let Some(test) = test_of_predicate(text, schema) {
                return Ok(test);
            }
            match crate::core::rql::expr::parse(text, schema) {
                Ok(term) => Ok(Predicate::Term(term)),
                Err(_) => Err(reason),
            }
        }
    }
}

/// `(q > 5) IS TRUE`, `(a = b) IS NULL`: a test whose operand is itself a
/// condition, which `parse_condition` cannot read since its operands are
/// scalars. The condition becomes the boolean-or-NULL expression it stands
/// for, and the test is a plain comparison on that. A test is never NULL, so
/// its negation is the opposite test and nothing more.
fn test_of_predicate(
    text: &str,
    schema: &[crate::core::row::schema::SchemaField],
) -> Option<Predicate> {
    use crate::core::rql::expr::{Cmp, Condition};
    let at = crate::core::rql::join::find_keyword(text, "IS", 0)?;
    let tail = text[at + "IS".len()..].to_ascii_uppercase();
    let cmp = match tail.split_whitespace().collect::<Vec<_>>().as_slice() {
        ["TRUE"] => Cmp::IsTrue,
        ["NOT", "TRUE"] => Cmp::IsNotTrue,
        ["FALSE"] => Cmp::IsFalse,
        ["NOT", "FALSE"] => Cmp::IsNotFalse,
        ["NULL"] | ["UNKNOWN"] => Cmp::IsNull,
        ["NOT", "NULL"] | ["NOT", "UNKNOWN"] => Cmp::IsNotNull,
        _ => return None,
    };
    let operand = parse_predicate(&text[..at], schema).ok()?;
    Some(Predicate::Leaf(Condition {
        left: Box::new(operand.into_expr()),
        cmp,
        right: None,
    }))
}

fn starts_with_word(text: &str, word: &str) -> bool {
    let text = text.trim_start();
    text.len() > word.len()
        && text[..word.len()].eq_ignore_ascii_case(word)
        && (text.as_bytes()[word.len()].is_ascii_whitespace() || text.as_bytes()[word.len()] == b'(')
}

/// Splits on a keyword at the top level: outside quotes and parentheses, and
/// outside `CASE … END`, whose `WHEN a AND b` belongs to the `CASE`. The `AND`
/// of a `BETWEEN` is not a split either. `None` when there is nothing to split.
fn split_top_level<'a>(text: &'a str, keyword: &str) -> Option<Vec<&'a str>> {
    let bytes = text.as_bytes();
    let mut parts: Vec<&str> = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut case_depth = 0usize;
    let mut between = false;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            quote @ (b'\'' | b'"') => {
                i += 1;
                while i < bytes.len() && bytes[i] != quote {
                    i += 1;
                }
                i += 1;
            }
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            b if b.is_ascii_alphanumeric() || b == b'_' => {
                let word_start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                if depth != 0 {
                    continue;
                }
                let word = &text[word_start..i];
                if word.eq_ignore_ascii_case("CASE") {
                    case_depth += 1;
                } else if word.eq_ignore_ascii_case("END") {
                    case_depth = case_depth.saturating_sub(1);
                } else if word.eq_ignore_ascii_case("BETWEEN") {
                    between = true;
                } else if case_depth == 0 && word.eq_ignore_ascii_case(keyword) {
                    if keyword.eq_ignore_ascii_case("AND") && between {
                        between = false;
                        continue;
                    }
                    parts.push(text[start..word_start].trim());
                    start = i;
                }
            }
            _ => i += 1,
        }
    }
    if parts.is_empty() {
        return None;
    }
    parts.push(text[start..].trim());
    if parts.iter().any(|p| p.is_empty()) {
        return None;
    }
    Some(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_round_trips_through_its_bytes() {
        let entry = GeneratedColumn {
            column: "total_amount".to_string(),
            expression: "subtotal + tax_amount + shipping_fee - discount_amount".to_string(),
            is_deleted: false,
        };
        assert_eq!(GeneratedColumn::from_vec(&entry.into_vec()).unwrap(), entry);
    }

    #[test]
    fn the_clause_is_split_off_and_the_rest_survives() {
        let (rest, expression) = split_clause(
            "total_amount NUMERIC(12, 4) GENERATED ALWAYS AS (subtotal + tax_amount) STORED",
        );
        assert_eq!(rest, "total_amount NUMERIC(12, 4)");
        assert_eq!(expression.as_deref(), Some("subtotal + tax_amount"));
    }

    #[test]
    fn a_constraint_after_the_clause_is_kept() {
        let (rest, expression) =
            split_clause("t NUMERIC GENERATED ALWAYS AS (a * b) STORED NOT NULL");
        assert_eq!(rest, "t NUMERIC NOT NULL");
        assert_eq!(expression.as_deref(), Some("a * b"));
    }

    #[test]
    fn nested_parentheses_do_not_end_the_expression_early() {
        let (_, expression) =
            split_clause("t NUMERIC GENERATED ALWAYS AS ((a + b) * (c - d)) STORED");
        assert_eq!(expression.as_deref(), Some("(a + b) * (c - d)"));
    }

    #[test]
    fn an_identity_column_is_left_alone() {
        // A different construct entirely — a sequence, not a computed value.
        let definition = "id INTEGER GENERATED ALWAYS AS IDENTITY";
        let (rest, expression) = split_clause(definition);
        assert_eq!(rest, definition);
        assert!(expression.is_none());
    }

    /// Folds a generation expression whose columns are already literals, as
    /// the insert path does, and renders the result as PostgreSQL prints it.
    fn fold(text: &str) -> String {
        use crate::core::db_type::DbType;
        use crate::core::rql::expr::Value;
        let expr = parse_expression(text, &[]).unwrap_or_else(|e| panic!("{}: {}", text, e));
        match crate::core::rql::expr::eval(&expr, &crate::core::row::row::Row::default(), &[]) {
            Ok(Value::Null) => "NULL".to_string(),
            Ok(Value::Data {
                bytes,
                db_type: DbType::BOOL,
            }) => if bytes[0] != 0 { "t" } else { "f" }.to_string(),
            Ok(Value::Data {
                bytes,
                db_type: DbType::I64,
            }) => i64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
            other => panic!("{}: unexpected {:?}", text, other),
        }
    }

    #[test]
    fn a_condition_folds_to_postgres_three_valued_answer() {
        // Each answer is PostgreSQL 17's for the same expression.
        for (text, expected) in [
            ("(7) > 5", "t"),
            ("(2) > 5", "f"),
            ("(NULL) > 5", "NULL"),
            ("(2) > 1 AND (2) < 10", "t"),
            ("(0) > 1 AND (NULL) < 10", "f"),
            ("(2) > 1 AND (NULL) < 10", "NULL"),
            ("(NULL) > 100 OR (3) = 3", "t"),
            ("(0) > 100 OR (NULL) = 3", "NULL"),
            ("NOT ((7) = (3))", "t"),
            ("NOT ((2) = (2))", "f"),
            ("NOT ((NULL) = (3))", "NULL"),
            ("NOT (NOT ((NULL) = (3)))", "NULL"),
            ("NOT ((0) > 1 AND (NULL) < 10)", "t"),
            ("('x') IS NULL", "f"),
            ("(NULL) IS NULL", "t"),
            ("(NULL) IS NOT NULL", "f"),
            ("((7) > 5) IS TRUE", "t"),
            ("((NULL) > 5) IS NOT TRUE", "t"),
            ("NOT ((NULL) > 5) IS TRUE", "t"),
            ("TRUE AND (NULL) > 1", "NULL"),
            ("FALSE AND (NULL) > 1", "f"),
            ("NOT TRUE", "f"),
        ] {
            assert_eq!(fold(text), expected, "{}", text);
        }
    }

    #[test]
    fn a_scalar_expression_keeps_its_meaning() {
        // Parsed by the scalar parser first, so nothing it already read changes.
        assert_eq!(fold("(7) * 10 + (3) - 1"), "72");
        assert_eq!(fold("((7) + (3)) * ((7) - (3))"), "40");
        assert_eq!(fold("(NULL) * 10"), "NULL");
    }

    #[test]
    fn the_and_of_a_case_or_a_between_is_not_a_split() {
        assert!(split_top_level("CASE WHEN a > 1 AND b < 2 THEN 1 END", "AND").is_none());
        assert!(split_top_level("q BETWEEN 1 AND 5", "AND").is_none());
        assert_eq!(
            split_top_level("q BETWEEN 1 AND 5 AND r = 'a AND b'", "AND"),
            Some(vec!["q BETWEEN 1 AND 5", "r = 'a AND b'"])
        );
        assert!(split_top_level("brand = 1", "AND").is_none());
    }

    fn entry(column: &str, expression: &str) -> GeneratedColumn {
        GeneratedColumn {
            column: column.to_string(),
            expression: expression.to_string(),
            is_deleted: false,
        }
    }

    #[test]
    fn postgres_refusals_are_made_at_definition() {
        let columns = ["a", "g1", "g2"];
        let refused = |list: &[GeneratedColumn]| validate(list, &columns).unwrap_err();
        assert_eq!(
            refused(&[entry("g1", "a + 1"), entry("g2", "g1 + 1")]),
            "cannot use generated column \"g1\" in column generation expression"
        );
        assert_eq!(
            refused(&[entry("g1", "g1 * 2")]),
            "cannot use generated column \"g1\" in column generation expression"
        );
        for volatile in ["now()", "a + random()", "CURRENT_TIMESTAMP", "gen_random_uuid()::text"] {
            assert_eq!(
                refused(&[entry("g1", volatile)]),
                "generation expression is not immutable",
                "{}",
                volatile
            );
        }
        for subquery in ["(SELECT 1)", "a + (select max(a) FROM t)"] {
            assert_eq!(
                refused(&[entry("g1", subquery)]),
                "cannot use subquery in column generation expression"
            );
        }
    }

    #[test]
    fn a_generated_declaration_is_recognised_and_a_column_name_is_not() {
        assert!(declares_generated(
            "CREATE TABLE t (a INT, g INT GENERATED ALWAYS AS ((SELECT 1)) STORED)"
        ));
        assert!(declares_generated(
            "create temp table t (a int,\n g int generated\talways as(a + 1) stored)"
        ));
        assert!(!declares_generated(
            "CREATE TABLE t AS SELECT generated FROM s WHERE id IN (SELECT id FROM u)"
        ));
        assert!(!declares_generated(
            "SELECT 1 FROM t WHERE x IN (SELECT 'GENERATED ALWAYS AS')"
        ));
    }

    #[test]
    fn what_postgres_accepts_is_accepted() {
        // A word inside a string, a column named like a clock keyword, and a
        // call to an immutable function are all fine.
        let columns = ["a", "current_date", "g1"];
        for expression in [
            "a || 'now() SELECT g1'",
            "current_date + 1",
            "abs(a) + 1",
            "CASE WHEN a > 1 THEN 'x' ELSE 'y' END",
            "COALESCE(a, 0) * 2",
        ] {
            assert_eq!(validate(&[entry("g1", expression)], &columns), Ok(()), "{}", expression);
        }
    }

    #[test]
    fn a_column_named_after_the_keyword_is_not_mistaken_for_one() {
        let definition = "generated_at TIMESTAMPTZ NOT NULL";
        let (rest, expression) = split_clause(definition);
        assert_eq!(rest, definition);
        assert!(expression.is_none());
    }
}
