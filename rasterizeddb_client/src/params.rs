//! `$n` placeholders, rendered into literals the server's lexer reads back as
//! exactly the value given, and the one-statement rule every text sent obeys.

use std::fmt;
use std::fmt::Write as _;
use std::marker::PhantomData;

use crate::to_sql::ToSql;
use crate::types::{Date, Interval, Time, Timestamp, Uuid};
use crate::{Error, Result};

/// One value for a `$n` placeholder.
///
/// Borrowed and `Copy`: building one never allocates. [`ToSql`] turns a Rust
/// value into one, and [`params!`](crate::params) does that for a whole list.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum Param<'a> {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    /// Rendered as a single-quoted literal. Must not contain a NUL, which no
    /// text value can hold.
    Text(&'a str),
    /// Rendered as a `'\x…'` hex literal, which the server decodes as
    /// PostgreSQL's `byteain` does.
    Bytes(&'a [u8]),
    /// For a `UBIGINT` column, or a `BIGINT` one when it fits.
    U64(u64),
    /// For a `HUGEINT` column, or a `NUMERIC` one.
    I128(i128),
    /// Written in the shortest form that reads back as the same `f32`, cast
    /// to `real` (`0.1::real`) so it equals a `real` column holding it;
    /// infinities and NaN as `'Infinity'`, `'-Infinity'`, `'NaN'`.
    F32(f32),
    /// As [`Param::F32`], for `f64`.
    F64(f64),
    /// An exact decimal, as text (`-12.50`, `1e-9`, `NaN`). Checked to be a
    /// number before it is written: it is not quoted.
    Numeric(&'a str),
    Uuid(Uuid),
    /// Written in UTC with an explicit `+00`, so a `timestamptz` column reads
    /// the same instant whatever the server's zone, and a `timestamp` column
    /// the same wall-clock time (PostgreSQL drops the zone there).
    Timestamp(Timestamp),
    Date(Date),
    Time(Time),
    Interval(Interval),
    /// A JSON document's text, for a `json` or `jsonb` column.
    Json(&'a str),
    /// An array literal (`'{1,2,NULL}'`) for an array column, or for
    /// `= ANY($1)`: `Param::Array(ArrayRef::new(&[1i64, 2]))`, or any
    /// `Vec<T>` or `&[T]` through [`ToSql`].
    Array(ArrayRef<'a>),
}

/// A borrowed slice of [`ToSql`] values, whatever their type, for
/// [`Param::Array`]. `Copy`, `Send` and `Sync`, like the rest of [`Param`].
#[derive(Clone, Copy)]
pub struct ArrayRef<'a> {
    data: *const (),
    len: usize,
    element: unsafe fn(*const (), usize) -> Param<'a>,
    _slice: PhantomData<&'a ()>,
}

// SAFETY: an `ArrayRef` is only made from a `&'a [T]` with `T: ToSql`, and
// `ToSql: Sync`, so the slice is `Send` and `Sync`, and the pointer is only
// ever read through it.
unsafe impl Send for ArrayRef<'_> {}
unsafe impl Sync for ArrayRef<'_> {}

impl<'a> ArrayRef<'a> {
    pub fn new<T: ToSql + 'a>(slice: &'a [T]) -> Self {
        /// # Safety
        /// `data` is the start of a live `[T]` and `i` is within it.
        unsafe fn element<'a, T: ToSql + 'a>(data: *const (), i: usize) -> Param<'a> {
            // SAFETY: as the function's contract says; the reference lives
            // for 'a because the slice it points into does.
            let value: &'a T = unsafe { &*data.cast::<T>().add(i) };
            value.to_param()
        }
        ArrayRef {
            data: slice.as_ptr().cast(),
            len: slice.len(),
            element: element::<T>,
            _slice: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Element `i`. Panics past the end, as indexing a slice does.
    pub fn get(&self, i: usize) -> Param<'a> {
        assert!(i < self.len, "index {i} past an array of {}", self.len);
        // SAFETY: `data`/`len` came from a `&'a [T]` in `new`, with the
        // matching `element::<T>`; `i` was checked against `len`.
        unsafe { (self.element)(self.data, i) }
    }
}

impl fmt::Debug for Param<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Param::Null => f.write_str("Null"),
            Param::Bool(v) => f.debug_tuple("Bool").field(v).finish(),
            Param::I16(v) => f.debug_tuple("I16").field(v).finish(),
            Param::I32(v) => f.debug_tuple("I32").field(v).finish(),
            Param::I64(v) => f.debug_tuple("I64").field(v).finish(),
            Param::Text(v) => f.debug_tuple("Text").field(v).finish(),
            Param::Bytes(v) => f.debug_tuple("Bytes").field(v).finish(),
            Param::U64(v) => f.debug_tuple("U64").field(v).finish(),
            Param::I128(v) => f.debug_tuple("I128").field(v).finish(),
            Param::F32(v) => f.debug_tuple("F32").field(v).finish(),
            Param::F64(v) => f.debug_tuple("F64").field(v).finish(),
            Param::Numeric(v) => f.debug_tuple("Numeric").field(v).finish(),
            Param::Uuid(v) => f.debug_tuple("Uuid").field(v).finish(),
            Param::Timestamp(v) => f.debug_tuple("Timestamp").field(v).finish(),
            Param::Date(v) => f.debug_tuple("Date").field(v).finish(),
            Param::Time(v) => f.debug_tuple("Time").field(v).finish(),
            Param::Interval(v) => f.debug_tuple("Interval").field(v).finish(),
            Param::Json(v) => f.debug_tuple("Json").field(v).finish(),
            Param::Array(a) => {
                let mut list = f.debug_list();
                for i in 0..a.len() {
                    list.entry(&a.get(i));
                }
                list.finish()
            }
        }
    }
}

/// Values compare as what they render to, so `I32(1) != I64(1)` (different
/// variants) and arrays compare element by element.
impl PartialEq for Param<'_> {
    fn eq(&self, other: &Self) -> bool {
        use Param::*;
        match (self, other) {
            (Null, Null) => true,
            (Bool(a), Bool(b)) => a == b,
            (I16(a), I16(b)) => a == b,
            (I32(a), I32(b)) => a == b,
            (I64(a), I64(b)) => a == b,
            (Text(a), Text(b)) => a == b,
            (Bytes(a), Bytes(b)) => a == b,
            (U64(a), U64(b)) => a == b,
            (I128(a), I128(b)) => a == b,
            (F32(a), F32(b)) => a.to_bits() == b.to_bits(),
            (F64(a), F64(b)) => a.to_bits() == b.to_bits(),
            (Numeric(a), Numeric(b)) => a == b,
            (Uuid(a), Uuid(b)) => a == b,
            (Timestamp(a), Timestamp(b)) => a == b,
            (Date(a), Date(b)) => a == b,
            (Time(a), Time(b)) => a == b,
            (Interval(a), Interval(b)) => a == b,
            (Json(a), Json(b)) => a == b,
            (Array(a), Array(b)) => {
                a.len() == b.len() && (0..a.len()).all(|i| a.get(i) == b.get(i))
            }
            _ => false,
        }
    }
}

impl<'a> From<&'a str> for Param<'a> {
    fn from(s: &'a str) -> Self {
        Param::Text(s)
    }
}

impl<'a> From<&'a [u8]> for Param<'a> {
    fn from(b: &'a [u8]) -> Self {
        Param::Bytes(b)
    }
}

macro_rules! param_from {
    ($($t:ty => $variant:ident),* $(,)?) => {$(
        impl From<$t> for Param<'_> {
            fn from(v: $t) -> Self {
                Param::$variant(v)
            }
        }
    )*};
}
param_from!(
    i64 => I64, i32 => I32, i16 => I16, bool => Bool, u64 => U64, i128 => I128, f32 => F32, f64 => F64,
    Uuid => Uuid, Timestamp => Timestamp, Date => Date, Time => Time, Interval => Interval,
);

impl<'a, T: Into<Param<'a>>> From<Option<T>> for Param<'a> {
    fn from(v: Option<T>) -> Self {
        v.map_or(Param::Null, Into::into)
    }
}

// ------------------------------------------------------------------ literals

/// `s` as a single-quoted string literal: `'` doubled, everything else as it
/// is. A backslash is an ordinary byte in such a literal (only `E'…'` reads
/// escapes), so nothing else needs escaping.
///
/// Refuses a NUL: a text value cannot hold one, and the server would store
/// something other than what was given.
pub fn quote_text(s: &str) -> Result<String> {
    let mut out = String::with_capacity(s.len() + 2);
    push_text(&mut out, s)?;
    Ok(out)
}

/// `bytes` as a `bytea` hex literal, `'\x…'` with lower-case digits.
pub fn quote_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2 + 4);
    push_bytes(&mut out, bytes);
    out
}

/// `name` as an identifier: bare when it is a plain lower-case name that is
/// not a keyword (`users`, `created_at`), otherwise double-quoted with `"`
/// doubled (`"Order"`, `"user"`). Refuses an empty name or one holding a NUL.
pub fn quote_ident(name: &str) -> Result<String> {
    let mut out = String::with_capacity(name.len() + 2);
    push_ident(&mut out, name)?;
    Ok(out)
}

pub fn push_ident(out: &mut String, name: &str) -> Result<()> {
    if name.is_empty() || name.contains('\0') {
        return Err(Error::Param(format!("{name:?} is not a usable identifier")));
    }
    let plain = name
        .bytes()
        .enumerate()
        .all(|(i, b)| b == b'_' || b.is_ascii_lowercase() || (i > 0 && b.is_ascii_digit()));
    if plain && !is_keyword(name) {
        out.push_str(name);
        return Ok(());
    }
    out.push('"');
    for c in name.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
    Ok(())
}

/// Words that cannot stand bare as a column or table name: PostgreSQL's
/// reserved keywords, plus the few this engine's parsers read as keywords.
fn is_keyword(name: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "between",
        "both", "by", "case", "cast", "check", "collate", "column", "constraint", "create",
        "current_date", "current_role", "current_time", "current_timestamp", "current_user",
        "default", "deferrable", "delete", "desc", "distinct", "do", "else", "end", "except",
        "exists", "false", "fetch", "filter", "for", "foreign", "from", "grant", "group", "having",
        "ilike", "in", "index", "initially", "inner", "insert", "intersect", "into", "is", "join",
        "key", "lateral", "leading", "left", "like", "limit", "localtime", "localtimestamp", "not",
        "null", "offset", "on", "only", "or", "order", "outer", "partition", "placing", "primary",
        "references", "returning", "right", "select", "session_user", "set", "some", "symmetric",
        "table", "then", "to", "trailing", "true", "union", "unique", "update", "user", "using",
        "values", "variadic", "when", "where", "window", "with",
    ];
    KEYWORDS.binary_search(&name).is_ok()
}

pub(crate) fn push_text(out: &mut String, s: &str) -> Result<()> {
    if s.contains('\0') {
        return Err(Error::Param(
            "a text value cannot contain a NUL character".to_string(),
        ));
    }
    out.reserve(s.len() + 2);
    out.push('\'');
    let mut start = 0;
    for (at, _) in s.match_indices('\'') {
        out.push_str(&s[start..=at]);
        out.push('\'');
        start = at + 1;
    }
    out.push_str(&s[start..]);
    out.push('\'');
    Ok(())
}

fn push_hex(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.reserve(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
}

fn push_bytes(out: &mut String, bytes: &[u8]) {
    out.reserve(bytes.len() * 2 + 4);
    out.push_str("'\\x");
    push_hex(out, bytes);
    out.push('\'');
}

/// A number, parenthesised when negative. Spliced after a `-` (`x-$1`), a bare
/// `-3` would make `x--3`, and `--` starts a comment that swallows the rest of
/// the line, the `WHERE` clause included.
fn push_int(out: &mut String, v: i64) {
    if v < 0 {
        let _ = write!(out, "({v})");
    } else {
        let _ = write!(out, "{v}");
    }
}

/// The shortest text that reads back as the same float (`Debug` is that for
/// Rust's floats: `0.1`, `1e-7`, `100.0`), parenthesised when negative.
/// Infinities and NaN have no numeric literal and go as the strings
/// PostgreSQL's float input reads.
fn push_float(out: &mut String, text: String, negative: bool, finite: bool, nan: bool) {
    if nan {
        out.push_str("'NaN'");
    } else if !finite {
        out.push_str(if negative { "'-Infinity'" } else { "'Infinity'" });
    } else if negative {
        let _ = write!(out, "({text})");
    } else {
        out.push_str(&text);
    }
}

fn check_year(year: i64, what: &str) -> Result<()> {
    if (1..=9999).contains(&year) {
        Ok(())
    } else {
        Err(Error::Param(format!(
            "a {what} in year {year} has no literal here: years 1 to 9999 are written"
        )))
    }
}

pub fn push_param(out: &mut String, param: &Param<'_>) -> Result<()> {
    match *param {
        Param::Null => out.push_str("NULL"),
        Param::Bool(true) => out.push_str("true"),
        Param::Bool(false) => out.push_str("false"),
        Param::I16(v) => push_int(out, v.into()),
        Param::I32(v) => push_int(out, v.into()),
        Param::I64(v) => push_int(out, v),
        Param::U64(v) => {
            let _ = write!(out, "{v}");
        }
        Param::I128(v) => {
            if v < 0 {
                let _ = write!(out, "({v})");
            } else {
                let _ = write!(out, "{v}");
            }
        }
        // Typed: a bare `0.1` is a numeric literal, which a `real` column's
        // 0.1f32 (widened, 0.10000000149…) never equals, as in PostgreSQL.
        Param::F32(v) => {
            push_float(
                out,
                format!("{v:?}"),
                v.is_sign_negative(),
                v.is_finite(),
                v.is_nan(),
            );
            out.push_str("::real");
        }
        Param::F64(v) => push_float(
            out,
            format!("{v:?}"),
            v.is_sign_negative(),
            v.is_finite(),
            v.is_nan(),
        ),
        Param::Numeric(n) => {
            if !crate::types::is_decimal(n) {
                return Err(Error::Param(format!("{n:?} is not a number")));
            }
            if n.eq_ignore_ascii_case("nan") {
                out.push_str("'NaN'");
            } else if n.starts_with('-') {
                let _ = write!(out, "({n})");
            } else {
                out.push_str(n.strip_prefix('+').unwrap_or(n));
            }
        }
        Param::Text(s) | Param::Json(s) => push_text(out, s)?,
        Param::Bytes(b) => push_bytes(out, b),
        Param::Uuid(u) => {
            let _ = write!(out, "'{u}'");
        }
        Param::Timestamp(t) => {
            check_year(t.date().ymd().0, "timestamp")?;
            let _ = write!(out, "'{t}+00'");
        }
        Param::Date(d) => {
            check_year(d.ymd().0, "date")?;
            let _ = write!(out, "'{d}'");
        }
        Param::Time(t) => {
            let _ = write!(out, "'{t}'");
        }
        Param::Interval(iv) => {
            let _ = write!(out, "'{iv}'");
        }
        Param::Array(a) => {
            let mut literal = String::with_capacity(2 + a.len() * 4);
            push_array_body(&mut literal, &a)?;
            push_text(out, &literal)?;
        }
    }
    Ok(())
}

/// The inside of an array literal, `{e1,e2,…}`: numbers bare, `NULL` for a
/// null, anything else double-quoted with `\` and `"` backslash-escaped, a
/// nested array as its own braces. The caller quotes the whole as text.
pub(crate) fn push_array_body(out: &mut String, array: &ArrayRef<'_>) -> Result<()> {
    out.push('{');
    for i in 0..array.len() {
        if i > 0 {
            out.push(',');
        }
        let element = array.get(i);
        match element {
            Param::Null => out.push_str("NULL"),
            Param::Bool(b) => out.push_str(if b { "t" } else { "f" }),
            Param::I16(v) => {
                let _ = write!(out, "{v}");
            }
            Param::I32(v) => {
                let _ = write!(out, "{v}");
            }
            Param::I64(v) => {
                let _ = write!(out, "{v}");
            }
            Param::U64(v) => {
                let _ = write!(out, "{v}");
            }
            Param::I128(v) => {
                let _ = write!(out, "{v}");
            }
            Param::F32(v) => push_array_float(out, v.into(), format!("{v:?}")),
            Param::F64(v) => push_array_float(out, v, format!("{v:?}")),
            Param::Numeric(n) => {
                if !crate::types::is_decimal(n) {
                    return Err(Error::Param(format!("{n:?} is not a number")));
                }
                out.push_str(n);
            }
            Param::Array(inner) => push_array_body(out, &inner)?,
            other => {
                let mut text = String::new();
                match other {
                    Param::Text(s) | Param::Json(s) => text.push_str(s),
                    Param::Bytes(b) => {
                        text.push_str("\\x");
                        push_hex(&mut text, b);
                    }
                    Param::Uuid(u) => text = u.to_string(),
                    Param::Timestamp(t) => {
                        check_year(t.date().ymd().0, "timestamp")?;
                        text = format!("{t}+00");
                    }
                    Param::Date(d) => {
                        check_year(d.ymd().0, "date")?;
                        text = d.to_string();
                    }
                    Param::Time(t) => text = t.to_string(),
                    Param::Interval(iv) => text = iv.to_string(),
                    _ => unreachable!("every other variant is handled above"),
                }
                out.push('"');
                for c in text.chars() {
                    if c == '"' || c == '\\' {
                        out.push('\\');
                    }
                    out.push(c);
                }
                out.push('"');
            }
        }
    }
    out.push('}');
    Ok(())
}

fn push_array_float(out: &mut String, v: f64, text: String) {
    if v.is_nan() {
        out.push_str("NaN");
    } else if v.is_infinite() {
        out.push_str(if v < 0.0 { "-Infinity" } else { "Infinity" });
    } else {
        out.push_str(&text);
    }
}

// ------------------------------------------------------------------ scanning

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

/// What the scanner stops at: a placeholder, or a `;` outside any quote or
/// comment.
enum Event<'s> {
    Placeholder {
        start: usize,
        end: usize,
        digits: &'s str,
    },
    Semicolon(usize),
}

/// Walks `sql` as the server's lexer would split it, skipping strings
/// (`E'…'` with its backslash escapes included), quoted identifiers,
/// dollar-quoted strings and comments.
struct Scanner<'s> {
    sql: &'s str,
    at: usize,
}

impl<'s> Iterator for Scanner<'s> {
    type Item = Event<'s>;

    fn next(&mut self) -> Option<Event<'s>> {
        let bytes = self.sql.as_bytes();
        while self.at < bytes.len() {
            let i = self.at;
            match bytes[i] {
                b'\'' => self.at = skip_quoted(bytes, i, b'\'', escape_string_at(bytes, i)),
                b'"' => self.at = skip_quoted(bytes, i, b'"', false),
                b'-' if bytes.get(i + 1) == Some(&b'-') => self.at = skip_line_comment(bytes, i),
                b'/' if bytes.get(i + 1) == Some(&b'*') => self.at = skip_block_comment(bytes, i),
                b';' => {
                    self.at = i + 1;
                    return Some(Event::Semicolon(i));
                }
                b'$' if i == 0 || !is_ident_byte(bytes[i - 1]) => {
                    let digits = bytes[i + 1..]
                        .iter()
                        .take_while(|b| b.is_ascii_digit())
                        .count();
                    if digits > 0 {
                        self.at = i + 1 + digits;
                        return Some(Event::Placeholder {
                            start: i,
                            end: i + 1 + digits,
                            digits: &self.sql[i + 1..i + 1 + digits],
                        });
                    }
                    self.at = dollar_quote_end(bytes, i).unwrap_or(i + 1);
                }
                _ => self.at = i + 1,
            }
        }
        None
    }
}

fn scan(sql: &str) -> Scanner<'_> {
    Scanner { sql, at: 0 }
}

/// Whether only whitespace, comments and further semicolons follow `from`.
fn only_trivia_after(bytes: &[u8], from: usize) -> bool {
    let mut i = from;
    while i < bytes.len() {
        match bytes[i] {
            b if b.is_ascii_whitespace() || b == b';' => i += 1,
            b'-' if bytes.get(i + 1) == Some(&b'-') => i = skip_line_comment(bytes, i),
            b'/' if bytes.get(i + 1) == Some(&b'*') => i = skip_block_comment(bytes, i),
            _ => return false,
        }
    }
    true
}

fn second_statement(at: usize) -> Error {
    Error::Unsupported(format!(
        "the text holds more than one statement (a second one starts after the ';' at byte \
         {at}). The native protocol runs only the first of an INSERT batch and silently drops \
         the rest, so this client sends one statement at a time; for many rows use one \
         multi-row INSERT"
    ))
}

/// The first word of the statement, upper-cased, past leading whitespace and
/// comments.
fn first_word(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut i = 0;
    loop {
        match bytes.get(i) {
            Some(b) if b.is_ascii_whitespace() || *b == b'(' => i += 1,
            Some(b'-') if bytes.get(i + 1) == Some(&b'-') => i = skip_line_comment(bytes, i),
            Some(b'/') if bytes.get(i + 1) == Some(&b'*') => i = skip_block_comment(bytes, i),
            _ => break,
        }
    }
    let word: String = sql[i..]
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect();
    word.to_ascii_uppercase()
}

/// Refuses what the native protocol cannot run as written, before it is sent:
/// a second statement (see [`second_statement`]), and transaction control,
/// which the native protocol does not recognise: every statement there
/// commits on its own.
pub fn check_statement(sql: &str) -> Result<()> {
    let bytes = sql.as_bytes();
    for event in scan(sql) {
        if let Event::Semicolon(at) = event
            && !only_trivia_after(bytes, at + 1)
        {
            return Err(second_statement(at));
        }
    }
    check_not_transaction_control(sql)
}

fn check_not_transaction_control(sql: &str) -> Result<()> {
    let word = first_word(sql);
    if matches!(
        word.as_str(),
        "BEGIN" | "START" | "COMMIT" | "END" | "ROLLBACK" | "ABORT" | "SAVEPOINT" | "RELEASE"
    ) {
        return Err(Error::Unsupported(format!(
            "{word}: the native protocol has no transactions; each statement commits on its own. \
             Put related writes in one statement (a multi-row INSERT, an UPDATE … WHERE, \
             INSERT … ON CONFLICT), or use the pgwire listener for a transaction block"
        )));
    }
    Ok(())
}

/// `sql` with every `$n` placeholder replaced by the literal for `params[n-1]`.
///
/// Placeholders are recognised where the server's lexer would see a token:
/// not inside a single-quoted string (`E'…'` with its backslash escapes
/// included), a double-quoted identifier, a dollar-quoted string, or a `--` or
/// `/* */` comment, and not as the tail of an identifier (`a$1`). A
/// placeholder numbered past the end of `params`, or `$0`, is an error; a
/// parameter no placeholder uses is not.
///
/// A second statement after a `;` is an error, as is transaction control
/// (see [`check_statement`]).
pub fn render(sql: &str, params: &[Param<'_>]) -> Result<String> {
    render_with(sql, params.len(), |i, out| push_param(out, &params[i]))
}

/// [`render`] for values given as [`ToSql`], as [`params!`](crate::params)
/// builds them.
pub fn render_dyn(sql: &str, params: &[&dyn ToSql]) -> Result<String> {
    render_with(sql, params.len(), |i, out| {
        push_param(out, &params[i].to_param())
    })
}

pub(crate) fn render_with(
    sql: &str,
    count: usize,
    mut write: impl FnMut(usize, &mut String) -> Result<()>,
) -> Result<String> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + count * 8);
    let mut copied = 0usize;
    for event in scan(sql) {
        match event {
            Event::Semicolon(at) => {
                if !only_trivia_after(bytes, at + 1) {
                    return Err(second_statement(at));
                }
            }
            Event::Placeholder { start, end, digits } => {
                if count == 0 {
                    // Still an error: a `$1` with nothing to fill it is a
                    // mistake to report, not a statement to send.
                    return Err(Error::Param(format!(
                        "${digits} has no value: the statement was given no parameters"
                    )));
                }
                let n: usize = digits
                    .parse()
                    .map_err(|_| Error::Param(format!("${digits} is not a usable number")))?;
                if n == 0 || n > count {
                    return Err(Error::Param(format!(
                        "${n} has no value: {count} parameter(s) were given"
                    )));
                }
                out.push_str(&sql[copied..start]);
                write(n - 1, &mut out)?;
                copied = end;
            }
        }
    }
    check_not_transaction_control(sql)?;
    out.push_str(&sql[copied..]);
    Ok(out)
}

/// Whether the quote at `quote` opens an `E'…'` string, whose backslashes
/// escape the next character: an `e` or `E` right before it that is not
/// itself the end of a longer identifier.
fn escape_string_at(bytes: &[u8], quote: usize) -> bool {
    quote >= 1
        && matches!(bytes[quote - 1], b'e' | b'E')
        && (quote == 1 || !is_ident_byte(bytes[quote - 2]))
}

/// The index just past the closing `delim` of the quoted run starting at
/// `start`. A doubled delimiter is an escaped one. An unterminated run ends at
/// the end of the text: nothing after an unclosed quote is a placeholder.
fn skip_quoted(bytes: &[u8], start: usize, delim: u8, backslash_escapes: bool) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        let b = bytes[i];
        if backslash_escapes && b == b'\\' {
            i += 2;
            continue;
        }
        if b == delim {
            if bytes.get(i + 1) == Some(&delim) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_line_comment(bytes: &[u8], start: usize) -> usize {
    match bytes[start..].iter().position(|&b| b == b'\n') {
        Some(n) => start + n + 1,
        None => bytes.len(),
    }
}

/// Block comments nest, as PostgreSQL's do.
fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut depth = 0usize;
    let mut i = start;
    while i + 1 < bytes.len() {
        if bytes[i] == b'/' && bytes[i + 1] == b'*' {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            depth -= 1;
            i += 2;
            if depth == 0 {
                return i;
            }
        } else {
            i += 1;
        }
    }
    bytes.len()
}

/// The index past a dollar-quoted string (`$$…$$`, `$tag$…$tag$`) opening at
/// `start`, or `None` if no tag opens there.
fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<usize> {
    let rest = &bytes[start + 1..];
    let tag_len = rest.iter().position(|&b| b == b'$')?;
    let tag = &rest[..tag_len];
    let valid_tag = tag
        .first()
        .is_none_or(|b| b.is_ascii_alphabetic() || *b == b'_' || *b >= 0x80)
        && tag
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80);
    if !valid_tag {
        return None;
    }
    let delimiter = &bytes[start..start + tag_len + 2];
    let body = start + tag_len + 2;
    let close = bytes[body..]
        .windows(delimiter.len())
        .position(|w| w == delimiter);
    Some(match close {
        Some(n) => body + n + delimiter.len(),
        None => bytes.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(sql: &str, params: &[Param<'_>]) -> String {
        render(sql, params).unwrap()
    }

    #[test]
    fn values_become_literals() {
        assert_eq!(
            r(
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    Param::Null,
                    Param::Bool(true),
                    Param::I16(5),
                    Param::I32(-6),
                    Param::I64(i64::MIN),
                    Param::Text("it's"),
                    Param::Bytes(&[0x00, 0xff, 0x10]),
                ]
            ),
            "VALUES (NULL, true, 5, (-6), (-9223372036854775808), 'it''s', '\\x00ff10')"
        );
    }

    #[test]
    fn the_new_types_become_literals_the_server_reads() {
        let ts: Timestamp = "2026-11-16 12:34:56.789".parse().unwrap();
        let uuid: Uuid = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".parse().unwrap();
        assert_eq!(
            r(
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                &[
                    Param::U64(u64::MAX),
                    Param::F64(1e-7),
                    Param::F64(-2.5),
                    Param::F32(0.1),
                    Param::F64(f64::NAN),
                    Param::F64(f64::NEG_INFINITY),
                    Param::Numeric("-12.340"),
                    Param::Uuid(uuid),
                    Param::Timestamp(ts),
                    Param::Interval(Interval::new(14, -3, 4_005_000_006)),
                    Param::Json("{\"a\":'x'}"),
                ]
            ),
            "VALUES (18446744073709551615, 1e-7, (-2.5), 0.1::real, 'NaN', '-Infinity', (-12.340), \
             'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2026-11-16 12:34:56.789+00', \
             '14 mons -3 days 4005000006 microseconds', '{\"a\":''x''}')"
        );
        assert_eq!(r("SELECT $1", &[Param::F64(-0.0)]), "SELECT (-0.0)");
        assert_eq!(r("SELECT $1", &[Param::F64(100.0)]), "SELECT 100.0");
        assert!(render("SELECT $1", &[Param::Numeric("1; DROP")]).is_err());
        assert!(render("SELECT $1", &[Param::Numeric("")]).is_err());
        assert_eq!(r("SELECT $1", &[Param::Numeric("+5")]), "SELECT 5");
        let ancient = Timestamp::from_micros(-70_000_000_000_000_000);
        assert!(render("SELECT $1", &[Param::Timestamp(ancient)]).is_err());
    }

    #[test]
    fn arrays_quote_their_elements() {
        let texts = ["a\\b", "c,d", "{e}", "NULL", "", "q\"z", "it's"];
        assert_eq!(
            r("SELECT $1", &[Param::Array(ArrayRef::new(&texts))]),
            r#"SELECT '{"a\\b","c,d","{e}","NULL","","q\"z","it''s"}'"#
        );
        let ints = vec![Some(1i64), None, Some(-3)];
        assert_eq!(r("SELECT $1", &[Param::Array(ArrayRef::new(&ints))]), "SELECT '{1,NULL,-3}'");
        let nested = [vec![1i32, 2], vec![3, 4]];
        assert_eq!(r("SELECT $1", &[Param::Array(ArrayRef::new(&nested))]), "SELECT '{{1,2},{3,4}}'");
        let empty: [i64; 0] = [];
        assert_eq!(r("SELECT $1", &[Param::Array(ArrayRef::new(&empty))]), "SELECT '{}'");
        let floats = [f64::NAN, 1.5, f64::INFINITY];
        assert_eq!(r("SELECT $1", &[Param::Array(ArrayRef::new(&floats))]), "SELECT '{NaN,1.5,Infinity}'");
        assert!(render("SELECT $1", &[Param::Array(ArrayRef::new(&["a\0"]))]).is_err());
    }

    #[test]
    fn identifiers_are_bare_only_when_plain() {
        assert_eq!(quote_ident("users").unwrap(), "users");
        assert_eq!(quote_ident("created_at2").unwrap(), "created_at2");
        assert_eq!(quote_ident("user").unwrap(), "\"user\"");
        assert_eq!(quote_ident("Order").unwrap(), "\"Order\"");
        assert_eq!(quote_ident("a\"b").unwrap(), "\"a\"\"b\"");
        assert_eq!(quote_ident("2fa").unwrap(), "\"2fa\"");
        assert!(quote_ident("").is_err());
        let mut sorted = ["user", "order", "select", "all", "with"];
        sorted.sort();
        assert!(sorted.iter().all(|w| is_keyword(w)));
    }

    #[test]
    fn the_keyword_list_is_sorted() {
        // `is_keyword` binary-searches it.
        assert!(is_keyword("all") && is_keyword("with") && is_keyword("user"));
        assert!(!is_keyword("users") && !is_keyword("name"));
    }

    #[test]
    fn a_second_statement_is_refused() {
        for sql in [
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)",
            "SELECT 1;SELECT 2",
            "UPDATE t SET a = 1; -- x\n DELETE FROM t",
        ] {
            assert!(matches!(render(sql, &[]), Err(Error::Unsupported(_))), "{sql}");
            assert!(matches!(check_statement(sql), Err(Error::Unsupported(_))), "{sql}");
        }
        for sql in [
            "INSERT INTO t VALUES (1);",
            "INSERT INTO t VALUES (1) ; ; -- done\n /* really */ ",
            "INSERT INTO t VALUES ('a;b')",
            "SELECT \"x;y\" FROM t -- ; DROP TABLE t",
            "SELECT $$;$$, E'\\';' FROM t",
        ] {
            assert!(render(sql, &[]).is_ok(), "{sql}");
            assert!(check_statement(sql).is_ok(), "{sql}");
        }
        // A value holding a `;` is a literal, not a second statement.
        assert_eq!(
            r("INSERT INTO t VALUES ($1)", &[Param::Text("1); DROP TABLE t; --")]),
            "INSERT INTO t VALUES ('1); DROP TABLE t; --')"
        );
    }

    #[test]
    fn transaction_control_is_refused() {
        for sql in ["BEGIN", "  begin;", "START TRANSACTION", "commit", "/* c */ ROLLBACK", "SAVEPOINT a"] {
            assert!(matches!(check_statement(sql), Err(Error::Unsupported(_))), "{sql}");
        }
        for sql in ["SELECT begin FROM t", "INSERT INTO commits VALUES (1)", "VACUUM t"] {
            assert!(check_statement(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn a_negative_after_a_minus_is_not_a_comment() {
        let sql = r("SELECT x-$1 FROM t WHERE k = 1", &[Param::I64(-3)]);
        assert_eq!(sql, "SELECT x-(-3) FROM t WHERE k = 1");
        assert!(!sql.contains("--"));
        let sql = r("SELECT x-$1 FROM t", &[Param::F64(-3.5)]);
        assert!(!sql.contains("--"), "{sql}");
        let sql = r("SELECT x-$1 FROM t", &[Param::Numeric("-3")]);
        assert!(!sql.contains("--"), "{sql}");
    }

    #[test]
    fn backslashes_and_comment_starters_stay_inside_the_literal() {
        assert_eq!(
            r(
                "SELECT $1",
                &[Param::Text("a\\b -- c /* d */ '; DROP TABLE t; --")]
            ),
            "SELECT 'a\\b -- c /* d */ ''; DROP TABLE t; --'"
        );
    }

    #[test]
    fn placeholders_in_strings_identifiers_and_comments_are_left_alone() {
        let sql =
            "SELECT '$1', \"$1\", E'\\'$1', $$ $1 $$, $q$ $1 $q$ -- $1\n /* $1 /* $1 */ $1 */ $1";
        assert_eq!(
            r(sql, &[Param::I64(9)]),
            "SELECT '$1', \"$1\", E'\\'$1', $$ $1 $$, $q$ $1 $q$ -- $1\n /* $1 /* $1 */ $1 */ 9"
        );
    }

    #[test]
    fn a_doubled_quote_does_not_end_the_string() {
        assert_eq!(
            r("SELECT 'a''$1', $1", &[Param::I64(1)]),
            "SELECT 'a''$1', 1"
        );
    }

    #[test]
    fn an_identifier_tail_is_not_a_placeholder() {
        assert_eq!(r("SELECT a$1, $1", &[Param::I64(2)]), "SELECT a$1, 2");
    }

    #[test]
    fn multi_digit_placeholders_are_one_number() {
        let params: Vec<Param<'_>> = (1..=12).map(|n| Param::I64(n * 100)).collect();
        assert_eq!(r("SELECT $1, $12, $10", &params), "SELECT 100, 1200, 1000");
    }

    #[test]
    fn a_missing_value_is_an_error() {
        assert!(render("SELECT $2", &[Param::I64(1)]).is_err());
        assert!(render("SELECT $0", &[Param::I64(1)]).is_err());
        assert!(render("SELECT $1", &[]).is_err());
        assert_eq!(render("SELECT '$1'", &[]).unwrap(), "SELECT '$1'");
    }

    #[test]
    fn a_nul_is_refused() {
        assert!(render("SELECT $1", &[Param::Text("a\0b")]).is_err());
        assert!(quote_text("a\0").is_err());
    }

    #[test]
    fn unicode_passes_through() {
        assert_eq!(r("SELECT $1", &[Param::Text("ж€😀'")]), "SELECT 'ж€😀'''");
        assert_eq!(r("SELECT 'ж', $1", &[Param::I64(1)]), "SELECT 'ж', 1");
    }

    #[test]
    fn an_unterminated_quote_hides_the_rest() {
        assert_eq!(
            r("SELECT $1, 'oops $1", &[Param::I64(1)]),
            "SELECT 1, 'oops $1"
        );
    }

    #[test]
    fn dyn_values_render_alike() {
        let name = String::from("o'k");
        let n: Option<i32> = None;
        assert_eq!(
            render_dyn("SELECT $1, $2, $3", &[&name, &n, &7u32]).unwrap(),
            "SELECT 'o''k', NULL, 7"
        );
    }
}
