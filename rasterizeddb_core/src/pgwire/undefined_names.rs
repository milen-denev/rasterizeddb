//! The undefined name PostgreSQL would have reported for a statement the
//! engine refused in its own words.
//!
//! PostgreSQL resolves every name of a statement before it runs any of it,
//! and a name that resolves to nothing is the statement's error: 42703
//! `column "x" does not exist`, 42883 `function f(integer) does not exist`.
//! The engine's readers meet such a name wherever they happen to read it and
//! word it as that reader sees it: `Column 'a + nosuch' not found in schema`
//! (42601), `function 'nosuchfn' is not supported yet` (0A000), `HAVING term
//! 'nosuch > 1' tests no aggregate` (42601), `subquery failed: …` (XX000).
//! A driver tells a stale schema or a missing extension by those two codes.
//!
//! [`diagnose`] reads the failed statement as PostgreSQL's analysis does —
//! `FROM` and its `ON` conditions, the select list, `WHERE`, `HAVING`,
//! `ORDER BY`, `GROUP BY`, each subquery in the scope of the query around
//! it — and reports the first name that is no column of any relation in
//! scope, or the function the refusal named when PostgreSQL has none by that
//! name or with those arguments. It is asked only after a statement has
//! failed, and only of the engine's own wordings of an unknown name; any
//! shape it cannot be sure of (a CTE, a derived table, a view, a function in
//! `FROM`, an alias column list, a set operation's `ORDER BY`) leaves the
//! engine's message as it was.

use crate::core::sql_scan::{
    dollar_quote_end, is_escape_string_quote, is_identifier_byte, quoted_end, skip_comment, string_literal_end,
};

/// The names of PostgreSQL 17's `pg_catalog` functions, one per line, sorted
/// bytewise (`SELECT DISTINCT lower(proname) FROM pg_proc WHERE pronamespace
/// = 'pg_catalog'::regnamespace`).
const PG17_FUNCTION_NAMES: &str = include_str!("pg17_function_names.txt");

/// The constructs PostgreSQL's grammar spells as calls that are no
/// `pg_proc` function, and the grouping words written before a list.
const CALL_CONSTRUCTS: &[&str] = &[
    "array", "coalesce", "cube", "extract", "greatest", "grouping", "json", "json_array", "json_arrayagg",
    "json_exists", "json_object", "json_objectagg", "json_query", "json_scalar", "json_serialize", "json_table",
    "json_value", "least", "merge_action", "nullif", "rollup", "row", "sets", "treat", "trim", "xmlattributes",
    "xmlconcat", "xmlelement", "xmlexists", "xmlforest", "xmlparse", "xmlpi", "xmlroot", "xmlserialize",
];

/// Whether PostgreSQL has a function, or a call-shaped construct, by this
/// (lowercased) name.
pub(super) fn is_postgres_function(name: &str) -> bool {
    // Sorted, so a binary search over the lines, collected once.
    static NAMES: std::sync::OnceLock<Vec<&'static str>> = std::sync::OnceLock::new();
    CALL_CONSTRUCTS.contains(&name)
        || NAMES.get_or_init(|| PG17_FUNCTION_NAMES.lines().collect()).binary_search(&name).is_ok()
}

/// The words that are never a column reference where an expression is read:
/// PostgreSQL's keywords as they occur inside expressions and clauses, and
/// the value keywords. A column so named, written bare, is not checked —
/// which only ever leaves the engine's message in place.
const KEYWORDS: &[&str] = &[
    "all", "and", "any", "array", "as", "asc", "asymmetric", "at", "between", "bigint", "bit", "boolean", "both",
    "by", "case", "cast", "char", "character", "collate", "collation", "cross", "cube", "current", "current_catalog",
    "current_date", "current_role", "current_schema", "current_time", "current_timestamp", "current_user", "date",
    "day", "dec", "decimal", "default", "desc", "distinct", "do", "double", "else", "end", "escape", "except",
    "exclude", "exists", "false", "fetch", "filter", "first", "float", "following", "for", "from", "full", "group",
    "groups", "having", "hour", "ilike", "in", "inner", "int", "integer", "intersect", "interval", "into", "is",
    "isnull", "join", "key", "last", "lateral", "leading", "left", "like", "limit", "localtime", "localtimestamp",
    "minute", "month", "natural", "next", "no", "not", "notnull", "nowait", "null", "nulls", "numeric", "of",
    "offset", "on", "only", "or", "order", "ordinality", "others", "outer", "over", "overlaps", "partition",
    "precision", "preceding", "range", "real", "recursive", "right", "rollup", "row", "rows", "second", "select",
    "session_user", "sets", "share", "similar", "skip", "smallint", "some", "symmetric", "system_user", "table",
    "tablesample", "then", "ties", "time", "timestamp", "to", "trailing", "true", "unbounded", "union", "unknown",
    "update", "user", "using", "values", "varchar", "varying", "when", "where", "window", "with", "within",
    "without", "year", "zone", "placing", "locked", "returning", "excluded", "value", "set",
];

fn is_keyword(word: &str) -> bool {
    KEYWORDS.contains(&word)
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Word,
    Quoted,
    Str,
    Num,
    Param,
    Open,
    Close,
    Comma,
    Dot,
    Cast,
    Op,
}

#[derive(Clone, Debug)]
struct Tok {
    kind: Kind,
    /// A word lowercased, a quoted identifier's name, an operator's or a
    /// number's text; empty for a string.
    text: String,
}

impl Tok {
    fn is_word(&self, word: &str) -> bool {
        self.kind == Kind::Word && self.text == word
    }
    /// A name: a word that is no keyword, or a quoted identifier.
    fn is_name(&self) -> bool {
        (self.kind == Kind::Word && !is_keyword(&self.text)) || self.kind == Kind::Quoted
    }
}

fn tokens(sql: &str) -> Option<Vec<Tok>> {
    let bytes = sql.as_bytes();
    let mut out: Vec<Tok> = Vec::new();
    let mut i = 0;
    let push = |out: &mut Vec<Tok>, kind, text: &str| out.push(Tok { kind, text: text.to_string() });
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if let Some(end) = skip_comment(bytes, i) {
            i = end;
            continue;
        }
        match b {
            b'\'' => {
                // `E'…'`, `B'…'`, `X'…'`, `N'…'`: the prefix is part of the string.
                if i >= 1
                    && matches!(bytes[i - 1], b'E' | b'e' | b'B' | b'b' | b'X' | b'x' | b'N' | b'n')
                    && out.last().is_some_and(|t| t.kind == Kind::Word && t.text.len() == 1)
                {
                    out.pop();
                }
                i = string_literal_end(bytes, i, is_escape_string_quote(bytes, i));
                push(&mut out, Kind::Str, "");
            }
            b'"' => {
                let end = quoted_end(bytes, i, b'"');
                let inner = sql.get(i + 1..end.saturating_sub(1).max(i + 1))?.replace("\"\"", "\"");
                push(&mut out, Kind::Quoted, &inner);
                i = end;
            }
            b'$' if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                i += 1;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                push(&mut out, Kind::Param, "");
            }
            b'$' => {
                i = dollar_quote_end(bytes, i)?;
                push(&mut out, Kind::Str, "");
            }
            b'0'..=b'9' => {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric()
                        || bytes[i] == b'.' && bytes.get(i + 1).is_none_or(|n| *n != b'.')
                        || matches!(bytes[i], b'+' | b'-') && matches!(bytes[i - 1], b'e' | b'E'))
                {
                    i += 1;
                }
                push(&mut out, Kind::Num, &sql[start..i]);
            }
            b'.' if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                let start = i;
                i += 1;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
                    i += 1;
                }
                push(&mut out, Kind::Num, &sql[start..i]);
            }
            b'(' | b'[' => {
                push(&mut out, Kind::Open, if b == b'(' { "(" } else { "[" });
                i += 1;
            }
            b')' | b']' => {
                push(&mut out, Kind::Close, if b == b')' { ")" } else { "]" });
                i += 1;
            }
            b',' => {
                push(&mut out, Kind::Comma, ",");
                i += 1;
            }
            b'.' => {
                push(&mut out, Kind::Dot, ".");
                i += 1;
            }
            b':' if bytes.get(i + 1) == Some(&b':') => {
                push(&mut out, Kind::Cast, "::");
                i += 2;
            }
            b';' => break,
            _ if is_identifier_byte(b) => {
                let start = i;
                while i < bytes.len() && (is_identifier_byte(bytes[i]) || bytes[i] == b'$') {
                    i += 1;
                }
                push(&mut out, Kind::Word, &sql[start..i].to_lowercase());
            }
            _ => {
                let start = i;
                while i < bytes.len() && b"+-*/<>=~!@#%^&|`?:".contains(&bytes[i]) {
                    i += 1;
                }
                if i == start {
                    i += 1;
                }
                push(&mut out, Kind::Op, &sql[start..i]);
            }
        }
    }
    Some(out)
}

/// One relation of a `FROM` list: the names a reference qualifies it by and
/// its columns, each with PostgreSQL's name for its type.
struct Relation {
    qualifier: String,
    columns: Vec<(String, String)>,
}

/// What reading a statement found.
enum Stop {
    /// The error PostgreSQL reports.
    Undefined(&'static str, String),
    /// A shape this reader does not follow; the engine's message stands.
    Unsure,
}

/// The function a refusal named, and how to tell its failing call.
#[derive(Clone, Debug)]
pub(super) struct NamedFunction {
    pub name: String,
    /// Whether PostgreSQL has a function by the name at all: then only a
    /// call with the arity the refusal rules out (`Some(n)`: not `n`
    /// arguments) or, with `None`, the first call, is the one reported.
    pub builtin: bool,
    pub arity: Option<usize>,
}

/// Which name a failure message reports as unknown, when it is one of the
/// engine's own wordings of an unknown name: `Some(None)` for a column
/// (or anything named in an expression), `Some(Some(f))` for a function.
pub(super) fn subject(message: &str) -> Option<Option<NamedFunction>> {
    let named = |name: &str, arity: Option<usize>| {
        let name = name.trim().trim_matches('"').to_ascii_lowercase();
        Some(Some(NamedFunction { builtin: is_postgres_function(&name), name, arity }))
    };
    if let Some(name) = message.strip_prefix("function '").and_then(|r| r.strip_suffix("' is not supported yet")) {
        return named(name, None).filter(|f| f.as_ref().is_some_and(|f| !f.builtin));
    }
    if let Some((name, rest)) = message.split_once(" takes exactly ") {
        let arity = match rest {
            "one argument" => 1,
            "two arguments" => 2,
            _ => return None,
        };
        return named(name, Some(arity));
    }
    if let Some(name) = message.strip_suffix("() takes no arguments") {
        return named(name, Some(0));
    }
    if let Some((name, _)) = message.split_once(" is not defined for ")
        && !name.contains(' ')
        && !name.starts_with('\'')
    {
        return named(name, None);
    }
    if let Some(rest) = message.strip_prefix("unknown column or unsupported keyword '")
        && let Some((name, _)) = rest.split_once('\'')
    {
        // A call whose name the WHERE reader took for a column.
        if !is_postgres_function(&name.to_ascii_lowercase()) {
            return Some(Some(NamedFunction { name: name.to_ascii_lowercase(), builtin: false, arity: None }));
        }
        return Some(None);
    }
    // `public.nosuchfn(1)` read as a column.
    if let Some(name) = message.strip_prefix("column \"").and_then(|r| r.strip_suffix("\" does not exist"))
        && let Some(open) = name.find('(')
    {
        let name = name[..open].rsplit('.').next().unwrap_or("");
        return named(name, None).filter(|f| f.as_ref().is_some_and(|f| !f.builtin));
    }
    const COLUMN_WORDINGS: &[&str] = &[
        " not found in schema",
        "is neither a column nor a value",
        "unknown identifier '",
        "uses CASE WHEN, which is not supported yet",
        "HAVING term '",
        "must appear in the GROUP BY clause",
        "is ambiguous",
        "could not be read as an expression",
        "subquery failed: ",
        "has no FROM clause",
        "unknown column or unsupported keyword",
    ];
    if COLUMN_WORDINGS.iter().any(|w| message.contains(w)) {
        return Some(None);
    }
    // A qualified name quoted whole: `column "f.nosuch" does not exist`.
    if let Some(name) = message.strip_prefix("column \"").and_then(|r| r.strip_suffix("\" does not exist"))
        && name.contains('.')
    {
        return Some(None);
    }
    None
}

/// The error PostgreSQL reports for `sql`, whose failure `function` (from
/// [`subject`]) describes, as `(SQLSTATE, message)`; `None` when the reader
/// finds no undefined name or is unsure. `columns_of` gives a table's
/// columns and their type names, `None` for anything that is not a table.
pub(super) fn diagnose(
    sql: &str,
    function: Option<&NamedFunction>,
    columns_of: &dyn Fn(&str) -> Option<Vec<(String, String)>>,
) -> Option<(&'static str, String)> {
    let toks = tokens(sql)?;
    let first = toks.first().filter(|t| t.kind == Kind::Word)?;
    let read = |report: bool| {
        let reader = Reader { toks: &toks, report, function, columns_of };
        match first.text.as_str() {
            "select" => Some(reader.select(0, toks.len(), &[])),
            "update" => Some(reader.update()),
            "delete" => Some(reader.delete()),
            "insert" => Some(reader.insert()),
            _ => None,
        }
    };
    if read(false)?.is_err() {
        return None;
    }
    match read(true)? {
        Err(Stop::Undefined(code, message)) => Some((code, message)),
        _ => None,
    }
}

struct Reader<'a> {
    toks: &'a [Tok],
    /// The first pass reports nothing: it only looks for text PostgreSQL's
    /// parser would refuse before any name is resolved (`id HAVNG id`),
    /// whose syntax error stands.
    report: bool,
    function: Option<&'a NamedFunction>,
    columns_of: &'a dyn Fn(&str) -> Option<Vec<(String, String)>>,
}

type Scope = Vec<Relation>;

impl Reader<'_> {
    /// The index of the token closing the bracket opened at `open`.
    fn close_of(&self, open: usize) -> Option<usize> {
        let mut depth = 0usize;
        for (i, t) in self.toks.iter().enumerate().skip(open) {
            match t.kind {
                Kind::Open => depth += 1,
                Kind::Close => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// The depth-0 positions in `start..end` of tokens `pick` accepts.
    fn top_level(&self, start: usize, end: usize, pick: impl Fn(usize, &Tok) -> bool) -> Vec<usize> {
        let mut depth = 0usize;
        let mut found = Vec::new();
        for i in start..end {
            let t = &self.toks[i];
            match t.kind {
                Kind::Open => depth += 1,
                Kind::Close => depth = depth.saturating_sub(1),
                _ if depth == 0 && pick(i, t) => found.push(i),
                _ => {}
            }
        }
        found
    }

    fn relation(&self, table: &str, qualifier: String) -> Result<Relation, Stop> {
        let columns = (self.columns_of)(table).ok_or(Stop::Unsure)?;
        Ok(Relation { qualifier, columns })
    }

    /// A `FROM` list, `start..end`: its relations, and the `ON` conditions
    /// to read once they are all in scope.
    fn from_list(&self, start: usize, end: usize) -> Result<(Scope, Vec<(usize, usize)>), Stop> {
        const JOIN_WORDS: &[&str] = &["join", "inner", "left", "right", "full", "outer", "cross", "natural"];
        let mut scope = Vec::new();
        let mut conditions = Vec::new();
        let mut i = start;
        while i < end {
            let t = &self.toks[i];
            if t.kind == Kind::Comma || (t.kind == Kind::Word && (JOIN_WORDS.contains(&t.text.as_str()) || t.text == "only")) {
                i += 1;
                continue;
            }
            if t.is_word("on") {
                // To the next join or comma at this depth.
                let stop = self
                    .top_level(i + 1, end, |_, t| t.kind == Kind::Comma || (t.kind == Kind::Word && JOIN_WORDS.contains(&t.text.as_str())))
                    .first()
                    .copied()
                    .unwrap_or(end);
                conditions.push((i + 1, stop));
                i = stop;
                continue;
            }
            if t.is_word("using") {
                let open = i + 1;
                if self.toks.get(open).is_none_or(|t| t.kind != Kind::Open) {
                    return Err(Stop::Unsure);
                }
                i = self.close_of(open).ok_or(Stop::Unsure)? + 1;
                continue;
            }
            // A table, possibly `schema.table`.
            if !(t.kind == Kind::Word && !is_keyword(&t.text) || t.kind == Kind::Quoted) {
                return Err(Stop::Unsure);
            }
            let mut table = t.text.clone();
            i += 1;
            if self.toks.get(i).is_some_and(|t| t.kind == Kind::Dot) {
                if !matches!(table.as_str(), "public" | "pg_catalog") {
                    return Err(Stop::Unsure);
                }
                let name = self.toks.get(i + 1).filter(|t| t.is_name()).ok_or(Stop::Unsure)?;
                table = name.text.clone();
                i += 2;
            }
            if self.toks.get(i).is_some_and(|t| t.kind == Kind::Open) {
                return Err(Stop::Unsure); // a function in FROM
            }
            let mut alias = None;
            if self.toks.get(i).is_some_and(|t| t.is_word("as")) {
                i += 1;
                alias = Some(self.toks.get(i).filter(|t| t.is_name()).ok_or(Stop::Unsure)?.text.clone());
                i += 1;
            } else if let Some(t) = self.toks.get(i).filter(|t| i < end && t.is_name()) {
                alias = Some(t.text.clone());
                i += 1;
            }
            if self.toks.get(i).is_some_and(|t| t.kind == Kind::Open || t.is_word("tablesample")) && i < end {
                return Err(Stop::Unsure); // an alias column list, a sample
            }
            let qualifier = alias.unwrap_or_else(|| table.clone());
            scope.push(self.relation(&table, qualifier)?);
        }
        if scope.is_empty() && start < end {
            return Err(Stop::Unsure);
        }
        Ok((scope, conditions))
    }

    fn select(&self, start: usize, end: usize, outer: &[&Scope]) -> Result<(), Stop> {
        let toks = self.toks;
        // A set operation: each branch on its own; the trailing ORDER BY
        // names the result's columns, which this does not read.
        let set_ops = self.top_level(start, end, |_, t| t.is_word("union") || t.is_word("intersect") || t.is_word("except"));
        if !set_ops.is_empty() {
            let mut from = start;
            for (n, &op) in set_ops.iter().chain(std::iter::once(&end)).enumerate() {
                let mut branch_end = op;
                if n == set_ops.len() {
                    branch_end = self
                        .top_level(from, end, |_, t| t.is_word("order") || t.is_word("limit") || t.is_word("offset"))
                        .first()
                        .copied()
                        .unwrap_or(end);
                }
                let mut b = from;
                while toks.get(b).is_some_and(|t| t.is_word("all") || t.is_word("distinct")) {
                    b += 1;
                }
                if toks.get(b).is_some_and(|t| t.kind == Kind::Open) {
                    let close = self.close_of(b).ok_or(Stop::Unsure)?;
                    self.select(b + 1, close, outer)?;
                } else if toks.get(b).is_some_and(|t| t.is_word("select")) {
                    self.select(b, branch_end, outer)?;
                } else {
                    return Err(Stop::Unsure);
                }
                from = op + 1;
            }
            return Ok(());
        }
        if !toks.get(start).is_some_and(|t| t.is_word("select")) {
            return Err(Stop::Unsure);
        }
        const CLAUSES: &[&str] = &[
            "from", "where", "group", "having", "window", "order", "limit", "offset", "fetch", "for", "into",
        ];
        let marks = self.top_level(start + 1, end, |i, t| {
            t.kind == Kind::Word
                && CLAUSES.contains(&t.text.as_str())
                // `IS DISTINCT FROM` and `EXTRACT(x FROM y)` are nested or
                // follow DISTINCT; at depth 0 a FROM after DISTINCT is that.
                && !(t.text == "from" && toks.get(i.wrapping_sub(1)).is_some_and(|p| p.is_word("distinct")))
                // `percentile_cont(…) WITHIN GROUP (ORDER BY …)`.
                && !(t.text == "group" && toks.get(i.wrapping_sub(1)).is_some_and(|p| p.is_word("within")))
        });
        let clause = |word: &str| -> Option<(usize, usize)> {
            let at = marks.iter().position(|&m| toks[m].text == word)?;
            let next = marks.get(at + 1).copied().unwrap_or(end);
            Some((marks[at] + 1, next))
        };
        if clause("window").is_some() || clause("into").is_some() {
            return Err(Stop::Unsure);
        }
        let list_end = marks.first().copied().unwrap_or(end);
        let (scope, conditions) = match clause("from") {
            Some((s, e)) => self.from_list(s, e)?,
            None => (Vec::new(), Vec::new()),
        };
        let mut scopes: Vec<&Scope> = vec![&scope];
        scopes.extend_from_slice(outer);

        for (s, e) in conditions {
            self.expression(s, e, &scopes, false)?;
        }
        // The select list, past DISTINCT [ON (…)] / ALL.
        let mut s = start + 1;
        if toks.get(s).is_some_and(|t| t.is_word("all")) {
            s += 1;
        } else if toks.get(s).is_some_and(|t| t.is_word("distinct")) {
            s += 1;
            if toks.get(s).is_some_and(|t| t.is_word("on")) {
                let close = self.close_of(s + 1).ok_or(Stop::Unsure)?;
                self.expression(s + 2, close, &scopes, false)?;
                s = close + 1;
            }
        }
        // Output names, which ORDER BY and GROUP BY may name bare.
        let mut aliases: Vec<String> = Vec::new();
        for item in self.items(s, list_end) {
            let (a, b) = item;
            if toks[b - 1].is_name() && (b - a == 1 || toks[b - 2].is_word("as") || ends_an_operand(&toks[b - 2])) {
                aliases.push(toks[b - 1].text.clone());
            }
            self.expression(a, b, &scopes, true)?;
        }
        if let Some((s, e)) = clause("where") {
            self.expression(s, e, &scopes, false)?;
        }
        if let Some((s, e)) = clause("having") {
            self.expression(s, e, &scopes, false)?;
        }
        for word in ["order", "group"] {
            if let Some((s, e)) = clause(word) {
                let s = if toks.get(s).is_some_and(|t| t.is_word("by")) { s + 1 } else { s };
                for (a, b) in self.items(s, e) {
                    // `ASC`, `DESC`, `NULLS FIRST|LAST`, `USING <`.
                    let b = (a..b).find(|&k| toks[k].is_word("using")).unwrap_or(b);
                    let mut b = b;
                    while b > a + 1 && ["asc", "desc", "nulls", "first", "last"].iter().any(|w| toks[b - 1].is_word(w)) {
                        b -= 1;
                    }
                    if b - a == 1 && toks[a].is_name() && aliases.contains(&toks[a].text) {
                        continue;
                    }
                    self.expression(a, b, &scopes, false)?;
                }
            }
        }
        Ok(())
    }

    /// The comma-separated items of `start..end` at depth 0.
    fn items(&self, start: usize, end: usize) -> Vec<(usize, usize)> {
        let commas = self.top_level(start, end, |_, t| t.kind == Kind::Comma);
        let mut out = Vec::new();
        let mut from = start;
        for c in commas.into_iter().chain(std::iter::once(end)) {
            if c > from {
                out.push((from, c));
            }
            from = c + 1;
        }
        out
    }

    fn update(&self) -> Result<(), Stop> {
        let toks = self.toks;
        let end = toks.len();
        let mut i = 1;
        if toks.get(i).is_some_and(|t| t.is_word("only")) {
            i += 1;
        }
        let (mut table, mut qualifier) = (toks.get(i).filter(|t| t.is_name()).ok_or(Stop::Unsure)?.text.clone(), None);
        i += 1;
        if toks.get(i).is_some_and(|t| t.kind == Kind::Dot) {
            if table != "public" {
                return Err(Stop::Unsure);
            }
            table = toks.get(i + 1).filter(|t| t.is_name()).ok_or(Stop::Unsure)?.text.clone();
            i += 2;
        }
        if toks.get(i).is_some_and(|t| t.is_word("as")) {
            i += 1;
        }
        if let Some(t) = toks.get(i).filter(|t| t.is_name()) {
            qualifier = Some(t.text.clone());
            i += 1;
        }
        if !toks.get(i).is_some_and(|t| t.is_word("set")) {
            return Err(Stop::Unsure);
        }
        let marks = self.top_level(i + 1, end, |_, t| t.is_word("from") || t.is_word("where") || t.is_word("returning"));
        let clause = |word: &str| {
            let at = marks.iter().position(|&m| toks[m].text == word)?;
            Some((marks[at] + 1, marks.get(at + 1).copied().unwrap_or(end)))
        };
        let mut scope = vec![self.relation(&table, qualifier.unwrap_or_else(|| table.clone()))?];
        if let Some((s, e)) = clause("from") {
            let (more, conditions) = self.from_list(s, e)?;
            scope.extend(more);
            let scopes: Vec<&Scope> = vec![&scope];
            for (s, e) in conditions {
                self.expression(s, e, &scopes, false)?;
            }
        }
        let scopes: Vec<&Scope> = vec![&scope];
        // Each assignment's value; a target list `(a, b) = …` reads alike.
        let set_end = marks.first().copied().unwrap_or(end);
        for (a, b) in self.items(i + 1, set_end) {
            let eq = (a..b).find(|&k| toks[k].kind == Kind::Op && toks[k].text == "=").ok_or(Stop::Unsure)?;
            self.expression(eq + 1, b, &scopes, false)?;
        }
        if let Some((s, e)) = clause("where") {
            self.expression(s, e, &scopes, false)?;
        }
        if let Some((s, e)) = clause("returning") {
            for (a, b) in self.items(s, e) {
                self.expression(a, b, &scopes, true)?;
            }
        }
        Ok(())
    }

    fn delete(&self) -> Result<(), Stop> {
        let toks = self.toks;
        let end = toks.len();
        if !toks.get(1).is_some_and(|t| t.is_word("from")) {
            return Err(Stop::Unsure);
        }
        let marks = self.top_level(2, end, |_, t| t.is_word("using") || t.is_word("where") || t.is_word("returning"));
        let clause = |word: &str| {
            let at = marks.iter().position(|&m| toks[m].text == word)?;
            Some((marks[at] + 1, marks.get(at + 1).copied().unwrap_or(end)))
        };
        let (mut scope, _) = self.from_list(2, marks.first().copied().unwrap_or(end))?;
        if scope.len() != 1 {
            return Err(Stop::Unsure);
        }
        if let Some((s, e)) = clause("using") {
            let (more, conditions) = self.from_list(s, e)?;
            scope.extend(more);
            let scopes: Vec<&Scope> = vec![&scope];
            for (s, e) in conditions {
                self.expression(s, e, &scopes, false)?;
            }
        }
        let scopes: Vec<&Scope> = vec![&scope];
        if let Some((s, e)) = clause("where") {
            self.expression(s, e, &scopes, false)?;
        }
        if let Some((s, e)) = clause("returning") {
            for (a, b) in self.items(s, e) {
                self.expression(a, b, &scopes, true)?;
            }
        }
        Ok(())
    }

    /// `INSERT … VALUES (…)`: the values see no column; `INSERT … SELECT`
    /// reads as its query does. `ON CONFLICT` and `RETURNING` are left to
    /// the engine's own messages.
    fn insert(&self) -> Result<(), Stop> {
        let toks = self.toks;
        let end = self
            .top_level(0, toks.len(), |_, t| t.is_word("on") || t.is_word("returning"))
            .first()
            .copied()
            .unwrap_or(toks.len());
        if let Some(&at) = self.top_level(0, end, |_, t| t.is_word("values")).first() {
            return self.expression(at + 1, end, &[], false);
        }
        if let Some(&at) = self.top_level(0, end, |_, t| t.is_word("select")).first() {
            return self.select(at, end, &[]);
        }
        Err(Stop::Unsure)
    }

    /// The type PostgreSQL gives an argument, `None` when this cannot say.
    fn argument_type(&self, start: usize, end: usize, scopes: &[&Scope]) -> Option<String> {
        let toks = &self.toks[start..end];
        // A cast at the end decides it.
        if let Some(cast) = toks.iter().rposition(|t| t.kind == Kind::Cast) {
            let words: Vec<&str> = toks[cast + 1..]
                .iter()
                .take_while(|t| matches!(t.kind, Kind::Word | Kind::Quoted))
                .map(|t| t.text.as_str())
                .collect();
            let depth_ok = toks[cast + 1..].iter().all(|t| matches!(t.kind, Kind::Word | Kind::Quoted | Kind::Num | Kind::Comma) || t.text == "(" || t.text == ")");
            return (!words.is_empty() && depth_ok).then(|| canonical_type(&words.join(" ")));
        }
        match toks {
            [t] | [Tok { kind: Kind::Op, .. }, t] if t.kind == Kind::Num => {
                if toks.len() == 2 && toks[0].text != "-" && toks[0].text != "+" {
                    return None;
                }
                let text = &t.text;
                Some(if text.contains(['.', 'e', 'E']) {
                    "numeric"
                } else if text.parse::<i32>().is_ok() {
                    "integer"
                } else if text.parse::<i64>().is_ok() {
                    "bigint"
                } else {
                    "numeric"
                }
                .to_string())
            }
            [t] if t.kind == Kind::Str => Some("unknown".to_string()),
            [t] if t.is_word("true") || t.is_word("false") => Some("boolean".to_string()),
            [t] if t.is_word("null") => Some("unknown".to_string()),
            [t] if t.is_name() => lookup(scopes, None, &t.text).map(str::to_string),
            [q, d, c] if q.is_name() && d.kind == Kind::Dot && c.is_name() => {
                lookup(scopes, Some(&q.text), &c.text).map(str::to_string)
            }
            _ => None,
        }
    }

    /// The function a refusal named, as PostgreSQL reports it.
    fn undefined_function(&self, display: &str, open: usize, close: usize, scopes: &[&Scope]) -> Stop {
        let args = self.items(open + 1, close);
        let types: Option<Vec<String>> = args.iter().map(|&(a, b)| self.argument_type(a, b, scopes)).collect();
        let message = match types {
            Some(types) => format!("function {}({}) does not exist", display, types.join(", ")),
            None => format!("function {} does not exist", display),
        };
        Stop::Undefined("42883", message)
    }

    /// Every name read in `start..end`, in order; a subquery in the scope
    /// of `scopes`.
    fn expression(&self, start: usize, end: usize, scopes: &[&Scope], alias_ok: bool) -> Result<(), Stop> {
        let toks = self.toks;
        let mut i = start;
        while i < end {
            let t = &toks[i];
            let prev = i.checked_sub(1).filter(|p| *p >= start).map(|p| &toks[p]);
            match t.kind {
                Kind::Open => {
                    if toks.get(i + 1).is_some_and(|n| n.is_word("select")) {
                        let close = self.close_of(i).ok_or(Stop::Unsure)?;
                        self.select(i + 1, close, scopes)?;
                        i = close + 1;
                        continue;
                    }
                    if toks.get(i + 1).is_some_and(|n| n.is_word("with") || n.is_word("values")) {
                        return Err(Stop::Unsure);
                    }
                    i += 1;
                }
                Kind::Cast => {
                    // The type: its words, a modifier, array brackets.
                    i += 1;
                    while i < end && matches!(toks[i].kind, Kind::Word | Kind::Quoted | Kind::Dot) {
                        i += 1;
                    }
                    if i < end && toks[i].kind == Kind::Open {
                        i = self.close_of(i).ok_or(Stop::Unsure)? + 1;
                    }
                    while i + 1 < end && toks[i].text == "[" && toks[i + 1].text == "]" {
                        i += 2;
                    }
                }
                Kind::Word | Kind::Quoted => {
                    let next = toks.get(i + 1).filter(|_| i + 1 < end);
                    // An alias, a cast's type, a collation, a typed literal.
                    if prev.is_some_and(|p| p.is_word("as") || p.is_word("collate") || p.is_word("over"))
                        || next.is_some_and(|n| n.kind == Kind::Str)
                    {
                        i += 1;
                        continue;
                    }
                    // A name right after an operand is an output name, the
                    // last word of a select item; anywhere else PostgreSQL's
                    // parser refuses it (`GROUP BY id HAVNG id`).
                    if prev.is_some_and(ends_an_operand) && !(t.kind == Kind::Word && is_keyword(&t.text)) {
                        if alias_ok && i + 1 == end {
                            i += 1;
                            continue;
                        }
                        return Err(Stop::Unsure);
                    }
                    if t.kind == Kind::Word && is_keyword(&t.text) && !next.is_some_and(|n| n.kind == Kind::Open && !is_keyword_call(&t.text)) {
                        i += 1;
                        continue;
                    }
                    // A qualified name: `q.c`, `q.*`, `s.f(…)`.
                    if next.is_some_and(|n| n.kind == Kind::Dot) {
                        let part = toks.get(i + 2).ok_or(Stop::Unsure)?;
                        if part.kind == Kind::Op && part.text == "*" {
                            i += 3;
                            continue;
                        }
                        if !(part.kind == Kind::Word || part.kind == Kind::Quoted) {
                            return Err(Stop::Unsure);
                        }
                        if toks.get(i + 3).is_some_and(|n| n.kind == Kind::Dot) {
                            return Err(Stop::Unsure);
                        }
                        if toks.get(i + 3).is_some_and(|n| n.kind == Kind::Open) {
                            let close = self.close_of(i + 3).ok_or(Stop::Unsure)?;
                            self.expression(i + 4, close, scopes, false)?;
                            if self.report && self.function.is_some_and(|f| !f.builtin && f.name == part.text) {
                                return Err(self.undefined_function(&format!("{}.{}", t.text, part.text), i + 3, close, scopes));
                            }
                            i = close + 1;
                            continue;
                        }
                        match resolve(scopes, Some(&t.text), &part.text) {
                            Resolved::Column(_) => {}
                            Resolved::NoColumn if !self.report => {}
                            Resolved::NoColumn => {
                                return Err(Stop::Undefined("42703", format!("column {}.{} does not exist", t.text, part.text)));
                            }
                            Resolved::NoRelation => return Err(Stop::Unsure),
                        }
                        i += 3;
                        continue;
                    }
                    // A call: its arguments are read first, as PostgreSQL does.
                    if next.is_some_and(|n| n.kind == Kind::Open && n.text == "(") {
                        let close = self.close_of(i + 1).ok_or(Stop::Unsure)?;
                        let mut args_from = i + 2;
                        if t.text == "extract" || t.text == "trim" {
                            // `EXTRACT(field FROM x)`, `TRIM(BOTH 'x' FROM s)`.
                            args_from += 1;
                        }
                        self.expression(args_from, close, scopes, false)?;
                        if let Some(f) = self.function.filter(|f| self.report && f.name == t.text) {
                            let arity = self.items(i + 2, close).len();
                            if !f.builtin || f.arity.is_none_or(|n| n != arity) {
                                return Err(self.undefined_function(&t.text, i + 1, close, scopes));
                            }
                        }
                        // `f(…) FILTER (WHERE …)`, `OVER (…)`: read on.
                        i = close + 1;
                        continue;
                    }
                    match resolve(scopes, None, &t.text) {
                        Resolved::Column(_) => {}
                        _ if !self.report => {}
                        _ => return Err(Stop::Undefined("42703", format!("column \"{}\" does not exist", t.text))),
                    }
                    i += 1;
                }
                // A literal right after an operand: no expression PostgreSQL
                // parses (`a = 1 0x10`).
                Kind::Num | Kind::Str | Kind::Param if prev.is_some_and(ends_an_operand) => return Err(Stop::Unsure),
                _ => i += 1,
            }
        }
        Ok(())
    }
}

/// Whether `t` ends an operand, so a name after it is that operand's alias
/// (`count(*) n`, `1 one`, `CASE … END c`).
fn ends_an_operand(t: &Tok) -> bool {
    t.is_name() || t.is_word("end") || matches!(t.kind, Kind::Num | Kind::Str | Kind::Param | Kind::Close)
}

/// Keywords written as a call whose parentheses hold expressions to read.
fn is_keyword_call(word: &str) -> bool {
    matches!(word, "array" | "row" | "cube" | "rollup" | "sets" | "exists" | "in" | "any" | "some" | "all" | "filter" | "over" | "values" | "cast" | "within" | "on" | "using" | "partition" | "not" | "and" | "or" | "when" | "then" | "else" | "case" | "by" | "where" | "having" | "select" | "from" | "is" | "between" | "like" | "ilike" | "to" | "overlaps")
}

enum Resolved<'a> {
    Column(&'a str),
    NoColumn,
    NoRelation,
}

/// `qualifier.name` or a bare `name`, innermost scope first.
fn resolve<'a>(scopes: &[&'a Scope], qualifier: Option<&str>, name: &str) -> Resolved<'a> {
    for scope in scopes {
        match qualifier {
            Some(q) => {
                if let Some(rel) = scope.iter().find(|r| r.qualifier == q) {
                    return match rel.columns.iter().find(|(c, _)| c == name) {
                        Some((_, ty)) => Resolved::Column(ty),
                        None => Resolved::NoColumn,
                    };
                }
            }
            None => {
                if let Some((_, ty)) = scope.iter().flat_map(|r| r.columns.iter()).find(|(c, _)| c == name) {
                    return Resolved::Column(ty);
                }
            }
        }
    }
    if qualifier.is_some() { Resolved::NoRelation } else { Resolved::NoColumn }
}

fn lookup<'a>(scopes: &[&'a Scope], qualifier: Option<&str>, name: &str) -> Option<&'a str> {
    match resolve(scopes, qualifier, name) {
        Resolved::Column(ty) => Some(ty),
        _ => None,
    }
}

/// A written type as PostgreSQL names it in a message.
fn canonical_type(written: &str) -> String {
    match written {
        "int" | "integer" | "int4" => "integer",
        "bigint" | "int8" => "bigint",
        "smallint" | "int2" => "smallint",
        "varchar" | "character varying" => "character varying",
        "char" | "character" | "bpchar" => "character",
        "decimal" | "numeric" => "numeric",
        "real" | "float4" => "real",
        "double precision" | "float" | "float8" => "double precision",
        "bool" | "boolean" => "boolean",
        "time" | "time without time zone" => "time without time zone",
        "timetz" | "time with time zone" => "time with time zone",
        "timestamp" | "timestamp without time zone" => "timestamp without time zone",
        "timestamptz" | "timestamp with time zone" => "timestamp with time zone",
        other => other,
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns(table: &str) -> Option<Vec<(String, String)>> {
        let cols: &[(&str, &str)] = match table {
            "fx" => &[("id", "integer"), ("a", "integer"), ("s", "text")],
            "fo" => &[("id", "integer"), ("name", "text")],
            _ => return None,
        };
        Some(cols.iter().map(|(c, t)| (c.to_string(), t.to_string())).collect())
    }

    fn diag(sql: &str, message: &str) -> Option<(&'static str, String)> {
        let subject = subject(message)?;
        diagnose(sql, subject.as_ref(), &columns)
    }

    #[test]
    fn undefined_columns_are_found_where_postgresql_reads_them() {
        let col = |n: &str| Some(("42703", format!("column \"{n}\" does not exist")));
        let msg = "Column 'x' not found in schema";
        assert_eq!(diag("SELECT nosuch + 1 FROM fx", msg), col("nosuch"));
        assert_eq!(diag("SELECT a + nosuch FROM fx", msg), col("nosuch"));
        assert_eq!(diag("SELECT CASE WHEN nosuch THEN 1 END FROM fx", msg), col("nosuch"));
        assert_eq!(diag("SELECT a FROM fx HAVING nosuch > 1", msg), col("nosuch"));
        assert_eq!(diag("SELECT a FROM fx ORDER BY a + nosuch", msg), col("nosuch"));
        assert_eq!(diag("SELECT id FROM fx WHERE id IN (SELECT nosuch FROM fx)", msg), col("nosuch"));
        assert_eq!(diag("SELECT a FROM fx WHERE EXISTS (SELECT 1 WHERE nosuch)", msg), col("nosuch"));
        // An output name is a name in ORDER BY alone, not in an expression.
        assert_eq!(diag("SELECT a + 1 AS b FROM fx ORDER BY b + nosuch", msg), col("b"));
        assert_eq!(diag("SELECT a + 1 AS b FROM fx ORDER BY b, nosuch", msg), col("nosuch"));
        // ON is read before the select list.
        assert_eq!(
            diag("SELECT nosuch FROM fx f JOIN fx g ON f.id = g.nosuch", msg),
            Some(("42703", "column g.nosuch does not exist".to_string()))
        );
        assert_eq!(
            diag("SELECT f.a + g.nosuch FROM fx f JOIN fx g ON f.id = g.id", msg),
            Some(("42703", "column g.nosuch does not exist".to_string()))
        );
        // A correlated name is the outer query's.
        assert_eq!(diag("SELECT a FROM fx WHERE EXISTS (SELECT 1 FROM fo WHERE fo.id = a + nosuch)", msg), col("nosuch"));
        assert_eq!(diag("UPDATE fx SET a = a + nosuch WHERE id = 1", msg), col("nosuch"));
        assert_eq!(diag("DELETE FROM fx WHERE a = nosuch + 1", msg), col("nosuch"));
    }

    #[test]
    fn a_statement_whose_names_all_resolve_keeps_its_message() {
        let msg = "Column 'x' not found in schema";
        for sql in [
            "SELECT a + 1 AS b, count(*) FROM fx GROUP BY 1 ORDER BY b DESC NULLS LAST",
            "SELECT s::varchar(10), a::double precision, CAST(a AS numeric(10,2)) FROM fx",
            "SELECT EXTRACT(year FROM now()), interval '1 day', date '2024-01-01' FROM fx",
            "SELECT a FROM fx WHERE s IS DISTINCT FROM 'x' AND a BETWEEN 1 AND 2 AND s COLLATE \"C\" < 'z'",
            "SELECT f.a, g.name FROM fx f LEFT JOIN fo g ON g.id = f.id WHERE g.name IS NOT NULL",
            "SELECT sum(a) FILTER (WHERE a > 1) OVER (PARTITION BY s ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM fx",
            "SELECT a FROM fx UNION SELECT id FROM fo ORDER BY 1",
            "SELECT trim(both 'x' from s), a b FROM fx",
            "SELECT current_date, current_user, ARRAY[a, 1], ROW(a, s) FROM fx",
        ] {
            assert_eq!(diag(sql, msg), None, "{sql}");
        }
        // Text PostgreSQL's parser refuses first: its syntax error stands.
        for sql in [
            "SELECT id FROM fx WHERE nosuch = 0 GROUP BY id HAVNG id > 1",
            "SELECT nosuch FROM fx WHERE a = 1 0x10",
            "SELECT nosuch FROM fx ORDER BY a b",
        ] {
            assert_eq!(diag(sql, msg), None, "{sql}");
        }
        // Unsure shapes keep the engine's message.
        for sql in [
            "WITH c AS (SELECT 1 AS x) SELECT nosuch FROM c",
            "SELECT nosuch FROM (SELECT 1 AS x) d",
            "SELECT nosuch FROM generate_series(1, 3) g",
            "SELECT nosuch FROM someview",
            "SELECT nosuch FROM fx AS q(x, y, z)",
        ] {
            assert_eq!(diag(sql, msg), None, "{sql}");
        }
    }

    #[test]
    fn an_unknown_function_is_42883_with_its_argument_types() {
        let f = |sql: &str, msg: &str| diag(sql, msg).map(|(c, m)| format!("{c} {m}"));
        assert_eq!(
            f("SELECT nosuchfn(a, 'x', 1.5, s) FROM fx", "function 'nosuchfn' is not supported yet").as_deref(),
            Some("42883 function nosuchfn(integer, unknown, numeric, text) does not exist")
        );
        assert_eq!(
            f("SELECT a FROM fx WHERE nosuchfn(a) = 1", "unknown column or unsupported keyword 'nosuchfn' in WHERE clause").as_deref(),
            Some("42883 function nosuchfn(integer) does not exist")
        );
        assert_eq!(
            f("SELECT length(1,2)", "length takes exactly one argument").as_deref(),
            Some("42883 function length(integer, integer) does not exist")
        );
        assert_eq!(
            f("SELECT abs('x'::text) FROM fx", "abs is not defined for STRING").as_deref(),
            Some("42883 function abs(text) does not exist")
        );
        assert_eq!(
            f("INSERT INTO fx VALUES (3, nosuchfn(1), 'z')", "function 'nosuchfn' is not supported yet").as_deref(),
            Some("42883 function nosuchfn(integer) does not exist")
        );
        assert_eq!(
            f("SELECT public.nosuchfn(1)", "column \"public.nosuchfn(1)\" does not exist").as_deref(),
            Some("42883 function public.nosuchfn(integer) does not exist")
        );
        // An argument's unknown column first, as PostgreSQL reads it.
        assert_eq!(
            f("SELECT nosuchfn(nosuch) FROM fx", "function 'nosuchfn' is not supported yet").as_deref(),
            Some("42703 column \"nosuch\" does not exist")
        );
        // A function PostgreSQL has that the engine does not: unchanged.
        assert_eq!(subject("function 'pg_backend_pid' is not supported yet").map(|s| s.is_some()), None);
        assert!(is_postgres_function("abs") && is_postgres_function("generate_series") && !is_postgres_function("nosuchfn"));
    }
}
