//! `CREATE INDEX` over plain columns.
//!
//! ```sql
//! CREATE INDEX [CONCURRENTLY] [IF NOT EXISTS] [name] ON [ONLY] table
//!     [USING btree | hash] (column [ASC | DESC] [NULLS FIRST | LAST], …)
//!     [INCLUDE (…)] [WITH (…)] [TABLESPACE …]
//! ```
//!
//! What this does not claim is left exactly where it went before:
//! - `CREATE UNIQUE INDEX` is a unique constraint here, and the constraint
//!   brings its own index;
//! - an index over a document path (`(doc->>'k')`) belongs to
//!   [`crate::core::rql::path_index`];
//! - a partial index (`WHERE …`) or an expression index (`(lower(email))`) is
//!   accepted and not built, as all index DDL was before. An index built
//!   without its predicate or expression would answer different questions
//!   from the ones it was declared for. The one expression read is a
//!   full-text one, `USING gin (to_tsvector('<configuration>', <document>))`
//!   ([`tsvector_expression`]).
//!
//! `ASC`/`DESC`, `NULLS`, `COLLATE` and an operator class only shape a
//! B-tree's scan order or its comparisons in PostgreSQL. This index orders by
//! the engine's own comparison either way, so they are read and set aside.
//! `INCLUDE` columns are fetched from the row regardless.

use super::Method;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndex {
    /// `None` when the statement names none; PostgreSQL then derives one.
    pub name: Option<String>,
    pub if_not_exists: bool,
    pub table: String,
    pub method: Option<Method>,
    pub columns: Vec<String>,
    /// What an unnamed index is named after when it is not its columns:
    /// `to_tsvector` for a full-text index, as PostgreSQL names an
    /// expression index after the function.
    pub label: Option<String>,
}

impl CreateIndex {
    /// The name PostgreSQL would give an unnamed index: `table_col1_col2_idx`.
    pub fn name_or_default(&self) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| {
                // Cut as every derived name is cut: the label survives.
                let what = self.label.clone().unwrap_or_else(|| self.columns.join("_"));
                crate::core::rql::unique_key::name_with_label(&format!("{}_{}", self.table, what), "_idx")
            })
    }
}

fn strip_ident(raw: &str) -> String {
    let raw = raw.trim().trim_end_matches([',', ';']);
    raw.split('.').next_back().unwrap_or(raw).trim().trim_matches('"').to_string()
}

fn is_plain_identifier(text: &str) -> bool {
    let t = text.trim();
    if t.starts_with('"') && t.ends_with('"') && t.len() >= 2 {
        return !t[1..t.len() - 1].contains('"');
    }
    !t.is_empty()
        && t.chars().next().is_some_and(|c| c.is_alphabetic() || c == '_')
        && t.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}

/// The index of the `)` closing the `(` at `open`, quotes respected.
fn matching_paren(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    for (i, &b) in bytes.iter().enumerate().skip(open) {
        match quote {
            Some(q) if b == q => quote = None,
            Some(_) => {}
            None => match b {
                b'\'' | b'"' => quote = Some(b),
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
    }
    None
}

/// Splits on commas at the top level (outside parentheses and quotes).
fn split_top(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let (mut depth, mut start, mut quote) = (0i32, 0usize, None::<char>);
    for (i, c) in text.char_indices() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    parts.push(&text[start..i]);
                    start = i + 1;
                }
                _ => {}
            },
        }
    }
    parts.push(&text[start..]);
    parts
}

/// One entry of the column list: the column and its operator class, or
/// `None` when it is an expression rather than a column.
fn column_of(part: &str) -> Option<(String, Option<String>)> {
    let words: Vec<&str> = part.split_whitespace().collect();
    let first = *words.first()?;
    if !is_plain_identifier(first) {
        return None;
    }
    // What may follow a column: ordering, null placement, a collation, an
    // operator class. Anything else is an expression this does not read.
    let mut opclass = None;
    let mut i = 1;
    while i < words.len() {
        let w = words[i].to_ascii_uppercase();
        match w.as_str() {
            "ASC" | "DESC" => i += 1,
            "NULLS" if i + 1 < words.len() => i += 2,
            "COLLATE" if i + 1 < words.len() => i += 2,
            // An operator class (`text_pattern_ops`, `jsonb_path_ops`).
            _ if is_plain_identifier(words[i]) => {
                opclass = Some(words[i].trim_matches('"').to_ascii_lowercase());
                i += 1
            }
            _ => return None,
        }
    }
    Some((first.trim_matches('"').to_string(), opclass))
}

/// Recognises a plain-column `CREATE INDEX`. `None` when the statement is not
/// one (so the caller's other readings apply); `Some(Err)` when it is one but
/// cannot be honoured as written.
pub fn parse_create(sql: &str) -> Option<Result<CreateIndex, String>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("CREATE INDEX") {
        return None;
    }
    let mut rest = trimmed["CREATE INDEX".len()..].trim_start();
    let take = |rest: &mut &str, word: &str| -> bool {
        let r = rest.to_ascii_uppercase();
        if r.starts_with(word) && r[word.len()..].starts_with(|c: char| c.is_whitespace()) {
            *rest = rest[word.len()..].trim_start();
            true
        } else {
            false
        }
    };
    take(&mut rest, "CONCURRENTLY");
    let if_not_exists = take(&mut rest, "IF NOT EXISTS");

    // `[name] ON`: the name is optional.
    let on_at = crate::core::rql::join::find_keyword(rest, "ON", 0)?;
    let name_text = rest[..on_at].trim();
    let name = (!name_text.is_empty()).then(|| strip_ident(name_text));
    let mut after_on = rest[on_at + 2..].trim_start();
    take(&mut after_on, "ONLY");

    let open = after_on.find('(')?;
    let mut head = after_on[..open].trim();
    let mut method = None;
    if let Some(using_at) = crate::core::rql::join::find_keyword(head, "USING", 0) {
        let m = head[using_at + "USING".len()..].trim();
        method = match Method::parse(m) {
            Some(m) => Some(m),
            None => {
                return Some(Err(format!(
                    "index method '{m}' is not supported; use btree or hash, or leave USING out to have one chosen"
                )));
            }
        };
        head = head[..using_at].trim();
    }
    let table = strip_ident(head);
    if table.is_empty() || !is_plain_identifier(head.split('.').next_back().unwrap_or(head)) {
        return None;
    }
    let close = matching_paren(after_on, open)?;
    let tail = after_on[close + 1..].trim();
    // A partial index answers only rows matching its predicate. Building it
    // over every row would make it a different index from the one declared.
    if crate::core::rql::join::find_keyword(tail, "WHERE", 0).is_some() {
        return None;
    }
    let mut columns = Vec::new();
    let mut opclasses = Vec::new();
    let parts = split_top(&after_on[open + 1..close]);
    // `USING gin (to_tsvector(…))`: a full-text index, the only expression
    // read. Without `USING gin` PostgreSQL would build a B-tree over the
    // vector, which answers nothing a query asks of one.
    if method == Some(Method::GinOps) && parts.len() == 1 {
        match tsvector_expression(parts[0]) {
            Some(Ok((simple, columns))) => {
                if columns.len() > super::decl::MAX_COLUMNS {
                    return Some(Err(format!(
                        "a full-text index may cover at most {} columns",
                        super::decl::MAX_COLUMNS
                    )));
                }
                if let Some(n) = &name
                    && n.len() > super::decl::NAME_LEN
                {
                    return Some(Err(format!("an index name may be at most {} bytes", super::decl::NAME_LEN)));
                }
                return Some(Ok(CreateIndex {
                    name,
                    if_not_exists,
                    table,
                    method: Some(Method::GinTsvector { simple }),
                    columns,
                    label: Some("to_tsvector".to_string()),
                }));
            }
            Some(Err(e)) => return Some(Err(e)),
            None => {}
        }
    }
    for part in parts {
        match column_of(part) {
            Some((c, opclass)) => {
                columns.push(c);
                opclasses.push(opclass);
            }
            // An expression index: not this module's.
            None => return None,
        }
    }
    if columns.is_empty() {
        return None;
    }
    // A B-tree or hash orders by the engine's own comparison whatever class
    // is named, so the class is set aside. For gin the class decides what is
    // indexed, and one that is not a jsonb class is refused rather than
    // quietly read as the default.
    if method == Some(Method::GinOps) {
        method = match opclasses.first().cloned().flatten().as_deref() {
            None | Some("jsonb_ops") => Some(Method::GinOps),
            Some("jsonb_path_ops") => Some(Method::GinPathOps),
            Some(other) => {
                return Some(Err(format!(
                    "operator class \"{other}\" is not supported for gin; use jsonb_ops or jsonb_path_ops"
                )));
            }
        };
    }
    if columns.len() > super::decl::MAX_COLUMNS {
        return Some(Err(format!(
            "an index may cover at most {} columns",
            super::decl::MAX_COLUMNS
        )));
    }
    if let Some(n) = &name {
        if n.len() > super::decl::NAME_LEN {
            return Some(Err(format!("an index name may be at most {} bytes", super::decl::NAME_LEN)));
        }
    }
    Some(Ok(CreateIndex {
        name,
        if_not_exists,
        table,
        method,
        columns,
        label: None,
    }))
}

/// Strips parentheses that enclose the whole of `text`.
fn unwrap_parens(mut text: &str) -> &str {
    loop {
        let t = text.trim();
        if t.starts_with('(') && matching_paren(t, 0) == Some(t.len() - 1) {
            text = &t[1..t.len() - 1];
        } else {
            return t;
        }
    }
}

/// The text of a single-quoted literal (`''` read as one quote), or `None`
/// when `text` is not exactly one.
fn quoted_literal(text: &str) -> Option<String> {
    let t = text.trim();
    let bytes = t.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'\'' || crate::core::sql_scan::quoted_end(bytes, 0, b'\'') != bytes.len() {
        return None;
    }
    Some(t[1..t.len() - 1].replace("''", "'"))
}

/// Whether every character of `text` ends a word for the full-text splitter
/// (`text_search`'s, which keeps letters, digits and an inner apostrophe): a
/// separator that adds no lexeme and joins none.
fn separates_words(text: &str) -> bool {
    text.chars().all(|c| !c.is_alphanumeric() && c != '\'')
}

/// A string literal, perhaps cast to a text type (`' '::text`, as
/// PostgreSQL prints one back).
fn literal_piece(text: &str) -> Option<String> {
    let t = unwrap_parens(text);
    if let Some(at) = t.rfind("::") {
        let cast = t[at + 2..].trim().to_ascii_lowercase();
        if matches!(cast.as_str(), "text" | "varchar" | "character varying") {
            return quoted_literal(unwrap_parens(&t[..at]));
        }
    }
    quoted_literal(t)
}

/// The operands of a `||` chain, nested parentheses flattened
/// (`(a || ' ') || b`, as PostgreSQL prints one back).
fn concat_pieces(text: &str) -> Vec<&str> {
    let parts = split_concat(unwrap_parens(text));
    if parts.len() == 1 {
        return vec![unwrap_parens(parts[0])];
    }
    parts.into_iter().flat_map(concat_pieces).collect()
}

/// Splits on `||` at the top level (outside parentheses and quotes).
fn split_concat(text: &str) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut parts = Vec::new();
    let (mut depth, mut start, mut i) = (0i32, 0usize, 0usize);
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                i = crate::core::sql_scan::quoted_end(bytes, i, bytes[i]);
                continue;
            }
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'|' if depth == 0 && bytes.get(i + 1) == Some(&b'|') => {
                parts.push(&text[start..i]);
                i += 2;
                start = i;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&text[start..]);
    parts
}

/// A column named on its own, perhaps qualified, quoted or cast to text.
fn text_column(piece: &str) -> Option<String> {
    let mut t = unwrap_parens(piece);
    if let Some(at) = t.rfind("::") {
        let cast = t[at + 2..].trim().to_ascii_lowercase();
        if !matches!(cast.as_str(), "text" | "varchar" | "character varying") {
            return None;
        }
        t = unwrap_parens(&t[..at]);
    }
    let last = t.rsplit('.').next().unwrap_or(t).trim();
    is_plain_identifier(last).then(|| last.trim_matches('"').to_string())
}

/// A full-text index's expression, `to_tsvector('<configuration>',
/// <document>)`: whether the configuration is `simple`, and the columns the
/// document reads. `None` when `part` is not a `to_tsvector` call (the
/// caller's other readings apply); `Err` when it is one this cannot index.
///
/// The document is one column, or columns joined by `||` with a separator
/// literal between each two, each column bare, cast to text, or inside
/// `coalesce(column, '')`. A separator must hold only characters that end a
/// word ([`separates_words`]): then the document's lexemes are exactly the
/// union of each column's own, which is what the index keys a row by, so a
/// query over any of the columns is answered from it. `title || body` joins
/// the last word of one to the first of the other into a lexeme neither
/// column has, and is refused rather than indexed as something else.
pub fn tsvector_expression(part: &str) -> Option<Result<(bool, Vec<String>), String>> {
    let t = unwrap_parens(part);
    let lower = t.to_ascii_lowercase();
    let rest = lower.strip_prefix("to_tsvector")?;
    let open = t.len() - rest.len() + (rest.len() - rest.trim_start().len());
    if !t[open..].starts_with('(') || matching_paren(t, open) != Some(t.len() - 1) {
        return None;
    }
    let unsupported = |why: &str| {
        Some(Err(format!(
            "this full-text index expression is not supported: {why}; index \
             to_tsvector('<configuration>', column [|| ' ' || column …])"
        )))
    };
    let args = split_top(&t[open + 1..t.len() - 1]);
    if args.len() == 1 {
        // PostgreSQL: the one-argument form reads the session's
        // `default_text_search_config`, so it is not IMMUTABLE (42P17).
        return Some(Err("functions in index expression must be marked IMMUTABLE".to_string()));
    }
    if args.len() != 2 {
        return unsupported("to_tsvector takes a configuration and a document");
    }
    let mut configuration = unwrap_parens(args[0]);
    if let Some(at) = configuration.rfind("::") {
        if !configuration[at + 2..].trim().eq_ignore_ascii_case("regconfig") {
            return unsupported("the configuration is not a literal");
        }
        configuration = configuration[..at].trim();
    }
    let Some(configuration) = quoted_literal(configuration) else {
        return unsupported("the configuration is not a literal");
    };
    let simple = crate::core::text_search::is_simple(&configuration);
    let mut columns: Vec<String> = Vec::new();
    // Whether the last piece was a column, so the next must be a separator.
    let mut after_column = false;
    for piece in concat_pieces(args[1]) {
        if let Some(text) = literal_piece(piece) {
            if !separates_words(&text) {
                return unsupported("a literal in the document holds words of its own");
            }
            if !text.is_empty() {
                after_column = false;
            }
            continue;
        }
        let column = if let Some(c) = text_column(piece) {
            c
        } else {
            // `coalesce(column, '')`.
            let l = piece.to_ascii_lowercase();
            let Some(inner) = l.strip_prefix("coalesce").map(str::trim_start).filter(|r| r.starts_with('(')) else {
                return unsupported("the document is not columns joined by separators");
            };
            let at = piece.len() - inner.len();
            if matching_paren(piece, at) != Some(piece.len() - 1) {
                return unsupported("the document is not columns joined by separators");
            }
            let args = split_top(&piece[at + 1..piece.len() - 1]);
            match (args.as_slice(), args.get(1).and_then(|d| literal_piece(d))) {
                ([c, _], Some(default)) if separates_words(&default) => match text_column(c) {
                    Some(c) => c,
                    None => return unsupported("coalesce does not read a column"),
                },
                _ => return unsupported("coalesce's default holds words of its own"),
            }
        };
        if after_column {
            return unsupported("two columns are joined with no separator between them");
        }
        if !columns.iter().any(|c| c.eq_ignore_ascii_case(&column)) {
            columns.push(column);
        }
        after_column = true;
    }
    if columns.is_empty() {
        return unsupported("the document reads no column");
    }
    Some(Ok((simple, columns)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(sql: &str) -> CreateIndex {
        parse_create(sql).expect("recognised").expect("valid")
    }

    #[test]
    fn full_text_indexes() {
        let c = ok("CREATE INDEX ft_body ON ft USING gin (to_tsvector('english', body))");
        assert_eq!(c.method, Some(Method::GinTsvector { simple: false }));
        assert_eq!(c.columns, vec!["body".to_string()]);
        let c = ok("CREATE INDEX ON ft USING GIN (to_tsvector('simple'::regconfig, coalesce(title, '') || ' ' || coalesce(body, '')))");
        assert_eq!(c.method, Some(Method::GinTsvector { simple: true }));
        assert_eq!(c.columns, vec!["title".to_string(), "body".to_string()]);
        assert_eq!(c.name_or_default(), "ft_to_tsvector_idx");
        // As PostgreSQL prints one back (pg_dump, pg_indexes).
        let c = ok("CREATE INDEX x ON public.ft USING gin (to_tsvector('english'::regconfig, \
                    ((COALESCE(title, ''::text) || ' '::text) || COALESCE(body, ''::text))))");
        assert_eq!(c.columns, vec!["title".to_string(), "body".to_string()]);
        let c = ok("CREATE INDEX x ON ft USING gin ((to_tsvector('english', title || ', ' || body::text)))");
        assert_eq!(c.columns, vec!["title".to_string(), "body".to_string()]);
        for (sql, why) in [
            ("CREATE INDEX x ON ft USING gin (to_tsvector(body))", "IMMUTABLE"),
            ("CREATE INDEX x ON ft USING gin (to_tsvector('english', title || body))", "no separator"),
            ("CREATE INDEX x ON ft USING gin (to_tsvector('english', title || ' and ' || body))", "words of its own"),
            ("CREATE INDEX x ON ft USING gin (to_tsvector('english', lower(body)))", "not columns"),
            ("CREATE INDEX x ON ft USING gin (to_tsvector(cfg, body))", "not a literal"),
            ("CREATE INDEX x ON ft USING gin (to_tsvector('english', title || '''' || body))", "words of its own"),
        ] {
            match parse_create(sql) {
                Some(Err(e)) => assert!(e.contains(why), "{sql}: {e}"),
                other => panic!("{sql} should be refused, got {other:?}"),
            }
        }
        // Without USING gin, an expression index is still not this module's.
        assert!(parse_create("CREATE INDEX x ON ft (to_tsvector('english', body))").is_none());
    }

    #[test]
    fn plain_column_indexes() {
        let c = ok("CREATE INDEX ix_cust ON orders (customer_id)");
        assert_eq!(c.name.as_deref(), Some("ix_cust"));
        assert_eq!(c.table, "orders");
        assert_eq!(c.columns, vec!["customer_id"]);
        assert_eq!(c.method, None);

        let c = ok("create index concurrently if not exists \"Ix\" on only public.\"Orders\" using HASH (\"Token\");");
        assert!(c.if_not_exists);
        assert_eq!(c.name.as_deref(), Some("Ix"));
        assert_eq!(c.table, "Orders");
        assert_eq!(c.method, Some(Method::Hash));
        assert_eq!(c.columns, vec!["Token"]);

        let c = ok("CREATE INDEX ON t USING btree (a DESC NULLS LAST, b text_pattern_ops) INCLUDE (c)");
        assert_eq!(c.name, None);
        assert_eq!(c.columns, vec!["a", "b"]);
        assert_eq!(c.name_or_default(), "t_a_b_idx");
        // A long table's derived name keeps its label within the limit.
        let long = format!("CREATE INDEX ON {} (a)", "t".repeat(125));
        let name = ok(&long).name_or_default();
        assert!(name.len() <= 128 && name.ends_with("_idx"), "{name}");
    }

    #[test]
    fn what_this_does_not_claim() {
        // Partial, expression, path and unique indexes stay with their own
        // handling.
        assert!(parse_create("CREATE INDEX ix ON t (a) WHERE a > 0").is_none());
        assert!(parse_create("CREATE INDEX ix ON t (lower(email))").is_none());
        assert!(parse_create("CREATE INDEX ix ON t ((doc->>'k'))").is_none());
        assert!(parse_create("CREATE UNIQUE INDEX ix ON t (a)").is_none());
        assert!(parse_create("CREATE TABLE t (a INT)").is_none());
        assert!(matches!(parse_create("CREATE INDEX ix ON t USING brin (a)"), Some(Err(_))));
    }

    #[test]
    fn gin_and_its_operator_classes() {
        assert_eq!(ok("CREATE INDEX g ON docs USING gin (doc)").method, Some(Method::GinOps));
        assert_eq!(ok("CREATE INDEX g ON docs USING GIN (doc jsonb_ops)").method, Some(Method::GinOps));
        assert_eq!(ok("CREATE INDEX g ON docs USING gin (doc jsonb_path_ops)").method, Some(Method::GinPathOps));
        assert!(matches!(parse_create("CREATE INDEX g ON docs USING gin (doc gin_trgm_ops)"), Some(Err(_))));
        // A B-tree still sets its class aside.
        assert_eq!(ok("CREATE INDEX b ON t (name text_pattern_ops)").method, None);
    }
}
