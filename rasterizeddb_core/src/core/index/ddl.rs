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
//!   from the ones it was declared for.
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
}

impl CreateIndex {
    /// The name PostgreSQL would give an unnamed index: `table_col1_col2_idx`.
    pub fn name_or_default(&self) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| format!("{}_{}_idx", self.table, self.columns.join("_")))
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
    for part in split_top(&after_on[open + 1..close]) {
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
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(sql: &str) -> CreateIndex {
        parse_create(sql).expect("recognised").expect("valid")
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
