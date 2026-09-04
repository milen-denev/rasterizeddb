//! Desugars SQL predicate forms into the comparison grammar the engine already
//! evaluates.
//!
//! The evaluator understands exactly one shape: `column <op> value`, combined
//! with `AND` / `OR` / parentheses. Rather than teach the tokenizer, the parser
//! and the transformer about each additional predicate form, the equivalent
//! basic expression is produced here first:
//!
//! ```text
//! x IN (1, 2)          =>  (x = 1 OR x = 2)
//! x NOT IN (1, 2)      =>  (x != 1 AND x != 2)
//! x BETWEEN 1 AND 9    =>  (x >= 1 AND x <= 9)
//! x LIKE 'ab%'         =>  x STARTSWITH 'ab'
//! t.x = 1              =>  x = 1
//! flag = true          =>  flag = 1
//! ```
//!
//! Two consequences are deliberate. The filter index sees the rewritten form,
//! so an `IN` list becomes several indexable equality probes instead of one
//! opaque predicate it would have to skip. And a form with no equivalent —
//! `IS NULL` until nullability lands, or a wildcard in the middle of a `LIKE`
//! pattern — returns an error naming the feature, rather than reaching a
//! tokenizer that would panic on it.

use smallvec::SmallVec;

use crate::core::row::schema::SchemaField;
use crate::core::rql::nullable;

/// Wraps a comparison so a `NULL` row cannot satisfy it.
///
/// SQL evaluates any comparison against `NULL` to *unknown*, and returns a row
/// only when the clause is *true*. The evaluator here is two-valued, so the
/// requirement is expressed as an explicit "and the column is not null". That
/// reproduces SQL's answer under `AND`, `OR` and negation alike, and stops
/// `country = ''` from matching a `NULL` row whose stored bytes happen to be
/// empty.
fn guard_null(column: &str, fragment: &str, schema: &SmallVec<[SchemaField; 20]>) -> String {
    match nullable::flag_field(column, schema) {
        None => fragment.to_string(),
        Some(flag) => format!("({} = 0 AND {})", flag.name, fragment),
    }
}

/// PostgreSQL's structural operator symbols, **longest first**.
///
/// The order is load-bearing: a prefix match runs down this list and stops at
/// the first hit, so `<<=` has to precede `<<` or the `=` would be left behind
/// and `client_ip <<= inet '…'` would silently become the strict `<<`, which
/// excludes the network itself.
///
/// `<->` is here too even though it yields a distance rather than a boolean:
/// lexing it is what lets the planner report it as an unsupported *operator*
/// rather than as an unexpected character.
pub const PG_OPERATORS: &[&str] = &["<<=", ">>=", "<->", "@>", "<@", "&&", "<<", ">>", "@@"];

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A bare word: column name, keyword, or unquoted literal.
    Word(String),
    /// A quoted string, carried with its original quoting intact.
    Str(String),
    /// An operator or punctuation run.
    Sym(String),
    LPar,
    RPar,
    Comma,
}

impl Tok {
    fn is_keyword(&self, kw: &str) -> bool {
        matches!(self, Tok::Word(w) if w.eq_ignore_ascii_case(kw))
    }

    fn render(&self) -> String {
        match self {
            Tok::Word(w) => w.clone(),
            Tok::Str(s) => s.clone(),
            Tok::Sym(s) => s.clone(),
            Tok::LPar => "(".to_string(),
            Tok::RPar => ")".to_string(),
            Tok::Comma => ",".to_string(),
        }
    }
}

fn lex(clause: &str) -> Result<Vec<Tok>, String> {
    let bytes = clause.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        let c = bytes[i];

        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        match c {
            b'\'' | b'"' => {
                let quote = c;
                let start = i;
                i += 1;
                loop {
                    if i >= bytes.len() {
                        return Err(format!("unterminated string literal at offset {}", start));
                    }
                    if bytes[i] == quote {
                        // A doubled quote is an escaped quote, not the end.
                        if i + 1 < bytes.len() && bytes[i + 1] == quote {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                // SQL splits the two quote characters by meaning: single
                // quotes make a string literal, double quotes make an
                // identifier. Keeping a double-quoted run as a `Word` lets it
                // resolve to a column; if it resolves to nothing it is rendered
                // back unchanged, which the engine's tokenizer still reads as a
                // literal.
                // A double-quoted run may be one segment of a qualified name:
                // ORMs write `"users"."name"`. Keep consuming `.segment` so the
                // whole reference stays a single token and can be resolved as
                // one column.
                if quote == b'"' {
                    while i < bytes.len() && bytes[i] == b'.' {
                        i += 1;
                        if i < bytes.len() && bytes[i] == b'"' {
                            i += 1;
                            while i < bytes.len() && bytes[i] != b'"' {
                                i += 1;
                            }
                            if i < bytes.len() {
                                i += 1;
                            }
                        } else {
                            while i < bytes.len()
                                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
                            {
                                i += 1;
                            }
                        }
                    }
                }
                let text = clause[start..i].to_string();
                out.push(if quote == b'"' {
                    Tok::Word(text)
                } else {
                    Tok::Str(text)
                });
            }
            b'(' => {
                out.push(Tok::LPar);
                i += 1;
            }
            b')' => {
                out.push(Tok::RPar);
                i += 1;
            }
            b',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            // A path into a document is one symbol, not arithmetic followed by
            // a comparison. Taken before the arms below because both would
            // otherwise claim a piece of it: `-` reads as subtraction, and the
            // `>` arm greedily takes a second `>` and produces a `>>` that is
            // not an operator anywhere in this grammar.
            b'-' if bytes.get(i + 1) == Some(&b'>') => {
                let start = i;
                i += if bytes.get(i + 2) == Some(&b'>') {
                    3
                } else {
                    2
                };
                out.push(Tok::Sym(clause[start..i].to_string()));
            }
            b'#' if bytes.get(i + 1) == Some(&b'>') => {
                let start = i;
                i += if bytes.get(i + 2) == Some(&b'>') {
                    3
                } else {
                    2
                };
                out.push(Tok::Sym(clause[start..i].to_string()));
            }
            // PostgreSQL's structural operators. Taken before the ordered
            // comparisons below, which would otherwise cut them in half: `<@`
            // would lex as `<` followed by a stray `@`, which is where
            // "unexpected character '@' in WHERE clause" came from.
            //
            // Longest first — `<<=` before `<<`, `>>=` before `>>` — or a
            // network containment test would lose its `=` and silently become
            // the strict operator.
            _ if PG_OPERATORS.iter().any(|sym| clause[i..].starts_with(*sym)) => {
                let symbol = PG_OPERATORS
                    .iter()
                    .find(|sym| clause[i..].starts_with(**sym))
                    .expect("the guard just found one");
                out.push(Tok::Sym((*symbol).to_string()));
                i += symbol.len();
            }
            b'=' | b'<' | b'>' | b'!' => {
                let start = i;
                i += 1;
                if i < bytes.len() && matches!(bytes[i], b'=' | b'>') {
                    i += 1;
                }
                out.push(Tok::Sym(clause[start..i].to_string()));
            }
            b'+' | b'-' | b'*' | b'/' | b'%' | b'^' => {
                out.push(Tok::Sym((c as char).to_string()));
                i += 1;
            }
            _ if c.is_ascii_alphanumeric() || c == b'_' || c == b'.' || c == b'$' => {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric()
                        || bytes[i] == b'_'
                        || bytes[i] == b'.'
                        || bytes[i] == b'$')
                {
                    i += 1;
                }
                out.push(Tok::Word(clause[start..i].to_string()));
            }
            // `?` is JSONB key existence: `data ? 'k'` asks whether the
            // document has that key at the top level.
            //
            // Compiled to `data->'k' <> ''` rather than given a predicate kind
            // of its own, because that form already means exactly this. A raw
            // path extract yields empty bytes for an absent key and the
            // encoded value for a present one — and a present key whose value
            // is JSON `null` encodes to something non-empty, which is the
            // distinction that separates `?` from `->> IS NOT NULL`. So the
            // rewrite is not an approximation: `{"k": null} ? 'k'` is true
            // here as it is in PostgreSQL.
            //
            // Parameters are substituted before a predicate reaches this, so a
            // `?` here is never a placeholder.
            b'?' => {
                // `?|` and `?&` take an array of keys and are a different
                // shape; refused by name rather than half-implemented.
                if matches!(bytes.get(i + 1), Some(b'|') | Some(b'&')) {
                    return Err(format!(
                        "the JSONB operator '?{}' is not supported yet;                          test each key with `?` and combine with OR/AND",
                        bytes[i + 1] as char
                    ));
                }
                let mut j = i + 1;
                while j < bytes.len() && (bytes[j] as char).is_whitespace() {
                    j += 1;
                }
                if bytes.get(j) != Some(&b'\'') {
                    return Err(
                        "the JSONB operator '?' needs a quoted key, as in `data ? 'name'`"
                            .to_string(),
                    );
                }
                // The key literal, with `''` as an escaped quote.
                let start = j;
                j += 1;
                loop {
                    match bytes.get(j) {
                        None => {
                            return Err("unterminated key after the JSONB operator '?'".to_string());
                        }
                        Some(b'\'') if bytes.get(j + 1) == Some(&b'\'') => j += 2,
                        Some(b'\'') => {
                            j += 1;
                            break;
                        }
                        Some(_) => j += 1,
                    }
                }
                let key = clause[start..j].to_string();
                out.push(Tok::Sym("->".to_string()));
                out.push(Tok::Str(key));
                out.push(Tok::Sym("<>".to_string()));
                out.push(Tok::Str("''".to_string()));
                i = j;
                continue;
            }
            // `::` is a cast, not a stray colon. Saying so sends the reader to
            // the construct rather than to a character they did not think they
            // typed.
            //
            // A cast applied to a *literal* is dropped rather than refused. The
            // column it is compared against already says what type the value
            // is, so `a = 'two'::text` asks exactly what `a = 'two'` asks, and
            // clients emit the cast form whenever a parameter's type would
            // otherwise be ambiguous. Refusing it failed ordinary reads of rows
            // that had been written by the very same client.
            //
            // Only over a literal: a cast on a column or an expression can
            // change what the comparison means, so those still say so plainly
            // rather than being silently reinterpreted.
            b':' if bytes.get(i + 1) == Some(&b':') => {
                let over_literal = matches!(out.last(), Some(Tok::Str(_)))
                    || matches!(
                        out.last(),
                        Some(Tok::Word(w))
                            if !w.is_empty()
                                && w.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-')
                    );
                // Over anything else the cast is *kept*, as one symbol, and
                // carried to the stages that can act on it.
                //
                // `(data->>'code')::bigint = 3` reads a JSON value as text and
                // asks for it to be compared as a number. Dropping the cast
                // compares the text `"3"` against the number `3` and matches
                // nothing — measured, not assumed — so the target type has to
                // travel with the operand rather than be discarded here. The
                // tokenizer turns it into a cast token and the parser applies
                // it to the value the path extracts.
                if !over_literal {
                    let mut k = i + 2;
                    while k < bytes.len() && (bytes[k] as char).is_whitespace() {
                        k += 1;
                    }
                    let name_start = k;
                    while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_')
                    {
                        k += 1;
                    }
                    if k == name_start || matches!(bytes.get(k), Some(b'(') | Some(b'[')) {
                        return Err(
                            "a `::` cast in a WHERE clause is not supported yet".to_string()
                        );
                    }
                    let cast = Tok::Sym(format!("::{}", &clause[name_start..k]));

                    // A wrapper that existed only to carry the cast comes off.
                    //
                    // In a `WHERE`, `(` opens a grouped *predicate*, so
                    // `(doc->>'n')::bigint = 3` reached the planner as
                    // "expected comparison operator, got RPar" — the group was
                    // parsed as a predicate and ended before any operator. The
                    // cast itself is kept, so removing the parentheses changes
                    // nothing about the comparison's type.
                    //
                    // Only when the group holds no arithmetic: parentheses
                    // around `a + b` are load-bearing, and dropping them would
                    // let a neighbouring `*` bind tighter and quietly change
                    // the value. Path operators bind to their own key and
                    // nothing else.
                    if matches!(out.last(), Some(Tok::RPar)) {
                        let mut depth = 0usize;
                        let mut open_at = None;
                        for (at, tok) in out.iter().enumerate().rev() {
                            match tok {
                                Tok::RPar => depth += 1,
                                Tok::LPar => {
                                    depth -= 1;
                                    if depth == 0 {
                                        open_at = Some(at);
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }
                        if let Some(open_at) = open_at {
                            let plain = out[open_at + 1..out.len() - 1].iter().all(|t| {
                                !matches!(t, Tok::Sym(sym)
                                    if matches!(sym.as_str(), "+" | "-" | "*" | "/" | "%" | "^"))
                            });
                            if plain {
                                let close_at = out.len() - 1;
                                out.remove(close_at);
                                out.remove(open_at);
                            }
                        }
                    }

                    out.push(cast);
                    i = k;
                    continue;
                }
                // Consume `::` and the type name that follows, keeping the
                // literal token already pushed.
                i += 2;
                while i < bytes.len() && (bytes[i] as char).is_whitespace() {
                    i += 1;
                }
                let name_start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                if i == name_start {
                    return Err("a `::` cast in a WHERE clause is not supported yet".to_string());
                }
                // A parameterised or array type — `numeric(10,2)`, `text[]` —
                // is not a bare name, and dropping only part of it would leave
                // stray punctuation for the parser to trip over.
                if matches!(bytes.get(i), Some(b'(') | Some(b'[')) {
                    return Err("a `::` cast in a WHERE clause is not supported yet".to_string());
                }

                // With the cast gone, a wrapper that existed only to carry it
                // goes too. `(data->>'code')::bigint` has to become
                // `data->>'code'` and not `(data->>'code')`, because a
                // parenthesised group is not a comparison operand here — it
                // reached the planner as "expected comparison operator, got
                // RPar".
                //
                // Only when the group holds no arithmetic. Parentheses around
                // `a + b` are load-bearing: dropping them would let a
                // neighbouring `*` bind tighter and silently change the value.
                // Path operators are not arithmetic — `->` and `->>` bind to
                // their own key and nothing else — so a path extract unwraps
                // safely.
                if matches!(out.last(), Some(Tok::RPar)) {
                    let mut depth = 0usize;
                    let mut open_at = None;
                    for (at, tok) in out.iter().enumerate().rev() {
                        match tok {
                            Tok::RPar => depth += 1,
                            Tok::LPar => {
                                depth -= 1;
                                if depth == 0 {
                                    open_at = Some(at);
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    if let Some(open_at) = open_at {
                        let plain = out[open_at + 1..out.len() - 1].iter().all(|t| {
                            !matches!(t, Tok::Sym(sym)
                                if matches!(sym.as_str(), "+" | "-" | "*" | "/" | "%" | "^"))
                        });
                        if plain {
                            let close_at = out.len() - 1;
                            out.remove(close_at);
                            out.remove(open_at);
                        }
                    }
                }
            }
            _ => {
                return Err(format!(
                    "unexpected character '{}' in WHERE clause",
                    c as char
                ));
            }
        }
    }

    Ok(out)
}

/// Resolves a possibly-qualified, possibly-quoted identifier to a schema column.
///
/// `users.name`, `u.name`, `"name"` and `NAME` all resolve to `name`, matching
/// Postgres' case-insensitive folding of unquoted identifiers.
fn resolve_column(raw: &str, schema: &SmallVec<[SchemaField; 20]>) -> Option<String> {
    let unquoted = raw.trim_matches('"');
    // Try the whole name first, so a column that genuinely contains a dot wins
    // over a qualifier interpretation.
    if let Some(f) = schema
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(unquoted) && !f.is_deleted)
    {
        return Some(f.name.clone());
    }
    // Each segment carries its own quoting in `"users"."name"`, so the last
    // segment has to be unquoted separately rather than relying on the trim
    // above having reached it.
    let bare = raw.rsplit('.').next()?.trim_matches('"');
    schema
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(bare) && !f.is_deleted)
        .map(|f| f.name.clone())
}

/// True when the token could begin a value: a literal, or a qualified name.
fn is_value_start(tok: &Tok) -> bool {
    matches!(tok, Tok::Str(_) | Tok::Word(_) | Tok::LPar)
}

/// Renders one value token, folding boolean keywords into the 1/0 the engine
/// stores. `NULL` is passed through for the caller to reject in context.
fn render_value(tok: &Tok) -> String {
    match tok {
        Tok::Word(w) if w.eq_ignore_ascii_case("true") => "1".to_string(),
        Tok::Word(w) if w.eq_ignore_ascii_case("false") => "0".to_string(),
        other => other.render(),
    }
}

/// Translates a `LIKE` pattern into the substring operator that matches it.
///
/// Returns the operator and the literal to compare against, or an error when
/// the pattern needs wildcard machinery the engine does not have.
fn like_to_operator(
    pattern_tok: &Tok,
    case_insensitive: bool,
) -> Result<(&'static str, String), String> {
    let raw = pattern_tok.render();
    let quote = raw.chars().next().unwrap_or('\'');
    let inner = if raw.len() >= 2 && (quote == '\'' || quote == '"') {
        &raw[1..raw.len() - 1]
    } else {
        raw.as_str()
    };

    let leading = inner.starts_with('%');
    let trailing = inner.ends_with('%') && inner.len() > 1;
    let core = inner.trim_start_matches('%').trim_end_matches('%');

    // A `_`, or a `%` that is not merely leading or trailing, cannot be reduced
    // to a prefix/suffix/substring test — `a%b` is not
    // `STARTSWITH 'a' AND CONTAINS 'b'`, which does not require `b` to follow
    // `a` and loses the ordering entirely once there are three segments. These
    // go to the pattern matcher with their wildcards intact.
    //
    // Deliberately not sargable: `sarg::op_of` only accepts `Token::Op` and five
    // comparisons, and a string operator arrives as `Token::Ident`, so this
    // yields `MaskExpr::All` and the scan is unfiltered rather than wrongly
    // pruned. `a_like_predicate_is_never_sargable` pins that.
    if inner.contains('_') || core.contains('%') {
        let op = if case_insensitive { "ILIKE" } else { "LIKE" };
        return Ok((op, format!("'{}'", inner)));
    }

    let op = match (leading, trailing, case_insensitive) {
        (true, true, false) => "CONTAINS",
        (false, true, false) => "STARTSWITH",
        (true, false, false) => "ENDSWITH",
        // No wildcard at all: LIKE degenerates to equality.
        (false, false, false) => "=",
        (true, true, true) => "ICONTAINS",
        (false, true, true) => "ISTARTSWITH",
        (true, false, true) => "IENDSWITH",
        (false, false, true) => "IEQUALS",
    };

    Ok((op, format!("'{}'", core)))
}

/// Rewrites a `WHERE` clause into the engine's basic comparison grammar.
///
/// Returns the rewritten clause, or an error naming the unsupported feature.
/// Replaces `<expr> = NULL` and `<expr> <> NULL` with a predicate that never
/// holds.
///
/// `IS NULL` and `IS NOT NULL` are left alone: those ask a different question
/// and the engine answers them.
fn fold_null_comparisons(clause: &str) -> String {
    let upper = clause.to_ascii_uppercase();
    let bytes = clause.as_bytes();
    let mut out = String::with_capacity(clause.len());
    let mut i = 0usize;
    let mut quote: Option<u8> = None;
    // Offset in `out` where the current term began, so the whole comparison can
    // be replaced rather than just its right-hand side.
    let mut term_start = 0usize;

    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = quote {
            // Whole characters, not bytes: pushing a byte of a multi-byte
            // character as a `char` re-encodes it, so `'Müller'` compared
            // against mojibake and matched nothing. Same defect, and same fix,
            // as `lexer_s1::strip_sql_comments`.
            let ch = clause[i..].chars().next().expect("in bounds");
            out.push(ch);
            if b == q {
                quote = None;
            }
            i += ch.len_utf8();
            continue;
        }
        if b == b'\'' || b == b'"' {
            quote = Some(b);
            out.push(b as char);
            i += 1;
            continue;
        }
        // A boolean separator ends the term the replacement would cover.
        for keyword in [" AND ", " OR ", "(", ")"] {
            if upper[i..].starts_with(keyword) {
                out.push_str(&clause[i..i + keyword.len()]);
                i += keyword.len();
                term_start = out.len();
                break;
            }
        }
        if i >= bytes.len() {
            break;
        }

        let is_eq =
            bytes[i] == b'=' || (upper[i..].starts_with("<>") || upper[i..].starts_with("!="));
        if is_eq {
            let width = if bytes[i] == b'=' { 1 } else { 2 };
            let rest = clause[i + width..].trim_start();
            let rest_upper = rest.to_ascii_uppercase();
            let ends_term = rest_upper == "NULL"
                || rest_upper.starts_with("NULL ")
                || rest_upper.starts_with("NULL)");
            if ends_term {
                // Everything from the start of this term through the NULL.
                out.truncate(term_start);
                out.push_str("1 = 0");
                let consumed =
                    (i + width) + (clause[i + width..].len() - rest.len()) + "NULL".len();
                i = consumed;
                continue;
            }
        }

        let ch = clause[i..].chars().next().expect("in bounds");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

pub fn rewrite_where(clause: &str, schema: &SmallVec<[SchemaField; 20]>) -> Result<String, String> {
    if clause.trim().is_empty() {
        return Ok(String::new());
    }

    // `x = NULL` is never true in SQL — NULL is not a value anything equals,
    // which is why `IS NULL` exists. Folded to a false predicate here so the
    // comparison never reaches the tokenizer, where an untyped NULL against a
    // typed column is a compile error rather than a row that does not match.
    let clause = &fold_null_comparisons(clause);

    let toks = lex(clause)?;
    let mut out: Vec<String> = Vec::with_capacity(toks.len());
    let mut i = 0usize;
    // Where the comparison currently being read began, as an index into `out`.
    //
    // A nullable column that turns up *inside* an arithmetic operand —
    // `subtotal + tax_amount > 100` — cannot guard itself by taking back the
    // two tokens before it, the way a column on the right of a comparison does:
    // the `+` is not the comparison, and popping it left the rest of the
    // comparison outside the guard's parentheses. The whole term has to be
    // wrapped, and this is where it starts.
    let mut term_start = 0usize;

    while i < toks.len() {
        let tok = &toks[i];

        // A term begins after each connective, and after a parenthesis that
        // opens a *boolean* group.
        //
        // The distinction matters: `(a = 1 OR b = 2)` is a group of terms and a
        // guard belongs inside it, while `(x - y) > z` is an operand and a
        // guard inserted inside it would turn the operand into a boolean group
        // — which is exactly what the planner then refused.
        if tok.is_keyword("and") || tok.is_keyword("or") {
            term_start = out.len() + 1;
        } else if matches!(tok, Tok::LPar) && opens_boolean_group(&toks, i) {
            term_start = out.len() + 1;
        }

        // A word that names a column may start one of the sugar forms.
        if let Tok::Word(raw) = tok {
            if let Some(col) = resolve_column(raw, schema) {
                // Look past the column at what follows.
                let next = toks.get(i + 1);

                // `col IS [NOT] NULL` and `col IS [NOT] DISTINCT FROM x`
                if next.map(|t| t.is_keyword("is")).unwrap_or(false) {
                    let negated = toks
                        .get(i + 2)
                        .map(|t| t.is_keyword("not"))
                        .unwrap_or(false);
                    let null_at = if negated { i + 3 } else { i + 2 };

                    // `IS [NOT] DISTINCT FROM` is the null-safe comparison: it
                    // answers true/false where `=` would answer NULL, which is
                    // why an ORM reaches for it on a nullable column.
                    //
                    // Expressed through the companion rather than a three-valued
                    // evaluator, like every other null-aware form here:
                    //
                    //   a IS DISTINCT FROM v      ->  (__null_a = 1 OR a != v)
                    //   a IS NOT DISTINCT FROM v  ->  (__null_a = 0 AND a = v)
                    //
                    // On a NOT NULL column there is no companion and no third
                    // outcome, so it degenerates to plain `!=` / `=`.
                    if toks
                        .get(null_at)
                        .map(|t| t.is_keyword("distinct"))
                        .unwrap_or(false)
                    {
                        if !toks
                            .get(null_at + 1)
                            .map(|t| t.is_keyword("from"))
                            .unwrap_or(false)
                        {
                            return Err("IS DISTINCT must be followed by FROM".to_string());
                        }
                        let value_at = null_at + 2;
                        let value = toks
                            .get(value_at)
                            .map(|t| t.render())
                            .ok_or("IS DISTINCT FROM names no value")?;

                        // `IS DISTINCT FROM NULL` is exactly `IS NOT NULL`, and
                        // the negation is `IS NULL`. Handled here because the
                        // rewrite below would compare against the word "NULL".
                        if value.eq_ignore_ascii_case("null") {
                            out.push(match nullable::flag_field(&col, schema) {
                                Some(flag) => format!("{} = {}", flag.name, u8::from(negated)),
                                None if negated => format!("{} != {}", col, col),
                                None => format!("{} = {}", col, col),
                            });
                            i = value_at + 1;
                            continue;
                        }

                        out.push(match nullable::flag_field(&col, schema) {
                            Some(flag) if negated => {
                                format!("({} = 0 AND {} = {})", flag.name, col, value)
                            }
                            Some(flag) => {
                                format!("({} = 1 OR {} != {})", flag.name, col, value)
                            }
                            None if negated => format!("{} = {}", col, value),
                            None => format!("{} != {}", col, value),
                        });
                        i = value_at + 1;
                        continue;
                    }

                    if !toks
                        .get(null_at)
                        .map(|t| t.is_keyword("null"))
                        .unwrap_or(false)
                    {
                        return Err(
                            "IS must be followed by NULL, NOT NULL, or [NOT] DISTINCT FROM"
                                .to_string(),
                        );
                    }

                    out.push(match nullable::flag_field(&col, schema) {
                        // The companion holds 1 for NULL, so the test is a
                        // plain equality against it.
                        Some(flag) => format!("{} = {}", flag.name, u8::from(!negated)),
                        // A `NOT NULL` column can never be NULL, so the test is
                        // a constant. Comparing the column against itself
                        // expresses both constants without needing a literal of
                        // the column's type.
                        None if negated => format!("{} = {}", col, col),
                        None => format!("{} != {}", col, col),
                    });
                    i = null_at + 1;
                    continue;
                }

                // `col = ANY('{a,b}')` and `col <> ALL('{a,b}')`.
                //
                // This is how a PostgreSQL client writes `IN` when the list is
                // a bound array rather than written out — sea-orm emits it for
                // every batched fetch by id, so an application doing ordinary
                // work hits it constantly. Rewritten into the same expansion
                // `IN` gets rather than given a path of its own.
                if let Some(rewritten) = any_or_all_expansion(&toks, i, &col, schema)? {
                    out.push(rewritten.0);
                    i = rewritten.1;
                    continue;
                }

                // `col [NOT] IN (...)` and `col [NOT] BETWEEN a AND b`
                let negated = next.map(|t| t.is_keyword("not")).unwrap_or(false);
                let kw_at = if negated { i + 2 } else { i + 1 };

                if toks.get(kw_at).map(|t| t.is_keyword("in")).unwrap_or(false) {
                    let (values, after) = parse_in_list(&toks, kw_at + 1)?;

                    // `x IN ()` arises when a subquery returned nothing. It can
                    // never hold, and `x NOT IN ()` always holds. Comparing the
                    // column against itself expresses both without needing a
                    // constant of the column's type.
                    if values.is_empty() {
                        out.push(if negated {
                            format!("{} = {}", col, col)
                        } else {
                            format!("{} != {}", col, col)
                        });
                        i = after;
                        continue;
                    }

                    let joiner = if negated { " AND " } else { " OR " };
                    let op = if negated { "!=" } else { "=" };
                    let parts: Vec<String> = values
                        .iter()
                        .map(|v| format!("{} {} {}", col, op, v))
                        .collect();
                    out.push(guard_null(
                        &col,
                        &format!("({})", parts.join(joiner)),
                        schema,
                    ));
                    i = after;
                    continue;
                }

                if toks
                    .get(kw_at)
                    .map(|t| t.is_keyword("between"))
                    .unwrap_or(false)
                {
                    let lo = toks
                        .get(kw_at + 1)
                        .filter(|t| is_value_start(t))
                        .ok_or("BETWEEN is missing its lower bound")?;
                    if !toks
                        .get(kw_at + 2)
                        .map(|t| t.is_keyword("and"))
                        .unwrap_or(false)
                    {
                        return Err("BETWEEN bounds must be separated by AND".to_string());
                    }
                    let hi = toks
                        .get(kw_at + 3)
                        .filter(|t| is_value_start(t))
                        .ok_or("BETWEEN is missing its upper bound")?;

                    // `NOT BETWEEN` is the complement: outside the range on
                    // either side, which is a disjunction rather than a
                    // conjunction.
                    let fragment = if negated {
                        format!(
                            "({} < {} OR {} > {})",
                            col,
                            render_value(lo),
                            col,
                            render_value(hi)
                        )
                    } else {
                        format!(
                            "({} >= {} AND {} <= {})",
                            col,
                            render_value(lo),
                            col,
                            render_value(hi)
                        )
                    };
                    out.push(guard_null(&col, &fragment, schema));
                    i = kw_at + 4;
                    continue;
                }

                // A `::` cast between the column and its operator —
                // `hardware_status::text SIMILAR TO '101%'`. The cast travels
                // with the column into the emitted text; the parser applies it
                // per row.
                let (kw_at, col) = match toks.get(kw_at) {
                    Some(Tok::Sym(sym)) if sym.starts_with("::") => {
                        (kw_at + 1, format!("{}{}", col, sym))
                    }
                    _ => (kw_at, col),
                };

                // `col [NOT] SIMILAR TO 'pattern'`. Two words, so it is
                // folded into the one-word operator the tokenizer's string-op
                // table can carry — the same trick `IS NULL` uses to become a
                // comparison against the nullability companion.
                if toks
                    .get(kw_at)
                    .map(|t| t.is_keyword("similar"))
                    .unwrap_or(false)
                    && toks
                        .get(kw_at + 1)
                        .map(|t| t.is_keyword("to"))
                        .unwrap_or(false)
                {
                    let pattern = toks
                        .get(kw_at + 2)
                        .ok_or("SIMILAR TO is missing its pattern")?;
                    let Tok::Str(literal) = pattern else {
                        return Err("SIMILAR TO needs a quoted pattern".to_string());
                    };
                    if negated {
                        return Err("NOT SIMILAR TO is not supported yet".to_string());
                    }
                    // `Tok::Str` keeps the quotes it was written with, so the
                    // literal is emitted as-is. Adding a pair around it made
                    // `''101%''`, which the tokenizer read as an empty string
                    // followed by a bare `101%`.
                    out.push(guard_null(
                        &col,
                        &format!("{} SIMILARTO {}", col, literal),
                        schema,
                    ));
                    i = kw_at + 3;
                    continue;
                }

                // `col [NOT] LIKE 'pattern'` / `ILIKE`
                let case_insensitive = toks
                    .get(kw_at)
                    .map(|t| t.is_keyword("ilike"))
                    .unwrap_or(false);
                let is_like = case_insensitive
                    || toks
                        .get(kw_at)
                        .map(|t| t.is_keyword("like"))
                        .unwrap_or(false);
                if is_like {
                    let pattern = toks.get(kw_at + 1).ok_or("LIKE is missing its pattern")?;
                    let (op, literal) = like_to_operator(pattern, case_insensitive)?;
                    if negated {
                        return Err("NOT LIKE is not supported yet".to_string());
                    }
                    out.push(guard_null(
                        &col,
                        &format!("{} {} {}", col, op, literal),
                        schema,
                    ));
                    i = kw_at + 2;
                    continue;
                }

                // A nullable column inside an arithmetic operand. The guard
                // is inserted in front of the whole term rather than wrapped
                // around it, which reads the same — `AND` binds tighter than
                // `OR`, so a term inside an `OR` chain is still guarded as a
                // unit — and needs nothing taken back.
                let preceded_by_arithmetic =
                    i > 0 && matches!(toks.get(i - 1), Some(Tok::Sym(sym)) if is_arithmetic(sym));
                if preceded_by_arithmetic {
                    if nullable::is_nullable(&col, schema) {
                        if let Some(flag) = nullable::flag_field(&col, schema) {
                            let at = term_start.min(out.len());
                            out.insert(at, format!("{} = 0 AND", flag.name));
                        }
                    }
                    out.push(col);
                    i += 1;
                    continue;
                }

                // A column on the right of an operator — `5 = age`, `a = b` —
                // is an operand, never a truth test; without this check the
                // `age` in `5 = age` became `age = 1` and the clause read
                // `5 = age = 1`.
                let preceded_by_operator = i > 0 && matches!(toks.get(i - 1), Some(Tok::Sym(_)));
                if preceded_by_operator {
                    // `<value> <op> <nullable col>` at the start of a term: the
                    // two tokens already emitted are the rest of this
                    // comparison, so take them back and guard the whole thing,
                    // as the left-operand form below does.
                    let term_starts_at_value = i >= 2
                        && matches!(toks.get(i - 2), Some(Tok::Word(_)) | Some(Tok::Str(_)))
                        && (i == 2
                            || matches!(toks.get(i - 3), Some(Tok::LPar))
                            || toks
                                .get(i - 3)
                                .map(|t| t.is_keyword("and") || t.is_keyword("or"))
                                .unwrap_or(false));
                    if nullable::is_nullable(&col, schema) && term_starts_at_value && out.len() >= 2
                    {
                        let op = out.pop().expect("two entries");
                        let lhs = out.pop().expect("two entries");
                        let fragment = format!("{} {} {}", lhs, op, col);
                        out.push(guard_null(&col, &fragment, schema));
                    } else {
                        out.push(col);
                    }
                    i += 1;
                    continue;
                }

                // A column standing on its own is a truth test — `WHERE flag`
                // — which the evaluator has no form for, so it becomes the
                // equality it means. "On its own" is decided by what follows:
                // an operator means this is an ordinary comparison.
                let followed_by_operator = matches!(toks.get(i + 1), Some(Tok::Sym(_)))
                    || toks
                        .get(i + 1)
                        .map(|t| {
                            t.is_keyword("contains")
                                || t.is_keyword("startswith")
                                || t.is_keyword("endswith")
                        })
                        .unwrap_or(false);

                if !followed_by_operator {
                    // `NOT flag` was emitted as a pending negation; consume it.
                    let negate = out.last().map(|s| s == "\u{1}NOT").unwrap_or(false);
                    let fragment = if negate {
                        out.pop();
                        format!("{} = 0", col)
                    } else {
                        format!("{} = 1", col)
                    };
                    out.push(guard_null(&col, &fragment, schema));
                    i += 1;
                    continue;
                }

                // An ordinary comparison. When the column can be `NULL` the
                // whole comparison has to be consumed, so the guard can wrap
                // it — emitting the column alone would leave the operator and
                // value outside the parentheses.
                if nullable::is_nullable(&col, schema) {
                    let end = comparison_end(&toks, i + 1);

                    // Unless the run that was consumed holds no comparison at
                    // all. That happens when the column is an *operand* inside
                    // parentheses — `(fulfilled_at - created_at) > duration` —
                    // where `comparison_end` stops at the closing bracket and
                    // the comparison itself is outside it. Wrapping that run
                    // produced `( … AND fulfilled_at - created_at ) > duration`:
                    // a boolean group where an operand belongs, and the planner
                    // refused the statement.
                    //
                    // The guard goes in front of the term instead, which reads
                    // the same — `AND` binds tighter than `OR` — and leaves the
                    // parentheses holding what they held.
                    let holds_comparison = toks[i + 1..end].iter().any(|t| {
                        matches!(t, Tok::Sym(sym) if !is_arithmetic(sym))
                            || t.is_keyword("contains")
                            || t.is_keyword("startswith")
                            || t.is_keyword("endswith")
                            || t.is_keyword("similarto")
                    });
                    if !holds_comparison {
                        if let Some(flag) = nullable::flag_field(&col, schema) {
                            let at = term_start.min(out.len());
                            out.insert(at, format!("{} = 0 AND", flag.name));
                        }
                        out.push(col);
                        i += 1;
                        continue;
                    }

                    let mut fragment = vec![col.clone()];
                    for tok in &toks[i + 1..end] {
                        fragment.push(match tok {
                            Tok::Word(w) => match resolve_column(w, schema) {
                                Some(c) => c,
                                None => render_value(tok),
                            },
                            other => other.render(),
                        });
                    }
                    out.push(guard_null(&col, &fragment.join(" "), schema));
                    i = end;
                    continue;
                }

                // Not nullable: emit the canonical name so the tokenizer's
                // schema lookup, which is exact-match, finds it.
                out.push(col);
                i += 1;
                continue;
            }

            // Not a column.
            if raw.eq_ignore_ascii_case("null") {
                return Err("NULL literals are not supported yet in WHERE clauses".to_string());
            }

            // `NOT` is recorded and resolved by whatever follows it. Only the
            // `NOT <boolean column>` form has an equivalent; `NOT (a AND b)`
            // would need the evaluator to negate a subtree.
            if raw.eq_ignore_ascii_case("not") {
                out.push("\u{1}NOT".to_string());
                i += 1;
                continue;
            }

            if raw.eq_ignore_ascii_case("true") || raw.eq_ignore_ascii_case("false") {
                out.push(render_value(tok));
                i += 1;
                continue;
            }

            // Words that are part of the grammar the evaluator understands.
            const PASS_THROUGH: [&str; 12] = [
                "AND",
                "OR",
                "LIKE",
                "ILIKE",
                "SIMILARTO",
                "CONTAINS",
                "STARTSWITH",
                "ENDSWITH",
                "ICONTAINS",
                "ISTARTSWITH",
                "IENDSWITH",
                "IEQUALS",
            ];
            if PASS_THROUGH.iter().any(|k| raw.eq_ignore_ascii_case(k)) {
                out.push(raw.to_ascii_uppercase());
                i += 1;
                continue;
            }

            // A bare number is a literal.
            if raw.bytes().next().is_some_and(|b| b.is_ascii_digit())
                || (raw.starts_with('-') && raw.len() > 1)
            {
                out.push(raw.clone());
                i += 1;
                continue;
            }

            // Anything else is a name the schema does not have. Reporting it
            // here matters: the tokenizer downstream panics on an unknown
            // identifier, which would kill the connection rather than fail the
            // statement.
            return Err(format!(
                "unknown column or unsupported keyword '{}' in WHERE clause",
                raw
            ));
        }

        out.push(tok.render());
        i += 1;
    }

    if out.iter().any(|s| s == "\u{1}NOT") {
        return Err(
            "NOT is only supported directly before a boolean column, as in `NOT flag`".to_string(),
        );
    }

    Ok(out.join(" "))
}

/// Finds where a comparison ends: at the next `AND`/`OR` outside parentheses,
/// at a parenthesis that closes a group opened before it, or at the end.
///
/// The result is the index just past the comparison's last token.
/// Whether the parenthesis at `at` opens a group of boolean terms rather than
/// a parenthesised operand.
///
/// Decided by what the group holds at its own top level: a comparison operator
/// or an `AND`/`OR` makes it boolean, and arithmetic alone does not.
fn opens_boolean_group(toks: &[Tok], at: usize) -> bool {
    let mut depth = 0usize;
    for tok in &toks[at..] {
        match tok {
            Tok::LPar => depth += 1,
            Tok::RPar => {
                depth -= 1;
                if depth == 0 {
                    return false;
                }
            }
            Tok::Sym(sym) if depth == 1 && !is_arithmetic(sym) && !sym.starts_with("::") => {
                return true;
            }
            t if depth == 1 && (t.is_keyword("and") || t.is_keyword("or")) => return true,
            _ => {}
        }
    }
    false
}

/// Whether a symbol is an arithmetic operator rather than a comparison.
fn is_arithmetic(symbol: &str) -> bool {
    matches!(symbol, "+" | "-" | "*" | "/" | "%" | "^")
}

fn comparison_end(toks: &[Tok], start: usize) -> usize {
    let mut depth = 0usize;
    let mut i = start;

    while i < toks.len() {
        match &toks[i] {
            Tok::LPar => depth += 1,
            Tok::RPar => {
                if depth == 0 {
                    return i;
                }
                depth -= 1;
            }
            t if depth == 0 && (t.is_keyword("and") || t.is_keyword("or")) => return i,
            _ => {}
        }
        i += 1;
    }

    i
}

/// Reads the parenthesised value list of an `IN`, returning the rendered values
/// and the index just past the closing parenthesis.
/// Expands `col = ANY('{…}')` / `col <> ALL('{…}')` into the comparison chain
/// `IN` and `NOT IN` produce, or `None` when the tokens are not that shape.
///
/// `= ANY` and `<> ALL` are the two that have an exact reading. `<> ANY` and
/// `= ALL` are deliberately left alone: they are legal SQL with quite different
/// meanings, and guessing at them would answer a question nobody asked.
fn any_or_all_expansion(
    toks: &[Tok],
    col_at: usize,
    col: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<Option<(String, usize)>, String> {
    let Some(Tok::Sym(op)) = toks.get(col_at + 1) else {
        return Ok(None);
    };
    let negated = match op.as_str() {
        "=" => false,
        "!=" | "<>" => true,
        _ => return Ok(None),
    };

    let Some(quantifier) = toks.get(col_at + 2) else {
        return Ok(None);
    };
    let quantifier = match quantifier {
        Tok::Word(w) if w.eq_ignore_ascii_case("any") && !negated => "any",
        Tok::Word(w) if w.eq_ignore_ascii_case("all") && negated => "all",
        _ => return Ok(None),
    };
    let _ = quantifier;

    if toks.get(col_at + 3) != Some(&Tok::LPar) {
        return Ok(None);
    }
    let Some(Tok::Str(literal)) = toks.get(col_at + 4) else {
        // A bound parameter — `= ANY($1)` — reaches here with the placeholder
        // unresolved. Left alone so it fails as an unsupported statement rather
        // than being rewritten into something that reads a `$1` as a value.
        return Ok(None);
    };
    if toks.get(col_at + 5) != Some(&Tok::RPar) {
        return Ok(None);
    }

    let Some(values) = pg_array_elements(literal) else {
        return Ok(None);
    };

    // An empty array: `= ANY('{}')` holds for nothing, `<> ALL('{}')` for
    // everything. Expressed by comparing the column with itself, which needs no
    // constant of the column's type.
    if values.is_empty() {
        let term = if negated {
            format!("{} = {}", col, col)
        } else {
            format!("{} != {}", col, col)
        };
        return Ok(Some((term, col_at + 6)));
    }

    let joiner = if negated { " AND " } else { " OR " };
    let op = if negated { "!=" } else { "=" };
    let parts: Vec<String> = values
        .iter()
        .map(|v| format!("{} {} {}", col, op, v))
        .collect();
    Ok(Some((
        guard_null(col, &format!("({})", parts.join(joiner)), schema),
        col_at + 6,
    )))
}

/// The elements of a PostgreSQL array literal `'{a,"b c",…}'`, each rendered as
/// a SQL literal.
///
/// `None` when the text is not an array literal, so the caller leaves the
/// statement alone rather than rewriting something it has misread.
fn pg_array_elements(literal: &str) -> Option<Vec<String>> {
    let inner = literal.trim();
    let inner = inner.strip_prefix('\'')?.strip_suffix('\'')?;
    // A doubled quote inside the literal is one quote.
    let inner = inner.replace("''", "'");
    let inner = inner.trim().strip_prefix('{')?.strip_suffix('}')?;

    if inner.trim().is_empty() {
        return Some(Vec::new());
    }

    let mut out: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escaped = false;
    for c in inner.chars() {
        if escaped {
            current.push(c);
            escaped = false;
            continue;
        }
        match c {
            '\\' if in_quotes => escaped = true,
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                out.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    if in_quotes {
        return None; // unterminated element
    }
    out.push(current);

    Some(
        out.into_iter()
            .map(|e| {
                let trimmed = e.trim();
                format!("'{}'", trimmed.replace('\'', "''"))
            })
            .collect(),
    )
}

fn parse_in_list(toks: &[Tok], start: usize) -> Result<(Vec<String>, usize), String> {
    if toks.get(start) != Some(&Tok::LPar) {
        return Err("IN must be followed by a parenthesised value list".to_string());
    }

    let mut values = Vec::new();
    let mut i = start + 1;

    loop {
        match toks.get(i) {
            None => return Err("unterminated IN value list".to_string()),
            Some(Tok::RPar) => {
                i += 1;
                break;
            }
            Some(Tok::Comma) => {
                i += 1;
            }
            Some(Tok::LPar) => {
                return Err("subqueries inside IN are not supported yet".to_string());
            }
            Some(t) => {
                if t.is_keyword("select") {
                    return Err("subqueries inside IN are not supported yet".to_string());
                }
                values.push(render_value(t));
                i += 1;
            }
        }
    }

    Ok((values, i))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db_type::DbType;

    fn schema() -> SmallVec<[SchemaField; 20]> {
        let mut s = SmallVec::new();
        for (i, (name, ty)) in [
            ("id", DbType::I32),
            ("name", DbType::STRING),
            ("country", DbType::STRING),
            ("age", DbType::I32),
            ("active", DbType::BOOL),
        ]
        .into_iter()
        .enumerate()
        {
            s.push(SchemaField::new(
                name.to_string(),
                ty.clone(),
                ty.get_size(),
                0,
                i as u64,
                false,
            ));
        }
        s
    }

    fn rw(clause: &str) -> String {
        rewrite_where(clause, &schema()).unwrap()
    }

    fn err(clause: &str) -> String {
        rewrite_where(clause, &schema()).unwrap_err()
    }

    #[test]
    fn plain_comparisons_pass_through() {
        assert_eq!(rw("id = 5"), "id = 5");
        assert_eq!(
            rw("age >= 18 AND name = 'bob'"),
            "age >= 18 AND name = 'bob'"
        );
    }

    #[test]
    fn in_becomes_a_disjunction_of_equalities() {
        assert_eq!(
            rw("country IN ('BG', 'DE')"),
            "(country = 'BG' OR country = 'DE')"
        );
        assert_eq!(rw("id IN (1)"), "(id = 1)");
    }

    #[test]
    fn not_in_becomes_a_conjunction_of_inequalities() {
        assert_eq!(rw("country NOT IN ('DE')"), "(country != 'DE')");
        assert_eq!(rw("id NOT IN (1, 2)"), "(id != 1 AND id != 2)");
    }

    #[test]
    fn between_becomes_two_bounds() {
        assert_eq!(rw("age BETWEEN 18 AND 65"), "(age >= 18 AND age <= 65)");
    }

    /// The complement of a range is a disjunction, not a conjunction — getting
    /// this backwards would silently return no rows.
    #[test]
    fn not_between_is_the_complement_not_the_negated_bounds() {
        assert_eq!(rw("age NOT BETWEEN 18 AND 65"), "(age < 18 OR age > 65)");
    }

    #[test]
    fn like_maps_each_wildcard_shape_to_its_operator() {
        assert_eq!(rw("name LIKE 'ab%'"), "name STARTSWITH 'ab'");
        assert_eq!(rw("name LIKE '%ab'"), "name ENDSWITH 'ab'");
        assert_eq!(rw("name LIKE '%ab%'"), "name CONTAINS 'ab'");
        // No wildcard: LIKE is just equality.
        assert_eq!(rw("name LIKE 'ab'"), "name = 'ab'");
    }

    /// `ILIKE` lowers to the case-insensitive counterparts, which are separate
    /// operators so the fingerprint index — which hashes raw bytes — declines
    /// them instead of silently dropping rows whose case differs.
    #[test]
    fn ilike_lowers_to_case_insensitive_operators() {
        assert_eq!(rw("name ILIKE '%ab%'"), "name ICONTAINS 'ab'");
        assert_eq!(rw("name ILIKE 'ab%'"), "name ISTARTSWITH 'ab'");
        assert_eq!(rw("name ILIKE '%ab'"), "name IENDSWITH 'ab'");
        assert_eq!(rw("name ILIKE 'ab'"), "name IEQUALS 'ab'");
        // LIKE stays case-sensitive.
        assert_eq!(rw("name LIKE '%ab%'"), "name CONTAINS 'ab'");
    }

    #[test]
    fn like_patterns_the_simple_operators_cannot_express_become_a_pattern_test() {
        // These used to be refused, and refusing was right at the time: `a%b` is
        // not `STARTSWITH 'a' AND CONTAINS 'b'`, which does not require `b` to
        // follow `a`. Now there is a matcher behind them, so they rewrite to a
        // `LIKE` carrying the pattern with its wildcards intact.
        assert_eq!(rw("name LIKE 'a%b'"), "name LIKE 'a%b'");
        assert_eq!(rw("name LIKE 'a_b'"), "name LIKE 'a_b'");
        assert_eq!(rw("name ILIKE 'a%b'"), "name ILIKE 'a%b'");

        // The shapes that *can* be reduced still are, so the cheaper operators
        // — and `STARTSWITH`'s range lowering in the filter — are not lost.
        assert_eq!(rw("name LIKE 'abc%'"), "name STARTSWITH 'abc'");
        assert_eq!(rw("name LIKE '%abc'"), "name ENDSWITH 'abc'");
        assert_eq!(rw("name LIKE '%abc%'"), "name CONTAINS 'abc'");
        assert_eq!(rw("name LIKE 'abc'"), "name = 'abc'");
    }

    #[test]
    fn qualified_and_quoted_column_names_resolve() {
        assert_eq!(rw("users.name = 'bob'"), "name = 'bob'");
        assert_eq!(rw("u.name = 'bob'"), "name = 'bob'");
        assert_eq!(rw("\"name\" = 'bob'"), "name = 'bob'");
    }

    /// Postgres folds unquoted identifiers to lower case, so a client may send
    /// any casing for a column the schema stores in one canonical form.
    #[test]
    fn column_names_resolve_case_insensitively() {
        assert_eq!(rw("NAME = 'bob'"), "name = 'bob'");
        assert_eq!(rw("Country IN ('BG')"), "(country = 'BG')");
    }

    #[test]
    fn boolean_literals_fold_to_stored_form() {
        assert_eq!(rw("active = true"), "active = 1");
        assert_eq!(rw("active = FALSE"), "active = 0");
    }

    #[test]
    fn string_literals_keep_their_contents() {
        assert_eq!(rw("name = 'O''Brien'"), "name = 'O''Brien'");
        // A keyword inside a literal is data, not syntax.
        assert_eq!(rw("name = 'BETWEEN'"), "name = 'BETWEEN'");
        assert_eq!(rw("name = 'a IN b'"), "name = 'a IN b'");
    }

    #[test]
    fn parentheses_and_grouping_survive() {
        assert_eq!(
            rw("(country = 'BG' OR country = 'DE') AND active = true"),
            "( country = 'BG' OR country = 'DE' ) AND active = 1"
        );
    }

    #[test]
    fn sugar_forms_compose_with_boolean_operators() {
        assert_eq!(
            rw("country IN ('BG', 'DE') AND age BETWEEN 18 AND 65"),
            "(country = 'BG' OR country = 'DE') AND (age >= 18 AND age <= 65)"
        );
    }

    #[test]
    fn a_bare_boolean_column_becomes_an_equality() {
        assert_eq!(rw("active"), "active = 1");
        assert_eq!(rw("NOT active"), "active = 0");
        assert_eq!(rw("active AND age > 3"), "active = 1 AND age > 3");
    }

    /// The tokenizer downstream panics on an identifier it cannot find, which
    /// takes the connection with it. Every such name has to be caught here.
    #[test]
    fn unknown_identifiers_are_rejected_rather_than_passed_on() {
        assert!(err("nosuchcolumn = 1").contains("unknown column"));
        assert!(err("EXISTS (SELECT 1)").contains("unknown column"));
        assert!(err("NOT (id = 1 AND age = 2)").contains("NOT is only supported"));
    }

    #[test]
    fn unsupported_forms_name_the_missing_feature() {
        assert!(err("id IN (SELECT id FROM t)").contains("subqueries"));
        assert!(err("name NOT LIKE 'a%'").contains("NOT LIKE"));
    }

    #[test]
    fn a_comparison_against_null_matches_nothing_rather_than_failing() {
        // This used to be refused with a message about NULL literals, on the
        // grounds that the author probably meant `IS NULL`. PostgreSQL does not
        // refuse it — `x = NULL` is simply never true — and the difference is
        // not academic: a claim query of the form
        // `WHERE id = (SELECT … LIMIT 1)` substitutes NULL when the subquery
        // finds nothing, so refusing turned "no work to claim" into a hard
        // error on every worker tick.
        assert_eq!(rw("country = NULL"), "1 = 0");
        assert_eq!(rw("country <> NULL"), "1 = 0");
        // The whole comparison goes, not just its right-hand side.
        assert_eq!(rw("id = 5 AND country = NULL"), "id = 5 AND 1 = 0");
        // `IS NULL` asks a different question and must not be folded away.
        assert_ne!(rw("country IS NULL"), "1 = 0");
    }

    /// A schema where `country` is nullable and `id` is not.
    fn nullable_schema() -> SmallVec<[SchemaField; 20]> {
        let mut s = schema();
        s.push(SchemaField::new(
            nullable::flag_column_name("country"),
            DbType::BOOL,
            1,
            0,
            s.len() as u64,
            false,
        ));
        s
    }

    fn rwn(clause: &str) -> String {
        rewrite_where(clause, &nullable_schema()).unwrap()
    }

    #[test]
    fn non_ascii_literals_pass_through_byte_for_byte() {
        assert_eq!(rw("name = 'Müller'"), "name = 'Müller'");
        assert_eq!(
            rw("name = '日本語' AND id = 1"),
            "name = '日本語' AND id = 1"
        );
        assert_eq!(rw("name != 'café'"), "name != 'café'");
        assert_eq!(
            rwn("country = 'Ελλάδα'"),
            "(__null_country = 0 AND country = 'Ελλάδα')"
        );
    }

    #[test]
    fn a_column_on_the_right_of_an_operator_is_an_operand_not_a_truth_test() {
        assert_eq!(rw("5 = age"), "5 = age");
        assert_eq!(rw("18 <= age AND active"), "18 <= age AND active = 1");
        assert_eq!(rw("id = age"), "id = age");
        assert_eq!(rw("(5 = age)"), "( 5 = age )");
    }

    #[test]
    fn a_nullable_column_on_the_right_is_guarded_like_one_on_the_left() {
        assert_eq!(
            rwn("'BG' = country"),
            "(__null_country = 0 AND 'BG' = country)"
        );
        assert_eq!(
            rwn("id = 1 AND 'BG' = country"),
            "id = 1 AND (__null_country = 0 AND 'BG' = country)"
        );
        assert_eq!(
            rwn("name = country"),
            "(__null_country = 0 AND name = country)"
        );
    }

    #[test]
    fn is_null_tests_the_companion_column() {
        assert_eq!(rwn("country IS NULL"), "__null_country = 1");
        assert_eq!(rwn("country IS NOT NULL"), "__null_country = 0");
    }

    /// A `NOT NULL` column can never be `NULL`, so the test is a constant. It
    /// must still be expressible, because an ORM emits `IS NULL` without
    /// knowing the constraint.
    #[test]
    fn is_null_on_a_non_nullable_column_is_a_constant() {
        assert_eq!(rw("id IS NULL"), "id != id");
        assert_eq!(rw("id IS NOT NULL"), "id = id");
    }

    /// The guard is what stops `country = ''` matching a `NULL` row: the bytes
    /// stored for a `NULL` string are also empty.
    #[test]
    fn comparisons_against_a_nullable_column_are_guarded() {
        assert_eq!(
            rwn("country = 'BG'"),
            "(__null_country = 0 AND country = 'BG')"
        );
        assert_eq!(
            rwn("country != 'BG'"),
            "(__null_country = 0 AND country != 'BG')"
        );
        assert_eq!(rwn("country = ''"), "(__null_country = 0 AND country = '')");
    }

    #[test]
    fn a_non_nullable_column_is_not_guarded() {
        assert_eq!(rwn("id = 5"), "id = 5");
    }

    #[test]
    fn the_guard_survives_composition_with_and_or_and_groups() {
        assert_eq!(
            rwn("country = 'BG' AND id = 5"),
            "(__null_country = 0 AND country = 'BG') AND id = 5"
        );
        assert_eq!(
            rwn("id = 5 OR country = 'DE'"),
            "id = 5 OR (__null_country = 0 AND country = 'DE')"
        );
        assert_eq!(
            rwn("(country = 'BG' OR id = 5) AND age > 1"),
            "( (__null_country = 0 AND country = 'BG') OR id = 5 ) AND age > 1"
        );
    }

    #[test]
    fn the_sugar_forms_are_guarded_too() {
        assert_eq!(
            rwn("country IN ('BG', 'DE')"),
            "(__null_country = 0 AND (country = 'BG' OR country = 'DE'))"
        );
        assert_eq!(
            rwn("country NOT IN ('DE')"),
            "(__null_country = 0 AND (country != 'DE'))"
        );
        assert_eq!(
            rwn("country LIKE 'B%'"),
            "(__null_country = 0 AND country STARTSWITH 'B')"
        );
    }

    /// `NOT IN` is where the guard changes the answer: SQL excludes a row whose
    /// column is `NULL`, because `NULL != 'DE'` is unknown rather than true.
    #[test]
    fn not_in_excludes_null_rows() {
        let rewritten = rwn("country NOT IN ('DE')");
        assert!(
            rewritten.starts_with("(__null_country = 0 AND"),
            "a NULL row must not satisfy NOT IN: {}",
            rewritten
        );
    }

    #[test]
    fn malformed_input_is_an_error_not_a_panic() {
        assert!(rewrite_where("name = 'unterminated", &schema()).is_err());
        assert!(rewrite_where("id IN (", &schema()).is_err());
        assert!(rewrite_where("age BETWEEN 1", &schema()).is_err());
        assert!(rewrite_where("age BETWEEN 1 OR 2", &schema()).is_err());
    }

    /// An empty list is what a subquery returning nothing leaves behind. It has
    /// to become a predicate that cannot hold, not a syntax error and not a
    /// predicate that matches everything.
    #[test]
    fn an_empty_in_list_can_never_hold() {
        assert_eq!(rw("id IN ()"), "id != id");
        assert_eq!(rw("id NOT IN ()"), "id = id");
    }

    #[test]
    fn empty_clause_stays_empty() {
        assert_eq!(rw(""), "");
        assert_eq!(rw("   "), "");
    }
}

#[cfg(test)]
mod any_all_tests {
    use super::*;
    use crate::core::db_type::DbType;

    fn schema() -> SmallVec<[SchemaField; 20]> {
        let mut s: SmallVec<[SchemaField; 20]> = SmallVec::new();
        for (i, (name, ty)) in [("id", DbType::STRING), ("age", DbType::I32)]
            .into_iter()
            .enumerate()
        {
            s.push(SchemaField::new(
                name.to_string(),
                ty,
                12,
                0,
                i as u64,
                false,
            ));
        }
        s
    }

    /// A PostgreSQL client writes `IN` as `= ANY(array)` whenever the list is
    /// bound rather than written out — sea-orm does it for every batched fetch
    /// by id, so this is an ordinary application's hot read path.
    #[test]
    fn equals_any_becomes_the_same_chain_as_in() {
        let out = rewrite_where("id = ANY('{\"a\",\"b\"}')", &schema()).expect("rewrites");
        assert!(out.contains("id = 'a'"), "{}", out);
        assert!(out.contains("id = 'b'"), "{}", out);
        assert!(out.contains(" OR "), "{}", out);
    }

    #[test]
    fn unquoted_and_spaced_elements_are_read() {
        let out = rewrite_where("id = ANY('{a,b c}')", &schema()).expect("rewrites");
        assert!(out.contains("id = 'a'"), "{}", out);
        assert!(out.contains("id = 'b c'"), "{}", out);
    }

    #[test]
    fn not_equals_all_is_the_negation() {
        let out = rewrite_where("id <> ALL('{\"a\",\"b\"}')", &schema()).expect("rewrites");
        assert!(out.contains("id != 'a'"), "{}", out);
        assert!(out.contains("id != 'b'"), "{}", out);
        assert!(out.contains(" AND "), "{}", out);
    }

    /// An empty array holds for nothing under `= ANY` and for everything under
    /// `<> ALL`, and neither needs a constant of the column's type.
    #[test]
    fn an_empty_array_is_expressed_without_a_typed_constant() {
        let out = rewrite_where("id = ANY('{}')", &schema()).expect("rewrites");
        assert!(out.contains("id != id"), "{}", out);
        let out = rewrite_where("id <> ALL('{}')", &schema()).expect("rewrites");
        assert!(out.contains("id = id"), "{}", out);
    }

    /// `<> ANY` and `= ALL` are legal SQL meaning something else entirely.
    /// Guessing at them would answer a question nobody asked, so they are left
    /// for the parser to refuse.
    #[test]
    fn the_quantifiers_without_an_exact_reading_are_left_alone() {
        for clause in ["id <> ANY('{\"a\"}')", "id = ALL('{\"a\"}')"] {
            let out = rewrite_where(clause, &schema());
            if let Ok(text) = out {
                assert!(
                    !text.contains(" OR ") && !text.contains(" AND id !="),
                    "{} should not have been expanded: {}",
                    clause,
                    text
                );
            }
        }
    }

    /// A bound array parameter has no elements to expand yet.
    #[test]
    fn a_bound_parameter_is_not_rewritten() {
        let out = rewrite_where("id = ANY($1)", &schema());
        if let Ok(text) = out {
            assert!(!text.contains(" OR "), "{}", text);
        }
    }

    #[test]
    fn a_uuid_batch_expands() {
        let clause = "id = ANY('{\"8bf0ea43-f179-41c6-ae8f-4a1354be8bdf\",\
                     \"1bb0afa7-1562-4f76-b200-81ce381abd10\"}')";
        let out = rewrite_where(clause, &schema()).expect("rewrites");
        assert!(
            out.contains("id = '8bf0ea43-f179-41c6-ae8f-4a1354be8bdf'"),
            "{}",
            out
        );
        assert!(
            out.contains("id = '1bb0afa7-1562-4f76-b200-81ce381abd10'"),
            "{}",
            out
        );
    }
}
