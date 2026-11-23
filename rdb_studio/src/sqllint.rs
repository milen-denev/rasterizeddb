//! Checks a buffer of SQL as it is typed, for the mistakes worth catching
//! before the server does — or that the server will not catch at all.
//!
//! # What it looks for, and what it deliberately does not
//!
//! Two kinds of finding. **Errors** are statements the server will refuse:
//! an unclosed string or bracket, a trailing comma. Catching those here only
//! saves a round trip. **Warnings** are the ones that matter more, because the
//! server will run them happily:
//!
//! - an `UPDATE` or `DELETE` with no `WHERE`, which rewrites the whole table;
//! - `= NULL`, which is never true and so silently matches nothing;
//! - two statements with the `;` between them missing, which a scratch buffer
//!   of half a dozen statements produces constantly;
//! - a table name that is not in the database, when the console knows the
//!   table list;
//! - a keyword one slip away from a real one — `FORM`, `WHRE`, `SELCT`.
//!
//! It is not a parser, and every rule here is written to stay quiet when it is
//! unsure. A linter that cries wolf on valid SQL teaches people to ignore it,
//! and then it is worse than none: so each rule names the shapes it must not
//! fire on, and the tests pin them. Nothing here ever stops a statement from
//! running — the server is the authority, this is a second pair of eyes.

use std::collections::HashSet;

use crate::sqllex::{self, Carry, Kind, Lexed, Located};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    Error,
    Warning,
}

/// One finding, anchored to a span of one line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub line: usize,
    /// Byte offsets within the line.
    pub start: usize,
    pub end: usize,
    pub severity: Severity,
    pub message: String,
}

/// What the linter knows about the database, beyond the text.
#[derive(Debug, Default, Clone, Copy)]
pub struct Context<'a> {
    /// Every table name, lower-cased, when it is safe to say that anything
    /// else is not a table. `None` turns the unknown-table check off — against
    /// PostgreSQL, whose list leaves out views, it would cry wolf on every one.
    pub tables: Option<&'a HashSet<String>>,
    /// Other names the console knows, lower-cased — columns — so that a column
    /// called `form` is never "corrected" to `FROM`.
    pub names: Option<&'a HashSet<String>>,
}

pub fn lint(text: &str, context: Context<'_>) -> Vec<Diagnostic> {
    let lexed = sqllex::lex(text);
    let mut out = Vec::new();

    if let Some((line, column, carry)) = &lexed.open {
        let (width, what) = match carry {
            Carry::Comment(_) => (2, "this comment is never closed — it needs a `*/`"),
            Carry::String { .. } => (1, "this string is never closed — it needs a `'`"),
            Carry::Quoted => (1, "this quoted name is never closed — it needs a `\"`"),
            Carry::Dollar(_) => (1, "this $-quoted body is never closed"),
            Carry::None => (1, ""),
        };
        let line_len = lexed.lines[*line].len();
        out.push(Diagnostic {
            line: *line,
            start: *column,
            end: (*column + width).min(line_len).max(*column),
            severity: Severity::Error,
            message: what.to_string(),
        });
    }

    let tokens: Vec<Tok<'_>> = significant(&lexed);
    let created = created_names(&tokens);

    for statement in tokens.split(|token| token.is(";")) {
        if !statement.is_empty() {
            Statement::new(statement).check(context, &created, &mut out);
        }
    }

    out.sort_by_key(|d| (d.line, d.start, d.severity));
    out.dedup();
    out
}

/// A token with what the rules need of it precomputed.
#[derive(Debug, Clone)]
struct Tok<'a> {
    line: usize,
    start: usize,
    end: usize,
    kind: Kind,
    text: &'a str,
    /// Upper-cased, for words; empty otherwise.
    upper: String,
    /// Nothing but whitespace and comments before it on its line.
    first_on_line: bool,
}

impl Tok<'_> {
    fn is(&self, punct: &str) -> bool {
        matches!(self.kind, Kind::Punct | Kind::Operator) && self.text == punct
    }

    fn word(&self, upper: &str) -> bool {
        self.kind == Kind::Word && self.upper == upper
    }

    fn any_word(&self, words: &[&str]) -> bool {
        self.kind == Kind::Word && words.contains(&self.upper.as_str())
    }

    /// A word that is not a keyword, or a quoted name: something that names a
    /// table, a column or an alias.
    fn is_name(&self) -> bool {
        match self.kind {
            Kind::Quoted => true,
            Kind::Word => !sqllex::is_keyword(&self.upper) && !sqllex::is_type(&self.upper),
            _ => false,
        }
    }
}

fn significant<'a>(lexed: &Lexed<'a>) -> Vec<Tok<'a>> {
    let mut out: Vec<Tok<'a>> = Vec::new();
    let mut last_line = usize::MAX;
    for located in &lexed.tokens {
        let Located { line, token } = *located;
        if token.kind == Kind::Comment {
            continue;
        }
        let text = lexed.text(located);
        out.push(Tok {
            line,
            start: token.start,
            end: token.end,
            kind: token.kind,
            text,
            upper: if token.kind == Kind::Word { text.to_ascii_uppercase() } else { String::new() },
            first_on_line: line != last_line,
        });
        last_line = line;
    }
    out
}

/// Tables and views the buffer itself creates or renames into being, so that
/// `CREATE TABLE t (…); INSERT INTO t …` does not call `t` unknown before the
/// table list has been refreshed.
fn created_names(tokens: &[Tok<'_>]) -> HashSet<String> {
    let mut names = HashSet::new();
    for (index, token) in tokens.iter().enumerate() {
        let creates = token.any_word(&["TABLE", "VIEW"])
            && tokens[..index].iter().rev().take(4).any(|t| t.word("CREATE"));
        let renames = token.word("TO") && index > 0 && tokens[index - 1].word("RENAME");
        if !creates && !renames {
            continue;
        }
        // `IF NOT EXISTS`
        let mut at = index + 1;
        while tokens.get(at).is_some_and(|t| t.any_word(&["IF", "NOT", "EXISTS"])) {
            at += 1;
        }
        if let Some(name) = tokens.get(at).filter(|t| matches!(t.kind, Kind::Word | Kind::Quoted)) {
            names.insert(sqllex::identifier(name.kind, name.text));
        }
    }
    names
}

/// Words that begin a clause, and so mean a comma before them was a mistake.
const AFTER_LIST: &[&str] = &[
    "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "RETURNING", "UNION",
    "INTERSECT", "EXCEPT", "WINDOW",
];

/// Words after which a list cannot have started yet.
const BEFORE_LIST: &[&str] = &["SELECT", "BY", "SET", "RETURNING", "DISTINCT", "VALUES"];

/// The clause keywords the `= NULL` rule reads its context from.
const CLAUSES: &[&str] = &[
    "SELECT", "FROM", "WHERE", "SET", "VALUES", "HAVING", "ON", "GROUP", "ORDER", "LIMIT",
    "RETURNING", "WHEN", "THEN", "ELSE", "USING",
];

/// Statements the missing-semicolon rule is willing to judge. Anything else —
/// `ALTER TABLE … DROP COLUMN` on two lines, `CREATE RULE … DO INSTEAD SELECT`,
/// `GRANT SELECT, INSERT` — has a keyword in the middle that looks like a new
/// statement and is not.
const SEMICOLON_CHECKED: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "SHOW", "SET", "TRUNCATE", "BEGIN",
    "COMMIT", "ROLLBACK", "VALUES", "CREATE",
];

/// Words that begin a statement when they begin a line.
const NEW_STATEMENT: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "SHOW", "TRUNCATE",
];

/// Words a statement can continue after, onto a line that starts with one of
/// [`NEW_STATEMENT`]: `UNION\nSELECT`, `DO\nUPDATE`, `FOR\nUPDATE`, `ON\nDELETE`.
const CONTINUES: &[&str] = &[
    "UNION", "ALL", "INTERSECT", "EXCEPT", "DISTINCT", "AS", "DO", "FOR", "OR", "ON", "THEN",
    "ELSE", "INSTEAD", "AFTER", "BEFORE", "EXPLAIN", "ANALYZE", "VERBOSE", "NOT", "EXISTS",
    "IN", "ANY", "SOME", "BY",
];

/// Every word a statement can begin with — far more than the typo check
/// targets, so that a real statement close to one of those is never flagged.
const STARTERS: &[&str] = &[
    "ABORT", "ADD", "ALTER", "ANALYZE", "ARCHIVE", "BACKUP", "BEGIN", "CALL", "CHECKPOINT",
    "CLOSE", "CLUSTER", "COMMENT", "COMMIT", "COPY", "CREATE", "DEALLOCATE", "DECLARE",
    "DELETE", "DESCRIBE", "DISABLE", "DISCARD", "DO", "DROP", "ENABLE", "END", "EXECUTE",
    "EXPLAIN", "FETCH", "GRANT", "IMPORT", "INSERT", "LISTEN", "LOAD", "LOCK", "MERGE", "MOVE",
    "NOTIFY", "PAUSE", "PREPARE", "PRUNE", "REASSIGN", "REBALANCE", "REFRESH", "REINDEX",
    "RELEASE", "RESET", "RESTORE", "RESUME", "RESYNC", "REVOKE", "ROLLBACK", "SAVEPOINT",
    "SECURITY", "SELECT", "SET", "SHOW", "START", "TABLE", "TRUNCATE", "UNLISTEN", "UPDATE",
    "USE", "VACUUM", "VALUES", "WITH",
];

/// The statement words a typo is checked against.
const STARTER_TARGETS: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "SHOW", "WITH",
    "TRUNCATE", "EXPLAIN", "BEGIN", "COMMIT", "ROLLBACK", "VALUES", "GRANT", "REVOKE",
];

/// The mid-statement words a typo is checked against.
const CLAUSE_TARGETS: &[&str] = &["FROM", "WHERE", "JOIN", "ORDER", "GROUP", "HAVING", "LIMIT"];

struct Statement<'s, 'a> {
    tokens: &'s [Tok<'a>],
    /// How many brackets enclose each token. A bracket itself is counted at
    /// the depth outside it.
    depth: Vec<usize>,
    /// For each `(`, its `)` if it has one.
    close: Vec<Option<usize>>,
    /// The innermost `(` around each token.
    enclosing: Vec<Option<usize>>,
    /// Whether a `(` opens a function's arguments rather than a subquery or a
    /// group — `EXTRACT(year FROM ts)` has a `FROM` that is not a table's.
    call: Vec<bool>,
}

impl<'s, 'a> Statement<'s, 'a> {
    fn new(tokens: &'s [Tok<'a>]) -> Self {
        let n = tokens.len();
        let mut depth = vec![0; n];
        let mut close = vec![None; n];
        let mut enclosing = vec![None; n];
        let mut call = vec![false; n];
        let mut stack: Vec<usize> = Vec::new();

        for (index, token) in tokens.iter().enumerate() {
            if token.is(")")
                && let Some(open) = stack.pop()
            {
                close[open] = Some(index);
            }
            depth[index] = stack.len();
            enclosing[index] = stack.last().copied();
            if token.is("(") {
                call[index] = index > 0 && {
                    let previous = &tokens[index - 1];
                    (previous.kind == Kind::Word
                        && (!sqllex::is_keyword(&previous.upper)
                            || sqllex::FUNCTION_KEYWORDS.contains(&previous.upper.as_str())))
                        || previous.kind == Kind::Quoted
                };
                stack.push(index);
            }
        }

        Statement { tokens, depth, close, enclosing, call }
    }

    fn get(&self, index: usize) -> Option<&Tok<'a>> {
        self.tokens.get(index)
    }

    fn check(&self, context: Context<'_>, created: &HashSet<String>, out: &mut Vec<Diagnostic>) {
        self.brackets(out);
        self.commas(out);
        self.null_comparisons(out);
        self.missing_semicolons(out);
        self.unfiltered_writes(out);
        self.typos(context, out);
        if let Some(tables) = context.tables {
            self.unknown_tables(tables, created, out);
        }
    }

    fn report(&self, out: &mut Vec<Diagnostic>, from: usize, to: usize, severity: Severity, message: String) {
        let first = &self.tokens[from];
        let last = &self.tokens[to];
        // A span is one line: if the two ends are on different lines, the
        // finding is anchored to the first token alone.
        let end = if last.line == first.line { last.end } else { first.end };
        out.push(Diagnostic { line: first.line, start: first.start, end, severity, message });
    }

    fn brackets(&self, out: &mut Vec<Diagnostic>) {
        let mut open: Vec<usize> = Vec::new();
        for (index, token) in self.tokens.iter().enumerate() {
            if token.is("(") {
                open.push(index);
            } else if token.is(")") && open.pop().is_none() {
                self.report(out, index, index, Severity::Error, "this `)` closes nothing".into());
            }
        }
        for index in open {
            self.report(out, index, index, Severity::Error, "this `(` is never closed".into());
        }
    }

    fn commas(&self, out: &mut Vec<Diagnostic>) {
        for (index, token) in self.tokens.iter().enumerate() {
            if !token.is(",") {
                continue;
            }
            let next = self.get(index + 1);
            let previous = index.checked_sub(1).and_then(|i| self.get(i));

            let message = match next {
                None => Some("a trailing comma ends this statement".to_string()),
                Some(next) if next.is(")") => Some("a trailing comma before `)`".to_string()),
                Some(next) if next.is(",") => Some("two commas with nothing between them".to_string()),
                Some(next) if next.any_word(AFTER_LIST) => {
                    Some(format!("a trailing comma before {}", next.upper))
                }
                _ => match previous {
                    Some(previous) if previous.is("(") => {
                        Some("a list cannot begin with a comma".to_string())
                    }
                    Some(previous) if previous.any_word(BEFORE_LIST) => {
                        Some(format!("nothing between {} and this comma", previous.upper))
                    }
                    _ => None,
                },
            };
            if let Some(message) = message {
                self.report(out, index, index, Severity::Error, message);
            }
        }
    }

    /// `x = NULL` is NULL, not true, for every `x` — so a `WHERE` built on it
    /// matches nothing and says nothing. In a `SET` it is an assignment, and
    /// fine.
    fn null_comparisons(&self, out: &mut Vec<Diagnostic>) {
        let mut clause: Vec<Option<&str>> = vec![None];
        for (index, token) in self.tokens.iter().enumerate() {
            let depth = self.depth[index];
            if token.is("(") {
                clause.truncate(depth + 1);
                clause.push(None);
                continue;
            }
            clause.resize(depth + 1, None);
            if token.any_word(CLAUSES) {
                clause[depth] = Some(token.upper.as_str());
                continue;
            }
            let negated = match token.text {
                "=" => false,
                "<>" | "!=" => true,
                _ => continue,
            };
            if token.kind != Kind::Operator {
                continue;
            }
            let Some(next) = self.get(index + 1) else { continue };
            if !next.word("NULL") {
                continue;
            }
            let assignment = clause[depth] == Some("SET")
                || self.tokens[0].any_word(&["SET", "RESET", "ALTER", "CREATE"]);
            if assignment {
                continue;
            }
            let message = if negated {
                format!("`{} NULL` is never true — use IS NOT NULL", token.text)
            } else {
                "`= NULL` is never true — use IS NULL".to_string()
            };
            self.report(out, index, index + 1, Severity::Warning, message);
        }
    }

    fn missing_semicolons(&self, out: &mut Vec<Diagnostic>) {
        let first = &self.tokens[0];
        if !first.any_word(SEMICOLON_CHECKED) {
            return;
        }
        for (index, token) in self.tokens.iter().enumerate().skip(1) {
            if !token.first_on_line || self.depth[index] != 0 || !token.any_word(NEW_STATEMENT) {
                continue;
            }
            // `INSERT INTO t (…)\nSELECT …` and `CREATE TABLE t AS\nSELECT …`
            // are one statement.
            if token.word("SELECT") && first.any_word(&["INSERT", "CREATE"]) {
                continue;
            }
            let previous = &self.tokens[index - 1];
            let ends_something = match previous.kind {
                Kind::Word => !previous.any_word(CONTINUES),
                Kind::Quoted | Kind::String | Kind::Number | Kind::Param => true,
                _ => previous.is(")") || previous.is("*"),
            };
            if ends_something {
                self.report(
                    out,
                    index,
                    index,
                    Severity::Warning,
                    format!("{} starts a new statement, but the one before it has no `;`", token.upper),
                );
            }
        }
    }

    /// Where the statement's real verb is: past `EXPLAIN [ANALYZE] (…)` and a
    /// `WITH` list.
    fn verb(&self) -> Option<usize> {
        let mut at = 0;
        while let Some(token) = self.get(at) {
            if token.any_word(&["EXPLAIN", "ANALYZE", "ANALYSE", "VERBOSE"]) {
                at += 1;
            } else if token.is("(") && at > 0 {
                at = self.close[at]? + 1;
            } else {
                break;
            }
        }
        if self.get(at)?.word("WITH") {
            return (at..self.tokens.len()).find(|&i| {
                self.depth[i] == 0 && self.tokens[i].any_word(&["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE"])
            });
        }
        Some(at)
    }

    /// `UPDATE t SET …` and `DELETE FROM t` with no `WHERE` touch every row.
    /// Checked for the statement's verb and for data-modifying CTEs inside it.
    fn unfiltered_writes(&self, out: &mut Vec<Diagnostic>) {
        let mut candidates: Vec<usize> = self.verb().into_iter().collect();
        candidates.extend((1..self.tokens.len()).filter(|&i| {
            self.tokens[i].any_word(&["UPDATE", "DELETE"]) && self.tokens[i - 1].is("(")
        }));

        for at in candidates {
            let verb = &self.tokens[at];
            let message = match verb.upper.as_str() {
                "UPDATE" => "UPDATE without WHERE changes every row of the table",
                "DELETE" => "DELETE without WHERE removes every row of the table",
                _ => continue,
            };
            let depth = self.depth[at];
            let filtered = self.tokens[at + 1..]
                .iter()
                .zip(&self.depth[at + 1..])
                .take_while(|(_, d)| **d >= depth)
                .any(|(token, d)| *d == depth && token.word("WHERE"));
            if !filtered {
                self.report(out, at, at, Severity::Warning, message.to_string());
            }
        }
    }

    fn typos(&self, context: Context<'_>, out: &mut Vec<Diagnostic>) {
        let known = |token: &Tok<'_>| {
            let lower = token.text.to_lowercase();
            context.tables.is_some_and(|t| t.contains(&lower))
                || context.names.is_some_and(|n| n.contains(&lower))
        };

        // The statement's first word: nothing else can stand there, so a near
        // miss of a statement keyword is almost certainly that keyword.
        let first = &self.tokens[0];
        if first.kind == Kind::Word
            && first.upper.len() >= 4
            && !STARTERS.contains(&first.upper.as_str())
            && let Some(target) = STARTER_TARGETS.iter().find(|t| one_slip(&first.upper, t, true))
        {
            self.report(
                out,
                0,
                0,
                Severity::Warning,
                format!("`{}` does not begin a statement — did you mean {}?", first.text, target),
            );
        }

        // Mid-statement, a word can be a name, so the rule needs more: the slip
        // must be a swapped or dropped letter (never an added one — `orders` is
        // a table, not a typo of ORDER), the word must not be a known name, and
        // what follows must be what follows the real keyword.
        for (index, token) in self.tokens.iter().enumerate().skip(1) {
            if token.kind != Kind::Word
                || token.upper.len() < 3
                || sqllex::is_keyword(&token.upper)
                || sqllex::is_type(&token.upper)
                || known(token)
            {
                continue;
            }
            let previous = &self.tokens[index - 1];
            if previous.word("AS") || previous.is(".") {
                continue;
            }
            let Some(next) = self.get(index + 1) else { continue };
            let Some(target) = CLAUSE_TARGETS.iter().find(|t| one_slip(&token.upper, t, false)) else {
                continue;
            };
            let fits = if matches!(*target, "ORDER" | "GROUP") {
                next.word("BY")
            } else {
                next.is_name() && !next.is("(")
            };
            if fits {
                self.report(
                    out,
                    index,
                    index,
                    Severity::Warning,
                    format!("`{}` — did you mean {}?", token.text, target),
                );
            }
        }
    }

    fn unknown_tables(&self, tables: &HashSet<String>, created: &HashSet<String>, out: &mut Vec<Diagnostic>) {
        let Some(verb) = self.verb() else { return };
        let verb_word = self.tokens[verb].upper.as_str();
        if !matches!(verb_word, "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "TABLE") {
            return;
        }

        // The names a `WITH` list defines: the word after `WITH`, `RECURSIVE`
        // or a top-level comma, before the verb.
        let mut defined: HashSet<String> = HashSet::new();
        for index in 1..verb {
            let previous = &self.tokens[index - 1];
            let token = &self.tokens[index];
            if self.depth[index] == 0
                && (previous.any_word(&["WITH", "RECURSIVE"]) || previous.is(","))
                && matches!(token.kind, Kind::Word | Kind::Quoted)
            {
                defined.insert(sqllex::identifier(token.kind, token.text));
            }
        }

        let check = |index: usize, out: &mut Vec<Diagnostic>| {
            let Some(token) = self.get(index) else { return };
            if !matches!(token.kind, Kind::Word | Kind::Quoted) || !token.is_name() {
                return;
            }
            // `schema.table`, and `generate_series(…)`, are not this rule's.
            if self.get(index + 1).is_some_and(|next| next.is(".") || next.is("(")) {
                return;
            }
            let name = sqllex::identifier(token.kind, token.text);
            let lower = name.to_lowercase();
            if lower.starts_with("pg_")
                || tables.contains(&lower)
                || created.contains(&name)
                || created.contains(&lower)
                || defined.contains(&name)
                || defined.contains(&lower)
            {
                return;
            }
            self.report(
                out,
                index,
                index,
                Severity::Warning,
                format!("`{name}` is not a table in this database — refresh the list if it was just created"),
            );
        };

        for (index, token) in self.tokens.iter().enumerate() {
            let inside_call = self.enclosing[index].is_some_and(|open| self.call[open]);
            if inside_call || token.kind != Kind::Word {
                continue;
            }
            match token.upper.as_str() {
                // `a IS DISTINCT FROM b` compares; it does not name a table.
                "FROM" if index > 0 && self.tokens[index - 1].word("DISTINCT") => {}
                "FROM" => self.table_list(index + 1, &check, out),
                "USING" if verb_word == "DELETE" && self.depth[index] == 0 => {
                    self.table_list(index + 1, &check, out)
                }
                "JOIN" => {
                    let mut at = index + 1;
                    while self.get(at).is_some_and(|t| t.any_word(&["ONLY", "LATERAL"])) {
                        at += 1;
                    }
                    check(at, out);
                }
                "INTO" if index > 0 && self.tokens[index - 1].word("INSERT") => check(index + 1, out),
                "UPDATE" | "TABLE" if index == verb => {
                    let mut at = index + 1;
                    while self.get(at).is_some_and(|t| t.word("ONLY")) {
                        at += 1;
                    }
                    check(at, out);
                }
                _ => {}
            }
        }
    }

    /// `FROM a, b AS x, (SELECT …) y, f(…) z`: checks each named table.
    fn table_list(&self, mut at: usize, check: &dyn Fn(usize, &mut Vec<Diagnostic>), out: &mut Vec<Diagnostic>) {
        loop {
            while self.get(at).is_some_and(|t| t.any_word(&["ONLY", "LATERAL"])) {
                at += 1;
            }
            let Some(token) = self.get(at) else { return };
            if token.is("(") {
                let Some(close) = self.close[at] else { return };
                at = close + 1;
            } else if token.is_name() {
                check(at, out);
                at += 1;
                while self.get(at).is_some_and(|t| t.is(".")) && self.get(at + 1).is_some_and(Tok::is_name) {
                    at += 2;
                }
                if self.get(at).is_some_and(|t| t.is("(")) {
                    let Some(close) = self.close[at] else { return };
                    at = close + 1;
                }
            } else {
                return;
            }
            // An alias, and its column list.
            if self.get(at).is_some_and(|t| t.word("AS")) {
                at += 1;
            }
            if self.get(at).is_some_and(Tok::is_name) {
                at += 1;
                if self.get(at).is_some_and(|t| t.is("(")) {
                    let Some(close) = self.close[at] else { return };
                    at = close + 1;
                }
            }
            if self.get(at).is_some_and(|t| t.is(",")) {
                at += 1;
            } else {
                return;
            }
        }
    }
}

/// Whether `word` is one slip from `target`: two adjacent letters swapped, one
/// dropped, or — with `substitute` — one replaced.
fn one_slip(word: &str, target: &str, substitute: bool) -> bool {
    let (w, t) = (word.as_bytes(), target.as_bytes());
    if w == t {
        return false;
    }
    if w.len() == t.len() {
        let differ: Vec<usize> = (0..w.len()).filter(|&i| w[i] != t[i]).collect();
        return match differ.as_slice() {
            [i] => substitute && *i < w.len(),
            [i, j] => *j == i + 1 && w[*i] == t[*j] && w[*j] == t[*i],
            _ => false,
        };
    }
    if w.len() + 1 == t.len() {
        // Dropping one letter from the target gives the word.
        return (0..t.len()).any(|skip| {
            t.iter()
                .enumerate()
                .filter(|(i, _)| *i != skip)
                .map(|(_, c)| c)
                .eq(w.iter())
        });
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn messages(sql: &str) -> Vec<String> {
        lint(sql, Context::default()).into_iter().map(|d| d.message).collect()
    }

    fn with_tables(sql: &str, tables: &[&str]) -> Vec<String> {
        let tables: HashSet<String> = tables.iter().map(|t| t.to_string()).collect();
        lint(sql, Context { tables: Some(&tables), names: None })
            .into_iter()
            .map(|d| d.message)
            .collect()
    }

    /// The property the whole module is built around: ordinary, correct SQL
    /// produces nothing.
    #[test]
    fn valid_sql_is_quiet() {
        for sql in [
            "SELECT a, b FROM t WHERE a = 1 ORDER BY b DESC LIMIT 10;",
            "UPDATE t SET a = NULL, b = 2 WHERE id = 3;",
            "DELETE FROM t WHERE id IN (SELECT id FROM u);",
            "INSERT INTO t (a, b)\nSELECT a, b FROM u;",
            "INSERT INTO t (a) VALUES (1)\nON CONFLICT (a) DO\nUPDATE SET a = excluded.a;",
            "SELECT * FROM t FOR\nUPDATE;",
            "SELECT 1\nUNION ALL\nSELECT 2;",
            "WITH x AS (SELECT 1 AS a)\nSELECT a FROM x;",
            "CREATE TABLE t (id INT, p INT REFERENCES p (id) ON\nDELETE CASCADE);",
            "ALTER TABLE t\nDROP COLUMN c;",
            "CREATE VIEW v AS\nSELECT 1;",
            "SELECT extract(year FROM ts), substring(s FROM 2 FOR 3) FROM t;",
            "SELECT a IS DISTINCT FROM b FROM t;",
            "SELECT form FROM t;",
            "SELECT * FROM orders o JOIN groups g ON g.id = o.gid WHERE o.x = 1;",
            "SET CONFIG cache = NULL;",
            "SELECT 'a;b', \"we;ird\" FROM t; -- ; in comment",
            "SHOW TABLES;\nSELECT 1;",
            "TRUNCATE t;",
            "DO $$ BEGIN UPDATE t SET a = 1; END $$;",
        ] {
            assert_eq!(messages(sql), Vec::<String>::new(), "flagged valid SQL: {sql}");
        }
    }

    #[test]
    fn unclosed_things_are_errors() {
        assert!(messages("SELECT 'abc FROM t")[0].contains("string is never closed"));
        assert!(messages("SELECT /* note")[0].contains("comment is never closed"));
        assert!(messages("SELECT \"col FROM t")[0].contains("quoted name"));
        assert!(messages("SELECT (1 + 2 FROM t")[0].contains("`(` is never closed"));
        assert!(messages("SELECT 1 + 2) FROM t")[0].contains("closes nothing"));
    }

    #[test]
    fn stray_commas() {
        assert_eq!(messages("SELECT a, b, FROM t"), vec!["a trailing comma before FROM"]);
        assert_eq!(messages("INSERT INTO t VALUES (1, 2,)"), vec!["a trailing comma before `)`"]);
        assert_eq!(messages("SELECT a,, b FROM t"), vec!["two commas with nothing between them"]);
        assert_eq!(messages("SELECT , a FROM t"), vec!["nothing between SELECT and this comma"]);
    }

    #[test]
    fn null_comparisons_are_warned_outside_assignments() {
        assert_eq!(messages("SELECT * FROM t WHERE a = NULL"), vec!["`= NULL` is never true — use IS NULL"]);
        assert_eq!(
            messages("SELECT * FROM t WHERE a <> null"),
            vec!["`<> NULL` is never true — use IS NOT NULL"]
        );
        // Inside a subquery in a SET, the WHERE is still a comparison.
        assert_eq!(
            messages("UPDATE t SET a = (SELECT b FROM u WHERE c = NULL) WHERE id = 1").len(),
            1
        );
    }

    #[test]
    fn unfiltered_writes_are_warned() {
        assert_eq!(messages("UPDATE t SET a = 1"), vec!["UPDATE without WHERE changes every row of the table"]);
        assert_eq!(messages("delete from t"), vec!["DELETE without WHERE removes every row of the table"]);
        // A WHERE inside a subquery is not the statement's.
        assert_eq!(messages("UPDATE t SET a = (SELECT 1 FROM u WHERE u.x = 1)").len(), 1);
        // EXPLAIN ANALYZE runs it.
        assert_eq!(messages("EXPLAIN ANALYZE DELETE FROM t").len(), 1);
        // A data-modifying CTE.
        assert_eq!(messages("WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d").len(), 1);
    }

    #[test]
    fn a_missing_semicolon_between_statements() {
        let found = lint("SELECT * FROM a\nSELECT * FROM b", Context::default());
        assert_eq!(found.len(), 1);
        assert_eq!((found[0].line, found[0].start), (1, 0));
        assert!(found[0].message.contains("no `;`"));

        assert_eq!(messages("UPDATE t SET a = 1 WHERE b = 2\nDELETE FROM t WHERE c = 3").len(), 1);
    }

    #[test]
    fn keyword_typos() {
        assert_eq!(messages("SELCT 1"), vec!["`SELCT` does not begin a statement — did you mean SELECT?"]);
        assert_eq!(messages("SELECT * FORM users"), vec!["`FORM` — did you mean FROM?"]);
        assert_eq!(messages("SELECT * FROM users WHRE id = 1"), vec!["`WHRE` — did you mean WHERE?"]);
        assert_eq!(messages("SELECT * FROM users ORDR BY id"), vec!["`ORDR` — did you mean ORDER?"]);
    }

    #[test]
    fn a_known_name_is_never_a_typo() {
        let names: HashSet<String> = ["form".to_string()].into();
        let found = lint("SELECT form f FROM t", Context { tables: None, names: Some(&names) });
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn unknown_tables_when_the_list_is_known() {
        assert_eq!(
            with_tables("SELECT * FROM users u JOIN ordrs o ON o.uid = u.id", &["users", "orders"]),
            vec!["`ordrs` is not a table in this database — refresh the list if it was just created"]
        );
        // Case-insensitive, CTEs, created tables, catalogues, functions and
        // schema-qualified names are all fine.
        for sql in [
            "SELECT * FROM USERS",
            "WITH recent AS (SELECT 1) SELECT * FROM recent, users",
            "CREATE TABLE fresh (id INT); INSERT INTO fresh VALUES (1); SELECT * FROM fresh",
            "SELECT * FROM pg_stat_user_tables",
            "SELECT * FROM generate_series(1, 3) g",
            "SELECT * FROM information_schema.columns",
            "SELECT * FROM (SELECT 1) AS x(a), users",
            "SHOW COLUMNS FROM whatever",
        ] {
            assert_eq!(with_tables(sql, &["users"]), Vec::<String>::new(), "{sql}");
        }
        assert_eq!(with_tables("SELECT * FROM users, nope", &["users"]).len(), 1);
        assert_eq!(with_tables("UPDATE nope SET a = 1 WHERE b = 2", &["users"]).len(), 1);
        assert_eq!(with_tables("INSERT INTO nope VALUES (1)", &["users"]).len(), 1);
    }

    #[test]
    fn spans_land_on_the_token() {
        let found = lint("SELECT *\nFROM t\nWHERE a = NULL", Context::default());
        assert_eq!(found.len(), 1);
        let d = &found[0];
        assert_eq!((d.line, d.start, d.end), (2, 8, 14));
    }

    #[test]
    fn slips() {
        assert!(one_slip("FORM", "FROM", false));
        assert!(one_slip("WHRE", "WHERE", false));
        assert!(!one_slip("ORDERS", "ORDER", false));
        assert!(!one_slip("FROM", "FROM", false));
        assert!(!one_slip("FRON", "FROM", false));
        assert!(one_slip("FRON", "FROM", true));
    }
}
