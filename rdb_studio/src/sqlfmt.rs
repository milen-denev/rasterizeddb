//! A deliberately small SQL formatter.
//!
//! # What it will and will not do
//!
//! It puts each clause on its own line and upper-cases keywords. It does not
//! re-indent expressions, align columns, or rewrite anything — because a
//! formatter that rearranges a statement can change it, and a console's format
//! button is pressed on statements people are about to run against production.
//!
//! The one hard rule: **it only ever touches bare words.** Everything inside a
//! string literal, a quoted identifier, a line comment or a block comment is
//! copied through byte for byte. `WHERE note = 'select from where'` comes back
//! with its literal intact, which is the property worth having and the one a
//! regular expression cannot give you.

/// Words that begin a clause, and so get a line of their own.
const CLAUSE: &[&str] = &[
    "SELECT", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "VALUES", "SET",
    "RETURNING", "UNION", "INTERSECT", "EXCEPT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER",
    "DROP", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "FULL", "ON", "WITH",
];

/// Words that read better on their own line inside a `WHERE`, but only at the
/// top level — an `AND` inside `(a AND b)` belongs where it is.
const CONNECTIVE: &[&str] = &["AND", "OR"];

/// Everything else worth upper-casing.
const KEYWORD: &[&str] = &[
    "AS", "ASC", "DESC", "BY", "INTO", "DISTINCT", "ALL", "AND", "OR", "NOT", "NULL", "IS", "IN",
    "LIKE", "ILIKE", "BETWEEN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END", "TRUE", "FALSE",
    "TABLE", "INDEX", "VIEW", "DATABASE", "COLUMN", "CONSTRAINT", "PRIMARY", "KEY", "FOREIGN",
    "REFERENCES", "UNIQUE", "DEFAULT", "CASCADE", "RESTRICT", "IF", "USING", "NULLS", "FIRST",
    "LAST", "COUNT", "SUM", "AVG", "MIN", "MAX",
];

/// Formats one statement, or several separated by `;`.
pub fn format(sql: &str) -> String {
    let tokens = tokenize(sql);
    let mut out = String::new();
    let mut depth = 0i32;
    // `GROUP BY` and `ORDER BY` are two words; the `BY` must not start a line
    // of its own, and neither must the `JOIN` in `LEFT JOIN`.
    let mut previous_word: Option<String> = None;

    for token in &tokens {
        match token {
            Token::Space => {
                if !out.ends_with('\n') && !out.is_empty() {
                    out.push(' ');
                }
            }
            Token::Verbatim(text) => {
                out.push_str(text);
                previous_word = None;
            }
            Token::Punctuation(c) => {
                match c {
                    '(' => depth += 1,
                    ')' => depth = (depth - 1).max(0),
                    _ => {}
                }
                // A comma before a space reads as `a, b`, never `a ,b`.
                if *c == ',' || *c == ')' {
                    while out.ends_with(' ') {
                        out.pop();
                    }
                }
                out.push(*c);
                if *c == ';' {
                    out.push('\n');
                }
                previous_word = None;
            }
            Token::Word(word) => {
                let upper = word.to_ascii_uppercase();
                let starts_clause = depth == 0
                    && CLAUSE.contains(&upper.as_str())
                    // `BY` follows `GROUP`/`ORDER`; `JOIN` follows `LEFT` and
                    // friends. Neither begins anything.
                    && !previous_word.as_deref().is_some_and(|previous| {
                        matches!(
                            previous,
                            "GROUP" | "ORDER" | "LEFT" | "RIGHT" | "INNER" | "OUTER" | "CROSS"
                                | "FULL" | "UNION" | "INSERT" | "DELETE"
                        )
                    });
                let starts_connective = depth == 0 && CONNECTIVE.contains(&upper.as_str());

                if (starts_clause || starts_connective) && !out.trim_end().is_empty() {
                    while out.ends_with(' ') {
                        out.pop();
                    }
                    if !out.ends_with('\n') {
                        out.push('\n');
                    }
                }

                let rendered = if CLAUSE.contains(&upper.as_str())
                    || KEYWORD.contains(&upper.as_str())
                {
                    upper.clone()
                } else {
                    word.clone()
                };
                out.push_str(&rendered);
                previous_word = Some(upper);
            }
        }
    }

    out.trim().to_string()
}

enum Token {
    /// A bare word: the only thing the formatter is allowed to change.
    Word(String),
    /// A literal, a quoted identifier or a comment. Copied through untouched.
    Verbatim(String),
    Punctuation(char),
    Space,
}

/// Splits SQL into things that may be reformatted and things that may not.
fn tokenize(sql: &str) -> Vec<Token> {
    let bytes: Vec<char> = sql.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0usize;

    while index < bytes.len() {
        let c = bytes[index];

        if c.is_whitespace() {
            while index < bytes.len() && bytes[index].is_whitespace() {
                index += 1;
            }
            tokens.push(Token::Space);
            continue;
        }

        // A string literal or a quoted identifier, to its closing quote.
        // A doubled quote inside one is an escaped quote, not the end.
        if c == '\'' || c == '"' {
            let quote = c;
            let mut text = String::from(quote);
            index += 1;
            while index < bytes.len() {
                let ch = bytes[index];
                text.push(ch);
                index += 1;
                if ch == quote {
                    if bytes.get(index) == Some(&quote) {
                        text.push(quote);
                        index += 1;
                        continue;
                    }
                    break;
                }
            }
            tokens.push(Token::Verbatim(text));
            continue;
        }

        if c == '-' && bytes.get(index + 1) == Some(&'-') {
            let mut text = String::new();
            while index < bytes.len() && bytes[index] != '\n' {
                text.push(bytes[index]);
                index += 1;
            }
            tokens.push(Token::Verbatim(text));
            // A line comment ends its line, and swallowing the newline would
            // comment out whatever the formatter put next.
            tokens.push(Token::Verbatim("\n".to_string()));
            continue;
        }

        if c == '/' && bytes.get(index + 1) == Some(&'*') {
            let mut text = String::from("/*");
            index += 2;
            while index < bytes.len() {
                if bytes[index] == '*' && bytes.get(index + 1) == Some(&'/') {
                    text.push_str("*/");
                    index += 2;
                    break;
                }
                text.push(bytes[index]);
                index += 1;
            }
            tokens.push(Token::Verbatim(text));
            continue;
        }

        if c.is_alphanumeric() || c == '_' || c == '$' || c == '.' {
            let mut word = String::new();
            while index < bytes.len()
                && (bytes[index].is_alphanumeric()
                    || bytes[index] == '_'
                    || bytes[index] == '$'
                    || bytes[index] == '.')
            {
                word.push(bytes[index]);
                index += 1;
            }
            tokens.push(Token::Word(word));
            continue;
        }

        tokens.push(Token::Punctuation(c));
        index += 1;
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clauses_go_on_their_own_lines() {
        let out = format("select a, b from users where a = 1 order by b desc limit 10");
        assert_eq!(
            out,
            "SELECT a, b\nFROM users\nWHERE a = 1\nORDER BY b DESC\nLIMIT 10"
        );
    }

    /// `BY` follows `ORDER`/`GROUP` and `JOIN` follows `LEFT`; neither starts a
    /// clause of its own.
    #[test]
    fn two_word_clauses_stay_together() {
        let out = format("select a from t group by a order by a");
        assert_eq!(out, "SELECT a\nFROM t\nGROUP BY a\nORDER BY a");

        let out = format("select * from a left join b on a.id = b.id");
        assert_eq!(out, "SELECT *\nFROM a\nLEFT JOIN b\nON a.id = b.id");
    }

    /// The property the whole tokenizer exists for. A literal that contains
    /// SQL keywords is data, and the formatter must not touch a byte of it.
    #[test]
    fn a_literal_is_never_reformatted() {
        let out = format("select * from t where note = 'select from where and or'");
        assert!(
            out.contains("'select from where and or'"),
            "the literal was rewritten: {out}"
        );
    }

    #[test]
    fn a_doubled_quote_does_not_end_a_literal() {
        let out = format("select * from t where a = 'it''s from here' and b = 2");
        assert!(out.contains("'it''s from here'"), "{out}");
        assert!(out.contains("AND b = 2"), "the statement continued: {out}");
    }

    /// A quoted identifier keeps its case: `"Name"` and `"name"` are different
    /// columns, and upper-casing one renames it.
    #[test]
    fn a_quoted_identifier_keeps_its_case() {
        let out = format("select \"userName\" from \"Users\"");
        assert!(out.contains("\"userName\""), "{out}");
        assert!(out.contains("\"Users\""), "{out}");
    }

    /// Swallowing the newline after a line comment would comment out whatever
    /// came next.
    #[test]
    fn a_line_comment_keeps_its_newline() {
        let out = format("-- keep me\nselect 1");
        assert!(out.starts_with("-- keep me\n"), "{out}");
        assert!(out.contains("SELECT 1"), "{out}");
    }

    #[test]
    fn a_block_comment_survives() {
        let out = format("select /* from where */ 1");
        assert!(out.contains("/* from where */"), "{out}");
    }

    /// An `AND` inside parentheses is part of an expression, not a new line.
    #[test]
    fn a_nested_connective_stays_inline() {
        let out = format("select * from t where (a = 1 and b = 2) and c = 3");
        assert!(out.contains("(a = 1 AND b = 2)"), "{out}");
        assert_eq!(out.matches("\nAND").count(), 1, "only the top-level AND breaks: {out}");
    }

    #[test]
    fn punctuation_hugs_what_it_should() {
        let out = format("select a , b from t");
        assert!(out.contains("SELECT a, b"), "{out}");
    }

    /// Formatting an already-formatted statement must not keep changing it.
    #[test]
    fn formatting_is_idempotent() {
        for sql in [
            "select a, b from users where a = 1 and b = 2 order by a",
            "insert into t (a, b) values (1, 'x') returning a",
            "-- note\nselect 1",
            "select * from a left join b on a.id = b.id where a.x = 'y'",
        ] {
            let once = format(sql);
            assert_eq!(format(&once), once, "not idempotent for {sql:?}");
        }
    }

    #[test]
    fn empty_input_is_empty_output() {
        assert_eq!(format(""), "");
        assert_eq!(format("   \n  "), "");
    }
}
