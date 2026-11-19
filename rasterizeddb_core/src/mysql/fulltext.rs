//! MySQL full-text search on the engine's: `FULLTEXT` keys and
//! `MATCH (…) AGAINST (…)`.
//!
//! A `FULLTEXT (a, b)` key is a GIN index over
//! `to_tsvector('simple', coalesce(a, '') || ' ' || coalesce(b, ''))`, and
//! `MATCH (a, b)` reads that same document, so the engine answers the match
//! from the index (`core::index::gin`). `simple` lowercases and neither stems
//! nor drops words, which is InnoDB's tokenizer: words of letters and digits,
//! compared without case.
//!
//! What InnoDB leaves out of its index it never finds, so a query here drops
//! the same words before it is made: InnoDB's default stop words
//! (`INFORMATION_SCHEMA.INNODB_FT_DEFAULT_STOPWORD`) and words shorter than
//! `innodb_ft_min_token_size` (3) or longer than `innodb_ft_max_token_size`
//! (84). The document keeps them, which changes no answer: a query never asks
//! for one.
//!
//! # Modes
//!
//! - Natural language (the default): a row matching any word.
//! - `IN BOOLEAN MODE`: `+word` must be present, `-word` must not, a bare
//!   word is optional (a group with no `+` needs one of its optional words),
//!   `word*` is a prefix, `"a phrase"` is the words in order, and `( … )`
//!   groups. `>`, `<` and `~` change only InnoDB's ranking, and are read as a
//!   bare word.
//! - `WITH QUERY EXPANSION` is refused: it searches again with words taken
//!   from the first answer's best rows, which ranks this engine computes
//!   differently.
//!
//! # Relevance
//!
//! `MATCH … AGAINST` read as a number (in a select list, an `ORDER BY`, a
//! comparison) is `ts_rank` of the row's document against the query, and 0
//! for a row that does not match, as InnoDB gives. The numbers are not
//! InnoDB's (its BM25-like weighting is its own), so an order by relevance
//! can differ where two rows score close.

use super::lex::{Tok, Token};
use super::lex::split_commas;
use super::translate::{MyError, ident_sql};

/// InnoDB's default stop words.
const STOP_WORDS: &[&str] = &[
    "a", "about", "an", "are", "as", "at", "be", "by", "com", "de", "en", "for", "from", "how", "i", "in", "is", "it",
    "la", "of", "on", "or", "that", "the", "this", "to", "was", "what", "when", "where", "who", "will", "with", "und",
    "www",
];

/// `innodb_ft_min_token_size` and `innodb_ft_max_token_size`, their defaults.
const MIN_TOKEN: usize = 3;
const MAX_TOKEN: usize = 84;

/// The full-text document of `columns` (each already SQL): what a `FULLTEXT`
/// key indexes and a `MATCH` over the same columns reads.
pub(crate) fn document(columns: &[String]) -> String {
    crate::sqlcompat::fulltext_document(columns)
}

/// The `CREATE INDEX` a `FULLTEXT` key over `columns` (names as written) is.
pub(crate) fn index_statement(index: &str, table: &str, columns: &[String], if_not_exists: bool) -> String {
    let columns: Vec<String> = columns.iter().map(|c| ident_sql(c)).collect();
    format!(
        "CREATE INDEX {}{} ON {} USING gin (to_tsvector('simple', {}))",
        if if_not_exists { "IF NOT EXISTS " } else { "" },
        ident_sql(index),
        ident_sql(table),
        document(&columns)
    )
}

/// The column names of a `FULLTEXT` key's list. A prefix length or an
/// expression is refused: InnoDB has neither on a full-text key.
pub(crate) fn key_columns(list: &[Token]) -> Result<Vec<String>, MyError> {
    let mut out = Vec::new();
    for part in split_commas(list) {
        match part.as_slice() {
            [t] if t.ident().is_some() => out.push(t.ident().unwrap().to_string()),
            _ => return Err(MyError::syntax("a FULLTEXT key lists columns")),
        }
    }
    if out.is_empty() {
        return Err(MyError::syntax("a FULLTEXT key lists columns"));
    }
    Ok(out)
}

/// Refuses what a `FULLTEXT` key may carry and this cannot honour.
pub(crate) fn check_key_options(after: &[Token]) -> Result<(), MyError> {
    if after.iter().any(|t| t.is_word("PARSER")) {
        return Err(MyError::unsupported("a FULLTEXT parser plugin (WITH PARSER)"));
    }
    Ok(())
}

/// How `AGAINST` reads its search string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Natural,
    Boolean,
}

/// The mode words after the search string: `IN NATURAL LANGUAGE MODE`,
/// `IN BOOLEAN MODE`, or none.
pub(crate) fn mode_of(words: &[Token]) -> Result<Mode, MyError> {
    let text: Vec<String> = words.iter().filter_map(|t| t.upper()).collect();
    let text: Vec<&str> = text.iter().map(String::as_str).collect();
    match text.as_slice() {
        [] | ["IN", "NATURAL", "LANGUAGE", "MODE"] => Ok(Mode::Natural),
        ["IN", "BOOLEAN", "MODE"] => Ok(Mode::Boolean),
        [.., "EXPANSION"] => Err(MyError::unsupported("full-text search WITH QUERY EXPANSION")),
        _ => Err(MyError::syntax("AGAINST takes a search string and a mode")),
    }
}

/// The words of `text` as the engine's full-text splitter makes them (letters
/// and digits, an apostrophe inside a word), lowercased.
fn words(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() || (c == '\'' && !current.is_empty()) {
            current.push(c);
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current).to_lowercase());
        }
    }
    if !current.is_empty() {
        out.push(current.to_lowercase());
    }
    out
}

/// Whether InnoDB indexes `word`.
fn indexed(word: &str) -> bool {
    let n = word.chars().count();
    (MIN_TOKEN..=MAX_TOKEN).contains(&n) && !STOP_WORDS.contains(&word)
}

/// A lexeme quoted for `to_tsquery`.
fn quoted(word: &str) -> String {
    format!("'{}'", word.replace('\'', "''"))
}

/// The `to_tsquery('simple', …)` text a search string means, or `None` when
/// it can match no row (nothing left once InnoDB's ignored words are out, or
/// only exclusions).
pub(crate) fn tsquery(search: &str, mode: Mode) -> Option<String> {
    match mode {
        Mode::Natural => {
            let mut terms: Vec<String> = Vec::new();
            for w in words(search) {
                if indexed(&w) && !terms.contains(&w) {
                    terms.push(w);
                }
            }
            if terms.is_empty() {
                return None;
            }
            Some(terms.iter().map(|w| quoted(w)).collect::<Vec<_>>().join(" | "))
        }
        Mode::Boolean => {
            let chars: Vec<char> = search.chars().collect();
            let mut at = 0;
            boolean_group(&chars, &mut at, false)
        }
    }
}

/// One `( … )` group of a boolean-mode string, or the whole of it.
fn boolean_group(s: &[char], at: &mut usize, nested: bool) -> Option<String> {
    let (mut required, mut optional, mut excluded) = (Vec::new(), Vec::new(), Vec::new());
    loop {
        while *at < s.len() && s[*at].is_whitespace() {
            *at += 1;
        }
        if *at >= s.len() {
            break;
        }
        if s[*at] == ')' {
            *at += 1;
            if nested {
                break;
            }
            continue;
        }
        // Operators before an item; the last of `+`/`-` decides.
        let mut sign = ' ';
        while *at < s.len() && matches!(s[*at], '+' | '-' | '~' | '<' | '>' | '@') {
            if matches!(s[*at], '+' | '-') {
                sign = s[*at];
            }
            *at += 1;
        }
        if *at >= s.len() {
            break;
        }
        let item: Option<String> = match s[*at] {
            '(' => {
                *at += 1;
                boolean_group(s, at, true).map(|g| format!("({g})"))
            }
            '"' => {
                *at += 1;
                let start = *at;
                while *at < s.len() && s[*at] != '"' {
                    *at += 1;
                }
                let phrase: String = s[start..*at].iter().collect();
                *at = (*at + 1).min(s.len());
                let ws = words(&phrase);
                (!ws.is_empty()).then(|| ws.iter().map(|w| quoted(w)).collect::<Vec<_>>().join(" <-> "))
            }
            _ => {
                let start = *at;
                while *at < s.len() && !s[*at].is_whitespace() && !matches!(s[*at], '(' | ')' | '"') {
                    *at += 1;
                }
                let raw: String = s[start..*at].iter().collect();
                let prefix = raw.ends_with('*');
                let ws = words(raw.trim_end_matches('*'));
                match ws.as_slice() {
                    [] => None,
                    // `word*`: a prefix, which InnoDB looks up whatever its
                    // length.
                    [w] if prefix => Some(format!("{}:*", quoted(w))),
                    [w] => indexed(w).then(|| quoted(w)),
                    // `e-mail`: InnoDB splits it, and needs both parts.
                    many => {
                        let kept: Vec<String> = many.iter().filter(|w| indexed(w)).map(|w| quoted(w)).collect();
                        (!kept.is_empty()).then(|| format!("({})", kept.join(" & ")))
                    }
                }
            }
        };
        let Some(item) = item else { continue };
        match sign {
            '+' => required.push(item),
            '-' => excluded.push(item),
            _ => optional.push(item),
        }
    }
    let positive = if !required.is_empty() {
        required.join(" & ")
    } else if !optional.is_empty() {
        format!("({})", optional.join(" | "))
    } else {
        return None;
    };
    let mut out = positive;
    for e in excluded {
        out.push_str(&format!(" & !{e}"));
    }
    Some(out)
}

/// The SQL a `MATCH (columns) AGAINST (search mode)` is: a condition, or
/// (`relevance`) its score. `search` is `None` for a `?` the statement binds
/// at execute, which is then translated again with the value in place
/// (`Conn::com_prepare`); the SQL made for it is only ever described.
pub(crate) fn expression(columns: &[String], search: Option<&str>, mode: Mode, relevance: bool) -> String {
    let vector = format!("to_tsvector('simple', {})", document(columns));
    let query = match search {
        Some(text) => match tsquery(text, mode) {
            Some(q) => format!("to_tsquery('simple', '{}')", q.replace('\'', "''")),
            // Nothing InnoDB would look for: no row matches.
            None => return if relevance { "0::float8".to_string() } else { "false".to_string() },
        },
        None => "to_tsquery('simple', '')".to_string(),
    };
    if relevance {
        format!("(CASE WHEN {vector} @@ {query} THEN ts_rank({vector}, {query})::float8 ELSE 0::float8 END)")
    } else {
        format!("({vector} @@ {query})")
    }
}

/// Whether `tokens` hold a `MATCH … AGAINST (?)`: the search string is bound
/// at execute, so the statement is translated then, not prepared.
pub(crate) fn binds_search_string(tokens: &[Token]) -> bool {
    tokens.windows(2).enumerate().any(|(k, w)| {
        w[0].is_word("AGAINST")
            && w[1].is_op("(")
            && tokens[k + 2..].iter().take_while(|t| !t.is_op(")")).any(|t| matches!(t.tok, Tok::Param(_)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn natural_language_drops_what_innodb_does_not_index() {
        assert_eq!(tsquery("The quick brown fox", Mode::Natural).as_deref(), Some("'quick' | 'brown' | 'fox'"));
        assert_eq!(tsquery("go to it", Mode::Natural), None);
        assert_eq!(tsquery("Fox fox FOX", Mode::Natural).as_deref(), Some("'fox'"));
        assert_eq!(tsquery("don't stop", Mode::Natural).as_deref(), Some("'don''t' | 'stop'"));
    }

    #[test]
    fn boolean_mode() {
        let q = |s: &str| tsquery(s, Mode::Boolean);
        assert_eq!(q("+apple +juice").as_deref(), Some("'apple' & 'juice'"));
        assert_eq!(q("+apple -macintosh").as_deref(), Some("'apple' & !'macintosh'"));
        assert_eq!(q("apple banana").as_deref(), Some("('apple' | 'banana')"));
        assert_eq!(q("+apple banana").as_deref(), Some("'apple'"));
        assert_eq!(q("apple*").as_deref(), Some("('apple':*)"));
        assert_eq!(q("\"some words here\"").as_deref(), Some("('some' <-> 'words' <-> 'here')"));
        assert_eq!(q("+apple +(>turnover <strudel)").as_deref(), Some("'apple' & (('turnover' | 'strudel'))"));
        assert_eq!(q("-apple"), None);
        assert_eq!(q("+the +fox").as_deref(), Some("'fox'"));
        assert_eq!(q("~apple").as_deref(), Some("('apple')"));
    }

    #[test]
    fn match_expressions() {
        let cols = vec!["\"title\"".to_string(), "\"body\"".to_string()];
        assert_eq!(
            expression(&cols, Some("fox"), Mode::Natural, false),
            "(to_tsvector('simple', coalesce(\"title\", '') || ' ' || coalesce(\"body\", '')) @@ to_tsquery('simple', '''fox'''))"
        );
        assert_eq!(expression(&cols, Some("the"), Mode::Natural, false), "false");
        assert_eq!(expression(&cols, Some("the"), Mode::Natural, true), "0::float8");
        assert_eq!(
            index_statement("t__ft", "t", &["title".into(), "body".into()], false),
            "CREATE INDEX t__ft ON t USING gin (to_tsvector('simple', coalesce(title, '') || ' ' || coalesce(body, '')))"
        );
    }
}
