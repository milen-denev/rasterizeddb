//! Normalises the many spellings of "give me a page of rows" into the one form
//! the clause splitters understand: `LIMIT n OFFSET m`.
//!
//! ```text
//! LIMIT ALL                        =>  (no limit)
//! OFFSET 10 ROWS                   =>  OFFSET 10
//! FETCH FIRST 10 ROWS ONLY         =>  LIMIT 10
//! FETCH NEXT ROW ONLY              =>  LIMIT 1
//! OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY
//!                                  =>  LIMIT 5 OFFSET 10
//! TAKE 10 SKIP 5                   =>  LIMIT 10 OFFSET 5
//! ```
//!
//! `LIMIT`/`OFFSET` is PostgreSQL's own spelling, `OFFSET … FETCH FIRST … ONLY`
//! is the SQL:2008 one every other engine also accepts, and `TAKE`/`SKIP` is
//! the LINQ-style spelling. All three mean the same thing here, so the rest of
//! the engine only ever sees the first.
//!
//! Rewriting happens on the statement text rather than on a parsed tree because
//! that is what the clause splitters in `lexer_query` and `join` consume — they
//! search the statement for ` LIMIT ` and ` OFFSET `, so a statement that spells
//! its page a different way would otherwise have the whole tail silently
//! ignored and return every row.

/// A word of the statement that sits outside quotes and at parenthesis depth 0.
///
/// Depth matters: `WHERE id IN (SELECT id FROM t LIMIT 1)` carries a `LIMIT`
/// that belongs to the subquery, not to this statement.
struct Word {
    start: usize,
    text: String,
}

/// Splits the statement into its top-level words.
///
/// Quoted text is stepped over whole — a string literal may contain anything,
/// including the word `LIMIT` — and so is anything nested in parentheses.
fn top_level_words(sql: &str) -> Vec<Word> {
    let bytes = sql.as_bytes();
    let mut words = Vec::new();
    let mut depth: i32 = 0;
    let mut i = 0;
    let mut word_start: Option<usize> = None;

    // Closes the word in progress, if there is one.
    macro_rules! flush {
        ($end:expr) => {
            if let Some(start) = word_start.take() {
                words.push(Word {
                    start,
                    text: sql[start..$end].to_string(),
                });
            }
        };
    }

    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'\'' | b'"' => {
                flush!(i);
                let quote = c;
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        // `''` inside a string literal is an escaped quote, not
                        // the end of it.
                        if quote == b'\'' && bytes.get(i + 1) == Some(&b'\'') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'(' => {
                flush!(i);
                depth += 1;
                i += 1;
            }
            b')' => {
                flush!(i);
                depth -= 1;
                i += 1;
            }
            _ if c.is_ascii_whitespace() || c == b',' || c == b';' => {
                flush!(i);
                i += 1;
            }
            _ => {
                if depth == 0 {
                    if word_start.is_none() {
                        word_start = Some(i);
                    }
                } else {
                    flush!(i);
                }
                i += 1;
            }
        }
    }
    flush!(bytes.len());

    words.retain(|w| !w.text.is_empty());
    words
}

fn is_count(token: &str) -> bool {
    !token.is_empty() && token.bytes().all(|b| b.is_ascii_digit())
}

/// True when a token was clearly *meant* as a row count even though it is not a
/// literal one — a bind parameter, or a negative number.
///
/// Such a token makes the clause a pagination clause that then fails to parse,
/// which is reported. Without this, `LIMIT $1` from a client that never bound
/// its parameter would look like something other than a `LIMIT` and the cap
/// would be dropped on the floor.
fn looks_like_intended_count(token: &str) -> bool {
    is_count(token)
        || token.starts_with('$')
        || token.starts_with('-')
        || token.starts_with(':')
        || token == "?"
}

fn parse_count(token: &str, keyword: &str) -> Result<u64, String> {
    token
        .parse::<u64>()
        .map_err(|_| format!("Invalid {} value '{}'", keyword, token))
}

/// The row cap and starting offset a statement's pagination clauses ask for.
#[derive(Debug, Default, PartialEq, Eq)]
struct Page {
    limit: Option<u64>,
    offset: Option<u64>,
}

impl Page {
    fn set_limit(&mut self, n: u64) -> Result<(), String> {
        if self.limit.is_some() {
            return Err("multiple LIMIT clauses are not allowed".to_string());
        }
        self.limit = Some(n);
        Ok(())
    }

    fn set_offset(&mut self, n: u64) -> Result<(), String> {
        if self.offset.is_some() {
            return Err("multiple OFFSET clauses are not allowed".to_string());
        }
        self.offset = Some(n);
        Ok(())
    }
}

/// Reads the pagination clauses out of `words`, which must run to the end of the
/// statement.
///
/// `LIMIT ALL` is PostgreSQL's way of writing "no cap at all", so it parses to
/// no limit rather than to a limit of zero.
fn parse_page(words: &[Word]) -> Result<Page, String> {
    let mut page = Page::default();
    let mut i = 0;

    while i < words.len() {
        let keyword = words[i].text.to_ascii_uppercase();
        let next = words.get(i + 1).map(|w| w.text.as_str());
        match keyword.as_str() {
            "LIMIT" => {
                let token = next.ok_or("LIMIT is present but no value was provided")?;
                if token.eq_ignore_ascii_case("ALL") {
                    // No cap. Recorded as "seen" all the same, so a second
                    // LIMIT is still rejected.
                    if page.limit.is_some() {
                        return Err("multiple LIMIT clauses are not allowed".to_string());
                    }
                } else {
                    page.set_limit(parse_count(token, "LIMIT")?)?;
                }
                i += 2;
            }
            "TAKE" => {
                let token = next.ok_or("TAKE is present but no value was provided")?;
                page.set_limit(parse_count(token, "TAKE")?)?;
                i += 2;
            }
            "SKIP" => {
                let token = next.ok_or("SKIP is present but no value was provided")?;
                page.set_offset(parse_count(token, "SKIP")?)?;
                i += 2;
            }
            "OFFSET" => {
                let token = next.ok_or("OFFSET is present but no value was provided")?;
                page.set_offset(parse_count(token, "OFFSET")?)?;
                i += 2;
                // `OFFSET 10 ROWS` — the SQL:2008 spelling. The noise word is
                // optional and carries no meaning.
                if words.get(i).is_some_and(|w| {
                    w.text.eq_ignore_ascii_case("ROW") || w.text.eq_ignore_ascii_case("ROWS")
                }) {
                    i += 1;
                }
            }
            "FETCH" => {
                let position = next.ok_or("FETCH must be followed by FIRST or NEXT")?;
                if !position.eq_ignore_ascii_case("FIRST") && !position.eq_ignore_ascii_case("NEXT")
                {
                    return Err(format!(
                        "FETCH must be followed by FIRST or NEXT, found '{}'",
                        position
                    ));
                }
                i += 2;

                // The count is optional: `FETCH FIRST ROW ONLY` means one row.
                let count = match words.get(i) {
                    Some(word)
                        if !word.text.eq_ignore_ascii_case("ROW")
                            && !word.text.eq_ignore_ascii_case("ROWS") =>
                    {
                        let n = parse_count(&word.text, "FETCH FIRST")?;
                        i += 1;
                        n
                    }
                    _ => 1,
                };

                match words.get(i) {
                    Some(word)
                        if word.text.eq_ignore_ascii_case("ROW")
                            || word.text.eq_ignore_ascii_case("ROWS") =>
                    {
                        i += 1;
                    }
                    Some(word) => {
                        return Err(format!(
                            "FETCH FIRST expects ROW or ROWS, found '{}'",
                            word.text
                        ));
                    }
                    None => return Err("FETCH FIRST is missing ROW ONLY".to_string()),
                }

                match words.get(i) {
                    Some(word) if word.text.eq_ignore_ascii_case("ONLY") => i += 1,
                    // `WITH TIES` returns the rows that tie with the last one on
                    // the ORDER BY key, so it can return more rows than asked
                    // for. Refused rather than silently downgraded to ONLY.
                    Some(word) if word.text.eq_ignore_ascii_case("WITH") => {
                        return Err("FETCH FIRST ... WITH TIES is not supported yet".to_string());
                    }
                    Some(word) => {
                        return Err(format!("FETCH FIRST expects ONLY, found '{}'", word.text));
                    }
                    None => return Err("FETCH FIRST is missing ONLY".to_string()),
                }

                page.set_limit(count)?;
            }
            other => {
                return Err(format!(
                    "'{}' cannot follow a LIMIT/OFFSET/FETCH clause",
                    other
                ));
            }
        }
    }

    Ok(page)
}

/// True when the word at `index` starts a pagination clause rather than merely
/// being a column that happens to share the name.
///
/// Only the first token after the keyword is inspected. `WHERE offset = 5` and
/// `SELECT take FROM t` therefore stay ordinary column references, while
/// `OFFSET 5` and `TAKE 10` are recognised.
fn starts_pagination(words: &[Word], index: usize) -> bool {
    let keyword = words[index].text.to_ascii_uppercase();
    let Some(next) = words.get(index + 1).map(|w| w.text.as_str()) else {
        return false;
    };
    match keyword.as_str() {
        "LIMIT" => looks_like_intended_count(next) || next.eq_ignore_ascii_case("ALL"),
        "OFFSET" | "TAKE" | "SKIP" => looks_like_intended_count(next),
        "FETCH" => next.eq_ignore_ascii_case("FIRST") || next.eq_ignore_ascii_case("NEXT"),
        _ => false,
    }
}

/// The row cap and the offset a clause made only of pagination asks for, as
/// `(limit, offset)`: `OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY` is `(Some(3),
/// Some(2))`, and `LIMIT ALL` no cap. Anything else in the clause is an
/// error, as it is at the end of a statement.
///
/// For a tail that has already been cut from its statement — a `UNION`'s,
/// whose page applies to the combined rows — where [`normalize_pagination`]
/// has no `FROM` to start its search after.
pub fn read_page_clause(clause: &str) -> Result<(Option<u64>, Option<u64>), String> {
    let trimmed = clause.trim().trim_end_matches(';').trim_end();
    let page = parse_page(&top_level_words(trimmed))?;
    Ok((page.limit, page.offset))
}

/// Rewrites a `SELECT`'s pagination clauses into the canonical
/// `LIMIT n OFFSET m` tail, leaving the rest of the statement untouched.
///
/// A statement with no pagination clause is returned as it came in.
pub fn normalize_pagination(sql: &str) -> Result<String, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let words = top_level_words(trimmed);

    // The select list is skipped: a bare `SELECT limit FROM t` names a column,
    // and only what follows `FROM` can open a pagination clause.
    let from_at = words
        .iter()
        .position(|w| w.text.eq_ignore_ascii_case("FROM"));
    let search_from = match from_at {
        Some(i) => i + 1,
        None => return Ok(trimmed.to_string()),
    };

    let mut start = None;
    for i in search_from..words.len() {
        if starts_pagination(&words, i) {
            start = Some(i);
            break;
        }
    }
    let Some(start) = start else {
        return Ok(trimmed.to_string());
    };

    let page = parse_page(&words[start..])?;

    // Nothing between the head and the end of the statement survives, so a
    // canonical tail is appended to the text before the first clause.
    let mut out = trimmed[..words[start].start].trim_end().to_string();
    if let Some(limit) = page.limit {
        out.push_str(&format!(" LIMIT {}", limit));
    }
    if let Some(offset) = page.offset {
        out.push_str(&format!(" OFFSET {}", offset));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(sql: &str) -> String {
        normalize_pagination(sql).unwrap()
    }

    fn err(sql: &str) -> String {
        normalize_pagination(sql).unwrap_err()
    }

    #[test]
    fn postgres_limit_offset_passes_through_unchanged() {
        assert_eq!(norm("SELECT id FROM t LIMIT 5"), "SELECT id FROM t LIMIT 5");
        assert_eq!(
            norm("SELECT id FROM t WHERE id > 3 ORDER BY id LIMIT 5 OFFSET 2"),
            "SELECT id FROM t WHERE id > 3 ORDER BY id LIMIT 5 OFFSET 2"
        );
    }

    #[test]
    fn a_statement_without_pagination_is_untouched() {
        assert_eq!(
            norm("SELECT id, name FROM t WHERE id > 3"),
            "SELECT id, name FROM t WHERE id > 3"
        );
    }

    /// `LIMIT ALL` is PostgreSQL for "every row", so it must not become
    /// `LIMIT 0` — the offset that follows it still applies.
    #[test]
    fn limit_all_drops_the_cap() {
        assert_eq!(norm("SELECT id FROM t LIMIT ALL"), "SELECT id FROM t");
        assert_eq!(
            norm("SELECT id FROM t LIMIT ALL OFFSET 4"),
            "SELECT id FROM t OFFSET 4"
        );
    }

    #[test]
    fn offset_accepts_the_row_noise_word() {
        assert_eq!(
            norm("SELECT id FROM t OFFSET 7 ROWS"),
            "SELECT id FROM t OFFSET 7"
        );
        assert_eq!(
            norm("SELECT id FROM t OFFSET 1 ROW"),
            "SELECT id FROM t OFFSET 1"
        );
    }

    #[test]
    fn fetch_first_becomes_a_limit() {
        assert_eq!(
            norm("SELECT id FROM t FETCH FIRST 10 ROWS ONLY"),
            "SELECT id FROM t LIMIT 10"
        );
        assert_eq!(
            norm("SELECT id FROM t FETCH NEXT 10 ROWS ONLY"),
            "SELECT id FROM t LIMIT 10"
        );
    }

    /// The count is optional in SQL:2008 and defaults to one row.
    #[test]
    fn fetch_first_without_a_count_means_one_row() {
        assert_eq!(
            norm("SELECT id FROM t FETCH FIRST ROW ONLY"),
            "SELECT id FROM t LIMIT 1"
        );
    }

    #[test]
    fn offset_then_fetch_is_the_standard_pairing() {
        assert_eq!(
            norm("SELECT id FROM t ORDER BY id OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY"),
            "SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 20"
        );
    }

    #[test]
    fn take_and_skip_map_onto_limit_and_offset() {
        assert_eq!(
            norm("SELECT id FROM t TAKE 10"),
            "SELECT id FROM t LIMIT 10"
        );
        assert_eq!(norm("SELECT id FROM t SKIP 5"), "SELECT id FROM t OFFSET 5");
        assert_eq!(
            norm("SELECT id FROM t ORDER BY id SKIP 5 TAKE 10"),
            "SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 5"
        );
    }

    /// A `LIMIT` belonging to a subquery is not this statement's page.
    #[test]
    fn a_nested_limit_is_left_alone() {
        let sql = "SELECT id FROM t WHERE id IN (SELECT id FROM u LIMIT 1) LIMIT 5";
        assert_eq!(norm(sql), sql);
    }

    /// A string literal may contain anything at all.
    #[test]
    fn a_keyword_inside_a_literal_is_not_a_clause() {
        let sql = "SELECT id FROM t WHERE name = 'LIMIT 5'";
        assert_eq!(norm(sql), sql);
    }

    /// `take`, `skip` and `offset` are ordinary words in PostgreSQL, so a column
    /// may be named after one.
    #[test]
    fn a_column_named_like_a_keyword_still_works() {
        assert_eq!(norm("SELECT take FROM t"), "SELECT take FROM t");
        assert_eq!(
            norm("SELECT id FROM t WHERE offset = 5"),
            "SELECT id FROM t WHERE offset = 5"
        );
        assert_eq!(
            norm("SELECT id FROM t WHERE skip > 1 LIMIT 5"),
            "SELECT id FROM t WHERE skip > 1 LIMIT 5"
        );
    }

    /// An unbound parameter is a count that failed to arrive, not a missing
    /// clause: dropping the cap would return the whole table instead.
    #[test]
    fn an_unbound_parameter_is_reported_rather_than_ignored() {
        assert!(err("SELECT id FROM t LIMIT $1").contains("Invalid LIMIT value"));
    }

    #[test]
    fn conflicting_clauses_are_rejected() {
        assert!(err("SELECT id FROM t LIMIT 5 LIMIT 6").contains("multiple LIMIT"));
        assert!(err("SELECT id FROM t LIMIT 5 FETCH FIRST 2 ROWS ONLY").contains("multiple LIMIT"));
        assert!(err("SELECT id FROM t OFFSET 5 SKIP 2").contains("multiple OFFSET"));
        assert!(err("SELECT id FROM t LIMIT 5 TAKE 2").contains("multiple LIMIT"));
    }

    #[test]
    fn a_malformed_fetch_is_reported() {
        assert!(err("SELECT id FROM t FETCH FIRST 5 ROWS").contains("ONLY"));
        assert!(err("SELECT id FROM t FETCH FIRST 5 ONLY").contains("ROW"));
        assert!(err("SELECT id FROM t FETCH FIRST 5 ROWS WITH TIES").contains("WITH TIES"));
    }

    /// Trailing junk used to be dropped in silence, which returned a plausible
    /// but unpaged result set.
    #[test]
    fn junk_after_a_page_is_reported() {
        assert!(err("SELECT id FROM t LIMIT 5 GARBAGE").contains("GARBAGE"));
    }

    #[test]
    fn a_trailing_semicolon_is_tolerated() {
        assert_eq!(norm("SELECT id FROM t TAKE 3;"), "SELECT id FROM t LIMIT 3");
    }
}
