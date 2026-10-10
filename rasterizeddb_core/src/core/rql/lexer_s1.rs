use crate::core::{processor::transformer::ComparerOperation, row::common::simd_compare_strings};

#[derive(Debug)]
pub enum QueryPurpose {
    CreateTable(String),
    DropTable(String),
    /// `CREATE INDEX ix ON t ((doc->>'k'))` — an index over a path into a
    /// document. Every other index DDL stays the no-op it was, because the
    /// engine already maintains an index over each ordinary column; a path is
    /// the case it cannot infer.
    CreateIndex(String),
    DropIndex(String),
    AddColumn(String),
    DropColumn(String),
    RenameColumn(String),
    InsertRow(InsertRowData),
    UpdateRow(String),
    DeleteRow(String),
    QueryRows(QueryRowsData),
    Vacuum(VacuumRequest),
    /// `LOCK [TABLE] a [, b] [IN <mode> MODE] [NOWAIT]`.
    ///
    /// A parse failure is carried rather than reported as an unrecognised
    /// statement, so the caller is told which clause was wrong instead of
    /// getting a bare syntax error.
    LockTable(Result<crate::core::rql::row_lock::LockTable, String>),
    /// `SET` / `SHOW` / `RESET FILTER` — tuning the filter index from SQL.
    /// Parsed and executed by `core::filter::sql`.
    FilterControl(String),
    /// `SHOW TABLES` / `SHOW ... STATS` / `SHOW SCHEMA MAP` /
    /// `SHOW`/`SET CONFIG` — the introspection surface a console reads.
    /// Parsed and executed by [`crate::core::introspect`].
    Introspect(crate::core::introspect::Introspect),
    /// An introspection statement that did not parse. Carried rather than
    /// reported as an unrecognised query, so the operator sees which clause
    /// was wrong instead of a bare syntax error.
    IntrospectError(String),
    /// `REPLICATION ...` / `SHOW REPLICATION ...` — running the cluster from
    /// SQL. Parsed and executed by [`crate::replication::sql`].
    Replication(String),
    /// `STORAGE ...` / `SHOW STORAGE ...` — the directories this database may
    /// write into, and where each table's files are. Parsed and executed by
    /// [`crate::core::storage::sql`].
    #[cfg(feature = "storage_pools")]
    Storage(String),
    /// `BACKUP` / `RESTORE` / `SHOW BACKUPS`. Parsed by `rql::lexer_backup` and
    /// executed by `rql::backup_exec`.
    #[cfg(feature = "s3_backup")]
    Backup(crate::core::rql::lexer_backup::BackupStatement),
    /// A backup statement that did not parse. Carried through rather than
    /// reported as an unrecognised query so the operator sees which clause was
    /// wrong instead of a bare syntax error.
    #[cfg(feature = "s3_backup")]
    BackupError(String),
}

/// A `VACUUM` statement.
///
/// `table_name` of `None` means every table. `full` selects compaction — which
/// rewrites the table and returns space to the filesystem, stopping all other
/// work on it — over the incremental reclamation that only rebuilds the free
/// list. Same distinction PostgreSQL draws with `VACUUM` and `VACUUM FULL`.
#[derive(Debug)]
pub struct VacuumRequest {
    pub table_name: Option<String>,
    pub full: bool,
}

#[derive(Debug)]
pub struct InsertRowData {
    pub table_name: String,
    pub query: String,
    /// `ON CONFLICT … DO NOTHING` was written, so a uniqueness violation is the
    /// statement working rather than failing.
    ///
    /// The arbiter, if one was given, is not consulted. PostgreSQL uses it to
    /// pick *which* constraint may conflict and errors when it names none; here
    /// any uniqueness violation is swallowed. That is more permissive, and the
    /// difference only shows up for a statement that would have been an error in
    /// PostgreSQL — never for one that would have succeeded.
    pub on_conflict_do_nothing: bool,
    /// The `DO UPDATE` half of an upsert, when one was written.
    ///
    /// Kept as text rather than parsed here: the assignments reference
    /// `EXCLUDED.<column>`, whose values are only known once the row that
    /// would have been inserted has been built, so resolving them is the
    /// executor's job.
    pub on_conflict_update: Option<ConflictUpdate>,
}

/// The `DO UPDATE SET … [WHERE …]` of an `INSERT … ON CONFLICT`.
#[derive(Debug, Clone)]
pub struct ConflictUpdate {
    /// Columns named by the arbiter, `ON CONFLICT (a, b)`. Empty when none was
    /// written, in which case the conflicting row is found by whichever unique
    /// column the insert actually collided on.
    pub arbiter: Vec<String>,
    /// The `SET` list, as written.
    pub assignments: String,
    /// The optional `WHERE` that decides whether the update happens at all.
    pub where_clause: Option<String>,
}

/// Splits a trailing `ON CONFLICT …` clause off an `INSERT`.
///
/// Returns the statement without the clause, whether it was `DO NOTHING`, and
/// the `DO UPDATE` half when there was one.
///
/// A `RETURNING` after the conflict clause belongs to the statement, not to the
/// clause, so it is moved back onto the `INSERT`. Cutting at `ON CONFLICT` and
/// discarding the rest would have dropped it, and the caller would have got no
/// rows back from a statement that asked for them.
fn split_on_conflict(query: &str) -> (String, bool, Option<ConflictUpdate>) {
    let upper = query.to_ascii_uppercase();
    let Some(at) = find_top_level(&upper, "ON CONFLICT") else {
        return (query.to_string(), false, None);
    };

    let clause = &query[at..];
    let clause_upper = &upper[at..];

    // `RETURNING` is the statement's, wherever it sits.
    let (clause, clause_upper, returning) = match find_top_level(clause_upper, "RETURNING") {
        Some(r) => (&clause[..r], &clause_upper[..r], Some(clause[r..].trim())),
        None => (clause, clause_upper, None),
    };

    let head = query[..at].trim_end().trim_end_matches(';').trim_end();
    let rebuilt = match returning {
        Some(r) => format!("{} {}", head, r),
        None => head.to_string(),
    };

    if clause_upper.contains("DO NOTHING") {
        return (rebuilt, true, None);
    }

    let Some(do_at) = clause_upper.find("DO UPDATE") else {
        // Neither form: leave the statement alone so it is reported rather than
        // silently treated as one of them.
        return (query.to_string(), false, None);
    };

    // `ON CONFLICT (a, b) DO UPDATE …`
    let arbiter_text = clause[..do_at].trim();
    let arbiter = match (arbiter_text.find('('), arbiter_text.rfind(')')) {
        (Some(open), Some(close)) if close > open => arbiter_text[open + 1..close]
            .split(',')
            .map(|c| c.trim().trim_matches('"').to_string())
            .filter(|c| !c.is_empty())
            .collect(),
        _ => Vec::new(),
    };

    let after_do = &clause[do_at + "DO UPDATE".len()..];
    let after_do_upper = &clause_upper[do_at + "DO UPDATE".len()..];
    let Some(set_at) = after_do_upper.find("SET") else {
        return (query.to_string(), false, None);
    };
    let body = &after_do[set_at + "SET".len()..];
    let body_upper = &after_do_upper[set_at + "SET".len()..];

    let (assignments, where_clause) = match find_top_level(body_upper, "WHERE") {
        Some(w) => (
            body[..w].trim().to_string(),
            Some(body[w + "WHERE".len()..].trim().to_string()),
        ),
        None => (body.trim().to_string(), None),
    };

    (
        rebuilt,
        false,
        Some(ConflictUpdate {
            arbiter,
            assignments,
            where_clause,
        }),
    )
}

/// Finds `needle` in `haystack` outside quotes and parentheses.
///
/// `haystack` must already be upper-cased; `needle` is matched literally.
fn find_top_level(haystack: &str, needle: &str) -> Option<usize> {
    let bytes = haystack.as_bytes();
    let needle_bytes = needle.as_bytes();
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
            }
            None => match b {
                b'\'' | b'"' => quote = Some(b),
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {
                    if depth == 0 && bytes[i..].starts_with(needle_bytes) {
                        return Some(i);
                    }
                }
            },
        }
        i += 1;
    }
    None
}

#[derive(Debug)]
pub struct QueryRowsData {
    pub table_name: String,
    pub query: String,
}

/// Normalises a relation reference into the bare table name.
///
/// ORMs quote every identifier and may schema-qualify it, so the token after
/// `FROM` or `INSERT INTO` arrives as `"users"` or `"public"."users"` rather
/// than `users`. A trailing `(` from `INSERT INTO t(...)` is dropped too.
fn normalize_table_name(raw: &str) -> String {
    let raw = raw.trim().trim_end_matches(|c| c == ',' || c == ';');
    let raw = raw.split('(').next().unwrap_or(raw);
    raw.split('.')
        .next_back()
        .unwrap_or(raw)
        .trim()
        .trim_matches('"')
        .to_string()
}

/// Recognizes common PostgreSQL SQL and returns a QueryPurpose with the query string.
/// Removes `--` line comments and `/* … */` block comments.
///
/// Text inside a string literal or a quoted identifier is data, so a `--` there
/// is left alone.
pub fn strip_sql_comments(sql: &str) -> String {
    let bytes = sql.as_bytes();
    // Bytes, not chars. `push(byte as char)` re-encoded every byte above 0x7F
    // as two UTF-8 bytes, so a statement carrying any non-ASCII text — an
    // accented name, a currency sign, an emoji — reached the engine
    // double-encoded and was *stored* that way. Nothing failed; the value read
    // back as mojibake. Scanning bytes is safe because every delimiter this
    // looks for is ASCII, and UTF-8 never encodes an ASCII byte inside a
    // multi-byte sequence.
    let mut out: Vec<u8> = Vec::with_capacity(sql.len());
    let mut i = 0usize;
    let mut quote: Option<u8> = None;

    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = quote {
            out.push(c);
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if c == b'\'' || c == b'"' {
            quote = Some(c);
            out.push(c);
            i += 1;
            continue;
        }
        if c == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            // The newline itself stays, so tokens either side do not merge.
            continue;
        }
        if c == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            // A block comment separates tokens, exactly as whitespace would.
            out.push(b' ');
            continue;
        }
        out.push(c);
        i += 1;
    }
    // The input was valid UTF-8 and only whole ASCII bytes were dropped, so
    // this cannot fail; `from_utf8_lossy` rather than an unwrap so that a
    // future edit which does cut a sequence degrades instead of panicking.
    match String::from_utf8(out) {
        Ok(text) => text,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    }
}

pub fn recognize_query_purpose(query: &str) -> Option<QueryPurpose> {
    use ComparerOperation::StartsWith;
    use QueryPurpose::*;
    // Every SQL client terminates a statement with a semicolon. Stripping it
    // once here covers every statement kind, rather than each clause parser
    // having to tolerate one — the `WHERE` lexer rejected it as an unexpected
    // character, so `… WHERE id = 1;` failed while `… WHERE id = 1` worked.
    // Comments are removed before anything reads the statement. A migration
    // that explains itself — `-- was COALESCE(...)` on the line above an
    // UPDATE — is ordinary SQL, and every clause parser would otherwise have to
    // tolerate one. Without this the whole statement came back as "syntax error
    // or unsupported query".
    // Escape strings (`E'it\'s'`) and dollar-quoted strings are rewritten
    // into the standard strings they mean before anything scans the text:
    // every scanner below knows standard strings, and an escaped quote ended
    // the literal early for all of them. See `sql_scan`. A no-op (borrowed)
    // for text that has neither, which includes everything the wire layer has
    // already normalised.
    let normalized = match crate::core::sql_scan::normalize_string_literals(query) {
        Ok(text) => text,
        Err(e) => {
            log::warn!("statement rejected: {}", e);
            return None;
        }
    };
    let without_comments = strip_sql_comments(&normalized);
    let query = without_comments.as_str();
    let query_trimmed = query.trim().trim_end_matches(';').trim_end();
    let query_trimmed = if query_trimmed.is_empty() {
        query.trim()
    } else {
        query_trimmed
    };
    let query_upper = query_trimmed.to_ascii_uppercase();
    let query_bytes = query_upper.as_bytes();

    // Recognised before the DDL/DML prefixes: `SET`/`SHOW`/`RESET` are not
    // otherwise statement keywords here, and the filter surface owns the whole
    // `... FILTER ...` shape.
    if crate::core::filter::sql::is_filter_statement(query_trimmed) {
        return Some(FilterControl(query_trimmed.to_string()));
    }

    // Likewise ahead of the rest: `REPLICATION` is not a statement keyword
    // anywhere else, and `SHOW REPLICATION` must not be read as a `SHOW` of
    // something called `REPLICATION`.
    if crate::replication::sql::is_replication_statement(query_trimmed) {
        return Some(Replication(query_trimmed.to_string()));
    }

    // Alongside the two above and for the same reason: `STORAGE` is not a
    // statement keyword anywhere else, and `SHOW STORAGE` must not be read as a
    // `SHOW` of a thing called `STORAGE`.
    #[cfg(feature = "storage_pools")]
    if crate::core::storage::sql::is_storage_statement(query_trimmed) {
        return Some(Storage(query_trimmed.to_string()));
    }

    // After the three surfaces above, each of which owns a longer `SHOW`
    // phrase, and before everything below: `SHOW TABLES` and `SET CONFIG` are
    // not otherwise statement keywords here.
    match crate::core::introspect::parse(query_trimmed) {
        Some(Ok(statement)) => return Some(Introspect(statement)),
        Some(Err(message)) => return Some(IntrospectError(message)),
        None => {}
    }

    // Also before the DDL/DML prefixes, and before the filter surface would see
    // a bare `SHOW`: `SHOW BACKUPS` and `RESTORE` share no keyword with any
    // other statement here, and `BACKUP` is not otherwise a keyword at all.
    #[cfg(feature = "s3_backup")]
    match crate::core::rql::lexer_backup::parse(query_trimmed) {
        Ok(Some(statement)) => return Some(Backup(statement)),
        Ok(None) => {}
        Err(message) => return Some(BackupError(message)),
    }

    if simd_compare_strings(query_bytes, b"VACUUM", &StartsWith) {
        // `VACUUM`, `VACUUM FULL`, `VACUUM t`, `VACUUM FULL t`. Postgres also
        // accepts a parenthesised option list and a column list; neither adds
        // anything this engine can act on, so both are ignored rather than
        // rejected — a client that sends `VACUUM (ANALYZE) t` gets its table
        // vacuumed instead of a syntax error.
        let mut words = query_trimmed.split_whitespace().skip(1).peekable();
        let mut full = false;
        let mut table_name = None;

        while let Some(word) = words.next() {
            let upper = word.to_ascii_uppercase();
            if upper.starts_with('(') {
                // Skip the option list, however it is spaced.
                if !word.ends_with(')') {
                    for rest in words.by_ref() {
                        if rest.ends_with(')') {
                            break;
                        }
                    }
                }
                continue;
            }
            match upper.as_str() {
                "FULL" => full = true,
                "FREEZE" | "VERBOSE" | "ANALYZE" | "ANALYSE" => {}
                _ => {
                    table_name = Some(normalize_table_name(word));
                    break;
                }
            }
        }

        Some(Vacuum(VacuumRequest { table_name, full }))
    } else if simd_compare_strings(query_bytes, b"CREATE INDEX", &StartsWith)
        && crate::core::rql::path_index::parse_create(query_trimmed).is_some()
    {
        Some(CreateIndex(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"DROP INDEX", &StartsWith) {
        Some(DropIndex(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"CREATE TABLE", &StartsWith) {
        Some(CreateTable(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"DROP TABLE", &StartsWith) {
        Some(DropTable(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"ALTER TABLE", &StartsWith) {
        let upper = query_trimmed.to_ascii_uppercase();
        // Every `ALTER TABLE` is routed to the one handler, which decides what
        // the statement asks for. Matching on `ADD COLUMN` and friends left
        // `ADD CONSTRAINT`, `ADD FOREIGN KEY` and bare `ADD col type` — all of
        // which a migration emits — reported as syntax errors.
        if upper.contains("DROP COLUMN") || upper.contains(" DROP ") {
            Some(DropColumn(query_trimmed.to_string()))
        } else if upper.contains("RENAME") {
            Some(RenameColumn(query_trimmed.to_string()))
        } else {
            Some(AddColumn(query_trimmed.to_string()))
        }
    } else if simd_compare_strings(query_bytes, b"LOCK", &StartsWith) {
        match crate::core::rql::row_lock::parse_lock_table(query_trimmed) {
            Some(outcome) => Some(LockTable(outcome)),
            None => None,
        }
    } else if simd_compare_strings(query_bytes, b"INSERT INTO", &StartsWith) {
        if let Some(table_name_extract) = query_trimmed.split_whitespace().nth(2) {
            let (query, on_conflict_do_nothing, on_conflict_update) =
                split_on_conflict(query_trimmed);
            Some(InsertRow(InsertRowData {
                table_name: normalize_table_name(table_name_extract),
                query,
                on_conflict_do_nothing,
                on_conflict_update,
            }))
        } else {
            None
        }
    } else if simd_compare_strings(query_bytes, b"UPDATE", &StartsWith) {
        Some(UpdateRow(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"DELETE FROM", &StartsWith) {
        Some(DeleteRow(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"SELECT", &StartsWith) {
        // Extract table name after the statement's own "FROM".
        //
        // Depth-aware, because a select item may contain one: `EXTRACT(EPOCH
        // FROM ts)` made the table name `ts)`, which then failed as "relation
        // does not exist" — naming nothing the author wrote, about a statement
        // whose real `FROM` names a table that is there.
        let tokens: Vec<&str> = query_trimmed.split_whitespace().collect();
        let from_index = {
            let mut depth = 0i32;
            let mut quote: Option<char> = None;
            let mut found = None;
            for (at, token) in tokens.iter().enumerate() {
                if depth == 0 && quote.is_none() && token.eq_ignore_ascii_case("FROM") {
                    found = Some(at);
                    break;
                }
                for c in token.chars() {
                    match quote {
                        Some(q) => {
                            if c == q {
                                quote = None;
                            }
                        }
                        None => match c {
                            '\'' | '"' => quote = Some(c),
                            '(' => depth += 1,
                            ')' => depth -= 1,
                            _ => {}
                        },
                    }
                }
            }
            found
        };
        if let Some(from_index) = from_index {
            if let Some(table_name_extract) = tokens.get(from_index + 1) {
                Some(QueryRows(QueryRowsData {
                    table_name: normalize_table_name(table_name_extract),
                    query: query_trimmed.to_string(),
                }))
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod strip_comment_tests {
    use super::strip_sql_comments;

    /// The comment stripper used to walk bytes and push each one as a `char`,
    /// which re-encoded everything above 0x7F. Every statement carrying an
    /// accent, a currency sign or an emoji arrived at the engine double-encoded
    /// and was stored that way — no error anywhere, just mojibake on read-back.
    #[test]
    fn non_ascii_text_survives_unchanged() {
        for text in [
            "INSERT INTO t (s) VALUES ('ünïcødé')",
            "INSERT INTO t (s) VALUES ('😀 emoji')",
            "INSERT INTO t (s) VALUES ('日本語')",
            "INSERT INTO t (s) VALUES ('naïve café — 10 €')",
            "SELECT * FROM t WHERE s = 'Ω'",
        ] {
            assert_eq!(strip_sql_comments(text), text, "mangled {:?}", text);
        }
    }

    #[test]
    fn non_ascii_survives_alongside_a_comment() {
        assert_eq!(
            strip_sql_comments("INSERT INTO t (s) VALUES ('café') -- naïve\n"),
            "INSERT INTO t (s) VALUES ('café') \n"
        );
    }

    #[test]
    fn comments_are_still_removed() {
        assert_eq!(
            strip_sql_comments("SELECT 1 -- trailing\nFROM t"),
            "SELECT 1 \nFROM t"
        );
        assert_eq!(strip_sql_comments("SELECT /* mid */ 1"), "SELECT   1");
        // A `--` inside a literal is data, not a comment.
        assert_eq!(
            strip_sql_comments("SELECT '-- not a comment'"),
            "SELECT '-- not a comment'"
        );
    }
}
