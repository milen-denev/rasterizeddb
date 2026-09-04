//! Cutting a statement's text at the point where a secret starts.
//!
//! Every surface that keeps or copies statement text needs this, and they do
//! not all live behind the same feature — the statement log is optional, the
//! server log is not. So it is here rather than inside
//! [`crate::core::query_stats`], with one implementation and one set of tests,
//! reachable from anywhere.
//!
//! # Why a keyword cut rather than a parser
//!
//! Deliberately cruder than parsing. Two grammars put a credential in statement
//! text today — role management (`PASSWORD`) and per-store S3 credentials
//! (`STORAGE ADD STORE … WITH (ACCESS_KEY_ID = '…', SECRET_ACCESS_KEY = '…')`)
//! — and a third added later should be redacted by this without anyone
//! remembering to come back here. A parser would have to be taught each one.
//!
//! # Why the cut takes everything after it
//!
//! The credential options live in one comma-separated `WITH (…)` clause, so
//! cutting at the *earliest* match removes every later one. That is what makes
//! the keyword list survivable when it drifts from the grammar: a spelling
//! added to [`crate::core::storage::kv_sql`] and not added here is still
//! covered as long as one of its siblings appears first.
//!
//! What is left still identifies the statement, which is the point of
//! truncating rather than dropping the record — an operator can see *that* a
//! store was configured without being handed its keys.

/// Keywords after which a statement is carrying something that must not be
/// stored, copied or logged. Matched as whole words, case-insensitively.
///
/// The source of truth for the store spellings is `ACCESS_KEY_OPTIONS` /
/// `SECRET_KEY_OPTIONS` / `SESSION_TOKEN_OPTIONS` in
/// `crate::core::storage::kv_sql`; they are repeated rather than imported
/// because that module is behind a feature and this must not be.
///
/// The bare words `SECRET` and `TOKEN` are included on purpose, and they are
/// not only option names: a statement with a column called `token` is very
/// likely carrying a session token in a literal. Truncating an ordinary query
/// costs a log line; not truncating that one costs the token.
pub const SECRET_BEARING: &[&str] = &[
    "PASSWORD",
    "ACCESS_KEY_ID",
    "ACCESS_KEY",
    "SECRET_ACCESS_KEY",
    "SECRET_KEY",
    "SECRET",
    "SESSION_TOKEN",
    "TOKEN",
];

/// `sql` with everything from the first secret-bearing keyword onward removed.
///
/// Returns the statement unchanged when it carries none, which is every
/// ordinary one — the cost is a scan of the text per surface that stores it.
pub fn statement(sql: &str) -> String {
    let mut earliest: Option<(usize, &str)> = None;
    for word in SECRET_BEARING {
        if let Some(at) = find_word_ignore_case(sql, word) {
            if earliest.is_none_or(|(best, _)| at < best) {
                earliest = Some((at, word));
            }
        }
    }
    match earliest {
        Some((at, word)) => format!("{}{} <redacted>", &sql[..at], word),
        None => sql.to_string(),
    }
}

/// The byte index of `word` in `text`, matched case-insensitively and only
/// where it stands as a whole word.
///
/// Whole-word matters in both directions: `password_hash` as a column name is
/// not a secret, and `SECRET_ACCESS_KEY` must not be missed because the shorter
/// `SECRET` is checked separately.
fn find_word_ignore_case(text: &str, word: &str) -> Option<usize> {
    let upper = text.to_ascii_uppercase();
    let mut from = 0usize;
    while let Some(at) = upper[from..].find(word) {
        let start = from + at;
        let end = start + word.len();
        let before_ok = start == 0 || !is_ident_byte(upper.as_bytes()[start - 1]);
        let after_ok = end >= upper.len() || !is_ident_byte(upper.as_bytes()[end]);
        if before_ok && after_ok {
            return Some(start);
        }
        from = end;
    }
    None
}

#[inline]
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_role_password_is_cut() {
        assert_eq!(
            statement("CREATE USER alice PASSWORD 's3cret'"),
            "CREATE USER alice PASSWORD <redacted>"
        );
        assert_eq!(
            statement("IMPORT FROM POSTGRES HOST 'h' USER 'u' PASSWORD 'p' INTO t"),
            "IMPORT FROM POSTGRES HOST 'h' USER 'u' PASSWORD <redacted>"
        );
    }

    /// The statement that writes S3 keys into a 0600 file. Every surface that
    /// keeps statement text would otherwise put them straight back into a
    /// readable one.
    #[test]
    fn store_credentials_are_cut_at_the_first_of_them() {
        let kept = statement(
            "STORAGE ADD STORE vault S3 WITH (ENDPOINT = 'http://127.0.0.1:9000', \
             BUCKET = 'b', ACCESS_KEY_ID = 'AKIAEXAMPLE', \
             SECRET_ACCESS_KEY = 'sh-hunter2', SESSION_TOKEN = 'tok-abc')",
        );
        for secret in ["AKIAEXAMPLE", "sh-hunter2", "tok-abc"] {
            assert!(!kept.contains(secret), "{secret} survived: {kept}");
        }
        // Still identifies the statement.
        assert!(kept.starts_with("STORAGE ADD STORE vault S3"), "{kept}");
    }

    #[test]
    fn every_spelling_is_caught_on_its_own() {
        for option in SECRET_BEARING {
            let sql = format!("STORAGE ADD STORE s S3 WITH ({} = 'leaked')", option);
            assert!(
                !statement(&sql).contains("leaked"),
                "{option}: {}",
                statement(&sql)
            );
        }
    }

    /// A column that merely contains one of the words is not a secret.
    #[test]
    fn an_ordinary_statement_is_left_alone() {
        for sql in [
            "SELECT password_hash FROM users",
            "SELECT * FROM tokens WHERE id = 1",
            "UPDATE t SET secretive = 1",
        ] {
            assert_eq!(statement(sql), sql, "wrongly truncated: {sql}");
        }
    }
}
