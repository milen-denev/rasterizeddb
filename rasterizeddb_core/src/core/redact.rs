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
    if let Some(kept) = kv_command(sql) {
        return kept;
    }
    match earliest_secret(sql.as_bytes()) {
        Some((at, word)) => format!("{}{} <redacted>", &sql[..at], word),
        None => sql.to_string(),
    }
}

/// What `statement(sql)` would be after keeping only its first `max_chars`
/// characters (with `…` appended when anything was cut), without copying or
/// scanning the whole of a large statement first.
///
/// Byte-identical to cutting the output of [`statement`]: pinned by
/// `bounded_matches_redact_then_truncate`. The scan covers the kept prefix
/// plus the longest keyword's length and one boundary byte, so a keyword that
/// *starts* before the cut is judged exactly as on the whole text — including
/// whether the byte after it continues the word. A keyword that starts at or
/// after the cut changes nothing kept, since redaction only ever removes text
/// from the keyword onwards. So no secret can straddle the cut: either its
/// keyword is before it and the text is cut there, or none of it is kept.
pub fn statement_bounded(sql: &str, max_chars: usize) -> String {
    if let Some(kept) = kv_command(sql) {
        return cut_chars(kept, max_chars);
    }
    // The byte where the `max_chars`-th character starts, if there is one.
    let cut = sql.char_indices().nth(max_chars).map(|(i, _)| i);
    let window_end = match cut {
        Some(c) => sql.len().min(c + KEYWORD_MAX_LEN + 2),
        None => sql.len(),
    };
    let bytes = &sql.as_bytes()[..window_end];
    let found = earliest_secret(bytes).filter(|&(at, _)| cut.is_none_or(|c| at < c));
    match found {
        Some((at, word)) => {
            // At most `max_chars` + the keyword + the marker: cheap to cut.
            let redacted = format!("{}{} <redacted>", &sql[..at], word);
            cut_chars(redacted, max_chars)
        }
        None => match cut {
            None => sql.to_string(),
            Some(c) => {
                let mut kept = String::with_capacity(c + '…'.len_utf8());
                kept.push_str(&sql[..c]);
                kept.push('…');
                kept
            }
        },
    }
}

/// A `KV …` statement (the key/value store's commands) reduced to its
/// command: `KV SET <redacted>`. `None` for anything else.
///
/// Keyword cutting cannot protect these. Their arguments are the
/// application's own keys and values, and a key is as likely as a value to be
/// a secret: `session:<token>`, `reset:<one-time-code>`. What stays still says
/// what ran, which is what an operator reading the log needs. Checked here
/// rather than in the store's module because that module is behind a feature
/// and this must not be.
fn kv_command(sql: &str) -> Option<String> {
    let t = sql.trim_start();
    let b = t.as_bytes();
    if b.len() < 3 || !b[..2].eq_ignore_ascii_case(b"KV") || !b[2].is_ascii_whitespace() {
        return None;
    }
    let rest = t[2..].trim_start();
    let command_end = rest.find(|c: char| c.is_ascii_whitespace() || c == ';').unwrap_or(rest.len());
    let command = &rest[..command_end];
    let has_arguments = !rest[command_end..].trim_matches(|c: char| c.is_ascii_whitespace() || c == ';').is_empty();
    Some(if has_arguments {
        format!("KV {command} <redacted>")
    } else {
        format!("KV {command}")
    })
}

/// `text` kept to its first `max_chars` characters, `…` marking a cut.
fn cut_chars(mut text: String, max_chars: usize) -> String {
    if let Some((cut, _)) = text.char_indices().nth(max_chars) {
        text.truncate(cut);
        text.push('…');
    }
    text
}

/// The longest [`SECRET_BEARING`] keyword, in bytes.
const KEYWORD_MAX_LEN: usize = {
    let (mut i, mut m) = (0, 0);
    while i < SECRET_BEARING.len() {
        if SECRET_BEARING[i].len() > m {
            m = SECRET_BEARING[i].len();
        }
        i += 1;
    }
    m
};

/// The byte index and canonical spelling of the earliest whole-word,
/// case-insensitive occurrence of any [`SECRET_BEARING`] keyword.
///
/// One pass, no allocation. Every keyword consists only of identifier bytes,
/// so a whole-word occurrence — one whose neighbours both fail
/// [`is_ident_byte`] — is exactly a *maximal* run of identifier bytes that
/// equals the keyword. So only the starts of runs are visited — found 64 bytes
/// at a time, and only those beginning with a keyword's first letter — and each
/// such run is compared against the list; the first that is a keyword is the
/// answer.
///
/// This replaced a loop that upper-cased the whole statement once per keyword
/// and searched it, which on a 1 MB bulk `INSERT` was eight 1 MB copies per
/// surface that keeps statement text. The answer is the same by construction —
/// `redaction_matches_the_per_keyword_search` pins it against that loop.
///
/// The underscore counts as an identifier byte on purpose: `password_hash` is a
/// column, not a secret, and `SECRET_ACCESS_KEY` must not be cut short at
/// `SECRET`. See the `prefix-matching` note on why `is_ascii_alphanumeric`
/// alone is the wrong boundary.
fn earliest_secret(text: &[u8]) -> Option<(usize, &'static str)> {
    // Shortest and longest keyword, so most runs are rejected on length alone.
    // Derived from the list so a keyword added to it cannot fall outside them.
    const MIN_LEN: usize = {
        let (mut i, mut m) = (0, usize::MAX);
        while i < SECRET_BEARING.len() {
            if SECRET_BEARING[i].len() < m {
                m = SECRET_BEARING[i].len();
            }
            i += 1;
        }
        m
    };
    const MAX_LEN: usize = {
        let (mut i, mut m) = (0, 0);
        while i < SECRET_BEARING.len() {
            if SECRET_BEARING[i].len() > m {
                m = SECRET_BEARING[i].len();
            }
            i += 1;
        }
        m
    };
    // Whether the identifier run starting at `start` is a keyword. Runs longer
    // than the longest keyword are given up on after `MAX_LEN + 1` bytes.
    let keyword_at = |start: usize| -> Option<&'static str> {
        let limit = text.len().min(start + MAX_LEN + 1);
        let mut end = start + 1;
        while end < limit && IDENT[text[end] as usize] {
            end += 1;
        }
        let run = &text[start..end];
        if !(MIN_LEN..=MAX_LEN).contains(&run.len())
            || (end < text.len() && IDENT[text[end] as usize])
        {
            return None;
        }
        SECRET_BEARING
            .iter()
            .find(|w| w.as_bytes().eq_ignore_ascii_case(run))
            .copied()
    };
    // A run can only be a keyword if it starts with one of their first letters.
    let starts_like_a_keyword =
        |b: u8| SECRET_BEARING.iter().any(|w| w.as_bytes()[0] == (b & !0x20));

    let n = text.len();
    let mut i = 0usize;
    // 64 bytes at a time: a bit mask of identifier bytes, and of run starts
    // whose first letter could begin a keyword. On ordinary statement text the
    // second is almost always empty, and only its bits are looked at.
    //
    // SAFETY: both loads are inside `text` (`i + 64 <= n`); AVX2 is assumed
    // here as in `core::row::common` (target-cpu=x86-64-v3).
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::*;
        #[inline(always)]
        unsafe fn masks(p: *const u8) -> (u32, u32) {
            unsafe {
                let x = _mm256_loadu_si256(p as *const __m256i);
                let in_range = |v: __m256i, lo: u8, hi: u8| {
                    _mm256_and_si256(
                        _mm256_cmpgt_epi8(v, _mm256_set1_epi8(lo as i8 - 1)),
                        _mm256_cmpgt_epi8(_mm256_set1_epi8(hi as i8 + 1), v),
                    )
                };
                // Bytes >= 0x80 are negative as `i8` and fall outside every
                // range, as they fail `is_ident_byte`.
                let folded = _mm256_or_si256(x, _mm256_set1_epi8(0x20));
                let ident = _mm256_or_si256(
                    _mm256_or_si256(in_range(folded, b'a', b'z'), in_range(x, b'0', b'9')),
                    _mm256_cmpeq_epi8(x, _mm256_set1_epi8(b'_' as i8)),
                );
                let mut first = _mm256_setzero_si256();
                for w in SECRET_BEARING {
                    let lower = w.as_bytes()[0] | 0x20;
                    first = _mm256_or_si256(
                        first,
                        _mm256_cmpeq_epi8(folded, _mm256_set1_epi8(lower as i8)),
                    );
                }
                (
                    _mm256_movemask_epi8(ident) as u32,
                    _mm256_movemask_epi8(first) as u32,
                )
            }
        }
        let p = text.as_ptr();
        let mut prev_ident = 0u64;
        while i + 64 <= n {
            let (lo_ident, lo_first) = masks(p.add(i));
            let (hi_ident, hi_first) = masks(p.add(i + 32));
            let ident = lo_ident as u64 | ((hi_ident as u64) << 32);
            let first = lo_first as u64 | ((hi_first as u64) << 32);
            let mut starts = ident & !((ident << 1) | prev_ident) & first;
            while starts != 0 {
                let at = i + starts.trailing_zeros() as usize;
                if let Some(word) = keyword_at(at) {
                    return Some((at, word));
                }
                starts &= starts - 1;
            }
            prev_ident = ident >> 63;
            i += 64;
        }
    }

    // The tail, and every byte on a target without the vector path.
    while i < n {
        let b = text[i];
        if IDENT[b as usize]
            && (i == 0 || !IDENT[text[i - 1] as usize])
            && starts_like_a_keyword(b)
        {
            if let Some(word) = keyword_at(i) {
                return Some((i, word));
            }
        }
        i += 1;
    }
    None
}

/// [`is_ident_byte`] as a table.
static IDENT: [bool; 256] = {
    let mut table = [false; 256];
    let mut b = 0;
    while b < 256 {
        table[b] = is_ident_byte(b as u8);
        b += 1;
    }
    table
};

#[inline]
const fn is_ident_byte(b: u8) -> bool {
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

    /// The implementation this replaced: one upper-cased copy and one search
    /// per keyword, earliest whole-word hit wins. Kept as the reference the
    /// one-pass scan must agree with byte for byte.
    fn reference_statement(sql: &str) -> String {
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

    /// Generated statements built from keywords, near-misses, identifier bytes,
    /// separators and multibyte characters, compared against the old search.
    #[test]
    fn redaction_matches_the_per_keyword_search() {
        let pieces: &[&str] = &[
            "password", "PASSWORD", "PassWord", "password_hash", "xpassword", "passwords",
            "access_key_id", "ACCESS_KEY", "access_key_idx", "secret_access_key",
            "SECRET_KEY", "secret", "Secret", "secretive", "_secret", "session_token",
            "token", "TOKENS", "tok", "token1", "1token", "é", "日本", "😀", "Ω",
            " ", "\n", "\t", "(", ")", ",", "=", "'", "'x'", "_", "a", "Z", "9", "--", ";",
            "SELECT", "FROM", "WITH", "SET", "user", "ßecret",
        ];
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = |bound: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % bound as u64) as usize
        };
        for round in 0..50_000 {
            // Long enough, a quarter of the time, to run the 64-byte blocks.
            let len = next(if round % 4 == 0 { 60 } else { 12 });
            let mut sql = String::new();
            for _ in 0..len {
                sql.push_str(pieces[next(pieces.len())]);
            }
            assert_eq!(statement(&sql), reference_statement(&sql), "input: {sql:?}");
        }
        for sql in ["", "token", "TOKEN", "x token", "token x", "é token", "tokené"] {
            assert_eq!(statement(sql), reference_statement(sql), "input: {sql:?}");
        }
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// redact_bench --nocapture`. A 1 MB bulk `INSERT` with no secret in it,
    /// the shape `query_stats::finish` sees on every load.
    #[test]
    #[ignore]
    fn redact_bench() {
        let mut sql = String::from("INSERT INTO events (id, name, payload) VALUES ");
        let mut i = 0u64;
        while sql.len() < 1 << 20 {
            sql.push_str(&format!("({i}, 'user_{i}', 'some payload text {i}'),"));
            i += 1;
        }
        sql.push_str("(0, 'x', 'y')");
        for round in 0..3 {
            let t = std::time::Instant::now();
            for _ in 0..20 {
                std::hint::black_box(reference_statement(std::hint::black_box(&sql)));
            }
            let old = t.elapsed() / 20;
            let t = std::time::Instant::now();
            for _ in 0..20 {
                std::hint::black_box(statement(std::hint::black_box(&sql)));
            }
            let new = t.elapsed() / 20;
            println!("round {round}: 1 MB INSERT  old {old:?}  new {new:?}");
        }
    }

    /// The old two-step: redact the whole text, then keep `max` characters.
    fn reference_bounded(sql: &str, max: usize) -> String {
        let text = statement(sql);
        if text.chars().count() <= max {
            return text;
        }
        let cut = text.char_indices().nth(max).map(|(i, _)| i).unwrap_or(text.len());
        let mut text = text;
        text.truncate(cut);
        text.push('…');
        text
    }

    #[test]
    fn kv_statements_keep_only_their_command() {
        assert_eq!(statement("KV SET session:abc123 'tok' EX 60"), "KV SET <redacted>");
        assert_eq!(statement("  kv get session:abc123;"), "KV get <redacted>");
        assert_eq!(statement("KV DBSIZE"), "KV DBSIZE");
        assert_eq!(statement("KV INFO;"), "KV INFO");
        assert_eq!(statement_bounded("KV HSET user:1 password hunter2", 5), "KV HS…");
        // Not a KV statement: unchanged rules apply.
        assert_eq!(statement("SELECT kv FROM t"), "SELECT kv FROM t");
        assert_eq!(statement("KVX SET a"), "KVX SET a");
    }

    /// Bounded redaction must equal redact-then-truncate byte for byte, for
    /// every cut position — above all where a keyword, or a near-miss like
    /// `TOKENS`, straddles the cut.
    #[test]
    fn bounded_matches_redact_then_truncate() {
        let pieces: &[&str] = &[
            "password", "PASSWORD", "password_hash", "passwords", "SECRET_ACCESS_KEY",
            "secret", "secretive", "token", "TOKENS", "tok", "é", "日本", "😀", " ", "'x'",
            "(", ",", "=", "_", "a", "9", "SELECT", "hunter2",
        ];
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut next = |bound: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % bound as u64) as usize
        };
        for round in 0..40_000 {
            let len = next(if round % 4 == 0 { 50 } else { 10 });
            let mut sql = String::new();
            for _ in 0..len {
                sql.push_str(pieces[next(pieces.len())]);
            }
            let max = next(sql.chars().count() + 30);
            assert_eq!(
                statement_bounded(&sql, max),
                reference_bounded(&sql, max),
                "input: {sql:?} max {max}"
            );
        }
        // Every cut position through a keyword and its secret.
        let sql = "UPDATE t SET a = 1 WHERE b = 2 PASSWORD 'hunter2' TOKENS x";
        for max in 0..sql.len() + 3 {
            let got = statement_bounded(sql, max);
            assert_eq!(got, reference_bounded(sql, max), "max {max}");
            assert!(!got.contains("hunter"), "max {max}: {got}");
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
