//! One snapshot for every read a read-only statement makes.
//!
//! A statement is answered in pieces here: an `IN (SELECT …)`, `EXISTS` or
//! scalar subquery runs first and is substituted as literals, a CTE is
//! materialised into a temporary relation, and only then does the outer query
//! read. Each piece used to take a snapshot of its own, so under READ
//! COMMITTED a transaction committing between two pieces showed in one and
//! not the other: a row moved from `b` to `a` appeared in both, and the
//! statement reported a state the database was never in. PostgreSQL reads a
//! whole statement under one snapshot.
//!
//! Inside [`scope`], the first versioned read takes the snapshot and every
//! later read reuses it, whichever transaction id it reads under. They differ
//! within one statement: a subquery runs under the session's id (`FROZEN_TX`
//! outside a block) and the outer query under an autocommit id of its own,
//! so a snapshot kept per id was never shared. A read-only statement writes
//! nothing those ids would need to see as their own except a CTE's temporary
//! relation, which is written under the transaction whose body read came
//! first, so that read's snapshot sees it, however late it was written.
//!
//! Only read-only statements are scoped ([`is_read_only`]). A write takes its
//! snapshot under the table's write lock, and its conflict retries must read
//! afresh each time: a retry under the first attempt's snapshot would find
//! the same superseded versions and conflict again.

use crate::core::mvcc::Visibility;

tokio::task_local! {
    static READ: parking_lot::Mutex<Option<Visibility>>;
}

/// Runs `f` with one snapshot for all its reads.
pub async fn scope<F: std::future::Future>(f: F) -> F::Output {
    READ.scope(parking_lot::Mutex::new(None), f).await
}

/// The statement's snapshot, taken with `take` on the first read. Outside
/// [`scope`], `take`'s, every time.
///
/// Only a snapshot is kept: `Visibility::All` is a table with no versions,
/// and whether the next table has them is its own question. Every versioned
/// table shares one transaction manager, so any table's snapshot serves all.
pub fn get_or_take(take: impl FnOnce() -> Visibility) -> Visibility {
    let kept = READ.try_with(|read| read.lock().clone());
    match kept {
        Ok(Some(visibility)) => visibility,
        Ok(None) => {
            let visibility = take();
            if matches!(visibility, Visibility::AsOf { .. }) {
                let _ = READ.try_with(|read| *read.lock() = Some(visibility.clone()));
            }
            visibility
        }
        Err(_) => take(),
    }
}

/// Whether the caller runs inside [`scope`]. For the tests that pin which
/// paths read a statement under one snapshot.
#[cfg(test)]
pub(crate) fn in_scope() -> bool {
    READ.try_with(|_| ()).is_ok()
}

/// Whether `sql` only reads, so its pieces may share one snapshot.
///
/// Conservative: a statement opening with `SELECT`, `VALUES`, `TABLE` or
/// `WITH`, with no `INSERT`, `UPDATE`, `DELETE`, `MERGE` or `INTO` and no
/// locking clause (`FOR UPDATE`, `FOR SHARE`, `FOR NO KEY UPDATE`, `FOR KEY
/// SHARE`) anywhere outside a quoted string or identifier. A column named
/// `update` costs only the sharing, never correctness.
pub fn is_read_only(sql: &str) -> bool {
    let words = words_outside_quotes(sql);
    let Some(first) = words.first() else {
        return false;
    };
    if !["SELECT", "VALUES", "TABLE", "WITH"].contains(&first.as_str()) {
        return false;
    }
    let writes = ["INSERT", "UPDATE", "DELETE", "MERGE", "INTO"];
    if words.iter().any(|w| writes.contains(&w.as_str())) {
        return false;
    }
    !words
        .windows(2)
        .any(|w| w[0] == "FOR" && ["SHARE", "NO", "KEY"].contains(&w[1].as_str()))
}

/// The bare words of `sql`, upper-cased, skipping quoted strings and
/// identifiers.
fn words_outside_quotes(sql: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote: Option<char> = None;
    for c in sql.chars() {
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            continue;
        }
        if c.is_alphanumeric() || c == '_' {
            word.push(c.to_ascii_uppercase());
            continue;
        }
        if !word.is_empty() {
            words.push(std::mem::take(&mut word));
        }
        if c == '\'' || c == '"' {
            quote = Some(c);
        }
    }
    if !word.is_empty() {
        words.push(word);
    }
    words
}

#[cfg(test)]
mod tests {
    use super::is_read_only;

    #[test]
    fn reads_are_read_only() {
        for sql in [
            "SELECT id FROM a WHERE id IN (SELECT id FROM b)",
            "select 1",
            "WITH m AS (SELECT id FROM b) SELECT * FROM a JOIN m ON m.id = a.id",
            "VALUES (1), (2)",
            "SELECT 'insert into t' AS note, \"update\" FROM t",
            "SELECT * FROM t FOR_X",
        ] {
            assert!(is_read_only(sql), "{sql}");
        }
    }

    #[test]
    fn writes_and_locking_reads_are_not() {
        for sql in [
            "UPDATE t SET a = 1",
            "WITH d AS (DELETE FROM t RETURNING id) SELECT * FROM d",
            "WITH i AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM i",
            "SELECT * INTO copy FROM t",
            "SELECT * FROM t WHERE id = 1 FOR UPDATE",
            "SELECT * FROM t FOR NO KEY UPDATE",
            "SELECT * FROM t FOR SHARE SKIP LOCKED",
            "SELECT * FROM t FOR KEY SHARE",
            "CREATE TABLE t (a INT)",
            "",
        ] {
            assert!(!is_read_only(sql), "{sql}");
        }
    }
}
