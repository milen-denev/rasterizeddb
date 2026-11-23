//! `SELECT … FOR UPDATE`, and the locks it takes.
//!
//! A locking clause is a promise the engine has to keep or refuse. Accepting one
//! and doing nothing is the worst of the three outcomes: the classic claim
//! query
//!
//! ```sql
//! WITH claimed AS (
//!     SELECT id FROM job WHERE status = 'pending' LIMIT 10 FOR UPDATE SKIP LOCKED
//! )
//! UPDATE job SET status = 'running' FROM claimed WHERE job.id = claimed.id
//! ```
//!
//! is written precisely so that two workers claim *different* rows. Without
//! locking every worker claims the same ten, and each reports success.

/// Which lock a `FOR …` clause asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockStrength {
    /// `FOR UPDATE` — exclusive; nobody else may lock or write the row.
    Update,
    /// `FOR SHARE` — several readers may hold it, writers may not.
    Share,
}

/// What to do when a row is already locked by someone else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockWait {
    /// Wait for the holder to finish. The default.
    Wait,
    /// Fail immediately rather than wait.
    NoWait,
    /// Leave the row out of the result.
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowLock {
    pub strength: LockStrength,
    pub behaviour: LockWait,
    /// The clause as PostgreSQL names it (`FOR NO KEY UPDATE`), for the
    /// refusals that quote it: the four spellings share two strengths here.
    pub clause: &'static str,
}

impl RowLock {
    /// PostgreSQL's refusal of this clause on a statement whose rows are not
    /// table rows (0A000): "FOR UPDATE is not allowed with DISTINCT clause".
    pub fn not_allowed_with(&self, what: &str) -> String {
        format!("{} is not allowed with {}", self.clause, what)
    }
}

/// What PostgreSQL's refusal names a set operation as.
pub const SET_OPERATIONS: &str = "UNION/INTERSECT/EXCEPT";

/// Splits a trailing locking clause off a `SELECT`.
///
/// Returns the statement without it, and the clause when there was one.
/// `FOR UPDATE OF <table>` is accepted and the table list ignored: it narrows
/// *which* relations of a join are locked, and locking more than asked for is
/// safe where locking fewer would not be.
pub fn split_locking_clause(sql: &str) -> Result<(String, Option<RowLock>), String> {
    let trimmed = sql.trim_end().trim_end_matches(';').trim_end();
    let Some(at) = crate::core::rql::join::find_keyword(trimmed, "FOR", 0) else {
        return Ok((sql.to_string(), None));
    };

    let tail = trimmed[at + "FOR".len()..].trim_start();
    let upper = tail.to_ascii_uppercase();

    let (strength, clause, rest) = if upper.starts_with("UPDATE") {
        (LockStrength::Update, "FOR UPDATE", &tail["UPDATE".len()..])
    } else if upper.starts_with("NO KEY UPDATE") {
        // Weaker than `FOR UPDATE` in PostgreSQL, but the difference only shows
        // against foreign-key locks, which this engine does not take. Treated as
        // the stronger one, which can refuse where PostgreSQL would allow but
        // never allows where PostgreSQL would refuse.
        (LockStrength::Update, "FOR NO KEY UPDATE", &tail["NO KEY UPDATE".len()..])
    } else if upper.starts_with("SHARE") {
        (LockStrength::Share, "FOR SHARE", &tail["SHARE".len()..])
    } else if upper.starts_with("KEY SHARE") {
        (LockStrength::Share, "FOR KEY SHARE", &tail["KEY SHARE".len()..])
    } else {
        // A `FOR` that begins no locking clause is left alone rather than
        // guessed at.
        return Ok((sql.to_string(), None));
    };

    let mut rest = rest.trim_start();

    // `OF a, b` names the relations to lock. Consumed as a comma-separated list
    // of identifiers and no further: scanning ahead for `NOWAIT`/`SKIP` instead
    // swallowed whatever sat between, so `FOR UPDATE OF t WAIT 5` was accepted
    // as a plain `FOR UPDATE` and the `WAIT 5` vanished.
    if rest.to_ascii_uppercase().starts_with("OF ") {
        let mut after = rest[3..].trim_start();
        loop {
            let end = after
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '"' || c == '.'))
                .unwrap_or(after.len());
            if end == 0 {
                return Err(format!(
                    "'{}' names no relation after OF",
                    trimmed[at..].trim()
                ));
            }
            after = after[end..].trim_start();
            match after.strip_prefix(',') {
                Some(next) => after = next.trim_start(),
                None => break,
            }
        }
        rest = after;
    }

    let upper_rest = rest.to_ascii_uppercase();
    let behaviour = if upper_rest.starts_with("NOWAIT") {
        rest = rest["NOWAIT".len()..].trim_start();
        LockWait::NoWait
    } else if upper_rest.starts_with("SKIP LOCKED") {
        rest = rest["SKIP LOCKED".len()..].trim_start();
        LockWait::Skip
    } else {
        LockWait::Wait
    };

    // PostgreSQL's grammar lets the locking clause and `LIMIT`/`OFFSET`/`FETCH`
    // appear in either order, and ORMs emit both. Whatever follows the lock is
    // therefore reattached to the statement rather than refused — refusing it
    // rejected the standard claim query
    // `… ORDER BY … FOR UPDATE SKIP LOCKED LIMIT 1` outright.
    let tail_upper = rest.to_ascii_uppercase();
    let tail_is_pagination = ["LIMIT", "OFFSET", "FETCH"]
        .iter()
        .any(|k| tail_upper.starts_with(k));
    if !rest.is_empty() && !tail_is_pagination {
        return Err(format!(
            "'{}' is not a supported locking clause",
            trimmed[at..].trim()
        ));
    }

    let head = trimmed[..at].trim_end();
    let lock = RowLock { strength, behaviour, clause };
    // A set operation's rows are not table rows; PostgreSQL refuses the lock.
    if ["UNION", "INTERSECT", "EXCEPT"]
        .iter()
        .any(|k| crate::core::rql::join::find_keyword(head, k, 0).is_some())
    {
        return Err(lock.not_allowed_with(SET_OPERATIONS));
    }
    let statement = if rest.is_empty() {
        head.to_string()
    } else {
        format!("{} {}", head, rest)
    };

    Ok((statement, Some(lock)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lock(sql: &str) -> (String, Option<RowLock>) {
        split_locking_clause(sql).expect("parses")
    }

    #[test]
    fn every_spelling_is_recognised() {
        let (rest, l) = lock("SELECT id FROM t FOR UPDATE");
        assert_eq!(rest, "SELECT id FROM t");
        assert_eq!(l.unwrap().strength, LockStrength::Update);
        assert_eq!(l.unwrap().behaviour, LockWait::Wait);

        let (_, l) = lock("SELECT id FROM t FOR UPDATE SKIP LOCKED");
        assert_eq!(l.unwrap().behaviour, LockWait::Skip);

        let (_, l) = lock("SELECT id FROM t FOR UPDATE NOWAIT");
        assert_eq!(l.unwrap().behaviour, LockWait::NoWait);

        let (_, l) = lock("SELECT id FROM t FOR SHARE");
        assert_eq!(l.unwrap().strength, LockStrength::Share);

        // `OF <table>` narrows which relations are locked; the list is ignored
        // and everything matched is locked, which errs toward locking more.
        let (rest, l) = lock("SELECT id FROM t JOIN u ON t.id = u.id FOR UPDATE OF t SKIP LOCKED");
        assert!(rest.ends_with("u.id"), "clause not stripped: {}", rest);
        assert_eq!(l.unwrap().behaviour, LockWait::Skip);
    }

    #[test]
    fn a_statement_without_a_locking_clause_is_untouched() {
        let sql = "SELECT id FROM t WHERE a = 1";
        let (rest, l) = lock(sql);
        assert_eq!(rest, sql);
        assert!(l.is_none());
    }

    #[test]
    fn the_clause_is_stripped_after_a_limit() {
        // `LIMIT 1 FOR UPDATE` previously reported "'FOR' cannot follow a
        // LIMIT/OFFSET/FETCH clause", because the tail was still attached when
        // pagination parsed it.
        let (rest, l) = lock("SELECT id FROM t ORDER BY a LIMIT 1 FOR UPDATE SKIP LOCKED");
        assert_eq!(rest, "SELECT id FROM t ORDER BY a LIMIT 1");
        assert_eq!(l.unwrap().behaviour, LockWait::Skip);
    }

    #[test]
    fn a_limit_after_the_locking_clause_is_kept() {
        // PostgreSQL accepts either order, and this is the order sea-orm emits
        // for a claim query. Refusing it rejected the whole statement with
        // "'FOR UPDATE SKIP LOCKED LIMIT 1' is not a supported locking clause".
        let (rest, l) = lock(
            "SELECT c.id FROM job AS c WHERE c.status = 'pending' \
             ORDER BY c.next_attempt_at ASC FOR UPDATE SKIP LOCKED LIMIT 1",
        );
        assert!(
            rest.ends_with("LIMIT 1"),
            "the LIMIT must survive: {}",
            rest
        );
        assert!(
            !rest.to_ascii_uppercase().contains("FOR UPDATE"),
            "{}",
            rest
        );
        assert_eq!(l.unwrap().behaviour, LockWait::Skip);

        let (rest, _) = lock("SELECT id FROM t FOR UPDATE OFFSET 5");
        assert!(rest.ends_with("OFFSET 5"), "{}", rest);
    }

    #[test]
    fn an_unrecognised_locking_clause_is_refused_not_ignored() {
        // Silently dropping one would leave the caller believing rows were
        // locked when nothing was.
        assert!(split_locking_clause("SELECT id FROM t FOR UPDATE OF t WAIT 5").is_err());
    }
}

/// A parsed `LOCK TABLE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockTable {
    pub tables: Vec<String>,
    /// The mode as PostgreSQL names it; [`PgLockMode::table_mode`] is the one
    /// of the four this engine takes (never weaker).
    ///
    /// [`PgLockMode::table_mode`]: crate::core::mvcc::table_locks::PgLockMode::table_mode
    pub mode: crate::core::mvcc::table_locks::PgLockMode,
    /// `NOWAIT` — fail instead of waiting for the holder.
    pub nowait: bool,
}

/// Parses `LOCK [TABLE] a [, b] [IN <mode> MODE] [NOWAIT]`.
///
/// Returns `None` when the statement is not a `LOCK`.
pub fn parse_lock_table(sql: &str) -> Option<Result<LockTable, String>> {
    use crate::core::mvcc::table_locks::PgLockMode;

    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("LOCK") {
        return None;
    }
    let mut rest = trimmed["LOCK".len()..].trim_start();
    if rest.to_ascii_uppercase().starts_with("TABLE") {
        rest = rest["TABLE".len()..].trim_start();
    }

    // The relation list runs to `IN`, `NOWAIT`, or the end.
    let upper_rest = rest.to_ascii_uppercase();
    let list_end = crate::core::rql::join::find_keyword(&upper_rest, "IN", 0)
        .into_iter()
        .chain(crate::core::rql::join::find_keyword(
            &upper_rest,
            "NOWAIT",
            0,
        ))
        .min()
        .unwrap_or(rest.len());

    let tables: Vec<String> = rest[..list_end]
        .split(',')
        .map(|name| {
            name.trim()
                .trim_matches('"')
                .rsplit('.')
                .next()
                .unwrap_or("")
                .trim_matches('"')
                .to_string()
        })
        .filter(|name| !name.is_empty())
        .collect();
    if tables.is_empty() {
        return Some(Err("LOCK TABLE names no relation".to_string()));
    }

    let mut tail = rest[list_end..].trim_start();
    let mut mode = PgLockMode::AccessExclusive; // PostgreSQL's default.

    if tail.to_ascii_uppercase().starts_with("IN ") {
        let after = tail[3..].trim_start();
        let upper_after = after.to_ascii_uppercase();
        let Some(mode_end) = crate::core::rql::join::find_keyword(&upper_after, "MODE", 0) else {
            return Some(Err(
                "LOCK TABLE ... IN <mode> needs the word MODE, as in `IN SHARE MODE`".to_string(),
            ));
        };
        let named = upper_after[..mode_end]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        mode = match named.as_str() {
            "ACCESS SHARE" => PgLockMode::AccessShare,
            "ROW SHARE" => PgLockMode::RowShare,
            "ROW EXCLUSIVE" => PgLockMode::RowExclusive,
            "SHARE UPDATE EXCLUSIVE" => PgLockMode::ShareUpdateExclusive,
            "SHARE" => PgLockMode::Share,
            "SHARE ROW EXCLUSIVE" => PgLockMode::ShareRowExclusive,
            "EXCLUSIVE" => PgLockMode::Exclusive,
            "ACCESS EXCLUSIVE" => PgLockMode::AccessExclusive,
            other => {
                return Some(Err(format!("'{}' is not a lock mode", other)));
            }
        };
        tail = after[mode_end + "MODE".len()..].trim_start();
    }

    let nowait = if tail.to_ascii_uppercase().starts_with("NOWAIT") {
        tail = tail["NOWAIT".len()..].trim_start();
        true
    } else {
        false
    };

    if !tail.is_empty() {
        return Some(Err(format!("'{}' is not part of a LOCK statement", tail)));
    }

    Some(Ok(LockTable {
        tables,
        mode,
        nowait,
    }))
}

#[cfg(test)]
mod lock_table_tests {
    use super::*;
    use crate::core::mvcc::table_locks::PgLockMode;

    fn parsed(sql: &str) -> LockTable {
        parse_lock_table(sql).expect("is a LOCK").expect("parses")
    }

    #[test]
    fn the_default_mode_is_access_exclusive() {
        // PostgreSQL's default, and the strongest — so an unqualified LOCK
        // never grants less than the caller expects.
        let l = parsed("LOCK TABLE t");
        assert_eq!(l.tables, vec!["t".to_string()]);
        assert_eq!(l.mode, PgLockMode::AccessExclusive);
        assert!(!l.nowait);
    }

    #[test]
    fn every_accepted_spelling_parses() {
        assert_eq!(parsed("LOCK t").tables, vec!["t".to_string()]);
        assert_eq!(
            parsed("LOCK TABLE a, b").tables,
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            parsed("LOCK TABLE t IN SHARE MODE").mode,
            PgLockMode::Share
        );
        assert_eq!(
            parsed("LOCK TABLE t IN ACCESS SHARE MODE").mode,
            PgLockMode::AccessShare
        );
        assert_eq!(
            parsed("LOCK TABLE t IN ACCESS EXCLUSIVE MODE").mode,
            PgLockMode::AccessExclusive
        );
        assert!(parsed("LOCK TABLE t IN EXCLUSIVE MODE NOWAIT").nowait);
        // Quoted and schema-qualified names are what an ORM emits.
        assert_eq!(
            parsed("LOCK TABLE \"public\".\"t\"").tables,
            vec!["t".to_string()]
        );
    }

    #[test]
    fn a_statement_that_is_not_a_lock_is_not_claimed() {
        assert!(parse_lock_table("SELECT 1").is_none());
    }

    #[test]
    fn an_unknown_mode_is_refused_rather_than_defaulted() {
        // Defaulting would hand back a weaker lock than asked for while
        // reporting success.
        assert!(
            parse_lock_table("LOCK TABLE t IN WEIRD MODE")
                .unwrap()
                .is_err()
        );
        assert!(parse_lock_table("LOCK TABLE t IN SHARE").unwrap().is_err());
        assert!(parse_lock_table("LOCK TABLE").unwrap().is_err());
    }
}
