//! `BACKUP`, `RESTORE` and `SHOW BACKUPS`.
//!
//! These are the same operations the CLI flags drive, reachable over the wire
//! so an operator does not need shell access to the database host to take a
//! snapshot or inspect what has been archived.
//!
//! One thing is deliberately *not* reachable from SQL: restoring over the live
//! directory. A statement that swapped the files underneath the server that was
//! executing it would leave every open table reading storage that no longer
//! describes what it thinks it does. `RESTORE ... TO '<dir>'` therefore always
//! writes somewhere new — which is enough for cloning, for verifying a backup,
//! and for recovering onto a fresh host — and restoring in place is the CLI's
//! `--restore-from-s3`, which runs before any table is opened.

use crate::core::backup::manifest::RecoveryTarget;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupStatement {
    /// Full snapshot, followed by an archive flush so it is immediately
    /// restorable.
    Snapshot,
    /// Cut the log and upload everything staged. The point-in-time barrier.
    ArchiveWal,
    /// List what the catalogue holds.
    Show,
    /// Apply the retention policy now.
    Prune,
    /// Take a fresh full snapshot and delete every earlier generation.
    ///
    /// The manual form of the scheduled rollover, and the way to test that the
    /// schedule will do what it says without waiting hours for it.
    Rollover,
    /// Rebuild into a directory that is not the live one.
    Restore {
        target_dir: String,
        target: RecoveryTarget,
    },
}

/// Parses a backup statement, or returns why it could not.
///
/// Returns `Ok(None)` when the text is not a backup statement at all, so the
/// caller can go on trying other grammars.
pub fn parse(sql: &str) -> Result<Option<BackupStatement>, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let words = tokenize(trimmed);
    if words.is_empty() {
        return Ok(None);
    }

    match words[0].to_ascii_uppercase().as_str() {
        "BACKUP" => parse_backup(&words).map(Some),
        "RESTORE" => parse_restore(&words).map(Some),
        "SHOW" if words.len() >= 2 && words[1].eq_ignore_ascii_case("BACKUPS") => {
            Ok(Some(BackupStatement::Show))
        }
        _ => Ok(None),
    }
}

fn parse_backup(words: &[String]) -> Result<BackupStatement, String> {
    let Some(what) = words.get(1) else {
        return Err(
            "BACKUP needs a subject: BACKUP DATABASE, BACKUP WAL, or BACKUP PRUNE".to_string(),
        );
    };

    match what.to_ascii_uppercase().as_str() {
        // `TO S3` is accepted and ignored: there is one configured destination
        // and naming it reads better in a runbook than leaving it implicit.
        "DATABASE" | "SNAPSHOT" => Ok(BackupStatement::Snapshot),
        "WAL" | "LOG" => Ok(BackupStatement::ArchiveWal),
        "PRUNE" => Ok(BackupStatement::Prune),
        "ROLLOVER" => Ok(BackupStatement::Rollover),
        other => Err(format!(
            "BACKUP {other} is not something this engine backs up; \
             expected DATABASE, WAL, PRUNE or ROLLOVER"
        )),
    }
}

fn parse_restore(words: &[String]) -> Result<BackupStatement, String> {
    if !words
        .get(1)
        .is_some_and(|w| w.eq_ignore_ascii_case("DATABASE"))
    {
        return Err("RESTORE only takes DATABASE: RESTORE DATABASE ... TO '<dir>'".to_string());
    }

    let mut target_dir: Option<String> = None;
    let mut target: Option<RecoveryTarget> = None;
    let mut index = 2;

    while index < words.len() {
        let keyword = words[index].to_ascii_uppercase();
        match keyword.as_str() {
            // `FROM S3` names the destination; `FROM SNAPSHOT '<id>'` pins one.
            "FROM" => {
                let next = words.get(index + 1).map(|w| w.to_ascii_uppercase());
                match next.as_deref() {
                    Some("S3") => index += 2,
                    Some("SNAPSHOT") => {
                        let id = unquote(words.get(index + 2).ok_or(
                            "FROM SNAPSHOT needs a snapshot id, as in FROM SNAPSHOT 'snap-...'",
                        )?);
                        set_once(&mut target, RecoveryTarget::Snapshot(id))?;
                        index += 3;
                    }
                    _ => {
                        return Err(
                            "FROM must be followed by S3 or SNAPSHOT '<id>'".to_string()
                        );
                    }
                }
            }
            "TO" => {
                let raw = words
                    .get(index + 1)
                    .ok_or("TO needs a directory, as in TO '/var/lib/rasterized-restored'")?;
                let dir = unquote(raw);
                if dir.is_empty() {
                    return Err("TO needs a non-empty directory path".to_string());
                }
                target_dir = Some(dir);
                index += 2;
            }
            "AT" => {
                let kind = words
                    .get(index + 1)
                    .map(|w| w.to_ascii_uppercase())
                    .ok_or("AT must be followed by LSN or TIME")?;
                let value = words
                    .get(index + 2)
                    .ok_or("AT needs a value, as in AT LSN 918273 or AT TIME '2026-08-20 10:00:00'")?;
                match kind.as_str() {
                    "LSN" => {
                        let lsn: u64 = unquote(value)
                            .parse()
                            .map_err(|_| format!("'{value}' is not a commit sequence"))?;
                        set_once(&mut target, RecoveryTarget::Lsn(lsn))?;
                    }
                    "TIME" | "TIMESTAMP" => {
                        let text = unquote(value);
                        let unix = crate::core::backup::timeutil::parse_rfc3339(&text)
                            .ok_or_else(|| {
                                format!(
                                    "'{text}' is not a timestamp; expected something like \
                                     '2026-08-20T10:00:00Z' or '2026-08-20 10:00:00'"
                                )
                            })?;
                        set_once(&mut target, RecoveryTarget::Time(unix))?;
                    }
                    other => {
                        return Err(format!("AT {other} is not a recovery target; use LSN or TIME"));
                    }
                }
                index += 3;
            }
            "LATEST" => {
                set_once(&mut target, RecoveryTarget::Latest)?;
                index += 1;
            }
            other => {
                return Err(format!("unexpected '{other}' in RESTORE DATABASE"));
            }
        }
    }

    let target_dir = target_dir.ok_or(
        "RESTORE DATABASE needs a destination: RESTORE DATABASE FROM S3 TO '/path/to/new/dir'. \
         Restoring over the live database is the server's --restore-from-s3 flag, because the \
         files cannot be swapped while tables are open.",
    )?;

    Ok(BackupStatement::Restore {
        target_dir,
        target: target.unwrap_or(RecoveryTarget::Latest),
    })
}

/// Rejects a second recovery target rather than letting the last one silently
/// win — a restore that stops somewhere other than where it was told to is not
/// a mistake anyone notices in time.
fn set_once(slot: &mut Option<RecoveryTarget>, value: RecoveryTarget) -> Result<(), String> {
    if slot.is_some() {
        return Err("RESTORE DATABASE takes one recovery target, not several".to_string());
    }
    *slot = Some(value);
    Ok(())
}

/// Splits on whitespace, keeping quoted strings whole.
fn tokenize(sql: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;

    for c in sql.chars() {
        match quote {
            Some(q) => {
                current.push(c);
                if c == q {
                    quote = None;
                }
            }
            None if c == '\'' || c == '"' => {
                current.push(c);
                quote = Some(c);
            }
            None if c.is_whitespace() => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
            }
            None => current.push(c),
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

fn unquote(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches(',');
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0];
        if (first == b'\'' || first == b'"') && bytes[bytes.len() - 1] == first {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(sql: &str) -> BackupStatement {
        parse(sql).unwrap().unwrap()
    }

    #[test]
    fn recognises_the_simple_forms() {
        assert_eq!(ok("BACKUP DATABASE"), BackupStatement::Snapshot);
        assert_eq!(ok("backup database to s3;"), BackupStatement::Snapshot);
        assert_eq!(ok("BACKUP WAL"), BackupStatement::ArchiveWal);
        assert_eq!(ok("BACKUP LOG"), BackupStatement::ArchiveWal);
        assert_eq!(ok("BACKUP PRUNE"), BackupStatement::Prune);
        assert_eq!(ok("SHOW BACKUPS"), BackupStatement::Show);
    }

    #[test]
    fn ignores_unrelated_statements() {
        assert!(parse("SELECT * FROM users").unwrap().is_none());
        assert!(parse("SHOW TABLES").unwrap().is_none());
        assert!(parse("").unwrap().is_none());
    }

    #[test]
    fn restores_to_a_directory() {
        assert_eq!(
            ok("RESTORE DATABASE FROM S3 TO '/var/lib/rdb-restored'"),
            BackupStatement::Restore {
                target_dir: "/var/lib/rdb-restored".to_string(),
                target: RecoveryTarget::Latest,
            }
        );
    }

    #[test]
    fn restores_at_an_lsn() {
        assert_eq!(
            ok("RESTORE DATABASE FROM S3 TO '/tmp/x' AT LSN 918273"),
            BackupStatement::Restore {
                target_dir: "/tmp/x".to_string(),
                target: RecoveryTarget::Lsn(918_273),
            }
        );
    }

    #[test]
    fn restores_at_a_time() {
        let BackupStatement::Restore { target, .. } =
            ok("RESTORE DATABASE TO '/tmp/x' AT TIME '2026-08-20T10:00:00Z'")
        else {
            panic!("expected a restore");
        };
        assert_eq!(
            target,
            RecoveryTarget::Time(
                crate::core::backup::timeutil::parse_rfc3339("2026-08-20T10:00:00Z").unwrap()
            )
        );
    }

    #[test]
    fn restores_a_named_snapshot() {
        assert_eq!(
            ok("RESTORE DATABASE FROM SNAPSHOT 'snap-20260820T100000Z-deadbeef' TO '/tmp/x'"),
            BackupStatement::Restore {
                target_dir: "/tmp/x".to_string(),
                target: RecoveryTarget::Snapshot("snap-20260820T100000Z-deadbeef".to_string()),
            }
        );
    }

    #[test]
    fn a_path_with_spaces_survives_quoting() {
        let BackupStatement::Restore { target_dir, .. } =
            ok("RESTORE DATABASE TO '/var/lib/my database/restore'")
        else {
            panic!("expected a restore");
        };
        assert_eq!(target_dir, "/var/lib/my database/restore");
    }

    #[test]
    fn a_restore_without_a_destination_says_why() {
        let err = parse("RESTORE DATABASE FROM S3").unwrap_err();
        assert!(err.contains("--restore-from-s3"), "{err}");
    }

    #[test]
    fn two_recovery_targets_are_rejected() {
        let err = parse("RESTORE DATABASE TO '/tmp/x' AT LSN 1 AT LSN 2").unwrap_err();
        assert!(err.contains("one recovery target"), "{err}");
    }

    #[test]
    fn a_bad_timestamp_says_what_was_expected() {
        let err = parse("RESTORE DATABASE TO '/tmp/x' AT TIME 'yesterday'").unwrap_err();
        assert!(err.contains("not a timestamp"), "{err}");
    }

    #[test]
    fn a_bad_lsn_is_rejected() {
        let err = parse("RESTORE DATABASE TO '/tmp/x' AT LSN abc").unwrap_err();
        assert!(err.contains("not a commit sequence"), "{err}");
    }

    #[test]
    fn an_unknown_backup_subject_is_rejected() {
        let err = parse("BACKUP TABLE users").unwrap_err();
        assert!(err.contains("expected DATABASE, WAL, PRUNE or ROLLOVER"), "{err}");
    }

    #[test]
    fn a_rollover_is_parsed() {
        assert!(matches!(parse("BACKUP ROLLOVER"), Ok(Some(BackupStatement::Rollover))));
        assert!(matches!(parse("backup rollover"), Ok(Some(BackupStatement::Rollover))));
    }
}
