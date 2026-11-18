//! The sequence statements, answered above the engine
//! (`core::rql::sequence_catalog` keeps the rows, `core::rql::sequence` the
//! counters), and what a statement needs before it may call `nextval`.

use super::*;
use crate::core::rql::sequence;
use crate::core::rql::sequence_catalog::{self, SequenceDef, SequenceStatement};

fn read_only(state: &SessionState) -> bool {
    state.transaction.as_ref().map(|t| t.read_only).unwrap_or(state.default_read_only)
}

/// Whether `name` is taken by a relation of any kind.
async fn relation_taken(database: &Arc<Database>, name: &str, tx: Option<TxId>) -> bool {
    database.has_table(name)
        || matches!(crate::core::rql::views::lookup(database, name, tx).await, Ok(Some(_)))
        || matches!(sequence_catalog::load(database, Some(&[name.to_string()]), tx).await, Ok(v) if !v.is_empty())
}

pub(super) async fn handle_sequence_statement<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    parsed: Result<SequenceStatement, sequence_catalog::Refusal>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let statement = match parsed {
        Ok(statement) => statement,
        Err((code, message)) => return fail_statement(state, stream, code, &message).await,
    };
    let verb = match &statement {
        SequenceStatement::Create { .. } => "CREATE SEQUENCE",
        SequenceStatement::Alter { .. } => "ALTER SEQUENCE",
        SequenceStatement::Drop { .. } => "DROP SEQUENCE",
    };
    if read_only(state) {
        return fail_statement(state, stream, "25006", &read_only_message(verb)).await;
    }
    if let Some(t) = state.transaction.as_mut() {
        t.queried = true;
    }
    if let Err(e) = sequence_catalog::ensure_loaded(database).await {
        return fail_statement(state, stream, "XX000", &e).await;
    }
    let tx = state.current_tx();
    match statement {
        SequenceStatement::Create { name, if_not_exists, options } => {
            if relation_taken(database, &name, tx).await
                || sequence_catalog::implicit_sequences(database).iter().any(|(s, _, _)| *s == name)
            {
                if if_not_exists {
                    crate::core::notice::push("NOTICE", "42P07", format!("relation \"{name}\" already exists, skipping"));
                    return send_command_complete(stream, verb).await;
                }
                return fail_statement(state, stream, "42P07", &format!("relation \"{name}\" already exists")).await;
            }
            let def = match sequence_catalog::definition_of(&name, &options) {
                Ok(def) => def,
                Err((code, message)) => return fail_statement(state, stream, code, &message).await,
            };
            if let Err(e) = sequence_catalog::store(database, &def, None, tx).await {
                return fail_statement(state, stream, sqlstate_for(&e), &e).await;
            }
            sequence_catalog::register(database, &def);
            send_command_complete(stream, verb).await
        }
        SequenceStatement::Alter { name, if_exists, options, rename_to } => {
            let found = sequence_catalog::load(database, Some(&[name.clone()]), tx).await.unwrap_or_default();
            let Some(mut def) = found.into_iter().next() else {
                if if_exists {
                    crate::core::notice::push("NOTICE", "00000", format!("relation \"{name}\" does not exist, skipping"));
                    return send_command_complete(stream, verb).await;
                }
                return fail_statement(state, stream, "42P01", &format!("relation \"{name}\" does not exist")).await;
            };
            // The counter as it stands, not as the row last wrote it ahead.
            if let Some(live) = sequence::explicit(database.id, &name) {
                def.last_value = live.last.load(std::sync::atomic::Ordering::SeqCst);
                def.is_called = live.called.load(std::sync::atomic::Ordering::SeqCst);
            }
            if let Some(to) = &rename_to {
                if relation_taken(database, to, tx).await {
                    return fail_statement(state, stream, "42P07", &format!("relation \"{to}\" already exists")).await;
                }
                def.name = to.clone();
            }
            if let Err((code, message)) = sequence_catalog::apply(&mut def, &options, false) {
                return fail_statement(state, stream, code, &message).await;
            }
            if let Err(e) = sequence_catalog::store(database, &def, Some(&name), tx).await {
                return fail_statement(state, stream, sqlstate_for(&e), &e).await;
            }
            sequence::forget_explicit(database.id, &name);
            sequence_catalog::register(database, &def);
            send_command_complete(stream, verb).await
        }
        SequenceStatement::Drop { names, if_exists, cascade } => {
            let found = sequence_catalog::load(database, Some(&names), tx).await.unwrap_or_default();
            let readers = sequence_catalog::implicit_sequences(database);
            for name in &names {
                if !found.iter().any(|d| d.name == *name) {
                    if if_exists {
                        crate::core::notice::push("NOTICE", "00000", format!("sequence \"{name}\" does not exist, skipping"));
                        continue;
                    }
                    if database.has_table(name) {
                        return fail_statement(state, stream, "42809", &format!("\"{name}\" is not a sequence")).await;
                    }
                    return fail_statement(state, stream, "42P01", &format!("sequence \"{name}\" does not exist")).await;
                }
                // A column default reads it: PostgreSQL's dependency.
                let users: Vec<&(String, String, String)> = readers.iter().filter(|(s, _, _)| s == name).collect();
                if let Some((_, table, column)) = users.first() {
                    if !cascade {
                        return fail_statement(
                            state,
                            stream,
                            "2BP01",
                            &format!(
                                "cannot drop sequence {name} because other objects depend on it: default value for \
                                 column {column} of table {table} depends on sequence {name} (use DROP ... CASCADE \
                                 to drop the dependent objects too)"
                            ),
                        )
                        .await;
                    }
                    for (_, table, column) in &users {
                        crate::core::notice::push(
                            "NOTICE",
                            "00000",
                            format!("drop cascades to default value for column {column} of table {table}"),
                        );
                        let sql = format!(
                            "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                            crate::core::rql::views::quote_ident(table),
                            crate::core::rql::views::quote_ident(column)
                        );
                        if let Err(e) = super::view_statements::run_internal(database, state, &sql).await? {
                            return send_error_response(stream, sqlstate_for(&e), &e).await;
                        }
                    }
                }
                if let Err(e) = sequence_catalog::remove(database, name, tx).await {
                    return fail_statement(state, stream, sqlstate_for(&e), &e).await;
                }
                sequence::forget_explicit(database.id, name);
            }
            send_command_complete(stream, verb).await
        }
    }
}

/// Before a statement runs: sequences read into memory, every sequence its
/// calls name checked to exist (42P01, as PostgreSQL's `regclass` input
/// refuses one), and `pg_get_serial_sequence(…)` answered. `Ok(None)` when
/// the statement is unchanged.
pub(super) async fn prepare_statement(
    database: &Arc<Database>,
    tx: Option<TxId>,
    stmt: &str,
) -> Result<Option<String>, (&'static str, String)> {
    if database.has_table(sequence_catalog::SEQUENCE_TABLE) {
        sequence_catalog::ensure_loaded(database).await.map_err(|e| ("XX000", e))?;
    }
    let lower_has = |needle: &[u8]| crate::core::sql_scan::contains_ignore_ascii_case(stmt, needle);
    if !(lower_has(b"val(") || lower_has(b"val (") || lower_has(b"pg_get_serial_sequence")) {
        return Ok(None);
    }
    for name in sequence_catalog::named_in(stmt) {
        if !sequence_catalog::exists(database, &name, tx).await {
            return Err(("42P01", format!("relation \"{name}\" does not exist")));
        }
    }
    // `'s'::regclass`, as pg_dump and ORMs write a sequence argument: the
    // name it reads is the text's (`sequence::sequence_name_of`).
    let without_regclass = strip_regclass_casts(stmt);
    let stmt = without_regclass.as_deref().unwrap_or(stmt);
    if !lower_has(b"pg_get_serial_sequence") {
        return Ok(without_regclass);
    }
    let explicit = sequence_catalog::load(database, None, tx).await.unwrap_or_default();
    Ok(Some(fold_serial_sequence(database, &explicit, stmt)))
}

/// `stmt` with every `::regclass` that follows a string literal taken out;
/// `None` when it has none. Outside literals only.
fn strip_regclass_casts(stmt: &str) -> Option<String> {
    if !crate::core::sql_scan::contains_ignore_ascii_case(stmt, b"::regclass") {
        return None;
    }
    let bytes = stmt.as_bytes();
    let mut out = String::with_capacity(stmt.len());
    let mut i = 0;
    let mut copied = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            let end = crate::core::sql_scan::string_literal_end(bytes, i, false);
            let tail = &stmt[end..];
            let cast = "::regclass";
            if tail.len() >= cast.len()
                && tail.as_bytes()[..cast.len()].eq_ignore_ascii_case(cast.as_bytes())
                && tail.as_bytes().get(cast.len()).is_none_or(|b| !(b.is_ascii_alphanumeric() || *b == b'_'))
            {
                out.push_str(&stmt[copied..end]);
                copied = end + cast.len();
                i = copied;
                continue;
            }
            i = end.max(i + 1);
            continue;
        }
        i += 1;
    }
    out.push_str(&stmt[copied..]);
    Some(out)
}

/// `pg_get_serial_sequence('table', 'column')` as the literal it answers:
/// the sequence the column's default reads (`'public.t_id_seq'`), or one
/// `OWNED BY` it, or NULL.
fn fold_serial_sequence(database: &Database, explicit: &[SequenceDef], stmt: &str) -> String {
    let lower = stmt.to_ascii_lowercase();
    let mut out = String::with_capacity(stmt.len());
    let mut cursor = 0;
    let mut from = 0;
    while let Some(at) = lower[from..].find("pg_get_serial_sequence").map(|a| a + from) {
        from = at + "pg_get_serial_sequence".len();
        let Some(open) = lower[from..].find('(').map(|o| o + from) else {
            break;
        };
        if !lower[from..open].trim().is_empty() {
            continue;
        }
        let Some(close) = crate::core::sql_scan::matching_close(stmt, open) else {
            break;
        };
        let args: Vec<String> = crate::core::sql_scan::split_top_level(&stmt[open + 1..close], b',')
            .into_iter()
            .map(|a| {
                let a = a.trim();
                let a = a.split("::").next().unwrap_or(a).trim();
                a.trim_matches('\'').replace("''", "'")
            })
            .collect();
        let [table, column] = args.as_slice() else {
            continue;
        };
        let table = sequence::sequence_name_of(table);
        let column = sequence::sequence_name_of(column);
        let answer = sequence_catalog::implicit_sequences(database)
            .into_iter()
            .find(|(_, t, c)| t.eq_ignore_ascii_case(&table) && c.eq_ignore_ascii_case(&column))
            .map(|(s, _, _)| s)
            .or_else(|| {
                explicit
                    .iter()
                    .find(|d| d.owned_by.eq_ignore_ascii_case(&format!("{table}.{column}")))
                    .map(|d| d.name.clone())
            });
        out.push_str(&stmt[cursor..at]);
        match answer {
            Some(seq) => out.push_str(&format!("'public.{}'", seq.replace('\'', "''"))),
            None => out.push_str("NULL"),
        }
        cursor = close + 1;
        from = cursor;
    }
    out.push_str(&stmt[cursor..]);
    out
}

/// Table DDL that bears on sequences: a table may not take a sequence's
/// name (42P07), and `DROP TABLE` takes the sequences `OWNED BY` its columns
/// with it. `Ok(true)` when the statement was answered here.
pub(super) async fn guard_table_ddl<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
    ddl: &crate::core::rql::views::TableDdl,
) -> io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::core::rql::views::TableDdl;
    let tx = state.current_tx();
    match ddl {
        TableDdl::Create { name, if_not_exists } => {
            let taken = matches!(sequence_catalog::load(database, Some(&[name.clone()]), tx).await, Ok(v) if !v.is_empty());
            if !taken {
                return Ok(false);
            }
            if *if_not_exists {
                crate::core::notice::push("NOTICE", "42P07", format!("relation \"{name}\" already exists, skipping"));
                send_command_complete(stream, "CREATE TABLE").await?;
            } else {
                fail_statement(state, stream, "42P07", &format!("relation \"{name}\" already exists")).await?;
            }
            Ok(true)
        }
        TableDdl::Drop { names, .. } => {
            let all = sequence_catalog::load(database, None, tx).await.unwrap_or_default();
            if let Some(seq) = all.iter().find(|d| names.contains(&d.name)) {
                fail_statement(
                    state,
                    stream,
                    "42809",
                    &format!("\"{}\" is not a table (use DROP SEQUENCE to remove it)", seq.name),
                )
                .await?;
                return Ok(true);
            }
            let owned: Vec<&SequenceDef> = all
                .iter()
                .filter(|d| {
                    d.owned_by.split('.').next().is_some_and(|t| names.iter().any(|n| n.eq_ignore_ascii_case(t)))
                })
                .collect();
            if owned.is_empty() {
                return Ok(false);
            }
            match super::view_statements::run_internal(database, state, stmt).await? {
                Ok(_) => {}
                Err(e) => {
                    send_error_response(stream, sqlstate_for(&e), &e).await?;
                    return Ok(true);
                }
            }
            for def in owned {
                if let Err(e) = sequence_catalog::remove(database, &def.name, tx).await {
                    fail_statement(state, stream, sqlstate_for(&e), &e).await?;
                    return Ok(true);
                }
                sequence::forget_explicit(database.id, &def.name);
            }
            send_command_complete(stream, "DROP TABLE").await?;
            Ok(true)
        }
        TableDdl::Alter { .. } => Ok(false),
    }
}
