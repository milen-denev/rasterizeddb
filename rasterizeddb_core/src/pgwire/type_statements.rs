//! The enum type statements, answered above the engine
//! (`core::rql::user_types` keeps the rows and rewrites their uses).
//!
//! A change to a type that columns hold (`ADD VALUE`, `RENAME VALUE`,
//! `RENAME TO`) replaces each column's constraint through the session's own
//! statement path (`view_statements::run_internal_ddl`), so a `ROLLBACK`
//! takes it back with the type's row.

use super::*;
use crate::core::rql::user_types::{self, EnumType, TypeStatement};

fn read_only(state: &SessionState) -> bool {
    state.transaction.as_ref().map(|t| t.read_only).unwrap_or(state.default_read_only)
}

/// The constraint of `column` of `table` replaced by `def`'s, its rows
/// first changed by `update` (an `UPDATE … SET` list) when given.
async fn replace_constraint(
    database: &Arc<Database>,
    state: &mut SessionState,
    table: &str,
    column: &str,
    old_type: &str,
    def: &EnumType,
    update: Option<String>,
) -> io::Result<Result<(), String>> {
    use crate::core::rql::views::quote_ident;
    let array = database
        .schema_view(table)
        .and_then(|v| v.schema().fields.iter().find(|f| f.name == column).map(|f| f.db_type == crate::core::db_type::DbType::ARRAY))
        .unwrap_or(false);
    let old = format!("{}{}__{}", user_types::CONSTRAINT_PREFIX, old_type, column);
    let mut statements = vec![format!("ALTER TABLE {} DROP CONSTRAINT {}", quote_ident(table), quote_ident(&old))];
    if let Some(set) = update {
        statements.push(format!("UPDATE {} SET {set}", quote_ident(table)));
    }
    statements.push(format!("ALTER TABLE {} ADD {}", quote_ident(table), user_types::column_constraint(def, column, array)));
    for sql in statements {
        let ddl = !sql.starts_with("UPDATE");
        let ran = if ddl {
            view_statements::run_internal_ddl(database, state, &sql).await?
        } else {
            view_statements::run_internal(database, state, &sql).await?
        };
        if let Err(e) = ran {
            return Ok(Err(e));
        }
    }
    Ok(Ok(()))
}

/// The enum columns, without those of a table this block drops at its end
/// (`DROP TABLE t; DROP TYPE e` in one block or batch).
fn live_enum_columns(database: &Arc<Database>, state: &SessionState) -> Vec<(String, String, String)> {
    let pending = state.transaction.as_ref().map(|t| database.block_drops_pending(t.id)).unwrap_or_default();
    user_types::enum_columns(database)
        .into_iter()
        .filter(|(table, _, _)| !pending.iter().any(|p| p.eq_ignore_ascii_case(table)))
        .collect()
}

pub(super) async fn handle_type_statement<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    parsed: Result<TypeStatement, user_types::Refusal>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let statement = match parsed {
        Ok(statement) => statement,
        Err((code, message)) => return fail_statement(state, stream, code, &message).await,
    };
    let verb = match &statement {
        TypeStatement::Create { .. } => "CREATE TYPE",
        TypeStatement::Drop { .. } => "DROP TYPE",
        _ => "ALTER TYPE",
    };
    if read_only(state) {
        return fail_statement(state, stream, "25006", &read_only_message(verb)).await;
    }
    if let Some(t) = state.transaction.as_mut() {
        t.queried = true;
    }
    user_types::ensure_loaded(database).await;
    let tx = state.current_tx();
    let find = |name: &str| {
        let name = name.to_string();
        let database = database.clone();
        async move { user_types::load(&database, Some(&[name]), tx).await.ok().and_then(|v| v.into_iter().next()) }
    };
    match statement {
        TypeStatement::Create { name, labels } => {
            if find(&name).await.is_some() || database.has_table(&name) {
                return fail_statement(state, stream, "42710", &format!("type \"{name}\" already exists")).await;
            }
            if let Err(e) = user_types::store(database, &EnumType { name, labels }, None, tx).await {
                return fail_statement(state, stream, sqlstate_for(&e), &e).await;
            }
            send_command_complete(stream, verb).await
        }
        TypeStatement::Drop { names, if_exists, cascade } => {
            let columns = live_enum_columns(database, state);
            let mut dropping = Vec::new();
            for name in &names {
                if find(name).await.is_none() {
                    if if_exists {
                        crate::core::notice::push("NOTICE", "00000", format!("type \"{name}\" does not exist, skipping"));
                        continue;
                    }
                    return fail_statement(state, stream, "42704", &format!("type \"{name}\" does not exist")).await;
                }
                if let Some((table, column, _)) = columns.iter().find(|(_, _, t)| t == name) {
                    if cascade {
                        return fail_statement(
                            state,
                            stream,
                            "0A000",
                            &format!(
                                "DROP TYPE … CASCADE would drop column {column} of table {table}; drop the columns of type {name} first"
                            ),
                        )
                        .await;
                    }
                    return fail_statement(
                        state,
                        stream,
                        "2BP01",
                        &format!(
                            "cannot drop type {name} because other objects depend on it: column {column} of table \
                             {table} depends on type {name}"
                        ),
                    )
                    .await;
                }
                dropping.push(name.clone());
            }
            for name in dropping {
                if let Err(e) = user_types::remove(database, &name, tx).await {
                    return fail_statement(state, stream, sqlstate_for(&e), &e).await;
                }
            }
            send_command_complete(stream, verb).await
        }
        TypeStatement::AddValue { name, label, if_not_exists, place } => {
            let Some(def) = find(&name).await else {
                return fail_statement(state, stream, "42704", &format!("type \"{name}\" does not exist")).await;
            };
            if def.labels.contains(&label) {
                if if_not_exists {
                    crate::core::notice::push("NOTICE", "42710", format!("enum label \"{label}\" already exists, skipping"));
                    return send_command_complete(stream, verb).await;
                }
                return fail_statement(state, stream, "42710", &format!("enum label \"{label}\" already exists")).await;
            }
            let labels = match user_types::place_label(&def.labels, &label, &place) {
                Ok(labels) => labels,
                Err((code, message)) => return fail_statement(state, stream, code, &message).await,
            };
            let new = EnumType { name: name.clone(), labels };
            change(database, state, stream, verb, &name, &new, None).await
        }
        TypeStatement::RenameValue { name, from, to } => {
            let Some(def) = find(&name).await else {
                return fail_statement(state, stream, "42704", &format!("type \"{name}\" does not exist")).await;
            };
            let Some(at) = def.labels.iter().position(|l| *l == from) else {
                return fail_statement(state, stream, "22023", &format!("\"{from}\" is not an existing enum label")).await;
            };
            if def.labels.contains(&to) {
                return fail_statement(state, stream, "42710", &format!("enum label \"{to}\" already exists")).await;
            }
            let mut labels = def.labels.clone();
            labels[at] = to.clone();
            let new = EnumType { name: name.clone(), labels };
            change(database, state, stream, verb, &name, &new, Some((from, to))).await
        }
        TypeStatement::Rename { name, to } => {
            let Some(def) = find(&name).await else {
                return fail_statement(state, stream, "42704", &format!("type \"{name}\" does not exist")).await;
            };
            if find(&to).await.is_some() || database.has_table(&to) {
                return fail_statement(state, stream, "42710", &format!("type \"{to}\" already exists")).await;
            }
            let new = EnumType { name: to, labels: def.labels };
            change(database, state, stream, verb, &name, &new, None).await
        }
    }
}

/// Stores `new` in place of the type `old_name` and replaces the
/// constraint of every column holding it; `rename` changes the rows'
/// label first. All of it or none: outside a block it runs in one of its
/// own, as PostgreSQL's `ALTER TYPE` is one statement.
async fn change<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    verb: &str,
    old_name: &str,
    new: &EnumType,
    rename: Option<(String, String)>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let own_block = state.transaction.is_none();
    if own_block {
        open_implicit_block(database, state);
    }
    let outcome = apply_change(database, state, old_name, new, rename).await?;
    if own_block && let Some(t) = state.transaction.take() {
        state.end_block_settings(&t);
        if outcome.is_err() || t.failed {
            database.block_ddl_abort(t.id).await;
            t.abort_unless_aborted(database);
            t.owner.release();
        } else if let Err((code, message)) = commit_block(database, state, t).await {
            return fail_statement(state, stream, code, &message).await;
        }
    }
    match outcome {
        Ok(()) => send_command_complete(stream, verb).await,
        Err(e) => fail_statement(state, stream, sqlstate_for(&e), &e).await,
    }
}

async fn apply_change(
    database: &Arc<Database>,
    state: &mut SessionState,
    old_name: &str,
    new: &EnumType,
    rename: Option<(String, String)>,
) -> io::Result<Result<(), String>> {
    let tx = state.current_tx();
    if let Err(e) = user_types::store(database, new, Some(old_name), tx).await {
        return Ok(Err(e));
    }
    let columns: Vec<(String, String)> = live_enum_columns(database, state)
        .into_iter()
        .filter(|(_, _, t)| t == old_name)
        .map(|(table, column, _)| (table, column))
        .collect();
    for (table, column) in columns {
        let update = rename.as_ref().map(|(from, to)| {
            use crate::auth::quote;
            let col = crate::core::rql::views::quote_ident(&column);
            let array = database
                .schema_view(&table)
                .and_then(|v| v.schema().fields.iter().find(|f| f.name == column).map(|f| f.db_type == crate::core::db_type::DbType::ARRAY))
                .unwrap_or(false);
            if array {
                format!("{col} = array_replace({col}, {}, {}) WHERE {} = ANY({col})", quote(from), quote(to), quote(from))
            } else {
                format!("{col} = {} WHERE {col} = {}", quote(to), quote(from))
            }
        });
        if let Err(e) = replace_constraint(database, state, &table, &column, old_name, new, update).await? {
            return Ok(Err(e));
        }
    }
    Ok(Ok(()))
}
