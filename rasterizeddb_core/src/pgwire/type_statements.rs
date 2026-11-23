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

/// `CREATE DOMAIN`, `DROP DOMAIN`, `ALTER DOMAIN` (`core::rql::domains`).
pub(super) async fn handle_domain_statement<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    parsed: Result<crate::core::rql::domains::DomainStatement, user_types::Refusal>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::core::rql::domains::{self, DomainAlter, DomainStatement};
    let statement = match parsed {
        Ok(statement) => statement,
        Err((code, message)) => return fail_statement(state, stream, code, &message).await,
    };
    let verb = match &statement {
        DomainStatement::Create(_) => "CREATE DOMAIN",
        DomainStatement::Drop { .. } => "DROP DOMAIN",
        DomainStatement::Alter { .. } => "ALTER DOMAIN",
    };
    if read_only(state) {
        return fail_statement(state, stream, "25006", &read_only_message(verb)).await;
    }
    if let Some(t) = state.transaction.as_mut() {
        t.queried = true;
    }
    domains::ensure_loaded(database).await;
    user_types::ensure_loaded(database).await;
    let tx = state.current_tx();
    let exists = |name: &str| {
        let name = name.to_string();
        let database = database.clone();
        async move {
            let names = [name];
            domains::load(&database, Some(&names), tx).await.is_ok_and(|v| !v.is_empty())
                || user_types::load(&database, Some(&names), tx).await.is_ok_and(|v| !v.is_empty())
                || database.has_table(&names[0])
        }
    };
    match statement {
        DomainStatement::Create(domain) => {
            if exists(&domain.name).await {
                return fail_statement(state, stream, "42710", &format!("type \"{}\" already exists", domain.name)).await;
            }
            if let Err(e) = domains::store(database, &domain, tx).await {
                return fail_statement(state, stream, sqlstate_for(&e), &e).await;
            }
            send_command_complete(stream, verb).await
        }
        DomainStatement::Drop { names, if_exists, cascade } => {
            let pending = state.transaction.as_ref().map(|t| database.block_drops_pending(t.id)).unwrap_or_default();
            let columns: Vec<(String, String, String)> = domains::domain_columns(database)
                .into_iter()
                .filter(|(table, _, _)| !pending.iter().any(|p| p.eq_ignore_ascii_case(table)))
                .collect();
            let mut dropping = Vec::new();
            for name in &names {
                let found = domains::load(database, Some(std::slice::from_ref(name)), tx).await.is_ok_and(|v| !v.is_empty());
                if !found {
                    if if_exists {
                        crate::core::notice::push("NOTICE", "00000", format!("type \"{name}\" does not exist, skipping"));
                        continue;
                    }
                    return fail_statement(state, stream, "42704", &format!("type \"{name}\" does not exist")).await;
                }
                if let Some((table, column, _)) = columns.iter().find(|(_, _, d)| d == name) {
                    if cascade {
                        return fail_statement(
                            state,
                            stream,
                            "0A000",
                            &format!("DROP DOMAIN … CASCADE would drop column {column} of table {table}; drop the columns of domain {name} first"),
                        )
                        .await;
                    }
                    return fail_statement(
                        state,
                        stream,
                        "2BP01",
                        &format!(
                            "cannot drop type {name} because other objects depend on it: column {column} of table {table} depends on type {name}"
                        ),
                    )
                    .await;
                }
                dropping.push(name.clone());
            }
            for name in dropping {
                if let Err(e) = domains::remove(database, &name, tx).await {
                    return fail_statement(state, stream, sqlstate_for(&e), &e).await;
                }
            }
            send_command_complete(stream, verb).await
        }
        DomainStatement::Alter { name, action } => {
            if let DomainAlter::RenameTo(to) = &action
                && exists(to).await
            {
                return fail_statement(state, stream, "42710", &format!("type \"{to}\" already exists")).await;
            }
            let own_block = state.transaction.is_none();
            if own_block {
                open_implicit_block(database, state);
            }
            let outcome = alter_domain(database, state, &name, action).await?;
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
                Err((code, message)) => fail_statement(state, stream, code, &message).await,
            }
        }
    }
}

/// `ALTER DOMAIN name action`, on the domain's row and on every column of
/// it (`domains::domain_columns`), through the session's statement path so
/// that one failure takes all of it back (the caller's block).
///
/// A new check or `NOT NULL` is tested against every column's rows before
/// anything changes, with PostgreSQL's errors (23514 `column "c" of table
/// "t" contains values that violate the new constraint`, 23502 `… contains
/// null values`). A column's own `DEFAULT` is kept: only a column with none,
/// or with the domain's, follows `SET`/`DROP DEFAULT`. `DROP NOT NULL`
/// leaves a primary key's column alone, and a column's own `NOT NULL` (its
/// [`domains::OWN_NOT_NULL`] marker).
async fn alter_domain(
    database: &Arc<Database>,
    state: &mut SessionState,
    name: &str,
    action: crate::core::rql::domains::DomainAlter,
) -> io::Result<Result<(), (&'static str, String)>> {
    use crate::core::rql::domains::{self, DomainAlter, CONSTRAINT_PREFIX};
    use crate::core::rql::views::quote_ident;
    let tx = state.current_tx();
    let Some(mut domain) = domains::load(database, Some(&[name.to_string()]), tx).await.ok().and_then(|mut v| v.pop()) else {
        return Ok(Err(("42704", format!("type \"{name}\" does not exist"))));
    };
    let pending = state.transaction.as_ref().map(|t| database.block_drops_pending(t.id)).unwrap_or_default();
    let columns: Vec<(String, String)> = domains::domain_columns(database)
        .into_iter()
        .filter(|(table, _, d)| d == name && !pending.iter().any(|p| p.eq_ignore_ascii_case(table)))
        .map(|(table, column, _)| (table, column))
        .collect();
    let check_name = |column: &str, check: &str| format!("{CONSTRAINT_PREFIX}{}__{column}__{check}", domain.name);
    let marker = |column: &str| format!("{CONSTRAINT_PREFIX}{}__{column}", domain.name);
    let has_constraint = |table: &str, constraint: &str| {
        database
            .schema_view(table)
            .is_some_and(|v| v.schema().checks.iter().any(|c| !c.is_deleted && c.name == constraint))
    };
    let mut statements: Vec<String> = Vec::new();
    let has_check = |d: &domains::Domain, n: &str| d.checks.iter().any(|(c, _)| c == n);
    let constraint_missing = |n: &str| ("42704", format!("constraint \"{n}\" of domain \"{name}\" does not exist"));

    match &action {
        DomainAlter::AddCheck { name: check, expr } => {
            let check = check.clone().unwrap_or_else(|| domain.next_check_name());
            if has_check(&domain, &check) {
                return Ok(Err(("42710", format!("constraint \"{check}\" for domain \"{name}\" already exists"))));
            }
            for (table, column) in &columns {
                let test = domains::value_as(expr, &quote_ident(column));
                let probe = format!("SELECT 1 FROM {} WHERE NOT ({test}) LIMIT 1", quote_ident(table));
                match view_statements::run_internal(database, state, &probe).await? {
                    Ok(tag) if tag != "SELECT 0" => {
                        return Ok(Err((
                            "23514",
                            format!("column \"{column}\" of table \"{table}\" contains values that violate the new constraint"),
                        )));
                    }
                    Ok(_) => {}
                    Err(e) => return Ok(Err((sqlstate_for(&e), e))),
                }
                statements.push(format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} CHECK ({test})",
                    quote_ident(table),
                    quote_ident(&check_name(column, &check))
                ));
                if has_constraint(table, &marker(column)) {
                    statements.push(format!("ALTER TABLE {} DROP CONSTRAINT {}", quote_ident(table), quote_ident(&marker(column))));
                }
            }
            domain.checks.push((check, expr.clone()));
        }
        DomainAlter::DropConstraint { name: check, if_exists } => {
            if !has_check(&domain, check) {
                if *if_exists {
                    crate::core::notice::push(
                        "NOTICE",
                        "00000",
                        format!("constraint \"{check}\" of domain \"{name}\" does not exist, skipping"),
                    );
                    return Ok(Ok(()));
                }
                return Ok(Err(constraint_missing(check)));
            }
            domain.checks.retain(|(c, _)| c != check);
            for (table, column) in &columns {
                // The last check gone, the marker keeps the column known.
                if domain.checks.is_empty() && !has_constraint(table, &marker(column)) {
                    statements.push(format!(
                        "ALTER TABLE {} ADD CONSTRAINT {} CHECK (TRUE)",
                        quote_ident(table),
                        quote_ident(&marker(column))
                    ));
                }
                if has_constraint(table, &check_name(column, check)) {
                    statements.push(format!(
                        "ALTER TABLE {} DROP CONSTRAINT {}",
                        quote_ident(table),
                        quote_ident(&check_name(column, check))
                    ));
                }
            }
        }
        DomainAlter::RenameConstraint { from, to } => {
            // PostgreSQL's own wording here (`for domain d`, unquoted).
            let Some(expr) = domain.checks.iter().find(|(c, _)| c == from).map(|(_, e)| e.clone()) else {
                return Ok(Err(("42704", format!("constraint \"{from}\" for domain {name} does not exist"))));
            };
            if has_check(&domain, to) {
                return Ok(Err(("42710", format!("constraint \"{to}\" for domain {name} already exists"))));
            }
            for (table, column) in &columns {
                statements.push(format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} CHECK ({})",
                    quote_ident(table),
                    quote_ident(&check_name(column, to)),
                    domains::value_as(&expr, &quote_ident(column))
                ));
                if has_constraint(table, &check_name(column, from)) {
                    statements.push(format!(
                        "ALTER TABLE {} DROP CONSTRAINT {}",
                        quote_ident(table),
                        quote_ident(&check_name(column, from))
                    ));
                }
            }
            for (c, _) in domain.checks.iter_mut().filter(|(c, _)| c == from) {
                *c = to.clone();
            }
        }
        DomainAlter::Nothing => return Ok(Ok(())),
        DomainAlter::ValidateConstraint(check) => {
            if !has_check(&domain, check) {
                return Ok(Err(constraint_missing(check)));
            }
            return Ok(Ok(()));
        }
        DomainAlter::SetNotNull => {
            if domain.not_null {
                return Ok(Ok(()));
            }
            for (table, column) in &columns {
                let probe = format!("SELECT 1 FROM {} WHERE {} IS NULL LIMIT 1", quote_ident(table), quote_ident(column));
                match view_statements::run_internal(database, state, &probe).await? {
                    Ok(tag) if tag != "SELECT 0" => {
                        return Ok(Err(("23502", format!("column \"{column}\" of table \"{table}\" contains null values"))));
                    }
                    Ok(_) => {}
                    Err(e) => return Ok(Err((sqlstate_for(&e), e))),
                }
                // Already NOT NULL, the column's own (or its key's): marked,
                // so the domain's DROP NOT NULL leaves it.
                let own = database.schema_view(table).is_some_and(|v| {
                    let schema = v.schema();
                    schema.fields.iter().any(|f| !f.is_deleted && f.name == *column)
                        && crate::core::rql::nullable::flag_field(column, &schema.fields).is_none()
                });
                let marker = domains::own_not_null_marker(&domain.name, column);
                if own && !has_constraint(table, &marker) {
                    statements.push(format!("ALTER TABLE {} ADD CONSTRAINT {} CHECK (TRUE)", quote_ident(table), quote_ident(&marker)));
                }
                statements.push(format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                    quote_ident(table),
                    quote_ident(column)
                ));
            }
            domain.not_null = true;
        }
        DomainAlter::DropNotNull => {
            if !domain.not_null {
                return Ok(Ok(()));
            }
            for (table, column) in &columns {
                let key = database.schema_view(table).is_some_and(|v| {
                    v.schema()
                        .primary_key
                        .as_deref()
                        .is_some_and(|k| k.split(',').any(|c| c.trim().trim_matches('"').eq_ignore_ascii_case(column)))
                });
                if !key && !has_constraint(table, &domains::own_not_null_marker(&domain.name, column)) {
                    statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                        quote_ident(table),
                        quote_ident(column)
                    ));
                }
            }
            domain.not_null = false;
        }
        DomainAlter::SetDefault(_) | DomainAlter::DropDefault => {
            let new = match &action {
                DomainAlter::SetDefault(expr) => Some(expr.clone()),
                _ => None,
            };
            for (table, column) in &columns {
                let current = database.schema_view(table).and_then(|v| {
                    crate::core::rql::column_default::expression_for(column, &v.schema().defaults).map(|d| d.trim().to_string())
                });
                // The column's own default stays.
                let follows = match (&current, &domain.default) {
                    (None, _) => true,
                    (Some(c), Some(old)) => c == old.trim(),
                    (Some(_), None) => false,
                };
                if !follows || current == new {
                    continue;
                }
                statements.push(match &new {
                    Some(expr) => format!("ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {expr}", quote_ident(table), quote_ident(column)),
                    None if current.is_some() => {
                        format!("ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT", quote_ident(table), quote_ident(column))
                    }
                    None => continue,
                });
            }
            domain.default = new;
        }
        DomainAlter::RenameTo(to) => {
            let old = domain.name.clone();
            for (table, column) in &columns {
                let Some(view) = database.schema_view(table) else { continue };
                let prefix = format!("{CONSTRAINT_PREFIX}{old}__{column}");
                let owned: Vec<(String, String)> = view
                    .schema()
                    .checks
                    .iter()
                    .filter(|c| !c.is_deleted && (c.name == prefix || c.name.starts_with(&format!("{prefix}__"))))
                    .map(|c| (c.name.clone(), c.expression.clone()))
                    .collect();
                for (constraint, expression) in owned {
                    let renamed = format!("{CONSTRAINT_PREFIX}{to}__{column}{}", &constraint[prefix.len()..]);
                    statements.push(format!(
                        "ALTER TABLE {} ADD CONSTRAINT {} CHECK ({expression})",
                        quote_ident(table),
                        quote_ident(&renamed)
                    ));
                    statements.push(format!("ALTER TABLE {} DROP CONSTRAINT {}", quote_ident(table), quote_ident(&constraint)));
                }
            }
            domain.name = to.clone();
        }
    }
    for sql in statements {
        if let Err(e) = view_statements::run_internal_ddl(database, state, &sql).await? {
            return Ok(Err((sqlstate_for(&e), e)));
        }
    }
    if let Err(e) = domains::remove(database, name, tx).await {
        return Ok(Err((sqlstate_for(&e), e)));
    }
    if let Err(e) = domains::store(database, &domain, tx).await {
        return Ok(Err((sqlstate_for(&e), e)));
    }
    Ok(Ok(()))
}
