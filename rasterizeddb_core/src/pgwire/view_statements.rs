//! The view statements and the view guards on table DDL, answered above the
//! engine (`core::rql::views` holds the catalogue and the expansion).

use super::*;
use crate::core::rql::views::{self, TableDdl, ViewDef, ViewKind, ViewStatement};

/// The last `CommandComplete` tag in a run's output, or "" when there is none.
fn last_command_tag(bytes: &[u8]) -> String {
    let mut at = 0;
    let mut tag = String::new();
    while at + 5 <= bytes.len() {
        let kind = bytes[at];
        let len = u32::from_be_bytes([bytes[at + 1], bytes[at + 2], bytes[at + 3], bytes[at + 4]]) as usize;
        let end = (at + 1 + len).min(bytes.len());
        if kind == b'C' && end > at + 5 {
            tag = String::from_utf8_lossy(&bytes[at + 5..end]).trim_end_matches('\0').to_string();
        }
        at = end.max(at + 5);
    }
    tag
}

/// Runs `sql` through this session's own statement path, for a view
/// statement: its output goes nowhere, and a failure comes back as its
/// message, already recorded on the session (a block already failed by it)
/// as any statement's is. The view guards let it through.
pub(super) async fn run_internal(database: &Arc<Database>, state: &mut SessionState, sql: &str) -> io::Result<Result<String, String>> {
    let mut sink = std::io::Cursor::new(Vec::<u8>::new());
    let was_internal = std::mem::replace(&mut state.view_internal, true);
    let ran = internal_run(Box::pin(execute_single_statement(database, state, &mut sink, sql, &[], true))).await;
    state.view_internal = was_internal;
    ran?;
    if state.statement_failed {
        return Ok(Err(state.statement_error.clone().unwrap_or_else(|| "statement failed".to_string())));
    }
    Ok(Ok(last_command_tag(sink.get_ref())))
}

/// [`run_internal`] for a query: the column names its `RowDescription` gave.
async fn run_for_view_described(
    database: &Arc<Database>,
    state: &mut SessionState,
    sql: &str,
) -> io::Result<Result<Vec<String>, String>> {
    let mut sink = std::io::Cursor::new(Vec::<u8>::new());
    let was_internal = std::mem::replace(&mut state.view_internal, true);
    let ran = internal_run(Box::pin(execute_single_statement(database, state, &mut sink, sql, &[], false))).await;
    state.view_internal = was_internal;
    ran?;
    if state.statement_failed {
        return Ok(Err(state.statement_error.clone().unwrap_or_else(|| "statement failed".to_string())));
    }
    Ok(Ok(row_description_names(sink.get_ref())))
}

/// The column names of the first `RowDescription` in a run's output.
fn row_description_names(bytes: &[u8]) -> Vec<String> {
    let mut at = 0;
    while at + 5 <= bytes.len() {
        let kind = bytes[at];
        let len = u32::from_be_bytes([bytes[at + 1], bytes[at + 2], bytes[at + 3], bytes[at + 4]]) as usize;
        let end = (at + 1 + len).min(bytes.len());
        if kind == b'T' && at + 7 <= end {
            let count = u16::from_be_bytes([bytes[at + 5], bytes[at + 6]]) as usize;
            let mut names = Vec::with_capacity(count);
            let mut i = at + 7;
            for _ in 0..count {
                let Some(nul) = bytes[i..end].iter().position(|b| *b == 0) else {
                    break;
                };
                names.push(String::from_utf8_lossy(&bytes[i..i + nul]).into_owned());
                // The name, its NUL, and 18 bytes of type information.
                i += nul + 1 + 18;
            }
            return names;
        }
        at = end.max(at + 5);
    }
    Vec::new()
}

/// Reports a failure a [`run_internal`] statement already recorded.
async fn report_run_failure<S>(stream: &mut S, message: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    send_error_response(stream, sqlstate_for(message), message).await
}

fn read_only(state: &SessionState) -> bool {
    state.transaction.as_ref().map(|t| t.read_only).unwrap_or(state.default_read_only)
}

/// Whether `name` is a relation a view's query may read: a table, a view,
/// or a catalogue relation.
async fn relation_exists(database: &Arc<Database>, name: &str, tx: Option<TxId>) -> bool {
    database.has_table(name)
        || crate::core::rql::catalog_views::is_catalog_view(name)
        || crate::core::rql::catalog_views::is_catalog_view(&format!("information_schema.{name}"))
        || views::lookup(database, name, tx).await.ok().flatten().is_some()
}

/// The view statements: `CREATE [OR REPLACE] [MATERIALIZED] VIEW`, `DROP
/// [MATERIALIZED] VIEW`, `REFRESH MATERIALIZED VIEW`, `ALTER … VIEW …
/// RENAME TO`.
pub(super) async fn handle_view_statement<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    parsed: Result<ViewStatement, views::Refusal>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let statement = match parsed {
        Ok(statement) => statement,
        Err((code, message)) => return fail_statement(state, stream, code, &message).await,
    };
    let verb = match &statement {
        ViewStatement::Create { kind: ViewKind::View, .. } => "CREATE VIEW",
        ViewStatement::Create { .. } => "CREATE MATERIALIZED VIEW",
        ViewStatement::Drop { kind: ViewKind::View, .. } => "DROP VIEW",
        ViewStatement::Drop { .. } => "DROP MATERIALIZED VIEW",
        ViewStatement::Refresh { .. } => "REFRESH MATERIALIZED VIEW",
        ViewStatement::Rename { kind: ViewKind::View, .. } => "ALTER VIEW",
        ViewStatement::Rename { .. } => "ALTER MATERIALIZED VIEW",
    };
    if read_only(state) {
        return fail_statement(state, stream, "25006", &read_only_message(verb)).await;
    }
    if let Some(t) = state.transaction.as_mut() {
        t.queried = true;
    }
    match statement {
        ViewStatement::Create { name, columns, query, or_replace, kind, if_not_exists, with_data } => {
            create_view(database, state, stream, name, columns, query, or_replace, kind, if_not_exists, with_data).await
        }
        ViewStatement::Drop { names, kind, if_exists, cascade } => {
            drop_views(database, state, stream, names, kind, if_exists, cascade, verb).await
        }
        ViewStatement::Refresh { name, with_data } => refresh(database, state, stream, name, with_data).await,
        ViewStatement::Rename { from, to, kind, if_exists } => {
            rename_view(database, state, stream, &from, &to, Some(kind), if_exists, verb).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn create_view<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    name: String,
    columns: Option<Vec<String>>,
    query: String,
    or_replace: bool,
    kind: ViewKind,
    if_not_exists: bool,
    with_data: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let tx = state.current_tx();
    let existing = match views::lookup(database, &name, tx).await {
        Ok(existing) => existing,
        Err(e) => return fail_statement(state, stream, "XX000", &e).await,
    };
    if existing.is_none() && or_replace && database.has_table(&name) {
        return fail_statement(state, stream, "42809", &format!("\"{name}\" is not a view")).await;
    }
    let taken = existing.is_some() || database.has_table(&name);
    if taken && (kind == ViewKind::Materialized || !or_replace || existing.is_none()) {
        if if_not_exists {
            crate::core::notice::push("NOTICE", "42P07", format!("relation \"{name}\" already exists, skipping"));
            return send_command_complete(stream, "CREATE MATERIALIZED VIEW").await;
        }
        return fail_statement(state, stream, "42P07", &format!("relation \"{name}\" already exists")).await;
    }
    if let Some(existing) = &existing
        && existing.kind != kind
    {
        return fail_statement(state, stream, "42809", &format!("\"{name}\" is not a {}", kind.noun())).await;
    }

    // Every relation the query names must exist, and must not be the view.
    let deps = views::query_deps(&query);
    if deps.iter().any(|d| d.eq_ignore_ascii_case(&name)) {
        return fail_statement(
            state,
            stream,
            "42P17",
            &format!("infinite recursion detected: the query of view \"{name}\" reads the view itself"),
        )
        .await;
    }
    for dep in &deps {
        if !relation_exists(database, dep, tx).await {
            return fail_statement(state, stream, "42P01", &format!("relation \"{dep}\" does not exist")).await;
        }
    }

    // The columns, as the query describes them.
    let expanded = match views::expand(database, &query, tx, Default::default()).await {
        Ok(text) => text.unwrap_or_else(|| query.clone()),
        Err((code, message)) => return fail_statement(state, stream, code, &message).await,
    };
    let described: Vec<String> = match Box::pin(describe_query(database, &expanded)).await {
        Ok(Some((fields, true))) if !fields.is_empty() => fields.into_iter().map(|(n, _)| n).collect(),
        Ok(_) => {
            // Not describable without running: run it for no rows, which
            // reports the query's own error, or its columns.
            let probe = format!("SELECT * FROM ({expanded}) AS __view_probe LIMIT 0");
            match run_for_view_described(database, state, &probe).await? {
                Ok(columns) if !columns.is_empty() => columns,
                Ok(_) => {
                    return fail_statement(
                        state,
                        stream,
                        "0A000",
                        &format!("the columns of the query of {} \"{name}\" could not be determined", kind.noun()),
                    )
                    .await;
                }
                Err(e) => return report_run_failure(stream, &e).await,
            }
        }
        Err(e) => return fail_statement(state, stream, "XX000", &e.to_string()).await,
    };
    let inner: Vec<String> = described;
    let mut names = inner.clone();
    if let Some(list) = &columns {
        if list.len() > names.len() {
            return fail_statement(
                state,
                stream,
                "42601",
                &format!("CREATE {} specifies more column names than columns", kind.noun().to_ascii_uppercase()),
            )
            .await;
        }
        for (at, column) in list.iter().enumerate() {
            names[at] = column.clone();
        }
    }
    let mut seen: Vec<&str> = Vec::new();
    for n in &names {
        if seen.contains(&n.as_str()) {
            return fail_statement(state, stream, "42701", &format!("column \"{n}\" specified more than once")).await;
        }
        seen.push(n);
    }
    // A column list renames positionally: an outer select over the query,
    // reading each inner column by the name it is described with.
    let renamed = names != inner;
    if renamed {
        let mut inner_seen: Vec<&str> = Vec::new();
        for n in &inner {
            if inner_seen.contains(&n.as_str()) {
                return fail_statement(
                    state,
                    stream,
                    "0A000",
                    &format!("a view whose query returns the column \"{n}\" twice cannot be renamed by a column list yet"),
                )
                .await;
            }
            inner_seen.push(n);
        }
    }
    let definition = if renamed {
        let items: Vec<String> = inner
            .iter()
            .zip(&names)
            .map(|(from, to)| format!("__v.{} AS {}", views::quote_ident(from), views::quote_ident(to)))
            .collect();
        format!("SELECT {} FROM ({}) AS __v", items.join(", "), query)
    } else {
        query.clone()
    };

    // CREATE OR REPLACE keeps every column the view had, in order, under
    // its name; it may add columns after them (PostgreSQL's rule).
    if let Some(existing) = &existing {
        if names.len() < existing.columns.len() {
            return fail_statement(state, stream, "42P16", "cannot drop columns from view").await;
        }
        for (old, new) in existing.columns.iter().zip(&names) {
            if old != new {
                return fail_statement(
                    state,
                    stream,
                    "42P16",
                    &format!("cannot change name of view column \"{old}\" to \"{new}\""),
                )
                .await;
            }
        }
    }

    let view = ViewDef { name: name.clone(), kind, definition: definition.clone(), columns: names.clone(), deps };
    let tag = if kind == ViewKind::Materialized {
        // The table, filled by the query (expanded, so a view it reads is
        // read as one), then its row.
        let column_list = names.iter().map(|n| views::quote_ident(n)).collect::<Vec<_>>().join(", ");
        let ctas = format!(
            "CREATE TABLE {} ({}) AS {}{}",
            views::quote_ident(&name),
            column_list,
            expanded,
            if with_data { "" } else { " WITH NO DATA" }
        );
        match run_internal(database, state, &ctas).await? {
            Ok(tag) => tag,
            Err(e) => return report_run_failure(stream, &e).await,
        }
    } else {
        "CREATE VIEW".to_string()
    };
    if let Err(e) = views::store(database, &view, existing.is_some(), tx).await {
        return fail_statement(state, stream, sqlstate_for(&e), &e).await;
    }
    // A materialized view reports its rows as `SELECT n`, as PostgreSQL's.
    let tag = if kind == ViewKind::View || tag.starts_with("SELECT") { tag } else { "SELECT 0".to_string() };
    send_command_complete(stream, &tag).await
}

/// The views to drop for `names`: each named one (checked to be of `kind`),
/// and with `cascade` every view that reads one of them, transitively; the
/// dependents first. `Err` for a refusal.
async fn drop_order(
    database: &Arc<Database>,
    names: &[String],
    kind: Option<ViewKind>,
    if_exists: bool,
    cascade: bool,
    tx: Option<TxId>,
) -> Result<Vec<ViewDef>, views::Refusal> {
    let all = views::load(database, None, tx).await.map_err(|e| ("XX000", e))?;
    let mut targets: Vec<ViewDef> = Vec::new();
    for name in names {
        match all.iter().find(|v| v.name == *name) {
            Some(view) => {
                if let Some(kind) = kind
                    && view.kind != kind
                {
                    return Err(("42809", format!("\"{name}\" is not a {}", kind.noun())));
                }
                targets.push(view.clone());
            }
            None if if_exists => {
                let noun = kind.unwrap_or(ViewKind::View).noun();
                crate::core::notice::push("NOTICE", "00000", format!("{noun} \"{name}\" does not exist, skipping"));
            }
            None if kind.is_some() && database.has_table(name) => {
                return Err(("42809", format!("\"{name}\" is not a {}", kind.unwrap_or(ViewKind::View).noun())));
            }
            None => {
                let noun = kind.unwrap_or(ViewKind::View).noun();
                return Err(("42P01", format!("{noun} \"{name}\" does not exist")));
            }
        }
    }
    // Dependents, transitively.
    let mut ordered: Vec<ViewDef> = Vec::new();
    let mut frontier: Vec<String> = targets.iter().map(|v| v.name.clone()).collect();
    let mut dropping: Vec<String> = frontier.clone();
    while let Some(name) = frontier.pop() {
        let lower = name.to_ascii_lowercase();
        for view in all.iter().filter(|v| v.deps.contains(&lower)) {
            if dropping.contains(&view.name) {
                continue;
            }
            let noun = all.iter().find(|v| v.name == name).map_or(ViewKind::View, |v| v.kind).noun();
            if !cascade {
                return Err((
                    "2BP01",
                    format!(
                        "cannot drop {noun} {name} because other objects depend on it: {} {} depends on {noun} \
                         {name} (use DROP ... CASCADE to drop the dependent objects too)",
                        view.kind.noun(),
                        view.name
                    ),
                ));
            }
            crate::core::notice::push("NOTICE", "00000", format!("drop cascades to {} {}", view.kind.noun(), view.name));
            dropping.push(view.name.clone());
            frontier.push(view.name.clone());
            ordered.push(view.clone());
        }
    }
    // Dependents were found outward from the targets, so dropped in reverse
    // they go before what they read.
    ordered.reverse();
    ordered.extend(targets);
    Ok(ordered)
}

/// Drops the views `order` lists, in order: each one's row, and a
/// materialized view's table.
async fn drop_in_order<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    order: &[ViewDef],
) -> io::Result<Result<(), ()>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let tx = state.current_tx();
    for view in order {
        if let Err(e) = views::remove(database, &view.name, tx).await {
            fail_statement(state, stream, sqlstate_for(&e), &e).await?;
            return Ok(Err(()));
        }
        if view.kind == ViewKind::Materialized {
            let sql = format!("DROP TABLE {}", views::quote_ident(&view.name));
            if let Err(e) = run_internal(database, state, &sql).await? {
                report_run_failure(stream, &e).await?;
                return Ok(Err(()));
            }
        }
    }
    Ok(Ok(()))
}

#[allow(clippy::too_many_arguments)]
async fn drop_views<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    names: Vec<String>,
    kind: ViewKind,
    if_exists: bool,
    cascade: bool,
    verb: &str,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let tx = state.current_tx();
    let order = match drop_order(database, &names, Some(kind), if_exists, cascade, tx).await {
        Ok(order) => order,
        Err((code, message)) => return fail_statement(state, stream, code, &message).await,
    };
    if drop_in_order(database, state, stream, &order).await?.is_err() {
        return Ok(());
    }
    send_command_complete(stream, verb).await
}

async fn refresh<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    name: String,
    with_data: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let tx = state.current_tx();
    let view = match views::lookup(database, &name, tx).await {
        Ok(Some(view)) if view.kind == ViewKind::Materialized => view,
        Ok(Some(_)) => {
            return fail_statement(state, stream, "0A000", &format!("\"{name}\" is not a materialized view")).await;
        }
        Ok(None) if database.has_table(&name) => {
            return fail_statement(state, stream, "0A000", &format!("\"{name}\" is not a materialized view")).await;
        }
        Ok(None) => {
            return fail_statement(state, stream, "42P01", &format!("relation \"{name}\" does not exist")).await;
        }
        Err(e) => return fail_statement(state, stream, "XX000", &e).await,
    };
    let expanded = match views::expand(database, &view.definition, tx, Default::default()).await {
        Ok(text) => text.unwrap_or_else(|| view.definition.clone()),
        Err((code, message)) => return fail_statement(state, stream, code, &message).await,
    };
    let quoted = views::quote_ident(&name);
    let mut statements = vec![format!("DELETE FROM {quoted}")];
    if with_data {
        let columns = view.columns.iter().map(|n| views::quote_ident(n)).collect::<Vec<_>>().join(", ");
        statements.push(format!("INSERT INTO {quoted} ({columns}) SELECT * FROM ({expanded}) AS __refresh"));
    }
    // Outside a block the two run as one transaction of their own, so no
    // reader sees the view emptied between them.
    let own_block = state.transaction.is_none();
    if own_block {
        if let Err(e) = run_internal(database, state, "BEGIN").await? {
            return report_run_failure(stream, &e).await;
        }
    }
    let previous = state.refreshing_materialized.replace(name.clone());
    let mut failure = None;
    for sql in &statements {
        if let Err(e) = run_internal(database, state, sql).await? {
            failure = Some(e);
            break;
        }
    }
    state.refreshing_materialized = previous;
    if own_block {
        let end = if failure.is_some() { "ROLLBACK" } else { "COMMIT" };
        let ended = run_internal(database, state, end).await?;
        if failure.is_none()
            && let Err(e) = ended
        {
            failure = Some(e);
        }
    }
    match failure {
        Some(e) => report_run_failure(stream, &e).await,
        None => send_command_complete(stream, "REFRESH MATERIALIZED VIEW").await,
    }
}

/// `ALTER [MATERIALIZED] VIEW from RENAME TO to`, and `ALTER TABLE` naming a
/// view (`kind` `None`: either kind).
#[allow(clippy::too_many_arguments)]
async fn rename_view<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    from: &str,
    to: &str,
    kind: Option<ViewKind>,
    if_exists: bool,
    verb: &str,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let tx = state.current_tx();
    let view = match views::lookup(database, from, tx).await {
        Ok(Some(view)) => view,
        Ok(None) if if_exists => {
            crate::core::notice::push("NOTICE", "00000", format!("relation \"{from}\" does not exist, skipping"));
            return send_command_complete(stream, verb).await;
        }
        Ok(None) if database.has_table(from) => {
            let noun = kind.unwrap_or(ViewKind::View).noun();
            return fail_statement(state, stream, "42809", &format!("\"{from}\" is not a {noun}")).await;
        }
        Ok(None) => return fail_statement(state, stream, "42P01", &format!("relation \"{from}\" does not exist")).await,
        Err(e) => return fail_statement(state, stream, "XX000", &e).await,
    };
    if let Some(kind) = kind
        && view.kind != kind
    {
        return fail_statement(state, stream, "42809", &format!("\"{from}\" is not a {}", kind.noun())).await;
    }
    if database.has_table(to) && !(view.kind == ViewKind::Materialized && to == from)
        || matches!(views::lookup(database, to, tx).await, Ok(Some(_)))
    {
        return fail_statement(state, stream, "42P07", &format!("relation \"{to}\" already exists")).await;
    }
    if view.kind == ViewKind::Materialized {
        let sql = format!("ALTER TABLE {} RENAME TO {}", views::quote_ident(from), views::quote_ident(to));
        if let Err(e) = run_internal(database, state, &sql).await? {
            return report_run_failure(stream, &e).await;
        }
    }
    if let Err(e) = views::rename(database, from, to, tx).await {
        return fail_statement(state, stream, sqlstate_for(&e), &e).await;
    }
    send_command_complete(stream, verb).await
}

/// The view guards on table DDL: a table may not take a view's name, `DROP
/// TABLE` and `ALTER TABLE` of a view are refused (as PostgreSQL's 42809) or,
/// for a rename, done as the view's, a table views read is not dropped from
/// under them (2BP01, or with `CASCADE` they go first), and a renamed table's
/// views follow it. `Ok(true)` when the statement was answered here.
pub(super) async fn guard_table_ddl<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    stmt: &str,
    ddl: TableDdl,
) -> io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let tx = state.current_tx();
    match ddl {
        TableDdl::Create { name, if_not_exists } => {
            if !matches!(views::lookup(database, &name, tx).await, Ok(Some(_))) {
                return Ok(false);
            }
            if if_not_exists {
                crate::core::notice::push("NOTICE", "42P07", format!("relation \"{name}\" already exists, skipping"));
                send_command_complete(stream, "CREATE TABLE").await?;
            } else {
                fail_statement(state, stream, "42P07", &format!("relation \"{name}\" already exists")).await?;
            }
            Ok(true)
        }
        TableDdl::Drop { names, cascade, .. } => {
            let found = views::views_among(database, &names, tx).await.unwrap_or_default();
            if let Some(view) = found.first() {
                let hint = match view.kind {
                    ViewKind::View => "DROP VIEW",
                    ViewKind::Materialized => "DROP MATERIALIZED VIEW",
                };
                fail_statement(state, stream, "42809", &format!("\"{}\" is not a table (use {hint} to remove it)", view.name))
                    .await?;
                return Ok(true);
            }
            let tables: Vec<String> = names.iter().filter(|n| database.has_table(n)).cloned().collect();
            if tables.is_empty() {
                return Ok(false);
            }
            // The views reading the tables, which go first with CASCADE.
            let all = views::load(database, None, tx).await.unwrap_or_default();
            let readers: Vec<String> = all
                .iter()
                .filter(|v| v.deps.iter().any(|d| tables.iter().any(|t| t.eq_ignore_ascii_case(d))))
                .map(|v| v.name.clone())
                .collect();
            if readers.is_empty() {
                return Ok(false);
            }
            if !cascade {
                let table = tables
                    .iter()
                    .find(|t| all.iter().any(|v| readers.contains(&v.name) && v.deps.contains(&t.to_ascii_lowercase())))
                    .cloned()
                    .unwrap_or_default();
                fail_statement(
                    state,
                    stream,
                    "2BP01",
                    &format!(
                        "cannot drop table {table} because other objects depend on it: view {} depends on table \
                         {table} (use DROP ... CASCADE to drop the dependent objects too)",
                        readers[0]
                    ),
                )
                .await?;
                return Ok(true);
            }
            for reader in &readers {
                crate::core::notice::push("NOTICE", "00000", format!("drop cascades to view {reader}"));
            }
            let order = match drop_order(database, &readers, None, true, true, tx).await {
                Ok(order) => order,
                Err((code, message)) => {
                    fail_statement(state, stream, code, &message).await?;
                    return Ok(true);
                }
            };
            if drop_in_order(database, state, stream, &order).await?.is_err() {
                return Ok(true);
            }
            Ok(false)
        }
        TableDdl::Alter { name, rename_to } => {
            if let Ok(Some(view)) = views::lookup(database, &name, tx).await {
                match rename_to {
                    Some(to) => {
                        let verb = "ALTER TABLE";
                        rename_view(database, state, stream, &name, &to, None, false, verb).await?;
                    }
                    None => {
                        let noun = view.kind.noun();
                        fail_statement(
                            state,
                            stream,
                            "42809",
                            &format!("\"{name}\" is not a table (ALTER {} supports RENAME TO)", noun.to_ascii_uppercase()),
                        )
                        .await?;
                    }
                }
                return Ok(true);
            }
            let Some(to) = rename_to else {
                return Ok(false);
            };
            if views::dependents(database, &name, tx).await.unwrap_or_default().is_empty() {
                return Ok(false);
            }
            // The rename, then its views pointed at the new name.
            match run_internal(database, state, stmt).await? {
                Ok(_) => {}
                Err(e) => {
                    report_run_failure(stream, &e).await?;
                    return Ok(true);
                }
            }
            if let Err(e) = views::retarget_dependents(database, &name, &to, tx).await {
                fail_statement(state, stream, sqlstate_for(&e), &e).await?;
                return Ok(true);
            }
            send_command_complete(stream, "ALTER TABLE").await?;
            Ok(true)
        }
    }
}
