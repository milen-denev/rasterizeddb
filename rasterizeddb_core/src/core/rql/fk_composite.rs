//! Composite foreign keys: the checks of a key over several columns, whose
//! values are compared as one tuple.
//!
//! A child row references the one parent row whose referenced columns hold
//! all of its key values at once. Checking each column on its own would
//! accept `(1, 2)` against parents `(1, 1)` and `(2, 2)`, and acting on each
//! column on its own would cascade a parent's delete into the children of
//! another parent sharing one of its values; so a composite key never goes
//! through the one-column paths (`ForeignKey::single` keeps it out of them)
//! and is handled here, with the same steps they take:
//!
//! - a child's check finds its parent as of now, takes the key-share lock
//!   on that parent's key (every key column's stored bytes, as one lock key,
//!   [`lock_key`]) and looks again under it;
//! - a parent's `DELETE` or key-moving `UPDATE` reads the tuples it removes
//!   through the statement's snapshot, locks each exclusively under the same
//!   name ([`lock_column`]), and then judges the referencing rows as of now.
//!
//! Probes are phrased as the queries the one-column paths phrase, with a
//! tuple's equalities joined by `AND` and tuples by `OR`.
//!
//! **MATCH SIMPLE** (the default): a row with any key column NULL references
//! nothing and is exempt. **MATCH FULL**: such a row must have every key
//! column NULL, else 23503. A parent tuple with a NULL in it is referenced by
//! no row under either.
//!
//! Only what a table with composite keys does reaches this module: a table
//! without one pays the `is_composite` test of each key on the paths it
//! already walked.

use super::*;
use crate::core::rql::foreign_key::ForeignKey;

/// A boxed future of this module's entry points.
pub(super) type Boxed<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// The lock name of a composite key's referenced columns: their names in
/// key order, joined by a byte no name holds. The child's check and the
/// parent's `DELETE` and `UPDATE` all take a key's locks under it.
pub(super) fn lock_column(parent_columns: &[String]) -> String {
    let mut out = String::from("\u{1f}");
    for (i, column) in parent_columns.iter().enumerate() {
        if i > 0 {
            out.push('\u{1f}');
        }
        out.push_str(&column.to_ascii_lowercase());
    }
    out
}

/// The lock key of one parent tuple: each key column's stored bytes, length
/// first, in key order.
pub(super) fn lock_key(cells: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(cells.iter().map(|c| c.len() + 4).sum());
    for cell in cells {
        out.extend_from_slice(&(cell.len() as u32).to_le_bytes());
        out.extend_from_slice(cell);
    }
    out
}

/// `(a = 1 AND b = 2) OR (a = 3 AND b = 4)`: rows holding one of `tuples`
/// in `columns`.
pub(super) fn tuple_predicate(columns: &[String], tuples: &[Vec<String>]) -> String {
    let one = |tuple: &Vec<String>| {
        columns
            .iter()
            .zip(tuple)
            .map(|(column, value)| format!("{column} = {value}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    match tuples {
        [tuple] => one(tuple),
        _ => tuples.iter().map(|t| format!("({})", one(t))).collect::<Vec<_>>().join(" OR "),
    }
}

/// How many tuples one probe carries: the one-column probe's literal count,
/// spread over the columns.
fn chunk_for(columns: usize) -> usize {
    (REFERENCED_PROBE_CHUNK / columns.max(1)).max(1)
}

/// The live fields `columns` name in `fields`, in that order.
fn key_fields(
    fields: &[crate::core::row::schema::SchemaField],
    columns: &[String],
    table: &str,
) -> Result<Vec<crate::core::row::schema::SchemaField>, String> {
    columns
        .iter()
        .map(|column| {
            fields
                .iter()
                .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column))
                .cloned()
                .ok_or_else(|| format!("column \"{column}\" of relation \"{table}\" does not exist"))
        })
        .collect()
}

/// The rows of `table_name` holding one of `tuples` (a literal per column of
/// `columns`), each as its key columns' stored bytes in `columns` order,
/// deduplicated — read **as of now** on `tx`'s behalf, as
/// `referenced_values_now` reads a one-column key's, and for the same
/// reason: a foreign key's question is about what is committed (and `tx`'s
/// own writes), never about an older snapshot.
///
/// A partitioned table is read through a fan-out routed on the predicate,
/// every partition under one as-of-now snapshot.
async fn tuples_now_fut(
    database: &Arc<Database>,
    table_name: &str,
    columns: &[String],
    tuples: &[Vec<String>],
    tx: TxId,
    limit: Option<u64>,
) -> Result<Vec<Vec<Vec<u8>>>, String> {
    let Some(relation) = database.relation_for(table_name, owner(), TableMode::AccessShare).await? else {
        return Err(format!("relation \"{}\" does not exist", table_name));
    };
    if let crate::core::database::Relation::Plain(table) = &relation {
        // A shell with no partitioned table behind it holds no rows: its
        // data calls refuse, which is the error, never an empty answer.
        table.data().map_err(|e| e.to_string())?;
    }
    let schema = &relation.schema().fields;
    let fields = key_fields(schema, columns, table_name)?;
    let names: Vec<String> = fields.iter().map(|f| f.name.clone()).collect();
    let requested = crate::core::rql::join::fetch_for(&fields, schema);
    let all_fields = schema.to_vec();

    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut out = Vec::new();
    for chunk in tuples.chunks(chunk_for(columns.len())) {
        let predicate = tuple_predicate(&names, chunk);
        // The constant fold works on a statement, so it is given one.
        let statement =
            fold_where_constants(&format!("SELECT {} FROM {} WHERE {}", names.join(", "), table_name, predicate));
        let Some(where_at) = crate::core::rql::join::find_keyword(&statement, "WHERE", 0) else {
            return Err("the probe lost its WHERE clause".to_string());
        };
        let raw = statement[where_at + "WHERE".len()..].trim();
        let rewritten = crate::core::tokenizer::predicate_rewrite::rewrite_where(raw, schema)?;
        let mut predicate_fields: Vec<crate::core::row::schema::SchemaField> = Vec::new();
        for name in crate::core::rql::lexer_query::extract_where_columns(&rewritten, schema) {
            if let Some(f) = schema.iter().find(|f| f.name == name) {
                predicate_fields.push(f.clone());
            }
        }
        let query_fetch = crate::core::rql::join::fetch_for(&predicate_fields, schema);
        let rows = match &relation {
            crate::core::database::Relation::Plain(table) => table
                .query_row_with_visibility(
                    &rewritten,
                    &all_fields,
                    query_fetch,
                    requested.clone(),
                    limit,
                    None,
                    table.visibility_now_for(tx),
                )
                .await
                .map_err(|e| e.to_string())?,
            crate::core::database::Relation::Partitioned(pt) => {
                crate::core::rql::row_source::RowSource::partitioned_with(
                    pt.clone(),
                    &rewritten,
                    pt.visibility_now_for(tx),
                )
                .map_err(|e| e.to_string())?
                .query_rows(&rewritten, &all_fields, query_fetch, requested.clone(), limit, None, tx)
                .await
                .map_err(|e| e.to_string())?
            }
        };
        for row in &rows {
            let mut cells = Vec::with_capacity(fields.len());
            for field in &fields {
                match row.columns.iter().find(|c| u64::from(c.schema_id) == field.write_order) {
                    Some(cell) => cells.push(cell.data.into_slice().to_vec()),
                    None => break,
                }
            }
            if cells.len() == fields.len() && seen.insert(lock_key(&cells)) {
                out.push(cells);
            }
        }
        if limit.is_some_and(|k| out.len() as u64 >= k) {
            break;
        }
    }
    Ok(out)
}

/// The distinct tuples of `columns` in the rows `where_clause` (engine form,
/// the statement's own) matches under `visibility`, with no key column NULL:
/// each as its lock key and its literals. What a parent's `DELETE` is about
/// to remove, read through the snapshot it then scans with.
async fn tuples_as_of_fut(
    target: &crate::core::database::Relation,
    columns: &[String],
    where_clause: &str,
    query_row_fetch: &crate::core::row::row::RowFetch,
    visibility: &crate::core::mvcc::Visibility,
    tx: TxId,
) -> Result<Vec<(Vec<u8>, Vec<String>)>, String> {
    let schema = &target.schema().fields;
    let fields = key_fields(schema, columns, &target.schema().name)?;
    let flags: Vec<Option<crate::core::row::schema::SchemaField>> = fields
        .iter()
        .map(|f| crate::core::rql::nullable::flag_field(&f.name, schema).cloned())
        .collect();
    let wanted: Vec<crate::core::row::schema::SchemaField> =
        fields.iter().cloned().chain(flags.iter().flatten().cloned()).collect();
    let requested = crate::core::rql::join::fetch_for(&wanted, schema);
    let rows = relation_rows_as_of(
        target,
        where_clause,
        &schema.to_vec(),
        query_row_fetch.clone(),
        requested,
        visibility,
        tx,
    )
    .await?;
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut out = Vec::new();
    for row in &rows {
        if let Some(tuple) = row_tuple(row, &fields, &flags)? {
            let key = lock_key(&tuple.iter().map(|(bytes, _)| bytes.clone()).collect::<Vec<_>>());
            if seen.insert(key.clone()) {
                out.push((key, tuple.into_iter().map(|(_, literal)| literal).collect()));
            }
        }
    }
    Ok(out)
}

/// A row's key tuple, `(stored bytes, literal)` per column, or `None` when
/// a key column is NULL or not in the row.
fn row_tuple(
    row: &crate::core::row::row::Row,
    fields: &[crate::core::row::schema::SchemaField],
    flags: &[Option<crate::core::row::schema::SchemaField>],
) -> Result<Option<Vec<(Vec<u8>, String)>>, String> {
    let mut tuple = Vec::with_capacity(fields.len());
    for (field, flag) in fields.iter().zip(flags) {
        let cell = |write_order: u64| row.columns.iter().find(|c| u64::from(c.schema_id) == write_order);
        if let Some(f) = flag
            && cell(f.write_order).is_some_and(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0)
        {
            return Ok(None);
        }
        let Some(value) = cell(field.write_order) else {
            return Ok(None);
        };
        if value.column_type == crate::core::db_type::DbType::NULL {
            return Ok(None);
        }
        let bytes = value.data.into_slice();
        tuple.push((bytes.to_vec(), key_literal(field, bytes)?));
    }
    Ok(Some(tuple))
}

/// PostgreSQL's 23503 for a child tuple with no parent.
pub(super) fn violation(table: &str, fk: &ForeignKey, values: &[String]) -> String {
    format!(
        "insert or update on table \"{}\" violates foreign key constraint \"{}\": key ({})=({}) is not present in \
         table \"{}\"",
        table,
        fk.name,
        fk.column_list(),
        values.iter().map(|v| key_display(v)).collect::<Vec<_>>().join(", "),
        fk.parent_table
    )
}

/// PostgreSQL's 23503 for a `MATCH FULL` key given some NULLs and some not.
pub(super) fn match_full_violation(table: &str, fk: &ForeignKey) -> String {
    format!(
        "insert or update on table \"{}\" violates foreign key constraint \"{}\": MATCH FULL does not allow mixing \
         of null and nonnull key values",
        table, fk.name
    )
}

/// What a row's key values `values` (a literal per key column, `NULL` for
/// none) ask of `fk`: `Ok(None)` when the row references nothing (MATCH
/// SIMPLE with a NULL, or every column NULL), `Err` for a MATCH FULL mix,
/// else the values.
pub(super) fn referenced_values<'v>(
    table: &str,
    fk: &ForeignKey,
    values: &'v [String],
) -> Result<Option<&'v [String]>, String> {
    let nulls = values.iter().filter(|v| v.trim().eq_ignore_ascii_case("NULL")).count();
    if nulls == 0 {
        return Ok(Some(values));
    }
    if fk.match_full && nulls < values.len() {
        return Err(match_full_violation(table, fk));
    }
    Ok(None)
}

/// The child side: `fk`, a composite key of `table_name` (`schema`, the
/// child's), holds for a row whose key columns are `values`, a literal per
/// column in key order.
///
/// The three steps of `check_foreign_keys_of`, on the tuple: found as of
/// now, its key-share lock taken, found again under the lock. A table's key
/// onto itself whose parent the statement may yet write is asked again at
/// the statement's end (`defer_self_reference`).
async fn check_child_row_fut(
    database: &Arc<Database>,
    schema: Option<&crate::core::row::schema::TableSchema>,
    table_name: &str,
    fk: &ForeignKey,
    values: &[String],
    tx: TxId,
) -> Result<(), String> {
    let Some(values) = referenced_values(table_name, fk, values)? else {
        return Ok(());
    };
    if !database.has_table(&fk.parent_table) {
        return Err(format!(
            "foreign key \"{}\" references relation \"{}\", which does not exist",
            fk.name, fk.parent_table
        ));
    }
    let unchecked = |e: String| format!("foreign key \"{}\" could not be checked: {}", fk.name, e);
    // Each value at its parent column's type (P4 round 3): one that type
    // cannot hold unchanged equals no parent.
    let mut probe = Vec::with_capacity(values.len());
    for (at, literal) in values.iter().enumerate() {
        let coerced = match schema.and_then(|child| fk_key_fields(database, child, fk, at)) {
            Some(((child, child_mods), (parent, parent_mods))) => {
                coerce_key_literal(literal, &child, &child_mods, &parent, &parent_mods)
            }
            None => Some(literal.clone()),
        };
        match coerced {
            Some(value) => probe.push(value),
            None => return Err(violation(table_name, fk, values)),
        }
    }
    let pairs = || -> Vec<(String, String)> { fk.columns.iter().cloned().zip(values.iter().cloned()).collect() };
    let deferred = || {
        let deferred = defer_self_reference(schema, table_name, fk, pairs());
        if deferred {
            database.transactions.forbid_in_place(tx);
        }
        deferred
    };
    let probes = std::slice::from_ref(&probe);
    let found = tuples_now(database, &fk.parent_table, &fk.parent_columns, probes, tx, Some(1))
        .await
        .map_err(unchecked)?;
    let Some(key) = found.into_iter().next() else {
        if deferred() {
            return Ok(());
        }
        return Err(violation(table_name, fk, values));
    };
    acquire_referenced_waiting(
        database,
        &fk.parent_table,
        &lock_column(&fk.parent_columns),
        &lock_key(&key),
        tx,
        crate::core::rql::row_lock::LockStrength::Share,
    )
    .await?;
    let still = tuples_now(database, &fk.parent_table, &fk.parent_columns, probes, tx, Some(1))
        .await
        .map_err(unchecked)?;
    if still.is_empty() {
        if deferred() {
            return Ok(());
        }
        return Err(violation(table_name, fk, values));
    }
    Ok(())
}

/// Parent tuples (literals of `fk`'s referenced columns) at the child's
/// column types, index for index: `None` for a tuple no child value can
/// equal (a value its child column's type cannot hold unchanged).
pub(super) fn parent_tuples_for_child(
    database: &Arc<Database>,
    child_table: &str,
    fk: &ForeignKey,
    tuples: &[Vec<String>],
) -> Vec<Option<Vec<String>>> {
    let view = database.schema_view(child_table);
    let fields: Vec<Option<_>> = (0..fk.columns.len())
        .map(|at| view.as_ref().and_then(|view| fk_key_fields(database, view.schema(), fk, at)))
        .collect();
    tuples
        .iter()
        .map(|tuple| {
            tuple
                .iter()
                .enumerate()
                .map(|(at, literal)| match &fields[at] {
                    Some(((child, child_mods), (parent, parent_mods))) => {
                        coerce_key_literal(literal, parent, parent_mods, child, child_mods)
                    }
                    None => Some(literal.clone()),
                })
                .collect::<Option<Vec<String>>>()
        })
        .collect()
}

/// Whether any row of `child_table` references one of `tuples` (child-typed
/// literals of `fk`'s columns), as of now.
async fn referenced_by_child_fut(
    database: &Arc<Database>,
    child_table: &str,
    fk: &ForeignKey,
    tuples: &[Vec<String>],
    tx: TxId,
) -> Result<bool, String> {
    if tuples.is_empty() {
        return Ok(false);
    }
    let found = tuples_now(database, child_table, &fk.columns, tuples, tx, Some(1))
        .await
        .map_err(|e| format!("foreign key \"{}\" could not be checked: {}", fk.name, e))?;
    Ok(!found.is_empty())
}

/// PostgreSQL's 23503 for a parent change a child still references.
pub(super) fn parent_violation(parent_table: &str, fk: &ForeignKey, child_table: &str) -> String {
    format!(
        "update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\"",
        parent_table, fk.name, child_table
    )
}

/// `ON DELETE` of composite key `fk` (on `child_table`) for the parent
/// tuples `tuples` a `DELETE` from `parent_table` removes, already locked
/// by the caller. A blocking key onto the table itself is put in `deferred`
/// to be asked once the delete has run.
#[allow(clippy::too_many_arguments)]
async fn on_delete_fut(
    database: &Arc<Database>,
    parent_table: &str,
    child_table: &str,
    fk: &ForeignKey,
    tuples: &[Vec<String>],
    _depth: usize,
    tx: TxId,
    deferred: &mut Vec<SelfReference>,
) -> Result<(), String> {
    let child_tuples: Vec<Vec<String>> =
        parent_tuples_for_child(database, child_table, fk, tuples).into_iter().flatten().collect();
    if child_tuples.is_empty() {
        return Ok(());
    }
    if !fk.on_delete.blocks() {
        return Err(format!(
            "foreign key \"{}\": ON DELETE {:?} of a composite key is not supported yet",
            fk.name, fk.on_delete
        ));
    }
    // The table onto itself: its referencing rows may be deleted by this
    // same statement, so asked once it has run.
    if child_table.eq_ignore_ascii_case(parent_table) {
        deferred.push(SelfReference {
            fk_name: fk.name.clone(),
            column: String::new(),
            literals: Vec::new(),
            composite: Some((fk.clone(), child_tuples)),
        });
        return Ok(());
    }
    if referenced_by_child(database, child_table, fk, &child_tuples, tx).await? {
        return Err(parent_violation(parent_table, fk, child_table));
    }
    Ok(())
}

/// The old tuples of `change` (a composite key's referenced columns moved by
/// an `UPDATE`) no row of `parent_table` holds any more, as of now: what a
/// child can no longer reference. Index into `change.old_tuples`.
async fn vanished_tuples_fut(
    database: &Arc<Database>,
    parent_table: &str,
    change: &KeyChange,
    tx: TxId,
) -> Result<Vec<usize>, String> {
    let present: std::collections::HashSet<Vec<u8>> =
        tuples_now(database, parent_table, &change.columns, &change.old_tuples, tx, None)
            .await?
            .iter()
            .map(|cells| lock_key(cells))
            .collect();
    Ok((0..change.old_stored.len()).filter(|i| !present.contains(&change.old_stored[*i])).collect())
}

/// `ON UPDATE` of composite key `fk` (on `child_table`) once the `UPDATE`
/// of `parent_table` has written: `NO ACTION` asks whether a child still
/// references an old tuple no parent holds now; `RESTRICT` of a table onto
/// itself (put off by `lock_keys_before_update`) every moved tuple.
async fn on_update_fut(
    database: &Arc<Database>,
    parent_table: &str,
    child_table: &str,
    fk: &ForeignKey,
    change: &KeyChange,
    _depth: usize,
    tx: TxId,
) -> Result<(), String> {
    use crate::core::rql::foreign_key::ReferentialAction;
    let restrict_self = fk.on_update == ReferentialAction::Restrict && child_table.eq_ignore_ascii_case(parent_table);
    let vanished: Vec<usize> = if restrict_self {
        (0..change.old_stored.len()).collect()
    } else {
        vanished_tuples(database, parent_table, change, tx).await.map_err(|e| {
            format!("foreign key \"{}\" could not be checked: {}", fk.name, e)
        })?
    };
    if vanished.is_empty() {
        return Ok(());
    }
    let old: Vec<Vec<String>> = vanished.iter().map(|i| change.old_tuples[*i].clone()).collect();
    let child_old: Vec<Vec<String>> =
        parent_tuples_for_child(database, child_table, fk, &old).into_iter().flatten().collect();
    if child_old.is_empty() {
        return Ok(());
    }
    if !fk.on_update.blocks() {
        return Err(format!(
            "foreign key \"{}\": ON UPDATE {:?} of a composite key is not supported yet",
            fk.name, fk.on_update
        ));
    }
    if referenced_by_child(database, child_table, fk, &child_old, tx).await? {
        return Err(parent_violation(parent_table, fk, child_table));
    }
    Ok(())
}

/// `ON UPDATE RESTRICT` of composite key `fk` (on another table), before the
/// `UPDATE` writes: refused at once when a child references a moved tuple.
async fn restrict_update_fut(
    database: &Arc<Database>,
    parent_table: &str,
    child_table: &str,
    fk: &ForeignKey,
    change: &KeyChange,
    tx: TxId,
) -> Result<(), String> {
    let child_old: Vec<Vec<String>> =
        parent_tuples_for_child(database, child_table, fk, &change.old_tuples).into_iter().flatten().collect();
    if referenced_by_child(database, child_table, fk, &child_old, tx).await? {
        return Err(parent_violation(parent_table, fk, child_table));
    }
    Ok(())
}

/// A written cell of key column `field` as a literal: by the column's
/// declared type, or, for an expression's result still in the type it
/// computed in (`a + 1` over an INT is a BIGINT, which the write narrows),
/// by that type, as `check_foreign_keys_for_update` renders one.
fn written_literal(
    field: &crate::core::row::schema::SchemaField,
    ty: &crate::core::db_type::DbType,
    bytes: &[u8],
) -> Result<String, String> {
    if *ty == field.db_type {
        return key_literal(field, bytes);
    }
    crate::core::rql::subquery::value_to_literal(ty, bytes)
        .or_else(|| key_literal(field, bytes).ok())
        .ok_or_else(|| format!("the new value of \"{}\" is unrepresentable", field.name))
}

/// The child side of an `UPDATE` that assigns a column of a composite key,
/// once the rows are written: each written row's new tuple, from `written`
/// (`Table::returning_image`: the rows with their computed assignments)
/// with the statement's constant assignments `constants` laid over them, is
/// checked as an insert's would be. A violation fails the statement, whose
/// abort takes the rows back (the write ran under the statement's snapshot,
/// so none was overwritten in place).
async fn check_written_rows_fut(
    database: &Arc<Database>,
    table_name: &str,
    keys: &[ForeignKey],
    constants: &[crate::core::row::row::ColumnWritePayload],
    written: &[crate::core::row::row::Row],
    tx: TxId,
) -> Result<(), String> {
    let Some(view) = database.schema_view(table_name) else {
        return Err(format!("relation \"{table_name}\" does not exist"));
    };
    let schema = std::sync::Arc::new(view.schema().clone());
    drop(view);
    for fk in keys {
        let fields = key_fields(&schema.fields, &fk.columns, table_name)?;
        let flags: Vec<Option<crate::core::row::schema::SchemaField>> = fields
            .iter()
            .map(|f| crate::core::rql::nullable::flag_field(&f.name, &schema.fields).cloned())
            .collect();
        let mut seen = std::collections::HashSet::new();
        for row in written {
            let mut values = Vec::with_capacity(fields.len());
            for (field, flag) in fields.iter().zip(&flags) {
                let constant = |write_order: u64| constants.iter().find(|c| u64::from(c.write_order) == write_order);
                let null_constant = flag
                    .as_ref()
                    .and_then(|f| constant(f.write_order))
                    .map(|c| crate::core::rql::nullable::flag_is_set(c.data.into_slice()));
                let value = match (constant(field.write_order), null_constant) {
                    (_, Some(true)) => None,
                    (Some(c), _) if c.column_type != crate::core::db_type::DbType::NULL => {
                        Some(written_literal(field, &c.column_type, c.data.into_slice())?)
                    }
                    (Some(_), _) => None,
                    (None, Some(false)) | (None, None) => {
                        let cell = |write_order: u64| row.columns.iter().find(|c| u64::from(c.schema_id) == write_order);
                        let is_null = null_constant.is_none()
                            && flag.as_ref().is_some_and(|f| {
                                cell(f.write_order).is_some_and(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0)
                            });
                        match cell(field.write_order) {
                            Some(c) if !is_null && c.column_type != crate::core::db_type::DbType::NULL => {
                                Some(written_literal(field, &c.column_type, c.data.into_slice())?)
                            }
                            Some(_) => None,
                            None => {
                                return Err(format!(
                                    "the foreign key \"{}\" could not be checked: the written row does not carry \
                                     column \"{}\"",
                                    fk.name, field.name
                                ));
                            }
                        }
                    }
                };
                values.push(value.unwrap_or_else(|| "NULL".to_string()));
            }
            if seen.insert(values.clone()) {
                check_child_row(database, Some(&schema), table_name, fk, &values, tx).await?;
            }
        }
    }
    Ok(())
}

/// [`tuples_now_fut`], boxed where it is made, so a caller's frame never holds
/// the future (a debug build's executor frames are near their stack limit).
#[inline(never)]
pub(super) fn tuples_now<'a>(
    database: &'a Arc<Database>,
    table_name: &'a str,
    columns: &'a [String],
    tuples: &'a [Vec<String>],
    tx: TxId,
    limit: Option<u64>,
) -> Boxed<'a, Result<Vec<Vec<Vec<u8>>>, String>> {
    Box::pin(tuples_now_fut(database, table_name, columns, tuples, tx, limit))
}

/// [`tuples_as_of_fut`], boxed where it is made, so a caller's frame never holds
/// the future (a debug build's executor frames are near their stack limit).
#[inline(never)]
pub(super) fn tuples_as_of<'a>(
    target: &'a crate::core::database::Relation,
    columns: &'a [String],
    where_clause: &'a str,
    query_row_fetch: &'a crate::core::row::row::RowFetch,
    visibility: &'a crate::core::mvcc::Visibility,
    tx: TxId,
) -> Boxed<'a, Result<Vec<(Vec<u8>, Vec<String>)>, String>> {
    Box::pin(tuples_as_of_fut(target, columns, where_clause, query_row_fetch, visibility, tx))
}

/// [`check_child_row_fut`], boxed where it is made, so a caller's frame never holds
/// the future (a debug build's executor frames are near their stack limit).
#[inline(never)]
pub(super) fn check_child_row<'a>(
    database: &'a Arc<Database>,
    schema: Option<&'a crate::core::row::schema::TableSchema>,
    table_name: &'a str,
    fk: &'a ForeignKey,
    values: &'a [String],
    tx: TxId,
) -> Boxed<'a, Result<(), String>> {
    Box::pin(check_child_row_fut(database, schema, table_name, fk, values, tx))
}

/// [`referenced_by_child_fut`], boxed where it is made, so a caller's frame never holds
/// the future (a debug build's executor frames are near their stack limit).
#[inline(never)]
pub(super) fn referenced_by_child<'a>(
    database: &'a Arc<Database>,
    child_table: &'a str,
    fk: &'a ForeignKey,
    tuples: &'a [Vec<String>],
    tx: TxId,
) -> Boxed<'a, Result<bool, String>> {
    Box::pin(referenced_by_child_fut(database, child_table, fk, tuples, tx))
}

/// [`on_delete_fut`], boxed where it is made, so a caller's frame never holds
/// the future (a debug build's executor frames are near their stack limit).
#[inline(never)]
pub(super) fn on_delete<'a>(
    database: &'a Arc<Database>,
    parent_table: &'a str,
    child_table: &'a str,
    fk: &'a ForeignKey,
    tuples: &'a [Vec<String>],
    _depth: usize,
    tx: TxId,
    deferred: &'a mut Vec<SelfReference>,
) -> Boxed<'a, Result<(), String>> {
    Box::pin(on_delete_fut(database, parent_table, child_table, fk, tuples, _depth, tx, deferred))
}

/// [`vanished_tuples_fut`], boxed where it is made, so a caller's frame never holds
/// the future (a debug build's executor frames are near their stack limit).
#[inline(never)]
pub(super) fn vanished_tuples<'a>(
    database: &'a Arc<Database>,
    parent_table: &'a str,
    change: &'a KeyChange,
    tx: TxId,
) -> Boxed<'a, Result<Vec<usize>, String>> {
    Box::pin(vanished_tuples_fut(database, parent_table, change, tx))
}

/// [`on_update_fut`], boxed where it is made, so a caller's frame never holds
/// the future (a debug build's executor frames are near their stack limit).
#[inline(never)]
pub(super) fn on_update<'a>(
    database: &'a Arc<Database>,
    parent_table: &'a str,
    child_table: &'a str,
    fk: &'a ForeignKey,
    change: &'a KeyChange,
    _depth: usize,
    tx: TxId,
) -> Boxed<'a, Result<(), String>> {
    Box::pin(on_update_fut(database, parent_table, child_table, fk, change, _depth, tx))
}

/// [`restrict_update_fut`], boxed where it is made, so a caller's frame never holds
/// the future (a debug build's executor frames are near their stack limit).
#[inline(never)]
pub(super) fn restrict_update<'a>(
    database: &'a Arc<Database>,
    parent_table: &'a str,
    child_table: &'a str,
    fk: &'a ForeignKey,
    change: &'a KeyChange,
    tx: TxId,
) -> Boxed<'a, Result<(), String>> {
    Box::pin(restrict_update_fut(database, parent_table, child_table, fk, change, tx))
}

/// [`check_written_rows_fut`], boxed where it is made, so a caller's frame never holds
/// the future (a debug build's executor frames are near their stack limit).
#[inline(never)]
pub(super) fn check_written_rows<'a>(
    database: &'a Arc<Database>,
    table_name: &'a str,
    keys: &'a [ForeignKey],
    constants: &'a [crate::core::row::row::ColumnWritePayload],
    written: &'a [crate::core::row::row::Row],
    tx: TxId,
) -> Boxed<'a, Result<(), String>> {
    Box::pin(check_written_rows_fut(database, table_name, keys, constants, written, tx))
}
