use rclite::Arc;

use log::{debug, error, info, warn};

use crate::core::mvcc::table_locks::TableMode;
use crate::core::mvcc::transaction::TxId;

/// The running statement's lock owner (§7 Owner): a block's root, or an
/// autocommit statement's O, begun now if nothing asked for it yet. Every
/// table this file reaches is taken through `Database::table_for` /
/// `relation_for` under it.
#[inline]
fn owner() -> Option<crate::core::mvcc::owner::StatementOwner> {
    crate::core::mvcc::owner::ensure()
}

use crate::core::{
    database::Database,
    rql::{
        lexer_query::row_fetch_from_select_query, lexer_s1::QueryPurpose,
        lexer_s1::recognize_query_purpose,
    },
};

/// Runs a joined `SELECT`: each relation is scanned once with its own share of
/// the `WHERE` clause pushed down, then the results are combined.
///
/// `tx` is the reader every relation is scanned as. It is not optional: a
/// transaction must see its own writes, and a join that scanned outside the
/// transaction did not. `BEGIN; INSERT …; SELECT … JOIN …` found nothing, and a
/// chained data-modifying CTE — which materialises its rows inside the
/// statement's transaction and then joins them — returned zero rows and
/// reported success.
async fn execute_join(sql: &str, database: Arc<Database>, tx: TxId) -> Vec<u8> {
    fn error(message: &str) -> Vec<u8> {
        let mut result = vec![3u8];
        result.extend_from_slice(message.as_bytes());
        result
    }

    // Every relation the join reads, at once and in name order (§7
    // Acquisition), before its schemas plan anything: the plan below reads
    // them through `schema_view`, under these locks.
    if let Err(e) = database
        .tables_for(&crate::core::rql::lock_set::statement_lock_set(sql), owner())
        .await
    {
        return error(&e);
    }
    let schema_of = |name: &str| database.schema_view(name).map(|v| v.schema().fields.clone());

    let plan = match crate::core::rql::join::plan_statement(sql, &schema_of) {
        Ok(p) => p,
        Err(e) => {
            error!("Cannot plan join: {}", e);
            return error(&e);
        }
    };
    // The relations the plan resolved, which the text scan may have spelled
    // differently: confirmed (a held lock is granted again from the fast
    // path) or taken, at once, before any is scanned.
    let planned: Vec<(String, TableMode)> = plan
        .relations
        .iter()
        .map(|r| (r.table.clone(), TableMode::AccessShare))
        .collect();
    if let Err(e) = database.tables_for(&planned, owner()).await {
        return error(&e);
    }

    // Each relation is kept as the scan's serialized rows: a decoded `Row` is
    // ~1.5 KB whatever it holds. `RDB_JOIN_ROWS=1` scans into `Vec<Row>`
    // instead, the path this replaced, for A/B measurement.
    let legacy_rows = legacy_join_rows();
    let mut scanned: Vec<crate::core::rql::join::RowSet> =
        Vec::with_capacity(plan.relations.len());
    // Every relation is read under one snapshot, the statement's. A snapshot
    // per scan let a transaction that committed between two of them show in
    // one relation and not the other: a transfer between two tables joined
    // to half of itself. All versioned tables share one transaction manager,
    // so the first one's snapshot is valid for the rest.
    let mut statement_snapshot: Option<crate::core::mvcc::Visibility> = None;
    for relation in &plan.relations {
        // A partitioned table's rows are in its partitions; `tables` holds
        // only its shell under this name (§3.3). Cloned out, so no map guard
        // spans the scan below.
        let source = match database
            .relation_for(&relation.table, owner(), TableMode::AccessShare)
            .await
        {
            Ok(Some(source)) => source,
            Ok(None) => return error(&format!("relation '{}' does not exist", relation.table)),
            Err(e) => return error(&e),
        };

        // The declared schema: a partitioned table's shell's, which every
        // partition's is a clone of, and which planned the join.
        let schema = &source.schema().fields;
        let requested = crate::core::rql::join::fetch_for(&relation.fetch_columns, schema);

        // The scan needs whatever the predicate reads, which may be columns the
        // join itself never looks at.
        let mut predicate_fields: Vec<crate::core::row::schema::SchemaField> = Vec::new();
        for name in
            crate::core::rql::lexer_query::extract_where_columns(&relation.predicate, schema)
        {
            if let Some(f) = schema.iter().find(|f| f.name == name) {
                predicate_fields.push(f.clone());
            }
        }
        let query_fetch = crate::core::rql::join::fetch_for(&predicate_fields, schema);
        let schema = schema.to_vec();

        // No limit is pushed down: it applies to the joined output, and a cap
        // here would drop rows that the join still needs.
        let set = match &source {
            crate::core::database::Relation::Plain(table) => {
                let visibility = match table.read_visibility_for(tx) {
                    crate::core::mvcc::Visibility::All => crate::core::mvcc::Visibility::All,
                    own => statement_snapshot.get_or_insert(own).clone(),
                };
                if legacy_rows {
                    table
                        .query_row_with_visibility(
                            &relation.predicate,
                            &schema,
                            query_fetch,
                            requested,
                            None,
                            None,
                            visibility,
                        )
                        .await
                        .map(crate::core::rql::join::RowSet::Rows)
                        .map_err(|e| e.to_string())
                } else {
                    table
                        .query_row_bytes_with_visibility(
                            &relation.predicate,
                            &schema,
                            query_fetch,
                            requested,
                            None,
                            None,
                            visibility,
                            true,
                        )
                        .await
                        .map_err(|e| e.to_string())
                        .and_then(crate::core::rql::join::RowSet::from_serialized)
                }
            }
            // Its partitions, under the statement's one snapshot (S19, I7):
            // the join's, handed to the fan-out, never one the fan-out takes
            // for itself, so a second partitioned relation, or a self-join's
            // other side, reads under the same one. Routed on this relation's
            // own pushed-down predicate, the exact text its partitions' scans
            // evaluate (I3); a join term naming another relation routes
            // nothing.
            crate::core::database::Relation::Partitioned(pt) => {
                let visibility =
                    statement_snapshot.get_or_insert_with(|| pt.read_visibility_for(tx)).clone();
                match crate::core::rql::row_source::RowSource::partitioned_with(
                    pt.clone(),
                    &relation.predicate,
                    visibility,
                ) {
                    Err(e) => Err(e.to_string()),
                    Ok(source) if legacy_rows => source
                        .query_rows(&relation.predicate, &schema, query_fetch, requested, None, None, tx)
                        .await
                        .map(crate::core::rql::join::RowSet::Rows)
                        .map_err(|e| e.to_string()),
                    Ok(source) => source
                        .query_bytes(&relation.predicate, &schema, query_fetch, requested, None, None, tx)
                        .await
                        .map_err(|e| e.to_string())
                        .and_then(crate::core::rql::join::RowSet::from_serialized),
                }
            }
        };
        match set {
            Ok(set) => scanned.push(set),
            Err(e) => {
                error!("Failed to scan '{}' for join: {}", relation.table, e);
                return error(&e);
            }
        }
    }

    let combined = match crate::core::rql::join::combine(&plan, &scanned) {
        Ok(rows) => rows,
        Err(e) => {
            error!("Cannot execute join: {}", e);
            return error(&e);
        }
    };
    // A shredded JSON column projected through the join has to be hydrated in
    // the projected rows (below), which needs them decoded. Everything else is
    // serialized straight from the tuples, or folded straight from them.
    // Only an item that reads a column: a computed one's field is named after
    // its alias, which may match a shredded column it never read.
    let has_shadow_json = plan.items.iter().any(|item| {
        item.reads_column()
            && plan.relations.get(item.rel).is_some_and(|relation| {
                database.schema_view(&relation.table).is_some_and(|v| {
                    crate::core::rql::json_map::shadow_for(&item.field.name, &v.schema().json_columns)
                        .is_some()
                })
            })
    });
    if !has_shadow_json && !legacy_rows {
        return match plan.aggregate.as_ref() {
            Some(agg) => {
                let mut result = vec![2u8];
                match crate::core::rql::join::fold_joined(&plan, agg, &combined, &mut result) {
                Ok(()) => result,
                Err(e) => {
                    error!("Aggregate over join failed: {}", e);
                    error(&e)
                }
                }
            }
            None => {
                let mut result = vec![2u8];
                match crate::core::rql::join::project_serialized(&plan, &combined, &mut result) {
                    Ok(()) => result,
                    Err(e) => {
                        error!("Join projection failed: {}", e);
                        error(&e)
                    }
                }
            }
        };
    }
    let mut rows = match crate::core::rql::join::project(&plan, combined) {
        Ok(rows) => rows,
        Err(e) => {
            error!("Join projection failed: {}", e);
            return error(&e);
        }
    };

    // A shredded JSON column projected through a join is still the stored
    // UUID, and nothing downstream of here would replace it: the join path
    // builds its own output columns rather than going through the single-table
    // projection that hydrates. So a document read through a join arrived as a
    // uuid under a column described as `jsonb` — the shape the reporting
    // application uses almost everywhere.
    //
    // Positional, because `project` has already made it so: output column `i`
    // is `plan.items[i]`. Done before the fold for the same reason the
    // single-table path does it — grouping must be on the document, not on a
    // reference that is unique per row.
    for (i, item) in plan.items.iter().enumerate() {
        if !item.reads_column() {
            continue;
        }
        let Some(relation) = plan.relations.get(item.rel) else {
            continue;
        };
        let Some(shadow_table) = database.schema_view(&relation.table).and_then(|v| {
            crate::core::rql::json_map::shadow_for(&item.field.name, &v.schema().json_columns)
                .map(|s| s.to_string())
        }) else {
            continue;
        };
        for row in rows.iter_mut() {
            let Some(cell) = row.columns.get_mut(i) else {
                continue;
            };
            if cell.column_type == crate::core::db_type::DbType::NULL
                || cell.data.into_slice().is_empty()
            {
                cell.column_type = crate::core::db_type::DbType::NULL;
                continue;
            }
            let reference = crate::core::value_codec::format_uuid(cell.data.into_slice());
            // An unmatched outer-join row, or a NULL document: either way the
            // column is NULL rather than an empty object.
            let Some(document) = Box::pin(load_json_document(
                &database,
                &shadow_table,
                &reference,
                0,
                tx,
            ))
            .await
            else {
                cell.data = crate::memory_pool::MEMORY_POOL.acquire(0);
                cell.column_type = crate::core::db_type::DbType::NULL;
                continue;
            };
            let rendered = document.to_string();
            let bytes = rendered.as_bytes();
            let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(bytes);
            cell.data = mb;
            // The rebuilt document is `JSON`, not `STRING`: the bytes are the
            // document text either way, but the type is what carries its
            // JSON-ness to anything downstream that has no schema to consult.
            // Marking it `STRING` lost that at exactly one place — materialising
            // a derived table samples the returned rows to pick the temp
            // relation's column types, so `(SELECT doc FROM t) AS sub` created
            // `sub.doc` as TEXT. The outer select then described the column from
            // the temp relation (text) while the recursive describe said jsonb,
            // and the document went out with no version byte: "unsupported JSONB
            // format version 123", 123 being `{`.
            cell.column_type = crate::core::db_type::DbType::JSON;
        }
    }
    let rows = rows;

    // An aggregate over a join folds the *projected* rows, whose columns are
    // positional — which is what the plan's synthetic fields address.
    let rows = match plan.aggregate.as_ref() {
        Some(agg) => match crate::core::rql::aggregate::fold(agg, &rows) {
            Ok(folded) => folded,
            Err(e) => {
                error!("Aggregate over join failed: {}", e);
                return error(&e);
            }
        },
        None => rows,
    };

    let mut result = vec![2u8];
    for row in &rows {
        row.extend_serialized(&mut result);
    }
    result
}

/// Runs one subquery and returns its rows.
async fn run_subquery(
    sql: &str,
    database: &Arc<Database>,
    tx: TxId,
) -> Result<Vec<crate::core::row::row::Row>, String> {
    run_subquery_for(sql, database, tx, SubqueryNeed::All).await
}

/// [`run_subquery`] for a caller that needs only part of the result — see
/// [`SubqueryNeed`]. The rows returned are then only good for that question.
async fn run_subquery_for(
    sql: &str,
    database: &Arc<Database>,
    tx: TxId,
    need: SubqueryNeed,
) -> Result<Vec<crate::core::row::row::Row>, String> {
    run_subquery_scaled(sql, database, tx, need, true).await.map(|(rows, _)| rows)
}

/// [`run_subquery_for`], also returning the scale each output column's
/// `DECIMAL` cells are stored at, when it is known. A materialised relation
/// declares its columns at those scales, so `NUMERIC(12,2)` read through a
/// CTE still prints two places.
///
/// With `normalize` false the cells are left at those scales rather than
/// brought to the engine's, for a caller that writes each one at its own:
/// rounding an `AVG`'s sixteen places to nine and storing the result at
/// sixteen again read 114.9166666666666667 back as 114.9166666670000000.
async fn run_subquery_scaled(
    sql: &str,
    database: &Arc<Database>,
    tx: TxId,
    need: SubqueryNeed,
    normalize: bool,
) -> Result<(Vec<crate::core::row::row::Row>, Option<Vec<u32>>), String> {
    // The inner query is executed exactly once, with no outer row in scope. A
    // correlated subquery would reference an outer column, which the inner
    // table's schema does not have, so it fails here by name rather than being
    // silently evaluated against the wrong relation.
    // A statement's constants are folded once, at the wire entry, before
    // anything parses the predicate. An inner statement never passes through
    // there: `WHERE next_attempt_at <= CURRENT_TIMESTAMP` inside a derived
    // table or a CTE still reached the tokenizer as an unknown column, and even
    // `now()` did, while both worked in the identical outer predicate.
    let folded = fold_where_constants_checked(sql)?;
    let sql = folded.as_str();

    // A subquery inside this one is resolved here, before the statement is
    // classified, rather than left to the executor. The scales below are read
    // from the parsed select list, and the parser cannot read a predicate
    // holding `IN (SELECT …)`: no scales came back, nothing was normalised, and
    // a `NUMERIC(12,2)` cell of 339.50 was read at the engine scale as
    // 0.000033950 by every CTE and derived table whose body had one. The
    // top-level path in pgwire substitutes first for the same reason. A
    // `UNION` keeps its own path, which runs each branch through here.
    let substituted;
    let sql = if crate::core::rql::join::split_union(sql).is_none()
        && (crate::core::rql::subquery::find_in_subquery(sql).is_some()
            || crate::core::rql::subquery::find_exists(sql).is_some()
            || crate::core::rql::subquery::find_scalar_subquery(sql).is_some())
    {
        substituted = Box::pin(substitute_in_subqueries(sql, database, tx)).await?;
        substituted.as_str()
    } else {
        sql
    };

    let purpose =
        recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse subquery {}", sql_literal(sql)))?;
    if !matches!(purpose, QueryPurpose::QueryRows(_)) {
        return Err("only a SELECT may appear as a subquery".to_string());
    }

    // Runs in the caller's transaction. Under `execute` it opened its own, so a
    // subquery could not see rows the statement had just written — a CTE
    // materialised into a temporary table read back as empty, and an empty
    // scalar subquery becomes `''`, which then failed to compare against a
    // typed column.
    let response = Box::pin(execute_with_need(purpose, database.clone(), 0, tx, need)).await;
    match response.first() {
        Some(2) => {}
        Some(3) => {
            let message = String::from_utf8_lossy(&response[1..]).to_string();
            return Err(format!("subquery failed: {}", message));
        }
        _ => return Err("subquery returned no result set".to_string()),
    }

    let mut rows = crate::core::row::row::vec_into_rows(&response[1..])
        .map_err(|e| format!("subquery result could not be read: {}", e))?;
    let scales = recognize_query_purpose(sql)
        .and_then(|purpose| query_output_scales(&purpose, database));
    if let Some(scales) = scales.as_ref().filter(|_| normalize) {
        normalize_decimal_scales(&mut rows, scales);
    }
    Ok((rows, scales))
}

/// The scale each output column's `DECIMAL` bytes are stored at, slot for
/// slot, for a `SELECT` — what a reader of the result must interpret them at.
///
/// One source for both readers: pgwire renders the client's rows by it, and
/// [`run_subquery_for`] uses it to bring a subquery's rows to the engine scale
/// before they become literals or a derived table's rows. They used to differ —
/// pgwire knew a `NUMERIC(10,2)` column was at two places, the subquery path
/// formatted every decimal at nine — so `(SELECT price …)` read 1.25 as
/// 0.000000125.
pub(crate) fn query_output_scales(
    purpose: &QueryPurpose,
    database: &Database,
) -> Option<Vec<u32>> {
    fn field_of(
        fields: &[crate::core::row::schema::SchemaField],
        schema_id: u64,
    ) -> Option<&crate::core::row::schema::SchemaField> {
        fields
            .iter()
            .find(|f| f.write_order == schema_id)
            .or_else(|| fields.get(schema_id as usize))
    }
    match purpose {
        // A join has no single table to take scales from; the plan knows what
        // each output column is (`JoinPlan::output_scales`). Without this arm a
        // join rendered every `NUMERIC(p, s)` at the engine scale.
        QueryPurpose::QueryRows(qr) if crate::core::rql::join::statement_has_join(&qr.query) => {
            let schema_of = |name: &str| database.schema_view(name).map(|v| v.schema().fields.clone());
            crate::core::rql::join::plan_statement(&qr.query, &schema_of)
                .ok()
                .map(|plan| plan.output_scales())
        }
        QueryPurpose::QueryRows(qr) => database.schema_view(&qr.table_name).and_then(|v| {
            let schema = v.schema();
            // Not skipped when the table declares no `NUMERIC(p, s)`: an
            // aggregate's scale is not the column's — `sum(int8)` is a numeric
            // at scale 0, `avg(int)` at sixteen places.
            crate::core::rql::lexer_query::row_fetch_from_select_query(purpose, &schema.fields)
                .ok()
                .map(|parsed| {
                    // Taken from the select list when there is one, slot for
                    // slot. The fetched columns are the *inputs*: `v + 1, id, v`
                    // fetches `v` and `id`, so reading scales off them gave the
                    // third output the scale of `id`'s slot.
                    use crate::core::rql::aggregate::{AggFunc, SelectItem};
                    let engine = crate::core::value_codec::DECIMAL_SCALE;
                    let declared = |name: &str| {
                        crate::core::rql::type_modifier::scale_of(name, &schema.type_modifiers) as u32
                    };
                    // A column read straight through keeps the scale it is
                    // stored at, and so do the aggregates that fold its raw
                    // integers. Anything computed is evaluated at the engine
                    // scale — `expr::eval` lifts every column it reads.
                    let item_scale = |item: &SelectItem| match item {
                        SelectItem::Column { field, .. } => declared(&field.name),
                        // The fold's own rule, `AggFunc::decimal_output_scale`:
                        // `SUM`/`MIN`/`MAX` at the argument's scale (0 for an
                        // integer), `AVG` at sixteen places or the column's.
                        // A computed argument is evaluated at the engine scale,
                        // which its synthetic field already says.
                        SelectItem::Aggregate {
                            func,
                            arg,
                            arg_expr,
                            ..
                        } => {
                            let arg = arg.as_ref().map(|f| {
                                let mut f = f.clone();
                                if arg_expr.is_none()
                                    && f.db_type == crate::core::db_type::DbType::DECIMAL
                                {
                                    f.scale = Some(declared(&f.name));
                                }
                                f
                            });
                            func.decimal_output_scale(arg.as_ref())
                        }
                        // A window value is stored at the grouped aggregate's
                        // scale for the same function — one rule, in
                        // `WindowSpec::decimal_output_scale`.
                        SelectItem::Window { spec, .. } => spec.decimal_output_scale(),
                        _ => engine,
                    };
                    if let Some(plan) = &parsed.aggregate {
                        return plan.items.iter().map(item_scale).collect::<Vec<_>>();
                    }
                    if !parsed.select_items.is_empty() {
                        // A row expression with a known display scale is sent
                        // at it — the select-list slot rounds the value there
                        // (`expr::at_display_scale`) — so `v * 2` over a
                        // `NUMERIC(10,2)` reads 2.20 as PostgreSQL's does,
                        // not 2.200000000. Only on this path: an aggregate
                        // plan's expression items are still at the engine
                        // scale.
                        return parsed
                            .select_items
                            .iter()
                            .map(|item| match item {
                                SelectItem::RowExpr { expr, .. } => {
                                    crate::core::rql::expr::display_scale(expr).unwrap_or(engine)
                                }
                                other => item_scale(other),
                            })
                            .collect::<Vec<_>>();
                    }
                    parsed
                        .requested_row_fetch
                        .columns_fetching_data
                        .iter()
                        .take(parsed.visible_columns)
                        .map(|c| {
                            field_of(&schema.fields, c.schema_id).map_or(0, |field| {
                                crate::core::rql::type_modifier::scale_of(
                                    &field.name,
                                    &schema.type_modifiers,
                                ) as u32
                            })
                        })
                        .collect::<Vec<_>>()
                })
        }),
        _ => None,
    }
}

/// The type of each output column of a single-table `SELECT`, slot for slot,
/// as its select list says — an expression's, an aggregate's or a constant's
/// as well as a column's. `None` where the statement is not one this reads
/// (a join, a `SELECT` with no `FROM`). [`query_output_scales`]'s companion:
/// a `UNION` resolves each column's type from both (`set_ops::
/// union_decimal_scales`).
pub(crate) fn query_output_types(
    purpose: &QueryPurpose,
    database: &Database,
    constants_as_numeric: bool,
) -> Option<Vec<crate::core::db_type::DbType>> {
    let QueryPurpose::QueryRows(qr) = purpose else { return None };
    if crate::core::rql::join::statement_has_join(&qr.query) {
        // A join's, from its plan, as `query_output_scales` takes its scales:
        // refused unread, `SELECT nn.c FROM nn JOIN mm … UNION SELECT c FROM
        // mm` was refused where base answered it.
        let schema_of = |name: &str| database.schema_view(name).map(|v| v.schema().fields.clone());
        let plan = crate::core::rql::join::plan_statement(&qr.query, &schema_of).ok()?;
        return Some(
            plan.output_types()
                .into_iter()
                .map(|(t, constant)| match t {
                    crate::core::db_type::DbType::F64 if constant && constants_as_numeric => {
                        crate::core::db_type::DbType::DECIMAL
                    }
                    other => other,
                })
                .collect(),
        );
    }
    let view = database.schema_view(&qr.table_name)?;
    let parsed = crate::core::rql::lexer_query::row_fetch_from_select_query(purpose, &view.schema().fields).ok()?;
    if let Some(plan) = &parsed.aggregate {
        return Some(plan.items.iter().map(|i| i.result_type()).collect());
    }
    if !parsed.select_items.is_empty() {
        // A `NULL` constant is reported as NULL, not as the text it is
        // described as: it fits a column of any type.
        return Some(
            parsed
                .select_items
                .iter()
                .map(|i| match i {
                    crate::core::rql::aggregate::SelectItem::Literal {
                        db_type: crate::core::db_type::DbType::NULL,
                        ..
                    } => crate::core::db_type::DbType::NULL,
                    // A written numeric constant (`SELECT 1.25`) is carried
                    // as a float but is PostgreSQL's `numeric`: a UNION
                    // column takes it as one (`set_ops::float_decimal`).
                    crate::core::rql::aggregate::SelectItem::Literal {
                        db_type: crate::core::db_type::DbType::F64,
                        ..
                    } if constants_as_numeric => crate::core::db_type::DbType::DECIMAL,
                    other => other.result_type(),
                })
                .collect(),
        );
    }
    None
}

/// Brings every `DECIMAL` cell of a subquery's rows to the engine scale, from
/// the scale [`query_output_scales`] says it is stored at.
///
/// Rows leaving one query for another carry no scale, and everything that
/// reads them — literal substitution, a derived table's temp relation —
/// assumes the engine's. Without this a `sum(bigint)` (a numeric at scale 0)
/// read through a derived table was a billionth of itself, and so was any
/// `NUMERIC(p, s)` column with `s` other than nine. A value with digits past
/// the engine's nine places is left whole, as a cell carrying its own scale,
/// for its reader to write at that scale; one without is brought to nine.
fn normalize_decimal_scales(rows: &mut [crate::core::row::row::Row], scales: &[u32]) {
    use crate::core::db_type::DbType;
    let target = crate::core::value_codec::DECIMAL_SCALE;
    // No early return when every column is at the engine scale: a cell that
    // carries its own scale still has to be normalised, and that is a
    // property of the cell, not of the column list.
    for row in rows.iter_mut() {
        for (i, cell) in row.columns.iter_mut().enumerate() {
            let Some(&from) = scales.get(i) else { break };
            if cell.column_type != DbType::DECIMAL {
                continue;
            }
            // A value carrying its own scale (an `AVG` result, see
            // `value_codec::decimal_cell_with_scale`) is brought to the engine
            // scale like any other, from *its* scale, and becomes an ordinary
            // sixteen-byte cell: what leaves a query for another is uniform.
            let (from, v) = match crate::core::value_codec::split_decimal_cell(cell.data.into_slice()) {
                // More places than the engine's, with digits there (`a + 1`
                // over a NUMERIC(20,12)): kept whole. Rounded to nine here,
                // `INSERT INTO t2 SELECT id, a + 1 FROM w` stored
                // 1.123456789000; a reader of the cell writes it at its scale.
                Some((v, Some(own))) if own > target && v % 10i128.pow(own - target) != 0 => continue,
                Some((v, Some(own))) => {
                    let lifted = if own <= target {
                        10i128.checked_pow(target - own).and_then(|f| v.checked_mul(f))
                    } else {
                        10i128.checked_pow(own - target).map(|f| {
                            let q = v / f;
                            let r = v % f;
                            if r.unsigned_abs() * 2 >= f as u128 {
                                if v < 0 { q - 1 } else { q + 1 }
                            } else {
                                q
                            }
                        })
                    };
                    if let Some(lifted) = lifted {
                        let mut block = crate::memory_pool::MEMORY_POOL.acquire(16);
                        block.into_slice_mut().copy_from_slice(&lifted.to_le_bytes());
                        cell.data = block;
                    }
                    continue;
                }
                Some((v, None)) => (from, v),
                None => continue,
            };
            if from == target {
                continue;
            }
            // A column with more places than the engine's, holding digits
            // there: kept whole, at its scale, as the cell carrying its own
            // scale above is. Rounded to nine here, `INSERT INTO t2 SELECT b
            // FROM t` over a NUMERIC(38,20) stored 1.12345678900000000000
            // for 1.12345678912345678912 — a plain copy, a join, a derived
            // table, a CTE, `min(b)`, every source whose cells are the
            // column's own.
            if from > target && 10i128.checked_pow(from - target).is_some_and(|f| v % f != 0) {
                let bytes = crate::core::value_codec::decimal_cell_with_scale(v, from);
                let mut block = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
                block.into_slice_mut().copy_from_slice(&bytes);
                cell.data = block;
                continue;
            }
            let lifted = if from < target {
                10i128.checked_pow(target - from).and_then(|f| v.checked_mul(f))
            } else {
                10i128.checked_pow(from - target).map(|f| {
                    let q = v / f;
                    let r = v % f;
                    if r.unsigned_abs() * 2 >= f as u128 {
                        if v < 0 { q - 1 } else { q + 1 }
                    } else {
                        q
                    }
                })
            };
            // A value the engine scale cannot hold is left as it was rather
            // than replaced by a different number.
            if let Some(lifted) = lifted {
                cell.data.into_slice_mut().copy_from_slice(&lifted.to_le_bytes());
            }
        }
    }
}

/// [`run_subquery`], returning the rows still serialized: the response with
/// its result tag stripped. For a caller that only copies or compares rows.
async fn run_subquery_serialized(
    sql: &str,
    database: &Arc<Database>,
    tx: TxId,
) -> Result<Vec<u8>, String> {
    let folded = fold_where_constants_checked(sql)?;
    let sql = folded.as_str();
    let purpose =
        recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse subquery {}", sql_literal(sql)))?;
    if !matches!(purpose, QueryPurpose::QueryRows(_)) {
        return Err("only a SELECT may appear as a subquery".to_string());
    }
    let mut response =
        Box::pin(execute_with_need(purpose, database.clone(), 0, tx, SubqueryNeed::All)).await;
    match response.first() {
        Some(2) => {
            response.remove(0);
            Ok(response)
        }
        Some(3) => Err(format!(
            "subquery failed: {}",
            String::from_utf8_lossy(&response[1..])
        )),
        _ => Err("subquery returned no result set".to_string()),
    }
}

/// Runs a `SELECT` and returns its rows, with its `DECIMAL` cells left at the
/// scale each output column stores them at, and those scales — see
/// [`run_subquery_scaled`].
///
/// Shares `run_subquery`'s contract — the statement must be a `SELECT`, and it
/// is executed with no outer row in scope — and exists so materialising a CTE
/// does not need a second way to run a query.
pub(crate) async fn run_query_rows_scaled(
    sql: &str,
    database: &Arc<Database>,
    tx: TxId,
) -> Result<(Vec<crate::core::row::row::Row>, Option<Vec<u32>>), String> {
    run_subquery_scaled(sql, database, tx, SubqueryNeed::All, false).await
}

/// Runs a data-modifying statement and returns the rows its `RETURNING` clause
/// produced.
///
/// A `WITH x AS (INSERT … RETURNING id)` runs its body for the effect *and*
/// reads the result, so neither `run_query_rows_scaled` (which refuses anything but a
/// SELECT) nor `run_statement` (which discards rows) fits. A statement that
/// succeeds without returning rows yields an empty vector rather than an error.
pub(crate) async fn run_returning_rows(
    sql: &str,
    database: &Arc<Database>,
    tx: TxId,
) -> Result<Vec<crate::core::row::row::Row>, String> {
    crate::owner_probe!(crate::core::mvcc::owner::probe::Site::ReturningBody, tx, database);
    let purpose = recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {}", sql_literal(sql)))?;
    // Runs inside the caller's transaction rather than opening its own. A
    // data-modifying CTE is part of one statement, so committing it separately
    // would leave its writes behind when the outer statement fails — or when
    // the block it belongs to is rolled back. Under `FROZEN_TX` it opens an
    // autocommit attempt, which joins this statement's owner O rather than
    // beginning a second one (§7: one owner per statement).
    let response = if tx == crate::core::mvcc::transaction::FROZEN_TX {
        Box::pin(execute(purpose, database.clone())).await
    } else {
        Box::pin(execute_in_txn(purpose, database.clone(), tx)).await
    };
    match response.first() {
        Some(2) => crate::core::row::row::vec_into_rows(&response[1..])
            .map_err(|e| format!("could not read the RETURNING rows: {}", e)),
        Some(3) => Err(String::from_utf8_lossy(&response[1..]).to_string()),
        _ => Ok(Vec::new()),
    }
}

/// Runs a statement for its effect and reports how many rows it affected.
///
/// Used for a data-modifying CTE whose `RETURNING` list is constants: the
/// statement runs without the clause, and its row count is what decides how
/// many constant rows the CTE publishes.
pub(crate) async fn run_statement_affected(
    sql: &str,
    database: &Arc<Database>,
    tx: TxId,
) -> Result<u64, String> {
    let purpose = recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {}", sql_literal(sql)))?;
    let response = Box::pin(execute_with_context(purpose, database.clone(), 0, tx)).await;
    match response.first() {
        Some(3) => Err(String::from_utf8_lossy(&response[1..]).to_string()),
        Some(0) if response.len() >= 9 => {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&response[1..9]);
            Ok(u64::from_le_bytes(arr))
        }
        Some(2) => crate::core::row::row::vec_into_rows(&response[1..])
            .map(|rows| rows.len() as u64)
            .map_err(|e| format!("could not read the result: {}", e)),
        _ => Ok(0),
    }
}

/// Runs a statement for its effect, reporting the engine's own error.
///
/// Used to create, fill and drop the temporary tables a CTE materialises into.
pub(crate) async fn run_statement(
    sql: &str,
    database: &Arc<Database>,
    tx: TxId,
) -> Result<(), String> {
    let purpose = recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse {}", sql_literal(sql)))?;
    // Runs inside the caller's transaction. Opening a fresh one made a CTE's
    // writes a *different* transaction from the statement they belong to, which
    // then collided with it — "could not serialize access due to concurrent
    // update", from a statement racing only itself.
    let response = Box::pin(execute_with_context(purpose, database.clone(), 0, tx)).await;
    match response.first() {
        Some(3) => Err(String::from_utf8_lossy(&response[1..]).to_string()),
        _ => Ok(()),
    }
}

/// The relation names and aliases the outer `FROM` clause introduces, used to
/// tell an outer column reference from an inner one inside an `EXISTS`.
fn outer_relation_names(sql: &str) -> Vec<String> {
    // A schema-qualified relation (`public.t`, `"public"."t"`) is named by
    // its last part too, which is how its columns are qualified (`t.id`).
    let mut names = outer_relation_names_as_written(sql);
    let mut parts: Vec<String> = Vec::new();
    for name in &names {
        if name.contains('.') {
            if let Some(last) = name.rsplit('.').next() {
                parts.push(last.trim_matches('"').to_string());
            }
        }
    }
    for n in names.iter_mut() {
        *n = n.split('.').map(|p| p.trim_matches('"')).collect::<Vec<_>>().join(".");
    }
    names.extend(parts);
    names
}

fn outer_relation_names_as_written(sql: &str) -> Vec<String> {
    use crate::core::rql::join::find_keyword;

    // `UPDATE t [AS] x SET …` names its relation before SET and has no FROM:
    // read as having none, a correlated EXISTS ran with no outer row and was
    // refused 42P01 (Django writes `UPDATE … WHERE EXISTS (…)`).
    // `UPDATE t … FROM u` names both: the target too, or an EXISTS
    // correlating with it ran with no outer row and was refused 42P01.
    let trimmed = sql.trim_start();
    let mut names = Vec::new();
    if trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("UPDATE") {
        let Some(set) = find_keyword(sql, "SET", 0) else { return Vec::new() };
        let head = sql[..set].trim();
        let mut words = head.split_whitespace().skip(1);
        if let Some(table) = words.next() {
            names.push(table.trim_matches('"').to_string());
        }
        match words.next() {
            Some(w) if w.eq_ignore_ascii_case("AS") => names.extend(words.next().map(|a| a.trim_matches('"').to_string())),
            Some(a) => names.push(a.trim_matches('"').to_string()),
            None => {}
        }
        if find_keyword(sql, "FROM", 0).is_none() {
            return names;
        }
    }

    let Some(from) = find_keyword(sql, "FROM", 0) else {
        return names;
    };
    let start = from + "FROM".len();
    let end = ["WHERE", "ORDER", "LIMIT", "OFFSET", "GROUP", "HAVING"]
        .iter()
        .filter_map(|kw| find_keyword(sql, kw, start))
        .min()
        .unwrap_or(sql.len());

    for segment in sql[start..end].split(|c| c == ',') {
        let mut words = segment.split_whitespace().peekable();
        while let Some(word) = words.next() {
            let upper_word = word.to_ascii_uppercase();
            if matches!(
                upper_word.as_str(),
                "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "OUTER" | "ON"
            ) {
                continue;
            }
            if upper_word == "AS" {
                continue;
            }
            names.push(word.trim_matches('"').to_string());
        }
    }
    names
}

/// Whether the `[NOT] EXISTS (…)` at `start..end` of `sql` is a value
/// rather than a condition: cast (`::`) or an operand of arithmetic.
fn exists_used_as_value(sql: &str, start: usize, end: usize) -> bool {
    let after = sql[end..].trim_start();
    let before = sql[..start].trim_end();
    after.starts_with("::")
        || matches!(after.as_bytes().first(), Some(b'+' | b'-' | b'*' | b'/' | b'%' | b'|'))
        || matches!(before.as_bytes().last(), Some(b'+' | b'-' | b'*' | b'/' | b'%' | b'|'))
}

/// The first `[NOT] EXISTS (…)` of `sql` that is not inside another
/// parenthesised `SELECT`.
fn top_level_exists(sql: &str) -> Option<crate::core::rql::subquery::ExistsClause> {
    let mut from = 0;
    loop {
        let mut clause = crate::core::rql::subquery::find_exists(&sql[from..])?;
        clause.start += from;
        clause.end += from;
        if !inside_a_subquery(sql, clause.start) {
            return Some(clause);
        }
        from = clause.end.max(from + 1);
    }
}

/// Whether `pos` lies inside a parenthesised `SELECT` or `WITH` of `sql`.
fn inside_a_subquery(sql: &str, pos: usize) -> bool {
    let bytes = sql.as_bytes();
    let mut stack: Vec<bool> = Vec::new();
    let mut i = 0;
    while i < pos.min(bytes.len()) {
        match bytes[i] {
            b'\'' => {
                i = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
                continue;
            }
            b'(' => {
                let rest = sql[i + 1..].trim_start();
                let head = rest.get(..6).map(str::to_ascii_uppercase).unwrap_or_default();
                stack.push(head.starts_with("SELECT") || head.starts_with("WITH"));
            }
            b')' => {
                stack.pop();
            }
            _ => {}
        }
        i += 1;
    }
    stack.iter().any(|s| *s)
}

/// Replaces every `IN (SELECT …)` with the literal list the inner query
/// produces, so the outer statement can then be planned as an ordinary one.
///
/// Bounded to a handful of substitutions: each pass runs a full inner query, and
/// a statement needing more than a few is better refused than executed slowly.
pub async fn substitute_in_subqueries(
    sql: &str,
    database: &Arc<Database>,
    tx: TxId,
) -> Result<String, String> {
    use crate::core::rql::subquery;

    crate::owner_probe!(crate::core::mvcc::owner::probe::Site::Subquery, tx, database);
    const MAX_SUBQUERIES: usize = 8;

    let mut current = sql.to_string();
    let write_statement = {
        let head = sql.trim_start();
        let word = head.get(..6).map(str::to_ascii_uppercase).unwrap_or_default();
        word == "UPDATE" || word == "DELETE"
    };

    // `EXISTS` is rewritten into the equivalent `IN` first, so the loop below
    // resolves both forms with one mechanism.
    for _ in 0..MAX_SUBQUERIES {
        // Only an EXISTS of this statement's own: one inside another
        // subquery (`id IN (SELECT t0.id FROM t AS t0 WHERE EXISTS (… t0.id))`,
        // Django's DELETE) correlates with that subquery's relation, and is
        // resolved when that subquery is run, below.
        let Some(clause) = top_level_exists(&current) else {
            break;
        };
        let outer = outer_relation_names(&current);

        // `ORDER BY` / `LIMIT` / `OFFSET` say nothing about *which* rows match,
        // so they are lifted off before anything reads the predicate — see
        // `strip_exists_tail` for what leaving them in place did.
        let (clause, bounds) = subquery::strip_exists_tail(&clause)?;
        if bounds.limit == Some(0) {
            // `LIMIT 0` returns nothing, for every outer row.
            let predicate = subquery::uncorrelated_exists_predicate(clause.negated, false);
            current = format!(
                "{}{}{}",
                &current[..clause.start],
                predicate,
                &current[clause.end..]
            );
            continue;
        }

        // An EXISTS correlating on several columns cannot become an `IN`, which
        // carries one column. Its rows are fetched once and expanded into a
        // tuple-wise predicate instead — exact, where a per-column `IN` would
        // match pairs that do not exist.
        // An EXISTS referencing no outer column asks one question of the
        // database, not one per row: run it once and fold the answer into a
        // constant the scan can evaluate.
        if let Some(inner) = subquery::uncorrelated_exists(&clause, &outer) {
            let rows = if bounds.offset > 0 {
                // `OFFSET k` asks whether there are more than k rows. Run as
                // written, less its ORDER BY, which cannot change the count.
                let sql = format!("{} LIMIT 1 OFFSET {}", inner, bounds.offset);
                run_subquery(&sql, database, tx).await?
            } else {
                // Only whether a row exists matters, so the scan may stop at
                // the first one rather than producing and encoding them all.
                run_subquery_for(&inner, database, tx, SubqueryNeed::Existence).await?
            };
            let predicate =
                subquery::uncorrelated_exists_predicate(clause.negated, !rows.is_empty());
            current = format!(
                "{}{}{}",
                &current[..clause.start],
                predicate,
                &current[clause.end..]
            );
            continue;
        }

        // As a value — under arithmetic or a cast (`x - EXISTS (…)::int`) —
        // its `IN` form, a predicate, came out as text no reader takes (XX000,
        // "is neither a column nor a value").
        if exists_used_as_value(&current, clause.start, clause.end) {
            return Err(
                "a correlated EXISTS used as a value (under arithmetic or a cast) is not supported; \
                 use it as a condition, or write a CASE over it"
                    .to_string(),
            );
        }
        if bounds.offset > 0 {
            // "More than k matching rows per outer row" has no `IN` form.
            return Err(
                "EXISTS with an OFFSET in a correlated subquery is not supported".to_string(),
            );
        }

        if let Some(spec) = subquery::multi_correlated_exists(&clause, &outer) {
            let spec = spec?;
            let rows = run_subquery(&spec.inner_sql, database, tx).await?;
            let exact: Vec<bool> = crate::core::rql::cte::blank_padded_columns(&spec.inner_sql, database)
                .into_iter()
                .map(|padded| !padded)
                .collect();
            let predicate = subquery::multi_exists_predicate(&spec, &rows, &exact)?;
            current = format!(
                "{}{}{}",
                &current[..clause.start],
                predicate,
                &current[clause.end..]
            );
            continue;
        }

        let replacement = subquery::exists_to_in(&clause, &outer)?;
        current = format!(
            "{}{}{}",
            &current[..clause.start],
            replacement,
            &current[clause.end..]
        );
    }

    // A subquery in value position resolves to a constant, which the select
    // list then treats exactly like a written literal.
    for _ in 0..MAX_SUBQUERIES {
        let Some(found) = subquery::find_scalar_subquery(&current) else {
            break;
        };
        // Two rows are enough to raise "more than one row".
        let rows = run_subquery_for(&found.inner_sql, database, tx, SubqueryNeed::Scalar).await?;
        let document = crate::core::rql::cte::document_columns(&found.inner_sql, database)
            .first()
            .copied()
            .unwrap_or(false);
        let exact = !crate::core::rql::cte::blank_padded_columns(&found.inner_sql, database)
            .first()
            .copied()
            .unwrap_or(false);
        let literal = subquery::rows_to_scalar(&rows, document, exact)?;
        current = format!(
            "{}{}{}",
            &current[..found.start],
            literal,
            &current[found.end..]
        );
    }

    for _ in 0..MAX_SUBQUERIES {
        let Some(found) = subquery::find_in_subquery(&current) else {
            return Ok(current);
        };

        // The inner query is executed exactly once, with no outer row in scope.
        // A correlated subquery would reference an outer column, which the
        // inner table's schema does not have, so it fails here by name rather
        // than being silently evaluated against the wrong relation.
        // Membership ignores order, so an unbounded ORDER BY is not run.
        let rows = match run_subquery_for(&found.inner_sql, database, tx, SubqueryNeed::Membership).await {
            Ok(rows) => rows,
            // One that reads the UPDATE's or DELETE's own row, from inside
            // (`id IN (SELECT … WHERE EXISTS (… t.id))`): run with no outer
            // row it named a "missing FROM-clause entry", a fault the
            // statement does not have.
            Err(e) if write_statement
                && outer_relation_names(sql)
                    .iter()
                    .any(|n| e.contains(&crate::core::sql_scan::missing_from_entry(n))) =>
            {
                return Err(
                    "a subquery inside IN that reads the row the UPDATE or DELETE writes (a correlated \
                     subquery) is not supported; write it as UPDATE … FROM / DELETE … USING"
                        .to_string(),
                );
            }
            Err(e) => return Err(e),
        };
        let document = crate::core::rql::cte::document_columns(&found.inner_sql, database)
            .first()
            .copied()
            .unwrap_or(false);
        let exact = !crate::core::rql::cte::blank_padded_columns(&found.inner_sql, database)
            .first()
            .copied()
            .unwrap_or(false);
        let (list, had_null) = subquery::rows_to_value_list(&rows, document, exact)?;

        // `x NOT IN (…, NULL)` is never true: x either equals a value (false)
        // or is compared against NULL (unknown). `x IN ()` expresses exactly
        // that — it never holds, and a NULL x is excluded as well.
        let (start, replacement) = match (had_null, found.negated_start) {
            (true, Some(not_at)) => (not_at, "IN ()".to_string()),
            _ => (found.start, list),
        };
        // A `character(n)` subquery's values meet a `character varying`
        // operand blank-padded, as PostgreSQL's `varchar = bpchar` resolves:
        // its trailing spaces are not part of it. As untyped literals they
        // were compared with it exactly, and `b IN (SELECT k …)` missed 'ab  '.
        let operand = if exact { None } else { varying_in_operand(&current, &found, database) };
        current = format!(
            "{}{}{}",
            &current[..start],
            replacement,
            &current[found.end..]
        );
        if let Some((from, to)) = operand {
            current = format!("{}rtrim({}){}", &current[..from], &current[from..to], &current[to..]);
        }
    }

    Err(format!(
        "more than {} subqueries in one statement is not supported",
        MAX_SUBQUERIES
    ))
}

/// The byte range of the operand of `found`'s `IN` when it is a bare
/// `character varying` column of one of the statement's relations; `None`
/// for anything else (another type, an expression, a column not found).
fn varying_in_operand(
    sql: &str,
    found: &crate::core::rql::subquery::InSubquery,
    database: &Arc<Database>,
) -> Option<(usize, usize)> {
    use crate::core::rql::type_modifier::TextTypmod;
    let mut head = sql[..found.negated_start.unwrap_or(found.start)].trim_end();
    if found.negated_start.is_none() {
        let cut = head.len().checked_sub(2)?;
        if !head.get(cut..)?.eq_ignore_ascii_case("IN") {
            return None;
        }
        head = head[..cut].trim_end();
    }
    let to = head.len();
    let from = head
        .char_indices()
        .rev()
        .take_while(|(_, c)| c.is_alphanumeric() || matches!(c, '_' | '.' | '"'))
        .last()
        .map(|(i, _)| i)?;
    let reference = &head[from..to];
    let (qualifier, column) = match reference.rsplit_once('.') {
        Some((q, c)) => (Some(q.trim_matches('"')), c.trim_matches('"')),
        None => (None, reference.trim_matches('"')),
    };
    let names = outer_relation_names(sql);
    let mut tables: Vec<&String> = names.iter().filter(|n| qualifier.is_none_or(|q| q == n.as_str())).collect();
    if tables.is_empty() {
        tables = names.iter().collect();
    }
    let varying = tables.iter().any(|name| {
        database.schema_view(name).is_some_and(|view| {
            view.schema().fields.iter().any(|f| {
                !f.is_deleted
                    && f.name == column
                    && f.db_type == crate::core::db_type::DbType::STRING
                    && matches!(f.text_typmod(), Some(TextTypmod::Varchar(_)))
            })
        })
    });
    varying.then_some((from, to))
}

/// Depth limit for cascading writes.
///
/// A cycle of `ON DELETE CASCADE` would otherwise recurse without end. The
/// limit is generous next to any real schema, so hitting it means a cycle.
const MAX_CASCADE_DEPTH: usize = 16;

/// A key value moved from one side of a foreign key to the other's column
/// type (P4 round 3), as PostgreSQL compares a cross-type key: `literal`,
/// a value of `from`, read at `from`'s type, written at `to`'s and rendered
/// as `to`'s literal, which the probe of `to`'s table then compares with its
/// column at its own type (so an `INT` 7 finds a `NUMERIC(10,2)` 7.00, and a
/// partitioned parent routes by the bytes the parent row was routed by).
///
/// `None` when the move loses the value (`7.50` to an integer, a
/// non-midnight timestamp to a date, a `DOUBLE` 0.1 to a `REAL`, an `INT8`
/// past an `INT4`): no value of `to` equals it, as PostgreSQL's comparison
/// in the wider type finds none. Unchanged when the two columns' types and
/// scales are one (the common case, no work), and when `literal` does not
/// read at `from`'s type (the probe then reports it as it always did).
fn coerce_key_literal(
    literal: &str,
    from: &crate::core::row::schema::SchemaField,
    from_modifiers: &[crate::core::rql::type_modifier::TypeModifier],
    to: &crate::core::row::schema::SchemaField,
    to_modifiers: &[crate::core::rql::type_modifier::TypeModifier],
) -> Option<String> {
    if from.db_type == to.db_type && from.scale == to.scale {
        return Some(literal.to_string());
    }
    let encode = |text: &str, field: &crate::core::row::schema::SchemaField, mods| {
        let out = crate::core::rql::lexer_ir::encode_column_value(text, field, mods);
        // An encoder's refusal is this coercion's only.
        let _ = crate::core::rql::lexer_ir::take_refusal();
        out.map(|block| block.into_slice().to_vec()).filter(|bytes| !bytes.is_empty())
    };
    let Some(from_bytes) = encode(literal.trim(), from, from_modifiers) else {
        return Some(literal.to_string());
    };
    // REAL and DOUBLE compare as PostgreSQL's cross-type float operators do,
    // in double precision: a REAL widens exactly, and a DOUBLE has a REAL
    // equal to it only when it is one exactly (`0.1` is not: its REAL is
    // 0.100000001490116…). Through text, the shortest renderings (`0.1` both)
    // would call them equal.
    use crate::core::db_type::DbType::{F32, F64};
    match (&from.db_type, &to.db_type, from_bytes.as_slice()) {
        (F32, F64, b) if b.len() == 4 => {
            return Some(f64::from(f32::from_le_bytes(b.try_into().ok()?)).to_string());
        }
        (F64, F32, b) if b.len() == 8 => {
            let wide = f64::from_le_bytes(b.try_into().ok()?);
            let narrow = wide as f32;
            return (f64::from(narrow) == wide).then(|| narrow.to_string());
        }
        _ => {}
    }
    let Ok(from_text) = key_literal(from, &from_bytes) else {
        return Some(literal.to_string());
    };
    // The value as the target type's reader takes it: a number in its plain
    // canonical form (an integer column does not read `7.00`), a date as the
    // date part of a timestamp (whose time the comparison below then checks).
    use crate::core::db_type::DbType as T;
    let input = match &to.db_type {
        T::I8 | T::I16 | T::I32 | T::I64 | T::I128 | T::U8 | T::U16 | T::U32 | T::U64 | T::U128 | T::F32 | T::F64
        | T::DECIMAL => crate::core::rql::foreign_key::canonical_key_text(&from_text, &from.db_type),
        T::DATE => {
            let bare = from_text.trim().trim_matches('\'');
            format!("'{}'", bare.split_whitespace().next().unwrap_or(bare))
        }
        _ => from_text.clone(),
    };
    let to_text = key_literal(to, &encode(&input, to, to_modifiers)?).ok()?;
    let same = crate::core::rql::foreign_key::canonical_key_text(&from_text, &from.db_type)
        == crate::core::rql::foreign_key::canonical_key_text(&to_text, &to.db_type);
    same.then_some(to_text)
}

/// `fk`'s two columns, the child's in `child` and the parent's (its own
/// table's for a self-reference), each with its table's type modifiers:
/// what [`coerce_key_literal`] moves values between. `None` when either is
/// not there (the probe then reports it).
#[allow(clippy::type_complexity)]
fn fk_key_fields(
    database: &Arc<Database>,
    child: &crate::core::row::schema::TableSchema,
    fk: &crate::core::rql::foreign_key::ForeignKey,
) -> Option<(
    (crate::core::row::schema::SchemaField, Vec<crate::core::rql::type_modifier::TypeModifier>),
    (crate::core::row::schema::SchemaField, Vec<crate::core::rql::type_modifier::TypeModifier>),
)> {
    let field = |schema: &crate::core::row::schema::TableSchema, name: &str| {
        schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
            .map(|f| (f.clone(), schema.type_modifiers.clone()))
    };
    let child_side = field(child, &fk.column)?;
    let parent_side = if fk.parent_table.eq_ignore_ascii_case(&child.name) {
        field(child, &fk.parent_column)?
    } else {
        let view = database.schema_view(&fk.parent_table)?;
        field(view.schema(), &fk.parent_column)?
    };
    Some((child_side, parent_side))
}

/// Parent key literals (`parent_column`'s values, as `key_literal` renders
/// them) at `fk`'s child column's type, index for index: `None` where the
/// value cannot equal any child value ([`coerce_key_literal`]). The parent
/// side's probes, actions and checks match children with these.
fn parent_literals_for_child(
    database: &Arc<Database>,
    child_table: &str,
    fk: &crate::core::rql::foreign_key::ForeignKey,
    literals: &[String],
) -> Vec<Option<String>> {
    let fields = database
        .schema_view(child_table)
        .and_then(|view| fk_key_fields(database, view.schema(), fk));
    match fields {
        Some(((child, child_mods), (parent, parent_mods))) => literals
            .iter()
            .map(|l| coerce_key_literal(l, &parent, &parent_mods, &child, &child_mods))
            .collect(),
        None => literals.iter().cloned().map(Some).collect(),
    }
}

/// A key literal as a violation message shows it: the value, unquoted,
/// without the cast a typed value carries (`'zz '::text`, a parameter bound
/// as text, shows `zz `).
fn key_display(literal: &str) -> String {
    let text = literal.trim();
    let quoted = |t: &str| t.starts_with('\'') && crate::core::sql_scan::quoted_end(t.as_bytes(), 0, b'\'') == t.len();
    let body = match text.rfind("::") {
        Some(at) if quoted(text[..at].trim_end()) => text[..at].trim_end(),
        _ => text,
    };
    match body.strip_prefix('\'').and_then(|b| b.strip_suffix('\'')) {
        Some(inner) if quoted(body) => inner.replace("''", "'"),
        _ => body.trim_matches('\'').to_string(),
    }
}

/// Checks that every foreign key on `table_name` is satisfied by `assignments`.
///
/// `assignments` maps a column name to the literal being written. A `NULL` is
/// exempt, as in SQL: a foreign key constrains what a value may be, not whether
/// there is one.
async fn check_foreign_keys(
    database: &Arc<Database>,
    table_name: &str,
    assignments: &[(String, String)],
    tx: TxId,
) -> Result<(), String> {
    // The child is the statement's write target: its RX is held (or taken).
    let table = database.table_for(table_name, owner(), TableMode::RowExclusive).await?;
    check_foreign_keys_of(database, table.as_ref().map(|t| &t.schema), table_name, assignments, tx).await
}

/// [`check_foreign_keys`] for a target the caller already resolved under its
/// ROW EXCLUSIVE (slice B8b): `schema` is the child's, `None` when no table
/// is registered under `table_name`.
async fn check_foreign_keys_of(
    database: &Arc<Database>,
    schema: Option<&crate::core::row::schema::TableSchema>,
    table_name: &str,
    assignments: &[(String, String)],
    tx: TxId,
) -> Result<(), String> {
    crate::owner_probe!(crate::core::mvcc::owner::probe::Site::ForeignKeyCheck, tx, database);
    let foreign_keys = match schema {
        Some(schema) if !schema.foreign_keys.is_empty() => schema.foreign_keys.clone(),
        _ => return Ok(()),
    };

    for fk in foreign_keys.iter() {
        let Some((_, literal)) = assignments
            .iter()
            .find(|(column, _)| column.eq_ignore_ascii_case(&fk.column))
        else {
            continue;
        };
        if literal.trim().eq_ignore_ascii_case("NULL") {
            continue;
        }

        if !database.has_table(&fk.parent_table) {
            return Err(format!(
                "foreign key \"{}\" references relation \"{}\", which does not exist",
                fk.name, fk.parent_table
            ));
        }

        let violation = || {
            format!(
                "insert or update on table \"{}\" violates foreign key constraint \"{}\": \
                 key ({})=({}) is not present in table \"{}\"",
                table_name,
                fk.name,
                fk.column,
                key_display(literal),
                fk.parent_table
            )
        };
        let unchecked =
            |e: String| format!("foreign key \"{}\" could not be checked: {}", fk.name, e);

        // Already found, and key-share locked by this transaction, by the
        // multi-row INSERT this row belongs to. The lock is held to commit, so
        // nothing can have removed the parent since.
        if FK_VERIFIED
            .try_with(|v| {
                v.borrow()
                    .contains(&verified_key(&fk.parent_table, &fk.parent_column, literal))
            })
            .unwrap_or(false)
        {
            continue;
        }

        // Three steps, and the order is the fix. Probing alone — what this
        // used to do — read the parent through this transaction's snapshot and
        // took no lock, so a concurrent DELETE of the parent and this INSERT
        // each saw the other's row still there and both committed: an orphan.
        //
        // 1. Find the parent as of now, for its stored key bytes.
        // 2. Take the key-share lock on them (PostgreSQL's `FOR KEY SHARE`),
        //    held to commit. A DELETE of that key holds the exclusive side
        //    from before it looks at referencing rows, so whichever of the two
        //    comes second waits for the first.
        // 3. Look again under the lock. A DELETE that committed between 1 and
        //    2 released its lock before this one was granted and left nothing
        //    to wait for — only the second look sees that the parent is gone.
        //
        // The value at the parent column's type first (P4 round 3): one that
        // type cannot hold unchanged equals no parent.
        let coerced = match schema.and_then(|child| fk_key_fields(database, child, fk)) {
            Some(((child, child_mods), (parent, parent_mods))) => {
                coerce_key_literal(literal, &child, &child_mods, &parent, &parent_mods)
            }
            None => Some(literal.clone()),
        };
        let Some(probe) = coerced else {
            return Err(violation());
        };
        let literal = &probe;
        let found = referenced_values_now(
            database,
            &fk.parent_table,
            &fk.parent_column,
            std::slice::from_ref(literal),
            tx,
            Some(1),
            KeySide::Parent,
        )
        .await
        .map_err(unchecked)?;
        let Some(key) = found.into_iter().next() else {
            return Err(violation());
        };
        acquire_referenced_waiting(
            database,
            &fk.parent_table,
            &fk.parent_column,
            &key,
            tx,
            crate::core::rql::row_lock::LockStrength::Share,
        )
        .await?;
        let still = referenced_values_now(
            database,
            &fk.parent_table,
            &fk.parent_column,
            std::slice::from_ref(literal),
            tx,
            Some(1),
            KeySide::Parent,
        )
        .await
        .map_err(unchecked)?;
        if still.is_empty() {
            return Err(violation());
        }
    }

    Ok(())
}

tokio::task_local! {
    /// `(parent table, parent column, literal)` triples a multi-row `INSERT`
    /// has already found and key-share locked for its transaction — see
    /// [`prevalidate_foreign_keys`]. Scoped to that statement's task.
    static FK_VERIFIED: std::cell::RefCell<std::collections::HashSet<(String, String, String)>>;
}

fn verified_key(table: &str, column: &str, literal: &str) -> (String, String, String) {
    (
        table.to_ascii_lowercase(),
        column.to_ascii_lowercase(),
        literal.trim().to_string(),
    )
}

/// Checks the foreign keys of every tuple of a multi-row `INSERT` at once.
///
/// Per key: one probe for the distinct literals, a key-share lock on each
/// value found, and one probe again under the locks — the same three steps
/// [`check_foreign_keys`] takes per row, in three round trips for the whole
/// statement instead of three per tuple. A 10 000-row insert into a child
/// table spent ~85 % of its time in per-row checks.
///
/// Returns the triples it proved. It proves a key's literals only when the
/// number of distinct values found equals the number of distinct literals:
/// each literal denotes one value and the probe returns only values some
/// literal denotes, so equal counts mean every literal matched. Anything short
/// of that — a missing parent, two spellings of one value, a parent inserted
/// by this same statement, a probe that fails to parse — proves nothing for
/// that key, and the per-row check runs and reports exactly as it would have.
async fn prevalidate_foreign_keys(
    database: &Arc<Database>,
    table_name: &str,
    statements: &[String],
    tx: TxId,
) -> std::collections::HashSet<(String, String, String)> {
    let mut verified = std::collections::HashSet::new();
    if statements.len() < 2 {
        return verified;
    }
    // On a lock failure nothing is proved: each row's own check meets the
    // same lock and reports it.
    let child = match database.table_for(table_name, owner(), TableMode::RowExclusive).await {
        Ok(Some(t)) => t,
        Ok(None) | Err(_) => return verified,
    };
    let foreign_keys = child.schema.foreign_keys.clone();
    if foreign_keys.is_empty() {
        return verified;
    }
    let rows: Vec<Vec<(String, String)>> = statements
        .iter()
        .map(|s| crate::core::rql::lexer_ir::assignments_of_insert(s).unwrap_or_default())
        .collect();

    for fk in foreign_keys.iter() {
        if !database.has_table(&fk.parent_table) {
            continue;
        }
        let mut seen = std::collections::HashSet::new();
        let mut literals: Vec<String> = Vec::new();
        for assignments in &rows {
            let Some((_, literal)) = assignments
                .iter()
                .find(|(column, _)| column.eq_ignore_ascii_case(&fk.column))
            else {
                continue;
            };
            let literal = literal.trim();
            if literal.eq_ignore_ascii_case("NULL") {
                continue;
            }
            if seen.insert(literal.to_string()) {
                literals.push(literal.to_string());
            }
        }
        if literals.is_empty() {
            continue;
        }
        // At the parent column's type (P4 round 3), deduplicated again there;
        // a value that type cannot hold is proved by nothing, and its row's
        // own check reports it. `raw` keeps what each row's check looks up.
        let raw = literals;
        let mut literals: Vec<String> = Vec::new();
        let mut proved_raw: Vec<String> = Vec::new();
        let fields = fk_key_fields(database, &child.schema, fk);
        for literal in &raw {
            let coerced = match &fields {
                Some(((c, cm), (p, pm))) => coerce_key_literal(literal, c, cm, p, pm),
                None => Some(literal.clone()),
            };
            if let Some(coerced) = coerced {
                proved_raw.push(literal.clone());
                if !literals.contains(&coerced) {
                    literals.push(coerced);
                }
            }
        }
        if literals.is_empty() {
            continue;
        }

        let Ok(found) =
            referenced_values_now(database, &fk.parent_table, &fk.parent_column, &literals, tx, None, KeySide::Parent)
                .await
        else {
            continue;
        };
        if found.len() != literals.len() {
            continue;
        }
        let mut locked = true;
        for key in &found {
            if acquire_referenced_waiting(
                database,
                &fk.parent_table,
                &fk.parent_column,
                key,
                tx,
                crate::core::rql::row_lock::LockStrength::Share,
            )
            .await
            .is_err()
            {
                // The per-row path will wait again and report the failure.
                locked = false;
                break;
            }
        }
        if !locked {
            continue;
        }
        let Ok(still) =
            referenced_values_now(database, &fk.parent_table, &fk.parent_column, &literals, tx, None, KeySide::Parent)
                .await
        else {
            continue;
        };
        if still.len() != literals.len() {
            continue;
        }
        for literal in &proved_raw {
            verified.insert(verified_key(&fk.parent_table, &fk.parent_column, literal));
        }
    }
    verified
}

/// How many literals one referenced-key probe carries. `IN` is rewritten into
/// a disjunction the evaluator tests per row, so one huge list costs more than
/// a few moderate ones.
const REFERENCED_PROBE_CHUNK: usize = 512;

/// Which side of a foreign key a probe reads (phase 3 P4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeySide {
    /// The referenced table, by the referenced column. On a partitioned
    /// table that column is a unique key's, so it holds every partition key
    /// column (§1b), and a foreign key is one column: it is the whole
    /// partition key, and the probe routes to one partition per value. A
    /// probe that would not route is refused, never fanned out, and never
    /// skipped.
    Parent,
    /// A referencing table, by its key column: routed when that column is
    /// the table's partition key, else read in every partition, since the
    /// question is whether any row anywhere references the values.
    Child,
}

/// The stored bytes of `column` in every row of `table_name` whose value is
/// one of `literals`, deduplicated — read **as of now** on `tx`'s behalf.
///
/// As of now, not through `tx`'s snapshot: a foreign-key check is a question
/// about what is committed (plus `tx`'s own writes), and a `REPEATABLE READ`
/// snapshot pinned before a concurrent commit answers it wrongly in both
/// directions — a parent deleted since still looks present, a child inserted
/// since still looks absent.
///
/// Calls the table directly with the predicate a `SELECT … WHERE column IN
/// (…)` would produce — folded and rewritten by the same functions — rather
/// than through the SQL entry, which cannot take a visibility, and which paid
/// a parse and a serialise/deserialise round trip per probe.
///
/// A partitioned table (phase 3 P4) is read under its logical name's lock
/// through a fan-out routed on the same predicate, every partition under one
/// as-of-now snapshot; `side` says whether the probe must route
/// ([`KeySide::Parent`]).
async fn referenced_values_now(
    database: &Arc<Database>,
    table_name: &str,
    column: &str,
    literals: &[String],
    tx: TxId,
    limit: Option<u64>,
    side: KeySide,
) -> Result<Vec<Vec<u8>>, String> {
    // A foreign key's other side, read: ACCESS SHARE (§7's "the FK parent
    // check"), taken here if the statement's set did not already hold it; a
    // partitioned table by its logical name.
    let Some(relation) = database.relation_for(table_name, owner(), TableMode::AccessShare).await? else {
        return Err(format!("relation \"{}\" does not exist", table_name));
    };
    if let crate::core::database::Relation::Plain(table) = &relation {
        // A shell with no partitioned table behind it holds no rows: its
        // data calls refuse, which is the error, never an empty answer.
        table.data().map_err(|e| e.to_string())?;
    }
    let schema = &relation.schema().fields;
    let Some(field) = schema
        .iter()
        .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column))
        .cloned()
    else {
        return Err(format!(
            "column \"{}\" of relation \"{}\" does not exist",
            column, table_name
        ));
    };
    let chunk_size = match &relation {
        crate::core::database::Relation::Partitioned(pt) => {
            if side == KeySide::Parent {
                let key = pt.key_columns().map_err(|e| e.to_string())?;
                if !(key.len() == 1 && key[0].eq_ignore_ascii_case(&field.name)) {
                    return Err(format!(
                        "a foreign key referencing column \"{}\" of partitioned table \"{}\" cannot be checked: \
                         its partition key is ({}), so the check would read every partition; this is not supported",
                        field.name,
                        table_name,
                        key.join(", ")
                    ));
                }
            }
            if routes_by_column(pt, &field) {
                // Within the router's point cap, so an `IN` list still
                // routes per value (§1b, "Multi-column IN").
                (crate::core::partition::route::POINT_CAP as usize).min(REFERENCED_PROBE_CHUNK)
            } else {
                // A child not partitioned by its key column reads every
                // partition whatever the chunk: the plain table's chunk, so
                // a parent's DELETE of N keys scans them about N/512 times
                // per pass, not N/256.
                REFERENCED_PROBE_CHUNK
            }
        }
        crate::core::database::Relation::Plain(_) => REFERENCED_PROBE_CHUNK,
    };
    let requested = crate::core::rql::join::fetch_for(std::slice::from_ref(&field), schema);
    let all_fields = schema.to_vec();

    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut values = Vec::new();
    for chunk in literals.chunks(chunk_size) {
        let predicate = if chunk.len() == 1 {
            format!("{} = {}", field.name, chunk[0])
        } else {
            format!("{} IN ({})", field.name, chunk.join(", "))
        };
        // The constant fold works on a statement, so it is given one.
        let statement = fold_where_constants(&format!(
            "SELECT {} FROM {} WHERE {}",
            field.name, table_name, predicate
        ));
        let Some(where_at) = crate::core::rql::join::find_keyword(&statement, "WHERE", 0) else {
            return Err("the probe lost its WHERE clause".to_string());
        };
        let raw = statement[where_at + "WHERE".len()..].trim();
        let rewritten = crate::core::tokenizer::predicate_rewrite::rewrite_where(raw, schema)?;

        // Whatever the rewritten predicate reads — a nullable column's guard
        // adds its companion — has to be fetched for the scan to test it.
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
                // The snapshot first, then the route (§4.4's order, which
                // `partitioned_with` and `partitioned_in` keep).
                let visibility = pt.visibility_now_for(tx);
                // Routed once: a column the table is not partitioned by
                // leading routes nowhere, so it is not tokenized for a route
                // at all; otherwise the route read here is the one planned.
                let route = if routes_by_column(pt, &field) {
                    pt.route_where(&rewritten).map_err(|e| e.to_string())?
                } else {
                    crate::core::partition::Route::All
                };
                let routed = matches!(
                    route,
                    crate::core::partition::Route::Points(_) | crate::core::partition::Route::Zero
                );
                #[cfg(test)]
                fk_probe_hooks::note(table_name, side);
                let source = if routed || side == KeySide::Child {
                    crate::core::rql::row_source::RowSource::partitioned_routed(pt.clone(), &route, visibility)
                } else {
                    // The router reads no key point in the predicate (a
                    // signed number, `k = -7` or `-7::bigint`, is none to
                    // it): each value routes by its own key bytes, as an
                    // INSERT of it would. A value that does not encode as a
                    // key (`7.5` for a BIGINT key) is read in every
                    // partition, its predicate deciding, as a plain parent's
                    // scan decides: correct, and the rare case.
                    match parent_key_ranges(pt, &field, chunk) {
                        Some(ranges) => {
                            crate::core::rql::row_source::RowSource::partitioned_in(pt.clone(), &ranges, visibility)
                        }
                        None => {
                            warn!(
                                "foreign key probe of partitioned table {table_name} reads every partition: a value \
                                 in ({predicate}) is no key of its type"
                            );
                            crate::core::rql::row_source::RowSource::partitioned_routed(pt.clone(), &route, visibility)
                        }
                    }
                };
                source
                    .map_err(|e| e.to_string())?
                    .query_rows(&rewritten, &all_fields, query_fetch, requested.clone(), limit, None, tx)
                    .await
                    .map_err(|e| e.to_string())?
            }
        };
        for row in &rows {
            if let Some(cell) = row.columns.first() {
                let bytes = cell.data.into_slice();
                if seen.insert(bytes.to_vec()) {
                    values.push(bytes.to_vec());
                }
            }
        }
        if limit.is_some_and(|k| values.len() as u64 >= k) {
            break;
        }
    }
    Ok(values)
}

/// Whether a predicate on `field` can route on partitioned table `pt`:
/// `field` is its leading partition key column (an equality on it narrows
/// to its value's block, §1b "What routes"). A key column below it routes
/// nothing on its own.
fn routes_by_column(
    pt: &crate::core::partition::table::PartitionedTable,
    field: &crate::core::row::schema::SchemaField,
) -> bool {
    pt.spec()
        .ok()
        .and_then(|spec| spec.levels().first().map(|level| u64::from(level.write_order) == field.write_order))
        .unwrap_or(true)
}

/// The point range of each of `literals` as a value of partitioned table
/// `pt`'s one key column `field`: the value encoded as an `INSERT` of it
/// into that column stores it (`lexer_ir::encode_column_value`, the one
/// encoder: casts, `CAST(… AS …)`, signs, quoted strings, UUIDs), so routed
/// by the bytes the parent row was routed by (`keyed_range`). `None` when
/// the key is not that one column or a value does not encode as one; the
/// caller then reads every partition, the probe's predicate deciding, as a
/// plain table's scan would.
fn parent_key_ranges(
    pt: &crate::core::partition::table::PartitionedTable,
    field: &crate::core::row::schema::SchemaField,
    literals: &[String],
) -> Option<Vec<crate::core::partition::PointRange>> {
    let spec = pt.spec().ok()?;
    let [level] = spec.levels() else {
        return None;
    };
    if u64::from(level.write_order) != field.write_order || level.db_type != field.db_type {
        return None;
    }
    literals
        .iter()
        .map(|literal| {
            let encoded = crate::core::rql::lexer_ir::encode_column_value(
                literal.trim(),
                field,
                &pt.shell().schema.type_modifiers,
            );
            // An encoder's refusal is this probe's only, never a later
            // statement's reason.
            let _ = crate::core::rql::lexer_ir::take_refusal();
            let bytes = encoded?.into_slice().to_vec();
            // An empty block is a NULL, which no key is.
            if bytes.is_empty() {
                return None;
            }
            pt.keyed_range(pt.uid(), &[(level.write_order, level.db_type.clone(), bytes)])
        })
        .collect()
}

/// Test probe: which partitioned tables a foreign-key probe read, and on
/// which side, so a test can tell a routed check from a skipped one.
#[cfg(test)]
pub(crate) mod fk_probe_hooks {
    use super::KeySide;

    static SEEN: parking_lot::Mutex<Vec<(String, bool)>> = parking_lot::Mutex::new(Vec::new());

    pub(super) fn note(table: &str, side: KeySide) {
        let mut seen = SEEN.lock();
        if seen.len() < 4096 {
            seen.push((table.to_string(), side == KeySide::Parent));
        }
    }

    /// The `(table, parent side)` probes of partitioned tables since the
    /// last call.
    pub(crate) fn take() -> Vec<(String, bool)> {
        std::mem::take(&mut *SEEN.lock())
    }
}

/// The rows of `target` matching `where_clause` as `visibility` sees them: a
/// plain table's scan, or a partitioned table's fan-out routed on the same
/// clause, every partition under that one `visibility` (phase 3 P4). What a
/// foreign key's statement-snapshot reads use, so the rows judged are the
/// rows the write then changes.
#[allow(clippy::too_many_arguments)]
async fn relation_rows_as_of(
    target: &crate::core::database::Relation,
    where_clause: &str,
    all_fields: &Vec<crate::core::row::schema::SchemaField>,
    query_row_fetch: crate::core::row::row::RowFetch,
    requested: crate::core::row::row::RowFetch,
    visibility: &crate::core::mvcc::Visibility,
    tx: TxId,
) -> Result<Vec<crate::core::row::row::Row>, String> {
    match target {
        crate::core::database::Relation::Plain(table) => table
            .query_row_with_visibility(
                where_clause,
                all_fields,
                query_row_fetch,
                requested,
                None,
                None,
                visibility.clone(),
            )
            .await
            .map_err(|e| e.to_string()),
        crate::core::database::Relation::Partitioned(pt) => {
            crate::core::rql::row_source::RowSource::partitioned_with(pt.clone(), where_clause, visibility.clone())
                .map_err(|e| e.to_string())?
                .query_rows(where_clause, all_fields, query_row_fetch, requested, None, None, tx)
                .await
                .map_err(|e| e.to_string())
        }
    }
}

/// Takes a referenced-key lock, waiting for its holder the way `FOR UPDATE`
/// does: polled, bounded by [`LOCK_WAIT_TIMEOUT`], and ended early by the
/// statement's own timeout or its client going away.
///
/// The bound is what stands in for deadlock detection: two transactions each
/// inserting a child of the parent the other is deleting would otherwise wait
/// on each other forever. The timeout is reported as a serialization failure,
/// which is what it is and what a client retries.
async fn acquire_referenced_waiting(
    database: &Arc<Database>,
    table: &str,
    column: &str,
    key: &[u8],
    tx: TxId,
    strength: crate::core::rql::row_lock::LockStrength,
) -> Result<(), String> {
    use crate::core::mvcc::row_locks::Acquired;

    // Until its holder is retired, not only while its outcome is
    // `InProgress` (`TransactionManager::unfinished`): what the caller reads
    // under the key next is read at a snapshot taken after it, and one taken
    // in a commit's window between record and retirement does not show the
    // holder's writes. A child insert then saw a deleted parent, and a
    // parent's DELETE missed an inserted child: an orphan either way
    // (`row_lock_handover_tests`).
    let live = |candidate: TxId| database.transactions.unfinished(candidate);
    let started = std::time::Instant::now();
    let deadline = started + LOCK_WAIT_TIMEOUT;
    let _waiting = database.row_locks.waiting_guard(tx);
    loop {
        let owner = match database
            .row_locks
            .acquire_referenced(table, column, key, tx, strength, &live)
        {
            Acquired::Yes => return Ok(()),
            Acquired::Held(owner) => owner,
        };
        if let Some(reason) = crate::core::cancel::tripped() {
            return Err(reason.eval_error().as_str().to_string());
        }
        if let Some(deadlock) = deadlock_check(
            database,
            tx,
            started,
            || {
                database
                    .row_locks
                    .referenced_blockers(table, column, key, tx, strength, &live)
            },
            &live,
        ) {
            return Err(deadlock);
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "could not serialize access: waited {}s for transaction {}, which holds a key \
                 of \"{}\" that a foreign key references",
                LOCK_WAIT_TIMEOUT.as_secs(),
                owner,
                table
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// The deadlock check every lock wait makes on each poll.
///
/// Once the wait has lasted `DEADLOCK_TIMEOUT` (1 s, PostgreSQL's
/// `deadlock_timeout` default), the waiter enters its edges into the wait-for
/// graph and asks whether it closes a cycle in which it is the victim. Exactly
/// one participant of a cycle is: it fails with `deadlock detected` (`40P01`),
/// its transaction's locks go with it, and the others proceed. Without this
/// every party to a crossed wait sat out the full `LOCK_WAIT_TIMEOUT` and then
/// all of them failed.
fn deadlock_check(
    database: &Arc<Database>,
    tx: TxId,
    waiting_since: std::time::Instant,
    blockers: impl FnOnce() -> smallvec::SmallVec<[TxId; 4]>,
    is_live: &dyn Fn(TxId) -> bool,
) -> Option<String> {
    if waiting_since.elapsed() < crate::core::mvcc::row_locks::DEADLOCK_TIMEOUT {
        return None;
    }
    let blockers = blockers();
    database.row_locks.set_waiting(tx, &blockers);
    if database.row_locks.deadlock_victim(tx, is_live) {
        database.row_locks.clear_waiting(tx);
        return Some(format!(
            "deadlock detected: transaction {} waits for transaction {}, which is waiting,              through a cycle, for transaction {}",
            tx,
            blockers.first().copied().unwrap_or(0),
            tx
        ));
    }
    None
}

/// Every foreign key in the database that points at `parent_table`, with the
/// table declaring it: the reverse index's answer
/// ([`Database::referencing_keys`], phase 3 P4), in child-name order. It used
/// to walk every table's schema, on every DELETE.
fn referencing_keys(
    database: &Arc<Database>,
    parent_table: &str,
) -> Vec<(String, crate::core::rql::foreign_key::ForeignKey)> {
    database.referencing_keys(parent_table)
}

/// The literal `ON DELETE SET DEFAULT` should write into `column`.
///
/// A column with no declared default reads as `NULL`, which is what PostgreSQL
/// does; that in turn may violate the very key being maintained, and is then
/// reported by the ordinary insert-time check rather than pre-empted here.
fn default_literal_for(database: &Arc<Database>, table: &str, column: &str) -> String {
    // The guard is dropped before returning: this is called from an async
    // function that goes on to await, and a DashMap guard held across an await
    // deadlocks the next writer.
    // Schema only, under the ROW EXCLUSIVE `apply_on_delete` took on the
    // child before it acts.
    let Some(view) = database.schema_view(table) else {
        return "NULL".to_string();
    };
    let schema = view.schema();
    let Some(expression) =
        crate::core::rql::column_default::expression_for(column, &schema.defaults)
    else {
        return "NULL".to_string();
    };
    let db_type = schema
        .fields
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(column))
        .map(|f| f.db_type.clone())
        .unwrap_or(crate::core::db_type::DbType::STRING);
    crate::core::rql::column_default::evaluate(expression, &db_type, database.id)
        .unwrap_or_else(|_| "NULL".to_string())
}

/// The values a `DELETE` is about to remove from one referenced column.
pub(crate) struct ReferencedColumn {
    /// The parent column some foreign key references.
    column: String,
    /// Each distinct value's stored bytes — what the key lock is taken on.
    stored: Vec<Vec<u8>>,
    /// The same values as SQL literals, index for index — what the
    /// referencing tables are matched against.
    literals: Vec<String>,
}

/// Applies each referencing table's `ON DELETE` action to the rows about to be
/// removed from `parent_table`.
///
/// `keys` holds, per referenced column, the values of those rows.
///
/// Every key is locked exclusively first (see
/// [`crate::core::mvcc::row_locks::RowLocks::acquire_referenced`]), so a child
/// insert still in flight is waited for rather than missed, and one starting
/// later waits for this transaction. Referencing rows are then judged **as of
/// now**, not through this transaction's snapshot, which may predate a child
/// that committed while it waited.
async fn apply_on_delete(
    database: &Arc<Database>,
    parent_table: &str,
    keys: &[ReferencedColumn],
    depth: usize,
    // The transaction of the DELETE that triggered this. A cascade must land in
    // it, or the effect would be visible while its cause was not.
    tx: TxId,
) -> Result<(), String> {
    use crate::core::rql::foreign_key::ReferentialAction;

    if keys.iter().all(|k| k.stored.is_empty()) {
        return Ok(());
    }
    if depth >= MAX_CASCADE_DEPTH {
        return Err(format!(
            "foreign key actions from \"{}\" nested more than {} levels deep, which suggests a \
             cycle",
            parent_table, MAX_CASCADE_DEPTH
        ));
    }

    for referenced in keys {
        for key in &referenced.stored {
            acquire_referenced_waiting(
                database,
                parent_table,
                &referenced.column,
                key,
                tx,
                crate::core::rql::row_lock::LockStrength::Update,
            )
            .await?;
        }
    }

    let referencing = referencing_keys(database, parent_table);
    // The children this delete reaches, locked at once in name order before
    // any is read or written: a check reads its child (ACCESS SHARE), an
    // action writes it (ROW EXCLUSIVE), as PostgreSQL's referential triggers
    // lock them when they run.
    let children: Vec<(String, TableMode)> = referencing
        .iter()
        .filter(|(_, fk)| {
            keys.iter()
                .any(|k| k.column.eq_ignore_ascii_case(&fk.parent_column) && !k.literals.is_empty())
        })
        .map(|(child, fk)| {
            let mode = if fk.on_delete.blocks() { TableMode::AccessShare } else { TableMode::RowExclusive };
            (child.clone(), mode)
        })
        .collect();
    database.tables_for(&children, owner()).await?;

    for (child_table, fk) in referencing {
        let Some(referenced) = keys
            .iter()
            .find(|k| k.column.eq_ignore_ascii_case(&fk.parent_column))
        else {
            continue;
        };
        if referenced.literals.is_empty() {
            continue;
        }
        // The keys at the child column's type (P4 round 3): a key no child
        // value can equal (`7.50` for an integer column) references nothing.
        let child_literals: Vec<String> = parent_literals_for_child(database, &child_table, &fk, &referenced.literals)
            .into_iter()
            .flatten()
            .collect();
        if child_literals.is_empty() {
            continue;
        }
        let list = child_literals.join(", ");
        let unchecked =
            |e: String| format!("foreign key \"{}\" could not be checked: {}", fk.name, e);
        let violation = || {
            format!(
                "update or delete on table \"{}\" violates foreign key constraint \"{}\" \
                 on table \"{}\"",
                parent_table, fk.name, child_table
            )
        };

        if fk.on_delete.blocks() {
            let referencing = referenced_values_now(
                database,
                &child_table,
                &fk.column,
                &child_literals,
                tx,
                Some(1),
                KeySide::Child,
            )
            .await
            .map_err(unchecked)?;
            if !referencing.is_empty() {
                return Err(violation());
            }
            continue;
        }

        let statement = match fk.on_delete {
            ReferentialAction::Cascade => {
                format!(
                    "DELETE FROM {} WHERE {} IN ({})",
                    child_table, fk.column, list
                )
            }
            ReferentialAction::SetNull => {
                format!(
                    "UPDATE {} SET {} = NULL WHERE {} IN ({})",
                    child_table, fk.column, fk.column, list
                )
            }
            ReferentialAction::SetDefault => {
                let literal = default_literal_for(database, &child_table, &fk.column);
                format!(
                    "UPDATE {} SET {} = {} WHERE {} IN ({})",
                    child_table, fk.column, literal, fk.column, list
                )
            }
            // The blocking actions returned above.
            _ => continue,
        };

        let Some(purpose) = recognize_query_purpose(&statement) else {
            return Err(format!("could not build the action for \"{}\"", fk.name));
        };

        // Recursion is what makes a cascade that triggers another cascade work,
        // rather than a second mechanism.
        let response = Box::pin(execute_with_context(
            purpose,
            database.clone(),
            depth + 1,
            tx,
        ))
        .await;
        if response.first() == Some(&3) {
            let message = String::from_utf8_lossy(&response[1..]).to_string();
            return Err(format!(
                "foreign key \"{}\" action on \"{}\" failed: {}",
                fk.name, child_table, message
            ));
        }

        // The action ran through this transaction's snapshot. Under
        // `REPEATABLE READ` that can predate a child committed while the key
        // lock was being waited for, and the action then never saw it. Asked
        // as of now, anything still referencing the keys is exactly such a
        // row — refused, never left behind as an orphan.
        let left = referenced_values_now(
            database,
            &child_table,
            &fk.column,
            &child_literals,
            tx,
            Some(1),
            KeySide::Child,
        )
        .await
        .map_err(unchecked)?;
        if !left.is_empty() {
            return Err(match fk.on_delete {
                // The declared default is itself one of the deleted keys.
                ReferentialAction::SetDefault => violation(),
                _ => format!(
                    "could not serialize access: a row of \"{}\" referencing \"{}\" was \
                     committed after this transaction's snapshot, so foreign key \"{}\" could \
                     not act on it",
                    child_table, parent_table, fk.name
                ),
            });
        }
    }

    Ok(())
}

/// The referenced values of the rows a `DELETE` is about to remove, one entry
/// per parent column some foreign key references.
///
/// Per column: this used to collect the values of whichever referenced column
/// came first and match *every* referencing table against them, so with two
/// foreign keys on two different columns of one parent a delete cascaded into
/// the children of a different, surviving parent.
///
/// Read through `visibility` — the snapshot the `DELETE` itself then scans
/// with — and with the statement's own engine-form predicate and fetch, so the
/// rows whose keys are processed are exactly the rows deleted. It used to be a
/// separate `SELECT` with a snapshot of its own; in `READ COMMITTED` a parent
/// row committed between that `SELECT` and the delete's scan was deleted with
/// its children never considered.
///
/// `target` is the table the `DELETE` scans: a plain one, or a partitioned
/// one, whose routed partitions are all read under `visibility` (phase 3 P4).
async fn keys_about_to_be_deleted(
    database: &Arc<Database>,
    target: &crate::core::database::Relation,
    table_name: &str,
    where_clause: &str,
    query_row_fetch: &crate::core::row::row::RowFetch,
    visibility: &crate::core::mvcc::Visibility,
    tx: TxId,
) -> Result<Vec<ReferencedColumn>, String> {
    let mut columns: Vec<String> = Vec::new();
    for (_, fk) in referencing_keys(database, table_name) {
        if !columns
            .iter()
            .any(|c| c.eq_ignore_ascii_case(&fk.parent_column))
        {
            columns.push(fk.parent_column);
        }
    }

    let mut out = Vec::with_capacity(columns.len());
    for column in columns {
        let values =
            column_values_as_of(target, &column, where_clause, query_row_fetch, visibility, tx)
                .await?;
        let mut referenced = ReferencedColumn {
            column,
            stored: Vec::with_capacity(values.len()),
            literals: Vec::with_capacity(values.len()),
        };
        for (bytes, literal) in values {
            referenced.stored.push(bytes);
            referenced.literals.push(literal);
        }
        out.push(referenced);
    }

    Ok(out)
}

/// The distinct non-NULL values of `column` in the rows `where_clause`
/// matches under `visibility`: stored bytes, and the same value as a literal.
///
/// The predicate is in engine form and `query_row_fetch` is what it reads —
/// both straight from the statement's own plan.
async fn column_values_as_of(
    target: &crate::core::database::Relation,
    column: &str,
    where_clause: &str,
    query_row_fetch: &crate::core::row::row::RowFetch,
    visibility: &crate::core::mvcc::Visibility,
    tx: TxId,
) -> Result<Vec<(Vec<u8>, String)>, String> {
    let schema = &target.schema().fields;
    let Some(field) = schema
        .iter()
        .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column))
        .cloned()
    else {
        return Err(format!(
            "column \"{}\" of relation \"{}\" does not exist",
            column,
            target.schema().name
        ));
    };
    // The companion, if the column is nullable: a NULL's value bytes are the
    // type's zero, and a zero key would be locked and cascaded as if real.
    let flag = crate::core::rql::nullable::flag_field(&field.name, schema).cloned();
    let mut wanted = vec![field.clone()];
    if let Some(f) = &flag {
        wanted.push(f.clone());
    }
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
        if let Some(f) = &flag {
            let is_null = row
                .columns
                .iter()
                .find(|c| u64::from(c.schema_id) == f.write_order)
                .is_some_and(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0);
            if is_null {
                continue;
            }
        }
        let Some(cell) = row.columns.iter().find(|c| u64::from(c.schema_id) == field.write_order) else {
            continue;
        };
        let bytes = cell.data.into_slice();
        if !seen.insert(bytes.to_vec()) {
            continue;
        }
        out.push((bytes.to_vec(), key_literal(&field, bytes)?));
    }
    Ok(out)
}

/// A stored key value as a SQL literal, rendered by the column's declared type
/// — never the cell's, which `physical()` has already erased — and at a
/// `NUMERIC`'s own scale.
fn key_literal(
    field: &crate::core::row::schema::SchemaField,
    bytes: &[u8],
) -> Result<String, String> {
    if field.db_type == crate::core::db_type::DbType::DECIMAL {
        if let (Some(scale), Ok(raw)) = (field.scale, <[u8; 16]>::try_from(bytes)) {
            return Ok(crate::core::value_codec::format_decimal_scaled(
                i128::from_le_bytes(raw),
                scale,
            ));
        }
    }
    // Refused rather than skipped: a key that cannot be named cannot have its
    // referencing rows found, and skipping it would orphan them in silence.
    crate::core::rql::subquery::value_to_literal(&field.db_type, bytes).ok_or_else(|| {
        format!(
            "foreign keys on column \"{}\" of type {:?} cannot be maintained",
            field.name, field.db_type
        )
    })
}

/// What an `UPDATE` is about to do to the keys foreign keys reference.
///
/// Read through the statement's snapshot, which the update then scans with, so
/// no key changes unseen. See `keys_about_to_be_deleted`.
pub(crate) struct KeyChanges {
    columns: Vec<KeyChange>,
}

/// One referenced column's changes: every old value the update moves, and
/// what it moves to (`None` for NULL). Unchanged values are left out — an
/// `UPDATE` that rewrites a key to itself changes nothing a child can see.
struct KeyChange {
    column: String,
    old_stored: Vec<Vec<u8>>,
    old_literals: Vec<String>,
    new_literals: Vec<Option<String>>,
}

/// Whether an `UPDATE` needs one snapshot for the whole statement: a foreign
/// key references a column it assigns, or one of its own foreign-key columns
/// is assigned an expression whose per-row values have to be checked.
fn update_needs_statement_snapshot(
    database: &Arc<Database>,
    schema: &crate::core::row::schema::TableSchema,
    table_name: &str,
    plan: &crate::core::rql::lexer_ur::UpdateRow,
) -> bool {
    let assigned = |name: &str| {
        schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
            .is_some_and(|f| {
                let wo = f.write_order as u32;
                plan.updates.iter().any(|u| u.write_order == wo)
                    || plan.expressions.iter().any(|(w, _)| *w == wo)
            })
    };
    let by_expression = |name: &str| {
        schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
            .is_some_and(|f| plan.expressions.iter().any(|(w, _)| *w == f.write_order as u32))
    };
    schema.foreign_keys.iter().any(|fk| by_expression(&fk.column))
        || referencing_keys(database, table_name)
            .iter()
            .any(|(_, fk)| assigned(&fk.parent_column))
}

/// The child side of an `UPDATE`'s foreign keys.
///
/// A constant assignment is checked as one literal, as an `INSERT`'s is. An
/// assignment that is an expression — `pid = pid + 1`, a `CASE`, which is also
/// what `ON UPDATE CASCADE` writes into a child — has a value per row, so each
/// row's new value is computed exactly as the write path will and every
/// distinct one is checked. It used to be pasted into the probe as if it were
/// a literal, which failed to parse at best and compared against the wrong
/// thing at worst.
async fn check_foreign_keys_for_update(
    database: &Arc<Database>,
    target: &crate::core::database::Relation,
    table_name: &str,
    plan: &crate::core::rql::lexer_ur::UpdateRow,
    assignments: &[(String, String)],
    visibility: Option<&crate::core::mvcc::Visibility>,
    tx: TxId,
) -> Result<(), String> {
    let schema = &target.schema().fields;
    let expression_of = |name: &str| {
        schema
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
            .and_then(|f| {
                plan.expressions
                    .iter()
                    .find(|(w, _)| *w == f.write_order as u32)
                    .map(|(_, e)| (f.clone(), e))
            })
    };

    let constants: Vec<(String, String)> = assignments
        .iter()
        .filter(|(column, _)| expression_of(column).is_none())
        .cloned()
        .collect();
    check_foreign_keys(database, table_name, &constants, tx).await?;

    let by_expression: Vec<_> = target
        .schema()
        .foreign_keys
        .iter()
        .filter_map(|fk| expression_of(&fk.column).map(|(f, e)| (fk.column.clone(), f, e)))
        .collect();
    if by_expression.is_empty() {
        return Ok(());
    }
    let Some(visibility) = visibility else {
        return Err("an expression assigned to a foreign-key column needs a statement snapshot"
            .to_string());
    };
    let full = crate::core::row::table::Table::<crate::core::storage_providers::backend::DbStorageProvider>::build_full_row_fetch(schema);
    let rows = relation_rows_as_of(
        target,
        &plan.where_clause,
        &schema.to_vec(),
        plan.query_row_fetch.clone(),
        full,
        visibility,
        tx,
    )
    .await?;
    for (column, field, expr) in by_expression {
        let mut seen = std::collections::HashSet::new();
        for row in &rows {
            let value = crate::core::rql::expr::eval_into(expr, row, schema.as_slice(), &field)?;
            let literal = match value {
                crate::core::rql::expr::Value::Null => continue,
                crate::core::rql::expr::Value::Data { bytes, db_type } => {
                    if db_type == field.db_type {
                        key_literal(&field, &bytes)?
                    } else {
                        crate::core::rql::subquery::value_to_literal(&db_type, &bytes)
                            .ok_or_else(|| {
                                format!("the new value of \"{}\" is unrepresentable", column)
                            })?
                    }
                }
            };
            if seen.insert(literal.clone()) {
                check_foreign_keys(database, table_name, &[(column.clone(), literal)], tx).await?;
            }
        }
    }
    Ok(())
}

/// The parent side of `ON UPDATE`: which referenced keys this `UPDATE`
/// changes, per row, computed exactly as the write path will — constant
/// assignments from the plan's payloads, expressions evaluated against each
/// row as it stands before the update.
///
/// `None` when no foreign key references a column the statement assigns, so
/// every other `UPDATE` pays one map lookup.
async fn keys_about_to_change(
    database: &Arc<Database>,
    target: &crate::core::database::Relation,
    table_name: &str,
    plan: &crate::core::rql::lexer_ur::UpdateRow,
    visibility: &crate::core::mvcc::Visibility,
    tx: TxId,
) -> Result<Option<KeyChanges>, String> {
    let schema = &target.schema().fields;
    let assigned = |write_order: u32| {
        plan.updates.iter().any(|u| u.write_order == write_order)
            || plan.expressions.iter().any(|(w, _)| *w == write_order)
    };

    let mut columns: Vec<crate::core::row::schema::SchemaField> = Vec::new();
    for (_, fk) in referencing_keys(database, table_name) {
        let Some(field) = schema
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&fk.parent_column))
        else {
            continue;
        };
        let flag = crate::core::rql::nullable::flag_field(&field.name, schema);
        let touched = assigned(field.write_order as u32)
            || flag.is_some_and(|f| assigned(f.write_order as u32));
        if touched && !columns.iter().any(|c| c.write_order == field.write_order) {
            columns.push(field.clone());
        }
    }
    if columns.is_empty() {
        return Ok(None);
    }

    let full = crate::core::row::table::Table::<crate::core::storage_providers::backend::DbStorageProvider>::build_full_row_fetch(schema);
    let rows = relation_rows_as_of(
        target,
        &plan.where_clause,
        &schema.to_vec(),
        plan.query_row_fetch.clone(),
        full,
        visibility,
        tx,
    )
    .await?;

    let cell = |row: &crate::core::row::row::Row, write_order: u64| -> Option<Vec<u8>> {
        row.columns
            .iter()
            .find(|c| c.schema_id as u64 == write_order)
            .map(|c| c.data.into_slice().to_vec())
    };

    let mut changes = Vec::with_capacity(columns.len());
    for field in columns {
        let flag = crate::core::rql::nullable::flag_field(&field.name, schema).cloned();
        let wo = field.write_order as u32;
        let mut change = KeyChange {
            column: field.name.clone(),
            old_stored: Vec::new(),
            old_literals: Vec::new(),
            new_literals: Vec::new(),
        };
        let mut index: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
        for row in &rows {
            let old_null = flag.as_ref().is_some_and(|f| {
                cell(row, f.write_order as u64).is_some_and(|b| b.first().copied().unwrap_or(0) != 0)
            });
            if old_null {
                continue;
            }
            let Some(old) = cell(row, field.write_order as u64) else {
                continue;
            };

            // The new value, as the write path will compute it.
            let new: Option<(Vec<u8>, crate::core::db_type::DbType)> = if let Some((_, expr)) =
                plan.expressions.iter().find(|(w, _)| *w == wo)
            {
                let value = crate::core::rql::expr::eval_into(expr, row, schema.as_slice(), &field)?;
                match value {
                    crate::core::rql::expr::Value::Null => None,
                    crate::core::rql::expr::Value::Data { bytes, db_type } => {
                        Some((bytes.to_vec(), db_type))
                    }
                }
            } else {
                let set_null = flag.as_ref().is_some_and(|f| {
                    plan.updates.iter().any(|u| {
                        u.write_order == f.write_order as u32
                            && u.data.into_slice().first().copied().unwrap_or(0) != 0
                    })
                });
                if set_null {
                    None
                } else {
                    match plan.updates.iter().find(|u| u.write_order == wo) {
                        Some(u) => Some((u.data.into_slice().to_vec(), field.db_type.clone())),
                        // Only the companion was assigned, and to "not null":
                        // the value bytes stay what they were.
                        None => Some((old.clone(), field.db_type.clone())),
                    }
                }
            };
            let new_literal = match &new {
                Some((bytes, ty)) if *ty == field.db_type => Some(key_literal(&field, bytes)?),
                // An expression's result is in the type it computed in (`3 - id`
                // over an INT is a BIGINT), which the write path narrows; its
                // literal is rendered in that type, and compared as a literal.
                Some((bytes, ty)) => Some(
                    crate::core::rql::subquery::value_to_literal(ty, bytes).ok_or_else(|| {
                        format!(
                            "cannot apply ON UPDATE for \"{}\": the new value is unrepresentable",
                            field.name
                        )
                    })?,
                ),
                None => None,
            };
            let old_literal = key_literal(&field, &old)?;
            if new_literal.as_deref() == Some(old_literal.as_str()) {
                continue;
            }
            match index.get(&old) {
                Some(&i) => {
                    if change.new_literals[i] != new_literal {
                        return Err(format!(
                            "cannot apply ON UPDATE for \"{}\": rows sharing the key {} are \
                             updated to different values",
                            field.name, change.old_literals[i]
                        ));
                    }
                }
                None => {
                    index.insert(old.clone(), change.old_stored.len());
                    change.old_literals.push(old_literal);
                    change.old_stored.push(old);
                    change.new_literals.push(new_literal);
                }
            }
        }
        if !change.old_stored.is_empty() {
            changes.push(change);
        }
    }
    if changes.is_empty() {
        return Ok(None);
    }
    Ok(Some(KeyChanges { columns: changes }))
}

/// Before the `UPDATE` writes: locks every key it moves — the same exclusive
/// key lock a `DELETE` takes, so a child insert in flight is waited for and
/// one arriving later waits — and applies `ON UPDATE RESTRICT`, which refuses
/// immediately rather than at the end of the statement.
async fn lock_keys_before_update(
    database: &Arc<Database>,
    parent_table: &str,
    changes: &KeyChanges,
    tx: TxId,
) -> Result<(), String> {
    for change in &changes.columns {
        for key in &change.old_stored {
            acquire_referenced_waiting(
                database,
                parent_table,
                &change.column,
                key,
                tx,
                crate::core::rql::row_lock::LockStrength::Update,
            )
            .await?;
        }
    }
    for (child_table, fk) in referencing_keys(database, parent_table) {
        if fk.on_update != crate::core::rql::foreign_key::ReferentialAction::Restrict {
            continue;
        }
        let Some(change) = changes
            .columns
            .iter()
            .find(|c| c.column.eq_ignore_ascii_case(&fk.parent_column))
        else {
            continue;
        };
        let child_literals: Vec<String> = parent_literals_for_child(database, &child_table, &fk, &change.old_literals)
            .into_iter()
            .flatten()
            .collect();
        if child_literals.is_empty() {
            continue;
        }
        let referencing = referenced_values_now(
            database,
            &child_table,
            &fk.column,
            &child_literals,
            tx,
            Some(1),
            KeySide::Child,
        )
        .await
        .map_err(|e| format!("foreign key \"{}\" could not be checked: {}", fk.name, e))?;
        if !referencing.is_empty() {
            return Err(format!(
                "update or delete on table \"{}\" violates foreign key constraint \"{}\" on \
                 table \"{}\"",
                parent_table, fk.name, child_table
            ));
        }
    }
    Ok(())
}

/// After the `UPDATE` wrote: each referencing table's `ON UPDATE` action.
///
/// `NO ACTION` is checked here, at the end of the statement as PostgreSQL
/// does, and only for old keys that no parent row still holds — an update that
/// moves one row's key onto another's (a swap) leaves nothing orphaned.
/// `CASCADE` rewrites every pair in one statement (`CASE`), so pairs that
/// chain — one row's new key another's old one — are applied to the children
/// as they were, not one after another. Afterwards, as for `ON DELETE`, anything still
/// referencing a vanished key as of now is a row the action could not see.
async fn apply_on_update(
    database: &Arc<Database>,
    parent_table: &str,
    changes: &KeyChanges,
    depth: usize,
    tx: TxId,
) -> Result<(), String> {
    use crate::core::rql::foreign_key::ReferentialAction;

    if depth >= MAX_CASCADE_DEPTH {
        return Err(format!(
            "foreign key actions from \"{}\" nested more than {} levels deep, which suggests a \
             cycle",
            parent_table, MAX_CASCADE_DEPTH
        ));
    }

    let referencing = referencing_keys(database, parent_table);
    // The children this update reaches, at once in name order (see
    // `apply_on_delete`): read for a check, written by an action.
    let children: Vec<(String, TableMode)> = referencing
        .iter()
        .filter(|(_, fk)| changes.columns.iter().any(|c| c.column.eq_ignore_ascii_case(&fk.parent_column)))
        .map(|(child, fk)| {
            let mode = match fk.on_update {
                ReferentialAction::NoAction | ReferentialAction::Restrict => TableMode::AccessShare,
                _ => TableMode::RowExclusive,
            };
            (child.clone(), mode)
        })
        .collect();
    database.tables_for(&children, owner()).await?;

    for (child_table, fk) in referencing {
        let Some(change) = changes
            .columns
            .iter()
            .find(|c| c.column.eq_ignore_ascii_case(&fk.parent_column))
        else {
            continue;
        };
        let unchecked =
            |e: String| format!("foreign key \"{}\" could not be checked: {}", fk.name, e);
        let violation = || {
            format!(
                "update or delete on table \"{}\" violates foreign key constraint \"{}\" on \
                 table \"{}\"",
                parent_table, fk.name, child_table
            )
        };

        // Old keys no parent holds any more, as of now.
        let present: std::collections::HashSet<Vec<u8>> = referenced_values_now(
            database,
            parent_table,
            &change.column,
            &change.old_literals,
            tx,
            None,
            KeySide::Parent,
        )
        .await
        .map_err(unchecked)?
        .into_iter()
        .collect();
        // Each old key at the child column's type (P4 round 3); one no child
        // value can equal references nothing, and is left out.
        let old_for_child = parent_literals_for_child(database, &child_table, &fk, &change.old_literals);
        let vanished: Vec<usize> = (0..change.old_stored.len())
            .filter(|i| !present.contains(&change.old_stored[*i]) && old_for_child[*i].is_some())
            .collect();
        if vanished.is_empty() {
            continue;
        }
        let vanished_literals: Vec<String> =
            vanished.iter().filter_map(|i| old_for_child[*i].clone()).collect();

        let statement = match fk.on_update {
            // Restrict refused before the write.
            ReferentialAction::NoAction | ReferentialAction::Restrict => None,
            ReferentialAction::Cascade => {
                let new_of = |i: usize| {
                    change.new_literals[i]
                        .clone()
                        .unwrap_or_else(|| "NULL".to_string())
                };
                let set = if vanished.len() == 1 {
                    new_of(vanished[0])
                } else {
                    let mut case = "CASE".to_string();
                    for i in &vanished {
                        case.push_str(&format!(
                            " WHEN {} = {} THEN {}",
                            fk.column,
                            old_for_child[*i].as_deref().unwrap_or("NULL"),
                            new_of(*i)
                        ));
                    }
                    case.push_str(&format!(" ELSE {} END", fk.column));
                    case
                };
                Some(format!(
                    "UPDATE {} SET {} = {} WHERE {} IN ({})",
                    child_table,
                    fk.column,
                    set,
                    fk.column,
                    vanished_literals.join(", ")
                ))
            }
            ReferentialAction::SetNull => Some(format!(
                "UPDATE {} SET {} = NULL WHERE {} IN ({})",
                child_table,
                fk.column,
                fk.column,
                vanished_literals.join(", ")
            )),
            ReferentialAction::SetDefault => {
                let literal = default_literal_for(database, &child_table, &fk.column);
                Some(format!(
                    "UPDATE {} SET {} = {} WHERE {} IN ({})",
                    child_table,
                    fk.column,
                    literal,
                    fk.column,
                    vanished_literals.join(", ")
                ))
            }
        };

        if let Some(statement) = statement {
            let Some(purpose) = recognize_query_purpose(&statement) else {
                return Err(format!("could not build the action for \"{}\"", fk.name));
            };
            let response =
                Box::pin(execute_with_context(purpose, database.clone(), depth + 1, tx)).await;
            if response.first() == Some(&3) {
                let message = String::from_utf8_lossy(&response[1..]).to_string();
                return Err(format!(
                    "foreign key \"{}\" action on \"{}\" failed: {}",
                    fk.name, child_table, message
                ));
            }
        }

        let left = referenced_values_now(
            database,
            &child_table,
            &fk.column,
            &vanished_literals,
            tx,
            Some(1),
            KeySide::Child,
        )
        .await
        .map_err(unchecked)?;
        if !left.is_empty() {
            return Err(match fk.on_update {
                ReferentialAction::Cascade | ReferentialAction::SetNull => format!(
                    "could not serialize access: a row of \"{}\" referencing \"{}\" was \
                     committed after this transaction's snapshot, so foreign key \"{}\" could \
                     not act on it",
                    child_table, parent_table, fk.name
                ),
                _ => violation(),
            });
        }
    }
    Ok(())
}

/// Adds columns to a table that already holds rows, by rewriting them.
///
/// A row is a fixed-width record whose column offsets come from the schema, so
/// a column appended to the schema alone would be read at an offset the stored
/// rows never wrote — landing inside whatever bytes happen to be there. The
/// only correct answer is to rewrite every row under the new layout.
///
/// The rows are read, the table is recreated with the new schema, and the rows
/// are written back. That costs a full pass and holds the table in memory,
/// which is why an empty table skips it entirely.
async fn rewrite_table_with_columns(
    database: &Arc<Database>,
    table_name: &str,
    new_columns: Vec<crate::core::rql::lexer_ct::CreateColumnData>,
    new_defaults: Vec<crate::core::rql::column_default::ColumnDefault>,
    // JSON columns being added alongside. The table's existing ones are read
    // from its schema; these are the new arrivals.
    new_json: Vec<crate::core::rql::json_map::JsonColumn>,
    // A new column's NUMERIC(p, s) and generation expression, carried into the
    // recreated table with the existing ones.
    new_modifiers: Vec<crate::core::rql::type_modifier::TypeModifier>,
    new_generated: Vec<crate::core::rql::generated::GeneratedColumn>,
    // The transaction asking. The rewrite keeps every version's own stamps
    // (R5), so this decides nothing about which rows survive; it says
    // whether the rewrite wrote versions of this family, which then hold the
    // table as any write does (`rewrite_rows`).
    reader: TxId,
) -> Result<(), String> {
    let new_columns = std::sync::Mutex::new(Some((new_columns, new_defaults, new_json, new_modifiers, new_generated)));
    rewrite_rows(database, table_name, reader, |definition, versions| {
        let (columns, defaults, json, modifiers, generated) =
            new_columns.lock().unwrap().take().expect("the definition is built once");
        definition.columns.extend(columns);
        definition.defaults.extend(defaults);
        definition.json_columns.extend(json);
        definition.type_modifiers.extend(modifiers);
        definition.generated.extend(generated);
        Ok(versions)
    })
    .await
}

/// One row version as a rewrite read it from the old files: its stamps, and
/// its values by **column name**.
///
/// By name, not by `write_order`: the recreated table renumbers its columns
/// from zero, so a write_order is only stable while the old numbering had no
/// gaps. `DROP COLUMN` leaves one: after dropping `b` from `(a, b, c)` the
/// surviving fields still read `a=0, c=2`, while the rewritten table numbers
/// them `a=0, c=1, d=2`, so `c`'s bytes were written into `d`, and every
/// later column shifted with it. That reported success and stored a corrupt
/// row. Names survive the rewrite unchanged.
struct RewrittenVersion {
    xmin: TxId,
    xmax: TxId,
    values: Vec<(String, crate::memory_pool::MemoryBlock, DbTypeRef)>,
}

/// Rewrites every row of `table_name` under a new definition, as one reload
/// under the ACCESS EXCLUSIVE the caller holds (`Database::rewrite_table`,
/// docs/persistent-indexes.md §7 "ALTER rewrites are one reload", R5).
///
/// A row is a fixed-width record whose column offsets come from the schema,
/// so a column appended to the schema alone would be read at an offset the
/// stored rows never wrote; every row is rewritten under the new layout.
///
/// The definition starts as the table's own, every declaration carried
/// (constraints, defaults, JSON registrations, NUMERIC modifiers, generation
/// expressions, checks, the PRIMARY KEY, index and path-index declarations
/// by column name), and `reshape` changes it and the versions read (a new
/// column, a retyped value). Read **after** the lock, from the retired
/// instance: every version some snapshot can still see, each written back
/// with its own `xmin` and `xmax`, so nobody's view of the rows changes. A
/// committed row stays committed whoever wrote it; a version the caller's
/// family wrote and has not committed stays its own, gone if it rolls back;
/// a committed row it deleted keeps that `xmax`, so a rollback revives it.
/// An aborted version, or one no snapshot can see, is left behind, and an
/// aborted deleter's stamp is cleared. A failure or a crash anywhere leaves
/// the old table.
///
/// When the caller's family has versions in the table (it wrote here before,
/// so it holds ROW EXCLUSIVE already), that lock is confirmed afterwards:
/// a block's ACCESS EXCLUSIVE then downgrades to it at the statement's end
/// rather than being released (finding L5).
async fn rewrite_rows<F>(database: &Arc<Database>, table_name: &str, reader: TxId, reshape: F) -> Result<(), String>
where
    F: FnOnce(
        &mut crate::core::rql::lexer_ct::CreateTable,
        Vec<RewrittenVersion>,
    ) -> Result<Vec<RewrittenVersion>, String>,
{
    use crate::core::rql::lexer_ct::{CreateColumnData, CreateTable};

    // The rewrite recreates the table's files under its name. A name older
    // builds accepted can be refused by the create; asked after the old files
    // were released, that refusal would lose every row.
    database
        .check_recreatable(table_name)
        .map_err(|e| format!("cannot rewrite '{}': {}", table_name, e))?;
    // The statement holds ACCESS EXCLUSIVE already (the ALTER arm, or a JSON
    // shadow's internal lock); confirmed, never newly waited for, here.
    database.table_for(table_name, owner(), TableMode::AccessExclusive).await?;

    struct Plan {
        versions: Vec<RewrittenVersion>,
        carried_indexes: Vec<(crate::core::index::IndexDecl, Vec<Option<String>>)>,
        carried_path_indexes: Vec<crate::core::rql::path_index::PathIndex>,
        defaults: Vec<crate::core::rql::column_default::ColumnDefault>,
    }

    let family = database.transactions.top_level(reader);
    let database_id = database.id;
    let read_db = database.clone();
    let written_by_family = database
        .rewrite_table(
            table_name,
            move |old| async move {
                let schema = &old.schema.fields;
                let mut ordered: Vec<crate::core::row::schema::SchemaField> =
                    schema.iter().filter(|f| !f.is_deleted).cloned().collect();
                ordered.sort_by_key(|f| f.write_order);
                let fetch = crate::core::rql::join::fetch_for(&ordered, schema);
                // The old numbering, so each fetched value can be named.
                let name_of: std::collections::HashMap<u64, String> =
                    schema.iter().map(|f| (f.write_order, f.name.clone())).collect();

                let horizon = old.version_horizon();
                let io_pointers = old.io_pointers().map_err(|e| e.to_string())?.clone();
                let io_rows = old.io_rows().map_err(|e| e.to_string())?.clone();
                let mut pointers = crate::core::row::row_pointer::RowPointerIterator::new(io_pointers)
                    .await
                    .map_err(|e| format!("could not read '{}' for rewrite: {}", old.schema.name, e))?;
                let mut versions = Vec::new();
                while let Some((_, pointer)) = pointers
                    .next_row_pointer_with_offset()
                    .await
                    .map_err(|e| format!("could not read '{}' for rewrite: {}", old.schema.name, e))?
                {
                    // Aborted, or dead for every snapshot that exists or can.
                    if pointer.deleted || horizon.is_reclaimable(false, pointer.xmin, pointer.xmax) {
                        continue;
                    }
                    // An aborted deleter's stamp never retired anything.
                    let xmax = if pointer.xmax != crate::core::mvcc::transaction::NO_TX
                        && read_db.transactions.outcome(pointer.xmax) == crate::core::mvcc::TxOutcome::Aborted
                    {
                        crate::core::mvcc::transaction::NO_TX
                    } else {
                        pointer.xmax
                    };
                    let row = pointer
                        .fetch_row(io_rows.clone(), &fetch)
                        .await
                        .map_err(|e| format!("could not read a row of '{}' for rewrite: {}", old.schema.name, e))?;
                    let values = row
                        .columns
                        .iter()
                        .filter_map(|c| {
                            name_of
                                .get(&u64::from(c.schema_id))
                                .map(|name| (name.clone(), c.data.clone(), c.column_type.clone()))
                        })
                        .collect();
                    versions.push(RewrittenVersion {
                        xmin: pointer.xmin,
                        xmax,
                        values,
                    });
                }

                let unique_columns: Vec<String> =
                    ordered.iter().filter(|f| f.is_unique).map(|f| f.name.clone()).collect();
                let mut definition = CreateTable {
                    table_name: old.schema.name.clone(),
                    columns: ordered
                        .iter()
                        .map(|f| CreateColumnData {
                            name: f.name.clone(),
                            // `literal_type`: a `timestamp` column is
                            // recreated as one, not as the `timestamptz`
                            // its storage type alone would declare.
                            data_type: f.literal_type(),
                        })
                        .collect(),
                    is_immutable: false,
                    if_not_exists: false,
                    foreign_keys: old.schema.foreign_keys.iter().cloned().collect(),
                    defaults: old.schema.defaults.iter().cloned().collect(),
                    json_columns: old.schema.json_columns.iter().cloned().collect(),
                    // Losing a column's declared scale here would silently
                    // reinterpret every decimal it holds.
                    type_modifiers: old.schema.type_modifiers.iter().cloned().collect(),
                    // A generation expression lost here would leave the column
                    // present and permanently NULL.
                    generated: old.schema.generated.iter().cloned().collect(),
                    // A constraint lost here stops being enforced with nothing said.
                    checks: old.schema.checks.iter().cloned().collect(),
                    unique_columns,
                    // Only the keys the rows were checked under. A predicate that
                    // no longer compiles has not been enforced, and recreated next
                    // to a new column of the name it reads, it would compile
                    // against values no row was checked against.
                    unique_keys: old
                        .schema
                        .unique_keys
                        .iter()
                        .filter(|k| match k.condition(schema) {
                            Ok(_) => true,
                            Err(e) => {
                                info!("unique index {} not carried across the rewrite: {}", k.describe(), e);
                                false
                            }
                        })
                        .cloned()
                        .collect(),
                    // The declared key goes back into the header: a rewrite used
                    // to drop it, and clients asking which column is the key got
                    // none after an ADD COLUMN.
                    primary_key: old.schema.primary_key.clone(),
                };
                // Index declarations, each with its columns by name: the rewrite
                // renumbers columns, and a name is what survives it.
                let carried_indexes = old
                    .schema
                    .secondary_indexes
                    .iter()
                    .map(|decl| {
                        let names: Vec<Option<String>> = decl
                            .columns
                            .iter()
                            .map(|wo| {
                                schema
                                    .iter()
                                    .find(|f| f.write_order as u32 == *wo && !f.is_deleted)
                                    .map(|f| f.name.clone())
                            })
                            .collect();
                        (decl.clone(), names)
                    })
                    .collect();
                // Path-index declarations, which a rewrite used to lose.
                let carried_path_indexes = old.schema.path_indexes.clone();
                let versions = reshape(&mut definition, versions)?;
                let defaults = definition.defaults.clone();
                Ok((
                    definition,
                    Plan {
                        versions,
                        carried_indexes,
                        carried_path_indexes,
                        defaults,
                    },
                ))
            },
            |plan, created| {
                let io_schema = created.io_schema.clone();
                let fields = created.schema.fields.clone();
                let carried_indexes = plan.carried_indexes.clone();
                let carried_path_indexes = plan.carried_path_indexes.clone();
                async move {
                    // Declarations go in before any row, so the open that
                    // follows builds the (empty) indexes and every row is
                    // indexed by its insert. Staged, then committed as one
                    // transaction (`torn_tail::SidecarWrites`).
                    let mut writes = crate::core::torn_tail::SidecarWrites::default();
                    for (decl, names) in carried_indexes {
                        // An index over a column this rewrite dropped goes
                        // with it, as in PostgreSQL.
                        let columns: Option<Vec<u32>> = names
                            .iter()
                            .map(|name| {
                                let name = name.as_ref()?;
                                fields
                                    .iter()
                                    .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
                                    .map(|f| f.write_order as u32)
                            })
                            .collect();
                        let Some(columns) = columns else {
                            info!("index {} dropped with its column", decl.name);
                            continue;
                        };
                        let decl = crate::core::index::IndexDecl { columns, ..decl };
                        crate::core::index::decl::stage(&*io_schema, &mut writes, &decl)
                            .map_err(|e| format!("could not carry index {}: {}", decl.name, e))?;
                    }
                    for path_index in &carried_path_indexes {
                        crate::core::rql::path_index::stage(&*io_schema, &mut writes, path_index)
                            .map_err(|e| format!("could not carry index {}: {}", path_index.name, e))?;
                    }
                    writes
                        .commit(&*io_schema)
                        .await
                        .map_err(|e| format!("could not carry the indexes: {e}"))?;
                    Ok(())
                }
            },
            |plan, table| {
                let database = database.clone();
                async move {
                    let table_name = table.schema.name.clone();
                    let schema = table.schema.fields.clone();
                    let mut family_versions = 0u64;
                    for (n, version) in plan.versions.into_iter().enumerate() {
                        let row = rewrite_payload(&table_name, &schema, &plan.defaults, &version.values, database_id)?;
                        for id in [version.xmin, version.xmax] {
                            if id != crate::core::mvcc::transaction::NO_TX
                                && id != crate::core::mvcc::transaction::FROZEN_TX
                                && database.transactions.top_level(id) == family
                            {
                                family_versions += 1;
                            }
                        }
                        table
                            .insert_version(row, version.xmin, version.xmax)
                            .await
                            .map_err(|e| format!("could not rewrite a row of '{}': {}", table_name, e))?;
                        if n == 0 {
                            // After the first row: the fill is under way.
                            crate::core::database::rewrite::point(&database, "mid_fill")?;
                        }
                    }
                    Ok(family_versions > 0)
                }
            },
        )
        .await
        .map_err(|failure| format!("{failure}"))?;
    if written_by_family {
        // Held already (the family wrote here first); confirmed, so the
        // statement's end keeps it (finding L5).
        database.table_for(table_name, owner(), TableMode::RowExclusive).await?;
    }
    Ok(())
}

/// One version's row under the rewritten schema: its values by name, and for
/// a column the row predates, the column's DEFAULT, else NULL (its companion
/// says so), else zeros.
fn rewrite_payload(
    table_name: &str,
    schema: &[crate::core::row::schema::SchemaField],
    defaults: &[crate::core::rql::column_default::ColumnDefault],
    values: &[(String, crate::memory_pool::MemoryBlock, DbTypeRef)],
    database_id: u64,
) -> Result<crate::core::row::row::RowWrite, String> {
    let mut payloads: smallvec::SmallVec<[crate::core::row::row::ColumnWritePayload; 32]> = smallvec::SmallVec::new();
    for field in schema.iter().filter(|f| !f.is_deleted) {
        let stored = values
            .iter()
            .find(|(name, _, _)| name.eq_ignore_ascii_case(&field.name));
        let data = match stored {
            Some((_, block, _)) => block.clone(),
            // A column the row predates. Fixed-width slots have to be
            // filled, and the new column's companion marks it NULL.
            None => {
                // A default gives the existing rows a value; without one
                // they take NULL, which is what makes `NOT NULL` without a
                // `DEFAULT` impossible here.
                let from_default = crate::core::rql::column_default::expression_for(
                    &field.name,
                    defaults,
                )
                .and_then(|e| {
                    crate::core::rql::column_default::evaluate(e, &field.db_type, database_id)
                        .ok()
                })
                .filter(|literal| literal != "NULL")
                .and_then(|literal| {
                    // A NUMERIC(p, s) column stores its values at its own
                    // scale; the plain encoder writes the engine's.
                    if field.db_type == crate::core::db_type::DbType::DECIMAL {
                        if let Some(scale) = field.scale {
                            return crate::core::value_codec::parse_decimal_rounded(
                                literal.trim_matches('\''),
                                scale,
                            )
                            .map(|v| v.to_le_bytes().to_vec());
                        }
                    }
                    // `literal_type`: a `timestamp` column's default keeps
                    // its wall-clock time, as an INSERT's would.
                    crate::core::value_codec::encode_literal(
                        &field.literal_type(),
                        literal.trim_matches('\''),
                    )
                    .or_else(|| {
                        crate::core::value_codec::coerce_literal(
                            literal.trim_matches('\'').as_bytes(),
                            &crate::core::db_type::DbType::STRING,
                            &field.db_type,
                        )
                    })
                });

                if let Some(bytes) = from_default {
                    let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
                    mb.into_slice_mut().copy_from_slice(&bytes);
                    mb
                } else if field.db_type.is_variable_width() {
                    crate::memory_pool::MemoryBlock::default()
                } else if crate::core::rql::nullable::is_hidden(&field.name) {
                    let tracked =
                        &field.name[crate::core::rql::nullable::NULL_FLAG_PREFIX.len()..];

                    // Does this row already hold a value for the column the
                    // companion tracks?
                    //
                    // A companion is new in two quite different situations,
                    // and only one of them means NULL:
                    //
                    // - `ADD COLUMN v TEXT` — `v` is new too, no row has a
                    //   value, so the companion says NULL unless a DEFAULT
                    //   fills it.
                    // - `ALTER COLUMN v DROP NOT NULL` — `v` is **not** new
                    //   and every row already has a value. Saying NULL here
                    //   does not lose the bytes; it makes the column report
                    //   NULL in projection and in `WHERE` while the data
                    //   sits intact underneath, which is worse than losing
                    //   it because nothing looks wrong.
                    //
                    // So the row itself decides, not the schema.
                    let tracked_present = schema
                        .iter()
                        .find(|f| f.name.eq_ignore_ascii_case(tracked))
                        .is_some_and(|f| {
                            values
                                .iter()
                                .any(|(name, _, _)| name.eq_ignore_ascii_case(&f.name))
                        });

                    let is_null = !tracked_present
                        && crate::core::rql::column_default::expression_for(
                            tracked,
                            defaults,
                        )
                        .map(|e| e.trim().eq_ignore_ascii_case("NULL"))
                        .unwrap_or(true);
                    let bytes = crate::core::rql::nullable::flag_bytes(is_null);
                    let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
                    mb.into_slice_mut().copy_from_slice(&bytes);
                    mb
                } else {
                    let mut mb = crate::memory_pool::MEMORY_POOL.acquire(field.size as usize);
                    mb.into_slice_mut().fill(0);
                    mb
                }
            }
        };

        // A fixed-width column occupies exactly its declared size in the
        // record. Writing fewer bytes than declared does not fail — it
        // shifts every column after it, so the next column's bytes are read
        // at the wrong offset and come back as a plausible number for a
        // value nobody wrote. Refusing here turns silent corruption of an
        // existing table into a failed `ALTER` that names the column.
        if !field.db_type.is_variable_width() && data.len() != field.size as usize {
            return Err(format!(
                "cannot rewrite '{}': column '{}' holds {} bytes but its type needs {}. \
                 Writing it would shift every later column and corrupt the row.",
                table_name,
                field.name,
                data.len(),
                field.size
            ));
        }

        payloads.push(crate::core::row::row::ColumnWritePayload {
            data,
            write_order: field.write_order as u32,
            column_type: field.db_type.physical(),
            size: field.size,
        });
    }

    payloads.sort_by_key(|c| c.write_order);
    Ok(crate::core::row::row::RowWrite {
        columns_writing_data: payloads,
    })
}

/// What fills an output slot no fetched column occupies.
enum SlotValue {
    /// A written constant, the same in every row.
    Constant(Vec<u8>),
    /// A nullary function, evaluated once per row.
    Scalar(crate::core::rql::aggregate::ScalarFunc),
    /// An expression over the row's own columns, and the scale its value is
    /// sent at when it is a `NUMERIC` (see `expr::display_scale`).
    RowExpression(crate::core::rql::expr::Expr, Option<u32>, bool),
}

type DbTypeRef = crate::core::db_type::DbType;

/// Replaces each JSON document in an `INSERT` with the reference to the row it
/// was stored as, or `None` when the statement has no JSON column to map.
async fn rewrite_json_values(
    database: &Arc<Database>,
    // The table whose JSON columns these are (the shadows' parent).
    parent: &str,
    sql: &str,
    assignments: &[(String, String)],
    json_columns: &[crate::core::rql::json_map::JsonColumn],
    tx: TxId,
) -> Result<Option<String>, String> {
    use crate::core::rql::json_map;

    if json_columns.is_empty() {
        return Ok(None);
    }
    // The documents first, parsed; then every shadow change all of them need
    // (keys first: no shadow row is written yet); then the rows.
    let mut documents: Vec<(String, &str, serde_json::Value)> = Vec::new();
    for (column, literal) in assignments {
        let Some(shadow_table) = json_map::shadow_for(column, json_columns) else {
            continue;
        };
        // A `::jsonb` cast comes off before anything reads the document.
        //
        // This path has to read the literal itself, because the document
        // becomes rows in a shadow table before the statement runs. The
        // unquoting below recognises a literal by its closing quote, and a
        // trailing cast means the token ends with the type name instead — so
        // the whole thing, cast included, went to the JSON parser and was
        // rejected at its first character:
        //
        //     invalid JSON for column "c_…": expected value at line 1 column 1
        //
        // A client writes `$n::jsonb` whenever it binds a document as text and
        // pins the type in SQL, which is the normal way to write one. Note this
        // is stripped for *reading* only: `literal` keeps the cast, because the
        // rewrite below finds it in the statement by its exact text.
        let trimmed = crate::core::rql::column_default::strip_redundant_cast(literal.trim());
        if trimmed.eq_ignore_ascii_case("NULL") || trimmed.is_empty() {
            continue;
        }

        // The literal is the document as written; unquoting it recovers the
        // JSON text.
        let text = if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
            trimmed[1..trimmed.len() - 1].replace("''", "'")
        } else {
            trimmed.to_string()
        };

        // The statement is re-executed once the documents have been mapped, so
        // by then this value is the UUID that was substituted in. A document is
        // an object or an array, and a bare UUID is neither — nor is it valid
        // JSON — so recognising one here is unambiguous, and it also lets a
        // caller insert a reference it already holds.
        if crate::core::value_codec::parse_uuid(&text).is_some() {
            continue;
        }

        let document: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| format!("invalid JSON for column \"{}\": {}", column, e))?;

        match document {
            // A JSON null leaves the column NULL rather than making a row.
            serde_json::Value::Null => continue,
            // An object becomes one row whose columns are its fields; an array
            // becomes a row per element, ordered by position; a bare scalar
            // becomes one row holding the value, marked as such. See the
            // scalar arm of `store_json_document` for why the last of those
            // stopped being refused.
            _ => {}
        }

        documents.push((literal.clone(), shadow_table, document));
    }
    if documents.is_empty() {
        return Ok(None);
    }
    crate::core::rql::shadow_lock::scoped(parent, true, async {
        for (_, shadow_table, document) in &documents {
            Box::pin(store_json_document(database, shadow_table, document, 0, tx)).await?;
        }
        Ok::<(), String>(())
    })
    .await?;
    let mut replacements: Vec<(String, String)> = Vec::new();
    crate::core::rql::shadow_lock::scoped(parent, false, async {
        for (literal, shadow_table, document) in &documents {
            let reference = Box::pin(store_json_document(database, shadow_table, document, 0, tx)).await?;
            replacements.push((literal.clone(), reference));
        }
        Ok::<(), String>(())
    })
    .await?;

    let mut rewritten = sql.to_string();
    for (from, to) in replacements {
        if let Some(at) = rewritten.find(&from) {
            rewritten.replace_range(at..at + from.len(), &to);
        }
    }
    Ok(Some(rewritten))
}

/// Column every shadow table is keyed by.
const JSON_KEY_COLUMN: &str = "__json_id";

/// Position of an array element within its array.
///
/// Present only on a shadow table backing a column that has held an array.
/// `-1` marks the container row of an empty array, which exists so that `[]`
/// can be told apart from `{}` and from NULL on the way back.
const JSON_IDX_COLUMN: &str = "__json_idx";

/// Which kind of container a row belongs to: `a` for an array, `s` for a bare
/// scalar, absent for an object. An object is the older shape, so its rows
/// carry no marker and keep reading back exactly as they did.
const JSON_KIND_COLUMN: &str = "__json_kind";

/// Prefix of the columns holding array elements that are not objects.
///
/// One column per element type rather than one shared column. A single column
/// would widen to text the first time an array of strings met an array of
/// numbers, and every number stored afterwards would read back quoted — the
/// value changed, with nothing reporting it. Separate slots keep `[1,2,3]` and
/// `["a"]` in the same column of the same table without either rewriting the
/// other.
const JSON_VALUE_PREFIX: &str = "__json_val";

/// Column holding the reference to an array element that is itself a container.
///
/// An element that is an array gets a shadow table of its own, exactly as a
/// nested object does. Without this, `json_map::infer` typed it as `TEXT[]` and
/// stored the Postgres array *literal* — so `[[1,2],[3,4]]` read back as
/// `[0,"{3,4}"]`: a rendering of the wrong kind, and a first element whose type
/// had been taken from the wrong column.
const JSON_ELEMENT_REF_COLUMN: &str = "__json_val_ref";

/// The element column a value of `db_type` belongs in.
fn json_value_column(db_type: &crate::core::db_type::DbType) -> String {
    use crate::core::db_type::DbType;
    let code = match db_type {
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 => "i",
        DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 => "u",
        DbType::F32 | DbType::F64 => "f",
        DbType::BOOL => "b",
        DbType::DATETIME => "ts",
        _ => "t",
    };
    format!("{}_{}", JSON_VALUE_PREFIX, code)
}

/// Stores a JSON document in the table backing a column, and returns the `UUID`
/// literal the parent row records.
///
/// The table is created on first use and extended as documents bring new keys,
/// so a schema is discovered from the data rather than declared up front.
async fn store_json_document(
    database: &Arc<Database>,
    shadow_table: &str,
    document: &serde_json::Value,
    depth: usize,
    tx: TxId,
) -> Result<String, String> {
    use crate::core::rql::json_map;

    if depth >= json_map::MAX_NESTING_DEPTH {
        return Err(format!(
            "JSON nested deeper than {} levels is not supported",
            json_map::MAX_NESTING_DEPTH
        ));
    }

    if !database.has_table(shadow_table) {
        // Under the shadow's DDL latch, and asked again there: two writers
        // carrying the first document of a column both found it missing, and
        // the second CREATE failed its statement.
        let _ddl = crate::core::rql::shadow_lock::ddl(database, shadow_table).await?;
        if !database.has_table(shadow_table) {
            run_ddl(
                database,
                &format!(
                    "CREATE TABLE {} ({} UUID NOT NULL)",
                    shadow_table, JSON_KEY_COLUMN
                ),
                tx,
            )
            .await?;
        }
    }

    let reference = new_json_reference();

    match document {
        serde_json::Value::Object(_) => {
            let assignments = Box::pin(object_assignments(
                database,
                shadow_table,
                document,
                depth,
                tx,
            ))
            .await?;
            write_shadow_row(
                database,
                shadow_table,
                &reference,
                None,
                None,
                assignments,
                tx,
            )
            .await?;
        }
        // An array becomes one row per element, all sharing the reference and
        // ordered by `__json_idx`. Storing it as a single opaque value would
        // have put it beyond every part of the engine that can see into a
        // document, which is the whole reason documents are shredded.
        serde_json::Value::Array(items) => {
            if items.is_empty() {
                // The container row. Without it an empty array would leave no
                // trace and read back as `{}`.
                write_shadow_row(
                    database,
                    shadow_table,
                    &reference,
                    Some(-1),
                    Some("a"),
                    Vec::new(),
                    tx,
                )
                .await?;
            }
            for (index, item) in items.iter().enumerate() {
                let assignments = match item {
                    serde_json::Value::Object(_) => {
                        Box::pin(object_assignments(database, shadow_table, item, depth, tx))
                            .await?
                    }
                    // An element that is itself an array becomes a document of
                    // its own, referenced by UUID.
                    serde_json::Value::Array(_) => {
                        let nested_table =
                            json_map::shadow_table_name(shadow_table, JSON_ELEMENT_REF_COLUMN);
                        let reference = Box::pin(store_json_document(
                            database,
                            &nested_table,
                            item,
                            depth + 1,
                            tx,
                        ))
                        .await?;
                        let (column, literal) = ensure_shadow_column(
                            database,
                            shadow_table,
                            JSON_ELEMENT_REF_COLUMN,
                            crate::core::db_type::DbType::UUID,
                            reference,
                            tx,
                        )
                        .await?;
                        vec![(column, literal)]
                    }
                    other => match json_map::infer(other) {
                        json_map::Inferred::Null => Vec::new(),
                        json_map::Inferred::Scalar { db_type, literal } => {
                            let column = json_value_column(&db_type);
                            let (column, literal) = ensure_shadow_column(
                                database,
                                shadow_table,
                                &column,
                                db_type,
                                literal,
                                tx,
                            )
                            .await?;
                            vec![(column, literal)]
                        }
                        json_map::Inferred::Nested => {
                            // `infer` only reports Nested for an object, which
                            // the arm above already took.
                            return Err(
                                "an array element of this shape is not supported".to_string()
                            );
                        }
                    },
                };
                write_shadow_row(
                    database,
                    shadow_table,
                    &reference,
                    Some(index as i64),
                    Some("a"),
                    assignments,
                    tx,
                )
                .await?;
            }
        }
        // A bare scalar — a string, a number, a boolean — is one row carrying
        // the value and a marker saying so.
        //
        // PostgreSQL takes any JSON value in a `jsonb` column, and a scalar is
        // the ordinary content of some of them: an attachment cell holding a
        // file id is a JSON string, not a document. Refusing it was defensible
        // while the reasoning was "a shadow table makes fields into columns and
        // elements into rows, and a scalar has neither" — but the element path
        // already stores scalars in typed value columns, so the representation
        // was there and only the top level lacked it. Reusing it keeps
        // `"a"` and `1` in the same column of the same table without either
        // rewriting the other, exactly as for array elements.
        other => {
            let assignments = match json_map::infer(other) {
                // A JSON null leaves the column NULL; the caller has already
                // skipped it, and this is here so the shape is total.
                json_map::Inferred::Null => Vec::new(),
                json_map::Inferred::Scalar { db_type, literal } => {
                    let column = json_value_column(&db_type);
                    let (column, literal) =
                        ensure_shadow_column(database, shadow_table, &column, db_type, literal, tx)
                            .await?;
                    vec![(column, literal)]
                }
                json_map::Inferred::Nested => {
                    // `infer` reports Nested only for a container, and both
                    // container arms are above.
                    return Err("a JSON value of this shape is not supported".to_string());
                }
            };
            write_shadow_row(
                database,
                shadow_table,
                &reference,
                None,
                Some("s"),
                assignments,
                tx,
            )
            .await?;
        }
    }

    Ok(format!("'{}'", reference))
}

/// A fresh v4 UUID, formatted, to identify one stored document.
fn new_json_reference() -> String {
    crate::core::value_codec::format_uuid(&crate::core::value_codec::random_uuid_v4())
}

/// Maps an object's fields onto shadow-table columns, creating and widening
/// them as needed.
async fn object_assignments(
    database: &Arc<Database>,
    shadow_table: &str,
    document: &serde_json::Value,
    depth: usize,
    tx: TxId,
) -> Result<Vec<(String, String)>, String> {
    use crate::core::rql::json_map::{self, Inferred};

    let serde_json::Value::Object(fields) = document else {
        return Err("a JSON column takes an object".to_string());
    };

    let mut assignments: Vec<(String, String)> = Vec::new();
    for (key, value) in fields {
        let column = json_map::column_name_for_key(key);
        // An array whose elements are themselves containers cannot be a
        // `TEXT[]`: rendering it flattens each element to a string, so
        // `{"nested":[[1,2],[3,4]]}` came back as `["[1,2]","[3,4]"]` — a
        // different value of a different type, reported as success. Such an
        // array gets a table of its own, exactly as a nested object does.
        // A flat array of scalars keeps the `TEXT[]` mapping, which round-trips
        // correctly and stays queryable.
        if let serde_json::Value::Array(items) = value {
            // `TEXT[]` stores every element as text, so it round-trips only an
            // array whose elements *are* text. `[1,2]` came back as
            // `["1","2"]` and `[true]` as `["true"]` — a different value of a
            // different type, reported as success. Anything else gets a table
            // of its own, where each element keeps its own column type.
            //
            // A string array keeps the `TEXT[]` mapping: it round-trips exactly
            // and stays queryable, which a shadow table does not.
            if items.iter().any(|item| !item.is_string()) {
                let nested_table = json_map::shadow_table_name(shadow_table, &column);
                let reference = Box::pin(store_json_document(
                    database,
                    &nested_table,
                    value,
                    depth + 1,
                    tx,
                ))
                .await?;
                let (column, literal) = ensure_shadow_column(
                    database,
                    shadow_table,
                    &column,
                    crate::core::db_type::DbType::UUID,
                    reference,
                    tx,
                )
                .await?;
                assignments.push((column, literal));
                continue;
            }
        }

        let (db_type, literal) = match json_map::infer(value) {
            Inferred::Null => continue,
            Inferred::Scalar { db_type, literal } => (db_type, literal),
            Inferred::Nested => {
                let nested_table = json_map::shadow_table_name(shadow_table, &column);
                let reference = Box::pin(store_json_document(
                    database,
                    &nested_table,
                    value,
                    depth + 1,
                    tx,
                ))
                .await?;
                (crate::core::db_type::DbType::UUID, reference)
            }
        };

        let (column, literal) =
            ensure_shadow_column(database, shadow_table, &column, db_type, literal, tx).await?;
        assignments.push((column, literal));
    }
    Ok(assignments)
}

/// Writes one row of a shadow table.
///
/// `index` is `Some` for a row belonging to an array. `kind` stamps the marker
/// that tells the read path what to rebuild — `a` for an array, `s` for a bare
/// scalar, `None` for an object. The two were one parameter while an index and
/// an array meant the same thing; a scalar is one row with a marker and no
/// index, so they are now separate.
async fn write_shadow_row(
    database: &Arc<Database>,
    shadow_table: &str,
    reference: &str,
    index: Option<i64>,
    kind: Option<&str>,
    assignments: Vec<(String, String)>,
    tx: TxId,
) -> Result<(), String> {
    let mut columns = vec![JSON_KEY_COLUMN.to_string()];
    let mut values = vec![format!("'{}'", reference)];

    if let Some(index) = index {
        let (idx_column, literal) = ensure_shadow_column(
            database,
            shadow_table,
            JSON_IDX_COLUMN,
            crate::core::db_type::DbType::I64,
            index.to_string(),
            tx,
        )
        .await?;
        columns.push(idx_column);
        values.push(literal);
    }

    if let Some(kind) = kind {
        let (kind_column, kind_literal) = ensure_shadow_column(
            database,
            shadow_table,
            JSON_KIND_COLUMN,
            crate::core::db_type::DbType::STRING,
            format!("'{}'", kind),
            tx,
        )
        .await?;
        columns.push(kind_column);
        values.push(kind_literal);
    }

    for (column, literal) in assignments {
        columns.push(column);
        values.push(literal);
    }

    // The keys-first walk writes no row: its columns are in place now.
    if crate::core::rql::shadow_lock::dry() {
        return Ok(());
    }
    // Shared with other row writers, never beside a rewrite of the shadow,
    // which would not carry a row appended under it (`shadow_lock`).
    let _writing = crate::core::rql::shadow_lock::writing(database, shadow_table).await;
    run_ddl(
        database,
        &format!(
            "INSERT INTO {} ({}) VALUES ({})",
            shadow_table,
            columns.join(", "),
            values.join(", ")
        ),
        tx,
    )
    .await
}

/// Makes sure `column` exists and can hold a value of `db_type`, widening it if
/// not, and returns the literal to write.
async fn ensure_shadow_column(
    database: &Arc<Database>,
    shadow_table: &str,
    column: &str,
    db_type: crate::core::db_type::DbType,
    literal: String,
    tx: TxId,
) -> Result<(String, String), String> {
    use crate::core::rql::json_map;

    let (resolved, existing) = resolve_shadow_column(database, shadow_table, column).await?;
    if let Some(existing) = &existing {
        if json_map::widen(existing, &db_type).is_none() {
            // The column already holds this.
            return Ok((resolved, json_map::reliteral(&literal, existing)));
        }
    }

    // A change: under the shadow's DDL latch (`shadow_lock`), and resolved
    // again there, since another writer may have made it meanwhile.
    let _ddl = crate::core::rql::shadow_lock::ddl(database, shadow_table).await?;
    let (column, existing) = resolve_shadow_column(database, shadow_table, column).await?;
    let column = column.as_str();

    let Some(existing) = existing else {
        // A key with no column yet gets one, which is what makes a document
        // with new fields extend the table rather than be rejected.
        run_ddl(
            database,
            &format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                shadow_table,
                column,
                json_map::sql_type_name(&db_type)
            ),
            tx,
        )
        .await?;
        return Ok((column.to_string(), literal));
    };

    match json_map::widen(&existing, &db_type) {
        // Another writer widened it first.
        None => Ok((column.to_string(), json_map::reliteral(&literal, &existing))),
        Some(widened) => {
            // Refusing here would make the column's type depend on whichever
            // document happened to arrive first. Dropping and re-adding would
            // widen the type but discard every value already stored, so the
            // column is retyped and its values converted.
            log::info!(
                "widening {}.{} from {} to {} for an incoming JSON value",
                shadow_table,
                column,
                existing,
                widened
            );
            Box::pin(retype_column(database, shadow_table, column, &widened, tx)).await?;
            Ok((column.to_string(), json_map::reliteral(&literal, &widened)))
        }
    }
}

/// The shadow column a key's value goes in, and its type if it exists.
///
/// The column is matched by exact name. A match that differs only in case is
/// a different key — JSON keys are case-sensitive — that the engine's
/// case-insensitive column names cannot hold under its own spelling, so that
/// key takes the hashed form instead. Matching loosely here was how `Date`
/// found `date`, skipped the `ADD COLUMN`, and then failed the `INSERT` on a
/// column the parser could not find.
async fn resolve_shadow_column(
    database: &Arc<Database>,
    shadow_table: &str,
    column: &str,
) -> Result<(String, Option<crate::core::db_type::DbType>), String> {
    use crate::core::rql::json_map;
    Ok({
        let entry = database.table_for(shadow_table, owner(), TableMode::AccessExclusive).await?;
        let fields: Vec<crate::core::row::schema::SchemaField> = entry
            .as_ref()
            .map(|t| {
                t.schema
                    .fields
                    .iter()
                    .filter(|f| !f.is_deleted)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let exact = |name: &str| {
            fields
                .iter()
                .find(|f| f.name == name)
                .map(|f| f.db_type.clone())
        };
        match exact(column) {
            Some(db_type) => (column.to_string(), Some(db_type)),
            None if fields.iter().any(|f| f.name.eq_ignore_ascii_case(column)) => {
                let renamed = json_map::column_name_for_case_collision(column);
                let existing = exact(&renamed);
                (renamed, existing)
            }
            None => (column.to_string(), None),
        }
    })
}

/// Changes a column's type, converting every stored value.
///
/// The values are read as the JSON they came from and re-rendered for the new
/// type, which is exactly the conversion that widening means here: an integer
/// column meeting a fractional number keeps its integers as numbers, and a
/// column becoming text keeps them as their text form.
async fn retype_column(
    database: &Arc<Database>,
    table_name: &str,
    column: &str,
    new_type: &crate::core::db_type::DbType,
    tx: TxId,
) -> Result<(), String> {
    use crate::core::rql::nullable;

    // The values are read as the documents they represent, so the conversion
    // goes through one representation rather than a matrix of type pairs.
    // Only the retyped column is converted; every other value keeps its own
    // bytes, so nothing goes through a text round trip it could not survive.
    //
    // Every version keeps its own stamps (`rewrite_rows`, R5): the documents
    // `tx` has written here and not committed, a batch whose later member is
    // the one widening the column, stay `tx`'s; other writers' rows stay
    // theirs. The rewrite used to recreate the table and insert every row
    // under `tx`, so a rollback of the widening statement emptied the
    // shadow table of every other document.
    let new_type = new_type.clone();
    rewrite_rows(database, table_name, tx, move |definition, mut versions| {
        let Some(declared) = definition.columns.iter_mut().find(|c| c.name.eq_ignore_ascii_case(column)) else {
            return Err(format!("column \"{}\" of \"{}\" does not exist", column, table_name));
        };
        let old_type = std::mem::replace(&mut declared.data_type, new_type.clone());
        let companion = nullable::flag_column_name(column);
        for version in &mut versions {
            let Some(at) = version.values.iter().position(|(name, _, _)| name.eq_ignore_ascii_case(column)) else {
                continue;
            };
            let converted = json_value_of(&old_type, version.values[at].1.into_slice())
                .and_then(|value| literal_of_json(&value, &new_type))
                .and_then(|literal| {
                    let text = literal
                        .strip_prefix('\'')
                        .and_then(|t| t.strip_suffix('\''))
                        .map(|t| t.replace("''", "'"))
                        .unwrap_or(literal);
                    crate::core::value_codec::encode_literal(&new_type, &text).or_else(|| {
                        crate::core::value_codec::coerce_literal(
                            text.as_bytes(),
                            &crate::core::db_type::DbType::STRING,
                            &new_type,
                        )
                    })
                });
            match converted {
                Some(bytes) => {
                    let mut block = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
                    block.into_slice_mut().copy_from_slice(&bytes);
                    version.values[at] = (version.values[at].0.clone(), block, new_type.clone());
                }
                // No value at the new type (a NULL, or one that does not
                // convert): the column reads NULL, as the statement-built
                // rewrite left it.
                None => {
                    version.values.remove(at);
                    if let Some(flag) = version.values.iter_mut().find(|(name, _, _)| name.eq_ignore_ascii_case(&companion)) {
                        let bytes = nullable::flag_bytes(true);
                        let mut block = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
                        block.into_slice_mut().copy_from_slice(&bytes);
                        flag.1 = block;
                    }
                }
            }
        }
        Ok(versions)
    })
    .await
}

/// Renders a JSON value as the literal a column of `db_type` takes.
fn literal_of_json(
    value: &serde_json::Value,
    db_type: &crate::core::db_type::DbType,
) -> Option<String> {
    use crate::core::db_type::DbType;
    use serde_json::Value;

    Some(match (db_type, value) {
        (_, Value::Null) => return None,
        (DbType::STRING, Value::String(s)) => crate::core::value_codec::quote_sql(s),
        // A value becoming text keeps its own text form rather than its JSON
        // form, so a number reads as `42` and not `"42"`.
        (DbType::STRING, other) => crate::core::value_codec::quote_sql(&match other {
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            other => other.to_string(),
        }),
        (DbType::BOOL, Value::Bool(b)) => if *b { "1" } else { "0" }.to_string(),
        (DbType::U64 | DbType::I64 | DbType::F64, Value::Number(n)) => n.to_string(),
        (DbType::DATETIME | DbType::INET | DbType::ARRAY | DbType::UUID, Value::String(s)) => {
            crate::core::value_codec::quote_sql(s)
        }
        (DbType::ARRAY, Value::Array(items)) => {
            match crate::core::rql::json_map::infer(&Value::Array(items.clone())) {
                crate::core::rql::json_map::Inferred::Scalar { literal, .. } => literal,
                _ => return None,
            }
        }
        // Anything else keeps whatever text it has.
        (_, other) => crate::core::value_codec::quote_sql(&other.to_string()),
    })
}

/// Whether a write failed because it would have duplicated a unique value.
///
/// Matched on the error kind rather than the message: `check_unique` reports
/// `AlreadyExists`, and a message match would break the moment someone reworded
/// it — which would turn `ON CONFLICT DO NOTHING` back into a hard failure
/// without touching either.
fn is_uniqueness_violation(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::AlreadyExists
}

/// Executes a multi-tuple `INSERT … VALUES (…), (…)`, or reports why it cannot.
///
/// `None` means the statement has a single tuple and the ordinary path should
/// handle it.
///
/// Each tuple is re-dispatched rather than parsed inline, so every per-row
/// behaviour — defaults, JSON mapping, foreign keys, uniqueness, RETURNING —
/// applies exactly as it does to a single-row insert, with no second
/// implementation to keep in step. It costs a re-parse per row; correctness
/// first, and `TableFilters::on_insert_batch` is where that is reclaimed.
///
/// `tx` is passed through, so every tuple shares one MVCC transaction and a
/// failure part-way leaves nothing visible rather than half a batch.
/// Runs `INSERT … SELECT <constants> [WHERE <constant>]`, which names no
/// relation.
///
/// Every select item must be a literal or a nullary scalar function, and the
/// condition must be constant — anything naming a column is refused, since
/// without a `FROM` there is nothing for it to name.
async fn constant_select_insert(
    prefix: &str,
    select_sql: &str,
    returning_tail: Option<&str>,
    conflict: &InsertConflict,
    database: &Arc<Database>,
    depth: usize,
    tx: TxId,
) -> Result<Vec<u8>, String> {
    use crate::core::rql::join::find_keyword;

    // Any `EXISTS`/`IN` in the condition is folded first — that is what turns
    // `WHERE NOT EXISTS (SELECT 1 FROM t)` into the constant this can evaluate.
    let substituted = substitute_in_subqueries(select_sql, database, tx).await?;
    let body = substituted.trim();
    if find_keyword(body, "SELECT", 0) != Some(0) {
        return Err("expected SELECT".to_string());
    }
    let after_select = &body["SELECT".len()..];

    let (items_text, condition) = match find_keyword(after_select, "WHERE", 0) {
        Some(at) => (
            &after_select[..at],
            Some(after_select[at + "WHERE".len()..].trim()),
        ),
        None => (after_select, None),
    };

    // The condition is constant by construction here; anything else would need a
    // row to evaluate against.
    if let Some(condition) = condition {
        if !constant_condition_holds(condition)? {
            // No row, but RETURNING still owes the client its description.
            if returning_tail.is_some() {
                return Ok(vec![2u8]);
            }
            let mut result = vec![0u8];
            result.extend_from_slice(&0u64.to_le_bytes());
            return Ok(result);
        }
    }

    let mut values: Vec<String> = Vec::new();
    for item in crate::core::rql::lexer_ct::split_definitions(items_text) {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        // A nullary scalar function is evaluated for this one row.
        let literal = if let Some(open) = item.find('(') {
            let name = item[..open].trim();
            let (func, db_type) = crate::core::rql::aggregate::ScalarFunc::parse(name)
                .ok_or_else(|| format!("'{}' is not a constant this can evaluate", item))?;
            crate::core::rql::subquery::value_to_literal(&db_type, &func.evaluate())
                .ok_or_else(|| format!("'{}' produced a value that cannot be written", item))?
        } else if item.starts_with('\'')
            || item.starts_with('"')
            || item
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_digit() || c == '-')
            || item.eq_ignore_ascii_case("NULL")
        {
            item.to_string()
        } else {
            return Err(format!(
                "'{}' names a column, but the SELECT has no FROM clause",
                item
            ));
        };
        values.push(literal);
    }

    if values.is_empty() {
        return Err("the SELECT list is empty".to_string());
    }

    let rewritten = format!(
        "{} VALUES ({}){}",
        prefix.trim_end(),
        values.join(", "),
        returning_tail
            .map(|tail| format!(" RETURNING {}", tail))
            .unwrap_or_default()
    );
    let mut purpose = recognize_query_purpose(&rewritten)
        .ok_or_else(|| "could not parse the rewritten constant INSERT".to_string())?;
    conflict.restore(&mut purpose);
    Ok(Box::pin(execute_with_context(purpose, database.clone(), depth, tx)).await)
}

/// The `ON CONFLICT` clause of an `INSERT`, as parsed.
///
/// The clause is split off the statement before anything else reads it, so a
/// statement *rewritten* from the original — `INSERT … SELECT` turned into
/// `INSERT … VALUES` — re-parses without it. Carried across and restored, as
/// the multi-row `VALUES` path does; without it `INSERT … SELECT … ON CONFLICT
/// DO NOTHING` raised a duplicate-key error for every conflicting row.
pub(crate) struct InsertConflict {
    pub do_nothing: bool,
    pub update: Option<crate::core::rql::lexer_s1::ConflictUpdate>,
}

impl InsertConflict {
    fn restore(&self, purpose: &mut QueryPurpose) {
        if let QueryPurpose::InsertRow(data) = purpose {
            data.on_conflict_do_nothing = self.do_nothing;
            data.on_conflict_update = self.update.clone();
        }
    }
}

/// Evaluates a condition built only from literals, such as the `1 = 1` an
/// uncorrelated `EXISTS` folds to.
fn constant_condition_holds(condition: &str) -> Result<bool, String> {
    let condition = condition.trim();
    for (op, holds) in [("!=", false), ("<>", false), ("=", true)] {
        if let Some(at) = condition.find(op) {
            let lhs = condition[..at].trim();
            let rhs = condition[at + op.len()..].trim();
            if lhs.chars().any(|c| c.is_alphabetic()) || rhs.chars().any(|c| c.is_alphabetic()) {
                break;
            }
            return Ok(if holds { lhs == rhs } else { lhs != rhs });
        }
    }
    Err(format!(
        "the SELECT has no FROM clause, so its condition '{}' must be constant",
        condition
    ))
}

/// Runs each branch of a `UNION` and combines the rows.
///
/// `UNION ALL` concatenates; bare `UNION` removes duplicates, compared by
/// value as `=` compares them rather than by the serialized bytes, which
/// carry the position of the column each value was read from. A trailing
/// `ORDER BY`, `LIMIT` and `OFFSET` apply to the combined result, as
/// PostgreSQL applies them. The combining is `set_ops::Combined`.
///
/// Kept out of `execute_with_context`'s own frame: that function is one large
/// match and its future is the sum of every arm's locals.
async fn execute_union(sql: &str, database: &Arc<Database>, tx: TxId) -> Vec<u8> {
    let parts = match crate::core::rql::join::split_union(sql) {
        Some(Ok(parts)) => parts,
        Some(Err(e)) => return ddl_error(&e),
        None => return ddl_error("not a UNION"),
    };

    // A document column compares by value, so `{"a":1}` and `{"a":1.0}` are
    // one row. A position is a document column when any branch says so
    // (PostgreSQL gives the column one type for all of them).
    let documents: Vec<bool> = parts.branches.iter().fold(Vec::new(), |mut acc, branch| {
        let here = crate::core::rql::cte::document_columns(branch, database);
        acc.resize(acc.len().max(here.len()), false);
        for (at, d) in here.into_iter().enumerate() {
            acc[at] |= d;
        }
        acc
    });
    // PostgreSQL names the combined columns after the first branch.
    let names = parts
        .branches
        .first()
        .map(|b| crate::core::rql::cte::output_names(b))
        .unwrap_or_default();
    let widen = crate::core::rql::set_ops::widened_integers(&parts, database);
    let mut combined = match crate::core::rql::set_ops::Combined::new(
        &parts,
        documents,
        widen,
        &names,
        vec![2u8],
    ) {
        Ok(combined) => combined,
        Err(e) => return ddl_error(&e),
    };
    // A NUMERIC column whose branches keep different scales: each cell at
    // its own (`set_ops::union_decimal_scales`).
    {
        // Each item's type: a column's as declared, and where that says
        // nothing (an expression, an aggregate, a constant) the select
        // list's own. A branch typed by neither is unknown, which a NUMERIC
        // column cannot be resolved with (refused below, never mixed).
        let declared: Vec<Vec<Option<crate::core::db_type::DbType>>> = parts
            .branches
            .iter()
            .enumerate()
            .map(|(at, b)| {
                let mut types = crate::core::rql::cte::declared_item_types_among(b, database, &[]);
                // Not in the first branch, which describes the column: a
                // constant there is described as the float it is carried as.
                if let Some(own) = recognize_query_purpose(b).and_then(|p| query_output_types(&p, database, at > 0)) {
                    types.resize(types.len().max(own.len()), None);
                    for (at, t) in own.into_iter().enumerate() {
                        if types[at].is_none() {
                            types[at] = Some(t);
                        }
                    }
                }
                types
            })
            .collect();
        if declared.iter().any(|d| d.iter().any(|t| *t == Some(crate::core::db_type::DbType::DECIMAL)))
            || parts.branches.len() > declared.iter().filter(|d| !d.is_empty()).count()
        {
            let scales: Vec<Option<Vec<u32>>> = parts
                .branches
                .iter()
                .map(|b| recognize_query_purpose(b).and_then(|p| query_output_scales(&p, database)))
                .collect();
            match crate::core::rql::set_ops::union_decimal_scales(&declared, &scales) {
                Ok(rescale) => combined.rescale(rescale),
                Err(e) => return ddl_error(&e),
            }
        }
    }

    // Each branch's rows stay serialized from the subquery response to the
    // result: no `Vec<Row>` (1.5 KB a row).
    for (at, branch) in parts.branches.iter().enumerate() {
        if branch.trim().is_empty() {
            return ddl_error("a UNION branch is empty");
        }
        if combined.is_full() {
            break;
        }
        let branch_bytes = match run_subquery_serialized(branch, database, tx).await {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("UNION branch failed: {}", e);
                return ddl_error(&e);
            }
        };
        if let Err(e) = combined.push(&branch_bytes, at < parts.distinct_prefix, at) {
            return ddl_error(&e);
        }
    }
    match combined.finish() {
        Ok(result) => result,
        Err(e) => ddl_error(&e),
    }
}

/// `text` as a SQL string literal, its quotes doubled: how a message quotes
/// statement or value text the client wrote. Embedded as it was, a quote
/// inside it ended the quoted span early for anything reading the message
/// (`sqlstate_for` skips quoted text), and the client's words after it read
/// as the message's own: `'(could not serialize access)'::int` in a CTE
/// came back 40001, and a retry loop resent it forever.
pub(crate) fn sql_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Whether `name` is the index a constraint of `table` owns: its primary
/// key's (`<table>_pkey`), a column declared `UNIQUE` (`<table>_<column>_key`),
/// or a full unique constraint. A partial unique index is not one: it answers
/// to `DROP INDEX` itself (`execute_drop_partial_unique`).
fn constraint_index(schema: &crate::core::row::schema::TableSchema, table: &str, name: &str) -> bool {
    let named = |n: &str| n.eq_ignore_ascii_case(name);
    (schema.primary_key.is_some() && named(&crate::core::rql::unique_key::pkey_name(table)))
        || schema.unique_keys.iter().any(|k| !k.is_deleted && k.predicate.is_none() && named(&k.name))
        || implicit_unique_column(name, table, &schema.fields).is_some()
}

/// The column an implicit unique-constraint name refers to.
///
/// PostgreSQL names the constraint behind a `UNIQUE` column
/// `<table>_<column>_key` (cut where long). An inline `UNIQUE` is a flag on
/// the field here, with no name stored, so the name is the one the table
/// derives now, and nothing else. After `ALTER TABLE … RENAME TO` that is the
/// new table's name: PostgreSQL keeps the old one in the constraint, and
/// asking for it here answers 42704 rather than guessing a column.
///
/// It used to guess: any `<anything>_<column>_key` matched, the longest
/// column winning, so on `order_items (id UNIQUE, item_id UNIQUE)` renamed
/// from `order_item`, `DROP CONSTRAINT order_item_id_key` (`id`'s name in
/// PostgreSQL) dropped `item_id`'s, and `bogus_id_key` dropped `id`'s.
fn implicit_unique_column(
    constraint: &str,
    table: &str,
    fields: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
) -> Option<String> {
    let given = constraint.trim().trim_matches('"');
    let unique = || fields.iter().filter(|f| f.is_unique && !f.is_deleted);
    // The name this table derives for the column, exactly (cut where long).
    let exact: Vec<&crate::core::row::schema::SchemaField> = unique()
        .filter(|f| given.eq_ignore_ascii_case(&crate::core::rql::unique_key::implicit_key_name(table, &f.name)))
        .collect();
    match exact.as_slice() {
        [one] => Some(one.name.clone()),
        // None, or two columns cut to one name, which CREATE TABLE refuses
        // (`implicit_name_collision`): never a guess between them.
        _ => None,
    }
}

/// Runs `UPDATE t SET … FROM source WHERE …`.
///
/// The write path applies one value to every matched row, so an assignment that
/// reads a *second* relation cannot go through it. Instead the two relations are
/// joined, the target's key and the new values are read out, and each row is
/// written back by key — every step a statement the engine already runs, rather
/// than a second write path to keep in step.
///
/// The cost is one `UPDATE` per matched row. That is the right trade for the
/// migrations this exists to serve, and it is stated rather than hidden.
async fn execute_update_from(
    sql: &str,
    database: &Arc<Database>,
    depth: usize,
    tx: TxId,
) -> Vec<u8> {
    use crate::core::rql::join::find_keyword;

    let Some(set_at) = find_keyword(sql, "SET", 0) else {
        return ddl_error("UPDATE has no SET clause");
    };
    let Some(from_at) = find_keyword(sql, "FROM", set_at) else {
        return ddl_error("UPDATE has no FROM clause");
    };

    // `UPDATE <table> [AS] <alias>`
    let head = sql[..set_at].trim();
    let mut head_words = head.split_whitespace().skip(1);
    let Some(table_token) = head_words.next() else {
        return ddl_error("UPDATE names no relation");
    };
    let table = table_token.trim_matches('"').to_string();
    // A partitioned target or source (S19, phase 3 P3) is read by the join
    // `update_from_join` runs through `run_subquery`, every relation and
    // partition under that join's one snapshot, and the target is written
    // back row by row by its composite identifying key (§1b).
    let target_alias = match head_words.next() {
        Some(w) if w.eq_ignore_ascii_case("AS") => head_words.next(),
        Some(w) => Some(w),
        None => None,
    }
    .map(|a| a.trim_matches('"').to_string())
    .unwrap_or_else(|| table.clone());

    let assignments_text = sql[set_at + "SET".len()..from_at].trim();

    // `RETURNING` belongs to the statement, not to the predicate. Left in, it
    // was swept into the `WHERE` and reached the tokenizer as the identifier
    // `RETURNING`.
    let (body, returning) = match crate::core::rql::lexer_ir::returning_split(sql) {
        Some((before, after)) => (before, Some(after.trim().to_string())),
        None => (sql, None),
    };
    let from_at = find_keyword(body, "FROM", set_at).unwrap_or(from_at);
    let after_from = &body[from_at + "FROM".len()..];
    let where_at = find_keyword(after_from, "WHERE", 0);
    let (source_text, condition) = match where_at {
        Some(at) => (
            after_from[..at].trim(),
            after_from[at + "WHERE".len()..].trim(),
        ),
        None => (after_from.trim(), ""),
    };
    if condition.is_empty() {
        return ddl_error(
            "UPDATE … FROM without a WHERE would pair every row with every other; refusing",
        );
    }
    // Every qualifier names the target or a source. One naming neither was
    // dropped by the join's reader, so `WHERE u.tid = nosuch.id AND u.id =
    // 2` paired every target row with u's row 2 and rewrote them all; and an
    // aliased target is known only by its alias (`UPDATE t x … WHERE u.tid =
    // t.id`). PostgreSQL's 42P01.
    {
        let explicit_alias = (target_alias != table).then_some(target_alias.as_str());
        let allowed = join_write_qualifiers(table_token, explicit_alias, source_text);
        let texts = [assignments_text, condition, returning.as_deref().unwrap_or("")];
        if let Some(refusal) = refuse_unknown_qualifier(&texts, &allowed) {
            return refusal;
        }
    }

    // A derived table is materialised first, so the join reads a real relation.
    let mut temps: Vec<String> = Vec::new();
    let source_clause = if source_text.trim_start().starts_with('(') {
        let Some(close) = source_text.rfind(')') else {
            return ddl_error("unterminated subquery in the FROM clause");
        };
        let inner = source_text[source_text.find('(').unwrap_or(0) + 1..close].trim();
        let alias = source_text[close + 1..]
            .trim()
            .trim_start_matches("AS ")
            .trim_start_matches("as ")
            .trim()
            .trim_matches('"');
        if alias.is_empty() {
            return ddl_error("a subquery in FROM must be given an alias");
        }
        match crate::core::rql::cte::materialize_query(inner, alias, sql, database, tx).await {
            Ok(temp) => {
                temps.push(temp.clone());
                format!("{} {}", temp, alias)
            }
            Err(e) => return ddl_error(&e),
        }
    } else {
        source_text.to_string()
    };

    let result = update_from_join(
        &table,
        &target_alias,
        assignments_text,
        &source_clause,
        condition,
        returning.as_deref(),
        database,
        depth,
        tx,
    )
    .await;

    crate::core::rql::cte::drop_temps(database, &temps, tx).await;

    match result {
        Ok(response) => response,
        Err(e) => ddl_error(&e),
    }
}

/// A `WHERE` term of the form `(SELECT …) <op> <literal>` whose inner query
/// reads columns of the outer row.
///
/// It cannot be folded into the join predicate: its value changes per candidate
/// row, so it is evaluated after the join, once per row, with the outer
/// references replaced by that row's values.
#[derive(Debug)]
struct CorrelatedTerm {
    /// The subquery text, outer references still in place.
    inner_sql: String,
    op: String,
    /// The constant the subquery's result is compared against.
    literal: String,
    /// Outer references the subquery reads, as written (`fwd.user_table_id`).
    outer_refs: Vec<String>,
}

/// Splits a condition into the terms a join can evaluate and the correlated
/// terms it cannot.
///
/// A term is correlated when it contains a `(SELECT …)` that mentions one of
/// `aliases` — the relations the *outer* statement names. A subquery that
/// mentions none of them is left alone: it is uncorrelated, and the ordinary
/// substitution path already turns it into a constant.
fn split_correlated_terms(
    condition: &str,
    aliases: &[String],
) -> Result<(String, Vec<CorrelatedTerm>), String> {
    let mut plain: Vec<String> = Vec::new();
    let mut correlated: Vec<CorrelatedTerm> = Vec::new();

    for term in crate::core::rql::join::split_conjuncts(condition) {
        let trimmed = term.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some(open) = trimmed.find('(') else {
            plain.push(trimmed.to_string());
            continue;
        };
        let after = trimmed[open + 1..].trim_start();
        if !after.to_ascii_uppercase().starts_with("SELECT") {
            plain.push(trimmed.to_string());
            continue;
        }
        let Some(close) = matching_paren_in(trimmed, open) else {
            return Err(format!("unterminated subquery in '{}'", trimmed));
        };
        let inner_sql = trimmed[open + 1..close].trim().to_string();
        let outer_refs = outer_references(&inner_sql, aliases);
        if outer_refs.is_empty() {
            // Uncorrelated: someone else's job.
            plain.push(trimmed.to_string());
            continue;
        }

        // Only `(SELECT …) <op> <literal>` is handled. Anything else is
        // refused rather than approximated.
        let rest = trimmed[close + 1..].trim();
        let mut op_len = 0usize;
        for candidate in ["<=", ">=", "<>", "!=", "=", "<", ">"] {
            if rest.starts_with(candidate) {
                op_len = candidate.len();
                break;
            }
        }
        if op_len == 0 {
            return Err(format!(
                "a correlated subquery must be compared against a constant, as in \
                 `(SELECT COUNT(*) …) = 1`; got '{}'",
                trimmed
            ));
        }
        let literal = rest[op_len..].trim().to_string();
        if literal.is_empty() {
            return Err(format!("'{}' compares against nothing", trimmed));
        }
        correlated.push(CorrelatedTerm {
            inner_sql,
            op: rest[..op_len].to_string(),
            literal,
            outer_refs,
        });
    }

    Ok((plain.join(" AND "), correlated))
}

/// Index of the `)` closing the `(` at `open`.
fn matching_paren_in(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    for (i, b) in bytes.iter().enumerate().skip(open) {
        match quote {
            Some(q) => {
                if *b == q {
                    quote = None;
                }
            }
            None => match b {
                b'\'' | b'"' => quote = Some(*b),
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            },
        }
    }
    None
}

/// Every `alias.column` in `sql` whose qualifier is one of `aliases`.
fn outer_references(sql: &str, aliases: &[String]) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0usize;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = quote {
            if b == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if b == b'\'' {
            quote = Some(b);
            i += 1;
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' || b == b'"' {
            let start = i;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric()
                    || bytes[i] == b'_'
                    || bytes[i] == b'"'
                    || bytes[i] == b'.')
            {
                i += 1;
            }
            let word = &sql[start..i];
            if let Some(dot) = word.find('.') {
                let qualifier = word[..dot].trim_matches('"');
                if aliases.iter().any(|a| a.eq_ignore_ascii_case(qualifier))
                    && !out.iter().any(|e| e == word)
                {
                    out.push(word.to_string());
                }
            }
            continue;
        }
        i += 1;
    }
    out
}

/// One `qualifier.column` reference in SQL text: its byte span and both
/// names with their quotes removed (`"s"."big"`, `s."big"`, `"s".big` and
/// `s.big` all name `s` and `big`).
struct QualifiedRef {
    start: usize,
    end: usize,
    qualifier: String,
    column: String,
}

/// Every two-part `qualifier.column` reference in `text`, outside string
/// literals, in order. A three-part name, and one following a `.`, is none.
fn qualified_ref_spans(text: &str) -> Vec<QualifiedRef> {
    let b = text.as_bytes();
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || !c.is_ascii();
    // One identifier at `i`: its end and its name, unquoted.
    let ident = |i: usize| -> Option<(usize, String)> {
        match b.get(i)? {
            b'"' => {
                let mut j = i + 1;
                let mut name = Vec::new();
                loop {
                    match b.get(j)? {
                        b'"' if b.get(j + 1) == Some(&b'"') => {
                            name.push(b'"');
                            j += 2;
                        }
                        b'"' => return Some((j + 1, String::from_utf8(name).ok()?)),
                        &c => {
                            name.push(c);
                            j += 1;
                        }
                    }
                }
            }
            &c if is_word(c) && !c.is_ascii_digit() => {
                let mut j = i;
                while b.get(j).is_some_and(|&c| is_word(c)) {
                    j += 1;
                }
                Some((j, text[i..j].to_string()))
            }
            _ => None,
        }
    };
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' {
            // A string literal, `''` an escaped quote inside it.
            i += 1;
            while i < b.len() {
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i += 1;
            continue;
        }
        let boundary = i == 0 || !(is_word(b[i - 1]) || b[i - 1] == b'.' || b[i - 1] == b'"');
        if boundary && (c == b'"' || (is_word(c) && !c.is_ascii_digit())) {
            let Some((e1, qualifier)) = ident(i) else {
                i += 1;
                continue;
            };
            if b.get(e1) == Some(&b'.')
                && let Some((e2, column)) = ident(e1 + 1)
            {
                if b.get(e2) != Some(&b'.') {
                    out.push(QualifiedRef { start: i, end: e2, qualifier, column });
                }
                // The rest of a longer dotted name is read on as its parts
                // follow a `.`, which no reference starts after.
                i = e2;
                continue;
            }
            i = e1;
            continue;
        }
        if is_word(c) {
            while i < b.len() && is_word(b[i]) {
                i += 1;
            }
            continue;
        }
        i += 1;
    }
    out
}

/// A reference's identity for matching: the qualifier compared as the
/// aliases are (ASCII case folded), the column exactly.
fn ref_key(qualifier: &str, column: &str) -> (String, String) {
    (qualifier.to_ascii_lowercase(), column.to_string())
}

/// Replaces each outer reference in `sql` with the literal it resolved to
/// (rendered by [`render_substituted`]).
///
/// One pass over `sql`, quote-aware: a reference inside a string literal is
/// text, not a reference (`'see s.x ' || s.x` stored `see 7 7`), and what
/// is substituted is never scanned again (a substituted string holding
/// `s.l` broke the statement). Whole references only, so `fwd.id` inside
/// `fwd.id_extra` is left alone, in any quoting (`"s".big`, `s."big"`).
fn substitute_outer_refs(sql: &str, bindings: &[(String, String)]) -> String {
    let keyed: Vec<((String, String), &str)> = bindings
        .iter()
        .filter_map(|(name, literal)| {
            let span = qualified_ref_spans(name).into_iter().next()?;
            Some((ref_key(&span.qualifier, &span.column), literal.as_str()))
        })
        .collect();
    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0usize;
    for span in qualified_ref_spans(sql) {
        let key = ref_key(&span.qualifier, &span.column);
        if let Some((_, literal)) = keyed.iter().find(|(k, _)| *k == key) {
            out.push_str(&sql[cursor..span.start]);
            render_substituted(&mut out, literal, &sql[span.end..]);
            cursor = span.end;
        }
    }
    out.push_str(&sql[cursor..]);
    out
}

/// Appends `literal`, a value substituted for a reference, to `out` (the
/// text before it), given the text after it (`rest`), so that it reads as
/// the one value it is wherever it lands.
///
/// What the engine's two parsers take, measured: an assignment's expression
/// parser takes a parenthesised literal anywhere and a bare one anywhere but
/// after a lone sign (`w = - 7` and `w = - -3` are read as literal text);
/// the predicate parser takes a bare literal anywhere, but a parenthesised
/// one not in `IN (…)`, `BETWEEN`, `LIKE` or `IS DISTINCT FROM`. And `::`
/// binds tighter than a sign (`-3::text` is `-(3::text)`). So:
/// - a value that does not begin with `-` (a positive number, a string,
///   `NULL`) goes in bare, as it did before any of this: `-s.x` is `-7`;
/// - a negative number goes in parenthesised before a cast (`(-3)::text`)
///   and after a sign (`x-(-3)`, never `x--3`, a comment; `-(-3)`, which
///   the predicate parser reads since this change), and bare elsewhere,
///   set apart by a space unless an opening parenthesis or a comma comes
///   before it (`IN (-3, 99)`, `BETWEEN -5 AND -1`, `2 * -3`).
fn render_substituted(out: &mut String, literal: &str, rest: &str) {
    let negative = literal.starts_with('-');
    let before = out.trim_end().chars().last();
    let cast_follows = rest.trim_start().starts_with("::");
    if negative && (cast_follows || matches!(before, Some('-') | Some('+'))) {
        out.push('(');
        out.push_str(literal);
        out.push(')');
    } else if negative && !matches!(before, None | Some('(') | Some(',')) && !out.ends_with(|c: char| c.is_whitespace()) {
        out.push(' ');
        out.push_str(literal);
    } else {
        out.push_str(literal);
    }
}

/// True when the scalar the subquery returned satisfies `<value> <op> <literal>`.
fn scalar_comparison_holds(value: &str, op: &str, literal: &str) -> Result<bool, String> {
    // A NULL on either side makes the comparison unknown, which is not true.
    // `rows_to_scalar` writes a NULL as the bare word, never quoted, so with
    // the quotes stripped below it compared as the text `NULL`: `<> 'x'` held
    // for a subquery that found nothing.
    if is_null_literal(value) || is_null_literal(literal) {
        return Ok(false);
    }
    let lhs = value.trim().trim_matches('\'');
    let rhs = literal.trim().trim_matches('\'');

    // Numbers compare numerically; anything else compares as text, which is
    // what an equality against a quoted constant wants.
    let ordering = match (lhs.parse::<f64>(), rhs.parse::<f64>()) {
        // PostgreSQL's float order, NaN included, shared with ORDER BY.
        (Ok(a), Ok(b)) => crate::core::processor::concurrent_processor::pg_float_cmp(a, b),
        _ => lhs.cmp(rhs),
    };
    use std::cmp::Ordering::*;
    Ok(match op {
        "=" => ordering == Equal,
        "!=" | "<>" => ordering != Equal,
        ">" => ordering == Greater,
        ">=" => ordering != Less,
        "<" => ordering == Less,
        "<=" => ordering != Greater,
        _ => return Err(format!("unsupported comparison '{}'", op)),
    })
}

/// An unquoted `NULL`, as `rows_to_scalar` writes one.
fn is_null_literal(text: &str) -> bool {
    text.trim().eq_ignore_ascii_case("NULL")
}

/// [`scalar_comparison_holds`] for a subquery that returns a document: both
/// sides compared as `jsonb` values, the way every other path compares them.
///
/// As text, `{"a": 1.0}` did not equal `{"a": 1}`, and spacing or key order
/// decided an equality that PostgreSQL answers by value.
fn document_comparison_holds(value: &str, op: &str, literal: &str) -> Result<bool, String> {
    if is_null_literal(value) || is_null_literal(literal) {
        return Ok(false);
    }
    // The constant may carry its cast: `= '{"a":1}'::jsonb`.
    let document = |text: &str| -> Result<Vec<u8>, String> {
        let text = text.trim();
        let text = match crate::core::rql::expr::trailing_cast(text) {
            Some((operand, ty))
                if ty.eq_ignore_ascii_case("jsonb") || ty.eq_ignore_ascii_case("json") =>
            {
                operand
            }
            _ => text,
        };
        crate::core::jsonb::encode_text(&crate::core::value_codec::unquote_sql(text))
    };
    let (lhs, rhs) = (document(value)?, document(literal)?);
    let malformed = || {
        format!(
            "cannot compare '{}' with '{}' as jsonb",
            value.trim(),
            literal.trim()
        )
    };
    use std::cmp::Ordering::*;
    Ok(match op {
        "=" => crate::core::jsonb::values_equal(&lhs, &rhs).ok_or_else(malformed)?,
        "!=" | "<>" => !crate::core::jsonb::values_equal(&lhs, &rhs).ok_or_else(malformed)?,
        _ => {
            let ordering = crate::core::jsonb::compare_values(&lhs, &rhs).ok_or_else(malformed)?;
            match op {
                ">" => ordering == Greater,
                ">=" => ordering != Less,
                "<" => ordering == Less,
                "<=" => ordering != Greater,
                _ => return Err(format!("unsupported comparison '{}'", op)),
            }
        }
    })
}

/// Joins the target with the source, then writes each row back by key.
#[allow(clippy::too_many_arguments)]
/// `DELETE FROM t [AS] a USING <source> WHERE <cond> [RETURNING …]` — the
/// multi-relation DELETE, PostgreSQL's counterpart of `UPDATE … FROM`.
/// Handled the same way: target and source are joined, the matched target
/// keys read out, and each row deleted through the single-relation path.
async fn execute_delete_using(
    sql: &str,
    database: &Arc<Database>,
    depth: usize,
    tx: TxId,
) -> Vec<u8> {
    use crate::core::rql::join::find_keyword;

    let (body, returning) = match crate::core::rql::lexer_ir::returning_split(sql) {
        Some((before, after)) => (before, Some(after.trim().to_string())),
        None => (sql, None),
    };

    let Some(using_at) = find_keyword(body, "USING", 0) else {
        return ddl_error("DELETE has no USING clause");
    };

    // `DELETE FROM <table> [AS] <alias>`
    let head = body[..using_at].trim();
    let mut head_words = head.split_whitespace().skip(2);
    let Some(table_token) = head_words.next() else {
        return ddl_error("DELETE names no relation");
    };
    let table = table_token.trim_matches('"').to_string();
    // As for `UPDATE … FROM`: a partitioned target or source is read by the
    // join, and the target deleted from by its identifying key (phase 3 P3).
    let target_alias = match head_words.next() {
        Some(w) if w.eq_ignore_ascii_case("AS") => head_words.next(),
        Some(w) => Some(w),
        None => None,
    }
    .map(|a| a.trim_matches('"').to_string())
    .unwrap_or_else(|| table.clone());

    let after_using = &body[using_at + "USING".len()..];
    let where_at = find_keyword(after_using, "WHERE", 0);
    let (source_text, condition) = match where_at {
        Some(at) => (
            after_using[..at].trim(),
            after_using[at + "WHERE".len()..].trim(),
        ),
        None => (after_using.trim(), ""),
    };
    if condition.is_empty() {
        return ddl_error(
            "DELETE … USING without a WHERE would pair every row with every other; refusing",
        );
    }
    // As for `UPDATE … FROM`: `DELETE FROM t USING u WHERE u.tid = nosuch.id
    // AND u.w = 200` deleted the whole table.
    {
        let explicit_alias = (target_alias != table).then_some(target_alias.as_str());
        let allowed = join_write_qualifiers(table_token, explicit_alias, source_text);
        let texts = [condition, returning.as_deref().unwrap_or("")];
        if let Some(refusal) = refuse_unknown_qualifier(&texts, &allowed) {
            return refusal;
        }
    }

    // A derived source is materialised first, exactly as `UPDATE … FROM` does.
    let mut temps: Vec<String> = Vec::new();
    let source_clause = if source_text.trim_start().starts_with('(') {
        let Some(close) = source_text.rfind(')') else {
            return ddl_error("unterminated subquery in the USING clause");
        };
        let inner = source_text[source_text.find('(').unwrap_or(0) + 1..close].trim();
        let alias = source_text[close + 1..]
            .trim()
            .trim_start_matches("AS ")
            .trim_start_matches("as ")
            .trim()
            .trim_matches('"');
        if alias.is_empty() {
            return ddl_error("a subquery in USING must be given an alias");
        }
        match crate::core::rql::cte::materialize_query(inner, alias, sql, database, tx).await {
            Ok(temp) => {
                temps.push(temp.clone());
                format!("{} {}", temp, alias)
            }
            Err(e) => return ddl_error(&e),
        }
    } else {
        source_text.to_string()
    };

    let result = delete_using_join(
        &table,
        &target_alias,
        &source_clause,
        condition,
        returning.as_deref(),
        database,
        depth,
        tx,
    )
    .await;

    crate::core::rql::cte::drop_temps(database, &temps, tx).await;

    match result {
        Ok(response) => response,
        Err(e) => ddl_error(&e),
    }
}

async fn delete_using_join(
    table: &str,
    target_alias: &str,
    source_clause: &str,
    condition: &str,
    returning: Option<&str>,
    database: &Arc<Database>,
    depth: usize,
    tx: TxId,
) -> Result<Vec<u8>, String> {
    // The target's key. Without one there is no way to name the row again.
    // Composite on every partitioned target, whose rows are written back
    // through the partitioned `DELETE`, each routed by the key to its one
    // partition (§1b).
    let key = {
        let table_ref = database.table_for(table, owner(), TableMode::RowExclusive).await?;
        let handle = table_ref
            .as_ref()
            .clone()
            .ok_or_else(|| format!("relation '{}' does not exist", table))?;
        identifying_key(&handle.schema).ok_or_else(|| {
            format!(
                "DELETE … USING needs a unique column on '{}' to name each row by",
                table
            )
        })?
    };
    let width = key.fields.len();

    // The source's columns the condition reads are carried out of the join,
    // so each row's delete can recheck the condition against the target row
    // as it is when deleted, with the source row it was joined to. The join
    // ran under its own snapshot; a target row changed since may no longer
    // match, and PostgreSQL rechecks it rather than deleting it.
    let target_columns: Vec<String> = {
        let entry = database.table_for(table, owner(), TableMode::RowExclusive).await?;
        entry
            .as_ref()
            .map(|h| h.schema.fields.iter().filter(|f| !f.is_deleted).map(|f| f.name.clone()).collect())
            .unwrap_or_default()
    };
    let recheck = recheck_terms(condition, target_alias, &target_columns)
        .filter(|t| !t.is_empty())
        .map(|terms| pad_varying_beside_padded(terms, table, target_alias, source_clause, database));
    let recheck_text = recheck.as_ref().map(|terms| terms.join(" AND "));
    let recheck_refs = recheck_text
        .as_deref()
        .map(|t| qualified_refs(t, &source_aliases(source_clause)))
        .unwrap_or_default();
    let mut projection: Vec<String> = key.names().map(|k| format!("{}.{}", target_alias, k)).collect();
    projection.extend(recheck_refs.iter().cloned());
    let select = format!(
        "SELECT {} FROM {} {}, {} WHERE {}",
        projection.join(", "),
        table,
        target_alias,
        source_clause,
        condition
    );
    // Unnormalised: each column at the scale it is stored at, which
    // `scales` gives. Normalising to the engine's nine places rounded a
    // `NUMERIC(20,12)` key, so two rows deduplicated into one written back
    // by a value matching neither (`DELETE 0` where 2 were due).
    let (rows, scales) = run_subquery_scaled(&select, database, tx, SubqueryNeed::All, false).await?;
    if rows.is_empty() {
        // With RETURNING, an empty row set: the client is owed a
        // RowDescription even when nothing matched, as dedd503 gives an
        // `INSERT … SELECT`. A bare count sent only the command tag.
        if returning.is_some() {
            return Ok(vec![2u8]);
        }
        let mut result = vec![0u8];
        result.extend_from_slice(&0u64.to_le_bytes());
        return Ok(result);
    }

    let returning_unqualified =
        returning.map(|r| format!("RETURNING {}", strip_qualifier(r.trim(), target_alias)));

    // One delete per DISTINCT key: several source rows may match one target
    // row, and it can only be deleted once. Every row is named before any is
    // deleted, so a key that cannot be written back (a NULL) refuses the
    // statement with nothing applied.
    let mut seen: ahash::HashSet<Vec<u8>> =
        ahash::HashSet::with_capacity_and_hasher(rows.len(), Default::default());
    let mut named_rows: Vec<(&crate::core::row::row::Row, String)> = Vec::with_capacity(rows.len());
    for row in &rows {
        if row.columns.len() < width {
            return Err("the join returned no key".to_string());
        }
        let cells: Vec<&crate::core::row::row::Column> = row.columns[..width].iter().collect();
        if seen.insert(key.row_key(&cells)?) {
            named_rows.push((row, key.predicate(&cells)?));
        }
    }
    let mut deleted = 0u64;
    let mut returned: Vec<u8> = Vec::new();
    for (row, named) in named_rows {
        let returning_text = match &returning_unqualified {
            Some(r) => format!(" {}", r),
            None => String::new(),
        };
        let key_only = format!("DELETE FROM {} WHERE {}{}", table, named, returning_text);
        let mut bindings: Vec<(String, String)> = Vec::with_capacity(recheck_refs.len());
        for (offset, reference) in recheck_refs.iter().enumerate() {
            let carried = row
                .columns
                .get(width + offset)
                .ok_or_else(|| "the join returned fewer values than the delete needs".to_string())?;
            let literal = joined_literal(carried, scales.as_deref(), width + offset)
                .ok_or_else(|| "a joined value cannot be written back".to_string())?;
            bindings.push((reference.clone(), literal));
        }
        let with_recheck = match &recheck_text {
            Some(text) => format!(
                "DELETE FROM {} WHERE {} AND ({}){}",
                table,
                named,
                substitute_outer_refs(&strip_qualifier(text, target_alias), &bindings),
                returning_text
            ),
            None => key_only.clone(),
        };
        let response = run_per_row_write(with_recheck, key_only, database, depth, tx).await?;
        if response.first() == Some(&3u8) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        if returning.is_some() && response.first() == Some(&2u8) {
            returned.extend_from_slice(&response[1..]);
        }
        // What the delete removed, not one per key: a row another transaction
        // deleted first, or one that no longer matches, counts nothing.
        deleted += affected_by(&response).unwrap_or(0);
    }

    if returning.is_some() {
        let mut result = vec![2u8];
        result.extend_from_slice(&returned);
        return Ok(result);
    }

    let mut result = vec![0u8];
    result.extend_from_slice(&deleted.to_le_bytes());
    Ok(result)
}

async fn update_from_join(
    table: &str,
    target_alias: &str,
    assignments_text: &str,
    source_clause: &str,
    condition: &str,
    // The statement's `RETURNING` list, if it has one. Each row is written back
    // individually, so the clause is carried onto each write and the results
    // gathered — otherwise a statement that asked for rows would get a count.
    returning: Option<&str>,
    database: &Arc<Database>,
    depth: usize,
    tx: TxId,
) -> Result<Vec<u8>, String> {
    // The target's key. Without one there is no way to name the row again, so
    // this is refused rather than guessed at. Composite on every partitioned
    // target, whose rows are written back through the partitioned `UPDATE`,
    // each routed by the key to its one partition (§1b).
    let key = {
        let table_ref = database.table_for(table, owner(), TableMode::RowExclusive).await?;
        let handle = table_ref
            .as_ref()
            .clone()
            .ok_or_else(|| format!("relation '{}' does not exist", table))?;
        // The guard is a shard read lock; it ends here, before any await.
        identifying_key(&handle.schema).ok_or_else(|| {
            format!(
                "UPDATE … FROM needs a unique column on '{}' to write each row back by",
                table
            )
        })?
    };
    let width = key.fields.len();

    let mut assignments: Vec<(String, String)> = Vec::new();
    for part in crate::core::rql::lexer_ct::split_definitions(assignments_text) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let Some(eq) = part.find('=') else {
            return Err(format!("assignment '{}' has no '='", part));
        };
        assignments.push((
            part[..eq].trim().trim_matches('"').to_string(),
            part[eq + 1..].trim().to_string(),
        ));
    }
    if assignments.is_empty() {
        return Err("UPDATE has no assignments".to_string());
    }

    // A correlated subquery is pulled out of the join predicate: its value
    // depends on the candidate row, so it can only be decided once that row
    // exists.
    let source_alias = source_clause
        .split_whitespace()
        .next_back()
        .unwrap_or(source_clause)
        .trim_matches('"')
        .to_string();
    let aliases = vec![target_alias.to_string(), source_alias];
    let (join_condition, correlated) = split_correlated_terms(condition, &aliases)?;
    if join_condition.trim().is_empty() {
        return Err(
            "UPDATE … FROM needs a join condition besides its correlated subquery; \
             without one every row would be paired with every other"
                .to_string(),
        );
    }

    // Each correlated term reads outer columns, so the join has to carry them
    // out alongside the key and the assigned values.
    let mut carried: Vec<String> = Vec::new();
    for term in &correlated {
        for reference in &term.outer_refs {
            if !carried.iter().any(|c| c == reference) {
                carried.push(reference.clone());
            }
        }
    }

    // An assigned value that is an expression over the target's own columns —
    // `t.attempts + 1`, `COALESCE(t.started_at, NOW())` — cannot be projected
    // through the join: its select list carries columns, literals and nullary
    // functions, and nothing that computes. The columns such an expression
    // reads are projected instead, and the expression is evaluated per row
    // once they are in hand.
    let target_schema: Vec<crate::core::row::schema::SchemaField> = {
        let entry = database.table_for(table, owner(), TableMode::RowExclusive).await?;
        let handle = entry
            .as_ref()
            .clone()
            .ok_or_else(|| format!("relation '{}' does not exist", table))?;
        handle.schema.fields.to_vec()
    };

    let mut projection: Vec<String> = key.names().map(|k| format!("{}.{}", target_alias, k)).collect();
    // For each assignment: either the output slot its value occupies, or the
    // expression to evaluate and the slots its columns landed in.
    let mut sources: Vec<AssignedValue> = Vec::with_capacity(assignments.len());
    let from_aliases = source_aliases(source_clause);

    for (_, value) in &assignments {
        let stripped = strip_qualifier(value, target_alias);
        let computed = if crate::core::rql::expr::needs_evaluation(&stripped)
            || crate::core::rql::lexer_query::looks_like_row_expression(&stripped)
        {
            crate::core::rql::expr::parse(&stripped, &target_schema).ok()
        } else {
            None
        };

        match computed {
            Some(expr) => {
                let mut slots = Vec::new();
                for field in crate::core::rql::expr::columns_of(&expr) {
                    slots.push((projection.len(), field.clone()));
                    projection.push(format!("{}.{}", target_alias, field.name));
                }
                let _ = slots;
                // The source's columns it reads, projected so each write
                // substitutes the joined row's values. Written as text into a
                // single-relation `UPDATE`, `s.big` would name the target's
                // own `big`: the parse above resolves a column by name, and so
                // does that statement, so `SET v = s.big / 100.0` stored
                // `t.big / 100.0` and reported success.
                let source_refs = qualified_refs(&stripped, &from_aliases);
                let source_at = projection.len();
                projection.extend(source_refs.iter().cloned());
                sources.push(AssignedValue::Computed {
                    text: stripped.clone(),
                    source_refs,
                    source_at,
                });
            }
            None => {
                sources.push(AssignedValue::Slot(projection.len()));
                projection.push(value.clone());
            }
        }
    }

    let carried_at = projection.len();
    projection.extend(carried.iter().cloned());
    // The source's columns the join condition reads, carried so each row's
    // write can recheck the condition against the target row as it is when
    // written, with the source row it was joined to, as PostgreSQL does.
    let recheck_at = projection.len();
    let target_columns: Vec<String> = target_schema.iter().filter(|f| !f.is_deleted).map(|f| f.name.clone()).collect();
    let recheck_text = recheck_terms(&join_condition, target_alias, &target_columns)
        .filter(|t| !t.is_empty())
        .map(|terms| pad_varying_beside_padded(terms, table, target_alias, source_clause, database).join(" AND "));
    let recheck_refs = recheck_text
        .as_deref()
        .map(|t| qualified_refs(t, &source_aliases(source_clause)))
        .unwrap_or_default();
    projection.extend(recheck_refs.iter().cloned());
    let select = format!(
        "SELECT {} FROM {} {}, {} WHERE {}",
        projection.join(", "),
        table,
        target_alias,
        source_clause,
        join_condition
    );

    // Unnormalised, as `delete_using_join` reads its join: every carried
    // value is written back at the scale it is stored at (`scales`).
    let (rows, scales) = run_subquery_scaled(&select, database, tx, SubqueryNeed::All, false).await?;
    if rows.is_empty() {
        // With RETURNING, an empty row set: the client is owed a
        // RowDescription even when nothing matched, as dedd503 gives an
        // `INSERT … SELECT`. A bare count sent only the command tag.
        if returning.is_some() {
            return Ok(vec![2u8]);
        }
        let mut result = vec![0u8];
        result.extend_from_slice(&0u64.to_le_bytes());
        return Ok(result);
    }

    // Each row is written back through a single-relation `UPDATE`, which has no
    // alias — so `RETURNING t.id` has to lose the `t.` or it names a relation
    // that statement never mentions.
    // `returning_split` hands back the column list without the keyword.
    let returning_unqualified =
        returning.map(|r| format!("RETURNING {}", strip_qualifier(r.trim(), target_alias)));

    for row in &rows {
        if row.columns.len() < projection.len() {
            return Err(format!(
                "the join returned {} of the {} values the update needs",
                row.columns.len(),
                projection.len()
            ));
        }
    }

    // Decide every row's correlated terms before writing any row.
    //
    // PostgreSQL evaluates the whole statement against the snapshot it started
    // with, so a subquery over the target table does not see the rows this
    // statement has already updated. Deciding row by row between the writes
    // let each subquery see the earlier writes — `(SELECT COUNT(*) … done = 1)
    // = 0` updated one row per group where PostgreSQL updates them all.
    //
    // Deciding first also means the answer depends only on the substituted
    // text, so each distinct one runs once: 4 000 joined rows over four groups
    // ran 4 000 subqueries, each a scan, and now run four.
    let mut keep_row: Vec<bool> = vec![true; rows.len()];
    if !correlated.is_empty() {
        // Whether each term's subquery reads a stored document, which a scan
        // reports as text; asked of the schema once, not of every row.
        let documents: Vec<bool> = correlated
            .iter()
            .map(|term| {
                crate::core::rql::cte::document_columns(&term.inner_sql, database)
                    .first()
                    .copied()
                    .unwrap_or(false)
            })
            .collect();
        let mut answers: std::collections::HashMap<String, (String, bool)> =
            std::collections::HashMap::new();
        for (i, row) in rows.iter().enumerate() {
            let mut bindings: Vec<(String, String)> = Vec::with_capacity(carried.len());
            for (offset, reference) in carried.iter().enumerate() {
                let column = &row.columns[carried_at + offset];
                let literal = joined_literal(column, scales.as_deref(), carried_at + offset)
                    .ok_or_else(|| "a joined value cannot be written".to_string())?;
                bindings.push((reference.clone(), literal));
            }
            for (term, declared_document) in correlated.iter().zip(&documents) {
                let resolved = substitute_outer_refs(&term.inner_sql, &bindings);
                let (value, document) = match answers.get(&resolved) {
                    Some(answer) => answer.clone(),
                    None => {
                        let produced = run_subquery(&resolved, database, tx).await?;
                        // A computed document — `doc->'a'` — is reported as
                        // one by the row itself.
                        let document = *declared_document
                            || produced.first().and_then(|r| r.columns.first()).is_some_and(
                                |c| {
                                    matches!(
                                        c.column_type,
                                        crate::core::db_type::DbType::JSON
                                            | crate::core::db_type::DbType::JSONB
                                    )
                                },
                            );
                        // Compared here as text against the term's literal
                        // (`scalar_comparison_holds`), never substituted.
                        let value =
                            crate::core::rql::subquery::rows_to_scalar(&produced, document, false)?;
                        answers.insert(resolved, (value.clone(), document));
                        (value, document)
                    }
                };
                let holds = if document {
                    document_comparison_holds(&value, &term.op, &term.literal)?
                } else {
                    scalar_comparison_holds(&value, &term.op, &term.literal)?
                };
                if !holds {
                    keep_row[i] = false;
                    break;
                }
            }
        }
    }

    // One write per DISTINCT key, from the first joined row naming it, as
    // PostgreSQL updates a target row once however many source rows it
    // joins: a second write applied `SET n = n + 1` twice, and counted and
    // returned the row twice. Every row is named before any is written, so a
    // key that cannot be written back (a NULL) refuses the statement with
    // nothing applied.
    let mut seen: ahash::HashSet<Vec<u8>> =
        ahash::HashSet::with_capacity_and_hasher(rows.len(), Default::default());
    let mut named_rows: Vec<(&crate::core::row::row::Row, String)> = Vec::with_capacity(rows.len());
    for (row, _) in rows.iter().zip(&keep_row).filter(|(_, keep)| **keep) {
        let cells: Vec<&crate::core::row::row::Column> = row.columns[..width].iter().collect();
        if seen.insert(key.row_key(&cells)?) {
            named_rows.push((row, key.predicate(&cells)?));
        }
    }
    let mut written = 0u64;
    let mut returned: Vec<u8> = Vec::new();
    // Each row is written by a statement of its own, which commits its
    // storage transaction as it returns, and a later row can still fail this
    // one (a CHECK, a key, a foreign key). An autocommit statement's writes
    // may otherwise overwrite in place, which its abort cannot undo: the rows
    // before the failure kept their new values.
    database.transactions.forbid_in_place(tx);
    for (row, named) in named_rows {
        let literal_at = |index: usize| -> Result<String, String> {
            joined_literal(&row.columns[index], scales.as_deref(), index)
                .ok_or_else(|| "a joined value cannot be written".to_string())
        };

        let mut sets: Vec<String> = Vec::with_capacity(assignments.len());
        for ((column, _), source) in assignments.iter().zip(sources.iter()) {
            let literal = match source {
                AssignedValue::Slot(at) => literal_at(*at)?,
                // Written into the generated statement as the expression it
                // is, rather than evaluated here and written as a literal.
                //
                // Evaluating it here meant rebuilding a row out of the join's
                // projected columns, and that projection does not carry a
                // column's NULL marker — so `COALESCE(t.started_at, '…')` on a
                // NULL column saw a present zero and stored the epoch instead
                // of the literal. The per-row `UPDATE` reads the stored row
                // directly and gets this right; the parse above is kept only to
                // decide that the value *is* an expression and to learn which
                // columns it reads.
                AssignedValue::Computed { text, source_refs, source_at } => {
                    let mut bindings: Vec<(String, String)> = Vec::with_capacity(source_refs.len());
                    for (offset, reference) in source_refs.iter().enumerate() {
                        let at = source_at + offset;
                        let literal = assigned_literal(&row.columns[at], scales.as_deref(), at)
                            .ok_or_else(|| "a joined value cannot be written".to_string())?;
                        bindings.push((reference.clone(), literal));
                    }
                    sets.push(format!("{} = {}", column, substitute_outer_refs(text, &bindings)));
                    continue;
                }
            };
            sets.push(format!("{} = {}", column, literal));
        }

        let returning_text = match &returning_unqualified {
            Some(r) => format!(" {}", r),
            None => String::new(),
        };
        let key_only = format!("UPDATE {} SET {} WHERE {}{}", table, sets.join(", "), named, returning_text);
        let mut bindings: Vec<(String, String)> = Vec::with_capacity(recheck_refs.len());
        for (offset, reference) in recheck_refs.iter().enumerate() {
            bindings.push((reference.clone(), literal_at(recheck_at + offset)?));
        }
        let with_recheck = match &recheck_text {
            Some(text) => format!(
                "UPDATE {} SET {} WHERE {} AND ({}){}",
                table,
                sets.join(", "),
                named,
                substitute_outer_refs(&strip_qualifier(text, target_alias), &bindings),
                returning_text
            ),
            None => key_only.clone(),
        };
        let response = run_per_row_write(with_recheck, key_only, database, depth, tx).await?;
        if response.first() == Some(&3u8) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        // A `RETURNING` write answers with serialized rows; they are gathered so
        // the statement returns one result set rather than one per row.
        if returning.is_some() && response.first() == Some(&2u8) {
            returned.extend_from_slice(&response[1..]);
        }
        // What the write changed, not one per key: a row deleted or changed
        // out of the join's reach since counts nothing.
        written += affected_by(&response).unwrap_or(0);
    }

    if returning.is_some() {
        let mut result = vec![2u8];
        result.extend_from_slice(&returned);
        return Ok(result);
    }

    let mut result = vec![0u8];
    result.extend_from_slice(&written.to_le_bytes());
    Ok(result)
}

/// The relation names a `USING` / `FROM` list makes visible: each comma-
/// separated item's alias, or its table name when it has none.
/// The qualifiers an `UPDATE … FROM` or `DELETE … USING` knows its
/// relations by: the target's (its alias alone when it has one) and each
/// source's, a derived one by its alias.
fn join_write_qualifiers(target: &str, target_alias: Option<&str>, sources: &str) -> Vec<String> {
    let mut allowed = crate::core::sql_scan::relation_qualifiers(target, target_alias);
    for item in crate::core::rql::lexer_ct::split_definitions(sources) {
        let item = item.trim();
        if item.starts_with('(') {
            let Some(close) = crate::core::sql_scan::matching_close(item, 0) else { continue };
            let alias = item[close + 1..].trim();
            let alias = alias.strip_prefix("AS ").or_else(|| alias.strip_prefix("as ")).unwrap_or(alias).trim();
            allowed.push(alias.trim_matches('"').to_string());
            continue;
        }
        let words: Vec<&str> = item.split_whitespace().collect();
        let alias = match words.as_slice() {
            [_, kw, a] if kw.eq_ignore_ascii_case("AS") => Some(*a),
            [_, a] => Some(*a),
            _ => None,
        };
        if let Some(name) = words.first() {
            allowed.extend(crate::core::sql_scan::relation_qualifiers(name, alias));
        }
    }
    allowed
}

/// The 42P01 refusal for the first qualifier in `texts` naming none of
/// `allowed`, or `None`.
fn refuse_unknown_qualifier(texts: &[&str], allowed: &[String]) -> Option<Vec<u8>> {
    let allowed: Vec<&str> = allowed.iter().map(String::as_str).collect();
    texts
        .iter()
        .find_map(|t| crate::core::sql_scan::unknown_qualifier(t, &allowed))
        .map(|q| ddl_error(&crate::core::sql_scan::missing_from_entry(&q)))
}

fn source_aliases(source_clause: &str) -> Vec<String> {
    crate::core::rql::lexer_ct::split_definitions(source_clause)
        .iter()
        .filter_map(|item| item.split_whitespace().next_back())
        .map(|name| name.trim_matches('"').to_string())
        .collect()
}

/// The `alias.column` references in `text` qualified by one of `aliases`,
/// each once, in order, in any quoting (`"s"."big"`), as written. String
/// literals are skipped.
fn qualified_refs(text: &str, aliases: &[String]) -> Vec<String> {
    let mut seen: Vec<(String, String)> = Vec::new();
    let mut found: Vec<String> = Vec::new();
    for span in qualified_ref_spans(text) {
        if !aliases.iter().any(|a| a.eq_ignore_ascii_case(&span.qualifier)) {
            continue;
        }
        let key = ref_key(&span.qualifier, &span.column);
        if !seen.contains(&key) {
            seen.push(key);
            found.push(text[span.start..span.end].to_string());
        }
    }
    found
}

/// `terms` with each comparison of a `character varying` column and a
/// `character(n)` one reading the varying side blank-padded (`rtrim`), as
/// PostgreSQL resolves `varchar = bpchar` and as the join itself compares
/// them (`join::KeyColumn`). Each source reference is substituted with its
/// joined value per row, and a value from a `character(n)` column is an
/// untyped literal by then, which a varying column compares with exactly:
/// `c.b = d.k` matched in the join and failed its recheck, writing nothing.
/// The target is `table` under `target_alias`; the source the first relation
/// of `source_clause` under its last word. Unchanged when neither table has
/// both kinds of column, which is the common case and costs a schema look.
fn pad_varying_beside_padded(
    terms: Vec<String>,
    table: &str,
    target_alias: &str,
    source_clause: &str,
    database: &Arc<Database>,
) -> Vec<String> {
    use crate::core::rql::type_modifier::TextTypmod;
    let fields_of = |name: &str| -> Vec<crate::core::row::schema::SchemaField> {
        database.schema_view(name).map(|v| v.schema().fields.to_vec()).unwrap_or_default()
    };
    let kinded = |fields: &[crate::core::row::schema::SchemaField]| {
        fields.iter().any(|f| {
            !f.is_deleted
                && f.db_type == crate::core::db_type::DbType::STRING
                && matches!(f.text_typmod(), Some(TextTypmod::Bpchar(_) | TextTypmod::Varchar(_)))
        })
    };
    let target = fields_of(table);
    let mut words = source_clause.split_whitespace();
    let source_table = words.next().unwrap_or("").trim_matches('"').to_string();
    let source_alias = source_clause.split_whitespace().next_back().unwrap_or("").trim_matches('"').to_string();
    let source = if source_table.is_empty() { Vec::new() } else { fields_of(&source_table) };
    if !kinded(&target) || !kinded(&source) {
        return terms;
    }
    // `Some(true)` for a `character(n)` column, `Some(false)` for a varying one.
    let kind = |reference: &str| -> Option<bool> {
        let reference = reference.trim();
        let (qualifier, column) = match reference.rsplit_once('.') {
            Some((q, c)) => (Some(q.trim_matches('"')), c.trim_matches('"')),
            None => (None, reference.trim_matches('"')),
        };
        let fields = match qualifier {
            None => &target,
            Some(q) if q == target_alias || q == table => &target,
            Some(q) if q == source_alias || q == source_table => &source,
            Some(_) => return None,
        };
        let field = fields.iter().find(|f| !f.is_deleted && f.name == column)?;
        if field.db_type != crate::core::db_type::DbType::STRING {
            return None;
        }
        match field.text_typmod() {
            Some(TextTypmod::Bpchar(_)) => Some(true),
            Some(TextTypmod::Varchar(_)) => Some(false),
            _ => None,
        }
    };
    terms
        .into_iter()
        .map(|term| {
            let Some((at, width)) = find_comparison(&term) else { return term };
            let (left, op, right) = (&term[..at], &term[at..at + width], &term[at + width..]);
            match (kind(left), kind(right)) {
                (Some(false), Some(true)) => format!("rtrim({}) {} {}", left.trim(), op, right.trim()),
                (Some(true), Some(false)) => format!("{} {} rtrim({})", left.trim(), op, right.trim()),
                _ => term,
            }
        })
        .collect()
}

/// The conjuncts of a join condition a per-row write must recheck against
/// the target row as it is when written, or `None` when the condition cannot
/// be split safely (a `BETWEEN`, whose `AND` is not a conjunction).
///
/// PostgreSQL rechecks the condition on the target row's newest version with
/// the source row it was joined to. A conjunct that reads only source columns
/// held when the join ran and still does, so only those reading the target
/// (through its alias, or a bare name of one of its columns) are kept. Their
/// source references are substituted with the joined values per row.
fn recheck_terms(
    condition: &str,
    target_alias: &str,
    target_columns: &[String],
) -> Option<Vec<String>> {
    let upper = condition.to_ascii_uppercase();
    if upper.contains(" BETWEEN ") {
        return None;
    }
    let target = [target_alias.to_string()];
    Some(
        crate::core::rql::join::split_conjuncts(condition)
            .into_iter()
            .filter(|term| {
                !qualified_refs(term, &target).is_empty() || bare_words(term).iter().any(|w| {
                    target_columns.iter().any(|c| c.eq_ignore_ascii_case(w))
                })
            })
            .collect(),
    )
}

/// The unqualified identifiers in `text`, outside quotes, that are not a
/// function name (followed by `(`) or part of a qualified reference.
fn bare_words(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || !b.is_ascii();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i += 1;
            while i < bytes.len() && bytes[i] != b'\'' {
                i += 1;
            }
            i += 1;
            continue;
        }
        if is_word(bytes[i]) && !bytes[i].is_ascii_digit() && (i == 0 || !(is_word(bytes[i - 1]) || bytes[i - 1] == b'.')) {
            let start = i;
            while i < bytes.len() && is_word(bytes[i]) {
                i += 1;
            }
            let qualified = bytes.get(i) == Some(&b'.');
            let call = text[i..].trim_start().starts_with('(');
            if !qualified && !call {
                out.push(text[start..i].to_string());
            }
            if qualified {
                // Skip the column after the dot.
                i += 1;
                while i < bytes.len() && is_word(bytes[i]) {
                    i += 1;
                }
            }
            continue;
        }
        i += 1;
    }
    out
}

/// Rows a per-row write reported, from its response: the count it carries,
/// or `None` for a `RETURNING` response, which carries rows instead.
fn affected_by(response: &[u8]) -> Option<u64> {
    match response.first() {
        Some(0) if response.len() >= 9 => Some(u64::from_le_bytes(response[1..9].try_into().ok()?)),
        _ => None,
    }
}

/// Runs one generated per-row write, `with_recheck`: the key and the join
/// condition's recheck against the row as it is when written. `key_only` is
/// the same write by key alone, which is what runs when there is nothing to
/// recheck (the two are then equal).
///
/// A recheck that cannot run is an error, never a write by key alone. That
/// fallback used to follow any error not known to be about the row: an
/// evaluator failure on the recheck's literal arithmetic (`t.v < 5 * 100` on
/// a NUMERIC column, "operand size does not match") silently wrote the row
/// with no recheck, which is the write a concurrently changed row must not
/// get.
/// Whether `sql` holds an `IN (SELECT …)`, an `EXISTS` or a scalar
/// subquery, which [`substitute_in_subqueries`] folds into literals.
fn has_subquery(sql: &str) -> bool {
    crate::core::rql::statement_snapshot::mentions_select(sql)
        && (crate::core::rql::subquery::find_in_subquery(sql).is_some()
            || crate::core::rql::subquery::find_exists(sql).is_some()
            || crate::core::rql::subquery::find_scalar_subquery(sql).is_some())
}

/// Folds `sql`'s subqueries into literals and runs the statement that
/// leaves, under the same transaction and depth.
async fn execute_flattened(sql: &str, verb: &str, database: Arc<Database>, depth: usize, tx: TxId) -> Vec<u8> {
    match substitute_in_subqueries(sql, &database, tx).await {
        Ok(flattened) => {
            let Some(purpose) = recognize_query_purpose(&flattened) else {
                return ddl_error(&format!("could not parse the {} after subquery substitution", verb));
            };
            Box::pin(execute_with_context(purpose, database, depth, tx)).await
        }
        Err(e) => ddl_error(&e),
    }
}

/// Whether an `UPDATE` or a `DELETE` reads before it writes, so its attempt
/// needs one snapshot for both (`statement_snapshot`): a subquery to fold, or
/// a `FROM`/`USING` source to join (a derived one materialised first). A
/// plain one reads nothing first and takes its snapshot under its lock.
fn write_reads_first(query: &QueryPurpose) -> bool {
    match query {
        QueryPurpose::UpdateRow(sql) => {
            has_subquery(sql) || crate::core::rql::join::find_keyword(sql, "FROM", 0).is_some()
        }
        QueryPurpose::DeleteRow(sql) => {
            has_subquery(sql) || crate::core::rql::join::find_keyword(sql, "USING", 0).is_some()
        }
        _ => false,
    }
}

/// One attempt of a statement in a retry loop: [`execute_with_context`],
/// inside a write scope of its own when it is a write that reads first
/// and no enclosing write scope (a CTE-fed write's) already holds the
/// statement's reads. Each attempt's scope is new, so a conflict retry reads afresh.
///
/// Not an `async fn`, and boxed once: the retry loops box every attempt, and
/// an `async fn` wrapper boxed the statement's future a second time.
fn execute_attempt(
    query: QueryPurpose,
    database: Arc<Database>,
    tx: TxId,
) -> std::pin::Pin<Box<impl std::future::Future<Output = Vec<u8>>>> {
    let scoped = write_reads_first(&query) && !crate::core::rql::statement_snapshot::in_write_scope();
    let attempt = execute_with_context(query, database, 0, tx);
    Box::pin(if scoped {
        futures::future::Either::Left(crate::core::rql::statement_snapshot::write_scope(attempt))
    } else {
        futures::future::Either::Right(attempt)
    })
}

/// The snapshot an `UPDATE`'s or a `DELETE`'s target rows are scanned under
/// when its statement read first: the one its reads took. Only the
/// statement's own target (`depth` 0); a foreign-key action it sets off
/// reads and writes its rows as of now, as before.
fn statement_snapshot_for(depth: usize, kept: impl FnOnce() -> Option<crate::core::mvcc::Visibility>) -> Option<crate::core::mvcc::Visibility> {
    if depth == 0 { kept() } else { None }
}

async fn run_per_row_write(
    with_recheck: String,
    key_only: String,
    database: &Arc<Database>,
    depth: usize,
    tx: TxId,
) -> Result<Vec<u8>, String> {
    // Folded as the wire folds a statement, which this generated one never
    // passes through: the recheck's substituted values meet literal
    // arithmetic (`v < 0.1234 * 100`), which only the fold computes at a
    // width a NUMERIC column compares with.
    let (with_recheck, key_only) = (fold_where_constants(&with_recheck), fold_where_constants(&key_only));
    // Each write by key reads its row as it is now and rechecks the
    // condition against it, PostgreSQL's recheck of a row changed since the
    // join read it: in a scope of its own, so it is not scanned under the
    // join's snapshot, which the statement's scope holds.
    let response = match recognize_query_purpose(&with_recheck) {
        Some(purpose) => {
            crate::core::rql::statement_snapshot::scope(Box::pin(execute_with_context(
                purpose,
                database.clone(),
                depth,
                tx,
            )))
            .await
        }
        None => {
            let mut refused = vec![3u8];
            refused.extend_from_slice(format!("could not parse the rewritten {}", sql_literal(&with_recheck)).as_bytes());
            refused
        }
    };
    if response.first() != Some(&3u8) || with_recheck == key_only {
        return Ok(response);
    }
    let text = String::from_utf8_lossy(&response[1..]).into_owned();
    if per_row_error_is_final(&text) {
        return Ok(response);
    }
    // Whether the failure was the recheck's or the write's own (a CHECK, a
    // key) is told by the write by key alone: its error is the statement's.
    // If it succeeds the recheck was what failed, and the statement fails
    // with it; the row it wrote is a version of this statement's
    // transaction, which that failure aborts (an `UPDATE … FROM` never
    // overwrites in place, `forbid_in_place`).
    let keyed = match recognize_query_purpose(&key_only) {
        Some(purpose) => {
            crate::core::rql::statement_snapshot::scope(Box::pin(execute_with_context(
                purpose,
                database.clone(),
                depth,
                tx,
            )))
            .await
        }
        None => return Ok(response),
    };
    if keyed.first() == Some(&3u8) {
        return Ok(keyed);
    }
    Err(format!(
        "the join condition could not be rechecked on the row being written ({text}), and writing \
         it without the recheck is not supported"
    ))
}

/// The literal a value read by an unnormalised join (`run_subquery_scaled`
/// with `normalize` false) is written back as: a sixteen-byte decimal at the
/// scale `scales` gives its output slot, exactly, every place kept; anything
/// else as `value_to_literal` renders it. Rendering such a cell as if at the
/// engine's nine places would misplace its point, and normalising it first
/// rounded away the places past nine.
fn joined_literal(cell: &crate::core::row::row::Column, scales: Option<&[u32]>, at: usize) -> Option<String> {
    let bytes = cell.data.into_slice();
    if cell.column_type == crate::core::db_type::DbType::DECIMAL {
        if let (Some(&scale), Some((v, None))) =
            (scales.and_then(|s| s.get(at)), crate::core::value_codec::split_decimal_cell(bytes))
        {
            return Some(crate::core::value_codec::format_decimal_scaled(v, scale));
        }
    }
    crate::core::rql::subquery::value_to_literal(&cell.column_type, bytes)
}

/// [`joined_literal`] for a value an assigned expression reads: a scaled
/// decimal is typed at its scale, `CAST(-1.50 AS NUMERIC(38, 2))`, since the
/// bare `-1.50` is a literal at the engine's scale and `s.d::text` on a
/// NUMERIC(6,2) stored '-1.5' where PostgreSQL stores '-1.50'. Only here:
/// a key predicate or a recheck compares by value, which no scale changes,
/// and a CAST there costs a point lookup its literal. Past the engine's
/// nine places a cast is refused, so those stay bare (and the expression
/// reads them at nine places, as before).
fn assigned_literal(cell: &crate::core::row::row::Column, scales: Option<&[u32]>, at: usize) -> Option<String> {
    let literal = joined_literal(cell, scales, at)?;
    let scale = scales.and_then(|s| s.get(at)).copied().unwrap_or(0);
    let typed = cell.column_type == crate::core::db_type::DbType::DECIMAL
        && (1..=crate::core::value_codec::DECIMAL_SCALE).contains(&scale)
        && literal.bytes().all(|b| b.is_ascii_digit() || b == b'-' || b == b'.');
    Some(if typed { format!("CAST({literal} AS NUMERIC(38, {scale}))") } else { literal })
}

/// Whether a per-row write's error ends `run_per_row_write` rather than
/// letting it retry by key alone without the join's recheck. The fallback is
/// for a recheck the single-relation `UPDATE`/`DELETE` cannot evaluate; every
/// error here is about the row or the statement, and the key-only write
/// would meet it too, or worse, succeed once the cause has passed and write
/// a row the recheck would have refused.
fn per_row_error_is_final(text: &str) -> bool {
    let marker = crate::core::row::table::Table::<
        crate::core::storage_providers::file_sync::LocalStorageProvider,
    >::CONFLICT_RETRYABLE;
    [
        "could not serialize",
        marker,
        // `refuse_write_to_locked_rows`.
        "is locked by transaction",
        // A lock wait's ends: NOWAIT, the timeout, a deadlock.
        "could not obtain lock",
        "timed out waiting",
        "deadlock detected",
        // `statement_timeout`, a client gone, `lock_timeout`.
        "canceling statement",
    ]
    .iter()
    .any(|m| text.contains(m))
}

#[cfg(test)]
mod per_row_error_tests {
    use super::per_row_error_is_final;

    /// A lock, a lock wait's end, a deadlock, a cancellation and a conflict
    /// each end the per-row write; only a recheck the single-relation write
    /// could not evaluate falls back to the key. The write that fell back on
    /// a lock error landed without its recheck once the holder committed.
    #[test]
    fn lock_deadlock_cancel_and_conflict_errors_never_fall_back_to_the_key() {
        for text in [
            "row of 't' is locked by transaction 9; the statement that locked it must finish first",
            "could not obtain lock on a row of 't': held by another transaction",
            "timed out waiting 10s for a lock on a row of 't' held by transaction 9",
            "deadlock detected: transaction 5 waits for transaction 9",
            "canceling statement due to statement timeout",
            "canceling statement due to lock timeout",
            "canceling statement because the client closed the connection",
            "could not serialize access due to concurrent update",
        ] {
            assert!(per_row_error_is_final(text), "{text}");
        }
        assert!(!per_row_error_is_final("column 'q.x' does not exist"));
    }

    /// What computes in a RETURNING list, and so may fail after the write.
    #[test]
    fn returning_can_fail_reads_items_not_words() {
        use super::returning_can_fail as f;
        for sql in [
            "UPDATE t SET n = 5 RETURNING n /(n-5)",
            "UPDATE t SET n = 5 RETURNING id, n /(m)",
            "UPDATE t SET n = 5 RETURNING 10/(n-5)",
            "UPDATE t SET n = 5 RETURNING n + 1 AS x",
            "UPDATE t SET n = 5 RETURNING n AND m",
            "UPDATE t SET n = 5 RETURNING n IS NULL",
            "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET n = 5 RETURNING n /(m)",
        ] {
            assert!(f(sql), "{sql}");
        }
        for sql in [
            "UPDATE t SET n = 5",
            "UPDATE t SET n = 5 RETURNING *",
            "UPDATE t SET n = 5 RETURNING id, n",
            "UPDATE t SET n = 5 RETURNING t.id, n AS m, n m, \"N\" \"x\"",
        ] {
            assert!(!f(sql), "{sql}");
        }
    }

    async fn exec(db: &rclite::Arc<crate::core::database::Database>, sql: &str) -> Vec<u8> {
        let purpose = super::recognize_query_purpose(sql).expect("parses");
        super::execute_with_sync(purpose, db.clone(), true).await
    }

    /// A recheck that cannot run is an error, never a write by key alone;
    /// a write that fails by itself still fails with its own error. The
    /// statement's abort takes back the key-only write the check made.
    ///
    /// Mutation: `run_per_row_write` returning the key-only write's response
    /// (the old fallback) makes the first call succeed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_recheck_that_cannot_run_is_an_error_not_a_write_by_key() {
        let db = rclite::Arc::new(crate::core::database::Database::new_with_backend("recheck_cannot_run", true).await);
        assert_eq!(exec(&db, "CREATE TABLE rr (id BIGINT PRIMARY KEY, v BIGINT, CHECK (v < 100))").await[0], 0);
        assert_eq!(exec(&db, "INSERT INTO rr (id, v) VALUES (1, 5)").await[0], 0);
        let tx = db.transactions.begin();
        let broken = "UPDATE rr SET v = 7 WHERE id = 1 AND (nosuch_column < 3)".to_string();
        let keyed = "UPDATE rr SET v = 7 WHERE id = 1".to_string();
        let got = super::run_per_row_write(broken.clone(), keyed, &db, 0, tx).await;
        let e = got.expect_err("an unrunnable recheck wrote by key");
        assert!(e.contains("could not be rechecked"), "{e}");
        db.abort_transaction(tx);
        // The write's own failure is reported as itself.
        let tx = db.transactions.begin();
        let failing = "UPDATE rr SET v = 500 WHERE id = 1".to_string();
        let got = super::run_per_row_write(broken.replace("v = 7", "v = 500"), failing, &db, 0, tx).await;
        let response = got.expect("the write's own error is a response, not the recheck's");
        assert_eq!(response.first(), Some(&3u8));
        assert!(String::from_utf8_lossy(&response[1..]).to_lowercase().contains("check"), "{:?}", String::from_utf8_lossy(&response));
        db.abort_transaction(tx);
        // Still 5: the aborted key-only write left nothing.
        let out = exec(&db, "DELETE FROM rr WHERE id = 1 AND v = 5").await;
        assert_eq!(out.first(), Some(&0u8), "{:?}", String::from_utf8_lossy(&out));
        assert_eq!(u64::from_le_bytes(out[1..9].try_into().unwrap()), 1, "the row's value changed");
    }
}

/// Whether `sql`'s RETURNING list computes something, which can fail
/// (`10/(v-7)`, 22012) after the write it reports on. An autocommit write
/// may overwrite in place, which the failure's abort cannot undo: the
/// statement failed and stored its value. Such a statement writes new
/// versions instead (`forbid_in_place`). A list of columns and `*` cannot
/// fail, and keeps the overwrite, which is what ORM upserts send.
fn returning_can_fail(sql: &str) -> bool {
    let Some((_, list)) = crate::core::rql::lexer_ir::returning_split(sql) else {
        return false;
    };
    // A plain identifier, or a quoted one.
    fn identifier(word: &str) -> bool {
        if let Some(inner) = word.strip_prefix('"') {
            return inner.ends_with('"') && inner.len() > 1;
        }
        !word.is_empty()
            && !word.starts_with(|c: char| c.is_ascii_digit())
            && word.chars().all(|c| c.is_alphanumeric() || c == '_')
    }
    // Words that continue an expression rather than name an alias.
    const OPERATOR_WORDS: &[&str] = &[
        "AND", "OR", "NOT", "IS", "IN", "LIKE", "ILIKE", "BETWEEN", "COLLATE", "SIMILAR", "AT", "OVERLAPS",
        "ISNULL", "NOTNULL", "ESCAPE",
    ];
    crate::core::rql::lexer_ct::split_definitions(list).iter().any(|item| {
        let item = item.trim().trim_end_matches(';').trim();
        // `value AS alias`, `value alias` or `value`. Anything else, and any
        // value that is not a column or `*`, computes. A second word is an
        // alias only when it is an identifier and no operator word: `n
        // /(n-5)` is one expression (and failed with 22012 after storing n).
        let value = match crate::core::rql::join::find_keyword(item, "AS", 0) {
            Some(at) => {
                let alias = item[at + 2..].trim();
                if !identifier(alias) {
                    return true;
                }
                item[..at].trim()
            }
            None => {
                let words: Vec<&str> = item.split_whitespace().collect();
                match words.as_slice() {
                    [value] => *value,
                    [value, alias]
                        if identifier(alias)
                            && !OPERATOR_WORDS.iter().any(|w| w.eq_ignore_ascii_case(alias)) =>
                    {
                        *value
                    }
                    _ => return true,
                }
            }
        };
        !(value == "*"
            || value.split('.').all(|part| part == "*" || identifier(part)))
    })
}

/// Where one assigned value comes from once the join has run.
enum AssignedValue {
    /// The join projected the value directly; this is its output slot.
    Slot(usize),
    /// The join could not compute it, so it is carried through as written and
    /// evaluated by the per-row `UPDATE` this path generates — the same path a
    /// plain `UPDATE` takes, and the only one that can see whether a column is
    /// NULL. The columns it reads are still projected, so the join's output
    /// slots line up with the assignments beside it. The source's columns it
    /// reads (`source_refs`, as written) are projected from `source_at` on,
    /// and substituted with each joined row's values before it is written.
    Computed {
        text: String,
        source_refs: Vec<String>,
        source_at: usize,
    },
}

/// Turns a conflicting `INSERT … ON CONFLICT … DO UPDATE` into the `UPDATE` it
/// asks for.
///
/// `EXCLUDED.<column>` names the value the insert would have written, so the
/// insert's own assignments are what resolve it. The conflicting row is found
/// by the arbiter columns, or the columns of the constraint `ON CONSTRAINT`
/// names, or — when neither was written — by whichever unique column the
/// statement supplies a value for.
///
/// On a partitioned table the generated `UPDATE` routes like any other,
/// through `execute_partitioned_update` (S18). Its `WHERE` pins every
/// partition-key column, because every unique key holds every key column
/// (`docs/partitioning.md` §1.5) and the target was checked to name them all
/// (`check_partitioned_conflict_target`), so it reaches exactly the
/// partition the insert collided in. A `SET` that would move the row's point,
/// `SET tenant_id = EXCLUDED.tenant_id + 1`, is refused there by
/// `assignments_keep_point` with 0A000 (a key change, phase 6) before any
/// partition is written; `SET tenant_id = EXCLUDED.tenant_id` substitutes
/// the literal the `WHERE` pins, so it keeps the point and runs.
///
/// Kept out of `execute_with_context`'s frame: that function is one large
/// match, so its future is the sum of every arm's locals.
#[inline(never)]
async fn apply_conflict_update(
    database: &Arc<Database>,
    table_name: &str,
    insert_sql: &str,
    spec: &crate::core::rql::lexer_s1::ConflictUpdate,
    depth: usize,
    tx: TxId,
) -> Vec<u8> {
    crate::owner_probe!(crate::core::mvcc::owner::probe::Site::ConflictUpdate, tx, database);
    let inserted =
        crate::core::rql::lexer_ir::assignments_of_insert(insert_sql).unwrap_or_default();
    if inserted.is_empty() {
        return ddl_error("ON CONFLICT DO UPDATE needs the inserted values to resolve EXCLUDED");
    }

    // The write target, ROW EXCLUSIVE (held from the INSERT that conflicted).
    let target = match database.table_for(table_name, owner(), TableMode::RowExclusive).await {
        Ok(target) => target,
        Err(e) => return ddl_error(&e),
    };

    // The proposed row as the column stores it: a key written `1.254` for a
    // NUMERIC(10,2) conflicted as 1.25, and the DO UPDATE then looked the
    // row up by `k = 1.254`, matched nothing and reported success — a lost
    // write. `EXCLUDED.k` is the stored value too, as in PostgreSQL.
    let inserted: Vec<(String, String)> = match target.clone() {
        Some(entry) => inserted
            .into_iter()
            .map(|(name, literal)| {
                let canonical = entry
                    .schema
                    .fields
                    .iter()
                    .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&name))
                    .and_then(|f| stored_numeric_literal(&literal, f).or_else(|| stored_text_literal(&literal, f)));
                (name, canonical.unwrap_or(literal))
            })
            .collect(),
        None => inserted,
    };

    let value_of = |column: &str| -> Option<String> {
        inserted
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, literal)| literal.clone())
    };

    // Which columns identify the row. An arbiter is used as written; without
    // one, the unique columns the insert supplies a value for are the only
    // thing it can have collided on.
    let keys: Vec<String> = if !spec.arbiter.is_empty() {
        spec.arbiter.clone()
    } else if let Some(name) = &spec.constraint {
        let entry = target.clone();
        let Some(table) = entry.as_ref().clone() else {
            return ddl_error(&format!("relation '{}' does not exist", table_name));
        };
        match constraint_columns(&table.schema, table_name, name) {
            Some(columns) => columns,
            None => {
                return ddl_error(&format!(
                    "constraint \"{}\" for table \"{}\" does not exist",
                    name, table_name
                ))
            }
        }
    } else {
        let entry = target.clone();
        let Some(table) = entry.as_ref().clone() else {
            return ddl_error(&format!("relation '{}' does not exist", table_name));
        };
        table
            .schema
            .fields
            .iter()
            .filter(|f| f.is_unique && !f.is_deleted)
            .map(|f| f.name.clone())
            .filter(|name| value_of(name).is_some())
            .collect()
    };
    if keys.is_empty() {
        return ddl_error(
            "ON CONFLICT DO UPDATE could not tell which row conflicted: name the \
             constraint's columns, as in `ON CONFLICT (id) DO UPDATE …`",
        );
    }

    let mut predicates: Vec<String> = Vec::with_capacity(keys.len() + 1);
    for key in &keys {
        let Some(literal) = value_of(key) else {
            return ddl_error(&format!(
                "ON CONFLICT names column '{}', which this INSERT does not write",
                key
            ));
        };
        predicates.push(format!("{} = {}", key, literal));
    }

    // When the arbiter is a partial unique index, the row that conflicted is
    // the one under its predicate. The key alone also matches every row
    // outside it — the soft-deleted copies of an email — and the update would
    // have rewritten all of them. An ordinary key over the same columns
    // admits one row per value, so its presence makes the predicate moot.
    if let Some(entry) = target.clone() {
        let same_columns = |columns: &[String]| {
            columns.len() == keys.len()
                && columns.iter().all(|c| keys.iter().any(|k| k.eq_ignore_ascii_case(c)))
        };
        let schema = &entry.schema;
        let full_key = schema.unique_keys.iter().any(|k| {
            !k.is_deleted && k.predicate.is_none() && same_columns(&k.columns)
        }) || (keys.len() == 1
            && (schema.primary_key.as_deref().is_some_and(|p| p.eq_ignore_ascii_case(&keys[0]))
                || schema
                    .fields
                    .iter()
                    .any(|f| f.is_unique && !f.is_deleted && f.name.eq_ignore_ascii_case(&keys[0]))));
        if !full_key {
            if let Some(predicate) = schema
                .unique_keys
                .iter()
                .find(|k| !k.is_deleted && same_columns(&k.columns))
                .and_then(|k| k.predicate.as_deref())
            {
                predicates.push(format!("({})", predicate));
            }
        }
    }

    // `EXCLUDED.<column>` of a column the INSERT does not write is the value
    // the row would have taken: its DEFAULT, or NULL. It was refused (XX000,
    // "has no value"). A default that is not a constant would be another
    // value when computed again here (a sequence's next number), so a
    // reference to one is refused by name.
    let mut inserted = inserted;
    if let Some(table) = target.as_ref().clone() {
        let schema = &table.schema;
        for field in schema.fields.iter().filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name)) {
            if inserted.iter().any(|(name, _)| name.eq_ignore_ascii_case(&field.name)) {
                continue;
            }
            let value = if crate::core::rql::generated::expression_for(&field.name, &schema.generated).is_some() {
                None
            } else {
                match crate::core::rql::column_default::expression_for(&field.name, &schema.defaults) {
                    None => Some("NULL".to_string()),
                    // A constant — a literal, possibly cast (`'(none)'`,
                    // `0`, `'x'::text`) — is the value the row took. Anything
                    // the default computes (`nextval(…)`, `CURRENT_TIMESTAMP`)
                    // would be another value computed again here.
                    Some(expression) if default_is_constant(expression) => Some(format!("({})", expression.trim())),
                    Some(_) => None,
                }
            };
            inserted.push((
                field.name.clone(),
                value.unwrap_or_else(|| format!("{}{}", EXCLUDED_UNAVAILABLE, field.name)),
            ));
        }
    }

    // The conflicting row is named by the table, by it in the default schema
    // (Prisma: `"public"."t"."id"`) or by the INSERT's alias (Ecto: `INSERT
    // INTO t AS t0 … SET v = t0.v + 1`); the generated UPDATE names it by
    // none, so each is taken off. `EXCLUDED` is read however it is quoted
    // (sea-orm: `"excluded"."v"`).
    // Aliased, the table is known by its alias alone (`INSERT INTO t AS x …
    // SET v = t.v` is 42P01 in PostgreSQL); `excluded` is the proposed row.
    let alias = insert_alias(insert_sql);
    let known = crate::core::sql_scan::relation_qualifiers(table_name, alias.as_deref());
    let known: Vec<&str> = known.iter().map(String::as_str).collect();
    let mut allowed = known.clone();
    allowed.push("excluded");
    // One scan: refused if unknown, `excluded` respelled, the table's own
    // qualifiers taken off; then each `EXCLUDED.<column>` becomes its value
    // (a literal or a constant default, which names no column to strip).
    let resolve = |text: &str| -> Result<String, String> {
        let text = crate::core::sql_scan::resolve_qualifiers(text, &allowed, "excluded", "EXCLUDED", &known)
            .map_err(|q| crate::core::sql_scan::missing_from_entry(&q))?;
        let text = substitute_excluded(&text, &inserted)?;
        Ok(fold_constants_in_predicate(&text))
    };

    let assignments = match resolve(&spec.assignments) {
        Ok(a) => a,
        Err(e) => return ddl_error(&e),
    };
    if let Some(condition) = &spec.where_clause {
        match resolve(condition) {
            Ok(c) => predicates.push(format!("({})", c)),
            Err(e) => return ddl_error(&e),
        }
    }

    // `RETURNING` was moved back onto the INSERT by the parser, so it is read
    // from there and carried onto the UPDATE.
    // Its qualifiers are the INSERT's (`RETURNING x.id` under `AS x`), which
    // the UPDATE, naming the table bare, does not have: taken off as the
    // SET's are.
    let returning = crate::core::rql::join::find_keyword(insert_sql, "RETURNING", 0)
        .map(|at| crate::core::sql_scan::strip_qualifiers(insert_sql[at..].trim(), &known));

    let statement = format!(
        "UPDATE {} SET {} WHERE {}{}",
        table_name,
        assignments,
        predicates.join(" AND "),
        match &returning {
            Some(r) => format!(" {}", r),
            None => String::new(),
        }
    );

    let Some(purpose) = recognize_query_purpose(&statement) else {
        return ddl_error(&format!("could not parse the rewritten {}", sql_literal(&statement)));
    };
    Box::pin(execute_with_context(purpose, database.clone(), depth, tx)).await
}

/// The columns of the unique constraint `name` on `schema`'s table, for `ON
/// CONFLICT ON CONSTRAINT name`: a table-level key (a composite `PRIMARY KEY`
/// among them, named `<table>_pkey`), a single-column `PRIMARY KEY`
/// (`<table>_pkey`), or an inline `UNIQUE` column (`<table>_<column>_key`).
/// `None` when no live constraint has that name.
fn constraint_columns(
    schema: &crate::core::row::schema::TableSchema,
    table_name: &str,
    name: &str,
) -> Option<Vec<String>> {
    if let Some(key) = schema
        .unique_keys
        .iter()
        .find(|k| !k.is_deleted && k.name.eq_ignore_ascii_case(name))
    {
        return Some(key.columns.clone());
    }
    if name.eq_ignore_ascii_case(&crate::core::rql::unique_key::pkey_name(table_name)) {
        if let Some(pk) = schema.primary_key.as_deref() {
            return Some(vec![pk.to_string()]);
        }
    }
    schema
        .fields
        .iter()
        .filter(|f| f.is_unique && !f.is_deleted)
        .find(|f| {
            let default = crate::core::rql::unique_key::UniqueKey::default_name(
                table_name,
                std::slice::from_ref(&f.name),
            );
            default.eq_ignore_ascii_case(name)
        })
        .map(|f| vec![f.name.clone()])
}

/// Takes the locks a `SELECT … FOR UPDATE` asks for, and reports which rows
/// survive.
///
/// Returns the rows the statement should return: all of them for `FOR UPDATE`
/// and `NOWAIT` (which either lock everything or fail), and only the ones this
/// transaction could lock for `SKIP LOCKED`.
///
/// The lock key is the table's identifying key ([`identifying_key`]), whose
/// cells the scan fetched for every row whether or not the select list names
/// them (`keys`, one entry per row, in the key's order, at their declared
/// scale). A table without one cannot be locked row-by-row here, and that is
/// refused rather than silently locking nothing — a claim query that locks
/// nothing hands every worker the same rows.
///
/// When a lock this transaction did not already hold was taken and a
/// transaction `since` could not see has committed since, the rows may be
/// older than their latest versions: `Err(LOCKED_ROWS_STALE)`, and
/// [`execute_query_rows`] scans again.
#[allow(clippy::too_many_arguments)]
async fn lock_selected_rows(
    database: &Arc<Database>,
    table_name: &str,
    rows: Vec<crate::core::row::row::Row>,
    keys: Vec<Vec<crate::core::row::row::Column>>,
    key: Option<&IdentifyingKey>,
    lock: crate::core::rql::row_lock::RowLock,
    // How many rows the statement will actually return. Locking stops there:
    // PostgreSQL locks the rows it hands back and no others, and locking the
    // whole scan would let one worker reserve the entire queue while claiming
    // two rows of it.
    wanted: Option<usize>,
    since: Option<&crate::core::mvcc::Visibility>,
    tx: TxId,
) -> Result<Vec<crate::core::row::row::Row>, String> {
    use crate::core::mvcc::row_locks::Acquired;
    use crate::core::rql::row_lock::LockWait;

    if rows.is_empty() {
        return Ok(rows);
    }

    // ROW EXCLUSIVE on the table, a partitioned one under its logical name,
    // which every partition's rows are locked by (§6.2).
    if database.table_for(table_name, owner(), TableMode::RowExclusive).await?.is_none() {
        return Err(format!("relation '{}' does not exist", table_name));
    }
    let Some(key) = key else {
        return Err(format!(
            "FOR UPDATE needs a unique column on '{}' to identify the rows it locks",
            table_name
        ));
    };
    if keys.len() != rows.len() {
        return Err(format!("internal error: {} rows but {} row keys to lock them by", rows.len(), keys.len()));
    }

    // Under REPEATABLE READ or SERIALIZABLE the snapshot is pinned, and
    // scanning again would see the same versions: a row newly locked whose
    // latest committed version is not the one the snapshot saw fails the
    // statement with 40001, as PostgreSQL's does.
    let pinned = database.transactions.has_pinned_snapshot(database.row_locks.txn_of(tx));
    let mut kept = Vec::with_capacity(rows.len());
    let mut newly = false;
    for (row, cells) in rows.into_iter().zip(keys) {
        if let Some(cap) = wanted {
            if kept.len() >= cap {
                break;
            }
        }
        let cells: Vec<&crate::core::row::row::Column> = cells.iter().collect();
        let row_key = key.row_key(&cells)?;
        let held = database.row_locks.holds(table_name, &row_key, tx);

        let outcome = acquire_row_lock(
            database,
            table_name,
            &row_key,
            lock.strength,
            matches!(lock.behaviour, LockWait::Wait),
            true,
            tx,
        )
        .await?;

        match outcome {
            Acquired::Yes => {
                if pinned && !held {
                    if let Some(since) = since {
                        if changed_since_pinned(database, table_name, key, &cells, since, tx).await? {
                            return Err(
                                "could not serialize access due to concurrent update".to_string(),
                            );
                        }
                    }
                }
                newly |= !held;
                kept.push(row);
            }
            Acquired::Held(owner) => match lock.behaviour {
                // The row belongs to someone else's claim; leaving it out is
                // exactly what the clause asks for.
                LockWait::Skip => continue,
                LockWait::NoWait => {
                    return Err(format!(
                        "could not obtain lock on a row of '{}': held by another transaction",
                        table_name
                    ));
                }
                LockWait::Wait => {
                    return Err(format!(
                        "timed out waiting {}s for a lock on a row of '{}' held by transaction {}",
                        LOCK_WAIT_TIMEOUT.as_secs(),
                        table_name,
                        owner
                    ));
                }
            },
        }
    }
    if newly && !pinned && since.is_some_and(|since| committed_since(database, since)) {
        return Err(LOCKED_ROWS_STALE.to_string());
    }
    Ok(kept)
}

/// Whether the row these key cells name has a latest committed version other
/// than the one `pinned` (a pinned snapshot) sees, or none: read by key
/// under both, in row mode, and compared by position, which names a version.
/// Only that row: a commit elsewhere in the table is no conflict.
async fn changed_since_pinned(
    database: &Arc<Database>,
    table_name: &str,
    key: &IdentifyingKey,
    cells: &[&crate::core::row::row::Column],
    pinned: &crate::core::mvcc::Visibility,
    tx: TxId,
) -> Result<bool, String> {
    let Some(relation) = database.relation_for(table_name, owner(), TableMode::RowExclusive).await? else {
        return Ok(false);
    };
    let named = key.predicate(cells)?;
    let fields: Vec<crate::core::row::schema::SchemaField> = relation.schema().fields.to_vec();
    let calc = crate::core::row::schema::SchemaCalculator::default();
    let fetch = crate::core::row::row::RowFetch {
        columns_fetching_data: key
            .fields
            .iter()
            .map(|f| {
                let (column_offset, schema_id) = calc.calculate_schema_offset(&f.name, &fields);
                crate::core::row::row::ColumnFetchingData {
                    column_offset,
                    column_type: f.db_type.physical(),
                    size: f.size,
                    schema_id,
                }
            })
            .collect(),
    };
    let now = crate::core::mvcc::Visibility::for_snapshot(
        database.transactions.snapshot_now(tx),
        database.transactions.clone(),
    );
    let read = |visibility: crate::core::mvcc::Visibility| {
        crate::core::rql::row_source::RowSource::rows_as_of(
            relation.clone(),
            &named,
            &fields,
            fetch.clone(),
            visibility,
            tx,
        )
    };
    let seen = read(pinned.clone()).await.map_err(|e| e.to_string())?;
    let latest = read(now).await.map_err(|e| e.to_string())?;
    Ok(seen.first().map(|r| r.position) != latest.first().map(|r| r.position))
}

/// Asks for `strength` on the row `key` ([`IdentifyingKey::row_key`]) of
/// `table_name`, a partitioned table's logical name, and, when `wait`, waits
/// for its holder: `Held` comes back only without `wait`, or once
/// [`LOCK_WAIT_TIMEOUT`] has passed.
///
/// One attempt, then act on the answer. Waiting is a loop with a bound rather
/// than an unbounded park: a lock whose holder never finishes would otherwise
/// hang the connection with nothing to report. Past `deadlock_timeout` the
/// wait is an edge of the wait-for graph that table locks share, keyed as the
/// lock is, so a cycle through it and a table lock ends in one 40P01.
async fn acquire_row_lock(
    database: &Arc<Database>,
    table_name: &str,
    key: &[u8],
    strength: crate::core::rql::row_lock::LockStrength,
    wait: bool,
    // A locking read (`FOR UPDATE`/`FOR SHARE`) rather than a writer taking
    // its rows' locks: it also waits out a fast-path writer of the table,
    // which writes without row locks (`RowLocks::mark_row_writes`).
    locker: bool,
    tx: TxId,
) -> Result<crate::core::mvcc::row_locks::Acquired, String> {
    use crate::core::mvcc::row_locks::Acquired;

    // A holder counts as live until it is retired, as a writer's mark does
    // below (`TransactionManager::unfinished`). Taking a dead holder's row
    // frees everything the holder holds, in every lock space
    // (`RowLocks::acquire`): a holder taken for dead in its commit's window
    // lost its referenced-key locks with it, and a child insert that came
    // next found its parent's key free and the parent still showing
    // (`row_lock_handover_tests`).
    let live = |candidate: TxId| database.transactions.unfinished(candidate);
    // A writer that marked the row counts as live until it is retired from
    // the running set, not only while its owner is: a commit records its
    // outcome first and retires after (`TransactionManager::commit`), and a
    // snapshot taken in between still lists it as running, so it does not
    // see the writer's version. A locker that ignored the mark in that
    // window kept the row, and a later round of `execute_query_rows` scanned
    // at such a snapshot, found the row locked by this transaction already,
    // did not ask `committed_since` about it, and returned the version the
    // writer had just replaced: the `SKIP LOCKED` queue handed a finished
    // job to a second worker (`row_lock_handover_tests`). Every write of a
    // row marks it, a lock holder's included, so the mark is what is asked;
    // a holder that wrote nothing changed nothing a locker could read stale.
    let owner_live = |o: TxId| database.transactions.owner_live(o) || database.transactions.is_running(o);
    let already = locker && database.row_locks.holds(table_name, key, tx);
    // Taken first, then the fast-path writers asked about: the order
    // `mark_row_writes` relies on. Held by the writer's owner until it ends.
    let acquire = || match database.row_locks.acquire(table_name, key, tx, strength, &live) {
        Acquired::Yes if locker => match database.row_locks.row_writer_on(table_name, key, tx, &owner_live) {
            Some(writer) => Acquired::Held(writer),
            None => Acquired::Yes,
        },
        outcome => outcome,
    };
    let mut outcome = acquire();
    if !wait {
        // Not waiting, so not keeping a lock taken on a row a fast-path
        // writer may yet change.
        if locker && !already && matches!(outcome, Acquired::Held(_)) {
            database.row_locks.release_row(table_name, key, tx);
        }
        return Ok(outcome);
    }
    let started = std::time::Instant::now();
    let deadline = started + LOCK_WAIT_TIMEOUT;
    let _waiting = database.row_locks.waiting_guard(tx);
    while let Acquired::Held(_) = outcome {
        // The statement's own bound comes first: `statement_timeout` and a
        // client that has gone both end the wait, as the same error a
        // cancelled scan reports. An error and never the rows locked so far —
        // a short claim would read as a complete one.
        if let Some(reason) = crate::core::cancel::tripped() {
            return Err(reason.eval_error().as_str().to_string());
        }
        if let Some(deadlock) = deadlock_check(
            database,
            tx,
            started,
            || {
                let mut blockers = database.row_locks.row_blockers(table_name, key, tx, strength, &live);
                if let Acquired::Held(writer) = &outcome {
                    if !blockers.contains(writer) {
                        blockers.push(*writer);
                    }
                }
                blockers
            },
            &live,
        ) {
            return Err(deadlock);
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        outcome = acquire();
    }
    Ok(outcome)
}

/// Takes the table locks a `LOCK TABLE` asks for, in the mode the statement
/// names mapped to one of the four (never weaker; `PgLockMode::table_mode`),
/// for the block's owner, held until the block ends.
///
/// Each relation is waited for through the table locks' one wait
/// (`Database::lock_table`'s): no bound of its own under `lock_timeout = 0`,
/// 55P03 past a positive one, 57014 on cancellation, 40P01 to a cycle's
/// victim (an S or AE requester, which `LOCK TABLE` in those modes is, comes
/// before any reader). `NOWAIT` asks once and fails with 55P03 (`could not
/// obtain lock`) rather than queue. A partitioned table is locked by its
/// logical name, which is what every statement on it locks.
#[inline(never)]
async fn execute_lock_table(
    database: &Arc<Database>,
    statement: crate::core::rql::row_lock::LockTable,
    tx: TxId,
) -> Vec<u8> {
    use crate::core::mvcc::table_locks::{Request, WaitRole};

    // PostgreSQL refuses this outside a transaction block, because the lock
    // would be released the instant the statement ended and the caller would be
    // left believing it held one. Same answer here, and for the same reason.
    if tx == crate::core::mvcc::transaction::FROZEN_TX {
        return ddl_error("LOCK TABLE can only be used in transaction blocks");
    }
    let Some(owner) = crate::core::mvcc::owner::ensure() else {
        return ddl_error("LOCK TABLE can only be used in transaction blocks");
    };
    let mode = statement.mode.table_mode();

    for name in &statement.tables {
        if !database.has_table(name) && !database.is_partitioned(name) {
            return ddl_error(&format!("relation '{}' does not exist", name));
        }
        let key = database.lock_key(name);
        let locks = database.row_locks.table_locks();
        if statement.nowait {
            if !locks.try_acquire(owner.owner, &key, mode) {
                return ddl_error(&format!("could not obtain lock on relation \"{}\"", name));
            }
            continue;
        }
        let wait = match locks.request(owner.owner, &key, mode, WaitRole::Other) {
            Request::Granted => continue,
            Request::Queued(wait) => wait,
        };
        let timeout = crate::core::cancel::lock_timeout();
        let cancelled = || crate::core::cancel::tripped().is_some();
        if let Err(e) = locks
            .wait_for(&wait, timeout, &cancelled, &|o| database.row_locks.table_deadlock_check(o))
            .await
        {
            return ddl_error(&crate::core::database::table_for::lock_error_message(e, &key, mode));
        }
    }

    ddl_ok(0)
}

/// The `WHERE` clause of an `UPDATE`/`DELETE`, with `RETURNING` removed and any
/// relation alias stripped from its column references.
///
/// Taking everything after `WHERE` swept `RETURNING j.id` into the predicate,
/// which reached the tokenizer as the identifier `RETURNING` and panicked the
/// worker — dropping the client's connection rather than reporting anything.
fn predicate_of(sql: &str, alias: Option<&str>) -> String {
    let head = match crate::core::rql::lexer_ir::returning_split(sql) {
        Some((before, _)) => before,
        None => sql,
    };
    let Some(at) = crate::core::rql::join::find_keyword(head, "WHERE", 0) else {
        return String::new();
    };
    let condition = head[at + "WHERE".len()..].trim();

    // `j.id = 2` becomes `id = 2`: the lookup selects from the table itself and
    // never sees the alias the statement gave it.
    match alias {
        Some(a) if !a.is_empty() => strip_qualifier(condition, a),
        _ => condition.to_string(),
    }
}

/// Refuses a write that would put NULL in a `NOT NULL` column.
///
/// The constraint was recorded on the column and enforced nowhere: an `INSERT`
/// naming NULL, an `INSERT` omitting the column, and an `UPDATE` setting it to
/// NULL were all accepted. Worse than accepting a null, a NULL written into a
/// fixed-width column became that type's zero — so `INSERT … VALUES (NULL, …)`
/// stored `0` in an `INT NOT NULL` and reported success.
///
/// A column is nullable exactly when it has a `__null_` companion; one without
/// has nowhere to record that it is null, which is why the value silently
/// became a zero.
///
/// `omitted_counts` distinguishes the two callers: an `INSERT` must account for
/// every column, while an `UPDATE` only touches the ones it names.
fn check_not_null(
    table: &str,
    schema: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
    supplied: &[(String, String)],
    defaults: &[crate::core::rql::column_default::ColumnDefault],
    generated: &[crate::core::rql::generated::GeneratedColumn],
    omitted_counts: bool,
) -> Result<(), String> {
    use crate::core::rql::nullable;

    let violation = |column: &str| {
        format!(
            "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
            column, table
        )
    };

    for field in schema.iter() {
        if field.is_deleted || nullable::is_hidden(&field.name) {
            continue;
        }
        // A companion means the column can record a null, so it is nullable.
        if nullable::flag_field(&field.name, schema).is_some() {
            continue;
        }

        // What a column with no value written takes: its default, which is
        // NULL when it has none.
        let defaulted = || {
            crate::core::rql::column_default::expression_for(&field.name, defaults)
                .is_some_and(|e| !e.trim().eq_ignore_ascii_case("NULL"))
        };
        match supplied
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(&field.name))
        {
            Some((_, value)) => {
                // The keyword, or a constant that evaluates to NULL
                // (`NULLIF(1, 1)`), which was stored as the type's zero.
                if crate::core::rql::lexer_ir::written_null(value) {
                    return Err(violation(&field.name));
                }
                // `DEFAULT` written for the column, in an INSERT or an
                // UPDATE, is its default: NULL when it has none, which a
                // NOT NULL column refuses (23502). Taken as a value, it
                // stored `''` in a text key and `0` in an integer. A
                // generated column's DEFAULT is its expression.
                if value.trim().eq_ignore_ascii_case("DEFAULT")
                    && crate::core::rql::generated::expression_for(&field.name, generated).is_none()
                    && !defaulted()
                {
                    return Err(violation(&field.name));
                }
            }
            None if omitted_counts => {
                // Omitted is NULL unless a default supplies a value.
                let filled =
                    crate::core::rql::column_default::expression_for(&field.name, defaults)
                        .is_some_and(|e| !e.trim().eq_ignore_ascii_case("NULL"));
                if !filled {
                    return Err(violation(&field.name));
                }
            }
            None => {}
        }
    }
    Ok(())
}

/// Refuses a write to a row another transaction holds `FOR UPDATE`.
///
/// A lock is worth only what writers make of it. Without this check a
/// `SELECT … FOR UPDATE` would reserve rows that another connection could then
/// overwrite, which is the lost update the clause exists to prevent.
///
/// Costs nothing when nobody holds a row lock — the common case — because the
/// lock table's count is consulted first and is zero.
///
/// **A writer takes the lock, not just checks it, when any row lock exists**,
/// and holds it to the end of its transaction, as PostgreSQL's `UPDATE` and
/// `DELETE` do. So inside a `BEGIN` block, a row this statement writes stays
/// locked against another transaction's `FOR UPDATE` until `COMMIT` — that is
/// intended, not a leak. Under autocommit the transaction ends with the
/// statement and so does the lock. A savepoint's locks belong to its
/// transaction (see `RowLocks::for_transactions`); they used to be recorded
/// under the savepoint's own id, which nothing ever released.
///
/// When no row lock exists at all the writer takes none, to spare every
/// `UPDATE` and `DELETE` the key lookup. The case that leaves open — a `FOR
/// UPDATE` arriving while such a write is still uncommitted — is not a lost
/// update: first-updater-wins makes that transaction's own later write of the
/// row fail with `40001`. It is a difference in *when* the conflict surfaces,
/// as a serialization failure rather than a lock wait.
///
/// The key is [`identifying_key`], the one `FOR UPDATE` locks by, framed the
/// same way ([`IdentifyingKey::row_key`]). This used to pick only a
/// column-level `UNIQUE`, so on a table identified by its primary key or a
/// table-level `UNIQUE (col)` the writer looked for locks under no column at
/// all and overwrote rows another transaction held. A partitioned table is
/// checked under its logical name with its shell's key, as `FOR UPDATE`
/// through it locks, whichever partition the row is in.
///
/// A held row fails the write at once rather than being waited for, unlike
/// PostgreSQL, whose `UPDATE` waits and then re-evaluates its row. Waiting is
/// sound only for a writer that re-reads the row and recomputes what it
/// writes after the wait, and not every writer here does: `UPDATE … FROM`
/// computes its `SET` values from the join before it writes, so a writer
/// that waited for the holder's `COMMIT` then wrote those values over the
/// holder's (`SET v = t.v + s.w` stored 100 where 105 was right). A locking
/// read waits ([`acquire_row_lock`]); a writer only asks.
#[inline(never)]
async fn refuse_write_to_locked_rows(
    database: &Arc<Database>,
    table_name: &str,
    where_clause: &str,
    tx: TxId,
) -> Result<(), String> {
    use crate::core::mvcc::row_locks::Acquired;
    use crate::core::rql::row_lock::LockStrength;

    // A whole-table lock (`LOCK TABLE`) is the table-lock manager's: the
    // statement holds ROW EXCLUSIVE on its target (taken before this, and
    // waited for behind any SHARE or ACCESS EXCLUSIVE holder), so only row
    // locks are left to check.
    // With no row locked anywhere the rows' locks are not taken. Each row is
    // marked as being written just before it is stamped instead, and a locker
    // that comes after this check waits for that mark
    // (`RowLocks::mark_row_writes`, `RowMarker`).
    if database.row_locks.is_empty() {
        // Between the check and the write: where a locker's `FOR UPDATE`
        // could land unseen (compat `row_lock_fast_path.rs`).
        #[cfg(any(test, feature = "index_store_failpoints"))]
        let _ = crate::core::power_loss::failpoint::hit("test.row_write_fast_path", std::path::Path::new(table_name));
        return Ok(());
    }

    let key = {
        let entry = database.table_for(table_name, owner(), TableMode::RowExclusive).await?;
        let Some(table) = entry.as_ref().clone() else {
            return Ok(());
        };
        // `lock_selected_rows` refuses a table without one, so nothing here
        // can be locked row-by-row and nothing can conflict.
        let Some(key) = identifying_key(&table.schema) else {
            return Ok(());
        };
        key
    };

    let select = format!(
        "SELECT {} FROM {}{}{}",
        key.names().collect::<Vec<_>>().join(", "),
        table_name,
        if where_clause.trim().is_empty() {
            ""
        } else {
            " WHERE "
        },
        where_clause.trim()
    );
    // Unnormalised, so each key cell is at its declared scale, as the
    // locker's are: normalising rounded a `NUMERIC(20,12)` key to nine places
    // and the writer met no lock.
    let (rows, _) = run_subquery_scaled(&select, database, tx, SubqueryNeed::All, false).await?;

    for row in &rows {
        let cells: Vec<&crate::core::row::row::Column> = row.columns.iter().collect();
        if cells.len() < key.fields.len() {
            continue;
        }
        // A row whose key holds a NULL cannot have been locked: `FOR UPDATE`
        // refuses to name it.
        let Ok(row_key) = key.row_key(&cells[..key.fields.len()]) else {
            continue;
        };
        // Asked for, not merely inspected: this transaction is about to write
        // the row, so it should hold the lock while it does.
        if let Acquired::Held(owner) =
            acquire_row_lock(database, table_name, &row_key, LockStrength::Update, false, false, tx).await?
        {
            return Err(format!(
                "row of '{}' is locked by transaction {}; the statement that locked it must \
                 finish first",
                table_name, owner
            ));
        }
    }
    Ok(())
}

/// How long `FOR UPDATE` waits before giving up.
///
/// A bound rather than an indefinite wait: two transactions each holding what
/// the other wants would otherwise hang both connections with nothing said.
/// Reported as a timeout, which names the holder and lets the caller retry.
const LOCK_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Folds constant expressions inside a statement's `WHERE` clause.
///
/// The predicate tokenizer reads column names and literals, not calls, so
/// `WHERE created_at < NOW()` reached it as the unknown column `NOW`. Folding
/// happens here, once, before anything parses the clause — and only for terms
/// that evaluate against an empty schema, which is what makes them constant.
///
/// Everything before `WHERE` is untouched, as is any trailing `ORDER BY`,
/// `GROUP BY`, `LIMIT` or `RETURNING`: those are not predicates and a literal
/// substituted into them would change what the statement means.
pub fn fold_where_constants(sql: &str) -> String {
    fold_where_constants_into(sql, &mut None)
}

/// [`fold_where_constants`], refusing a statement whose constants PostgreSQL
/// refuses to compute: integer literals whose arithmetic overflows their own
/// type (`2147483647 + 1` is `integer out of range` whatever column it is
/// compared with), and a `numeric` constant this engine cannot compute with
/// exactly. Left as text, the first was computed at the compared column's
/// width, and against a `bigint` it answered rows.
pub fn fold_where_constants_checked(sql: &str) -> Result<String, String> {
    let mut refused = None;
    let folded = fold_where_constants_into(sql, &mut refused);
    match refused {
        Some(e) => Err(e),
        None => Ok(folded),
    }
}

fn fold_where_constants_into(sql: &str, refused: &mut Option<String>) -> String {
    // A join's `ON` is a predicate too, and reaches the same tokenizer:
    // `ON n.nspname = current_schema()` arrived there as the identifier
    // `current_schema` and panicked the worker.
    let sql = fold_on_clauses(sql, refused);
    let sql = sql.as_str();

    use crate::core::rql::join::find_keyword;

    let Some(where_at) = find_keyword(sql, "WHERE", 0) else {
        return sql.to_string();
    };
    let body_start = where_at + "WHERE".len();
    let body = &sql[body_start..];

    let end = [
        "GROUP BY",
        "ORDER BY",
        "LIMIT",
        "OFFSET",
        "RETURNING",
        "FETCH",
    ]
    .iter()
    .filter_map(|k| find_keyword(body, k, 0))
    .min()
    .unwrap_or(body.len());

    format!(
        "{}{}{}",
        &sql[..body_start],
        fold_constants_in_predicate_into(&body[..end], refused),
        &body[end..]
    )
}

/// Folds the constant parts of every `JOIN … ON` predicate.
///
/// Each `ON` runs to whatever clause comes next, so the region folded is
/// bounded by the following `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY` or `LIMIT`.
fn fold_on_clauses(sql: &str, refused: &mut Option<String>) -> String {
    use crate::core::rql::join::find_keyword;

    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0usize;

    while let Some(at) = find_keyword(&sql[cursor..], "ON", 0) {
        let start = cursor + at + "ON".len();
        let rest = &sql[start..];
        let end = [
            "JOIN",
            "WHERE",
            "GROUP",
            "ORDER",
            "LIMIT",
            "OFFSET",
            "RETURNING",
        ]
        .iter()
        .filter_map(|k| find_keyword(rest, k, 0))
        .min()
        .unwrap_or(rest.len());

        out.push_str(&sql[cursor..start]);
        out.push_str(&fold_constants_in_predicate_into(&rest[..end], refused));
        cursor = start + end;
    }
    out.push_str(&sql[cursor..]);
    out
}

/// Evaluates the constant parts of a predicate, leaving the rest as written.
///
/// The predicate tokenizer reads column names and literals, not calls, so
/// `lease_until < NOW()` reached it as the unknown column `NOW`. Anything that
/// evaluates with an *empty* schema is by definition constant — a column
/// reference fails to resolve there — so that is what decides whether a term is
/// folded or left alone.
fn fold_constants_in_predicate(text: &str) -> String {
    fold_constants_in_predicate_into(text, &mut None)
}

/// [`fold_constants_in_predicate`], noting in `refused` the first constant
/// PostgreSQL would refuse to compute (see [`fold_where_constants_checked`]).
fn fold_constants_in_predicate_into(text: &str, refused: &mut Option<String>) -> String {
    fn fold_operand(operand: &str, refused: &mut Option<String>) -> String {
        // The statement's own `;` is not part of its last operand: left on
        // it, `id <= 5 + 100;` (as pgbench and many drivers send it) and
        // `ts < NOW();` were never folded.
        let trimmed = operand.trim().trim_end_matches(';').trim_end();
        // The surrounding whitespace is part of the statement, not of the
        // operand. Dropping it ran the substituted literal straight into the
        // next keyword — `< '2026-…'ORDER BY id` — so the clause splitter no
        // longer found ` ORDER BY ` and handed `ORDER` to the tokenizer as a
        // column name.
        let lead = &operand[..operand.len() - operand.trim_start().len()];
        let tail = &operand[lead.len() + trimmed.len()..];
        let wrap = |value: String| format!("{}{}{}", lead, value, tail);
        if let Some(typed) = typed_text_literal(trimmed) {
            return wrap(typed);
        }
        match integer_arithmetic(trimmed) {
            IntegerFold::Folded(value) => return wrap(value),
            IntegerFold::Overflow(message) => {
                refused.get_or_insert(message);
                return operand.to_string();
            }
            IntegerFold::No => {}
        }
        // Arithmetic over numeric literals alone (`0.1234 * 100`) is folded
        // too, at the exact decimal the evaluator computes. Left as text it
        // was evaluated against the column, and a NUMERIC column refused the
        // result's width: "operand size does not match".
        if trimmed.is_empty() || !(crate::core::rql::expr::needs_evaluation(trimmed) || literal_arithmetic(trimmed)) {
            return operand.to_string();
        }
        let expr = match crate::core::rql::expr::parse(trimmed, &[]) {
            Ok(expr) => expr,
            Err(e) => {
                // Arithmetic over a constant the engine can only carry as a
                // float that is not the number written: computed by the
                // tokenizer at the column's width, it failed as "operand size
                // does not match" (XX000).
                if literal_arithmetic(trimmed) && e.starts_with("numeric literal ") {
                    refused.get_or_insert(e);
                }
                return operand.to_string();
            }
        };
        let empty = crate::core::row::row::Row {
            position: 0,
            length: 0,
            columns: smallvec::SmallVec::new(),
        };
        let Ok(value) = crate::core::rql::expr::eval(&expr, &empty, &[]) else {
            return operand.to_string();
        };
        match value {
            crate::core::rql::expr::Value::Null => wrap("NULL".to_string()),
            crate::core::rql::expr::Value::Data { bytes, db_type } => {
                match crate::core::rql::subquery::value_to_literal(&db_type, &bytes) {
                    // A computed string is typed text (`CAST(… AS TEXT)`,
                    // `lower(…)`, `'a' || ' '`, `NULLIF`, `COALESCE`), which
                    // a `character(n)` value compares with exactly. Folded to
                    // a bare literal it would be read blank-padded, its
                    // trailing spaces dropped, so one ending in a space keeps
                    // its type. A cast to `varchar` or `character(n)` compares
                    // blank-padded and is folded bare.
                    Some(literal)
                        if db_type == crate::core::db_type::DbType::STRING
                            && bytes.last() == Some(&b' ')
                            && !yields_padded_text(&expr) =>
                    {
                        wrap(format!("{literal}::text"))
                    }
                    Some(literal) => wrap(literal),
                    None => operand.to_string(),
                }
            }
        }
    }

    /// Whether `expr` is a cast to `character varying` or `character(n)`,
    /// whose value a `character(n)` one compares with blank-padded.
    fn yields_padded_text(expr: &crate::core::rql::expr::Expr) -> bool {
        use crate::core::rql::type_modifier::TextTypmod;
        matches!(
            expr,
            crate::core::rql::expr::Expr::TypmodCast {
                typmod: crate::core::rql::expr::CastTypmod::Text(TextTypmod::Varchar(_) | TextTypmod::Bpchar(_)),
                ..
            }
        )
    }

    /// `text '…'`, the typed-literal spelling of `'…'::text`, respelled so,
    /// when the string ends in a space: read as an untyped literal its
    /// trailing spaces were dropped against a `character(n)` value.
    fn typed_text_literal(text: &str) -> Option<String> {
        let head = text.get(..4)?;
        if !head.eq_ignore_ascii_case("text") {
            return None;
        }
        let literal = text[4..].trim_start();
        if !literal.starts_with('\'') {
            return None;
        }
        let whole = crate::core::sql_scan::quoted_end(literal.as_bytes(), 0, b'\'') == literal.len();
        (whole && literal[..literal.len() - 1].ends_with(' ')).then(|| format!("{literal}::text"))
    }

    let mut out = String::with_capacity(text.len());
    for conjunct in split_boolean_terms(text) {
        match conjunct {
            BooleanPiece::Separator(sep) => out.push_str(sep),
            BooleanPiece::Term(term) => {
                // A bare boolean literal is a predicate in its own right, and
                // the tokenizer reads only comparisons — `WHERE false` reached
                // the parser as an empty token stream ("Unexpected end of token
                // stream"), in `SELECT`, `UPDATE` and `DELETE` alike.
                //
                // It is not an exotic thing to write: an ORM renders it as the
                // identity element of a filter chain it collected no conditions
                // for — sea-orm's empty `Condition::all()` is `WHERE TRUE` and
                // its empty `Condition::any()` is `WHERE FALSE` — so a query
                // built from an empty list failed outright instead of returning
                // everything or nothing.
                //
                // Written as the comparison it means, the existing machinery
                // answers it; `1=0` already worked, which is what makes this
                // the whole fix rather than a new evaluation path.
                if let Some(equivalent) = boolean_literal_term(term) {
                    out.push_str(&equivalent);
                    continue;
                }
                // A parenthesised group is folded inside. Read as one
                // comparison, `(v < 0.1234 * 100)` split into `(v ` and
                // ` 0.1234 * 100)`, neither of which folds, and the
                // arithmetic reached the evaluator, which compares its
                // result to a NUMERIC column at the wrong width ("operand
                // size does not match"). `UPDATE … FROM`'s per-row recheck
                // is written as `AND (<terms>)`, so its recheck failed the
                // same way.
                if let Some((lead, inner, tail)) = parenthesised(term) {
                    out.push_str(lead);
                    out.push_str(&fold_constants_in_predicate_into(inner, refused));
                    out.push_str(tail);
                    continue;
                }
                // Fold each side of the comparison independently: only one of
                // them is usually constant.
                match find_comparison(term) {
                    Some((at, width)) => {
                        let left = fold_operand(&term[..at], refused);
                        let op = &term[at..at + width];
                        let right = fold_operand(&term[at + width..], refused);
                        // A number on the left of arithmetic over columns
                        // (`12 = v * 1`) is written the other way round: the
                        // predicate compiler reads arithmetic on the left
                        // only, and failed on it ("unexpected Op(*) after the
                        // end of the expression").
                        match flipped_comparison(op) {
                            Some(flipped)
                                if crate::core::rql::expr::is_plain_number(left.trim())
                                    && has_top_level_arithmetic(right.trim()) =>
                            {
                                let lead = &left[..left.len() - left.trim_start().len()];
                                let tail = &right[right.trim_end().len()..];
                                out.push_str(lead);
                                out.push_str(right.trim());
                                out.push(' ');
                                out.push_str(flipped);
                                out.push(' ');
                                out.push_str(left.trim());
                                out.push_str(tail);
                            }
                            _ => {
                                out.push_str(&left);
                                out.push_str(op);
                                out.push_str(&right);
                            }
                        }
                    }
                    None => out.push_str(term),
                }
            }
        }
    }
    out
}

/// Whether `text` is numeric literals joined by `+ - * /`, with parentheses:
/// nothing a column, a quote or a call could hide in, and at least one
/// operator past a leading sign. At least one literal is a decimal (`0.5`),
/// so the arithmetic is NUMERIC: integers alone are
/// [`fold_integer_arithmetic`]'s, which leaves an `int4` overflow
/// (`2147483647 + 1`) and integer division to the evaluator, which raises
/// PostgreSQL's 22003 for the first. Folded here, the overflow became a
/// value and the comparison answered rows.
fn literal_arithmetic(text: &str) -> bool {
    text.contains('.')
        && text.bytes().all(|b| b.is_ascii_digit() || b".+-*/() \t".contains(&b))
        && text.trim_start().trim_start_matches(['-', '+', '(']).bytes().any(|b| b"+-*/".contains(&b))
        && text.bytes().any(|b| b.is_ascii_digit())
}

/// `text` as one integer literal, when it is nothing but integer literals
/// joined by `+`, `-`, `*`, `/` and `%`, with parentheses and a minus on a
/// literal.
///
/// Left as text, `id <= 500000 + 100` was evaluated on every row and could
/// use no index: ~170 times slower than `id <= 500100`, which is what an ORM
/// sends whenever it offsets a bound parameter. Folded, it is that literal.
///
/// Computed as PostgreSQL computes it, or not at all. A literal is `int4`
/// when it fits and `int8` otherwise; an operation takes the wider of its
/// two; a result outside its type is PostgreSQL's "integer out of range",
/// so the text is left for the engine, which reports exactly that, as is a
/// zero divisor. Nothing else is folded: a literal beyond `int8`
/// (`numeric`), any other character. `None` also for a lone literal, which
/// needs no folding.
fn fold_integer_arithmetic(text: &str) -> Option<String> {
    match integer_arithmetic(text) {
        IntegerFold::Folded(value) => Some(value),
        IntegerFold::Overflow(_) | IntegerFold::No => None,
    }
}

/// What [`integer_arithmetic`] made of a text.
enum IntegerFold {
    /// The one integer literal it computes to.
    Folded(String),
    /// Integer literal arithmetic that PostgreSQL refuses as out of its type's
    /// range, with that refusal's message.
    Overflow(String),
    /// Not integer literal arithmetic, or something the evaluator answers
    /// (a zero divisor's 22012).
    No,
}

/// [`fold_integer_arithmetic`], telling an overflow apart from text that is
/// not integer arithmetic at all.
fn integer_arithmetic(text: &str) -> IntegerFold {
    #[derive(Clone, Copy, PartialEq, PartialOrd)]
    enum Ty {
        Int4,
        Int8,
    }
    fn typed(v: i64) -> Ty {
        if i32::try_from(v).is_ok() { Ty::Int4 } else { Ty::Int8 }
    }
    fn apply(l: (i64, Ty), r: (i64, Ty), op: u8, overflow: &mut Option<Ty>) -> Option<(i64, Ty)> {
        let ty = if l.1 > r.1 { l.1 } else { r.1 };
        // A zero divisor is the evaluator's 22012; anything else that does
        // not fit is the type's own overflow.
        if matches!(op, b'/' | b'%') && r.0 == 0 {
            return None;
        }
        let v = match op {
            b'+' => l.0.checked_add(r.0),
            b'-' => l.0.checked_sub(r.0),
            // Integer division truncates toward zero, as PostgreSQL's does.
            b'/' => l.0.checked_div(r.0),
            b'%' => l.0.checked_rem(r.0),
            _ => l.0.checked_mul(r.0),
        }
        .filter(|v| ty == Ty::Int8 || i32::try_from(*v).is_ok());
        // Past an overflow the value no longer matters, only whether the
        // rest of the text is arithmetic too, so the parse goes on.
        if v.is_none() {
            overflow.get_or_insert(ty);
        }
        Some((v.unwrap_or(0), ty))
    }
    struct P<'a> {
        b: &'a [u8],
        i: usize,
        ops: usize,
        overflow: Option<Ty>,
    }
    impl P<'_> {
        fn peek(&mut self) -> Option<u8> {
            while self.b.get(self.i).is_some_and(u8::is_ascii_whitespace) {
                self.i += 1;
            }
            self.b.get(self.i).copied()
        }
        fn expr(&mut self) -> Option<(i64, Ty)> {
            let mut acc = self.term()?;
            while let Some(op @ (b'+' | b'-')) = self.peek() {
                self.i += 1;
                self.ops += 1;
                let rhs = self.term()?;
                acc = apply(acc, rhs, op, &mut self.overflow)?;
            }
            Some(acc)
        }
        fn term(&mut self) -> Option<(i64, Ty)> {
            let mut acc = self.factor()?;
            while let Some(op @ (b'*' | b'/' | b'%')) = self.peek() {
                self.i += 1;
                self.ops += 1;
                let rhs = self.factor()?;
                acc = apply(acc, rhs, op, &mut self.overflow)?;
            }
            Some(acc)
        }
        fn literal(&mut self, negative: bool) -> Option<(i64, Ty)> {
            let start = self.i;
            while self.b.get(self.i).is_some_and(u8::is_ascii_digit) {
                self.i += 1;
            }
            // `1.5`, `1e3`, `1_000` and `12abc` are not integer literals.
            if self.i == start || self.b.get(self.i).is_some_and(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_')) {
                return None;
            }
            let digits = std::str::from_utf8(&self.b[start..self.i]).ok()?;
            // Negated as part of the literal, as PostgreSQL's parser does, so
            // `-2147483648` is the `int4` it is there.
            let v: i64 = if negative { format!("-{digits}").parse().ok()? } else { digits.parse().ok()? };
            Some((v, typed(v)))
        }
        fn factor(&mut self) -> Option<(i64, Ty)> {
            match self.peek()? {
                b'(' => {
                    self.i += 1;
                    let v = self.expr()?;
                    if self.peek()? != b')' {
                        return None;
                    }
                    self.i += 1;
                    Some(v)
                }
                b'-' => {
                    self.i += 1;
                    if !self.b.get(self.i).is_some_and(u8::is_ascii_digit) {
                        return None;
                    }
                    self.literal(true)
                }
                b'0'..=b'9' => self.literal(false),
                _ => None,
            }
        }
    }
    let mut p = P { b: text.as_bytes(), i: 0, ops: 0, overflow: None };
    match p.expr() {
        Some(_) if p.peek().is_some() || p.ops == 0 => IntegerFold::No,
        Some(_) if p.overflow == Some(Ty::Int4) => IntegerFold::Overflow("integer out of range".to_string()),
        Some(_) if p.overflow == Some(Ty::Int8) => IntegerFold::Overflow("bigint out of range".to_string()),
        Some((v, _)) => IntegerFold::Folded(v.to_string()),
        None => IntegerFold::No,
    }
}

/// A predicate term that is nothing but `TRUE` or `FALSE`, as the equivalent
/// comparison.
///
/// Quoted and qualified names are left alone: `"true"` is a column, and only a
/// bare keyword is the literal.
fn boolean_literal_term(term: &str) -> Option<String> {
    let trimmed = term.trim();
    let replacement = match trimmed.to_ascii_uppercase().as_str() {
        "TRUE" => "1=1",
        "FALSE" => "1=0",
        _ => return None,
    };
    // The surrounding whitespace belongs to the statement, not the term —
    // dropping it would run the substitution into the next keyword.
    let lead = &term[..term.len() - term.trim_start().len()];
    let tail = &term[term.trim_end().len()..];
    Some(format!("{}{}{}", lead, replacement, tail))
}

enum BooleanPiece<'a> {
    Term(&'a str),
    Separator(&'a str),
}

/// `term` as `(`, what its outer parentheses hold, and `)` (each with the
/// whitespace around it), when one pair encloses the whole term. Quotes are
/// skipped; `(a) < (b)` is not one group.
fn parenthesised(term: &str) -> Option<(&str, &str, &str)> {
    let open = term.len() - term.trim_start().len();
    let close = term.trim_end().len().checked_sub(1)?;
    let bytes = term.as_bytes();
    if bytes.get(open) != Some(&b'(') || bytes.get(close) != Some(&b')') || close <= open {
        return None;
    }
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    for (i, &b) in bytes.iter().enumerate().take(close + 1).skip(open) {
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
            }
            None => match b {
                b'\'' | b'"' => quote = Some(b),
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 && i != close {
                        return None;
                    }
                }
                _ => {}
            },
        }
    }
    (depth == 0).then(|| (&term[..open + 1], &term[open + 1..close], &term[close..]))
}

/// Splits a predicate into terms and the `AND`/`OR` between them.
fn split_boolean_terms(text: &str) -> Vec<BooleanPiece<'_>> {
    let upper = text.to_ascii_uppercase();
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
                i += 1;
                continue;
            }
            None => match b {
                b'\'' | b'"' => {
                    quote = Some(b);
                    i += 1;
                    continue;
                }
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            },
        }
        if depth == 0
            && let Some(keyword) = [" AND ", " OR "].into_iter().find(|k| upper[i..].starts_with(k))
        {
            out.push(BooleanPiece::Term(&text[start..i]));
            out.push(BooleanPiece::Separator(&text[i..i + keyword.len()]));
            i += keyword.len();
            start = i;
            // The next term's first byte is read by the next pass, not
            // stepped over: it was, so the `(` opening `((a) AND (b))` went
            // uncounted and the term was split at its inner `AND`.
            continue;
        }
        i += 1;
    }
    out.push(BooleanPiece::Term(&text[start..]));
    out
}

/// Offset and width of a comparison operator at the top level of `term`.
fn find_comparison(term: &str) -> Option<(usize, usize)> {
    let bytes = term.as_bytes();
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    for i in 0..bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
                continue;
            }
            None => match b {
                b'\'' | b'"' => quote = Some(b),
                b'(' => depth += 1,
                b')' => depth -= 1,
                b'=' | b'<' | b'>' | b'!' if depth == 0 => {
                    let mut width = 1;
                    if i + 1 < bytes.len() && matches!(bytes[i + 1], b'=' | b'>') {
                        width = 2;
                    }
                    return Some((i, width));
                }
                _ => {}
            },
        }
    }
    None
}

/// The comparison that means the same with its operands swapped.
fn flipped_comparison(op: &str) -> Option<&'static str> {
    Some(match op {
        "=" => "=",
        "<>" => "<>",
        "!=" => "!=",
        "<" => ">",
        ">" => "<",
        "<=" => ">=",
        ">=" => "<=",
        _ => return None,
    })
}

/// Whether `text` has a `+ - * / %` outside quotes and parentheses, past a
/// leading sign.
fn has_top_level_arithmetic(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i) {
            i = end.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            // `->` and `->>` reach into a document; they are not a minus.
            b'-' if bytes.get(i + 1) == Some(&b'>') => {}
            b'+' | b'-' | b'*' | b'/' | b'%' if depth == 0 && i > 0 => return true,
            _ => {}
        }
        i += 1;
    }
    false
}

/// The alias an `INSERT INTO t AS a …` gives its target, unquoted.
/// The value `literal` (a constant, or a constant expression: `1.254`,
/// `'1.254'`, `1.254::numeric`, `CAST(… AS NUMERIC(10,3))`, `1.25 + 0.004`)
/// becomes when assigned to `field`, written back as a literal: evaluated
/// into the column as an INSERT stores it (`expr::eval_into`, PostgreSQL's
/// assignment cast: a NUMERIC rounded half away from zero at its scale, an
/// integer from text or a fraction). `None` for anything that is not such
/// a constant of a numeric column, which is left as written.
///
/// The one key an `ON CONFLICT` statement looks rows up by, checks its own
/// rows against and hands `EXCLUDED`: from literal text, `1.254::numeric`
/// matched no row, and `(0.995, …), (1.00, …)` were two keys that the
/// column stores as one.
pub(crate) fn stored_numeric_literal(literal: &str, field: &crate::core::row::schema::SchemaField) -> Option<String> {
    use crate::core::db_type::DbType;
    let integer = matches!(
        field.db_type,
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64
    );
    if field.db_type != DbType::DECIMAL && !integer {
        return None;
    }
    let text = literal.trim();
    // A quoted number is an untyped constant the column reads as its own.
    let unquoted = text
        .strip_prefix('\'')
        .and_then(|t| t.strip_suffix('\''))
        .map(str::trim)
        .filter(|t| !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'+' | b'e' | b'E')));
    let text = unquoted.unwrap_or(text);
    if text.eq_ignore_ascii_case("NULL") || text.eq_ignore_ascii_case("DEFAULT") {
        return None;
    }
    // A plain number the column holds exactly, read without the parse
    // and evaluation below, which an upsert ran for every column of every
    // row: an integer into an integer column, or a decimal with no more
    // places than its column's scale. Anything else (rounding, an
    // exponent, an expression, a value out of range) takes the general path.
    if let Some(stored) = stored_plain_number(text, field, integer) {
        return Some(stored);
    }
    evaluated_numeric_literal(text, field)
}

/// The bytes one arbiter column of one `VALUES` row keys on, for the batch
/// 21000 check; `None` when the row can equal no other on it, so it is left
/// out. A value written is evaluated into the column (tagged 1); an omitted
/// column or `DEFAULT` takes the column's default when that is a constant
/// (two rows both defaulting to `5` are one key). A constant this cannot
/// evaluate into the column (a TIME, an INET) is compared as written, tagged
/// 0, so it meets only another such text, never an evaluated key, NULL or
/// DEFAULT: two `'10:00:00'` are one key, as on base. `None` for NULL,
/// which equals nothing (`(1, NULL), (1, NULL)` under `UNIQUE (x, k)`
/// inserts both), for a default computed per row (a sequence's next
/// value), and for a generated column.
fn batch_key_part(
    written: Option<&str>,
    field: &crate::core::row::schema::SchemaField,
    defaults: &[crate::core::rql::column_default::ColumnDefault],
    generated: &[crate::core::rql::generated::GeneratedColumn],
) -> Option<Vec<u8>> {
    let literal = match written.map(str::trim) {
        Some(w) if !w.eq_ignore_ascii_case("DEFAULT") => w.to_string(),
        _ => {
            if crate::core::rql::generated::expression_for(&field.name, generated).is_some() {
                return None;
            }
            match crate::core::rql::column_default::expression_for(&field.name, defaults) {
                Some(expression) if default_is_constant(expression) => expression.trim().to_string(),
                _ => return None,
            }
        }
    };
    if literal.eq_ignore_ascii_case("NULL") {
        return None;
    }
    match stored_key_bytes(&literal, field) {
        Some(bytes) => Some(std::iter::once(1u8).chain(bytes).collect()),
        None => match crate::core::rql::expr::parse(&literal, &[]) {
            // NULL however written (`NULL::text`, `(NULL)`) equals nothing.
            Ok(expr)
                if crate::core::rql::expr::eval(&expr, &crate::core::row::row::Row::default(), &[])
                    .is_ok_and(|v| v.is_null()) =>
            {
                None
            }
            _ => Some(std::iter::once(0u8).chain(literal.bytes()).collect()),
        },
    }
}

/// `literal` as a column of declared text length stores it, as an untyped
/// literal, when that differs from what is written: a typed text value
/// (`'a  '::text`, a parameter bound as text, `'b '::varchar(5)`) into a
/// `character(3)` key is `'a'`. The conflict was found on the stored key; a
/// look-up by the value as written compares exactly (text against
/// `character(n)` is text), matched nothing and lost the write. `None` for
/// a plain untyped literal, which the comparison reads as the column does,
/// and for anything that is not a constant.
fn stored_text_literal(literal: &str, field: &crate::core::row::schema::SchemaField) -> Option<String> {
    use crate::core::rql::type_modifier::TextTypmod;
    if field.db_type != crate::core::db_type::DbType::STRING
        || !matches!(field.text_typmod(), Some(TextTypmod::Bpchar(_)) | Some(TextTypmod::Varchar(1..)))
    {
        return None;
    }
    let text = literal.trim();
    let plain = |t: &str| {
        t.len() >= 2 && t.starts_with('\'') && crate::core::sql_scan::quoted_end(t.as_bytes(), 0, b'\'') == t.len()
    };
    if plain(text) || text.eq_ignore_ascii_case("NULL") || text.eq_ignore_ascii_case("DEFAULT") {
        return None;
    }
    // `text 'a  '` and `'a  '::text`: the literal, which the comparison reads
    // as the column does.
    let bare = crate::core::rql::column_default::strip_redundant_cast(text);
    if bare.len() != text.len() && plain(bare) {
        return Some(bare.to_string());
    }
    let expr = crate::core::rql::expr::parse(text, &[]).ok()?;
    if !crate::core::rql::expr::columns_of(&expr).is_empty() {
        return None;
    }
    let value = crate::core::rql::expr::eval_into(&expr, &crate::core::row::row::Row::default(), &[], field).ok()?;
    let crate::core::rql::expr::Value::Data { bytes, db_type } = value else { return None };
    if db_type != crate::core::db_type::DbType::STRING {
        return None;
    }
    let held = match crate::core::rql::type_modifier::assign_bytes(field, &bytes) {
        Ok(Some(held)) => held,
        Ok(None) => bytes,
        // Too long for the column: the INSERT itself reports it.
        Err(_) => return None,
    };
    crate::core::rql::subquery::value_to_literal(&crate::core::db_type::DbType::STRING, &held)
}

/// The bytes `literal` is stored as in `field`, for comparing two rows'
/// keys: a number as [`stored_numeric_literal`] writes it, any other
/// constant evaluated into the column (a UUID's sixteen bytes, a
/// timestamp's microseconds, a boolean's byte). `None` for anything that is
/// not a constant the column takes.
fn stored_key_bytes(literal: &str, field: &crate::core::row::schema::SchemaField) -> Option<Vec<u8>> {
    if let Some(number) = stored_numeric_literal(literal, field) {
        return Some(number.into_bytes());
    }
    // `text 'y'` is the literal `'y'` (`column_default::strip_redundant_cast`).
    let text = crate::core::rql::column_default::typed_text_literal_or(literal.trim());
    if text.eq_ignore_ascii_case("NULL") || text.eq_ignore_ascii_case("DEFAULT") {
        return None;
    }
    // An integer into a text column is its digits, as PostgreSQL's
    // assignment cast writes them: `(5)` and `('5')` are one key.
    if field.db_type == crate::core::db_type::DbType::STRING
        && let Ok(n) = text.parse::<i128>()
    {
        return Some(n.to_string().into_bytes());
    }
    let expr = crate::core::rql::expr::parse(text, &[]).ok()?;
    if !crate::core::rql::expr::columns_of(&expr).is_empty() {
        return None;
    }
    match crate::core::rql::expr::eval_into(&expr, &crate::core::row::row::Row::default(), &[], field).ok()? {
        // In a column of declared text length, as the column holds it
        // (`type_modifier::assign_bytes`): `'y'` and `'y '` are one
        // `character(3)` key, and applied last-wins without this.
        crate::core::rql::expr::Value::Data { bytes, db_type } if db_type == field.db_type => {
            match crate::core::rql::type_modifier::assign_bytes(field, &bytes) {
                Ok(Some(held)) => Some(held),
                _ => Some(bytes),
            }
        }
        _ => None,
    }
}

/// [`stored_numeric_literal`]'s general path: `text` parsed and evaluated
/// into `field` as an INSERT stores it.
fn evaluated_numeric_literal(text: &str, field: &crate::core::row::schema::SchemaField) -> Option<String> {
    use crate::core::db_type::DbType;
    let expr = crate::core::rql::expr::parse(text, &[]).ok()?;
    if !crate::core::rql::expr::columns_of(&expr).is_empty() {
        return None;
    }
    let value = crate::core::rql::expr::eval_into(&expr, &crate::core::row::row::Row::default(), &[], field).ok()?;
    let crate::core::rql::expr::Value::Data { bytes, db_type } = value else { return None };
    match db_type {
        DbType::DECIMAL => {
            let raw = i128::from_le_bytes(bytes.as_slice().try_into().ok()?);
            Some(crate::core::value_codec::format_decimal_scaled(
                raw,
                field.scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE),
            ))
        }
        DbType::I8 => Some(i8::from_le_bytes(bytes.as_slice().try_into().ok()?).to_string()),
        DbType::I16 => Some(i16::from_le_bytes(bytes.as_slice().try_into().ok()?).to_string()),
        DbType::I32 => Some(i32::from_le_bytes(bytes.as_slice().try_into().ok()?).to_string()),
        DbType::I64 => Some(i64::from_le_bytes(bytes.as_slice().try_into().ok()?).to_string()),
        DbType::U8 => Some(u8::from_le_bytes(bytes.as_slice().try_into().ok()?).to_string()),
        DbType::U16 => Some(u16::from_le_bytes(bytes.as_slice().try_into().ok()?).to_string()),
        DbType::U32 => Some(u32::from_le_bytes(bytes.as_slice().try_into().ok()?).to_string()),
        DbType::U64 => Some(u64::from_le_bytes(bytes.as_slice().try_into().ok()?).to_string()),
        _ => None,
    }
}

/// [`stored_numeric_literal`] for a plain number its column holds exactly:
/// `[+-]digits` for an integer column (in its range), `[+-]digits[.digits]`
/// with no more places than the scale for a NUMERIC one. `None` otherwise.
fn stored_plain_number(text: &str, field: &crate::core::row::schema::SchemaField, integer: bool) -> Option<String> {
    use crate::core::db_type::DbType;
    let body = text.strip_prefix(['+', '-']).unwrap_or(text);
    let (whole, fraction) = match body.split_once('.') {
        Some((w, f)) => (w, Some(f)),
        None => (body, None),
    };
    if !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.is_none_or(|f| f.bytes().all(|b| b.is_ascii_digit()))
        || whole.len() + fraction.map_or(0, str::len) == 0
        || whole.len() > 30
    {
        return None;
    }
    if integer {
        if fraction.is_some() {
            return None;
        }
        let value: i128 = text.parse().ok()?;
        let fits = match field.db_type {
            DbType::I8 => i8::try_from(value).is_ok(),
            DbType::I16 => i16::try_from(value).is_ok(),
            DbType::I32 => i32::try_from(value).is_ok(),
            DbType::I64 => i64::try_from(value).is_ok(),
            DbType::U8 => u8::try_from(value).is_ok(),
            DbType::U16 => u16::try_from(value).is_ok(),
            DbType::U32 => u32::try_from(value).is_ok(),
            DbType::U64 => u64::try_from(value).is_ok(),
            _ => false,
        };
        return fits.then(|| value.to_string());
    }
    let scale = field.scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE);
    if fraction.is_some_and(|f| f.len() > scale as usize) {
        return None;
    }
    let raw = crate::core::value_codec::parse_decimal_scaled(text, scale)?;
    Some(crate::core::value_codec::format_decimal_scaled(raw, scale))
}

#[cfg(test)]
mod stored_plain_number_tests {
    use super::*;
    use crate::core::db_type::DbType;

    /// The fast path answers exactly what evaluation answers, wherever it
    /// answers at all.
    ///
    /// Mutation: `stored_plain_number` keeping more places than the scale.
    #[test]
    fn the_fast_path_agrees_with_evaluation() {
        let texts = [
            "0", "-0", "+5", "007", "2023", "-2147483648", "2147483648", "9223372036854775807",
            "9223372036854775808", "-129", "255", "1.25", "1.254", "-1.255", "0.5", ".5", "5.",
            "1.2500", "12345678901.99", "1e2", "1.0", "-0.00",
        ];
        let types = [
            (DbType::I8, None),
            (DbType::I16, None),
            (DbType::I32, None),
            (DbType::I64, None),
            (DbType::U8, None),
            (DbType::U32, None),
            (DbType::DECIMAL, Some(2)),
            (DbType::DECIMAL, Some(0)),
            (DbType::DECIMAL, None),
        ];
        let mut answered = 0;
        for (db_type, scale) in types {
            let integer = db_type != DbType::DECIMAL;
            let mut field = crate::core::row::schema::SchemaField::new("k".to_string(), db_type.clone(), 0, 0, 0, false);
            field.scale = scale;
            for text in texts {
                if let Some(fast) = stored_plain_number(text, &field, integer) {
                    answered += 1;
                    assert_eq!(Some(fast), evaluated_numeric_literal(text, &field), "{db_type:?} {scale:?} {text}");
                }
            }
        }
        assert!(answered > 40, "the fast path answered only {answered}");
    }
}

fn insert_alias(insert_sql: &str) -> Option<String> {
    let into = crate::core::rql::join::find_keyword(insert_sql, "INTO", 0)?;
    let mut words = insert_sql[into + "INTO".len()..].split_whitespace();
    words.next()?;
    let next = words.next()?;
    if !next.eq_ignore_ascii_case("AS") {
        return None;
    }
    // Kept quoted when written so: `AS "X"` is `X` exactly, which a bare `x`
    // (folded) is not.
    let alias = words.next()?.split('(').next()?;
    (!alias.trim_matches('"').is_empty()).then(|| alias.to_string())
}

/// Whether a column default is a constant: a literal, or one under a cast.
fn default_is_constant(expression: &str) -> bool {
    fn constant(e: &crate::core::rql::expr::Expr) -> bool {
        use crate::core::rql::expr::Expr;
        match e {
            Expr::Literal { .. } | Expr::Null => true,
            Expr::Cast(inner, _) => constant(inner),
            Expr::NumericCast { value, .. } | Expr::TypmodCast { value, .. } => constant(value),
            _ => false,
        }
    }
    crate::core::rql::expr::parse(expression.trim(), &[]).is_ok_and(|e| constant(&e))
}

/// What `apply_conflict_update` puts for an `EXCLUDED` column it cannot
/// give a value for; never a literal anything writes.
const EXCLUDED_UNAVAILABLE: &str = "\u{1}unavailable:";

/// Replaces every `EXCLUDED.<column>` with the value the insert would have
/// written for it.
/// Where `EXCLUDED.` starts in `text` as a reference: outside quotes and
/// comments, not the tail of a longer word.
fn find_excluded_marker(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = crate::core::sql_scan::skip_quoted(bytes, i).or_else(|| crate::core::sql_scan::skip_comment(bytes, i)) {
            // A double-quoted name is an identifier; only literals are text.
            if bytes[i] != b'"' {
                i = end.max(i + 1);
                continue;
            }
        }
        if bytes.len() - i >= 9
            && bytes[i..i + 9].eq_ignore_ascii_case(b"EXCLUDED.")
            && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_' || bytes[i - 1] == b'.'))
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn substitute_excluded(text: &str, inserted: &[(String, String)]) -> Result<String, String> {
    const MARKER: &str = "EXCLUDED.";
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    loop {
        // Outside string literals and dollar quotes, as a whole word: text
        // that says `excluded.v` is text (it was replaced inside the string).
        let Some(at) = find_excluded_marker(rest) else {
            out.push_str(rest);
            return Ok(out);
        };
        out.push_str(&rest[..at]);
        let after = &rest[at + MARKER.len()..];
        let end = after
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '"'))
            .unwrap_or(after.len());
        let column = after[..end].trim_matches('"');
        let Some((_, literal)) = inserted
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
        else {
            return Err(format!("column excluded.{} does not exist", column));
        };
        if literal.starts_with(EXCLUDED_UNAVAILABLE) {
            return Err(format!(
                "EXCLUDED.{} is not supported: this INSERT does not write the column, and its default is \
                 computed when the row is (a sequence, CURRENT_TIMESTAMP), so computing it again here would \
                 give another value",
                column
            ));
        }
        rest = &after[end..];
        // `EXCLUDED.c IS [NOT] NULL` over the value now known is a constant;
        // left as `NULL IS NULL` the predicate reader refused it ("NULL
        // literals are not supported").
        let words: Vec<&str> = rest.split_whitespace().take(3).collect();
        let test = match words.as_slice() {
            [is, null, ..] if is.eq_ignore_ascii_case("IS") && null.eq_ignore_ascii_case("NULL") => Some((true, 2)),
            [is, not, null] if is.eq_ignore_ascii_case("IS")
                && not.eq_ignore_ascii_case("NOT")
                && null.eq_ignore_ascii_case("NULL") => Some((false, 3)),
            _ => None,
        };
        if let Some((want_null, n)) = test {
            let is_null = literal.trim().trim_start_matches('(').trim_end_matches(')').trim().eq_ignore_ascii_case("NULL");
            out.push_str(if is_null == want_null { "1 = 1" } else { "1 = 0" });
            // Past the test's words.
            let mut cut = rest;
            for _ in 0..n {
                cut = cut.trim_start();
                let w = cut.find(char::is_whitespace).unwrap_or(cut.len());
                cut = &cut[w..];
            }
            rest = cut;
            continue;
        }
        out.push_str(literal);
    }
}

/// Removes `<table>.` qualifiers, which an `UPDATE`'s own clauses do not carry.
fn strip_qualifier(text: &str, table: &str) -> String {
    // String literals are text, not references: `'see t.x'` keeps its `t.`.
    let mut out = String::with_capacity(text.len());
    let b = text.as_bytes();
    let (mut i, mut from) = (0usize, 0usize);
    while i < b.len() {
        if b[i] == b'\'' {
            out.push_str(&strip_qualifier_unquoted(&text[from..i], table));
            let start = i;
            i += 1;
            while i < b.len() {
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i = (i + 1).min(b.len());
            out.push_str(&text[start..i]);
            from = i;
            continue;
        }
        i += 1;
    }
    out.push_str(&strip_qualifier_unquoted(&text[from..], table));
    out
}

/// [`strip_qualifier`] over text holding no string literal.
fn strip_qualifier_unquoted(text: &str, table: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    let quoted = format!("\"{}\".", table);
    loop {
        let lower_rest = rest.to_ascii_lowercase();
        let bare = format!("{}.", table.to_ascii_lowercase());
        let candidates = [
            lower_rest.find(&bare),
            lower_rest.find(&quoted.to_ascii_lowercase()),
        ];
        let Some(at) = candidates.iter().flatten().min().copied() else {
            out.push_str(rest);
            return out;
        };
        // Only when it stands alone, so `other_table.x` is untouched.
        let starts_word = at == 0 || {
            let b = rest.as_bytes()[at - 1];
            !(b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
        };
        let width = if lower_rest[at..].starts_with(&quoted.to_ascii_lowercase()) {
            quoted.len()
        } else {
            bare.len()
        };
        out.push_str(&rest[..at]);
        if !starts_word {
            out.push_str(&rest[at..at + width]);
        }
        rest = &rest[at + width..];
    }
}

/// The output types of `SELECT * FROM <table> …`: the table's visible
/// columns, in order. Empty for anything else.
fn star_output_types(select_sql: &str, database: &Arc<Database>) -> Vec<Option<crate::core::db_type::DbType>> {
    let Some(from) = crate::core::rql::join::find_keyword(select_sql, "FROM", 0) else {
        return Vec::new();
    };
    let head: Vec<&str> = select_sql[..from].split_whitespace().collect();
    if !(head.len() == 2 && head[0].eq_ignore_ascii_case("SELECT") && head[1] == "*") {
        return Vec::new();
    }
    let Some(name) = select_sql[from + "FROM".len()..].split_whitespace().next() else {
        return Vec::new();
    };
    // Schema only, under the statement's set (the source of an `INSERT …
    // SELECT`, ACCESS SHARE).
    let Some(view) = database.schema_view(name.trim_matches('"').trim_end_matches(';')) else {
        return Vec::new();
    };
    let mut fields: Vec<_> = view
        .schema()
        .fields
        .iter()
        .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
        .collect();
    fields.sort_by_key(|f| f.write_order);
    fields.into_iter().map(|f| Some(f.db_type.clone())).collect()
}

/// Runs an `INSERT … SELECT`, or returns `None` when the statement is not one.
///
/// The query supplies the rows, so it runs first and its results become a
/// `VALUES` clause that the ordinary tuple path then writes. That composition
/// is the point: defaults, JSON mapping, foreign keys, uniqueness and
/// `RETURNING` all apply exactly as they do to any other insert, and there is
/// no second write path to keep in step.
///
/// **All rows are collected before any is written.** Reading and writing the
/// same table in one statement — `INSERT INTO t … SELECT … FROM t` — would
/// otherwise feed each inserted row back in as new input and never terminate.
/// PostgreSQL materialises the query for the same reason.
///
/// Kept out of `execute_with_context`'s own frame deliberately: that function
/// is one large `match`, so its future is the sum of every arm's locals, and it
/// sits close enough to a default 2 MB worker stack that inlining another loop
/// here overflows it. The server sets a 32 MB stack in `main.rs`, so the
/// failure appears only where the default applies — tests — and reports itself
/// as a runtime abort rather than as anything pointing here.
async fn insert_from_select(
    query: &str,
    conflict: &InsertConflict,
    database: &Arc<Database>,
    depth: usize,
    tx: TxId,
) -> Option<Vec<u8>> {
    let (head_sql, returning_tail) = match crate::core::rql::lexer_ir::returning_split(query) {
        Some((head, tail)) => (head, Some(tail)),
        None => (query, None),
    };

    let (prefix, select_sql) = crate::core::rql::lexer_ir::insert_select_source(head_sql)?;

    // `SELECT <constants> [WHERE <constant>]` with no FROM is PostgreSQL's
    // implicit single-row source: one row when the condition holds, none when
    // it does not. It arises from `INSERT … SELECT … WHERE NOT EXISTS (…)` —
    // the standard "insert only if absent" idiom — once the EXISTS has been
    // folded to a constant.
    if crate::core::rql::join::find_keyword(select_sql, "FROM", 0).is_none() {
        return Some(
            match constant_select_insert(prefix, select_sql, returning_tail, conflict, database, depth, tx)
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    error!("INSERT … SELECT without FROM: {}", e);
                    ddl_error(&e)
                }
            },
        );
    }

    let rows = match run_subquery(select_sql, database, tx).await {
        Ok(rows) => rows,
        Err(e) => {
            error!("INSERT … SELECT: {}", e);
            return Some(ddl_error(&e));
        }
    };

    // An empty result is the statement succeeding with nothing to do, which is
    // what PostgreSQL reports. With RETURNING it is an empty row set, not a
    // count: the client is owed a RowDescription for the rows it asked for,
    // even when there are none, and a bare count sent only the command tag.
    if rows.is_empty() {
        if returning_tail.is_some() {
            return Some(vec![2u8]);
        }
        let mut result = vec![0u8];
        result.extend_from_slice(&0u64.to_le_bytes());
        return Some(result);
    }

    // A scanned row reports a document, a byte string and an array by their
    // physical type, which is text, so their stored bytes were written back as
    // a text literal: copying a JSONB column failed with 22P02. The query's
    // declared output types say what each value really is.
    let declared = {
        let declared = crate::core::rql::cte::declared_item_types(select_sql, database);
        if declared.is_empty() { star_output_types(select_sql, database) } else { declared }
    };
    let erased = |t: &crate::core::db_type::DbType| {
        matches!(
            t,
            crate::core::db_type::DbType::JSONB | crate::core::db_type::DbType::BYTEA | crate::core::db_type::DbType::ARRAY
        )
    };
    let mut tuples: Vec<String> = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut parts: Vec<String> = Vec::with_capacity(row.columns.len());
        for (at, column) in row.columns.iter().enumerate() {
            let column_type = match declared.get(at).cloned().flatten() {
                Some(t) if erased(&t) && column.column_type != crate::core::db_type::DbType::NULL => t,
                _ => column.column_type.clone(),
            };
            // `value_to_literal` quotes and escapes, so a value holding a quote
            // or a parenthesis rebuilds as data rather than as syntax.
            let Some(literal) = crate::core::rql::subquery::value_to_literal(
                &column_type,
                column.data.into_slice(),
            ) else {
                return Some(ddl_error(&format!(
                    "INSERT … SELECT returned a {} value that cannot be written",
                    column.column_type
                )));
            };
            parts.push(literal);
        }
        tuples.push(format!("({})", parts.join(", ")));
    }

    let rewritten = format!(
        "{} VALUES {}{}",
        prefix.trim_end(),
        tuples.join(", "),
        returning_tail
            .map(|tail| format!(" RETURNING {}", tail))
            .unwrap_or_default()
    );

    let Some(mut purpose) = recognize_query_purpose(&rewritten) else {
        return Some(ddl_error(
            "could not parse the statement rewritten from INSERT … SELECT",
        ));
    };
    conflict.restore(&mut purpose);
    Some(Box::pin(execute_with_context(purpose, database.clone(), depth, tx)).await)
}

/// The statement with its column list supplied, for an `INSERT … VALUES`
/// that named none; `None` when it already had one or is another form.
///
/// `schema` is the target's, resolved once by the INSERT arm under its ROW
/// EXCLUSIVE (slice B8b): no lookup or lock of its own. Synchronous, so its
/// locals (a copy of the schema) live on the stack for the call only, never
/// in `execute_with_context`'s future.
fn supply_insert_column_list(
    row_data: &crate::core::rql::lexer_s1::InsertRowData,
    schema: Option<&crate::core::row::schema::TableSchema>,
) -> Option<Result<QueryPurpose, String>> {
    let schema = schema?;
    let (fields, generated) = (schema.fields.clone(), schema.generated.clone());
    // A generated column takes no value from the client, so it is not one of
    // the positions `INSERT INTO t VALUES (…)` fills — PostgreSQL maps the
    // tuple across the columns that *can* be written. Leaving it in the list
    // shifted every value after it by one.
    let fields: smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> = fields
        .into_iter()
        .filter(|f| crate::core::rql::generated::expression_for(&f.name, &generated).is_none())
        .collect();
    let rewritten = crate::core::rql::lexer_ir::supply_column_list(&row_data.query, &fields)?;
    let Some(mut purpose) = recognize_query_purpose(&rewritten) else {
        return Some(Err(
            "could not parse the statement after supplying its column list".to_string(),
        ));
    };
    // `row_data.query` is the text with `ON CONFLICT` already cut off, so the
    // re-parse cannot find the clause and it is carried across instead.
    // Without this, `INSERT INTO t VALUES (…) ON CONFLICT DO NOTHING` raised
    // the very violation it was written to skip.
    InsertConflict {
        do_nothing: row_data.on_conflict_do_nothing,
        update: row_data.on_conflict_update.clone(),
    }
    .restore(&mut purpose);
    Some(Ok(purpose))
}

/// Computes a table's `GENERATED ALWAYS AS (…) STORED` columns for one row and
/// rewrites the statement to carry them.
///
/// `None` when the table declares none, which is the common case and costs one
/// map lookup. Otherwise the statement comes back with each generated column
/// appended to its column list and the computed literal to its `VALUES` tuple.
///
/// The rewritten statement replaces the original **in place** rather than being
/// re-dispatched through `execute_with_context`. A re-dispatch would reach this
/// function again, find the generated column now present in the statement, and
/// report it as a client-supplied value — the check below firing on this
/// function's own output. Substituting locally means that check sees only what
/// the client actually wrote.
///
/// # How the expression is evaluated
///
/// By substituting each referenced column's literal into the expression text
/// and folding what remains. That is done rather than building a synthetic row
/// and evaluating against it, because building one means re-implementing how a
/// literal becomes bytes — and a second implementation of that is exactly how
/// an `UPDATE` once stored `{"has ''quote''"}` where the source said
/// `{"has 'quote'"}`. Here there is no second encoder: the literal the client
/// wrote reaches the same encoder it always would, having been through an
/// arithmetic fold in between.
///
/// `schema` is the target's, resolved once by the INSERT arm (slice B8b).
fn supply_generated_values(
    row_data: &crate::core::rql::lexer_s1::InsertRowData,
    schema: Option<&crate::core::row::schema::TableSchema>,
    database: &Arc<Database>,
) -> Option<Result<crate::core::rql::lexer_s1::InsertRowData, String>> {
    let (fields, generated, defaults) = {
        let schema = schema?;
        if schema.generated.is_empty() {
            return None;
        }
        (
            schema.fields.clone(),
            schema.generated.clone(),
            schema.defaults.clone(),
        )
    };

    let assignments = crate::core::rql::lexer_ir::assignments_of_insert(&row_data.query)?;

    // A value the client supplied for a generated column is an error in
    // PostgreSQL, and reporting it matters more than accepting it: a client
    // that believed it had set the column would otherwise be silently
    // overruled. `DEFAULT` is the one spelling PostgreSQL allows, and it means
    // "compute it", so it is dropped rather than refused.
    let mut assignments = assignments;
    for g in &generated {
        let Some(position) = assignments
            .iter()
            .position(|(name, _)| name.eq_ignore_ascii_case(&g.column))
        else {
            continue;
        };
        if !assignments[position]
            .1
            .trim()
            .eq_ignore_ascii_case("DEFAULT")
        {
            return Some(Err(format!(
                "cannot insert a non-DEFAULT value into column \"{}\": it is a generated column",
                g.column
            )));
        }
        assignments.remove(position);
    }
    // A column written as `DEFAULT` holds what it would hold left out: its
    // default, which an expression reading it reads. Kept as the word, it
    // reached the expression as text ("'DEFAULT' is neither a column nor a
    // value").
    assignments.retain(|(_, value)| !value.trim().eq_ignore_ascii_case("DEFAULT"));

    // What each column holds for this row: the literal written, or its default.
    //
    // A default is evaluated only for a column an expression reads, and the
    // value it gave is then written into the statement, so the row stores the
    // value the expression saw. Evaluating every default here and leaving the
    // row writer to evaluate them again drew two values from a `BIGSERIAL`
    // per insert, so the ids went 2, 4, 6, and an expression over `id` would
    // have been computed from an id no row has.
    let read: Vec<String> = generated
        .iter()
        .flat_map(|g| expression_columns(&g.expression, &fields))
        .collect();
    let mut values: Vec<(String, String)> = assignments.clone();
    let mut supplied_defaults: Vec<(String, String)> = Vec::new();
    for field in fields.iter().filter(|f| !f.is_deleted) {
        if values
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case(&field.name))
        {
            continue;
        }
        let literal = read
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&field.name))
            .then(|| crate::core::rql::column_default::expression_for(&field.name, &defaults))
            .flatten()
            .and_then(|e| crate::core::rql::column_default::evaluate(e, &field.db_type, database.id).ok())
            .unwrap_or_else(|| "NULL".to_string());
        if !literal.eq_ignore_ascii_case("NULL") {
            supplied_defaults.push((field.name.clone(), literal.clone()));
        }
        values.push((field.name.clone(), literal));
    }
    // The expression reads each input as stored, after it is coerced to its
    // column's type, as PostgreSQL computes it.
    for (name, literal) in values.iter_mut() {
        if let Some(field) = fields.iter().find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name)) {
            *literal = crate::core::rql::generated::as_stored(literal, field);
        }
    }

    let mut computed: Vec<(String, String)> = Vec::with_capacity(generated.len());
    for g in &generated {
        let substituted = match substitute_columns(&g.expression, &values) {
            Ok(text) => text,
            Err(reason) => return Some(Err(reason)),
        };
        let field_type = fields
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(&g.column))
            .map(|f| f.db_type.clone())
            .unwrap_or(crate::core::db_type::DbType::STRING);

        // Folded against an empty row: every column reference is gone, so what
        // is left is a constant.
        let literal = match crate::core::rql::generated::parse_expression(&substituted, &[])
            .and_then(|expr| {
                crate::core::rql::expr::eval(&expr, &crate::core::row::row::Row::default(), &[])
            }) {
            Ok(crate::core::rql::expr::Value::Null) => "NULL".to_string(),
            Ok(crate::core::rql::expr::Value::Data { bytes, db_type }) => {
                // Rendered at the *column's* type, not the arithmetic's: a sum
                // of decimals folds to a decimal, and the column may be
                // narrower or wider than the fold's intermediate.
                let coerced = crate::core::rql::expr::coerce_to_public(
                    crate::core::rql::expr::Value::Data {
                        bytes: bytes.clone(),
                        db_type: db_type.clone(),
                    },
                    &field_type,
                );
                let (bytes, db_type) = match coerced {
                    crate::core::rql::expr::Value::Data { bytes, db_type } => (bytes, db_type),
                    crate::core::rql::expr::Value::Null => (bytes, db_type),
                };
                match crate::core::rql::subquery::value_to_literal(&db_type, &bytes) {
                    Some(literal) => literal,
                    None => "NULL".to_string(),
                }
            }
            Err(reason) => {
                return Some(Err(format!(
                    "the generated column \"{}\" could not be computed: {}",
                    g.column, reason
                )));
            }
        };
        computed.push((g.column.clone(), literal));
    }

    supplied_defaults.extend(computed);
    // A column the statement wrote as `DEFAULT` and that is supplied here (a
    // generated column, or a default an expression read) is taken out of the
    // statement first. Appended beside the written `DEFAULT`, the column was
    // named twice and the row builder kept the first: a generated column
    // given `DEFAULT` stored 0 or NULL instead of its expression's value.
    let written_default: Vec<String> = supplied_defaults
        .iter()
        .filter(|(name, _)| {
            crate::core::rql::lexer_ir::assignments_of_insert(&row_data.query)
                .unwrap_or_default()
                .iter()
                .any(|(c, v)| c.eq_ignore_ascii_case(name) && v.trim().eq_ignore_ascii_case("DEFAULT"))
        })
        .map(|(name, _)| name.clone())
        .collect();
    let base = if written_default.is_empty() {
        Some(row_data.query.clone())
    } else {
        crate::core::rql::lexer_ir::without_insert_assignments(&row_data.query, &written_default)
    };
    let rewritten = match base.and_then(|base| append_insert_values(&base, &supplied_defaults)) {
        Some(sql) => sql,
        None => {
            return Some(Err(format!(
                "the statement's shape could not carry the generated column of '{}'",
                row_data.table_name
            )));
        }
    };
    Some(Ok(crate::core::rql::lexer_s1::InsertRowData {
        table_name: row_data.table_name.clone(),
        query: rewritten,
        on_conflict_do_nothing: row_data.on_conflict_do_nothing,
        on_conflict_update: row_data.on_conflict_update.clone(),
        on_conflict_target: row_data.on_conflict_target.clone(),
    }))
}

/// PostgreSQL's refusals of a `CREATE TABLE`'s generation expressions; see
/// `generated::validate`. Its own function so its locals stay out of
/// `execute_with_context`'s frame.
fn validate_generated(
    create_table: &crate::core::rql::lexer_ct::CreateTable,
) -> Result<(), String> {
    if create_table.generated.is_empty() {
        return Ok(());
    }
    let columns: Vec<&str> = create_table.columns.iter().map(|c| c.name.as_str()).collect();
    crate::core::rql::generated::validate(&create_table.generated, &columns)
}

/// A `CREATE TABLE`'s `CHECK`s, each compiled against the declared columns,
/// as `ADD CONSTRAINT` compiles one. A check that could not be compiled was
/// stored, logged at every write and never enforced: `CHECK (COALESCE(x, 1)
/// > 0)` took `x = -3`.
fn validate_checks(create_table: &crate::core::rql::lexer_ct::CreateTable) -> Result<(), String> {
    if create_table.checks.is_empty() {
        return Ok(());
    }
    // Every column, the nullability companions included: a check is
    // compiled against the table as a write will see it.
    let fields: Vec<crate::core::row::schema::SchemaField> = create_table
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let mut field =
                crate::core::row::schema::SchemaField::new(c.name.clone(), c.data_type.clone(), 0, 0, i as u64, false);
            field.scale = create_table
                .type_modifiers
                .iter()
                .find(|m| m.column.eq_ignore_ascii_case(&c.name))
                .map(|m| m.scale as u32);
            field.set_text_typmod(crate::core::rql::type_modifier::text_typmod_of(&c.name, &create_table.type_modifiers));
            field
        })
        .collect();
    for check in &create_table.checks {
        match compile_check(&check.expression, &fields) {
            Err(reason) => {
                return Err(format!("check constraint \"{}\" cannot be compiled: {}", check.name, reason));
            }
            Ok(CompiledCheck::Unenforced(why)) => warn_unenforced_check(&check.name, &create_table.table_name, &why),
            Ok(_) => {}
        }
    }
    Ok(())
}

/// `UPDATE … SET c = DEFAULT`, with each such assignment of a column that is
/// not generated replaced by what PostgreSQL replaces it with: the column's
/// default expression, or NULL when it has none. `None` when the statement
/// has no such assignment.
///
/// The update parser read the word as a value: a text column stored the
/// seven characters `DEFAULT` and reported success, and an integer one was
/// refused as 22P02. A NOT NULL column with no default is refused (23502)
/// before this, by `check_not_null`, as NULL is. A generated column's
/// `DEFAULT` is left for [`generated_update_statement`].
fn default_update_statement(query: &QueryPurpose, schema: &crate::core::row::schema::TableSchema) -> Option<QueryPurpose> {
    let QueryPurpose::UpdateRow(sql) = query else {
        return None;
    };
    let defaulted = |column: &str, value: &str| {
        value.trim().eq_ignore_ascii_case("DEFAULT")
            && crate::core::rql::generated::expression_for(column, &schema.generated).is_none()
    };
    let assignments = crate::core::rql::lexer_ur::assignments_of_update(sql)?;
    if !assignments.iter().any(|(column, value)| defaulted(column, value)) {
        return None;
    }
    // Rebuilt as `generated_update_statement` rebuilds it, from the clause
    // `assignments_of_update` reads.
    let (head, returning) = match crate::core::rql::lexer_ir::returning_split(sql) {
        Some((head, tail)) => (head, Some(tail)),
        None => (sql.as_str(), None),
    };
    let set_at = crate::core::rql::join::find_keyword(head, "SET", 0)?;
    let body_at = set_at + "SET".len();
    let end = crate::core::rql::join::find_keyword(head, "WHERE", body_at).unwrap_or(head.len());
    let sets: Vec<String> = crate::core::rql::lexer_ct::split_definitions(&head[body_at..end])
        .into_iter()
        .map(|assignment| {
            let mut parts = assignment.splitn(2, '=');
            let written = parts.next().unwrap_or("").trim();
            let column = written.trim_matches('"');
            let value = parts.next().unwrap_or("").trim();
            if defaulted(column, value) {
                let default = crate::core::rql::column_default::expression_for(column, &schema.defaults)
                    .map(str::trim)
                    .filter(|e| !e.is_empty())
                    .unwrap_or("NULL");
                format!("{} = {}", written, default)
            } else {
                assignment.trim().to_string()
            }
        })
        .collect();
    let rewritten = format!(
        "{} SET {} {}{}",
        head[..set_at].trim_end(),
        sets.join(", "),
        head[end..].trim_start(),
        returning.map(|tail| format!(" RETURNING {}", tail)).unwrap_or_default()
    );
    recognize_query_purpose(rewritten.trim_end())
}

/// An `UPDATE` on a table with generated columns, checked and put in the shape
/// the update parser reads.
///
/// Assigning a generated column is refused, as PostgreSQL refuses it, except
/// to `DEFAULT`, which means "recompute it". The parser would try to store the
/// word `DEFAULT` as a value, so that assignment is rewritten to `g = g`, a
/// placeholder whose expression [`recompute_generated_in_update`] then
/// replaces. `Ok(None)` when the statement needs no rewrite.
fn generated_update_statement(
    query: &QueryPurpose,
    schema: &crate::core::row::schema::TableSchema,
) -> Result<Option<QueryPurpose>, String> {
    if schema.generated.is_empty() {
        return Ok(None);
    }
    let QueryPurpose::UpdateRow(sql) = query else {
        return Ok(None);
    };
    let Some(assignments) = crate::core::rql::lexer_ur::assignments_of_update(sql) else {
        return Ok(None);
    };
    let mut defaulted = false;
    for (name, value) in &assignments {
        if crate::core::rql::generated::expression_for(name, &schema.generated).is_none() {
            continue;
        }
        if !value.trim().eq_ignore_ascii_case("DEFAULT") {
            return Err(format!(
                "column \"{}\" can only be updated to DEFAULT: it is a generated column",
                name
            ));
        }
        defaulted = true;
    }
    if !defaulted {
        return Ok(None);
    }

    // The SET list is rebuilt with each `g = DEFAULT` replaced, and the rest
    // of the statement kept as written. Found the same way
    // `assignments_of_update` finds it, so the two read the same clause.
    let (head, returning) = match crate::core::rql::lexer_ir::returning_split(sql) {
        Some((head, tail)) => (head, Some(tail)),
        None => (sql.as_str(), None),
    };
    let malformed = || "could not read the SET clause of the UPDATE".to_string();
    let set_at = crate::core::rql::join::find_keyword(head, "SET", 0).ok_or_else(malformed)?;
    let body_at = set_at + "SET".len();
    let end = crate::core::rql::join::find_keyword(head, "WHERE", body_at).unwrap_or(head.len());
    let sets: Vec<String> = crate::core::rql::lexer_ct::split_definitions(&head[body_at..end])
        .into_iter()
        .map(|assignment| {
            let mut parts = assignment.splitn(2, '=');
            let column = parts.next().unwrap_or("").trim().trim_matches('"').to_string();
            let value = parts.next().unwrap_or("").trim();
            if value.eq_ignore_ascii_case("DEFAULT")
                && crate::core::rql::generated::expression_for(&column, &schema.generated).is_some()
            {
                format!("{} = {}", column, column)
            } else {
                assignment.trim().to_string()
            }
        })
        .collect();
    let rewritten = format!(
        "{} SET {} {}{}",
        head[..set_at].trim_end(),
        sets.join(", "),
        head[end..].trim_start(),
        returning
            .map(|tail| format!(" RETURNING {}", tail))
            .unwrap_or_default()
    );
    recognize_query_purpose(rewritten.trim_end())
        .map(Some)
        .ok_or_else(malformed)
}

/// Adds to an `UPDATE`'s plan the generated columns it has to recompute.
///
/// PostgreSQL computes a generated column from the row as updated. The plan's
/// expressions are evaluated against the row as it was, so each assigned
/// input is replaced in the generation expression by the expression assigned
/// to it: `tot = q * p` under `SET q = 10` becomes `tot = (10) * p`, and under
/// `SET p = p * 2` it becomes `tot = q * (p * 2)`. Unassigned columns read the
/// same before and after, so they are left as columns.
///
/// This used to refuse any `UPDATE` of an input, because the stale value the
/// alternative left behind would have reported success.
fn recompute_generated_in_update(
    plan: &mut crate::core::rql::lexer_ur::UpdateRow,
    sql: &str,
    schema: &crate::core::row::schema::TableSchema,
) -> Result<(), String> {
    if schema.generated.is_empty() {
        return Ok(());
    }
    let Some(assignments) = crate::core::rql::lexer_ur::assignments_of_update(sql) else {
        return Ok(());
    };
    // A generated column's own `g = g` placeholder is not an input of anything.
    // A timestamp assigned as a literal goes in typed as its column, as the
    // INSERT path substitutes it (`generated::as_stored`).
    let inputs: Vec<(String, String)> = assignments
        .into_iter()
        .filter(|(name, _)| {
            crate::core::rql::generated::expression_for(name, &schema.generated).is_none()
        })
        .map(|(name, value)| {
            let quoted = value.trim().starts_with('\'');
            match schema.fields.iter().find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&name)) {
                Some(f) if quoted && f.db_type == crate::core::db_type::DbType::DATETIME => {
                    let typed = crate::core::rql::generated::as_stored(&value, f);
                    (name, typed)
                }
                _ => (name, value),
            }
        })
        .collect();
    let fields: Vec<crate::core::row::schema::SchemaField> = schema.fields.to_vec();

    for g in schema.generated.iter().filter(|g| !g.is_deleted) {
        let Some(field) = schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&g.column))
        else {
            continue;
        };
        let write_order = field.write_order as u32;
        let defaulted = plan.updates.iter().any(|u| u.write_order == write_order);
        let read = expression_columns(&g.expression, &schema.fields);
        let touched = inputs
            .iter()
            .any(|(name, _)| read.iter().any(|r| r.eq_ignore_ascii_case(name)));
        if !touched && !defaulted {
            continue;
        }

        let substituted = substitute_columns(&g.expression, &inputs)?;
        let expression = crate::core::rql::generated::parse_expression(&substituted, &fields)
            .map_err(|reason| {
                format!(
                    "the generated column \"{}\" could not be recomputed: {}",
                    g.column, reason
                )
            })?;
        // Cast to the column's type, which is what the insert path renders the
        // value as. Arithmetic widens integers to int8 and folds into the
        // engine's decimal, and either stored raw in a narrower column is
        // another number.
        let expression = if crate::core::rql::expr::static_type(&expression) == field.db_type {
            expression
        } else {
            crate::core::rql::expr::Expr::Cast(Box::new(expression), field.db_type.clone())
        };

        plan.expressions.retain(|(wo, _)| *wo != write_order);
        plan.expressions.push((write_order, expression));
        if !defaulted {
            // The placeholder holds the column's slot in the payload list until
            // the row is seen, exactly as the update parser's own expression
            // assignments do.
            plan.updates.push(crate::core::row::row::ColumnWritePayload {
                data: crate::memory_pool::MemoryBlock::default(),
                write_order,
                column_type: field.db_type.physical(),
                size: field.size,
            });
        }
    }
    Ok(())
}

/// The schema columns an expression names.
///
/// Whole words only, and only names the schema has — so `subtotal + 30` names
/// one column, and a table with both `tax` and `tax_amount` does not have the
/// shorter name found inside the longer one.
fn expression_columns(
    expression: &str,
    fields: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
) -> Vec<String> {
    // A byte of a multi-byte character is part of a name, as it is to the
    // parser: `naïve` is one column, not `na` and `ve`.
    fn is_word_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'_' || !b.is_ascii()
    }
    let bytes = expression.as_bytes();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if !is_word_byte(bytes[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && is_word_byte(bytes[i]) {
            i += 1;
        }
        let word = &expression[start..i];
        if fields
            .iter()
            .any(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(word))
            && !out
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(word))
        {
            out.push(word.to_string());
        }
    }
    out
}

/// Replaces each column name in an expression with the literal that row holds.
///
/// Matching is on whole words. Without that, a table with columns `tax` and
/// `tax_amount` had the shorter name substituted inside the longer one, leaving
/// arithmetic over a fragment — the same prefix-matching mistake that once
/// deleted a column and a predicate elsewhere in this parser.
fn substitute_columns(expression: &str, values: &[(String, String)]) -> Result<String, String> {
    // A byte of a multi-byte character is part of a name, so `naïve` is found
    // whole. It was not, and each of its bytes outside a word was pushed as a
    // `char` of its own, turning `naïve * 2` into `naÃ¯ve * 2`.
    fn is_word_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'_' || !b.is_ascii()
    }

    let bytes = expression.as_bytes();
    let mut out = String::with_capacity(expression.len());
    let mut i = 0usize;
    while i < bytes.len() {
        // A quoted literal in the expression is copied through untouched.
        if bytes[i] == b'\'' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&expression[start..i]);
            continue;
        }
        if !is_word_byte(bytes[i]) {
            out.push(bytes[i] as char);
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && is_word_byte(bytes[i]) {
            i += 1;
        }
        let word = &expression[start..i];
        match values
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(word))
        {
            // A NULL input is substituted as NULL and left to the evaluator,
            // which is strict in NULL where PostgreSQL is: `NULL + 5` is NULL.
            // Short-circuiting the whole expression to NULL here was wrong for
            // everything that is not strict, so `COALESCE(s, 'x')`, `s IS
            // NULL` and `NULL > 1 OR r = 3` all stored NULL.
            Some((_, literal)) => {
                out.push('(');
                out.push_str(literal.trim());
                out.push(')');
            }
            None => out.push_str(word),
        }
    }
    Ok(out)
}

/// Appends `(column, literal)` pairs to an `INSERT`'s column list and its
/// single `VALUES` tuple.
///
/// `None` when the statement is not the one-tuple, explicit-column-list shape
/// this understands — which is the shape every insert has by the time it
/// reaches here, the column list having been supplied and the tuples split.
fn append_insert_values(sql: &str, extra: &[(String, String)]) -> Option<String> {
    if extra.is_empty() {
        return Some(sql.to_string());
    }
    // A RETURNING clause is set aside so its parentheses are not mistaken for
    // the value tuple's.
    let (head, returning) = match crate::core::rql::lexer_ir::returning_split(sql) {
        Some((head, tail)) => (head.to_string(), Some(tail.to_string())),
        None => (sql.to_string(), None),
    };

    let values_at = crate::core::rql::join::find_keyword(&head, "VALUES", 0)?;
    let columns_close = head[..values_at].rfind(')')?;
    let tuple_open = head[values_at..].find('(')? + values_at;
    let tuple_close = head.rfind(')')?;
    if tuple_close <= tuple_open {
        return None;
    }

    let names = extra
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>()
        .join(", ");
    let literals = extra
        .iter()
        .map(|(_, literal)| literal.clone())
        .collect::<Vec<_>>()
        .join(", ");

    let mut out = String::with_capacity(head.len() + names.len() + literals.len() + 8);
    out.push_str(&head[..columns_close]);
    out.push_str(", ");
    out.push_str(&names);
    out.push_str(&head[columns_close..tuple_close]);
    out.push_str(", ");
    out.push_str(&literals);
    out.push_str(&head[tuple_close..]);
    // `returning_split` hands back the list without its keyword, so the keyword
    // is written back here. Without it the list was left after the tuple as
    // text the row parser ignores: the insert ran, the tag said INSERT 0 1,
    // and RETURNING produced no rows.
    if let Some(returning) = returning {
        out.push_str(" RETURNING ");
        out.push_str(&returning);
    }
    Some(out)
}

async fn insert_values_batch(
    row_data: &crate::core::rql::lexer_s1::InsertRowData,
    database: &Arc<Database>,
    depth: usize,
    tx: TxId,
) -> Option<Vec<u8>> {
    let statements = match crate::core::rql::lexer_ir::split_values_tuples(&row_data.query)? {
        Err(reason) => {
            error!("INSERT rejected: {}", reason);
            return Some(ddl_error(&format!(
                "the VALUES clause could not be fully parsed ({}); refusing rather than \
                 inserting the part that was understood",
                reason
            )));
        }
        Ok(statements) => statements,
    };

    // Every tuple's foreign keys, checked in one probe per key rather than one
    // full query per row. What it verifies the per-row check then skips.
    let verified =
        prevalidate_foreign_keys(database, &row_data.table_name, &statements, tx).await;
    FK_VERIFIED
        .scope(
            std::cell::RefCell::new(verified),
            insert_values_tuples(row_data, statements, database, depth, tx),
        )
        .await
}

/// The per-tuple half of [`insert_values_batch`].
async fn insert_values_tuples(
    row_data: &crate::core::rql::lexer_s1::InsertRowData,
    statements: Vec<String>,
    database: &Arc<Database>,
    depth: usize,
    tx: TxId,
) -> Option<Vec<u8>> {
    // Two rows of one `INSERT … ON CONFLICT DO UPDATE` with the same arbiter
    // key would update one row twice, the second time the row the first had
    // just written. PostgreSQL refuses the statement (21000) rather than let
    // the order of the VALUES list decide the outcome; checked before any row
    // is written, each key as its column stores it (`stored_key_bytes`).
    if let Some(spec) = row_data.on_conflict_update.as_ref().filter(|s| !s.arbiter.is_empty()) {
        if statements.len() > 1 {
            // Compared as the column stores each key (`stored_numeric_literal`):
            // as written, `(0.995, …), (1.00, …)` into a NUMERIC(10,2) were two
            // keys, and `(5.0, …), (5.00, …)` applied last-wins.
            let schema = database.schema_view(&row_data.table_name).map(|v| {
                let schema = v.schema();
                (schema.fields.clone(), schema.defaults.clone(), schema.generated.clone())
            });
            let mut seen: std::collections::HashSet<Vec<Vec<u8>>> = std::collections::HashSet::new();
            for stmt in &statements {
                let values = crate::core::rql::lexer_ir::assignments_of_insert(stmt).unwrap_or_default();
                // Every type, not only numbers: `'A0EE…'` and `'a0ee…'` are
                // one UUID, `'2020-01-01'` and `'2020-01-01 00:00:00'` one
                // timestamp, `true` and `'t'` one boolean, and each pair
                // applied last-wins. A row whose key has a part that equals
                // nothing (`batch_key_part`) is left out.
                let key: Option<Vec<Vec<u8>>> = spec
                    .arbiter
                    .iter()
                    .map(|column| {
                        let (fields, defaults, generated) = schema.as_ref()?;
                        let field = fields.iter().find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column))?;
                        let written = values.iter().find(|(name, _)| name.eq_ignore_ascii_case(column)).map(|(_, l)| l.as_str());
                        batch_key_part(written, field, defaults, generated)
                    })
                    .collect();
                if let Some(key) = key {
                    if !seen.insert(key) {
                        return Some(ddl_error(
                            "ON CONFLICT DO UPDATE command cannot affect row a second time: \
                             ensure that no rows proposed for insertion within the same command \
                             have duplicate constrained values",
                        ));
                    }
                }
            }
        }
    }

    // Each tuple is written by a statement of its own, and a conflicting
    // one's `DO UPDATE` may overwrite its row in place, which the abort of a
    // later tuple's failure could not undo (`forbid_in_place`).
    if statements.len() > 1 && row_data.on_conflict_update.is_some() {
        database.transactions.forbid_in_place(tx);
    }

    // The statement's target, resolved once: its ROW EXCLUSIVE is held from
    // the statement's dispatch, so this is the lock's fast path, and each
    // tuple goes straight to the row's half of the insert with it.
    let target = match Box::pin(database.relation_for(&row_data.table_name, owner(), TableMode::RowExclusive)).await {
        Ok(target) => target,
        Err(e) => return Some(ddl_error(&e)),
    };

    let mut inserted = 0u64;
    let mut returned: Vec<u8> = Vec::new();
    let mut any_returning = false;
    // A tuple returned a row, so it wrote one: an inserted row or a `DO
    // UPDATE`d one. A `RETURNING` reply with no row (a `DO NOTHING` that
    // skipped, a `DO UPDATE … WHERE` that declined) wrote nothing, so it
    // leaves the statement as replayable as a skipped count of zero does.
    let mut returned_a_row = false;
    for stmt in statements {
        // A tuple that loses a uniqueness race is replayed *here*, rather than
        // by re-running the whole statement upstream, because only here is
        // "applied nothing" actually true: by the time tuple 200 conflicts,
        // tuples 1..=199 are already written, and a whole-statement replay
        // would meet its own rows.
        //
        // This is the path a migration takes, and it is how the conflict
        // reached a client after the autocommit retry was in place: PMS pages
        // its tier copy as a multi-row `INSERT … ON CONFLICT DO NOTHING`
        // inside an explicit `BEGIN`, and an explicit block does not go
        // through `execute_with_sync` at all. It aborted a demote that had
        // already copied 165,000 rows.
        // A conflict waits on the transaction it met, with a wait-for edge
        // from the statement's owner (slice B6, finding L1).
        let mut pacing = crate::core::rql::conflict_wait::Pacing::new(crate::core::rql::conflict_wait::Site::Batch);
        let response = loop {
            // The row's statement is built here, not recognized again: it was
            // cut from `row_data.query`, which the statement's own recognition
            // already normalised, stripped of comments and of `ON CONFLICT`, so
            // running the recognizer over each tuple only re-derived the table
            // name the statement already has, copying the text three times to
            // do it. A 1,000-row `INSERT` paid that 1,000 times.
            //
            // `ON CONFLICT` comes from the parsed clause, not text: it was split
            // off before the tuples were, so a row re-read from its text said
            // `on_conflict_do_nothing: false`, and `INSERT … VALUES (…), (…) ON
            // CONFLICT (k) DO NOTHING` *raised* a duplicate-key error the moment
            // any tuple conflicted, inserting none of the rest. It stalled PMS's
            // Ultra→Cold migration on 192.168.1.7, which pages exactly this
            // shape. The tail the splitter carries onto each row already holds
            // `RETURNING`, and `ON CONFLICT` has to precede it.
            let purpose = QueryPurpose::InsertRow(crate::core::rql::lexer_s1::InsertRowData {
                table_name: row_data.table_name.clone(),
                query: stmt.clone(),
                on_conflict_do_nothing: row_data.on_conflict_do_nothing,
                on_conflict_update: row_data.on_conflict_update.clone(),
                // Checked at the statement's first dispatch (see the field).
                on_conflict_target: Vec::new(),
            });
            let QueryPurpose::InsertRow(tuple) = &purpose else {
                unreachable!("built as an InsertRow just above");
            };
            crate::owner_probe!(crate::core::mvcc::owner::probe::Site::BatchTuple, tx, database);
            let (response, blocker) = crate::core::rql::conflict_wait::capture(Box::pin(insert_row_resolved(
                database,
                &purpose,
                tuple,
                target.clone(),
                depth,
                tx,
            )))
            .await;
            if !is_retryable_conflict(&response) {
                break response;
            }
            // The transaction that won has to end for this to clear: waited
            // for, in the graph, rather than slept on.
            let waiter = database.row_locks.owner_of(tx);
            match pacing.pause(database, waiter, blocker, MAX_CONFLICT_RETRIES).await {
                crate::core::rql::conflict_wait::Pause::Retry => {}
                crate::core::rql::conflict_wait::Pause::GiveUp => break response,
                crate::core::rql::conflict_wait::Pause::Fail(message) => break ddl_error(&message),
            }
        };
        match response.first() {
            // Having applied nothing, the statement as a whole is still safe
            // for the caller to replay, so the marker is left on. Once a tuple
            // has landed it is not safe, and the conflict surfaces as itself
            // rather than inviting a replay that would re-insert those rows.
            Some(3) => {
                return Some(if inserted > 0 || returned_a_row {
                    strip_conflict_marker(response)
                } else {
                    response
                });
            }
            Some(2) => {
                // RETURNING: concatenated so the batch reports every row. The
                // count comes from the rows themselves downstream, so nothing
                // is added here.
                any_returning = true;
                returned_a_row |= response.len() > 1;
                returned.extend_from_slice(&response[1..]);
            }
            // A row-count reply carries the count as a `u64`, and it is not
            // always one: `ON CONFLICT … DO NOTHING` answers zero for a tuple
            // it skipped. Counting statements instead of rows reported
            // `INSERT 0 2` for a two-tuple statement that inserted one row and
            // skipped the other — the number a client uses to decide whether
            // its write landed.
            Some(0) if response.len() >= 9 => {
                let mut n = [0u8; 8];
                n.copy_from_slice(&response[1..9]);
                inserted += u64::from_le_bytes(n);
            }
            _ => inserted += 1,
        }
    }

    if any_returning {
        let mut result = vec![2u8];
        result.extend_from_slice(&returned);
        return Some(result);
    }
    let mut result = vec![0u8];
    result.extend_from_slice(&inserted.to_le_bytes());
    Some(result)
}

/// Whether `CREATE TABLE IF NOT EXISTS` has nothing to do.
///
/// Checked before the create rather than by interpreting its failure, because
/// `add_table` reports one error for every reason it can fail — so "already
/// exists" and "the schema was invalid" would be indistinguishable.
///
/// `#[inline(never)]` is load-bearing, not a hint: inlined, its `info!` format
/// temporaries land back in `execute_with_context`'s frame, which is what
/// overflowed the stack in the first place.
#[inline(never)]
/// Runs a `STORAGE ...` / `SHOW STORAGE ...` statement.
///
/// Out of line so that its locals do not join `execute_with_context`'s frame —
/// see the note on the `CREATE TABLE` arm.
#[cfg(feature = "storage_pools")]
async fn execute_storage(sql: &str, database: &Arc<Database>) -> Vec<u8> {
    // The structured forms return rows rather than lines; everything else in
    // this surface is text meant to be read.
    if crate::core::storage::sql::is_structured(sql) {
        return match crate::core::storage::sql::execute_structured(sql, database) {
            Ok(rows) => rows,
            Err(e) => {
                error!("storage statement failed: {}", e);
                ddl_error(&e)
            }
        };
    }
    match crate::core::storage::sql::execute(sql, database).await {
        Ok(lines) => {
            info!("storage: {}", lines.join("; "));
            crate::core::storage::sql::text_result(&lines)
        }
        Err(e) => {
            error!("storage statement failed: {}", e);
            ddl_error(&e)
        }
    }
}

fn create_table_already_satisfied(
    create_table: &crate::core::rql::lexer_ct::CreateTable,
    database: &Arc<Database>,
) -> bool {
    if !create_table.if_not_exists {
        return false;
    }
    // A partitioned table that could not be opened has no shell in `tables`,
    // but it exists: a CREATE would only be refused with "already exists".
    let exists = database.has_table(&create_table.table_name)
        || database.is_partitioned(&create_table.table_name);
    if exists {
        info!(
            "Table '{}' already exists; IF NOT EXISTS makes that success",
            create_table.table_name
        );
    }
    exists
}

/// `CREATE TABLE … PARTITION BY HASH`. The database makes every check that
/// depends on it (nullability, uniqueness, foreign keys, the cap, the
/// storage); replicated as the statement text, like a plain CREATE, so a
/// replica re-runs those same checks on the same replicated state.
async fn execute_create_partitioned(
    database: &Arc<Database>,
    create_table: crate::core::rql::lexer_ct::CreateTable,
    clause: crate::core::rql::lexer_ct::PartitionClause,
    tx: TxId,
    depth: usize,
    query: &QueryPurpose,
) -> Vec<u8> {
    let name = create_table.table_name.clone();
    let key: Vec<&str> = clause.columns.iter().map(String::as_str).collect();
    match database
        .create_partitioned_table(create_table, &key, clause.partitions_or_default())
        .await
    {
        Ok(()) => {
            info!("Partitioned table {} created", name);
            note_write(database, tx, depth, &query_sql(query), None);
            vec![0u8]
        }
        Err(e) => {
            error!("Cannot create partitioned table {}: {}", name, e);
            // Past its catalogue row the table is installed whatever the sync
            // or the open said, and replication is told so, as for a plain
            // CREATE. A refusal (the name taken, a key it cannot route) left
            // nothing.
            if !matches!(
                e.kind(),
                std::io::ErrorKind::InvalidInput | std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::Unsupported
            ) && database.is_partitioned(&name)
            {
                note_write(database, tx, depth, &query_sql(query), None);
            }
            ddl_error(&partitioned_ddl_message(&e))
        }
    }
}

/// An error from partitioned DDL, worded so the wire reports what its kind
/// says: an `Unsupported` refusal whose text lacks "not supported" (the
/// nullability and uniqueness refusals) would otherwise reach the client as
/// XX000, an internal error, rather than 0A000.
fn partitioned_ddl_message(e: &std::io::Error) -> String {
    let message = e.to_string();
    if e.kind() == std::io::ErrorKind::Unsupported
        && !message.to_ascii_lowercase().contains("not supported")
    {
        format!("{message} (not supported on a partitioned table)")
    } else {
        message
    }
}

/// Refuses an `ON CONFLICT` target on a partitioned table that leaves out a
/// partition-key column, with PostgreSQL's 42P10 ("there is no unique or
/// exclusion constraint matching the ON CONFLICT specification"): every
/// unique key of a partitioned table holds every key column
/// (`docs/partitioning.md` §1.5), so such a target matches none, in
/// PostgreSQL as here. Checked at the statement's first dispatch, before
/// anything is read or written. A plain table, a targetless clause and `ON
/// CONSTRAINT` pass: a constraint is a unique key, which holds the key.
///
/// That is what makes ON CONFLICT exact per partition (S18): the target's
/// key columns place the probe, the insert and the follow-up `UPDATE` in the
/// row's one partition, whose own uniqueness check (in-flight writers
/// included) decides the conflict.
/// An `ON CONFLICT (cols)` target names exactly the columns of one unique key,
/// in any order, or the statement is refused with 42P10, as PostgreSQL infers
/// its arbiter index.
///
/// A target that is only part of a key was taken as the key: `ON CONFLICT
/// (tenant_id) DO UPDATE` over a key on `(tenant_id, grp, id)` rewrote to an
/// UPDATE matching every row of the tenant, and updated them all. A target
/// that is a superset of a key is not a key either, so it is refused too.
fn check_conflict_target_infers_a_key(
    database: &Database,
    row_data: &crate::core::rql::lexer_s1::InsertRowData,
) -> Result<(), String> {
    let target: &[String] = match &row_data.on_conflict_update {
        Some(spec) => &spec.arbiter,
        None if row_data.on_conflict_do_nothing => &row_data.on_conflict_target,
        None => return Ok(()),
    };
    if target.is_empty() {
        return Ok(());
    }
    // Schema only, under the INSERT's ROW EXCLUSIVE from the statement's set.
    let schema = match database.schema_view(&row_data.table_name) {
        Some(view) => view.schema().clone(),
        None => match database.partitioned_table(&row_data.table_name) {
            Some(pt) => pt.shell().schema.clone(),
            None => return Ok(()),
        },
    };
    let mut wanted: Vec<String> = target.iter().map(|c| c.trim_matches('"').to_ascii_lowercase()).collect();
    wanted.sort();
    wanted.dedup();
    let same = |columns: &[String]| {
        let mut have: Vec<String> = columns.iter().map(|c| c.to_ascii_lowercase()).collect();
        have.sort();
        have.dedup();
        have == wanted
    };
    let single = |name: &str| wanted.len() == 1 && wanted[0].eq_ignore_ascii_case(name);
    let inferred = schema.unique_keys.iter().any(|k| !k.is_deleted && same(&k.columns))
        || schema.primary_key.as_deref().is_some_and(single)
        || schema.fields.iter().any(|f| f.is_unique && !f.is_deleted && single(&f.name));
    if inferred {
        Ok(())
    } else {
        Err(format!(
            "there is no unique or exclusion constraint matching the ON CONFLICT specification: \
             no unique key of \"{}\" is exactly ({})",
            row_data.table_name,
            target.join(", ")
        ))
    }
}

fn check_partitioned_conflict_target(
    database: &Database,
    row_data: &crate::core::rql::lexer_s1::InsertRowData,
) -> Result<(), String> {
    let table = &row_data.table_name;
    let target: &[String] = match &row_data.on_conflict_update {
        Some(spec) => &spec.arbiter,
        None if row_data.on_conflict_do_nothing => &row_data.on_conflict_target,
        None => return Ok(()),
    };
    if target.is_empty() {
        return Ok(());
    }
    let Some(pt) = database.partitioned_table(table) else {
        return Ok(());
    };
    let spec = pt.spec().map_err(|e| e.to_string())?;
    let fields = &pt.shell().schema.fields;
    for level in spec.levels() {
        let Some(field) = fields
            .iter()
            .find(|f| !f.is_deleted && f.write_order == u64::from(level.write_order))
        else {
            continue;
        };
        if !target.iter().any(|c| c.eq_ignore_ascii_case(&field.name)) {
            return Err(format!(
                "there is no unique or exclusion constraint matching the ON CONFLICT specification: \
                 every unique key of partitioned table \"{table}\" holds its partition key column \
                 \"{}\", which the target ({}) leaves out",
                field.name,
                target.join(", ")
            ));
        }
    }
    Ok(())
}

/// `ALTER TABLE t ADD [CONSTRAINT name] FOREIGN KEY (c) REFERENCES p (k) …`
/// on a plain or a partitioned table, onto a plain or a partitioned parent
/// (phase 3 P4), under the statement's ACCESS EXCLUSIVE on `t` and SHARE on
/// `p` (`alter_lock_set`), so neither side's rows or keys change while it
/// runs. Checked as PostgreSQL checks it, before anything is written:
///
/// - the name as written (bare or quoted, cut to its stored form), 42710 when
///   another constraint of the table has it; unnamed, PostgreSQL's
///   `<t>_<c>_fkey`, numbered past the taken names;
/// - the referencing column (42703), the referenced table (42P01), column
///   (42703) and a unique key on exactly it (42830); on a partitioned parent
///   that column is its whole partition key, so every check routes;
/// - **every existing row** (23503): each distinct non-NULL value of `c`,
///   in every partition of a partitioned `t`, must be a key of `p`, probed
///   as an insert's check probes it (routed on a partitioned `p`). The plain
///   path used to add the key unvalidated, so rows that already broke it
///   stayed, and the table held a constraint that was never true of it.
async fn add_foreign_key(
    database: &Arc<Database>,
    table_name: &str,
    schema: &crate::core::row::schema::TableSchema,
    description: &str,
    tx: TxId,
) -> Vec<u8> {
    use crate::core::rql::foreign_key;

    let (named, rest) = match crate::core::rql::unique_key::split_constraint_name(description) {
        Ok(split) => split,
        Err(e) => return ddl_error(&e),
    };
    // `NOT VALID`: the key is enforced on every write from now on, and the
    // rows already there are left unchecked until `VALIDATE CONSTRAINT`, as
    // in PostgreSQL. What an import uses for a source key PostgreSQL itself
    // never validated.
    let (rest, not_valid) = strip_not_valid(rest);
    let clause_at = crate::core::rql::join::find_keyword(rest, "FOREIGN", 0).or_else(|| crate::core::rql::join::find_keyword(rest, "REFERENCES", 0)).unwrap_or(0);
    let mut fk = match foreign_key::parse_clause(&rest[clause_at..], table_name, named.clone(), None) {
        Ok(fk) => fk,
        Err(e) => return ddl_error(&e),
    };
    if !crate::core::rql::nullable::visible_fields(&schema.fields)
        .iter()
        .any(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&fk.column))
    {
        return ddl_error(&format!("column \"{}\" referenced in foreign key constraint does not exist", fk.column));
    }
    if named.is_some() {
        if constraint_name_taken(schema, table_name, &fk.name) {
            return ddl_error(&format!("constraint \"{}\" for table \"{}\" already exists", fk.name, table_name));
        }
    } else {
        let stem = foreign_key::key_stem(table_name, &fk.column);
        fk.name = match crate::core::rql::unique_key::choose_name(&stem, foreign_key::FKEY_LABEL, |candidate| {
            constraint_name_taken(schema, table_name, candidate)
        }) {
            Ok(name) => name,
            Err(e) => return ddl_error(&e),
        };
    }
    if let Err(e) = fk.storable() {
        return ddl_error(&e);
    }
    let refusal = if fk.parent_table.eq_ignore_ascii_case(table_name) {
        let key = match database.partitioned_table(table_name) {
            Some(pt) => match pt.key_columns() {
                Ok(key) => Some(key),
                Err(e) => return ddl_error(&e.to_string()),
            },
            None => None,
        };
        foreign_key::referenced_key_refusal(&fk, &foreign_key::ReferencedSide::of_schema(schema, table_name, key))
    } else {
        database.existing_parent_refusal(&fk)
    };
    if let Some(why) = refusal {
        return ddl_error(&why);
    }
    // The two columns' types (42804): the child's as it is, the parent's as
    // it is (this table's own, for a self-reference).
    let child_type = schema.fields.iter().find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&fk.column)).map(|f| f.db_type.clone());
    let parent_type = if fk.parent_table.eq_ignore_ascii_case(table_name) {
        schema.fields.iter().find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&fk.parent_column)).map(|f| f.db_type.clone())
    } else {
        database.column_type(&fk.parent_table, &fk.parent_column)
    };
    if let (Some(child), Some(parent)) = (child_type, parent_type)
        && let Some(why) = foreign_key::type_refusal(&fk, &child, &parent)
    {
        return ddl_error(&why);
    }
    if not_valid {
        info!("ALTER TABLE {table_name}: foreign key {} added NOT VALID; its existing rows are not checked", fk.name);
    } else {
        match existing_rows_break_foreign_key(database, table_name, &fk, tx).await {
            Ok(None) => {}
            Ok(Some(violation)) => return ddl_error(&violation),
            Err(e) => return ddl_error(&format!("foreign key \"{}\" could not be validated: {e}", fk.name)),
        }
    }
    // §7 step 3: the declaration is the reload's work, under this table's
    // ACCESS EXCLUSIVE and SHARE on the parent (finding L11: a parent DELETE
    // in flight finishes before the key exists, and none starts until it is
    // enforced). The sidecar is the source of truth; reloading picks it up,
    // and the reload's swap files it in `fk_reverse`.
    match database
        .reload_with(table_name, |old| async move {
            foreign_key::append(old.io_schema.clone(), &fk).await.map_err(|e| e.to_string())
        })
        .await
    {
        Ok(()) => ddl_ok(0),
        Err(failure) => reload_failed(failure, ""),
    }
}

/// `clause` without a trailing `NOT VALID` (two whole words, any case),
/// and whether it had one.
fn strip_not_valid(clause: &str) -> (&str, bool) {
    let trimmed = clause.trim_end().trim_end_matches(';').trim_end();
    let mut words = trimmed.rsplitn(3, char::is_whitespace);
    match (words.next(), words.next(), words.next()) {
        (Some(valid), Some(not), Some(head)) if valid.eq_ignore_ascii_case("VALID") && not.eq_ignore_ascii_case("NOT") => {
            (head.trim_end(), true)
        }
        _ => (clause, false),
    }
}

/// The violation an `ADD FOREIGN KEY` (or `VALIDATE CONSTRAINT`) is refused
/// for: the first distinct non-NULL value of the key's column, in
/// `table_name`'s rows (every partition of a partitioned one) as of now, that
/// is no key of the parent. `None` when every row satisfies it.
///
/// **Streamed, in bounded memory** (P4 round 2): the first build read the
/// whole child into a `Vec<Row>` and a `Vec<String>`, about 630 bytes a row,
/// so 6 GB for a ten-million-row child at an import's last step. Now each
/// table's pointer file is walked as a scan walks it (a read ticket held
/// against reclamation, deleted slots and invisible versions skipped by the
/// scans' own `Visibility::visible`), only the column and its NULL companion
/// fetched into one reused `Row`, and values gathered into a chunk of
/// [`REFERENCED_PROBE_CHUNK`] distinct literals, probed and dropped. The
/// dedupe set is bounded ([`VALIDATION_SEEN_CAP`]; cleared when full, so a
/// value seen again after that is probed again, which only costs a probe).
/// Memory is O(chunk + cap), whatever the child's size; time stays one pass.
///
/// Each value is probed as an insert's check probes it ([`referenced_values_now`],
/// the child's literal against the parent's column, routed on a partitioned
/// parent). A chunk whose values were all found proves them (the probe
/// returns only values some literal denotes, deduplicated); one that came up
/// short is split in halves until the missing value is alone.
///
/// The caller holds ACCESS EXCLUSIVE on the child (ADD, VALIDATE), so no
/// statement writes it during the walk, and SHARE on the parent.
async fn existing_rows_break_foreign_key(
    database: &Arc<Database>,
    table_name: &str,
    fk: &crate::core::rql::foreign_key::ForeignKey,
    tx: TxId,
) -> Result<Option<String>, String> {
    // Held by the statement (ACCESS EXCLUSIVE): confirmed, never waited for.
    let Some(child) = database.relation_for(table_name, owner(), TableMode::AccessExclusive).await? else {
        return Err(format!("relation \"{table_name}\" does not exist"));
    };
    let visibility = match &child {
        crate::core::database::Relation::Plain(table) => {
            table.data().map_err(|e| e.to_string())?;
            table.visibility_now_for(tx)
        }
        crate::core::database::Relation::Partitioned(pt) => pt.visibility_now_for(tx),
    };
    let schema = &child.schema().fields;
    let Some(field) = schema.iter().find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&fk.column)).cloned() else {
        return Err(format!("column \"{}\" of relation \"{}\" does not exist", fk.column, table_name));
    };
    let flag = crate::core::rql::nullable::flag_field(&field.name, schema).cloned();
    let mut wanted = vec![field.clone()];
    wanted.extend(flag.clone());
    let fetch = crate::core::rql::join::fetch_for(&wanted, schema);
    let holders = database.row_tables_held(table_name).map_err(|e| e.to_string())?;
    use crate::core::storage_providers::traits::StorageIO as _;

    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut pending: Vec<String> = Vec::with_capacity(REFERENCED_PROBE_CHUNK);
    let mut row = crate::core::row::row::Row::default();
    for holder in &holders {
        let d = holder.data().map_err(|e| e.to_string())?;
        // A read ticket, as a scan holds: reclamation hands out no extent
        // this walk may still fetch from.
        let _ticket = d.free_space.is_enabled().then(|| d.readers.enter());
        let n_slots = d.io_pointers.get_len().await / crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN as u64;
        let mut pointers = crate::core::row::row_pointer::RowPointerIterator::new(d.io_pointers.clone())
            .await
            .map_err(|e| format!("could not read \"{table_name}\" to validate the key: {e:?}"))?;
        let mut walked = 0u64;
        while let Some((offset, pointer)) = pointers
            .next_row_pointer_with_offset()
            .await
            .map_err(|e| format!("could not read \"{table_name}\" to validate the key: {e:?}"))?
        {
            if offset / crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN as u64 >= n_slots {
                break;
            }
            walked += 1;
            if walked % 4096 == 0 {
                if let Some(reason) = crate::core::cancel::tripped() {
                    return Err(reason.eval_error().as_str().to_string());
                }
                tokio::task::yield_now().await;
            }
            if !visibility.visible(pointer.deleted, pointer.xmin, pointer.xmax) {
                continue;
            }
            pointer
                .fetch_row_reuse_async(&*d.io_rows, &fetch, &mut row)
                .await
                .map_err(|e| format!("could not read a row of \"{table_name}\" to validate the key: {e}"))?;
            let cell = |write_order: u64| row.columns.iter().find(|c| u64::from(c.schema_id) == write_order);
            if let Some(f) = &flag
                && cell(f.write_order).is_some_and(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0)
            {
                continue;
            }
            let Some(value) = cell(field.write_order) else {
                continue;
            };
            let bytes = value.data.into_slice();
            if seen.contains(bytes) {
                continue;
            }
            if seen.len() >= VALIDATION_SEEN_CAP {
                seen.clear();
            }
            seen.insert(bytes.to_vec());
            pending.push(key_literal(&field, bytes)?);
            if pending.len() >= REFERENCED_PROBE_CHUNK {
                if let Some(violation) = first_value_without_parent(database, table_name, fk, &pending, tx).await? {
                    return Ok(Some(violation));
                }
                pending.clear();
            }
        }
    }
    first_value_without_parent(database, table_name, fk, &pending, tx).await
}

/// Distinct values [`existing_rows_break_foreign_key`] remembers before it
/// forgets them all and starts again: 64 Ki, a few megabytes.
const VALIDATION_SEEN_CAP: usize = 64 * 1024;

/// The first of `literals` (child values of `fk`'s column) that is no key of
/// its parent, as `existing_rows_break_foreign_key` reports it, or `None`.
async fn first_value_without_parent(
    database: &Arc<Database>,
    table_name: &str,
    fk: &crate::core::rql::foreign_key::ForeignKey,
    literals: &[String],
    tx: TxId,
) -> Result<Option<String>, String> {
    let violation = |value: &str| {
        format!(
            "insert or update on table \"{}\" violates foreign key constraint \"{}\": key ({})=({}) is not \
             present in table \"{}\"",
            table_name,
            fk.name,
            fk.column,
            value.trim_matches('\''),
            fk.parent_table
        )
    };
    // Each value at the parent column's type (P4 round 3); one that type
    // cannot hold unchanged has no parent.
    let fields = database.schema_view(table_name).and_then(|view| fk_key_fields(database, view.schema(), fk));
    // (the child's value as it is named, the value probed)
    let mut probes: Vec<(&String, String)> = Vec::with_capacity(literals.len());
    for literal in literals {
        let coerced = match &fields {
            Some(((c, cm), (p, pm))) => coerce_key_literal(literal, c, cm, p, pm),
            None => Some(literal.clone()),
        };
        match coerced {
            Some(probe) => probes.push((literal, probe)),
            None => return Ok(Some(violation(literal))),
        }
    }
    let mut pending: Vec<&[(&String, String)]> = probes.chunks(REFERENCED_PROBE_CHUNK).rev().collect();
    while let Some(chunk) = pending.pop() {
        if chunk.is_empty() {
            continue;
        }
        let values: Vec<String> = chunk.iter().map(|(_, probe)| probe.clone()).collect();
        let found =
            referenced_values_now(database, &fk.parent_table, &fk.parent_column, &values, tx, None, KeySide::Parent)
                .await?;
        if found.len() >= chunk.len() {
            continue;
        }
        if chunk.len() == 1 {
            return Ok(Some(violation(chunk[0].0)));
        }
        let (left, right) = chunk.split_at(chunk.len() / 2);
        pending.push(right);
        pending.push(left);
    }
    Ok(None)
}

/// Rewrites every JSON column's default in a `CREATE TABLE` into the reference
/// its document was stored as.
async fn resolve_json_defaults(
    database: &Arc<Database>,
    mut create_table: crate::core::rql::lexer_ct::CreateTable,
    tx: TxId,
) -> Result<crate::core::rql::lexer_ct::CreateTable, String> {
    if create_table.json_columns.is_empty() || create_table.defaults.is_empty() {
        return Ok(create_table);
    }
    let table_name = create_table.table_name.clone();
    for entry in create_table.defaults.iter_mut() {
        let is_json = create_table
            .json_columns
            .iter()
            .any(|j| j.column.eq_ignore_ascii_case(&entry.column) && !j.is_deleted);
        if !is_json {
            continue;
        }
        if let Some(resolved) = json_default_reference(
            database,
            &table_name,
            &entry.column,
            Some(entry.expression.clone()),
            true,
            tx,
        )
        .await?
        {
            entry.expression = resolved;
        }
    }
    Ok(create_table)
}

/// Turns a JSON column's `DEFAULT '<document>'` into the reference the rows
/// taking that default will hold.
///
/// A JSON column stores a UUID pointing at the document's rows, so a default
/// written as JSON text has to become one too. Without this the literal was
/// handed to a UUID column, failed to encode, and left the column empty — which
/// read back as `{}` whatever the default actually said, so `DEFAULT '[]'` and
/// `DEFAULT '{"k":1}'` produced the same wrong answer and reported success.
///
/// The document is stored once and shared by every row that takes the default.
/// That is safe because nothing writes through the reference: assigning a new
/// value to a JSON column stores a fresh document and points the row at that
/// instead.
async fn json_default_reference(
    database: &Arc<Database>,
    table: &str,
    column: &str,
    expression: Option<String>,
    is_json: bool,
    tx: TxId,
) -> Result<Option<String>, String> {
    crate::owner_probe!(crate::core::mvcc::owner::probe::Site::JsonDefault, tx, database);
    let Some(expression) = expression else {
        return Ok(None);
    };
    if !is_json {
        return Ok(Some(expression));
    }
    let text = crate::core::rql::column_default::strip_redundant_cast(expression.trim());
    if text.eq_ignore_ascii_case("NULL") {
        return Ok(Some(expression));
    }
    let unquoted = text.trim_matches('\'');
    let document: serde_json::Value = match serde_json::from_str(unquoted) {
        Ok(v) => v,
        // Not JSON text at all — a function call, say. Left as written.
        Err(_) => return Ok(Some(expression)),
    };
    if document.is_null() {
        return Ok(Some(expression));
    }
    let shadow = crate::core::rql::json_map::shadow_table_name(table, column);
    // Keys first, then the rows (`shadow_lock`).
    crate::core::rql::shadow_lock::scoped(table, true, Box::pin(store_json_document(database, &shadow, &document, 0, tx)))
        .await?;
    let reference = crate::core::rql::shadow_lock::scoped(
        table,
        false,
        Box::pin(store_json_document(database, &shadow, &document, 0, tx)),
    )
    .await?;
    Ok(Some(reference))
}

/// Whether a `DEFAULT` for a JSON column is a value the mapping can hold.
///
/// Every JSON value is: an object becomes columns, an array becomes rows, and a
/// bare scalar becomes one row holding the value. The scalar case was refused
/// while the mapping had no top-level representation for it; it does now, and
/// this has to agree with the insert path or a default would be rejected at
/// declaration time for a value an `INSERT` accepts.
///
/// What is still refused is text that is not JSON at all, which the check runs
/// at declaration time to catch loudly rather than storing something nobody
/// wrote.
fn json_default_is_an_object(expression: &str) -> Result<(), String> {
    // The same stripper `column_default::evaluate` applies, so both see the
    // value rather than one of them seeing the cast too.
    let text = crate::core::rql::column_default::strip_redundant_cast(expression.trim());
    if text.eq_ignore_ascii_case("NULL") {
        return Ok(());
    }
    let unquoted = text.trim_matches('\'');
    match serde_json::from_str::<serde_json::Value>(unquoted) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("DEFAULT {} is not valid JSON: {}", text, e)),
    }
}

/// Runs a statement and turns a failure into a message.
/// Runs a statement the JSON mapping generated, inside the caller's
/// transaction.
///
/// Inside it, not beside it. Each of these used to go through [`execute`] —
/// its own autocommit, its own `sync_durable`, its own outcome flush — so a
/// document with N nested objects cost a client statement N+1 transactions and
/// 2(N+1) device flushes, and an import that batches 5,000 rows to one
/// transaction paid the flushes per row anyway. Joining `tx` makes the shadow
/// rows part of the statement that caused them: durable when it is, gone when
/// it is not, and published to peers as one family.
///
/// The DDL a new key needs (`CREATE TABLE`, `ADD COLUMN`) joins too. DDL is
/// not transactional in this engine either way, so a rollback leaves it behind
/// as it always did; what joining buys is that the rewrite `ADD COLUMN` runs
/// reads through `tx`'s snapshot and therefore keeps the rows `tx` has written
/// and not yet committed. Run beside the transaction it would not see them,
/// and the drop-and-recreate would discard them with nothing reporting it.
async fn run_ddl(database: &Arc<Database>, sql: &str, tx: TxId) -> Result<(), String> {
    crate::owner_probe!(crate::core::mvcc::owner::probe::Site::ShadowDdl, tx, database);
    let purpose =
        recognize_query_purpose(sql).ok_or_else(|| format!("could not parse: {}", sql))?;
    let response = Box::pin(execute_in_txn(purpose, database.clone(), tx)).await;
    if response.first() == Some(&3) {
        // With the statement, truncated. "Schema mismatch" on a statement the
        // engine built for itself says nothing about which statement, and the
        // one the client sent is not it.
        const SHOWN: usize = 400;
        let excerpt: String = if sql.chars().count() > SHOWN {
            format!(
                "{}… [{} chars]",
                sql.chars().take(SHOWN).collect::<String>(),
                sql.chars().count()
            )
        } else {
            sql.to_string()
        };
        return Err(format!(
            "{}\n  while storing a JSON document: {}",
            String::from_utf8_lossy(&response[1..]),
            excerpt
        ));
    }
    Ok(())
}

/// Rebuilds the document stored under `reference` in `shadow_table`.
///
/// One reference may cover several rows: an array is stored as a row per
/// element, so the rows are gathered and ordered by `__json_idx` before being
/// rebuilt. An object still occupies exactly one row and takes the same path it
/// always did.
/// Retype every NULL column in `rows` as `DbType::NULL`, from its companion.
///
/// A nullable column stores its NULL-ness in a hidden `__null_<name>` flag
/// column, so the value column of a NULL holds zeros — an empty string for
/// TEXT, `0` for `BIGINT`. The row-mode select path retypes those before it
/// trims the companions away; the aggregate path folds the scanned rows
/// without ever doing so, and therefore grouped on the zeros: `GROUP BY label`
/// merged NULL with `''`, and `GROUP BY amount` reported a NULL group as `0`.
///
/// Matched by write order rather than by position, unlike the select path: the
/// fold's input is whatever the scan returned, whose column order is not the
/// output order the flag positions are indexed against.
fn retype_nulls_from_flags(
    schema: &crate::core::row::schema::TableSchema,
    rows: &mut [crate::core::row::row::Row],
) {
    let pairs = null_flag_pairs(schema);
    if pairs.is_empty() {
        return;
    }
    for row in rows.iter_mut() {
        retype_row_nulls(&pairs, row);
    }
}

/// `(value write_order, flag write_order)` for every nullable column.
fn null_flag_pairs(schema: &crate::core::row::schema::TableSchema) -> Vec<(u64, u64)> {
    schema
        .fields
        .iter()
        .filter(|f| !crate::core::rql::nullable::is_hidden(&f.name))
        .filter_map(|f| {
            crate::core::rql::nullable::flag_field(&f.name, &schema.fields)
                .map(|flag| (f.write_order, flag.write_order))
        })
        .collect()
}

/// [`retype_nulls_from_flags`] for one row.
fn retype_row_nulls(pairs: &[(u64, u64)], row: &mut crate::core::row::row::Row) {
    for (value_order, flag_order) in pairs {
        let is_null = row
            .columns
            .iter()
            .find(|c| u64::from(c.schema_id) == *flag_order)
            .is_some_and(|c| crate::core::rql::nullable::flag_is_set(c.data.into_slice()));
        if is_null {
            if let Some(c) = row.columns.iter_mut().find(|c| u64::from(c.schema_id) == *value_order) {
                c.column_type = crate::core::db_type::DbType::NULL;
            }
        }
    }
}

/// `RDB_JOIN_ROWS=1`: scan a join's relations into `Vec<Row>`, the path the
/// serialized `RowSet` replaced. Read once. For A/B measurement only.
fn legacy_join_rows() -> bool {
    static LEGACY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *LEGACY.get_or_init(|| std::env::var_os("RDB_JOIN_ROWS").is_some_and(|v| v == "1"))
}

/// `RDB_FOLD_ROWS=1`: fold through a materialised `Vec<Row>`, the path the
/// streaming fold replaced. Read once. For A/B measurement only.
fn legacy_row_fold() -> bool {
    static LEGACY: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *LEGACY.get_or_init(|| std::env::var_os("RDB_FOLD_ROWS").is_some_and(|v| v == "1"))
}

/// Folds a scan's serialized rows into groups, decoding one row at a time.
///
/// Each row is retyped from its NULL flags exactly as
/// [`retype_nulls_from_flags`] retypes a `Vec<Row>`, then pushed into the
/// fold, then dropped — so what is resident is the scan's compact bytes and
/// the groups, never every matched row as a `Row`.
fn fold_serialized(
    schema: &crate::core::row::schema::TableSchema,
    plan: &crate::core::rql::aggregate::AggregatePlan,
    bytes: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let pairs = null_flag_pairs(schema);
    let fold_part = |part: &[u8]| -> Result<crate::core::rql::aggregate::Folder<'_>, String> {
        let mut folder = crate::core::rql::aggregate::Folder::new(plan);
        for slice in crate::core::row::row::serialized_rows(part) {
            let slice = slice.map_err(|e| format!("a scanned row could not be read: {}", e))?;
            let mut row = crate::core::row::row::Row::decode(slice)
                .map_err(|e| format!("a scanned row could not be read: {}", e))?;
            if !pairs.is_empty() {
                retype_row_nulls(&pairs, &mut row);
            }
            folder.push(&row)?;
        }
        Ok(folder)
    };

    // A large input is folded in parallel: contiguous parts, one folder each,
    // merged in order. Merging part 0, then 1, … keeps the serial fold's
    // first-seen group order, and the first error is the one the serial fold
    // would have hit (every earlier part folded cleanly).
    let parts = fold_parts(bytes);
    if parts.len() <= 1 {
        return fold_part(bytes)?.finish_into(out);
    }
    // Many groups make every part's table nearly as large as the whole one, so
    // the merge costs what the parallelism saved, and the partial tables cost
    // memory (measured: 100k groups over 1M rows, 256 MB serial against 413 MB
    // in eight parts, no faster). The first part is folded alone first; if it
    // already holds more than one group per 16 rows the rest is folded
    // serially into it.
    let mut folder = fold_part(parts[0])?;
    let sampled = crate::core::row::row::count_serialized_rows(parts[0]) as usize;
    if folder.groups().saturating_mul(16) > sampled {
        for part in &parts[1..] {
            for slice in crate::core::row::row::serialized_rows(part) {
                let slice = slice.map_err(|e| format!("a scanned row could not be read: {}", e))?;
                let mut row = crate::core::row::row::Row::decode(slice)
                    .map_err(|e| format!("a scanned row could not be read: {}", e))?;
                if !pairs.is_empty() {
                    retype_row_nulls(&pairs, &mut row);
                }
                folder.push(&row)?;
            }
        }
        return folder.finish_into(out);
    }
    let folded: Vec<Result<crate::core::rql::aggregate::Folder<'_>, String>> =
        std::thread::scope(|scope| {
            // The statement's clock goes with each worker: an aggregate's
            // FILTER or argument may read `now()`.
            let clock = crate::core::statement_clock::current();
            let fold_part = &fold_part;
            let handles: Vec<_> = parts
                .iter()
                .skip(1)
                .map(|part| scope.spawn(move || crate::core::statement_clock::on_thread(clock, || fold_part(part))))
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or_else(|_| Err("a fold worker panicked".to_string())))
                .collect()
        });
    for part in folded {
        folder.merge(part?);
    }
    folder.finish_into(out)
}

/// Splits a scan's serialized rows into contiguous parts for a parallel fold:
/// one part below [`PARALLEL_FOLD_MIN_ROWS`], otherwise one per available
/// core up to eight (`RDB_FOLD_THREADS` overrides, `1` forces the serial
/// fold). Parts end on row boundaries.
fn fold_parts(bytes: &[u8]) -> Vec<&[u8]> {
    const PARALLEL_FOLD_MIN_ROWS: u64 = 100_000;
    static THREADS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    let threads = *THREADS.get_or_init(|| {
        std::env::var("RDB_FOLD_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get().min(8)))
            .max(1)
    });
    let rows = crate::core::row::row::count_serialized_rows(bytes);
    if threads <= 1 || rows < PARALLEL_FOLD_MIN_ROWS {
        return vec![bytes];
    }
    let per_part = rows.div_ceil(threads as u64);
    let mut parts = Vec::with_capacity(threads);
    let mut start = 0usize;
    let mut at = 0usize;
    let mut in_part = 0u64;
    for row in crate::core::row::row::serialized_rows(bytes) {
        let Ok(row) = row else { break };
        at += row.len();
        in_part += 1;
        if in_part == per_part {
            parts.push(&bytes[start..at]);
            start = at;
            in_part = 0;
        }
    }
    // The rest, including any tail a damaged header cut short — the part's
    // own fold reports that.
    if start < bytes.len() {
        parts.push(&bytes[start..]);
    }
    parts
}

/// Replace every shredded JSON column in `rows` with its document, as text.
///
/// The scan returns the stored `UUID` that points at the shadow table; the
/// client asked for the document. The row-mode select path does this inline
/// after trimming, but the aggregate path folds the scanned rows *before* that
/// point, so it has to hydrate first — and it must, for two reasons. The group
/// key is the value being folded on, and grouping on the reference groups on a
/// per-row-unique value: `GROUP BY doc` returned one group per row instead of
/// one per distinct document. The emitted key is that same value, so it also
/// went out as a UUID under a column the description called `jsonb`.
///
/// Unlike the select path this does not stop at `visible_columns`: the fold
/// reads columns by write order, including ones no output position shows.
async fn hydrate_json_columns(
    database: &Arc<Database>,
    schema: &crate::core::row::schema::TableSchema,
    rows: &mut [crate::core::row::row::Row],
    tx: TxId,
) {
    if schema.json_columns.is_empty() {
        return;
    }
    for row in rows.iter_mut() {
        for cell in row.columns.iter_mut() {
            if cell.column_type == crate::core::db_type::DbType::NULL {
                continue;
            }
            let Some(field) = schema
                .fields
                .iter()
                .find(|f| f.write_order == u64::from(cell.schema_id))
            else {
                continue;
            };
            let Some(shadow_table) =
                crate::core::rql::json_map::shadow_for(&field.name, &schema.json_columns)
            else {
                continue;
            };
            let shadow_table = shadow_table.to_string();
            let reference = crate::core::value_codec::format_uuid(cell.data.into_slice());
            // No document behind the reference means the column was NULL, and
            // it has to stay NULL. Rendering the absence as `{}` — which the
            // select path does — turns `SELECT doc` on a NULL into an empty
            // object, and beside a `GROUP BY` it silently merges the NULL
            // group with the genuinely-empty-object one.
            let Some(document) = Box::pin(load_json_document(
                database,
                &shadow_table,
                &reference,
                0,
                tx,
            ))
            .await
            else {
                cell.data = crate::memory_pool::MEMORY_POOL.acquire(0);
                cell.column_type = crate::core::db_type::DbType::NULL;
                continue;
            };
            let rendered = document.to_string();
            let bytes = rendered.as_bytes();
            let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(bytes);
            cell.data = mb;
            // The rebuilt document is `JSON`, not `STRING`: the bytes are the
            // document text either way, but the type is what carries its
            // JSON-ness to anything downstream that has no schema to consult.
            // Marking it `STRING` lost that at exactly one place — materialising
            // a derived table samples the returned rows to pick the temp
            // relation's column types, so `(SELECT doc FROM t) AS sub` created
            // `sub.doc` as TEXT. The outer select then described the column from
            // the temp relation (text) while the recursive describe said jsonb,
            // and the document went out with no version byte: "unsupported JSONB
            // format version 123", 123 being `{`.
            cell.column_type = crate::core::db_type::DbType::JSON;
        }
    }
}

async fn load_json_document(
    database: &Arc<Database>,
    shadow_table: &str,
    reference: &str,
    depth: usize,
    tx: TxId,
) -> Option<serde_json::Value> {
    use crate::core::db_type::DbType;
    use crate::core::rql::json_map;

    if depth >= json_map::MAX_NESTING_DEPTH || !database.has_table(shadow_table) {
        return None;
    }

    let all_fields: Vec<crate::core::row::schema::SchemaField> = {
        // A shadow table: covered by its parent's lock (`lock_exempt`).
        let table = database
            .table_for(shadow_table, owner(), TableMode::AccessShare)
            .await
            .ok()
            .flatten()?;
        crate::core::rql::nullable::visible_fields(&table.schema.fields)
            .into_iter()
            .filter(|f| f.name != JSON_KEY_COLUMN)
            .collect()
    };
    if all_fields.is_empty() {
        return Some(serde_json::Value::Object(serde_json::Map::new()));
    }

    let names: Vec<String> = all_fields.iter().map(|f| f.name.clone()).collect();
    // No LIMIT: an array's elements are separate rows under one reference, and
    // taking the first would silently return a one-element array.
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = '{}'",
        names.join(", "),
        shadow_table,
        JSON_KEY_COLUMN,
        reference
    );
    let rows = run_subquery(&sql, database, tx).await.ok()?;
    if rows.is_empty() {
        return None;
    }

    let position = |name: &str| all_fields.iter().position(|f| f.name == name);
    let idx_at = position(JSON_IDX_COLUMN);
    let kind_at = position(JSON_KIND_COLUMN);
    let element_ref_at = position(JSON_ELEMENT_REF_COLUMN);
    let value_columns: Vec<usize> = all_fields
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name.starts_with(JSON_VALUE_PREFIX) && f.name != JSON_ELEMENT_REF_COLUMN)
        .map(|(i, _)| i)
        .collect();

    // A cell that is NULL carries the NULL type rather than the column's.
    let present = |row: &crate::core::row::row::Row, at: usize| -> Option<Vec<u8>> {
        let cell = row.columns.get(at)?;
        if cell.column_type == DbType::NULL {
            return None;
        }
        Some(cell.data.into_slice().to_vec())
    };

    let kind_is = |marker: &[u8]| match kind_at {
        Some(at) => rows
            .iter()
            .any(|r| present(r, at).is_some_and(|b| b == marker)),
        None => false,
    };
    let is_array = kind_is(b"a");
    let is_scalar = kind_is(b"s");

    // Fields the document itself owns — the engine's bookkeeping columns are
    // not part of anybody's JSON.
    let doc_fields: Vec<(usize, &crate::core::row::schema::SchemaField)> = all_fields
        .iter()
        .enumerate()
        .filter(|(_, f)| {
            f.name != JSON_IDX_COLUMN
                && f.name != JSON_KIND_COLUMN
                && !f.name.starts_with(JSON_VALUE_PREFIX)
        })
        .collect();

    let build_object = |row: &crate::core::row::row::Row| -> serde_json::Value {
        let mut object = serde_json::Map::new();
        for (index, field) in &doc_fields {
            let Some(bytes) = present(row, *index) else {
                continue;
            };
            let value = match field.db_type {
                DbType::UUID => {
                    // Resolved below; a nested reference needs an await, which
                    // a closure cannot do, so it is marked and filled in after.
                    serde_json::Value::String(format!(
                        "{}{}",
                        NESTED_MARKER,
                        crate::core::value_codec::format_uuid(&bytes)
                    ))
                }
                _ => match json_value_of(&field.db_type, &bytes) {
                    Some(v) => v,
                    None => continue,
                },
            };
            object.insert(field.name.clone(), value);
        }
        serde_json::Value::Object(object)
    };

    let mut result = if is_scalar {
        // One row, one value slot. Which slot depends on the scalar's type,
        // the same way an array element's does.
        let row = rows.first()?;
        value_columns
            .iter()
            .find_map(|at| {
                present(row, *at).and_then(|bytes| json_value_of(&all_fields[*at].db_type, &bytes))
            })
            .unwrap_or(serde_json::Value::Null)
    } else if is_array {
        let idx_at = idx_at?;
        let mut elements: Vec<(i64, serde_json::Value)> = Vec::new();
        for row in &rows {
            let Some(bytes) = present(row, idx_at) else {
                continue;
            };
            let index = i64::from_le_bytes(bytes.as_slice().try_into().ok()?);
            // The container row of an empty array carries no element.
            if index < 0 {
                continue;
            }
            // An element that is itself a container lives in its own table.
            let nested = element_ref_at
                .and_then(|at| present(row, at))
                .map(|bytes| crate::core::value_codec::format_uuid(&bytes));
            let value = if let Some(reference) = nested {
                let nested_table =
                    json_map::shadow_table_name(shadow_table, JSON_ELEMENT_REF_COLUMN);
                Box::pin(load_json_document(
                    database,
                    &nested_table,
                    &reference,
                    depth + 1,
                    tx,
                ))
                .await
                .unwrap_or(serde_json::Value::Null)
            } else {
                // Otherwise a scalar when one of the value slots holds
                // something, and an object when none does.
                let scalar = value_columns
                    .iter()
                    .find_map(|at| present(row, *at).map(|bytes| (*at, bytes)));
                match scalar {
                    Some((at, bytes)) => json_value_of(&all_fields[at].db_type, &bytes)
                        .unwrap_or(serde_json::Value::Null),
                    None => build_object(row),
                }
            };
            elements.push((index, value));
        }
        elements.sort_by_key(|(index, _)| *index);
        serde_json::Value::Array(elements.into_iter().map(|(_, v)| v).collect())
    } else {
        build_object(rows.first()?)
    };

    Box::pin(resolve_nested_references(
        database,
        shadow_table,
        &mut result,
        depth,
        tx,
    ))
    .await;
    Some(result)
}

/// Marks a string that is really a reference to a nested document, so the
/// object builder can stay synchronous and the resolution happen afterwards.
const NESTED_MARKER: &str = "\u{0}__json_ref:";

/// Replaces every marked reference in `value` with the document it points at.
async fn resolve_nested_references(
    database: &Arc<Database>,
    shadow_table: &str,
    value: &mut serde_json::Value,
    depth: usize,
    tx: TxId,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, entry) in map.iter_mut() {
                if let serde_json::Value::String(text) = entry {
                    if let Some(reference) = text.strip_prefix(NESTED_MARKER) {
                        let reference = reference.to_string();
                        let nested_table =
                            crate::core::rql::json_map::shadow_table_name(shadow_table, key);
                        *entry = match Box::pin(load_json_document(
                            database,
                            &nested_table,
                            &reference,
                            depth + 1,
                            tx,
                        ))
                        .await
                        {
                            Some(nested) => nested,
                            None => serde_json::Value::String(reference),
                        };
                        continue;
                    }
                }
                Box::pin(resolve_nested_references(
                    database,
                    shadow_table,
                    entry,
                    depth,
                    tx,
                ))
                .await;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items.iter_mut() {
                Box::pin(resolve_nested_references(
                    database,
                    shadow_table,
                    item,
                    depth,
                    tx,
                ))
                .await;
            }
        }
        _ => {}
    }
}

/// Renders a stored column value back to the JSON value it came from.
fn json_value_of(
    db_type: &crate::core::db_type::DbType,
    bytes: &[u8],
) -> Option<serde_json::Value> {
    use crate::core::db_type::DbType;
    use serde_json::Value;

    fn le<const N: usize>(b: &[u8]) -> Option<[u8; N]> {
        if b.len() != N {
            return None;
        }
        let mut a = [0u8; N];
        a.copy_from_slice(b);
        Some(a)
    }

    Some(match db_type {
        DbType::BOOL => Value::Bool(bytes.first().copied().unwrap_or(0) != 0),
        DbType::U64 => Value::from(u64::from_le_bytes(le::<8>(bytes)?)),
        DbType::I64 => Value::from(i64::from_le_bytes(le::<8>(bytes)?)),
        DbType::F64 => serde_json::Number::from_f64(f64::from_le_bytes(le::<8>(bytes)?))
            .map(Value::Number)
            .unwrap_or(Value::Null),
        DbType::DATETIME => Value::String(crate::core::value_codec::format_timestamp_us(
            i64::from_le_bytes(le::<8>(bytes)?),
        )),
        DbType::INET => Value::String(crate::core::value_codec::format_inet(bytes)?),
        DbType::ARRAY => {
            // The stored form is Postgres' binary array; its elements come back
            // as the JSON array they were written from.
            let rendered = crate::core::value_codec::format_array(bytes)?;
            let inner = rendered.trim_start_matches('{').trim_end_matches('}');
            if inner.is_empty() {
                Value::Array(Vec::new())
            } else {
                Value::Array(
                    split_array_elements(inner)
                        .into_iter()
                        .map(|e| match e {
                            None => Value::Null,
                            Some(text) => Value::String(text),
                        })
                        .collect(),
                )
            }
        }
        _ => Value::String(String::from_utf8_lossy(bytes).into_owned()),
    })
}

/// Splits the body of a rendered array literal back into its elements.
fn split_array_elements(inner: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut was_quoted = false;
    let mut chars = inner.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\\' if quoted => {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            '"' => {
                quoted = !quoted;
                was_quoted = true;
            }
            ',' if !quoted => {
                out.push(finish_element(&current, was_quoted));
                current.clear();
                was_quoted = false;
            }
            _ => current.push(c),
        }
    }
    out.push(finish_element(&current, was_quoted));
    out
}

fn finish_element(raw: &str, was_quoted: bool) -> Option<String> {
    if !was_quoted && raw.trim().eq_ignore_ascii_case("NULL") {
        return None;
    }
    Some(if was_quoted {
        raw.to_string()
    } else {
        raw.trim().to_string()
    })
}

/// A status-3 error frame carrying `message`.
fn ddl_error(message: &str) -> Vec<u8> {
    let mut result = vec![3u8];
    result.extend_from_slice(message.as_bytes());
    result
}

/// A status-0 frame reporting `rows` affected.
fn ddl_ok(rows: u64) -> Vec<u8> {
    let mut result = vec![0u8];
    result.extend_from_slice(&rows.to_le_bytes());
    result
}

/// Gives back one mode on drop: a bare `VACUUM`'s per-table ROW EXCLUSIVE.
struct ModeRelease {
    database: Arc<Database>,
    owner: Option<crate::core::mvcc::table_locks::LockOwner>,
    table: String,
    mode: TableMode,
}

impl Drop for ModeRelease {
    fn drop(&mut self) {
        if let Some(owner) = self.owner {
            self.database.row_locks.table_locks().release_mode(owner, &self.table, self.mode);
        }
    }
}

/// One table's own lock owner for a bare autocommit `VACUUM` (finding L6):
/// begun for the table and ended, its lock released, when the table is done,
/// as PostgreSQL vacuums each table in a transaction of its own.
struct VacuumOwner {
    database: Arc<Database>,
    owner: crate::core::mvcc::owner::StatementOwner,
}

impl Drop for VacuumOwner {
    fn drop(&mut self) {
        self.database.row_locks.release_owner(self.owner.owner);
        self.database.transactions.end_owner(self.owner.owner.id());
    }
}

/// What gives a bare `VACUUM`'s lock on one table back when it moves on.
#[allow(dead_code)] // held for its `Drop`
enum VacuumRelease {
    /// A table's own owner (autocommit).
    Owner(VacuumOwner),
    /// The block's one mode on the table (a block has one owner).
    Mode(ModeRelease),
}

/// The mode `VACUUM` takes on each table it vacuums (finding L6). Plain
/// `VACUUM` rebuilds the free list beside running DML, so it takes ROW
/// EXCLUSIVE: it conflicts with a DDL's ACCESS EXCLUSIVE (no reload swaps or
/// retires the instance under it) and with SHARE, not with INSERT, UPDATE,
/// DELETE or SELECT. PostgreSQL's SHARE UPDATE EXCLUSIVE has no exact match
/// in the four modes, and the never-weaker mapping (ACCESS EXCLUSIVE) would
/// stop all DML for a plain VACUUM, which PostgreSQL never does; ROW
/// EXCLUSIVE keeps what matters, the conflict with DDL. `VACUUM FULL`
/// rewrites the table and takes ACCESS EXCLUSIVE, as in PostgreSQL.
pub(crate) fn vacuum_mode(full: bool) -> TableMode {
    if full {
        TableMode::AccessExclusive
    } else {
        TableMode::RowExclusive
    }
}

/// `VACUUM`'s lock on one table, in [`vacuum_mode`], through
/// `relation_for`. A named `VACUUM t` holds it under the statement's owner
/// to the statement's end. A bare `VACUUM` (`per_table`) gives each table's
/// back when it moves on, so it never holds every table at once: in
/// autocommit each table is taken under an owner of its own
/// ([`VacuumOwner`]); in a block, under the block's owner, released unless
/// the block held that mode before.
async fn vacuum_lock(
    database: &Arc<Database>,
    name: &str,
    full: bool,
    per_table: bool,
) -> Result<(Option<crate::core::database::Relation>, Option<VacuumRelease>), String> {
    use crate::core::mvcc::owner::{OwnerKind, StatementOwner};
    let mode = vacuum_mode(full);
    let statement_owner = owner();
    if per_table && statement_owner.is_some_and(|o| o.kind != OwnerKind::Block) {
        let Some(own) = crate::core::mvcc::table_locks::LockOwner::autocommit(database.transactions.begin_owner())
        else {
            return Err("internal error: no lock owner for VACUUM".to_string());
        };
        let guard = VacuumOwner {
            database: database.clone(),
            owner: StatementOwner {
                owner: own,
                kind: OwnerKind::Statement,
            },
        };
        let relation = database.relation_for(name, Some(guard.owner), mode).await?;
        return Ok((relation, Some(VacuumRelease::Owner(guard))));
    }
    let held_before = statement_owner.is_some_and(|o| {
        database
            .row_locks
            .table_locks()
            .holds(o.owner, &database.lock_key(name))
            .contains(mode)
    });
    let relation = database.relation_for(name, statement_owner, mode).await?;
    let release = (per_table && !held_before).then(|| {
        VacuumRelease::Mode(ModeRelease {
            database: database.clone(),
            owner: statement_owner.map(|o| o.owner),
            table: database.lock_key(name).into_owned(),
            mode,
        })
    });
    Ok((relation, release))
}

/// The sequences a `DROP TABLE` takes with it, read from the table's schema
/// once the drop holds ACCESS EXCLUSIVE on it (so no ALTER can change what
/// it owns in between). Empty when the statement does not parse or names no
/// table: the drop reports that.
async fn sequences_owned_by_dropped(sql: &str, database: &Arc<Database>) -> Result<Vec<String>, String> {
    let Ok((name, _)) = crate::core::rql::lexer_alter::parse_drop_table(sql) else {
        return Ok(Vec::new());
    };
    let Some(table) = database.table_for(&name, owner(), TableMode::AccessExclusive).await? else {
        return Ok(Vec::new());
    };
    let (fields, defaults) = (table.schema.fields.clone(), table.schema.defaults.clone());
    drop(table);
    Ok(crate::core::rql::sequence::owned_by(
        &name,
        &fields,
        &defaults,
        &sequences_named_elsewhere(database, &name),
    ))
}

async fn execute_drop_table(sql: &str, database: &Arc<Database>) -> Vec<u8> {
    use crate::core::rql::lexer_alter;

    let (table_name, if_exists) = match lexer_alter::parse_drop_table(sql) {
        Ok(v) => v,
        Err(e) => {
            error!("Cannot parse DROP TABLE: {}", e);
            return ddl_error(&e);
        }
    };

    // ACCESS EXCLUSIVE on the name, a partitioned table's logical one
    // included, before anything is dropped (B4 marks the slot dropped under
    // it, so queued waiters get 42P01).
    if let Err(e) = database
        .relation_for(&table_name, owner(), TableMode::AccessExclusive)
        .await
    {
        return ddl_error(&e);
    }
    // This transaction's own row locks on it would outlive it, keyed by a
    // name a table created next could take.
    if owner().is_some_and(|o| database.row_locks.txn_holds_rows_on(&table_name, o.owner.id())) {
        return row_locks_in_use(&table_name, "DROP TABLE");
    }
    if let Err(e) = Box::pin(drop_dependent_foreign_keys(database, &table_name, sql)).await {
        return ddl_error(&e);
    }
    // Between the two steps of `DROP … CASCADE`: the keys are gone, the
    // table not yet (a crash here leaves a statement that runs again).
    if let Err(e) = crate::core::database::rewrite::point(database, "fkdrop_after_keys") {
        return ddl_error(&e);
    }
    match database.drop_table(&table_name).await {
        Ok(true) => ddl_ok(0),
        Ok(false) if if_exists => ddl_ok(0),
        Ok(false) => ddl_error(&format!("relation '{}' does not exist", table_name)),
        Err(e) => {
            error!("Failed to drop table {}: {}", table_name, e);
            ddl_error(&e.to_string())
        }
    }
}

/// Whether a `DROP TABLE` ends in `CASCADE`, PostgreSQL's request to drop
/// what depends on the table too (`RESTRICT`, the default, refuses instead).
fn drop_table_cascades(sql: &str) -> bool {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .next_back()
        .is_some_and(|w| w.eq_ignore_ascii_case("CASCADE"))
}

/// The foreign keys of other tables that reference `table_name`, which a
/// `DROP TABLE` of it must not leave behind (phase 3 P4, for plain and
/// partitioned tables alike), as PostgreSQL treats them:
///
/// - without `CASCADE`: refused, 2BP01 ("cannot drop table p because other
///   objects depend on it"), before anything is dropped;
/// - with `CASCADE`: each dependent constraint is dropped (its record
///   tombstoned, its table reloaded under ACCESS EXCLUSIVE), never the
///   tables holding them, and only then the table.
///
/// The table's own self-references go with it. The drop used to leave the
/// keys in place, so every later insert of a non-NULL value into a child
/// failed naming a parent that did not exist, and a table created under the
/// name became their parent. Called under the DROP's ACCESS EXCLUSIVE on
/// `table_name`, so no key naming it can be added meanwhile (an ADD or a
/// CREATE takes SHARE on its parent).
async fn drop_dependent_foreign_keys(database: &Arc<Database>, table_name: &str, sql: &str) -> Result<(), String> {
    // A name no table has (`IF EXISTS`) has nothing depending on it, whatever
    // keys a plain table declared on it before it existed.
    if !database.has_table(table_name) && !database.is_partitioned(table_name) {
        return Ok(());
    }
    let mut dependents: Vec<(String, String)> = database
        .referencing_keys(table_name)
        .into_iter()
        .filter(|(child, fk)| !fk.is_deleted && !child.eq_ignore_ascii_case(table_name))
        .map(|(child, fk)| (child, fk.name))
        .collect();
    if dependents.is_empty() {
        return Ok(());
    }
    if !drop_table_cascades(sql) {
        let (child, name) = &dependents[0];
        return Err(format!(
            "cannot drop table {table_name} because other objects depend on it: constraint {name} on table {child} \
             depends on table {table_name} (HINT: use DROP ... CASCADE to drop the dependent objects too)"
        ));
    }
    dependents.sort();
    let mut children: Vec<String> = dependents.into_iter().map(|(child, _)| child).collect();
    children.dedup();
    // Each child is changed, so held as a DDL holds its table (taken with the
    // statement's set when the text said CASCADE: `ddl_lock_extras`).
    for child in &children {
        database.table_for(child, owner(), TableMode::AccessExclusive).await?;
    }
    for child in children {
        let parent = table_name.to_string();
        database
            .reload_with(&child, |old| async move {
                crate::core::rql::foreign_key::remove_referencing(old.io_schema.clone(), &parent)
                    .await
                    .map(drop)
                    .map_err(|e| e.to_string())
            })
            .await
            .map_err(|failure| format!("could not drop the foreign keys of \"{child}\" naming \"{table_name}\": {failure}"))?;
        info!("DROP TABLE {table_name} CASCADE dropped the foreign keys of {child} naming it");
    }
    Ok(())
}

/// Describes the first pair of rows that would break `key`, or `None` when the
/// data already satisfies it.
///
/// A constraint added to a table that already breaks it was never true of that
/// table, so `ALTER TABLE … ADD CONSTRAINT … UNIQUE` has to look before it
/// agrees — PostgreSQL reports "key is duplicated" for the same reason. One
/// scan, at DDL time.
/// Refuses a row that a `CHECK` constraint rejects.
///
/// Evaluated against the row **as it will be stored**, built from the payload
/// about to be written — so it sees the defaults that were filled in and the
/// generated columns that were computed, exactly as a later reader would.
/// Checking the statement's literals instead would miss both.
///
/// `Ok(())` when every constraint holds, or when the table declares none,
/// which is the common case and costs one `is_empty`.
#[inline(never)]
fn check_constraints_hold(
    table_name: &str,
    schema: &crate::core::row::schema::TableSchema,
    payloads: &[crate::core::row::row::ColumnWritePayload],
) -> Result<(), String> {
    if schema.checks.is_empty() {
        return Ok(());
    }

    let mut row = crate::core::row::row::Row::default();
    for payload in payloads {
        row.columns.push(crate::core::row::row::Column {
            schema_id: crate::core::row::row::schema_id_of(payload.write_order as u64),
            data: payload.data.clone(),
            column_type: payload.column_type.clone(),
        });
    }
    let row = &row;

    for constraint in &schema.checks {
        if constraint.is_deleted {
            continue;
        }

        // A check that cannot be compiled is refused when it is declared
        // (`compile_check`); one stored before that rule is logged and
        // skipped rather than failing every insert into the table.
        let compiled = match compile_check_cached(&constraint.expression, &schema.fields) {
            Ok(compiled) => compiled,
            Err(reason) => {
                report_unenforced_once(table_name, &constraint.name, &reason, false);
                continue;
            }
        };
        if let CompiledCheck::Unenforced(why) = compiled.as_ref() {
            report_unenforced_once(table_name, &constraint.name, why, true);
            continue;
        }

        match compiled.violated(row, &schema.fields, table_name, &constraint.name) {
            Ok(false) => {}
            Ok(true) => {
                return Err(format!(
                    "new row for relation \"{}\" violates check constraint \"{}\"",
                    table_name, constraint.name
                ));
            }
            // A check that cannot be evaluated for this row fails the
            // statement: it was logged and the row stored unchecked.
            Err(reason) => return Err(check_evaluation_failed(&constraint.name, table_name, &reason)),
        }
    }
    Ok(())
}

/// The failure of a check that could not be evaluated for a row, with its
/// reason (which carries the SQLSTATE: `division by zero` is 22012).
fn check_evaluation_failed(name: &str, table: &str, reason: &str) -> String {
    format!("check constraint \"{name}\" of relation \"{table}\" could not be evaluated for the row: {reason}")
}

/// A `CHECK`, compiled to tell whether a row violates it.
///
/// PostgreSQL refuses a row only when a check is FALSE; UNKNOWN (a NULL
/// result) passes, so `x > 0` and `x IS NULL OR x > 0` both take a NULL `x`.
/// It used to be compiled against the table *without* its nullability
/// companions, which made the rewriter read every column as NOT NULL:
/// `x IS NULL` became a constant FALSE and `x IS NOT NULL` a constant TRUE.
/// A guess about NULL inputs papered over half of that and refused the
/// other half, so `CHECK (x IS NULL OR x > 0)` refused a NULL `x`.
pub(crate) enum CompiledCheck {
    /// `NOT (check)` lowered as a `WHERE` clause over the whole table, the
    /// question `ALTER TABLE … ADD CHECK` asks of the stored rows: a row
    /// violates the check exactly when it matches. `NOT` is pushed down
    /// three-valued and every NULL-reading comparison is guarded, so two
    /// values are enough to evaluate it.
    Violation(crate::core::rql::expr::Cond),
    /// The check as written, for what the rewriter cannot read but the
    /// evaluator can (`COALESCE(x, 1) > 0`), evaluated with three values: a
    /// row violates it when it is FALSE.
    Direct(crate::core::rql::expr::Cond),
    /// `NOT (check)` lowered as for `Violation`, but in a shape only the
    /// scan's predicate engine reads — `LIKE`, `SIMILAR TO`, `doc ? 'k'`,
    /// `doc @> …`, a lifted `CASE` — run by that engine against the row, as
    /// a `WHERE` runs it against a stored one. A check is then enforced on
    /// a write exactly as `ADD CONSTRAINT` judged the stored rows by it.
    Scan {
        plan: crate::core::processor::transformer::TransformerPlan,
        width: usize,
    },
    /// A check whose regular expression is outside what this engine can
    /// match as PostgreSQL does (`pg_regex`): kept, and not enforced, rather
    /// than enforced another way or refused (which would lose the table on
    /// a restore). Said so where it is declared.
    Unenforced(String),
    /// A check over regular expressions each of which stands as a boolean
    /// under AND, OR and NOT alone, so one left UNKNOWN for a value still
    /// gives the check's exact three-valued answer (`violated`).
    Monotone(Box<CompiledCheck>),
}

impl CompiledCheck {
    /// Whether `row` violates the check. `fields` is the table's whole
    /// schema, so a column whose companion is set reads as NULL.
    ///
    /// A regular expression that could not be answered for this value as
    /// PostgreSQL would (a locale class over non-ASCII text) is UNKNOWN. A
    /// check whose every such call sits under AND, OR and NOT ([`Monotone`])
    /// is judged with it UNKNOWN, which is exact: FALSE whatever the call
    /// answers is FALSE (`s ~ '^\w+$' AND n > 0` with n = -1 is refused,
    /// as in PostgreSQL; it was accepted). Any other check is not judged for
    /// the row, which is counted against `table`'s `name` and said once per
    /// statement (`notice::count`).
    ///
    /// [`Monotone`]: CompiledCheck::Monotone
    pub(crate) fn violated(
        &self,
        row: &crate::core::row::row::Row,
        fields: &[crate::core::row::schema::SchemaField],
        table: &str,
        name: &str,
    ) -> Result<bool, String> {
        let _ = crate::core::rql::expr::take_check_uncertainty();
        let verdict = self.violated_as_written(row, fields)?;
        if let Some(why) = crate::core::rql::expr::take_check_uncertainty() {
            if verdict && matches!(self, CompiledCheck::Monotone(_)) {
                return Ok(true);
            }
            crate::core::notice::count(&format!("unjudged\u{1}{table}\u{1}{name}"), || {
                format!(
                    "check constraint \"{name}\" of relation \"{table}\" was not judged for some rows: {why}, \
                     where PostgreSQL's answer depends on the locale; those rows were accepted"
                )
            });
            return Ok(false);
        }
        Ok(verdict)
    }

    fn violated_as_written(
        &self,
        row: &crate::core::row::row::Row,
        fields: &[crate::core::row::schema::SchemaField],
    ) -> Result<bool, String> {
        match self {
            CompiledCheck::Unenforced(_) => Ok(false),
            CompiledCheck::Monotone(inner) => inner.violated_as_written(row, fields),
            CompiledCheck::Violation(cond) => crate::core::rql::expr::eval_cond(cond, row, fields),
            CompiledCheck::Direct(cond) => {
                Ok(crate::core::rql::expr::eval_cond_3vl(cond, row, fields)? == Some(false))
            }
            CompiledCheck::Scan { plan, width } => {
                use crate::core::processor::concurrent_processor::RowView;
                use crate::core::processor::transformer::TransformerProcessor;
                // The row's cells by write order, as a scan addresses them.
                let mut map: smallvec::SmallVec<[u8; 32]> = smallvec::SmallVec::new();
                map.resize((*width).max(1), u8::MAX);
                for (pos, column) in row.columns.iter().enumerate() {
                    let wo = column.schema_id as usize;
                    if pos >= u8::MAX as usize {
                        return Err("a check over more than 254 columns cannot be evaluated".to_string());
                    }
                    if wo < map.len() {
                        map[wo] = pos as u8;
                    }
                }
                let mut transformers = smallvec::SmallVec::new();
                let mut intermediates = smallvec::SmallVec::new();
                let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediates);
                processor.load_plan(plan);
                let mut results = smallvec::SmallVec::new();
                processor
                    .execute_row(&RowView::new(row, &map), &mut results)
                    .map_err(|e| e.as_str().to_string())
            }
        }
    }

    /// The table's columns the check reads, a nullability companion counted
    /// as its column: `x IS NOT NULL` lowers to a test of `__null_x`, and is
    /// a check an assignment to `x` can break.
    fn columns(
        &self,
        fields: &[crate::core::row::schema::SchemaField],
    ) -> Vec<crate::core::row::schema::SchemaField> {
        let cond = match self {
            CompiledCheck::Unenforced(_) => return Vec::new(),
            CompiledCheck::Monotone(inner) => return inner.columns(fields),
            CompiledCheck::Violation(cond) | CompiledCheck::Direct(cond) => cond,
            // What the plan reads is not listed here, so every column is
            // taken as read: an UPDATE judges the check against each row.
            CompiledCheck::Scan { .. } => {
                return fields
                    .iter()
                    .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
                    .cloned()
                    .collect();
            }
        };
        let mut read = Vec::new();
        collect_condition_columns(cond, &mut read);
        let mut out: Vec<crate::core::row::schema::SchemaField> = Vec::new();
        for field in read {
            let field = match field.name.get(crate::core::rql::nullable::NULL_FLAG_PREFIX.len()..) {
                Some(base) if crate::core::rql::nullable::is_hidden(&field.name) => fields
                    .iter()
                    .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(base))
                    .cloned()
                    .unwrap_or(field),
                _ => field,
            };
            if !out.iter().any(|f| f.write_order == field.write_order) {
                out.push(field);
            }
        }
        out
    }
}

/// [`compile_check`], remembered per thread by the expression and the
/// schema it was compiled against: an INSERT judges every row, and compiling
/// each check again per row made a bulk load with checks some 40% slower.
fn compile_check_cached(
    expression: &str,
    fields: &[crate::core::row::schema::SchemaField],
) -> Result<std::sync::Arc<CompiledCheck>, String> {
    use std::hash::{Hash, Hasher};
    thread_local! {
        static CACHE: std::cell::RefCell<std::collections::HashMap<u64, (String, std::sync::Arc<CompiledCheck>)>> =
            std::cell::RefCell::new(std::collections::HashMap::new());
    }
    // Keyed by everything the compilation reads: the text and each field's
    // name, type, position, scale, declared text length and liveness. The
    // length decides whether a check on a `character(n)` column is enforced
    // (`compile_check`) and how its literals read: left out, a check compiled
    // while the column was an older binary's (unenforced) kept being
    // answered from the cache after `ALTER COLUMN … TYPE character(n)`.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    expression.hash(&mut hasher);
    for f in fields {
        (&f.name, f.db_type.to_string(), f.write_order, f.scale, f.text_typmod(), f.is_deleted).hash(&mut hasher);
    }
    let key = hasher.finish();
    if let Some(hit) = CACHE.with(|c| {
        c.borrow().get(&key).filter(|(text, _)| text == expression).map(|(_, compiled)| compiled.clone())
    }) {
        return Ok(hit);
    }
    let compiled = std::sync::Arc::new(compile_check(expression, fields)?);
    CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        // Bounded: a long-lived worker sees many tables.
        if cache.len() >= 512 {
            cache.clear();
        }
        cache.insert(key, (expression.to_string(), compiled.clone()));
    });
    Ok(compiled)
}

/// Compiles a `CHECK`'s expression against a table's whole schema, its
/// nullability companions included (see [`CompiledCheck`]).
///
/// An expression neither the rewriter nor the evaluator reads is an error,
/// which a `CREATE TABLE` or `ADD CONSTRAINT` declaring it reports. It used
/// to be stored, logged at every write and never enforced: `CHECK
/// (COALESCE(x, 1) > 0)` took `x = -3`.
pub(crate) fn compile_check(
    expression: &str,
    fields: &[crate::core::row::schema::SchemaField],
) -> Result<CompiledCheck, String> {
    let fields_sv: smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> =
        fields.iter().cloned().collect();
    // `pg_dump`'s canonical spellings, in the shape the rewriter reads.
    let original = expression;
    let expression = crate::core::rql::check::normalize_expression(expression);
    let expression = expression.as_str();
    // A written regular expression this engine cannot match as PostgreSQL
    // does: the check is kept and not enforced.
    if let Some(why) = unsupported_check_pattern(expression) {
        return Ok(CompiledCheck::Unenforced(why));
    }
    // `character(n)` compares blank-padded in PostgreSQL ('AB ' = 'AB'). A
    // column with a recorded length is stored and compared that way here
    // (`type_modifier::TextTypmod`), so `tag = ANY (ARRAY['AB'::bpchar,
    // …])`, which `pg_dump` writes for every such check, is enforced. One an
    // older binary created holds its values as written (and that binary cast
    // it `::bpchar` in every check reading it): a check casting to `bpchar`
    // that reads a text column without a recorded `character(n)` length is
    // kept unenforced, as it was.
    let reads_unpadded_text = || {
        let names: Vec<String> = fields.iter().filter(|f| !f.is_deleted).map(|f| f.name.clone()).collect();
        crate::core::rql::check::columns_read(original, &names).iter().any(|name| {
            fields.iter().any(|f| {
                !f.is_deleted
                    && f.name == *name
                    && f.db_type == crate::core::db_type::DbType::STRING
                    && !matches!(f.text_typmod(), Some(crate::core::rql::type_modifier::TextTypmod::Bpchar(_)))
            })
        })
    };
    if compares_bpchar(original) && reads_unpadded_text() {
        return Ok(CompiledCheck::Unenforced(
            "it compares character(n) values, which this engine does not compare blank-padded as PostgreSQL does"
                .to_string(),
        ));
    }
    let compiled = compile_check_lowered(expression, &fields_sv, fields);
    match compiled {
        // What none of the readers takes, around a regular expression, is
        // kept unenforced too: refusing the CREATE loses the table and its
        // rows on a `psql -f` restore.
        Err(e) if expression.contains(crate::core::rql::check::CHECK_REGEXP_LIKE) => {
            Ok(CompiledCheck::Unenforced(format!("this form of check around a regular expression ({e})")))
        }
        Ok(compiled) if regex_calls_are_monotone(expression) => Ok(CompiledCheck::Monotone(Box::new(compiled))),
        other => other,
    }
}

/// Whether `text` (as written) casts to `bpchar` or `character` — not
/// `character varying`.
fn compares_bpchar(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            i = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
            continue;
        }
        if lower[i..].starts_with("::") {
            let rest = lower[i + 2..].trim_start().trim_start_matches('"');
            if rest.starts_with("bpchar") {
                return true;
            }
            if let Some(after) = rest.strip_prefix("character") {
                if !after.trim_start().starts_with("varying") && !after.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
                    return true;
                }
            }
            i += 2;
            continue;
        }
        i += 1;
    }
    false
}

/// Whether every regular expression in the normalized `expression` stands
/// as a boolean term under AND, OR and NOT alone, at every level of
/// parentheses around it — `(call = TRUE)`, `(call = FALSE)`, `NOT call`,
/// `call` — so an UNKNOWN call leaves the check UNKNOWN exactly when
/// PostgreSQL's answer could go either way. One read by anything that sees
/// a NULL as a value (`IS`, `COALESCE`, `CASE`, a comparison, a function)
/// is not. `false` when there is no call.
fn regex_calls_are_monotone(expression: &str) -> bool {
    use crate::core::sql_scan::matching_close;
    let name = crate::core::rql::check::CHECK_REGEXP_LIKE;
    if !expression.contains(name) {
        return false;
    }
    let word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let ends_with_word = |text: &str, w: &str| {
        let t = text.to_ascii_uppercase();
        t.ends_with(w) && t.len().checked_sub(w.len() + 1).is_none_or(|i| !word(t.as_bytes()[i]))
    };
    let starts_with_word = |text: &str, w: &str| {
        let t = text.to_ascii_uppercase();
        t.starts_with(w) && t.as_bytes().get(w.len()).is_none_or(|b| !word(*b))
    };
    // The innermost `(` open at `pos`, outside quotes.
    let enclosing_open = |pos: usize| -> Option<usize> {
        let bytes = expression.as_bytes();
        let mut stack = Vec::new();
        let mut i = 0;
        while i < pos {
            match bytes[i] {
                b'\'' => {
                    i = crate::core::sql_scan::quoted_end(bytes, i, b'\'');
                    continue;
                }
                b'(' => stack.push(i),
                b')' => {
                    stack.pop();
                }
                _ => {}
            }
            i += 1;
        }
        stack.pop()
    };
    let mut from = 0;
    while let Some(at) = expression[from..].find(name).map(|a| a + from) {
        let open = at + name.len();
        let Some(close) = matching_close(expression, open) else { return false };
        let mut span = (at, close + 1);
        loop {
            let before = expression[..span.0].trim_end();
            let after = expression[span.1..].trim_start();
            let prev_ok = before.is_empty()
                || before.ends_with('(')
                || ["AND", "OR", "NOT"].iter().any(|w| ends_with_word(before, w));
            let truth_test = ["= TRUE", "= FALSE"].iter().any(|t| {
                after.to_ascii_uppercase().starts_with(t) && {
                    let rest = after[t.len()..].trim_start();
                    rest.is_empty() || rest.starts_with(')') || starts_with_word(rest, "AND") || starts_with_word(rest, "OR")
                }
            });
            let next_ok = after.is_empty()
                || after.starts_with(')')
                || starts_with_word(after, "AND")
                || starts_with_word(after, "OR")
                || truth_test;
            if !(prev_ok && next_ok) {
                return false;
            }
            match enclosing_open(span.0) {
                None => break,
                Some(o) => match matching_close(expression, o) {
                    Some(c) => span = (o, c + 1),
                    None => return false,
                },
            }
        }
        from = close + 1;
    }
    true
}

/// Whether this binary enforces the check `expression`: `Ok(None)` when it
/// does, `Ok(Some(why))` when it keeps it unenforced, `Err(why)` when it
/// cannot compile it (one stored by an older binary). `SHOW CHECKS` reads it.
pub(crate) fn check_status(
    expression: &str,
    fields: &[crate::core::row::schema::SchemaField],
) -> Result<Option<String>, String> {
    match compile_check(expression, fields)? {
        CompiledCheck::Unenforced(why) => Ok(Some(why)),
        _ => Ok(None),
    }
}

/// What `SHOW CHECKS` says of the check `expression`: its status and why.
/// `enforced (ASCII-exact)` is a check with a regular expression whose
/// answer depends on the locale: answered for ASCII text, and for other text
/// judged only where that answer cannot change the result (`Monotone`), or
/// not judged for the row at all.
pub(crate) fn check_listing(
    expression: &str,
    fields: &[crate::core::row::schema::SchemaField],
) -> (&'static str, String) {
    match compile_check(expression, fields) {
        Err(why) => ("cannot be compiled", why),
        Ok(CompiledCheck::Unenforced(why)) => ("not enforced", why),
        Ok(compiled) => {
            let normalized = crate::core::rql::check::normalize_expression(expression);
            if !regex_locale_dependent(&normalized) {
                return ("enforced", String::new());
            }
            let how = if matches!(compiled, CompiledCheck::Monotone(_)) {
                "a row with other text is judged only where the regular expression's answer cannot change the result"
            } else {
                "a row with other text is not judged"
            };
            (
                "enforced (ASCII-exact)",
                format!("a locale-dependent regular expression is answered for ASCII text; {how}"),
            )
        }
    }
}

/// Whether any regular expression in the normalized check depends on the
/// locale for non-ASCII text: a constant one that does, or one read from the
/// row, which may.
fn regex_locale_dependent(normalized: &str) -> bool {
    let name = crate::core::rql::check::CHECK_REGEXP_LIKE;
    let mut from = 0;
    while let Some(at) = normalized[from..].find(name).map(|a| a + from) {
        from = at + name.len();
        let open = from;
        let Some(close) = crate::core::sql_scan::matching_close(normalized, open) else { continue };
        let args = crate::core::sql_scan::split_top_level(&normalized[open + 1..close], b',');
        let constant = |text: Option<&&str>| -> Option<String> {
            let e = crate::core::rql::expr::parse(text?.trim(), &[]).ok()?;
            match crate::core::rql::expr::eval(&e, &crate::core::row::row::Row::default(), &[]).ok()? {
                crate::core::rql::expr::Value::Data { bytes, .. } => String::from_utf8(bytes).ok(),
                crate::core::rql::expr::Value::Null => None,
            }
        };
        let Some(pattern) = constant(args.get(1)) else { return true };
        let flags = if args.len() > 2 {
            match constant(args.get(2)) {
                Some(f) => f,
                None => return true,
            }
        } else {
            String::new()
        };
        if crate::core::rql::pg_regex::compile_cached(&pattern, &flags).is_ok_and(|re| re.locale_dependent()) {
            return true;
        }
    }
    false
}

/// Logs, once per process for each table's check, one this binary does not
/// enforce or cannot compile: at the table's first write after a start, so
/// one declared by another session or stored by an older binary is said in
/// the log too (`SHOW CHECKS` lists them).
pub(crate) fn report_unenforced_once(table: &str, name: &str, why: &str, compiled: bool) {
    static SEEN: std::sync::Mutex<Vec<(String, String)>> = std::sync::Mutex::new(Vec::new());
    let mut seen = SEEN.lock().unwrap_or_else(|e| e.into_inner());
    if seen.iter().any(|(t, n)| t == table && n == name) {
        return;
    }
    if seen.len() >= 4096 {
        seen.clear();
    }
    seen.push((table.to_string(), name.to_string()));
    if compiled {
        log::warn!("check constraint \"{name}\" of relation \"{table}\" is not enforced: {why}");
    } else {
        log::error!("check constraint \"{name}\" of relation \"{table}\" could not be compiled and is not enforced: {why}");
    }
}

/// Says, where a check is declared, that it is kept and not enforced: in
/// the server log and as a WARNING notice to the client.
fn warn_unenforced_check(name: &str, table: &str, why: &str) {
    let message = format!(
        "check constraint \"{name}\" of relation \"{table}\" is not enforced: {why}; rows that \
         violate it are accepted"
    );
    log::warn!("{message}");
    crate::core::notice::push("WARNING", "01000", message);
}

/// The reason a check's written pattern is outside the verified subset of
/// PostgreSQL regular expressions (`pg_regex`), for the first such pattern;
/// `None` when every pattern written as a constant is inside it (or invalid
/// as PostgreSQL finds it invalid, which is then answered as PostgreSQL
/// answers it: 2201B for each value).
fn unsupported_check_pattern(expression: &str) -> Option<String> {
    let name = crate::core::rql::check::CHECK_REGEXP_LIKE;
    let mut from = 0;
    while let Some(at) = expression[from..].find(name).map(|a| a + from) {
        from = at + name.len();
        let open = from;
        if expression.as_bytes().get(open) != Some(&b'(') {
            continue;
        }
        let Some(close) = crate::core::sql_scan::matching_close(expression, open) else { continue };
        let args = crate::core::sql_scan::split_top_level(&expression[open + 1..close], b',');
        let constant = |text: Option<&&str>| -> Option<String> {
            let e = crate::core::rql::expr::parse(text?.trim(), &[]).ok()?;
            match crate::core::rql::expr::eval(&e, &crate::core::row::row::Row::default(), &[]).ok()? {
                crate::core::rql::expr::Value::Data { bytes, .. } => String::from_utf8(bytes).ok(),
                crate::core::rql::expr::Value::Null => None,
            }
        };
        let Some(pattern) = constant(args.get(1)) else { continue };
        let flags = if args.len() > 2 { constant(args.get(2)).unwrap_or_default() } else { String::new() };
        if let Err(crate::core::rql::pg_regex::PgRegexError::Unsupported(why)) =
            crate::core::rql::pg_regex::compile_cached(&pattern, &flags)
        {
            return Some(format!(
                "the regular expression {} uses {why}, which this engine does not match as PostgreSQL does",
                crate::core::value_codec::quote_sql(&pattern)
            ));
        }
    }
    None
}

fn compile_check_lowered(
    expression: &str,
    fields_sv: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
    fields: &[crate::core::row::schema::SchemaField],
) -> Result<CompiledCheck, String> {
    // The question is "is the check FALSE": `NOT (check)` as a WHERE, and
    // where the rewriter cannot negate the expression (a bare `CASE`),
    // `(check) IS FALSE`, which is the same question.
    let mut lowered = Err(String::new());
    for question in [format!("NOT ({})", expression), format!("({}) IS FALSE", expression)] {
        let attempt = crate::core::tokenizer::predicate_rewrite::rewrite_where(&question, fields_sv);
        if let Ok(text) = &attempt {
            if let Ok(condition) = crate::core::rql::expr::parse_cond(text, fields) {
                return Ok(CompiledCheck::Violation(condition));
            }
            // The scan's predicate engine reads more than the condition
            // evaluator: run it.
            let schema_arc = rclite::Arc::new(fields_sv.clone());
            let scanned =
                crate::core::processor::concurrent_processor::ConcurrentProcessor::prepare_query(text, &schema_arc)
                    .and_then(|(no_filter, tokens)| {
                        if no_filter {
                            return Err("an empty check".to_string());
                        }
                        crate::core::processor::concurrent_processor::ConcurrentProcessor::compile_query_plan(
                            &schema_arc,
                            &tokens,
                        )
                    });
            if let Ok(plan) = scanned {
                let width = fields.iter().map(|f| f.write_order as usize + 1).max().unwrap_or(0);
                return Ok(CompiledCheck::Scan { plan, width });
            }
        }
        if lowered.is_err() {
            lowered = attempt;
        }
    }
    // Names resolve among the columns a client sees; the companions are read
    // only when the row is.
    let visible = crate::core::rql::nullable::visible_fields(fields_sv);
    // A regular expression's `(f(…) = TRUE)` is what the rewriter reads; the
    // condition reader takes the call itself, which a comparison or a NOT
    // around it then reads (`(s ~ 'a') = false`, `NOT (s ~ 'x')`).
    let bare = crate::core::rql::check::bare_regex_calls(expression);
    crate::core::rql::expr::parse_cond(expression, &visible)
        .or_else(|e| crate::core::rql::expr::parse_cond(&bare, &visible).map_err(|_| e))
        // A check that is a boolean expression rather than a comparison (a
        // bare `CASE … END`) is compared with TRUE, which keeps its NULL.
        .or_else(|e| crate::core::rql::expr::parse_cond(&format!("({}) = TRUE", expression), &visible).map_err(|_| e))
        .map(CompiledCheck::Direct)
        .map_err(|direct| {
        // One code for a check neither path reads (0A000), whatever the
        // reader's own words would have mapped to.
        format!(
            "this check is not supported: {}",
            lowered.err().filter(|e| !e.is_empty()).unwrap_or(direct)
        )
    })
}

/// Refuses an `UPDATE` whose assignments violate a `CHECK`.
///
/// Only the constraints whose every column the statement assigns can be judged:
/// `UPDATE t SET status = 99` supplies everything `CHECK (status BETWEEN 1 AND
/// 10)` reads, while `UPDATE t SET lo = 5` supplies half of
/// `CHECK (lo < hi)` and the other half is in a row this has not read.
///
/// The rest, every check that also reads a column the statement leaves alone
/// or assigns an expression, are judged row by row by the write, against the
/// row as it will be stored ([`UpdateRowChecks`]). They used to go unjudged:
/// `UPDATE t SET lo = 5` under `CHECK (lo < hi)` stored a row with `hi` 3 and
/// reported success.
#[inline(never)]
fn check_constraints_hold_for_update(
    table_name: &str,
    schema: &crate::core::row::schema::TableSchema,
    plan: &crate::core::rql::lexer_ur::UpdateRow,
) -> Result<(), String> {
    if schema.checks.is_empty() {
        return Ok(());
    }

    let assigned: std::collections::HashSet<u64> =
        plan.updates.iter().map(|p| p.write_order as u64).collect();
    // A column assigned an expression of the row, `q = q + 1` or a recomputed
    // generated column, holds an empty placeholder in the plan. Its value
    // exists only once the row is read, so a check reading it is judged by the
    // write instead; see `UpdateRowChecks`.
    let computed: std::collections::HashSet<u64> =
        plan.expressions.iter().map(|(wo, _)| *wo as u64).collect();

    let mut candidate = crate::core::row::row::Row::default();
    for payload in &plan.updates {
        candidate.columns.push(crate::core::row::row::Column {
            schema_id: crate::core::row::row::schema_id_of(payload.write_order as u64),
            data: payload.data.clone(),
            column_type: payload.column_type.clone(),
        });
    }

    for constraint in schema.checks.iter().filter(|c| !c.is_deleted) {
        let Ok(compiled) = compile_check(&constraint.expression, &schema.fields) else {
            continue;
        };
        // Every column the check reads has to be one this statement assigns,
        // or the row it would be judged against is incomplete.
        let columns = compiled.columns(&schema.fields);
        if columns.is_empty() || !columns.iter().all(|f| assigned.contains(&f.write_order)) {
            continue;
        }
        if columns.iter().any(|f| computed.contains(&f.write_order)) {
            continue;
        }

        if compiled
            .violated(&candidate, &schema.fields, table_name, &constraint.name)
            .map_err(|reason| check_evaluation_failed(&constraint.name, table_name, &reason))?
        {
            return Err(format!(
                "new row for relation \"{}\" violates check constraint \"{}\"",
                table_name, constraint.name
            ));
        }
    }
    Ok(())
}

/// The `CHECK`s an `UPDATE` can only judge row by row, compiled once for the
/// statement and judged by the write against each row as it will be stored.
///
/// These are the checks [`check_constraints_hold_for_update`] leaves out
/// because a column they read is assigned an expression of the row. The rule
/// for which checks are enforced on update is otherwise the same: every column
/// a check reads has to be assigned, so `CHECK (lo < hi)` under `SET lo = lo
/// + 1` is still not judged. A generated column the statement recomputes
/// counts as assigned, which is what makes `CHECK (tot >= 0)` on `tot = q * p`
/// enforceable under `SET q = 5`.
pub(crate) struct UpdateRowChecks {
    /// Each check's name, compiled.
    checks: Vec<(String, CompiledCheck)>,
}

impl UpdateRowChecks {
    /// Empty unless the table has a check this statement's assignments can
    /// change and [`check_constraints_hold_for_update`] cannot judge alone.
    pub(crate) fn compile(
        schema: &crate::core::row::schema::TableSchema,
        updates: &[crate::core::row::row::ColumnWritePayload],
        expressions: &[(u32, crate::core::rql::expr::Expr)],
    ) -> Self {
        let mut out = UpdateRowChecks { checks: Vec::new() };
        if schema.checks.is_empty() {
            return out;
        }

        for constraint in schema.checks.iter().filter(|c| !c.is_deleted) {
            // Compiled as `check_constraints_hold_for_update` compiles it, so
            // the two agree on which checks each one judges.
            let Ok(compiled) = compile_check(&constraint.expression, &schema.fields) else {
                continue;
            };
            let columns = compiled.columns(&schema.fields);
            let assigned = |wo: u64| updates.iter().any(|u| u.write_order as u64 == wo);
            let computed = |wo: u64| expressions.iter().any(|(e, _)| *e as u64 == wo);
            // A check reading no assigned column held before and still does.
            // One whose every column is assigned a constant was judged before
            // the scan. Every other one needs the stored row: a column it
            // reads is left alone, or assigned an expression of the row.
            let judged_up_front = columns.iter().all(|f| assigned(f.write_order))
                && !columns.iter().any(|f| computed(f.write_order));
            if columns.is_empty() || !columns.iter().any(|f| assigned(f.write_order)) || judged_up_front {
                continue;
            }
            out.checks.push((constraint.name.clone(), compiled));
        }
        out
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.checks.is_empty()
    }

    /// Refuses a row, given as the payloads about to be written, that one of
    /// the checks finds FALSE. Three-valued, as `check_constraints_hold` is
    /// on insert: a check that is UNKNOWN passes.
    pub(crate) fn hold(
        &self,
        table_name: &str,
        fields: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
        payloads: &[crate::core::row::row::ColumnWritePayload],
    ) -> Result<(), String> {
        if self.checks.is_empty() {
            return Ok(());
        }
        let mut row = crate::core::row::row::Row::default();
        for payload in payloads {
            row.columns.push(crate::core::row::row::Column {
                schema_id: crate::core::row::row::schema_id_of(payload.write_order as u64),
                data: payload.data.clone(),
                column_type: payload.column_type.clone(),
            });
        }
        for (name, compiled) in &self.checks {
            match compiled.violated(&row, fields, table_name, name) {
                Ok(false) => {}
                Ok(true) => {
                    return Err(format!(
                        "new row for relation \"{}\" violates check constraint \"{}\"",
                        table_name, name
                    ));
                }
                Err(reason) => return Err(check_evaluation_failed(name, table_name, &reason)),
            }
        }
        Ok(())
    }
}

/// Every column a condition tree reads.
fn collect_condition_columns(
    condition: &crate::core::rql::expr::Cond,
    out: &mut Vec<crate::core::row::schema::SchemaField>,
) {
    match condition {
        crate::core::rql::expr::Cond::Cmp(c) => {
            out.extend(crate::core::rql::expr::condition_columns(c));
        }
        crate::core::rql::expr::Cond::And(parts) | crate::core::rql::expr::Cond::Or(parts) => {
            for part in parts {
                collect_condition_columns(part, out);
            }
        }
        crate::core::rql::expr::Cond::Not(inner) => collect_condition_columns(inner, out),
    }
}

/// Works out where each of a table's sequences resumes, once per process.
///
/// A `SERIAL` column's counter lives in memory (see
/// [`crate::core::rql::sequence`]), so a reopened database has to learn what is
/// already stored before it hands out a value — otherwise the first insert
/// after a restart reissues key 1 and collides with the row that has it.
///
/// Costs one scan of the table, and only on the first insert into it in this
/// process, and only for a table that actually declares a serial column. A
/// table with none returns before reading anything.
///
/// Per table, not per sequence: a sequence another table also reads (a
/// hand-written `DEFAULT nextval('…')`) may already have been seeded, from
/// that table's rows, which can be below this table's. Skipping the seed
/// then, as this did, handed this table keys it already held. So each table
/// raises the counter to its own floor ([`crate::core::rql::sequence::seed`]
/// keeps the larger) and marks it done
/// ([`crate::core::rql::sequence::mark_floored`]).
///
/// A partitioned table's sequence is the logical table's, one for all its
/// partitions (§10), so it is seeded from the maximum over every materialized
/// partition; an unmaterialized one holds no row. Read through the shell, the
/// scan failed and left the sequence unseeded, and a `SERIAL` restarted at 1
/// after every restart: a silent duplicate on a column with no unique key.
/// So a partitioned table that cannot be read is an error here, which the
/// insert reports; a plain table's failure is only logged, as it always was.
///
/// `target` is the INSERT's target, resolved by the caller under its ROW
/// EXCLUSIVE (a partitioned one by its name); callers ask
/// [`has_unseeded_sequence`] first, so the common insert neither boxes nor
/// runs this.
async fn seed_sequences(
    database: &Arc<Database>,
    table_name: &str,
    target: Option<&crate::core::database::Relation>,
) -> Result<(), String> {
    let (table, partitioned) = match target {
        Some(crate::core::database::Relation::Plain(table)) => (table.clone(), None),
        Some(crate::core::database::Relation::Partitioned(pt)) => (pt.shell().clone(), Some(pt.clone())),
        None => return Ok(()),
    };

    // Which columns are serial, and which of their sequences still need a seed.
    let mut wanted: Vec<(String, crate::core::row::schema::SchemaField)> = Vec::new();
    for field in table.schema.fields.iter().filter(|f| !f.is_deleted) {
        let Some(expression) =
            crate::core::rql::column_default::expression_for(&field.name, &table.schema.defaults)
        else {
            continue;
        };
        let Some(name) = crate::core::rql::sequence::parse_nextval(expression) else {
            continue;
        };
        if !crate::core::rql::sequence::is_floored(database.id, &name, table_name) {
            wanted.push((name, field.clone()));
        }
    }
    if wanted.is_empty() {
        return Ok(());
    }

    let schema = &table.schema.fields;
    let fetch_fields: Vec<crate::core::row::schema::SchemaField> =
        wanted.iter().map(|(_, f)| f.clone()).collect();
    let fetch = crate::core::rql::join::fetch_for(&fetch_fields, schema);
    let empty = crate::core::rql::join::fetch_for(&[], schema);
    if let Some(pt) = partitioned {
        // From the durable reservation when the table has one (§10, phase 3
        // P5a): no partition is opened or read. The reservation is at or
        // above every value a partition holds, so it is the table's floor.
        // Only a sequence it has no entry for is seeded from the maximum,
        // and then reserved.
        let mut unseeded = Vec::new();
        for (name, field) in wanted {
            if pt
                .resume_serial(&name)
                .map_err(|e| format!("could not read '{}' to seed its sequences: {}", table_name, e))?
            {
                crate::core::rql::sequence::mark_floored(database.id, &name, table_name);
            } else {
                unseeded.push((name, field));
            }
        }
        if unseeded.is_empty() {
            return Ok(());
        }
        let wanted = unseeded;
        let fetch_fields: Vec<crate::core::row::schema::SchemaField> =
            wanted.iter().map(|(_, f)| f.clone()).collect();
        let fetch = crate::core::rql::join::fetch_for(&fetch_fields, schema);
        let mut rows = Vec::new();
        let partitions = pt
            .epoch()
            .and_then(|epoch| pt.materialized(&epoch, &crate::core::partition::Route::All))
            .map_err(|e| format!("could not read '{}' to seed its sequences: {}", table_name, e))?;
        for (pid, partition) in partitions {
            let found = partition
                .query_row("", &schema.to_vec(), empty.clone(), fetch.clone(), None, None)
                .await
                .map_err(|e| {
                    format!(
                        "could not read partition {} of '{}' to seed its sequences: {}",
                        pid, table_name, e
                    )
                })?;
            rows.extend(found);
        }
        for (name, field) in wanted {
            let last =
                crate::core::rql::sequence::max_of_column(&rows, field.write_order, &field.db_type)
                    .unwrap_or(0);
            crate::core::rql::sequence::seed(database.id, &name, last);
            // The counter is at or above `last` now, whoever seeded it.
            pt.track_serial(&name, last, true)
                .map_err(|e| format!("could not reserve '{}''s sequence {}: {}", table_name, name, e))?;
            crate::core::rql::sequence::mark_floored(database.id, &name, table_name);
        }
        return Ok(());
    }
    let rows = match table
        .query_row("", &schema.to_vec(), empty, fetch, None, None)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            // Seeding at zero would hand out a key the table may already hold,
            // so the failure is reported rather than papered over. The insert
            // itself then fails on the duplicate, which names the real problem.
            log::error!(
                "could not read '{}' to seed its sequences: {}",
                table_name,
                e
            );
            return Ok(());
        }
    };

    for (name, field) in wanted {
        let last =
            crate::core::rql::sequence::max_of_column(&rows, field.write_order, &field.db_type)
                .unwrap_or(0);
        crate::core::rql::sequence::seed(database.id, &name, last);
        crate::core::rql::sequence::mark_floored(database.id, &name, table_name);
    }
    Ok(())
}

/// Whether `schema` (table `table_name`'s) declares a serial column whose
/// sequence this table has not raised to its floor in this process: what
/// [`seed_sequences`] would do anything for. One pass over the defaults; no
/// allocation for a table without a `nextval` default, which is nearly every
/// insert.
fn has_unseeded_sequence(
    database: &Database,
    table_name: &str,
    schema: &crate::core::row::schema::TableSchema,
) -> bool {
    if schema.defaults.is_empty() {
        return false;
    }
    schema.fields.iter().filter(|f| !f.is_deleted).any(|field| {
        crate::core::rql::column_default::expression_for(&field.name, &schema.defaults)
            .and_then(crate::core::rql::sequence::parse_nextval)
            .is_some_and(|name| !crate::core::rql::sequence::is_floored(database.id, &name, table_name))
    })
}

async fn existing_rows_break_unique(
    database: &Arc<Database>,
    table_name: &str,
    key: &crate::core::rql::unique_key::UniqueKey,
) -> Result<Option<String>, String> {
    let entry = database.table_for(table_name, owner(), TableMode::AccessExclusive).await?;
    let Some(table) = entry.as_ref().clone() else {
        return Err(format!("relation '{}' does not exist", table_name));
    };
    // The rows: the table's, or each open partition's. Exact per partition,
    // since a partitioned table's unique key holds every partition key
    // column (docs/partitioning.md §1b), so equal tuples share a partition.
    let holders = database.row_tables_held(table_name).map_err(|e| e.to_string())?;

    let schema = &table.schema.fields;
    let mut fields = Vec::with_capacity(key.columns.len());
    for column in &key.columns {
        let field = schema
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(column) && !f.is_deleted)
            .ok_or_else(|| format!("column '{}' does not exist", column))?;
        fields.push(field.clone());
    }

    // The NULL companions come too. A NULL in a *fixed-width* column is stored
    // as that type's zeroes, not as an absent value, so emptiness cannot tell a
    // NULL uuid from a real all-zero one — and the companion is the only thing
    // that can. Without it four NULL rows looked like four copies of the same
    // uuid, and a `UNIQUE` constraint PostgreSQL was perfectly happy with was
    // refused as holding duplicates.
    let schema_sv: smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> =
        schema.iter().cloned().collect();
    let mut fetch_fields = fields.clone();
    let mut flag_of: Vec<Option<u64>> = Vec::with_capacity(fields.len());
    for field in &fields {
        match crate::core::rql::nullable::flag_field(&field.name, &schema_sv) {
            Some(flag) => {
                if !fetch_fields.iter().any(|f| f.name == flag.name) {
                    fetch_fields.push(flag.clone());
                }
                flag_of.push(Some(flag.write_order));
            }
            None => flag_of.push(None),
        }
    }

    // A partial key holds only among the rows its predicate is true for, so
    // only those are compared, and its columns are fetched to judge that.
    let condition = key.condition(schema)?;
    if let Some(condition) = condition {
        for field in crate::core::rql::expr::cond_columns(condition) {
            let flag = crate::core::rql::nullable::flag_field(&field.name, &schema_sv).cloned();
            for f in std::iter::once(field).chain(flag) {
                if !fetch_fields.iter().any(|g| g.write_order == f.write_order) {
                    fetch_fields.push(f);
                }
            }
        }
    }

    let fetch = crate::core::rql::join::fetch_for(&fetch_fields, schema);
    let empty = crate::core::rql::join::fetch_for(&[], schema);
    let mut rows = Vec::new();
    for holder in &holders {
        rows.extend(
            holder
                .query_row("", &schema.to_vec(), empty.clone(), fetch.clone(), None, None)
                .await
                .map_err(|e| {
                    format!(
                        "could not read '{}' to validate the constraint: {}",
                        table_name, e
                    )
                })?,
        );
    }

    let mut seen: ahash::HashSet<Vec<u8>> =
        ahash::HashSet::with_capacity_and_hasher(rows.len(), Default::default());
    for row in &rows {
        if let Some(condition) = condition {
            if !crate::core::rql::expr::eval_cond(condition, row, schema)? {
                continue;
            }
        }
        // Length-prefixed, so ('ab', 'c') and ('a', 'bc') are different tuples
        // rather than the same run of bytes.
        let mut encoded: Vec<u8> = Vec::new();
        let mut has_null = false;
        for (index, field) in fields.iter().enumerate() {
            let Some(column) = row
                .columns
                .iter()
                .find(|c| u64::from(c.schema_id) == field.write_order)
            else {
                has_null = true;
                break;
            };
            // NULLS DISTINCT: a tuple containing a NULL conflicts with nothing.
            //
            // NULL is read from the companion (or a cell typed NULL), never
            // from emptiness: `''` and a zero-length BYTEA are values, and
            // treating them as NULL let a constraint be added over rows that
            // already held them twice. Only a nullable column whose companion
            // is missing from the row falls back to emptiness — the same rule
            // `Table::written_value_is_null` applies at insert time.
            let bytes = column.data.into_slice();
            let is_null = column.column_type == crate::core::db_type::DbType::NULL
                || match flag_of[index] {
                    None => false,
                    Some(order) => match row.columns.iter().find(|c| u64::from(c.schema_id) == order) {
                        Some(c) => crate::core::rql::nullable::flag_is_set(c.data.into_slice()),
                        None => bytes.is_empty(),
                    },
                };
            if is_null {
                has_null = true;
                break;
            }
            // A document by its value, as the insert-time check compares it.
            if field.db_type == crate::core::db_type::DbType::JSONB {
                crate::core::jsonb::push_group_key(bytes, &mut encoded);
                continue;
            }
            encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            encoded.extend_from_slice(bytes);
        }
        if has_null {
            continue;
        }
        if !seen.insert(encoded) {
            // The conflicting tuple is named, so the operator can find the
            // rows to clean up. "Holds duplicates" alone forces a manual
            // GROUP BY hunt over a table this statement already read.
            let mut values: Vec<String> = Vec::new();
            for field in fields.iter() {
                let rendered = row
                    .columns
                    .iter()
                    .find(|c| u64::from(c.schema_id) == field.write_order)
                    .and_then(|c| {
                        crate::core::rql::subquery::value_to_literal(
                            &field.db_type,
                            c.data.into_slice(),
                        )
                    })
                    .unwrap_or_else(|| "?".to_string());
                values.push(rendered);
            }
            // "duplicate key value" is what maps the refusal to 23505, the
            // code PostgreSQL gives `could not create unique index`.
            return Ok(Some(format!(
                "duplicate key value: the table already holds duplicate values for ({}): \
                 first duplicate is ({})",
                key.columns.join(", "),
                values.join(", ")
            )));
        }
    }

    Ok(None)
}

/// Applies every action in an `ALTER TABLE`.
///
/// A statement may carry a comma-separated action list, and each action is
/// applied in turn. The first failure returns, so a rejected action is not
/// followed by later ones landing on a half-changed table.
async fn execute_alter_table(sql: &str, database: &Arc<Database>, tx: TxId) -> Vec<u8> {
    use crate::core::rql::lexer_alter;

    let plans = match lexer_alter::parse_alter_table_actions_tagged(sql) {
        Ok(p) => p,
        Err(e) => {
            error!("Cannot parse ALTER TABLE: {}", e);
            return ddl_error(&e);
        }
    };

    // Before `IF EXISTS`: a partitioned table that could not be opened is
    // absent from `tables` yet exists. Every action is checked before the
    // first runs, since each writes its sidecar before it reloads; each arm
    // asks again under its ACCESS EXCLUSIVE.
    for (plan, _) in &plans {
        if let Some(why) = database.partitioned_table(&plan.table_name).and_then(|t| t.broken().map(str::to_string)) {
            return ddl_error(&why);
        }
        if let Some(e) = database.partitioned_alter_refusal(plan) {
            return ddl_error(&e);
        }
    }

    // `ALTER TABLE IF EXISTS …` on a missing relation is a notice on
    // PostgreSQL, not an error. With non-transactional DDL every replayed
    // migration depends on that tolerance: a retry runs its statements over
    // whatever the failed attempt already applied, so a rename that has
    // already happened must not wedge the chain.
    let mut head = sql.split_whitespace();
    let if_exists = head.next().is_some_and(|w| w.eq_ignore_ascii_case("ALTER"))
        && head.next().is_some_and(|w| w.eq_ignore_ascii_case("TABLE"))
        && head.next().is_some_and(|w| w.eq_ignore_ascii_case("IF"))
        && head
            .next()
            .is_some_and(|w| w.eq_ignore_ascii_case("EXISTS"));
    if if_exists {
        if let Some((first, _)) = plans.first() {
            if !database.has_table(&first.table_name) {
                info!(
                    "ALTER TABLE IF EXISTS {}: relation absent, nothing to do",
                    first.table_name
                );
                return ddl_ok(0);
            }
        }
    }

    let mut result = ddl_ok(0);
    // Set while an `ADD COLUMN IF NOT EXISTS` found its column: the
    // constraints written on it are part of that definition, and go with it.
    let mut skipping = false;
    // Every column this statement has added, for undoing it.
    let mut added: Vec<(String, String)> = Vec::new();
    for (plan, inline) in plans {
        if inline {
            if skipping {
                continue;
            }
        } else {
            skipping = match &plan.action {
                // Under the ACCESS EXCLUSIVE the statement already holds, so
                // the column cannot appear between this look and the add.
                lexer_alter::AlterAction::AddColumn { name, if_not_exists: true, .. } => {
                    match database.relation_for(&plan.table_name, owner(), TableMode::AccessExclusive).await {
                        Ok(relation) => relation.is_some_and(|r| {
                            r.schema().fields.iter().any(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
                        }),
                        Err(e) => return ddl_error(&e),
                    }
                }
                _ => false,
            };
        }
        let added_now = match (&plan.action, skipping) {
            (lexer_alter::AlterAction::AddColumn { name, .. }, false) => Some((plan.table_name.clone(), name.clone())),
            _ => None,
        };
        result = execute_alter_table_action(plan, database, tx).await;
        // `ddl_error` frames lead with status 3.
        if result.first() == Some(&3u8) {
            // A constraint written on a column failed — a DEFAULT the rows
            // share breaks UNIQUE, has no parent, fails the CHECK. Every
            // column this statement added goes, and its constraints with it:
            // in PostgreSQL the statement changes nothing. Other actions'
            // failures keep what earlier actions did, as DDL here always has.
            if inline {
                for (table_name, name) in added.drain(..).rev() {
                    let undo = lexer_alter::AlterTable {
                        table_name,
                        action: lexer_alter::AlterAction::DropColumn { name, if_exists: true },
                    };
                    let undone = execute_alter_table_action(undo, database, tx).await;
                    if undone.first() == Some(&3u8) {
                        error!(
                            "ALTER TABLE: a constraint on an added column failed and the column could not \
                             be dropped again: {}",
                            String::from_utf8_lossy(&undone[1..])
                        );
                    }
                }
            }
            return result;
        }
        added.extend(added_now);
    }
    result
}

/// Frees a column name still held by a dropped column.
///
/// A dropped column keeps its slot in the row layout and is only tombstoned, so
/// its *name* stays in the schema too. Nothing can refer to it again — but every
/// by-name lookup in the engine takes the first field it matches, and there are
/// more than twenty such lookups across the query paths, so a dead field
/// sharing a live one's name shadows it silently. Retiring the name here keeps
/// the schema's names unique, which is what all of those lookups assume.
///
/// The retired name is derived from the write_order, which is unique within a
/// table, so repeating the dance leaves no collision behind either.
async fn retire_tombstoned_name<S: crate::core::storage_providers::traits::StorageIO>(
    schema: &mut crate::core::row::schema::TableSchema,
    name: &str,
    io_schema: Arc<S>,
) -> Result<(), String> {
    loop {
        let Some(order) = schema
            .fields
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(name) && f.is_deleted)
            .map(|f| f.write_order)
        else {
            return Ok(());
        };

        let retired = format!("__dropped_{}_{}", name, order);
        // Renaming to a name it already carries would spin.
        if retired.eq_ignore_ascii_case(name) {
            return Ok(());
        }
        schema
            .rename_field(name, &retired, io_schema.clone())
            .await?;
    }
}

/// What `ALTER COLUMN column TYPE declared` changes about a text column's
/// declared length (`type_modifier::TextTypmod`), which shares text's
/// storage: the marker records to write, after the stored values have been
/// brought to the new type, as PostgreSQL's rewrite brings them.
///
/// - To `character(n)`: every value held to `n` (22001 past it) and stored
///   without its trailing spaces, so `'p'` and `'p '` under a UNIQUE are one
///   key (23505), as they are in PostgreSQL.
/// - To `character varying(n)`: every value held to `n`, excess spaces cut.
/// - From `character(n)` to text: nothing; the values are already stored
///   without their trailing spaces, which is what the conversion gives.
/// - A `character(n)` column an older binary created (no length,
///   [`type_modifier::is_legacy_bpchar`]) holds its values as written, so it
///   is converted as text is. Restating its type is how it gets one.
///
/// Nothing when the column keeps its type and length.
async fn text_type_change(
    database: &Arc<Database>,
    table_name: &str,
    schema: &crate::core::row::schema::TableSchema,
    field: &crate::core::row::schema::SchemaField,
    declared: &str,
    row_count: u64,
    tx: TxId,
) -> Result<Vec<crate::core::rql::type_modifier::TypeModifier>, String> {
    use crate::core::rql::type_modifier::{self, TextTypmod};
    use crate::core::db_type::DbType;
    // A time column's declared precision: the values rounded to it, as
    // PostgreSQL's rewrite rounds them.
    if matches!(field.db_type, DbType::DATETIME | DbType::TIME | DbType::TIMETZ | DbType::INTERVAL) {
        let wanted = match type_modifier::parse_time_precision(declared) {
            Some(Ok(t)) => Some(t),
            Some(Err(e)) => return Err(e),
            None => None,
        };
        let had = field.text_typmod();
        if wanted == had {
            return Ok(Vec::new());
        }
        let narrower = match (had, wanted) {
            (_, None) => false,
            // Fewer digits, or other interval fields, change stored values.
            (Some(TextTypmod::Precision(h)), Some(TextTypmod::Precision(w))) => {
                use type_modifier::{INTERVAL_FIELDS_SHIFT, PRECISION_DIGITS};
                w & PRECISION_DIGITS < h & PRECISION_DIGITS || w >> INTERVAL_FIELDS_SHIFT != h >> INTERVAL_FIELDS_SHIFT
            }
            _ => true,
        };
        if row_count > 0 && narrower {
            let column = format!("\"{}\"", field.name.replace('"', "\"\""));
            let sql = format!("UPDATE {table_name} SET {column} = CAST({column} AS {})", declared.trim());
            let purpose = recognize_query_purpose(&sql)
                .ok_or_else(|| format!("could not convert column \"{}\" to {}", field.name, declared.trim()))?;
            let response = Box::pin(execute_in_txn(purpose, database.clone(), tx)).await;
            if response.first() == Some(&3) {
                return Err(String::from_utf8_lossy(&response[1..]).to_string());
            }
        }
        return Ok(type_modifier::text_typmod_records(&field.name, wanted, &schema.type_modifiers));
    }
    if field.db_type != DbType::STRING {
        return Ok(Vec::new());
    }
    let wanted = match type_modifier::parse_text_typmod(declared) {
        Some(Ok(t)) => Some(t),
        Some(Err(e)) => return Err(e),
        None => None,
    };
    let had = field.text_typmod();
    let legacy = type_modifier::is_legacy_bpchar(&field.name, &schema.type_modifiers);
    if wanted == had && !legacy {
        return Ok(Vec::new());
    }
    if row_count > 0 {
        // Its values already in `character(n)`'s stored form, or not.
        let stripped = matches!(had, Some(TextTypmod::Bpchar(_))) && !legacy;
        let simple = |name: &str| {
            name.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
                && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        };
        let column = if simple(&field.name) { field.name.clone() } else { format!("\"{}\"", field.name.replace('"', "\"\"")) };
        // Past the length is an error unless the excess is spaces.
        if let Some(n) = wanted.and_then(|t| t.length()) {
            let shrinks = !matches!(had, Some(TextTypmod::Bpchar(m) | TextTypmod::Varchar(m)) if !legacy && m != 0 && m <= n);
            if shrinks {
                let probe = format!("SELECT 1 FROM {table_name} WHERE length(rtrim({column})) > {n} LIMIT 1");
                if !run_subquery(&probe, database, tx).await?.is_empty() {
                    return Err(format!("value too long for type {}", wanted.expect("has a length").type_name()));
                }
            }
        }
        let rewrite = match wanted {
            // Stored without trailing spaces; a key two values now share is
            // the UPDATE's 23505.
            Some(TextTypmod::Bpchar(_)) if !stripped => {
                Some(format!("UPDATE {table_name} SET {column} = rtrim({column}) WHERE {column} LIKE '% '"))
            }
            // Excess spaces cut (anything else was refused above).
            Some(TextTypmod::Varchar(n)) if n > 0 => {
                Some(format!("UPDATE {table_name} SET {column} = rpad({column}, {n}) WHERE length({column}) > {n}"))
            }
            // `character(n)` to text loses the padding, which an older
            // binary's column still stores.
            None | Some(TextTypmod::Varchar(0)) if legacy => Some(format!("UPDATE {table_name} SET {column} = rtrim({column}) WHERE {column} LIKE '% '")),
            _ => None,
        };
        if let Some(sql) = rewrite {
            let purpose = recognize_query_purpose(&sql)
                .ok_or_else(|| format!("could not convert column \"{}\" to {}", field.name, declared.trim()))?;
            let response = Box::pin(execute_in_txn(purpose, database.clone(), tx)).await;
            if response.first() == Some(&3) {
                return Err(String::from_utf8_lossy(&response[1..]).to_string());
            }
        }
        // An older binary's `character(n)` column kept every check reading it
        // unenforced, so its rows were never judged by them. Given a length
        // the column's checks are enforced from now on (`compile_check`), and
        // as `ADD CONSTRAINT` does, a stored row one of them is false for
        // refuses the change rather than sit under a check it breaks.
        if legacy && matches!(wanted, Some(TextTypmod::Bpchar(_))) {
            let names: Vec<String> = schema.fields.iter().filter(|f| !f.is_deleted).map(|f| f.name.clone()).collect();
            for check in schema.checks.iter().filter(|c| !c.is_deleted) {
                let reads = crate::core::rql::check::columns_read(&check.expression, &names);
                if !reads.iter().any(|c| c.eq_ignore_ascii_case(&field.name)) {
                    continue;
                }
                // Still reading another column held as written: it stays
                // unenforced (`compile_check`), and judges nothing.
                let still_declined = reads.iter().any(|c| {
                    !c.eq_ignore_ascii_case(&field.name)
                        && schema.fields.iter().any(|f| {
                            !f.is_deleted
                                && f.name == *c
                                && f.db_type == crate::core::db_type::DbType::STRING
                                && !matches!(f.text_typmod(), Some(TextTypmod::Bpchar(_)))
                        })
                });
                if still_declined && compares_bpchar(&check.expression) {
                    continue;
                }
                let normalized = crate::core::rql::check::normalize_expression(&check.expression);
                let probe = format!("SELECT 1 FROM {table_name} WHERE NOT ({normalized}) LIMIT 1");
                if !run_subquery(&probe, database, tx).await?.is_empty() {
                    return Err(format!(
                        "check constraint \"{}\" of relation \"{}\" is violated by some row",
                        check.name, table_name
                    ));
                }
            }
        }
    }
    // An older binary's marker is retired when the column leaves
    // `character(n)`, and superseded by one carrying the length when not.
    Ok(type_modifier::text_typmod_records(&field.name, wanted, &schema.type_modifiers))
}

/// `ALTER COLUMN … TYPE` naming the type a column already has, with what it
/// says about a NUMERIC column's `(precision, scale)`.
///
/// The type names compare equal without their modifiers, so a scale change
/// used to be reported as a no-op and kept the old scale: every later value
/// was silently rounded to it. A decimal is stored as an integer at its
/// column's scale, so on a table with rows a scale change is refused, as a
/// width change is. On an empty one the new modifier is recorded. A changed
/// precision at the same scale is only recorded. Returns whether anything was
/// written.
fn numeric_modifier_change(
    schema: &crate::core::row::schema::TableSchema,
    column: &str,
    declared: &str,
    row_count: u64,
) -> Result<Option<crate::core::rql::type_modifier::TypeModifier>, String> {
    use crate::core::rql::type_modifier::{self, TypeModifier};
    let current = schema
        .type_modifiers
        .iter()
        .find(|m| m.column.eq_ignore_ascii_case(column) && !m.is_deleted);
    let wanted = match type_modifier::parse_numeric_modifier(declared) {
        Some(Ok(pair)) => Some(pair),
        Some(Err(e)) => return Err(e),
        None => None,
    };
    let scale_now = current.map_or(type_modifier::DEFAULT_DECIMAL_SCALE, |m| m.scale);
    let scale_wanted = wanted.map_or(type_modifier::DEFAULT_DECIMAL_SCALE, |(_, s)| s);
    let entry = match (current, wanted) {
        (None, None) => return Ok(None),
        (Some(m), Some((p, s))) if m.precision == p && m.scale == s => return Ok(None),
        (_, Some((precision, scale))) => TypeModifier {
            column: column.to_string(),
            precision,
            scale,
            is_deleted: false,
        },
        (Some(_), None) => TypeModifier {
            column: column.to_string(),
            precision: 0,
            scale: 0,
            is_deleted: true,
        },
    };
    if scale_now != scale_wanted && row_count > 0 {
        return Err(format!(
            "ALTER COLUMN {} TYPE {} would rewrite every row from scale {} to scale {}, which is \
             not supported yet",
            column, declared, scale_now, scale_wanted
        ));
    }
    if entry.is_deleted && scale_now == scale_wanted {
        // Unconstrained at the scale it already stores: nothing to change.
        return Ok(None);
    }
    Ok(Some(entry))
}

/// `expression` with every reference to column `from` spelled `to` instead.
///
/// For the stored text of a `CHECK` or a generation expression, which names
/// the columns it reads: after `RENAME COLUMN` it named one that no longer
/// existed, and the check was skipped as uncompilable — every row it forbids
/// accepted — while the generated column refused every insert.
///
/// A scanner rather than a parser, but one that tracks what a word can sit
/// inside. String literals (`'…'`, `E'…'`, `$tag$…$tag$`) and comments are
/// copied untouched, so `CHECK (a <> 'a')` keeps its value. A quoted
/// identifier is compared by its content. And a matching word is a column
/// reference only when it is not a function name (followed by `(`), a cast's
/// type (after `::`, or after `AS` in `CAST(x AS text)`), a typed literal's
/// type or a literal's prefix (followed by a string: `date '2000-01-01'`,
/// `X'00'`, `B'01'`, `N'…'`, `U&'…'`), `EXTRACT`'s field (`EXTRACT(epoch
/// FROM ts)`) or a qualifier (followed by `.`); after a `.` it is one only
/// when the qualifier is the table itself. Renaming any of those rewrote a
/// type or keyword, and the check stopped compiling and was skipped.
pub(crate) fn rename_column_references(expression: &str, table: &str, from: &str, to: &str) -> String {
    let chars: Vec<char> = expression.chars().collect();
    let len = chars.len();
    let next_significant = |mut j: usize| {
        while j < len && chars[j].is_whitespace() {
            j += 1;
        }
        chars.get(j).copied()
    };
    // Copies the literal opening at `i`; returns the index after it.
    let copy_literal = |mut i: usize, backslash: bool, out: &mut String| {
        out.push('\'');
        i += 1;
        while i < len {
            let c = chars[i];
            out.push(c);
            i += 1;
            if backslash && c == '\\' {
                if i < len {
                    out.push(chars[i]);
                    i += 1;
                }
                continue;
            }
            if c == '\'' {
                if chars.get(i) == Some(&'\'') {
                    out.push('\'');
                    i += 1;
                    continue;
                }
                break;
            }
        }
        i
    };
    let plain = to
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && to.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    let quoted = format!("\"{}\"", to.replace('"', "\"\""));

    #[derive(PartialEq)]
    enum Prev {
        Other,
        Cast,
        // After a `.`: whether what stood before it was the table's name.
        Dot(bool),
    }
    let mut prev = Prev::Other;
    // The identifier just before, which a following `.` makes a qualifier.
    let mut last_word: Option<String> = None;
    // Just past `EXTRACT (`, where the next word is the field to extract.
    let mut extract_field = false;
    let mut out = String::with_capacity(expression.len());
    let mut i = 0;
    while i < len {
        let c = chars[i];
        if c.is_whitespace() {
            out.push(c);
            i += 1;
            continue;
        }
        if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < len && chars[i] != '\n' {
                out.push(chars[i]);
                i += 1;
            }
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            out.push_str("/*");
            i += 2;
            while i < len && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                out.push(chars[i]);
                i += 1;
            }
            if i < len {
                out.push_str("*/");
                i += 2;
            }
            continue;
        }
        let in_extract_field = std::mem::take(&mut extract_field);
        if c == '\'' {
            i = copy_literal(i, false, &mut out);
            prev = Prev::Other;
            last_word = None;
            continue;
        }
        if c == '$' {
            // `$tag$ … $tag$`, copied whole; a lone `$` is just a character.
            let mut j = i + 1;
            while j < len && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
                j += 1;
            }
            if j < len && chars[j] == '$' && !chars.get(i + 1).is_some_and(|d| d.is_ascii_digit()) {
                let tag: String = chars[i..=j].iter().collect();
                let body_start = j + 1;
                let rest: String = chars[body_start..].iter().collect();
                let end = rest
                    .find(&tag)
                    .map(|at| body_start + rest[..at].chars().count() + tag.chars().count())
                    .unwrap_or(len);
                out.extend(&chars[i..end]);
                i = end;
                prev = Prev::Other;
                last_word = None;
                continue;
            }
            out.push(c);
            i += 1;
            prev = Prev::Other;
            last_word = None;
            continue;
        }
        let word = if c == '"' {
            let mut inner = String::new();
            let mut j = i + 1;
            while j < len {
                if chars[j] == '"' {
                    if chars.get(j + 1) == Some(&'"') {
                        inner.push('"');
                        j += 2;
                        continue;
                    }
                    j += 1;
                    break;
                }
                inner.push(chars[j]);
                j += 1;
            }
            Some((inner, j, true))
        } else if c.is_alphabetic() || c == '_' {
            let mut j = i;
            while j < len && (chars[j].is_alphanumeric() || chars[j] == '_' || chars[j] == '$') {
                j += 1;
            }
            let text: String = chars[i..j].iter().collect();
            // `E'…'`: a literal with backslash escapes, not a word.
            if text.eq_ignore_ascii_case("e") && chars.get(j) == Some(&'\'') {
                out.push(c);
                i = copy_literal(j, true, &mut out);
                prev = Prev::Other;
                last_word = None;
                continue;
            }
            Some((text, j, false))
        } else {
            None
        };
        if let Some((text, end, was_quoted)) = word {
            let after = next_significant(end);
            let after_as = last_word
                .as_deref()
                .is_some_and(|w| w.eq_ignore_ascii_case("as"));
            // `U&'…'`: the prefix runs straight into `&'`.
            let unicode_prefix = !was_quoted
                && text.eq_ignore_ascii_case("u")
                && chars.get(end) == Some(&'&')
                && chars.get(end + 1) == Some(&'\'');
            let is_reference = text.eq_ignore_ascii_case(from)
                && prev != Prev::Cast
                && after != Some('(')
                && after != Some('.')
                && prev != Prev::Dot(false)
                && !(after == Some('\'') && !was_quoted)
                && !unicode_prefix
                && !after_as
                && !in_extract_field;
            if is_reference {
                out.push_str(if plain && !was_quoted { to } else { &quoted });
            } else {
                out.extend(&chars[i..end]);
            }
            i = end;
            prev = Prev::Other;
            last_word = Some(text);
            continue;
        }
        if c.is_ascii_digit() {
            while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == '.')
            {
                out.push(chars[i]);
                i += 1;
            }
            prev = Prev::Other;
            last_word = None;
            continue;
        }
        if c == ':' && chars.get(i + 1) == Some(&':') {
            out.push_str("::");
            i += 2;
            prev = Prev::Cast;
            last_word = None;
            continue;
        }
        if c == '.' {
            out.push(c);
            i += 1;
            prev = Prev::Dot(
                last_word
                    .take()
                    .is_some_and(|w| w.eq_ignore_ascii_case(table)),
            );
            continue;
        }
        extract_field = c == '(' && last_word.as_deref().is_some_and(|w| w.eq_ignore_ascii_case("extract"));
        out.push(c);
        i += 1;
        prev = Prev::Other;
        last_word = None;
    }
    out
}

/// Every sequence a `nextval` default of a table in `database` other than
/// `except` names, as [`crate::core::rql::sequence::named_by`] spells them.
///
/// What a table may not take with it when it is dropped or renamed: something
/// else still counts on these.
fn sequences_named_elsewhere(
    database: &Arc<Database>,
    except: &str,
) -> std::collections::HashSet<String> {
    let mut named = std::collections::HashSet::new();
    database.each_schema(|table, schema| {
        if table != except {
            named.extend(crate::core::rql::sequence::named_by(&schema.defaults));
        }
    });
    named
}

/// Fills a generated column just added to a table that already holds rows:
/// `UPDATE t SET g = DEFAULT`, which recomputes it from each row exactly as an
/// UPDATE of its inputs would.
async fn compute_generated_column(database: &Arc<Database>, table_name: &str, column: &str, tx: TxId) -> Vec<u8> {
    let sql = format!("UPDATE {} SET {} = DEFAULT", table_name, column);
    let Some(purpose) = recognize_query_purpose(&sql) else {
        return ddl_error(&format!("could not compute the generated column \"{}\"", column));
    };
    let response = Box::pin(execute_in_txn(purpose, database.clone(), tx)).await;
    match response.first() {
        Some(3) => ddl_error(&format!(
            "could not compute the generated column \"{}\": {}",
            column,
            String::from_utf8_lossy(&response[1..])
        )),
        _ => ddl_ok(0),
    }
}

/// `ALTER TABLE … ADD [CONSTRAINT name] CHECK (expression)`.
///
/// As PostgreSQL does it: the expression must compile against the table, every
/// existing row must satisfy it (a NULL result passes), and only then is it
/// stored and enforced on every later write.
async fn add_check_constraint(
    table_name: &str,
    schema: &crate::core::row::schema::TableSchema,
    description: &str,
    expression: String,
    database: &Arc<Database>,
    tx: TxId,
) -> Vec<u8> {
    // Bare or quoted, as a UNIQUE's name is read (`split_constraint_name`).
    let named = match crate::core::rql::unique_key::split_constraint_name(description) {
        Ok((named, _)) => named,
        Err(e) => return ddl_error(&e),
    };
    // Unnamed, it is named as PostgreSQL names it (`check::unnamed_name`).
    let name = match named {
        Some(name) => name,
        None => {
            let columns: Vec<String> = crate::core::rql::nullable::visible_fields(&schema.fields)
                .iter()
                .filter(|f| !f.is_deleted)
                .map(|f| f.name.clone())
                .collect();
            match crate::core::rql::check::unnamed_name(table_name, &expression, &columns, |n| {
                constraint_name_taken(schema, table_name, n)
            }) {
                Ok(name) => name,
                Err(e) => return ddl_error(&e),
            }
        }
    };
    if schema.checks.iter().any(|c| !c.is_deleted && c.name.eq_ignore_ascii_case(&name)) {
        return ddl_error(&format!("constraint \"{}\" for relation \"{}\" already exists", name, table_name));
    }
    // A `character(n)` column an older binary created holds its values as
    // written, so a check on it cannot compare them blank-padded: it is cast
    // `::bpchar` where the check reads it, as that binary stored such
    // checks, and kept unenforced. A column with a recorded length
    // (`type_modifier::TextTypmod`) needs none of this.
    let bpchar = crate::core::rql::type_modifier::legacy_bpchar_columns(&schema.type_modifiers);
    let expression = if bpchar.is_empty() {
        expression
    } else {
        crate::core::rql::check::cast_columns_to_bpchar(&expression, &bpchar)
    };

    // It must compile, or it would be stored and then skipped on every write.
    let unenforced = match compile_check(&expression, &schema.fields) {
        Err(reason) => {
            return ddl_error(&format!("check constraint \"{}\" cannot be compiled: {}", name, reason));
        }
        Ok(CompiledCheck::Unenforced(why)) => {
            warn_unenforced_check(&name, table_name, &why);
            true
        }
        Ok(_) => false,
    };

    // A row for which it is false (NULL is not false) refuses the statement.
    // A check kept unenforced judges no row, the stored ones included.
    // A stored row whose regex answer depends on the locale (non-ASCII text
    // under a locale class) is judged as a write judges it: a check where
    // that answer cannot change the result (`regex_calls_are_monotone`) is
    // judged with it UNKNOWN, any other is not judged for the row; either
    // way the rows not judged are said, once, as a WARNING (they were not).
    let normalized = crate::core::rql::check::normalize_expression(&expression);
    let uncertain = crate::core::rql::check::regex_uncertainty_predicate(&normalized);
    let probe = match (&uncertain, regex_calls_are_monotone(&normalized)) {
        (Some(u), false) => format!("SELECT 1 FROM {table_name} WHERE NOT ({normalized}) AND NOT ({u}) LIMIT 1"),
        _ => format!("SELECT 1 FROM {table_name} WHERE NOT ({normalized}) LIMIT 1"),
    };
    let result = if unenforced { Ok(Vec::new()) } else { run_subquery(&probe, database, tx).await };
    if let (Ok(rows), Some(u), false) = (&result, &uncertain, unenforced) {
        if rows.is_empty() {
            if let Ok(found) = run_subquery(&format!("SELECT count(*) FROM {table_name} WHERE {u}"), database, tx).await {
                let n = found
                    .first()
                    .and_then(|r| r.columns.first())
                    .and_then(|c| <[u8; 8]>::try_from(c.data.into_slice()).ok())
                    .map(i64::from_le_bytes)
                    .unwrap_or(0);
                if n > 0 {
                    let message = format!(
                        "check constraint \"{name}\" of relation \"{table_name}\" was not judged for {n} existing \
                         {}: a locale-dependent class or case-insensitivity met a non-ASCII value, where PostgreSQL's \
                         answer depends on the locale",
                        if n == 1 { "row" } else { "rows" }
                    );
                    log::warn!("{message}");
                    crate::core::notice::push("WARNING", "01000", message);
                }
            }
        }
    }
    match result {
        Ok(rows) if rows.is_empty() => {}
        Ok(_) => {
            return ddl_error(&format!(
                "check constraint \"{}\" of relation \"{}\" is violated by some row",
                name, table_name
            ));
        }
        Err(e) => return ddl_error(&format!("could not check the existing rows against \"{}\": {}", name, e)),
    }

    let entry = crate::core::rql::check::CheckConstraint {
        name,
        expression,
        is_deleted: false,
    };
    // §7 step 3: the reload's work, after the rows were checked under the
    // same ACCESS EXCLUSIVE.
    match database
        .reload_with(table_name, |old| async move {
            crate::core::rql::check::append(old.io_schema.clone(), &entry)
                .await
                .map_err(|e| e.to_string())
        })
        .await
    {
        Ok(()) => ddl_ok(0),
        Err(failure) => reload_failed(failure, ""),
    }
}

async fn execute_alter_table_action(
    plan: crate::core::rql::lexer_alter::AlterTable,
    database: &Arc<Database>,
    tx: TxId,
) -> Vec<u8> {
    use crate::core::rql::lexer_alter::AlterAction;
    use crate::core::rql::nullable;

    let (schema, io_schema, row_count) = {
        // ACCESS EXCLUSIVE before the schema is read (§7): every reader and
        // writer of the table has finished, and none starts until this
        // statement ends.
        let table_ref = match database
            .table_for(&plan.table_name, owner(), TableMode::AccessExclusive)
            .await
        {
            Ok(table_ref) => table_ref,
            Err(e) => return ddl_error(&e),
        };
        let Some(table) = table_ref.as_ref().clone() else {
            return ddl_error(&format!("relation '{}' does not exist", plan.table_name));
        };
        // A partitioned table's rows are in its partitions: its length is
        // theirs, and its schema its shell's (docs/partitioning.md §11).
        let row_count = match database.rows_len_held(&plan.table_name) {
            Ok(len) => len,
            Err(e) => return ddl_error(&e.to_string()),
        };
        // A schema change is written to storage and the table handle rebuilt
        // from it, because `Table` owns its schema by value.
        (table.schema.clone(), table.io_schema.clone(), row_count)
    };

    // The rest of the action's complete lock set (§7 step 1, finding L11):
    // the RENAME target, the FK children whose sidecars it rewrites, the
    // parent of a foreign key it adds or drops. The statement took it with
    // its own name, in name order; read again here under this table's
    // ACCESS EXCLUSIVE, which stops a child appearing (adding one takes
    // SHARE on this table), and anything new is taken now, with graph
    // edges, before a single sidecar is written. So a 55P03 or 40P01 here
    // has written nothing.
    if let Err(e) = database.lock_names(&alter_lock_set(&plan, database), owner()).await {
        return ddl_error(&e);
    }
    // This transaction's own row locks on the table, which a RENAME or a new
    // identifying key would orphan (another session's make the DDL wait for
    // its ROW EXCLUSIVE). Before a sidecar is written.
    if database.row_locks.txn_holds_rows_on(&plan.table_name, tx) {
        if let Some(what) = orphans_row_locks(&plan, &schema) {
            return row_locks_in_use(&plan.table_name, what);
        }
    }
    // What a partitioned table cannot do yet, under the lock, before a
    // sidecar is written (docs/partitioning.md §11).
    if let Some(e) = database.partitioned_alter_refusal(&plan) {
        return ddl_error(&e);
    }

    // Every arm ends the statement itself. Since B5 each one's writes are its
    // reload's work (`reload_with`, `reload_atomic[_with]`, `rewrite_rows`),
    // so none writes and then reloads, and nothing falls through to a reload
    // here.
    match plan.action {
        // Logged rather than silent, so a migration that depended on one is
        // traceable afterwards.
        AlterAction::NoOp { reason } => {
            info!(
                "ALTER TABLE {}: accepted without acting ({})",
                plan.table_name, reason
            );
            return ddl_ok(0);
        }
        // A foreign key's rows checked now, as `ADD` would have checked
        // them without `NOT VALID` (23503 for the first orphan, nothing
        // written either way: no record says "not valid", so validating an
        // already-valid key checks again and passes). A CHECK, UNIQUE or
        // primary key was checked when it was added: a no-op. A name no
        // constraint has is 42704, as in PostgreSQL (P4 round 3): reported as
        // done, a migration validating the wrong name (an older binary's
        // `fk_<c>_<p>`, a typo) left a NOT VALID key's orphans unflagged.
        AlterAction::ValidateConstraint { name } => {
            let fk = schema.foreign_keys.iter().find(|fk| !fk.is_deleted && fk.name.eq_ignore_ascii_case(&name)).cloned();
            let Some(fk) = fk else {
                if constraint_name_taken(&schema, &plan.table_name, &name) {
                    info!("ALTER TABLE {}: VALIDATE CONSTRAINT {name}: checked when it was added", plan.table_name);
                    return ddl_ok(0);
                }
                return ddl_error(&format!(
                    "constraint \"{}\" of relation \"{}\" does not exist",
                    name, plan.table_name
                ));
            };
            return match Box::pin(existing_rows_break_foreign_key(database, &plan.table_name, &fk, tx)).await {
                Ok(None) => ddl_ok(0),
                Ok(Some(violation)) => ddl_error(&violation),
                Err(e) => ddl_error(&format!("foreign key \"{}\" could not be validated: {e}", fk.name)),
            };
        }
        AlterAction::AddColumn {
            name,
            db_type,
            nullable: is_nullable,
            if_not_exists,
            default,
            json,
            modifier,
            generated,
            text_typmod,
        } => {
            if nullable::is_hidden(&name) {
                return ddl_error("column names beginning with __null_ are reserved");
            }
            // A dropped column keeps its slot in the row layout but is no
            // longer a column, so it must not block one of the same name.
            let exists = schema
                .fields
                .iter()
                .any(|f| f.name.eq_ignore_ascii_case(&name) && !f.is_deleted);
            if exists {
                return if if_not_exists {
                    ddl_ok(0)
                } else {
                    ddl_error(&format!("column '{}' already exists", name))
                };
            }

            // A row is a fixed-width record, so a column added to a table that
            // already holds rows would be read at an offset those rows never
            // wrote. The rows are rewritten under the new layout instead.
            // A default is validated before anything is written, so a bad
            // expression fails the statement rather than half of it.
            if let Some(expression) = default.as_deref() {
                if let Err(e) = crate::core::rql::column_default::validate(expression, &db_type) {
                    return ddl_error(&e);
                }
                // A JSON column stores a *document*: its fields become columns
                // of a table of its own, so a top-level array or scalar has
                // nothing to map. An `INSERT` of one is already refused; a
                // `DEFAULT` was not, and quietly became `{}` — the same value
                // rejected in one position and silently changed in the other.
                if json {
                    if let Err(e) = json_default_is_an_object(expression) {
                        return ddl_error(&e);
                    }
                }
            }
            // A generated column is computed from its row, as in PostgreSQL:
            // it cannot also have a DEFAULT, and its expression must compile
            // against the table it is being added to.
            if let Some(expression) = generated.as_deref() {
                if default.is_some() {
                    return ddl_error(&format!(
                        "both default and generation expression specified for column \"{}\"",
                        name
                    ));
                }
                let mut fields: smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> =
                    schema.fields.iter().filter(|f| !f.is_deleted).cloned().collect();
                fields.push(crate::core::row::schema::SchemaField::new(
                    name.clone(),
                    db_type.clone(),
                    db_type.get_size(),
                    0,
                    u64::MAX,
                    false,
                ));
                if expression_columns(expression, &fields)
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(&name))
                {
                    return ddl_error(&format!("generation expression of \"{}\" cannot reference itself", name));
                }
                if let Err(reason) = crate::core::rql::generated::parse_expression(expression, &fields) {
                    return ddl_error(&format!(
                        "the generation expression of \"{}\" cannot be compiled: {}",
                        name, reason
                    ));
                }
            }
            let new_modifier = modifier.map(|(precision, scale)| crate::core::rql::type_modifier::TypeModifier {
                column: name.clone(),
                precision,
                scale,
                is_deleted: false,
            });
            let new_generated = generated.clone().map(|expression| crate::core::rql::generated::GeneratedColumn {
                column: name.clone(),
                expression,
                is_deleted: false,
            });

            let fills_existing = default
                .as_deref()
                .map(|e| !e.trim().eq_ignore_ascii_case("NULL"))
                .unwrap_or(false)
                || generated.is_some();

            // A declaration still filed under the name — a dead column's, from
            // before drops retired theirs — is retired first. Left, the new
            // column inherited it: the old DEFAULT, generation expression or
            // NUMERIC scale, a JSON registration, or a composite UNIQUE key,
            // foreign key or CHECK that then bound the new column (a dropped
            // `ON DELETE CASCADE` key deleting rows by its values).
            match crate::core::rql::column_declarations::retire_for_new_column(
                database,
                &plan.table_name,
                &schema,
                &io_schema,
                &name,
            )
            .await
            {
                Ok((wrote, others)) => {
                    for other in others {
                        if let Err(e) = database.reload_table(&other).await {
                            return ddl_error(&e.to_string());
                        }
                    }
                    // The rewrite below reads the open table's declarations.
                    if wrote && row_count > 0 {
                        if let Err(e) = database.reload_table(&plan.table_name).await {
                            return ddl_error(&e.to_string());
                        }
                    }
                }
                Err(e) => return ddl_error(&e),
            }

            if row_count > 0 {
                if !is_nullable && !fills_existing {
                    // The existing rows have no value to put there. PostgreSQL
                    // needs a DEFAULT for exactly this reason.
                    return ddl_error(&format!(
                        "ADD COLUMN {} NOT NULL on '{}' is not supported because it already \
                         holds rows and there is no DEFAULT to fill them with",
                        name, plan.table_name
                    ));
                }

                let mut added = vec![crate::core::rql::lexer_ct::CreateColumnData {
                    name: name.clone(),
                    data_type: db_type,
                }];
                // The companion exists whenever the column can be NULL. A
                // NOT NULL column filled by a default needs none.
                if is_nullable {
                    added.push(crate::core::rql::lexer_ct::CreateColumnData {
                        name: nullable::flag_column_name(&name),
                        data_type: nullable::FLAG_TYPE,
                    });
                }

                let default = match json_default_reference(
                    &database,
                    &plan.table_name,
                    &name,
                    default,
                    json,
                    tx,
                )
                .await
                {
                    Ok(d) => d,
                    Err(e) => return ddl_error(&e),
                };
                let new_defaults = default
                    .map(|expression| {
                        vec![crate::core::rql::column_default::ColumnDefault {
                            column: name.clone(),
                            expression,
                            is_deleted: false,
                        }]
                    })
                    .unwrap_or_default();

                // The rewrite carries the table's existing JSON columns across;
                // a newly added one has to join them.
                let new_json = if json {
                    vec![crate::core::rql::json_map::JsonColumn {
                        column: name.clone(),
                        shadow_table: crate::core::rql::json_map::shadow_table_name(
                            &plan.table_name,
                            &name,
                        ),
                        is_deleted: false,
                    }]
                } else {
                    Vec::new()
                };

                if let Err(e) = Box::pin(rewrite_table_with_columns(
                    &database,
                    &plan.table_name,
                    added,
                    new_defaults,
                    new_json,
                    // With its `character(n)` record, as the in-place add
                    // stages it (`type_modifier::BPCHAR_MARKER`).
                    // A former column's record under the name is retired.
                    new_modifier
                        .into_iter()
                        .chain(crate::core::rql::type_modifier::text_typmod_records(
                            &name,
                            text_typmod,
                            &schema.type_modifiers,
                        ))
                        .collect(),
                    new_generated.into_iter().collect(),
                    tx,
                ))
                .await
                {
                    error!("{}", e);
                    return ddl_error(&e);
                }
                // The rows already there get the column's value the way an
                // UPDATE gives one: computed from each row.
                return match generated {
                    Some(_) => compute_generated_column(&database, &plan.table_name, &name, tx).await,
                    None => ddl_ok(0),
                };
            }

            // A row's `Column::schema_id` is 32 bits, so a write order past
            // that could not be carried by any row. Refused before anything
            // is written, rather than truncated into another column's id.
            let highest_new = schema
                .fields
                .iter()
                .map(|f| f.write_order)
                .max()
                .map_or(0, |m| m.saturating_add(1))
                .saturating_add(is_nullable as u64);
            if let Err(e) = crate::core::row::row::check_write_order(highest_new) {
                return ddl_error(&e);
            }

            // The document a JSON DEFAULT names is stored first, in its own
            // table (a statement of its own there); everything this table
            // records goes in as the reload's work below.
            let default =
                match json_default_reference(&database, &plan.table_name, &name, default, json, tx)
                    .await
                {
                    Ok(d) => d,
                    Err(e) => return ddl_error(&e),
                };
            let table_name = plan.table_name.clone();
            // Every write of this arm as one reload's work, all or nothing
            // (B5): the dead names retired, the DEFAULT, NUMERIC scale and
            // generation expression, the field and its companion, the JSON
            // registration. They were separately durable writes, and a
            // failure between two left a declaration filed under a column
            // that did not exist, which the next column of the name took.
            return match database
                .reload_atomic(&plan.table_name, |old| async move {
                    let io_schema = old.io_schema.clone();
                    let mut schema = schema;
                    // A dropped column still holding this name — or its
                    // companion's — is retired first, exactly as `RENAME
                    // COLUMN` does. `add_field` skips a name the schema already
                    // holds, tombstones included, so `DROP COLUMN a` then `ADD
                    // COLUMN a` on an empty table reported success and added
                    // nothing: every later reference to `a` failed.
                    for taken in [name.clone(), nullable::flag_column_name(&name)] {
                        retire_tombstoned_name(&mut schema, &taken, io_schema.clone()).await?;
                    }
                    // The column's sidecar records, staged and committed as
                    // one transaction at the end (`torn_tail::SidecarWrites`).
                    let mut writes = crate::core::torn_tail::SidecarWrites::default();
                    if let Some(expression) = default {
                        let entry = crate::core::rql::column_default::ColumnDefault {
                            column: name.clone(),
                            expression,
                            is_deleted: false,
                        };
                        crate::core::rql::column_default::stage(&*io_schema, &mut writes, &entry)
                            .map_err(|e| e.to_string())?;
                    }
                    if let Some(entry) = &new_modifier {
                        crate::core::rql::type_modifier::stage(&*io_schema, &mut writes, entry)
                            .map_err(|e| e.to_string())?;
                    }
                    // Its declared text length (`type_modifier::TextTypmod`),
                    // and a former column's marker under the name retired.
                    for record in crate::core::rql::type_modifier::text_typmod_records(
                        &name,
                        text_typmod,
                        &schema.type_modifiers,
                    ) {
                        crate::core::rql::type_modifier::stage(&*io_schema, &mut writes, &record)
                            .map_err(|e| e.to_string())?;
                        schema.type_modifiers.retain(|m| !m.column.eq_ignore_ascii_case(&record.column));
                        if !record.is_deleted {
                            schema.type_modifiers.push(record);
                        }
                    }
                    if let Some(entry) = &new_generated {
                        crate::core::rql::generated::stage(&*io_schema, &mut writes, entry)
                            .map_err(|e| e.to_string())?;
                    }
                    // A field record that cannot be written fails the change,
                    // which puts the schema files back (`reload_atomic`).
                    schema
                        .try_add_field(io_schema.clone(), name.clone(), db_type, false)
                        .await
                        .map_err(|e| e.to_string())?;
                    if is_nullable {
                        schema
                            .try_add_field(
                                io_schema.clone(),
                                nullable::flag_column_name(&name),
                                nullable::FLAG_TYPE,
                                false,
                            )
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                    // A JSON column is a `UUID` plus a table of its own, and
                    // the sidecar entry is what connects the two. Registered
                    // here exactly as `Database::add_table` does for `CREATE
                    // TABLE`, so the two statements produce the same thing.
                    if json {
                        let entry = crate::core::rql::json_map::JsonColumn {
                            column: name.clone(),
                            shadow_table: crate::core::rql::json_map::shadow_table_name(&table_name, &name),
                            is_deleted: false,
                        };
                        crate::core::rql::json_map::stage(&*io_schema, &mut writes, &entry)
                            .map_err(|e| e.to_string())?;
                    }
                    writes.commit(&*io_schema).await.map_err(|e| e.to_string())?;
                    Ok(())
                })
                .await
            {
                Ok(()) => ddl_ok(0),
                Err(failure) => reload_failed(failure, ""),
            };
        }
        AlterAction::DropColumn { name, if_exists } => {
            // The live column of the name: a dropped one holding it is not a
            // column, and re-tombstoning it left the live column standing.
            let field = match schema
                .fields
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(&name) && !f.is_deleted)
            {
                Some(field) if !nullable::is_hidden(&name) => field.clone(),
                _ => {
                    return if if_exists {
                        ddl_ok(0)
                    } else {
                        ddl_error(&format!("column '{}' does not exist", name))
                    };
                }
            };

            // The partial unique indexes, the column, its companion and its
            // declarations go as the reload's work, all or nothing (B5): a
            // failure part-way leaves the column and its indexes as they were.
            let dropped_keys = crate::core::rql::unique_key::dropped_with_column(
                &schema.unique_keys,
                &schema.fields,
                &field.name,
            );
            let table_name = plan.table_name.clone();
            // The FK children whose keys reference the column: their sidecars
            // change with it, so they are held aside with it (one intent).
            let children = database.fk_children_on(&plan.table_name, Some(&[name.as_str()]));
            let others = match database
                .reload_atomic_with(&plan.table_name, children, |old| async move {
                    // A partial unique index whose predicate reads the column
                    // goes with it, as in PostgreSQL.
                    for key in dropped_keys {
                        crate::core::rql::unique_key::remove(old.io_schema.clone(), &key)
                            .await
                            .map_err(|e| e.to_string())?;
                        info!("index {} dropped with column {}", key, field.name);
                    }
                    // The column keeps its slot in the row layout and is marked
                    // deleted: reclaiming the bytes would move every column
                    // after it, which is the same rewrite `ADD COLUMN` needs.
                    // Its companion and every declaration filed under its name
                    // go with it, in one log transaction. Left behind, they
                    // were picked up by the next column to take the name — the
                    // retype dance's renamed temporary, or a plain re-`ADD` —
                    // which then defaulted to the dropped column's value, or
                    // was bound by its foreign key, UNIQUE key or CHECK.
                    crate::core::rql::column_declarations::drop_column(
                        database,
                        &table_name,
                        &schema,
                        &old.io_schema,
                        &name,
                    )
                    .await
                })
                .await
            {
                Ok(others) => others,
                Err(failure) => return reload_failed(failure, ""),
            };
            // FK children whose sidecars named the column, reopened after it.
            for other in others {
                if let Err(e) = database.reload_table(&other).await {
                    return ddl_error(&e.to_string());
                }
            }
            return ddl_ok(0);

        }
        AlterAction::RenameColumn { from, to } => {
            if nullable::is_hidden(&to) {
                return ddl_error("column names beginning with __null_ are reserved");
            }
            // A dropped column keeps its slot in the row layout and is only
            // tombstoned, so both checks must ignore tombstones — as
            // `ADD COLUMN` already does. Otherwise the standard retype dance
            // (add temp, copy, drop original, rename temp to the original name)
            // failed on the rename with "column already exists", naming a
            // column that had just been dropped.
            if !schema
                .fields
                .iter()
                .any(|f| f.name.eq_ignore_ascii_case(&from) && !f.is_deleted)
            {
                return ddl_error(&format!("column '{}' does not exist", from));
            }
            if schema
                .fields
                .iter()
                .any(|f| f.name.eq_ignore_ascii_case(&to) && !f.is_deleted)
            {
                return ddl_error(&format!("column '{}' already exists", to));
            }

            // One transaction: the field and its NULL companion, the column's
            // DEFAULT, generation expression, NUMERIC scale and JSON
            // registration, and every UNIQUE key, foreign key (either side),
            // CHECK, generation expression, path index and the PRIMARY KEY
            // header that mention it. A dropped column still holding `to` gives
            // the name up, and whatever is still filed under `to` is retired
            // rather than inherited. Renaming touches no row bytes: the layout
            // is keyed by write order.
            //
            // Each of these used to be its own durable write. A power cut
            // between two left the field renamed without its companion (every
            // NULL read as 0) or constraints naming a column that no longer
            // existed, skipped as "not enforced" for good.
            //
            // Run as the reload's work, all or nothing with the reload (B5).
            let table_name = plan.table_name.clone();
            // The FK children whose keys name either column: their sidecars
            // change with it, so they are held aside with it (one intent).
            let children = database.fk_children_on(&plan.table_name, Some(&[from.as_str(), to.as_str()]));
            let others = match database
                .reload_atomic_with(&plan.table_name, children, |old| async move {
                    let others = crate::core::rql::column_declarations::rename_column(
                        database,
                        &table_name,
                        &schema,
                        &old.io_schema,
                        &from,
                        &to,
                    )
                    .await?;
                    crate::core::rql::column_declarations::crash_points::reached(&table_name, "rename.after_commit");
                    Ok(others)
                })
                .await
            {
                Ok(others) => others,
                Err(failure) => return reload_failed(failure, ""),
            };
            // A table whose foreign key named the renamed parent column.
            for other in others {
                if let Err(e) = database.reload_table(&other).await {
                    return ddl_error(&e.to_string());
                }
            }
            return ddl_ok(0);
        }
        AlterAction::RenameTable { to } => {
            // Used to parse as a *column* rename and come back as "RENAME
            // COLUMN must be followed by TO" — a misleading message about a
            // statement that mentions no column. Then it was refused honestly.
            // Now it is carried out: the name is part of the on-disk layout, so
            // this moves files and re-keys the catalogue rather than editing a
            // record, and `Database::rename_table` owns that ordering.
            //
            // Its `SERIAL` sequences follow it, as `<new>_<column>_seq`. Left
            // reading the old name, a new table created under the old name
            // generated the same one and the two tables shared a counter.
            let followed = crate::core::rql::sequence::follow_rename(
                &plan.table_name,
                &to,
                &schema.defaults,
                &sequences_named_elsewhere(database, &plan.table_name),
            );
            if let Err(e) = database.rename_table(&plan.table_name, &to).await {
                return ddl_error(&format!(
                    "could not rename {} to {}: {}",
                    plan.table_name, to, e
                ));
            }
            // SERIAL floor marks are by table name: the new name's are a
            // dropped table's, whose rows are not this table's, and the old
            // name's would let a table created under it skip its seed.
            crate::core::rql::sequence::forget_table_floors(database.id, &plan.table_name);
            crate::core::rql::sequence::forget_table_floors(database.id, &to);
            // Every foreign key naming the table as its parent — the
            // children's, and its own self-references, which moved with its
            // files — names the new one. Left naming the old name, a delete
            // from the renamed parent found no referencing key and orphaned
            // the children, and a table later created under the old name
            // became their parent: checked against, and cascaded from.
            match crate::core::rql::column_declarations::retarget_parent(
                database,
                &plan.table_name,
                &to,
            )
            .await
            {
                Ok(changed) => {
                    for table in changed {
                        if let Err(e) = database.reload_table(&table).await {
                            return ddl_error(&e.to_string());
                        }
                    }
                }
                Err(e) => {
                    return ddl_error(&format!(
                        "renamed {} to {}, but {}",
                        plan.table_name, to, e
                    ));
                }
            }
            if followed.is_empty() {
                return ddl_ok(0);
            }
            // The renamed table under its new name, ACCESS EXCLUSIVE too.
            if let Err(e) = database.table_for(&to, owner(), TableMode::AccessExclusive).await {
                return ddl_error(&e);
            }
            // Every default that follows, as one reload's work (B5): all of
            // them or none. The counters move only once they are durable.
            let defaults: Vec<_> = followed.iter().map(|(entry, _, _)| entry.clone()).collect();
            if let Err(failure) = database
                .reload_atomic(&to, |renamed| async move {
                    // One transaction for all of them (`torn_tail::SidecarWrites`).
                    let mut writes = crate::core::torn_tail::SidecarWrites::default();
                    for entry in &defaults {
                        crate::core::rql::column_default::stage(&*renamed.io_schema, &mut writes, entry)
                            .map_err(|e| format!("sequence default for {} could not follow: {}", entry.column, e))?;
                    }
                    writes
                        .commit(&*renamed.io_schema)
                        .await
                        .map_err(|e| format!("the sequence defaults could not follow: {e}"))?;
                    Ok(())
                })
                .await
            {
                return reload_failed(failure, "renamed, but its sequences could not follow");
            }
            for (_, from, to) in &followed {
                crate::core::rql::sequence::rename(database.id, from, to);
            }
            return ddl_ok(0);
        }
        AlterAction::ConstraintClause { drop, description } => {
            use crate::core::rql::foreign_key;

            // `DROP CONSTRAINT [IF EXISTS] <name>`. Both sidecars are tried,
            // since the name alone does not say which kind it was. A name
            // matching neither is PostgreSQL's 42704, unless `IF EXISTS`
            // makes it a no-op: reported as done, a key a client meant to drop
            // went on refusing rows.
            if drop {
                let (dropped, if_exists) = match crate::core::rql::unique_key::dropped_constraint(&description) {
                    Ok((name, if_exists)) => (Some(name), if_exists),
                    Err(e) => return ddl_error(&e),
                };
                if let Some(name) = dropped.clone() {
                    // What the name matches, read from the open schema (the
                    // sidecars' loaded copy, which nothing changes while this
                    // statement holds ACCESS EXCLUSIVE): a name matching
                    // nothing reloads nothing.
                    let named = |n: &str| n.eq_ignore_ascii_case(&name);
                    let stored = schema.foreign_keys.iter().any(|fk| !fk.is_deleted && named(&fk.name))
                        || schema.unique_keys.iter().any(|k| !k.is_deleted && named(&k.name))
                        || schema.checks.iter().any(|c| !c.is_deleted && named(&c.name));
                    // A column declared `UNIQUE` inline is a flag on the field,
                    // not a named sidecar, so no name could ever match it —
                    // and `DROP CONSTRAINT <table>_<column>_key`, which is what
                    // PostgreSQL calls it and what every migration written
                    // against PostgreSQL asks for, silently did nothing. The
                    // constraint then outlived the migration meant to replace
                    // it and rejected rows the new one allows.
                    // The primary key answers to its own name only
                    // (`primary_key_constraint`): an inline one's flag is not
                    // also `<table>_<column>_key`, which PostgreSQL never
                    // calls it, and a key added as `k1` is not `<table>_pkey`.
                    let pk = primary_key_constraint(&schema, &plan.table_name);
                    let pk_named = pk.as_ref().is_some_and(|p| named(&p.name));
                    let implicit = implicit_unique_column(&name, &plan.table_name, &schema.fields).filter(|column| {
                        !pk.as_ref().is_some_and(|p| p.inline && p.column.as_deref().is_some_and(|c| c.eq_ignore_ascii_case(column)))
                    });
                    // An inline primary key: the header's column and its
                    // UNIQUE flag, no sidecar.
                    let primary = pk.as_ref().filter(|p| p.inline && pk_named).and_then(|p| p.column.clone());
                    // The header's column stops being the primary key with
                    // its constraint. A table-level `PRIMARY KEY (id)` is a
                    // sidecar, removed above, and the header used to stay, so
                    // the catalogs and the row locks went on calling a column
                    // the key that nothing kept unique.
                    let header_goes = pk_named && pk.as_ref().is_some_and(|p| p.column.is_some());
                    if stored || implicit.is_some() || primary.is_some() || header_goes {
                        // §7 step 3: the removals are the reload's work. Both
                        // sidecars are tried, since the name alone does not say
                        // which kind it was; the sidecars are the source of
                        // truth, and the reload picks the removal up.
                        let mut schema = schema.clone();
                        let reloaded = database
                            .reload_with(&plan.table_name, |old| async move {
                                let io_schema = old.io_schema.clone();
                                let removed = foreign_key::remove(io_schema.clone(), &name)
                                    .await
                                    .map_err(|e| e.to_string())?;
                                let removed = crate::core::rql::unique_key::remove(io_schema.clone(), &name)
                                    .await
                                    .map_err(|e| e.to_string())?
                                    || removed;
                                // A CHECK is retired by a tombstone under its
                                // name; the last record of a name wins when the
                                // sidecar is read.
                                let check = schema
                                    .checks
                                    .iter()
                                    .find(|c| !c.is_deleted && c.name.eq_ignore_ascii_case(&name))
                                    .cloned();
                                let removed = match check {
                                    Some(check) => {
                                        let tombstone = crate::core::rql::check::CheckConstraint {
                                            is_deleted: true,
                                            ..check
                                        };
                                        crate::core::rql::check::append(io_schema.clone(), &tombstone)
                                            .await
                                            .map_err(|e| e.to_string())?;
                                        true
                                    }
                                    None => removed,
                                };
                                if !removed && let Some(column) = primary {
                                    // The column keeps NOT NULL, as in
                                    // PostgreSQL; its uniqueness was the key's.
                                    schema.primary_key = None;
                                    schema.rewrite_header(io_schema.clone()).await.map_err(|e| e.to_string())?;
                                    schema.set_field_unique(&column, false, io_schema).await;
                                } else {
                                    if !removed && let Some(column) = implicit {
                                        schema.set_field_unique(&column, false, io_schema.clone()).await;
                                    }
                                    if header_goes {
                                        schema.primary_key = None;
                                        schema.rewrite_header(io_schema).await.map_err(|e| e.to_string())?;
                                    }
                                }
                                Ok(())
                            })
                            .await;
                        return match reloaded {
                            Ok(()) => ddl_ok(0),
                            Err(failure) => reload_failed(failure, ""),
                        };
                    }
                }

                if if_exists {
                    info!(
                        "ALTER TABLE {}: no stored constraint matched ({}); IF EXISTS makes that a no-op",
                        plan.table_name, description
                    );
                    return ddl_ok(0);
                }
                return ddl_error(&format!(
                    "constraint \"{}\" of relation \"{}\" does not exist",
                    dropped.unwrap_or_default(),
                    plan.table_name
                ));
            }

            // A foreign key is enforced, so it is stored rather than noted, on
            // a plain or a partitioned table, onto either (phase 3 P4). By
            // whole words outside quotes: as substrings, a constraint named
            // `user_preferences_pkey` (pg_dump's spelling) or `"prefs_x_key"`
            // was read as a foreign key and failed to parse as one.
            let keyword = |word: &str| crate::core::rql::join::find_keyword(&description, word, 0).is_some();
            if keyword("FOREIGN") || keyword("REFERENCES") {
                return Box::pin(add_foreign_key(database, &plan.table_name, &schema, &description, tx)).await;
            }

            // `UNIQUE (…)` / `PRIMARY KEY (…)`, of any arity. Enforced, so it
            // is stored rather than noted — and validated against the rows
            // already there, since a constraint the existing data breaks was
            // never true of this table.
            // `CREATE UNIQUE INDEX IF NOT EXISTS` becomes this statement, and
            // the tolerance has to survive the rewrite: with it, a constraint
            // that is already there is success rather than an error.
            // Taken out in front or after `CONSTRAINT`, where the rewrite
            // puts it (`unique_key::strip_if_not_exists`).
            let (description, tolerate_existing) = crate::core::rql::unique_key::strip_if_not_exists(&description);

            match crate::core::rql::unique_key::parse_clause(&description, &plan.table_name) {
                Err(e) => return ddl_error(&e),
                Ok(Some(key)) => {
                    for column in &key.columns {
                        if !schema
                            .fields
                            .iter()
                            .any(|f| f.name.eq_ignore_ascii_case(column) && !f.is_deleted)
                        {
                            return ddl_error(&format!("column '{}' does not exist", column));
                        }
                    }
                    // A partial unique index's predicate has to be one the
                    // uniqueness check can judge. One it cannot is refused
                    // here, rather than stored as a key nobody enforces.
                    if let Err(e) = key.condition(&schema.fields) {
                        return ddl_error(&e);
                    }
                    // `PRIMARY KEY` makes this key the table's, whatever its
                    // arity, as in PostgreSQL: a second one is refused
                    // (42P16) before its name is looked at.
                    let primary = crate::core::rql::unique_key::is_primary_key_clause(&description);
                    if primary && has_primary_key(&schema, &plan.table_name) {
                        return ddl_error(&format!(
                            "multiple primary keys for table \"{}\" are not allowed",
                            plan.table_name
                        ));
                    }
                    // A composite primary key is recognised by its name,
                    // `<table>_pkey`, alone: the sidecar has no field saying
                    // "primary", so one named otherwise would read as a
                    // UNIQUE, and a second PRIMARY KEY would be accepted
                    // beside it. Refused rather than taken for what it is not.
                    if primary
                        && key.columns.len() > 1
                        && !key.name.eq_ignore_ascii_case(&crate::core::rql::unique_key::pkey_name(&plan.table_name))
                    {
                        return ddl_error(&format!(
                            "a composite PRIMARY KEY named \"{}\" is not supported: a key over several columns is \
                             recognised as the primary key of \"{}\" only by the name \"{}\"; name it that or \
                             leave it unnamed",
                            key.name,
                            plan.table_name,
                            crate::core::rql::unique_key::pkey_name(&plan.table_name)
                        ));
                    }
                    // An unnamed UNIQUE whose derived name is taken gets the
                    // next free one, as PostgreSQL's `t_a_key1`, rather than
                    // an error; a named one keeps its "already exists".
                    let mut key = key;
                    let unnamed = matches!(crate::core::rql::unique_key::split_constraint_name(&description), Ok((None, _)));
                    if unnamed && !primary && constraint_name_taken(&schema, &plan.table_name, &key.name) {
                        let stem = crate::core::rql::unique_key::key_stem(&plan.table_name, &key.columns);
                        key.name = match crate::core::rql::unique_key::choose_name(&stem, "_key", |candidate| {
                            constraint_name_taken(&schema, &plan.table_name, candidate)
                        }) {
                            Ok(name) => name,
                            Err(e) => return ddl_error(&e),
                        };
                    }
                    if schema
                        .unique_keys
                        .iter()
                        .any(|k| k.name.eq_ignore_ascii_case(&key.name))
                    {
                        if tolerate_existing {
                            info!(
                                "Constraint '{}' already exists on '{}'; IF NOT EXISTS makes \
                                 that success",
                                key.name, plan.table_name
                            );
                            return ddl_ok(0);
                        }
                        return ddl_error(&format!(
                            "constraint '{}' already exists on '{}'",
                            key.name, plan.table_name
                        ));
                    }

                    match existing_rows_break_unique(database, &plan.table_name, &key).await {
                        Err(e) => return ddl_error(&e),
                        Ok(Some(conflict)) => {
                            return ddl_error(&format!(
                                "could not add UNIQUE constraint {}: {}",
                                key.describe(),
                                conflict
                            ));
                        }
                        Ok(None) => {}
                    }

                    if primary {
                        return add_primary_key(&plan.table_name, schema, key, database, tx).await;
                    }

                    // §7 step 3: the declaration is the reload's work. The
                    // sidecar is the source of truth; reloading picks it up,
                    // which is also what arms the write lock for it.
                    return match database
                        .reload_with(&plan.table_name, |old| async move {
                            crate::core::rql::unique_key::append(old.io_schema.clone(), &key)
                                .await
                                .map_err(|e| e.to_string())
                        })
                        .await
                    {
                        Ok(()) => ddl_ok(0),
                        Err(failure) => reload_failed(failure, ""),
                    };
                }
                Ok(None) => {}
            }

            // `ADD [CONSTRAINT name] CHECK (…)`, enforced from now on as one
            // declared in `CREATE TABLE` is. It used to fall through to the
            // warning below: accepted, stored nowhere, and a violating insert
            // then succeeded.
            // Read after the constraint's name: a quoted name holding the
            // word (`"my check"`) is not the clause.
            let after_name = match crate::core::rql::unique_key::split_constraint_name(&description) {
                Ok((_, rest)) => rest.to_string(),
                Err(e) => return ddl_error(&e),
            };
            let (_, check_expression) = crate::core::rql::check::split_clause(&after_name);
            if let Some(expression) = check_expression {
                return add_check_constraint(&plan.table_name, &schema, &description, expression, database, tx)
                    .await;
            }

            // A UNIQUE or PRIMARY KEY that was not read as one is an error:
            // reported as done, it enforced nothing, and a duplicate went in.
            // Read after the constraint's name, which may be any word.
            let after_name = after_name.to_ascii_uppercase();
            if after_name.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).any(|w| w == "UNIQUE" || w == "PRIMARY") {
                return ddl_error(&format!(
                    "the constraint `{}` on '{}' could not be read as a UNIQUE or PRIMARY KEY, and an unenforced \
                     one is not supported",
                    description, plan.table_name
                ));
            }
            // Anything left — `EXCLUDE` — is accepted so a migration can
            // proceed, and reported as unenforced rather than as done.
            warn!(
                "ALTER TABLE {}: constraint accepted but not enforced ({})",
                plan.table_name, description
            );
            return ddl_ok(0);
        }
        AlterAction::SetDefault { name, expression } => {
            if !schema
                .fields
                .iter()
                .any(|f| f.name.eq_ignore_ascii_case(&name))
            {
                return ddl_error(&format!("column '{}' does not exist", name));
            }
            let entry = crate::core::rql::column_default::ColumnDefault {
                column: name,
                // Dropping a default is recorded as a tombstone, so the newest
                // record still decides and the file keeps its stride.
                expression: expression.clone().unwrap_or_default(),
                is_deleted: expression.is_none(),
            };
            // §7 step 3: the reload's work.
            return match database
                .reload_with(&plan.table_name, |old| async move {
                    crate::core::rql::column_default::append(old.io_schema.clone(), &entry)
                        .await
                        .map_err(|e| e.to_string())
                })
                .await
            {
                Ok(()) => ddl_ok(0),
                Err(failure) => reload_failed(failure, ""),
            };
        }
        AlterAction::AlterColumn { name, detail } => {
            let upper = detail.to_ascii_uppercase();
            let words: Vec<&str> = detail.split_whitespace().collect();
            let first = words
                .first()
                .map(|w| w.to_ascii_uppercase())
                .unwrap_or_default();

            let Some(field) = schema
                .fields
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(&name) && !f.is_deleted)
                .cloned()
            else {
                return ddl_error(&format!("column '{}' does not exist", name));
            };

            // `TYPE t`. sea-orm's `modify_column` restates the whole column
            // definition even when only nullability moves, so the type it names
            // is usually the one the column already has — a statement that asks
            // for no change, and used to be refused as unsupported.
            if first == "TYPE" {
                // `USING (expr)` transforms every stored value. Nothing here
                // runs it, so it must not fall into the same-type no-op below:
                // `TYPE int USING (v * 100)` on an int column would report
                // success with every row untransformed.
                if words.iter().any(|w| w.eq_ignore_ascii_case("USING")) {
                    return ddl_error(&format!(
                        "ALTER COLUMN {} TYPE ... USING is not supported yet;                          rewrite via an added column and an UPDATE instead",
                        name
                    ));
                }
                let type_tokens: Vec<&str> = words[1..].to_vec();
                return match crate::core::rql::lexer_alter::parse_column_type(&type_tokens) {
                    // `timestamp` to `timestamptz` or back: the same bytes,
                    // so only the field's flag changes (no row is rewritten).
                    Ok((db_type, _))
                        if field.db_type == crate::core::db_type::DbType::DATETIME
                            && matches!(
                                db_type,
                                crate::core::db_type::DbType::DATETIME | crate::core::db_type::DbType::TIMESTAMP
                            )
                            && db_type != field.literal_type() =>
                    {
                        // A DEFAULT or CHECK of the column holding a zoned
                        // literal (`'… 09:00+05'`) is re-read under the new
                        // type and would mean another value; PostgreSQL keeps
                        // the value it stored. Refused, rather than silently
                        // changed, until it is restated without a zone.
                        let mentions = |text: &str| {
                            expression_columns(text, &schema.fields).iter().any(|c| c.eq_ignore_ascii_case(&field.name))
                        };
                        let zoned_default = crate::core::rql::column_default::expression_for(&field.name, &schema.defaults)
                            .filter(|e| crate::core::sql_scan::has_zoned_timestamp_literal(e));
                        let zoned_check = schema
                            .checks
                            .iter()
                            .find(|c| !c.is_deleted && mentions(&c.expression) && crate::core::sql_scan::has_zoned_timestamp_literal(&c.expression));
                        if let Some(text) = zoned_default.map(str::to_string).or_else(|| zoned_check.map(|c| c.expression.clone())) {
                            return ddl_error(&format!(
                                "ALTER COLUMN {} TYPE cannot change between timestamp and timestamptz while its \
                                 DEFAULT or a CHECK ({}) holds a literal with a time zone, which the new type \
                                 would read as another value; restate it without the zone first (not supported)",
                                field.name, text
                            ));
                        }
                        // A generated column reading this one: PostgreSQL
                        // refuses to alter the type of a column a generated
                        // column uses, whatever the expression holds.
                        if let Some(g) = schema.generated.iter().find(|g| !g.is_deleted && mentions(&g.expression)) {
                            return ddl_error(&format!(
                                "cannot alter type of a column used by a generated column: column \"{}\" is used \
                                 by generated column \"{}\" (not supported)",
                                field.name, g.column
                            ));
                        }
                        let without_time_zone = db_type == crate::core::db_type::DbType::TIMESTAMP;
                        let table_name = plan.table_name.clone();
                        let column = field.name.clone();
                        match database
                            .reload_atomic(&plan.table_name, |old| async move {
                                crate::core::rql::column_declarations::set_time_zone(
                                    &table_name,
                                    &old.io_schema,
                                    &column,
                                    without_time_zone,
                                )
                                .await
                                .map(|_| ())
                            })
                            .await
                        {
                            Ok(()) => ddl_ok(0),
                            Err(failure) => reload_failed(failure, ""),
                        }
                    }
                    // `literal_type`: a `timestamp` column restated as
                    // `timestamp` asks for no change.
                    Ok((db_type, _)) if db_type == field.literal_type() => {
                        let numeric = match numeric_modifier_change(&schema, &name, &type_tokens.join(" "), row_count) {
                            Ok(entry) => entry,
                            Err(e) => return ddl_error(&e),
                        };
                        // `character(n)`, `character varying(n)` and text are one
                        // storage type here. What moves is the column's declared
                        // length (`type_modifier::TextTypmod`), and the stored
                        // values where the new type holds them otherwise: held
                        // to the length (22001), and a `character(n)` value
                        // without its trailing spaces (23505 when two keys
                        // become one), as PostgreSQL's rewrite does.
                        let markers = match text_type_change(&database, &plan.table_name, &schema, &field, &type_tokens.join(" "), row_count, tx).await {
                            Ok(markers) => markers,
                            Err(e) => return ddl_error(&e),
                        };
                        if numeric.is_none() && markers.is_empty() {
                            return ddl_ok(0);
                        }
                        // The reload's work, all or nothing (B5): one sidecar
                        // transaction. The checks reading the column are left
                        // as they are: a `character(n)` column is compared as
                        // PostgreSQL compares it, so they stay enforced.
                        match database
                            .reload_atomic(&plan.table_name, |old| async move {
                                let io = old.io_schema.clone();
                                let mut writes = crate::core::torn_tail::SidecarWrites::default();
                                for entry in numeric.iter().chain(markers.iter()) {
                                    crate::core::rql::type_modifier::stage(&*io, &mut writes, entry)
                                        .map_err(|e| e.to_string())?;
                                }
                                writes.commit(&*io).await.map_err(|e| e.to_string())
                            })
                            .await
                        {
                            Ok(()) => ddl_ok(0),
                            Err(failure) => reload_failed(failure, ""),
                        }
                    }
                    // A real change still needs every row rewritten under the
                    // new width, which is the restriction `ADD COLUMN` names.
                    Ok((db_type, _)) => ddl_error(&format!(
                        "ALTER COLUMN {} TYPE {:?} would rewrite every row from {:?}, which is not supported yet",
                        name, db_type, field.db_type
                    )),
                    Err(e) => ddl_error(&e),
                };
            }

            if upper.contains("NOT NULL") {
                let currently_nullable = nullable::is_nullable(&name, &schema.fields);
                if first == "DROP" {
                    if currently_nullable {
                        return ddl_ok(0);
                    }
                    // A partitioned table here holds no rows (a populated one
                    // was refused above): the companion is a metadata change,
                    // as ADD COLUMN on an empty table adds one.
                    if database.is_partitioned(&plan.table_name) {
                        let flag = nullable::flag_column_name(&name);
                        return match database
                            .reload_atomic(&plan.table_name, |old| async move {
                                let mut schema = schema;
                                retire_tombstoned_name(&mut schema, &flag, old.io_schema.clone()).await?;
                                schema
                                    .try_add_field(old.io_schema.clone(), flag, nullable::FLAG_TYPE, false)
                                    .await
                                    .map_err(|e| e.to_string())?;
                                Ok(())
                            })
                            .await
                        {
                            Ok(()) => ddl_ok(0),
                            Err(failure) => reload_failed(failure, ""),
                        };
                    }
                    // Making a column nullable means giving it a `__null_`
                    // companion, which is an ordinary fixed-width column — so
                    // the same rewrite `ADD COLUMN` uses does the work.
                    //
                    // This was tried once and reverted, because it appeared to
                    // lose every existing value: the schema came out right —
                    // `id`, `v`, `__null_v` at the correct offsets — but
                    // `SELECT id FROM t WHERE v = 100` came back empty on rows
                    // that held 100.
                    //
                    // The bytes were never lost. The rewrite fills a new
                    // column's companion by asking whether the *tracked column*
                    // has a `DEFAULT`, which is the right question when both are
                    // new — `ADD COLUMN v TEXT` leaves existing rows with no
                    // value for `v`, so the companion should say NULL. Here only
                    // the companion is new and every row already has a value, so
                    // that answer marked the whole column NULL while the data sat
                    // intact underneath. Worse than losing it: nothing looked
                    // wrong.
                    //
                    // The fill now asks the *row* whether it carries a value for
                    // the tracked column rather than asking the schema about
                    // defaults. See `rewrite_table_with_columns`.
                    let companion = crate::core::rql::lexer_ct::CreateColumnData {
                        name: nullable::flag_column_name(&name),
                        data_type: nullable::FLAG_TYPE,
                    };
                    return match Box::pin(rewrite_table_with_columns(
                        &database,
                        &plan.table_name,
                        vec![companion],
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                        tx,
                    ))
                    .await
                    {
                        Ok(()) => ddl_ok(0),
                        Err(e) => {
                            error!("{}", e);
                            ddl_error(&e)
                        }
                    };
                }

                // SET NOT NULL.
                if !currently_nullable {
                    return ddl_ok(0);
                }

                // PostgreSQL refuses if any row would violate the new
                // constraint, and so does this: accepting it while leaving
                // nulls in place would make the schema claim something the
                // data does not honour.
                if row_count > 0 {
                    let probe = format!(
                        "SELECT {} FROM {} WHERE {} IS NULL",
                        name, plan.table_name, name
                    );
                    match run_subquery(&probe, database, tx).await {
                        Ok(rows) if !rows.is_empty() => {
                            return ddl_error(&format!(
                                "column \"{}\" of relation \"{}\" contains null values",
                                name, plan.table_name
                            ));
                        }
                        Ok(_) => {}
                        Err(e) => return ddl_error(&e),
                    }
                }

                // Nullability is carried by the companion `__null_` column, so
                // removing it is what makes the column NOT NULL. Tombstoned
                // rather than reclaimed, exactly as `DROP COLUMN` does, because
                // moving the columns after it is the rewrite being avoided.
                let flag_name = nullable::flag_field(&name, &schema.fields).map(|flag| flag.name.clone());
                // The reload's work, all or nothing (B5).
                return match database
                    .reload_atomic(&plan.table_name, |old| async move {
                        if let Some(flag_name) = flag_name {
                            let mut schema = schema;
                            schema.mark_field_as_deleted(&flag_name, old.io_schema.clone()).await;
                        }
                        Ok(())
                    })
                    .await
                {
                    Ok(()) => ddl_ok(0),
                    Err(failure) => reload_failed(failure, ""),
                };
            } else {
                warn!(
                    "ALTER TABLE {}: ALTER COLUMN {} {} ignored",
                    plan.table_name, name, detail
                );
                return ddl_ok(0);
            }
        }
    }
}

/// `ADD [CONSTRAINT n] PRIMARY KEY (…)`, its key already checked against the
/// rows: its columns made NOT NULL (refused, as PostgreSQL refuses it, when
/// one holds a NULL), the key stored, and a one-column key recorded in the
/// header as `CREATE TABLE` records it, so the catalogs and the row locks
/// call it the primary key. It used to be stored as a UNIQUE only: after a
/// DROP took the header with it, the table had no primary key. A composite
/// key is the sidecar named `<table>_pkey`, as at CREATE; the header holds
/// one column, so a composite one under another name is not recognised as
/// the primary key (no format change made here).
async fn add_primary_key(
    table_name: &str,
    schema: crate::core::row::schema::TableSchema,
    key: crate::core::rql::unique_key::UniqueKey,
    database: &Arc<Database>,
    tx: TxId,
) -> Vec<u8> {
    use crate::core::rql::nullable;
    let mut columns = Vec::new();
    for column in &key.columns {
        let Some(field) = schema.fields.iter().find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column)) else {
            return ddl_error(&format!("column '{}' does not exist", column));
        };
        columns.push(field.name.clone());
    }
    let mut flags = Vec::new();
    for column in &columns {
        let Some(flag) = nullable::flag_field(column, &schema.fields) else {
            continue;
        };
        let probe = format!("SELECT 1 FROM {} WHERE {} IS NULL LIMIT 1", table_name, column);
        match run_subquery(&probe, database, tx).await {
            Ok(rows) if rows.is_empty() => flags.push(flag.name.clone()),
            Ok(_) => {
                return ddl_error(&format!(
                    "column \"{}\" of relation \"{}\" contains null values",
                    column, table_name
                ));
            }
            Err(e) => return ddl_error(&e),
        }
    }
    let header = (columns.len() == 1).then(|| columns[0].clone());
    // The reload's work, all or nothing (B5): the key, the companions
    // retired, the header.
    match database
        .reload_atomic(table_name, |old| async move {
            let io_schema = old.io_schema.clone();
            crate::core::rql::unique_key::append(io_schema.clone(), &key).await.map_err(|e| e.to_string())?;
            let mut schema = schema;
            for flag in &flags {
                schema.mark_field_as_deleted(flag, io_schema.clone()).await;
            }
            if let Some(column) = header {
                schema.primary_key = Some(column);
                schema.rewrite_header(io_schema).await.map_err(|e| e.to_string())?;
            }
            Ok(())
        })
        .await
    {
        Ok(()) => ddl_ok(0),
        Err(failure) => reload_failed(failure, ""),
    }
}

/// A table's primary key as a constraint, derived from what the schema
/// records (no field says which key is primary):
///
/// - the header's column with its UNIQUE flag set: declared inline
///   (`id … PRIMARY KEY`), named `<table>_pkey`, no sidecar;
/// - the header's column without it: table-level, the one-column sidecar
///   on that column, `<table>_pkey` when there is one, else the first
///   (`ADD CONSTRAINT k1 PRIMARY KEY (a)` is `k1`); with no sidecar either
///   (a header an older DROP left behind), `<table>_pkey`;
/// - no header: the whole sidecar named `<table>_pkey`, a composite key.
///   One named otherwise is refused at ADD, as nothing could tell it from
///   a UNIQUE.
struct PrimaryKeyConstraint {
    name: String,
    /// The header's column; `None` for a composite key.
    column: Option<String>,
    inline: bool,
}

fn primary_key_constraint(schema: &crate::core::row::schema::TableSchema, table: &str) -> Option<PrimaryKeyConstraint> {
    let pkey = crate::core::rql::unique_key::pkey_name(table);
    let whole = || schema.unique_keys.iter().filter(|k| !k.is_deleted && k.predicate.is_none());
    let Some(column) = schema.primary_key.clone() else {
        return whole()
            .find(|k| k.name.eq_ignore_ascii_case(&pkey))
            .map(|k| PrimaryKeyConstraint { name: k.name.clone(), column: None, inline: false });
    };
    let inline = schema.fields.iter().any(|f| !f.is_deleted && f.is_unique && f.name.eq_ignore_ascii_case(&column));
    if inline {
        return Some(PrimaryKeyConstraint { name: pkey, column: Some(column), inline: true });
    }
    let on_column = |k: &&crate::core::rql::unique_key::UniqueKey| k.columns.len() == 1 && k.columns[0].eq_ignore_ascii_case(&column);
    let name = whole()
        .filter(on_column)
        .find(|k| k.name.eq_ignore_ascii_case(&pkey))
        .or_else(|| whole().find(on_column))
        .map_or(pkey, |k| k.name.clone());
    Some(PrimaryKeyConstraint { name, column: Some(column), inline: false })
}

/// Whether `schema` has a primary key ([`primary_key_constraint`]).
fn has_primary_key(schema: &crate::core::row::schema::TableSchema, table: &str) -> bool {
    primary_key_constraint(schema, table).is_some()
}

/// Whether a constraint of `table` is already called `name`: a UNIQUE,
/// CHECK or FOREIGN KEY, the primary key's `<table>_pkey`, or a UNIQUE
/// column's implicit `<table>_<column>_key`.
fn constraint_name_taken(schema: &crate::core::row::schema::TableSchema, table: &str, name: &str) -> bool {
    let named = |n: &str| n.eq_ignore_ascii_case(name);
    schema.unique_keys.iter().any(|k| !k.is_deleted && named(&k.name))
        || schema.checks.iter().any(|c| !c.is_deleted && named(&c.name))
        || schema.foreign_keys.iter().any(|fk| !fk.is_deleted && named(&fk.name))
        || primary_key_constraint(schema, table).is_some_and(|p| named(&p.name))
        || implicit_unique_column(name, table, &schema.fields).is_some()
}

/// A reload's failure (§7 steps 2-4) as a DDL reports it: its own work's
/// message, or why the table could not be reopened after it, prefixed with
/// what had already happened.
fn reload_failed(failure: crate::core::database::reload::ReloadFailure, reopen_context: &str) -> Vec<u8> {
    use crate::core::database::reload::ReloadFailure;
    match failure {
        ReloadFailure::Work(e) => ddl_error(&e),
        ReloadFailure::Reopen(e) if reopen_context.is_empty() => ddl_error(&e.to_string()),
        ReloadFailure::Reopen(e) => ddl_error(&format!("{reopen_context}: {e}")),
    }
}

/// Records an index over a path into a document.
///
/// The declaration is persisted and the table reopened so it takes effect; it
/// does not build anything here. What makes a path worth indexing is that the
/// engine cannot infer it — there are unboundedly many paths a document might
/// contain, and only the queries say which one matters.
async fn execute_create_path_index(sql: &str, database: &Arc<Database>) -> Vec<u8> {
    use crate::core::rql::path_index;

    let statement = match path_index::parse_create(sql) {
        Some(Ok(s)) => s,
        Some(Err(e)) => return ddl_error(&e),
        None => return ddl_error("this is not an index over a document path"),
    };

    let (declared, column_is_a_document) = {
        // ACCESS EXCLUSIVE before the schema is read (§7); the declaration
        // is written inside the reload, under it.
        let entry = match database
            .table_for(&statement.table, owner(), TableMode::AccessExclusive)
            .await
        {
            Ok(entry) => entry,
            Err(e) => return ddl_error(&e),
        };
        let Some(table) = entry.as_ref().clone() else {
            return ddl_error(&format!("relation '{}' does not exist", statement.table));
        };
        let is_document = table.schema.fields.iter().any(|f| {
            f.name.eq_ignore_ascii_case(&statement.path.column)
                && !f.is_deleted
                && f.db_type == crate::core::db_type::DbType::JSONB
        });
        (table.schema.path_indexes.clone(), is_document)
    };

    if !column_is_a_document {
        return ddl_error(&format!(
            "'{}' is not a JSONB column of '{}', so there is no document to index a path into. \
             A column shredded into a shadow table is indexed through that table's own columns \
             instead.",
            statement.path.column, statement.table
        ));
    }

    let key = statement.path.index_key();
    if let Some(existing) = declared.iter().find(|i| i.path == key) {
        return if statement.if_not_exists || existing.name == statement.name {
            ddl_ok(0)
        } else {
            ddl_error(&format!("'{}' already indexes that path", existing.name))
        };
    }
    if declared
        .iter()
        .any(|i| i.name.eq_ignore_ascii_case(&statement.name))
    {
        return if statement.if_not_exists {
            ddl_ok(0)
        } else {
            ddl_error(&format!(
                "an index called '{}' already exists",
                statement.name
            ))
        };
    }

    let entry = crate::core::rql::path_index::PathIndex {
        name: statement.name.clone(),
        path: key,
        is_deleted: false,
    };
    // §7 step 3: the declaration is the reload's work.
    let reloaded = database
        .reload_with(&statement.table, |old| async move {
            path_index::append(old.io_schema.clone(), &entry).await.map_err(|e| e.to_string())
        })
        .await;
    if let Err(failure) = reloaded {
        return reload_failed(failure, "the index was recorded but the table could not be reopened");
    }
    ddl_ok(0)
}

/// Whether an index of this name exists anywhere in the database. Index
/// names are database-wide, as in PostgreSQL.
fn index_name_taken(database: &Arc<Database>, name: &str) -> Option<String> {
    let mut taken = None;
    database.each_schema(|table, schema| {
        if taken.is_none()
            && (schema.path_indexes.iter().any(|i| i.name.eq_ignore_ascii_case(name))
                || schema
                    .secondary_indexes
                    .iter()
                    .any(|i| i.name.eq_ignore_ascii_case(name)))
        {
            taken = Some(table.to_string());
        }
    });
    if taken.is_some() {
        return taken;
    }
    None
}

/// `CREATE INDEX` over plain columns: records the declaration and reopens the
/// table, which builds the index from its rows.
async fn execute_create_index(sql: &str, database: &Arc<Database>) -> Vec<u8> {
    use crate::core::index::{self, IndexDecl};

    let statement = match index::ddl::parse_create(sql) {
        Some(Ok(s)) => s,
        Some(Err(e)) => return ddl_error(&e),
        None => return ddl_error("this is not an index over table columns"),
    };
    let name = statement.name_or_default();

    let (columns, types) = {
        // ACCESS EXCLUSIVE before the schema is read (§7); the declaration
        // is written inside the reload, under it.
        let entry = match database
            .table_for(&statement.table, owner(), TableMode::AccessExclusive)
            .await
        {
            Ok(entry) => entry,
            Err(e) => return ddl_error(&e),
        };
        let Some(table) = entry.as_ref().clone() else {
            return ddl_error(&format!("relation '{}' does not exist", statement.table));
        };
        let mut columns = Vec::new();
        let mut types = Vec::new();
        for column in &statement.columns {
            match table
                .schema
                .fields
                .iter()
                .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column))
            {
                Some(f) => {
                    columns.push(f.write_order as u32);
                    types.push(f.db_type.clone());
                }
                None => {
                    return ddl_error(&format!(
                        "column '{}' does not exist in '{}'",
                        column, statement.table
                    ));
                }
            }
        }
        (columns, types)
    };

    if index_name_taken(database, &name).is_some() {
        return if statement.if_not_exists {
            ddl_ok(0)
        } else {
            ddl_error(&format!("relation \"{}\" already exists", name))
        };
    }

    let method = match index::choose_method(statement.method, &types) {
        Ok(m) => m,
        Err(e) => return ddl_error(&e),
    };
    let decl = IndexDecl {
        name: name.clone(),
        method,
        unique: false,
        columns,
        is_deleted: false,
    };
    // §7 step 3: the declaration is the reload's work, so a lock wait that
    // fails (55P03, 40P01) has written nothing.
    let reloaded = database
        .reload_with(&statement.table, |old| async move {
            index::decl::append(old.io_schema.clone(), &decl).await.map_err(|e| e.to_string())
        })
        .await;
    if let Err(failure) = reloaded {
        return reload_failed(failure, "the index was recorded but the table could not be reopened");
    }
    info!("Created {} index {} on {}", method.name(), name, statement.table);
    ddl_ok(0)
}

/// `DROP INDEX` of an index `CREATE INDEX` declared. `None` when no table
/// declares one by that name, so the caller tries the other kinds.
async fn execute_drop_index(sql: &str, database: &Arc<Database>) -> Option<Vec<u8>> {
    let (name, _if_exists) = crate::core::rql::path_index::parse_drop(sql)?;
    // Index names are database-wide: the table is found by a metadata walk
    // (no lock), then locked ACCESS EXCLUSIVE (a late find, §7) and read
    // again under the lock, which a concurrent DROP INDEX may have beaten.
    let mut found: Option<String> = None;
    database.each_schema(|table, schema| {
        if found.is_none() && schema.secondary_indexes.iter().any(|i| i.name.eq_ignore_ascii_case(&name)) {
            found = Some(table.to_string());
        }
    });
    let table_name = found?;
    let table = match database.table_for(&table_name, owner(), TableMode::AccessExclusive).await {
        Ok(table) => table?,
        Err(e) => return Some(ddl_error(&e)),
    };
    let decl = table
        .schema
        .secondary_indexes
        .iter()
        .find(|i| i.name.eq_ignore_ascii_case(&name))
        .cloned()?;
    drop(table);
    let tombstone = crate::core::index::IndexDecl {
        is_deleted: true,
        ..decl
    };
    // §7 step 3: the tombstone is the reload's work.
    let reloaded = database
        .reload_with(&table_name, |old| async move {
            crate::core::index::decl::append(old.io_schema.clone(), &tombstone)
                .await
                .map_err(|e| e.to_string())
        })
        .await;
    if let Err(failure) = reloaded {
        return Some(reload_failed(failure, "the index was dropped but the table could not be reopened"));
    }
    Some(ddl_ok(0))
}

/// `DROP INDEX` of a partial unique index, which is kept as a partial unique
/// key. `None` when no table has one by that name, so the caller tries the
/// other kinds.
///
/// Only partial keys answer to `DROP INDEX` here. A full unique index is
/// stored the same way as `ADD CONSTRAINT … UNIQUE`, and the two cannot be told
/// apart afterwards; PostgreSQL refuses to drop the latter's index, so that
/// case is left to the constraint.
async fn execute_drop_partial_unique(sql: &str, database: &Arc<Database>) -> Option<Vec<u8>> {
    let (name, _if_exists) = crate::core::rql::path_index::parse_drop(sql)?;
    // Found by a metadata walk, then locked and read again (see
    // `execute_drop_index`).
    let is_it = |k: &crate::core::rql::unique_key::UniqueKey| {
        !k.is_deleted && k.predicate.is_some() && k.name.eq_ignore_ascii_case(&name)
    };
    let mut found: Option<String> = None;
    database.each_schema(|table, schema| {
        if found.is_none() && schema.unique_keys.iter().any(is_it) {
            found = Some(table.to_string());
        }
    });
    let table_name = found?;
    let table = match database.table_for(&table_name, owner(), TableMode::AccessExclusive).await {
        Ok(table) => table?,
        Err(e) => return Some(ddl_error(&e)),
    };
    if !table.schema.unique_keys.iter().any(is_it) {
        return None;
    }
    drop(table);
    // §7 step 3: the key's removal is the reload's work.
    let reloaded = database
        .reload_with(&table_name, |old| {
            let (name, table_name) = (name.clone(), table_name.clone());
            async move {
                match crate::core::rql::unique_key::remove(old.io_schema.clone(), &name).await {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(format!(
                        "index \"{}\" could not be found in the sidecar of '{}'",
                        name, table_name
                    )),
                    Err(e) => Err(e.to_string()),
                }
            }
        })
        .await;
    if let Err(failure) = reloaded {
        return Some(reload_failed(failure, "the index was dropped but the table could not be reopened"));
    }
    Some(ddl_ok(0))
}

/// Removes an index over a path, by the name it was declared under.
async fn execute_drop_path_index(sql: &str, database: &Arc<Database>) -> Vec<u8> {
    use crate::core::rql::path_index;

    let Some((name, if_exists)) = path_index::parse_drop(sql) else {
        return ddl_error("could not read the index name");
    };

    // Index names are database-wide, so the table holding it is found rather
    // than named.
    let mut found: Option<String> = None;
    database.each_schema(|table, schema| {
        if found.is_none() && schema.path_indexes.iter().any(|i| i.name.eq_ignore_ascii_case(&name)) {
            found = Some(table.to_string());
        }
    });

    let Some(table_name) = found else {
        // An index a constraint owns is refused, as PostgreSQL refuses it:
        // dropping it silently did nothing, so the key it backs stayed
        // enforced while the client was told it was gone. The constraint is
        // what goes, by `ALTER TABLE … DROP CONSTRAINT`.
        let mut owner: Option<String> = None;
        database.each_schema(|table, schema| {
            if owner.is_none() && constraint_index(schema, table, &name) {
                owner = Some(table.to_string());
            }
        });
        if let Some(table) = owner {
            return ddl_error(&format!(
                "cannot drop index {name} because constraint {name} on table {table} requires it \
                 (HINT: you can drop constraint {name} on table {table} instead)"
            ));
        }
        // Not an error when the index is one this engine never stored: an
        // ordinary index is a no-op here, so dropping it is one too.
        return ddl_ok(0);
    };

    match database.table_for(&table_name, owner(), TableMode::AccessExclusive).await {
        Ok(Some(_)) => {}
        Ok(None) if if_exists => return ddl_ok(0),
        Ok(None) => return ddl_error("relation vanished"),
        Err(e) => return ddl_error(&e),
    }

    let entry = crate::core::rql::path_index::PathIndex {
        name,
        path: String::new(),
        is_deleted: true,
    };
    // §7 step 3: the tombstone is the reload's work.
    let reloaded = database
        .reload_with(&table_name, |old| async move {
            path_index::append(old.io_schema.clone(), &entry).await.map_err(|e| e.to_string())
        })
        .await;
    if let Err(failure) = reloaded {
        return reload_failed(failure, "the index was dropped but the table could not be reopened");
    }
    ddl_ok(0)
}

/// The SQL text behind a purpose, for the clauses the plan does not carry.
fn query_sql(query: &QueryPurpose) -> String {
    match query {
        QueryPurpose::UpdateRow(sql) | QueryPurpose::DeleteRow(sql) => sql.clone(),
        // A lock is a property of the transaction, not a change to any row, so
        // it has no statement text worth recording or replicating.
        QueryPurpose::LockTable(_) => String::new(),
        QueryPurpose::InsertRow(d) => d.query.clone(),
        QueryPurpose::QueryRows(d) => d.query.clone(),
        // A read: nothing to retry or replicate.
        QueryPurpose::PreparedRows(_) => String::new(),
        QueryPurpose::CreateIndex(s) | QueryPurpose::DropIndex(s) => s.clone(),
        QueryPurpose::CreateTable(s)
        | QueryPurpose::DropTable(s)
        | QueryPurpose::AddColumn(s)
        | QueryPurpose::DropColumn(s)
        | QueryPurpose::RenameColumn(s) => s.clone(),
        QueryPurpose::Vacuum(_) => String::new(),
        QueryPurpose::FilterControl(sql) | QueryPurpose::Replication(sql) => sql.clone(),
        #[cfg(feature = "storage_pools")]
        QueryPurpose::Storage(sql) => sql.clone(),
        QueryPurpose::Introspect(_) => String::new(),
        QueryPurpose::IntrospectError(message) => message.clone(),
        // The key/value store is not replicated, and a value may be a secret
        // (a session token), so there is nothing to record.
        #[cfg(feature = "kv_cache")]
        QueryPurpose::KvCache(_) => String::new(),
        #[cfg(feature = "s3_backup")]
        QueryPurpose::Backup(_) => String::new(),
        #[cfg(feature = "s3_backup")]
        QueryPurpose::BackupError(sql) => sql.clone(),
    }
}

/// Sorts a serialized result by evaluated `ORDER BY` expressions and decodes
/// the rows the caller will use.
///
/// NULL placement follows PostgreSQL: by default a NULL sorts as the largest
/// value — last ascending, first descending — and an explicit `NULLS
/// FIRST`/`LAST` overrides that independently of the direction, which is why
/// the null arm sits outside the `descending` reversal.
///
/// `cap` is how many rows from the front of the order the caller will look
/// at — `OFFSET + LIMIT` — and `None` when it needs every row (see the guard
/// at the call site). The result is always exactly the first `cap` rows a
/// *stable* sort of the whole input would produce, in that order: ties are
/// broken on the row's position in the input, which makes the order total, so
/// a selection followed by a sort of the survivors agrees with the full sort.
///
/// What it costs, against the stable sort of `(keys, Row)` it replaces: the
/// sort moves `(keys, index)` pairs rather than ~1.5 KB rows, and with a cap
/// no row outside the page is kept at all — each is decoded only for as long
/// as its keys take to evaluate, and the page is decoded again from its bytes.
fn sort_rows_by_expressions(
    rows_bytes: &[u8],
    keys: &[(crate::core::rql::expr::Expr, bool, Option<bool>)],
    schema: &[crate::core::row::schema::SchemaField],
    cap: Option<usize>,
) -> Result<Vec<crate::core::row::row::Row>, String> {
    use crate::core::row::row::Row;

    let directions: Vec<(bool, Option<bool>)> = keys
        .iter()
        .map(|(_, descending, nulls)| (*descending, *nulls))
        .collect();
    // Every key is evaluated once per row, up front — a comparator must not
    // error, and re-evaluating inside one would be quadratic anyway. Every
    // row's keys are evaluated even with a cap, so a key that fails on any
    // row still fails the statement.
    let wide: Vec<bool> = keys.iter().map(|(expr, _, _)| crate::core::rql::expr::output_is_wide(expr)).collect();
    let eval_keys = |row: &Row| -> Result<Vec<crate::core::rql::expr::Value>, String> {
        let mut values = Vec::with_capacity(keys.len());
        for ((expr, _, _), wide) in keys.iter().zip(&wide) {
            values.push(crate::core::rql::expr::eval_output_planned(expr, *wide, row, schema)?);
        }
        Ok(values)
    };

    let total = crate::core::row::row::count_serialized_rows(rows_bytes) as usize;
    match cap.filter(|c| *c < total) {
        Some(cap) => {
            let mut keyed: Vec<(Vec<crate::core::rql::expr::Value>, usize)> =
                Vec::with_capacity(total);
            let mut spans: Vec<&[u8]> = Vec::with_capacity(total);
            for (index, span) in crate::core::row::row::serialized_rows(rows_bytes).enumerate() {
                let span = span.map_err(|e| e.to_string())?;
                let row = Row::decode(span).map_err(|e| e.to_string())?;
                keyed.push((eval_keys(&row)?, index));
                spans.push(span);
            }
            order_keyed_rows(&mut keyed, &directions, Some(cap));
            keyed
                .iter()
                .map(|(_, index)| Row::decode(spans[*index]).map_err(|e| e.to_string()))
                .collect()
        }
        None => {
            let mut rows =
                crate::core::row::row::vec_into_rows(rows_bytes).map_err(|e| e.to_string())?;
            let mut keyed: Vec<(Vec<crate::core::rql::expr::Value>, usize)> =
                Vec::with_capacity(rows.len());
            for (index, row) in rows.iter().enumerate() {
                keyed.push((eval_keys(row)?, index));
            }
            order_keyed_rows(&mut keyed, &directions, None);
            // Each index appears once, so every row is taken exactly once.
            Ok(keyed
                .iter()
                .map(|(_, index)| std::mem::take(&mut rows[*index]))
                .collect())
        }
    }
}

/// Puts `(keys, input position)` pairs in `ORDER BY` order, keeping only the
/// first `cap` when one is given.
///
/// The input position is the last key, so equal keys keep their input order —
/// the answer a stable sort gives — and the order is total, which is what lets
/// `select_nth_unstable_by` partition at the cap and agree with a full sort
/// about which rows are in the page.
fn order_keyed_rows(
    keyed: &mut Vec<(Vec<crate::core::rql::expr::Value>, usize)>,
    directions: &[(bool, Option<bool>)],
    cap: Option<usize>,
) {
    let order = |a: &(Vec<crate::core::rql::expr::Value>, usize),
                 b: &(Vec<crate::core::rql::expr::Value>, usize)| {
        crate::core::rql::expr::compare_key_values(&a.0, &b.0, directions)
            .then_with(|| a.1.cmp(&b.1))
    };
    if let Some(cap) = cap {
        if cap == 0 {
            keyed.clear();
            return;
        }
        if cap < keyed.len() {
            keyed.select_nth_unstable_by(cap - 1, order);
            keyed.truncate(cap);
        }
    }
    keyed.sort_unstable_by(order);
}

/// Where the post-scan loop takes its rows from: the page an expression sort
/// already decoded, or the scan's serialized result, decoded a row at a time.
enum PageRows<'a> {
    Sorted(std::vec::IntoIter<crate::core::row::row::Row>),
    Serialized(crate::core::row::row::SerializedRows<'a>),
}

impl PageRows<'_> {
    fn next_row(&mut self) -> Option<Result<crate::core::row::row::Row, String>> {
        match self {
            PageRows::Sorted(rows) => rows.next().map(Ok),
            PageRows::Serialized(spans) => Some(
                spans
                    .next()?
                    .and_then(crate::core::row::row::Row::decode)
                    .map_err(|e| e.to_string()),
            ),
        }
    }

    /// How many rows are left, for sizing; a header walk for serialized rows.
    fn remaining_hint(&self, all: &[u8]) -> usize {
        match self {
            PageRows::Sorted(rows) => rows.len(),
            PageRows::Serialized(_) => crate::core::row::row::count_serialized_rows(all) as usize,
        }
    }
}

/// The sort `sort_rows_by_expressions` replaced, kept as the reference its
/// differential test compares against.
#[cfg(test)]
fn sort_rows_by_expressions_reference(
    rows: Vec<crate::core::row::row::Row>,
    keys: &[(crate::core::rql::expr::Expr, bool, Option<bool>)],
    schema: &[crate::core::row::schema::SchemaField],
) -> Result<Vec<crate::core::row::row::Row>, String> {
    let mut keyed: Vec<(
        Vec<crate::core::rql::expr::Value>,
        crate::core::row::row::Row,
    )> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values = Vec::with_capacity(keys.len());
        for (expr, _, _) in keys {
            values.push(crate::core::rql::expr::eval(expr, &row, schema)?);
        }
        keyed.push((values, row));
    }

    let directions: Vec<(bool, Option<bool>)> = keys
        .iter()
        .map(|(_, descending, nulls)| (*descending, *nulls))
        .collect();
    keyed.sort_by(|a, b| crate::core::rql::expr::compare_key_values(&a.0, &b.0, &directions));

    Ok(keyed.into_iter().map(|(_, row)| row).collect())
}

#[cfg(test)]
mod expression_sort_tests {
    //! The capped, index-sorted expression sort against the stable sort it
    //! replaced, over data built to make them disagree if they can: heavy ties,
    //! NULLs in both placements, NaN and signed zero, empty strings, and keys
    //! of more than one column.
    use super::{sort_rows_by_expressions, sort_rows_by_expressions_reference};
    use crate::core::db_type::DbType;
    use crate::core::row::row::{Column, Row, rows_into_vec, vec_into_rows};
    use crate::core::row::schema::SchemaField;
    use crate::memory_pool::MEMORY_POOL;
    use smallvec::SmallVec;

    fn schema() -> Vec<SchemaField> {
        [
            ("id", DbType::I64, 8),
            ("a", DbType::I32, 4),
            ("s", DbType::STRING, 0),
            ("f", DbType::F64, 8),
            ("n", DbType::I64, 8),
            ("__null_n", DbType::BOOL, 1),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, (name, ty, size))| {
            SchemaField::new(name.to_string(), ty, size, 0, i as u64, false)
        })
        .collect()
    }

    fn col(schema_id: u64, ty: DbType, bytes: &[u8]) -> Column {
        let mut data = MEMORY_POOL.acquire(bytes.len());
        if !bytes.is_empty() {
            data.into_slice_mut().copy_from_slice(bytes);
        }
        Column {
            schema_id: crate::core::row::row::schema_id_of(schema_id),
            data,
            column_type: ty,
        }
    }

    /// Rows whose columns are ordered as a scan returns them, with `id` the
    /// row's identity and never a key, so a mispaired row shows as a wrong id.
    fn rows_bytes(count: usize, seed: u64) -> Vec<u8> {
        let floats = [
            0.0f64,
            -0.0,
            1.5,
            -1.5,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1.5,
        ];
        let strings = ["", "a", "b", "ab", "", "B", "a"];
        let mut state = seed | 1;
        let mut next = move |m: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % m
        };
        let mut rows = Vec::with_capacity(count);
        for id in 0..count as i64 {
            let null = next(4) == 0;
            let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
            columns.push(col(0, DbType::I64, &id.to_le_bytes()));
            columns.push(col(1, DbType::I32, &(next(5) as i32 - 2).to_le_bytes()));
            columns.push(col(2, DbType::STRING, strings[next(7) as usize].as_bytes()));
            columns.push(col(3, DbType::F64, &floats[next(8) as usize].to_le_bytes()));
            columns.push(col(
                4,
                DbType::I64,
                &(if null { 0 } else { next(3) as i64 }).to_le_bytes(),
            ));
            columns.push(col(5, DbType::BOOL, &[null as u8]));
            rows.push(Row {
                position: id as u64,
                columns,
                length: 0,
            });
        }
        rows_into_vec(rows)
    }

    fn ids(rows: &[Row]) -> Vec<i64> {
        rows.iter()
            .map(|r| i64::from_le_bytes(r.columns[0].data.into_slice().try_into().unwrap()))
            .collect()
    }

    #[test]
    fn capped_and_uncapped_agree_with_the_stable_sort() {
        let schema = schema();
        let key_sets: &[&[&str]] = &[
            &["a"],
            &["n"],
            &["f"],
            &["s"],
            &["NULLIF(s, '')"],
            &["a", "n"],
            &["n", "f", "a"],
            &["NULLIF(s, '')", "a", "f"],
            // Same-typed operands: COALESCE over BIGINT and INTEGER yields
            // values of two widths, which `expr::compare` does not order
            // consistently — a separate defect, and one that makes *any* two
            // sorts disagree, so it cannot be part of this comparison.
            &["COALESCE(n, id)", "s"],
        ];
        // (descending, NULLS FIRST/LAST/default) for every key.
        let placements: &[(bool, Option<bool>)] = &[
            (false, None),
            (true, None),
            (false, Some(true)),
            (true, Some(false)),
            (false, Some(false)),
            (true, Some(true)),
        ];
        let mut checked = 0;
        for (seed, count) in [(7u64, 0usize), (11, 1), (13, 9), (17, 257), (19, 1500)] {
            let bytes = rows_bytes(count, seed);
            for keys in key_sets {
                for (k, placement) in placements.iter().enumerate() {
                    // Mix the placements across keys so a multi-key set does
                    // not use one direction for all of them.
                    let exprs: Vec<_> = keys
                        .iter()
                        .enumerate()
                        .map(|(i, text)| {
                            let (desc, nulls) = placements[(k + i) % placements.len()];
                            let _ = placement;
                            (
                                crate::core::rql::expr::parse(text, &schema).unwrap(),
                                desc,
                                nulls,
                            )
                        })
                        .collect();
                    let reference = ids(&sort_rows_by_expressions_reference(
                        vec_into_rows(&bytes).unwrap(),
                        &exprs,
                        &schema,
                    )
                    .unwrap());
                    assert_eq!(reference.len(), count);
                    for cap in [
                        None,
                        Some(0),
                        Some(1),
                        Some(2),
                        Some(7),
                        Some(50),
                        Some(count.saturating_sub(1)),
                        Some(count),
                        Some(count + 3),
                    ] {
                        let got =
                            ids(&sort_rows_by_expressions(&bytes, &exprs, &schema, cap).unwrap());
                        let want = &reference[..cap.unwrap_or(count).min(count)];
                        assert_eq!(
                            got, want,
                            "keys {:?} placement {} cap {:?} rows {}",
                            keys, k, cap, count
                        );
                        checked += 1;
                    }
                }
            }
        }
        assert!(checked > 1000);
    }

    /// The rows that come back are whole: every column, not just the keys.
    #[test]
    fn capped_rows_carry_every_column() {
        let schema = schema();
        let bytes = rows_bytes(300, 5);
        let exprs = vec![(
            crate::core::rql::expr::parse("f", &schema).unwrap(),
            true,
            None,
        )];
        let all = vec_into_rows(&bytes).unwrap();
        let got = sort_rows_by_expressions(&bytes, &exprs, &schema, Some(10)).unwrap();
        assert_eq!(got.len(), 10);
        for row in &got {
            let id = i64::from_le_bytes(row.columns[0].data.into_slice().try_into().unwrap());
            let original = &all[id as usize];
            assert_eq!(row.columns.len(), original.columns.len());
            for (a, b) in row.columns.iter().zip(original.columns.iter()) {
                assert_eq!(a.schema_id, b.schema_id);
                assert_eq!(a.column_type, b.column_type);
                assert_eq!(a.data.into_slice(), b.data.into_slice());
            }
        }
    }

    /// A key that fails on a row outside the page still fails the statement.
    #[test]
    fn a_key_error_past_the_page_still_fails() {
        let schema = schema();
        let bytes = rows_bytes(200, 3);
        // `a` is -2..=2, so some row divides by zero.
        let exprs = vec![(
            crate::core::rql::expr::parse("id / a", &schema).unwrap(),
            false,
            None,
        )];
        assert!(sort_rows_by_expressions(&bytes, &exprs, &schema, Some(1)).is_err());
        assert!(sort_rows_by_expressions(&bytes, &exprs, &schema, None).is_err());
    }

    /// A damaged buffer is an error, not a panic and not a short answer.
    #[test]
    fn a_truncated_buffer_is_an_error() {
        let schema = schema();
        let bytes = rows_bytes(20, 9);
        let exprs = vec![(
            crate::core::rql::expr::parse("a", &schema).unwrap(),
            false,
            None,
        )];
        let cut = &bytes[..bytes.len() - 3];
        assert!(sort_rows_by_expressions(cut, &exprs, &schema, Some(2)).is_err());
        assert!(sort_rows_by_expressions(cut, &exprs, &schema, None).is_err());
    }
}

/// Reads the rows a write is about to affect, for its `RETURNING` clause.
///
/// `updates` is `Some` for an `UPDATE`, and its assignments are overlaid on the
/// rows read, so the caller sees post-update values without a second read.
/// `None` is a `DELETE`, which reports the rows as they were.
///
/// Returns `Ok(None)` when the statement has no `RETURNING` clause.
/// Replaces a JSON column's stored reference with the document it points at.
///
/// A JSON column holds a `UUID` naming the row its document was shredded into.
/// `SELECT` swaps that for the document; `RETURNING` did not, so an
/// `INSERT … RETURNING query_builder_data` handed the client sixteen bytes of
/// raw UUID where it expected JSON — reported as success, and read by the
/// driver as "expected value at line 1 column 1".
async fn hydrate_json_in_returning(
    database: &Arc<Database>,
    rows: &mut [crate::core::row::row::Row],
    fields: &[crate::core::row::schema::SchemaField],
    json_columns: &[crate::core::rql::json_map::JsonColumn],
    tx: TxId,
) {
    if json_columns.is_empty() {
        return;
    }
    for (index, field) in fields.iter().enumerate() {
        let Some(shadow_table) = crate::core::rql::json_map::shadow_for(&field.name, json_columns)
        else {
            continue;
        };
        for row in rows.iter_mut() {
            let Some(cell) = row.columns.get(index) else {
                continue;
            };
            // An absent or NULL reference stays as it is: there is no document.
            if cell.column_type == crate::core::db_type::DbType::NULL
                || cell.data.into_slice().len() != crate::core::value_codec::UUID_LEN
            {
                continue;
            }
            let reference = crate::core::value_codec::format_uuid(cell.data.into_slice());
            let rendered = Box::pin(load_json_document(
                database,
                shadow_table,
                &reference,
                0,
                tx,
            ))
            .await
            .map(|v| v.to_string())
            .unwrap_or_else(|| "{}".to_string());
            if let Some(cell) = row.columns.get_mut(index) {
                let bytes = rendered.as_bytes();
                let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
                mb.into_slice_mut().copy_from_slice(bytes);
                cell.data = mb;
                cell.column_type = crate::core::db_type::DbType::JSON;
            }
        }
    }
}

/// The name of the column at `write_order`, for a companion lookup.
fn field_name_of(
    schema: &[crate::core::row::schema::SchemaField],
    write_order: u64,
) -> Option<String> {
    schema
        .iter()
        .find(|f| f.write_order == write_order)
        .map(|f| f.name.clone())
}

/// Whether `sql` has a `RETURNING` list, which must resolve against `schema`.
///
/// Asked before a write, so a list that cannot resolve fails the statement
/// before it changes anything; the rows themselves are produced afterwards,
/// from what the write collected.
fn returning_planned(sql: &str, schema: &[crate::core::row::schema::SchemaField]) -> Result<bool, String> {
    match crate::core::rql::lexer_ir::returning_items(sql, schema) {
        None => Ok(false),
        Some(Err(e)) => Err(e),
        Some(Ok(_)) => match crate::core::rql::lexer_ir::returning_columns(sql, schema) {
            Some(Ok(_)) => Ok(true),
            _ => Err("RETURNING list could not be resolved".to_string()),
        },
    }
}

async fn returning_rows_for_write<S: crate::core::storage_providers::traits::StorageIO>(
    table: &crate::core::row::table::Table<S>,
    sql: &str,
    full_rows: Vec<crate::core::row::row::Row>,
    updates: Option<&smallvec::SmallVec<[crate::core::row::row::ColumnWritePayload; 32]>>,
    // Assignments the write path evaluates per row. `RETURNING` reports values
    // *after* the change, so it has to evaluate them too — and against a row
    // that carries the columns they read, which the RETURNING list alone may
    // not include.
    expressions: &[(u32, crate::core::rql::expr::Expr)],
    database: &Arc<Database>,
    tx: TxId,
) -> Result<Option<Vec<crate::core::row::row::Row>>, String> {
    crate::owner_probe!(crate::core::mvcc::owner::probe::Site::Returning, tx, database);
    let schema = &table.schema.fields;

    let items = match crate::core::rql::lexer_ir::returning_items(sql, schema) {
        None => return Ok(None),
        Some(Err(e)) => return Err(e),
        Some(Ok(items)) => items,
    };
    // The items as fields, aligned by position — what the JSON hydration and
    // the output row's typing read.
    let fields = match crate::core::rql::lexer_ir::returning_columns(sql, schema) {
        Some(Ok(f)) => f,
        _ => return Err("RETURNING list could not be resolved".to_string()),
    };

    // What the scan must fetch: every stored column an item echoes, every
    // column a computed item reads, and whatever the assignments read.
    // `SET a = b` returning `a` has to load `b` as well or the expression
    // evaluates against a row that does not hold it — which came back empty
    // while the stored value was written correctly.
    let mut fetch_fields: Vec<crate::core::row::schema::SchemaField> = Vec::new();
    for item in &items {
        match item {
            crate::core::rql::lexer_ir::ReturningItem::Column(f) => {
                if !fetch_fields.iter().any(|x| x.name == f.name) {
                    fetch_fields.push(f.clone());
                }
            }
            crate::core::rql::lexer_ir::ReturningItem::Computed { expr, .. } => {
                for f in crate::core::rql::expr::columns_of(expr) {
                    if !fetch_fields.iter().any(|x| x.name == f.name) {
                        fetch_fields.push(f.clone());
                    }
                }
            }
        }
    }
    // Each referenced column's NULL companion comes too. This fetch is a raw
    // read of the row's bytes, not a projection, so a NULL fixed-width column
    // arrives as its type's zeroes — indistinguishable from a real zero. Only
    // the companion says which it is, and `COALESCE(started_at, '…')` on a NULL
    // column returned the zero timestamp without it.
    let schema_sv: smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> =
        schema.iter().cloned().collect();
    let mut null_flags: Vec<(usize, usize)> = Vec::new();
    // Every echoed column's companion, not just the ones an assignment reads:
    // a plain `RETURNING note` on a NULL column reported the empty string
    // while `SELECT note` reported NULL, and a NULL array crashed the
    // client's decoder outright.
    for at in 0..fetch_fields.len() {
        let name = fetch_fields[at].name.clone();
        if crate::core::rql::nullable::is_hidden(&name) {
            continue;
        }
        let Some(flag) = crate::core::rql::nullable::flag_field(&name, &schema_sv) else {
            continue;
        };
        if !fetch_fields.iter().any(|f| f.name == flag.name) {
            fetch_fields.push(flag.clone());
        }
        let flag_at = fetch_fields
            .iter()
            .position(|f| f.name == flag.name)
            .expect("just pushed");
        if !null_flags.iter().any(|(v, f)| *v == at && *f == flag_at) {
            null_flags.push((at, flag_at));
        }
    }
    let computed_exprs: Vec<crate::core::rql::expr::Expr> = items
        .iter()
        .filter_map(|item| match item {
            crate::core::rql::lexer_ir::ReturningItem::Computed { expr, .. } => Some(expr.clone()),
            _ => None,
        })
        .collect();
    let all_exprs: Vec<&crate::core::rql::expr::Expr> = expressions
        .iter()
        .map(|(_, e)| e)
        .chain(computed_exprs.iter())
        .collect();
    for expression in all_exprs {
        for referenced in crate::core::rql::expr::columns_of(expression) {
            let at = match fetch_fields.iter().position(|f| f.name == referenced.name) {
                Some(at) => at,
                None => {
                    fetch_fields.push(referenced.clone());
                    fetch_fields.len() - 1
                }
            };
            if let Some(flag) = crate::core::rql::nullable::flag_field(&referenced.name, &schema_sv)
            {
                if !fetch_fields.iter().any(|f| f.name == flag.name) {
                    fetch_fields.push(flag.clone());
                }
                let flag_at = fetch_fields
                    .iter()
                    .position(|f| f.name == flag.name)
                    .expect("just pushed");
                null_flags.push((at, flag_at));
            }
        }
    }
    // The rows the write changed, as they were before it, each projected to
    // the columns this list reads, in `fetch_fields` order (the positions
    // `null_flags` index).
    //
    // They come from the write itself, read under its lock and its snapshot.
    // A read of its own here (this used to call `query_row_in_txn`) ran under
    // another snapshot: a row inserted or deleted by a transaction committing
    // in between was changed but not reported, or reported but not changed,
    // and the client was told about rows the statement never touched.
    let mut rows: Vec<crate::core::row::row::Row> = full_rows
        .into_iter()
        .map(|full| {
            let mut row = crate::core::row::row::Row::default();
            for field in &fetch_fields {
                if let Some(column) = full.columns.iter().find(|c| u64::from(c.schema_id) == field.write_order) {
                    row.columns.push(column.clone());
                }
            }
            row
        })
        .collect();

    // Marked before anything reads the row: an expression has to see a NULL
    // where the row holds one, and a `DELETE … RETURNING` — which has no
    // assignments at all — has to report one too.
    for row in rows.iter_mut() {
        for (value_at, flag_at) in &null_flags {
            let is_null = row
                .columns
                .get(*flag_at)
                .is_some_and(|c| crate::core::rql::nullable::flag_is_set(c.data.into_slice()));
            if is_null {
                if let Some(cell) = row.columns.get_mut(*value_at) {
                    cell.column_type = crate::core::db_type::DbType::NULL;
                }
            }
        }
    }

    if let Some(updates) = updates {
        for row in rows.iter_mut() {
            let mut computed: Vec<(u64, crate::memory_pool::MemoryBlock, DbTypeRef)> = Vec::new();
            for (write_order, expression) in expressions {
                // Propagated rather than skipped: a skipped evaluation fell
                // through to the assignment's empty placeholder below, which
                // reported "" for a column whose write was about to succeed —
                // a silent wrong answer. The write path fails the statement on
                // the same evaluation error, and this runs before it, so both
                // now fail identically.
                // At the target's declared scale, as the write path computes it.
                let value = match schema
                    .iter()
                    .find(|f| f.write_order as u32 == *write_order as u32)
                {
                    // Held to the column's declared length or precision, as
                    // the write path stores it: a text-typed `'qq   '` into
                    // `character(4)` is reported `qq  `, not as assigned.
                    Some(target) => crate::core::rql::expr::held_to_text_length(
                        crate::core::rql::expr::eval_into(expression, row, schema, target)?,
                        target,
                    )?,
                    None => crate::core::rql::expr::eval(expression, row, schema)?,
                };
                // NULL is a value here, not an absence: without an entry the
                // placeholder overlay reported the empty string where the
                // write path was about to store NULL and set its companion.
                if matches!(value, crate::core::rql::expr::Value::Null) {
                    computed.push((
                        u64::from(*write_order),
                        crate::memory_pool::MemoryBlock::default(),
                        crate::core::db_type::DbType::NULL,
                    ));
                    continue;
                }
                if let crate::core::rql::expr::Value::Data { bytes, .. } = value {
                    // Fitted to the column's declared width, and reported as
                    // the column's own type — exactly as the write path stores
                    // it. Arithmetic widens to an i64, so `SET n = n + 1` on an
                    // `int4` produced eight bytes; handed to the renderer as an
                    // `int4` those decoded to nothing and `RETURNING` came back
                    // empty while the stored value was right.
                    let Some(target) = schema.iter().find(|f| f.write_order as u32 == *write_order)
                    else {
                        continue;
                    };
                    let variable = target.db_type.is_variable_width();
                    let width = if variable {
                        bytes.len()
                    } else {
                        target.size as usize
                    };
                    let mut mb = crate::memory_pool::MEMORY_POOL.acquire(width);
                    let slot = mb.into_slice_mut();
                    slot.fill(0);
                    let n = bytes.len().min(slot.len());
                    slot[..n].copy_from_slice(&bytes[..n]);
                    computed.push((u64::from(*write_order), mb, target.db_type.physical()));
                }
            }

            for column in row.columns.iter_mut() {
                if let Some((_, data, db_type)) = computed
                    .iter()
                    .find(|(order, _, _)| *order == u64::from(column.schema_id))
                {
                    column.data = data.clone();
                    column.column_type = db_type.clone();
                    continue;
                }
                if let Some(update) = updates
                    .iter()
                    .find(|u| u64::from(u.write_order) == u64::from(column.schema_id))
                {
                    column.data = update.data.clone();
                    // An assignment of NULL overlays a NULL, not an empty value
                    // of the column's type.
                    //
                    // The payload carries the column's declared type whatever
                    // it holds, so `SET c = NULL` overlaid empty bytes *typed
                    // as the column*. Downstream that is a present value of
                    // zero length rather than an absent one, and on the wire a
                    // zero-length field is not NULL: NULL is length −1, which
                    // a client checks before decoding, while length 0 goes
                    // straight into the decoder. For an array that is fatal —
                    // the decoder reads a four-byte dimension count first, so
                    // an empty payload panics inside the client's buffer
                    // ("advance out of bounds: the len is 0 but advancing by
                    // 4") instead of returning an error. sea-orm's
                    // `model.update()` writes every column, so any row with a
                    // NULL array killed the connection decoding its own
                    // `RETURNING`.
                    //
                    // The companion the write path sets alongside is what says
                    // NULL, so it is consulted here rather than guessing from
                    // emptiness: an empty *string* is a real value and must
                    // stay one.
                    let assigned_null = crate::core::rql::nullable::flag_field(
                        &field_name_of(schema, u64::from(column.schema_id)).unwrap_or_default(),
                        &schema_sv,
                    )
                    .and_then(|flag| {
                        updates
                            .iter()
                            .find(|u| u64::from(u.write_order) == flag.write_order)
                            .map(|u| crate::core::rql::nullable::flag_is_set(u.data.into_slice()))
                    })
                    .unwrap_or(false);
                    column.column_type = if assigned_null {
                        crate::core::db_type::DbType::NULL
                    } else {
                        update.column_type.clone()
                    };
                }
            }
        }
    }

    // The output row is built from the items: a stored column is echoed from
    // the (post-overlay) fetched row, a computed item is evaluated against
    // it — after the overlay, so `RETURNING (n - 1)` sees the value the
    // statement stored, which is what PostgreSQL reports.
    for row in rows.iter_mut() {
        let mut built: crate::core::row::row::RowColumns =
            smallvec::SmallVec::new();
        for (position, item) in items.iter().enumerate() {
            match item {
                crate::core::rql::lexer_ir::ReturningItem::Column(f) => {
                    let cell = row
                        .columns
                        .iter()
                        .find(|c| u64::from(c.schema_id) == f.write_order)
                        .cloned()
                        .map(|mut c| {
                            c.schema_id = crate::core::row::row::schema_id_of(position as u64);
                            c
                        });
                    match cell {
                        Some(c) => built.push(c),
                        None => built.push(crate::core::row::row::Column {
                            schema_id: crate::core::row::row::schema_id_of(position as u64),
                            data: crate::memory_pool::MemoryBlock::default(),
                            column_type: crate::core::db_type::DbType::NULL,
                        }),
                    }
                }
                crate::core::rql::lexer_ir::ReturningItem::Computed { expr, db_type, .. } => {
                    // Re-encoded as the type this item is DESCRIBED as: the
                    // arithmetic is done in i64/f64, so `counter - 1` on an
                    // `int4` column produced eight bytes under an `int4` OID
                    // and the client refused to decode it.
                    // Exactly when it reads a NUMERIC with more places than
                    // the engine's, as a select list's item is: through the
                    // engine `RETURNING b - 67e-18` over a NUMERIC(38,20) was
                    // refused for the places past nine.
                    let value = crate::core::rql::expr::coerce_to(
                        crate::core::rql::expr::eval_output(expr, row, schema)?,
                        db_type,
                    );
                    // A NUMERIC is shown at the scale PostgreSQL gives the
                    // expression (the larger of its operands' for `+`, their
                    // sum for `*`), carried in the cell as SELECT's computed
                    // items carry it. Without it the wire trimmed the engine's
                    // nine places to the value's own: `amt * 2` on a
                    // NUMERIC(10,2) holding 1.50 arrived as 3, not 3.00.
                    let shown = match (db_type, crate::core::rql::expr::display_scale(expr)) {
                        (crate::core::db_type::DbType::DECIMAL, Some(scale)) => Some(scale),
                        _ => None,
                    };
                    let value = crate::core::rql::expr::at_display_scale(value, shown)?;
                    let (bytes, ty) = match value {
                        crate::core::rql::expr::Value::Null => {
                            (Vec::new(), crate::core::db_type::DbType::NULL)
                        }
                        crate::core::rql::expr::Value::Data { bytes, .. } => match shown {
                            Some(scale) if bytes.len() == 16 => (
                                crate::core::value_codec::decimal_cell_with_scale(
                                    i128::from_le_bytes(bytes.as_slice().try_into().expect("sixteen bytes")),
                                    scale,
                                ),
                                db_type.clone(),
                            ),
                            _ => (bytes, db_type.clone()),
                        },
                    };
                    let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
                    mb.into_slice_mut().copy_from_slice(&bytes);
                    built.push(crate::core::row::row::Column {
                        schema_id: crate::core::row::row::schema_id_of(position as u64),
                        data: mb,
                        column_type: ty,
                    });
                }
            }
        }
        row.columns = built;
    }

    hydrate_json_in_returning(database, &mut rows, &fields, &table.schema.json_columns, tx).await;

    Ok(Some(rows))
}

/// The columns that identify one row, for the statements that need to name
/// rows they have already chosen (`docs/partitioning.md` §1b).
///
/// `FOR UPDATE` locks a row by them, a writer checks the same lock, and
/// `UPDATE … FROM` and `DELETE … USING` write each joined row back by them.
/// Only an inline `UNIQUE` used to count, so a table whose key is written
/// `PRIMARY KEY` or as a table-level `UNIQUE (id)` — the ordinary way an ORM
/// emits one — was refused as having no unique column at all, though it
/// plainly had one. And only one column counted, so a composite key, which is
/// the only kind a partitioned table has (every unique key holds every
/// partition-key column), refused all four statements.
struct IdentifyingKey {
    /// In the key's declared order, which is the order the framed tuple and
    /// the write-back predicate use.
    fields: Vec<crate::core::row::schema::SchemaField>,
    /// Each field's declared `NUMERIC` scale (`type_modifier::scale_of`),
    /// the scale every key cell this is given is at.
    scales: Vec<u32>,
}

impl IdentifyingKey {
    fn names(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.name.as_str())
    }

    /// The bytes one row is locked and deduplicated by, from its key cells in
    /// [`Self::fields`] order: one column's stored bytes, exactly what a
    /// one-column key always used, or the framed tuple of several (each
    /// component `encode_canonical`, which is length-prefixed, so `(1, 23)`
    /// and `(12, 3)` cannot meet). The same function on every side — the
    /// locker, the writer that checks, the deadlock detector's blockers — so
    /// they name one row one way.
    ///
    /// `Err` for a row whose key holds a NULL: it names no row (a unique key
    /// admits any number of NULLs), and the bytes a NULL travels as would
    /// name every NULL row at once.
    ///
    /// **Every cell must be at its column's declared scale**, never rescaled:
    /// the select pipeline's, and `run_subquery_scaled` with `normalize`
    /// false. A sixteen-byte decimal does not say its scale, and the paths
    /// differ: `NUMERIC(10,2)`'s 1.50 is 150 in the pipeline and was
    /// 1500000000 through a normalising subquery, so a lock keyed by one and
    /// checked by the other never met; normalising also rounds past nine
    /// places, so two `NUMERIC(20,12)` keys became one. A decimal is keyed by
    /// its value: the unscaled integer with trailing zeros stripped, and the
    /// scale left (a seventeen-byte cell's own scale where it carries one).
    fn row_key(&self, cells: &[&crate::core::row::row::Column]) -> Result<Vec<u8>, String> {
        if let Some((field, _)) =
            self.fields.iter().zip(cells).find(|(_, c)| c.column_type == crate::core::db_type::DbType::NULL)
        {
            return Err(format!(
                "a row whose key column '{}' is NULL cannot be identified by its key",
                field.name
            ));
        }
        let value = |i: usize, cell: &crate::core::row::row::Column| -> Vec<u8> {
            let bytes = cell.data.into_slice();
            if self.fields[i].db_type != crate::core::db_type::DbType::DECIMAL {
                return bytes.to_vec();
            }
            let Some((mut v, own)) = crate::core::value_codec::split_decimal_cell(bytes) else {
                return bytes.to_vec();
            };
            let mut scale = own.unwrap_or(self.scales[i]);
            while scale > 0 && v % 10 == 0 {
                v /= 10;
                scale -= 1;
            }
            if v == 0 {
                scale = 0;
            }
            let mut out = v.to_le_bytes().to_vec();
            out.push(scale as u8);
            out
        };
        if let [only] = cells {
            return Ok(value(0, only));
        }
        let mut out = Vec::new();
        for (i, (field, cell)) in self.fields.iter().zip(cells).enumerate() {
            let bytes = value(i, cell);
            let canonical = field.db_type != crate::core::db_type::DbType::DECIMAL
                && crate::core::index::key::encode_canonical(&field.db_type, &bytes, &mut out).is_some();
            if !canonical {
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(&bytes);
            }
        }
        Ok(out)
    }

    /// `a = 1 AND b = 'x'`: the predicate that names the one row these key
    /// cells (in [`Self::fields`] order) belong to. On a partitioned table it
    /// pins every partition-key column, so the write it guards is routed to
    /// that row's one partition.
    ///
    /// A decimal is written at its declared scale, every place kept, so the
    /// predicate names exactly the stored value.
    fn predicate(&self, cells: &[&crate::core::row::row::Column]) -> Result<String, String> {
        self.row_key(cells)?;
        let mut terms = Vec::with_capacity(self.fields.len());
        for ((field, cell), scale) in self.fields.iter().zip(cells).zip(&self.scales) {
            let literal = joined_literal(cell, Some(std::slice::from_ref(scale)), 0)
                .ok_or_else(|| "a joined key cannot be written back".to_string())?;
            terms.push(format!("{} = {}", field.name, literal));
        }
        Ok(terms.join(" AND "))
    }
}

/// Marks the rows an `UPDATE` or `DELETE` stamps, by their row-lock key,
/// before each is stamped (`RowLocks::mark_row_writes`). Built once per
/// statement by the table's write loops; `None` for a table with no key a row
/// lock can name, which no locker can lock row by row either.
pub(crate) struct RowMarker {
    locks: std::sync::Arc<crate::core::mvcc::row_locks::RowLocks>,
    table: String,
    key: IdentifyingKey,
    /// Each key field's nullability companion, by write order.
    flags: Vec<Option<u64>>,
    transactions: std::sync::Arc<crate::core::mvcc::transaction::TransactionManager>,
    /// The owner's mark already covers the whole table: a row need only be
    /// checked, and only while some row is locked.
    table_marked: std::sync::atomic::AtomicBool,
}

impl RowMarker {
    pub(crate) fn for_table(
        schema: &crate::core::row::schema::TableSchema,
        transactions: &std::sync::Arc<crate::core::mvcc::transaction::TransactionManager>,
    ) -> Option<Self> {
        let Some(locks) = transactions.row_locks() else {
            // Every database ties its row locks to its transaction manager
            // where it builds both (`Database::new`). A manager made anywhere
            // else turns row marking off, and with it the lost-update guard
            // of the no-row-lock fast path: said once, not silently. (Not a
            // debug assertion: unit tests version tables under a bare
            // manager, with no row locks and no lockers.)
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                log::warn!("a transaction manager has no row locks tied; writes are not marked for FOR UPDATE")
            });
            return None;
        };
        let key = identifying_key(schema)?;
        let flags = key
            .fields
            .iter()
            .map(|f| crate::core::rql::nullable::flag_field(&f.name, &schema.fields).map(|g| g.write_order))
            .collect();
        Some(Self {
            locks,
            table: schema.name.clone(),
            key,
            flags,
            transactions: transactions.clone(),
            table_marked: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Whether the next row needs [`RowMarker::mark`] at all: not once the
    /// table is marked whole while no row is locked anywhere — a locker that
    /// comes later finds the table's mark. Lets a bulk `DELETE` skip reading
    /// each row's key.
    pub(crate) fn needed(&self) -> bool {
        !(self.table_marked.load(std::sync::atomic::Ordering::Relaxed) && self.locks.is_empty())
    }

    /// The columns [`RowMarker::mark`] reads, for a write loop that has not
    /// fetched the row.
    pub(crate) fn fetch(&self, fields: &[crate::core::row::schema::SchemaField]) -> crate::core::row::row::RowFetch {
        let calc = crate::core::row::schema::SchemaCalculator::default();
        let flags = self.flags.iter().flatten().filter_map(|wo| fields.iter().find(|f| f.write_order == *wo));
        crate::core::row::row::RowFetch {
            columns_fetching_data: self
                .key
                .fields
                .iter()
                .chain(flags)
                .map(|f| {
                    let (column_offset, schema_id) = calc.calculate_schema_offset(&f.name, fields);
                    crate::core::row::row::ColumnFetchingData {
                        column_offset,
                        column_type: f.db_type.physical(),
                        size: f.size,
                        schema_id,
                    }
                })
                .collect(),
        }
    }

    /// Marks `row` (its key columns fetched as stored) as being written by
    /// `tx`, or refuses with the transaction holding its row lock. A row
    /// whose key is NULL names no lockable row and is not marked.
    pub(crate) fn mark(&self, row: &crate::core::row::row::Row, tx: TxId) -> Result<(), (String, TxId)> {
        if !self.needed() {
            return Ok(());
        }
        let cell = |wo: u64| row.columns.iter().find(|c| c.schema_id as u64 == wo);
        let mut cells = Vec::with_capacity(self.key.fields.len());
        for (field, flag) in self.key.fields.iter().zip(&self.flags) {
            if flag.and_then(cell).is_some_and(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0) {
                return Ok(());
            }
            match cell(field.write_order) {
                Some(c) => cells.push(c),
                None => return Ok(()),
            }
        }
        let Ok(key) = self.key.row_key(&cells) else {
            return Ok(());
        };
        // By outcome alone, unlike a locker's checks (`acquire_row_lock`): a
        // holder whose commit is recorded and not yet retired may be passed
        // here. If it wrote this row, the version this writer is stamping
        // carries its `xmax`, and `Table::check_write_conflict`, which runs
        // first, has refused it; if it wrote nothing here, its lock guarded
        // nothing that changed. Nothing is freed on the way: `mark_row_writes`
        // only reads the holders.
        let live = |candidate: TxId| {
            matches!(
                self.transactions.status().outcome(candidate),
                crate::core::mvcc::transaction::TxOutcome::InProgress
            )
        };
        match self.locks.mark_row_writes(&self.table, &[&key], tx, &live) {
            Ok(crate::core::mvcc::row_locks::Marked::Rows) => Ok(()),
            Ok(crate::core::mvcc::row_locks::Marked::Table) => {
                self.table_marked.store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
            Err(holder) => Err((
                format!(
                    "row of '{}' is locked by transaction {}; the statement that locked it must finish first",
                    self.table, holder
                ),
                holder,
            )),
        }
    }
}

/// The key that identifies `schema`'s rows, chosen as PostgreSQL names a row
/// by its key, or `None` when the table has none to name a row by.
///
/// 1. The primary key: one column (`schema.primary_key`), or a composite one,
///    which is the table-level key named `<table>_pkey` (`lexer_ct` records
///    only a one-column primary key as such).
/// 2. Else a `UNIQUE` key, inline or table-level, whose every column is
///    `NOT NULL`: the fewest columns, then the first declared.
/// 3. Else, as before composite keys counted, a one-column `UNIQUE` that may
///    hold NULL. Its NULL rows cannot be named, and [`IdentifyingKey::row_key`]
///    refuses them one by one rather than naming every NULL row at once.
///
/// A primary key's rows hold no NULL in PostgreSQL. Here a column of a
/// table-level `PRIMARY KEY (a, b)` written without `NOT NULL` is nullable,
/// so the same refusal covers such a row. A partial key is never chosen:
/// rows outside its predicate share its values, and writing back by it would
/// reach every one of them.
fn identifying_key(schema: &crate::core::row::schema::TableSchema) -> Option<IdentifyingKey> {
    let live = |name: &str| {
        schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
            .cloned()
    };
    let resolve = |columns: &[String]| columns.iter().map(|c| live(c)).collect::<Option<Vec<_>>>();
    let whole = || schema.unique_keys.iter().filter(|k| !k.is_deleted && k.predicate.is_none());

    let key = |fields: Vec<crate::core::row::schema::SchemaField>| {
        let scales = fields
            .iter()
            .map(|f| u32::from(crate::core::rql::type_modifier::scale_of(&f.name, &schema.type_modifiers)))
            .collect();
        IdentifyingKey { fields, scales }
    };
    if let Some(field) = schema.primary_key.as_deref().and_then(live) {
        return Some(key(vec![field]));
    }
    let pkey = crate::core::rql::unique_key::pkey_name(&schema.name);
    if let Some(fields) = whole().find(|k| k.name.eq_ignore_ascii_case(&pkey)).and_then(|k| resolve(&k.columns)) {
        return Some(key(fields));
    }

    let candidates = || {
        schema
            .fields
            .iter()
            .filter(|f| f.is_unique && !f.is_deleted)
            .map(|f| vec![f.clone()])
            .chain(whole().filter_map(|k| resolve(&k.columns)))
    };
    let not_null = |fields: &Vec<crate::core::row::schema::SchemaField>| {
        fields.iter().all(|f| !crate::core::rql::nullable::is_nullable(&f.name, &schema.fields))
    };
    let mut best: Option<Vec<crate::core::row::schema::SchemaField>> = None;
    for fields in candidates().filter(not_null) {
        if best.as_ref().is_none_or(|b| fields.len() < b.len()) {
            best = Some(fields);
        }
    }
    best.or_else(|| candidates().find(|fields| fields.len() == 1)).map(key)
}

/// [`identifying_key`] as the columns (by write order) and scales a row
/// lock's key is framed from: what must not change under a transaction's
/// row locks.
fn identifying_shape(schema: &crate::core::row::schema::TableSchema) -> Option<Vec<(u64, u32)>> {
    identifying_key(schema).map(|k| k.fields.iter().map(|f| f.write_order).zip(k.scales).collect())
}

/// The schema `plan` would leave, for the actions that can change which key
/// identifies a row (a UNIQUE or PRIMARY KEY added or dropped, NOT NULL set
/// or dropped, a column dropped); `None` for every other action, which
/// leaves the key as it is. Built in memory before anything is written, so
/// [`orphans_row_locks`] can compare the key before and after.
fn schema_after_for_identity(
    plan: &crate::core::rql::lexer_alter::AlterTable,
    schema: &crate::core::row::schema::TableSchema,
) -> Option<crate::core::row::schema::TableSchema> {
    use crate::core::rql::lexer_alter::AlterAction;
    use crate::core::rql::nullable;
    let mut after = schema.clone();
    match &plan.action {
        AlterAction::ConstraintClause { drop: false, description } => {
            let (clause, _) = crate::core::rql::unique_key::strip_if_not_exists(description);
            let key = crate::core::rql::unique_key::parse_clause(&clause, &plan.table_name).ok()??;
            let primary = crate::core::rql::unique_key::is_primary_key_clause(&clause);
            if primary && has_primary_key(&after, &plan.table_name) {
                return None; // refused: 42P16
            }
            if primary
                && key.columns.len() > 1
                && !key.name.eq_ignore_ascii_case(&crate::core::rql::unique_key::pkey_name(&plan.table_name))
            {
                return None; // refused: 0A000
            }
            if after.unique_keys.iter().any(|k| !k.is_deleted && k.name.eq_ignore_ascii_case(&key.name)) {
                return None;
            }
            if primary {
                // As `add_primary_key` leaves it: NOT NULL, and a one-column
                // key in the header.
                for column in &key.columns {
                    let flag = nullable::flag_column_name(column);
                    for f in after.fields.iter_mut().filter(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&flag)) {
                        f.is_deleted = true;
                    }
                }
                if let [column] = key.columns.as_slice() {
                    after.primary_key = after
                        .fields
                        .iter()
                        .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column))
                        .map(|f| f.name.clone());
                }
            }
            after.unique_keys.push(key);
        }
        AlterAction::ConstraintClause { drop: true, description } => {
            // As the DROP arm matches it: the primary key by its own name.
            let name = dropped_constraint_name(description)?;
            let pk = primary_key_constraint(schema, &plan.table_name);
            let pk_named = pk.as_ref().is_some_and(|p| p.name.eq_ignore_ascii_case(&name));
            for key in after.unique_keys.iter_mut().filter(|k| k.name.eq_ignore_ascii_case(&name)) {
                key.is_deleted = true;
            }
            let inline_pk = pk.as_ref().filter(|p| p.inline).and_then(|p| p.column.clone());
            let implicit = implicit_unique_column(&name, &plan.table_name, &after.fields)
                .filter(|c| !inline_pk.as_deref().is_some_and(|p| p.eq_ignore_ascii_case(c)));
            let unflag = implicit.or_else(|| inline_pk.filter(|_| pk_named));
            if let Some(column) = unflag {
                for f in after.fields.iter_mut().filter(|f| f.name.eq_ignore_ascii_case(&column)) {
                    f.is_unique = false;
                }
            }
            if pk_named {
                after.primary_key = None;
            }
        }
        AlterAction::AlterColumn { name, detail } if detail.to_ascii_uppercase().contains("NOT NULL") => {
            let flag = nullable::flag_column_name(name);
            if detail.trim_start().to_ascii_uppercase().starts_with("DROP") {
                if !nullable::is_nullable(name, &after.fields) {
                    after.fields.push(crate::core::row::schema::SchemaField::new(
                        flag,
                        nullable::FLAG_TYPE,
                        nullable::FLAG_TYPE.get_size(),
                        0,
                        u64::MAX,
                        false,
                    ));
                }
            } else {
                for f in after.fields.iter_mut().filter(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&flag)) {
                    f.is_deleted = true;
                }
            }
        }
        AlterAction::DropColumn { name, .. } => {
            let flag = nullable::flag_column_name(name);
            for f in after
                .fields
                .iter_mut()
                .filter(|f| !f.is_deleted && (f.name.eq_ignore_ascii_case(name) || f.name.eq_ignore_ascii_case(&flag)))
            {
                f.is_deleted = true;
            }
        }
        _ => return None,
    }
    Some(after)
}

/// What `plan` would do to the row locks a transaction holds on its table:
/// they are keyed by the table's name and its identifying key's bytes, and
/// nothing re-keys them, so a RENAME TO, or a change of which key
/// identifies a row (compared before and after: [`identifying_shape`]),
/// would leave them naming rows no lookup finds, and another session could
/// write a row this transaction holds. `None` when the locks stay valid.
fn orphans_row_locks(
    plan: &crate::core::rql::lexer_alter::AlterTable,
    schema: &crate::core::row::schema::TableSchema,
) -> Option<&'static str> {
    if matches!(plan.action, crate::core::rql::lexer_alter::AlterAction::RenameTable { .. }) {
        return Some("RENAME TO");
    }
    let after = schema_after_for_identity(plan, schema)?;
    (identifying_shape(schema) != identifying_shape(&after)).then_some("change the key that identifies its rows")
}

/// The refusal a DDL gets from a transaction holding row locks on `table`
/// that the DDL would orphan: PostgreSQL's 55006, object in use.
fn row_locks_in_use(table: &str, what: &str) -> Vec<u8> {
    ddl_error(&format!(
        "cannot {what} on \"{table}\" because it is being used by active queries in this session: the transaction \
         holds row locks on the table, which this would orphan; end the transaction first"
    ))
}

/// How many times a write replays a conflict before reporting it.
///
/// This used to be five, with backoff capped at 40 ms — about 120 ms in total
/// — on the reasoning that "a conflict clears as soon as the transaction that
/// won ends, so the retries only have to outlast one other statement". That is
/// true of an autocommit winner and false of the shape applications actually
/// use: a winner inside an explicit block holds its row until the client sends
/// `COMMIT`, which is a network round trip away and may be several statements
/// away. Against that, 120 ms simply runs out, and a `40001` reaches a client
/// that would have waited happily — PostgreSQL blocks until the other
/// transaction ends rather than budgeting at all.
///
/// Twelve attempts with the backoff below is about 1.5 s, which outlasts an
/// ordinary block without turning a genuinely stuck row into a hang. It is a
/// bound on *waiting*, not on correctness: the replay only ever re-runs a
/// statement that has applied nothing, so a caller either gets its write or an
/// error, never a partial one.
///
/// Since slice B6 (finding L1) a conflict that names the transaction it met
/// waits on that transaction instead (`conflict_wait`), and such waits are
/// bounded by `conflict_wait::CONFLICT_WAIT_BUDGET` rather than counted
/// here. This bounds the replays that waited for nothing: a blocker that had
/// already ended, or a conflict that named none.
const MAX_CONFLICT_RETRIES: usize = 12;

/// Whether a replayed write's answer reports that it changed nothing.
///
/// Either a rows-affected count of zero, or a `RETURNING` answer carrying no
/// rows.
fn retry_matched_nothing(response: &[u8]) -> bool {
    match response.first() {
        Some(1) if response.len() >= 9 => {
            let mut n = [0u8; 8];
            n.copy_from_slice(&response[1..9]);
            u64::from_le_bytes(n) == 0
        }
        Some(2) => response.len() <= 1,
        _ => false,
    }
}


/// How much unwritten outcome bitmap is allowed to pile up before a read-only
/// statement pays to fold it away. Four transactions to the byte, so this is
/// about a million of them.
const OUTCOME_FOLD_BYTES: usize = 256 * 1024;

/// Writes out the accumulated outcome bits once they have grown large.
///
/// Read-only statements skip the flush, so nothing clears the dirty range and
/// it grows with the id counter. Left alone the next writing statement would
/// have to push the whole accumulation out in one write — a day of ten thousand
/// reads a second is a couple of hundred megabytes of it. This bounds that at
/// one `fdatasync` per million or so read-only statements, which is not a cost
/// anything can measure.
fn fold_outcomes_if_large(database: &Arc<Database>) {
    if database.transactions.dirty_outcome_bytes() < OUTCOME_FOLD_BYTES {
        return;
    }
    // Best effort. The bits are still in memory, and the next statement that
    // has to be durable writes them for real.
    if let Err(e) = database.transactions.flush() {
        error!("Could not fold the accumulated transaction outcomes: {}", e);
    }
}

/// The text a statement's known table set is read from (§7 Acquisition),
/// for the statements that read or write tables by name. `None` for the
/// rest: a prepared parse locks its one table where it reads it, `LOCK
/// TABLE` and `VACUUM` lock in their own arms, and the surfaces that touch
/// no table (filters, replication, introspection, …) take none.
fn statement_lock_text(query: &QueryPurpose) -> Option<&str> {
    match query {
        QueryPurpose::QueryRows(d) => Some(&d.query),
        QueryPurpose::InsertRow(d) => Some(&d.query),
        QueryPurpose::UpdateRow(sql)
        | QueryPurpose::DeleteRow(sql)
        | QueryPurpose::CreateIndex(sql)
        | QueryPurpose::DropTable(sql)
        | QueryPurpose::AddColumn(sql)
        | QueryPurpose::DropColumn(sql)
        | QueryPurpose::RenameColumn(sql) => Some(sql),
        // `CREATE TABLE n AS SELECT … FROM t` reads t (`lock_set`); a plain
        // CREATE TABLE's set is empty there, and its arm locks its own name
        // and its FK parents.
        QueryPurpose::CreateTable(sql) => Some(sql),
        _ => None,
    }
}

/// Takes the statement's whole known table set at once, before any part of
/// it runs (§7 Acquisition; `lock_set`): every relation it names at any
/// depth, with the foreign-key parents of every table it writes (ACCESS
/// SHARE, the parent check's mode), deduplicated and in name order. What the
/// text cannot show is taken late, where it is reached.
pub(crate) async fn lock_statement_set(database: &Arc<Database>, sql: &str) -> Result<(), String> {
    let statement_owner = owner();
    if statement_owner.is_none() {
        return Ok(());
    }
    // A single-table write's set is its target, which its arm takes; its
    // FK parents are taken where their check reads them (a late find, as §7
    // Acquisition lists it).
    if crate::core::rql::lock_set::is_single_target_write(sql) {
        return Ok(());
    }
    let mut set = crate::core::rql::lock_set::statement_lock_set(sql);
    if set.is_empty() {
        return Ok(());
    }
    if refused_on_a_partitioned_table(sql, database) {
        return Ok(());
    }
    let parents: Vec<(String, TableMode)> = set
        .iter()
        .filter(|(_, mode)| *mode == TableMode::RowExclusive)
        .filter_map(|(name, _)| database.schema_view(name))
        .flat_map(|view| {
            view.schema()
                .foreign_keys
                .iter()
                .map(|fk| (fk.parent_table.clone(), TableMode::AccessShare))
                .collect::<Vec<_>>()
        })
        .collect();
    set.extend(parents);
    // A DDL's complete lock set (§7 step 1): the names its text shows plus
    // what only the schemas can say (`ddl_lock_extras`), every name locked
    // whether or not a table is registered under it yet, all in one name
    // order. So a RENAME takes its old and new names together, and two
    // crossed RENAMEs, or a RENAME and a CREATE of its target, serialise
    // instead of deadlocking on the order they asked in.
    if let Some(extras) = ddl_lock_extras(sql, database) {
        set.extend(extras);
        return database.lock_names(&set, statement_owner).await;
    }
    database.tables_for(&set, statement_owner).await
}

/// `CREATE TABLE`'s lock set: ACCESS EXCLUSIVE on the name it creates,
/// which serialises it against a concurrent CREATE, DROP or RENAME of that
/// name and keeps statements queued behind a DROP of it from meeting the new
/// table (they reach the name first; see `table_for`), and SHARE on every
/// parent its foreign keys reference (finding L11: a child appearing beside
/// a parent's DELETE or DDL in flight; SHARE for `alter_lock_set`'s reason).
/// In name order.
async fn lock_create_target(
    database: &Arc<Database>,
    create_table: &crate::core::rql::lexer_ct::CreateTable,
) -> Result<(), String> {
    let mut set = vec![(create_table.table_name.clone(), TableMode::AccessExclusive)];
    set.extend(
        create_table
            .foreign_keys
            .iter()
            .map(|fk| (fk.parent_table.clone(), TableMode::Share)),
    );
    database.lock_names(&set, owner()).await
}

/// Whether `sql` is an ALTER TABLE its arm refuses on a partitioned table
/// before reading anything (`Database::partitioned_alter_refusal`: what
/// would rewrite every row, a key column's change, a foreign key, a UNIQUE
/// without every key column): such a statement takes no lock, so it is
/// refused at once instead of first waiting for every reader of the table.
/// The metadata forms lock as on a plain table (docs/partitioning.md §11).
fn refused_on_a_partitioned_table(sql: &str, database: &Database) -> bool {
    let first = sql.split_whitespace().next().unwrap_or("");
    first.eq_ignore_ascii_case("ALTER")
        && crate::core::rql::lexer_alter::parse_alter_table_actions(sql)
            .is_ok_and(|plans| plans.iter().any(|plan| database.partitioned_alter_refusal(plan).is_some()))
}

/// What an `ALTER TABLE` or `DROP TABLE` locks beyond the names its text
/// shows (§7 step 1), or `None` for any other statement: for ALTER, each
/// action's [`alter_lock_set`]. Read from the schemas with no lock, so the
/// arm reads it again under its ACCESS EXCLUSIVE and takes anything new
/// before it writes.
fn ddl_lock_extras(sql: &str, database: &Database) -> Option<Vec<(String, TableMode)>> {
    let first = sql.split_whitespace().next()?;
    if first.eq_ignore_ascii_case("DROP") {
        // `DROP TABLE p CASCADE` changes every table whose foreign keys name
        // `p` (phase 3 P4): each one, ACCESS EXCLUSIVE, in the one name order.
        if drop_table_cascades(sql) {
            if let Ok((name, _)) = crate::core::rql::lexer_alter::parse_drop_table(sql) {
                return Some(
                    database
                        .fk_children(&name)
                        .into_iter()
                        .filter(|child| !child.eq_ignore_ascii_case(&name))
                        .map(|child| (child, TableMode::AccessExclusive))
                        .collect(),
                );
            }
        }
        return Some(Vec::new());
    }
    if !first.eq_ignore_ascii_case("ALTER") {
        return None;
    }
    let plans = crate::core::rql::lexer_alter::parse_alter_table_actions(sql).ok()?;
    Some(plans.iter().flat_map(|plan| alter_lock_set(plan, database)).collect())
}

/// The rest of one ALTER action's complete lock set (§7 step 1, finding
/// L11), beyond ACCESS EXCLUSIVE on its own table:
/// - `RENAME TO new`: the new name, ACCESS EXCLUSIVE;
/// - an action that may rewrite other tables' foreign-key sidecars, through
///   `column_declarations`: for `RENAME TO` every FK child of the table, for
///   `ADD`, `DROP` and `RENAME COLUMN` the children whose keys reference that
///   column; ACCESS EXCLUSIVE, since each is reloaded;
/// - `ADD` or `DROP` of a foreign key: its parent, SHARE. PostgreSQL takes
///   SHARE ROW EXCLUSIVE there; SHARE is the mode of the four that conflicts
///   with what matters (a parent DELETE's ROW EXCLUSIVE, a parent DDL's
///   ACCESS EXCLUSIVE) without making the DDL wait for the parent's readers.
///   ACCESS EXCLUSIVE, the never-weaker mapping, would.
pub(crate) fn alter_lock_set(
    plan: &crate::core::rql::lexer_alter::AlterTable,
    database: &Database,
) -> Vec<(String, TableMode)> {
    use crate::core::rql::lexer_alter::AlterAction;
    let table = plan.table_name.as_str();
    let children = |columns: Option<&[&str]>| {
        database
            .fk_children_on(table, columns)
            .into_iter()
            .map(|child| (child, TableMode::AccessExclusive))
            .collect::<Vec<_>>()
    };
    match &plan.action {
        AlterAction::RenameTable { to } => {
            let mut set = children(None);
            set.push((to.clone(), TableMode::AccessExclusive));
            set
        }
        // The children `column_declarations` reads and rewrites for the
        // column: those whose keys reference it (or, for a RENAME, its new
        // name's leftovers), never the rest.
        AlterAction::AddColumn { name, .. } | AlterAction::DropColumn { name, .. } => {
            children(Some(&[name.as_str()]))
        }
        AlterAction::RenameColumn { from, to } => children(Some(&[from.as_str(), to.as_str()])),
        AlterAction::ConstraintClause { drop: false, description } => fk_parent_of_clause(description)
            .map(|parent| vec![(parent, TableMode::Share)])
            .unwrap_or_default(),
        // Validating a key reads the parent: SHARE on it, as ADD takes, so
        // no parent row goes while the child's rows are checked.
        AlterAction::ValidateConstraint { name } => database
            .schema_view(table)
            .map(|view| {
                view.schema()
                    .foreign_keys
                    .iter()
                    .filter(|fk| !fk.is_deleted && fk.name.eq_ignore_ascii_case(name))
                    .map(|fk| (fk.parent_table.clone(), TableMode::Share))
                    .collect()
            })
            .unwrap_or_default(),
        AlterAction::ConstraintClause { drop: true, description } => {
            let Some(name) = dropped_constraint_name(description) else {
                return Vec::new();
            };
            database
                .schema_view(table)
                .map(|view| {
                    view.schema()
                        .foreign_keys
                        .iter()
                        .filter(|fk| !fk.is_deleted && fk.name.eq_ignore_ascii_case(&name))
                        .map(|fk| (fk.parent_table.clone(), TableMode::Share))
                        .collect()
                })
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

/// The name `ALTER TABLE … DROP CONSTRAINT [IF EXISTS] <name>` drops, read
/// as the constraint arm reads it.
fn dropped_constraint_name(description: &str) -> Option<String> {
    crate::core::rql::unique_key::dropped_constraint(description).ok().map(|(name, _)| name)
}

/// The parent an `ADD [CONSTRAINT n] FOREIGN KEY … REFERENCES p` names,
/// parsed as the constraint arm parses it; `None` for any other clause.
fn fk_parent_of_clause(description: &str) -> Option<String> {
    // Whole words outside quotes, as the constraint arm reads it.
    let find = |word: &str| crate::core::rql::join::find_keyword(description, word, 0);
    let clause_at = find("FOREIGN").or_else(|| find("REFERENCES"))?;
    crate::core::rql::foreign_key::parse_clause(&description[clause_at..], "", None, None)
        .ok()
        .map(|fk| fk.parent_table)
}

/// A DDL statement of an explicit block, from before it runs to its end
/// (finding L5, docs/persistent-indexes.md §7 "As built (B5)").
///
/// DDL is not transactional here: what a DDL statement changed is changed
/// whether the block commits or not. So the ACCESS EXCLUSIVE it took on a
/// table is not held to the block's end, as PostgreSQL holds it, which
/// would stop every reader of the table for as long as the block stays
/// open. At the statement's end each table it newly holds ACCESS EXCLUSIVE
/// on goes back to what the block needs there:
/// - **downgraded to ROW EXCLUSIVE** when the block has written the table's
///   file set (it holds ROW EXCLUSIVE beside the AE: it wrote the table
///   before, or the rewrite carried its versions, `rewrite_rows`). Its
///   uncommitted versions are in the files, and its own later writes need
///   the lock; a DDL of another owner must keep waiting for the block, as
///   it waits for any writer (its index build and, from B11, its seal must
///   not run beside the block's versions), while readers go on;
/// - **released** otherwise, back to whatever the block held before (an
///   ACCESS SHARE a REPEATABLE READ block took by reading it stays).
///
/// An ACCESS EXCLUSIVE the block held before the statement (a `LOCK TABLE`,
/// an earlier statement's) is not touched, and `LOCK TABLE` itself is no
/// DDL here: it holds what it names to the block's end.
pub(crate) struct BlockDdl {
    owner: crate::core::mvcc::table_locks::LockOwner,
    held_before: Vec<String>,
}

/// Whether `sql` is a DDL statement for [`BlockDdl`]: one that takes ACCESS
/// EXCLUSIVE because it changes a table (ALTER, CREATE, DROP), not one that
/// asks for the lock itself.
fn is_block_ddl(sql: &str) -> bool {
    let head = sql
        .trim_start_matches(|c: char| c.is_whitespace() || c == '(')
        .split(|c: char| c.is_whitespace() || c == ';')
        .next()
        .unwrap_or("");
    ["ALTER", "CREATE", "DROP"].iter().any(|k| head.eq_ignore_ascii_case(k))
}

impl BlockDdl {
    /// Before a block statement runs: `None` unless it is DDL.
    pub(crate) fn begin(database: &Database, root: TxId, sql: &str) -> Option<BlockDdl> {
        if !is_block_ddl(sql) {
            return None;
        }
        let owner = crate::core::mvcc::table_locks::LockOwner::block(database.transactions.top_level(root))?;
        Some(BlockDdl {
            owner,
            held_before: database.row_locks.table_locks().exclusive_tables(owner),
        })
    }

    /// After it, on every way out (an error too: a DDL that failed may have
    /// taken its lock and written nothing, and holds nothing then).
    pub(crate) fn end(self, database: &Database) {
        use crate::core::mvcc::table_locks::TableMode;
        let locks = database.row_locks.table_locks();
        for table in locks.exclusive_tables(self.owner) {
            if self.held_before.contains(&table) {
                continue;
            }
            if locks.holds(self.owner, &table).contains(TableMode::RowExclusive) {
                locks.downgrade(self.owner, &table, TableMode::AccessExclusive, TableMode::RowExclusive);
            } else {
                locks.release_mode(self.owner, &table, TableMode::AccessExclusive);
            }
        }
    }
}

pub async fn execute(query: QueryPurpose, database: Arc<Database>) -> Vec<u8> {
    execute_with_sync(query, database, crate::synchronous_commit()).await
}

/// [`execute`] as one statement of a named internal owner (§7 Owner): login
/// role lookups and the role catalogue's bootstrap, the importer's DDL, the
/// native protocol. Inside an owner scope already, it joins that one.
pub async fn execute_as(
    kind: crate::core::mvcc::owner::OwnerKind,
    query: QueryPurpose,
    database: Arc<Database>,
) -> Vec<u8> {
    let (transactions, row_locks) = (database.transactions.clone(), database.row_locks.clone());
    crate::core::mvcc::owner::statement(&transactions, &row_locks, kind, execute(query, database)).await
}

/// `execute`, with the caller choosing whether the commit waits for its sync.
///
/// `wait` is the effective `synchronous_commit` — the server default unless the
/// session has set its own. Passing `false` does not skip the outcome record,
/// only the `fdatasync` of the database's own log: the records are in the log
/// either way, and [`crate::core::log_writer`] takes the sync within
/// [`crate::wal_writer_delay_ms`]. A crash in that window loses whole
/// transactions off the end of the log and never part of one. A writing
/// statement still syncs every *other* storage location holding unsynced
/// writes before it commits, whichever transaction wrote them — that is what
/// keeps it whole across disks (`Database::commit_written_transaction`).
pub async fn execute_with_sync(
    query: QueryPurpose,
    database: Arc<Database>,
    wait: bool,
) -> Vec<u8> {
    // One owner O for the statement, whatever protocol or internal caller it
    // came from (finding L3): the wire's scope if it opened one, otherwise a
    // new one, ended (its locks released, after the durability steps below)
    // when this returns or is dropped.
    let (transactions, row_locks) = (database.transactions.clone(), database.row_locks.clone());
    crate::core::mvcc::owner::statement(
        &transactions,
        &row_locks,
        crate::core::mvcc::owner::OwnerKind::Statement,
        execute_autocommit(query, database, wait),
    )
    .await
}

/// [`execute_with_sync`] inside its owner scope.
async fn execute_autocommit(query: QueryPurpose, database: Arc<Database>, wait: bool) -> Vec<u8> {
    // Autocommit: a statement arriving outside an explicit transaction still
    // gets one. It has to. A DELETE has to stamp `xmax` with an id that other
    // snapshots can order themselves against, and reclamation reads a bare
    // tombstone as "predates versioning, safe to reuse" — which would let it
    // free a row an open transaction is still entitled to read.
    // A write conflict on an AUTOCOMMIT statement is replayed rather than
    // reported. PostgreSQL's default isolation is READ COMMITTED, where a
    // second writer of the same row WAITS for the first and then re-evaluates
    // its predicate against the new version — it never raises `40001`. This
    // engine applies first-updater-wins at every isolation level, so two
    // ordinary concurrent updates of one row surfaced a serialization failure
    // to drivers that assume the default and do not retry (sqlx, sea-orm,
    // Hibernate): two handlers rendering one page became two failed requests.
    // Re-running the whole statement in a fresh transaction is what the wait
    // would have produced, because the failed attempt is aborted first and
    // leaves nothing behind.
    //
    // The replay is never allowed to become a SILENT wrong answer. A retry
    // that comes back having matched NOTHING is not reported as an empty
    // success — the original conflict is returned instead. The row plainly
    // existed a moment ago, so "no rows" there means the retry lost it, not
    // that it is gone, and an error the caller can retry beats a quiet lie.
    //
    // Only here, never inside an explicit block: there the client owns the
    // transaction and its snapshot, so a conflict is genuinely its business,
    // and replaying one statement of a block would break its atomicity.
    let retry_sql = query_sql(&query);
    // `InsertRow` joins the two write statements that were already replayed.
    // The safety argument is the same one stated above and is *stronger* for an
    // insert: the uniqueness check runs before the row is written, so a
    // statement it rejects has applied nothing at all. The conflict it raises
    // means "another transaction is writing this value and has not finished" —
    // a question with no answer yet, which a replay resolves once that
    // transaction commits or aborts.
    let retryable = matches!(
        query,
        QueryPurpose::UpdateRow(_) | QueryPurpose::DeleteRow(_) | QueryPurpose::InsertRow(_)
    ) && !retry_sql.is_empty();
    // The conflict clause is parsed off the statement before `retry_sql` is
    // taken, so a re-parse loses it — the same way the multi-row split did,
    // where it turned `ON CONFLICT DO NOTHING` into a duplicate-key error.
    let insert_conflict = match &query {
        QueryPurpose::InsertRow(data) => {
            Some((data.on_conflict_do_nothing, data.on_conflict_update.clone()))
        }
        _ => None,
    };
    // The statement's whole known table set, at once, before its first
    // attempt: held by O, so every retry runs under the same locks.
    if let Some(sql) = statement_lock_text(&query)
        && !crate::core::rql::lock_set::is_single_target_write(sql)
    {
        if let Err(e) = Box::pin(lock_statement_set(&database, sql)).await {
            return ddl_error(&e);
        }
    }
    let mut pending: Option<QueryPurpose> = Some(query);
    // The first conflict seen, kept so a later attempt that matches nothing can
    // report it rather than a misleading empty success.
    let mut first_conflict: Option<Vec<u8>> = None;
    let mut attempt = 0usize;
    // O, begun before the first attempt and older than each (§7 Owner). The
    // first attempt runs as O itself (slice B8b): one id per statement. A
    // later attempt (and every attempt of a loop whose O was already taken:
    // a nested statement, a scope's second statement) acts for O through an
    // alias held across its commit or abort: the statement's table locks and
    // graph node are O's and survive a retry, while each attempt's row locks
    // are its own and end with it (L4). Before O's own transaction is aborted
    // with the statement going on, `owner::outlive` registers O as live past
    // it, so nothing prunes the locks the statement still holds.
    let mut first = crate::core::mvcc::owner::claim_attempt();
    let owner = crate::core::mvcc::owner::ensure().map(|o| o.owner);
    let mut _alias: Option<crate::core::mvcc::row_locks::AliasGuard<'_>> = None;
    // Each conflict waits on the transaction it met, with a wait-for edge
    // from O (slice B6, finding L1): see `conflict_wait`.
    let mut pacing = crate::core::rql::conflict_wait::Pacing::new(crate::core::rql::conflict_wait::Site::Autocommit);
    let (tx, response) = loop {
        let tx = match first.take() {
            Some(o) => o,
            None => database.transactions.begin_autocommit(),
        };
        _alias = owner.map(|o| database.row_locks.owner_alias(tx, o));
        crate::owner_probe!(crate::core::mvcc::owner::probe::Site::Attempt, tx, database);

        // The purpose is consumed by each attempt, so later ones re-read it
        // from the statement text. `retryable` already established that the
        // text is non-empty and was recognised once.
        let purpose = match pending.take() {
            Some(p) => Some(p),
            None => recognize_query_purpose(&retry_sql).map(|mut reparsed| {
                if let Some((do_nothing, update)) = insert_conflict.clone() {
                    if let QueryPurpose::InsertRow(data) = &mut reparsed {
                        data.on_conflict_do_nothing = do_nothing;
                        data.on_conflict_update = update;
                    }
                }
                reparsed
            }),
        };
        let Some(purpose) = purpose else {
            break (
                tx,
                first_conflict
                    .take()
                    .unwrap_or_else(|| ddl_error("write conflict")),
            );
        };

        // A panic inside the statement is caught here, so its transaction is
        // aborted like any failed statement's. Unwinding past this point used
        // to leave it `InProgress` — invisible writes nobody would resolve, and
        // `oldest_active` pinned below it for the life of the process.
        //
        // A prepared `SELECT` is boxed on its own. Boxing it through
        // `execute_with_context` allocated and copied a future sized for every
        // statement arm, on the per-execution path of a statement whose whole
        // dispatch is a table lookup and the scan.
        let (response, blocker) = {
            use futures::FutureExt;
            // The blocker is read only to pace a replay, so a statement this
            // loop never replays (a read, DDL) runs without the capture's
            // task-local scope, which is entered and left on every poll
            // (slice B8b). A blocker noted inside it still reaches an
            // enclosing capture, as the capture would have passed it on.
            let (caught, blocker) = match purpose {
                QueryPurpose::PreparedRows(prepared) => (
                    std::panic::AssertUnwindSafe(Box::pin(execute_prepared_rows(
                        prepared,
                        database.clone(),
                        tx,
                        SubqueryNeed::All,
                    )))
                    .catch_unwind()
                    .await,
                    None,
                ),
                purpose if !retryable => (
                    std::panic::AssertUnwindSafe(Box::pin(execute_with_context(
                        purpose,
                        database.clone(),
                        0,
                        tx,
                    )))
                    .catch_unwind()
                    .await,
                    None,
                ),
                purpose => {
                    crate::core::rql::conflict_wait::capture(
                        std::panic::AssertUnwindSafe(execute_attempt(purpose, database.clone(), tx))
                            .catch_unwind(),
                    )
                    .await
                }
            };
            let response = match caught {
                Ok(response) => response,
                Err(_) => {
                    error!("statement panicked; its transaction {} is aborted", tx);
                    ddl_error("internal error: the statement panicked and was rolled back")
                }
            };
            (response, blocker)
        };
        let conflicted = is_retryable_conflict(&response);

        if !conflicted || !retryable {
            // A replay that matched nothing reports the original conflict. The
            // row plainly existed a moment ago, so "no rows" means the retry
            // lost it, not that it is gone, and an error the caller can retry
            // beats a quiet lie.
            if let Some(conflict) = first_conflict.take() {
                if !conflicted && retry_matched_nothing(&response) {
                    let wrote = database.transactions.wrote_family(tx);
                    crate::core::mvcc::owner::outlive(tx);
                    database.abort_transaction(tx);
                    if wrote {
                        let _ = database.transactions.flush();
                    }
                    break (tx, conflict);
                }
            }
            break (tx, response);
        }

        if first_conflict.is_none() {
            first_conflict = Some(response.clone());
        }

        // Asked before the abort, which retires the transaction and forgets
        // the answer — the ordering the commit path below is careful about.
        let attempt_wrote = database.transactions.wrote_family(tx);
        crate::core::mvcc::owner::outlive(tx);
        database.abort_transaction(tx);
        if attempt_wrote {
            let _ = database.transactions.flush();
        }

        // Waits for the transaction the conflict met (after the attempt's
        // abort, so the attempt holds nothing anyone waits for), or backs
        // off when there is none to wait for.
        let waiter = owner.map_or(tx, |o| o.id());
        match pacing.pause(&database, waiter, blocker, MAX_CONFLICT_RETRIES).await {
            crate::core::rql::conflict_wait::Pause::Retry => attempt += 1,
            crate::core::rql::conflict_wait::Pause::GiveUp => break (tx, response),
            crate::core::rql::conflict_wait::Pause::Fail(message) => break (tx, ddl_error(&message)),
        }
    };
    let _ = attempt;
    // The loop is done with `tx`: if it is O, whatever acts for O after its
    // end below registers it (`owner::ensure`); the commit registers nothing.
    crate::core::mvcc::owner::attempt_ended(tx);

    // Did this statement put anything on disk? Asked before the commit, which
    // retires the transaction and forgets the answer.
    //
    // A statement that stamped no version needs no durable outcome record:
    // nothing on disk carries this id, so a restart that finds no record of it
    // reads it as aborted, which is the truth. Skipping the flush is what takes
    // the `fdatasync` — and with it nearly all of the latency — off a `SELECT`.
    let wrote = database.transactions.wrote_family(tx);

    // The response's first byte is the status; 3 is a failure. An autocommit
    // statement that failed must leave nothing behind, and aborting is what
    // makes its versions invisible.
    if response.first() == Some(&3) {
        crate::core::mvcc::owner::outlive(tx);
        database.abort_transaction(tx);
        if wrote {
            let _ = database.transactions.flush();
        }
        return strip_conflict_marker(response);
    }

    if !wrote {
        // Nothing durable refers to this transaction, so there is nothing to
        // make durable. The outcome bits stay in memory and are folded into the
        // next writing statement's flush.
        let family = database.commit_transaction(tx);
        fold_outcomes_if_large(&database);
        database.publish_transaction(family).await;
        return response;
    }

    // Rows on every other storage location are made durable before the
    // commit, not after: once committed, any session's root flush can carry
    // this outcome to disk, and it must not get there ahead of them. In both
    // `wait` modes — see `Database::commit_written_transaction`.
    let family = match database.commit_written_transaction(tx) {
        Ok(family) => family,
        Err(e) => {
            error!("Could not make the written rows durable: {}", e);
            crate::core::mvcc::owner::outlive(tx);
            database.abort_transaction(tx);
            let _ = database.transactions.flush();
            let mut failed = vec![3u8];
            failed.extend_from_slice(
                format!(
                    "could not commit: the rows written could not be made durable ({})",
                    e
                )
                .as_bytes(),
            );
            return failed;
        }
    };

    // The outcome record goes into the log without a sync of its own, and the
    // sync below covers it along with the rows. They are records in the same
    // log, appended rows-first, and replay stops at the first one it cannot
    // read whole — so a torn tail can lose the outcome but can never keep it
    // while losing the rows. Two syncs of one file, back to back, was what an
    // `INSERT` used to cost.
    if let Err(e) = database.transactions.flush_deferred() {
        error!("Could not record the transaction outcome: {}", e);
        crate::core::mvcc::owner::outlive(tx);
        database.abort_transaction(tx);
        let mut failed = vec![3u8];
        failed.extend_from_slice(
            format!(
                "could not commit: the transaction outcome could not be made durable ({})",
                e
            )
            .as_bytes(),
        );
        return failed;
    }

    // The one sync. This statement is the point the debt is paid, because it is
    // the point the client is told the statement worked.
    //
    // Unless the session asked not to wait, in which case the records stay in
    // the page cache and the log writer syncs them on its next round. What is
    // given up is the last `wal_writer_delay_ms` of transactions on a crash;
    // what is not given up is their integrity, because the log is still written
    // in the same order and replay still stops at the first record it cannot
    // read whole.
    if wait {
        // The database's own log only: the other locations were synced before
        // the commit.
        if let Err(e) = database.sync_own_location() {
            error!("Could not make the written rows durable: {}", e);
            // Back to aborted, which is what a restart would conclude anyway, so
            // the client and the disk agree instead of the client holding a
            // third story.
            crate::core::mvcc::owner::outlive(tx);
            database.abort_transaction(tx);
            let _ = database.transactions.flush();
            let mut failed = vec![3u8];
            failed.extend_from_slice(
                format!(
                    "could not commit: the rows written could not be made durable ({})",
                    e
                )
                .as_bytes(),
            );
            return failed;
        }
    }

    // Only now, with the outcome durable, does this transaction exist as far as
    // the rest of the cluster is concerned. Publishing before the flush would
    // let peers apply a write that the branch above has just disowned.
    // Nothing happens here on a node that is not replicating.
    database.publish_transaction(family).await;

    response
}

/// Records a statement that changed data, so replication can ship it.
///
/// Called at the point a write is known to have succeeded rather than when it
/// was parsed, so a statement that failed is never replicated, and called with
/// the SQL that actually ran — which for a statement the engine rewrote (a JSON
/// document being decomposed into its shadow table, say) is the rewritten form,
/// concrete values and all. That is what keeps a generated identifier the same
/// on every node instead of being generated afresh on each.
///
/// `depth` is the foreign-key cascade depth. Only the statement the client sent
/// is recorded: a replica running it triggers the same cascades itself, and
/// shipping both would apply them twice.
/// One row of a bulk load (`IMPORT … FROM POSTGRES`), written exactly as the
/// `INSERT` arm of [`execute_with_context`] writes the single-tuple,
/// column-naming statement `sql` — the same row builder, the same checks, the
/// same replication record — without the statement-level work that does not
/// apply to such a statement: classifying it, the conflict-retry wrapper, the
/// multi-row and `INSERT … SELECT` probes, `RETURNING`, the second parse for
/// foreign keys.
///
/// `None` hands the row back to the ordinary path, which then decides it.
/// That is the answer whenever this would not be the exact same write: a
/// table with JSON columns (their shadow writes rewrite the statement), a
/// generated column, a statement the row builder refuses, and any failure at
/// all — every check here runs before the row is written, so a refused row
/// has applied nothing, and the ordinary path reports it with its own words
/// and retries a write conflict as it always does.
#[cfg(feature = "pg_import")]
pub(crate) async fn insert_loaded_row(
    database: &Arc<Database>,
    table_name: &str,
    sql: &str,
    tx: TxId,
) -> Option<()> {
    // A partitioned table takes the statement path, which routes the row.
    if database.is_partitioned(table_name) {
        return None;
    }
    // The import batch's owner takes ROW EXCLUSIVE, as the statement path's
    // INSERT would; on a lock failure the ordinary path reports it. Resolved
    // once for the seed, the checks and the write (slice B8b).
    let table = database
        .table_for(table_name, owner(), TableMode::RowExclusive)
        .await
        .ok()
        .flatten()?;
    if has_unseeded_sequence(database, table_name, &table.schema) {
        let target = crate::core::database::Relation::Plain(table.clone());
        Box::pin(seed_sequences(database, table_name, Some(&target))).await.ok()?;
    }
    let schema = &table.schema;
    if !schema.json_columns.is_empty() {
        return None;
    }
    let row_data = crate::core::rql::lexer_s1::InsertRowData {
        table_name: table_name.to_string(),
        query: sql.to_string(),
        on_conflict_do_nothing: false,
        on_conflict_target: Vec::new(),
        on_conflict_update: None,
    };
    // Asked of the schema rather than by computing the values: computing them
    // evaluates the defaults the expressions read, and a sequence advanced by
    // a probe is an id no row gets.
    if !schema.generated.is_empty() {
        return None;
    }
    let assignments = crate::core::rql::lexer_ir::assignments_of_insert(sql);
    check_not_null(
        table_name,
        &schema.fields,
        assignments.as_deref().unwrap_or_default(),
        &schema.defaults,
        &schema.generated,
        true,
    )
    .ok()?;
    if let Some(assignments) = assignments.as_deref() {
        check_foreign_keys_of(database, Some(schema), table_name, assignments, tx)
            .await
            .ok()?;
    }
    let purpose = QueryPurpose::InsertRow(row_data);
    let insert_row = crate::core::rql::lexer_ir::insert_row_with_defaults(
        &purpose,
        &schema.fields,
        &schema.defaults,
        &schema.type_modifiers,
        database.id,
    )?;
    check_constraints_hold(table_name, schema, &insert_row.columns_writing_data).ok()?;
    table.insert_row_in_txn(insert_row, tx).await.ok()?;
    note_write(database, tx, 0, sql, Some(table_name));
    Some(())
}

#[inline]
fn note_write(database: &Arc<Database>, tx: TxId, depth: usize, sql: &str, table: Option<&str>) {
    if depth == 0 {
        database.replication.note_statement(tx, sql, table);
    }
}

/// Runs a statement inside a transaction the caller owns.
///
/// The caller decides the outcome, so nothing here commits or aborts. This is
/// what an explicit `BEGIN` uses, and what a statement's own internal work —
/// a foreign-key cascade, a subquery — joins so that it sees the same rows.
/// Removes the internal retryable marker before a message reaches a client.
fn clean_conflict_text(text: &str) -> String {
    text.strip_suffix(
        crate::core::row::table::Table::<
            crate::core::storage_providers::file_sync::LocalStorageProvider,
        >::CONFLICT_RETRYABLE,
    )
    .unwrap_or(text)
    .to_string()
}

fn strip_conflict_marker(mut response: Vec<u8>) -> Vec<u8> {
    if response.first() != Some(&3) {
        return response;
    }
    let text = String::from_utf8_lossy(&response[1..]).to_string();
    if let Some(trimmed) = text.strip_suffix(
        crate::core::row::table::Table::<
            crate::core::storage_providers::file_sync::LocalStorageProvider,
        >::CONFLICT_RETRYABLE,
    ) {
        response.truncate(1);
        response.extend_from_slice(trimmed.as_bytes());
    }
    response
}

/// True when a failed statement conflicted having applied nothing, so
/// re-running it is equivalent to having waited.
fn is_retryable_conflict(response: &[u8]) -> bool {
    response.first() == Some(&3)
        && String::from_utf8_lossy(&response[1..]).ends_with(
            crate::core::row::table::Table::<
                crate::core::storage_providers::file_sync::LocalStorageProvider,
            >::CONFLICT_RETRYABLE,
        )
}

/// Runs a statement inside a caller's transaction, replaying a write conflict
/// the way `READ COMMITTED` would.
///
/// PostgreSQL does not abort the second writer of a row at the default
/// isolation level: it blocks until the first transaction ends and then
/// re-evaluates **that statement**, leaving the rest of the block untouched.
/// This engine waits for the transaction the conflict met (with a wait-for
/// edge, slice B6: `conflict_wait`) and then re-runs the statement, which is
/// equivalent precisely because the statement is only replayed when it had
/// applied nothing (see `CONFLICT_RETRYABLE`).
///
/// A `REPEATABLE READ` block pins its snapshot, so a replay there sees the
/// same world and conflicts again; the bounded attempts then surface the
/// error, which is what that level is supposed to do.
pub async fn execute_in_txn(query: QueryPurpose, database: Arc<Database>, tx: TxId) -> Vec<u8> {
    // Under the scope already in place (a statement's sub-execution joins its
    // owner); otherwise under the transaction's root, which is a block's owner
    // and is released by whoever ends the block.
    let transactions = database.transactions.clone();
    crate::core::mvcc::owner::under(
        &transactions,
        tx,
        crate::core::mvcc::owner::OwnerKind::Block,
        execute_in_txn_inner(query, database, tx),
    )
    .await
}

async fn execute_in_txn_inner(query: QueryPurpose, database: Arc<Database>, tx: TxId) -> Vec<u8> {
    let retry_sql = query_sql(&query);
    // `InsertRow` for the same reason the autocommit path replays it: the
    // uniqueness check runs before the row is written, so a statement it
    // rejects has applied nothing. A multi-row insert is the exception, and it
    // arrives here with its marker already stripped once a tuple has landed —
    // `insert_values_batch` retries at the tuple instead.
    let retryable = matches!(
        query,
        QueryPurpose::UpdateRow(_) | QueryPurpose::DeleteRow(_) | QueryPurpose::InsertRow(_)
    ) && !retry_sql.is_empty();
    // `retry_sql` has the conflict clause already parsed off it, so a re-parse
    // would turn `ON CONFLICT DO NOTHING` back into a duplicate-key error.
    let insert_conflict = match &query {
        QueryPurpose::InsertRow(data) => {
            Some((data.on_conflict_do_nothing, data.on_conflict_update.clone()))
        }
        _ => None,
    };

    // The statement's whole known table set, at once, before it runs (see
    // `execute_autocommit`); the block's owner holds it to the block's end.
    if let Some(sql) = statement_lock_text(&query)
        && !crate::core::rql::lock_set::is_single_target_write(sql)
    {
        if let Err(e) = Box::pin(lock_statement_set(&database, sql)).await {
            return ddl_error(&e);
        }
    }
    let mut query = query;
    // A conflict waits on the transaction it met, with a wait-for edge from
    // the owner (slice B6, finding L1): see `conflict_wait`.
    let mut pacing = crate::core::rql::conflict_wait::Pacing::new(crate::core::rql::conflict_wait::Site::Block);
    loop {
        // Every attempt runs under the same transaction, so under the same
        // owner: a block's root, or the statement's O its transaction acts for.
        crate::owner_probe!(crate::core::mvcc::owner::probe::Site::InTxnAttempt, tx, database);
        // See `execute_with_sync`: a prepared `SELECT` skips the box sized for
        // every statement arm.
        let (response, blocker) = match query {
            QueryPurpose::PreparedRows(prepared) => {
                crate::core::rql::conflict_wait::capture(Box::pin(execute_prepared_rows(
                    prepared,
                    database.clone(),
                    tx,
                    SubqueryNeed::All,
                )))
                .await
            }
            query => {
                crate::core::rql::conflict_wait::capture(execute_attempt(query, database.clone(), tx)).await
            }
        };
        if !retryable || !is_retryable_conflict(&response) {
            return strip_conflict_marker(response);
        }
        // Until the transaction that won ends: a real wait, in the graph.
        let waiter = database.row_locks.owner_of(tx);
        match pacing.pause(&database, waiter, blocker, MAX_CONFLICT_RETRIES).await {
            crate::core::rql::conflict_wait::Pause::Retry => {}
            crate::core::rql::conflict_wait::Pause::GiveUp => return strip_conflict_marker(response),
            crate::core::rql::conflict_wait::Pause::Fail(message) => return ddl_error(&message),
        }
        query = match recognize_query_purpose(&retry_sql) {
            Some(mut reparsed) => {
                if let Some((do_nothing, update)) = insert_conflict.clone() {
                    if let QueryPurpose::InsertRow(data) = &mut reparsed {
                        data.on_conflict_do_nothing = do_nothing;
                        data.on_conflict_update = update;
                    }
                }
                reparsed
            }
            None => return strip_conflict_marker(response),
        };
    }
}

/// `depth` counts how many foreign-key actions deep this statement is, so a
/// cycle of cascades terminates instead of recursing forever.
pub async fn execute_with_context(
    query: QueryPurpose,
    database: Arc<Database>,
    depth: usize,
    // The transaction this statement's reads and writes belong to.
    tx: TxId,
) -> Vec<u8> {
    execute_with_need(query, database, depth, tx, SubqueryNeed::All).await
}

/// How much of a `SELECT`'s result its caller will look at.
///
/// A subquery is often asked a narrower question than "every row": `EXISTS`
/// only needs to know whether there is one, `IN` only which values occur, and
/// a scalar subquery only whether there is more than one. Applied to the
/// parsed statement of the single-table path, never to text, and only to the
/// statement it is handed with — anything that statement runs in turn is
/// executed with [`SubqueryNeed::All`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubqueryNeed {
    All,
    /// Whether any row exists.
    Existence,
    /// The set of values; order and multiplicity do not matter.
    Membership,
    /// At most one row is legal, so two are enough to tell.
    Scalar,
}

/// Narrows a parsed single-table `SELECT` to what `need` asks for.
///
/// The allowlist is the fields that change which rows come back: an
/// `OFFSET`, an aggregate (its LIMIT and ORDER BY apply to its output), a
/// window function (computed over every row that survived `WHERE`, so
/// capping the scan would change its values), `DISTINCT` (paged after the
/// dedup), and a locking clause (which rows get locked is part of the
/// result). Any of them present and the statement is left as written.
pub(crate) fn apply_subquery_need(
    query_row: &mut crate::core::rql::lexer_query::QueryRows,
    need: SubqueryNeed,
) {
    if need == SubqueryNeed::All {
        return;
    }
    let plain = query_row.offset.unwrap_or(0) == 0
        && query_row.aggregate.is_none()
        && query_row.window_slots.is_empty()
        && !query_row.distinct
        && query_row.row_lock.is_none();
    if !plain {
        return;
    }
    match need {
        SubqueryNeed::All => {}
        SubqueryNeed::Existence => {
            // `LIMIT 0` stays 0; anything else becomes 1. Order cannot change
            // whether a row exists.
            let cap = query_row.written_limit.map_or(1, |l| l.min(1));
            query_row.limit = Some(cap);
            query_row.written_limit = Some(cap);
            query_row.order_by = None;
            query_row.order_expressions.clear();
        }
        SubqueryNeed::Membership => {
            // Without a LIMIT the order decides nothing about which values
            // occur. With one it decides which rows are kept, so it stays.
            if query_row.written_limit.is_none() && query_row.limit.is_none() {
                query_row.order_by = None;
                query_row.order_expressions.clear();
            }
        }
        SubqueryNeed::Scalar => {
            // An explicit LIMIT (with its ORDER BY) picks the row and is kept.
            // Without one, more than one row is an error and two rows are
            // enough to raise it; which two does not matter.
            if query_row.written_limit.is_none() && query_row.limit.is_none() {
                query_row.limit = Some(2);
                query_row.written_limit = Some(2);
                query_row.order_by = None;
                query_row.order_expressions.clear();
            }
        }
    }
}

/// A prepared `SELECT`'s whole dispatch: its table, the parse narrowed to what
/// the caller needs, the scan.
///
/// One function for every entry point, so the statement wrappers can box just
/// this instead of [`execute_with_context`]'s future, and none of them can
/// drift from what [`execute_with_need`] does with the same statement.
async fn execute_prepared_rows(
    prepared: Box<crate::core::rql::prepared::PreparedRows>,
    database: Arc<Database>,
    tx: TxId,
    need: SubqueryNeed,
) -> Vec<u8> {
    let prepared = *prepared;
    #[cfg(test)]
    crate::core::rql::prepared::test_counters::note_learned_run(
        &prepared.table_name,
        crate::core::rql::statement_snapshot::in_scope(),
    );
    // A partitioned table, looked up by its logical name on every execution,
    // as the text path does: the parse is the only thing cached. Its `WHERE`
    // is this execution's values rendered into the template that two
    // text-path executions verified, and it is routed here afresh, with the
    // materialized flags read after this execution's snapshot (I11, §4.1).
    // Nothing about a route outlives the execution that computed it.
    //
    // S16b: a full-key point shape also carries this execution's key, bound
    // from its own values through the statement's cached derivation; the
    // fan-out reads that one partition without routing the rendered `WHERE`,
    // and still reads whether it is materialized after this execution's
    // snapshot. A key bound for another table or key is ignored there.
    let relation = match database
        .relation_for(&prepared.table_name, owner(), TableMode::AccessShare)
        .await
    {
        Ok(relation) => relation,
        Err(e) => return ddl_error(&e),
    };
    // Re-checked now that the lock is held (`docs/persistent-indexes.md` §7
    // `shape_epoch`, Q7; finding L7): pgwire chose this parse before the
    // statement waited for its lock, and a DDL that swapped the table while
    // it waited (a reshape, or a DROP and CREATE under the name) leaves a
    // parse, a description and scales from another instance. Nothing is
    // read; pgwire runs the statement's text instead.
    if relation.is_some() && database.slot_shape_epoch(&prepared.table_name) != Some(prepared.shape_epoch) {
        return ddl_error(crate::core::rql::prepared::STALE_PLAN);
    }
    if let Some(crate::core::database::Relation::Partitioned(pt)) = relation.clone() {
        let mut query_row = prepared.query_row;
        apply_subquery_need(&mut query_row, need);
        return match partitioned_source(&query_row, pt, prepared.key.as_ref(), tx) {
            Ok(source) => execute_query_rows(query_row, source, &database, tx).await,
            Err(refusal) => refusal,
        };
    }
    let Some(crate::core::database::Relation::Plain(table)) = relation else {
        return ddl_error("Table not found");
    };
    let mut query_row = prepared.query_row;
    apply_subquery_need(&mut query_row, need);
    execute_query_rows(query_row, crate::core::rql::row_source::RowSource::Single(table), &database, tx).await
}

/// The half of an `INSERT` that writes its one row: generated columns,
/// RETURNING, NOT NULL, the JSON mapping, foreign keys, the write and its
/// conflict handling. Everything before it is the statement's, done once:
/// the conflict-target checks, the lock (`target`, resolved under ROW
/// EXCLUSIVE), seeding sequences, the column list, `INSERT … SELECT` and
/// the split of a multi-row `VALUES`.
///
/// `insert_values_tuples` calls this once per tuple. It used to send every
/// tuple back through the whole dispatch as a statement of its own, so a
/// 1,000-row `INSERT` ran the statement-level half 1,000 times: the
/// recognizer, the lock lookup, the column-list and `SELECT` scans and a
/// second batch split, each over the row's text. Its own function, boxed,
/// also keeps these locals out of `execute_with_need`'s future.
async fn insert_row_resolved(
    database: &Arc<Database>,
    query: &QueryPurpose,
    row_data: &crate::core::rql::lexer_s1::InsertRowData,
    target: Option<crate::core::database::Relation>,
    depth: usize,
    tx: TxId,
) -> Vec<u8> {
    // Generated columns, computed now that the statement is one tuple
    // with an explicit column list. After the batch split, so each row
    // of a multi-row insert is computed from its own values.
    //
    // The rewritten statement is substituted for the original here
    // rather than re-dispatched — see `supply_generated_values`.
    //
    // Both `row_data` and `query` are shadowed. The row parser below
    // reads the whole `QueryPurpose`, not the `row_data` beside it, so
    // rebinding only one of them left the computed value in a statement
    // nothing went on to parse — the column stayed NULL and the insert
    // still reported success.
    let generated_rewrite =
        match supply_generated_values(row_data, target.as_ref().map(|t| t.schema()), &database) {
            Some(Ok(data)) => Some(QueryPurpose::InsertRow(data)),
            Some(Err(e)) => return ddl_error(&e),
            None => None,
        };
    let (row_data, query) = match &generated_rewrite {
        Some(purpose @ QueryPurpose::InsertRow(data)) => (data, purpose),
        _ => (row_data, query),
    };

    let table_name = &row_data.table_name;
    debug!("Insert row into table: {}", table_name);
    // The lookup guard is a shard read lock, and it must end here.
    //
    // `rewrite_json_values` below creates the table backing a JSON
    // column, which takes a *write* lock on the shard the new table's
    // name hashes to. Held across that await, the read lock this task
    // owns and the write lock it waits for are the same lock whenever
    // the two names share a shard — a deadlock with no second thread
    // in it.
    //
    // It is intermittent for a reason worth stating: `tables` is keyed
    // by `ahash::RandomState`, which seeds itself per `Database`. The
    // shard a name lands on is therefore drawn afresh at every start-up,
    // so a fixed statement against a fixed schema hangs on some servers
    // and not others. It read as a flaky test — a suite stalling at a
    // different place each run while every test in it passed alone.
    //
    // Cloning the `Arc` costs a refcount and owes nothing to the map.
    //
    // A partitioned table's rows go to its partitions (§9), so `table`
    // is then its shell: the schema every check below reads, never the
    // writer. Resolved through `relation`, as a shell found in `tables`
    // by name refuses every write.
    // The target resolved at the top of the arm, under its ROW
    // EXCLUSIVE.
    let (table, partitioned) = match target {
        Some(crate::core::database::Relation::Plain(table)) => (table, None),
        Some(crate::core::database::Relation::Partitioned(pt)) => {
            if let Err(e) = pt.check() {
                error!("Cannot insert into {}: {}", table_name, e);
                return ddl_error(&e.to_string());
            }
            (pt.shell().clone(), Some(pt))
        }
        None => {
            error!("Table not found");
            let mut result = vec![3u8];
            result.extend_from_slice("Table not found".as_bytes());
            return result;
        }
    };

    let schema = &table.schema;

    // A RETURNING clause turns the write into a statement that also
    // produces rows. Resolving it before the insert means a bad column
    // name fails the statement instead of being reported after the row
    // has already been written.
    let returning = match crate::core::rql::lexer_ir::returning_columns(
        &row_data.query,
        &schema.fields,
    ) {
        Some(Ok(fields)) => Some(fields),
        Some(Err(e)) => {
            error!("Cannot insert row: {}", e);
            let mut result = vec![3u8];
            result.extend_from_slice(e.as_bytes());
            return result;
        }
        None => None,
    };

    // A JSON document is stored in the table backing its column, and
    // the statement rewritten to carry the reference — so the row that
    // reaches the writer holds a UUID like any other.
    // Parsed once: the foreign-key check below reads the same statement, and
    // a multi-row insert pays this per row.
    let parsed_assignments = crate::core::rql::lexer_ir::assignments_of_insert(&row_data.query);
    let insert_assignments: &[(String, String)] = parsed_assignments.as_deref().unwrap_or_default();

    // A `NOT NULL` column must actually receive a value. Checked before
    // anything is written, so a rejected row leaves nothing behind.
    if let Err(e) = check_not_null(
        &row_data.table_name,
        &schema.fields,
        &insert_assignments,
        &schema.defaults,
        &schema.generated,
        true,
    ) {
        error!("{}", e);
        return ddl_error(&e);
    }
    let statement = match rewrite_json_values(
        &database,
        &row_data.table_name,
        &row_data.query,
        &insert_assignments,
        &schema.json_columns,
        tx,
    )
    .await
    {
        Ok(sql) => sql,
        Err(e) => {
            error!("{}", e);
            return ddl_error(&e);
        }
    };
    if let Some(sql) = statement {
        let Some(rewritten) = recognize_query_purpose(&sql) else {
            return ddl_error("could not parse the statement after JSON mapping");
        };
        return Box::pin(execute_with_context(rewritten, database.clone(), depth, tx))
            .await;
    }

    // Foreign keys are checked before the row is written, so a
    // violation leaves nothing behind to undo.
    match &parsed_assignments {
        // Built only for a table that has foreign keys, and boxed: the
        // check's future is most of this one's, which a multi-row insert
        // builds and copies once per row. Without any it answers `Ok` at
        // once, after the probe noted here in its place.
        Some(_) if table.schema.foreign_keys.is_empty() => {
            crate::owner_probe!(crate::core::mvcc::owner::probe::Site::ForeignKeyCheck, tx, database);
        }
        Some(assignments) => {
            if let Err(e) =
                Box::pin(check_foreign_keys_of(&database, Some(&table.schema), table_name, assignments, tx)).await
            {
                error!("{}", e);
                return ddl_error(&e);
            }
        }
        None => {
            // Falls through to the parser below, which reports why.
        }
    }

    // Handle insert row
    if let Some(insert_row) = crate::core::rql::lexer_ir::insert_row_with_defaults(
        &query,
        &schema.fields,
        &schema.defaults,
        &schema.type_modifiers,
        database.id,
    ) {
        // Every `CHECK` the table declares, evaluated against the row
        // as it will be stored. Before the write, so a violation
        // leaves nothing behind to undo.
        //
        // The row it judges is built inside the helper, not here. This
        // arm's locals are the ones that overflowed a worker's stack
        // once already, and a `Row` plus a loop in it was enough to do
        // so again.
        if let Err(e) = check_constraints_hold(
            table_name,
            &table.schema,
            &insert_row.columns_writing_data,
        ) {
            error!("{}", e);
            return ddl_error(&e);
        }

        // The returned row is built from the payload about to be
        // written, so it reflects exactly what was stored without
        // reading the row back off disk.
        // The returned row is the payload about to be written, every
        // column of it, so RETURNING reports exactly what was stored
        // without reading the row back. It goes through the same
        // builder UPDATE and DELETE use, which evaluates a computed
        // item against the row. Echoing payloads by the item's write
        // order instead returned NULL for `RETURNING id AS k`, whose
        // item is a computed echo with a synthetic write order, and
        // for any expression.
        let returned_row = returning.as_ref().map(|_| crate::core::row::row::Row {
            position: 0,
            columns: insert_row
                .columns_writing_data
                .iter()
                .map(|c| crate::core::row::row::Column {
                    schema_id: crate::core::row::row::schema_id_of(c.write_order as u64),
                    data: c.data.clone(),
                    column_type: c.column_type.clone(),
                })
                .collect(),
            length: 0,
        });

        // A partitioned table routes the final row, defaults and
        // generated values included, to its partition, materializing
        // it on first write; the partition's own unique check is exact
        // because every unique key holds every key column (§1b). Boxed:
        // routing adds a materialization to this arm's future.
        let outcome = match &partitioned {
            Some(pt) => Box::pin(pt.insert_row_in_txn(insert_row, tx)).await,
            None => table.insert_row_in_txn(insert_row, tx).await,
        };
        if outcome.is_ok() {
            // Recorded here, with the statement as it actually ran: by
            // this point a JSON rewrite has already happened and its
            // generated identifiers are literals in the text, so a
            // replica writes the same values rather than new ones.
            note_write(&database, tx, depth, &row_data.query, Some(table_name));
            if let Some(row) = returned_row {
                // JSON columns still hold the reference the rewrite put
                // there; the builder hydrates them into documents.
                let built = Box::pin(returning_rows_for_write(
                    &table,
                    &row_data.query,
                    vec![row],
                    None,
                    &[],
                    &database,
                    tx,
                ))
                .await;
                return match built {
                    Ok(Some(rows)) => {
                        let mut result = vec![2u8];
                        for row in &rows {
                            row.extend_serialized(&mut result);
                        }
                        result
                    }
                    Ok(None) => ddl_ok(1),
                    Err(e) => ddl_error(&e),
                };
            }
            // Keep status=0 for backward compatibility, but also include rows-affected.
            // pgwire can use this to emit INSERT 0 <n>.
            let mut result = vec![0u8];
            result.extend_from_slice(&1u64.to_le_bytes());
            return result;
        } else if row_data.on_conflict_update.is_some()
            && outcome
                .as_ref()
                .err()
                .is_some_and(|e| is_uniqueness_violation(e))
        {
            // `ON CONFLICT … DO UPDATE`: the row is already there, so
            // the statement becomes an update of it. Built from the
            // values this insert would have written, which is what
            // `EXCLUDED` names.
            let spec = row_data
                .on_conflict_update
                .as_ref()
                .expect("checked just above");
            return Box::pin(apply_conflict_update(
                &database,
                table_name,
                &row_data.query,
                spec,
                depth,
                tx,
            ))
            .await;
        } else if row_data.on_conflict_do_nothing
            && outcome
                .as_ref()
                .err()
                .is_some_and(|e| is_uniqueness_violation(e))
        {
            // `ON CONFLICT DO NOTHING`: the duplicate is the statement
            // working, not failing. Reported as zero rows affected, which
            // is what PostgreSQL returns and what a seed migration
            // re-run against an existing database depends on. With
            // RETURNING it is an empty row set rather than a count:
            // PostgreSQL still sends the RowDescription, and a bare
            // count sent only the command tag (as `INSERT … SELECT`
            // did before dedd503).
            if returning.is_some() {
                return vec![2u8];
            }
            let mut result = vec![0u8];
            result.extend_from_slice(&0u64.to_le_bytes());
            return result;
        } else {
            // The reason has to reach the client: an application
            // distinguishes a uniqueness violation from a genuine
            // failure, and cannot do that with a generic message.
            let message = outcome
                .err()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "the row could not be inserted".to_string());
            // A retryable conflict is not a failure yet — the caller
            // above replays the statement, and either it succeeds or it
            // exhausts its attempts and is reported then, once. Logged
            // at `error!` it was neither: one deploy produced 593 ERROR
            // lines in a minute for statements that all went on to
            // succeed, and nothing in the log said so. Volume like that
            // teaches an operator to skim exactly the level a real
            // failure arrives at.
            if message.ends_with(
                <crate::core::row::table::Table<
                    crate::core::storage_providers::backend::DbStorageProvider,
                >>::CONFLICT_RETRYABLE,
            ) {
                log::debug!("insert conflicted and will be replayed: {}", message);
            } else {
                error!("Failed to insert row: {}", message);
            }
            let mut result = vec![3u8];
            result.extend_from_slice(message.as_bytes());
            return result;
        }
    } else {
        // A value the column cannot hold is reported as PostgreSQL
        // reports it — the reason, and through it the SQLSTATE — rather
        // than as the generic mismatch every refusal used to become.
        if let Some(reason) = crate::core::rql::lexer_ir::take_refusal() {
            let mut result = vec![3u8];
            result.extend_from_slice(reason.as_bytes());
            return result;
        }
        error!("Cannot insert row due to schema mismatch");
        let mut result: [u8; 1] = [1u8; 1];
        result[0] = 3;
        let mut result = result.to_vec();
        let error_message = "Cannot insert row due to schema mismatch";
        let mut error_message_bytes = Vec::with_capacity(error_message.len());
        error_message_bytes.extend_from_slice(error_message.as_bytes());
        result.extend_from_slice(&mut error_message_bytes);
        return result.to_vec();
    }
}

/// [`execute_with_context`] for a statement whose caller needs only part of
/// its result. See [`SubqueryNeed`].
pub(crate) async fn execute_with_need(
    query: QueryPurpose,
    database: Arc<Database>,
    depth: usize,
    tx: TxId,
    need: SubqueryNeed,
) -> Vec<u8> {
    crate::owner_probe!(crate::core::mvcc::owner::probe::Site::Dispatch { depth }, tx, database);
    // A cached parse goes straight to the scan: it is the parse the plain
    // branch below made for this statement, and was only cached because that
    // branch is all the statement ever took.
    let query = match query {
        QueryPurpose::PreparedRows(prepared) => {
            return Box::pin(execute_prepared_rows(prepared, database, tx, need)).await;
        }
        other => other,
    };
    match &query {
        // Taken above, before the borrow.
        QueryPurpose::PreparedRows(_) => ddl_error("internal error: a prepared parse reached the text dispatch"),
        // An index over a document path. Recorded in the table's sidecar, which
        // is read when the table is opened.
        QueryPurpose::CreateIndex(sql) => {
            let response = if crate::core::rql::path_index::parse_create(sql).is_some() {
                Box::pin(execute_create_path_index(sql, &database)).await
            } else {
                Box::pin(execute_create_index(sql, &database)).await
            };
            if response.first() != Some(&3) {
                note_write(&database, tx, depth, sql, None);
            }
            return response;
        }
        QueryPurpose::DropIndex(sql) => {
            let response = match Box::pin(execute_drop_index(sql, &database)).await {
                Some(response) => response,
                None => match Box::pin(execute_drop_partial_unique(sql, &database)).await {
                    Some(response) => response,
                    None => Box::pin(execute_drop_path_index(sql, &database)).await,
                },
            };
            if response.first() != Some(&3) {
                note_write(&database, tx, depth, sql, None);
            }
            return response;
        }
        // `LOCK TABLE` changes nothing and reads nothing: it takes a lock the
        // transaction holds until it ends.
        QueryPurpose::LockTable(statement) => {
            let statement = match statement {
                Ok(s) => s.clone(),
                Err(e) => return ddl_error(e),
            };
            return Box::pin(execute_lock_table(&database, statement, tx)).await;
        }
        // Backup statements touch no table and take no transaction: they act on
        // the database directory as a whole and on an object store, neither of
        // which the row-level machinery below has anything to say about.
        #[cfg(feature = "s3_backup")]
        QueryPurpose::Backup(statement) => {
            return crate::core::rql::backup_exec::execute(statement.clone()).await;
        }
        #[cfg(feature = "s3_backup")]
        QueryPurpose::BackupError(message) => {
            error!("backup statement rejected: {}", message);
            return ddl_error(message);
        }
        // Tuning statements touch no table and take no transaction: they set
        // process state that later filter builds read. Handled first so the
        // rest of this match keeps its table-oriented shape.
        // Cluster control, like the filter statements above: no table, no
        // transaction, and a result set of text rather than rows of a relation.
        QueryPurpose::Replication(sql) => {
            // The structured forms return rows rather than lines; everything
            // else in this surface is text meant to be read.
            if crate::replication::sql::is_structured(sql) {
                return match crate::replication::sql::execute_structured(sql, &database) {
                    Ok(rows) => rows,
                    Err(e) => {
                        error!("replication statement failed: {}", e);
                        ddl_error(&e)
                    }
                };
            }
            return match crate::replication::sql::execute(sql, &database).await {
                Ok(lines) => {
                    info!("replication: {}", lines.join("; "));
                    crate::replication::sql::text_result(&lines)
                }
                Err(e) => {
                    error!("replication statement failed: {}", e);
                    ddl_error(&e)
                }
            };
        }
        // Storage placement, like the surfaces above: no table, no transaction,
        // and a result set describing where the database keeps its files rather
        // than rows of a relation. `STORAGE MOVE`/`REBALANCE` do touch tables,
        // but at the file level — they take the table handle out of the map
        // themselves, which is not something a statement's transaction has
        // anything to say about.
        #[cfg(feature = "storage_pools")]
        QueryPurpose::Storage(sql) => {
            // In a helper, and boxed, for the reason spelled out on the
            // `CREATE TABLE` arm below: this function is one giant match, so
            // its frame is the sum of every arm's locals and it already sits
            // close to a worker stack. Nothing new belongs inline here.
            return Box::pin(execute_storage(sql, &database)).await;
        }
        // Introspection, like the surfaces above: no table, no transaction, and
        // a result set describing the database rather than rows of a relation.
        QueryPurpose::Introspect(statement) => {
            return crate::core::introspect::execute(statement, &database).await;
        }
        QueryPurpose::IntrospectError(message) => {
            error!("introspection statement rejected: {}", message);
            return ddl_error(message);
        }
        // The key/value store: no table, no transaction. The command runs
        // against the store, which has its own per-key atomicity.
        #[cfg(feature = "kv_cache")]
        QueryPurpose::KvCache(sql) => {
            return crate::core::kv_cache::sql::execute(sql, &database.kv_cache).await;
        }
        QueryPurpose::FilterControl(sql) => {
            return match crate::core::filter::sql::execute(sql) {
                Ok(lines) => {
                    info!("filter control: {}", lines.join("; "));
                    let mut out = vec![0u8];
                    out.extend_from_slice(&(lines.len() as u64).to_le_bytes());
                    for line in lines {
                        out.push(b'\n');
                        out.extend_from_slice(line.as_bytes());
                    }
                    out
                }
                Err(e) => {
                    error!("filter control failed: {}", e);
                    ddl_error(&e)
                }
            };
        }
        QueryPurpose::CreateTable(_) => {
            // Handle create table
            let (parsed, partition, rejection) =
                crate::core::rql::lexer_ct::create_table_with_partition(&query);
            if let Some(reason) = rejection {
                error!("Cannot create table: {}", reason);
                return ddl_error(&reason);
            }
            if let Err(reason) =
                parsed.as_ref().map_or(Ok(()), |ct| validate_generated(ct).and_then(|()| validate_checks(ct)))
            {
                error!("Cannot create table: {}", reason);
                return ddl_error(&reason);
            }
            if let Some(create_table) = parsed {
                // The statement temporaries' prefix, before the name is
                // locked: a table under it would be taken for one (no table
                // locks, memory-backed, reaped at startup).
                if let Some(reason) = crate::core::rql::cte::reserved_name_refusal(&create_table.table_name) {
                    return ddl_error(&reason);
                }
                // ACCESS EXCLUSIVE on the new name and SHARE on each parent it
                // references, before anything is checked or written (§7; see
                // `lock_create_target`).
                if let Err(e) = Box::pin(lock_create_target(&database, &create_table)).await {
                    return ddl_error(&e);
                }
                // `IF NOT EXISTS` means an existing table is success. Checked
                // before the create rather than by interpreting its failure,
                // since `add_table` reports one error for every reason.
                // In a helper, not inline. `execute_with_context` is one giant
                // match, so its frame is the sum of every arm's locals, and it
                // sits close enough to a 2 MB worker stack that adding an
                // `info!` here — purely for the format temporaries — overflowed
                // it on a `CREATE TABLE IF NOT EXISTS` against an existing
                // table. Anything new in this function belongs behind a call.
                if create_table_already_satisfied(&create_table, &database) {
                    return ddl_ok(0);
                }
                if let Some(clause) = partition {
                    return Box::pin(execute_create_partitioned(
                        &database,
                        create_table,
                        clause,
                        tx,
                        depth,
                        &query,
                    ))
                    .await;
                }
                // A JSON column's DEFAULT is a document, and the column holds a
                // reference. Resolved before the table is registered, so the
                // recorded default is the reference every row will take.
                let create_table =
                    match Box::pin(resolve_json_defaults(&database, create_table, tx)).await {
                        Ok(ct) => ct,
                        Err(e) => {
                            error!("{}", e);
                            return ddl_error(&e);
                        }
                    };
                let created_name = create_table.table_name.clone();
                let added = database.add_table(create_table).await;
                // Past its commit point a CREATE whose last sync failed has
                // installed the table and reports the error: replication is
                // told all the same, or the replica would lack a table every
                // later statement names.
                if added.as_ref().is_err_and(|e| {
                    !matches!(e.kind(), std::io::ErrorKind::InvalidInput | std::io::ErrorKind::AlreadyExists)
                }) && database.has_table(&created_name)
                {
                    note_write(&database, tx, depth, &query_sql(&query), None);
                }
                if let Err(e) = &added {
                    // The guards in `add_table` say why a name is refused; a
                    // generic message would leave the client guessing.
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::InvalidInput | std::io::ErrorKind::AlreadyExists
                    ) {
                        error!("Failed to add table: {}", e);
                        return ddl_error(&e.to_string());
                    }
                }
                if added.is_ok() {
                    info!("Table created successfully");
                    // No table name is attached to schema changes: the
                    // last-writer-wins comparison is about row data, and a
                    // `CREATE TABLE` dropped as "superseded" would leave every
                    // statement after it with nowhere to go.
                    note_write(&database, tx, depth, &query_sql(&query), None);
                    let mut result: [u8; 1] = [0u8; 1];
                    result[0] = 0;
                    return result.to_vec();
                } else {
                    error!("Failed to add table");
                    let mut result: [u8; 1] = [1u8; 1];
                    result[0] = 3;
                    let mut result = result.to_vec();
                    let error_message = "Error occurred";
                    let mut error_message_bytes = Vec::with_capacity(error_message.len());
                    error_message_bytes.extend_from_slice(error_message.as_bytes());
                    result.extend_from_slice(&mut error_message_bytes);
                    return result.to_vec();
                }
            } else {
                error!("Table already exists");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Table already exists";
                let mut error_message_bytes = Vec::with_capacity(error_message.len());
                error_message_bytes.extend_from_slice(error_message.as_bytes());
                result.extend_from_slice(&mut error_message_bytes);
                return result.to_vec();
            }
        }
        // Each of these is boxed for the same reason: keeping a branch's
        // locals out of the enclosing future.
        QueryPurpose::DropTable(sql) => {
            // Which sequences the table owns, read before the drop takes the
            // schema that says so.
            let owned = match Box::pin(sequences_owned_by_dropped(sql, &database)).await {
                Ok(owned) => owned,
                Err(e) => return ddl_error(&e),
            };
            let response = Box::pin(execute_drop_table(sql, &database)).await;
            if response.first() != Some(&3) {
                // The sequences go with the table. Without this a table dropped
                // and recreated under the same name kept counting from where
                // the old one stopped, so its first row got a key well above 1.
                // This database's only: another database's table of the same
                // name is a different table.
                crate::core::rql::sequence::forget(database.id, &owned);
                // And the name's floor marks on the sequences it shared: a
                // table later created or renamed under the name has not
                // raised them to its own rows.
                if let Ok((name, _)) = crate::core::rql::lexer_alter::parse_drop_table(sql) {
                    crate::core::rql::sequence::forget_table_floors(database.id, &name);
                }
                note_write(&database, tx, depth, sql, None);
            }
            response
        }
        QueryPurpose::AddColumn(sql)
        | QueryPurpose::DropColumn(sql)
        | QueryPurpose::RenameColumn(sql) => {
            let response = Box::pin(execute_alter_table(sql, &database, tx)).await;
            if response.first() != Some(&3) {
                note_write(&database, tx, depth, sql, None);
            }
            response
        }
        QueryPurpose::InsertRow(row_data) => {
            // A conflict's `DO UPDATE` may overwrite in place, and a RETURNING
            // that computes is evaluated after it (`returning_can_fail`).
            if row_data.on_conflict_update.is_some() && returning_can_fail(&row_data.query) {
                database.transactions.forbid_in_place(tx);
            }
            // A partitioned table refuses an ON CONFLICT target its unique
            // keys cannot match (42P10) before anything runs: the source of
            // an `INSERT … SELECT`, a default's sequence, or the first row of
            // a multi-row insert.
            if let Err(e) = check_partitioned_conflict_target(&database, row_data) {
                error!("{}", e);
                return ddl_error(&e);
            }
            if let Err(e) = check_conflict_target_infers_a_key(&database, row_data) {
                error!("{}", e);
                return ddl_error(&e);
            }

            // The target, ROW EXCLUSIVE (a partitioned one by its logical
            // name): held from the statement's set, or taken here, once.
            // Resolved once for every step below that reads it (the seed,
            // the column list, generated values, the write, the foreign
            // keys; slice B8b) rather than looked up and confirmed at each.
            // Nothing can swap it meanwhile: a reload, DROP or MOVE needs the
            // ACCESS EXCLUSIVE this statement's ROW EXCLUSIVE excludes, and
            // the owner holds it to the statement's end.
            let target = match Box::pin(database.relation_for(&row_data.table_name, owner(), TableMode::RowExclusive)).await {
                Ok(target) => target,
                Err(e) => return ddl_error(&e),
            };

            // Every column the statement names is one of the target's, before
            // anything is computed or written: PostgreSQL's 42703. An unknown
            // name reached the row builder, which failed the statement as a
            // schema mismatch (XX000), or a NOT NULL check on another column
            // (23502) did first; an `INSERT … SELECT` whose source was empty
            // reported success.
            if let (Some(t), Some(named)) = (target.as_ref(), crate::core::rql::lexer_ir::named_columns(&row_data.query)) {
                if let Some(unknown) = named
                    .iter()
                    .find(|n| !t.schema().fields.iter().any(|f| !f.is_deleted && f.name == **n))
                {
                    return ddl_error(&format!(
                        "column {} of table {} does not exist",
                        crate::core::value_codec::double_quoted(unknown),
                        crate::core::value_codec::double_quoted(&row_data.table_name)
                    ));
                }
            }

            // Before anything reads a default: a `SERIAL` column's counter is
            // in memory and a reopened database has not yet learned what the
            // table already holds. Skipped for a table with no serial column,
            // and after the first insert for one that has.
            if target.as_ref().is_some_and(|t| has_unseeded_sequence(&database, &row_data.table_name, t.schema())) {
                if let Err(e) = Box::pin(seed_sequences(&database, &row_data.table_name, target.as_ref())).await {
                    error!("{}", e);
                    return ddl_error(&e);
                }
            }

            // `INSERT INTO t VALUES (…)` names no columns, so the list is
            // supplied from the schema and the statement re-dispatched in the
            // one shape every parser below understands. First, so that the
            // multi-row split and the `SELECT` source see the list too.
            // Synchronous: the schema copy lives on the stack for the call,
            // never in this frame's future.
            match supply_insert_column_list(row_data, target.as_ref().map(|t| t.schema())) {
                Some(Ok(purpose)) => {
                    return Box::pin(execute_with_context(purpose, database.clone(), depth, tx))
                        .await;
                }
                Some(Err(e)) => return ddl_error(&e),
                None => {}
            }

            // `VALUES (…), (…), (…)` is one statement per tuple. Handled here,
            // before anything else looks at the query, because the parser below
            // reads a single tuple and used to discard the rest in silence — a
            // three-row insert wrote one row and reported `INSERT 0 1`.
            //
            // Each tuple is re-dispatched rather than parsed inline, so every
            // per-row behaviour — defaults, JSON mapping, foreign keys,
            // uniqueness, RETURNING — applies exactly as it does to a
            // single-row insert, with no second implementation to keep in step.
            // It costs a re-parse per row; correctness first, and the batch
            // path `on_insert_batch` is the place to reclaim it.
            // Kept out of this function's frame on purpose — see
            // `insert_from_select`.
            if let Some(response) =
                Box::pin(insert_from_select(
                    &row_data.query,
                    &InsertConflict {
                        do_nothing: row_data.on_conflict_do_nothing,
                        update: row_data.on_conflict_update.clone(),
                    },
                    &database,
                    depth,
                    tx,
                ))
                .await
            {
                return response;
            }

            // Kept in its own function, not inlined here. `execute_with_context`'s
            // future is the sum of every statement arm's locals, and it is
            // already close enough to the limit that adding a batch loop's
            // locals plus one nesting level overflowed a 2 MB worker stack on a
            // two-row insert. Boxing it puts those locals on the heap.
            match Box::pin(insert_values_batch(&row_data, &database, depth, tx)).await {
                Some(response) => return response,
                None => {}
            }

            // The statement-level work is done; the row is written by the
            // half `insert_values_tuples` also calls, once per tuple, with
            // the target resolved here.
            Box::pin(insert_row_resolved(&database, &query, row_data, target, depth, tx)).await
        }
        QueryPurpose::UpdateRow(_) => {
            if let QueryPurpose::UpdateRow(sql) = &query
                && returning_can_fail(sql)
            {
                database.transactions.forbid_in_place(tx);
            }
            // Subqueries are folded first, exactly as the SELECT arm does,
            // and then the flattened statement is dispatched. Without this an
            // `UPDATE … SET c = (SELECT …)` reached the UPDATE parser with the
            // word SELECT in its assignment and came back as "Cannot parse
            // UPDATE query", naming nothing. Folded here, inside the attempt,
            // rather than at the wire: the attempt's reads share one snapshot
            // and its rows are scanned under it (`statement_snapshot`), and a
            // conflict retry folds them again under its own.
            if let QueryPurpose::UpdateRow(sql) = &query
                && has_subquery(sql)
                && !(crate::core::rql::join::find_keyword(sql, "FROM", 0).is_none()
                    && crate::core::rql::subquery::find_correlated_scalar_subquery(sql).is_some())
            {
                return Box::pin(execute_flattened(sql, "UPDATE", database.clone(), depth, tx)).await;
            }
            // `UPDATE t SET … FROM src WHERE …` reads one relation to write
            // another, which the single-relation write path cannot express.
            // Handled by joining the two, reading the values out, and writing
            // them back row by row — see `execute_update_from`.
            if let QueryPurpose::UpdateRow(sql) = &query {
                if crate::core::rql::join::find_keyword(sql, "FROM", 0).is_some() {
                    return Box::pin(execute_update_from(sql, &database, depth, tx)).await;
                }
                // A subquery reading the row being updated has a value per
                // row, which nothing here evaluates: left in place, its text
                // reached the assignment as a value (22P02, naming the
                // subquery), and a DO UPDATE's, stripped of its qualifier,
                // read the inner table's own column and stored NULL.
                if crate::core::rql::subquery::find_correlated_scalar_subquery(sql).is_some()
                {
                    return ddl_error(
                        "a correlated subquery in UPDATE is not supported: one that reads \
                         the row being updated has a value per row, which this cannot evaluate; \
                         write it as UPDATE … FROM",
                    );
                }
                // A row another transaction holds `FOR UPDATE` may not be
                // written underneath it.
                let mut words = sql.split_whitespace().skip(1);
                let name = words.next().unwrap_or("").trim_matches('"').to_string();
                // `UPDATE t AS j SET …` qualifies its predicate with the alias,
                // which the generated lookup below does not know about.
                let alias = match words.next() {
                    Some(w) if w.eq_ignore_ascii_case("AS") => words.next(),
                    Some(w) if !w.eq_ignore_ascii_case("SET") => Some(w),
                    _ => None,
                }
                .map(|a| a.trim_matches('"').to_string());
                let condition = predicate_of(sql, alias.as_deref());

                // Setting a `NOT NULL` column to NULL is refused, as it is on
                // insert. Only the assignments are checked: an UPDATE leaves
                // every column it does not name alone.
                {
                    // ROW EXCLUSIVE on the target before its schema is read.
                    let entry = match Box::pin(database.table_for(&name, owner(), TableMode::RowExclusive)).await {
                        Ok(entry) => entry,
                        Err(e) => return ddl_error(&e),
                    };
                    if let Some(table) = entry.as_ref().clone() {
                        let assignments = crate::core::rql::lexer_ur::assignments_of_update(sql)
                            .unwrap_or_default();
                        if let Err(e) = check_not_null(
                            &name,
                            &table.schema.fields,
                            &assignments,
                            &table.schema.defaults,
                            &table.schema.generated,
                            false,
                        ) {
                            error!("{}", e);
                            return ddl_error(&e);
                        }
                    }
                }
                // A statement holding a subquery is checked once it is
                // flattened and re-dispatched, below: checked here as well, the
                // raw predicate ran the subquery a second time — a `FOR UPDATE
                // SKIP LOCKED` claim's subquery among them.
                let redispatched = crate::core::rql::subquery::find_in_subquery(sql).is_some()
                    || crate::core::rql::subquery::find_exists(sql).is_some()
                    || crate::core::rql::subquery::find_scalar_subquery(sql).is_some();
                if !redispatched {
                    if let Err(e) = Box::pin(refuse_write_to_locked_rows(
                        &database, &name, &condition, tx,
                    ))
                    .await
                    {
                        error!("{}", e);
                        return ddl_error(&e);
                    }
                }
            }

            let schema_fields;

            // Parse UPDATE ... using schema
            // We need the table to obtain schema, so first read the table name in a lightweight way.
            let table_name = match &query {
                QueryPurpose::UpdateRow(sql) => sql
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("")
                    .trim_matches('"')
                    .to_string(),
                _ => String::new(),
            };

            if table_name.is_empty() {
                error!("UPDATE missing table name");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "UPDATE missing table name";
                result.extend_from_slice(error_message.as_bytes());
                return result;
            }

            // Cloned out so the map guard — a shard read lock — ends here
            // rather than at the end of the arm. The JSON rewrite below creates
            // the table backing a JSON column, which writes this same map; held
            // across it, this is the deadlock the INSERT arm had.
            let table = {
                let entry = match Box::pin(database.table_for(&table_name, owner(), TableMode::RowExclusive)).await {
                    Ok(entry) => entry,
                    Err(e) => return ddl_error(&e),
                };
                let Some(entry) = entry else {
                    error!("Table not found");
                    let mut result = vec![3u8];
                    result.extend_from_slice("Table not found".as_bytes());
                    return result;
                };
                entry
            };

            // A generated column is computed from the row, so an `UPDATE` to
            // one of its inputs recomputes it; see
            // `recompute_generated_in_update`, which adds it to the plan below.
            // Here the statement is checked, and `SET g = DEFAULT` put in a
            // shape the parser reads. The rewrite replaces `query` in place
            // rather than being re-dispatched, as the INSERT arm's does, so
            // the check sees only what the client wrote.
            // `SET c = DEFAULT` is the column's default, or NULL.
            let default_rewrite = default_update_statement(&query, &table.schema);
            let query = default_rewrite.as_ref().unwrap_or(&query);
            let generated_rewrite = match generated_update_statement(query, &table.schema) {
                Ok(rewrite) => rewrite,
                Err(reason) => {
                    error!("{}", reason);
                    return ddl_error(&reason);
                }
            };
            let query = generated_rewrite.as_ref().unwrap_or(query);

            // A JSON document assigned by an UPDATE is stored in the table
            // backing its column and the statement rewritten to carry the
            // reference, exactly as an INSERT does.
            //
            // Without this the document went to a `UUID` column as raw text and
            // read back as `{}` — the write reported `UPDATE 1` and the row's
            // previous contents were gone. A silent total loss of the value,
            // which is worse than refusing the statement would have been.
            if !table.schema.json_columns.is_empty() {
                let sql = query_sql(&query);
                let assignments =
                    crate::core::rql::lexer_ur::assignments_of_update(&sql).unwrap_or_default();
                match rewrite_json_values(
                    &database,
                    &table.schema.name,
                    &sql,
                    &assignments,
                    &table.schema.json_columns,
                    tx,
                )
                .await
                {
                    Ok(Some(rewritten_sql)) => {
                        let Some(rewritten) = recognize_query_purpose(&rewritten_sql) else {
                            return ddl_error("could not parse the statement after JSON mapping");
                        };
                        return Box::pin(execute_with_context(
                            rewritten,
                            database.clone(),
                            depth,
                            tx,
                        ))
                        .await;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!("{}", e);
                        return ddl_error(&e);
                    }
                }
            }

            schema_fields = &table.schema.fields;

            let schema_sv = schema_fields
                .iter()
                .cloned()
                .collect::<smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>>();

            // As for DELETE: report what actually stopped the statement.
            let mut update_plan = match crate::core::rql::lexer_ur::update_row_with_reason(
                &query,
                &schema_sv,
                &table.schema.type_modifiers,
            ) {
                (Some(plan), _) => plan,
                (None, reason) => {
                    let message = reason.unwrap_or_else(|| "Cannot parse UPDATE query".to_string());
                    error!("Cannot parse UPDATE query: {}", message);
                    return ddl_error(&message);
                }
            };
            // Before anything reads the plan, so the checks, RETURNING and the
            // write all see the recomputed columns.
            if let Err(e) =
                recompute_generated_in_update(&mut update_plan, &query_sql(&query), &table.schema)
            {
                error!("{}", e);
                return ddl_error(&e);
            }

            // A `CHECK` the assignments can be judged against on their own.
            // The values are the ones the plan already encoded, so this reads
            // exactly what the write will store.
            if let Err(e) =
                check_constraints_hold_for_update(&table_name, &table.schema, &update_plan)
            {
                error!("{}", e);
                return ddl_error(&e);
            }

            // A partitioned table's rows are in its partitions (§9): `table`
            // above is its shell, whose schema every check so far read.
            if let Some(pt) = database.partitioned_table(&table_name) {
                return Box::pin(execute_partitioned_update(
                    query,
                    update_plan,
                    pt,
                    &database,
                    &table_name,
                    depth,
                    tx,
                ))
                .await;
            }

            // One snapshot for the statement when its foreign keys need to know
            // which rows it changes: the rows judged and the rows written must
            // be the same rows. The one its reads took when it read first (a
            // subquery, a FROM source). `None` — every other UPDATE — leaves
            // the write to take its own, as before.
            let foreign_keys_read =
                update_needs_statement_snapshot(&database, &table.schema, &table_name, &update_plan);
            // With no foreign-key work, the target's snapshot is settled under
            // its write lock (`statement_visibility_deferred`).
            let statement_visibility = statement_snapshot_for(depth, || {
                if foreign_keys_read { table.statement_visibility(tx) } else { table.statement_visibility_deferred(tx) }
            })
            .or_else(|| foreign_keys_read.then(|| table.visibility_for(tx)));
            // What the foreign-key work reads under: only when it has any.
            let foreign_key_visibility = statement_visibility.as_ref().filter(|_| foreign_keys_read);
            let target = crate::core::database::Relation::Plain(table.clone());

            if let Some(assignments) =
                crate::core::rql::lexer_ur::assignments_of_update(&query_sql(&query))
            {
                if let Err(e) = check_foreign_keys_for_update(
                    &database,
                    &target,
                    &table_name,
                    &update_plan,
                    &assignments,
                    foreign_key_visibility,
                    tx,
                )
                .await
                {
                    error!("{}", e);
                    return ddl_error(&e);
                }
            }

            // The parent side: keys this statement moves, locked before the
            // write and acted on after it. `None` — the common case — unless a
            // foreign key references a column the statement assigns.
            let key_changes = match foreign_key_visibility {
                None => None,
                Some(visibility) => match keys_about_to_change(
                    &database,
                    &target,
                    &table_name,
                    &update_plan,
                    visibility,
                    tx,
                )
                .await
                {
                    Ok(changes) => changes,
                    Err(e) => {
                        error!("{}", e);
                        return ddl_error(&e);
                    }
                },
            };
            if let Some(changes) = key_changes.as_ref() {
                if let Err(e) = lock_keys_before_update(&database, &table_name, changes, tx).await {
                    error!("{}", e);
                    return ddl_error(&e);
                }
            }

            // `RETURNING` on an `UPDATE` reports the rows *after* the change:
            // the rows the write itself matched, as they were, with the
            // assignments overlaid. Not read again afterwards, since the update
            // may stop a row matching its own `WHERE`. The list is resolved
            // first, so a bad one fails the statement before it writes.
            let returning_wanted = match returning_planned(&query_sql(&query), &table.schema.fields) {
                Ok(wanted) => wanted,
                Err(e) => {
                    error!("Cannot resolve RETURNING: {}", e);
                    let mut result = vec![3u8];
                    result.extend_from_slice(e.as_bytes());
                    return result;
                }
            };
            // The write consumes the assignments; `RETURNING` overlays them after.
            let returning_updates: Option<smallvec::SmallVec<[crate::core::row::row::ColumnWritePayload; 32]>> =
                returning_wanted.then(|| {
                    update_plan
                        .updates
                        .iter()
                        .map(|u| crate::core::row::row::ColumnWritePayload {
                            data: u.data.clone(),
                            write_order: u.write_order,
                            column_type: u.column_type.clone(),
                            size: u.size,
                        })
                        .collect()
                });
            let mut before: Vec<crate::core::row::row::Row> = Vec::new();

            match table
                .update_rows_as_of(
                    &update_plan.where_clause,
                    update_plan.query_row_fetch,
                    update_plan.updates,
                    &update_plan.expressions,
                    None,
                    tx,
                    statement_visibility,
                    returning_wanted.then_some(&mut before),
                )
                .await
            {
                Ok(n) => {
                    info!("Updated {} rows", n);
                    if let Some(changes) = key_changes.as_ref() {
                        if let Err(e) =
                            apply_on_update(&database, &table_name, changes, depth, tx).await
                        {
                            error!("{}", e);
                            return ddl_error(&e);
                        }
                    }
                    note_write(&database, tx, depth, &query_sql(&query), Some(&table_name));
                    let returning = match returning_updates {
                        Some(updates) => match returning_rows_for_write(
                            &table,
                            &query_sql(&query),
                            before,
                            Some(&updates),
                            &update_plan.expressions,
                            &database,
                            tx,
                        )
                        .await
                        {
                            Ok(rows) => rows,
                            Err(e) => {
                                error!("Cannot resolve RETURNING: {}", e);
                                let mut result = vec![3u8];
                                result.extend_from_slice(e.as_bytes());
                                return result;
                            }
                        },
                        None => None,
                    };
                    if let Some(rows) = returning {
                        let mut result = vec![2u8];
                        for row in &rows {
                            row.extend_serialized(&mut result);
                        }
                        return result;
                    }
                    // Keep status=0 for backward compatibility, but also include rows-affected.
                    let mut result = vec![0u8];
                    result.extend_from_slice(&n.to_le_bytes());
                    return result;
                }
                Err(e) => {
                    // The retry marker is an internal signal for `execute_in_txn`;
                    // it must never appear in a log a human reads.
                    error!(
                        "Failed to update rows: {}",
                        clean_conflict_text(&e.to_string())
                    );
                    let mut result = vec![3u8];
                    result.extend_from_slice(e.to_string().as_bytes());
                    return result;
                }
            }
        }
        QueryPurpose::Vacuum(request) => {
            // `VACUUM` rebuilds the free list so later writes reuse dead space.
            // `VACUUM FULL` compacts: it rewrites the table so live rows and
            // live pointer records sit in one run at the front of their files,
            // and returns the tail to the filesystem — at the cost of stopping
            // everything else on that table for the duration.
            //
            // A partitioned table is vacuumed partition by partition (§11):
            // its shell, which `tables` also holds under its name, has no
            // rows, and vacuuming it failed the statement, so a bare VACUUM
            // failed as soon as a partitioned table existed (F2).
            let names: Vec<String> = match &request.table_name {
                Some(name) => {
                    if !database.has_table(name) && !database.is_partitioned(name) {
                        error!("VACUUM: table {} not found", name);
                        let mut result = vec![3u8];
                        result.extend_from_slice(format!("Table {} not found", name).as_bytes());
                        return result;
                    }
                    vec![name.clone()]
                }
                None => {
                    let mut names: Vec<String> = database
                        .table_names()
                        .into_iter()
                        .chain(database.partitioned_tables_all().iter().map(|t| t.name().to_string()))
                        .collect();
                    names.sort();
                    names.dedup();
                    names
                }
            };

            let mut reclaimed: u64 = 0;
            // Tables compacted whose space could not be given back.
            let mut untruncated: Vec<(String, &'static str)> = Vec::new();
            for name in names {
                // ROW EXCLUSIVE (plain) or ACCESS EXCLUSIVE (FULL) on each
                // table while it is vacuumed (finding L6, `vacuum_mode`):
                // either conflicts with a DDL's ACCESS EXCLUSIVE, so no reload
                // swaps or retires the instance under the vacuum. A bare
                // VACUUM takes each table under an owner of its own and gives
                // it back when it moves on, so it never holds every table at
                // once.
                let relation = match Box::pin(vacuum_lock(
                    &database,
                    &name,
                    request.full,
                    request.table_name.is_none(),
                ))
                .await
                {
                    Ok(relation) => relation,
                    Err(e) => {
                        let mut result = vec![3u8];
                        result.extend_from_slice(e.as_bytes());
                        return result;
                    }
                };
                let _released = relation.1;
                // Resolved and cloned per table rather than held across the
                // await: a DashMap reference kept over one would block anything
                // else touching the map for as long as the vacuum runs.
                let tables: Vec<_> =
                    match relation.0 {
                        None => continue,
                        Some(crate::core::database::Relation::Plain(table)) => vec![table],
                        Some(crate::core::database::Relation::Partitioned(pt)) => {
                            match vacuum_targets(&pt) {
                                Ok(parts) => parts,
                                // A bare VACUUM goes on past a table that
                                // cannot be opened, and says so; naming it
                                // fails the statement, as any VACUUM error does.
                                Err(e) if request.table_name.is_none() => {
                                    crate::core::notice::push(
                                        "WARNING",
                                        "01000",
                                        format!("VACUUM skipped partitioned table \"{name}\": {e}"),
                                    );
                                    continue;
                                }
                                Err(e) => {
                                    error!("VACUUM failed for {}: {}", name, e);
                                    let mut result = vec![3u8];
                                    result.extend_from_slice(
                                        format!("VACUUM failed for {}: {}", name, e).as_bytes(),
                                    );
                                    return result;
                                }
                            }
                        }
                    };

                for table in tables {
                    if request.full {
                        match table.compact().await {
                            Ok(report) => {
                                reclaimed = reclaimed
                                    .saturating_add(report.rows_bytes_freed)
                                    .saturating_add(report.pointer_bytes_freed);
                                // One line per logical table, however
                                // many of its partitions kept their tail.
                                if let Some(why) = report.truncation_skipped
                                    && !untruncated.iter().any(|(n, _)| *n == name)
                                {
                                    untruncated.push((name.clone(), why));
                                }
                            }
                            Err(e) => {
                                error!("VACUUM FULL failed for {}: {}", name, e);
                                let mut result = vec![3u8];
                                result.extend_from_slice(
                                    format!("VACUUM FULL failed for {}: {}", name, e).as_bytes(),
                                );
                                return result;
                            }
                        }
                    } else {
                        match table.vacuum_now().await {
                            Ok(report) => {
                                reclaimed = reclaimed.saturating_add(report.reclaimed_bytes);
                                if report.deferred {
                                    info!(
                                        "VACUUM on {} deferred: a query older than the scan is still running",
                                        name
                                    );
                                }
                            }
                            Err(e) => {
                                error!("VACUUM failed for {}: {}", name, e);
                                let mut result = vec![3u8];
                                result.extend_from_slice(
                                    format!("VACUUM failed for {}: {}", name, e).as_bytes(),
                                );
                                return result;
                            }
                        }
                    }
                }
            }

            // A VACUUM FULL that moved every row but returned nothing used to
            // report plain success, with the reason only in a warn-level log
            // line — invisible at the production level. It still succeeds, as
            // PostgreSQL's would (maintenance jobs and ORMs expect it to), but
            // the client is told why with a WARNING, and the table's
            // `vacuum_full_untruncated` counter records it.
            if !untruncated.is_empty() {
                let names: Vec<&str> = untruncated.iter().map(|(n, _)| n.as_str()).collect();
                let message = format!(
                    "VACUUM FULL compacted {} but could not return the space to the filesystem: \
                     {}. The data is intact; run VACUUM FULL again when write load allows",
                    names.join(", "),
                    untruncated[0].1,
                );
                crate::core::notice::push("WARNING", "01000", message);
            }

            info!("VACUUM reclaimed {} bytes", reclaimed);
            // Same shape as UPDATE and DELETE report rows affected: status byte
            // then a little-endian u64.
            let mut result = vec![0u8];
            result.extend_from_slice(&reclaimed.to_le_bytes());
            result
        }
        QueryPurpose::DeleteRow(_) => {
            // Subqueries are folded first and the flattened statement
            // dispatched, inside the attempt, as the UPDATE arm does.
            if let QueryPurpose::DeleteRow(sql) = &query
                && has_subquery(sql)
            {
                return Box::pin(execute_flattened(sql, "DELETE", database.clone(), depth, tx)).await;
            }
            // `DELETE … USING` names a second relation, which the
            // single-relation path cannot express — and must never degrade
            // into `DELETE FROM t` (see the refusal in `lexer_dr` and the
            // 13,585 rows it commemorates). Joined, keyed, and deleted row by
            // row — see `execute_delete_using`.
            if let QueryPurpose::DeleteRow(sql) = &query {
                if crate::core::rql::join::find_keyword(sql, "USING", 0).is_some() {
                    return Box::pin(execute_delete_using(sql, &database, depth, tx)).await;
                }
            }
            // Parse DELETE ... using schema
            let table_name = match &query {
                QueryPurpose::DeleteRow(sql) => sql
                    .split_whitespace()
                    .nth(2)
                    .unwrap_or("")
                    .trim_matches('"')
                    .to_string(),
                _ => String::new(),
            };

            if !table_name.is_empty() {
                if let QueryPurpose::DeleteRow(sql) = &query {
                    let condition = predicate_of(sql, None);
                    if let Err(e) = Box::pin(refuse_write_to_locked_rows(
                        &database,
                        &table_name,
                        &condition,
                        tx,
                    ))
                    .await
                    {
                        error!("{}", e);
                        return ddl_error(&e);
                    }
                }
            }

            if table_name.is_empty() {
                error!("DELETE missing table name");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "DELETE missing table name";
                result.extend_from_slice(error_message.as_bytes());
                return result;
            }

            // ROW EXCLUSIVE on the target, a partitioned one by its name.
            let table_ref = match Box::pin(database.table_for(&table_name, owner(), TableMode::RowExclusive)).await {
                Ok(table_ref) => table_ref,
                Err(e) => return ddl_error(&e),
            };
            let table_clone = table_ref.as_ref().clone();
            let table = if let Some(table) = table_clone {
                table
            } else {
                error!("Table not found");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Table not found";
                result.extend_from_slice(error_message.as_bytes());
                return result;
            };

            let schema_sv = table
                .schema
                .fields
                .iter()
                .cloned()
                .collect::<smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>>();

            // The reason travels with the failure: an unsupported predicate
            // used to reach the client as "Cannot parse DELETE query" while
            // the same clause in a SELECT named the actual feature.
            let delete_plan =
                match crate::core::rql::lexer_dr::delete_row_with_reason(&query, &schema_sv) {
                    (Some(plan), _) => plan,
                    (None, reason) => {
                        let message =
                            reason.unwrap_or_else(|| "Cannot parse DELETE query".to_string());
                        error!("Cannot parse DELETE query: {}", message);
                        return ddl_error(&message);
                    }
                };

            // A partitioned table's rows are in its partitions (§9); `table`
            // is its shell, whose schema the parse read.
            if let Some(pt) = database.partitioned_table(&table_name) {
                return Box::pin(execute_partitioned_delete(
                    &query,
                    delete_plan,
                    pt,
                    &database,
                    &table_name,
                    depth,
                    tx,
                ))
                .await;
            }

            // Referencing rows are dealt with before the parent goes, so a
            // blocking action can still refuse and a cascade still has the
            // parent's key to match on.
            // One snapshot for the statement when foreign keys point here: the
            // keys whose referencing rows are dealt with and the rows the scan
            // deletes must be the same rows. See `keys_about_to_be_deleted`.
            // Or the one its reads took, when it read first (a subquery).
            let referenced = !referencing_keys(&database, &table_name).is_empty();
            let statement_visibility = statement_snapshot_for(depth, || {
                if referenced { table.statement_visibility(tx) } else { table.statement_visibility_deferred(tx) }
            })
            .or_else(|| referenced.then(|| table.visibility_for(tx)));
            match match statement_visibility.as_ref().filter(|_| referenced) {
                Some(visibility) => {
                    keys_about_to_be_deleted(
                        &database,
                        &crate::core::database::Relation::Plain(table.clone()),
                        &table_name,
                        &delete_plan.where_clause,
                        &delete_plan.query_row_fetch,
                        visibility,
                        tx,
                    )
                    .await
                }
                None => Ok(Vec::new()),
            } {
                Ok(keys) => {
                    if let Err(e) = apply_on_delete(&database, &table_name, &keys, depth, tx).await
                    {
                        error!("{}", e);
                        return ddl_error(&e);
                    }
                }
                Err(e) => {
                    error!("Could not read the rows a DELETE would affect: {}", e);
                    return ddl_error(&e);
                }
            }

            // `RETURNING` on a `DELETE` reports the rows as they were: the
            // ones the delete itself removed, read under its lock and snapshot.
            // The list is resolved first, so a bad one fails the statement
            // before it deletes anything.
            let returning_wanted = match returning_planned(&query_sql(&query), &table.schema.fields) {
                Ok(wanted) => wanted,
                Err(e) => {
                    error!("Cannot resolve RETURNING: {}", e);
                    let mut result = vec![3u8];
                    result.extend_from_slice(e.as_bytes());
                    return result;
                }
            };
            let mut before: Vec<crate::core::row::row::Row> = Vec::new();

            match table
                .delete_rows_as_of(
                    &delete_plan.where_clause,
                    delete_plan.query_row_fetch,
                    None,
                    tx,
                    statement_visibility,
                    returning_wanted.then_some(&mut before),
                )
                .await
            {
                Ok(n) => {
                    info!("Deleted {} rows", n);
                    note_write(&database, tx, depth, &query_sql(&query), Some(&table_name));
                    let returning = if returning_wanted {
                        match returning_rows_for_write(&table, &query_sql(&query), before, None, &[], &database, tx).await {
                            Ok(rows) => rows,
                            Err(e) => {
                                error!("Cannot resolve RETURNING: {}", e);
                                let mut result = vec![3u8];
                                result.extend_from_slice(e.as_bytes());
                                return result;
                            }
                        }
                    } else {
                        None
                    };
                    if let Some(rows) = returning {
                        let mut result = vec![2u8];
                        for row in &rows {
                            row.extend_serialized(&mut result);
                        }
                        return result;
                    }
                    // Keep status=0 for backward compatibility, but also include rows-affected.
                    let mut result = vec![0u8];
                    result.extend_from_slice(&n.to_le_bytes());
                    return result;
                }
                Err(e) => {
                    // The retry marker is an internal signal for `execute_in_txn`;
                    // it must never appear in a log a human reads.
                    error!(
                        "Failed to delete rows: {}",
                        clean_conflict_text(&e.to_string())
                    );
                    let mut result = vec![3u8];
                    result.extend_from_slice(e.to_string().as_bytes());
                    return result;
                }
            }
        }
        QueryPurpose::QueryRows(query_rows) => {
            crate::core::rql::prepared::note_select_entry();
            // `A UNION [ALL] B` is run branch by branch and the rows combined.
            // Kept out of this frame — see `execute_union`.
            if crate::core::rql::join::split_union(&query_rows.query).is_some() {
                return Box::pin(execute_union(&query_rows.query, &database, tx)).await;
            }

            // An uncorrelated `IN (SELECT …)` is run first and folded into the
            // outer statement as a literal list, which then follows the ordinary
            // path — including filter pruning on the resulting equalities.
            if crate::core::rql::subquery::find_in_subquery(&query_rows.query).is_some()
                || crate::core::rql::subquery::find_exists(&query_rows.query).is_some()
                || crate::core::rql::subquery::find_scalar_subquery(&query_rows.query).is_some()
            {
                match substitute_in_subqueries(&query_rows.query, &database, tx).await {
                    Ok(flattened) => {
                        let Some(purpose) = recognize_query_purpose(&flattened) else {
                            let mut result = vec![3u8];
                            result.extend_from_slice(
                                b"cannot parse the statement after subquery substitution",
                            );
                            return result;
                        };
                        return Box::pin(execute_in_txn(purpose, database.clone(), tx)).await;
                    }
                    Err(e) => {
                        error!("Cannot run subquery: {}", e);
                        let mut result = vec![3u8];
                        result.extend_from_slice(e.as_bytes());
                        return result;
                    }
                }
            }

            // A joined SELECT has one schema and one RowFetch per relation, so
            // it takes its own path rather than the single-table plan.
            if crate::core::rql::join::statement_has_join(&query_rows.query) {
                return Box::pin(execute_join(&query_rows.query, database, tx)).await;
            }

            // Handle query rows
            let table_name = &query_rows.table_name;
            debug!("Query rows from table: {}", table_name);

            // A partitioned table's rows are in its partitions: read under one
            // snapshot, routed only on proof (§4, §8). Its shell, which `tables`
            // holds under this name, has the schema and no rows.
            //
            // ACCESS SHARE on it first, a partitioned table by its logical
            // name (a locking clause adds ROW EXCLUSIVE where it locks rows).
            let relation = match Box::pin(database.relation_for(table_name, owner(), TableMode::AccessShare)).await {
                Ok(relation) => relation,
                Err(e) => return ddl_error(&e),
            };
            if let Some(crate::core::database::Relation::Partitioned(pt)) = relation.clone() {
                return Box::pin(execute_partitioned_select(&query, pt, &database, tx, need)).await;
            }

            let table_clone = match relation {
                Some(crate::core::database::Relation::Plain(table)) => Some(table),
                _ => None,
            };
            let table = if let Some(table) = table_clone {
                table
            } else {
                error!("Table not found");
                let mut result: [u8; 1] = [1u8; 1];
                result[0] = 3;
                let mut result = result.to_vec();
                let error_message = "Table not found";
                let mut error_message_bytes = Vec::with_capacity(error_message.len());
                error_message_bytes.extend_from_slice(error_message.as_bytes());
                result.extend_from_slice(&mut error_message_bytes);
                return result.to_vec();
            };
            let schema = &table.schema;

            // A parse failure carries the reason the statement was rejected —
            // an unsupported predicate form, an unknown column. Reporting it
            // verbatim is the difference between the client seeing which
            // feature is missing and seeing a generic mismatch.
            #[cfg(test)]
            crate::core::rql::prepared::test_counters::note_parse(table_name);
            let query_row = match row_fetch_from_select_query(&query, &schema.fields) {
                Ok(mut q) => {
                    apply_subquery_need(&mut q, need);
                    q
                }
                Err(e) => {
                    error!("Cannot query rows: {}", e);
                    let mut result = vec![3u8];
                    result.extend_from_slice(e.as_bytes());
                    return result;
                }
            };

            crate::core::rql::prepared::note_plain(
                table_name,
                &query_row,
                database.slot_shape_epoch(table_name).unwrap_or(0),
            );
            let source = crate::core::rql::row_source::RowSource::Single(table.clone());
            Box::pin(execute_query_rows(query_row, source, &database, tx)).await
        }
    }
}

/// A single-table `SELECT` on a partitioned table (`docs/partitioning.md`
/// §4, §8, S10): parsed against the shell's schema, then read through a
/// [`RowSource`](crate::core::rql::row_source::RowSource) that routes the
/// parse's own `WHERE` clause and reads every reached partition under one
/// snapshot. Everything after the scan is the plain table's path.
///
/// A prepared statement on it learns its parse like a plain table's (S15):
/// the parse is noted here, under the logical name, and a learned execution
/// comes back through [`execute_prepared_rows`], which routes that
/// execution's own rendered `WHERE` (I11). `FOR UPDATE`/`SHARE` locks each
/// returned row under the logical name by the shell's identifying key
/// (`lock_selected_rows`, phase 3 P3), so a lock taken through any partition
/// is the one a writer through the logical name checks.
async fn execute_partitioned_select(
    query: &QueryPurpose,
    pt: Arc<crate::core::partition::table::PartitionedTable>,
    database: &Arc<Database>,
    tx: TxId,
    need: SubqueryNeed,
) -> Vec<u8> {
    let name = pt.name().to_string();
    #[cfg(test)]
    crate::core::rql::prepared::test_counters::note_parse(&name);
    let query_row = match row_fetch_from_select_query(query, &pt.shell().schema.fields) {
        Ok(mut q) => {
            apply_subquery_need(&mut q, need);
            q
        }
        Err(e) => {
            error!("Cannot query rows: {}", e);
            let mut result = vec![3u8];
            result.extend_from_slice(e.as_bytes());
            return result;
        }
    };
    crate::core::rql::prepared::note_plain(&name, &query_row, database.slot_shape_epoch(&name).unwrap_or(0));
    match partitioned_source(&query_row, pt, None, tx) {
        Ok(source) => execute_query_rows(query_row, source, database, tx).await,
        Err(refusal) => refusal,
    }
}

/// The read half of a partitioned `SELECT`, shared by the text path and a
/// learned parse, up to the plain table's pipeline (`execute_query_rows`,
/// which the caller runs over the source): a source routed on this
/// `query_row`'s own `WHERE`, or on a learned parse's
/// bound `key` (S16b) (one snapshot, then the map, then the flags, §4.4).
/// `Err` is the reply. Not `async`, so the parse is not moved through one
/// more future on its way to the scan (the S16b profile).
fn partitioned_source(
    query_row: &crate::core::rql::lexer_query::QueryRows,
    pt: Arc<crate::core::partition::table::PartitionedTable>,
    key: Option<&crate::core::rql::prepared::BoundKey>,
    tx: TxId,
) -> Result<crate::core::rql::row_source::RowSource, Vec<u8>> {
    // `FanOut::plan` logs a failure with the table's name, so the success
    // path neither clones the table's `Arc` nor its name.
    crate::core::rql::row_source::RowSource::for_select(
        crate::core::database::Relation::Partitioned(pt),
        &query_row.where_clause,
        key,
        tx,
    )
    .map_err(|e| ddl_error(&e.to_string()))
}

/// The tables a `VACUUM` of partitioned table `pt` visits: its materialized
/// partitions, by pid. An unmaterialized one has no files to vacuum.
fn vacuum_targets(
    pt: &crate::core::partition::table::PartitionedTable,
) -> std::io::Result<
    Vec<rclite::Arc<crate::core::row::table::Table<crate::core::storage_providers::backend::DbStorageProvider>>>,
> {
    pt.check()?;
    let epoch = pt.epoch()?;
    Ok(pt
        .materialized(&epoch, &crate::core::partition::Route::All)?
        .into_iter()
        .map(|(_, part)| part)
        .collect())
}

/// What the partitioned `UPDATE` and `DELETE` check before they write, in
/// common: a `RETURNING` list that resolves against the shell's schema,
/// which every partition shares. A bad list fails as it does on a plain
/// table, before anything is written. `Ok(true)`: the statement returns rows
/// (S17). Foreign keys, either way, are the arms' own work since phase 3 P4,
/// as on a plain table.
fn check_partitioned_write(
    shell: &crate::core::row::table::Table<crate::core::storage_providers::backend::DbStorageProvider>,
    sql: &str,
) -> Result<bool, String> {
    returning_planned(sql, &shell.schema.fields)
}

/// A partitioned write's reply: the `RETURNING` rows (status 2, zero rows for
/// an empty route) when the statement has a list, else the count. Built only
/// after every partition succeeded, from the pre-images the partitions
/// collected under the statement's one snapshot (several partitions) or the
/// partition's own `write_lock` (one), with the assignments overlaid as the
/// plain arms do: never read again, so no row is reported that the statement
/// did not change. The shell supplies the schema; no data accessor follows.
async fn partitioned_write_reply(
    pt: &crate::core::partition::table::PartitionedTable,
    sql: &str,
    n: u64,
    returning: Option<(
        Vec<crate::core::row::row::Row>,
        Option<&smallvec::SmallVec<[crate::core::row::row::ColumnWritePayload; 32]>>,
        &[(u32, crate::core::rql::expr::Expr)],
    )>,
    database: &Arc<Database>,
    tx: TxId,
) -> Vec<u8> {
    if let Some((before, updates, expressions)) = returning {
        match returning_rows_for_write(pt.shell(), sql, before, updates, expressions, database, tx).await {
            Ok(Some(rows)) => {
                let mut result = vec![2u8];
                for row in &rows {
                    row.extend_serialized(&mut result);
                }
                return result;
            }
            Ok(None) => {}
            Err(e) => {
                error!("Cannot resolve RETURNING: {}", e);
                return ddl_error(&e);
            }
        }
    }
    let mut result = vec![0u8];
    result.extend_from_slice(&n.to_le_bytes());
    result
}

/// An `UPDATE` on a partitioned table (`docs/partitioning.md` §9, S11), from
/// the plan the arm parsed against the shell's schema, after the arm's
/// NOT NULL, generated-column and CHECK work.
///
/// Before anything is written it resolves the `RETURNING` list and refuses
/// any key assignment not proven to keep every matched row's point
/// ([`PartitionedTable::assignments_keep_point`]): a key change would move
/// the row to another partition (phase 6), and refused here it writes
/// nothing in any partition, where the range guard alone would refuse it
/// only after earlier partitions had written. The guard still checks every
/// merged row. Then [`write_partitions`] visits the routed partitions, each
/// collecting its pre-images when the statement returns rows (S17); the
/// rows are built only once every partition has succeeded, so a failed
/// statement returns none.
///
/// Prepared, it stays on the text path (`note_rewritten`), so every
/// execution routes its own values.
///
/// [`PartitionedTable::assignments_keep_point`]: crate::core::partition::table::PartitionedTable::assignments_keep_point
/// [`write_partitions`]: crate::core::rql::row_source::write_partitions
async fn execute_partitioned_update(
    query: &QueryPurpose,
    update_plan: crate::core::rql::lexer_ur::UpdateRow,
    pt: Arc<crate::core::partition::table::PartitionedTable>,
    database: &Arc<Database>,
    table_name: &str,
    depth: usize,
    tx: TxId,
) -> Vec<u8> {
    crate::core::rql::prepared::note_rewritten();
    let sql = query_sql(query);
    let returning_wanted = match check_partitioned_write(pt.shell(), &sql) {
        Ok(wanted) => wanted,
        Err(e) => {
            error!("{}", e);
            return ddl_error(&e);
        }
    };
    let keeps_point = {
        let assigned: Vec<(u32, &[u8])> =
            update_plan.updates.iter().map(|u| (u.write_order, u.data.into_slice())).collect();
        let computed: Vec<u32> = update_plan.expressions.iter().map(|(wo, _)| *wo).collect();
        pt.assignments_keep_point(&update_plan.where_clause, &assigned, &computed)
    };
    match keeps_point {
        Ok(true) => {}
        Ok(false) => {
            error!("UPDATE on {}: {}", table_name, crate::core::partition::guard::KEY_CHANGE_REFUSAL);
            return ddl_error(crate::core::partition::guard::KEY_CHANGE_REFUSAL);
        }
        Err(e) => {
            error!("Cannot update partitioned table {}: {}", table_name, e);
            return ddl_error(&e.to_string());
        }
    }
    // Foreign keys (phase 3 P4), as the plain arm runs them and in its
    // order: one snapshot for the statement when they need to know which
    // rows it changes, the child side's checks, the parent side's keys
    // locked before the write and acted on after it. Every read routes on
    // the statement's `WHERE` over the partitions, under that snapshot.
    let (statement_visibility, key_changes) =
        match partitioned_update_foreign_keys(database, &pt, table_name, &update_plan, &sql, depth, tx).await {
            Ok(found) => found,
            Err(e) => {
                error!("{}", e);
                return ddl_error(&e);
            }
        };
    let crate::core::rql::lexer_ur::UpdateRow {
        where_clause,
        query_row_fetch,
        updates,
        expressions,
        ..
    } = update_plan;
    // The write consumes its copies of the assignments; `RETURNING` overlays
    // these after it, as the plain arm does.
    let returning_updates: Option<smallvec::SmallVec<[crate::core::row::row::ColumnWritePayload; 32]>> =
        returning_wanted.then(|| {
            updates
                .iter()
                .map(|u| crate::core::row::row::ColumnWritePayload {
                    data: u.data.clone(),
                    write_order: u.write_order,
                    column_type: u.column_type.clone(),
                    size: u.size,
                })
                .collect()
        });
    let mut before: Vec<crate::core::row::row::Row> = Vec::new();
    let outcome = crate::core::rql::row_source::write_partitions_as_of(
        &pt,
        &where_clause,
        tx,
        statement_visibility,
        returning_wanted.then_some(&mut before),
        |part, visibility, collecting| {
            // A payload is not `Clone`; each partition consumes its own copy.
            let updates: smallvec::SmallVec<[crate::core::row::row::ColumnWritePayload; 32]> = updates
                .iter()
                .map(|u| crate::core::row::row::ColumnWritePayload {
                    data: u.data.clone(),
                    write_order: u.write_order,
                    column_type: u.column_type.clone(),
                    size: u.size,
                })
                .collect();
            let (fetch, expressions, where_clause) = (query_row_fetch.clone(), &expressions[..], &where_clause);
            async move {
                let mut before = Vec::new();
                let n = part
                    .update_rows_as_of(
                        where_clause,
                        fetch,
                        updates,
                        expressions,
                        None,
                        tx,
                        visibility,
                        collecting.then_some(&mut before),
                    )
                    .await?;
                Ok((n, before))
            }
        },
    )
    .await;
    match outcome {
        Ok(n) => {
            info!("Updated {} rows", n);
            if let Some(changes) = key_changes.as_ref() {
                if let Err(e) = apply_on_update(database, table_name, changes, depth, tx).await {
                    error!("{}", e);
                    return ddl_error(&e);
                }
            }
            note_write(database, tx, depth, &sql, Some(table_name));
            let returning = returning_updates.as_ref().map(|u| (before, Some(u), &expressions[..]));
            partitioned_write_reply(&pt, &sql, n, returning, database, tx).await
        }
        Err(e) => {
            // The retry marker is for the retry loops, never a log.
            error!("Failed to update rows: {}", clean_conflict_text(&e.to_string()));
            let mut result = vec![3u8];
            result.extend_from_slice(e.to_string().as_bytes());
            result
        }
    }
}

/// A partitioned `UPDATE`'s foreign-key work before it writes (phase 3 P4),
/// the plain arm's steps over the partitions: the statement snapshot when a
/// key needs it (`update_needs_statement_snapshot`), the child side's
/// checks (`check_foreign_keys_for_update`, every assigned key probed in
/// its parent, which routes), and the parent side's moving keys
/// (`keys_about_to_change`, read over the routed partitions under that
/// snapshot) locked before the write (`lock_keys_before_update`). Returns
/// the snapshot the write must use and the keys `apply_on_update` acts on
/// after it.
async fn partitioned_update_foreign_keys(
    database: &Arc<Database>,
    pt: &Arc<crate::core::partition::table::PartitionedTable>,
    table_name: &str,
    plan: &crate::core::rql::lexer_ur::UpdateRow,
    sql: &str,
    depth: usize,
    tx: TxId,
) -> Result<(Option<crate::core::mvcc::Visibility>, Option<KeyChanges>), String> {
    let schema = &pt.shell().schema;
    let foreign_keys_read = update_needs_statement_snapshot(database, schema, table_name, plan);
    let statement_visibility = statement_snapshot_for(depth, || pt.statement_visibility(tx))
        .or_else(|| foreign_keys_read.then(|| pt.visibility_for(tx)));
    let foreign_key_visibility = statement_visibility.as_ref().filter(|_| foreign_keys_read);
    let target = crate::core::database::Relation::Partitioned(pt.clone());
    if let Some(assignments) = crate::core::rql::lexer_ur::assignments_of_update(sql) {
        check_foreign_keys_for_update(
            database,
            &target,
            table_name,
            plan,
            &assignments,
            foreign_key_visibility,
            tx,
        )
        .await?;
    }
    let key_changes = match foreign_key_visibility {
        None => None,
        Some(visibility) => keys_about_to_change(database, &target, table_name, plan, visibility, tx).await?,
    };
    if let Some(changes) = key_changes.as_ref() {
        lock_keys_before_update(database, table_name, changes, tx).await?;
    }
    Ok((statement_visibility, key_changes))
}

/// A `DELETE` on a partitioned table (§9, S11): the `RETURNING` list resolved
/// before anything is deleted, then [`write_partitions`] over the routed
/// partitions, each collecting the rows it deleted when the statement returns
/// them (S17), reported once every partition has succeeded. Prepared, it
/// stays on the text path, as the `UPDATE` does.
///
/// [`write_partitions`]: crate::core::rql::row_source::write_partitions
async fn execute_partitioned_delete(
    query: &QueryPurpose,
    delete_plan: crate::core::rql::lexer_dr::DeleteRow,
    pt: Arc<crate::core::partition::table::PartitionedTable>,
    database: &Arc<Database>,
    table_name: &str,
    depth: usize,
    tx: TxId,
) -> Vec<u8> {
    crate::core::rql::prepared::note_rewritten();
    let sql = query_sql(query);
    let returning_wanted = match check_partitioned_write(pt.shell(), &sql) {
        Ok(wanted) => wanted,
        Err(e) => {
            error!("{}", e);
            return ddl_error(&e);
        }
    };
    let crate::core::rql::lexer_dr::DeleteRow { where_clause, query_row_fetch, .. } = delete_plan;
    // Referencing rows first (phase 3 P4), as the plain arm deals with them:
    // one snapshot for the statement when a foreign key points here, the
    // keys the routed partitions are about to lose read under it, their
    // referenced-key locks taken under the logical name and each child's
    // action run, and the same snapshot handed to every partition's write.
    let referenced = !referencing_keys(database, table_name).is_empty();
    let statement_visibility = statement_snapshot_for(depth, || pt.statement_visibility(tx))
        .or_else(|| referenced.then(|| pt.visibility_for(tx)));
    if let Some(visibility) = statement_visibility.as_ref().filter(|_| referenced) {
        let target = crate::core::database::Relation::Partitioned(pt.clone());
        let keys = match keys_about_to_be_deleted(database, &target, table_name, &where_clause, &query_row_fetch, visibility, tx)
            .await
        {
            Ok(keys) => keys,
            Err(e) => {
                error!("Could not read the rows a DELETE would affect: {}", e);
                return ddl_error(&e);
            }
        };
        if let Err(e) = apply_on_delete(database, table_name, &keys, depth, tx).await {
            error!("{}", e);
            return ddl_error(&e);
        }
    }
    let mut before: Vec<crate::core::row::row::Row> = Vec::new();
    let outcome = crate::core::rql::row_source::write_partitions_as_of(
        &pt,
        &where_clause,
        tx,
        statement_visibility,
        returning_wanted.then_some(&mut before),
        |part, visibility, collecting| {
            let (fetch, where_clause) = (query_row_fetch.clone(), &where_clause);
            async move {
                let mut before = Vec::new();
                let n = part
                    .delete_rows_as_of(where_clause, fetch, None, tx, visibility, collecting.then_some(&mut before))
                    .await?;
                Ok((n, before))
            }
        },
    )
    .await;
    match outcome {
        Ok(n) => {
            info!("Deleted {} rows", n);
            note_write(database, tx, depth, &sql, Some(table_name));
            let returning = returning_wanted.then_some((before, None, &[][..]));
            partitioned_write_reply(&pt, &sql, n, returning, database, tx).await
        }
        Err(e) => {
            error!("Failed to delete rows: {}", clean_conflict_text(&e.to_string()));
            let mut result = vec![3u8];
            result.extend_from_slice(e.to_string().as_bytes());
            result
        }
    }
}

/// A parsed single-table `SELECT`, run: the half of the `QueryRows` arm that
/// reads only the parse, never the statement text. A prepared statement
/// enters here with a parse it cached at its first execution.
///
/// A locking read (`FOR UPDATE`/`SHARE`) scans under a snapshot, then locks
/// what it found. A row it locks only then may have been changed and
/// committed after the snapshot, by the holder it waited for or by one that
/// finished between the scan and the lock (`SKIP LOCKED` never waits, and
/// meets that just the same): returning the row as scanned handed a claim
/// query a job its holder had already finished. So when a lock this
/// transaction did not already hold was taken and some transaction the scan
/// could not see has committed since, the statement is scanned again at a
/// fresh snapshot, as PostgreSQL re-reads a locked row's latest version: a
/// row gone or no longer matching drops out, a changed one comes back as it
/// is now. The rows locked by an earlier round stay locked, so a later round
/// takes new locks only for rows newly reached, and the rounds end. Under
/// `REPEATABLE READ` the snapshot is pinned and every round sees the same
/// rows.
pub(crate) async fn execute_query_rows(
    query_row: crate::core::rql::lexer_query::QueryRows,
    source: crate::core::rql::row_source::RowSource,
    database: &Arc<Database>,
    tx: TxId,
) -> Vec<u8> {
    if query_row.row_lock.is_none() || query_row.aggregate.is_some() {
        return execute_query_rows_once(query_row, source, database, tx, None).await;
    }
    let relation = source.rescan_relation();
    // Inside a write that reads first (`UPDATE … SET job = (SELECT … FOR
    // UPDATE SKIP LOCKED)`), a locking read is not one of the reads that
    // share the statement's snapshot: it locks the latest versions, and a
    // round scanned under the statement's older snapshot, then re-scanned
    // under the same one, handed a job its holder had finished (79 of 300
    // claimed twice). Each round reads in a scope of its own, planned there
    // too, so its scan, and the `since` its staleness is judged against,
    // are taken fresh.
    let own_rounds = crate::core::rql::statement_snapshot::in_write_scope();
    let mut source = (!own_rounds).then_some(source);
    for _ in 0..MAX_LOCKING_ROUNDS {
        let round = async {
            let source = match source.take() {
                Some(source) => source,
                None => match crate::core::rql::row_source::RowSource::for_select(
                    relation.clone(),
                    &query_row.where_clause,
                    None,
                    tx,
                ) {
                    Ok(source) => source,
                    Err(e) => return ddl_error(&e.to_string()),
                },
            };
            // What the scan can see, taken no later than its own snapshot: a
            // partitioned source's planned one, else one taken now (a plain
            // table's scan takes its own after this, so this one is no newer).
            let since = source.planned_visibility().unwrap_or_else(|| {
                crate::core::mvcc::Visibility::for_snapshot(database.transactions.snapshot(tx), database.transactions.clone())
            });
            execute_query_rows_once(query_row.clone(), source, database, tx, Some(&since)).await
        };
        let out = if own_rounds {
            crate::core::rql::statement_snapshot::scope(round).await
        } else {
            round.await
        };
        if out.first() != Some(&3u8) || &out[1..] != LOCKED_ROWS_STALE.as_bytes() {
            return out;
        }
    }
    ddl_error(
        "could not serialize access due to concurrent update: the rows a locking read found kept changing \
         as it locked them",
    )
}

/// How many times a locking read scans before it gives up on rows that keep
/// changing under it ([`execute_query_rows`]).
const MAX_LOCKING_ROUNDS: usize = 4;

/// What `lock_selected_rows` reports when a row it newly locked may have
/// changed since the scan: the caller scans again. Never reaches a client.
const LOCKED_ROWS_STALE: &str = "\u{0}locked rows stale";

/// Whether a transaction `since` could not see has committed since: then a
/// row read under it may no longer be the row's latest version. Conservative
/// past a wide id range.
fn committed_since(database: &Arc<Database>, since: &crate::core::mvcc::Visibility) -> bool {
    let crate::core::mvcc::Visibility::AsOf { snapshot, .. } = since else {
        return false;
    };
    let (from, to) = (snapshot.xmin, database.transactions.next_id());
    if to.saturating_sub(from) > 4096 {
        return true;
    }
    (from..to).any(|id| {
        matches!(
            database.transactions.status().outcome(id),
            crate::core::mvcc::transaction::TxOutcome::Committed
        ) && !since.visible(false, id, crate::core::mvcc::transaction::NO_TX)
    })
}

/// Boxed and type-erased, so [`execute_query_rows`]'s loop around it does not
/// deepen every caller's future type (it overflowed the trait solver's
/// recursion limit at `process_incoming_queries`).
fn execute_query_rows_once<'a>(
    query_row: crate::core::rql::lexer_query::QueryRows,
    source: crate::core::rql::row_source::RowSource,
    database: &'a Arc<Database>,
    tx: TxId,
    // A locking read's round: the snapshot its scan reads no later than.
    since: Option<&'a crate::core::mvcc::Visibility>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<u8>> + Send + 'a>> {
    Box::pin(execute_query_rows_body(query_row, source, database, tx, since))
}

async fn execute_query_rows_body(
    mut query_row: crate::core::rql::lexer_query::QueryRows,
    source: crate::core::rql::row_source::RowSource,
    database: &Arc<Database>,
    tx: TxId,
    since: Option<&crate::core::mvcc::Visibility>,
) -> Vec<u8> {
    let schema = source.schema();
    // A locking read keys each row by the table's identifying key, which the
    // select list need not name: its columns (and a nullable one's NULL
    // companion) ride after everything else the scan returns, are read off
    // each row before it is trimmed, and never reach the client. One key per
    // table, whichever columns a statement selects, so every locker and
    // writer of a row names it alike.
    let lock_fetch: Option<(IdentifyingKey, Vec<(u64, Option<u64>)>)> = match &query_row.row_lock {
        Some(_) if query_row.aggregate.is_none() => {
            let Some(key) = identifying_key(schema) else {
                return ddl_error(&format!(
                    "FOR UPDATE needs a unique column on '{}' to identify the rows it locks",
                    schema.name
                ));
            };
            let calc = crate::core::row::schema::SchemaCalculator::default();
            let mut ids = Vec::with_capacity(key.fields.len());
            for field in &key.fields {
                let companion = crate::core::rql::nullable::flag_field(&field.name, &schema.fields).cloned();
                for f in std::iter::once(field).chain(companion.as_ref()) {
                    let (offset, schema_id) = calc.calculate_schema_offset(&f.name, &schema.fields);
                    query_row.requested_row_fetch.columns_fetching_data.push(
                        crate::core::row::row::ColumnFetchingData {
                            column_offset: offset,
                            column_type: f.db_type.physical(),
                            size: f.size,
                            schema_id,
                        },
                    );
                }
                ids.push((field.write_order, companion.map(|c| c.write_order)));
            }
            Some((key, ids))
        }
        _ => None,
    };
    // An aggregate query folds the scanned rows into groups, so it
    // takes the row-mode path: the bytes path never materialises a
    // `Vec<Row>` for the fold to read.
    if let Some(plan) = query_row.aggregate.clone() {
        // `SELECT COUNT(*)` and nothing else: the scan counts matches
        // and never builds a row. The general path below fetches every
        // matching row into a `Vec<Row>` for the fold to walk, which is
        // ~1.5 GB of `Row` for a million-row count and measured four
        // times the cost of *sorting* the same table.
        //
        // `is_row_count_only` is where the safety lives — in particular
        // it rejects `COUNT(*) FILTER (WHERE …)`, whose `arg` is `None`
        // exactly like a bare `COUNT(*)` and which would otherwise be
        // answered with the unfiltered count.
        if crate::core::rql::aggregate::is_row_count_only(&plan) {
            match source
                .count(
                    &query_row.where_clause,
                    &schema.fields.to_vec(),
                    query_row.query_row_fetch,
                    tx,
                )
                .await
            {
                Ok(count) => {
                    let mut result = vec![2u8];
                    for row in &crate::core::rql::aggregate::row_count_result(&plan, count)
                    {
                        row.extend_serialized(&mut result);
                    }
                    return result;
                }
                Err(e) => {
                    error!("Failed to execute count query: {}", e);
                    let mut result = vec![3u8];
                    result.extend_from_slice(e.to_string().as_bytes());
                    return result;
                }
            }
        }

        // The fold has to see the `__null_` companions of every column
        // it reads, or a NULL is indistinguishable from the zeros its
        // value column holds. The select path fetches them because the
        // projection needs them; the aggregate fetch asks only for the
        // value columns, so they are added here.
        let aggregate_fetch = {
            let mut wanted: Vec<crate::core::row::schema::SchemaField> = Vec::new();
            for c in &query_row.requested_row_fetch.columns_fetching_data {
                if let Some(f) = schema.fields.iter().find(|f| f.write_order == c.schema_id)
                {
                    wanted.push(f.clone());
                }
            }
            for f in wanted.clone() {
                if let Some(flag) =
                    crate::core::rql::nullable::flag_field(&f.name, &schema.fields)
                {
                    if !wanted.iter().any(|w| w.name == flag.name) {
                        wanted.push(flag.clone());
                    }
                }
            }
            crate::core::rql::join::fetch_for(&wanted, &schema.fields)
        };
        // Folded straight from the scan's serialized rows, one decoded
        // row at a time: the `Vec<Row>` the path below builds is
        // 1.5 KB per matched row (1.56 GB for a million) before the
        // fold reads any of it. A shredded JSON column still takes
        // that path, because its documents are hydrated (async, per
        // row) before grouping. `RDB_FOLD_ROWS=1` forces it, for A/B.
        if schema.json_columns.is_empty() && !legacy_row_fold() {
            let folded = match source
                .query_bytes(
                    &query_row.where_clause,
                    &schema.fields,
                    query_row.query_row_fetch.clone(),
                    aggregate_fetch.clone(),
                    // No limit and no ordering, as below.
                    None,
                    None,
                    tx,
                )
                .await
            {
                Ok(bytes) => {
                    let mut result = vec![2u8];
                    fold_serialized(&schema, &plan, &bytes, &mut result).map(|()| result)
                }
                Err(e) => Err(e.to_string()),
            };
            return match folded {
                Ok(result) => result,
                Err(e) => {
                    error!("Aggregate failed: {}", e);
                    let mut result = vec![3u8];
                    result.extend_from_slice(e.as_bytes());
                    result
                }
            };
        }
        match source
            .query_rows(
                &query_row.where_clause,
                &schema.fields.to_vec(),
                query_row.query_row_fetch,
                aggregate_fetch,
                // Deliberately no limit and no ordering: both apply to
                // the aggregated output, and a limit here would also
                // enable early scan termination and truncate the input.
                None,
                None,
                tx,
            )
            .await
        {
            Ok(mut rows) => {
                retype_nulls_from_flags(&schema, &mut rows);
                hydrate_json_columns(&database, &schema, &mut rows, tx).await;
                let grouped = match crate::core::rql::aggregate::fold(&plan, &rows) {
                    Ok(folded) => folded,
                    Err(e) => {
                        error!("Aggregate failed: {}", e);
                        let mut result = vec![3u8];
                        result.extend_from_slice(e.as_bytes());
                        return result;
                    }
                };
                let mut result = vec![2u8];
                for row in &grouped {
                    row.extend_serialized(&mut result);
                }
                return result;
            }
            Err(e) => {
                error!("Failed to execute aggregate query: {}", e);
                let mut result = vec![3u8];
                result.extend_from_slice(e.to_string().as_bytes());
                return result;
            }
        }
    }

    let offset = query_row.offset.unwrap_or(0);
    // A sort key that was not selected is fetched anyway and trimmed
    // here, so the client never sees the extra column.
    let visible_columns = query_row.visible_columns;
    let trim_sort_key =
        visible_columns < query_row.requested_row_fetch.columns_fetching_data.len();
    // A predicate the scan cannot answer from the requested columns
    // alone makes it fetch the ones it needs, and those came back in
    // the row. Without decoding it there is nothing to trim them from,
    // so the extra column went out to the client — which counted the
    // fields against the RowDescription and dropped the connection
    // with "unexpected field count in D message". Only shapes the
    // scan actually over-fetches for set this, so the ordinary path
    // still skips the decode. The parse can only say the predicate
    // reads another column; whether the rows came back holding it is
    // the scan's answer, combined with this once it returns.
    let predicate_needs_extra_columns = query_row.predicate_needs_extra_columns;
    // `FOR UPDATE` has to see the rows to lock them, so it forces the
    // decode the same way the other row-level steps do.
    // Window functions are computed after the scan, over every row
    // that survived `WHERE` — which is where SQL puts them, and why
    // they need the whole result set rather than one row at a time.
    let window_slots = query_row.window_slots.clone();
    let has_windows = !window_slots.is_empty();
    // Expression sort keys: evaluated per fetched row, sorted here,
    // because the scan can only sort by stored columns.
    let order_exprs = query_row.order_expressions.clone();
    let has_order_exprs = !order_exprs.is_empty();
    let row_lock = query_row.row_lock;
    let lock_table_name = schema.name.clone();
    let skip_locked = matches!(
        row_lock,
        Some(crate::core::rql::row_lock::RowLock {
            behaviour: crate::core::rql::row_lock::LockWait::Skip,
            ..
        })
    );
    let distinct = query_row.distinct;
    let written_limit = query_row.written_limit;
    let null_flags = query_row.null_flag_positions.clone();
    let has_nullable = null_flags.iter().any(Option::is_some);

    // Which output positions hold a JSON reference, and the table to
    // rebuild each from. A JSON column stores a UUID; the client is
    // owed the document.
    // Keyed by `schema_id`, which names the column, rather than by
    // output position.
    //
    // A position is not stable: a constant or a row expression fills an
    // output slot no fetched column occupies, and the select list may
    // put it first. Indexing by position then rewrote the wrong slot
    // and the document came back NULL — `SELECT sb FROM t` and
    // `SELECT sb, title FROM t` were right while `SELECT sb, 1` and
    // `SELECT sb, title || 'x'` silently lost the value. A schema id
    // cannot drift, whatever the select list does.
    let json_positions: Vec<(u64, String)> = if schema.json_columns.is_empty() {
        Vec::new()
    } else {
        query_row
            .requested_row_fetch
            .columns_fetching_data
            .iter()
            .take(query_row.visible_columns)
            .filter_map(|c| {
                let field = schema
                    .fields
                    .iter()
                    .find(|f| f.write_order == c.schema_id)?;
                crate::core::rql::json_map::shadow_for(&field.name, &schema.json_columns)
                    .map(|t| (c.schema_id, t.to_string()))
            })
            .collect()
    };
    let has_json = !json_positions.is_empty();
    // Constants and per-row scalars both fill output positions that no
    // fetched column occupies. Merged into one ordered list so the
    // splice below stays index-correct whichever kind comes first.
    let mut value_slots: Vec<(usize, SlotValue, crate::core::db_type::DbType)> = query_row
        .literal_slots
        .iter()
        .map(|(position, bytes, db_type)| {
            (
                *position,
                SlotValue::Constant(bytes.clone()),
                db_type.clone(),
            )
        })
        .chain(
            query_row
                .scalar_slots
                .iter()
                .map(|(position, func, db_type)| {
                    (*position, SlotValue::Scalar(*func), db_type.clone())
                }),
        )
        .chain(
            query_row
                .expr_slots
                .iter()
                .map(|(position, expr, db_type)| {
                    (
                        *position,
                        SlotValue::RowExpression(
                            expr.clone(),
                            crate::core::rql::expr::display_scale(expr),
                            // Decided here, once: see `expr::output_is_wide`.
                            crate::core::rql::expr::output_is_wide(expr),
                        ),
                        db_type.clone(),
                    )
                }),
        )
        .collect();
    value_slots.sort_by_key(|(position, ..)| *position);
    // Borrowed: the schema cannot change under this statement, and a copy
    // here was a `Vec` and a `String` per column on every execution.
    let slot_schema: &[crate::core::row::schema::SchemaField] = &schema.fields;
    let has_literals = !value_slots.is_empty();
    // The stored document columns, which `DISTINCT` keys by value. A scan
    // types their cells as text, so only the schema can say which they are.
    let document_ids: Vec<u64> = schema
        .fields
        .iter()
        .filter(|f| !f.is_deleted && f.db_type == crate::core::db_type::DbType::JSONB)
        .map(|f| f.write_order)
        .collect();

    {
        match source
            .query_bytes_reporting(
                &query_row.where_clause,
                slot_schema,
                query_row.query_row_fetch,
                query_row.requested_row_fetch,
                // `SKIP LOCKED` filters *after* the scan, so a limit
                // pushed down would be spent on rows that are then
                // skipped — two workers claiming from the head of the
                // same queue would leave the second with nothing, which
                // is the opposite of what the clause is for. The limit
                // is applied below instead, once the skipping is done.
                // A window is computed over every row `WHERE` kept,
                // before paging, so a limit pushed into the scan made
                // `SUM(v) OVER ()` the sum of the page. Paged below.
                if skip_locked || has_windows { None } else { query_row.limit },
                query_row.order_by,
                tx,
            )
            .await
        {
            Ok((rows_bytes, scan_overfetched)) => {
                info!(
                    "Query executed successfully, returning {} bytes",
                    rows_bytes.len()
                );

                // There is something to trim only when both hold. A
                // predicate reading only selected columns adds none, and
                // a scan that re-fetched just the requested columns for
                // each matched row sent none of the predicate's, so its
                // bytes are already the projection: decoding them only
                // to re-encode the same bytes was wasted work.
                let may_overfetch = predicate_needs_extra_columns && scan_overfetched;

                // Everything below that needs a row decoded. `OFFSET`
                // is deliberately not in it: on its own it only drops
                // rows from the front, which the length headers can do
                // without decoding anything (below).
                let needs_rows = trim_sort_key
                    || distinct
                    || has_nullable
                    || has_literals
                    || has_json
                    || may_overfetch
                    || row_lock.is_some()
                    || has_windows
                    || has_order_exprs;

                // How many rows of an expression sort's output the
                // steps below can use, when that is fewer than all of
                // them. Cutting there lets the sort select rather than
                // sort, and keeps the page's rows — and only those —
                // for the per-row work (NULL retyping, trimming, JSON
                // hydration with its document loads, slot evaluation).
                //
                // An allowlist over `QueryRows`; each field is here
                // because of what it does with the rows past the page:
                //  * `distinct` — duplicates are removed after this
                //    point, so the page is not known until they are:
                //    blocks.
                //  * `window_slots` — a window is computed over every
                //    row that survived WHERE: blocks.
                //  * `row_lock` (any, including `SKIP LOCKED`) — rows
                //    the lock skips are replaced by later ones: blocks.
                //  * `written_limit` — no limit, no cap.
                //  * `offset` — part of the cap; the skip below still
                //    runs over the capped rows.
                //  * `aggregate` — returned above, never reaches here.
                //  * `limit`/`order_by` — what the scan was given; an
                //    expression sort runs over an unbounded, unsorted
                //    scan (`lexer_query`'s expression branch).
                //  * `literal_slots`, `scalar_slots`, `expr_slots`,
                //    `null_flag_positions`, `predicate_needs_extra_
                //    columns`, `visible_columns`, `select_items`, JSON
                //    columns — per row, nothing reads another row. A
                //    select expression that would fail on a row outside
                //    the page is therefore not evaluated there, which is
                //    what every LIMIT pushed into the scan already does.
                //  * `where_clause`, the fetches, `table_name` — spent
                //    by the scan.
                // A field added to `QueryRows` that looks at more than
                // one row has to be added here as a blocker.
                let expr_sort_cap: Option<usize> =
                    if !distinct && !has_windows && row_lock.is_none() && !skip_locked {
                        written_limit.map(|limit| {
                            usize::try_from(limit.saturating_add(offset))
                                .unwrap_or(usize::MAX)
                        })
                    } else {
                        None
                    };

                // Where the page starts in `rows_bytes`. Non-zero only
                // when `OFFSET` is the one thing to do here.
                let mut page_start = 0usize;

                // `OFFSET` drops rows from the front. The scan already
                // fetched `limit + offset` of them, so the skip happens
                // here rather than costing another pass over the table.
                let rows_bytes = if offset > 0 && !needs_rows {
                    // The rows it drops are walked past by their
                    // headers, never decoded; what remains is already
                    // in the wire format.
                    match crate::core::row::row::skip_serialized_rows(&rows_bytes, offset) {
                        Ok(page) => {
                            page_start = rows_bytes.len() - page.len();
                            rows_bytes
                        }
                        Err(e) => {
                            error!("Failed to page the result set: {}", e);
                            let mut result = vec![3u8];
                            result.extend_from_slice(e.to_string().as_bytes());
                            return result;
                        }
                    }
                } else if needs_rows || offset > 0 {
                    let decoded: Result<PageRows<'_>, String> =
                        if has_order_exprs {
                            // A key reaching into a *shredded* JSON
                            // column would read a stored UUID, evaluate
                            // to NULL on every row, and quietly not sort
                            // at all.
                            let shadowed = order_exprs.iter().any(|(expr, ..)| {
                                crate::core::rql::expr::columns_of(expr).iter().any(|f| {
                                    crate::core::rql::json_map::shadow_for(
                                        &f.name,
                                        &schema.json_columns,
                                    )
                                    .is_some()
                                })
                            });
                            if shadowed {
                                let e = "ORDER BY into a JSON column stored as shadow tables is not supported; store the column as native JSONB";
                                error!("{}", e);
                                let mut result = vec![3u8];
                                result.extend_from_slice(e.as_bytes());
                                return result;
                            }
                            // Sorted before anything is trimmed: the
                            // keys read columns that ride after the
                            // visible ones, and `column_value` finds
                            // NULL companions in the same row.
                            // Decoding every row, evaluating its keys
                            // and sorting is CPU with no await in it;
                            // above a size it leaves the async worker
                            // so other sessions' tasks queued on that
                            // thread keep running (as the scan's own
                            // sort does).
                            let sort = || {
                                sort_rows_by_expressions(
                                    &rows_bytes,
                                    &order_exprs,
                                    &slot_schema,
                                    expr_sort_cap,
                                )
                            };
                            let large = crate::core::row::row::count_serialized_rows(&rows_bytes)
                                >= crate::core::processor::concurrent_processor::SORT_OFF_RUNTIME_ROWS
                                    as u64;
                            let multi_thread = tokio::runtime::Handle::try_current()
                                .is_ok_and(|h| {
                                    h.runtime_flavor()
                                        == tokio::runtime::RuntimeFlavor::MultiThread
                                });
                            let sorted = if large
                                && multi_thread
                                && crate::core::processor::concurrent_processor::latency_guards_on()
                            {
                                tokio::task::block_in_place(sort)
                            } else {
                                sort()
                            };
                            sorted
                            .map(|rows| PageRows::Sorted(rows.into_iter()))
                            .map_err(|e| {
                                error!("ORDER BY failed: {}", e);
                                e
                            })
                        } else {
                            // Decoded one at a time as the loop below
                            // asks for them, never as a `Vec<Row>`.
                            Ok(PageRows::Serialized(
                                crate::core::row::row::serialized_rows(&rows_bytes),
                            ))
                        };
                    match decoded {
                        Ok(mut source) => {
                            // The page boundaries, decided before the
                            // loop so a streaming pass can apply them
                            // as it goes. `DISTINCT` removes rows, so
                            // paging has to run over what survived
                            // rather than over the scan's output. An
                            // expression sort ran over an unbounded
                            // scan, so the page is cut here too.
                            let (skip, take) = if distinct
                                || skip_locked
                                || has_order_exprs
                                || has_windows
                            {
                                (offset as usize, written_limit)
                            } else {
                                (offset as usize, None)
                            };

                            // Rows go straight from decode through the
                            // per-row steps into the output buffer, one
                            // at a time, unless a later step needs all
                            // of them together: a window is computed
                            // over the whole result, and a lock claims
                            // rows before any is handed over (and with
                            // SKIP LOCKED decides which rows the page
                            // holds). Holding a decoded copy of every
                            // row as well as the bytes it came from was
                            // ~1.5 KB a row on top of the result.
                            let streaming = !has_windows && row_lock.is_none();
                            let mut out: Vec<u8> = Vec::new();
                            let mut skipped = 0usize;
                            let mut emitted = 0u64;

                            // Trim first, so `DISTINCT` compares only
                            // the columns the client asked for and does
                            // not treat rows as distinct because of a
                            // sort key it cannot see.
                            let mut kept: Vec<crate::core::row::row::Row> = if streaming {
                                Vec::new()
                            } else {
                                Vec::with_capacity(source.remaining_hint(&rows_bytes))
                            };
                            // A locking read's key cells, one entry per kept row.
                            let mut kept_keys: Vec<Vec<crate::core::row::row::Column>> = Vec::new();
                            // Sized only when `DISTINCT` will use it:
                            // a hash table with room for every row is
                            // tens of megabytes on a large result.
                            let mut seen: ahash::HashSet<Vec<u8>> =
                                ahash::HashSet::with_capacity_and_hasher(
                                    if distinct {
                                        source.remaining_hint(&rows_bytes)
                                    } else {
                                        0
                                    },
                                    Default::default(),
                                );

                            let mut processed = 0usize;
                            loop {
                                // The page is full: nothing after this
                                // can reach the client, so it is not
                                // decoded either.
                                if streaming && take.is_some_and(|cap| emitted >= cap) {
                                    break;
                                }
                                // Nothing in this loop suspends unless
                                // a row needs a JSON document loaded,
                                // so a large result would hold the
                                // worker thread from first row to last
                                // and every task queued behind it —
                                // other sessions' point reads — would
                                // wait. One yield per 1024 rows.
                                processed += 1;
                                if processed % 1024 == 0
                                    && crate::core::processor::concurrent_processor::latency_guards_on()
                                {
                                    tokio::task::yield_now().await;
                                }
                                let mut row = match source.next_row() {
                                    None => break,
                                    Some(Ok(row)) => row,
                                    Some(Err(e)) => {
                                        error!("Failed to trim the result set: {}", e);
                                        let mut result = vec![3u8];
                                        result.extend_from_slice(e.as_bytes());
                                        return result;
                                    }
                                };
                                // Retyping a `NULL` column as
                                // `DbType::NULL` is what makes the wire
                                // layer send an actual NULL: its
                                // formatters yield no value for that
                                // type. It must happen before the
                                // companions are trimmed away.
                                if has_nullable {
                                    for (i, flag_pos) in null_flags.iter().enumerate() {
                                        let Some(p) = flag_pos else { continue };
                                        let is_null = row
                                            .columns
                                            .get(*p)
                                            .map(|c| {
                                                crate::core::rql::nullable::flag_is_set(
                                                    c.data.into_slice(),
                                                )
                                            })
                                            .unwrap_or(false);
                                        if is_null {
                                            if let Some(c) = row.columns.get_mut(i) {
                                                c.column_type =
                                                    crate::core::db_type::DbType::NULL;
                                            }
                                        }
                                    }
                                }

                                // Trimming is driven by the row itself
                                // rather than by the plan: the scan may
                                // return more columns than were asked
                                // for, and `visible_columns` is how
                                // many the client is owed.
                                let overfetched = row.columns.len() > visible_columns
                                    && (visible_columns > 0 || has_literals);
                                // A locking read's key cells, read before the
                                // trim drops them: the last of each schema id
                                // is the one the lock fetch appended.
                                let row_key_cells: Option<Vec<crate::core::row::row::Column>> =
                                    lock_fetch.as_ref().map(|(_, ids)| {
                                        let last = |id: u64| {
                                            row.columns.iter().rev().find(|c| u64::from(c.schema_id) == id)
                                        };
                                        ids.iter()
                                            .map(|(id, companion)| {
                                                let null = companion
                                                    .and_then(|c| last(c))
                                                    .is_some_and(|c| {
                                                        crate::core::rql::nullable::flag_is_set(c.data.into_slice())
                                                    });
                                                match last(*id) {
                                                    Some(c) if !null => c.deep_clone(),
                                                    _ => crate::core::row::row::Column {
                                                        schema_id: *id as u32,
                                                        data: crate::memory_pool::MemoryBlock::default(),
                                                        column_type: crate::core::db_type::DbType::NULL,
                                                    },
                                                }
                                            })
                                            .collect()
                                    });
                                let mut row =
                                    if trim_sort_key || has_nullable || overfetched || lock_fetch.is_some() {
                                        let mut trimmed = row;
                                        trimmed.columns.truncate(visible_columns);
                                        trimmed
                                    } else {
                                        row
                                    };

                                // A JSON column's stored UUID is
                                // replaced by the document it refers
                                // to, rendered as text — which is what
                                // a client asked for when it declared
                                // the column JSON.
                                if has_json {
                                    for (schema_id, shadow_table) in &json_positions {
                                        let Some(index) = row
                                            .columns
                                            .iter()
                                            .position(|c| u64::from(c.schema_id) == *schema_id)
                                        else {
                                            continue;
                                        };
                                        let Some(cell) = row.columns.get(index) else {
                                            continue;
                                        };
                                        if cell.column_type
                                            == crate::core::db_type::DbType::NULL
                                        {
                                            continue;
                                        }
                                        let reference =
                                            crate::core::value_codec::format_uuid(
                                                cell.data.into_slice(),
                                            );
                                        let rendered = Box::pin(load_json_document(
                                            &database,
                                            shadow_table,
                                            &reference,
                                            0,
                                            tx,
                                        ))
                                        .await
                                        .map(|v| v.to_string())
                                        .unwrap_or_else(|| "{}".to_string());

                                        if let Some(cell) = row.columns.get_mut(index) {
                                            let bytes = rendered.as_bytes();
                                            let mut mb = crate::memory_pool::MEMORY_POOL
                                                .acquire(bytes.len());
                                            mb.into_slice_mut().copy_from_slice(bytes);
                                            cell.data = mb;
                                            cell.column_type =
                                                crate::core::db_type::DbType::JSON;
                                        }
                                    }
                                }

                                // Constants occupy output positions
                                // that no fetched column fills, so they
                                // are spliced in at their index. Going
                                // in ascending order keeps the later
                                // positions correct as earlier ones
                                // shift.
                                if has_literals {
                                    // Every slot's value is computed
                                    // before any is spliced in: a row
                                    // expression reads its columns by
                                    // write order, and inserting a slot
                                    // column shifts what those lookups
                                    // find.
                                    let mut computed: Vec<(
                                        usize,
                                        Vec<u8>,
                                        crate::core::db_type::DbType,
                                    )> = Vec::with_capacity(value_slots.len());
                                    for (position, slot, db_type) in &value_slots {
                                        // A scalar is evaluated *here*,
                                        // inside the per-row loop:
                                        // `gen_random_uuid()` has to
                                        // differ in every row, and
                                        // hoisting it would write one
                                        // value into all of them.
                                        let (value, ty) = match slot {
                                            SlotValue::Scalar(func) => {
                                                (func.evaluate(), db_type.clone())
                                            }
                                            SlotValue::Constant(bytes) => {
                                                (bytes.clone(), db_type.clone())
                                            }
                                            SlotValue::RowExpression(expr, scale, wide) => {
                                                // Same rule as
                                                // `RETURNING`: the
                                                // value is re-encoded
                                                // as the type the row
                                                // description already
                                                // announced for this
                                                // slot — and a NUMERIC
                                                // at the scale
                                                // `query_output_scales`
                                                // announces for it.
                                                match crate::core::rql::expr::eval_output_planned(
                                                    expr,
                                                    *wide,
                                                    &row,
                                                    &slot_schema,
                                                )
                                                .map(|v| {
                                                    crate::core::rql::expr::coerce_to(
                                                        v, db_type,
                                                    )
                                                })
                                                .and_then(|v| {
                                                    crate::core::rql::expr::at_display_scale(
                                                        v, *scale,
                                                    )
                                                }) {
                                                    Ok(crate::core::rql::expr::Value::Data {
                                                        bytes,
                                                        db_type: ty,
                                                    }) => (bytes, ty),
                                                    // A NULL is typed as
                                                    // such so the wire
                                                    // layer sends one
                                                    // rather than an
                                                    // empty value.
                                                    Ok(crate::core::rql::expr::Value::Null) => (
                                                        Vec::new(),
                                                        crate::core::db_type::DbType::NULL,
                                                    ),
                                                    Err(e) => {
                                                        error!(
                                                            "Cannot evaluate select expression: {}",
                                                            e
                                                        );
                                                        let mut result = vec![3u8];
                                                        result.extend_from_slice(e.as_bytes());
                                                        return result;
                                                    }
                                                }
                                            }
                                        };
                                        computed.push((*position, value, ty));
                                    }
                                    for (position, value, db_type) in &computed {
                                        let mut mb = crate::memory_pool::MEMORY_POOL
                                            .acquire(value.len());
                                        if !value.is_empty() {
                                            mb.into_slice_mut().copy_from_slice(value);
                                        }
                                        let column = crate::core::row::row::Column {
                                            schema_id: crate::core::row::row::schema_id_of(*position as u64),
                                            data: mb,
                                            column_type: db_type.clone(),
                                        };
                                        let at = (*position).min(row.columns.len());
                                        row.columns.insert(at, column);
                                    }
                                    // One output column per select
                                    // item. `visible_columns` counts
                                    // only *fetched* columns, so
                                    // `max(slots)` gave 1 for
                                    // `SELECT 1, name` — where the
                                    // answer is 2 — and the row went
                                    // out one field short of the
                                    // RowDescription, which a client
                                    // rejects as "unexpected field
                                    // count in D message".
                                    let output_columns =
                                        query_row.select_items.len().max(value_slots.len());
                                    row.columns.truncate(output_columns);
                                }
                                let row = row;

                                if distinct {
                                    // Compare the serialized value
                                    // bytes, which is the same identity
                                    // the client sees — and whether
                                    // the cell is NULL. A NULL's value
                                    // bytes are its type's zeroes,
                                    // empty for TEXT and BYTEA, so a
                                    // key of bytes alone merged NULL
                                    // with '' and answered two rows
                                    // for {NULL, '', 'a'}.
                                    //
                                    // A document is the exception: it
                                    // is keyed by its value, so
                                    // `{"a":1}` and `{"a":1.0}` are one
                                    // row, as `=` says. A stored one
                                    // arrives typed as text, so its
                                    // column is known from the schema;
                                    // a computed one carries `jsonb`.
                                    let mut key = Vec::new();
                                    for (position, c) in row.columns.iter().enumerate() {
                                        let is_null = c.column_type
                                            == crate::core::db_type::DbType::NULL;
                                        key.push(is_null as u8);
                                        let document = !is_null
                                            && (c.column_type
                                                == crate::core::db_type::DbType::JSONB
                                                || (!value_slots
                                                    .iter()
                                                    .any(|(p, ..)| *p == position)
                                                    && document_ids
                                                        .contains(&u64::from(c.schema_id))));
                                        if document {
                                            crate::core::jsonb::push_group_key(
                                                c.data.into_slice(),
                                                &mut key,
                                            );
                                            continue;
                                        }
                                        key.extend_from_slice(
                                            &(c.data.into_slice().len() as u32)
                                                .to_le_bytes(),
                                        );
                                        key.extend_from_slice(c.data.into_slice());
                                    }
                                    if !seen.insert(key) {
                                        continue;
                                    }
                                }

                                if streaming {
                                    if skipped < skip {
                                        skipped += 1;
                                        continue;
                                    }
                                    row.extend_serialized(&mut out);
                                    emitted += 1;
                                    continue;
                                }
                                kept.push(row);
                                if let Some(cells) = row_key_cells {
                                    kept_keys.push(cells);
                                }
                            }
                            if streaming {
                                out
                            } else {

                            // Numbered over the rows that survived the
                            // predicate, before any paging: PostgreSQL
                            // computes a window before `LIMIT`, so
                            // trimming first would number a page rather
                            // than the result.
                            let mut kept = kept;
                            if has_windows {
                                for (position, spec) in &window_slots {
                                    // An exact SUM/AVG that overflows
                                    // is an error, as the grouped
                                    // aggregate's is — never a NULL.
                                    let values = match crate::core::rql::window::assign(
                                        &kept, spec,
                                    ) {
                                        Ok(values) => values,
                                        Err(e) => {
                                            error!("Window function failed: {}", e);
                                            let mut result = vec![3u8];
                                            result.extend_from_slice(e.as_bytes());
                                            return result;
                                        }
                                    };
                                    // The type the window produces, not
                                    // `I64`: an `AVG` is a decimal and
                                    // a `LAG(created_at)` is a
                                    // timestamp, and describing either
                                    // as a bigint sends the client
                                    // eight bytes it decodes as the
                                    // wrong thing.
                                    let column_type = spec.result_type();
                                    for (row, value) in
                                        kept.iter_mut().zip(values.into_iter())
                                    {
                                        // A NULL window value — no
                                        // preceding row for a `LAG`, an
                                        // empty frame for an `AVG` —
                                        // is an empty block, which is
                                        // how a NULL travels here.
                                        let bytes = value.unwrap_or_default();
                                        let mut mb = crate::memory_pool::MEMORY_POOL
                                            .acquire(bytes.len());
                                        mb.into_slice_mut().copy_from_slice(&bytes);
                                        let at = (*position).min(row.columns.len());
                                        row.columns.insert(
                                            at,
                                            crate::core::row::row::Column {
                                                schema_id: crate::core::row::row::schema_id_of(*position as u64),
                                                data: mb,
                                                column_type: column_type.clone(),
                                            },
                                        );
                                    }
                                }
                                for row in kept.iter_mut() {
                                    row.columns
                                        .truncate(query_row.select_items.len().max(1));
                                }
                            }

                            // `FOR UPDATE` claims the rows before they
                            // are handed over, so a row this
                            // transaction could not lock never reaches
                            // the client. With `SKIP LOCKED` this can
                            // return fewer rows than `LIMIT` asked
                            // for — which is the point: the missing
                            // ones belong to somebody else's claim.
                            let kept = match row_lock {
                                None => kept,
                                Some(lock) => match Box::pin(lock_selected_rows(
                                    &database,
                                    &lock_table_name,
                                    kept,
                                    std::mem::take(&mut kept_keys),
                                    lock_fetch.as_ref().map(|(key, _)| key),
                                    lock,
                                    // Everything the client will see:
                                    // the page plus whatever OFFSET
                                    // discards ahead of it.
                                    written_limit.map(|l| l as usize + offset as usize),
                                    since,
                                    tx,
                                ))
                                .await
                                {
                                    Ok(locked) => locked,
                                    Err(e) => {
                                        error!("Cannot lock rows: {}", e);
                                        let mut result = vec![3u8];
                                        result.extend_from_slice(e.as_bytes());
                                        return result;
                                    }
                                },
                            };

                            for row in kept.iter().skip(skip) {
                                if let Some(cap) = take {
                                    if emitted >= cap {
                                        break;
                                    }
                                }
                                row.extend_serialized(&mut out);
                                emitted += 1;
                            }
                            out
                            }
                        }
                        Err(e) => {
                            let mut result = vec![3u8];
                            result.extend_from_slice(e.as_bytes());
                            return result;
                        }
                    }
                } else {
                    rows_bytes
                };

                let page = &rows_bytes[page_start..];
                let mut final_vec = Vec::with_capacity(1 + page.len());
                final_vec.push(2u8);
                final_vec.extend_from_slice(page);
                return final_vec;
            }
            Err(e) => {
                // Report what actually went wrong. Evaluation failures
                // like `WHERE x / 0` carry a specific message, and a
                // generic "Error occurred" would leave the client
                // unable to tell a bad expression from a dead server.
                error!("Failed to execute query: {}", e);
                let mut result = vec![3u8];
                result.extend_from_slice(e.to_string().as_bytes());
                return result;
            }
        }
    }
}

#[cfg(test)]
mod predicate_extraction_tests {
    use super::*;

    #[test]
    fn returning_is_not_swept_into_the_predicate() {
        // The trap: taking everything after WHERE carried `RETURNING j.id`
        // into the predicate, which reached the tokenizer as the identifier
        // `RETURNING` and panicked the worker, dropping the connection.
        let sql = "UPDATE claimq AS j SET status = 'running' WHERE j.id = 2 RETURNING j.id";
        assert_eq!(predicate_of(sql, Some("j")), "id = 2");
    }

    #[test]
    fn the_alias_is_stripped_so_the_lookup_can_resolve_columns() {
        let sql = "UPDATE t AS j SET a = 1 WHERE j.b = 3 AND j.c = 4";
        assert_eq!(predicate_of(sql, Some("j")), "b = 3 AND c = 4");
        // Another relation's qualifier is left alone.
        let sql = "UPDATE t AS j SET a = 1 WHERE j.b = other.c";
        assert_eq!(predicate_of(sql, Some("j")), "b = other.c");
    }

    #[test]
    fn a_statement_without_a_where_has_no_predicate() {
        assert_eq!(predicate_of("DELETE FROM t", None), "");
        assert_eq!(predicate_of("DELETE FROM t RETURNING id", None), "");
    }

    #[test]
    fn a_delete_keeps_its_predicate() {
        assert_eq!(
            predicate_of("DELETE FROM t WHERE a = 1 RETURNING id", None),
            "a = 1"
        );
    }
}

#[cfg(test)]
mod predicate_folding_tests {
    use super::*;

    /// Integer arithmetic folds to PostgreSQL's answer, or not at all.
    #[test]
    fn integer_arithmetic_folds_as_postgres_computes_it() {
        for (text, want) in [
            ("500000 + 100", Some("500100")),
            ("499999 + 1", Some("500000")),
            ("3 - 10", Some("-7")),
            ("2 + 3 * 4", Some("14")),
            ("(2 + 3) * 4", Some("20")),
            ("5 - -3", Some("8")),
            ("-2147483648 + 0", Some("-2147483648")),
            // int8 because a literal is: no overflow there.
            ("3000000000 + 1", Some("3000000001")),
            ("2147483648 * 2", Some("4294967296")),
            // int4 + int4 overflows in PostgreSQL ("integer out of range"):
            // left for the engine, which says the same.
            ("2000000000 + 2000000000", None),
            ("2147483647 + 1", None),
            ("46341 * 46341", None),
            // Beyond int8 is numeric.
            ("9223372036854775807 + 1", None),
            ("99999999999999999999 + 1", None),
            // Integer division truncates toward zero; a zero divisor (22012)
            // and int4's `-2147483648 / -1` (22003) are the evaluator's.
            ("7 / 2", Some("3")),
            ("-7 / 2", Some("-3")),
            ("7 % 2", Some("1")),
            ("100 / 4 * 2", Some("50")),
            ("7 / 0", None),
            ("7 % 0", None),
            ("-2147483648 / -1", None),
            // Not integer literals, or nothing to fold.
            ("1.5 + 1", None),
            ("1e3 + 1", None),
            ("id + 1", None),
            ("'5' + 3", None),
            ("$1 + 1", None),
            ("5", None),
            ("-5", None),
            ("(5)", None),
            ("- 5 + 1", None),
            ("5 +", None),
            ("(5 + 1", None),
            ("", None),
        ] {
            assert_eq!(fold_integer_arithmetic(text).as_deref(), want, "{text:?}");
        }
    }

    #[test]
    fn a_where_bound_with_arithmetic_folds_to_a_literal() {
        assert_eq!(
            fold_where_constants("SELECT id FROM t WHERE id >= 500000 AND id <= 500000 + 100 ORDER BY id"),
            "SELECT id FROM t WHERE id >= 500000 AND id <= 500100 ORDER BY id"
        );
        assert_eq!(fold_where_constants("SELECT id FROM t WHERE id + 1 = 5"), "SELECT id FROM t WHERE id + 1 = 5");
        // The statement's `;` stays where it was.
        assert_eq!(
            fold_where_constants("SELECT id FROM t WHERE id >= 5 AND id <= 5 + 100;"),
            "SELECT id FROM t WHERE id >= 5 AND id <= 105;"
        );
        assert_eq!(fold_where_constants("SELECT id FROM t WHERE id = 7 ;  "), "SELECT id FROM t WHERE id = 7 ;  ");
        assert_eq!(fold_where_constants("SELECT id FROM t WHERE 2 * 3 < id"), "SELECT id FROM t WHERE 6 < id");
        // Only the WHERE: the select list and LIMIT are not predicates.
        assert_eq!(
            fold_where_constants("SELECT 1 + 1 FROM t WHERE a = 1 + 1 LIMIT 2"),
            "SELECT 1 + 1 FROM t WHERE a = 2 LIMIT 2"
        );
    }

    /// A parenthesised comparison is folded inside: `UPDATE … FROM`'s
    /// recheck is written as `AND (<terms>)`. Mutation: `parenthesised`
    /// returning `None` leaves every one of these unfolded.
    #[test]
    fn a_parenthesised_comparison_folds_inside() {
        assert_eq!(
            fold_where_constants("SELECT id FROM t WHERE id > 0 AND (v < 5 * 4) ORDER BY id"),
            "SELECT id FROM t WHERE id > 0 AND (v < 20) ORDER BY id"
        );
        assert_eq!(
            fold_where_constants("UPDATE t SET a = 1 WHERE id = 1 AND ((v < 2 * 3) AND (w > 1 + 1))"),
            "UPDATE t SET a = 1 WHERE id = 1 AND ((v < 6) AND (w > 2))"
        );
        // int4 overflow is the evaluator's 22003, never a folded value; nor is
        // integer arithmetic the decimal folder's.
        assert_eq!(
            fold_where_constants("SELECT id FROM t WHERE b = 2147483647 + 1"),
            "SELECT id FROM t WHERE b = 2147483647 + 1"
        );
        assert_eq!(
            fold_where_constants("SELECT id FROM t WHERE (b = 2147483647 * 2 - 1)"),
            "SELECT id FROM t WHERE (b = 2147483647 * 2 - 1)"
        );
        let folded = fold_where_constants("SELECT id FROM t WHERE (v < 0.1234 * 100)");
        assert!(!folded.contains('*'), "{folded}");
        // Not one group, and not a comparison's own parentheses.
        assert_eq!(fold_where_constants("SELECT id FROM t WHERE (a) < (b)"), "SELECT id FROM t WHERE (a) < (b)");
        assert_eq!(fold_where_constants("SELECT id FROM t WHERE (a = ')')"), "SELECT id FROM t WHERE (a = ')')");
    }

    #[test]
    fn a_folded_constant_keeps_the_space_before_the_next_clause() {
        let folded = fold_where_constants("SELECT id FROM t WHERE ts < NOW() ORDER BY id");
        // The trap: trimming the operand ran the literal into the keyword —
        // `< '2026-…'ORDER BY id` — so the clause splitter stopped finding
        // ` ORDER BY ` and handed `ORDER` to the tokenizer as a column name,
        // which panicked the worker and dropped the client's connection.
        assert!(
            folded.contains(" ORDER BY id"),
            "ORDER BY must stay separated: {}",
            folded
        );
        assert!(
            !folded.contains("NOW()"),
            "NOW() should have been folded: {}",
            folded
        );
    }

    #[test]
    fn only_the_where_clause_is_folded() {
        // A constant substituted into a select list or an ORDER BY would change
        // what the statement means, so folding stops at the clause boundary.
        let folded = fold_where_constants("SELECT NOW() FROM t ORDER BY a");
        assert!(folded.contains("SELECT NOW()"), "{}", folded);

        let untouched = "INSERT INTO t (a) VALUES (1)";
        assert_eq!(fold_where_constants(untouched), untouched);
    }

    #[test]
    fn a_column_reference_is_left_alone() {
        // Only what evaluates against an *empty* schema is constant; a column
        // fails to resolve there, which is what keeps it out of the folding.
        let folded = fold_where_constants("UPDATE t SET a = 1 WHERE b < c");
        assert!(folded.contains("b < c"), "{}", folded);
    }

    #[test]
    fn both_sides_of_a_conjunction_are_folded() {
        let folded =
            fold_where_constants("UPDATE t SET a = 1 WHERE lease_until < NOW() OR owner = 'x'");
        assert!(!folded.contains("NOW()"), "{}", folded);
        assert!(folded.contains("owner = 'x'"), "{}", folded);
        assert!(
            folded.contains(" OR "),
            "the separator must survive: {}",
            folded
        );
    }
}

#[cfg(test)]
mod update_from_tests {
    use super::*;

    fn aliases() -> Vec<String> {
        vec!["fwd".to_string(), "bl".to_string()]
    }

    #[test]
    fn a_correlated_subquery_is_split_out_of_the_join_condition() {
        let condition = "fwd.value_type = 5 AND bl.related_table_id = fwd.user_table_id \
                         AND (SELECT COUNT(*) FROM t bl2 WHERE bl2.related_table_id = \
                         fwd.user_table_id AND bl2.user_table_id = fwd.related_table_id) = 1";
        let (join, correlated) = split_correlated_terms(condition, &aliases()).unwrap();

        // The join keeps only what it can evaluate itself.
        assert!(join.contains("fwd.value_type = 5"));
        assert!(join.contains("bl.related_table_id = fwd.user_table_id"));
        assert!(
            !join.contains("SELECT"),
            "the subquery must not reach the join predicate: {}",
            join
        );

        assert_eq!(correlated.len(), 1);
        let term = &correlated[0];
        assert_eq!(term.op, "=");
        assert_eq!(term.literal, "1");
        // Both outer columns have to be carried, or the substitution below
        // would leave an unresolved reference in the subquery.
        assert_eq!(term.outer_refs.len(), 2);
        assert!(term.outer_refs.iter().any(|r| r == "fwd.user_table_id"));
        assert!(term.outer_refs.iter().any(|r| r == "fwd.related_table_id"));
    }

    #[test]
    fn an_uncorrelated_subquery_is_left_for_the_ordinary_path() {
        // It resolves to a constant once, so pulling it into the per-row loop
        // would run it once per candidate row for the same answer.
        let condition = "fwd.id = bl.id AND (SELECT COUNT(*) FROM other) = 3";
        let (join, correlated) = split_correlated_terms(condition, &aliases()).unwrap();
        assert!(correlated.is_empty());
        assert!(join.contains("SELECT COUNT(*) FROM other"));
    }

    #[test]
    fn a_correlated_subquery_not_compared_against_a_constant_is_refused() {
        // Refusing is the point: a shape this cannot evaluate must not be
        // quietly dropped from the predicate, which would widen the UPDATE.
        let condition =
            "fwd.id = bl.id AND (SELECT id FROM t WHERE t.x = fwd.user_table_id) IS NULL";
        let err = split_correlated_terms(condition, &aliases()).unwrap_err();
        assert!(err.contains("compared against a constant"), "got: {}", err);
    }

    /// A negative value is rendered to read as one value wherever it lands
    /// (`x-(-3)`, never `x--3`, a comment), a reference in a string literal
    /// is text, every quoting of a reference is one, and substituted text is
    /// never scanned again.
    ///
    /// Mutation: `render_substituted` pushing the literal bare, or
    /// `qualified_ref_spans` not skipping string literals, fails this.
    #[test]
    fn source_references_are_substituted_quote_aware_and_parenthesised() {
        let bindings = vec![
            ("s.neg".to_string(), "-3".to_string()),
            ("s.l".to_string(), "'has s.neg -- in it'".to_string()),
            ("s.big".to_string(), "123".to_string()),
        ];
        assert_eq!(substitute_outer_refs("x-s.neg, label = 'z'", &bindings), "x-(-3), label = 'z'");
        assert_eq!(substitute_outer_refs("v IN (s.neg, 99)", &bindings), "v IN (-3, 99)");
        assert_eq!(substitute_outer_refs("-s.neg", &bindings), "-(-3)");
        assert_eq!(substitute_outer_refs("s.neg::text", &bindings), "(-3)::text");
        assert_eq!(substitute_outer_refs("v BETWEEN s.neg AND 2*s.neg", &bindings), "v BETWEEN -3 AND 2* -3");
        assert_eq!(substitute_outer_refs("-s.big", &bindings), "-123");
        assert_eq!(
            substitute_outer_refs("'see s.neg ' || s.l || 'it''s s.neg'", &bindings),
            "'see s.neg ' || 'has s.neg -- in it' || 'it''s s.neg'"
        );
        assert_eq!(
            substitute_outer_refs(r#""s".big + s."big" + "s"."big" + S.big + s.bigger + s.big.x"#, &bindings),
            r#"123 + 123 + 123 + 123 + s.bigger + s.big.x"#
        );
        assert_eq!(qualified_refs(r#"t.x - "s"."big" + 's.neg' + s.neg"#, &["s".to_string()]), [r#""s"."big""#, "s.neg"]);
        assert_eq!(strip_qualifier("t.x || 'see t.x' || t.y", "t"), "x || 'see t.x' || y");
    }

    #[test]
    fn outer_references_are_substituted_whole_word_only() {
        let bindings = vec![
            ("fwd.id".to_string(), "7".to_string()),
            ("fwd.id_extra".to_string(), "9".to_string()),
        ];
        let out = substitute_outer_refs(
            "SELECT COUNT(*) FROM t WHERE t.a = fwd.id AND t.b = fwd.id_extra",
            &bindings,
        );
        assert!(out.contains("t.a = 7"), "got: {}", out);
        // The trap: substituting `fwd.id` first would have turned
        // `fwd.id_extra` into `7_extra`.
        assert!(out.contains("t.b = 9"), "got: {}", out);
        assert!(!out.contains("7_extra"), "got: {}", out);
    }

    #[test]
    fn scalar_comparisons_are_numeric_where_both_sides_are_numbers() {
        assert!(scalar_comparison_holds("1", "=", "1").unwrap());
        assert!(!scalar_comparison_holds("2", "=", "1").unwrap());
        assert!(scalar_comparison_holds("10", ">", "9").unwrap());
        // Bytewise would put "10" before "9"; numerically it does not.
        assert!(!scalar_comparison_holds("10", "<", "9").unwrap());
        assert!(scalar_comparison_holds("'a'", "=", "'a'").unwrap());
    }
}

#[cfg(test)]
mod fold_parts_tests {
    use super::*;

    /// The parallel fold's parts tile the input on row boundaries: nothing is
    /// dropped, duplicated or cut mid-row, and a small input is one part.
    #[test]
    fn parts_tile_the_rows_on_row_boundaries() {
        use crate::core::row::row::{Column, Row, count_serialized_rows, serialized_rows};
        let mut bytes = Vec::new();
        for i in 0..250_003u64 {
            let mut columns: crate::core::row::row::RowColumns = smallvec::SmallVec::new();
            let width = (i % 5) as usize;
            let mut mb = crate::memory_pool::MEMORY_POOL.acquire(width);
            mb.into_slice_mut().fill(i as u8);
            columns.push(Column {
                schema_id: 0,
                data: mb,
                column_type: crate::core::db_type::DbType::STRING,
            });
            Row { position: 0, columns, length: 0 }.extend_serialized(&mut bytes);
        }
        let parts = fold_parts(&bytes);
        assert!(parts.len() > 1 || std::thread::available_parallelism().map_or(1, |n| n.get()) == 1);
        assert_eq!(parts.concat(), bytes);
        let mut total = 0;
        for part in &parts {
            assert!(serialized_rows(part).all(|r| r.is_ok()), "a part ends mid-row");
            total += count_serialized_rows(part);
        }
        assert_eq!(total, 250_003);
        assert_eq!(fold_parts(&bytes[..1000]).len(), 1);
    }
}

#[cfg(test)]
mod future_size_tests {
    //! How big a statement's future is. Every statement `Box::pin`s it at
    //! least once — an allocation plus a copy of the whole state machine —
    //! so its size is a per-statement cost, not only a stack concern.

    use super::*;

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// statement_future_sizes --nocapture`.
    #[tokio::test]
    #[ignore]
    async fn statement_future_sizes() {
        let database = Arc::new(Database::new_with_backend("future_sizes", true).await);
        let purpose = || recognize_query_purpose("SELECT a FROM t WHERE a = 1").expect("parse");
        let tx = crate::core::mvcc::transaction::FROZEN_TX;
        let f = execute_with_context(purpose(), database.clone(), 0, tx);
        println!("execute_with_context: {} bytes", std::mem::size_of_val(&f));
        drop(f);
        let f = execute_with_need(purpose(), database.clone(), 0, tx, SubqueryNeed::All);
        println!("execute_with_need:    {} bytes", std::mem::size_of_val(&f));
        drop(f);
        let f = execute_with_sync(purpose(), database.clone(), false);
        println!("execute_with_sync:    {} bytes", std::mem::size_of_val(&f));
        drop(f);
        let f = execute_in_txn(purpose(), database.clone(), tx);
        println!("execute_in_txn:       {} bytes", std::mem::size_of_val(&f));
        drop(f);

        // What the boxing itself costs per statement — allocation, the move
        // into the box, the free — against what a whole point read costs.
        run_sql(&database, "CREATE TABLE small (id BIGINT NOT NULL, s TEXT NOT NULL)").await;
        let values: Vec<String> = (0..100).map(|i| format!("({i}, 's{i}')")).collect();
        run_sql(&database, &format!("INSERT INTO small (id, s) VALUES {}", values.join(", "))).await;
        let sql = "SELECT s FROM small WHERE id = 42";
        let parsed = || recognize_query_purpose(sql).expect("parse");
        for round in 0..6 {
            let n = 100_000;
            let t = std::time::Instant::now();
            for _ in 0..n {
                std::hint::black_box(parsed());
            }
            let parse = t.elapsed() / n;
            let t = std::time::Instant::now();
            for _ in 0..n {
                let f = Box::pin(execute_with_context(parsed(), database.clone(), 0, tx));
                std::hint::black_box(&f);
                drop(f);
            }
            let boxing = (t.elapsed() / n).saturating_sub(parse);
            let n = 20_000;
            let t = std::time::Instant::now();
            for _ in 0..n {
                std::hint::black_box(execute(parsed(), database.clone()).await);
            }
            let statement = t.elapsed() / n;
            println!("round {round}: Box::pin of the statement future {boxing:?}; whole point SELECT {statement:?}");
        }
    }

    async fn run_sql(database: &Arc<Database>, sql: &str) -> Vec<u8> {
        execute(recognize_query_purpose(sql).expect("parse"), database.clone()).await
    }
}

#[cfg(test)]
#[path = "shadow_latch_tests.rs"]
mod shadow_latch_tests;

#[cfg(test)]
#[path = "row_lock_handover_tests.rs"]
mod row_lock_handover_tests;
