use rclite::Arc;

use log::{debug, error, info, warn};

use crate::core::mvcc::transaction::TxId;

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

    let schema_of = |name: &str| database.tables.get(name).map(|t| t.schema.fields.clone());

    let plan = match crate::core::rql::join::plan_statement(sql, &schema_of) {
        Ok(p) => p,
        Err(e) => {
            error!("Cannot plan join: {}", e);
            return error(&e);
        }
    };

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
        // Cloned out so the map guard does not span the scan below.
        let Some(table) = database
            .tables
            .get(&relation.table)
            .map(|t| t.value().clone())
        else {
            return error(&format!("relation '{}' does not exist", relation.table));
        };

        let schema = &table.schema.fields;
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

        let visibility = match table.visibility_for(tx) {
            crate::core::mvcc::Visibility::All => crate::core::mvcc::Visibility::All,
            own => statement_snapshot.get_or_insert(own).clone(),
        };

        // No limit is pushed down: it applies to the joined output, and a cap
        // here would drop rows that the join still needs.
        let set = if legacy_rows {
            table
                .query_row_with_visibility(
                    &relation.predicate,
                    &schema.to_vec(),
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
                    &schema.to_vec(),
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
                database.tables.get(&relation.table).is_some_and(|t| {
                    crate::core::rql::json_map::shadow_for(&item.field.name, &t.schema.json_columns)
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
                crate::core::rql::join::project_serialized(&plan, &combined, &mut result);
                result
            }
        };
    }
    let mut rows = crate::core::rql::join::project(&plan, combined);

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
        let Some(shadow_table) = database.tables.get(&relation.table).and_then(|t| {
            crate::core::rql::json_map::shadow_for(&item.field.name, &t.schema.json_columns)
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
    let folded = fold_where_constants(sql);
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
        recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse subquery '{}'", sql))?;
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
            let schema_of = |name: &str| database.tables.get(name).map(|t| t.schema.fields.clone());
            crate::core::rql::join::plan_statement(&qr.query, &schema_of)
                .ok()
                .map(|plan| plan.output_scales())
        }
        QueryPurpose::QueryRows(qr) => database.tables.get(&qr.table_name).and_then(|t| {
            let schema = &t.schema;
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

/// Brings every `DECIMAL` cell of a subquery's rows to the engine scale, from
/// the scale [`query_output_scales`] says it is stored at.
///
/// Rows leaving one query for another carry no scale, and everything that
/// reads them — literal substitution, a derived table's temp relation —
/// assumes the engine's. Without this a `sum(bigint)` (a numeric at scale 0)
/// read through a derived table was a billionth of itself, and so was any
/// `NUMERIC(p, s)` column with `s` other than nine. Rounded half away from
/// zero when the stored scale is wider.
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
    let folded = fold_where_constants(sql);
    let sql = folded.as_str();
    let purpose =
        recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse subquery '{}'", sql))?;
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
    let purpose = recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse '{}'", sql))?;
    // Runs inside the caller's transaction rather than opening its own. A
    // data-modifying CTE is part of one statement, so committing it separately
    // would leave its writes behind when the outer statement fails — or when
    // the block it belongs to is rolled back.
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
    let purpose = recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse '{}'", sql))?;
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
    let purpose = recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse '{}'", sql))?;
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
    use crate::core::rql::join::find_keyword;

    let Some(from) = find_keyword(sql, "FROM", 0) else {
        return Vec::new();
    };
    let start = from + "FROM".len();
    let end = ["WHERE", "ORDER", "LIMIT", "OFFSET", "GROUP", "HAVING"]
        .iter()
        .filter_map(|kw| find_keyword(sql, kw, start))
        .min()
        .unwrap_or(sql.len());

    let mut names = Vec::new();
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

    const MAX_SUBQUERIES: usize = 8;

    let mut current = sql.to_string();

    // `EXISTS` is rewritten into the equivalent `IN` first, so the loop below
    // resolves both forms with one mechanism.
    for _ in 0..MAX_SUBQUERIES {
        let Some(clause) = subquery::find_exists(&current) else {
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

        if bounds.offset > 0 {
            // "More than k matching rows per outer row" has no `IN` form.
            return Err(
                "EXISTS with an OFFSET in a correlated subquery is not supported".to_string(),
            );
        }

        if let Some(spec) = subquery::multi_correlated_exists(&clause, &outer) {
            let spec = spec?;
            let rows = run_subquery(&spec.inner_sql, database, tx).await?;
            let predicate = subquery::multi_exists_predicate(&spec, &rows)?;
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
        let literal = subquery::rows_to_scalar(&rows, document)?;
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
        let rows =
            run_subquery_for(&found.inner_sql, database, tx, SubqueryNeed::Membership).await?;
        let document = crate::core::rql::cte::document_columns(&found.inner_sql, database)
            .first()
            .copied()
            .unwrap_or(false);
        let (list, had_null) = subquery::rows_to_value_list(&rows, document)?;

        // `x NOT IN (…, NULL)` is never true: x either equals a value (false)
        // or is compared against NULL (unknown). `x IN ()` expresses exactly
        // that — it never holds, and a NULL x is excluded as well.
        let (start, replacement) = match (had_null, found.negated_start) {
            (true, Some(not_at)) => (not_at, "IN ()".to_string()),
            _ => (found.start, list),
        };
        current = format!(
            "{}{}{}",
            &current[..start],
            replacement,
            &current[found.end..]
        );
    }

    Err(format!(
        "more than {} subqueries in one statement is not supported",
        MAX_SUBQUERIES
    ))
}

/// Depth limit for cascading writes.
///
/// A cycle of `ON DELETE CASCADE` would otherwise recurse without end. The
/// limit is generous next to any real schema, so hitting it means a cycle.
const MAX_CASCADE_DEPTH: usize = 16;

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
    let foreign_keys = {
        let entry = database.tables.get(table_name);
        match entry.as_ref() {
            None => return Ok(()),
            Some(t) => t.schema.foreign_keys.clone(),
        }
    };
    if foreign_keys.is_empty() {
        return Ok(());
    }

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

        if !database.tables.contains_key(&fk.parent_table) {
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
                literal.trim_matches('\''),
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
        let found = referenced_values_now(
            database,
            &fk.parent_table,
            &fk.parent_column,
            std::slice::from_ref(literal),
            tx,
            Some(1),
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
    let foreign_keys = match database.tables.get(table_name) {
        Some(t) => t.schema.foreign_keys.clone(),
        None => return verified,
    };
    if foreign_keys.is_empty() {
        return verified;
    }
    let rows: Vec<Vec<(String, String)>> = statements
        .iter()
        .map(|s| crate::core::rql::lexer_ir::assignments_of_insert(s).unwrap_or_default())
        .collect();

    for fk in foreign_keys.iter() {
        if !database.tables.contains_key(&fk.parent_table) {
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

        let Ok(found) =
            referenced_values_now(database, &fk.parent_table, &fk.parent_column, &literals, tx, None)
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
            referenced_values_now(database, &fk.parent_table, &fk.parent_column, &literals, tx, None)
                .await
        else {
            continue;
        };
        if still.len() != literals.len() {
            continue;
        }
        for literal in &literals {
            verified.insert(verified_key(&fk.parent_table, &fk.parent_column, literal));
        }
    }
    verified
}

/// How many literals one referenced-key probe carries. `IN` is rewritten into
/// a disjunction the evaluator tests per row, so one huge list costs more than
/// a few moderate ones.
const REFERENCED_PROBE_CHUNK: usize = 512;

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
async fn referenced_values_now(
    database: &Arc<Database>,
    table_name: &str,
    column: &str,
    literals: &[String],
    tx: TxId,
    limit: Option<u64>,
) -> Result<Vec<Vec<u8>>, String> {
    let Some(table) = database.tables.get(table_name).map(|t| t.value().clone()) else {
        return Err(format!("relation \"{}\" does not exist", table_name));
    };
    let schema = &table.schema.fields;
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
    let requested = crate::core::rql::join::fetch_for(std::slice::from_ref(&field), schema);
    let all_fields = schema.to_vec();

    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut values = Vec::new();
    for chunk in literals.chunks(REFERENCED_PROBE_CHUNK) {
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

        let rows = table
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
            .map_err(|e| e.to_string())?;
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

    let live = |candidate: TxId| {
        matches!(
            database.transactions.status().outcome(candidate),
            crate::core::mvcc::transaction::TxOutcome::InProgress
        )
    };
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

/// Every foreign key in the database that points at `parent_table`.
fn referencing_keys(
    database: &Arc<Database>,
    parent_table: &str,
) -> Vec<(String, crate::core::rql::foreign_key::ForeignKey)> {
    let mut out = Vec::new();
    for entry in database.tables.iter() {
        for fk in entry.value().schema.foreign_keys.iter() {
            if fk.parent_table.eq_ignore_ascii_case(parent_table) {
                out.push((entry.key().clone(), fk.clone()));
            }
        }
    }
    out
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
    let Some(entry) = database.tables.get(table) else {
        return "NULL".to_string();
    };
    let schema = &entry.value().schema;
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

    for (child_table, fk) in referencing_keys(database, parent_table) {
        let Some(referenced) = keys
            .iter()
            .find(|k| k.column.eq_ignore_ascii_case(&fk.parent_column))
        else {
            continue;
        };
        if referenced.literals.is_empty() {
            continue;
        }
        let list = referenced.literals.join(", ");
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
                &referenced.literals,
                tx,
                Some(1),
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
            &referenced.literals,
            tx,
            Some(1),
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
async fn keys_about_to_be_deleted<S: crate::core::storage_providers::traits::StorageIO>(
    database: &Arc<Database>,
    table: &crate::core::row::table::Table<S>,
    table_name: &str,
    where_clause: &str,
    query_row_fetch: &crate::core::row::row::RowFetch,
    visibility: &crate::core::mvcc::Visibility,
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
            column_values_as_of(table, &column, where_clause, query_row_fetch, visibility)
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
async fn column_values_as_of<S: crate::core::storage_providers::traits::StorageIO>(
    table: &crate::core::row::table::Table<S>,
    column: &str,
    where_clause: &str,
    query_row_fetch: &crate::core::row::row::RowFetch,
    visibility: &crate::core::mvcc::Visibility,
) -> Result<Vec<(Vec<u8>, String)>, String> {
    let schema = &table.schema.fields;
    let Some(field) = schema
        .iter()
        .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(column))
        .cloned()
    else {
        return Err(format!(
            "column \"{}\" of relation \"{}\" does not exist",
            column, table.schema.name
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

    let rows = table
        .query_row_with_visibility(
            where_clause,
            &schema.to_vec(),
            query_row_fetch.clone(),
            requested,
            None,
            None,
            visibility.clone(),
        )
        .await
        .map_err(|e| e.to_string())?;

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
fn update_needs_statement_snapshot<S: crate::core::storage_providers::traits::StorageIO>(
    database: &Arc<Database>,
    table: &crate::core::row::table::Table<S>,
    table_name: &str,
    plan: &crate::core::rql::lexer_ur::UpdateRow,
) -> bool {
    let assigned = |name: &str| {
        table
            .schema
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
        table
            .schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
            .is_some_and(|f| plan.expressions.iter().any(|(w, _)| *w == f.write_order as u32))
    };
    table.schema.foreign_keys.iter().any(|fk| by_expression(&fk.column))
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
async fn check_foreign_keys_for_update<S: crate::core::storage_providers::traits::StorageIO>(
    database: &Arc<Database>,
    table: &crate::core::row::table::Table<S>,
    table_name: &str,
    plan: &crate::core::rql::lexer_ur::UpdateRow,
    assignments: &[(String, String)],
    visibility: Option<&crate::core::mvcc::Visibility>,
    tx: TxId,
) -> Result<(), String> {
    let schema = &table.schema.fields;
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

    let by_expression: Vec<_> = table
        .schema
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
    let full = crate::core::row::table::Table::<S>::build_full_row_fetch(schema);
    let rows = table
        .query_row_with_visibility(
            &plan.where_clause,
            &schema.to_vec(),
            plan.query_row_fetch.clone(),
            full,
            None,
            None,
            visibility.clone(),
        )
        .await
        .map_err(|e| e.to_string())?;
    for (column, field, expr) in by_expression {
        let mut seen = std::collections::HashSet::new();
        for row in &rows {
            let value = crate::core::rql::expr::eval(expr, row, schema.as_slice())
                .and_then(|v| crate::core::rql::expr::from_engine_scale(v, &field))?;
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
async fn keys_about_to_change<S: crate::core::storage_providers::traits::StorageIO>(
    database: &Arc<Database>,
    table: &crate::core::row::table::Table<S>,
    table_name: &str,
    plan: &crate::core::rql::lexer_ur::UpdateRow,
    visibility: &crate::core::mvcc::Visibility,
) -> Result<Option<KeyChanges>, String> {
    let schema = &table.schema.fields;
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

    let full = crate::core::row::table::Table::<S>::build_full_row_fetch(schema);
    let rows = table
        .query_row_with_visibility(
            &plan.where_clause,
            &schema.to_vec(),
            plan.query_row_fetch.clone(),
            full,
            None,
            None,
            visibility.clone(),
        )
        .await
        .map_err(|e| e.to_string())?;

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
                let value = crate::core::rql::expr::eval(expr, row, schema.as_slice())
                    .and_then(|v| crate::core::rql::expr::from_engine_scale(v, &field))?;
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
        let referencing = referenced_values_now(
            database,
            &child_table,
            &fk.column,
            &change.old_literals,
            tx,
            Some(1),
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

    for (child_table, fk) in referencing_keys(database, parent_table) {
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
        )
        .await
        .map_err(unchecked)?
        .into_iter()
        .collect();
        let vanished: Vec<usize> = (0..change.old_stored.len())
            .filter(|i| !present.contains(&change.old_stored[*i]))
            .collect();
        if vanished.is_empty() {
            continue;
        }
        let vanished_literals: Vec<String> =
            vanished.iter().map(|i| change.old_literals[*i].clone()).collect();

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
                            change.old_literals[*i],
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
    // The transaction asking. The rows are read as of now on its behalf, so
    // that what it has written here and not yet committed survives the
    // rebuild — a frozen read would not see them, and the drop would take
    // them — without narrowing to a snapshot it may have pinned, which would
    // take what others have committed since.
    reader: TxId,
) -> Result<(), String> {
    use crate::core::rql::lexer_ct::{CreateColumnData, CreateTable};

    let (
        existing_columns,
        foreign_keys,
        mut defaults,
        json_columns,
        unique_columns,
        unique_keys,
        type_modifiers,
        generated,
        checks,
        carried_indexes,
        carried_path_indexes,
        old_field_names,
        rows,
    ) = {
        let entry = database.tables.get(table_name);
        let Some(table) = entry.as_ref().clone() else {
            return Err(format!("relation '{}' does not exist", table_name));
        };

        let schema = &table.schema.fields;
        let mut ordered: Vec<crate::core::row::schema::SchemaField> =
            schema.iter().filter(|f| !f.is_deleted).cloned().collect();
        ordered.sort_by_key(|f| f.write_order);

        let fetch = crate::core::rql::join::fetch_for(&ordered, schema);
        let empty = crate::core::rql::join::fetch_for(&[], schema);
        let visibility = table.visibility_now_for(reader);
        let rows = table
            .query_row_with_visibility("", &schema.to_vec(), empty, fetch, None, None, visibility)
            .await
            .map_err(|e| format!("could not read '{}' for rewrite: {}", table_name, e))?;

        let columns: Vec<CreateColumnData> = ordered
            .iter()
            .map(|f| CreateColumnData {
                name: f.name.clone(),
                data_type: f.db_type.clone(),
            })
            .collect();

        // A rewrite drops and recreates the table, so every constraint has to be
        // carried across or `ALTER TABLE … ADD COLUMN` would quietly drop the
        // table's uniqueness along with it.
        let unique_columns: Vec<String> = ordered
            .iter()
            .filter(|f| f.is_unique)
            .map(|f| f.name.clone())
            .collect();

        // The old numbering, so each fetched value can be named before the
        // table is dropped and renumbered.
        let old_field_names: std::collections::HashMap<u64, String> = schema
            .iter()
            .map(|f| (f.write_order, f.name.clone()))
            .collect();

        (
            columns,
            table.schema.foreign_keys.clone(),
            table.schema.defaults.clone(),
            table.schema.json_columns.clone(),
            unique_columns,
            // Only the keys the rows were checked under. A predicate that no
            // longer compiles has not been enforced, and recreated next to a
            // new column of the name it reads, it would compile against
            // values no row was checked against: the reinsert below then
            // failed on the first duplicate, after the table was dropped.
            table
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
                .collect::<Vec<_>>(),
            table.schema.type_modifiers.clone(),
            // Carried across for the same reason the type modifiers are: a
            // rewrite drops the table, and a generation expression lost here
            // would leave the column present and permanently NULL.
            table.schema.generated.clone(),
            // Carried for the same reason: a rewrite drops the table, and a
            // constraint lost here stops being enforced with nothing said.
            table.schema.checks.clone(),
            // Index declarations, each with its columns by name: the rewrite
            // renumbers columns, and a name is what survives it.
            table
                .schema
                .secondary_indexes
                .iter()
                .map(|decl| {
                    let names: Vec<Option<String>> = decl
                        .columns
                        .iter()
                        .map(|wo| {
                            table
                                .schema
                                .fields
                                .iter()
                                .find(|f| f.write_order as u32 == *wo && !f.is_deleted)
                                .map(|f| f.name.clone())
                        })
                        .collect();
                    (decl.clone(), names)
                })
                .collect::<Vec<_>>(),
            // Path-index declarations, which a rewrite used to lose: the
            // filter then stopped carrying the path, silently.
            table.schema.path_indexes.clone(),
            old_field_names,
            rows,
        )
    };

    // Values are captured by **column name**, not by `write_order`.
    //
    // The recreated table renumbers its columns from zero, so a write_order is
    // only stable while the old numbering had no gaps. `DROP COLUMN` leaves
    // one: after dropping `b` from `(a, b, c)` the surviving fields still read
    // `a=0, c=2`, while the rewritten table numbers them `a=0, c=1, d=2` — so
    // `c`'s bytes were written into `d`, and every later column shifted with
    // it. That reported success and stored a corrupt row. Names survive the
    // rewrite unchanged, so they are what the values are matched on.
    let name_of: std::collections::HashMap<u64, String> = old_field_names;
    let mut captured: Vec<Vec<(String, crate::memory_pool::MemoryBlock, DbTypeRef)>> =
        Vec::with_capacity(rows.len());
    for row in &rows {
        captured.push(
            row.columns
                .iter()
                .filter_map(|c| {
                    name_of
                        .get(&u64::from(c.schema_id))
                        .map(|name| (name.clone(), c.data.clone(), c.column_type.clone()))
                })
                .collect(),
        );
    }

    let mut columns = existing_columns;
    columns.extend(new_columns);
    let carries_unique_keys = unique_keys.iter().any(|k| !k.is_deleted);

    database
        .drop_table(table_name)
        .await
        .map_err(|e| format!("could not clear '{}': {}", table_name, e))?;
    database
        .add_table(CreateTable {
            table_name: table_name.to_string(),
            columns,
            is_immutable: false,
            // A rewrite always creates the table it just dropped.
            if_not_exists: false,
            foreign_keys: foreign_keys.into_iter().collect(),
            defaults: {
                defaults.extend(new_defaults.iter().cloned());
                defaults.into_iter().collect()
            },
            json_columns: json_columns.into_iter().chain(new_json).collect(),
            // Carried across the rewrite. Losing a column's declared scale here
            // would silently reinterpret every decimal it holds.
            type_modifiers: type_modifiers.into_iter().collect(),
            generated: generated.into_iter().collect(),
            checks: checks.into_iter().collect(),
            unique_columns,
            unique_keys: unique_keys.into_iter().collect(),
            // A shadow table for a JSON document has no declared key.
            primary_key: None,
        })
        .await
        .map_err(|e| format!("could not recreate '{}': {}", table_name, e))?;

    // Index declarations go back before the rows do, so reopening builds the
    // (still empty) indexes and every reinserted row is indexed by its insert.
    //
    // Table-level unique keys reopen it too. `add_table` registers them on a
    // table whose indexes were already built, so without a reopen their
    // implicit indexes were missing until the next restart: `pg_indexes`
    // stopped listing a partial unique index after `ADD COLUMN`, and a
    // migration that checks there before creating it would have tried again.
    if !carried_indexes.is_empty() || !carried_path_indexes.is_empty() || carries_unique_keys {
        let (io_schema, fields) = {
            let entry = database.tables.get(table_name);
            let Some(table) = entry.as_ref().clone() else {
                return Err(format!("relation '{}' vanished during rewrite", table_name));
            };
            (table.io_schema.clone(), table.schema.fields.clone())
        };
        for (decl, names) in carried_indexes {
            // An index over a column this rewrite dropped goes with it, as in
            // PostgreSQL.
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
            crate::core::index::decl::append(io_schema.clone(), &decl)
                .await
                .map_err(|e| format!("could not carry index {}: {}", decl.name, e))?;
        }
        for path_index in &carried_path_indexes {
            crate::core::rql::path_index::append(io_schema.clone(), path_index)
                .await
                .map_err(|e| format!("could not carry index {}: {}", path_index.name, e))?;
        }
        database
            .reload_table(table_name)
            .await
            .map_err(|e| format!("could not reopen '{}' with its indexes: {}", table_name, e))?;
    }

    let entry = database.tables.get(table_name);
    let Some(table) = entry.as_ref().clone() else {
        return Err(format!("relation '{}' vanished during rewrite", table_name));
    };
    let schema = table.schema.fields.clone();

    for values in captured {
        let mut payloads: smallvec::SmallVec<[crate::core::row::row::ColumnWritePayload; 32]> =
            smallvec::SmallVec::new();

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
                        &table.schema.defaults,
                    )
                    .and_then(|e| {
                        crate::core::rql::column_default::evaluate(e, &field.db_type, database.id)
                            .ok()
                    })
                    .filter(|literal| literal != "NULL")
                    .and_then(|literal| {
                        crate::core::value_codec::encode_literal(
                            &field.db_type,
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
                                &table.schema.defaults,
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
        table
            .insert_row(crate::core::row::row::RowWrite {
                columns_writing_data: payloads,
            })
            .await
            .map_err(|e| format!("could not rewrite a row of '{}': {}", table_name, e))?;
    }

    Ok(())
}

/// What fills an output slot no fetched column occupies.
enum SlotValue {
    /// A written constant, the same in every row.
    Constant(Vec<u8>),
    /// A nullary function, evaluated once per row.
    Scalar(crate::core::rql::aggregate::ScalarFunc),
    /// An expression over the row's own columns, and the scale its value is
    /// sent at when it is a `NUMERIC` (see `expr::display_scale`).
    RowExpression(crate::core::rql::expr::Expr, Option<u32>),
}

type DbTypeRef = crate::core::db_type::DbType;

/// Replaces each JSON document in an `INSERT` with the reference to the row it
/// was stored as, or `None` when the statement has no JSON column to map.
async fn rewrite_json_values(
    database: &Arc<Database>,
    sql: &str,
    assignments: &[(String, String)],
    json_columns: &[crate::core::rql::json_map::JsonColumn],
    tx: TxId,
) -> Result<Option<String>, String> {
    use crate::core::rql::json_map;

    if json_columns.is_empty() {
        return Ok(None);
    }

    let mut replacements: Vec<(String, String)> = Vec::new();
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

        let reference = Box::pin(store_json_document(
            database,
            shadow_table,
            &document,
            0,
            tx,
        ))
        .await?;
        replacements.push((literal.clone(), reference));
    }

    if replacements.is_empty() {
        return Ok(None);
    }

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

    if !database.tables.contains_key(shadow_table) {
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

    // The column is matched by exact name. A match that differs only in case
    // is a different key — JSON keys are case-sensitive — that the engine's
    // case-insensitive column names cannot hold under its own spelling, so
    // that key takes the hashed form instead. Matching loosely here was how
    // `Date` found `date`, skipped the `ADD COLUMN`, and then failed the
    // `INSERT` on a column the parser could not find.
    let (column, existing) = {
        let entry = database.tables.get(shadow_table);
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
    };
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
        // The column already holds this.
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
    use crate::core::rql::json_map;

    // Read every row as the documents they represent, so the conversion goes
    // through one representation rather than a matrix of type pairs.
    //
    // As of now, on `tx`'s behalf: `tx` may have written rows here it has not
    // committed — a batch of documents whose later member is the one widening
    // the column — and the rebuild below keeps only what this read returns. A
    // frozen read would miss them; `tx`'s own pinned snapshot, if it has one,
    // would miss what others committed since. Both losses would be silent.
    let (fields, rows) = {
        let entry = database.tables.get(table_name);
        let Some(table) = entry.as_ref().clone() else {
            return Err(format!("relation '{}' does not exist", table_name));
        };
        let visible = crate::core::rql::nullable::visible_fields(&table.schema.fields);
        let fetch = crate::core::rql::join::fetch_for(&visible, &table.schema.fields);
        let empty = crate::core::rql::join::fetch_for(&[], &table.schema.fields);
        let visibility = table.visibility_now_for(tx);
        let rows = table
            .query_row_with_visibility(
                "",
                &table.schema.fields.to_vec(),
                empty,
                fetch,
                None,
                None,
                visibility,
            )
            .await
            .map_err(|e| format!("could not read '{}' to retype: {}", table_name, e))?;
        (visible, rows)
    };

    // Capture each row as literals before the table is rebuilt.
    let mut captured: Vec<Vec<(String, String)>> = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut values: Vec<(String, String)> = Vec::new();
        for (index, field) in fields.iter().enumerate() {
            let Some(cell) = row.columns.get(index) else {
                continue;
            };
            if cell.column_type == crate::core::db_type::DbType::NULL {
                continue;
            }
            let bytes = cell.data.into_slice();

            // Only the column being retyped is converted. Every other value
            // is re-rendered from its own bytes by its own type, so nothing is
            // put through a text round trip that could not survive one — a
            // UUID read as text would come back as replacement characters.
            let literal = if field.name.eq_ignore_ascii_case(column) {
                let Some(value) = json_value_of(&field.db_type, bytes) else {
                    continue;
                };
                match literal_of_json(&value, new_type) {
                    Some(literal) => literal,
                    None => continue,
                }
            } else {
                match crate::core::rql::subquery::value_to_literal(&field.db_type, bytes) {
                    Some(literal) => literal,
                    None => continue,
                }
            };
            values.push((field.name.clone(), literal));
        }
        captured.push(values);
    }

    // Rebuild with the column at its new type.
    let columns: Vec<crate::core::rql::lexer_ct::CreateColumnData> = fields
        .iter()
        .map(|f| crate::core::rql::lexer_ct::CreateColumnData {
            name: f.name.clone(),
            data_type: if f.name.eq_ignore_ascii_case(column) {
                new_type.clone()
            } else {
                f.db_type.clone()
            },
        })
        .collect();

    let declarations: Vec<String> = columns
        .iter()
        .map(|c| {
            let nullability = if c.name == JSON_KEY_COLUMN {
                " NOT NULL"
            } else {
                ""
            };
            format!(
                "{} {}{}",
                c.name,
                json_map::sql_type_name(&c.data_type),
                nullability
            )
        })
        .collect();

    database
        .drop_table(table_name)
        .await
        .map_err(|e| format!("could not clear '{}': {}", table_name, e))?;
    run_ddl(
        database,
        &format!("CREATE TABLE {} ({})", table_name, declarations.join(", ")),
        tx,
    )
    .await?;

    for values in captured {
        if values.is_empty() {
            continue;
        }
        let names: Vec<&str> = values.iter().map(|(n, _)| n.as_str()).collect();
        let literals: Vec<&str> = values.iter().map(|(_, l)| l.as_str()).collect();
        run_ddl(
            database,
            &format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table_name,
                names.join(", "),
                literals.join(", ")
            ),
            tx,
        )
        .await?;
    }

    Ok(())
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
        if let Err(e) = combined.push(&branch_bytes, at < parts.distinct_prefix) {
            return ddl_error(&e);
        }
    }
    match combined.finish() {
        Ok(result) => result,
        Err(e) => ddl_error(&e),
    }
}

/// The column an implicit unique-constraint name refers to.
///
/// PostgreSQL names the constraint behind a `UNIQUE` column `<table>_<column>_key`.
/// The table part is not checked: a table renamed since the constraint was
/// created keeps the old name in it, which is exactly the case that arises when
/// a migration renames a table and a later one drops the constraint.
fn implicit_unique_column(
    constraint: &str,
    fields: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
) -> Option<String> {
    let lower = constraint.trim().trim_matches('"').to_ascii_lowercase();
    let stem = lower.strip_suffix("_key")?;
    fields
        .iter()
        .filter(|f| f.is_unique && !f.is_deleted)
        .find(|f| {
            stem.ends_with(&format!("_{}", f.name.to_ascii_lowercase()))
                || stem == f.name.to_ascii_lowercase()
        })
        .map(|f| f.name.clone())
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

/// Replaces each outer reference in `sql` with the literal it resolved to.
///
/// Whole-word only, and longest first, so `fwd.id` inside `fwd.id_extra` is
/// left alone.
fn substitute_outer_refs(sql: &str, bindings: &[(String, String)]) -> String {
    let mut ordered: Vec<&(String, String)> = bindings.iter().collect();
    ordered.sort_by_key(|(name, _)| std::cmp::Reverse(name.len()));

    let mut out = sql.to_string();
    for (name, literal) in ordered {
        let mut result = String::with_capacity(out.len());
        let mut rest = out.as_str();
        while let Some(at) = rest.find(name.as_str()) {
            let before_ok = at == 0 || {
                let b = rest.as_bytes()[at - 1];
                !(b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
            };
            let end = at + name.len();
            let after_ok = end >= rest.len() || {
                let b = rest.as_bytes()[end];
                !(b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
            };
            result.push_str(&rest[..at]);
            if before_ok && after_ok {
                result.push_str(literal);
            } else {
                result.push_str(&rest[at..end]);
            }
            rest = &rest[end..];
        }
        result.push_str(rest);
        out = result;
    }
    out
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
    let key = {
        let table_ref = database.tables.get(table);
        let handle = table_ref
            .as_ref()
            .clone()
            .ok_or_else(|| format!("relation '{}' does not exist", table))?;
        let field = identifying_field(&handle.schema).ok_or_else(|| {
            format!(
                "DELETE … USING needs a unique column on '{}' to name each row by",
                table
            )
        })?;
        field.name.clone()
    };

    // The source's columns the condition reads are carried out of the join,
    // so each row's delete can recheck the condition against the target row
    // as it is when deleted, with the source row it was joined to. The join
    // ran under its own snapshot; a target row changed since may no longer
    // match, and PostgreSQL rechecks it rather than deleting it.
    let target_columns: Vec<String> = {
        let entry = database.tables.get(table);
        entry
            .as_ref()
            .map(|h| h.schema.fields.iter().filter(|f| !f.is_deleted).map(|f| f.name.clone()).collect())
            .unwrap_or_default()
    };
    let recheck = recheck_terms(condition, target_alias, &target_columns).filter(|t| !t.is_empty());
    let recheck_text = recheck.as_ref().map(|terms| terms.join(" AND "));
    let recheck_refs = recheck_text
        .as_deref()
        .map(|t| qualified_refs(t, &source_aliases(source_clause)))
        .unwrap_or_default();
    let mut projection = vec![format!("{}.{}", target_alias, key)];
    projection.extend(recheck_refs.iter().cloned());
    let select = format!(
        "SELECT {} FROM {} {}, {} WHERE {}",
        projection.join(", "),
        table,
        target_alias,
        source_clause,
        condition
    );
    let rows = run_subquery(&select, database, tx).await?;
    if rows.is_empty() {
        let mut result = vec![0u8];
        result.extend_from_slice(&0u64.to_le_bytes());
        return Ok(result);
    }

    let returning_unqualified =
        returning.map(|r| format!("RETURNING {}", strip_qualifier(r.trim(), target_alias)));

    // One delete per DISTINCT key: several source rows may match one target
    // row, and it can only be deleted once.
    let mut seen: ahash::HashSet<Vec<u8>> =
        ahash::HashSet::with_capacity_and_hasher(rows.len(), Default::default());
    let mut deleted = 0u64;
    let mut returned: Vec<u8> = Vec::new();
    for row in &rows {
        let column = row
            .columns
            .first()
            .ok_or_else(|| "the join returned no key".to_string())?;
        if !seen.insert(column.data.into_slice().to_vec()) {
            continue;
        }
        let literal = crate::core::rql::subquery::value_to_literal(
            &column.column_type,
            column.data.into_slice(),
        )
        .ok_or_else(|| "a joined key cannot be written back".to_string())?;

        let returning_text = match &returning_unqualified {
            Some(r) => format!(" {}", r),
            None => String::new(),
        };
        let key_only = format!("DELETE FROM {} WHERE {} = {}{}", table, key, literal, returning_text);
        let mut bindings: Vec<(String, String)> = Vec::with_capacity(recheck_refs.len());
        for (offset, reference) in recheck_refs.iter().enumerate() {
            let carried = row
                .columns
                .get(1 + offset)
                .ok_or_else(|| "the join returned fewer values than the delete needs".to_string())?;
            let literal = crate::core::rql::subquery::value_to_literal(&carried.column_type, carried.data.into_slice())
                .ok_or_else(|| "a joined value cannot be written back".to_string())?;
            bindings.push((reference.clone(), literal));
        }
        let with_recheck = match &recheck_text {
            Some(text) => format!(
                "DELETE FROM {} WHERE {} = {} AND ({}){}",
                table,
                key,
                literal,
                strip_qualifier(&substitute_outer_refs(text, &bindings), target_alias),
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
    // this is refused rather than guessed at.
    let key = {
        let table_ref = database.tables.get(table);
        let handle = table_ref
            .as_ref()
            .clone()
            .ok_or_else(|| format!("relation '{}' does not exist", table))?;
        // The guard is a shard read lock; it ends here, before any await.
        let field = identifying_field(&handle.schema).ok_or_else(|| {
            format!(
                "UPDATE … FROM needs a unique column on '{}' to write each row back by",
                table
            )
        })?;
        field.name.clone()
    };

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
        let entry = database.tables.get(table);
        let handle = entry
            .as_ref()
            .clone()
            .ok_or_else(|| format!("relation '{}' does not exist", table))?;
        handle.schema.fields.to_vec()
    };

    let mut projection: Vec<String> = vec![format!("{}.{}", target_alias, key)];
    // For each assignment: either the output slot its value occupies, or the
    // expression to evaluate and the slots its columns landed in.
    let mut sources: Vec<AssignedValue> = Vec::with_capacity(assignments.len());

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
                sources.push(AssignedValue::Computed {
                    text: stripped.clone(),
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
        .map(|terms| terms.join(" AND "));
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

    let rows = run_subquery(&select, database, tx).await?;
    if rows.is_empty() {
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
                let literal = crate::core::rql::subquery::value_to_literal(
                    &column.column_type,
                    column.data.into_slice(),
                )
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
                        let value =
                            crate::core::rql::subquery::rows_to_scalar(&produced, document)?;
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

    let mut written = 0u64;
    let mut returned: Vec<u8> = Vec::new();
    for (row, _) in rows.iter().zip(&keep_row).filter(|(_, keep)| **keep) {
        let literal_at = |index: usize| -> Result<String, String> {
            let column = &row.columns[index];
            crate::core::rql::subquery::value_to_literal(
                &column.column_type,
                column.data.into_slice(),
            )
            .ok_or_else(|| "a joined value cannot be written".to_string())
        };

        let key_literal = literal_at(0)?;
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
                AssignedValue::Computed { text, .. } => {
                    sets.push(format!("{} = {}", column, text));
                    continue;
                }
            };
            sets.push(format!("{} = {}", column, literal));
        }

        let returning_text = match &returning_unqualified {
            Some(r) => format!(" {}", r),
            None => String::new(),
        };
        let key_only = format!(
            "UPDATE {} SET {} WHERE {} = {}{}",
            table,
            sets.join(", "),
            key,
            key_literal,
            returning_text
        );
        let mut bindings: Vec<(String, String)> = Vec::with_capacity(recheck_refs.len());
        for (offset, reference) in recheck_refs.iter().enumerate() {
            bindings.push((reference.clone(), literal_at(recheck_at + offset)?));
        }
        let with_recheck = match &recheck_text {
            Some(text) => format!(
                "UPDATE {} SET {} WHERE {} = {} AND ({}){}",
                table,
                sets.join(", "),
                key,
                key_literal,
                strip_qualifier(&substitute_outer_refs(text, &bindings), target_alias),
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
fn source_aliases(source_clause: &str) -> Vec<String> {
    crate::core::rql::lexer_ct::split_definitions(source_clause)
        .iter()
        .filter_map(|item| item.split_whitespace().next_back())
        .map(|name| name.trim_matches('"').to_string())
        .collect()
}

/// The `alias.column` references in `text` qualified by one of `aliases`,
/// each once, in order. Quoted text is skipped.
fn qualified_refs(text: &str, aliases: &[String]) -> Vec<String> {
    let bytes = text.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || !b.is_ascii();
    let mut found: Vec<String> = Vec::new();
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
        if is_word(bytes[i]) && (i == 0 || !(is_word(bytes[i - 1]) || bytes[i - 1] == b'.')) {
            let start = i;
            while i < bytes.len() && (is_word(bytes[i]) || bytes[i] == b'.') {
                i += 1;
            }
            let word = &text[start..i];
            if let Some((qualifier, column)) = word.split_once('.') {
                if !column.is_empty()
                    && !column.contains('.')
                    && aliases.iter().any(|a| a.eq_ignore_ascii_case(qualifier))
                    && !found.iter().any(|f| f == word)
                {
                    found.push(word.to_string());
                }
            }
            continue;
        }
        i += 1;
    }
    found
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

/// Runs one generated per-row write. When it carries a recheck of the join
/// condition and that cannot run per row (the condition names a column the
/// rewritten statement cannot resolve), the key-only form is run instead,
/// which is what this path did before it rechecked.
async fn run_per_row_write(
    with_recheck: String,
    key_only: String,
    database: &Arc<Database>,
    depth: usize,
    tx: TxId,
) -> Result<Vec<u8>, String> {
    let mut statement = with_recheck;
    for attempt in 0..2 {
        let response = match recognize_query_purpose(&statement) {
            Some(purpose) => Box::pin(execute_with_context(purpose, database.clone(), depth, tx)).await,
            None => {
                let mut refused = vec![3u8];
                refused.extend_from_slice(format!("could not parse the rewritten '{}'", statement).as_bytes());
                refused
            }
        };
        if response.first() != Some(&3u8) || attempt == 1 || statement == key_only {
            return Ok(response);
        }
        // A conflict is the statement's to retry whole, never the fallback's.
        let text = String::from_utf8_lossy(&response[1..]);
        let marker = crate::core::row::table::Table::<
            crate::core::storage_providers::file_sync::LocalStorageProvider,
        >::CONFLICT_RETRYABLE;
        if text.contains("could not serialize") || text.contains(marker) {
            return Ok(response);
        }
        debug!("per-row recheck could not run ({}); writing by key alone", text);
        statement = key_only.clone();
    }
    unreachable!("the loop returns on its second pass")
}

/// Where one assigned value comes from once the join has run.
enum AssignedValue {
    /// The join projected the value directly; this is its output slot.
    Slot(usize),
    /// The join could not compute it, so it is carried through as written and
    /// evaluated by the per-row `UPDATE` this path generates — the same path a
    /// plain `UPDATE` takes, and the only one that can see whether a column is
    /// NULL. The columns it reads are still projected, so the join's output
    /// slots line up with the assignments beside it.
    Computed { text: String },
}

/// Turns a conflicting `INSERT … ON CONFLICT … DO UPDATE` into the `UPDATE` it
/// asks for.
///
/// `EXCLUDED.<column>` names the value the insert would have written, so the
/// insert's own assignments are what resolve it. The conflicting row is found
/// by the arbiter columns, or — when none was written — by whichever unique
/// column the statement supplies a value for.
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
    let inserted =
        crate::core::rql::lexer_ir::assignments_of_insert(insert_sql).unwrap_or_default();
    if inserted.is_empty() {
        return ddl_error("ON CONFLICT DO UPDATE needs the inserted values to resolve EXCLUDED");
    }

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
    } else {
        let entry = database.tables.get(table_name);
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
    if let Some(entry) = database.tables.get(table_name) {
        let same_columns = |columns: &[String]| {
            columns.len() == keys.len()
                && columns.iter().all(|c| keys.iter().any(|k| k.eq_ignore_ascii_case(c)))
        };
        let schema = &entry.value().schema;
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

    let resolve = |text: &str| -> Result<String, String> {
        let text = substitute_excluded(text, &inserted)?;
        Ok(fold_constants_in_predicate(&strip_qualifier(
            &text, table_name,
        )))
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
    let returning = crate::core::rql::join::find_keyword(insert_sql, "RETURNING", 0)
        .map(|at| insert_sql[at..].trim().to_string());

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
        return ddl_error(&format!("could not parse the rewritten '{}'", statement));
    };
    Box::pin(execute_with_context(purpose, database.clone(), depth, tx)).await
}

/// Takes the locks a `SELECT … FOR UPDATE` asks for, and reports which rows
/// survive.
///
/// Returns the rows the statement should return: all of them for `FOR UPDATE`
/// and `NOWAIT` (which either lock everything or fail), and only the ones this
/// transaction could lock for `SKIP LOCKED`.
///
/// The lock key is the row's unique column. A table without one cannot be
/// locked row-by-row here, and that is refused rather than silently locking
/// nothing — a claim query that locks nothing hands every worker the same rows.
async fn lock_selected_rows(
    database: &Arc<Database>,
    table_name: &str,
    rows: Vec<crate::core::row::row::Row>,
    lock: crate::core::rql::row_lock::RowLock,
    // How many rows the statement will actually return. Locking stops there:
    // PostgreSQL locks the rows it hands back and no others, and locking the
    // whole scan would let one worker reserve the entire queue while claiming
    // two rows of it.
    wanted: Option<usize>,
    tx: TxId,
) -> Result<Vec<crate::core::row::row::Row>, String> {
    use crate::core::mvcc::row_locks::Acquired;
    use crate::core::rql::row_lock::LockWait;

    if rows.is_empty() {
        return Ok(rows);
    }

    let key_field = {
        let entry = database.tables.get(table_name);
        let table = entry
            .as_ref()
            .clone()
            .ok_or_else(|| format!("relation '{}' does not exist", table_name))?;
        identifying_field(&table.schema).ok_or_else(|| {
            format!(
                "FOR UPDATE needs a unique column on '{}' to identify the rows it locks",
                table_name
            )
        })?
    };

    // The key has to be in the row, which means the statement has to have
    // selected it. Refused rather than approximated: locking the wrong row is
    // worse than not locking.
    let key_at = |row: &crate::core::row::row::Row| -> Option<Vec<u8>> {
        row.columns
            .iter()
            .find(|c| u64::from(c.schema_id) == key_field.write_order)
            .map(|c| c.data.into_slice().to_vec())
    };

    let live = |candidate: TxId| {
        matches!(
            database.transactions.status().outcome(candidate),
            crate::core::mvcc::transaction::TxOutcome::InProgress
        )
    };

    let mut kept = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(cap) = wanted {
            if kept.len() >= cap {
                break;
            }
        }
        let Some(key) = key_at(&row) else {
            return Err(format!(
                "FOR UPDATE needs '{}' in the select list to identify the rows it locks",
                key_field.name
            ));
        };

        // One attempt, then act on the answer. Waiting is a loop with a bound
        // rather than an unbounded park: a lock whose holder never finishes
        // would otherwise hang the connection with nothing to report.
        let mut outcome = database
            .row_locks
            .acquire(table_name, &key, tx, lock.strength, &live);
        if matches!(lock.behaviour, LockWait::Wait) {
            let started = std::time::Instant::now();
            let deadline = started + LOCK_WAIT_TIMEOUT;
            let _waiting = database.row_locks.waiting_guard(tx);
            while let Acquired::Held(_) = outcome {
                // The statement's own bound comes first: `statement_timeout`
                // and a client that has gone both end the wait, as the same
                // error a cancelled scan reports. An error and never the rows
                // locked so far — a short claim would read as a complete one.
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
                            .row_blockers(table_name, &key, tx, lock.strength, &live)
                    },
                    &live,
                ) {
                    return Err(deadlock);
                }
                if std::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                outcome = database
                    .row_locks
                    .acquire(table_name, &key, tx, lock.strength, &live);
            }
        }

        match outcome {
            Acquired::Yes => kept.push(row),
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
    Ok(kept)
}

/// Takes the table locks a `LOCK TABLE` asks for.
///
/// Locks are held for the transaction, so outside an explicit block the
/// statement's own autocommit transaction ends immediately and the lock with
/// it — which is what PostgreSQL does, and it warns for the same reason.
#[inline(never)]
async fn execute_lock_table(
    database: &Arc<Database>,
    statement: crate::core::rql::row_lock::LockTable,
    tx: TxId,
) -> Vec<u8> {
    use crate::core::mvcc::row_locks::Acquired;

    // PostgreSQL refuses this outside a transaction block, because the lock
    // would be released the instant the statement ended and the caller would be
    // left believing it held one. Same answer here, and for the same reason.
    if tx == crate::core::mvcc::transaction::FROZEN_TX {
        return ddl_error("LOCK TABLE can only be used in transaction blocks");
    }

    let live = |candidate: TxId| {
        matches!(
            database.transactions.status().outcome(candidate),
            crate::core::mvcc::transaction::TxOutcome::InProgress
        )
    };

    for name in &statement.tables {
        if !database.tables.contains_key(name) {
            return ddl_error(&format!("relation '{}' does not exist", name));
        }

        let mut outcome = database
            .row_locks
            .acquire_table(name, tx, statement.mode, &live);
        if !statement.nowait {
            let started = std::time::Instant::now();
            let deadline = started + LOCK_WAIT_TIMEOUT;
            let _waiting = database.row_locks.waiting_guard(tx);
            while let Acquired::Held(_) = outcome {
                // `statement_timeout` and a departed client end the wait, as
                // the cancellation error rather than the lock-timeout one.
                if let Some(reason) = crate::core::cancel::tripped() {
                    return ddl_error(reason.eval_error().as_str());
                }
                if let Some(deadlock) = deadlock_check(
                    &database,
                    tx,
                    started,
                    || {
                        database
                            .row_locks
                            .table_blockers(name, tx, statement.mode, &live)
                    },
                    &live,
                ) {
                    return ddl_error(&deadlock);
                }
                if std::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                outcome = database
                    .row_locks
                    .acquire_table(name, tx, statement.mode, &live);
            }
        }

        if let Acquired::Held(owner) = outcome {
            return ddl_error(&if statement.nowait {
                format!("could not obtain lock on relation '{}'", name)
            } else {
                format!(
                    "timed out waiting {}s for a lock on relation '{}' held by transaction {}",
                    LOCK_WAIT_TIMEOUT.as_secs(),
                    name,
                    owner
                )
            });
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

        match supplied
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(&field.name))
        {
            Some((_, value)) => {
                if value.trim().eq_ignore_ascii_case("NULL") {
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
/// The key column is [`identifying_field`], the one `FOR UPDATE` locks by. This
/// used to pick only a column-level `UNIQUE`, so on a table identified by its
/// primary key or a table-level `UNIQUE (col)` the writer looked for locks
/// under no column at all and overwrote rows another transaction held.
#[inline(never)]
async fn refuse_write_to_locked_rows(
    database: &Arc<Database>,
    table_name: &str,
    where_clause: &str,
    tx: TxId,
) -> Result<(), String> {
    use crate::core::mvcc::row_locks::Acquired;
    use crate::core::rql::row_lock::LockStrength;

    let live = |candidate: TxId| {
        matches!(
            database.transactions.status().outcome(candidate),
            crate::core::mvcc::transaction::TxOutcome::InProgress
        )
    };

    // A whole-table lock answers for every row in it, so it is checked first
    // and without needing to know which rows the statement touches.
    if let Some(owner) = database
        .row_locks
        .table_conflict(table_name, tx, true, &live)
    {
        return Err(format!(
            "relation '{}' is locked by transaction {}; the statement that locked it must \
             finish first",
            table_name, owner
        ));
    }

    if database.row_locks.is_empty() {
        return Ok(());
    }

    let key_field = {
        let entry = database.tables.get(table_name);
        let Some(table) = entry.as_ref().clone() else {
            return Ok(());
        };
        // `lock_selected_rows` refuses a table without one, so nothing here
        // can be locked row-by-row and nothing can conflict.
        let Some(field) = identifying_field(&table.schema) else {
            return Ok(());
        };
        field
    };

    let select = format!(
        "SELECT {} FROM {}{}{}",
        key_field.name,
        table_name,
        if where_clause.trim().is_empty() {
            ""
        } else {
            " WHERE "
        },
        where_clause.trim()
    );
    let rows = run_subquery(&select, database, tx).await?;

    for row in &rows {
        let Some(cell) = row.columns.first() else {
            continue;
        };
        let key = cell.data.into_slice();
        // Asked for, not merely inspected: this transaction is about to write
        // the row, so it should hold the lock while it does.
        if let Acquired::Held(owner) =
            database
                .row_locks
                .acquire(table_name, key, tx, LockStrength::Update, &live)
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
    // A join's `ON` is a predicate too, and reaches the same tokenizer:
    // `ON n.nspname = current_schema()` arrived there as the identifier
    // `current_schema` and panicked the worker.
    let sql = fold_on_clauses(sql);
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
        fold_constants_in_predicate(&body[..end]),
        &body[end..]
    )
}

/// Folds the constant parts of every `JOIN … ON` predicate.
///
/// Each `ON` runs to whatever clause comes next, so the region folded is
/// bounded by the following `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY` or `LIMIT`.
fn fold_on_clauses(sql: &str) -> String {
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
        out.push_str(&fold_constants_in_predicate(&rest[..end]));
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
    fn fold_operand(operand: &str) -> String {
        let trimmed = operand.trim();
        if trimmed.is_empty() || !crate::core::rql::expr::needs_evaluation(trimmed) {
            return operand.to_string();
        }
        // The surrounding whitespace is part of the statement, not of the
        // operand. Dropping it ran the substituted literal straight into the
        // next keyword — `< '2026-…'ORDER BY id` — so the clause splitter no
        // longer found ` ORDER BY ` and handed `ORDER` to the tokenizer as a
        // column name.
        let lead = &operand[..operand.len() - operand.trim_start().len()];
        let tail = &operand[operand.trim_end().len()..];
        let wrap = |value: String| format!("{}{}{}", lead, value, tail);
        let Ok(expr) = crate::core::rql::expr::parse(trimmed, &[]) else {
            return operand.to_string();
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
                    Some(literal) => wrap(literal),
                    None => operand.to_string(),
                }
            }
        }
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
                // Fold each side of the comparison independently: only one of
                // them is usually constant.
                match find_comparison(term) {
                    Some((at, width)) => {
                        out.push_str(&fold_operand(&term[..at]));
                        out.push_str(&term[at..at + width]);
                        out.push_str(&fold_operand(&term[at + width..]));
                    }
                    None => out.push_str(term),
                }
            }
        }
    }
    out
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
        if depth == 0 {
            for keyword in [" AND ", " OR "] {
                if upper[i..].starts_with(keyword) {
                    out.push(BooleanPiece::Term(&text[start..i]));
                    out.push(BooleanPiece::Separator(&text[i..i + keyword.len()]));
                    i += keyword.len();
                    start = i;
                    break;
                }
            }
        }
        if i < bytes.len() && start <= i {
            i += 1;
        }
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

/// Replaces every `EXCLUDED.<column>` with the value the insert would have
/// written for it.
fn substitute_excluded(text: &str, inserted: &[(String, String)]) -> Result<String, String> {
    const MARKER: &str = "EXCLUDED.";
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    loop {
        let upper = rest.to_ascii_uppercase();
        let Some(at) = upper.find(MARKER) else {
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
            return Err(format!(
                "EXCLUDED.{} has no value: this INSERT does not write that column",
                column
            ));
        };
        out.push_str(literal);
        rest = &after[end..];
    }
}

/// Removes `<table>.` qualifiers, which an `UPDATE`'s own clauses do not carry.
fn strip_qualifier(text: &str, table: &str) -> String {
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
    // what PostgreSQL reports.
    if rows.is_empty() {
        let mut result = vec![0u8];
        result.extend_from_slice(&0u64.to_le_bytes());
        return Some(result);
    }

    let mut tuples: Vec<String> = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut parts: Vec<String> = Vec::with_capacity(row.columns.len());
        for column in &row.columns {
            // `value_to_literal` quotes and escapes, so a value holding a quote
            // or a parenthesis rebuilds as data rather than as syntax.
            let Some(literal) = crate::core::rql::subquery::value_to_literal(
                &column.column_type,
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
/// In its own function so its locals — a copy of the schema — live on the
/// heap under `Box::pin` rather than in `execute_with_context`'s frame.
async fn supply_insert_column_list(
    row_data: &crate::core::rql::lexer_s1::InsertRowData,
    database: &Arc<Database>,
) -> Option<Result<QueryPurpose, String>> {
    let (fields, generated) = database.tables.get(&row_data.table_name).map(|entry| {
        let schema = &entry.value().schema;
        (schema.fields.clone(), schema.generated.clone())
    })?;
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
async fn supply_generated_values(
    row_data: &crate::core::rql::lexer_s1::InsertRowData,
    database: &Arc<Database>,
) -> Option<Result<crate::core::rql::lexer_s1::InsertRowData, String>> {
    let (fields, generated, defaults) = {
        let entry = database.tables.get(&row_data.table_name)?;
        let schema = &entry.value().schema;
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
    let rewritten = match append_insert_values(&row_data.query, &supplied_defaults) {
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
    let inputs: Vec<(String, String)> = assignments
        .into_iter()
        .filter(|(name, _)| {
            crate::core::rql::generated::expression_for(name, &schema.generated).is_none()
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
    let mut inserted = 0u64;
    let mut returned: Vec<u8> = Vec::new();
    let mut any_returning = false;
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
        let mut attempt = 0usize;
        let response = loop {
            let Some(mut purpose) = recognize_query_purpose(&stmt) else {
                return Some(ddl_error("could not re-parse a row of the VALUES clause"));
            };
            // `ON CONFLICT` was split off the statement before the tuples were, so
            // `row_data.query` — the text these rows were cut from — no longer
            // carries it, and re-parsing a row yields `on_conflict_do_nothing:
            // false`. Restoring the parsed clause is what makes a multi-row upsert
            // behave like the single-row one.
            //
            // Without this, `INSERT … VALUES (…), (…) ON CONFLICT (k) DO NOTHING`
            // *raised* a duplicate-key error the moment any tuple conflicted — and
            // because the loop below returns on the first error, the tuples that
            // did not conflict were never inserted either. A statement whose whole
            // point is "insert what fits and skip the rest" instead inserted
            // nothing and failed. It stalled PMS's Ultra→Cold migration on
            // 192.168.1.7, which pages exactly this shape.
            //
            // Restored as parsed values rather than re-appended as text: the tail
            // the splitter carries onto each row already holds `RETURNING`, and
            // `ON CONFLICT` has to precede it.
            if let QueryPurpose::InsertRow(ref mut data) = purpose {
                data.on_conflict_do_nothing = row_data.on_conflict_do_nothing;
                data.on_conflict_update = row_data.on_conflict_update.clone();
            }
            let response =
                Box::pin(execute_with_context(purpose, database.clone(), depth, tx)).await;
            if !is_retryable_conflict(&response) || attempt >= MAX_CONFLICT_RETRIES {
                break response;
            }
            attempt += 1;
            // The transaction that won has to get to run for this to clear, so
            // backing off beats spinning on it.
            tokio::time::sleep(conflict_backoff(attempt)).await;
        };
        match response.first() {
            // Having applied nothing, the statement as a whole is still safe
            // for the caller to replay, so the marker is left on. Once a tuple
            // has landed it is not safe, and the conflict surfaces as itself
            // rather than inviting a replay that would re-insert those rows.
            Some(3) => {
                return Some(if inserted > 0 || any_returning {
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
    let exists = database.tables.contains_key(&create_table.table_name);
    if exists {
        info!(
            "Table '{}' already exists; IF NOT EXISTS makes that success",
            create_table.table_name
        );
    }
    exists
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
    let reference = store_json_document(database, &shadow, &document, 0, tx).await?;
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
            let handles: Vec<_> = parts
                .iter()
                .skip(1)
                .map(|part| scope.spawn(|| fold_part(part)))
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

    if depth >= json_map::MAX_NESTING_DEPTH || !database.tables.contains_key(shadow_table) {
        return None;
    }

    let all_fields: Vec<crate::core::row::schema::SchemaField> = {
        let entry = database.tables.get(shadow_table);
        let table = entry.as_ref().clone()?;
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

async fn execute_drop_table(sql: &str, database: &Arc<Database>) -> Vec<u8> {
    use crate::core::rql::lexer_alter;

    let (table_name, if_exists) = match lexer_alter::parse_drop_table(sql) {
        Ok(v) => v,
        Err(e) => {
            error!("Cannot parse DROP TABLE: {}", e);
            return ddl_error(&e);
        }
    };

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

    // The schema without the nullability companions. Handing the rewriter the
    // full schema makes it wrap every reference in `__null_x = 0 AND …`, which
    // turns "this column is NULL" into "the check failed" — the opposite of
    // what a check does with NULL.
    let visible: Vec<crate::core::row::schema::SchemaField> =
        crate::core::rql::nullable::visible_fields(&schema.fields);
    let visible_sv: smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> =
        visible.iter().cloned().collect();

    for constraint in &schema.checks {
        if constraint.is_deleted {
            continue;
        }

        // `BETWEEN`, `IN` and the rest are sugar the condition evaluator does
        // not read; the predicate rewriter lowers them to the comparisons it
        // does. A check it cannot lower is logged and skipped rather than
        // failing every insert into the table.
        let lowered = match crate::core::tokenizer::predicate_rewrite::rewrite_where(
            &constraint.expression,
            &visible_sv,
        ) {
            Ok(text) => text,
            Err(reason) => {
                log::error!(
                    "check constraint '{}' on '{}' could not be compiled and is not being                      enforced: {}",
                    constraint.name,
                    table_name,
                    reason
                );
                continue;
            }
        };

        let condition = match crate::core::rql::expr::parse_cond(&lowered, &visible) {
            Ok(condition) => condition,
            Err(reason) => {
                log::error!(
                    "check constraint '{}' on '{}' could not be compiled and is not being                      enforced: {}",
                    constraint.name,
                    table_name,
                    reason
                );
                continue;
            }
        };

        // A check whose inputs include a NULL passes without being evaluated —
        // PostgreSQL rejects a row only when a check is *false*, and a
        // comparison against NULL is NULL rather than false. Unless the check
        // tests for NULL itself, in which case that test is the point.
        let upper = constraint.expression.to_ascii_uppercase();
        let tests_null = upper.contains("IS NULL") || upper.contains("IS NOT NULL");
        if !tests_null && condition_reads_a_null(&condition, &schema.fields, row) {
            continue;
        }

        match crate::core::rql::expr::eval_cond(&condition, row, &visible) {
            Ok(true) => {}
            Ok(false) => {
                return Err(format!(
                    "new row for relation \"{}\" violates check constraint \"{}\"",
                    table_name, constraint.name
                ));
            }
            Err(reason) => {
                log::error!(
                    "check constraint '{}' on '{}' could not be evaluated and is not being                      enforced for this row: {}",
                    constraint.name,
                    table_name,
                    reason
                );
            }
        }
    }
    Ok(())
}

/// Refuses an `UPDATE` whose assignments violate a `CHECK`.
///
/// Only the constraints whose every column the statement assigns can be judged:
/// `UPDATE t SET status = 99` supplies everything `CHECK (status BETWEEN 1 AND
/// 10)` reads, while `UPDATE t SET lo = 5` supplies half of
/// `CHECK (lo < hi)` and the other half is in a row this has not read.
///
/// **The rest are not enforced on update.** An `UPDATE` can therefore leave a
/// row that violates a multi-column check. That is stated rather than hidden,
/// and it is the narrower half of the choice: evaluating a check against a
/// value it does not have would reject rows that are perfectly valid.
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

    let visible: Vec<crate::core::row::schema::SchemaField> =
        crate::core::rql::nullable::visible_fields(&schema.fields);
    let visible_sv: smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> =
        visible.iter().cloned().collect();

    for constraint in schema.checks.iter().filter(|c| !c.is_deleted) {
        let Ok(lowered) = crate::core::tokenizer::predicate_rewrite::rewrite_where(
            &constraint.expression,
            &visible_sv,
        ) else {
            continue;
        };
        let Ok(condition) = crate::core::rql::expr::parse_cond(&lowered, &visible) else {
            continue;
        };

        // Every column the check reads has to be one this statement assigns,
        // or the row it would be judged against is incomplete.
        let mut columns = Vec::new();
        collect_condition_columns(&condition, &mut columns);
        if columns.is_empty() || !columns.iter().all(|f| assigned.contains(&f.write_order)) {
            continue;
        }
        if columns.iter().any(|f| computed.contains(&f.write_order)) {
            continue;
        }

        if let Ok(false) = crate::core::rql::expr::eval_cond(&condition, &candidate, &visible) {
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
    visible: Vec<crate::core::row::schema::SchemaField>,
    /// Each check's name, its condition, and whether it tests for NULL itself.
    checks: Vec<(String, crate::core::rql::expr::Cond, bool)>,
}

impl UpdateRowChecks {
    /// Empty, at the cost of two `is_empty`s, unless the table has checks and
    /// the statement assigns an expression.
    pub(crate) fn compile(
        schema: &crate::core::row::schema::TableSchema,
        updates: &[crate::core::row::row::ColumnWritePayload],
        expressions: &[(u32, crate::core::rql::expr::Expr)],
    ) -> Self {
        let mut out = UpdateRowChecks {
            visible: Vec::new(),
            checks: Vec::new(),
        };
        if schema.checks.is_empty() || expressions.is_empty() {
            return out;
        }
        out.visible = crate::core::rql::nullable::visible_fields(&schema.fields);
        let visible_sv: smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> =
            out.visible.iter().cloned().collect();

        for constraint in schema.checks.iter().filter(|c| !c.is_deleted) {
            // Compiled as `check_constraints_hold_for_update` compiles it, so
            // the two agree on which checks each one judges.
            let Ok(lowered) = crate::core::tokenizer::predicate_rewrite::rewrite_where(
                &constraint.expression,
                &visible_sv,
            ) else {
                continue;
            };
            let Ok(condition) = crate::core::rql::expr::parse_cond(&lowered, &out.visible) else {
                continue;
            };
            let mut columns = Vec::new();
            collect_condition_columns(&condition, &mut columns);
            let assigned = |wo: u64| updates.iter().any(|u| u.write_order as u64 == wo);
            let computed = |wo: u64| expressions.iter().any(|(e, _)| *e as u64 == wo);
            if columns.is_empty()
                || !columns.iter().all(|f| assigned(f.write_order))
                || !columns.iter().any(|f| computed(f.write_order))
            {
                continue;
            }
            let upper = constraint.expression.to_ascii_uppercase();
            let tests_null = upper.contains("IS NULL") || upper.contains("IS NOT NULL");
            out.checks.push((constraint.name.clone(), condition, tests_null));
        }
        out
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.checks.is_empty()
    }

    /// Refuses a row, given as the payloads about to be written, that one of
    /// the checks finds false. NULL is handled as `check_constraints_hold`
    /// handles it on insert: a check whose inputs include a NULL passes.
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
        for (name, condition, tests_null) in &self.checks {
            if !tests_null && condition_reads_a_null(condition, fields, &row) {
                continue;
            }
            match crate::core::rql::expr::eval_cond(condition, &row, &self.visible) {
                Ok(true) => {}
                Ok(false) => {
                    return Err(format!(
                        "new row for relation \"{}\" violates check constraint \"{}\"",
                        table_name, name
                    ));
                }
                Err(reason) => {
                    log::error!(
                        "check constraint '{}' on '{}' could not be evaluated and is not being \
                         enforced for this row: {}",
                        name,
                        table_name,
                        reason
                    );
                }
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
    }
}

/// Whether any column a condition reads holds NULL in this row.
fn condition_reads_a_null(
    condition: &crate::core::rql::expr::Cond,
    schema: &smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]>,
    row: &crate::core::row::row::Row,
) -> bool {
    let mut columns = Vec::new();
    collect_condition_columns(condition, &mut columns);

    columns.iter().any(|field| {
        let Some(flag) = crate::core::rql::nullable::flag_field(&field.name, schema) else {
            return false;
        };
        row.columns
            .iter()
            .find(|c| u64::from(c.schema_id) == flag.write_order)
            .is_some_and(|c| crate::core::rql::nullable::flag_is_set(c.data.into_slice()))
    })
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
async fn seed_sequences(database: &Arc<Database>, table_name: &str) {
    let table = {
        let Some(entry) = database.tables.get(table_name) else {
            return;
        };
        entry.value().clone()
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
        if !crate::core::rql::sequence::is_seeded(database.id, &name) {
            wanted.push((name, field.clone()));
        }
    }
    if wanted.is_empty() {
        return;
    }

    let schema = &table.schema.fields;
    let fetch_fields: Vec<crate::core::row::schema::SchemaField> =
        wanted.iter().map(|(_, f)| f.clone()).collect();
    let fetch = crate::core::rql::join::fetch_for(&fetch_fields, schema);
    let empty = crate::core::rql::join::fetch_for(&[], schema);
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
            return;
        }
    };

    for (name, field) in wanted {
        let last =
            crate::core::rql::sequence::max_of_column(&rows, field.write_order, &field.db_type)
                .unwrap_or(0);
        crate::core::rql::sequence::seed(database.id, &name, last);
    }
}

async fn existing_rows_break_unique(
    database: &Arc<Database>,
    table_name: &str,
    key: &crate::core::rql::unique_key::UniqueKey,
) -> Result<Option<String>, String> {
    let entry = database.tables.get(table_name);
    let Some(table) = entry.as_ref().clone() else {
        return Err(format!("relation '{}' does not exist", table_name));
    };

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
    let rows = table
        .query_row("", &schema.to_vec(), empty, fetch, None, None)
        .await
        .map_err(|e| {
            format!(
                "could not read '{}' to validate the constraint: {}",
                table_name, e
            )
        })?;

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

    let plans = match lexer_alter::parse_alter_table_actions(sql) {
        Ok(p) => p,
        Err(e) => {
            error!("Cannot parse ALTER TABLE: {}", e);
            return ddl_error(&e);
        }
    };

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
        if let Some(first) = plans.first() {
            if database.tables.get(&first.table_name).is_none() {
                info!(
                    "ALTER TABLE IF EXISTS {}: relation absent, nothing to do",
                    first.table_name
                );
                return ddl_ok(0);
            }
        }
    }

    let mut result = ddl_ok(0);
    for plan in plans {
        result = execute_alter_table_action(plan, database, tx).await;
        // `ddl_error` frames lead with status 3.
        if result.first() == Some(&3u8) {
            return result;
        }
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

/// Moves a column's by-name declarations — its `DEFAULT`, its generation
/// expression and its NUMERIC scale — from `from` to `to`, or retires them when
/// `to` is `None` (the column was dropped).
///
/// Each lives in an append-only sidecar where the newest record for a name
/// wins, so moving is two appends: the declaration under the new name, then a
/// tombstone under the old. A declaration still filed under `to` with nothing
/// to replace it — left by a dropped column before drops retired theirs — is
/// tombstoned too, so the renamed column does not inherit it.
///
/// `schema` is the table as loaded, whose lists hold only live records.
/// Returns whether anything was written.
async fn move_column_declarations<S: crate::core::storage_providers::traits::StorageIO>(
    schema: &crate::core::row::schema::TableSchema,
    from: &str,
    to: Option<&str>,
    io_schema: Arc<S>,
) -> Result<bool, String> {
    use crate::core::rql::{column_default, generated, type_modifier};
    let named = |a: &str, b: &str| a.eq_ignore_ascii_case(b);

    let default_of = |name: &str| schema.defaults.iter().find(|d| named(&d.column, name));
    let mut defaults = Vec::new();
    if let Some(to) = to {
        match default_of(from) {
            Some(d) => defaults.push(column_default::ColumnDefault {
                column: to.to_string(),
                expression: d.expression.clone(),
                is_deleted: false,
            }),
            None if default_of(to).is_some() => defaults.push(column_default::ColumnDefault {
                column: to.to_string(),
                expression: String::new(),
                is_deleted: true,
            }),
            None => {}
        }
    }
    if default_of(from).is_some() {
        defaults.push(column_default::ColumnDefault {
            column: from.to_string(),
            expression: String::new(),
            is_deleted: true,
        });
    }
    for entry in &defaults {
        column_default::append(io_schema.clone(), entry)
            .await
            .map_err(|e| e.to_string())?;
    }

    let generated_of = |name: &str| schema.generated.iter().find(|g| named(&g.column, name));
    let mut gens = Vec::new();
    if let Some(to) = to {
        match generated_of(from) {
            Some(g) => gens.push(generated::GeneratedColumn {
                column: to.to_string(),
                expression: g.expression.clone(),
                is_deleted: false,
            }),
            None if generated_of(to).is_some() => gens.push(generated::GeneratedColumn {
                column: to.to_string(),
                expression: String::new(),
                is_deleted: true,
            }),
            None => {}
        }
    }
    if generated_of(from).is_some() {
        gens.push(generated::GeneratedColumn {
            column: from.to_string(),
            expression: String::new(),
            is_deleted: true,
        });
    }
    for entry in &gens {
        generated::append(io_schema.clone(), entry)
            .await
            .map_err(|e| e.to_string())?;
    }

    let modifier_of = |name: &str| {
        schema
            .type_modifiers
            .iter()
            .find(|m| named(&m.column, name) && !m.is_deleted)
    };
    let retired = |column: &str| type_modifier::TypeModifier {
        column: column.to_string(),
        precision: 0,
        scale: 0,
        is_deleted: true,
    };
    let mut modifiers = Vec::new();
    if let Some(to) = to {
        match modifier_of(from) {
            Some(m) => modifiers.push(type_modifier::TypeModifier {
                column: to.to_string(),
                ..m.clone()
            }),
            None if modifier_of(to).is_some() => modifiers.push(retired(to)),
            None => {}
        }
    }
    if modifier_of(from).is_some() {
        modifiers.push(retired(from));
    }
    for entry in &modifiers {
        type_modifier::append(io_schema.clone(), entry)
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(!(defaults.is_empty() && gens.is_empty() && modifiers.is_empty()))
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
async fn retype_numeric_modifier<S: crate::core::storage_providers::traits::StorageIO>(
    schema: &crate::core::row::schema::TableSchema,
    column: &str,
    declared: &str,
    row_count: u64,
    io_schema: Arc<S>,
) -> Result<bool, String> {
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
        (None, None) => return Ok(false),
        (Some(m), Some((p, s))) if m.precision == p && m.scale == s => return Ok(false),
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
        return Ok(false);
    }
    type_modifier::append(io_schema, &entry)
        .await
        .map_err(|e| e.to_string())?;
    Ok(true)
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
/// type (after `::`) or a qualifier (followed by `.`); after a `.` it is one
/// only when the qualifier is the table itself.
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
            let is_reference = text.eq_ignore_ascii_case(from)
                && prev != Prev::Cast
                && after != Some('(')
                && after != Some('.')
                && prev != Prev::Dot(false);
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
        out.push(c);
        i += 1;
        prev = Prev::Other;
        last_word = None;
    }
    out
}

/// The constraint records `RENAME COLUMN` has to rewrite: every by-name record
/// outside the column's own declarations that mentions it.
///
/// Planned before the rename writes anything, so a rewrite that cannot be
/// stored — an expression grown past its sidecar's limit — refuses the
/// statement instead of leaving it half done.
struct ColumnReferences {
    unique_keys: Vec<crate::core::rql::unique_key::UniqueKey>,
    /// `(table, io, record)` for each foreign key that names the column, on
    /// this table as the child or on any table with this one as the parent.
    foreign_keys: Vec<(
        String,
        Arc<crate::core::storage_providers::backend::DbStorageProvider>,
        crate::core::rql::foreign_key::ForeignKey,
    )>,
    checks: Vec<crate::core::rql::check::CheckConstraint>,
    generated: Vec<crate::core::rql::generated::GeneratedColumn>,
}

fn plan_column_references(
    database: &Arc<Database>,
    table: &str,
    schema: &crate::core::row::schema::TableSchema,
    from: &str,
    to: &str,
) -> Result<ColumnReferences, String> {
    use crate::core::rql::{check, generated, unique_key};
    let named = |a: &str, b: &str| a.eq_ignore_ascii_case(b);

    // A partial index's predicate names columns too. Left naming `from`, it
    // no longer compiled, and a key that cannot read its predicate is not
    // enforced: renaming `deleted_at` let every live duplicate in.
    let mut unique_keys = Vec::new();
    for k in schema.unique_keys.iter().filter(|k| !k.is_deleted) {
        let predicate = k
            .predicate
            .as_deref()
            .map(|p| rename_column_references(p, table, from, to));
        if !k.columns.iter().any(|c| named(c, from)) && predicate.as_deref() == k.predicate.as_deref() {
            continue;
        }
        if predicate
            .as_ref()
            .is_some_and(|p| p.len() > unique_key::MAX_PREDICATE_LEN)
        {
            return Err(format!(
                "cannot rename column {} to {}: the predicate of unique index {} would exceed {} characters",
                from,
                to,
                k.name,
                unique_key::MAX_PREDICATE_LEN
            ));
        }
        unique_keys.push(unique_key::UniqueKey {
            columns: k
                .columns
                .iter()
                .map(|c| if named(c, from) { to.to_string() } else { c.clone() })
                .collect(),
            predicate,
            ..k.clone()
        });
    }

    // Every table's, since a parent's column is named in its children's
    // sidecars. Collected before anything awaits: no map guard is held then.
    let mut foreign_keys = Vec::new();
    for entry in database.tables.iter() {
        let owner = entry.key();
        for fk in entry.value().schema.foreign_keys.iter().filter(|f| !f.is_deleted) {
            let child_side = named(owner, table) && named(&fk.column, from);
            let parent_side = named(&fk.parent_table, table) && named(&fk.parent_column, from);
            if !(child_side || parent_side) {
                continue;
            }
            let mut moved = fk.clone();
            if child_side {
                moved.column = to.to_string();
            }
            if parent_side {
                moved.parent_column = to.to_string();
            }
            foreign_keys.push((owner.clone(), entry.value().io_schema.clone(), moved));
        }
    }

    let mut checks = Vec::new();
    for c in schema.checks.iter().filter(|c| !c.is_deleted) {
        let expression = rename_column_references(&c.expression, table, from, to);
        if expression == c.expression {
            continue;
        }
        if expression.len() > check::MAX_EXPRESSION_LEN {
            return Err(format!(
                "cannot rename column {} to {}: check constraint {} would exceed {} characters",
                from,
                to,
                c.name,
                check::MAX_EXPRESSION_LEN
            ));
        }
        checks.push(check::CheckConstraint {
            name: c.name.clone(),
            expression,
            is_deleted: false,
        });
    }

    let mut gens = Vec::new();
    for g in schema.generated.iter().filter(|g| !g.is_deleted) {
        let expression = rename_column_references(&g.expression, table, from, to);
        if expression == g.expression {
            continue;
        }
        if expression.len() > generated::MAX_EXPRESSION_LEN {
            return Err(format!(
                "cannot rename column {} to {}: the expression of generated column {} would \
                 exceed {} characters",
                from,
                to,
                g.column,
                generated::MAX_EXPRESSION_LEN
            ));
        }
        gens.push(generated::GeneratedColumn {
            column: g.column.clone(),
            expression,
            is_deleted: false,
        });
    }

    Ok(ColumnReferences {
        unique_keys,
        foreign_keys,
        checks,
        generated: gens,
    })
}

/// Writes what [`plan_column_references`] planned, and returns the *other*
/// tables whose sidecars changed, which the caller reopens.
///
/// A unique key or foreign key moves as an append of the renamed copy, then a
/// tombstone on the original (the first live record of that name, which the
/// copy is not). In that order a crash between the two leaves the original
/// naming a column that no longer exists, which enforcement skips, rather
/// than no record at all. A check or generation expression is superseded by
/// appending, the newest record of its name winning.
///
/// `generated` is written after the column's own declarations moved, and none
/// of it is filed under the renamed column itself, whose expression cannot
/// read itself.
async fn write_column_references(
    references: ColumnReferences,
    table: &str,
    io_schema: Arc<crate::core::storage_providers::backend::DbStorageProvider>,
) -> Result<Vec<String>, String> {
    use crate::core::rql::{check, foreign_key, generated, unique_key};
    let failed = |e: std::io::Error| e.to_string();

    for key in &references.unique_keys {
        unique_key::append(io_schema.clone(), key).await.map_err(failed)?;
        unique_key::remove(io_schema.clone(), &key.name).await.map_err(failed)?;
    }
    let mut others = Vec::new();
    for (owner, io, fk) in &references.foreign_keys {
        foreign_key::append(io.clone(), fk).await.map_err(failed)?;
        foreign_key::remove(io.clone(), &fk.name).await.map_err(failed)?;
        if !owner.eq_ignore_ascii_case(table) && !others.contains(owner) {
            others.push(owner.clone());
        }
    }
    for c in &references.checks {
        check::append(io_schema.clone(), c).await.map_err(failed)?;
    }
    for g in &references.generated {
        generated::append(io_schema.clone(), g).await.map_err(failed)?;
    }
    Ok(others)
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
    database
        .tables
        .iter()
        .filter(|table| table.key().as_str() != except)
        .flat_map(|table| {
            crate::core::rql::sequence::named_by(&table.value().schema.defaults).collect::<Vec<_>>()
        })
        .collect()
}

async fn execute_alter_table_action(
    plan: crate::core::rql::lexer_alter::AlterTable,
    database: &Arc<Database>,
    tx: TxId,
) -> Vec<u8> {
    use crate::core::rql::lexer_alter::AlterAction;
    use crate::core::rql::nullable;

    // The map guard is released immediately: `reload_table` removes the entry,
    // and holding a read guard across that would deadlock the shard.
    let (mut schema, io_schema, row_count) = {
        let table_ref = database.tables.get(&plan.table_name);
        let Some(table) = table_ref.as_ref().clone() else {
            return ddl_error(&format!("relation '{}' does not exist", plan.table_name));
        };
        // A schema change is written to storage and the table handle rebuilt
        // from it, because `Table` owns its schema by value.
        (
            table.schema.clone(),
            table.io_schema.clone(),
            table.len.load(std::sync::atomic::Ordering::Relaxed),
        )
    };

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
        AlterAction::AddColumn {
            name,
            db_type,
            nullable: is_nullable,
            if_not_exists,
            default,
            json,
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
            let fills_existing = default
                .as_deref()
                .map(|e| !e.trim().eq_ignore_ascii_case("NULL"))
                .unwrap_or(false);

            // A declaration still filed under the name — a dead column's,
            // from before drops retired theirs or from a power loss between a
            // drop's separately durable writes — is retired first. Left, the
            // new column inherited it: the old DEFAULT, or a generation
            // expression, or a NUMERIC scale it silently rounded to.
            match move_column_declarations(&schema, &name, None, io_schema.clone()).await {
                // The rewrite below reads the open table's declarations.
                Ok(true) if row_count > 0 => {
                    if let Err(e) = database.reload_table(&plan.table_name).await {
                        return ddl_error(&e.to_string());
                    }
                }
                Ok(_) => {}
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

                return match Box::pin(rewrite_table_with_columns(
                    &database,
                    &plan.table_name,
                    added,
                    new_defaults,
                    new_json,
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

            // A dropped column still holding this name — or its companion's —
            // is retired first, exactly as `RENAME COLUMN` does. `add_field`
            // skips a name the schema already holds, tombstones included, so
            // `DROP COLUMN a` then `ADD COLUMN a` on an empty table reported
            // success and added nothing: every later reference to `a` failed.
            for taken in [name.clone(), nullable::flag_column_name(&name)] {
                if let Err(e) = retire_tombstoned_name(&mut schema, &taken, io_schema.clone()).await {
                    return ddl_error(&e);
                }
            }

            let default =
                match json_default_reference(&database, &plan.table_name, &name, default, json, tx)
                    .await
                {
                    Ok(d) => d,
                    Err(e) => return ddl_error(&e),
                };
            if let Some(expression) = default {
                let entry = crate::core::rql::column_default::ColumnDefault {
                    column: name.clone(),
                    expression,
                    is_deleted: false,
                };
                if let Err(e) =
                    crate::core::rql::column_default::append(io_schema.clone(), &entry).await
                {
                    return ddl_error(&e.to_string());
                }
            }

            schema
                .add_field(io_schema.clone(), name.clone(), db_type, false)
                .await;
            if is_nullable {
                schema
                    .add_field(
                        io_schema.clone(),
                        nullable::flag_column_name(&name),
                        nullable::FLAG_TYPE,
                        false,
                    )
                    .await;
            }

            // A JSON column is a `UUID` plus a table of its own, and the sidecar
            // entry is what connects the two. Registered here exactly as
            // `Database::add_table` does for `CREATE TABLE`, so the two
            // statements produce the same thing rather than one working and the
            // other refusing.
            if json {
                let entry = crate::core::rql::json_map::JsonColumn {
                    column: name.clone(),
                    shadow_table: crate::core::rql::json_map::shadow_table_name(
                        &plan.table_name,
                        &name,
                    ),
                    is_deleted: false,
                };
                if let Err(e) = crate::core::rql::json_map::append(io_schema.clone(), &entry).await
                {
                    return ddl_error(&e.to_string());
                }
                schema.json_columns.push(entry);
            }
        }
        AlterAction::DropColumn { name, if_exists } => {
            let Some(field) = schema
                .fields
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(&name))
                .cloned()
            else {
                return if if_exists {
                    ddl_ok(0)
                } else {
                    ddl_error(&format!("column '{}' does not exist", name))
                };
            };

            // A partial unique index whose predicate reads the column goes
            // with it, as in PostgreSQL. Done first, so a failure here leaves
            // the column and its index as they were.
            for key in crate::core::rql::unique_key::dropped_with_column(
                &schema.unique_keys,
                &schema.fields,
                &field.name,
            ) {
                if let Err(e) = crate::core::rql::unique_key::remove(io_schema.clone(), &key).await {
                    return ddl_error(&e.to_string());
                }
                info!("index {} dropped with column {}", key, field.name);
            }

            // The column keeps its slot in the row layout and is marked
            // deleted: reclaiming the bytes would move every column after it,
            // which is the same rewrite `ADD COLUMN` needs.
            schema
                .mark_field_as_deleted(&field.name, io_schema.clone())
                .await;
            // Its by-name declarations go with it. Left behind, they were
            // picked up by the next column to take the name — the retype
            // dance's renamed temporary, or a plain re-`ADD` — which then
            // defaulted to the *dropped* column's value.
            if let Err(e) =
                move_column_declarations(&schema, &field.name, None, io_schema.clone()).await
            {
                return ddl_error(&e);
            }
            if let Some(flag) = nullable::flag_field(&field.name, &schema.fields) {
                let flag_name = flag.name.clone();
                schema
                    .mark_field_as_deleted(&flag_name, io_schema.clone())
                    .await;
            }
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

            // The constraints that name the column — composite UNIQUE keys,
            // foreign keys on either side, CHECK and generation expressions —
            // are by-name records too. Left naming `from`, each silently
            // stopped being enforced (a parent's renamed key instead failed
            // every DELETE from it). Planned here, before anything is written.
            let references =
                match plan_column_references(database, &plan.table_name, &schema, &from, &to) {
                    Ok(references) => references,
                    Err(e) => return ddl_error(&e),
                };

            // A tombstone still holding the target name is retired first.
            //
            // Renaming *onto* a dropped column's name leaves two fields called
            // `to` — one dead, one live — and every by-name lookup in the
            // engine takes the first it finds, which is the dead one at the old
            // offset with the old type. The retype dance (add temp, copy, drop
            // original, rename temp to the original name) does exactly this, so
            // `payload_format` came back holding the *pre-migration* value and
            // the statement reported success. `TableSchema::load` then keeps
            // the lowest write_order per name, which makes it survive a
            // restart.
            //
            // A dropped column can never be named again, so its name is free to
            // give up. Retiring it keeps every name in the schema unique, which
            // is what the lookups have always assumed.
            if let Err(e) = retire_tombstoned_name(&mut schema, &to, io_schema.clone()).await {
                return ddl_error(&e);
            }

            // Renaming touches no row bytes: the name is metadata, and the
            // layout is keyed by write order.
            if let Err(e) = schema.rename_field(&from, &to, io_schema.clone()).await {
                return ddl_error(&e);
            }

            // The column's DEFAULT, generation expression and NUMERIC scale
            // are filed by name, so they follow it. Left under `from`, the
            // renamed column silently lost all three — an omitted value stored
            // NULL instead of its default, and every decimal was read at the
            // engine's scale rather than its own.
            if let Err(e) = move_column_declarations(&schema, &from, Some(&to), io_schema.clone()).await
            {
                return ddl_error(&e);
            }
            match write_column_references(references, &plan.table_name, io_schema.clone()).await {
                Ok(others) => {
                    // This table is reopened below; a child whose foreign key
                    // named the renamed parent column is reopened here.
                    for other in others {
                        if let Err(e) = database.reload_table(&other).await {
                            return ddl_error(&e.to_string());
                        }
                    }
                }
                Err(e) => return ddl_error(&e),
            }

            // The NULL companion is named after its column, so it has to move
            // too. Left behind, the renamed column had no companion — it could
            // no longer be NULL — and an orphan `__null_<old>` stayed in the
            // table for good. The retype dance (add temp, copy, drop, rename)
            // does this on every nullable column, so the orphans accumulate.
            let old_flag = nullable::flag_column_name(&from);
            if schema
                .fields
                .iter()
                .any(|f| f.name.eq_ignore_ascii_case(&old_flag) && !f.is_deleted)
            {
                let new_flag = nullable::flag_column_name(&to);
                // The companion collides the same way its column does.
                if let Err(e) =
                    retire_tombstoned_name(&mut schema, &new_flag, io_schema.clone()).await
                {
                    return ddl_error(&e);
                }
                if let Err(e) = schema
                    .rename_field(&old_flag, &new_flag, io_schema.clone())
                    .await
                {
                    return ddl_error(&e);
                }
            }
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
            if followed.is_empty() {
                return ddl_ok(0);
            }
            let renamed_io = {
                let entry = database.tables.get(&to);
                let Some(table) = entry.as_ref() else {
                    return ddl_error(&format!("relation '{}' vanished during rename", to));
                };
                table.io_schema.clone()
            };
            for (entry, from, to) in &followed {
                if let Err(e) =
                    crate::core::rql::column_default::append(renamed_io.clone(), entry).await
                {
                    return ddl_error(&format!(
                        "renamed, but sequence {} could not follow as {}: {}",
                        from, to, e
                    ));
                }
                crate::core::rql::sequence::rename(database.id, from, to);
            }
            return match database.reload_table(&to).await {
                Ok(()) => ddl_ok(0),
                Err(e) => ddl_error(&e.to_string()),
            };
        }
        AlterAction::ConstraintClause { drop, description } => {
            use crate::core::rql::foreign_key;

            let upper = description.to_ascii_uppercase();

            // `DROP CONSTRAINT <name>`. Both sidecars are tried, since the name
            // alone does not say which kind it was. A name matching neither is
            // not an error: the constraint is gone either way, which is what
            // the statement asked for.
            if drop {
                let name = description
                    .split_whitespace()
                    .find(|w| {
                        !matches!(
                            w.to_ascii_uppercase().as_str(),
                            "CONSTRAINT" | "IF" | "EXISTS"
                        )
                    })
                    .map(|n| n.trim_matches('"').to_string());

                if let Some(name) = name {
                    let removed = match foreign_key::remove(io_schema.clone(), &name).await {
                        Ok(found) => found,
                        Err(e) => return ddl_error(&e.to_string()),
                    };
                    let removed = match crate::core::rql::unique_key::remove(
                        io_schema.clone(),
                        &name,
                    )
                    .await
                    {
                        Ok(found) => removed || found,
                        Err(e) => return ddl_error(&e.to_string()),
                    };

                    if removed {
                        // The sidecars are the source of truth; reloading picks
                        // the removal up.
                        return match database.reload_table(&plan.table_name).await {
                            Ok(()) => ddl_ok(0),
                            Err(e) => ddl_error(&e.to_string()),
                        };
                    }

                    // A column declared `UNIQUE` inline is a flag on the field,
                    // not a named sidecar, so no name could ever match it —
                    // and `DROP CONSTRAINT <table>_<column>_key`, which is what
                    // PostgreSQL calls it and what every migration written
                    // against PostgreSQL asks for, silently did nothing. The
                    // constraint then outlived the migration meant to replace
                    // it and rejected rows the new one allows.
                    if let Some(column) = implicit_unique_column(&name, &schema.fields) {
                        schema
                            .set_field_unique(&column, false, io_schema.clone())
                            .await;
                        return match database.reload_table(&plan.table_name).await {
                            Ok(()) => ddl_ok(0),
                            Err(e) => ddl_error(&e.to_string()),
                        };
                    }
                }

                warn!(
                    "ALTER TABLE {}: no stored constraint matched ({})",
                    plan.table_name, description
                );
                return ddl_ok(0);
            }

            // A foreign key is enforced, so it is stored rather than noted.
            if upper.contains("FOREIGN KEY") || upper.contains("REFERENCES") {
                let name = if upper.starts_with("CONSTRAINT") {
                    description
                        .split_whitespace()
                        .nth(1)
                        .map(|n| n.trim_matches('"').to_string())
                } else {
                    None
                };
                let clause_at = upper
                    .find("FOREIGN KEY")
                    .or_else(|| upper.find("REFERENCES"))
                    .unwrap_or(0);

                let fk = match foreign_key::parse_clause(&description[clause_at..], name, None) {
                    Ok(fk) => fk,
                    Err(e) => return ddl_error(&e),
                };
                if !schema
                    .fields
                    .iter()
                    .any(|f| f.name.eq_ignore_ascii_case(&fk.column))
                {
                    return ddl_error(&format!("column '{}' does not exist", fk.column));
                }
                if let Err(e) = foreign_key::append(io_schema.clone(), &fk).await {
                    return ddl_error(&e.to_string());
                }
                // The sidecar is the source of truth; reloading picks it up.
                return match database.reload_table(&plan.table_name).await {
                    Ok(()) => ddl_ok(0),
                    Err(e) => ddl_error(&e.to_string()),
                };
            }

            // `UNIQUE (…)` / `PRIMARY KEY (…)`, of any arity. Enforced, so it
            // is stored rather than noted — and validated against the rows
            // already there, since a constraint the existing data breaks was
            // never true of this table.
            // `CREATE UNIQUE INDEX IF NOT EXISTS` becomes this statement, and
            // the tolerance has to survive the rewrite: with it, a constraint
            // that is already there is success rather than an error.
            let (description, tolerate_existing) = {
                let trimmed = description.trim_start();
                let upper = trimmed.to_ascii_uppercase();
                if upper.starts_with("IF NOT EXISTS") {
                    (
                        trimmed["IF NOT EXISTS".len()..].trim_start().to_string(),
                        true,
                    )
                } else {
                    (description.clone(), false)
                }
            };

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

                    if let Err(e) =
                        crate::core::rql::unique_key::append(io_schema.clone(), &key).await
                    {
                        return ddl_error(&e.to_string());
                    }
                    // The sidecar is the source of truth; reloading picks it up,
                    // which is also what arms the write lock for it.
                    return match database.reload_table(&plan.table_name).await {
                        Ok(()) => ddl_ok(0),
                        Err(e) => ddl_error(&e.to_string()),
                    };
                }
                Ok(None) => {}
            }

            // Anything left — `CHECK`, `EXCLUDE` — is accepted so a migration
            // can proceed, and reported as unenforced rather than as done.
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
            if let Err(e) =
                crate::core::rql::column_default::append(io_schema.clone(), &entry).await
            {
                return ddl_error(&e.to_string());
            }
            return match database.reload_table(&plan.table_name).await {
                Ok(()) => ddl_ok(0),
                Err(e) => ddl_error(&e.to_string()),
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
                    Ok((db_type, _)) if db_type == field.db_type => {
                        match retype_numeric_modifier(
                            &schema,
                            &name,
                            &type_tokens.join(" "),
                            row_count,
                            io_schema.clone(),
                        )
                        .await
                        {
                            Ok(false) => ddl_ok(0),
                            Ok(true) => match database.reload_table(&plan.table_name).await {
                                Ok(()) => ddl_ok(0),
                                Err(e) => ddl_error(&e.to_string()),
                            },
                            Err(e) => ddl_error(&e),
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
                if let Some(flag) = nullable::flag_field(&name, &schema.fields) {
                    let flag_name = flag.name.clone();
                    schema
                        .mark_field_as_deleted(&flag_name, io_schema.clone())
                        .await;
                }
                // Falls through to the reload below.
            } else {
                warn!(
                    "ALTER TABLE {}: ALTER COLUMN {} {} ignored",
                    plan.table_name, name, detail
                );
                return ddl_ok(0);
            }
        }
    }

    match database.reload_table(&plan.table_name).await {
        Ok(()) => ddl_ok(0),
        Err(e) => {
            error!("Failed to reload {} after ALTER: {}", plan.table_name, e);
            ddl_error(&e.to_string())
        }
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

    let (io_schema, declared, column_is_a_document) = {
        let entry = database.tables.get(&statement.table);
        let Some(table) = entry.as_ref().clone() else {
            return ddl_error(&format!("relation '{}' does not exist", statement.table));
        };
        let is_document = table.schema.fields.iter().any(|f| {
            f.name.eq_ignore_ascii_case(&statement.path.column)
                && !f.is_deleted
                && f.db_type == crate::core::db_type::DbType::JSONB
        });
        (
            table.io_schema.clone(),
            table.schema.path_indexes.clone(),
            is_document,
        )
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
    if let Err(e) = path_index::append(io_schema, &entry).await {
        return ddl_error(&e.to_string());
    }
    if let Err(e) = database.reload_table(&statement.table).await {
        return ddl_error(&format!(
            "the index was recorded but the table could not be reopened: {}",
            e
        ));
    }
    ddl_ok(0)
}

/// Whether an index of this name exists anywhere in the database. Index
/// names are database-wide, as in PostgreSQL.
fn index_name_taken(database: &Arc<Database>, name: &str) -> Option<String> {
    for entry in database.tables.iter() {
        let schema = &entry.value().schema;
        if schema.path_indexes.iter().any(|i| i.name.eq_ignore_ascii_case(name))
            || schema
                .secondary_indexes
                .iter()
                .any(|i| i.name.eq_ignore_ascii_case(name))
        {
            return Some(entry.key().clone());
        }
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

    let (io_schema, columns, types) = {
        let entry = database.tables.get(&statement.table);
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
        (table.io_schema.clone(), columns, types)
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
    if let Err(e) = index::decl::append(io_schema, &decl).await {
        return ddl_error(&e.to_string());
    }
    if let Err(e) = database.reload_table(&statement.table).await {
        return ddl_error(&format!(
            "the index was recorded but the table could not be reopened: {}",
            e
        ));
    }
    info!("Created {} index {} on {}", method.name(), name, statement.table);
    ddl_ok(0)
}

/// `DROP INDEX` of an index `CREATE INDEX` declared. `None` when no table
/// declares one by that name, so the caller tries the other kinds.
async fn execute_drop_index(sql: &str, database: &Arc<Database>) -> Option<Vec<u8>> {
    let (name, _if_exists) = crate::core::rql::path_index::parse_drop(sql)?;
    let (table_name, io_schema, decl) = database.tables.iter().find_map(|entry| {
        entry
            .value()
            .schema
            .secondary_indexes
            .iter()
            .find(|i| i.name.eq_ignore_ascii_case(&name))
            .map(|d| (entry.key().clone(), entry.value().io_schema.clone(), d.clone()))
    })?;
    let tombstone = crate::core::index::IndexDecl {
        is_deleted: true,
        ..decl
    };
    if let Err(e) = crate::core::index::decl::append(io_schema, &tombstone).await {
        return Some(ddl_error(&e.to_string()));
    }
    if let Err(e) = database.reload_table(&table_name).await {
        return Some(ddl_error(&format!(
            "the index was dropped but the table could not be reopened: {}",
            e
        )));
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
    let (table_name, io_schema) = database.tables.iter().find_map(|entry| {
        entry
            .value()
            .schema
            .unique_keys
            .iter()
            .any(|k| !k.is_deleted && k.predicate.is_some() && k.name.eq_ignore_ascii_case(&name))
            .then(|| (entry.key().clone(), entry.value().io_schema.clone()))
    })?;
    match crate::core::rql::unique_key::remove(io_schema, &name).await {
        Ok(true) => {}
        Ok(false) => {
            return Some(ddl_error(&format!(
                "index \"{}\" could not be found in the sidecar of '{}'",
                name, table_name
            )));
        }
        Err(e) => return Some(ddl_error(&e.to_string())),
    }
    if let Err(e) = database.reload_table(&table_name).await {
        return Some(ddl_error(&format!(
            "the index was dropped but the table could not be reopened: {}",
            e
        )));
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
    for entry in database.tables.iter() {
        if entry
            .value()
            .schema
            .path_indexes
            .iter()
            .any(|i| i.name.eq_ignore_ascii_case(&name))
        {
            found = Some(entry.key().clone());
            break;
        }
    }

    let Some(table_name) = found else {
        // Not an error when the index is one this engine never stored: an
        // ordinary index is a no-op here, so dropping it is one too.
        return ddl_ok(0);
    };

    let io_schema = {
        let entry = database.tables.get(&table_name);
        let Some(table) = entry.as_ref().clone() else {
            return if if_exists {
                ddl_ok(0)
            } else {
                ddl_error("relation vanished")
            };
        };
        table.io_schema.clone()
    };

    let entry = crate::core::rql::path_index::PathIndex {
        name,
        path: String::new(),
        is_deleted: true,
    };
    if let Err(e) = path_index::append(io_schema, &entry).await {
        return ddl_error(&e.to_string());
    }
    if let Err(e) = database.reload_table(&table_name).await {
        return ddl_error(&format!(
            "the index was dropped but the table could not be reopened: {}",
            e
        ));
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
    let eval_keys = |row: &Row| -> Result<Vec<crate::core::rql::expr::Value>, String> {
        let mut values = Vec::with_capacity(keys.len());
        for (expr, _, _) in keys {
            values.push(crate::core::rql::expr::eval(expr, row, schema)?);
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
                let value = crate::core::rql::expr::eval(expression, row, schema)?;
                // Back to the target's declared scale, as the write path does.
                let value = match schema
                    .iter()
                    .find(|f| f.write_order as u32 == *write_order as u32)
                {
                    Some(target) => crate::core::rql::expr::from_engine_scale(value, target)?,
                    None => value,
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
                    let value = crate::core::rql::expr::coerce_to(
                        crate::core::rql::expr::eval(expr, row, schema)?,
                        db_type,
                    );
                    let (bytes, ty) = match value {
                        crate::core::rql::expr::Value::Null => {
                            (Vec::new(), crate::core::db_type::DbType::NULL)
                        }
                        crate::core::rql::expr::Value::Data { bytes, .. } => {
                            (bytes, db_type.clone())
                        }
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

/// A column that identifies one row, for the statements that need to name rows
/// they have already chosen.
///
/// `FOR UPDATE` and `UPDATE … FROM` both need to write a row back — or lock it —
/// by something that picks it out uniquely. Only an inline `UNIQUE` used to
/// count, so a table whose key is written `PRIMARY KEY` or as a table-level
/// `UNIQUE (id)` — the ordinary way an ORM emits one — was refused as having no
/// unique column at all, though it plainly had one.
fn identifying_field(
    schema: &crate::core::row::schema::TableSchema,
) -> Option<crate::core::row::schema::SchemaField> {
    let live = |name: &str| {
        schema
            .fields
            .iter()
            .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(name))
            .cloned()
    };

    if let Some(field) = schema.fields.iter().find(|f| f.is_unique && !f.is_deleted) {
        return Some(field.clone());
    }
    if let Some(key) = schema.primary_key.as_ref().and_then(|k| live(k)) {
        return Some(key);
    }
    // A composite key names no single column, so it cannot stand in for one.
    // Nor can a partial one: rows outside its predicate share its values, and
    // writing back by it would reach every one of them.
    schema
        .unique_keys
        .iter()
        .filter(|k| !k.is_deleted && k.columns.len() == 1 && k.predicate.is_none())
        .find_map(|k| live(&k.columns[0]))
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
const MAX_CONFLICT_RETRIES: usize = 12;

/// How long to wait before replaying attempt `n` of a conflicted write.
///
/// Doubling, capped per sleep so a late attempt does not stall a request for
/// seconds on its own. The winner is another task that has to be scheduled,
/// so backing off beats spinning.
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

fn conflict_backoff(attempt: usize) -> std::time::Duration {
    std::time::Duration::from_millis((1u64 << attempt.min(8)).min(250))
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

pub async fn execute(query: QueryPurpose, database: Arc<Database>) -> Vec<u8> {
    execute_with_sync(query, database, crate::synchronous_commit()).await
}

/// `execute`, with the caller choosing whether the commit waits for its sync.
///
/// `wait` is the effective `synchronous_commit` — the server default unless the
/// session has set its own. Passing `false` does not skip the outcome record,
/// only the `fdatasync`: the records are in the log either way, and
/// [`crate::core::log_writer`] takes the sync within
/// [`crate::wal_writer_delay_ms`]. A crash in that window loses whole
/// transactions off the end of the log and never part of one.
pub async fn execute_with_sync(
    query: QueryPurpose,
    database: Arc<Database>,
    wait: bool,
) -> Vec<u8> {
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
    let mut pending: Option<QueryPurpose> = Some(query);
    // The first conflict seen, kept so a later attempt that matches nothing can
    // report it rather than a misleading empty success.
    let mut first_conflict: Option<Vec<u8>> = None;
    let mut attempt = 0usize;
    let (tx, response) = loop {
        let tx = database.transactions.begin_autocommit();

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
        let response = {
            use futures::FutureExt;
            let caught = match purpose {
                QueryPurpose::PreparedRows(prepared) => {
                    std::panic::AssertUnwindSafe(Box::pin(execute_prepared_rows(
                        prepared,
                        database.clone(),
                        tx,
                        SubqueryNeed::All,
                    )))
                    .catch_unwind()
                    .await
                }
                purpose => {
                    std::panic::AssertUnwindSafe(Box::pin(execute_with_context(
                        purpose,
                        database.clone(),
                        0,
                        tx,
                    )))
                    .catch_unwind()
                    .await
                }
            };
            match caught {
                Ok(response) => response,
                Err(_) => {
                    error!("statement panicked; its transaction {} is aborted", tx);
                    ddl_error("internal error: the statement panicked and was rolled back")
                }
            }
        };
        let conflicted = is_retryable_conflict(&response);

        if !conflicted || !retryable || attempt >= MAX_CONFLICT_RETRIES {
            // A replay that matched nothing reports the original conflict. The
            // row plainly existed a moment ago, so "no rows" means the retry
            // lost it, not that it is gone, and an error the caller can retry
            // beats a quiet lie.
            if let Some(conflict) = first_conflict.take() {
                if !conflicted && retry_matched_nothing(&response) {
                    let wrote = database.transactions.wrote_family(tx);
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
        database.abort_transaction(tx);
        if attempt_wrote {
            let _ = database.transactions.flush();
        }

        attempt += 1;
        tokio::time::sleep(conflict_backoff(attempt)).await;
    };
    let _ = attempt;

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

    let family = database.commit_transaction(tx);

    // The outcome record goes into the log without a sync of its own, and the
    // sync below covers it along with the rows. They are records in the same
    // log, appended rows-first, and replay stops at the first one it cannot
    // read whole — so a torn tail can lose the outcome but can never keep it
    // while losing the rows. Two syncs of one file, back to back, was what an
    // `INSERT` used to cost.
    if let Err(e) = database.transactions.flush_deferred() {
        error!("Could not record the transaction outcome: {}", e);
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
        if let Err(e) = database.sync_durable() {
            error!("Could not make the written rows durable: {}", e);
            // Back to aborted, which is what a restart would conclude anyway, so
            // the client and the disk agree instead of the client holding a
            // third story.
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
    Box::pin(seed_sequences(database, table_name)).await;
    let table = database.tables.get(table_name)?.value().clone();
    let schema = &table.schema;
    if !schema.json_columns.is_empty() {
        return None;
    }
    let row_data = crate::core::rql::lexer_s1::InsertRowData {
        table_name: table_name.to_string(),
        query: sql.to_string(),
        on_conflict_do_nothing: false,
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
        true,
    )
    .ok()?;
    if let Some(assignments) = assignments.as_deref() {
        check_foreign_keys(database, table_name, assignments, tx)
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
/// This engine cannot block — there is no lock manager — so it re-runs the
/// statement instead, which is equivalent precisely because the statement is
/// only replayed when it had applied nothing (see `CONFLICT_RETRYABLE`).
///
/// A `REPEATABLE READ` block pins its snapshot, so a replay there sees the
/// same world and conflicts again; the bounded attempts then surface the
/// error, which is what that level is supposed to do.
pub async fn execute_in_txn(query: QueryPurpose, database: Arc<Database>, tx: TxId) -> Vec<u8> {
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

    let mut query = query;
    let mut attempt = 0usize;
    loop {
        // See `execute_with_sync`: a prepared `SELECT` skips the box sized for
        // every statement arm.
        let response = match query {
            QueryPurpose::PreparedRows(prepared) => {
                Box::pin(execute_prepared_rows(prepared, database.clone(), tx, SubqueryNeed::All))
                    .await
            }
            query => Box::pin(execute_with_context(query, database.clone(), 0, tx)).await,
        };
        if !retryable || attempt >= MAX_CONFLICT_RETRIES || !is_retryable_conflict(&response) {
            return strip_conflict_marker(response);
        }
        attempt += 1;
        // Long enough for the transaction that won to end; it is another task
        // that has to run, so backing off beats spinning.
        tokio::time::sleep(conflict_backoff(attempt)).await;
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
    let table = database.tables.get(&prepared.table_name).map(|t| t.value().clone());
    let Some(table) = table else {
        return ddl_error("Table not found");
    };
    let mut query_row = prepared.query_row;
    apply_subquery_need(&mut query_row, need);
    execute_query_rows(query_row, table, &database, tx).await
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
            let (parsed, rejection) = crate::core::rql::lexer_ct::create_table_with_reason(&query);
            if let Some(reason) = rejection {
                error!("Cannot create table: {}", reason);
                return ddl_error(&reason);
            }
            if let Err(reason) = parsed.as_ref().map_or(Ok(()), validate_generated) {
                error!("Cannot create table: {}", reason);
                return ddl_error(&reason);
            }
            if let Some(create_table) = parsed {
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
                if database.add_table(create_table).await.is_ok() {
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
            let owned = crate::core::rql::lexer_alter::parse_drop_table(sql)
                .ok()
                .and_then(|(name, _)| {
                    // Copied out so no map guard is held across the scan of
                    // the other tables below.
                    let (fields, defaults) = {
                        let table = database.tables.get(&name)?;
                        let schema = &table.value().schema;
                        (schema.fields.clone(), schema.defaults.clone())
                    };
                    Some(crate::core::rql::sequence::owned_by(
                        &name,
                        &fields,
                        &defaults,
                        &sequences_named_elsewhere(&database, &name),
                    ))
                })
                .unwrap_or_default();
            let response = Box::pin(execute_drop_table(sql, &database)).await;
            if response.first() != Some(&3) {
                // The sequences go with the table. Without this a table dropped
                // and recreated under the same name kept counting from where
                // the old one stopped, so its first row got a key well above 1.
                // This database's only: another database's table of the same
                // name is a different table.
                crate::core::rql::sequence::forget(database.id, &owned);
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
            // Before anything reads a default: a `SERIAL` column's counter is
            // in memory and a reopened database has not yet learned what the
            // table already holds. Returns immediately for a table with no
            // serial column, and after the first insert for one that has.
            Box::pin(seed_sequences(&database, &row_data.table_name)).await;

            // `INSERT INTO t VALUES (…)` names no columns, so the list is
            // supplied from the schema and the statement re-dispatched in the
            // one shape every parser below understands. First, so that the
            // multi-row split and the `SELECT` source see the list too. Boxed
            // for the reason `insert_values_batch` is: this frame is near the
            // worker's stack limit, and the schema copy does not belong in it.
            match Box::pin(supply_insert_column_list(row_data, &database)).await {
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
                match Box::pin(supply_generated_values(row_data, &database)).await {
                    Some(Ok(data)) => Some(QueryPurpose::InsertRow(data)),
                    Some(Err(e)) => return ddl_error(&e),
                    None => None,
                };
            let (row_data, query) = match &generated_rewrite {
                Some(purpose @ QueryPurpose::InsertRow(data)) => (data, purpose),
                _ => (row_data, &query),
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
            let table = {
                let Some(entry) = database.tables.get(table_name) else {
                    error!("Table not found");
                    let mut result = vec![3u8];
                    result.extend_from_slice("Table not found".as_bytes());
                    return result;
                };
                entry.value().clone()
            };

            let schema = &table.schema;
            // Copied out: the table handle is borrowed across the write below,
            // and the hydration needs the list afterwards.
            let json_columns_for_returning = schema.json_columns.clone();

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
            let insert_assignments =
                crate::core::rql::lexer_ir::assignments_of_insert(&row_data.query)
                    .unwrap_or_default();

            // A `NOT NULL` column must actually receive a value. Checked before
            // anything is written, so a rejected row leaves nothing behind.
            if let Err(e) = check_not_null(
                &row_data.table_name,
                &schema.fields,
                &insert_assignments,
                &schema.defaults,
                true,
            ) {
                error!("{}", e);
                return ddl_error(&e);
            }
            let statement = match rewrite_json_values(
                &database,
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
            match crate::core::rql::lexer_ir::assignments_of_insert(&row_data.query) {
                Some(assignments) => {
                    if let Err(e) =
                        check_foreign_keys(&database, table_name, &assignments, tx).await
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
                let returned_row = returning.as_ref().map(|fields| {
                    let mut columns: crate::core::row::row::RowColumns =
                        smallvec::SmallVec::new();
                    for field in fields {
                        let payload = insert_row
                            .columns_writing_data
                            .iter()
                            .find(|c| c.write_order as u64 == field.write_order);
                        // Whether the row stores a NULL here is the
                        // companion's business, not the value's: a NULL
                        // occupies its slot as zeroes (or as nothing at all),
                        // which is indistinguishable from a real zero or an
                        // empty string. Typed as the column's own type it
                        // reached the client as `0` or `""` — a silent wrong
                        // answer on every nullable column — and for an array
                        // it was worse: a zero-length payload where the
                        // decoder reads five `i32` headers panicked the
                        // client outright. `DbType::NULL` is what makes the
                        // wire layer send an actual NULL.
                        let is_null = payload.is_none() || {
                            let flag_name =
                                crate::core::rql::nullable::flag_column_name(&field.name);
                            schema
                                .fields
                                .iter()
                                .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&flag_name))
                                .and_then(|flag| {
                                    insert_row
                                        .columns_writing_data
                                        .iter()
                                        .find(|c| c.write_order as u64 == flag.write_order)
                                })
                                .is_some_and(|c| {
                                    crate::core::rql::nullable::flag_is_set(c.data.into_slice())
                                })
                        };
                        columns.push(crate::core::row::row::Column {
                            schema_id: crate::core::row::row::schema_id_of(field.write_order),
                            data: if is_null {
                                crate::memory_pool::MemoryBlock::default()
                            } else {
                                payload.map(|p| p.data.clone()).unwrap_or_default()
                            },
                            column_type: if is_null {
                                crate::core::db_type::DbType::NULL
                            } else {
                                field.db_type.physical()
                            },
                        });
                    }
                    crate::core::row::row::Row {
                        position: 0,
                        columns,
                        length: 0,
                    }
                });

                let outcome = table.insert_row_in_txn(insert_row, tx).await;
                if outcome.is_ok() {
                    // Recorded here, with the statement as it actually ran: by
                    // this point a JSON rewrite has already happened and its
                    // generated identifiers are literals in the text, so a
                    // replica writes the same values rather than new ones.
                    note_write(&database, tx, depth, &row_data.query, Some(table_name));
                    if let Some(row) = returned_row {
                        // The row was assembled from what was written, so its
                        // JSON columns still hold the reference the rewrite put
                        // there rather than the document the client sent.
                        let mut rows = vec![row];
                        if let Some(fields) = returning.as_ref() {
                            hydrate_json_in_returning(
                                &database,
                                &mut rows,
                                fields,
                                &json_columns_for_returning,
                                tx,
                            )
                            .await;
                        }
                        let mut result = vec![2u8];
                        rows[0].extend_serialized(&mut result);
                        return result;
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
                    // re-run against an existing database depends on.
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
        QueryPurpose::UpdateRow(_) => {
            // `UPDATE t SET … FROM src WHERE …` reads one relation to write
            // another, which the single-relation write path cannot express.
            // Handled by joining the two, reading the values out, and writing
            // them back row by row — see `execute_update_from`.
            if let QueryPurpose::UpdateRow(sql) = &query {
                if crate::core::rql::join::find_keyword(sql, "FROM", 0).is_some() {
                    return Box::pin(execute_update_from(sql, &database, depth, tx)).await;
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
                    let entry = database.tables.get(&name);
                    if let Some(table) = entry.as_ref().clone() {
                        let assignments = crate::core::rql::lexer_ur::assignments_of_update(sql)
                            .unwrap_or_default();
                        if let Err(e) = check_not_null(
                            &name,
                            &table.schema.fields,
                            &assignments,
                            &table.schema.defaults,
                            false,
                        ) {
                            error!("{}", e);
                            return ddl_error(&e);
                        }
                    }
                }
                if let Err(e) = Box::pin(refuse_write_to_locked_rows(
                    &database, &name, &condition, tx,
                ))
                .await
                {
                    error!("{}", e);
                    return ddl_error(&e);
                }
            }

            // Subqueries are folded first, exactly as the SELECT arm does.
            // Without this an `UPDATE … SET c = (SELECT …)` reached the UPDATE
            // parser with the word SELECT in its assignment and came back as
            // "Cannot parse UPDATE query", naming nothing.
            if let QueryPurpose::UpdateRow(sql) = &query {
                if crate::core::rql::subquery::find_in_subquery(sql).is_some()
                    || crate::core::rql::subquery::find_exists(sql).is_some()
                    || crate::core::rql::subquery::find_scalar_subquery(sql).is_some()
                {
                    match substitute_in_subqueries(sql, &database, tx).await {
                        Ok(flattened) => {
                            let Some(purpose) = recognize_query_purpose(&flattened) else {
                                return ddl_error(
                                    "could not parse the UPDATE after subquery substitution",
                                );
                            };
                            return Box::pin(execute_with_context(
                                purpose,
                                database.clone(),
                                depth,
                                tx,
                            ))
                            .await;
                        }
                        Err(e) => return ddl_error(&e),
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
                let Some(entry) = database.tables.get(&table_name) else {
                    error!("Table not found");
                    let mut result = vec![3u8];
                    result.extend_from_slice("Table not found".as_bytes());
                    return result;
                };
                entry.value().clone()
            };

            // A generated column is computed from the row, so an `UPDATE` to
            // one of its inputs recomputes it; see
            // `recompute_generated_in_update`, which adds it to the plan below.
            // Here the statement is checked, and `SET g = DEFAULT` put in a
            // shape the parser reads. The rewrite replaces `query` in place
            // rather than being re-dispatched, as the INSERT arm's does, so
            // the check sees only what the client wrote.
            let generated_rewrite = match generated_update_statement(&query, &table.schema) {
                Ok(rewrite) => rewrite,
                Err(reason) => {
                    error!("{}", reason);
                    return ddl_error(&reason);
                }
            };
            let query = generated_rewrite.as_ref().unwrap_or(&query);

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

            // One snapshot for the statement when its foreign keys need to know
            // which rows it changes: the rows judged and the rows written must
            // be the same rows. `None` — every other UPDATE — leaves the write
            // to take its own, as before.
            let statement_visibility =
                update_needs_statement_snapshot(&database, &table, &table_name, &update_plan)
                    .then(|| table.visibility_for(tx));

            if let Some(assignments) =
                crate::core::rql::lexer_ur::assignments_of_update(&query_sql(&query))
            {
                if let Err(e) = check_foreign_keys_for_update(
                    &database,
                    &table,
                    &table_name,
                    &update_plan,
                    &assignments,
                    statement_visibility.as_ref(),
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
            let key_changes = match statement_visibility.as_ref() {
                None => None,
                Some(visibility) => match keys_about_to_change(
                    &database,
                    &table,
                    &table_name,
                    &update_plan,
                    visibility,
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
            let names: Vec<String> = match &request.table_name {
                Some(name) => {
                    if !database.tables.contains_key(name) {
                        error!("VACUUM: table {} not found", name);
                        let mut result = vec![3u8];
                        result.extend_from_slice(format!("Table {} not found", name).as_bytes());
                        return result;
                    }
                    vec![name.clone()]
                }
                None => database
                    .tables
                    .iter()
                    .map(|entry| entry.key().clone())
                    .collect(),
            };

            let mut reclaimed: u64 = 0;
            // Tables compacted whose space could not be given back.
            let mut untruncated: Vec<(String, &'static str)> = Vec::new();
            for name in names {
                // Resolved and cloned per table rather than held across the
                // await: a DashMap reference kept over one would block anything
                // else touching the map for as long as the vacuum runs.
                let Some(table) = database.tables.get(&name).map(|t| t.value().clone()) else {
                    continue;
                };

                if request.full {
                    match table.compact().await {
                        Ok(report) => {
                            reclaimed = reclaimed
                                .saturating_add(report.rows_bytes_freed)
                                .saturating_add(report.pointer_bytes_freed);
                            if let Some(why) = report.truncation_skipped {
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

            let table_ref = database.tables.get(&table_name);
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

            // Referencing rows are dealt with before the parent goes, so a
            // blocking action can still refuse and a cascade still has the
            // parent's key to match on.
            // One snapshot for the statement when foreign keys point here: the
            // keys whose referencing rows are dealt with and the rows the scan
            // deletes must be the same rows. See `keys_about_to_be_deleted`.
            let statement_visibility = (!referencing_keys(&database, &table_name).is_empty())
                .then(|| table.visibility_for(tx));
            match match statement_visibility.as_ref() {
                Some(visibility) => {
                    keys_about_to_be_deleted(
                        &database,
                        &table,
                        &table_name,
                        &delete_plan.where_clause,
                        &delete_plan.query_row_fetch,
                        visibility,
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
            let table_ref = database.tables.get(table_name);
            let table_clone = table_ref.as_ref().clone();
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

            crate::core::rql::prepared::note_plain(table_name, &query_row);
            Box::pin(execute_query_rows(query_row, table.value().clone(), &database, tx)).await
        }
    }
}

/// A parsed single-table `SELECT`, run: the half of the `QueryRows` arm that
/// reads only the parse, never the statement text. A prepared statement
/// enters here with a parse it cached at its first execution.
pub(crate) async fn execute_query_rows(
    query_row: crate::core::rql::lexer_query::QueryRows,
    table: Arc<crate::core::row::table::Table<crate::core::storage_providers::backend::DbStorageProvider>>,
    database: &Arc<Database>,
    tx: TxId,
) -> Vec<u8> {
    let schema = &table.schema;
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
            match table
                .count_rows_in_txn(
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
            let folded = match table
                .query_row_bytes_in_txn(
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
        match table
            .query_row_in_txn(
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
    let lock_table_name = table.schema.name.clone();
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
        match table
            .query_row_bytes_reporting_in_txn(
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
                                let mut row =
                                    if trim_sort_key || has_nullable || overfetched {
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
                                            SlotValue::RowExpression(expr, scale) => {
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
                                                match crate::core::rql::expr::eval(
                                                    expr,
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
                                    lock,
                                    // Everything the client will see:
                                    // the page plus whatever OFFSET
                                    // discards ahead of it.
                                    written_limit.map(|l| l as usize + offset as usize),
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
