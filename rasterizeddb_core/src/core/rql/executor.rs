use rclite::Arc;

use log::{debug, error, info, warn};

use crate::core::mvcc::transaction::TxId;

use crate::core::{
    database::Database, rql::{
        lexer_query::row_fetch_from_select_query, lexer_s1::QueryPurpose,
        lexer_s1::recognize_query_purpose,
    }
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

    let schema_of = |name: &str| {
        database
            .tables
            .get(name)
            .map(|t| t.schema.fields.clone())
    };

    let plan = match crate::core::rql::join::plan_statement(sql, &schema_of) {
        Ok(p) => p,
        Err(e) => {
            error!("Cannot plan join: {}", e);
            return error(&e);
        }
    };

    let mut scanned: Vec<Vec<crate::core::row::row::Row>> = Vec::with_capacity(plan.relations.len());
    for relation in &plan.relations {
        // Cloned out so the map guard does not span the scan below.
        let Some(table) = database.tables.get(&relation.table).map(|t| t.value().clone()) else {
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

        // No limit is pushed down: it applies to the joined output, and a cap
        // here would drop rows that the join still needs.
        match table
            .query_row_in_txn(
                &relation.predicate,
                &schema.to_vec(),
                query_fetch,
                requested,
                None,
                None,
                tx,
            )
            .await
        {
            Ok(rows) => scanned.push(rows),
            Err(e) => {
                error!("Failed to scan '{}' for join: {}", relation.table, e);
                return error(&e.to_string());
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
        if item.literal.is_some() || item.scalar.is_some() {
            continue;
        }
        let Some(relation) = plan.relations.get(item.rel) else {
            continue;
        };
        let Some(shadow_table) = database
            .tables
            .get(&relation.table)
            .and_then(|t| {
                crate::core::rql::json_map::shadow_for(&item.field.name, &t.schema.json_columns)
                    .map(|s| s.to_string())
            })
        else {
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
            let Some(document) =
                Box::pin(load_json_document(&database, &shadow_table, &reference, 0, tx)).await
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
    // The inner query is executed exactly once, with no outer row in scope. A
    // correlated subquery would reference an outer column, which the inner
    // table's schema does not have, so it fails here by name rather than being
    // silently evaluated against the wrong relation.
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
    let response = Box::pin(execute_with_context(purpose, database.clone(), 0, tx)).await;
    match response.first() {
        Some(2) => {}
        Some(3) => {
            let message = String::from_utf8_lossy(&response[1..]).to_string();
            return Err(format!("subquery failed: {}", message));
        }
        _ => return Err("subquery returned no result set".to_string()),
    }

    crate::core::row::row::vec_into_rows(&response[1..])
        .map_err(|e| format!("subquery result could not be read: {}", e))
}

/// Runs a `SELECT` and returns its rows.
///
/// Shares `run_subquery`'s contract — the statement must be a `SELECT`, and it
/// is executed with no outer row in scope — and exists so materialising a CTE
/// does not need a second way to run a query.
pub(crate) async fn run_query_rows(
    sql: &str,
    database: &Arc<Database>,
    tx: TxId,
) -> Result<Vec<crate::core::row::row::Row>, String> {
    run_subquery(sql, database, tx).await
}

/// Runs a data-modifying statement and returns the rows its `RETURNING` clause
/// produced.
///
/// A `WITH x AS (INSERT … RETURNING id)` runs its body for the effect *and*
/// reads the result, so neither `run_query_rows` (which refuses anything but a
/// SELECT) nor `run_statement` (which discards rows) fits. A statement that
/// succeeds without returning rows yields an empty vector rather than an error.
pub(crate) async fn run_returning_rows(
    sql: &str,
    database: &Arc<Database>,
    tx: TxId,
) -> Result<Vec<crate::core::row::row::Row>, String> {
    let purpose =
        recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse '{}'", sql))?;
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
    let purpose =
        recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse '{}'", sql))?;
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
    let purpose =
        recognize_query_purpose(sql).ok_or_else(|| format!("cannot parse '{}'", sql))?;
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
            if matches!(upper_word.as_str(), "JOIN" | "INNER" | "LEFT" | "RIGHT" | "OUTER" | "ON") {
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

        // An EXISTS correlating on several columns cannot become an `IN`, which
        // carries one column. Its rows are fetched once and expanded into a
        // tuple-wise predicate instead — exact, where a per-column `IN` would
        // match pairs that do not exist.
        // An EXISTS referencing no outer column asks one question of the
        // database, not one per row: run it once and fold the answer into a
        // constant the scan can evaluate.
        if let Some(inner) = subquery::uncorrelated_exists(&clause, &outer) {
            let rows = run_subquery(&inner, database, tx).await?;
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
        let rows = run_subquery(&found.inner_sql, database, tx).await?;
        let literal = subquery::rows_to_scalar(&rows)?;
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
        let rows = run_subquery(&found.inner_sql, database, tx).await?;
        let list = subquery::rows_to_value_list(&rows)?;

        current = format!("{}{}{}", &current[..found.start], list, &current[found.end..]);
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
    tx: TxId
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

        // Phrased as an ordinary query, so the check is filtered and evaluated
        // by the same paths as any other predicate.
        let probe = format!(
            "SELECT {} FROM {} WHERE {} = {} LIMIT 1",
            fk.parent_column, fk.parent_table, fk.parent_column, literal
        );
        let rows = run_subquery(&probe, database, tx).await.map_err(|e| {
            format!("foreign key \"{}\" could not be checked: {}", fk.name, e)
        })?;

        if rows.is_empty() {
            return Err(format!(
                "insert or update on table \"{}\" violates foreign key constraint \"{}\": \
                 key ({})=({}) is not present in table \"{}\"",
                table_name,
                fk.name,
                fk.column,
                literal.trim_matches('\''),
                fk.parent_table
            ));
        }
    }

    Ok(())
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
    let Some(expression) = crate::core::rql::column_default::expression_for(column, &schema.defaults)
    else {
        return "NULL".to_string();
    };
    let db_type = schema
        .fields
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(column))
        .map(|f| f.db_type.clone())
        .unwrap_or(crate::core::db_type::DbType::STRING);
    crate::core::rql::column_default::evaluate(expression, &db_type)
        .unwrap_or_else(|_| "NULL".to_string())
}

/// Applies each referencing table's `ON DELETE` action to the rows about to be
/// removed from `parent_table`.
///
/// `keys` are the referenced values of those rows, as SQL literals.
async fn apply_on_delete(
    database: &Arc<Database>,
    parent_table: &str,
    keys: &[String],
    depth: usize,
    // The transaction of the DELETE that triggered this. A cascade must land in
    // it, or the effect would be visible while its cause was not.
    tx: TxId,
) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    if depth >= MAX_CASCADE_DEPTH {
        return Err(format!(
            "foreign key actions from \"{}\" nested more than {} levels deep, which suggests a \
             cycle",
            parent_table, MAX_CASCADE_DEPTH
        ));
    }

    for (child_table, fk) in referencing_keys(database, parent_table) {
        let list = keys.join(", ");

        if fk.on_delete.blocks() {
            let probe = format!(
                "SELECT {} FROM {} WHERE {} IN ({}) LIMIT 1",
                fk.column, child_table, fk.column, list
            );
            let rows = run_subquery(&probe, database, tx).await.map_err(|e| {
                format!("foreign key \"{}\" could not be checked: {}", fk.name, e)
            })?;
            if !rows.is_empty() {
                return Err(format!(
                    "update or delete on table \"{}\" violates foreign key constraint \"{}\" \
                     on table \"{}\"",
                    parent_table, fk.name, child_table
                ));
            }
            continue;
        }

        let statement = match fk.on_delete {
            crate::core::rql::foreign_key::ReferentialAction::Cascade => {
                format!("DELETE FROM {} WHERE {} IN ({})", child_table, fk.column, list)
            }
            crate::core::rql::foreign_key::ReferentialAction::SetNull => {
                format!(
                    "UPDATE {} SET {} = NULL WHERE {} IN ({})",
                    child_table, fk.column, fk.column, list
                )
            }
            crate::core::rql::foreign_key::ReferentialAction::SetDefault => {
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

    Ok(())
}

/// The referenced values of the rows a `DELETE` is about to remove.
async fn keys_about_to_be_deleted(
    database: &Arc<Database>,
    table_name: &str,
    where_clause: &str,
    tx: TxId
) -> Result<Vec<String>, String> {
    let columns: Vec<String> = referencing_keys(database, table_name)
        .into_iter()
        .map(|(_, fk)| fk.parent_column)
        .collect();
    let Some(column) = columns.first() else {
        return Ok(Vec::new());
    };

    let probe = if where_clause.trim().is_empty() {
        format!("SELECT {} FROM {}", column, table_name)
    } else {
        format!("SELECT {} FROM {} WHERE {}", column, table_name, where_clause)
    };

    let rows = run_subquery(&probe, database, tx).await?;
    let mut keys = Vec::with_capacity(rows.len());
    for row in &rows {
        let Some(cell) = row.columns.first() else { continue };
        let Some(literal) = crate::core::rql::subquery::value_to_literal(
            &cell.column_type,
            cell.data.into_slice(),
        ) else {
            continue;
        };
        keys.push(literal);
    }

    Ok(keys)
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
        old_field_names,
        rows,
    ) = {
        let entry = database.tables.get(table_name);
        let Some(table) = entry.as_ref().clone() else {
            return Err(format!("relation '{}' does not exist", table_name));
        };

        let schema = &table.schema.fields;
        let mut ordered: Vec<crate::core::row::schema::SchemaField> = schema
            .iter()
            .filter(|f| !f.is_deleted)
            .cloned()
            .collect();
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
            .map(|f| CreateColumnData { name: f.name.clone(), data_type: f.db_type.clone() })
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
        let old_field_names: std::collections::HashMap<u64, String> =
            schema.iter().map(|f| (f.write_order, f.name.clone())).collect();

        (
            columns,
            table.schema.foreign_keys.clone(),
            table.schema.defaults.clone(),
            table.schema.json_columns.clone(),
            unique_columns,
            table.schema.unique_keys.clone(),
            table.schema.type_modifiers.clone(),
            // Carried across for the same reason the type modifiers are: a
            // rewrite drops the table, and a generation expression lost here
            // would leave the column present and permanently NULL.
            table.schema.generated.clone(),
            // Carried for the same reason: a rewrite drops the table, and a
            // constraint lost here stops being enforced with nothing said.
            table.schema.checks.clone(),
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
                        .get(&c.schema_id)
                        .map(|name| (name.clone(), c.data.clone(), c.column_type.clone()))
                })
                .collect(),
        );
    }

    let mut columns = existing_columns;
    columns.extend(new_columns);

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

    let entry = database.tables.get(table_name);
    let Some(table) = entry.as_ref().clone() else {
        return Err(format!("relation '{}' vanished during rewrite", table_name));
    };
    let schema = table.schema.fields.clone();

    for values in captured {
        let mut payloads: smallvec::SmallVec<
            [crate::core::row::row::ColumnWritePayload; 32],
        > = smallvec::SmallVec::new();

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
                        crate::core::rql::column_default::evaluate(e, &field.db_type).ok()
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
                                values.iter().any(|(name, _, _)| {
                                    name.eq_ignore_ascii_case(&f.name)
                                })
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
                        let mut mb =
                            crate::memory_pool::MEMORY_POOL.acquire(field.size as usize);
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
            .insert_row(crate::core::row::row::RowWrite { columns_writing_data: payloads })
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
    /// An expression over the row's own columns.
    RowExpression(crate::core::rql::expr::Expr),
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

        let reference =
            Box::pin(store_json_document(database, shadow_table, &document, 0, tx)).await?;
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
            &format!("CREATE TABLE {} ({} UUID NOT NULL)", shadow_table, JSON_KEY_COLUMN),
            tx,
        )
        .await?;
    }

    let reference = new_json_reference();

    match document {
        serde_json::Value::Object(_) => {
            let assignments =
                Box::pin(object_assignments(database, shadow_table, document, depth, tx)).await?;
            write_shadow_row(database, shadow_table, &reference, None, None, assignments, tx)
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
                write_shadow_row(database, shadow_table, &reference, Some(-1), Some("a"), Vec::new(), tx)
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
                        let nested_table = json_map::shadow_table_name(
                            shadow_table,
                            JSON_ELEMENT_REF_COLUMN,
                        );
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
                    // `infer` reports Nested only for a container, and both
                    // container arms are above.
                    return Err("a JSON value of this shape is not supported".to_string());
                }
            };
            write_shadow_row(database, shadow_table, &reference, None, Some("s"), assignments, tx)
                .await?;
        }
    }

    Ok(format!("'{}'", reference))
}

/// A fresh v4 UUID, formatted, to identify one stored document.
fn new_json_reference() -> String {
    let mut id = [0u8; 16];
    for byte in id.iter_mut() {
        *byte = fastrand::u8(..);
    }
    id[6] = (id[6] & 0x0f) | 0x40;
    id[8] = (id[8] & 0x3f) | 0x80;
    crate::core::value_codec::format_uuid(&id)
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
            if items
                .iter()
                .any(|item| !item.is_string())
            {
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
            .map(|t| t.schema.fields.iter().filter(|f| !f.is_deleted).cloned().collect())
            .unwrap_or_default();
        let exact = |name: &str| {
            fields.iter().find(|f| f.name == name).map(|f| f.db_type.clone())
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
            let Some(cell) = row.columns.get(index) else { continue };
            if cell.column_type == crate::core::db_type::DbType::NULL {
                continue;
            }
            let bytes = cell.data.into_slice();

            // Only the column being retyped is converted. Every other value
            // is re-rendered from its own bytes by its own type, so nothing is
            // put through a text round trip that could not survive one — a
            // UUID read as text would come back as replacement characters.
            let literal = if field.name.eq_ignore_ascii_case(column) {
                let Some(value) = json_value_of(&field.db_type, bytes) else { continue };
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
            let nullability = if c.name == JSON_KEY_COLUMN { " NOT NULL" } else { "" };
            format!("{} {}{}", c.name, json_map::sql_type_name(&c.data_type), nullability)
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
            || item.chars().next().is_some_and(|c| c.is_ascii_digit() || c == '-')
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
    let purpose = recognize_query_purpose(&rewritten)
        .ok_or_else(|| "could not parse the rewritten constant INSERT".to_string())?;
    Ok(Box::pin(execute_with_context(purpose, database.clone(), depth, tx)).await)
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
/// `UNION ALL` concatenates; bare `UNION` removes duplicates, compared on the
/// serialised row so two rows are equal exactly when every column is. A
/// trailing `LIMIT` applies to the combined result, as PostgreSQL does.
///
/// Kept out of `execute_with_context`'s own frame: that function is one large
/// match and its future is the sum of every arm's locals.
async fn execute_union(sql: &str, database: &Arc<Database>, tx: TxId) -> Vec<u8> {
    let Some((branches, all, limit)) = crate::core::rql::join::split_union(sql) else {
        return ddl_error("not a UNION");
    };

    let mut rows: Vec<crate::core::row::row::Row> = Vec::new();
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();

    for branch in &branches {
        if branch.trim().is_empty() {
            return ddl_error("a UNION branch is empty");
        }
        let branch_rows = match run_subquery(branch, database, tx).await {
            Ok(rows) => rows,
            Err(e) => {
                error!("UNION branch failed: {}", e);
                return ddl_error(&e);
            }
        };
        for row in branch_rows {
            if !all {
                let mut key = Vec::new();
                row.extend_serialized(&mut key);
                if !seen.insert(key) {
                    continue;
                }
            }
            rows.push(row);
            if limit.is_some_and(|n| rows.len() as u64 >= n) {
                break;
            }
        }
        if limit.is_some_and(|n| rows.len() as u64 >= n) {
            break;
        }
    }

    let mut result = vec![2u8];
    for row in &rows {
        row.extend_serialized(&mut result);
    }
    result
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
        .find(|f| stem.ends_with(&format!("_{}", f.name.to_ascii_lowercase()))
            || stem == f.name.to_ascii_lowercase())
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
    let lhs = value.trim().trim_matches('\'');
    let rhs = literal.trim().trim_matches('\'');

    // Numbers compare numerically; anything else compares as text, which is
    // what an equality against a quoted constant wants.
    let ordering = match (lhs.parse::<f64>(), rhs.parse::<f64>()) {
        (Ok(a), Ok(b)) => a.partial_cmp(&b).ok_or_else(|| {
            format!("cannot order '{}' against '{}'", lhs, rhs)
        })?,
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

    let select = format!(
        "SELECT {}.{} FROM {} {}, {} WHERE {}",
        target_alias, key, table, target_alias, source_clause, condition
    );
    let rows = run_subquery(&select, database, tx).await?;
    if rows.is_empty() {
        let mut result = vec![0u8];
        result.extend_from_slice(&0u64.to_le_bytes());
        return Ok(result);
    }

    let returning_unqualified = returning
        .map(|r| format!("RETURNING {}", strip_qualifier(r.trim(), target_alias)));

    // One delete per DISTINCT key: several source rows may match one target
    // row, and it can only be deleted once.
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
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

        let statement = format!(
            "DELETE FROM {} WHERE {} = {}{}",
            table,
            key,
            literal,
            match &returning_unqualified {
                Some(r) => format!(" {}", r),
                None => String::new(),
            }
        );
        let purpose = recognize_query_purpose(&statement)
            .ok_or_else(|| format!("could not parse the rewritten '{}'", statement))?;
        let response =
            Box::pin(execute_with_context(purpose, database.clone(), depth, tx)).await;
        if response.first() == Some(&3u8) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        if returning.is_some() && response.first() == Some(&2u8) {
            returned.extend_from_slice(&response[1..]);
        }
        deleted += 1;
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
                sources.push(AssignedValue::Computed { text: stripped.clone() });
            }
            None => {
                sources.push(AssignedValue::Slot(projection.len()));
                projection.push(value.clone());
            }
        }
    }

    let carried_at = projection.len();
    projection.extend(carried.iter().cloned());
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
    let returning_unqualified = returning
        .map(|r| format!("RETURNING {}", strip_qualifier(r.trim(), target_alias)));

    let mut written = 0u64;
    let mut returned: Vec<u8> = Vec::new();
    for row in &rows {
        if row.columns.len() < projection.len() {
            return Err(format!(
                "the join returned {} of the {} values the update needs",
                row.columns.len(),
                projection.len()
            ));
        }
        let literal_at = |index: usize| -> Result<String, String> {
            let column = &row.columns[index];
            crate::core::rql::subquery::value_to_literal(
                &column.column_type,
                column.data.into_slice(),
            )
            .ok_or_else(|| "a joined value cannot be written".to_string())
        };

        // Decide the correlated terms for this row before writing anything.
        if !correlated.is_empty() {
            let mut bindings: Vec<(String, String)> = Vec::with_capacity(carried.len());
            for (offset, reference) in carried.iter().enumerate() {
                bindings.push((reference.clone(), literal_at(carried_at + offset)?));
            }
            let mut keep = true;
            for term in &correlated {
                let resolved = substitute_outer_refs(&term.inner_sql, &bindings);
                let produced = run_subquery(&resolved, database, tx).await?;
                let value = crate::core::rql::subquery::rows_to_scalar(&produced)?;
                if !scalar_comparison_holds(&value, &term.op, &term.literal)? {
                    keep = false;
                    break;
                }
            }
            if !keep {
                continue;
            }
        }

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

        let statement = format!(
            "UPDATE {} SET {} WHERE {} = {}{}",
            table,
            sets.join(", "),
            key,
            key_literal,
            match &returning_unqualified {
                Some(r) => format!(" {}", r),
                None => String::new(),
            }
        );
        let purpose = recognize_query_purpose(&statement)
            .ok_or_else(|| format!("could not parse the rewritten '{}'", statement))?;
        let response =
            Box::pin(execute_with_context(purpose, database.clone(), depth, tx)).await;
        if response.first() == Some(&3u8) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        // A `RETURNING` write answers with serialized rows; they are gathered so
        // the statement returns one result set rather than one per row.
        if returning.is_some() && response.first() == Some(&2u8) {
            returned.extend_from_slice(&response[1..]);
        }
        written += 1;
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
    let inserted = crate::core::rql::lexer_ir::assignments_of_insert(insert_sql)
        .unwrap_or_default();
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

    let resolve = |text: &str| -> Result<String, String> {
        let text = substitute_excluded(text, &inserted)?;
        Ok(fold_constants_in_predicate(&strip_qualifier(&text, table_name)))
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
            .find(|c| c.schema_id == key_field.write_order)
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
        let mut outcome = database.row_locks.acquire(
            table_name,
            &key,
            tx,
            lock.strength,
            &live,
        );
        if matches!(lock.behaviour, LockWait::Wait) {
            let deadline = std::time::Instant::now() + LOCK_WAIT_TIMEOUT;
            while let Acquired::Held(_) = outcome {
                if std::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                outcome = database.row_locks.acquire(
                    table_name,
                    &key,
                    tx,
                    lock.strength,
                    &live,
                );
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
                    ))
                }
                LockWait::Wait => {
                    return Err(format!(
                        "timed out waiting {}s for a lock on a row of '{}' held by transaction {}",
                        LOCK_WAIT_TIMEOUT.as_secs(),
                        table_name,
                        owner
                    ))
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

        let mut outcome =
            database.row_locks.acquire_table(name, tx, statement.mode, &live);
        if !statement.nowait {
            let deadline = std::time::Instant::now() + LOCK_WAIT_TIMEOUT;
            while let Acquired::Held(_) = outcome {
                if std::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                outcome =
                    database.row_locks.acquire_table(name, tx, statement.mode, &live);
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
                let filled = crate::core::rql::column_default::expression_for(
                    &field.name,
                    defaults,
                )
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
/// Costs nothing when nobody holds a lock — the common case — because the lock
/// table is consulted first and is empty.
///
/// **Writers do not take locks of their own here.** A row being updated is not
/// thereby locked against a later `FOR UPDATE`; MVCC's own write serialization
/// covers concurrent writers. Extending this to acquire as well as check is
/// the remaining step toward PostgreSQL's behaviour.
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
    if let Some(owner) = database.row_locks.table_conflict(table_name, tx, true, &live) {
        return Err(format!(
            "relation '{}' is locked by transaction {}; the statement that locked it must \
             finish first",
            table_name, owner
        ));
    }

    if database.row_locks.is_empty() {
        return Ok(());
    }

    let (key_field, schema_fields) = {
        let entry = database.tables.get(table_name);
        let Some(table) = entry.as_ref().clone() else {
            return Ok(());
        };
        let Some(field) = table
            .schema
            .fields
            .iter()
            .find(|f| f.is_unique && !f.is_deleted)
            .cloned()
        else {
            // Nothing here can be locked row-by-row, so nothing can conflict.
            return Ok(());
        };
        (field, table.schema.fields.clone())
    };

    let select = format!(
        "SELECT {} FROM {}{}{}",
        key_field.name,
        table_name,
        if where_clause.trim().is_empty() { "" } else { " WHERE " },
        where_clause.trim()
    );
    let rows = run_subquery(&select, database, tx).await?;

    for row in &rows {
        let Some(cell) = row.columns.first() else { continue };
        let key = cell.data.into_slice();
        // Asked for, not merely inspected: this transaction is about to write
        // the row, so it should hold the lock while it does.
        if let Acquired::Held(owner) = database.row_locks.acquire(
            table_name,
            key,
            tx,
            LockStrength::Update,
            &live,
        ) {
            let _ = &schema_fields;
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

    let end = ["GROUP BY", "ORDER BY", "LIMIT", "OFFSET", "RETURNING", "FETCH"]
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
        let end = ["JOIN", "WHERE", "GROUP", "ORDER", "LIMIT", "OFFSET", "RETURNING"]
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
fn substitute_excluded(
    text: &str,
    inserted: &[(String, String)],
) -> Result<String, String> {
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
        let candidates = [lower_rest.find(&bare), lower_rest.find(&quoted.to_ascii_lowercase())];
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
            match constant_select_insert(prefix, select_sql, returning_tail, database, depth, tx)
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

    let Some(purpose) = recognize_query_purpose(&rewritten) else {
        return Some(ddl_error(
            "could not parse the statement rewritten from INSERT … SELECT",
        ));
    };
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
    let (fields, generated) = database
        .tables
        .get(&row_data.table_name)
        .map(|entry| {
            let schema = &entry.value().schema;
            (schema.fields.clone(), schema.generated.clone())
        })?;
    // A generated column takes no value from the client, so it is not one of
    // the positions `INSERT INTO t VALUES (…)` fills — PostgreSQL maps the
    // tuple across the columns that *can* be written. Leaving it in the list
    // shifted every value after it by one.
    let fields: smallvec::SmallVec<[crate::core::row::schema::SchemaField; 20]> = fields
        .into_iter()
        .filter(|f| {
            crate::core::rql::generated::expression_for(&f.name, &generated).is_none()
        })
        .collect();
    let rewritten = crate::core::rql::lexer_ir::supply_column_list(&row_data.query, &fields)?;
    Some(
        recognize_query_purpose(&rewritten)
            .ok_or_else(|| "could not parse the statement after supplying its column list".to_string()),
    )
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
        if !assignments[position].1.trim().eq_ignore_ascii_case("DEFAULT") {
            return Some(Err(format!(
                "cannot insert a non-DEFAULT value into column \"{}\": it is a generated column",
                g.column
            )));
        }
        assignments.remove(position);
    }

    // What each column holds for this row: the literal written, or its default.
    let mut values: Vec<(String, String)> = assignments.clone();
    for field in fields.iter().filter(|f| !f.is_deleted) {
        if values.iter().any(|(name, _)| name.eq_ignore_ascii_case(&field.name)) {
            continue;
        }
        let literal = crate::core::rql::column_default::expression_for(&field.name, &defaults)
            .and_then(|e| crate::core::rql::column_default::evaluate(e, &field.db_type).ok())
            .unwrap_or_else(|| "NULL".to_string());
        values.push((field.name.clone(), literal));
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
        let literal = match crate::core::rql::expr::parse(&substituted, &[]).and_then(|expr| {
            crate::core::rql::expr::eval(
                &expr,
                &crate::core::row::row::Row::default(),
                &[],
            )
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

    let rewritten = match append_insert_values(&row_data.query, &computed) {
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

/// Why an `UPDATE` cannot be run against a table with generated columns, if it
/// cannot.
///
/// `None` when the statement touches no input of any generated column, which is
/// every `UPDATE` on a table that has none and most on a table that has.
fn generated_update_conflict(
    query: &QueryPurpose,
    schema: &crate::core::row::schema::TableSchema,
) -> Option<String> {
    if schema.generated.is_empty() {
        return None;
    }
    let QueryPurpose::UpdateRow(sql) = query else {
        return None;
    };
    let assignments = crate::core::rql::lexer_ur::assignments_of_update(sql)?;

    for g in &schema.generated {
        // Assigning the generated column itself is an error in PostgreSQL too.
        if assignments
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case(&g.column))
        {
            return Some(format!(
                "column \"{}\" can only be updated to DEFAULT: it is a generated column",
                g.column
            ));
        }
        let inputs = expression_columns(&g.expression, &schema.fields);
        for (name, _) in &assignments {
            if inputs.iter().any(|input| input.eq_ignore_ascii_case(name)) {
                return Some(format!(
                    "updating \"{}\" would leave the generated column \"{}\" stale, and                      recomputing it in an UPDATE is not supported yet; rewrite the row with                      DELETE and INSERT",
                    name, g.column
                ));
            }
        }
    }
    None
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
    fn is_word_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'_'
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
            && !out.iter().any(|existing| existing.eq_ignore_ascii_case(word))
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
    fn is_word_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'_'
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
            // A NULL input makes the whole expression NULL, as it does in
            // PostgreSQL — and saying so here beats folding `NULL + 5` into
            // something that looks like a number.
            Some((_, literal)) if literal.trim().eq_ignore_ascii_case("NULL") => {
                return Ok("NULL".to_string());
            }
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
    if let Some(returning) = returning {
        out.push(' ');
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

    let mut inserted = 0u64;
    let mut returned: Vec<u8> = Vec::new();
    let mut any_returning = false;
    for stmt in statements {
        let Some(purpose) = recognize_query_purpose(&stmt) else {
            return Some(ddl_error("could not re-parse a row of the VALUES clause"));
        };
        let response =
            Box::pin(execute_with_context(purpose, database.clone(), depth, tx)).await;
        match response.first() {
            Some(3) => return Some(response),
            Some(2) => {
                // RETURNING: concatenated so the batch reports every row.
                any_returning = true;
                returned.extend_from_slice(&response[1..]);
                inserted += 1;
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
    let purpose = recognize_query_purpose(sql)
        .ok_or_else(|| format!("could not parse: {}", sql))?;
    let response = Box::pin(execute_in_txn(purpose, database.clone(), tx)).await;
    if response.first() == Some(&3) {
        // With the statement, truncated. "Schema mismatch" on a statement the
        // engine built for itself says nothing about which statement, and the
        // one the client sent is not it.
        const SHOWN: usize = 400;
        let excerpt: String = if sql.chars().count() > SHOWN {
            format!("{}… [{} chars]", sql.chars().take(SHOWN).collect::<String>(), sql.chars().count())
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
    let pairs: Vec<(u64, u64)> = schema
        .fields
        .iter()
        .filter(|f| !crate::core::rql::nullable::is_hidden(&f.name))
        .filter_map(|f| {
            crate::core::rql::nullable::flag_field(&f.name, &schema.fields)
                .map(|flag| (f.write_order, flag.write_order))
        })
        .collect();
    if pairs.is_empty() {
        return;
    }
    for row in rows.iter_mut() {
        for (value_order, flag_order) in &pairs {
            let is_null = row
                .columns
                .iter()
                .find(|c| c.schema_id == *flag_order)
                .is_some_and(|c| {
                    crate::core::rql::nullable::flag_is_set(c.data.into_slice())
                });
            if is_null {
                if let Some(c) = row.columns.iter_mut().find(|c| c.schema_id == *value_order) {
                    c.column_type = crate::core::db_type::DbType::NULL;
                }
            }
        }
    }
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
            let Some(field) = schema.fields.iter().find(|f| f.write_order == cell.schema_id)
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
            let Some(document) =
                Box::pin(load_json_document(database, &shadow_table, &reference, 0, tx)).await
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
    tx: TxId
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
        .filter(|(_, f)| {
            f.name.starts_with(JSON_VALUE_PREFIX) && f.name != JSON_ELEMENT_REF_COLUMN
        })
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
            let Some(bytes) = present(row, *index) else { continue };
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
            let Some(bytes) = present(row, idx_at) else { continue };
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

    Box::pin(resolve_nested_references(database, shadow_table, &mut result, depth, tx)).await;
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
                Box::pin(resolve_nested_references(database, shadow_table, entry, depth, tx))
                    .await;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items.iter_mut() {
                Box::pin(resolve_nested_references(database, shadow_table, item, depth, tx))
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
    Some(if was_quoted { raw.to_string() } else { raw.trim().to_string() })
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
            schema_id: payload.write_order as u64,
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

    let mut candidate = crate::core::row::row::Row::default();
    for payload in &plan.updates {
        candidate.columns.push(crate::core::row::row::Column {
            schema_id: payload.write_order as u64,
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

        if let Ok(false) = crate::core::rql::expr::eval_cond(&condition, &candidate, &visible) {
            return Err(format!(
                "new row for relation \"{}\" violates check constraint \"{}\"",
                table_name, constraint.name
            ));
        }
    }
    Ok(())
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
            .find(|c| c.schema_id == flag.write_order)
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
        let Some(expression) = crate::core::rql::column_default::expression_for(
            &field.name,
            &table.schema.defaults,
        ) else {
            continue;
        };
        let Some(name) = crate::core::rql::sequence::parse_nextval(expression) else {
            continue;
        };
        if !crate::core::rql::sequence::is_seeded(&name) {
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
            log::error!("could not read '{}' to seed its sequences: {}", table_name, e);
            return;
        }
    };

    for (name, field) in wanted {
        let last = crate::core::rql::sequence::max_of_column(
            &rows,
            field.write_order,
            &field.db_type,
        )
        .unwrap_or(0);
        crate::core::rql::sequence::seed(&name, last);
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

    let fetch = crate::core::rql::join::fetch_for(&fetch_fields, schema);
    let empty = crate::core::rql::join::fetch_for(&[], schema);
    let rows = table
        .query_row("", &schema.to_vec(), empty, fetch, None, None)
        .await
        .map_err(|e| format!("could not read '{}' to validate the constraint: {}", table_name, e))?;

    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for row in &rows {
        // Length-prefixed, so ('ab', 'c') and ('a', 'bc') are different tuples
        // rather than the same run of bytes.
        let mut encoded: Vec<u8> = Vec::new();
        let mut has_null = false;
        for (index, field) in fields.iter().enumerate() {
            let Some(column) = row.columns.iter().find(|c| c.schema_id == field.write_order) else {
                has_null = true;
                break;
            };
            // NULLS DISTINCT: a tuple containing a NULL conflicts with nothing.
            let flagged_null = flag_of[index].is_some_and(|order| {
                row.columns
                    .iter()
                    .find(|c| c.schema_id == order)
                    .is_some_and(|c| {
                        crate::core::rql::nullable::flag_is_set(c.data.into_slice())
                    })
            });
            let bytes = column.data.into_slice();
            if flagged_null || bytes.is_empty() {
                has_null = true;
                break;
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
                    .find(|c| c.schema_id == field.write_order)
                    .and_then(|c| {
                        crate::core::rql::subquery::value_to_literal(
                            &field.db_type,
                            c.data.into_slice(),
                        )
                    })
                    .unwrap_or_else(|| "?".to_string());
                values.push(rendered);
            }
            return Ok(Some(format!(
                "the table already holds duplicate values for ({}): first duplicate is ({})",
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
        && head.next().is_some_and(|w| w.eq_ignore_ascii_case("EXISTS"));
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
        schema.rename_field(name, &retired, io_schema.clone()).await?;
    }
}

async fn execute_alter_table_action(
    plan: crate::core::rql::lexer_alter::AlterTable,
    database: &Arc<Database>,
    tx: TxId
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
            info!("ALTER TABLE {}: accepted without acting ({})", plan.table_name, reason);
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
                if let Err(e) = crate::core::rql::column_default::evaluate(expression, &db_type) {
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
                if let Err(e) =
                    crate::core::rql::json_map::append(io_schema.clone(), &entry).await
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

            // The column keeps its slot in the row layout and is marked
            // deleted: reclaiming the bytes would move every column after it,
            // which is the same rewrite `ADD COLUMN` needs.
            schema.mark_field_as_deleted(&field.name, io_schema.clone()).await;
            if let Some(flag) = nullable::flag_field(&field.name, &schema.fields) {
                let flag_name = flag.name.clone();
                schema.mark_field_as_deleted(&flag_name, io_schema.clone()).await;
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
                if let Err(e) = schema.rename_field(&old_flag, &new_flag, io_schema.clone()).await {
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
            return match database.rename_table(&plan.table_name, &to).await {
                Ok(()) => ddl_ok(0),
                Err(e) => ddl_error(&format!(
                    "could not rename {} to {}: {}",
                    plan.table_name, to, e
                )),
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
                        schema.set_field_unique(&column, false, io_schema.clone()).await;
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
                if !schema.fields.iter().any(|f| f.name.eq_ignore_ascii_case(&fk.column)) {
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
                    (trimmed["IF NOT EXISTS".len()..].trim_start().to_string(), true)
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
            if !schema.fields.iter().any(|f| f.name.eq_ignore_ascii_case(&name)) {
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
                    Ok((db_type, _)) if db_type == field.db_type => ddl_ok(0),
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
                    schema.mark_field_as_deleted(&flag_name, io_schema.clone()).await;
                }
                // Falls through to the reload below.
            } else {
                warn!("ALTER TABLE {}: ALTER COLUMN {} {} ignored", plan.table_name, name, detail);
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
        (table.io_schema.clone(), table.schema.path_indexes.clone(), is_document)
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
            ddl_error(&format!(
                "'{}' already indexes that path",
                existing.name
            ))
        };
    }
    if declared.iter().any(|i| i.name.eq_ignore_ascii_case(&statement.name)) {
        return if statement.if_not_exists {
            ddl_ok(0)
        } else {
            ddl_error(&format!("an index called '{}' already exists", statement.name))
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
        return ddl_error(&format!("the index was recorded but the table could not be reopened: {}", e));
    }
    ddl_ok(0)
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
            return if if_exists { ddl_ok(0) } else { ddl_error("relation vanished") };
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
        return ddl_error(&format!("the index was dropped but the table could not be reopened: {}", e));
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
        #[cfg(feature = "s3_backup")]
        QueryPurpose::Backup(_) => String::new(),
        #[cfg(feature = "s3_backup")]
        QueryPurpose::BackupError(sql) => sql.clone(),
    }
}

/// Sorts materialised rows by evaluated `ORDER BY` expressions.
///
/// NULL placement follows PostgreSQL: by default a NULL sorts as the largest
/// value — last ascending, first descending — and an explicit `NULLS
/// FIRST`/`LAST` overrides that independently of the direction, which is why
/// the null arm sits outside the `descending` reversal.
fn sort_rows_by_expressions(
    rows: Vec<crate::core::row::row::Row>,
    keys: &[(crate::core::rql::expr::Expr, bool, Option<bool>)],
    schema: &[crate::core::row::schema::SchemaField],
) -> Result<Vec<crate::core::row::row::Row>, String> {
    use std::cmp::Ordering;

    // Every key is evaluated once per row, up front — a comparator must not
    // error, and re-evaluating inside one would be quadratic anyway.
    let mut keyed: Vec<(Vec<crate::core::rql::expr::Value>, crate::core::row::row::Row)> =
        Vec::with_capacity(rows.len());
    for row in rows {
        let mut values = Vec::with_capacity(keys.len());
        for (expr, _, _) in keys {
            values.push(crate::core::rql::expr::eval(expr, &row, schema)?);
        }
        keyed.push((values, row));
    }

    keyed.sort_by(|a, b| {
        for (i, (_, descending, nulls_first)) in keys.iter().enumerate() {
            let av = &a.0[i];
            let bv = &b.0[i];
            let nulls_come_first = nulls_first.unwrap_or(*descending);
            let ord = match (av.is_null(), bv.is_null()) {
                (true, true) => Ordering::Equal,
                (true, false) => {
                    if nulls_come_first { Ordering::Less } else { Ordering::Greater }
                }
                (false, true) => {
                    if nulls_come_first { Ordering::Greater } else { Ordering::Less }
                }
                (false, false) => {
                    // Incomparable values keep their scan order rather than
                    // panicking the comparator.
                    let ord = crate::core::rql::expr::compare(av, bv)
                        .unwrap_or(Ordering::Equal);
                    if *descending { ord.reverse() } else { ord }
                }
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });

    Ok(keyed.into_iter().map(|(_, row)| row).collect())
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
        let Some(shadow_table) =
            crate::core::rql::json_map::shadow_for(&field.name, json_columns)
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
            let rendered = Box::pin(load_json_document(database, shadow_table, &reference, 0, tx))
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

async fn returning_rows_for_write<S: crate::core::storage_providers::traits::StorageIO>(
    table: &crate::core::row::table::Table<S>,
    sql: &str,
    where_clause: &str,
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
            crate::core::rql::lexer_ir::ReturningItem::Computed { expr, .. } => {
                Some(expr.clone())
            }
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
            if let Some(flag) =
                crate::core::rql::nullable::flag_field(&referenced.name, &schema_sv)
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
    let requested = crate::core::rql::join::fetch_for(&fetch_fields, schema);

    let mut predicate_fields: Vec<crate::core::row::schema::SchemaField> = Vec::new();
    for name in crate::core::rql::lexer_query::extract_where_columns(where_clause, schema) {
        if let Some(f) = schema.iter().find(|f| f.name == name) {
            predicate_fields.push(f.clone());
        }
    }
    let query_fetch = crate::core::rql::join::fetch_for(&predicate_fields, schema);

    // Read under the writer's own transaction, not a fresh snapshot.
    //
    // This used to call `query_row`, which reads as `FROZEN_TX`: a snapshot in
    // which every concurrent transaction is running and none is the caller's
    // own. Two things went wrong with that. Inside a block it could not see
    // rows this transaction had already written, so `UPDATE … RETURNING` after
    // an earlier `UPDATE` in the same block reported the *pre-block* values for
    // every column the second statement did not assign. And under concurrency
    // the fresh snapshot is taken later than the statement's own, which widens
    // the window in which a row being replaced is visible through neither its
    // old version nor its new one — the write then applied while `RETURNING`
    // came back empty, and a client that expects its row back saw the statement
    // fail with the value already saved.
    let mut rows = table
        .query_row_in_txn(where_clause, &schema.to_vec(), query_fetch, requested, None, None, tx)
        .await
        .map_err(|e| e.to_string())?;

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
                    let Some(target) = schema
                        .iter()
                        .find(|f| f.write_order as u32 == *write_order)
                    else {
                        continue;
                    };
                    let variable = target.db_type.is_variable_width();
                    let width = if variable { bytes.len() } else { target.size as usize };
                    let mut mb = crate::memory_pool::MEMORY_POOL.acquire(width);
                    let slot = mb.into_slice_mut();
                    slot.fill(0);
                    let n = bytes.len().min(slot.len());
                    slot[..n].copy_from_slice(&bytes[..n]);
                    computed.push((u64::from(*write_order), mb, target.db_type.physical()));
                }
            }

            for column in row.columns.iter_mut() {
                if let Some((_, data, db_type)) =
                    computed.iter().find(|(order, _, _)| *order == column.schema_id)
                {
                    column.data = data.clone();
                    column.column_type = db_type.clone();
                    continue;
                }
                if let Some(update) = updates
                    .iter()
                    .find(|u| u64::from(u.write_order) == column.schema_id)
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
                        &field_name_of(schema, column.schema_id).unwrap_or_default(),
                        &schema_sv,
                    )
                    .and_then(|flag| {
                        updates
                            .iter()
                            .find(|u| u64::from(u.write_order) == flag.write_order)
                            .map(|u| {
                                crate::core::rql::nullable::flag_is_set(u.data.into_slice())
                            })
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
        let mut built: smallvec::SmallVec<[crate::core::row::row::Column; 32]> =
            smallvec::SmallVec::new();
        for (position, item) in items.iter().enumerate() {
            match item {
                crate::core::rql::lexer_ir::ReturningItem::Column(f) => {
                    let cell = row
                        .columns
                        .iter()
                        .find(|c| c.schema_id == f.write_order)
                        .cloned()
                        .map(|mut c| {
                            c.schema_id = position as u64;
                            c
                        });
                    match cell {
                        Some(c) => built.push(c),
                        None => built.push(crate::core::row::row::Column {
                            schema_id: position as u64,
                            data: crate::memory_pool::MemoryBlock::default(),
                            column_type: crate::core::db_type::DbType::NULL,
                        }),
                    }
                }
                crate::core::rql::lexer_ir::ReturningItem::Computed {
                    expr, db_type, ..
                } => {
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
                        schema_id: position as u64,
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
    schema
        .unique_keys
        .iter()
        .filter(|k| !k.is_deleted && k.columns.len() == 1)
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
    let retryable = matches!(
        query,
        QueryPurpose::UpdateRow(_) | QueryPurpose::DeleteRow(_)
    ) && !retry_sql.is_empty();
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
            None => recognize_query_purpose(&retry_sql),
        };
        let Some(purpose) = purpose else {
            break (tx, first_conflict.take().unwrap_or_else(|| ddl_error("write conflict")));
        };

        let response =
            Box::pin(execute_with_context(purpose, database.clone(), 0, tx)).await;
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
            format!("could not commit: the transaction outcome could not be made durable ({})", e)
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
                format!("could not commit: the rows written could not be made durable ({})", e)
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
#[inline]
fn note_write(
    database: &Arc<Database>,
    tx: TxId,
    depth: usize,
    sql: &str,
    table: Option<&str>,
) {
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
    if let Some(trimmed) = text.strip_suffix(crate::core::row::table::Table::<
        crate::core::storage_providers::file_sync::LocalStorageProvider,
    >::CONFLICT_RETRYABLE)
    {
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
    let retryable = matches!(
        query,
        QueryPurpose::UpdateRow(_) | QueryPurpose::DeleteRow(_)
    ) && !retry_sql.is_empty();

    let mut query = query;
    let mut attempt = 0usize;
    loop {
        let response = Box::pin(execute_with_context(query, database.clone(), 0, tx)).await;
        if !retryable || attempt >= MAX_CONFLICT_RETRIES || !is_retryable_conflict(&response) {
            return strip_conflict_marker(response);
        }
        attempt += 1;
        // Long enough for the transaction that won to end; it is another task
        // that has to run, so backing off beats spinning.
        tokio::time::sleep(conflict_backoff(attempt)).await;
        query = match recognize_query_purpose(&retry_sql) {
            Some(q) => q,
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
    match &query {
        // An index over a document path. Recorded in the table's sidecar, which
        // is read when the table is opened.
        QueryPurpose::CreateIndex(sql) => {
            let response = Box::pin(execute_create_path_index(sql, &database)).await;
            if response.first() != Some(&3) {
                note_write(&database, tx, depth, sql, None);
            }
            return response;
        }
        QueryPurpose::DropIndex(sql) => {
            let response = Box::pin(execute_drop_path_index(sql, &database)).await;
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
            let (parsed, rejection) =
                crate::core::rql::lexer_ct::create_table_with_reason(&query);
            if let Some(reason) = rejection {
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
                let create_table = match Box::pin(resolve_json_defaults(
                    &database,
                    create_table,
                    tx,
                ))
                .await
                {
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
            let response = Box::pin(execute_drop_table(sql, &database)).await;
            if response.first() != Some(&3) {
                // The sequences go with the table. Without this a table dropped
                // and recreated under the same name kept counting from where
                // the old one stopped, so its first row got a key well above 1.
                if let Ok((name, _)) = crate::core::rql::lexer_alter::parse_drop_table(sql) {
                    crate::core::rql::sequence::forget_table(&name);
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
                Box::pin(insert_from_select(&row_data.query, &database, depth, tx)).await
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
                    if let Err(e) = check_foreign_keys(&database, table_name, &assignments, tx).await {
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
                    let mut columns: smallvec::SmallVec<[crate::core::row::row::Column; 32]> =
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
                                .find(|f| {
                                    !f.is_deleted
                                        && f.name.eq_ignore_ascii_case(&flag_name)
                                })
                                .and_then(|flag| {
                                    insert_row.columns_writing_data.iter().find(|c| {
                                        c.write_order as u64 == flag.write_order
                                    })
                                })
                                .is_some_and(|c| {
                                    crate::core::rql::nullable::flag_is_set(c.data.into_slice())
                                })
                        };
                        columns.push(crate::core::row::row::Column {
                            schema_id: field.write_order,
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
                    error!("Failed to insert row: {}", message);
                    let mut result = vec![3u8];
                    result.extend_from_slice(message.as_bytes());
                    return result;
                }
            } else {
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
                        let assignments =
                            crate::core::rql::lexer_ur::assignments_of_update(sql)
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
                    &database,
                    &name,
                    &condition,
                    tx,
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
                QueryPurpose::UpdateRow(sql) => sql.split_whitespace().nth(1).unwrap_or("").trim_matches('"').to_string(),
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
            // one of its inputs has to recompute it. This engine's `UPDATE`
            // applies one value per column across the matched set and has no
            // per-row evaluation to hang the recomputation on, so the statement
            // is **refused** rather than allowed to leave a stale number
            // behind.
            //
            // Refusing is the deliberate choice. The alternative — accept the
            // update and let the generated column keep the value it had — is a
            // wrong number that reports success, which is the failure this
            // engine is most often bitten by. A loud limitation can be worked
            // around; a quiet one is found in a report months later.
            if let Some(reason) =
                generated_update_conflict(&query, &table.schema)
            {
                error!("{}", reason);
                return ddl_error(&reason);
            }

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
            let update_plan = match crate::core::rql::lexer_ur::update_row_with_reason(
                &query,
                &schema_sv,
                &table.schema.type_modifiers,
            ) {
                (Some(plan), _) => plan,
                (None, reason) => {
                    let message =
                        reason.unwrap_or_else(|| "Cannot parse UPDATE query".to_string());
                    error!("Cannot parse UPDATE query: {}", message);
                    return ddl_error(&message);
                }
            };

            // A `CHECK` the assignments can be judged against on their own.
            // The values are the ones the plan already encoded, so this reads
            // exactly what the write will store.
            if let Err(e) =
                check_constraints_hold_for_update(&table_name, &table.schema, &update_plan)
            {
                error!("{}", e);
                return ddl_error(&e);
            }

            if let Some(assignments) =
                crate::core::rql::lexer_ur::assignments_of_update(&query_sql(&query))
            {
                if let Err(e) = check_foreign_keys(&database, &table_name, &assignments, tx).await {
                    error!("{}", e);
                    return ddl_error(&e);
                }
            }

            // `RETURNING` on an `UPDATE` reports the rows *after* the change.
            // They are read before the write and the assignments are overlaid
            // on the copies, rather than re-reading afterwards — a second read
            // would use the same `WHERE`, which the update itself may have
            // stopped matching.
            let returning = match returning_rows_for_write(
                &table,
                &query_sql(&query),
                &update_plan.where_clause,
                Some(&update_plan.updates),
                &update_plan.expressions,
                &database,
                tx,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    error!("Cannot resolve RETURNING: {}", e);
                    let mut result = vec![3u8];
                    result.extend_from_slice(e.as_bytes());
                    return result;
                }
            };

            match table
                .update_rows_with_expressions(
                    &update_plan.where_clause,
                    update_plan.query_row_fetch,
                    update_plan.updates,
                    &update_plan.expressions,
                    None,
                    tx,
                )
                .await
            {
                Ok(n) => {
                    info!("Updated {} rows", n);
                    note_write(&database, tx, depth, &query_sql(&query), Some(&table_name));
                    // A write that applied rows must not report none back.
                    //
                    // `RETURNING` is resolved from a read taken before the
                    // write, so that the values it reports are the ones the
                    // statement is about to store even where the assignment
                    // stops the row matching its own `WHERE`. That read runs
                    // against a table other statements are writing, and it can
                    // come back empty for a row this statement then updates:
                    // the version being replaced is already invisible to this
                    // reader while its replacement is not yet in the scan's
                    // extent. Reported as-is that is a success carrying no
                    // rows, which a client that expects its row back reads as
                    // "no rows returned" — with the value in fact saved.
                    //
                    // Re-read once the write has landed. This transaction sees
                    // its own writes, so the row is now there for it. The
                    // overlay is deliberately not applied a second time: the
                    // row already holds the new values, and re-applying an
                    // expression assignment would report `n + 1` twice.
                    let returning = match returning {
                        Some(rows) if n > 0 && rows.is_empty() => {
                            match returning_rows_for_write(
                                &table,
                                &query_sql(&query),
                                &update_plan.where_clause,
                                None,
                                &[],
                                &database,
                                tx,
                            )
                            .await
                            {
                                Ok(Some(again)) if !again.is_empty() => Some(again),
                                _ => {
                                    warn!(
                                        "UPDATE applied {} rows on {} but RETURNING resolved none",
                                        n, table_name
                                    );
                                    Some(rows)
                                }
                            }
                        }
                        other => other,
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
                    error!("Failed to update rows: {}", clean_conflict_text(&e.to_string()));
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
            let delete_plan = match crate::core::rql::lexer_dr::delete_row_with_reason(
                &query,
                &schema_sv,
            ) {
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
            match keys_about_to_be_deleted(&database, &table_name, &delete_plan.where_clause, tx).await
            {
                Ok(keys) => {
                    if let Err(e) =
                        apply_on_delete(&database, &table_name, &keys, depth, tx).await
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

            // `RETURNING` on a `DELETE` reports the rows as they were, so they
            // are read before the delete removes them.
            let returning = match returning_rows_for_write(
                &table,
                &query_sql(&query),
                &delete_plan.where_clause,
                None,
                &[],
                &database,
                tx,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    error!("Cannot resolve RETURNING: {}", e);
                    let mut result = vec![3u8];
                    result.extend_from_slice(e.as_bytes());
                    return result;
                }
            };

            match table
                .delete_rows_in_txn(
                    &delete_plan.where_clause,
                    delete_plan.query_row_fetch,
                    None,
                    tx,
                )
                .await
            {
                Ok(n) => {
                    info!("Deleted {} rows", n);
                    note_write(&database, tx, depth, &query_sql(&query), Some(&table_name));
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
                    error!("Failed to delete rows: {}", clean_conflict_text(&e.to_string()));
                    let mut result = vec![3u8];
                    result.extend_from_slice(e.to_string().as_bytes());
                    return result;
                }
            }
        }
        QueryPurpose::QueryRows(query_rows) => {
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
                            result.extend_from_slice(b"cannot parse the statement after subquery substitution");
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
                Ok(q) => q,
                Err(e) => {
                    error!("Cannot query rows: {}", e);
                    let mut result = vec![3u8];
                    result.extend_from_slice(e.as_bytes());
                    return result;
                }
            };

            // An aggregate query folds the scanned rows into groups, so it
            // takes the row-mode path: the bytes path never materialises a
            // `Vec<Row>` for the fold to read.
            if let Some(plan) = query_row.aggregate.clone() {
                // The fold has to see the `__null_` companions of every column
                // it reads, or a NULL is indistinguishable from the zeros its
                // value column holds. The select path fetches them because the
                // projection needs them; the aggregate fetch asks only for the
                // value columns, so they are added here.
                let aggregate_fetch = {
                    let mut wanted: Vec<crate::core::row::schema::SchemaField> = Vec::new();
                    for c in &query_row.requested_row_fetch.columns_fetching_data {
                        if let Some(f) =
                            schema.fields.iter().find(|f| f.write_order == c.schema_id)
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
                        let grouped =
                            match crate::core::rql::aggregate::fold(&plan, &rows) {
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
            // still skips the decode.
            let may_overfetch = query_row.predicate_needs_extra_columns;
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
                        crate::core::rql::json_map::shadow_for(
                            &field.name,
                            &schema.json_columns,
                        )
                        .map(|t| (c.schema_id, t.to_string()))
                    })
                    .collect()
            };
            let has_json = !json_positions.is_empty();
            // Constants and per-row scalars both fill output positions that no
            // fetched column occupies. Merged into one ordered list so the
            // splice below stays index-correct whichever kind comes first.
            let mut value_slots: Vec<(usize, SlotValue, crate::core::db_type::DbType)> =
                query_row
                    .literal_slots
                    .iter()
                    .map(|(position, bytes, db_type)| {
                        (*position, SlotValue::Constant(bytes.clone()), db_type.clone())
                    })
                    .chain(query_row.scalar_slots.iter().map(|(position, func, db_type)| {
                        (*position, SlotValue::Scalar(*func), db_type.clone())
                    }))
                    .chain(query_row.expr_slots.iter().map(|(position, expr, db_type)| {
                        (*position, SlotValue::RowExpression(expr.clone()), db_type.clone())
                    }))
                    .collect();
            value_slots.sort_by_key(|(position, ..)| *position);
            let slot_schema = schema.fields.to_vec();
            let has_literals = !value_slots.is_empty();

            {
                match table
                    .query_row_bytes_in_txn(
                        &query_row.where_clause,
                        &schema.fields.to_vec(),
                        query_row.query_row_fetch,
                        query_row.requested_row_fetch,
                        // `SKIP LOCKED` filters *after* the scan, so a limit
                        // pushed down would be spent on rows that are then
                        // skipped — two workers claiming from the head of the
                        // same queue would leave the second with nothing, which
                        // is the opposite of what the clause is for. The limit
                        // is applied below instead, once the skipping is done.
                        if skip_locked { None } else { query_row.limit },
                        query_row.order_by,
                        tx,
                    )
                    .await
                {
                    Ok(rows_bytes) => {
                        info!(
                            "Query executed successfully, returning {} bytes",
                            rows_bytes.len()
                        );

                        // `OFFSET` drops rows from the front. The scan already
                        // fetched `limit + offset` of them, so the skip happens
                        // here rather than costing another pass over the table.
                        let rows_bytes = if offset > 0
                            || trim_sort_key
                            || distinct
                            || has_nullable
                            || has_literals
                            || has_json
                            || may_overfetch
                            || row_lock.is_some()
                            || has_windows
                            || has_order_exprs
                        {
                            match crate::core::row::row::vec_into_rows(&rows_bytes) {
                                Ok(rows) => {
                                    // Sorted before anything is trimmed: the
                                    // keys read columns that ride after the
                                    // visible ones, and `column_value` finds
                                    // NULL companions in the same row.
                                    let rows = if has_order_exprs {
                                        // A key reaching into a *shredded*
                                        // JSON column would read a stored
                                        // UUID, evaluate to NULL on every
                                        // row, and quietly not sort at all.
                                        let shadowed = order_exprs.iter().any(|(expr, ..)| {
                                            crate::core::rql::expr::columns_of(expr)
                                                .iter()
                                                .any(|f| {
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
                                        match sort_rows_by_expressions(
                                            rows,
                                            &order_exprs,
                                            &slot_schema,
                                        ) {
                                            Ok(sorted) => sorted,
                                            Err(e) => {
                                                error!("ORDER BY failed: {}", e);
                                                let mut result = vec![3u8];
                                                result.extend_from_slice(e.as_bytes());
                                                return result;
                                            }
                                        }
                                    } else {
                                        rows
                                    };
                                    // Trim first, so `DISTINCT` compares only
                                    // the columns the client asked for and does
                                    // not treat rows as distinct because of a
                                    // sort key it cannot see.
                                    let mut kept: Vec<crate::core::row::row::Row> =
                                        Vec::with_capacity(rows.len());
                                    let mut seen: std::collections::HashSet<Vec<u8>> =
                                        std::collections::HashSet::new();

                                    for row in rows.into_iter() {
                                        // Retyping a `NULL` column as
                                        // `DbType::NULL` is what makes the wire
                                        // layer send an actual NULL: its
                                        // formatters yield no value for that
                                        // type. It must happen before the
                                        // companions are trimmed away.
                                        let mut row = row;
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
                                        let mut row = if trim_sort_key || has_nullable || overfetched
                                        {
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
                                                    .position(|c| c.schema_id == *schema_id)
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
                                                    let mut mb =
                                                        crate::memory_pool::MEMORY_POOL
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
                                            let mut computed: Vec<(usize, Vec<u8>, crate::core::db_type::DbType)> =
                                                Vec::with_capacity(value_slots.len());
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
                                                    SlotValue::RowExpression(expr) => {
                                                        // Same rule as
                                                        // `RETURNING`: the
                                                        // value is re-encoded
                                                        // as the type the row
                                                        // description already
                                                        // announced for this
                                                        // slot.
                                                        match crate::core::rql::expr::eval(
                                                            expr,
                                                            &row,
                                                            &slot_schema,
                                                        )
                                                        .map(|v| {
                                                            crate::core::rql::expr::coerce_to(
                                                                v, db_type,
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
                                                let mut mb =
                                                    crate::memory_pool::MEMORY_POOL.acquire(value.len());
                                                if !value.is_empty() {
                                                    mb.into_slice_mut().copy_from_slice(value);
                                                }
                                                let column = crate::core::row::row::Column {
                                                    schema_id: *position as u64,
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
                                            let output_columns = query_row
                                                .select_items
                                                .len()
                                                .max(value_slots.len());
                                            row.columns.truncate(output_columns);
                                        }
                                        let row = row;

                                        if distinct {
                                            // Compare the serialized value
                                            // bytes, which is the same identity
                                            // the client sees.
                                            let mut key = Vec::new();
                                            for c in &row.columns {
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

                                        kept.push(row);
                                    }

                                    // Numbered over the rows that survived the
                                    // predicate, before any paging: PostgreSQL
                                    // computes a window before `LIMIT`, so
                                    // trimming first would number a page rather
                                    // than the result.
                                    let mut kept = kept;
                                    if has_windows {
                                        for (position, spec) in &window_slots {
                                            let values =
                                                crate::core::rql::window::assign(&kept, spec);
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
                                                let mut mb =
                                                    crate::memory_pool::MEMORY_POOL
                                                        .acquire(bytes.len());
                                                mb.into_slice_mut().copy_from_slice(&bytes);
                                                let at = (*position).min(row.columns.len());
                                                row.columns.insert(
                                                    at,
                                                    crate::core::row::row::Column {
                                                        schema_id: *position as u64,
                                                        data: mb,
                                                        column_type: column_type.clone(),
                                                    },
                                                );
                                            }
                                        }
                                        for row in kept.iter_mut() {
                                            row.columns.truncate(
                                                query_row.select_items.len().max(1),
                                            );
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
                                            written_limit
                                                .map(|l| l as usize + offset as usize),
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

                                    // `DISTINCT` removes rows, so paging has to
                                    // run over what survived rather than over
                                    // the scan's output.
                                    // An expression sort ran over an unbounded
                                    // scan, so the page is cut here too.
                                    let (skip, take) = if distinct || skip_locked || has_order_exprs
                                    {
                                        (offset as usize, written_limit)
                                    } else {
                                        (offset as usize, None)
                                    };

                                    let mut out = Vec::with_capacity(rows_bytes.len());
                                    let mut emitted = 0u64;
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
                                Err(e) => {
                                    error!("Failed to trim the result set: {}", e);
                                    let mut result = vec![3u8];
                                    result.extend_from_slice(e.to_string().as_bytes());
                                    return result;
                                }
                            }
                        } else {
                            rows_bytes
                        };

                        let mut final_vec = Vec::with_capacity(1 + rows_bytes.len());
                        final_vec.push(2u8);
                        final_vec.extend_from_slice(&rows_bytes);
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
        assert_eq!(predicate_of("DELETE FROM t WHERE a = 1 RETURNING id", None), "a = 1");
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
        assert!(!folded.contains("NOW()"), "NOW() should have been folded: {}", folded);
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
        let folded = fold_where_constants(
            "UPDATE t SET a = 1 WHERE lease_until < NOW() OR owner = 'x'",
        );
        assert!(!folded.contains("NOW()"), "{}", folded);
        assert!(folded.contains("owner = 'x'"), "{}", folded);
        assert!(folded.contains(" OR "), "the separator must survive: {}", folded);
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
