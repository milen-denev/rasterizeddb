//! `INNER` and `LEFT JOIN` planning and execution.
//!
//! Joins get their own path end to end, because the single-table plan is built
//! from one schema and one `RowFetch`, and a join needs one of each per
//! relation. Splitting them keeps the single-table path — which is every hot
//! query — untouched.
//!
//! The strategy is a hash join over materialised rows: each relation is scanned
//! once, with whatever part of the `WHERE` clause belongs to it pushed down, and
//! the results are combined by hashing the right-hand side on its join key. The
//! per-relation scan is the ordinary scan, so filter pruning and predicate
//! evaluation still apply exactly as they do without a join.
//!
//! # What is deliberately refused
//!
//! A `WHERE` conjunct that references two relations at once is rejected rather
//! than evaluated after the join. Accepting it would mean re-implementing the
//! predicate evaluator over combined rows, and quietly evaluating it *before*
//! the join — which is what a naive push-down would do — changes the answer.

use std::collections::HashMap;

use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::row::row::{Column, ColumnFetchingData, Row, RowFetch};
use crate::core::row::schema::{SchemaCalculator, SchemaField};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    /// `FROM a, b` and `a CROSS JOIN b`: every left row paired with every right
    /// row. It has no join key, so it is the one kind `combine` handles without
    /// building a hash index.
    Cross,
}

/// One relation in the `FROM` clause, after the first.
#[derive(Debug, Clone)]
pub struct JoinedRelation {
    pub kind: JoinKind,
    /// Index into [`JoinPlan::relations`] of the side this one is joined to.
    pub left_rel: usize,
    /// The equality pairs making up the join key, as canonical column names —
    /// `(column on the left relation, column on this one)`.
    ///
    /// A `Vec` rather than one pair because a composite key is ordinary SQL:
    /// every junction table is joined on both of its columns. Matching on one
    /// and filtering on the other is not the same join — it would fan out on the
    /// first column and then discard, which gives the right rows only when the
    /// first column is already unique.
    pub keys: Vec<(String, String)>,
}

/// A relation participating in the join, with the name it is addressed by.
#[derive(Debug, Clone)]
pub struct Relation {
    pub table: String,
    /// The alias, if one was given; otherwise the table name.
    pub label: String,
    /// How this relation attaches to the ones before it. `None` for the first.
    pub join: Option<JoinedRelation>,
    /// The part of the `WHERE` clause that references only this relation,
    /// already desugared. Empty when there is none.
    pub predicate: String,
    /// Columns this relation must fetch: join keys, selected columns and any
    /// sort key.
    pub fetch_columns: Vec<SchemaField>,
    /// Keep only the combined rows where this relation found **no** match —
    /// the anti-join `LEFT JOIN r ON … WHERE r.key IS NULL`.
    ///
    /// Applied after the join, never as a scan predicate. Pushed down it would
    /// filter the right side to rows whose key is null (none), leaving the
    /// index empty so *every* left row survives as unmatched — which is how
    /// this returned all rows instead of the unmatched ones.
    pub require_unmatched: bool,
}

/// A predicate evaluated over combined rows, after the join.
///
/// The combined tuple is flattened into one synthetic row whose schema holds
/// every fetched column of every relation, named `<label>__<column>` so two
/// relations with a column of the same name stay distinct. The ordinary
/// per-row evaluator then runs against it, so cross-relation predicates mean
/// exactly what they mean in a single-table `WHERE`.
#[derive(Debug, Clone)]
pub struct PostJoinFilter {
    /// The synthetic schema, in write order.
    pub schema: Vec<SchemaField>,
    /// The predicate, rewritten against `schema`.
    pub where_clause: String,
    /// `columns[relation][i]` is the synthetic write order of that relation's
    /// `fetch_columns[i]`.
    pub columns: Vec<Vec<u64>>,
    /// Terms the predicate tokenizer cannot read — a comparison against `||`,
    /// `replace()` or a cast — evaluated by the expression evaluator instead.
    ///
    /// Kept apart rather than folded into `where_clause` because the two are
    /// evaluated by different machinery; both must hold for a row to survive.
    pub expr_terms: Vec<crate::core::rql::expr::Condition>,
}

/// One output column: which relation it comes from, and what it is called.
#[derive(Debug, Clone)]
pub struct JoinSelectItem {
    pub rel: usize,
    pub field: SchemaField,
    pub label: String,
    /// A nullary scalar function — `now()`, `gen_random_uuid()` — occupying this
    /// output position instead of a column. Evaluated per row, so a UUID
    /// differs in every one; `rel` and `field` are placeholders when set.
    pub scalar: Option<crate::core::rql::aggregate::ScalarFunc>,
    /// A constant occupying this output position: `SELECT a.id, 0, NULL FROM …`.
    /// Like `scalar`, it reads no column.
    pub literal: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct JoinPlan {
    pub relations: Vec<Relation>,
    pub items: Vec<JoinSelectItem>,
    /// Sort keys as `(relation, column, descending)`, in the order written.
    /// Later keys only break the ties the earlier ones left.
    pub order_by: Vec<(usize, SchemaField, bool)>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    /// `WHERE` terms that reference more than one relation, evaluated after the
    /// join against the combined row.
    ///
    /// These cannot be pushed into a relation's own scan: that happens *before*
    /// the join, and a predicate spanning two relations has no meaning there —
    /// pushing it would change which rows pair up rather than which pairs
    /// survive.
    pub post_join: Option<PostJoinFilter>,
    /// `SELECT DISTINCT`: identical projected rows collapse to one.
    ///
    /// Applied to the *projected* columns, which is what the select list
    /// produces, and before `OFFSET`/`LIMIT` — SQL evaluates DISTINCT first, so
    /// a `LIMIT 10` over a distinct set means ten distinct rows rather than
    /// however many of the first ten happened to differ.
    pub distinct: bool,
    /// Set when the SELECT list aggregates over the joined rows.
    ///
    /// The join projects first, then this folds. Its `SelectItem`s address the
    /// projected rows positionally, because a joined row has no single schema
    /// for `write_order` to refer to — `items[i]` becomes `write_order == i`.
    /// When present, `order_by`, `limit` and `offset` belong to the fold and are
    /// left unset on the join itself.
    pub aggregate: Option<crate::core::rql::aggregate::AggregatePlan>,
}

impl JoinPlan {
    /// Output column names and types, in select order.
    pub fn output_columns(&self) -> Vec<(String, DbType)> {
        if let Some(agg) = &self.aggregate {
            return agg
                .items
                .iter()
                .map(|i| (i.label().to_string(), i.result_type()))
                .collect();
        }
        self.items
            .iter()
            .map(|i| (i.label.clone(), i.field.db_type.clone()))
            .collect()
    }
}

/// Builds a `RowFetch` for a set of schema fields.
pub fn fetch_for(fields: &[SchemaField], schema: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let calc = SchemaCalculator::default();
    let mut columns_fetching_data = SmallVec::new();
    for field in fields {
        let (offset, schema_id) = calc.calculate_schema_offset(&field.name, schema);
        columns_fetching_data.push(ColumnFetchingData {
            column_offset: offset,
            column_type: field.db_type.physical(),
            size: field.size,
            schema_id,
        });
    }
    RowFetch {
        columns_fetching_data,
    }
}

/// Reads one column's bytes out of a scanned row.
fn column_bytes<'a>(row: &'a Row, write_order: u64) -> Option<&'a [u8]> {
    row.columns
        .iter()
        .find(|c| c.schema_id == write_order)
        .map(|c| c.data.into_slice())
}

/// One combined tuple: the row contributed by each relation, or `None` where a
/// `LEFT JOIN` found no match.
type Combined = Vec<Option<Row>>;

/// Executes the join over already-scanned per-relation rows.
///
/// `scanned[i]` holds the rows of `plan.relations[i]`. Relations must be joined
/// in declaration order, each to one that precedes it, which is what the `FROM`
/// clause guarantees.
/// Executes the join, or says why it could not.
///
/// The error is returned rather than swallowed: a post-join predicate that
/// fails to compile used to produce an empty result, which reads as a query
/// that matched nothing rather than one that never ran.
pub fn combine(plan: &JoinPlan, scanned: &[Vec<Row>]) -> Result<Vec<Combined>, String> {
    if plan.relations.is_empty() || scanned.is_empty() {
        return Ok(Vec::new());
    }

    let mut combined: Vec<Combined> = scanned[0]
        .iter()
        .map(|r| {
            let mut slots: Combined = vec![None; plan.relations.len()];
            slots[0] = Some(r.clone());
            slots
        })
        .collect();

    for (idx, relation) in plan.relations.iter().enumerate().skip(1) {
        let Some(join) = relation.join.as_ref() else {
            continue;
        };

        // A cross join pairs every combined row with every row of this
        // relation. No key, no hash index — just the product.
        if join.kind == JoinKind::Cross {
            let mut next: Vec<Combined> = Vec::with_capacity(combined.len() * scanned[idx].len());
            for tuple in &combined {
                for row in &scanned[idx] {
                    let mut extended = tuple.clone();
                    extended[idx] = Some(row.clone());
                    next.push(extended);
                }
            }
            combined = next;
            continue;
        }

        // Resolve every key column to the write order the scanned rows carry.
        let mut left_wos: Vec<u64> = Vec::with_capacity(join.keys.len());
        let mut right_wos: Vec<u64> = Vec::with_capacity(join.keys.len());
        let mut resolved = !join.keys.is_empty();
        for (left_name, right_name) in &join.keys {
            let l = plan.relations[join.left_rel]
                .fetch_columns
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(left_name) && !f.is_deleted)
                .map(|f| f.write_order);
            let r = relation
                .fetch_columns
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(right_name) && !f.is_deleted)
                .map(|f| f.write_order);
            match (l, r) {
                (Some(l), Some(r)) => {
                    left_wos.push(l);
                    right_wos.push(r);
                }
                _ => {
                    resolved = false;
                    break;
                }
            }
        }

        if !resolved {
            // A join key that was not fetched cannot match anything; an inner
            // join is then empty and a left join keeps its left rows.
            if join.kind == JoinKind::Inner {
                return Ok(Vec::new());
            }
            continue;
        }

        /// Every key column of a row, or `None` if any is absent.
        ///
        /// A `Vec` of slices rather than a concatenation: joining the bytes
        /// would make `("ab", "c")` and `("a", "bc")` the same key, which is a
        /// wrong match rather than a missed one.
        fn key_of<'r>(row: &'r Row, wos: &[u64]) -> Option<Vec<&'r [u8]>> {
            wos.iter().map(|wo| column_bytes(row, *wo)).collect()
        }

        // Hash the right side once, then probe per combined row. Duplicate keys
        // are kept, so a one-to-many join fans out as SQL requires.
        let mut index: HashMap<Vec<&[u8]>, Vec<&Row>> = HashMap::new();
        for row in &scanned[idx] {
            if let Some(key) = key_of(row, &right_wos) {
                index.entry(key).or_default().push(row);
            }
        }

        let mut next: Vec<Combined> = Vec::with_capacity(combined.len());
        for tuple in combined.into_iter() {
            let key = tuple[join.left_rel]
                .as_ref()
                .and_then(|r| key_of(r, &left_wos));

            match key.as_ref().and_then(|k| index.get(k)) {
                Some(matches) => {
                    for m in matches {
                        let mut extended = tuple.clone();
                        extended[idx] = Some((*m).clone());
                        next.push(extended);
                    }
                }
                None => {
                    // An inner join drops the tuple; a left join keeps it with
                    // the right side absent.
                    if join.kind == JoinKind::Left {
                        next.push(tuple);
                    }
                }
            }
        }
        combined = next;
    }

    // Cross-relation `WHERE` terms are answered here, for the same reason: the
    // combined row is the first thing that has columns from both sides.
    if let Some(filter) = plan.post_join.as_ref() {
        match apply_post_join(plan, filter, &combined) {
            Ok(keep) => {
                let mut iter = keep.into_iter();
                combined.retain(|_| iter.next().unwrap_or(false));
            }
            Err(e) => {
                // Returning no rows here made a failed predicate look like a
                // query that simply matched nothing — the caller sees success.
                log::error!("post-join predicate failed: {}", e);
                return Err(e);
            }
        }
    }

    // Anti-joins are applied here, after every relation has been matched: the
    // question "did this relation find a row" only has an answer now.
    if plan.relations.iter().any(|r| r.require_unmatched) {
        combined.retain(|tuple| {
            plan.relations
                .iter()
                .enumerate()
                .all(|(i, r)| !r.require_unmatched || tuple[i].is_none())
        });
    }

    Ok(combined)
}

/// Evaluates the post-join predicate over each combined tuple.
///
/// The tuple is flattened into one synthetic row laid out to `filter.schema`,
/// then run through the ordinary per-row evaluator — the same one a
/// single-table `WHERE` uses, so a cross-relation predicate means exactly what
/// it would mean there. A relation left unmatched by an outer join contributes
/// empty values, which is how a NULL is stored.
fn apply_post_join(
    plan: &JoinPlan,
    filter: &PostJoinFilter,
    combined: &[Combined],
) -> Result<Vec<bool>, String> {
    use crate::core::processor::concurrent_processor::ConcurrentProcessor;
    use crate::core::processor::concurrent_processor::RowView;
    use crate::core::processor::transformer::TransformerProcessor;

    if combined.is_empty() {
        return Ok(Vec::new());
    }

    let schema_sv: SmallVec<[SchemaField; 20]> = filter.schema.iter().cloned().collect();
    let schema_arc = rclite::Arc::new(schema_sv);
    let (no_filter, tokens) =
        ConcurrentProcessor::prepare_query(&filter.where_clause, &schema_arc)?;
    if no_filter && filter.expr_terms.is_empty() {
        return Ok(vec![true; combined.len()]);
    }

    // Only when there is something for it to compile. Every term may have gone
    // to the expression evaluator, leaving an empty clause — and compiling an
    // empty plan fails, which `combine` used to turn into zero rows.
    let mut transformers = SmallVec::new();
    let mut intermediates = SmallVec::new();
    let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediates);
    let compiled;
    if !no_filter {
        compiled = ConcurrentProcessor::compile_query_plan(&schema_arc, &tokens)?;
        processor.load_plan(&compiled);
    }

    let mut out = Vec::with_capacity(combined.len());
    let mut bool_buffer: SmallVec<[(bool, Option<crate::core::processor::transformer::Next>); 20]> =
        SmallVec::new();

    for tuple in combined {
        // One row carrying every fetched column of every relation, in the
        // synthetic write order.
        let mut columns: SmallVec<[Column; 32]> = SmallVec::new();
        for (rel, mapping) in filter.columns.iter().enumerate() {
            for (i, write_order) in mapping.iter().enumerate() {
                let field = &plan.relations[rel].fetch_columns[i];
                let source = tuple.get(rel).and_then(|r| r.as_ref()).and_then(|row| {
                    row.columns
                        .iter()
                        .find(|c| c.schema_id == field.write_order)
                });
                let data = source.map(|c| c.data.clone()).unwrap_or_default();

                // A NULL carries through the join as a NULL. The evaluator
                // reads a cell's *type* to decide that, so the flag the
                // companion holds has to become one here or it is lost: the
                // combined row is rebuilt from declared types and would
                // otherwise present every NULL as a value holding zeroes.
                let is_null = plan.relations[rel]
                    .fetch_columns
                    .iter()
                    .find(|f| {
                        crate::core::rql::nullable::is_hidden(&f.name)
                            && f.name.len() > crate::core::rql::nullable::NULL_FLAG_PREFIX.len()
                            && f.name[crate::core::rql::nullable::NULL_FLAG_PREFIX.len()..]
                                .eq_ignore_ascii_case(&field.name)
                    })
                    .and_then(|flag| {
                        tuple.get(rel).and_then(|r| r.as_ref()).and_then(|row| {
                            row.columns
                                .iter()
                                .find(|c| c.schema_id == flag.write_order)
                                .map(|c| {
                                    crate::core::rql::nullable::flag_is_set(c.data.into_slice())
                                })
                        })
                    })
                    .unwrap_or(false);

                columns.push(Column {
                    schema_id: *write_order,
                    data,
                    column_type: if is_null {
                        crate::core::db_type::DbType::NULL
                    } else {
                        field.db_type.clone()
                    },
                });
            }
        }

        let row = Row {
            position: 0,
            length: 0,
            columns,
        };
        let mut map: SmallVec<[u8; 32]> = SmallVec::new();
        map.resize(filter.schema.len(), u8::MAX);
        for (pos, column) in row.columns.iter().enumerate() {
            let wo = column.schema_id as usize;
            if wo < map.len() {
                map[wo] = pos as u8;
            }
        }

        let mut matched = if no_filter {
            true
        } else {
            processor
                .execute_row(&RowView::new(&row, &map), &mut bool_buffer)
                .map_err(|e| format!("{:?}", e))?
        };

        // Both halves must hold. The synthetic row is addressed by write order,
        // which is exactly how the evaluator looks a column up.
        if matched {
            for condition in &filter.expr_terms {
                if !crate::core::rql::expr::eval_condition(condition, &row, &filter.schema)? {
                    matched = false;
                    break;
                }
            }
        }
        out.push(matched);
    }

    Ok(out)
}

/// Projects combined tuples into output rows, then sorts and pages them.
pub fn project(plan: &JoinPlan, combined: Vec<Combined>) -> Vec<Row> {
    let mut out: Vec<Row> = Vec::with_capacity(combined.len());

    for tuple in &combined {
        let mut columns: SmallVec<[Column; 32]> = SmallVec::new();
        for (i, item) in plan.items.iter().enumerate() {
            let data = if let Some(bytes) = item.literal.as_ref() {
                let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
                if !bytes.is_empty() {
                    mb.into_slice_mut().copy_from_slice(bytes);
                }
                mb
            } else if let Some(func) = item.scalar {
                // Evaluated here, inside the per-row loop, for the same reason
                // the single-table path does: a value computed once would be
                // repeated in every row.
                let bytes = func.evaluate();
                let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
                mb.into_slice_mut().copy_from_slice(&bytes);
                mb
            } else {
                tuple[item.rel]
                    .as_ref()
                    .and_then(|r| {
                        r.columns
                            .iter()
                            .find(|c| c.schema_id == item.field.write_order)
                            .map(|c| c.data.clone())
                    })
                    .unwrap_or_default()
            };

            // A NULL leaves the join as a NULL.
            //
            // The output row was built with each column's *declared* type
            // whatever the value, and a NULL is recorded by a companion column
            // rather than by the bytes — a NULL fixed-width column holds its
            // type's zeroes. So a NULL integer left the join as `0` and a NULL
            // text as `''`: not a formatting slip, a different value, with
            // nothing reporting it. `LEFT JOIN` was the worse case, because the
            // whole unmatched side arrived as zeroes rather than as NULLs, so
            // "no matching row" and "a matching row full of defaults" became
            // indistinguishable.
            //
            // Only the fetched-column branch: a literal and a scalar function
            // both produce a value by construction.
            let is_null = if item.literal.is_some() || item.scalar.is_some() {
                false
            } else {
                match tuple[item.rel].as_ref() {
                    // The relation contributed no row at all — an outer join
                    // that matched nothing. Every column of it is NULL.
                    None => true,
                    Some(r) => plan.relations[item.rel]
                        .fetch_columns
                        .iter()
                        .find(|f| {
                            crate::core::rql::nullable::is_hidden(&f.name)
                                && f.name.len() > crate::core::rql::nullable::NULL_FLAG_PREFIX.len()
                                && f.name[crate::core::rql::nullable::NULL_FLAG_PREFIX.len()..]
                                    .eq_ignore_ascii_case(&item.field.name)
                        })
                        .and_then(|flag| {
                            r.columns
                                .iter()
                                .find(|c| c.schema_id == flag.write_order)
                                .map(|c| {
                                    crate::core::rql::nullable::flag_is_set(c.data.into_slice())
                                })
                        })
                        .unwrap_or(false),
                }
            };

            columns.push(Column {
                // Output columns are positional: a joined row has no single
                // schema for its ids to refer to.
                schema_id: i as u64,
                data,
                column_type: if is_null {
                    DbType::NULL
                } else {
                    item.field.db_type.clone()
                },
            });
        }
        out.push(Row {
            position: 0,
            columns,
            length: 0,
        });
    }

    // Sorting runs here rather than in the scan, because the order is over the
    // combined rows and a per-relation sort would not survive the join.
    //
    // A key addresses a projected column by position, so it is resolved once
    // here rather than per comparison.
    let sort_keys: Vec<(usize, bool, DbType)> = plan
        .order_by
        .iter()
        .filter_map(|(rel, field, descending)| {
            let idx = plan
                .items
                .iter()
                .position(|i| i.rel == *rel && i.field.write_order == field.write_order)?;
            Some((idx, *descending, field.db_type.clone()))
        })
        .collect();

    if !sort_keys.is_empty() {
        out.sort_by(|a, b| {
            for (idx, descending, ty) in sort_keys.iter() {
                let av = a
                    .columns
                    .get(*idx)
                    .map(|c| c.data.into_slice())
                    .unwrap_or(&[]);
                let bv = b
                    .columns
                    .get(*idx)
                    .map(|c| c.data.into_slice())
                    .unwrap_or(&[]);
                let ord = compare_by_type(av, bv, ty);
                let ord = if *descending { ord.reverse() } else { ord };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // `SELECT DISTINCT`, applied to the projected columns and *before*
    // `OFFSET`/`LIMIT`: SQL evaluates it first, so `LIMIT 10` over a distinct
    // set means ten distinct rows, not however many of the first ten differed.
    //
    // Two rows are the same when every projected column holds the same bytes
    // and the same type. The type is part of the key because a NULL arrives as
    // empty bytes typed `NULL`, and an empty string is empty bytes typed
    // `STRING` — telling those apart is what keeps a NULL from collapsing into
    // an empty value. Order is preserved: the first of each group is kept, so
    // an `ORDER BY` that has already run is not disturbed.
    if plan.distinct {
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        out.retain(|row| {
            let mut key: Vec<u8> = Vec::new();
            for column in row.columns.iter() {
                let bytes = column.data.into_slice();
                key.push(column.column_type.to_byte());
                key.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                key.extend_from_slice(bytes);
            }
            seen.insert(key)
        });
    }

    if let Some(off) = plan.offset {
        out = out.into_iter().skip(off as usize).collect();
    }
    if let Some(limit) = plan.limit {
        out.truncate(limit as usize);
    }

    out
}

fn compare_by_type(a: &[u8], b: &[u8], ty: &DbType) -> std::cmp::Ordering {
    if a.is_empty() || b.is_empty() {
        // An absent value from an unmatched LEFT JOIN sorts first, the way SQL
        // orders NULLs under NULLS FIRST.
        return a.len().cmp(&b.len());
    }
    // The same comparator the single-table sort uses, so `ORDER BY` means the
    // same thing with and without a join. Note the evaluator's `Less` panics on
    // STRING, so it cannot be used here.
    crate::core::processor::concurrent_processor::compare_bytes_as_type(ty.clone(), a, b)
}

/// A relation reference as written in the `FROM` clause, before its schema is
/// known.
#[derive(Debug, Clone)]
pub struct RelationRef {
    pub table: String,
    pub label: String,
    pub join: Option<RawJoin>,
}

#[derive(Debug, Clone)]
pub struct RawJoin {
    pub kind: JoinKind,
    /// `ON` terms beyond the join equality, as written.
    ///
    /// Each must reference only the relation being joined, and is applied as a
    /// filter on its scan. That is exact for both join kinds — an `ON` term is
    /// part of what "matches", so a left row whose partner fails it is simply
    /// unmatched and still survives with NULLs. A `WHERE` term is *not* the
    /// same thing and must not be pushed down this way; see `anti_join_column`.
    pub extra: Vec<String>,
    /// `left.column = right.column`, as written, each side qualified.
    pub left_qualified: String,
    pub right_qualified: String,
}

/// Parses the `FROM` clause into its relations and join conditions.
///
/// `text` with its last `n` whitespace-separated words removed.
fn trim_last_words(text: &str, n: usize) -> &str {
    let mut end = text.trim_end().len();
    for _ in 0..n {
        let head = text[..end].trim_end();
        match head.rfind(char::is_whitespace) {
            Some(at) => end = at,
            None => return "",
        }
    }
    text[..end].trim_end()
}

/// Position of the first top-level comma in a `FROM` clause, if any.
fn find_top_level_comma(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    for (i, c) in bytes.iter().enumerate() {
        match quote {
            Some(q) => {
                if *c == q {
                    quote = None;
                }
            }
            None => match *c {
                b'\'' | b'"' => quote = Some(*c),
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                b',' if depth == 0 => return Some(i),
                _ => {}
            },
        }
    }
    None
}

/// Splits a `FROM` clause on top-level commas.
pub(crate) fn split_top_level_commas(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut rest = text;
    while let Some(at) = find_top_level_comma(rest) {
        parts.push(rest[..at].trim().to_string());
        rest = &rest[at + 1..];
    }
    parts.push(rest.trim().to_string());
    parts
}

/// Accepts `FROM a [AS] x JOIN b [AS] y ON x.c = y.d [JOIN …]`, with `INNER`
/// and `LEFT [OUTER]` joins. `CROSS`, `RIGHT` and `FULL` are refused by name so
/// the failure says which join type is missing.
pub fn parse_from_clause(from_clause: &str) -> Result<Vec<RelationRef>, String> {
    let upper = from_clause.to_ascii_uppercase();

    for unsupported in ["RIGHT JOIN", "FULL JOIN", "RIGHT OUTER", "FULL OUTER"] {
        if upper.contains(unsupported) {
            return Err(format!("{} is not supported yet", unsupported));
        }
    }

    // `FROM a, b` is a cross join written the old way. Each top-level comma
    // starts a new relation that pairs with everything before it; the parts
    // themselves may still contain explicit JOINs, so each is parsed normally
    // and only the *first* relation of each part carries the cross.
    if find_top_level_comma(from_clause).is_some() {
        let mut out: Vec<RelationRef> = Vec::new();
        for (index, part) in split_top_level_commas(from_clause).into_iter().enumerate() {
            let mut refs = parse_from_clause(&part)?;
            if index > 0 {
                if let Some(first) = refs.first_mut() {
                    first.join = Some(RawJoin {
                        kind: JoinKind::Cross,
                        extra: Vec::new(),
                        left_qualified: String::new(),
                        right_qualified: String::new(),
                    });
                }
            }
            out.extend(refs);
        }
        return Ok(out);
    }

    // Split on the JOIN keywords, keeping which kind introduced each segment.
    let mut segments: Vec<(Option<JoinKind>, String)> = Vec::new();
    let mut cursor = 0usize;
    let mut pending_kind: Option<JoinKind> = None;

    while cursor < from_clause.len() {
        // Whole-word, so `users u\nJOIN domain d` splits like the single-line
        // form. Matching `" JOIN "` literally left the whole clause as one
        // relation, which then reported "the first relation cannot have an ON
        // clause".
        let next = find_keyword(&from_clause[cursor..], "JOIN", 0);
        match next {
            None => {
                segments.push((pending_kind, from_clause[cursor..].trim().to_string()));
                break;
            }
            Some(rel) => {
                let at = cursor + rel;
                // The words immediately before JOIN say what kind it is, and
                // belong to the keyword rather than to the relation.
                let head = from_clause[cursor..at].trim_end();
                let head_upper = head.to_ascii_uppercase();
                // The trailing word says what kind of join it is; it may be
                // separated by any whitespace, including a newline.
                let last_word = head_upper.split_whitespace().last().unwrap_or("");
                let second_last = {
                    let mut it = head_upper.split_whitespace().rev();
                    it.next();
                    it.next().unwrap_or("")
                };
                let (relation_text, kind) = if last_word == "OUTER" && second_last == "LEFT" {
                    (trim_last_words(head, 2), JoinKind::Left)
                } else if last_word == "LEFT" {
                    (trim_last_words(head, 1), JoinKind::Left)
                } else if last_word == "INNER" {
                    (trim_last_words(head, 1), JoinKind::Inner)
                } else if last_word == "CROSS" {
                    (trim_last_words(head, 1), JoinKind::Cross)
                } else {
                    (head, JoinKind::Inner)
                };

                segments.push((pending_kind, relation_text.trim().to_string()));
                pending_kind = Some(kind);
                cursor = at + "JOIN".len();
            }
        }
    }

    let mut out: Vec<RelationRef> = Vec::new();
    for (kind, segment) in segments {
        let (relation_text, on_text) = match find_keyword(&segment, "ON", 0) {
            Some(i) => (
                &segment[..i],
                Some(segment[i + "ON".len()..].trim().to_string()),
            ),
            None => (segment.as_str(), None),
        };

        let (table, label) = split_relation_alias(relation_text)?;

        let join = match (kind, on_text) {
            (None, None) => None,
            (None, Some(_)) => {
                return Err("the first relation cannot have an ON clause".to_string());
            }
            (Some(JoinKind::Cross), None) => Some(RawJoin {
                kind: JoinKind::Cross,
                extra: Vec::new(),
                left_qualified: String::new(),
                right_qualified: String::new(),
            }),
            (Some(_), None) => {
                return Err(format!("JOIN of '{}' is missing its ON condition", table));
            }
            (Some(kind), Some(on)) => {
                let (l, r, extra) = split_on_condition(&on)?;
                Some(RawJoin {
                    kind,
                    extra,
                    left_qualified: l,
                    right_qualified: r,
                })
            }
        };

        out.push(RelationRef { table, label, join });
    }

    if out.is_empty() {
        return Err("FROM clause names no relation".to_string());
    }
    Ok(out)
}

fn split_relation_alias(text: &str) -> Result<(String, String), String> {
    let mut parts = text.split_whitespace();
    let table = parts
        .next()
        .ok_or_else(|| "FROM clause names no relation".to_string())?
        .trim_matches('"')
        .to_string();

    let alias = match parts.next() {
        None => None,
        Some(w) if w.eq_ignore_ascii_case("AS") => parts.next().map(|a| a.trim_matches('"')),
        Some(w) => Some(w.trim_matches('"')),
    };

    let label = alias.map(str::to_string).unwrap_or_else(|| table.clone());
    Ok((table, label))
}

/// Splits `a.x = b.y` into its two qualified sides.
/// Splits an `ON` clause into the join equality and any extra filter terms.
///
/// `ON a.x = b.y AND b.z = 5` is the shape an ORM emits for a conditional join.
/// The equality between two relations is the hash key; every other term becomes
/// a filter on the relation being joined.
fn split_on_condition(on: &str) -> Result<(String, String, Vec<String>), String> {
    if on.to_ascii_uppercase().contains(" OR ") {
        return Err(format!(
            "join condition '{}' uses OR, which is not supported yet: only terms combined \
             with AND can be applied as a filter on the joined relation",
            on.trim()
        ));
    }

    let mut equality: Option<(String, String)> = None;
    let mut extra: Vec<String> = Vec::new();

    for term in split_conjuncts(on) {
        let term = term.trim().to_string();
        if term.is_empty() {
            continue;
        }
        // The join key is the one equality whose sides are both qualified
        // column references; anything else filters a single relation.
        let is_key = match term.find('=') {
            Some(eq) => {
                let (lq, _) = split_qualified(term[..eq].trim());
                let rhs = term[eq + 1..].trim();
                let (rq, _) = split_qualified(rhs);
                // A *string* literal, not merely a quoted thing. In PostgreSQL
                // single quotes delimit strings and double quotes delimit
                // identifiers, so `"domain"."id"` is a qualified column and
                // treating it as a literal broke every sea-orm join — the ORM
                // quotes every identifier it emits. An unqualified `"Alice"`
                // still fails the `rq.is_some()` test below and lands in
                // `extra`, so dropping the double quote here costs nothing.
                let rhs_is_literal =
                    rhs.starts_with('\'') || rhs.chars().next().is_some_and(|c| c.is_ascii_digit());
                lq.is_some() && rq.is_some() && !rhs_is_literal
            }
            None => false,
        };

        if is_key && equality.is_none() {
            let eq = term.find('=').expect("checked above");
            let left = term[..eq].trim().to_string();
            let right = term[eq + 1..].trim().to_string();
            if left.is_empty() || right.is_empty() {
                return Err(format!("join condition '{}' is incomplete", term));
            }
            equality = Some((left, right));
        } else {
            extra.push(term);
        }
    }

    // No column-to-column equality is not fatal. `ON c.relname = 'x' || f(t.id)`
    // is a perfectly good inner join; it simply has no hash key, and an inner
    // join on a predicate is exactly a cross join filtered by that predicate.
    // The caller decides — for an outer join the two are *not* equivalent, so
    // it refuses there.
    let Some((left, right)) = equality else {
        return Ok((String::new(), String::new(), extra));
    };
    Ok((left, right, extra))
}

/// What a `WHERE` conjunct on the null-supplying side of an outer join means.
enum AntiJoin {
    /// `r.col IS NULL` where `col` cannot be null in a matched row, so the
    /// test is exactly "this relation found no match".
    Unmatched,
    /// Anything else — refused, because evaluating it before the join gives a
    /// different answer and there is no evaluator over combined rows.
    NotApplicable(String),
}

/// Classifies a conjunct that references only relation `rel`, which sits on the
/// null-supplying side of an outer join.
fn anti_join_column(
    conjunct: &str,
    rel: usize,
    relations: &[RelationRef],
    schemas: &[SmallVec<[SchemaField; 20]>],
) -> AntiJoin {
    let trimmed = conjunct.trim();
    let upper = trimmed.to_ascii_uppercase();
    let Some(head) = upper.strip_suffix("IS NULL") else {
        return AntiJoin::NotApplicable(format!(
            "WHERE term '{}' filters '{}', which is the outer side of a LEFT JOIN. Only \
             '<column> IS NULL' is supported there; anything else would be applied before the \
             join and change which rows count as unmatched",
            trimmed, relations[rel].label
        ));
    };
    let column_text = trimmed[..head.trim_end().len()].trim();
    let (_, column) = split_qualified(column_text);

    // Only sound when a matched row cannot itself hold NULL there. A nullable
    // column would make `IS NULL` true for a row that *did* match, which is not
    // the same question.
    match crate::core::rql::nullable::flag_field(&column, &schemas[rel]) {
        Some(_) => AntiJoin::NotApplicable(format!(
            "WHERE term '{}' tests a nullable column on the outer side of a LEFT JOIN, where \
             IS NULL cannot distinguish 'no matching row' from 'matched with a NULL value'",
            trimmed
        )),
        None => AntiJoin::Unmatched,
    }
}

/// True when a term is built only from literals and operators, naming nothing.
///
/// Used to tell `1 = 1` — which is meaningful and constant — from `nmae = 1`,
/// where the identifier is a typo that must still be reported.
fn is_constant_term(term: &str) -> bool {
    // Words that are operators or literals rather than references. A term made
    // only of literals and these is constant: `'' ILIKE '%x%'` is false for
    // every row, exactly as `1 = 0` is.
    //
    // Checked against a list rather than by "does it look like a column",
    // because the point of this function is that a *misspelled* column must not
    // be waved through as a constant — the error it raises is what tells an
    // operator they typed a name wrong.
    const OPERATOR_WORDS: &[&str] = &[
        "and",
        "or",
        "not",
        "is",
        "null",
        "true",
        "false",
        "unknown",
        "like",
        "ilike",
        "similar",
        "to",
        "between",
        "in",
        "escape",
        "collate",
        "distinct",
        "from",
        "contains",
        "icontains",
        "startswith",
        "istartswith",
        "endswith",
        "iendswith",
        "iequals",
        "any",
        "all",
    ];

    let mut quote: Option<char> = None;
    let mut word = String::new();
    let mut ok = true;

    let finish_word = |word: &mut String, ok: &mut bool| {
        if !word.is_empty() {
            // A numeric literal is a value, not a reference —  is the
            // constant this function was originally written for.
            let is_number = word.parse::<f64>().is_ok();
            if !is_number && !OPERATOR_WORDS.iter().any(|k| k.eq_ignore_ascii_case(word)) {
                *ok = false;
            }
            word.clear();
        }
    };

    for c in term.chars() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => {
                if c == '\'' || c == '"' {
                    finish_word(&mut word, &mut ok);
                    quote = Some(c);
                } else if c.is_alphanumeric() || c == '_' {
                    word.push(c);
                } else {
                    finish_word(&mut word, &mut ok);
                }
            }
        }
        if !ok {
            return false;
        }
    }
    finish_word(&mut word, &mut ok);
    ok
}

/// Index of `keyword` as a whole word at or after `start`, ignoring occurrences
/// inside string literals.
///
/// Only matches at **top level**: a keyword inside parentheses belongs to a
/// subquery, so `SELECT x WHERE NOT EXISTS (SELECT 1 FROM t)` has no top-level
/// `FROM` — which is exactly the question a caller asking "does this SELECT name
/// a relation" needs answered.
///
/// Whitespace-agnostic on purpose: a newline separates as well as a space. The
/// literal `" FROM "` searches this replaces silently failed on the multi-line
/// SQL every ORM emits — `…\nFROM users u` — and a scan that finds no `FROM`
/// does not report a problem, it reports *no relations*, which surfaces
/// somewhere else entirely as "EXISTS must correlate…".
pub fn find_keyword(sql: &str, keyword: &str, start: usize) -> Option<usize> {
    let upper = sql.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let kw = keyword.as_bytes();
    let mut quote: Option<u8> = None;
    let mut depth = 0usize;
    let mut i = start;
    while i + kw.len() <= bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => {
                if c == b'\'' || c == b'"' {
                    quote = Some(c);
                } else if c == b'(' {
                    depth += 1;
                } else if c == b')' {
                    depth = depth.saturating_sub(1);
                } else if depth == 0
                    && &bytes[i..i + kw.len()] == kw
                    && (i == 0
                        || bytes[i - 1].is_ascii_whitespace()
                        || bytes[i - 1] == b')'
                        || bytes[i - 1] == b',')
                    && bytes
                        .get(i + kw.len())
                        .is_none_or(|n| n.is_ascii_whitespace() || *n == b'(')
                {
                    return Some(i);
                }
            }
        }
        i += 1;
    }
    None
}

/// Finds a multi-word clause keyword, allowing any whitespace between the
/// words.
///
/// `find_keyword` takes its needle literally, so `"ORDER BY"` only matches when
/// exactly one space separates the two words. An ORM that wraps a line between
/// them would slip past it.
pub fn find_phrase(sql: &str, words: &[&str], start: usize) -> Option<usize> {
    let mut from = start;
    loop {
        let at = find_keyword(sql, words[0], from)?;
        let mut cursor = at + words[0].len();
        let mut matched = true;
        for word in &words[1..] {
            let rest = &sql[cursor..];
            let trimmed = rest.trim_start();
            if trimmed.len() == rest.len() {
                // No whitespace between the words, so this is not the phrase.
                matched = false;
                break;
            }
            if !trimmed
                .get(..word.len())
                .is_some_and(|w| w.eq_ignore_ascii_case(word))
            {
                matched = false;
                break;
            }
            cursor += (rest.len() - trimmed.len()) + word.len();
        }
        if matched {
            return Some(at);
        }
        from = at + words[0].len();
    }
}

/// Splits `SELECT … UNION [ALL] SELECT …` into its branches, with any trailing
/// top-level `LIMIT` lifted off because it applies to the combined result.
///
/// Returns `None` when there is no top-level `UNION`. The second element is
/// `true` for `UNION ALL` (keep duplicates) and `false` for bare `UNION`
/// (distinct), which is the difference PostgreSQL draws.
///
/// Before this existed the parser simply stopped at `UNION` and answered with
/// the first branch alone — a well-formed query silently returning a subset.
pub fn split_union(sql: &str) -> Option<(Vec<String>, bool, Option<u64>)> {
    let mut branches: Vec<String> = Vec::new();
    let mut all = true;
    let mut seen_union = false;
    let mut rest = sql;

    loop {
        let Some(at) = find_keyword(rest, "UNION", 0) else {
            branches.push(rest.trim().to_string());
            break;
        };
        seen_union = true;
        branches.push(rest[..at].trim().to_string());
        let after = rest[at + "UNION".len()..].trim_start();
        // `ALL` keeps duplicates; its absence means distinct.
        rest = match find_keyword(after, "ALL", 0) {
            Some(0) => &after["ALL".len()..],
            _ => {
                all = false;
                after
            }
        };
    }

    if !seen_union {
        return None;
    }

    // A trailing LIMIT belongs to the union, not to its last branch.
    let mut limit = None;
    if let Some(last) = branches.last_mut() {
        if let Some(at) = find_keyword(last, "LIMIT", 0) {
            let tail = last[at + "LIMIT".len()..].trim();
            if let Ok(n) = tail.split_whitespace().next().unwrap_or("").parse::<u64>() {
                limit = Some(n);
                *last = last[..at].trim().to_string();
            }
        }
    }

    Some((branches, all, limit))
}

/// Splits a qualified name into `(qualifier, column)`.
pub fn split_qualified(name: &str) -> (Option<String>, String) {
    let cleaned = name.trim().trim_matches('"');
    match cleaned.rsplit_once('.') {
        Some((q, c)) => (
            Some(q.trim_matches('"').to_string()),
            c.trim_matches('"').to_string(),
        ),
        None => (None, cleaned.to_string()),
    }
}

/// Splits a `WHERE` clause into top-level `AND` conjuncts, respecting quotes and
/// parentheses.
pub fn split_conjuncts(where_clause: &str) -> Vec<String> {
    let bytes = where_clause.as_bytes();
    let upper = where_clause.to_ascii_uppercase();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut i = 0usize;
    let mut quote: Option<u8> = None;

    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
                i += 1;
            }
            None => match c {
                b'\'' | b'"' => {
                    quote = Some(c);
                    i += 1;
                }
                b'(' => {
                    depth += 1;
                    i += 1;
                }
                b')' => {
                    depth = depth.saturating_sub(1);
                    i += 1;
                }
                _ if depth == 0 && upper.as_bytes()[i..].starts_with(b" AND ") && i > start => {
                    out.push(where_clause[start..i].trim().to_string());
                    i += " AND ".len();
                    start = i;
                }
                _ => i += 1,
            },
        }
    }

    let tail = where_clause[start..].trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    out
}

/// Rewrites `pg_column_size(<relation>)` into that relation's stored row width.
///
/// A row here is fixed-width, so the size of one is known from the schema
/// without reading anything. `SUM(pg_column_size(t))` over rows is therefore
/// `COUNT(*) * <width>`, which the aggregate path can answer directly.
///
/// Only the whole-relation form is rewritten. `pg_column_size(<column>)` names
/// a value, and the expression evaluator measures that itself.
fn rewrite_relation_size(
    item: &str,
    relations: &[RelationRef],
    schemas: &[SmallVec<[SchemaField; 20]>],
) -> String {
    const FUNC: &str = "pg_column_size";
    let lower = item.to_ascii_lowercase();
    let Some(at) = lower.find(FUNC) else {
        return item.to_string();
    };
    let after = &item[at + FUNC.len()..];
    let Some(open) = after.find('(') else {
        return item.to_string();
    };
    if !after[..open].trim().is_empty() {
        return item.to_string();
    }
    let Some(close) = after[open..].find(')').map(|c| open + c) else {
        return item.to_string();
    };
    let arg = after[open + 1..close].trim().trim_matches('"');

    // Qualified, so it names a column of a relation rather than the relation.
    if arg.contains('.') {
        return item.to_string();
    }

    let Some(idx) = relations
        .iter()
        .position(|r| r.label.eq_ignore_ascii_case(arg) || r.table.eq_ignore_ascii_case(arg))
    else {
        return item.to_string();
    };

    let width: u64 = schemas[idx]
        .iter()
        .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
        .map(|f| f.size as u64)
        .sum();

    let before = &item[..at];
    let rest = &after[close + 1..];

    let trimmed_before = before.trim_end();
    if trimmed_before.to_ascii_uppercase().ends_with("SUM(") {
        let head = &trimmed_before[..trimmed_before.len() - "SUM(".len()];
        let tail = rest.strip_prefix(')').unwrap_or(rest);
        return format!("{}COUNT(*) * {}{}", head, width, tail);
    }
    format!("{}{}{}", before, width, rest)
}

/// Resolves a parsed `FROM` clause and `SELECT` list against real schemas.
///
/// `schema_of` looks a relation's schema up by table name; the caller supplies
/// it so this module does not depend on `Database`.
pub fn plan(
    relations: Vec<RelationRef>,
    select_list: &str,
    where_clause: &str,
    group_by_clause: Option<&str>,
    having_clause: Option<&str>,
    order_by: Vec<crate::core::rql::order_by::SortTerm>,
    limit: Option<u64>,
    offset: Option<u64>,
    schema_of: &dyn Fn(&str) -> Option<SmallVec<[SchemaField; 20]>>,
) -> Result<JoinPlan, String> {
    let schemas: Vec<SmallVec<[SchemaField; 20]>> = relations
        .iter()
        .map(|r| {
            schema_of(&r.table).ok_or_else(|| format!("relation '{}' does not exist", r.table))
        })
        .collect::<Result<_, _>>()?;

    // `pg_column_size(c)` names a whole *relation*, not a column, and has to be
    // answered from the row layout before anything reads the select list as
    // expressions — the projection and the aggregate stage each parse it
    // separately, and rewriting in only one left the other reporting
    // "Column 'pg_column_size(c)' not found in schema".
    let rewritten_select_list = split_top_level_commas(select_list)
        .into_iter()
        .map(|raw| rewrite_relation_size(raw.trim(), &relations, &schemas))
        .collect::<Vec<_>>()
        .join(", ");
    let select_list = rewritten_select_list.as_str();

    let index_of_label = |label: &str| -> Option<usize> {
        relations.iter().position(|r| {
            r.label.eq_ignore_ascii_case(label) || r.table.eq_ignore_ascii_case(label)
        })
    };

    // Resolves a possibly-qualified column to (relation, field). An unqualified
    // name is only accepted when exactly one relation has it — otherwise the
    // reference is genuinely ambiguous and guessing would silently read the
    // wrong table.
    let resolve = |name: &str| -> Result<(usize, SchemaField), String> {
        let (qualifier, column) = split_qualified(name);
        match qualifier {
            Some(q) => {
                let idx = index_of_label(&q)
                    .ok_or_else(|| format!("unknown relation '{}' in '{}'", q, name))?;
                let field = schemas[idx]
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(&column) && !f.is_deleted)
                    .ok_or_else(|| {
                        format!(
                            "column '{}' not found in relation '{}'",
                            column, relations[idx].label
                        )
                    })?;
                Ok((idx, field.clone()))
            }
            None => {
                let mut found: Option<(usize, SchemaField)> = None;
                for (i, s) in schemas.iter().enumerate() {
                    if let Some(f) = s
                        .iter()
                        .find(|f| f.name.eq_ignore_ascii_case(&column) && !f.is_deleted)
                    {
                        if found.is_some() {
                            return Err(format!(
                                "column '{}' is ambiguous; qualify it with a relation name",
                                column
                            ));
                        }
                        found = Some((i, f.clone()));
                    }
                }
                found.ok_or_else(|| format!("Column '{}' not found in schema", name))
            }
        }
    };

    // --- select list ---------------------------------------------------
    // Function calls are collected separately: they name columns to fetch, but
    // the output column they produce belongs to the fold, not to the join.
    let mut aggregate_calls: Vec<String> = Vec::new();
    let mut items: Vec<JoinSelectItem> = Vec::new();
    if select_list.trim() == "*" {
        for (i, s) in schemas.iter().enumerate() {
            let mut fields: Vec<&SchemaField> = s.iter().collect();
            fields.sort_by_key(|f| f.write_order);
            for f in fields {
                items.push(JoinSelectItem {
                    rel: i,
                    field: f.clone(),
                    label: f.name.clone(),
                    scalar: None,
                    literal: None,
                });
            }
        }
    } else {
        // Split on commas the select list actually separates items with, not on
        // every comma. `COALESCE(a, NOW())` is one item; splitting inside it
        // produced the fragment `NOW())`, which came back as the baffling
        // "NOW() takes no arguments" from a statement that never called NOW
        // with an argument.
        for raw in split_top_level_commas(select_list) {
            let item = raw.trim();
            if item.is_empty() {
                continue;
            }
            if item.contains('(') {
                // A nullary scalar function is not an aggregate: it adds a
                // column without grouping anything, so it must not push the
                // statement onto the aggregate path — which would then demand
                // every other column appear in GROUP BY.
                let (expr, alias) = split_expr_alias(item);
                if let Some(open) = expr.find('(') {
                    let name = expr[..open].trim();
                    if let Some((func, db_type)) =
                        crate::core::rql::aggregate::ScalarFunc::parse(name)
                    {
                        let close = expr
                            .rfind(')')
                            .ok_or_else(|| format!("unterminated function call in '{}'", item))?;
                        if !expr[open + 1..close].trim().is_empty() {
                            return Err(format!("{}() takes no arguments", name));
                        }
                        items.push(JoinSelectItem {
                            rel: 0,
                            field: SchemaField::new(
                                name.to_ascii_lowercase(),
                                db_type.clone(),
                                db_type.get_size(),
                                0,
                                0,
                                false,
                            ),
                            label: alias.unwrap_or_else(|| name.to_ascii_lowercase()),
                            scalar: Some(func),
                            literal: None,
                        });
                        continue;
                    }
                }
                // Handled by the aggregate branch below; a non-aggregate call
                // is still refused there by name.
                aggregate_calls.push(item.to_string());
                continue;
            }
            // A constant reads no column: `SELECT a.id, 0, NULL FROM …`.
            // Resolving it as a column reported "Column '0' not found".
            if let Ok(parsed) = crate::core::rql::expr::parse(item, &[]) {
                let (bytes, db_type) = match parsed {
                    crate::core::rql::expr::Expr::Literal { bytes, db_type } => (bytes, db_type),
                    crate::core::rql::expr::Expr::Null => (Vec::new(), DbType::STRING),
                    _ => (Vec::new(), DbType::STRING),
                };
                items.push(JoinSelectItem {
                    rel: 0,
                    field: SchemaField::new(
                        "?column?".to_string(),
                        db_type.clone(),
                        db_type.get_size(),
                        0,
                        0,
                        false,
                    ),
                    label: "?column?".to_string(),
                    scalar: None,
                    literal: Some(bytes),
                });
                continue;
            }

            let mut parts = item.split_whitespace();
            let head = parts.next().unwrap_or(item);
            let alias = match parts.next() {
                Some(w) if w.eq_ignore_ascii_case("AS") => parts.next(),
                Some(w) => Some(w),
                None => None,
            };
            let (rel, field) = resolve(head)?;
            let label = alias
                .map(|a| a.trim_matches('"').to_string())
                .unwrap_or_else(|| field.name.clone());
            items.push(JoinSelectItem {
                rel,
                field,
                label,
                scalar: None,
                literal: None,
            });
        }
    }
    if items.is_empty() && aggregate_calls.is_empty() {
        return Err("SELECT list is empty".to_string());
    }

    // --- join conditions -----------------------------------------------
    let mut joins: Vec<Option<JoinedRelation>> = vec![None; relations.len()];
    let mut on_extra: Vec<Vec<String>> = vec![Vec::new(); relations.len()];
    // `ON` conditions with no hash key, which become post-join predicates.
    let mut expression_joins: Vec<String> = Vec::new();
    for (i, r) in relations.iter().enumerate() {
        let Some(raw) = r.join.as_ref() else { continue };

        // A cross join has no key: every row of this relation pairs with every
        // combined row so far, so there is nothing to resolve.
        if raw.kind == JoinKind::Cross {
            joins[i] = Some(JoinedRelation {
                kind: JoinKind::Cross,
                left_rel: i.saturating_sub(1),
                keys: Vec::new(),
            });
            continue;
        }

        // An `ON` with no column-to-column equality has no hash key. For an
        // inner join that is still a valid join — `A JOIN B ON p` is exactly
        // `A CROSS JOIN B WHERE p` — so it becomes a cross join and the whole
        // condition is evaluated after pairing. For an outer join the two are
        // *not* equivalent: a left row with no match must survive with NULLs,
        // and a cross join followed by a filter would drop it. So that is
        // refused rather than answered wrongly.
        if raw.left_qualified.is_empty() && raw.right_qualified.is_empty() {
            if raw.kind != JoinKind::Inner {
                return Err(format!(
                    "an outer join on '{}' needs an equality between two columns: without \
                     one the unmatched rows it must keep cannot be identified",
                    raw.extra.join(" AND ").trim()
                ));
            }
            joins[i] = Some(JoinedRelation {
                kind: JoinKind::Cross,
                left_rel: i.saturating_sub(1),
                keys: Vec::new(),
            });
            expression_joins.extend(raw.extra.iter().cloned());
            continue;
        }

        let (a_rel, a_field) = resolve(&raw.left_qualified)?;
        let (b_rel, b_field) = resolve(&raw.right_qualified)?;

        // Whichever side names this relation is its own key; the other side
        // must name a relation already joined, so the plan can be built left to
        // right.
        let (left_rel, left_field, right_field) = if b_rel == i {
            (a_rel, a_field, b_field)
        } else if a_rel == i {
            (b_rel, b_field, a_field)
        } else {
            return Err(format!(
                "the ON condition for '{}' does not reference it",
                r.label
            ));
        };

        if left_rel >= i {
            return Err(format!(
                "'{}' is joined to a relation that appears after it",
                r.label
            ));
        }

        let mut keys = vec![(left_field.name.clone(), right_field.name.clone())];

        // Extra `ON` terms filter the relation being joined. Unlike a `WHERE`
        // term this is exact on the null-supplying side too: an `ON` term
        // decides what counts as a *match*, so a left row whose only candidate
        // fails it is unmatched and still survives with NULLs — which is
        // precisely what filtering the right scan produces.
        for term in &raw.extra {
            let referenced = relations_referenced(term, &relations, &schemas);
            match referenced.as_slice() {
                [only] if *only == i => on_extra[i].push(term.clone()),
                [] => {
                    return Err(format!(
                        "ON term '{}' references no column of '{}'",
                        term.trim(),
                        r.label
                    ));
                }
                // A term spanning this relation and the one it joins to is a
                // further *equality* of the join key, not a filter — every
                // junction table is joined on both its columns. Matching on one
                // and filtering on the other is a different join: it fans out on
                // the first and then discards, which agrees with SQL only when
                // the first column is already unique.
                //
                // Anything else spanning two relations still has to be
                // evaluated after the join, which this engine does not do.
                [a, b] if (*a == i && *b == left_rel) || (*a == left_rel && *b == i) => {
                    let malformed = || {
                        format!(
                            "ON term '{}' relates '{}' to '{}' but is not an equality between \
                             two columns, so it cannot be part of the join key and would have \
                             to be evaluated after the join",
                            term.trim(),
                            r.label,
                            relations[left_rel].label
                        )
                    };
                    let eq = term.find('=').ok_or_else(malformed)?;
                    // Not `!=`, `>=` or `<=` wearing an `=`.
                    let lhs = term[..eq].trim();
                    if lhs.ends_with(['!', '>', '<']) || term[eq + 1..].starts_with('=') {
                        return Err(malformed());
                    }
                    let (p_rel, p_field) = resolve(lhs)?;
                    let (q_rel, q_field) = resolve(term[eq + 1..].trim())?;
                    // Order the pair the same way as the primary key: left
                    // relation first, this one second.
                    if p_rel == left_rel && q_rel == i {
                        keys.push((p_field.name.clone(), q_field.name.clone()));
                    } else if q_rel == left_rel && p_rel == i {
                        keys.push((q_field.name.clone(), p_field.name.clone()));
                    } else {
                        return Err(malformed());
                    }
                }
                _ => {
                    return Err(format!(
                        "ON term '{}' spans more than the relation being joined, which is not \
                         supported yet: it would have to be evaluated after the join",
                        term.trim()
                    ));
                }
            }
        }

        joins[i] = Some(JoinedRelation {
            kind: raw.kind,
            left_rel,
            keys,
        });
    }

    // --- WHERE, split per relation --------------------------------------
    // Seeded with the extra `ON` terms, which are filters on their own relation
    // and are safe on the null-supplying side where a `WHERE` term is not.
    let mut predicates: Vec<Vec<String>> = on_extra;
    let mut anti_join: Vec<bool> = vec![false; relations.len()];
    let mut cross_relation: Vec<String> = expression_joins;
    for conjunct in split_conjuncts(where_clause) {
        if conjunct.trim().is_empty() {
            continue;
        }
        let referenced = relations_referenced(&conjunct, &relations, &schemas);
        match referenced.len() {
            1 => {
                let rel = referenced[0];
                // A relation on the null-supplying side of an outer join must
                // not have its `WHERE` terms pushed into its own scan: the scan
                // happens *before* the join, so filtering it changes which left
                // rows count as unmatched. The module already refuses
                // two-relation conjuncts for exactly this reason; the same
                // argument applies here and was simply not carried over.
                let null_supplying = joins[rel]
                    .as_ref()
                    .is_some_and(|j| j.kind == JoinKind::Left);
                if null_supplying {
                    match anti_join_column(&conjunct, rel, &relations, &schemas) {
                        AntiJoin::Unmatched => {
                            anti_join[rel] = true;
                        }
                        AntiJoin::NotApplicable(reason) => return Err(reason),
                    }
                } else {
                    predicates[rel].push(conjunct);
                }
            }
            0 if is_constant_term(&conjunct) => {
                // A term of pure literals — `1 = 1` / `1 = 0` — references no
                // relation but is still meaningful. These arise from resolving a
                // correlated `EXISTS` whose subquery returned nothing, where the
                // answer is "true for every row" or "false for every row". The
                // scan evaluates constants, so it rides along with the first
                // relation rather than needing a plan-level short circuit.
                predicates[0].push(conjunct);
            }
            0 => {
                // Still an error for anything naming something: a misspelled
                // column must not be quietly treated as a constant.
                return Err(format!(
                    "WHERE term '{}' does not reference any column of the joined relations",
                    conjunct.trim()
                ));
            }
            _ => {
                // Spans relations, so it can only be answered once the rows are
                // combined. Pushing it into either scan would run it *before*
                // the join and change which rows pair up rather than which
                // pairs survive.
                cross_relation.push(conjunct);
            }
        }
    }

    // --- aggregate stage -------------------------------------------------
    //
    // The fold reads the *projected* join rows, whose columns are positional,
    // so each grouping key and aggregate argument is expressed as a synthetic
    // field whose `write_order` is that column's index in `items`. Building it
    // here — before the per-relation fetch is computed — means any column the
    // fold needs is added to `items` in time to be fetched.
    let aggregate = build_aggregate_stage(
        select_list,
        group_by_clause,
        having_clause,
        &order_by,
        limit,
        offset,
        &mut items,
        &resolve,
    )?;

    // A select item this cannot produce is refused, not dropped.
    //
    // Every item containing `(` is set aside above for the aggregate stage to
    // claim, because that is where a call becomes an output column. The stage
    // only forms when the list holds a real aggregate, so a *non*-aggregate
    // call — `COALESCE(s.a, t.b) AS eff`, or a parenthesised `(t.b + 1)` —
    // was set aside and then claimed by nobody. The statement succeeded and
    // the column was simply absent from the result, which a client meets as
    // "no column found for name: eff": an error about its own query, naming
    // nothing that would lead anyone here.
    //
    // Evaluating these over a join is a real feature — the value has to be
    // computed per combined row, the way a post-join predicate already is —
    // and until it exists, saying so is the only honest answer.
    if aggregate.is_none() {
        if let Some(orphan) = aggregate_calls.first() {
            return Err(format!(
                "an expression in the SELECT list of a join is not supported yet: '{}'.                  Compute it in the client, or select the columns it reads and combine them there.",
                orphan.trim()
            ));
        }
    }

    // With an aggregate, ordering and paging belong to the fold: they act on
    // groups, which do not exist until it has run.
    let (order_by, limit, offset) = if aggregate.is_some() {
        (Vec::new(), None, None)
    } else {
        (order_by, limit, offset)
    };

    // --- ORDER BY --------------------------------------------------------
    let mut order_keys: Vec<(usize, SchemaField, bool)> = Vec::with_capacity(order_by.len());
    for term in &order_by {
        // The join's sort compares projected bytes and cannot place NULLs, so
        // a placement it cannot honour is refused rather than quietly dropped.
        if term.nulls_first.is_some() {
            return Err("ORDER BY ... NULLS FIRST/LAST is not supported in a join yet".to_string());
        }
        // An output label wins over a column name, matching how a single-table
        // query resolves it.
        if let Some(item) = items
            .iter()
            .find(|i| i.label.eq_ignore_ascii_case(&term.token))
        {
            order_keys.push((item.rel, item.field.clone(), term.descending));
        } else {
            let (rel, field) = resolve(&term.token)?;
            // The sort reads the key out of the *projected* row, so a key that
            // is not in the SELECT list has nothing to read. Refusing says so,
            // where returning the rows unsorted would not.
            if !items
                .iter()
                .any(|i| i.rel == rel && i.field.write_order == field.write_order)
            {
                return Err(format!(
                    "ORDER BY term '{}' must appear in the SELECT list of a join",
                    term.token
                ));
            }
            order_keys.push((rel, field, term.descending));
        }
    }
    let order_by = order_keys;

    // --- what each relation must fetch -----------------------------------
    let mut out_relations: Vec<Relation> = Vec::with_capacity(relations.len());
    for (i, r) in relations.iter().enumerate() {
        let mut fetch: Vec<SchemaField> = Vec::new();
        let push = |f: &SchemaField, fetch: &mut Vec<SchemaField>| {
            if !fetch.iter().any(|e| e.write_order == f.write_order) {
                fetch.push(f.clone());
            }
        };

        for item in items.iter().filter(|it| it.rel == i) {
            push(&item.field, &mut fetch);
        }
        // This relation's own join key, and the key another relation joins to
        // it on.
        // Every column of the key, not just the first: a composite key that
        // fetched one column would match on that one alone and silently return
        // the wrong rows.
        if let Some(j) = joins[i].as_ref() {
            for (_, right_name) in &j.keys {
                if let Some(f) = schemas[i]
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(right_name) && !f.is_deleted)
                {
                    push(f, &mut fetch);
                }
            }
        }
        for j in joins.iter().flatten() {
            if j.left_rel == i {
                for (left_name, _) in &j.keys {
                    if let Some(f) = schemas[i]
                        .iter()
                        .find(|f| f.name.eq_ignore_ascii_case(left_name) && !f.is_deleted)
                    {
                        push(f, &mut fetch);
                    }
                }
            }
        }
        for (rel, field, _) in order_by.iter() {
            if *rel == i {
                push(field, &mut fetch);
            }
        }
        // Columns named only by a cross-relation term still have to be read, or
        // the post-join filter would have nothing to test.
        for conjunct in &cross_relation {
            for field in schemas[i].iter() {
                if term_names_column(conjunct, &r.label, &r.table, &field.name) {
                    push(field, &mut fetch);
                }
            }
        }
        // Every fetched column's NULL companion comes with it.
        //
        // A NULL is not a property of a value's bytes here — a NULL
        // fixed-width column is stored as its type's zeroes, which is
        // indistinguishable from a real zero. The companion column is what
        // says which it is. Without it a post-join predicate saw every NULL as
        // a present value holding zeroes, so `COALESCE(a, b)` returned `a`
        // where `a` was NULL instead of falling through to `b`, and
        // `a IS NOT NULL` answered true for a NULL.
        //
        // Added after the loops above rather than inside them, so a companion
        // is fetched exactly once however many reasons there were to read the
        // column it tracks.
        let schema_sv: smallvec::SmallVec<[SchemaField; 20]> = schemas[i].iter().cloned().collect();
        let tracked: Vec<String> = fetch
            .iter()
            .map(|f| f.name.clone())
            .filter(|n| !crate::core::rql::nullable::is_hidden(n))
            .collect();
        for name in tracked {
            if let Some(flag) = crate::core::rql::nullable::flag_field(&name, &schema_sv) {
                push(flag, &mut fetch);
            }
        }

        // A relation with nothing to fetch still has to produce rows for the
        // join to see.
        if fetch.is_empty() {
            if let Some(f) = schemas[i].iter().min_by_key(|f| f.write_order) {
                fetch.push(f.clone());
            }
        }

        let predicate = if predicates[i].is_empty() {
            String::new()
        } else {
            let joined = predicates[i].join(" AND ");
            crate::core::tokenizer::predicate_rewrite::rewrite_where(&joined, &schemas[i])?
        };

        out_relations.push(Relation {
            table: r.table.clone(),
            label: r.label.clone(),
            join: joins[i].clone(),
            predicate,
            fetch_columns: fetch,
            require_unmatched: anti_join[i],
        });
    }

    let post_join = if cross_relation.is_empty() {
        None
    } else {
        Some(build_post_join_filter(&cross_relation, &out_relations)?)
    };

    Ok(JoinPlan {
        relations: out_relations,
        items,
        order_by,
        limit,
        offset,
        aggregate,
        post_join,
        // Set by the caller that parsed the select list; `plan` is also reached
        // from paths that have no DISTINCT to report.
        distinct: false,
    })
}

/// `text` with a leading `keyword` removed, when it is there as a whole word.
///
/// Whole-word so a column named `distinctive` is not mistaken for the keyword.
fn strip_keyword_prefix<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = text.trim_start();
    if trimmed.len() < keyword.len() {
        return None;
    }
    if !trimmed[..keyword.len()].eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = &trimmed[keyword.len()..];
    match rest.chars().next() {
        None => Some(rest),
        Some(c) if c.is_whitespace() || c == '(' => Some(rest.trim_start()),
        _ => None,
    }
}

/// True when `term` names `column` of the relation labelled `label`.
///
/// Qualified (`r.col`) or bare — a bare name is taken to belong to this
/// relation, which over-fetches at worst.
fn term_names_column(term: &str, label: &str, table: &str, column: &str) -> bool {
    let upper = term.to_ascii_uppercase();
    let col = column.to_ascii_uppercase();
    for prefix in [label, table] {
        let qualified = format!("{}.{}", prefix.to_ascii_uppercase(), col);
        if upper.contains(&qualified) {
            return true;
        }
    }
    // Bare mention as a whole word.
    upper.match_indices(&col).any(|(at, _)| {
        let before_ok = at == 0
            || !upper.as_bytes()[at - 1].is_ascii_alphanumeric()
                && upper.as_bytes()[at - 1] != b'_'
                && upper.as_bytes()[at - 1] != b'.';
        let after = at + col.len();
        let after_ok = after >= upper.len()
            || !(upper.as_bytes()[after].is_ascii_alphanumeric()
                || upper.as_bytes()[after] == b'_');
        before_ok && after_ok
    })
}

/// Flattens every relation's fetched columns into one synthetic schema and
/// rewrites the cross-relation terms against it.
///
/// Names become `<label>__<column>` so two relations that both have `id` stay
/// distinct — the reason the combined row needs a schema of its own rather than
/// reusing either side's.
fn build_post_join_filter(
    terms: &[String],
    relations: &[Relation],
) -> Result<PostJoinFilter, String> {
    let mut schema: Vec<SchemaField> = Vec::new();
    let mut columns: Vec<Vec<u64>> = Vec::with_capacity(relations.len());
    let mut renames: Vec<(String, String)> = Vec::new();

    for relation in relations.iter() {
        let mut mapping = Vec::with_capacity(relation.fetch_columns.len());
        for field in &relation.fetch_columns {
            let write_order = schema.len() as u64;
            let name = format!("{}__{}", relation.label, field.name);
            renames.push((format!("{}.{}", relation.label, field.name), name.clone()));
            if !relation.table.eq_ignore_ascii_case(&relation.label) {
                renames.push((format!("{}.{}", relation.table, field.name), name.clone()));
            }
            let mut synthetic = field.clone();
            synthetic.name = name;
            synthetic.write_order = write_order;
            synthetic.offset = 0;
            schema.push(synthetic);
            mapping.push(write_order);
        }
        columns.push(mapping);
    }

    // Longest first, so `a.bc` is not partly rewritten by a rule for `a.b`.
    renames.sort_by_key(|(from, _)| std::cmp::Reverse(from.len()));

    let schema_sv: smallvec::SmallVec<[SchemaField; 20]> = schema.iter().cloned().collect();

    // Each term is tried against the predicate tokenizer first, and handed to
    // the expression evaluator only if that cannot read it. Splitting per term
    // rather than per statement keeps the fast path for everything ordinary:
    // one term needing `||` does not push the rest onto the slower evaluator.
    let mut tokenizer_terms: Vec<String> = Vec::new();
    let mut expr_terms: Vec<crate::core::rql::expr::Condition> = Vec::new();

    for term in terms {
        let mut rewritten = term.clone();
        for (from, to) in &renames {
            rewritten = replace_qualified(&rewritten, from, to);
        }

        match crate::core::tokenizer::predicate_rewrite::rewrite_where(&rewritten, &schema_sv) {
            Ok(clause)
                if crate::core::tokenizer::query_tokenizer::try_tokenize(&clause, &schema_sv)
                    .is_ok() =>
            {
                tokenizer_terms.push(clause);
            }
            _ => {
                // Refused rather than dropped if this fails too: a join term
                // silently ignored would widen the result set.
                expr_terms.push(crate::core::rql::expr::parse_condition(
                    &rewritten, &schema,
                )?);
            }
        }
    }

    let where_clause = tokenizer_terms.join(" AND ");
    Ok(PostJoinFilter {
        schema,
        where_clause,
        columns,
        expr_terms,
    })
}

/// Replaces a qualified name with another, matching whole identifiers only and
/// skipping string literals.
fn replace_qualified(sql: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = quote {
            out.push(c as char);
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if c == b'\'' || c == b'"' {
            quote = Some(c);
            out.push(c as char);
            i += 1;
            continue;
        }
        if sql[i..].len() >= from.len() && sql[i..i + from.len()].eq_ignore_ascii_case(from) {
            let before_ok =
                i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
            let after = i + from.len();
            let after_ok = after >= bytes.len()
                || !(bytes[after].is_ascii_alphanumeric() || bytes[after] == b'_');
            if before_ok && after_ok {
                out.push_str(to);
                i = after;
                continue;
            }
        }
        out.push(c as char);
        i += 1;
    }
    out
}

/// Builds the fold that runs over the projected join rows, or `None` when the
/// `SELECT` list has no aggregate call.
#[allow(clippy::too_many_arguments)]
fn build_aggregate_stage(
    select_list: &str,
    group_by_clause: Option<&str>,
    having_clause: Option<&str>,
    order_by: &[crate::core::rql::order_by::SortTerm],
    limit: Option<u64>,
    offset: Option<u64>,
    items: &mut Vec<JoinSelectItem>,
    resolve: &dyn Fn(&str) -> Result<(usize, SchemaField), String>,
) -> Result<Option<crate::core::rql::aggregate::AggregatePlan>, String> {
    use crate::core::rql::aggregate::{AggFunc, Having, SelectItem};

    // Only a genuine aggregate puts the statement on the fold path. A nullary
    // scalar like `now()` adds a column without grouping anything; counting it
    // here demanded every other column appear in GROUP BY, so
    // `SELECT a.id, b.id, now() FROM …` was refused.
    // An aggregate *anywhere* in the item puts the statement on the fold path.
    // Testing only the outermost call meant `COALESCE(SUM(x), 0)` was not
    // recognised as aggregating at all: it fell through to the projection,
    // which has no aggregate to project, and the statement answered one row of
    // NULL instead of the sum.
    // Named rather than left to fail deeper as "'__agg_0 FILTER (WHERE …)' is
    // neither a column nor a value", which describes the internal name this
    // path substituted rather than anything the author wrote. The
    // single-relation path carries the clause; a join does not yet.
    for item in split_top_level(select_list) {
        let upper = item.to_ascii_uppercase();
        if let Some(at) = upper.find("FILTER") {
            let before_ok = at == 0 || !upper.as_bytes()[at - 1].is_ascii_alphanumeric();
            if before_ok && item[at + "FILTER".len()..].trim_start().starts_with('(') {
                return Err(
                    "an aggregate FILTER (WHERE …) is not supported over a join yet".to_string(),
                );
            }
        }
    }

    let has_call = select_list.trim() != "*"
        && split_top_level(select_list).iter().any(|item| {
            let (expr, _) = split_expr_alias(item.trim());
            !crate::core::rql::lexer_query::find_aggregate_calls(expr).is_empty()
        });

    // A GROUP BY makes the query grouped by itself — `SELECT a.city FROM a
    // JOIN b … GROUP BY a.city` is a DISTINCT, and an aggregate may appear
    // only in the HAVING. `SELECT *` is the one shape still refused: the fold
    // checks each named column against the keys, and a star names none.
    let grouped = has_call || (group_by_clause.is_some() && select_list.trim() != "*");

    if !grouped {
        if group_by_clause.is_some() {
            return Err(
                "GROUP BY over a join needs the columns listed explicitly, not '*'".to_string(),
            );
        }
        if having_clause.is_some() {
            return Err("HAVING requires an aggregate in the SELECT list".to_string());
        }
        return Ok(None);
    }

    // Adds a column to the join's output if it is not already there, and
    // returns a field addressing it by position in the projected row.
    fn positional(rel: usize, field: SchemaField, items: &mut Vec<JoinSelectItem>) -> SchemaField {
        let pos = match items
            .iter()
            .position(|i| i.rel == rel && i.field.write_order == field.write_order)
        {
            Some(p) => p,
            None => {
                items.push(JoinSelectItem {
                    rel,
                    field: field.clone(),
                    label: field.name.clone(),
                    scalar: None,
                    literal: None,
                });
                items.len() - 1
            }
        };
        SchemaField::new(
            field.name.clone(),
            field.db_type.clone(),
            field.size,
            0,
            pos as u64,
            false,
        )
    }

    let mut group_by: Vec<SchemaField> = Vec::new();
    if let Some(list) = group_by_clause {
        for raw in list.split(',') {
            let name = raw.trim().split_whitespace().next().unwrap_or("");
            if name.is_empty() {
                continue;
            }
            let (rel, field) = resolve(name)?;
            group_by.push(positional(rel, field, items));
        }
    }

    let mut fold_items: Vec<SelectItem> = Vec::new();
    for raw in split_top_level(select_list) {
        let entry = raw.trim();
        if entry.is_empty() {
            continue;
        }
        let (expr, alias) = split_expr_alias(entry);

        // An expression *around* an aggregate — `COALESCE(SUM(x), 0)`,
        // `SUM(x) / COUNT(*)`. Each aggregate is pulled out into its own input
        // and the surrounding expression is evaluated once per emitted group,
        // exactly as the single-relation path does.
        let calls = crate::core::rql::lexer_query::find_aggregate_calls(expr);
        let wrapped =
            !calls.is_empty() && !(calls.len() == 1 && calls[0].0 == 0 && calls[0].1 == expr.len());
        if wrapped {
            let mut inputs: Vec<crate::core::rql::aggregate::ComputedInput> = Vec::new();
            let mut synth_schema: Vec<SchemaField> = Vec::new();
            let mut rewritten = String::with_capacity(expr.len());
            let mut cursor = 0usize;

            for (start, end) in &calls {
                rewritten.push_str(&expr[cursor..*start]);
                let call = &expr[*start..*end];
                let open = call.find('(').expect("a call has one");
                let close = call.rfind(')').expect("a call has one");
                let name = call[..open].trim();
                let func = AggFunc::parse(name)
                    .ok_or_else(|| format!("function '{}' is not supported yet", name))?;
                let arg_text = call[open + 1..close].trim();

                let arg = if arg_text == "*" {
                    if func != AggFunc::Count {
                        return Err(format!("{}(*) is not valid", name.to_uppercase()));
                    }
                    None
                } else {
                    let (rel, field) = resolve(arg_text)?;
                    Some(positional(rel, field, items))
                };

                let slot = inputs.len();
                let ty = func.result_type(arg.as_ref().map(|f: &SchemaField| &f.db_type));
                let synth_name = format!("__agg_{}", slot);
                synth_schema.push(SchemaField::new(
                    synth_name.clone(),
                    ty.clone(),
                    ty.get_size(),
                    0,
                    slot as u64,
                    false,
                ));
                inputs.push(crate::core::rql::aggregate::ComputedInput { func, arg });
                rewritten.push_str(&synth_name);
                cursor = *end;
            }
            rewritten.push_str(&expr[cursor..]);

            let parsed = crate::core::rql::expr::parse(&rewritten, &synth_schema)?;
            let db_type = crate::core::rql::expr::static_type(&parsed);
            fold_items.push(SelectItem::Computed {
                expr: parsed,
                inputs,
                db_type,
                label: alias.unwrap_or_else(|| "?column?".to_string()),
            });
            continue;
        }

        if let Some(open) = expr.find('(') {
            let func_name = expr[..open].trim();
            let Some(func) = AggFunc::parse(func_name) else {
                return Err(format!("function '{}' is not supported yet", func_name));
            };
            let close = expr
                .rfind(')')
                .ok_or_else(|| format!("unterminated function call in '{}'", entry))?;
            let arg_text = expr[open + 1..close].trim();

            let arg = if arg_text == "*" {
                if func != AggFunc::Count {
                    return Err(format!("{}(*) is not valid", func_name.to_uppercase()));
                }
                None
            } else {
                let (rel, field) = resolve(arg_text)?;
                Some(positional(rel, field, items))
            };

            fold_items.push(SelectItem::Aggregate {
                func,
                arg,
                label: alias.unwrap_or_else(|| func.name().to_string()),
                // A join does not carry an aggregate FILTER through the fold
                // yet; the single-relation path parses it.
                filter: None,
            });
        } else {
            let (rel, field) = resolve(expr)?;
            let label = alias.unwrap_or_else(|| field.name.clone());
            let synthetic = positional(rel, field, items);
            if !group_by
                .iter()
                .any(|g| g.write_order == synthetic.write_order)
            {
                return Err(format!(
                    "column '{}' must appear in GROUP BY or be used in an aggregate",
                    label
                ));
            }
            fold_items.push(SelectItem::Column {
                field: synthetic,
                label,
            });
        }
    }

    let having = match having_clause {
        None => None,
        Some(text) => {
            let (func, arg_text, op, literal) = split_having(text)?;
            let arg = match arg_text {
                None => None,
                Some(name) => {
                    let (rel, field) = resolve(&name)?;
                    Some(positional(rel, field, items))
                }
            };
            Some(Having {
                func,
                arg,
                op,
                literal,
            })
        }
    };

    // Ordering is by output label: a group has no relation to sort within.
    let mut fold_order = Vec::with_capacity(order_by.len());
    for term in order_by {
        if term.nulls_first.is_some() {
            return Err(
                "ORDER BY ... NULLS FIRST/LAST is not supported for grouped queries yet"
                    .to_string(),
            );
        }
        let label = crate::core::rql::aggregate::resolve_order_label(&term.token, &fold_items)
            .ok_or_else(|| format!("ORDER BY term '{}' is not in the SELECT list", term.token))?;
        fold_order.push((label, term.descending));
    }

    Ok(Some(crate::core::rql::aggregate::AggregatePlan {
        items: fold_items,
        group_by,
        computed_keys: Vec::new(),
        having,
        order_by: fold_order,
        limit,
        offset,
    }))
}

/// Splits a comma-separated list on commas outside parentheses and quotes.
fn split_top_level(list: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut quote: Option<char> = None;

    for c in list.chars() {
        match quote {
            Some(q) => {
                current.push(c);
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' => {
                    quote = Some(c);
                    current.push(c);
                }
                '(' => {
                    depth += 1;
                    current.push(c);
                }
                ')' => {
                    depth = depth.saturating_sub(1);
                    current.push(c);
                }
                ',' if depth == 0 => out.push(std::mem::take(&mut current)),
                _ => current.push(c),
            },
        }
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

/// Separates a select item from its output name.
fn split_expr_alias(entry: &str) -> (&str, Option<String>) {
    let tail_start = entry.rfind(')').map(|i| i + 1).unwrap_or(0);
    let tail = &entry[tail_start..];

    // Nothing after the last `)` means there is no alias and the whole item is
    // the expression. Without this the fall-through below split the item on
    // whitespace, so `COALESCE(SUM(x), 0)` became `COALESCE(SUM(x),` with the
    // alias `0)` — reported back as "unterminated call".
    if tail_start > 0 && tail.trim().is_empty() {
        return (entry.trim_end(), None);
    }

    let mut words = tail.split_whitespace();

    match (words.next(), words.next()) {
        (Some(first), Some(second)) if first.eq_ignore_ascii_case("AS") => (
            entry[..tail_start].trim_end(),
            Some(second.trim_matches('"').to_string()),
        ),
        (Some(first), None) if tail_start > 0 => (
            entry[..tail_start].trim_end(),
            Some(first.trim_matches('"').to_string()),
        ),
        _ => {
            let mut parts = entry.split_whitespace();
            let head = parts.next().unwrap_or(entry);
            let alias = match parts.next() {
                Some(w) if w.eq_ignore_ascii_case("AS") => {
                    parts.next().map(|a| a.trim_matches('"').to_string())
                }
                // An operator is part of the expression, not a name for it.
                Some(w)
                    if w.chars()
                        .next()
                        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_') =>
                {
                    Some(w.trim_matches('"').to_string())
                }
                Some(_) => return (entry.trim(), None),
                None => None,
            };
            (head, alias)
        }
    }
}

/// Splits `HAVING <aggregate> <op> <literal>` into its parts, leaving the
/// argument unresolved so the caller can place it positionally.
fn split_having(
    text: &str,
) -> Result<
    (
        crate::core::rql::aggregate::AggFunc,
        Option<String>,
        crate::core::rql::aggregate::HavingOp,
        String,
    ),
    String,
> {
    use crate::core::rql::aggregate::{AggFunc, HavingOp};

    let text = text.trim();
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut op_at = None;
    for (i, b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b'=' | b'<' | b'>' | b'!' if depth == 0 => {
                op_at = Some(i);
                break;
            }
            _ => {}
        }
    }
    let op_at = op_at.ok_or_else(|| {
        "HAVING must compare an aggregate against a constant, as in `HAVING COUNT(*) > 1`"
            .to_string()
    })?;

    let mut op_end = op_at + 1;
    if op_end < bytes.len() && matches!(bytes[op_end], b'=' | b'>') {
        op_end += 1;
    }
    let op = HavingOp::parse(&text[op_at..op_end]).ok_or_else(|| {
        format!(
            "unknown comparison operator in HAVING: '{}'",
            &text[op_at..op_end]
        )
    })?;

    let lhs = text[..op_at].trim();
    let literal = text[op_end..].trim().to_string();
    if literal.is_empty() {
        return Err("HAVING is missing the value to compare against".to_string());
    }

    let open = lhs
        .find('(')
        .ok_or_else(|| "HAVING must test an aggregate, not a plain column".to_string())?;
    let close = lhs
        .rfind(')')
        .ok_or_else(|| format!("unterminated function call in '{}'", lhs))?;
    let func = AggFunc::parse(lhs[..open].trim())
        .ok_or_else(|| format!("function '{}' is not supported yet", lhs[..open].trim()))?;
    let arg_text = lhs[open + 1..close].trim();
    let arg = if arg_text == "*" {
        None
    } else {
        Some(arg_text.to_string())
    };

    Ok((func, arg, op, literal))
}

/// True when a `SELECT` statement's `FROM` clause contains a join.
pub fn statement_has_join(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    // Whole-word matching, or a statement formatted across lines is not seen as
    // a join at all and goes to the single-table path — which then reports
    // "JOIN is not supported yet" about a join the engine does support.
    let Some(from) = find_keyword(sql, "FROM", 0) else {
        return false;
    };
    let start = from + "FROM".len();
    let end = ["WHERE", "ORDER", "LIMIT", "OFFSET", "GROUP", "HAVING"]
        .iter()
        .filter_map(|kw| find_keyword(sql, kw, start))
        .min()
        .unwrap_or(upper.len());
    find_keyword(&sql[start..end], "JOIN", 0).is_some() || sql[start..end].contains(',')
}

/// Splits a joined `SELECT` into its clauses and plans it.
pub fn plan_statement(
    sql: &str,
    schema_of: &dyn Fn(&str) -> Option<SmallVec<[SchemaField; 20]>>,
) -> Result<JoinPlan, String> {
    // Same normalisation the single-table path applies: the clause split below
    // only recognises `LIMIT` and `OFFSET`.
    let sql = crate::core::tokenizer::pagination::normalize_pagination(sql.trim())?;
    let sql = sql.as_str();
    let upper = sql.to_ascii_uppercase();

    // Whole-word matching throughout: the literal `" SELECT "` / `" FROM "`
    // forms fail on any statement formatted across lines, which is how every
    // ORM emits them.
    if !upper.trim_start().starts_with("SELECT") {
        return Err("Not a SELECT query".to_string());
    }
    let from_at =
        find_keyword(sql, "FROM", 0).ok_or_else(|| "Missing FROM in SELECT query".to_string())?;

    let select_start = find_keyword(sql, "SELECT", 0)
        .map(|i| i + "SELECT".len())
        .unwrap_or(0);
    let mut select_list = sql[select_start..from_at].trim();
    let mut distinct = false;
    if let Some(rest) = strip_keyword_prefix(select_list, "DISTINCT") {
        // `DISTINCT ON (expr)` keeps the *first* row of each group under the
        // ORDER BY, which is a different operation from collapsing identical
        // rows and would silently return the wrong one of each group if it were
        // treated as plain DISTINCT.
        if strip_keyword_prefix(rest, "ON").is_some() {
            return Err("SELECT DISTINCT ON is not supported yet".to_string());
        }
        distinct = true;
        select_list = rest;
    }

    let after_from = &sql[from_at + "FROM".len()..];

    let where_at = find_keyword(after_from, "WHERE", 0);
    let order_at = find_keyword(after_from, "ORDER", 0);
    let limit_at = find_keyword(after_from, "LIMIT", 0);
    let offset_at = find_keyword(after_from, "OFFSET", 0);
    let group_at = find_keyword(after_from, "GROUP", 0);
    let having_at = find_keyword(after_from, "HAVING", 0);

    let from_end = [where_at, order_at, limit_at, offset_at, group_at, having_at]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(after_from.len());

    let where_clause = match where_at {
        None => "",
        Some(w) => {
            let start = w + "WHERE".len();
            let end = [order_at, limit_at, offset_at, group_at, having_at]
                .into_iter()
                .flatten()
                .filter(|p| *p >= start)
                .min()
                .unwrap_or(after_from.len());
            after_from[start..end].trim()
        }
    };

    let group_by_clause = group_at.map(|g| {
        let start = g + "GROUP".len();
        let end = [having_at, order_at, limit_at, offset_at]
            .into_iter()
            .flatten()
            .filter(|p| *p >= start)
            .min()
            .unwrap_or(after_from.len());
        let clause = after_from[start..end].trim_start();
        clause
            .strip_prefix("BY")
            .or_else(|| clause.strip_prefix("by"))
            .unwrap_or(clause)
            .trim()
    });

    let having_clause = having_at.map(|h| {
        let start = h + "HAVING".len();
        let end = [order_at, limit_at, offset_at]
            .into_iter()
            .flatten()
            .filter(|p| *p >= start)
            .min()
            .unwrap_or(after_from.len());
        after_from[start..end].trim()
    });

    let order_by = match order_at {
        None => Vec::new(),
        Some(o) => {
            // `find_keyword` matched only the first word of `ORDER BY`.
            let start = o + "ORDER".len();
            let end = [limit_at, offset_at]
                .into_iter()
                .flatten()
                .filter(|p| *p >= start)
                .min()
                .unwrap_or(after_from.len());
            let clause = after_from[start..end].trim_start();
            let clause = clause
                .strip_prefix("BY")
                .or_else(|| clause.strip_prefix("by"))
                .unwrap_or(clause);
            crate::core::rql::order_by::parse(clause)?
        }
    };

    let parse_count = |at: Option<usize>, kw: &str| -> Result<Option<u64>, String> {
        let Some(p) = at else { return Ok(None) };
        let start = p + kw.len();
        let token = after_from[start..]
            .trim()
            .split_whitespace()
            .next()
            .unwrap_or("");
        token
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("Invalid {} value '{}'", kw.trim(), token))
    };
    let limit = parse_count(limit_at, "LIMIT")?;
    let offset = parse_count(offset_at, "OFFSET")?;

    let relations = parse_from_clause(after_from[..from_end].trim())?;
    let mut planned = plan(
        relations,
        select_list,
        where_clause,
        group_by_clause,
        having_clause,
        order_by,
        limit,
        offset,
        schema_of,
    )?;
    planned.distinct = distinct;
    Ok(planned)
}

/// Which relations a `WHERE` conjunct refers to.
fn relations_referenced(
    conjunct: &str,
    relations: &[RelationRef],
    schemas: &[SmallVec<[SchemaField; 20]>],
) -> Vec<usize> {
    let mut hits: Vec<usize> = Vec::new();

    for token in
        conjunct.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.' || c == '"'))
    {
        let token = token.trim_matches('"');
        if token.is_empty() {
            continue;
        }
        let (qualifier, column) = split_qualified(token);
        match qualifier {
            Some(q) => {
                if let Some(i) = relations.iter().position(|r| {
                    r.label.eq_ignore_ascii_case(&q) || r.table.eq_ignore_ascii_case(&q)
                }) {
                    if schemas[i]
                        .iter()
                        .any(|f| f.name.eq_ignore_ascii_case(&column))
                        && !hits.contains(&i)
                    {
                        hits.push(i);
                    }
                }
            }
            None => {
                for (i, s) in schemas.iter().enumerate() {
                    if s.iter().any(|f| f.name.eq_ignore_ascii_case(&column)) && !hits.contains(&i)
                    {
                        hits.push(i);
                    }
                }
            }
        }
    }

    hits
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn on_splits_the_join_key_from_its_extra_filters() {
        let (l, r, extra) =
            split_on_condition("r.domain_id = d.id AND r.name = 'SuperAdministrator'").unwrap();
        assert_eq!(l, "r.domain_id");
        assert_eq!(r, "d.id");
        assert_eq!(extra, vec!["r.name = 'SuperAdministrator'".to_string()]);
    }

    #[test]
    fn on_still_works_with_a_single_term() {
        let (l, r, extra) = split_on_condition("a.x = b.y").unwrap();
        assert_eq!((l.as_str(), r.as_str()), ("a.x", "b.y"));
        assert!(extra.is_empty());
    }

    #[test]
    fn an_on_without_a_column_equality_yields_no_key() {
        // A lone literal comparison names no join key. That is reported by
        // returning an empty key rather than by failing: an *inner* join on a
        // predicate is a cross join filtered by it, which the planner builds.
        // The planner still refuses the outer-join case, where the two are not
        // equivalent.
        let (l, r, extra) = split_on_condition("r.name = 'x'").unwrap();
        assert!(l.is_empty() && r.is_empty(), "no key: {:?} {:?}", l, r);
        assert_eq!(extra, vec!["r.name = 'x'".to_string()]);

        // OR still cannot be expressed as a per-relation filter.
        assert!(split_on_condition("a.x = b.y OR a.z = b.w").is_err());
    }

    #[test]
    fn a_literal_comparison_is_never_mistaken_for_the_join_key() {
        // `'2024-01-01'` is qualified-looking only if you squint; it must not
        // become the join key or the real key would be pushed down as a filter.
        let (l, r, extra) =
            split_on_condition("b.created = '2024-01-01' AND a.id = b.aid").unwrap();
        assert_eq!((l.as_str(), r.as_str()), ("a.id", "b.aid"));
        assert_eq!(extra, vec!["b.created = '2024-01-01'".to_string()]);
    }
    use crate::memory_pool::MEMORY_POOL;

    fn field(name: &str, ty: DbType, write_order: u64) -> SchemaField {
        SchemaField::new(
            name.to_string(),
            ty.clone(),
            ty.get_size(),
            0,
            write_order,
            false,
        )
    }

    fn mb(bytes: &[u8]) -> crate::memory_pool::MemoryBlock {
        let mut m = MEMORY_POOL.acquire(bytes.len());
        m.into_slice_mut().copy_from_slice(bytes);
        m
    }

    fn row(cells: &[(u64, DbType, &[u8])]) -> Row {
        let mut columns: SmallVec<[Column; 32]> = SmallVec::new();
        for (schema_id, ty, bytes) in cells {
            columns.push(Column {
                schema_id: *schema_id,
                data: mb(bytes),
                column_type: ty.clone(),
            });
        }
        Row {
            position: 0,
            columns,
            length: 0,
        }
    }

    /// users(id, name, domain_id) joined to domain(id, name).
    fn two_relation_plan(kind: JoinKind) -> JoinPlan {
        let u_id = field("id", DbType::STRING, 0);
        let u_name = field("name", DbType::STRING, 1);
        let u_domain = field("domain_id", DbType::STRING, 2);
        let d_id = field("id", DbType::STRING, 0);
        let d_name = field("name", DbType::STRING, 1);

        JoinPlan {
            relations: vec![
                Relation {
                    table: "users".into(),
                    label: "u".into(),
                    join: None,
                    predicate: String::new(),
                    require_unmatched: false,
                    fetch_columns: vec![u_id, u_name.clone(), u_domain],
                },
                Relation {
                    table: "domain".into(),
                    label: "d".into(),
                    join: Some(JoinedRelation {
                        kind,
                        left_rel: 0,
                        keys: vec![("domain_id".into(), "id".into())],
                    }),
                    predicate: String::new(),
                    require_unmatched: false,
                    fetch_columns: vec![d_id, d_name.clone()],
                },
            ],
            items: vec![
                JoinSelectItem {
                    rel: 0,
                    field: u_name,
                    label: "name".into(),
                    scalar: None,
                    literal: None,
                },
                JoinSelectItem {
                    rel: 1,
                    field: d_name,
                    label: "domain".into(),
                    scalar: None,
                    literal: None,
                },
            ],
            order_by: Vec::new(),
            limit: None,
            offset: None,
            aggregate: None,
            post_join: None,
            distinct: false,
        }
    }

    fn users() -> Vec<Row> {
        vec![
            row(&[
                (0, DbType::STRING, b"u1"),
                (1, DbType::STRING, b"Alice"),
                (2, DbType::STRING, b"d1"),
            ]),
            row(&[
                (0, DbType::STRING, b"u2"),
                (1, DbType::STRING, b"Bob"),
                (2, DbType::STRING, b"d1"),
            ]),
            row(&[
                (0, DbType::STRING, b"u3"),
                (1, DbType::STRING, b"Carol"),
                (2, DbType::STRING, b"d2"),
            ]),
        ]
    }

    fn domains() -> Vec<Row> {
        vec![
            row(&[(0, DbType::STRING, b"d1"), (1, DbType::STRING, b"Acme")]),
            row(&[(0, DbType::STRING, b"d2"), (1, DbType::STRING, b"Globex")]),
        ]
    }

    fn rendered(rows: &[Row]) -> Vec<String> {
        rows.iter()
            .map(|r| {
                r.columns
                    .iter()
                    .map(|c| String::from_utf8_lossy(c.data.into_slice()).into_owned())
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect()
    }

    #[test]
    fn inner_join_pairs_rows_on_the_key() {
        let plan = two_relation_plan(JoinKind::Inner);
        let out = project(&plan, combine(&plan, &[users(), domains()]).expect("join"));
        assert_eq!(
            rendered(&out),
            vec!["Alice|Acme", "Bob|Acme", "Carol|Globex"]
        );
    }

    #[test]
    fn inner_join_drops_rows_with_no_match() {
        let plan = two_relation_plan(JoinKind::Inner);
        // Only d2 exists, so Alice and Bob have nothing to join to.
        let only_globex = vec![domains()[1].clone()];
        let out = project(
            &plan,
            combine(&plan, &[users(), only_globex]).expect("join"),
        );
        assert_eq!(rendered(&out), vec!["Carol|Globex"]);
    }

    #[test]
    fn left_join_keeps_unmatched_rows_with_an_empty_right_side() {
        let plan = two_relation_plan(JoinKind::Left);
        let only_globex = vec![domains()[1].clone()];
        let out = project(
            &plan,
            combine(&plan, &[users(), only_globex]).expect("join"),
        );
        assert_eq!(rendered(&out), vec!["Alice|", "Bob|", "Carol|Globex"]);
    }

    /// A one-to-many join must produce one output row per match, not one per
    /// left row.
    #[test]
    fn a_key_matching_several_rows_fans_out() {
        let plan = two_relation_plan(JoinKind::Inner);
        let mut ds = domains();
        // A second domain sharing d1's key.
        ds.push(row(&[
            (0, DbType::STRING, b"d1"),
            (1, DbType::STRING, b"Acme Two"),
        ]));
        let out = project(&plan, combine(&plan, &[users(), ds]).expect("join"));
        assert_eq!(out.len(), 5, "Alice and Bob each match two rows");
    }

    #[test]
    fn ordering_runs_over_the_combined_rows() {
        let mut plan = two_relation_plan(JoinKind::Inner);
        let name = plan.relations[0].fetch_columns[1].clone();
        plan.order_by = vec![(0, name, true)];
        let out = project(&plan, combine(&plan, &[users(), domains()]).expect("join"));
        assert_eq!(
            rendered(&out),
            vec!["Carol|Globex", "Bob|Acme", "Alice|Acme"]
        );
    }

    /// The second key decides the order within a tie on the first.
    #[test]
    fn ordering_over_several_keys_breaks_ties_with_the_later_ones() {
        let mut plan = two_relation_plan(JoinKind::Inner);
        let domain = plan.relations[1].fetch_columns[1].clone();
        let name = plan.relations[0].fetch_columns[1].clone();
        // Domain ascending, and within one domain the name descending. Alice
        // and Bob tie on Acme, so only the second key separates them.
        plan.order_by = vec![(1, domain, false), (0, name, true)];
        let out = project(&plan, combine(&plan, &[users(), domains()]).expect("join"));
        assert_eq!(
            rendered(&out),
            vec!["Bob|Acme", "Alice|Acme", "Carol|Globex"]
        );
    }

    #[test]
    fn paging_runs_after_the_join_not_before() {
        let mut plan = two_relation_plan(JoinKind::Inner);
        let name = plan.relations[0].fetch_columns[1].clone();
        plan.order_by = vec![(0, name, false)];
        plan.offset = Some(1);
        plan.limit = Some(1);
        let out = project(&plan, combine(&plan, &[users(), domains()]).expect("join"));
        assert_eq!(rendered(&out), vec!["Bob|Acme"]);
    }

    #[test]
    fn an_empty_left_side_yields_no_rows() {
        let plan = two_relation_plan(JoinKind::Inner);
        let out = project(
            &plan,
            combine(&plan, &[Vec::new(), domains()]).expect("join"),
        );
        assert!(out.is_empty());
    }

    /// A left join whose right side is empty still returns every left row.
    #[test]
    fn left_join_against_an_empty_relation_keeps_every_left_row() {
        let plan = two_relation_plan(JoinKind::Left);
        let out = project(&plan, combine(&plan, &[users(), Vec::new()]).expect("join"));
        assert_eq!(rendered(&out), vec!["Alice|", "Bob|", "Carol|"]);
    }
}

#[cfg(test)]
mod quoted_join_keys {
    use super::*;

    /// An ORM quotes every identifier it emits, so the join key arrives as
    /// `"users"."domain_id" = "domain"."id"`.
    ///
    /// Treating a leading `"` as a string literal made every sea-orm join fail
    /// with *"join condition must contain an equality between two columns"* —
    /// about a condition that is exactly that. In PostgreSQL single quotes
    /// delimit strings and double quotes delimit identifiers; the two are not
    /// interchangeable and a parser that conflates them rejects the ORM's normal
    /// output.
    #[test]
    fn a_quoted_qualified_column_is_a_join_key_not_a_literal() {
        let (left, right, extra) =
            split_on_condition(r#""users"."domain_id" = "domain"."id""#).expect("quoted key");
        assert_eq!(left, r#""users"."domain_id""#);
        assert_eq!(right, r#""domain"."id""#);
        assert!(extra.is_empty());
    }

    /// The unquoted form must keep working, and both must reach the same key.
    #[test]
    fn quoting_does_not_change_which_term_is_the_key() {
        let (l1, r1, e1) =
            split_on_condition(r#""u"."did" = "d"."id" AND "d"."nm" = 'x'"#).expect("quoted");
        let (l2, r2, e2) = split_on_condition("u.did = d.id AND d.nm = 'x'").expect("bare");

        assert_eq!(
            e1.len(),
            1,
            "the literal comparison is an extra, not the key"
        );
        assert_eq!(e2.len(), 1);
        assert!(l1.contains("did") && r1.contains("id"));
        assert!(l2.contains("did") && r2.contains("id"));
    }

    /// The property the double-quote check was reaching for: a comparison
    /// against a literal must never be mistaken for the join key, or the real
    /// key would be pushed down as a filter and the join would be silently
    /// wrong. Single quotes are what actually mark one.
    #[test]
    fn a_string_literal_is_still_never_the_key() {
        let (left, right, extra) =
            split_on_condition(r#""d"."nm" = 'admin' AND "u"."did" = "d"."id""#)
                .expect("literal first");
        assert!(
            left.contains("did") && right.contains("id"),
            "{} = {}",
            left,
            right
        );
        assert_eq!(extra.len(), 1);
        assert!(extra[0].contains("'admin'"));

        // An unqualified double-quoted value is not a column reference either,
        // so it cannot become a key by losing the literal test. It yields no
        // key at all, which the planner turns into a filtered cross join for an
        // inner join and refuses for an outer one.
        let (l, r, extra) = split_on_condition(r#""d"."nm" = "Alice""#).unwrap();
        assert!(
            l.is_empty() && r.is_empty(),
            "must not become a key: {} = {}",
            l,
            r
        );
        assert_eq!(extra.len(), 1);
    }
}

#[cfg(test)]
mod relation_size_tests {
    use super::*;

    fn field(name: &str, size: u32, order: u64) -> SchemaField {
        SchemaField::new(name.to_string(), DbType::I32, size, 0, order, false)
    }

    #[test]
    fn a_whole_relation_size_becomes_a_row_width() {
        let relations = vec![RelationRef {
            table: "case".to_string(),
            label: "c".to_string(),
            join: None,
        }];
        let schemas = vec![smallvec::smallvec![field("id", 4, 0), field("n", 8, 1)]];
        assert_eq!(
            rewrite_relation_size(
                "COALESCE(SUM(pg_column_size(c)), 0) AS total_bytes",
                &relations,
                &schemas
            ),
            "COALESCE(COUNT(*) * 12, 0) AS total_bytes"
        );
    }
}

#[cfg(test)]
mod constant_term_tests {
    use super::is_constant_term;

    /// A term of pure literals is constant even when the operator joining them
    /// is a word. `'' ILIKE '%x%'` is false for every row, and rejecting it as
    /// a column reference stopped a real application's query outright.
    #[test]
    fn a_word_operator_between_literals_is_still_constant() {
        for term in [
            "'' ILIKE '%4806fcdf%'",
            "'' LIKE '%x%'",
            "'a' IS NOT NULL",
            "1 = 1",
            "'a' = 'b' AND 'c' != 'd'",
            "TRUE",
            "'x' CONTAINS 'y'",
        ] {
            assert!(is_constant_term(term), "{} should be constant", term);
        }
    }

    /// The reason this is a list and not "does it look like a column": a
    /// misspelled name has to keep raising an error rather than being waved
    /// through as a constant that quietly matches nothing.
    #[test]
    fn a_name_is_not_constant() {
        for term in [
            "nam ILIKE '%x%'",
            "id = 1",
            "'a' = other_column",
            "lower('X') = 'x'",
        ] {
            assert!(!is_constant_term(term), "{} should not be constant", term);
        }
    }
}
