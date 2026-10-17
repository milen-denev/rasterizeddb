use smallvec::SmallVec;

use crate::core::{
    processor::transformer::ComparerOperation,
    row::{
        common::simd_compare_strings,
        row::{ColumnFetchingData, RowFetch},
        schema::{SchemaCalculator, SchemaField},
    },
    rql::lexer_s1::QueryPurpose,
};

pub struct DeleteRow {
    pub table_name: String,
    pub where_clause: String,
    pub query_row_fetch: RowFetch,
}

fn extract_where_columns(
    where_clause: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> std::collections::HashSet<String> {
    let mut where_columns = std::collections::HashSet::new();
    if where_clause.is_empty() {
        return where_columns;
    }

    let where_bytes = where_clause.as_bytes();
    // As in `lexer_query::extract_where_columns`: an underscore is part of an
    // identifier, so `foo` must not match inside `foo_id`, and a dropped column
    // cannot be referenced at all.
    let is_word_byte = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    for field in schema.iter().filter(|f| !f.is_deleted) {
        let field_bytes = field.name.as_bytes();
        if field_bytes.len() > where_bytes.len() {
            continue;
        }
        for i in 0..=where_bytes.len() - field_bytes.len() {
            let slice = &where_bytes[i..i + field_bytes.len()];
            if simd_compare_strings(slice, field_bytes, &ComparerOperation::Equals) {
                let start_ok = i == 0 || !is_word_byte(where_bytes[i - 1]);
                let end_ok = i + field_bytes.len() == where_bytes.len()
                    || !is_word_byte(where_bytes[i + field_bytes.len()]);
                if start_ok && end_ok {
                    where_columns.insert(field.name.clone());
                    break;
                }
            }
        }
    }

    where_columns
}

/// Attempts to parse a QueryPurpose::DeleteRow variant into a DeleteRow struct.
/// Returns None if the QueryPurpose is not DeleteRow or parsing fails.
/// Parses a `DELETE`, discarding why it was rejected.
pub fn delete_row_from_query_purpose(
    qp: &QueryPurpose,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Option<DeleteRow> {
    delete_row_with_reason(qp, schema).0
}

/// Parses a `DELETE`, also returning why it was rejected.
///
/// The reason matters: without it an unsupported `WHERE` clause reached the
/// client as "Cannot parse DELETE query", which names neither the clause nor
/// the feature. `SELECT` with the same predicate reports the real cause, so the
/// two disagreed about the same statement. Mirrors `create_table_with_reason`,
/// which exists for exactly this reason.
pub fn delete_row_with_reason(
    qp: &QueryPurpose,
    schema: &SmallVec<[SchemaField; 20]>,
) -> (Option<DeleteRow>, Option<String>) {
    let mut rejection: Option<String> = None;
    let parsed = parse_delete_row(qp, schema, &mut rejection);
    (parsed, rejection)
}

fn parse_delete_row(
    qp: &QueryPurpose,
    schema: &SmallVec<[SchemaField; 20]>,
    rejection: &mut Option<String>,
) -> Option<DeleteRow> {
    if let QueryPurpose::DeleteRow(sql) = qp {
        let sql = sql.as_str();
        let sql_upper = sql.to_ascii_uppercase();
        if !sql_upper.starts_with("DELETE FROM") {
            return None;
        }
        // DELETE FROM table_name WHERE ...
        // Split before parsing, for the same reason as `UPDATE`.
        let sql = match crate::core::rql::lexer_ir::returning_split(sql) {
            Some((head, _)) => head,
            None => sql,
        };
        let sql_upper = sql.to_ascii_uppercase();

        // `DELETE … USING other WHERE t.k = other.k` deletes only the rows the
        // join matches. Nothing here implements it, and the clause used to be
        // *ignored*: the predicate then referenced a relation the scan did not
        // have, matched every row, and the statement emptied the table.
        //
        // That is not a missing feature failing safely — it is a targeted
        // delete silently becoming `DELETE FROM t`. It destroyed 13,585 rows of
        // a real table. Refused until it is implemented, because an error the
        // caller can see is the only acceptable answer to a statement this
        // engine cannot carry out.
        if crate::core::rql::join::find_keyword(sql, "USING", 0).is_some() {
            return None;
        }

        let mut parts = sql.split_whitespace();
        parts.next(); // DELETE
        parts.next(); // FROM
        let table_name = parts.next()?.trim_matches('"');
        // Matched as a whole word, not as a substring. `find("WHERE")` hit the
        // first occurrence anywhere — including inside a relation name, so
        // `DELETE FROM "wherehouse" WHERE id = 1` took `house" WHERE id = 1`
        // for its predicate and the statement failed with an error about a
        // clause nobody wrote.
        let where_idx = crate::core::rql::join::find_keyword(sql, "WHERE", 0);
        let _ = &sql_upper;
        let where_clause = if let Some(idx) = where_idx {
            let clause = sql[idx + 5..].trim();
            // Remove leading "WHERE" if present (case-insensitive)
            let clause_upper = clause.to_ascii_uppercase();
            if clause_upper.starts_with("WHERE ") {
                clause[6..].trim().to_string()
            } else {
                clause.to_string()
            }
        } else {
            // `DELETE FROM t` with no `WHERE` deletes every row, which is
            // what PostgreSQL does with it. Only an alias may follow the
            // table: anything else is a clause this parser did not
            // recognise, and reading it as "no condition" would turn a
            // misread statement into one that empties the table.
            let rest: Vec<&str> = parts.collect();
            let alias_only = match rest.as_slice() {
                [] => true,
                [alias] | [_, alias] => {
                    (rest.len() == 1 || rest[0].eq_ignore_ascii_case("AS"))
                        && !alias.eq_ignore_ascii_case("AS")
                        && alias
                            .trim_matches('"')
                            .chars()
                            .all(|c| c.is_alphanumeric() || c == '_')
                }
                _ => false,
            };
            if !alias_only || table_name.is_empty() {
                return None;
            }
            return Some(DeleteRow {
                table_name: table_name.to_string(),
                where_clause: String::new(),
                query_row_fetch: RowFetch {
                    columns_fetching_data: SmallVec::new(),
                },
            });
        };
        if table_name.is_empty() || where_clause.is_empty() {
            return None;
        }

        // Desugared for the same reasons as `UPDATE`: quoted and qualified
        // names, the sugar predicate forms, and the guards that stop a `NULL`
        // row satisfying a comparison — which for a `DELETE` decides whether a
        // row is destroyed.
        let where_clause =
            match crate::core::tokenizer::predicate_rewrite::rewrite_scan_where(&where_clause, schema) {
                Ok(clause) => clause,
                Err(reason) => {
                    rejection.replace(reason);
                    return None;
                }
            };

        let where_columns = extract_where_columns(&where_clause, schema);
        let schema_calc = SchemaCalculator::default();
        let mut query_columns_fetching_data = SmallVec::new();
        for col_name in &where_columns {
            let field = schema
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(col_name.as_str()) && !f.is_deleted)?;
            let offset = schema_calc.calculate_schema_offset(&field.name, schema);
            query_columns_fetching_data.push(ColumnFetchingData {
                column_offset: offset.0,
                column_type: field.db_type.physical(),
                size: field.size,
                schema_id: offset.1,
            });
        }
        Some(DeleteRow {
            table_name: table_name.to_string(),
            where_clause,
            query_row_fetch: RowFetch {
                columns_fetching_data: query_columns_fetching_data,
            },
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db_type::DbType;
    use crate::core::row::schema::SchemaField;
    use crate::core::rql::lexer_s1::QueryPurpose;

    fn make_schema() -> SmallVec<[SchemaField; 20]> {
        smallvec::smallvec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::U64,
                size: 8,
                offset: 0,
                write_order: 0,
                is_unique: true,
                is_deleted: false,
                scale: None,
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 12,
                offset: 8,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
                scale: None,
            },
        ]
    }

    #[test]
    fn test_delete_row_basic() {
        let sql = r#"DELETE FROM users WHERE id = 1"#;
        let qp = QueryPurpose::DeleteRow(sql.to_string());
        let result =
            delete_row_from_query_purpose(&qp, &make_schema()).expect("Should parse successfully");
        assert_eq!(result.table_name, "users");
        assert_eq!(result.where_clause, "id = 1");
    }

    #[test]
    fn test_delete_row_with_quotes() {
        let sql = r#"DELETE FROM "my_table" WHERE name = 'Alice'"#;
        let qp = QueryPurpose::DeleteRow(sql.to_string());
        let result =
            delete_row_from_query_purpose(&qp, &make_schema()).expect("Should parse successfully");
        assert_eq!(result.table_name, "my_table");
        assert_eq!(result.where_clause, "name = 'Alice'");
    }

    #[test]
    fn test_delete_row_without_where_deletes_everything() {
        for sql in ["DELETE FROM users", "DELETE FROM users u", "DELETE FROM users AS u"] {
            let qp = QueryPurpose::DeleteRow(sql.to_string());
            let result = delete_row_from_query_purpose(&qp, &make_schema())
                .unwrap_or_else(|| panic!("{sql} should parse"));
            assert_eq!(result.table_name, "users");
            assert_eq!(result.where_clause, "");
        }
        // Anything after the table that is not an alias is a clause nobody
        // parsed, and must not be read as "no condition".
        for sql in [
            "DELETE FROM users WHERE",
            "DELETE FROM users u v",
            "DELETE FROM users AS",
            "DELETE FROM users id = 1",
        ] {
            let qp = QueryPurpose::DeleteRow(sql.to_string());
            assert!(delete_row_from_query_purpose(&qp, &make_schema()).is_none(), "{sql}");
        }
    }

    #[test]
    fn test_delete_row_wrong_form_should_fail() {
        let sql = r#"REMOVE FROM users WHERE id = 1"#;
        let qp = QueryPurpose::DeleteRow(sql.to_string());
        assert!(delete_row_from_query_purpose(&qp, &make_schema()).is_none());
    }
}
