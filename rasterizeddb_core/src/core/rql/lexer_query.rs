use itertools::Itertools;
use log::info;
use smallvec::SmallVec;
use std::collections::HashSet;

use crate::core::{processor::transformer::ComparerOperation, row::{common::simd_compare_strings, row::{ColumnFetchingData, RowFetch}, schema::{SchemaCalculator, SchemaField}}, rql::lexer_s1::QueryPurpose};

#[derive(Debug)]
pub struct QueryRows {
    pub table_name: String,
    pub query_row_fetch: RowFetch,
    pub requested_row_fetch: RowFetch,
    pub where_clause: String,
    pub limit: Option<u64>,
    pub order_by: Option<String>,
}

/// Extracts column names referenced in a WHERE clause
/// Uses simple string parsing to find potential column names by looking for schema field names
///
/// # Arguments
/// * `where_clause` - The WHERE clause string (without the WHERE keyword)
/// * `schema` - Vec<SchemaField> representing the table schema
///
/// # Returns
/// HashSet<String> - Set of column names found in the WHERE clause
fn extract_where_columns(
    where_clause: &str,
    schema: &SmallVec<[SchemaField; 20]>,
) -> HashSet<String> {
    let mut where_columns = HashSet::new();
    if where_clause.is_empty() {
        return where_columns;
    }

    let where_bytes = where_clause.as_bytes();

    // Check each schema field to see if it appears in the WHERE clause
    for field in schema.iter() {
        let field_bytes = field.name.as_bytes();

        // Look for the column name in the WHERE clause using SIMD comparison
        // We need to be careful about word boundaries to avoid false matches
        for i in 0..where_bytes.len().saturating_sub(field_bytes.len()) {
            if i + field_bytes.len() <= where_bytes.len() {
                let slice = &where_bytes[i..i + field_bytes.len()];
                if simd_compare_strings(slice, field_bytes, &ComparerOperation::Equals) {
                    // Check word boundaries - ensure we're not matching part of a larger word
                    let start_ok = i == 0 || !where_bytes[i - 1].is_ascii_alphanumeric();
                    let end_ok = i + field_bytes.len() == where_bytes.len()
                        || !where_bytes[i + field_bytes.len()].is_ascii_alphanumeric();

                    if start_ok && end_ok {
                        where_columns.insert(field.name.clone());
                        break; // Found this column, no need to keep searching
                    }
                }
            }
        }
    }

    where_columns
}

/// Parses a QueryPurpose::QueryRows variant and returns QueryRows with separate RowFetch structures
/// for query evaluation (WHERE clause columns) and result output (SELECT columns)
///
/// # Arguments
/// * `query_purpose` - QueryPurpose enum (should be QueryPurpose::QueryRows)  
/// * `schema` - Vec<SchemaField> representing the table schema
///
/// # Returns
/// QueryRows - Contains query_row_fetch (WHERE columns), requested_row_fetch (SELECT columns), and WHERE clause
pub fn row_fetch_from_select_query(
    query_purpose: &QueryPurpose,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Result<QueryRows, String> {
    let sql = match query_purpose {
        QueryPurpose::QueryRows(qr_data) => qr_data.query.trim(),
        _ => return Err("Not a SELECT/QueryRows query".to_string()),
    };
    let sql_upper = sql.to_ascii_uppercase();
    let sql_bytes = sql_upper.as_bytes();
    let select_prefix = b"SELECT ";
    let from_kw = b" FROM ";
    if !simd_compare_strings(
        &sql_bytes[..select_prefix.len().min(sql_bytes.len())],
        select_prefix,
        &ComparerOperation::Equals,
    ) {
        return Err("Not a SELECT query".to_string());
    }
    let mut from_pos = None;
    for i in 0..sql_bytes.len().saturating_sub(from_kw.len()) {
        if simd_compare_strings(
            &sql_bytes[i..i + from_kw.len()],
            from_kw,
            &ComparerOperation::Equals,
        ) {
            from_pos = Some(i);
            break;
        }
    }
    let from_pos = from_pos.ok_or("Missing FROM in SELECT query")?;

    let columns_part = sql[select_prefix.len()..from_pos].trim();

    let after_from = &sql[from_pos + from_kw.len()..];
    let after_from_upper = &sql_upper[from_pos + from_kw.len()..];

    // Find optional clauses (positions are within after_from).
    let where_pos = after_from_upper.find(" WHERE ");
    let order_pos = after_from_upper.find(" ORDER BY ");
    let limit_pos = after_from_upper.find(" LIMIT ");

    // Table name ends at the earliest clause marker.
    let table_end = [where_pos, order_pos, limit_pos]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(after_from.len());

    let table_part = after_from[..table_end].trim();

    // WHERE clause is everything after WHERE up to ORDER BY / LIMIT.
    let where_part = if let Some(wp) = where_pos {
        let start = wp + " WHERE ".len();
        let end = [order_pos, limit_pos]
            .into_iter()
            .flatten()
            .filter(|p| *p >= start)
            .min()
            .unwrap_or(after_from.len());
        after_from[start..end].trim()
    } else {
        ""
    };

    // ORDER BY field (single field for now), optionally with ASC/DESC.
    let order_by = if let Some(op) = order_pos {
        let start = op + " ORDER BY ".len();
        let end = [limit_pos]
            .into_iter()
            .flatten()
            .filter(|p| *p >= start)
            .min()
            .unwrap_or(after_from.len());
        let expr = after_from[start..end].trim();
        if expr.is_empty() {
            return Err("ORDER BY is present but no field was provided".to_string());
        }
        let field_token = expr
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_matches('"');
        if field_token.is_empty() {
            return Err("ORDER BY is present but no field was provided".to_string());
        }

        let schema_field = schema
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(field_token))
            .ok_or_else(|| format!("ORDER BY field '{}' not found in schema", field_token))?;

        Some(schema_field.name.clone())
    } else {
        None
    };

    // LIMIT value
    let limit = if let Some(lp) = limit_pos {
        let start = lp + " LIMIT ".len();
        let expr = after_from[start..].trim();
        if expr.is_empty() {
            return Err("LIMIT is present but no value was provided".to_string());
        }
        let token = expr.split_whitespace().next().unwrap_or("");
        let n = token
            .parse::<u64>()
            .map_err(|_| format!("Invalid LIMIT value '{}'", token))?;
        Some(n)
    } else {
        None
    };

    // Parse requested columns (SELECT columns)
    let requested_columns: Vec<&str> = if columns_part == "*" {
        schema
            .iter()
            .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
            .map(|f| f.name.as_str())
            .collect()
    } else {
        columns_part.split(',').map(|s| s.trim()).collect()
    };

    // Extract columns needed for WHERE clause evaluation
    let where_columns = extract_where_columns(where_part, schema);

    let schema_calc = SchemaCalculator::default();

    // Build requested_row_fetch for SELECT columns
    let mut requested_columns_fetching_data = SmallVec::new();
    for col in &requested_columns {
        let field = schema
            .iter()
            .find(|f| {
                simd_compare_strings(
                    f.name.as_bytes(),
                    col.as_bytes(),
                    &ComparerOperation::Equals,
                )
            })
            .ok_or_else(|| format!("Column '{}' not found in schema", col))?;
        let offset = schema_calc.calculate_schema_offset(&field.name, schema);
        requested_columns_fetching_data.push(ColumnFetchingData {
            column_offset: offset.0,
            column_type: field.db_type.clone(),
            size: field.size,
            schema_id: offset.1,
        });
    }

    // Build query_row_fetch for WHERE clause columns
    let mut query_columns_fetching_data = SmallVec::new();
    for col_name in &where_columns {
        let field = schema
            .iter()
            .find(|f| {
                simd_compare_strings(
                    f.name.as_bytes(),
                    col_name.as_bytes(),
                    &ComparerOperation::Equals,
                )
            })
            .ok_or_else(|| format!("WHERE column '{}' not found in schema", col_name))?;
        let offset = schema_calc.calculate_schema_offset(&field.name, schema);
        query_columns_fetching_data.push(ColumnFetchingData {
            column_offset: offset.0,
            column_type: field.db_type.clone(),
            size: field.size,
            schema_id: offset.1,
        });
    }

    let query_rows = QueryRows {
        table_name: table_part.to_string(),
        query_row_fetch: RowFetch {
            columns_fetching_data: query_columns_fetching_data,
        },
        requested_row_fetch: RowFetch {
            columns_fetching_data: requested_columns_fetching_data,
        },
        where_clause: where_part.to_string(),
        limit,
        order_by,
    };

    info!("Parsed QueryRows: {:?}", query_rows);

    Ok(query_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db_type::DbType;

    fn make_schema() -> SmallVec<[SchemaField; 20]> {
        smallvec::smallvec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::U64,
                size: 8,
                offset: 0,
                write_order: 0,
                is_unique: true,
                is_deleted: false
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 8,
                write_order: 1,
                is_unique: false,
                is_deleted: false
            },
            SchemaField {
                name: "age".to_string(),
                db_type: DbType::U8,
                size: 1,
                offset: 40,
                write_order: 2,
                is_unique: false,
                is_deleted: false
            },
            SchemaField {
                name: "email".to_string(),
                db_type: DbType::STRING,
                size: 64,
                offset: 41,
                write_order: 3,
                is_unique: false,
                is_deleted: false
            },
        ]
    }

    #[test]
    fn test_select_all_columns() {
        let schema = make_schema();
        let query = "SELECT * FROM users WHERE age > 18";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();
        assert_eq!(
            qr.requested_row_fetch.columns_fetching_data.len(),
            schema.len()
        );
        assert_eq!(qr.query_row_fetch.columns_fetching_data.len(), 1); // Only 'age' for WHERE clause
        assert_eq!(qr.where_clause, "age > 18");
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.limit, None);
        assert_eq!(qr.order_by, None);
    }

    #[test]
    fn test_select_specific_columns() {
        let schema = make_schema();
        let query = "SELECT id, name FROM users WHERE name = 'John'";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();
        assert_eq!(qr.requested_row_fetch.columns_fetching_data.len(), 2);
        assert_eq!(
            qr.requested_row_fetch.columns_fetching_data[0].column_type,
            DbType::U64
        );
        assert_eq!(
            qr.requested_row_fetch.columns_fetching_data[1].column_type,
            DbType::STRING
        );
        assert_eq!(qr.query_row_fetch.columns_fetching_data.len(), 1); // Only 'name' for WHERE clause
        assert_eq!(qr.where_clause, "name = 'John'");
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.limit, None);
        assert_eq!(qr.order_by, None);
    }

    #[test]
    fn test_select_no_where() {
        let schema = make_schema();
        let query = "SELECT id, email FROM users";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();
        assert_eq!(qr.requested_row_fetch.columns_fetching_data.len(), 2);
        assert_eq!(qr.query_row_fetch.columns_fetching_data.len(), 0); // No WHERE clause, so no query columns
        assert_eq!(qr.where_clause, "");
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.limit, None);
        assert_eq!(qr.order_by, None);
    }

    #[test]
    fn test_column_not_found() {
        let schema = make_schema();
        let query = "SELECT id, not_a_column FROM users";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let result = row_fetch_from_select_query(&qp, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_simd_compare_strings_for_column_names() {
        let schema = make_schema();
        let col_name = "name";
        let col_bytes = col_name.as_bytes();
        let found = schema.iter().any(|f| {
            simd_compare_strings(f.name.as_bytes(), col_bytes, &ComparerOperation::Equals)
        });
        assert!(found);
        let not_found = schema.iter().any(|f| {
            simd_compare_strings(
                f.name.as_bytes(),
                b"not_a_column",
                &ComparerOperation::Equals,
            )
        });
        assert!(!not_found);
    }

    #[test]
    fn test_simd_compare_strings_for_where_clause() {
        // Simulate a WHERE clause comparison using SIMD
        let clause = "name = 'John'";
        let target = "name = 'John'";
        let result = simd_compare_strings(
            clause.as_bytes(),
            target.as_bytes(),
            &ComparerOperation::Equals,
        );
        assert!(result);
        let not_result =
            simd_compare_strings(clause.as_bytes(), b"age > 18", &ComparerOperation::Equals);
        assert!(!not_result);
    }

    #[test]
    fn test_query_vs_requested_columns() {
        let schema = make_schema();
        let query = "SELECT id, name, email FROM users WHERE age > 18 AND name CONTAINS 'John'";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();

        // Requested columns should be id, name, email (3 columns)
        assert_eq!(qr.requested_row_fetch.columns_fetching_data.len(), 3);

        // Query columns should be age and name (2 columns from WHERE clause)
        assert_eq!(qr.query_row_fetch.columns_fetching_data.len(), 2);

        assert_eq!(qr.where_clause, "age > 18 AND name CONTAINS 'John'");
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.limit, None);
        assert_eq!(qr.order_by, None);
    }

    #[test]
    fn test_complex_where_clause_column_extraction() {
        let schema = make_schema();
        let query = "SELECT name FROM users WHERE id = 999 AND age > 21";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();

        // Requested columns should be just name (1 column)
        assert_eq!(qr.requested_row_fetch.columns_fetching_data.len(), 1);

        // Query columns should be id and age (2 columns from WHERE clause)
        assert_eq!(qr.query_row_fetch.columns_fetching_data.len(), 2);

        assert_eq!(qr.where_clause, "id = 999 AND age > 21");
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.limit, None);
        assert_eq!(qr.order_by, None);
    }

    #[test]
    fn test_select_with_order_by_and_limit() {
        let schema = make_schema();
        let query = "SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();

        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.where_clause, "age > 18");
        assert_eq!(qr.order_by.as_deref(), Some("name"));
        assert_eq!(qr.limit, Some(10));
    }

    #[test]
    fn test_select_with_limit_only() {
        let schema = make_schema();
        let query = "SELECT id FROM users LIMIT 5";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let qr = row_fetch_from_select_query(&qp, &schema).unwrap();
        assert_eq!(qr.table_name, "users");
        assert_eq!(qr.where_clause, "");
        assert_eq!(qr.order_by, None);
        assert_eq!(qr.limit, Some(5));
    }

    #[test]
    fn test_select_with_order_by_unknown_column_should_fail() {
        let schema = make_schema();
        let query = "SELECT id FROM users ORDER BY not_a_column";
        let qp = crate::core::rql::lexer_s1::QueryPurpose::QueryRows(
            crate::core::rql::lexer_s1::QueryRowsData {
                table_name: "users".to_string(),
                query: query.to_string(),
            },
        );
        let result = row_fetch_from_select_query(&qp, &schema);
        assert!(result.is_err());
    }
}
