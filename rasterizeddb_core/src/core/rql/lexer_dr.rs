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

fn extract_where_columns(where_clause: &str, schema: &SmallVec<[SchemaField; 20]>) -> std::collections::HashSet<String> {
    let mut where_columns = std::collections::HashSet::new();
    if where_clause.is_empty() {
        return where_columns;
    }

    let where_bytes = where_clause.as_bytes();
    for field in schema.iter() {
        let field_bytes = field.name.as_bytes();
        for i in 0..where_bytes.len().saturating_sub(field_bytes.len()) {
            if i + field_bytes.len() <= where_bytes.len() {
                let slice = &where_bytes[i..i + field_bytes.len()];
                if simd_compare_strings(slice, field_bytes, &ComparerOperation::Equals) {
                    let start_ok = i == 0 || !where_bytes[i - 1].is_ascii_alphanumeric();
                    let end_ok = i + field_bytes.len() == where_bytes.len()
                        || !where_bytes[i + field_bytes.len()].is_ascii_alphanumeric();
                    if start_ok && end_ok {
                        where_columns.insert(field.name.clone());
                        break;
                    }
                }
            }
        }
    }

    where_columns
}

/// Attempts to parse a QueryPurpose::DeleteRow variant into a DeleteRow struct.
/// Returns None if the QueryPurpose is not DeleteRow or parsing fails.
pub fn delete_row_from_query_purpose(
    qp: &QueryPurpose,
    schema: &SmallVec<[SchemaField; 20]>,
) -> Option<DeleteRow> {
    if let QueryPurpose::DeleteRow(sql) = qp {
        let sql = sql.as_str();
        let sql_upper = sql.to_ascii_uppercase();
        if !sql_upper.starts_with("DELETE FROM") {
            return None;
        }
        // DELETE FROM table_name WHERE ...
        let mut parts = sql.split_whitespace();
        parts.next(); // DELETE
        parts.next(); // FROM
        let table_name = parts.next()?.trim_matches('"');
        // Find WHERE
        let where_idx = sql_upper.find("WHERE");
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
            String::new()
        };
        if table_name.is_empty() || where_clause.is_empty() {
            return None;
        }

        let where_columns = extract_where_columns(&where_clause, schema);
        let schema_calc = SchemaCalculator::default();
        let mut query_columns_fetching_data = SmallVec::new();
        for col_name in &where_columns {
            let field = schema
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(col_name.as_str()))?;
            let offset = schema_calc.calculate_schema_offset(&field.name, schema);
            query_columns_fetching_data.push(ColumnFetchingData {
                column_offset: offset.0,
                column_type: field.db_type.clone(),
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
    use crate::core::rql::lexer_s1::QueryPurpose;
    use crate::core::db_type::DbType;
    use crate::core::row::schema::SchemaField;

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
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 12,
                offset: 8,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
            },
        ]
    }

    #[test]
    fn test_delete_row_basic() {
        let sql = r#"DELETE FROM users WHERE id = 1"#;
        let qp = QueryPurpose::DeleteRow(sql.to_string());
        let result = delete_row_from_query_purpose(&qp, &make_schema()).expect("Should parse successfully");
        assert_eq!(result.table_name, "users");
        assert_eq!(result.where_clause, "id = 1");
    }

    #[test]
    fn test_delete_row_with_quotes() {
        let sql = r#"DELETE FROM "my_table" WHERE name = 'Alice'"#;
        let qp = QueryPurpose::DeleteRow(sql.to_string());
        let result = delete_row_from_query_purpose(&qp, &make_schema()).expect("Should parse successfully");
        assert_eq!(result.table_name, "my_table");
        assert_eq!(result.where_clause, "name = 'Alice'");
    }

    #[test]
    fn test_delete_row_missing_where_should_fail() {
        let sql = r#"DELETE FROM users"#;
        let qp = QueryPurpose::DeleteRow(sql.to_string());
        assert!(delete_row_from_query_purpose(&qp, &make_schema()).is_none());
    }

    #[test]
    fn test_delete_row_wrong_form_should_fail() {
        let sql = r#"REMOVE FROM users WHERE id = 1"#;
        let qp = QueryPurpose::DeleteRow(sql.to_string());
        assert!(delete_row_from_query_purpose(&qp, &make_schema()).is_none());
    }
}
