use crate::core::rql_v2::lexer_s1::QueryPurpose;

pub struct DeleteRow {
    pub table_name: String,
    pub where_clause: String,
}

/// Attempts to parse a QueryPurpose::DeleteRow variant into a DeleteRow struct.
/// Returns None if the QueryPurpose is not DeleteRow or parsing fails.
pub fn delete_row_from_query_purpose(qp: &QueryPurpose) -> Option<DeleteRow> {
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
        Some(DeleteRow {
            table_name: table_name.to_string(),
            where_clause,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::rql_v2::lexer_s1::QueryPurpose;

    #[test]
    fn test_delete_row_basic() {
        let sql = r#"DELETE FROM users WHERE id = 1"#;
        let qp = QueryPurpose::DeleteRow(sql.to_string());
        let result = delete_row_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "users");
        assert_eq!(result.where_clause, "id = 1");
    }

    #[test]
    fn test_delete_row_with_quotes() {
        let sql = r#"DELETE FROM "my_table" WHERE name = 'Alice'"#;
        let qp = QueryPurpose::DeleteRow(sql.to_string());
        let result = delete_row_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "my_table");
        assert_eq!(result.where_clause, "name = 'Alice'");
    }

    #[test]
    fn test_delete_row_missing_where_should_fail() {
        let sql = r#"DELETE FROM users"#;
        let qp = QueryPurpose::DeleteRow(sql.to_string());
        assert!(delete_row_from_query_purpose(&qp).is_none());
    }

    #[test]
    fn test_delete_row_wrong_form_should_fail() {
        let sql = r#"REMOVE FROM users WHERE id = 1"#;
        let qp = QueryPurpose::DeleteRow(sql.to_string());
        assert!(delete_row_from_query_purpose(&qp).is_none());
    }
}
