use crate::core::rql_v2::lexer_s1::QueryPurpose;

pub struct DropTable {
    pub table_name: String,
}

/// Attempts to parse a QueryPurpose::DropTable variant into a DropTable struct.
/// Returns None if the QueryPurpose is not DropTable or parsing fails.
pub fn drop_table_from_query_purpose(qp: &QueryPurpose) -> Option<DropTable> {
    if let QueryPurpose::DropTable(sql) = qp {
        let sql = sql.as_str();
        // Robustly extract table name after DROP and TABLE, skipping arbitrary whitespace
        let sql_upper = sql.to_ascii_uppercase();
        let mut words = sql_upper.split_whitespace();
        if words.next()? != "DROP" {
            return None;
        }
        if words.next()? != "TABLE" {
            return None;
        }
        // Find the start of the table name in the original string
        let mut after_drop_table = sql;
        for _ in 0..2 {
            let idx = after_drop_table.find(|c: char| !c.is_whitespace())?;
            after_drop_table = &after_drop_table[idx..];
            let ws = after_drop_table
                .find(char::is_whitespace)
                .unwrap_or(after_drop_table.len());
            after_drop_table = &after_drop_table[ws..];
        }
        let idx = after_drop_table.find(|c: char| !c.is_whitespace())?;
        after_drop_table = &after_drop_table[idx..];
        // Table name is up to the next whitespace or ';' or end
        let mut table_name = after_drop_table.trim();
        if let Some(ws_idx) = table_name.find(char::is_whitespace) {
            table_name = &table_name[..ws_idx];
        }
        if let Some(semi_idx) = table_name.find(';') {
            table_name = &table_name[..semi_idx];
        }
        let table_name = table_name.trim_matches('"').to_string();
        if table_name.is_empty() {
            return None;
        }
        Some(DropTable { table_name })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::rql_v2::lexer_s1::QueryPurpose;

    #[test]
    fn test_drop_table_basic() {
        let sql = r#"DROP TABLE users"#;
        let qp = QueryPurpose::DropTable(sql.to_string());
        let result = drop_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "users");
    }

    #[test]
    fn test_drop_table_with_quotes_and_spaces() {
        let sql = r#"  DROP   TABLE   "users"  "#;
        let qp = QueryPurpose::DropTable(sql.to_string());
        let result = drop_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "users");
    }

    #[test]
    fn test_drop_table_with_semicolon() {
        let sql = r#"DROP TABLE test_table;"#;
        let qp = QueryPurpose::DropTable(sql.to_string());
        let result = drop_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "test_table");
    }

    #[test]
    fn test_drop_table_invalid_sql() {
        let sql = r#"DROP TABLE   "#;
        let qp = QueryPurpose::DropTable(sql.to_string());
        assert!(drop_table_from_query_purpose(&qp).is_none());
    }
}
