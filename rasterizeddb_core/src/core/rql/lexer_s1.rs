use crate::core::{processor::transformer::ComparerOperation, row::common::simd_compare_strings};

#[derive(Debug)]
pub enum QueryPurpose {
    CreateTable(String),
    DropTable(String),
    AddColumn(String),
    DropColumn(String),
    RenameColumn(String),
    InsertRow(InsertRowData),
    UpdateRow(String),
    DeleteRow(String),
    QueryRows(QueryRowsData),
}

#[derive(Debug)]
pub struct InsertRowData {
    pub table_name: String,
    pub query: String,
}

#[derive(Debug)]
pub struct QueryRowsData {
    pub table_name: String,
    pub query: String,
}

/// Recognizes common PostgreSQL SQL and returns a QueryPurpose with the query string.
pub fn recognize_query_purpose(query: &str) -> Option<QueryPurpose> {
    use ComparerOperation::StartsWith;
    use QueryPurpose::*;
    let query_trimmed = query.trim_start();
    let query_upper = query_trimmed.to_ascii_uppercase();
    let query_bytes = query_upper.as_bytes();

    if simd_compare_strings(query_bytes, b"CREATE TABLE", &StartsWith) {
        Some(CreateTable(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"DROP TABLE", &StartsWith) {
        Some(DropTable(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"ALTER TABLE", &StartsWith) {
        let upper = query_trimmed.to_ascii_uppercase();
        if upper.contains("ADD COLUMN") {
            Some(AddColumn(query_trimmed.to_string()))
        } else if upper.contains("DROP COLUMN") {
            Some(DropColumn(query_trimmed.to_string()))
        } else if upper.contains("RENAME COLUMN") {
            Some(RenameColumn(query_trimmed.to_string()))
        } else {
            None
        }
    } else if simd_compare_strings(query_bytes, b"INSERT INTO", &StartsWith) {
        if let Some(table_name_extract) = query_trimmed.split_whitespace().nth(2) {
            Some(InsertRow(InsertRowData {
                table_name: table_name_extract.trim().to_string(),
                query: query_trimmed.to_string(),
            }))
        } else {
            None
        }
    } else if simd_compare_strings(query_bytes, b"UPDATE", &StartsWith) {
        Some(UpdateRow(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"DELETE FROM", &StartsWith) {
        Some(DeleteRow(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"SELECT", &StartsWith) {
        // Extract table name after "FROM"
        let tokens: Vec<&str> = query_trimmed.split_whitespace().collect();
        if let Some(from_index) = tokens.iter().position(|&t| t.eq_ignore_ascii_case("FROM")) {
            if let Some(table_name_extract) = tokens.get(from_index + 1) {
                Some(QueryRows(QueryRowsData {
                    table_name: table_name_extract.trim().to_string(),
                    query: query_trimmed.to_string(),
                }))
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}
