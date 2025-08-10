use crate::core::{row_v2::{common::simd_compare_strings, transformer::ComparerOperation}};

pub enum QueryPurpose {
    CreateTable(String),
    DropTable(String),
    AddColumn(String),
    DropColumn(String),
    RenameColumn(String),
    InsertRow(String),
    UpdateRow(String),
    DeleteRow(String),
    QueryRows(String),
}

pub fn test() -> bool {
    simd_compare_strings(b"hello", b"hello", &ComparerOperation::Equals)
}

/// Recognizes common PostgreSQL SQL and returns a QueryPurpose with the query string.
pub fn recognize_query_purpose(query: &str) -> Option<QueryPurpose> {
    use QueryPurpose::*;
    use ComparerOperation::StartsWith;
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
        Some(InsertRow(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"UPDATE", &StartsWith) {
        Some(UpdateRow(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"DELETE FROM", &StartsWith) {
        Some(DeleteRow(query_trimmed.to_string()))
    } else if simd_compare_strings(query_bytes, b"SELECT", &StartsWith) {
        Some(QueryRows(query_trimmed.to_string()))
    } else {
        None
    }
}
