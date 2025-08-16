use crate::core::{db_type::DbType, rql_v2::lexer_s1::QueryPurpose};

pub struct CreateColumnData {
    pub name: String,
    pub data_type: DbType,
}

pub struct CreateTable {
    pub table_name: String,
    pub columns: Vec<CreateColumnData>,
    pub is_immutable: bool,
}

/// Attempts to parse a QueryPurpose::CreateTable variant into a CreateTable struct.
/// Returns None if the QueryPurpose is not CreateTable or parsing fails.
pub fn create_table_from_query_purpose(qp: &QueryPurpose) -> Option<CreateTable> {
    if let QueryPurpose::CreateTable(sql) = qp {
        let sql = sql.as_str();
        // Robustly extract table name after CREATE and TABLE, skipping arbitrary whitespace
        let sql_upper = sql.to_ascii_uppercase();
        let mut words = sql_upper.split_whitespace();
        if words.next()? != "CREATE" { return None; }
        if words.next()? != "TABLE" { return None; }
        // Find the start of the table name in the original string
        let mut after_create_table = sql;
        for _ in 0..2 {
            let idx = after_create_table.find(|c: char| !c.is_whitespace())?;
            after_create_table = &after_create_table[idx..];
            let ws = after_create_table.find(char::is_whitespace).unwrap_or(after_create_table.len());
            after_create_table = &after_create_table[ws..];
        }
        let idx = after_create_table.find(|c: char| !c.is_whitespace())?;
        after_create_table = &after_create_table[idx..];
        // Table name is up to the next whitespace or '('
        let paren_idx = after_create_table.find('(').unwrap_or(after_create_table.len());
        let mut table_name = after_create_table[..paren_idx].trim();
        if let Some(ws_idx) = table_name.find(char::is_whitespace) {
            table_name = &table_name[..ws_idx];
        }
        let table_name = table_name.trim_matches('"').to_string();
        // Find the part between the first '(' and the last ')'
        let start = sql.find('(')?;
        let end = sql.rfind(')')?;
        let columns_str = &sql[start+1..end];
        let mut columns = Vec::new();
        for col_def in columns_str.split(',') {
            let col_def = col_def.trim();
            if col_def.is_empty() { continue; }
            // Split by whitespace to get name and type
            let mut parts = col_def.split_whitespace();
            let name = parts.next()?.trim_matches('"').to_string();
            let type_str = parts.next()?;
            // Map SQL type to DbType (very basic mapping, extend as needed)
            let data_type = match type_str.to_ascii_uppercase().as_str() {
                // Signed ints
                "BIGINT" | "INT8" => DbType::I64,
                "INT" | "INTEGER" | "INT4" => DbType::I32,
                "SMALLINT" | "INT2" => DbType::I16,
                "TINYINT" | "INT1" => DbType::I8,
                // Unsigned ints
                "UBIGINT" | "UINT64" | "UINT8" => DbType::U64,
                "UINT" | "UINT32" | "UINT4" => DbType::U32,
                "USMALLINT" | "UINT16" | "UINT2" => DbType::U16,
                "UTINYINT" | "UINT1" => DbType::U8,
                // Float
                "FLOAT" | "REAL" => DbType::F32,
                "DOUBLE" | "DOUBLE PRECISION" => DbType::F64,
                // Char/String
                "CHAR" => DbType::CHAR,
                "TEXT" | "VARCHAR" | "STRING" => DbType::STRING,
                // Date/time
                "DATETIME" | "TIMESTAMP" => DbType::DATETIME,
                // Boolean
                "BOOL" | "BOOLEAN" => DbType::U8,
                _ => return None
            };
            columns.push(CreateColumnData { name, data_type });
        }
        Some(CreateTable { table_name, columns, is_immutable: false })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_all_types() {
        let sql = r#"CREATE TABLE all_types (
            i8_col INT1,
            i16_col INT2,
            i32_col INT4,
            i64_col INT8,
            u8_col UINT1,
            u16_col UINT2,
            u32_col UINT4,
            u64_col UINT8,
            float_col FLOAT,
            real_col REAL,
            double_col DOUBLE PRECISION,
            char_col CHAR,
            text_col TEXT,
            varchar_col VARCHAR,
            string_col STRING,
            datetime_col DATETIME,
            timestamp_col TIMESTAMP,
            bool_col BOOL,
            boolean_col BOOLEAN
        )"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let result = create_table_from_query_purpose(&qp).expect("Should parse successfully");
        let types = [
            DbType::I8, DbType::I16, DbType::I32, DbType::I64,
            DbType::U8, DbType::U16, DbType::U32, DbType::U64,
            DbType::F32, DbType::F32, DbType::F64,
            DbType::CHAR, DbType::STRING, DbType::STRING, DbType::STRING,
            DbType::DATETIME, DbType::DATETIME,
            DbType::U8, DbType::U8
        ];
        let names = [
            "i8_col", "i16_col", "i32_col", "i64_col",
            "u8_col", "u16_col", "u32_col", "u64_col",
            "float_col", "real_col", "double_col",
            "char_col", "text_col", "varchar_col", "string_col",
            "datetime_col", "timestamp_col",
            "bool_col", "boolean_col"
        ];
        assert_eq!(result.table_name, "all_types");
        assert_eq!(result.columns.len(), types.len());
        for (i, col) in result.columns.iter().enumerate() {
            assert_eq!(col.name, names[i]);
            assert_eq!(&col.data_type, &types[i], "Type mismatch for column {}", col.name);
        }
    }

    #[test]
    fn test_create_table_from_query_purpose() {
        let sql = r#"CREATE TABLE "users" (
            id INT,
            name TEXT,
            age INT
        )"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let result = create_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "users");
        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].data_type, DbType::I32);
        assert_eq!(result.columns[1].name, "name");
        assert_eq!(result.columns[1].data_type, DbType::STRING);
        assert_eq!(result.columns[2].name, "age");
        assert_eq!(result.columns[2].data_type, DbType::I32);
    }

    #[test]
    fn test_create_table_with_various_types_and_quotes() {
        let sql = r#"CREATE TABLE "complex_table" (
            "id" INT4,
            "username" VARCHAR,
            "score" DOUBLE PRECISION,
            "created_at" TIMESTAMP,
            "is_active" INT1,
            "desc" TEXT
        )"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let result = create_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "complex_table");
        assert_eq!(result.columns.len(), 6);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[0].data_type, DbType::I32);
        assert_eq!(result.columns[1].name, "username");
        assert_eq!(result.columns[1].data_type, DbType::STRING);
        assert_eq!(result.columns[2].name, "score");
        assert_eq!(result.columns[2].data_type, DbType::F64);
        assert_eq!(result.columns[3].name, "created_at");
        assert_eq!(result.columns[3].data_type, DbType::DATETIME);
        assert_eq!(result.columns[4].name, "is_active");
        assert_eq!(result.columns[4].data_type, DbType::I8);
        assert_eq!(result.columns[5].name, "desc");
        assert_eq!(result.columns[5].data_type, DbType::STRING);
    }

    #[test]
    fn test_create_table_with_extra_spaces_and_newlines() {
        let sql = r#"  CREATE   TABLE   test_space   (
            col1   INT  ,
            col2   TEXT
        )  "#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let result = create_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "test_space");
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "col1");
        assert_eq!(result.columns[0].data_type, DbType::I32);
        assert_eq!(result.columns[1].name, "col2");
        assert_eq!(result.columns[1].data_type, DbType::STRING);
    }

    #[test]
    #[should_panic(expected = "Should not parse successfully")]
    fn test_create_table_with_unknown_type() {
        let sql = r#"CREATE TABLE t1 (foo BAR)"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let _result = create_table_from_query_purpose(&qp).expect("Should not parse successfully");
    }

    #[test]
    fn test_create_table_with_no_columns() {
        let sql = r#"CREATE TABLE empty_table ()"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        let result = create_table_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "empty_table");
        assert_eq!(result.columns.len(), 0);
    }

    #[test]
    fn test_create_table_invalid_sql() {
        let sql = r#"CREATE TABLE missing_paren id INT, name TEXT"#;
        let qp = QueryPurpose::CreateTable(sql.to_string());
        assert!(create_table_from_query_purpose(&qp).is_none());
    }
}
