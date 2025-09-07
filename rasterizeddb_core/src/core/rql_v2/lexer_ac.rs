use crate::core::{db_type::DbType, rql_v2::lexer_s1::QueryPurpose};

pub struct AddColumn {
    pub table_name: String,
    pub column: AddColumnData,
}

pub struct AddColumnData {
    pub name: String,
    pub data_type: DbType,
}

/// Attempts to parse a QueryPurpose::AddColumn variant into an AddColumn struct.
/// Returns None if the QueryPurpose is not AddColumn or parsing fails.
pub fn add_column_from_query_purpose(qp: &QueryPurpose) -> Option<AddColumn> {
    if let QueryPurpose::AddColumn(sql) = qp {
        let sql = sql.as_str();
        let sql_upper = sql.to_ascii_uppercase();
        if !sql_upper.starts_with("ALTER TABLE") {
            return None;
        }
        // ALTER TABLE table_name ADD COLUMN column_name datatype;
        let mut parts = sql.split_whitespace();
        parts.next(); // ALTER
        parts.next(); // TABLE
        let table_name = parts.next()?.trim_matches('"');
        // Find "ADD" and "COLUMN"
        while let Some(word) = parts.next() {
            if word.to_ascii_uppercase() == "ADD" {
                if let Some(next_word) = parts.next() {
                    if next_word.to_ascii_uppercase() == "COLUMN" {
                        break;
                    }
                }
            }
        }
        let col_name = parts.next()?.trim_matches('"');
        let type_parts: Vec<&str> = parts.collect();
        if col_name.is_empty() || type_parts.is_empty() {
            return None;
        }
        let type_str = type_parts.join(" ");
        let type_str_upper = type_str.to_ascii_uppercase();
        let data_type = match type_str_upper.as_str() {
            // Signed ints
            "INT" | "INTEGER" | "INT4" => DbType::I32,
            "BIGINT" | "INT8" => DbType::I64,
            "SMALLINT" | "INT2" => DbType::I16,
            "TINYINT" | "INT1" => DbType::I8,
            // Unsigned ints
            "UINT" | "UINT32" | "UINT4" => DbType::U32,
            "UBIGINT" | "UINT64" | "UINT8" => DbType::U64,
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
            _ => return None,
        };
        let column = AddColumnData {
            name: col_name.to_string(),
            data_type,
        };
        Some(AddColumn {
            table_name: table_name.to_string(),
            column,
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
    fn test_add_column_add_column_form_should_fail() {
        let sql = r#"ADD COLUMN users age INT"#;
        let qp = QueryPurpose::AddColumn(sql.to_string());
        assert!(add_column_from_query_purpose(&qp).is_none());
    }

    #[test]
    fn test_add_column_alter_table_form() {
        let sql = r#"ALTER TABLE users ADD COLUMN age INT"#;
        let qp = QueryPurpose::AddColumn(sql.to_string());
        let result = add_column_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "users");
        assert_eq!(result.column.name, "age");
        assert_eq!(result.column.data_type, DbType::I32);
    }

    #[test]
    fn test_add_column_with_quotes_and_types() {
        let sql = r#"ALTER TABLE "my_table" ADD COLUMN "flag" BOOL"#;
        let qp = QueryPurpose::AddColumn(sql.to_string());
        let result = add_column_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "my_table");
        assert_eq!(result.column.name, "flag");
        assert_eq!(result.column.data_type, DbType::U8);
    }

    #[test]
    fn test_add_column_unsigned_types() {
        let sql = r#"ALTER TABLE t1 ADD COLUMN uval UINT4"#;
        let qp = QueryPurpose::AddColumn(sql.to_string());
        let result = add_column_from_query_purpose(&qp).expect("Should parse successfully");
        assert_eq!(result.table_name, "t1");
        assert_eq!(result.column.name, "uval");
        assert_eq!(result.column.data_type, DbType::U32);
    }

    #[test]
    fn test_add_column_all_types() {
        let types = [
            ("i8_col", "INT1", DbType::I8),
            ("i16_col", "INT2", DbType::I16),
            ("i32_col", "INT4", DbType::I32),
            ("i64_col", "INT8", DbType::I64),
            ("u8_col", "UINT1", DbType::U8),
            ("u16_col", "UINT2", DbType::U16),
            ("u32_col", "UINT4", DbType::U32),
            ("u64_col", "UINT8", DbType::U64),
            ("float_col", "FLOAT", DbType::F32),
            ("real_col", "REAL", DbType::F32),
            ("double_col", "DOUBLE PRECISION", DbType::F64),
            ("char_col", "CHAR", DbType::CHAR),
            ("text_col", "TEXT", DbType::STRING),
            ("varchar_col", "VARCHAR", DbType::STRING),
            ("string_col", "STRING", DbType::STRING),
            ("datetime_col", "DATETIME", DbType::DATETIME),
            ("timestamp_col", "TIMESTAMP", DbType::DATETIME),
            ("bool_col", "BOOL", DbType::U8),
            ("boolean_col", "BOOLEAN", DbType::U8),
        ];
        for (col, typ, expected) in types.iter() {
            let sql = format!("ALTER TABLE t1 ADD COLUMN {} {}", col, typ);
            let qp = QueryPurpose::AddColumn(sql);
            let result = add_column_from_query_purpose(&qp).expect("Should parse successfully");
            assert_eq!(result.table_name, "t1");
            assert_eq!(result.column.name, *col);
            assert_eq!(
                result.column.data_type, *expected,
                "Type mismatch for column {}",
                col
            );
        }
    }

    #[test]
    fn test_add_column_invalid_sql() {
        let sql = r#"ALTER TABLE t1 ADD COLUMN"#;
        let qp = QueryPurpose::AddColumn(sql.to_string());
        assert!(add_column_from_query_purpose(&qp).is_none());
    }
}
