use log::{error, warn};
use smallvec::SmallVec;

use crate::core::row::row::{ColumnWritePayload, RowWrite};
use crate::core::row::schema::SchemaField;
use crate::core::{
    db_type::DbType,
    rql::lexer_s1::QueryPurpose,
};

use crate::memory_pool::{MEMORY_POOL, MemoryBlock};

/// Convert a string value to a MemoryBlock based on the database type
#[inline(always)]
fn value_to_mb(value_str: &str, db_type: &DbType) -> MemoryBlock {
    #[inline(always)]
    fn strip_surrounding_quotes(s: &str) -> &str {
        let s = s.trim();
        if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
            if s.len() >= 2 {
                return &s[1..s.len() - 1];
            }
        }
        s
    }

    #[inline(always)]
    fn parse_postgres_bool(s: &str) -> Option<u8> {
        // PostgreSQL accepts many inputs for bool:
        // https://www.postgresql.org/docs/current/datatype-boolean.html
        let lower = s.trim().to_ascii_lowercase();
        match lower.as_str() {
            "true" | "t" | "yes" | "y" | "on" | "1" => Some(1),
            "false" | "f" | "no" | "n" | "off" | "0" => Some(0),
            _ => None,
        }
    }

    match db_type {
        DbType::I8 => {
            let val: i8 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I16 => {
            let val: i16 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I32 => {
            let val: i32 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I64 => {
            let val: i64 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::I128 => {
            let val: i128 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U8 => {
            let val: u8 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U16 => {
            let val: u16 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U32 => {
            let val: u32 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U64 => {
            let val: u64 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::U128 => {
            let val: u128 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::F32 => {
            let val: f32 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::F64 => {
            let val: f64 = value_str.parse().unwrap_or_default();
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::BOOL => {
            let stripped = strip_surrounding_quotes(value_str);
            let val_u8 = parse_postgres_bool(stripped).unwrap_or_else(|| {
                warn!(
                    "Failed to parse BOOL value {:?}; defaulting to false",
                    value_str
                );
                0
            });
            let bytes = val_u8.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::DATETIME => {
            // Internal representation is 8 bytes.
            // Currently we accept integer epochs (seconds/ms/us) as i64.
            let stripped = strip_surrounding_quotes(value_str);
            let val: i64 = stripped.parse().unwrap_or_else(|_| {
                warn!(
                    "Failed to parse DATETIME value {:?} as i64; defaulting to 0",
                    value_str
                );
                0
            });
            let bytes = val.to_le_bytes();
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }
        DbType::STRING | DbType::CHAR => {
            let stripped = strip_surrounding_quotes(value_str);
            let b = stripped.as_bytes();
            let mut mb = MEMORY_POOL.acquire(b.len());
            mb.into_slice_mut().copy_from_slice(b);
            mb
        }
        _ => {
            panic!("Unsupported database type: {}", db_type);
        }
    }
}

/// Attempts to parse a QueryPurpose::InsertRow variant into a RowWrite object, using the provided schema.
/// Returns None if the QueryPurpose is not InsertRow or parsing fails.
/// Handles multi-line, whitespace, and trailing comma issues robustly.
pub fn insert_row_from_query_purpose(
    qp: &QueryPurpose,
    schema: &[SchemaField],
) -> Option<RowWrite> {
    if let QueryPurpose::InsertRow(sql) = qp {
        let sql = sql.query.as_str();
        // Trim leading whitespace and newlines
        let sql_trimmed =
            sql.trim_start_matches(|c: char| c.is_whitespace() || c == '\n' || c == '\r');
        let sql_upper = sql_trimmed.to_ascii_uppercase();
        if !sql_upper.starts_with("INSERT INTO") {
            error!("INSERT parse failed: query does not start with INSERT INTO. prefix={:?}", sql_trimmed.chars().take(80).collect::<String>());
            return None;
        }
        let sql = sql_trimmed;

        // --- Helper to extract parenthesis block robustly ---
        fn extract_paren_block(
            s: &str,
            open: char,
            close: char,
            start: usize,
        ) -> Option<(usize, usize)> {
            let mut depth = 0;
            let mut begin = None;
            let mut chars = s.char_indices().skip(start);
            while let Some((i, c)) = chars.next() {
                if c == open {
                    if depth == 0 {
                        begin = Some(i);
                    }
                    depth += 1;
                } else if c == close {
                    depth -= 1;
                    if depth == 0 {
                        return begin.map(|b| (b, i));
                    }
                }
            }
            None
        }

        // --- Helper to parse comma-separated values respecting quotes ---
        fn parse_csv_values(s: &str) -> Vec<String> {
            let mut values = Vec::new();
            let mut current_value = String::new();
            let mut in_quotes = false;
            let mut quote_char = '"';
            let mut chars = s.chars();

            while let Some(c) = chars.next() {
                match c {
                    '"' | '\'' if !in_quotes => {
                        in_quotes = true;
                        quote_char = c;
                    }
                    c if c == quote_char && in_quotes => {
                        in_quotes = false;
                    }
                    ',' if !in_quotes => {
                        let trimmed = current_value.trim();
                        // Strip surrounding quotes if present
                        let cleaned = if (trimmed.starts_with('"') && trimmed.ends_with('"'))
                            || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
                        {
                            &trimmed[1..trimmed.len() - 1]
                        } else {
                            trimmed
                        };
                        values.push(cleaned.to_string());
                        current_value.clear();
                    }
                    _ => {
                        current_value.push(c);
                    }
                }
            }

            // Don't forget the last value
            let trimmed = current_value.trim();
            if !trimmed.is_empty() {
                // Strip surrounding quotes if present
                let cleaned = if (trimmed.starts_with('"') && trimmed.ends_with('"'))
                    || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
                {
                    &trimmed[1..trimmed.len() - 1]
                } else {
                    trimmed
                };
                values.push(cleaned.to_string());
            }

            values
        }

        // Find columns block after "INSERT INTO table_name"
        let insert_kw = match sql_upper.find("INSERT INTO") {
            Some(v) => v,
            None => {
                error!("INSERT parse failed: could not find INSERT INTO keyword. prefix={:?}", sql_trimmed.chars().take(120).collect::<String>());
                return None;
            }
        };
        // Skip table name: find first '(' after "INSERT INTO"
        let table_paren_start = match sql[insert_kw..].find('(') {
            Some(v) => v + insert_kw,
            None => {
                error!("INSERT parse failed: missing column list parentheses after INSERT INTO. prefix={:?}", sql_trimmed.chars().take(200).collect::<String>());
                return None;
            }
        };
        let (col_start, col_end) = match extract_paren_block(sql, '(', ')', table_paren_start) {
            Some(v) => v,
            None => {
                error!("INSERT parse failed: could not extract column list parentheses block. prefix={:?}", sql_trimmed.chars().take(200).collect::<String>());
                return None;
            }
        };
        let columns_str = &sql[col_start + 1..col_end];

        // Find values block after "VALUES"
        let values_kw = match sql_upper.find("VALUES") {
            Some(v) => v,
            None => {
                error!("INSERT parse failed: missing VALUES keyword. prefix={:?}", sql_trimmed.chars().take(200).collect::<String>());
                return None;
            }
        };
        let values_kw_real = match sql[values_kw..].to_ascii_lowercase().find("values") {
            Some(v) => values_kw + v,
            None => {
                error!("INSERT parse failed: could not locate VALUES keyword in original query. prefix={:?}", sql_trimmed.chars().take(200).collect::<String>());
                return None;
            }
        };
        let values_paren_start = match sql[values_kw_real..].find('(') {
            Some(v) => v + values_kw_real,
            None => {
                error!("INSERT parse failed: missing VALUES(...) parentheses. prefix={:?}", sql_trimmed.chars().take(220).collect::<String>());
                return None;
            }
        };
        let (val_start, val_end) = match extract_paren_block(sql, '(', ')', values_paren_start) {
            Some(v) => v,
            None => {
                error!("INSERT parse failed: could not extract VALUES parentheses block. prefix={:?}", sql_trimmed.chars().take(220).collect::<String>());
                return None;
            }
        };
        let values_str = &sql[val_start + 1..val_end];

        // Parse columns (simple split since column names shouldn't contain commas)
        let columns: SmallVec<[&str; 32]> = columns_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.trim_matches('"').trim_matches('\''))
            .collect();

        // Parse values using the CSV parser that strips quotes
        let values = parse_csv_values(values_str);

        if columns.len() != values.len() {
            error!(
                "INSERT parse failed: columns/values count mismatch (columns={}, values={}). values_str_len={} columns_str_len={} sql_prefix={:?}",
                columns.len(),
                values.len(),
                values_str.len(),
                columns_str.len(),
                &sql_trimmed.chars().take(200).collect::<String>()
            );
            return None;
        }

        let mut columns_writing_data = SmallVec::<[ColumnWritePayload; 32]>::new();

        for (col_name, value) in columns.iter().zip(values.iter()) {
            // Find schema field
            let Some(schema_field) = schema.iter().find(|f| f.name == *col_name) else {
                error!(
                    "INSERT parse failed: column {:?} not found in schema (schema_fields={:?})",
                    col_name,
                    schema.iter().map(|f| f.name.as_str()).collect::<Vec<_>>()
                );
                return None;
            };
            // Convert value to MemoryBlock based on the database type
            let data = value_to_mb(value, &schema_field.db_type);

            let payload = ColumnWritePayload {
                data,
                write_order: schema_field.write_order as u32,
                column_type: schema_field.db_type.clone(),
                size: schema_field.size as u32,
            };
            columns_writing_data.push(payload);
        }

        // Sort by write_order before creating the RowWrite
        columns_writing_data.sort_by_key(|col| col.write_order);
        Some(RowWrite {
            columns_writing_data,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db_type::DbType;
    use crate::core::rql::lexer_s1::InsertRowData;

    #[test]
    fn test_insert_row_basic() {
        let schema = vec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 0,
                write_order: 0,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 4 + 8,
                offset: 4,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
            },
        ];
        let sql = r#"INSERT INTO users (id, name) VALUES (1, "Alice")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "users".to_string(),
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), 2);
        assert_eq!(row_write.columns_writing_data[0].column_type, DbType::I32);
        assert_eq!(
            row_write.columns_writing_data[1].column_type,
            DbType::STRING
        );
        assert_eq!(row_write.columns_writing_data[0].size, 4);
        assert_eq!(row_write.columns_writing_data[1].size, 12);
    }

    #[test]
    fn test_insert_row_mismatched_columns() {
        let schema = vec![SchemaField {
            name: "id".to_string(),
            db_type: DbType::I32,
            size: 4,
            offset: 0,
            write_order: 0,
            is_unique: false,
            is_deleted: false,
        }];
        let sql = r#"INSERT INTO users (id, name) VALUES (1, "Alice")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "users".to_string(),
        });
        assert!(insert_row_from_query_purpose(&qp, &schema).is_none());
    }

    #[test]
    fn test_insert_row_many_columns_full_check() {
        let schema = vec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 0,
                write_order: 0,
                is_unique: true,
                is_deleted: false,
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 16,
                offset: 4,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "age".to_string(),
                db_type: DbType::I16,
                size: 2,
                offset: 20,
                write_order: 2,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "email".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 22,
                write_order: 3,
                is_unique: true,
                is_deleted: false,
            },
            SchemaField {
                name: "active".to_string(),
                db_type: DbType::U8,
                size: 1,
                offset: 54,
                write_order: 4,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "score".to_string(),
                db_type: DbType::F64,
                size: 8,
                offset: 55,
                write_order: 5,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "created_at".to_string(),
                db_type: DbType::I64,
                size: 8,
                offset: 63,
                write_order: 6,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "notes".to_string(),
                db_type: DbType::STRING,
                size: 64,
                offset: 71,
                write_order: 7,
                is_unique: false,
                is_deleted: false,
            },
        ];

        let sql = r#"INSERT INTO users (id, name, age, email, active, score, created_at, notes) VALUES (42, "Bob", 27, "bob@example.com", 1, 99.5, 1680000000, "Test user")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "users".to_string(),
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), schema.len());

        for (i, payload) in row_write.columns_writing_data.iter().enumerate() {
            let field = &schema[i];
            assert_eq!(
                payload.write_order, field.write_order as u32,
                "write_order mismatch at col {}",
                i
            );
            assert_eq!(
                payload.column_type, field.db_type,
                "db_type mismatch at col {}",
                i
            );
            assert_eq!(
                payload.size, field.size as u32,
                "size mismatch at col {}",
                i
            );
            // Check data bytes (match binary representation for numeric types)
            let expected_value: Vec<u8> = match field.db_type {
                DbType::I32 => 42i32.to_le_bytes().to_vec(),
                DbType::I16 => 27i16.to_le_bytes().to_vec(),
                DbType::U8 => [1u8].to_vec(),
                DbType::F64 => 99.5f64.to_le_bytes().to_vec(),
                DbType::I64 => 1680000000i64.to_le_bytes().to_vec(),
                DbType::STRING => match field.name.as_str() {
                    "name" => b"Bob".to_vec(),
                    "email" => b"bob@example.com".to_vec(),
                    "notes" => b"Test user".to_vec(),
                    _ => panic!("Unexpected STRING column name: {}", field.name),
                },
                _ => panic!("Unexpected column type: {:?}", field.db_type),
            };
            let actual_bytes = payload.data.into_slice();
            assert_eq!(
                actual_bytes,
                expected_value.as_slice(),
                "data bytes mismatch at col {}",
                i
            );
        }

        // Check that write_order is sorted
        let mut last_order = 0;
        for payload in &row_write.columns_writing_data {
            assert!(payload.write_order >= last_order, "write_order not sorted");
            last_order = payload.write_order;
        }
    }

    #[test]
    fn test_insert_row_column_order_mixed() {
        let schema = vec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 0,
                write_order: 0,
                is_unique: true,
                is_deleted: false,
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 16,
                offset: 4,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "age".to_string(),
                db_type: DbType::I16,
                size: 2,
                offset: 20,
                write_order: 2,
                is_unique: false,
                is_deleted: false,
            },
        ];
        let sql = r#"INSERT INTO users (age, id, name) VALUES (30, 100, "Charlie")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "users".to_string(),
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), 3);

        // Should be sorted by write_order: id (0), name (1), age (2)
        assert_eq!(row_write.columns_writing_data[0].write_order, 0);
        assert_eq!(row_write.columns_writing_data[0].column_type, DbType::I32);
        assert_eq!(
            row_write.columns_writing_data[0].data.into_slice(),
            100i32.to_le_bytes()
        );

        assert_eq!(row_write.columns_writing_data[1].write_order, 1);
        assert_eq!(
            row_write.columns_writing_data[1].column_type,
            DbType::STRING
        );
        assert_eq!(
            row_write.columns_writing_data[1].data.into_slice(),
            b"Charlie"
        );

        assert_eq!(row_write.columns_writing_data[2].write_order, 2);
        assert_eq!(row_write.columns_writing_data[2].column_type, DbType::I16);
        assert_eq!(
            row_write.columns_writing_data[2].data.into_slice(),
            30i16.to_le_bytes()
        );
    }

    #[test]
    fn test_insert_row_edge_cases() {
        let schema = vec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 0,
                write_order: 0,
                is_unique: true,
                is_deleted: false,
            },
            SchemaField {
                name: "flag".to_string(),
                db_type: DbType::U8,
                size: 1,
                offset: 4,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "desc".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 5,
                write_order: 2,
                is_unique: false,
                is_deleted: false,
            },
        ];
        let sql = r#"INSERT INTO users (id, flag, desc) VALUES (0, 255, "Edge")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "users".to_string(),
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(
            row_write.columns_writing_data[0].data.into_slice(),
            0i32.to_le_bytes()
        );
        assert_eq!(row_write.columns_writing_data[1].data.into_slice(), [255u8]);
        assert_eq!(row_write.columns_writing_data[2].data.into_slice(), b"Edge");
    }

    #[test]
    fn test_insert_row_float_and_char() {
        let schema = vec![
            SchemaField {
                name: "score".to_string(),
                db_type: DbType::F64,
                size: 8,
                offset: 0,
                write_order: 0,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "grade".to_string(),
                db_type: DbType::CHAR,
                size: 1,
                offset: 8,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
            },
        ];
        let sql = r#"INSERT INTO results (score, grade) VALUES (3.14, "A")"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "results".to_string(),
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(
            row_write.columns_writing_data[0].data.into_slice(),
            3.14f64.to_le_bytes()
        );
        assert_eq!(row_write.columns_writing_data[1].data.into_slice(), b"A");
    }

    #[test]
    fn test_insert_row_with_whitespace_and_termination() {
        let schema = vec![
            SchemaField {
                name: "id".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 0,
                write_order: 0,
                is_unique: true,
                is_deleted: false,
            },
            SchemaField {
                name: "name".to_string(),
                db_type: DbType::STRING,
                size: 16,
                offset: 4,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
            },
        ];
        // Leading/trailing whitespace, tabs, newlines, and semicolon termination
        let sqls = [
            "   \n\tINSERT INTO users (id, name) VALUES (123, \"Test\")\n\t  ",
            "\r\nINSERT INTO users (id, name) VALUES (456, \"User\");\n",
            "\t\n  INSERT INTO users (id, name) VALUES (789, \"Whitespace\")  ;  \n",
        ];
        let expected = [
            123i32.to_le_bytes(),
            456i32.to_le_bytes(),
            789i32.to_le_bytes(),
        ];
        let expected_names = [&b"Test"[..], &b"User"[..], &b"Whitespace"[..]];
        for ((sql, exp_id), exp_name) in sqls.iter().zip(expected.iter()).zip(expected_names.iter())
        {
            let qp = QueryPurpose::InsertRow(InsertRowData {
                query: sql.to_string(),
                table_name: "users".to_string(),
            });
            let row_write =
                insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
            assert_eq!(row_write.columns_writing_data[0].data.into_slice(), exp_id);
            assert_eq!(
                row_write.columns_writing_data[1].data.into_slice(),
                *exp_name
            );
        }
    }

    #[test]
    fn test_insert_row_bool_postgres_literals() {
        let schema = vec![
            SchemaField {
                name: "ok".to_string(),
                db_type: DbType::BOOL,
                size: 1,
                offset: 0,
                write_order: 0,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "flag".to_string(),
                db_type: DbType::BOOL,
                size: 1,
                offset: 1,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
            },
        ];

        let sql = r#"INSERT INTO t (ok, flag) VALUES (true, 'f')"#;
        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "t".to_string(),
        });
        let row_write =
            insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), 2);
        assert_eq!(row_write.columns_writing_data[0].column_type, DbType::BOOL);
        assert_eq!(row_write.columns_writing_data[0].data.into_slice(), [1u8]);
        assert_eq!(row_write.columns_writing_data[1].column_type, DbType::BOOL);
        assert_eq!(row_write.columns_writing_data[1].data.into_slice(), [0u8]);

        let sql2 = r#"INSERT INTO t (ok, flag) VALUES (On, 0)"#;
        let qp2 = QueryPurpose::InsertRow(InsertRowData {
            query: sql2.to_string(),
            table_name: "t".to_string(),
        });
        let row_write2 =
            insert_row_from_query_purpose(&qp2, &schema).expect("Should parse successfully");
        assert_eq!(row_write2.columns_writing_data[0].data.into_slice(), [1u8]);
        assert_eq!(row_write2.columns_writing_data[1].data.into_slice(), [0u8]);
    }

    #[test]
    fn test_insert_row_employees_full_schema() {
        use crate::core::rql::lexer_s1::{InsertRowData, QueryPurpose};
        use crate::core::db_type::DbType;

        let schema = vec![
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
                size: 32,
                offset: 8,
                write_order: 1,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "job_title".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 40,
                write_order: 2,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "salary".to_string(),
                db_type: DbType::F32,
                size: 4,
                offset: 72,
                write_order: 3,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "department".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 76,
                write_order: 4,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "age".to_string(),
                db_type: DbType::I32,
                size: 4,
                offset: 108,
                write_order: 5,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "manager".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 112,
                write_order: 6,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "location".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 144,
                write_order: 7,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "hire_date".to_string(),
                db_type: DbType::U64,
                size: 8,
                offset: 176,
                write_order: 8,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "degree".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 184,
                write_order: 9,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "skills".to_string(),
                db_type: DbType::STRING,
                size: 64,
                offset: 216,
                write_order: 10,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "current_project".to_string(),
                db_type: DbType::STRING,
                size: 32,
                offset: 280,
                write_order: 11,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "performance_score".to_string(),
                db_type: DbType::F32,
                size: 4,
                offset: 312,
                write_order: 12,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "is_active".to_string(),
                db_type: DbType::U8,
                size: 1,
                offset: 316,
                write_order: 13,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "created_at".to_string(),
                db_type: DbType::U64,
                size: 8,
                offset: 317,
                write_order: 14,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "updated_at".to_string(),
                db_type: DbType::U64,
                size: 8,
                offset: 325,
                write_order: 15,
                is_unique: false,
                is_deleted: false,
            },
            SchemaField {
                name: "is_fired".to_string(),
                db_type: DbType::U8,
                size: 1,
                offset: 333,
                write_order: 16,
                is_unique: false,
                is_deleted: false,
            },
        ];

        let sql = r#"INSERT INTO employees (
            id, name, job_title, salary, department, age, manager, location, hire_date,
            degree, skills, current_project, performance_score, is_active,
            created_at, updated_at, is_fired
        ) VALUES (
            1, 'Alice Smith', 'Software Engineer', 120000.5, 'Engineering', 29, 'Bob Johnson', 'New York', 1680000000,
            'BSc Computer Science', 'Rust, SQL, Git', 'Migration Tool', 4.5, 1, 1680000001, 1680000002, 0
        )"#;

        let qp = QueryPurpose::InsertRow(InsertRowData {
            query: sql.to_string(),
            table_name: "employees".to_string(),
        });
        let row_write =
            super::insert_row_from_query_purpose(&qp, &schema).expect("Should parse successfully");
        assert_eq!(row_write.columns_writing_data.len(), schema.len());

        // Check a few representative fields:
        assert_eq!(row_write.columns_writing_data[0].column_type, DbType::U64);
        assert_eq!(
            row_write.columns_writing_data[0].data.into_slice(),
            1u64.to_le_bytes()
        );
        assert_eq!(
            row_write.columns_writing_data[1].column_type,
            DbType::STRING
        );
        assert_eq!(
            row_write.columns_writing_data[1].data.into_slice(),
            b"Alice Smith"
        );
        assert_eq!(
            row_write.columns_writing_data[2].column_type,
            DbType::STRING
        );
        assert_eq!(
            row_write.columns_writing_data[2].data.into_slice(),
            b"Software Engineer"
        );
        assert_eq!(row_write.columns_writing_data[3].column_type, DbType::F32);
        assert_eq!(
            row_write.columns_writing_data[3].data.into_slice(),
            120000.5f32.to_le_bytes()
        );
        assert_eq!(
            row_write.columns_writing_data[4].data.into_slice(),
            b"Engineering"
        );
        assert_eq!(
            row_write.columns_writing_data[5].data.into_slice(),
            29i32.to_le_bytes()
        );
        assert_eq!(
            row_write.columns_writing_data[6].data.into_slice(),
            b"Bob Johnson"
        );
        assert_eq!(
            row_write.columns_writing_data[7].data.into_slice(),
            b"New York"
        );
        assert_eq!(
            row_write.columns_writing_data[8].data.into_slice(),
            1680000000u64.to_le_bytes()
        );
        assert_eq!(
            row_write.columns_writing_data[9].data.into_slice(),
            b"BSc Computer Science"
        );
        assert_eq!(
            row_write.columns_writing_data[10].data.into_slice(),
            b"Rust, SQL, Git"
        );
        assert_eq!(
            row_write.columns_writing_data[11].data.into_slice(),
            b"Migration Tool"
        );
        assert_eq!(
            row_write.columns_writing_data[12].data.into_slice(),
            4.5f32.to_le_bytes()
        );
        assert_eq!(row_write.columns_writing_data[13].data.into_slice(), [1u8]);
        assert_eq!(
            row_write.columns_writing_data[14].data.into_slice(),
            1680000001u64.to_le_bytes()
        );
        assert_eq!(
            row_write.columns_writing_data[15].data.into_slice(),
            1680000002u64.to_le_bytes()
        );
        assert_eq!(row_write.columns_writing_data[16].data.into_slice(), [0u8]);
    }
}
