use crate::core::rql_v2::lexer_s1::QueryPurpose;

pub struct RenameColumn {
	pub table_name: String,
	pub old_name: String,
	pub new_name: String,
}

/// Attempts to parse a QueryPurpose::RenameColumn variant into a RenameColumn struct.
/// Returns None if the QueryPurpose is not RenameColumn or parsing fails.
pub fn rename_column_from_query_purpose(qp: &QueryPurpose) -> Option<RenameColumn> {
	if let QueryPurpose::RenameColumn(sql) = qp {
		let sql = sql.as_str();
		let sql_upper = sql.to_ascii_uppercase();
		if !sql_upper.starts_with("ALTER TABLE") {
			return None;
		}
		// ALTER TABLE table_name RENAME COLUMN old_name TO new_name;
		let mut parts = sql.split_whitespace();
		parts.next(); // ALTER
		parts.next(); // TABLE
		let table_name = parts.next()?.trim_matches('"');
		// Find "RENAME" and "COLUMN"
		while let Some(word) = parts.next() {
			if word.to_ascii_uppercase() == "RENAME" {
				if let Some(next_word) = parts.next() {
					if next_word.to_ascii_uppercase() == "COLUMN" {
						break;
					}
				}
			}
		}
		let old_name = parts.next()?.trim_matches('"');
		let to_word = parts.next()?;
		if to_word.to_ascii_uppercase() != "TO" {
			return None;
		}
		let new_name = parts.next()?.trim_matches('"');
		let mut parts_check = parts.clone();
		if old_name.is_empty() || new_name.is_empty() || parts_check.next().is_some() {
			return None;
		}
		Some(RenameColumn {
			table_name: table_name.to_string(),
			old_name: old_name.to_string(),
			new_name: new_name.to_string(),
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
	fn test_rename_column_alter_table_form() {
		let sql = r#"ALTER TABLE users RENAME COLUMN age TO years"#;
		let qp = QueryPurpose::RenameColumn(sql.to_string());
		let result = rename_column_from_query_purpose(&qp).expect("Should parse successfully");
		assert_eq!(result.table_name, "users");
		assert_eq!(result.old_name, "age");
		assert_eq!(result.new_name, "years");
	}

	#[test]
	fn test_rename_column_with_quotes() {
		let sql = r#"ALTER TABLE "my_table" RENAME COLUMN "flag" TO "is_active""#;
		let qp = QueryPurpose::RenameColumn(sql.to_string());
		let result = rename_column_from_query_purpose(&qp).expect("Should parse successfully");
		assert_eq!(result.table_name, "my_table");
		assert_eq!(result.old_name, "flag");
		assert_eq!(result.new_name, "is_active");
	}

	#[test]
	fn test_rename_column_invalid_sql() {
		let sql = r#"ALTER TABLE t1 RENAME COLUMN age"#;
		let qp = QueryPurpose::RenameColumn(sql.to_string());
		assert!(rename_column_from_query_purpose(&qp).is_none());
	}

	#[test]
	fn test_rename_column_wrong_form_should_fail() {
		let sql = r#"RENAME COLUMN users age TO years"#;
		let qp = QueryPurpose::RenameColumn(sql.to_string());
		assert!(rename_column_from_query_purpose(&qp).is_none());
	}
}
