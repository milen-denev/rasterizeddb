/// Step 1
pub mod lexer_s1;

// Create Table
pub mod lexer_ct;

/// Drop Table
pub mod lexer_dt;

/// Add Column
pub mod lexer_ac;

/// Drop Column
pub mod lexer_dc;

/// Rename Column
pub mod lexer_rc;

/// Insert Row
pub mod lexer_ir;

/// Update Row
pub mod lexer_ur;

/// Delete Row
pub mod lexer_dr;

/// Query Rows
pub mod lexer_query;

/// `ORDER BY` clause parsing, shared by the single-table and join planners.
pub mod order_by;

pub mod aggregate;
/// `HAVING` parsing and evaluation, shared by the single-table and join planners.
pub mod having;
pub mod column_default;
pub mod cte;
pub mod expr;
pub mod foreign_key;
pub mod join;
pub mod lock_set;
pub mod json_map;
pub mod path_index;
pub mod prepared;

pub mod catalog_views;
pub mod check;
pub mod executor;
pub mod generated;
pub mod lexer_alter;
pub mod nullable;
pub mod row_lock;
/// What a single-table `SELECT` reads: one table, or a partitioned table's
/// partitions under one snapshot.
pub(crate) mod row_source;
pub mod sequence;
/// Combining the branches of a `UNION`.
pub mod set_ops;
pub mod statement_snapshot;
pub mod subquery;
pub mod type_modifier;
/// Table-level and composite `UNIQUE` constraints.
pub mod unique_key;
pub mod window;

#[cfg(test)]
mod rename_column_tests;

#[cfg(test)]
mod alter_declaration_tests;

#[cfg(test)]
mod rename_column_crash_tests;

pub(crate) mod column_declarations;

#[cfg(feature = "s3_backup")]
pub mod backup_exec;
/// `BACKUP`, `RESTORE` and `SHOW BACKUPS`, behind the `s3_backup` feature.
#[cfg(feature = "s3_backup")]
pub mod lexer_backup;
