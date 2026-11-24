//! `update_many`: many rows changed by one `UPDATE … CASE` statement, all or
//! nothing.

use std::collections::HashMap;

use super::{
    ColumnRole, Model, Write, decode_models, ident, maintained_sets, primary_key, select_list,
    soft_delete_column, version_column,
};
use crate::executor::Executor;
use crate::value::Value;
use crate::{Error, Result};

/// Bytes of statement text [`ModelExt::update_many`](super::ModelExt::update_many)
/// sends, at most. Past it the call is refused: splitting the change into
/// several statements would give up its atomicity.
pub const UPDATE_MANY_MAX_BYTES: usize = 1 << 20;

/// One row's key, as SQL: its literal (one key column) or each column's.
struct RowKey {
    literals: Vec<String>,
}

impl RowKey {
    fn of<M: Model>(m: &M, key: &[usize]) -> Result<RowKey> {
        let mut literals = Vec::with_capacity(key.len());
        for &i in key {
            let mut out = String::new();
            m.write_value(i, &mut out)?;
            if out == "NULL" {
                return Err(Error::Param(format!(
                    "{}.{} is NULL in a row given to update_many",
                    M::TABLE,
                    M::COLUMNS[i].name
                )));
            }
            literals.push(out);
        }
        Ok(RowKey { literals })
    }

    /// `a = 1 AND b = 2`.
    fn predicate(&self, names: &[String]) -> String {
        names
            .iter()
            .zip(&self.literals)
            .map(|(n, l)| format!("{n} = {l}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn text(&self) -> String {
        self.literals.join(", ")
    }
}

/// What `update_many` sends, and what it needs to explain a refusal.
pub(crate) struct UpdateMany {
    pub(crate) sql: String,
    /// `SELECT <key…>, <version> FROM t WHERE <the rows' keys>`: read after
    /// a refusal to say which rows were missing or moved.
    probe: String,
    keys: Vec<String>,
    versions: Vec<Option<String>>,
}

/// `UPDATE t SET a = CASE id WHEN 1 THEN CAST(… AS <type>) … END, …,
/// version = version + 1,
/// updated_at = now() WHERE id IN (…) AND (SELECT COUNT(*) FROM t WHERE
/// <every row's key, and version>) = n RETURNING …`.
///
/// The count guard makes it all or nothing: when a row is missing (or, with
/// a version column, at another version than the struct's), the guard is
/// false for every row and nothing is updated.
pub(crate) fn update_many_sql<M: Model>(rows: &[&M]) -> Result<UpdateMany> {
    let key = primary_key::<M>()?;
    let names: Vec<String> = key
        .iter()
        .map(|&i| ident(M::COLUMNS[i].name))
        .collect::<Result<_>>()?;
    let version = version_column::<M>();
    let mut keys = Vec::with_capacity(rows.len());
    let mut seen = HashMap::new();
    let mut versions = Vec::with_capacity(rows.len());
    for (n, row) in rows.iter().enumerate() {
        row.__rdb_before(Write::Update)?;
        let k = RowKey::of(*row, &key)?;
        if let Some(first) = seen.insert(k.text(), n) {
            return Err(Error::Param(format!(
                "update_many was given the row with key ({}) twice (rows {first} and {n})",
                k.text()
            )));
        }
        versions.push(match version {
            Some(v) => {
                let mut out = String::new();
                row.write_value(v, &mut out)?;
                Some(out)
            }
            None => None,
        });
        keys.push(k);
    }
    let table = ident(M::TABLE)?;
    let single = names.len() == 1;
    let mut sets = Vec::new();
    for (i, column) in M::COLUMNS.iter().enumerate() {
        if column.primary_key || column.serial || column.role != ColumnRole::Plain {
            continue;
        }
        let name = ident(column.name)?;
        let mut case = if single {
            format!("{name} = CASE {} ", names[0])
        } else {
            format!("{name} = CASE ")
        };
        for (row, k) in rows.iter().zip(&keys) {
            if single {
                case.push_str(&format!("WHEN {} THEN ", k.literals[0]));
            } else {
                case.push_str(&format!("WHEN {} THEN ", k.predicate(&names)));
            }
            // Cast to the column's type: the server stores an untyped
            // literal from a CASE as text, which corrupts a BYTEA ('\x09'
            // kept as four characters) or an array column.
            case.push_str("CAST(");
            row.write_value(i, &mut case)?;
            case.push_str(&format!(" AS {}) ", column.sql_type));
        }
        case.push_str("END");
        sets.push(case);
    }
    if sets.is_empty() {
        return Err(Error::Unsupported(format!(
            "{} has no column outside its primary key to update",
            M::TABLE
        )));
    }
    sets.extend(maintained_sets::<M>(None)?);

    let in_keys = if single {
        format!(
            "{} IN ({})",
            names[0],
            keys.iter()
                .map(|k| k.literals[0].clone())
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        format!(
            "({})",
            keys.iter()
                .map(|k| format!("({})", k.predicate(&names)))
                .collect::<Vec<_>>()
                .join(" OR ")
        )
    };
    let live = match soft_delete_column::<M>() {
        Some(c) => format!(" AND {} IS NULL", ident(c)?),
        None => String::new(),
    };
    // The rows as the caller read them: each key, at the version it holds.
    let expected = match version {
        Some(v) => {
            let v = ident(M::COLUMNS[v].name)?;
            format!(
                "({})",
                keys.iter()
                    .zip(&versions)
                    .map(|(k, version)| format!(
                        "({} AND {v} = {})",
                        k.predicate(&names),
                        version.as_deref().expect("versioned")
                    ))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            )
        }
        None => in_keys.clone(),
    };
    let sql = format!(
        "UPDATE {table} SET {} WHERE {in_keys}{live} AND (SELECT COUNT(*) FROM {table} WHERE \
         {expected}{live}) = {} RETURNING {}",
        sets.join(", "),
        rows.len(),
        select_list::<M>()?
    );
    if sql.len() > UPDATE_MANY_MAX_BYTES {
        return Err(Error::Unsupported(format!(
            "update_many of {} rows is {} bytes of SQL, past the {} a statement may hold here: \
             splitting it would make it several statements, which could half-apply. Update \
             fewer rows per call, or run several update_many calls in a transaction",
            rows.len(),
            sql.len(),
            UPDATE_MANY_MAX_BYTES
        )));
    }
    let mut probe_columns = names.clone();
    if let Some(v) = version {
        probe_columns.push(ident(M::COLUMNS[v].name)?);
    }
    let probe = format!(
        "SELECT {} FROM {table} WHERE {in_keys}{live}",
        probe_columns.join(", ")
    );
    Ok(UpdateMany {
        sql,
        probe,
        keys: keys.iter().map(RowKey::text).collect(),
        versions,
    })
}

impl UpdateMany {
    /// Runs it: the rows as stored, in the order given; nothing updated and
    /// [`Error::Conflict`] when a row was missing or had moved on.
    pub(crate) async fn run<M: Model, E: Executor + ?Sized>(self, db: &E) -> Result<Vec<M>> {
        let n = self.keys.len();
        let rows = db.run(&self.sql).await?;
        if rows.len() == n {
            // Put the rows back in the order given.
            let key = primary_key::<M>()?;
            let mut position = HashMap::with_capacity(n);
            for (i, k) in self.keys.iter().enumerate() {
                position.insert(k.clone(), i);
            }
            let stored: Vec<M> = decode_models(&rows)?;
            let mut slots: Vec<Option<M>> = (0..n).map(|_| None).collect();
            for m in stored {
                let at = position
                    .get(&RowKey::of(&m, &key)?.text())
                    .copied()
                    .ok_or_else(|| {
                        Error::Protocol("update_many returned a row it was not given".to_string())
                    })?;
                slots[at] = Some(m);
            }
            return slots
                .into_iter()
                .map(|m| m.ok_or_else(|| Error::Protocol("update_many lost a row".into())))
                .collect();
        }
        if !rows.is_empty() {
            return Err(Error::RowCount {
                expected: "every row given",
                got: rows.len(),
            });
        }
        // Nothing was updated: say why, from the rows as they are now.
        let now = db.run(&self.probe).await?;
        let key_width = now
            .get(0)
            .map_or(0, |r| r.len().saturating_sub(usize::from(self.versions[0].is_some())));
        let mut found: HashMap<String, Option<String>> = HashMap::new();
        for row in now.iter() {
            let mut parts = Vec::new();
            for i in 0..key_width {
                let value: Value = row.get(i)?;
                let mut out = String::new();
                crate::params::push_param(&mut out, &crate::ToSql::to_param(&value))?;
                parts.push(out);
            }
            let version = if self.versions[0].is_some() {
                Some(match row.get::<Value>(key_width)? {
                    Value::I64(v) => v.to_string(),
                    Value::I32(v) => v.to_string(),
                    Value::I16(v) => v.to_string(),
                    other => format!("{other:?}"),
                })
            } else {
                None
            };
            found.insert(parts.join(", "), version);
        }
        let mut problems = Vec::new();
        for (key, expected) in self.keys.iter().zip(&self.versions) {
            match (found.get(key), expected) {
                (None, _) => problems.push(format!("({key}) is gone")),
                (Some(Some(now)), Some(expected)) if now != expected => {
                    problems.push(format!("({key}) is at version {now}, not {expected}"))
                }
                _ => {}
            }
        }
        if problems.is_empty() {
            problems.push("the rows changed while the statement ran".to_string());
        }
        Err(Error::Conflict(format!(
            "update_many of {n} {} rows updated none: {}",
            M::TABLE,
            problems.join("; ")
        )))
    }
}
