//! The upsert builders: `db.upsert(&row)` and `db.insert_many(&rows)`, with
//! `.on_conflict(…)` and `.do_update(…)` / `.do_update_all_except(…)` /
//! `.do_nothing()`. Each awaits as it is (`IntoFuture`).

use std::fmt;
use std::future::IntoFuture;

use super::projection::Columns;
use super::{
    BoxFuture, ColumnRole, INSERT_CHUNK_BYTES, INSERT_CHUNK_ROWS, Model, VersionProbe, ident,
    insert_chunks, maintained_sets, primary_key, push_insert_value, push_version_check,
    select_list,
};
use crate::executor::Executor;
use crate::{Error, Result};

/// What an `INSERT … ON CONFLICT` does with a row that clashes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Every column but the key, the conflict target, serial and creation
    /// columns, from the row given (`EXCLUDED`).
    UpdateDefault,
    /// These columns from the row given.
    Update(Vec<&'static str>),
    /// The default set, less these.
    UpdateAllExcept(Vec<&'static str>),
    /// Nothing: the row given is skipped.
    Nothing,
}

fn check_columns<M: Model>(names: &[&'static str], what: &str) -> Result<()> {
    for name in names {
        if !M::COLUMNS.iter().any(|c| c.name == *name) {
            return Err(Error::Param(format!(
                "{what} names {name}, which is not a column of {}",
                M::TABLE
            )));
        }
    }
    Ok(())
}

/// `INSERT INTO t (…) VALUES (…), … ON CONFLICT [(target)] DO …
/// RETURNING …`.
///
/// With no target, or the primary key as the target, every column is
/// written as given, a serial key included (the upsert names its row);
/// with another target, serial columns are left to the server, as an
/// insert leaves them. The default update writes every column but the
/// key, the target, serial and creation columns; a version and an update
/// time move as on any update, and with `check_version` (one row) the
/// update matches only the stored row at the struct's version.
pub(crate) fn upsert_statement<M: Model>(
    rows: &[&M],
    target: Option<&[&'static str]>,
    action: &Action,
    check_version: bool,
) -> Result<String> {
    if rows.is_empty() {
        return Err(Error::Param("an insert needs at least one row".to_string()));
    }
    let key: Vec<&'static str> = primary_key::<M>()?
        .into_iter()
        .map(|i| M::COLUMNS[i].name)
        .collect();
    if let Some(target) = target {
        if target.is_empty() {
            return Err(Error::Param("on_conflict names no column".to_string()));
        }
        check_columns::<M>(target, "on_conflict")?;
    }
    let names_key = |t: &[&'static str]| t.len() == key.len() && t.iter().all(|c| key.contains(c));
    let by_key = target.is_none_or(names_key);
    let written: Vec<usize> = (0..M::COLUMNS.len())
        .filter(|&i| by_key || !M::COLUMNS[i].serial)
        .collect();

    let table = ident(M::TABLE)?;
    let mut sql = format!("INSERT INTO {table} (");
    for (n, &i) in written.iter().enumerate() {
        if n > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&ident(M::COLUMNS[i].name)?);
    }
    sql.push_str(") VALUES ");
    for (r, row) in rows.iter().enumerate() {
        if r > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        for (n, &i) in written.iter().enumerate() {
            if n > 0 {
                sql.push_str(", ");
            }
            push_insert_value(*row, i, &mut sql)?;
        }
        sql.push(')');
    }
    sql.push_str(" ON CONFLICT");
    let arbiter: Vec<&'static str> = match (target, action) {
        (None, Action::Nothing) => Vec::new(),
        (None, _) => key.clone(),
        (Some(t), _) => t.to_vec(),
    };
    if !arbiter.is_empty() {
        let quoted = arbiter
            .iter()
            .map(|c| ident(c))
            .collect::<Result<Vec<_>>>()?;
        sql.push_str(&format!(" ({})", quoted.join(", ")));
    }
    let defaults = || -> Vec<&'static str> {
        M::COLUMNS
            .iter()
            .filter(|c| {
                !c.primary_key
                    && !c.serial
                    && !arbiter.contains(&c.name)
                    && !matches!(
                        c.role,
                        ColumnRole::CreatedAt | ColumnRole::Version | ColumnRole::UpdatedAt
                    )
            })
            .map(|c| c.name)
            .collect()
    };
    let columns = match action {
        Action::Nothing => {
            sql.push_str(" DO NOTHING");
            sql.push_str(" RETURNING ");
            sql.push_str(&select_list::<M>()?);
            return Ok(sql);
        }
        Action::UpdateDefault => defaults(),
        Action::Update(columns) => {
            check_columns::<M>(columns, "do_update")?;
            columns.clone()
        }
        Action::UpdateAllExcept(except) => {
            check_columns::<M>(except, "do_update_all_except")?;
            defaults()
                .into_iter()
                .filter(|c| !except.contains(c))
                .collect()
        }
    };
    for name in &columns {
        let column = M::COLUMNS
            .iter()
            .find(|c| c.name == *name)
            .expect("checked");
        if matches!(
            column.role,
            ColumnRole::Version | ColumnRole::UpdatedAt | ColumnRole::CreatedAt
        ) {
            return Err(Error::Param(format!(
                "do_update names {name}, which the ORM maintains itself"
            )));
        }
    }
    let mut sets = Vec::new();
    for name in &columns {
        let name = ident(name)?;
        sets.push(format!("{name} = EXCLUDED.{name}"));
    }
    sets.extend(maintained_sets::<M>(Some(&table))?);
    if sets.is_empty() {
        // Nothing to change (a model that is all key): a no-op assignment
        // still returns the stored row through RETURNING, in the same
        // statement, where reading it again after a DO NOTHING would race
        // a delete.
        let name = ident(arbiter[0])?;
        sets.push(format!("{name} = EXCLUDED.{name}"));
    }
    sql.push_str(" DO UPDATE SET ");
    sql.push_str(&sets.join(", "));
    if check_version {
        push_version_check(rows[0], " WHERE ", Some(&table), &mut sql)?;
    }
    sql.push_str(" RETURNING ");
    sql.push_str(&select_list::<M>()?);
    Ok(sql)
}

// ---------------------------------------------------------------- Upsert

/// One row, inserted or, when it clashes, updated: from
/// [`ModelExt::upsert`](super::ModelExt::upsert). Awaiting it runs it and
/// returns the row as stored.
///
/// By default the clash is on the primary key and every other column is
/// updated from the row given. [`on_conflict`](Self::on_conflict) names
/// another unique key; [`do_update`](Self::do_update) and
/// [`do_update_all_except`](Self::do_update_all_except) choose the columns
/// an update writes; [`do_nothing`](Self::do_nothing) keeps the stored row.
#[must_use = "an upsert does nothing until it is awaited"]
pub struct Upsert<'a, E, M> {
    db: &'a E,
    row: &'a M,
    target: Option<Vec<&'static str>>,
    action: Action,
}

impl<E, M> fmt::Debug for Upsert<'_, E, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Upsert")
            .field("target", &self.target)
            .field("action", &self.action)
            .finish_non_exhaustive()
    }
}

impl<'a, E: Executor, M: Model> Upsert<'a, E, M> {
    pub(crate) fn new(db: &'a E, row: &'a M) -> Self {
        Upsert {
            db,
            row,
            target: None,
            action: Action::UpdateDefault,
        }
    }

    /// The unique key a clash is on: one column or a tuple
    /// (`(Account::tenant, Account::email)`). The server absorbs a clash on
    /// this key only; a clash on any other unique key is still its error.
    /// With a target other than the primary key, serial columns are left to
    /// the server, as an insert leaves them.
    pub fn on_conflict(mut self, columns: impl Columns<M>) -> Self {
        self.target = Some(columns.names());
        self
    }

    /// On a clash, write only these columns (one or a tuple) from the row
    /// given, and the version and update time the ORM maintains.
    pub fn do_update(mut self, columns: impl Columns<M>) -> Self {
        self.action = Action::Update(columns.names());
        self
    }

    /// On a clash, write every column the default would but these.
    pub fn do_update_all_except(mut self, columns: impl Columns<M>) -> Self {
        self.action = Action::UpdateAllExcept(columns.names());
        self
    }

    /// On a clash, keep the stored row: awaiting it returns the row
    /// inserted, or `None` when one was already there.
    pub fn do_nothing(self) -> UpsertNothing<'a, E, M> {
        UpsertNothing {
            db: self.db,
            row: self.row,
            target: self.target,
        }
    }

    /// The statement awaiting it sends.
    pub fn to_sql(&self) -> Result<String> {
        self.row.__rdb_before(super::Write::Upsert)?;
        upsert_statement(&[self.row], self.target.as_deref(), &self.action, true)
    }
}

impl<'a, E: Executor, M: Model + 'a> IntoFuture for Upsert<'a, E, M> {
    type Output = Result<M>;
    type IntoFuture = BoxFuture<'a, Result<M>>;

    fn into_future(self) -> Self::IntoFuture {
        let sql = self.to_sql();
        let probe = VersionProbe::of(self.row);
        let db = self.db;
        Box::pin(async move {
            // The row as stored comes back from the statement itself
            // (RETURNING): inserted or updated, never read again after.
            let rows = db.run(&sql?).await?;
            if rows.is_empty()
                && let Some(probe) = probe?
            {
                return Err(probe.classify(db).await?.unwrap_or_else(|| {
                    Error::Conflict(
                        "the upsert stored nothing and no row has its key: it was deleted \
                         while the statement ran"
                            .to_string(),
                    )
                }));
            }
            super::decode_model(&rows)?.ok_or(Error::RowCount {
                expected: "exactly 1",
                got: 0,
            })
        })
    }
}

/// [`Upsert::do_nothing`]: awaiting it returns the row inserted, or `None`
/// when the key was taken.
#[must_use = "an upsert does nothing until it is awaited"]
pub struct UpsertNothing<'a, E, M> {
    db: &'a E,
    row: &'a M,
    target: Option<Vec<&'static str>>,
}

impl<E, M> fmt::Debug for UpsertNothing<'_, E, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpsertNothing")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl<E: Executor, M: Model> UpsertNothing<'_, E, M> {
    /// The statement awaiting it sends.
    pub fn to_sql(&self) -> Result<String> {
        self.row.__rdb_before(super::Write::Insert)?;
        upsert_statement(&[self.row], self.target.as_deref(), &Action::Nothing, false)
    }
}

impl<'a, E: Executor, M: Model + 'a> IntoFuture for UpsertNothing<'a, E, M> {
    type Output = Result<Option<M>>;
    type IntoFuture = BoxFuture<'a, Result<Option<M>>>;

    fn into_future(self) -> Self::IntoFuture {
        let sql = self.to_sql();
        let db = self.db;
        Box::pin(async move { super::decode_model(&db.run(&sql?).await?) })
    }
}

// ------------------------------------------------------------ InsertMany

/// Many rows in multi-row statements: from
/// [`ModelExt::insert_many`](super::ModelExt::insert_many). Awaiting it
/// returns the rows as stored.
///
/// Plain, a clash is the server's error. With
/// [`on_conflict`](Self::on_conflict) (alone, or with
/// [`do_update`](Self::do_update) /
/// [`do_update_all_except`](Self::do_update_all_except)) a clashing row
/// updates the stored one; with [`do_nothing`](Self::do_nothing) it is
/// skipped, and only the rows inserted come back.
///
/// Up to [`INSERT_CHUNK_ROWS`] rows and about [`INSERT_CHUNK_BYTES`] of text
/// go in one statement. A statement is atomic; several are not, unless they
/// run in a transaction.
#[must_use = "an insert does nothing until it is awaited"]
pub struct InsertMany<'a, E, M> {
    db: &'a E,
    rows: &'a [M],
    conflict: Option<(Option<Vec<&'static str>>, Action)>,
}

impl<E, M> fmt::Debug for InsertMany<'_, E, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InsertMany")
            .field("rows", &self.rows.len())
            .field("conflict", &self.conflict)
            .finish_non_exhaustive()
    }
}

impl<'a, E: Executor, M: Model> InsertMany<'a, E, M> {
    pub(crate) fn new(db: &'a E, rows: &'a [M]) -> Self {
        InsertMany {
            db,
            rows,
            conflict: None,
        }
    }

    fn action(mut self, target: Option<Vec<&'static str>>, action: Option<Action>) -> Self {
        let (old_target, old_action) = self
            .conflict
            .take()
            .unwrap_or((None, Action::UpdateDefault));
        self.conflict = Some((target.or(old_target), action.unwrap_or(old_action)));
        self
    }

    /// The unique key a clash is on (one column or a tuple); a clashing row
    /// updates the stored one, as [`Upsert`] does, unless
    /// [`do_nothing`](Self::do_nothing) says otherwise.
    pub fn on_conflict(self, columns: impl Columns<M>) -> Self {
        self.action(Some(columns.names()), None)
    }

    /// On a clash, write only these columns from the row given.
    pub fn do_update(self, columns: impl Columns<M>) -> Self {
        self.action(None, Some(Action::Update(columns.names())))
    }

    /// On a clash, write every column the default would but these.
    pub fn do_update_all_except(self, columns: impl Columns<M>) -> Self {
        self.action(None, Some(Action::UpdateAllExcept(columns.names())))
    }

    /// On a clash, skip the row (with no [`on_conflict`](Self::on_conflict),
    /// a clash on any unique key). Only the rows inserted come back.
    pub fn do_nothing(self) -> Self {
        self.action(None, Some(Action::Nothing))
    }

    /// The statements awaiting it sends, in order.
    pub fn to_sql(&self) -> Result<Vec<String>> {
        let write = match &self.conflict {
            Some((_, action)) if *action != Action::Nothing => super::Write::Upsert,
            _ => super::Write::Insert,
        };
        for row in self.rows {
            row.__rdb_before(write)?;
        }
        let Some((target, action)) = &self.conflict else {
            return insert_chunks(self.rows);
        };
        if *action != Action::Nothing && super::version_column::<M>().is_some() {
            return Err(Error::Unsupported(format!(
                "{} has a #[rdb(version)] column: a multi-row upsert cannot check each row's \
                 version; upsert the rows one at a time, or use update_many",
                M::TABLE
            )));
        }
        let rows: Vec<&M> = self.rows.iter().collect();
        let mut out = Vec::new();
        let mut pending: Vec<&[&M]> = rows.chunks(INSERT_CHUNK_ROWS).rev().collect();
        while let Some(chunk) = pending.pop() {
            let sql = upsert_statement(chunk, target.as_deref(), action, false)?;
            if sql.len() > INSERT_CHUNK_BYTES && chunk.len() > 1 {
                let (a, b) = chunk.split_at(chunk.len() / 2);
                pending.push(b);
                pending.push(a);
            } else {
                out.push(sql);
            }
        }
        Ok(out)
    }
}

impl<'a, E: Executor, M: Model + 'a> IntoFuture for InsertMany<'a, E, M> {
    type Output = Result<Vec<M>>;
    type IntoFuture = BoxFuture<'a, Result<Vec<M>>>;

    fn into_future(self) -> Self::IntoFuture {
        let statements = self.to_sql();
        let db = self.db;
        Box::pin(async move {
            // Answers are kept as they came and decoded at the end, so the
            // future holds no `M` across a send (`M` need not be `Send`).
            let mut answers = Vec::new();
            for sql in statements? {
                answers.push(db.run(&sql).await?);
            }
            let mut out = Vec::new();
            for rows in &answers {
                out.extend(super::decode_models::<M>(rows)?);
            }
            Ok(out)
        })
    }
}
