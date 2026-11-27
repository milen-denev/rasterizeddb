//! Relations between models: declared on the struct, loaded eagerly without
//! N+1, loaded lazily, filtered on, counted, and (many-to-many) linked.
//!
//! See the [relations chapter](crate::guide::relations) for the whole tour.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use super::query::{Expr, Order, Select, SendSql};
use super::{BoxFuture, Model, ident};
use crate::executor::Executor;
use crate::params::{push_ident, push_param};
use crate::to_sql::ToSql;
use crate::value::Value;
use crate::{Error, Result};

/// Parent keys per statement when loading a level, at most. A has-many or
/// belongs-to level past it is read in several statements; each parent's
/// rows are always in one of them, so a per-relation order holds.
pub const RELATION_CHUNK: usize = 1000;

/// What kind of relation a [`Relation`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationKind {
    /// This model holds the key of one `C` (`posts.user_id` → `users.id`).
    BelongsTo,
    /// One `C` holds this model's key.
    HasOne,
    /// Any number of `C` hold this model's key.
    HasMany,
    /// Linked through a join table holding both keys.
    ManyToMany,
}

/// The join table of a many-to-many relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Through {
    /// The join table.
    pub table: &'static str,
    /// Its column holding the parent's key.
    pub local: &'static str,
    /// Its column holding the related row's key.
    pub foreign: &'static str,
}

/// A relation from model `P` to model `C`, as `#[derive(Model)]` declares it
/// for a relation field: `User::posts`.
///
/// Give it to [`Select::with`] to load it with the rows,
/// [`Select::where_has`] to filter on it, [`Select::with_count`] to count
/// it, or call [`load`](With::load) for rows already read. Narrow what it
/// loads with [`filter`](Self::filter), [`order_by`](Self::order_by),
/// [`limit`](Self::limit) (per parent) and [`with`](Self::with) (nested).
pub struct Relation<P, C> {
    name: &'static str,
    kind: RelationKind,
    parent_key: Option<&'static str>,
    child_key: Option<&'static str>,
    through: Option<Through>,
    fill: fn(&mut P, Vec<C>),
    count: Option<fn(&mut P, i64)>,
}

impl<P, C> Clone for Relation<P, C> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<P, C> Copy for Relation<P, C> {}

impl<P, C> fmt::Debug for Relation<P, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Relation")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("parent_key", &self.parent_key)
            .field("child_key", &self.child_key)
            .field("through", &self.through)
            .finish()
    }
}

impl<P, C> Relation<P, C> {
    /// What the derive writes for a relation field. `parent_key` is the
    /// column on `P` that matches, `child_key` the column on `C` (`None`:
    /// that model's single-column primary key); `fill` stores the loaded
    /// rows in the field, `count` stores a count in a `count_of` field.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        name: &'static str,
        kind: RelationKind,
        parent_key: Option<&'static str>,
        child_key: Option<&'static str>,
        through: Option<Through>,
        fill: fn(&mut P, Vec<C>),
        count: Option<fn(&mut P, i64)>,
    ) -> Self {
        Relation {
            name,
            kind,
            parent_key,
            child_key,
            through,
            fill,
            count,
        }
    }

    /// The relation field's name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Its kind.
    pub const fn kind(&self) -> RelationKind {
        self.kind
    }

    /// The join table, for a many-to-many relation.
    pub const fn through(&self) -> Option<Through> {
        self.through
    }
}

/// Where loaded rows go: the relation field's type. `Vec<C>` for a has-many
/// or many-to-many relation; `Option<C>` or `Option<Box<C>>` for a
/// belongs-to or has-one relation (the first row, if any).
pub trait RelationSlot<C> {
    /// Stores the rows loaded for one parent, in the relation's order.
    fn fill(&mut self, rows: Vec<C>);
}

impl<C> RelationSlot<C> for Vec<C> {
    fn fill(&mut self, rows: Vec<C>) {
        *self = rows;
    }
}

impl<C> RelationSlot<C> for Option<C> {
    fn fill(&mut self, rows: Vec<C>) {
        *self = rows.into_iter().next();
    }
}

impl<C> RelationSlot<C> for Option<Box<C>> {
    fn fill(&mut self, rows: Vec<C>) {
        *self = rows.into_iter().next().map(Box::new);
    }
}

// ------------------------------------------------------------------ keys

/// The matching columns, as positions in `P::COLUMNS` and `C::COLUMNS`.
struct Keys {
    parent: usize,
    child: usize,
}

fn single_key<M: Model>(relation: &str) -> Result<&'static str> {
    let mut key = M::COLUMNS.iter().filter(|c| c.primary_key);
    match (key.next(), key.next()) {
        (Some(column), None) => Ok(column.name),
        _ => Err(Error::Unsupported(format!(
            "relation {relation}: {} has no single-column primary key to match on; name the \
             column in the relation (local_key, owner_key or related_key)",
            M::TABLE
        ))),
    }
}

fn column_of<M: Model>(name: &str, relation: &str) -> Result<usize> {
    M::COLUMNS
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| {
            Error::Unsupported(format!(
                "relation {relation} names {}.{name}, which the model does not map",
                M::TABLE
            ))
        })
}

/// Column `i` of `m` as a literal; `None` for a NULL.
fn literal<M: Model>(m: &M, i: usize) -> Result<Option<String>> {
    let mut out = String::new();
    m.write_value(i, &mut out)?;
    Ok((out != "NULL").then_some(out))
}

/// A cell read back as the literal [`literal`] writes for the same value.
fn value_literal(value: &Value) -> Result<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    let mut out = String::new();
    push_param(&mut out, &value.to_param())?;
    Ok(Some(out))
}

fn in_list(column: &str, literals: &[&String]) -> Result<Expr> {
    let mut sql = ident(column)?;
    sql.push_str(" IN (");
    for (i, l) in literals.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(l);
    }
    sql.push(')');
    Ok(Expr::trusted(sql))
}

impl<P: Model, C: Model> Relation<P, C> {
    fn qualified(&self) -> String {
        format!("{}.{}", P::TABLE, self.name)
    }

    fn keys(&self) -> Result<Keys> {
        let name = self.qualified();
        let (parent, child) = match self.kind {
            RelationKind::BelongsTo => (
                self.parent_key.ok_or_else(|| {
                    Error::Unsupported(format!("relation {name}: belongs_to needs foreign_key"))
                })?,
                match self.child_key {
                    Some(k) => k,
                    None => single_key::<C>(&name)?,
                },
            ),
            RelationKind::HasOne | RelationKind::HasMany => (
                match self.parent_key {
                    Some(k) => k,
                    None => single_key::<P>(&name)?,
                },
                self.child_key.ok_or_else(|| {
                    Error::Unsupported(format!(
                        "relation {name}: has_many/has_one need foreign_key"
                    ))
                })?,
            ),
            RelationKind::ManyToMany => {
                if self.through.is_none() {
                    return Err(Error::Unsupported(format!(
                        "relation {name}: many_to_many needs through"
                    )));
                }
                (
                    match self.parent_key {
                        Some(k) => k,
                        None => single_key::<P>(&name)?,
                    },
                    match self.child_key {
                        Some(k) => k,
                        None => single_key::<C>(&name)?,
                    },
                )
            }
        };
        Ok(Keys {
            parent: column_of::<P>(parent, &name)?,
            child: column_of::<C>(child, &name)?,
        })
    }

    /// Everything the relation loads, before narrowing: the same as
    /// `With::from(relation)`.
    pub fn all(self) -> With<P, C> {
        With::from(self)
    }

    /// Only related rows matching `expr` (several filters must all hold).
    pub fn filter(self, expr: Expr) -> With<P, C> {
        With::from(self).filter(expr)
    }

    /// Related rows in this order, for each parent.
    pub fn order_by(self, order: Order) -> With<P, C> {
        With::from(self).order_by(order)
    }

    /// At most `n` related rows for each parent (after the order). Applied
    /// here, after the rows arrive: every matching row is read.
    pub fn limit(self, n: u64) -> With<P, C> {
        With::from(self).limit(n)
    }

    /// Soft-deleted related rows too (see [`Select::with_deleted`]).
    pub fn with_deleted(self) -> With<P, C> {
        With::from(self).with_deleted()
    }

    /// Only soft-deleted related rows.
    pub fn only_deleted(self) -> With<P, C> {
        With::from(self).only_deleted()
    }

    /// Loads `nested` on the related rows too: `User::posts.with(Post::comments)`.
    pub fn with<G>(self, nested: impl Into<With<C, G>>) -> With<P, C>
    where
        C: Send + 'static,
        G: Model + Clone + Send + Sync + 'static,
    {
        With::from(self).with(nested)
    }

    fn link_sql(&self, parent: &P, keys: &Keys) -> Result<(Through, String)> {
        let name = self.qualified();
        let through = self
            .through
            .filter(|_| self.kind == RelationKind::ManyToMany)
            .ok_or_else(|| {
                Error::Unsupported(format!(
                    "relation {name} is not many-to-many: link rows by setting their keys"
                ))
            })?;
        let key = literal(parent, keys.parent)?
            .ok_or_else(|| Error::Param(format!("relation {name}: the parent's key is NULL")))?;
        Ok((through, key))
    }

    fn child_literals<'c>(
        children: impl IntoIterator<Item = &'c C>,
        keys: &Keys,
        name: &str,
    ) -> Result<Vec<String>>
    where
        C: 'c,
    {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for child in children {
            let key = literal(child, keys.child)?.ok_or_else(|| {
                Error::Param(format!("relation {name}: a related row's key is NULL"))
            })?;
            if seen.insert(key.clone()) {
                out.push(key);
            }
        }
        Ok(out)
    }

    fn attach_sql(&self, parent: &P, children: &[String], keys: &Keys) -> Result<Option<String>> {
        let (through, key) = self.link_sql(parent, keys)?;
        if children.is_empty() {
            return Ok(None);
        }
        let mut sql = format!(
            "INSERT INTO {} ({}, {}) VALUES ",
            ident(through.table)?,
            ident(through.local)?,
            ident(through.foreign)?
        );
        for (i, child) in children.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({key}, {child})"));
        }
        sql.push_str(" ON CONFLICT DO NOTHING");
        Ok(Some(sql))
    }

    fn detach_sql(
        &self,
        parent: &P,
        children: Option<&[String]>,
        negate: bool,
        keys: &Keys,
    ) -> Result<Option<String>> {
        let (through, key) = self.link_sql(parent, keys)?;
        let mut sql = format!(
            "DELETE FROM {} WHERE {} = {key}",
            ident(through.table)?,
            ident(through.local)?
        );
        match children {
            None => {}
            Some([]) if !negate => return Ok(None),
            Some([]) => {}
            Some(children) => {
                sql.push_str(" AND ");
                push_ident(&mut sql, through.foreign)?;
                sql.push_str(if negate { " NOT IN (" } else { " IN (" });
                sql.push_str(&children.join(", "));
                sql.push(')');
            }
        }
        Ok(Some(sql))
    }

    /// Many-to-many: links `parent` to each of `children` with one
    /// `INSERT INTO <join table> … ON CONFLICT DO NOTHING`. How many links
    /// were added (one already there is skipped). The join table needs a
    /// primary key or unique key on its two columns, or a repeated link is
    /// stored twice.
    pub fn attach<'c, E: Executor>(
        &self,
        parent: &P,
        children: impl IntoIterator<Item = &'c C>,
        db: &E,
    ) -> impl Future<Output = Result<u64>> + Send
    where
        C: 'c,
    {
        let sql = self.keys().and_then(|keys| {
            let children = Self::child_literals(children, &keys, &self.qualified())?;
            self.attach_sql(parent, &children, &keys)
        });
        async move {
            match sql? {
                None => Ok(0),
                Some(sql) => Ok(db.run(&sql).await?.rows_affected().unwrap_or(0)),
            }
        }
    }

    /// Many-to-many: removes the links between `parent` and each of
    /// `children`, in one `DELETE`. How many were removed.
    pub fn detach<'c, E: Executor>(
        &self,
        parent: &P,
        children: impl IntoIterator<Item = &'c C>,
        db: &E,
    ) -> impl Future<Output = Result<u64>> + Send
    where
        C: 'c,
    {
        let sql = self.keys().and_then(|keys| {
            let children = Self::child_literals(children, &keys, &self.qualified())?;
            self.detach_sql(parent, Some(&children), false, &keys)
        });
        async move {
            match sql? {
                None => Ok(0),
                Some(sql) => Ok(db.run(&sql).await?.rows_affected().unwrap_or(0)),
            }
        }
    }

    /// Many-to-many: removes every link of `parent`. How many.
    pub fn detach_all<E: Executor>(
        &self,
        parent: &P,
        db: &E,
    ) -> impl Future<Output = Result<u64>> + Send {
        let sql = self
            .keys()
            .and_then(|keys| self.detach_sql(parent, None, false, &keys));
        async move {
            let sql = sql?.expect("a DELETE of every link");
            Ok(db.run(&sql).await?.rows_affected().unwrap_or(0))
        }
    }

    /// Many-to-many: makes `children` exactly the rows linked to `parent`:
    /// one `DELETE` of the links to others, then one `INSERT … ON CONFLICT
    /// DO NOTHING` of the missing ones.
    ///
    /// Two statements: outside a transaction a failure between them leaves
    /// the old links removed and the new ones not yet added, and another
    /// writer can interleave. Run it in a transaction to make it one change.
    pub fn sync<'c, E: Executor>(
        &self,
        parent: &P,
        children: impl IntoIterator<Item = &'c C>,
        db: &E,
    ) -> impl Future<Output = Result<Synced>> + Send
    where
        C: 'c,
    {
        let sql = self.keys().and_then(|keys| {
            let children = Self::child_literals(children, &keys, &self.qualified())?;
            Ok((
                self.detach_sql(parent, Some(&children), true, &keys)?,
                self.attach_sql(parent, &children, &keys)?,
            ))
        });
        async move {
            let (detach, attach) = sql?;
            let mut synced = Synced::default();
            if let Some(sql) = detach {
                synced.detached = db.run(&sql).await?.rows_affected().unwrap_or(0);
            }
            if let Some(sql) = attach {
                synced.attached = db.run(&sql).await?.rows_affected().unwrap_or(0);
            }
            Ok(synced)
        }
    }
}

/// What [`Relation::sync`] changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Synced {
    /// Links removed.
    pub detached: u64,
    /// Links added.
    pub attached: u64,
}

// ------------------------------------------------------------------ With

/// A relation to load, narrowed: from [`Relation::filter`],
/// [`order_by`](Relation::order_by), [`limit`](Relation::limit) or
/// [`with`](Relation::with), or `Relation::all()`. Every method that takes a
/// relation takes this too.
pub struct With<P, C> {
    relation: Relation<P, C>,
    select: Select<C>,
    per_parent: Option<u64>,
}

impl<P, C> Clone for With<P, C> {
    fn clone(&self) -> Self {
        With {
            relation: self.relation,
            select: self.select.clone(),
            per_parent: self.per_parent,
        }
    }
}

impl<P, C> fmt::Debug for With<P, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("With")
            .field("relation", &self.relation)
            .field("select", &self.select)
            .field("per_parent", &self.per_parent)
            .finish()
    }
}

impl<P, C: Model> From<Relation<P, C>> for With<P, C> {
    fn from(relation: Relation<P, C>) -> Self {
        With {
            relation,
            select: Select::new(),
            per_parent: None,
        }
    }
}

impl<P: Model, C: Model> With<P, C> {
    /// Only related rows matching `expr` (several filters must all hold).
    pub fn filter(mut self, expr: Expr) -> Self {
        self.select = self.select.filter(expr);
        self
    }

    /// Related rows in this order, for each parent.
    pub fn order_by(mut self, order: Order) -> Self {
        self.select = self.select.order_by(order);
        self
    }

    /// At most `n` related rows for each parent, after the order. Applied
    /// here, after the rows arrive (a per-parent `LIMIT` needs a window
    /// over a derived table, which the native protocol refuses): every
    /// matching row is read.
    pub fn limit(mut self, n: u64) -> Self {
        self.per_parent = Some(n);
        self
    }

    /// Loads `nested` on the related rows too.
    pub fn with<G>(mut self, nested: impl Into<With<C, G>>) -> Self
    where
        C: Send + 'static,
        G: Model + Clone + Send + Sync + 'static,
    {
        self.select = self.select.with(nested);
        self
    }

    /// Counts `relation` on the related rows too.
    pub fn with_count<G>(mut self, relation: impl Into<With<C, G>>) -> Self
    where
        C: Send + 'static,
        G: Model + Send + Sync + 'static,
    {
        self.select = self.select.with_count(relation);
        self
    }

    /// Soft-deleted related rows too (see [`Select::with_deleted`]).
    pub fn with_deleted(mut self) -> Self {
        self.select = self.select.with_deleted();
        self
    }

    /// Only soft-deleted related rows.
    pub fn only_deleted(mut self) -> Self {
        self.select = self.select.only_deleted();
        self
    }

    /// The relation this narrows.
    pub fn relation(&self) -> Relation<P, C> {
        self.relation
    }

    /// Loads the relation into rows already read: one statement for the
    /// whole slice (per [`RELATION_CHUNK`] parents), and one per nested
    /// relation level, never one per parent.
    pub async fn load<E: Executor>(&self, parents: &mut [P], db: &E) -> Result<()>
    where
        C: Clone + Send,
    {
        self.load_dyn(db, parents).await
    }

    async fn load_dyn(&self, db: &dyn SendSql, parents: &mut [P]) -> Result<()>
    where
        C: Clone + Send,
    {
        let keys = self.relation.keys()?;
        let parent_keys: Vec<Option<String>> = parents
            .iter()
            .map(|p| literal(p, keys.parent))
            .collect::<Result<_>>()?;
        let mut uses: HashMap<&String, usize> = HashMap::new();
        let mut distinct: Vec<&String> = Vec::new();
        for key in parent_keys.iter().flatten() {
            let n = uses.entry(key).or_insert(0);
            if *n == 0 {
                distinct.push(key);
            }
            *n += 1;
        }
        let mut groups: HashMap<String, Vec<C>> = HashMap::new();
        if !distinct.is_empty() {
            match self.relation.kind {
                RelationKind::ManyToMany => {
                    self.load_through(db, &distinct, &keys, &mut groups).await?
                }
                _ => {
                    let column = C::COLUMNS[keys.child].name;
                    for chunk in distinct.chunks(RELATION_CHUNK) {
                        let select = self.select.clone().filter(in_list(column, chunk)?);
                        for child in select.all_dyn(db).await? {
                            if let Some(key) = literal(&child, keys.child)? {
                                groups.entry(key).or_default().push(child);
                            }
                        }
                    }
                }
            }
        }
        for (parent, key) in parents.iter_mut().zip(&parent_keys) {
            let mut rows = match key {
                None => Vec::new(),
                Some(key) => {
                    let left = uses.get_mut(key).expect("counted");
                    *left -= 1;
                    if *left == 0 {
                        groups.remove(key).unwrap_or_default()
                    } else {
                        groups.get(key).cloned().unwrap_or_default()
                    }
                }
            };
            if let Some(n) = self.per_parent {
                rows.truncate(usize::try_from(n).unwrap_or(usize::MAX));
            }
            (self.relation.fill)(parent, rows);
        }
        Ok(())
    }

    /// Many-to-many: the join rows for a chunk of parents, then the related
    /// rows they name, in one statement each per chunk.
    async fn load_through(
        &self,
        db: &dyn SendSql,
        parents: &[&String],
        keys: &Keys,
        groups: &mut HashMap<String, Vec<C>>,
    ) -> Result<()>
    where
        C: Clone + Send,
    {
        let through = self.relation.through.expect("checked by keys()");
        let column = C::COLUMNS[keys.child].name;
        for chunk in parents.chunks(RELATION_CHUNK) {
            let mut sql = format!(
                "SELECT {}, {} FROM {} WHERE ",
                ident(through.local)?,
                ident(through.foreign)?,
                ident(through.table)?
            );
            sql.push_str(in_list(through.local, chunk)?.to_sql()?);
            let links = db.send_sql(&sql).await?;
            let mut parents_of: HashMap<String, Vec<String>> = HashMap::new();
            let mut children: Vec<String> = Vec::new();
            for row in links.iter() {
                let (Some(parent), Some(child)) = (
                    value_literal(&row.get::<Value>(0)?)?,
                    value_literal(&row.get::<Value>(1)?)?,
                ) else {
                    continue;
                };
                let entry = parents_of.entry(child.clone()).or_default();
                if entry.is_empty() {
                    children.push(child);
                }
                entry.push(parent);
            }
            if children.is_empty() {
                continue;
            }
            let refs: Vec<&String> = children.iter().collect();
            let select = self.select.clone().filter(in_list(column, &refs)?);
            for child in select.all_dyn(db).await? {
                let Some(key) = literal(&child, keys.child)? else {
                    continue;
                };
                if let Some(owners) = parents_of.get(&key) {
                    for owner in owners {
                        groups.entry(owner.clone()).or_default().push(child.clone());
                    }
                }
            }
        }
        Ok(())
    }

    /// `parent_key [NOT] IN (SELECT …)`: whether a parent has a related row
    /// matching this relation's filters.
    pub(crate) fn has_expr(&self, negated: bool) -> Result<Expr> {
        if self.per_parent.is_some() || self.select.has_order() || self.select.has_includes() {
            return Err(Error::Unsupported(format!(
                "where_has({}) takes filters only: limit, order and nested relations do not apply",
                self.relation.qualified()
            )));
        }
        let keys = self.relation.keys()?;
        let parent = ident(P::COLUMNS[keys.parent].name)?;
        let child = ident(C::COLUMNS[keys.child].name)?;
        let narrowing = self.select.where_sql()?;
        let mut inner = match self.relation.kind {
            RelationKind::ManyToMany => {
                let through = self.relation.through.expect("checked by keys()");
                let mut inner = format!(
                    "SELECT {local} FROM {} WHERE {local} IS NOT NULL",
                    ident(through.table)?,
                    local = ident(through.local)?
                );
                if let Some(narrowing) = narrowing {
                    inner.push_str(&format!(
                        " AND {} IN (SELECT {child} FROM {} WHERE {narrowing})",
                        ident(through.foreign)?,
                        ident(C::TABLE)?
                    ));
                }
                inner
            }
            _ => {
                let mut inner = format!(
                    "SELECT {child} FROM {} WHERE {child} IS NOT NULL",
                    ident(C::TABLE)?
                );
                if let Some(narrowing) = narrowing {
                    inner.push_str(&format!(" AND ({narrowing})"));
                }
                inner
            }
        };
        inner = if negated {
            // A parent whose key is NULL has nothing related: it belongs
            // with the "doesn't have" rows, where `NULL NOT IN (…)` would
            // drop it.
            format!("({parent} IS NULL OR {parent} NOT IN ({inner}))")
        } else {
            format!("{parent} IN ({inner})")
        };
        Ok(Expr::trusted(inner))
    }

    /// Counts per parent key: `SELECT key, COUNT(*) … GROUP BY key`.
    async fn count_dyn(&self, db: &dyn SendSql, parents: &mut [P]) -> Result<()> {
        let set = self.relation.count.ok_or_else(|| {
            Error::Unsupported(format!(
                "with_count({0}): {1} has no field to hold the count; add \
                 `#[rdb(count_of = \"{2}\")] {2}_count: i64`",
                self.relation.qualified(),
                P::TABLE,
                self.relation.name
            ))
        })?;
        if self.per_parent.is_some() || self.select.has_order() || self.select.has_includes() {
            return Err(Error::Unsupported(format!(
                "with_count({}) takes filters only: limit, order and nested relations do not apply",
                self.relation.qualified()
            )));
        }
        let keys = self.relation.keys()?;
        let parent_keys: Vec<Option<String>> = parents
            .iter()
            .map(|p| literal(p, keys.parent))
            .collect::<Result<_>>()?;
        let mut seen = HashSet::new();
        let distinct: Vec<&String> = parent_keys
            .iter()
            .flatten()
            .filter(|k| seen.insert(*k))
            .collect();
        let child = C::COLUMNS[keys.child].name;
        let narrowing = self.select.where_sql()?;
        let mut counts: HashMap<String, i64> = HashMap::new();
        for chunk in distinct.chunks(RELATION_CHUNK) {
            let sql = match self.relation.kind {
                RelationKind::ManyToMany => {
                    let through = self.relation.through.expect("checked by keys()");
                    let local = ident(through.local)?;
                    let mut sql = format!(
                        "SELECT {local}, COUNT(*) FROM {} WHERE {}",
                        ident(through.table)?,
                        in_list(through.local, chunk)?.to_sql()?
                    );
                    if let Some(narrowing) = &narrowing {
                        sql.push_str(&format!(
                            " AND {} IN (SELECT {} FROM {} WHERE {narrowing})",
                            ident(through.foreign)?,
                            ident(child)?,
                            ident(C::TABLE)?
                        ));
                    }
                    sql.push_str(&format!(" GROUP BY {local}"));
                    sql
                }
                _ => {
                    let column = ident(child)?;
                    let mut sql = format!(
                        "SELECT {column}, COUNT(*) FROM {} WHERE {}",
                        ident(C::TABLE)?,
                        in_list(child, chunk)?.to_sql()?
                    );
                    if let Some(narrowing) = &narrowing {
                        sql.push_str(&format!(" AND ({narrowing})"));
                    }
                    sql.push_str(&format!(" GROUP BY {column}"));
                    sql
                }
            };
            for row in db.send_sql(&sql).await?.iter() {
                if let Some(key) = value_literal(&row.get::<Value>(0)?)? {
                    counts.insert(key, row.get::<i64>(1)?);
                }
            }
        }
        for (parent, key) in parents.iter_mut().zip(&parent_keys) {
            set(
                parent,
                key.as_ref()
                    .and_then(|k| counts.get(k))
                    .copied()
                    .unwrap_or(0),
            );
        }
        Ok(())
    }
}

// --------------------------------------------------------------- includes

/// Something loaded after a [`Select`]'s rows arrive: a relation, or a
/// relation's count.
pub(crate) trait Include<P>: Send + Sync {
    fn load<'a>(&'a self, db: &'a dyn SendSql, parents: &'a mut [P]) -> BoxFuture<'a, Result<()>>;
    /// Where the related model's rows live, for a router to choose the
    /// shard the relation loads from.
    #[cfg_attr(not(feature = "sharding"), allow(dead_code))]
    fn child(&self) -> super::Sharding;
}

pub(crate) struct Eager<P, C>(pub(crate) With<P, C>);

impl<P, C> Include<P> for Eager<P, C>
where
    P: Model + Send + 'static,
    C: Model + Clone + Send + Sync + 'static,
{
    fn load<'a>(&'a self, db: &'a dyn SendSql, parents: &'a mut [P]) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.0.load_dyn(db, parents))
    }
    fn child(&self) -> super::Sharding {
        C::SHARD
    }
}

pub(crate) struct Count<P, C>(pub(crate) With<P, C>);

impl<P, C> Include<P> for Count<P, C>
where
    P: Model + Send + 'static,
    C: Model + Send + Sync + 'static,
{
    fn load<'a>(&'a self, db: &'a dyn SendSql, parents: &'a mut [P]) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.0.count_dyn(db, parents))
    }
    fn child(&self) -> super::Sharding {
        C::SHARD
    }
}

pub(crate) type Includes<P> = Vec<Arc<dyn Include<P>>>;

// ------------------------------------------------------------------- lazy

/// Loading a relation into one row already read: `user.load(User::posts,
/// &db)`. Implemented for every [`Model`]; in the prelude.
pub trait ModelRelations: Model {
    /// Loads `relation` into this row: one statement (and one per nested
    /// level). For many rows at once use [`With::load`], which loads them
    /// all in one statement.
    fn load<'a, C, E>(
        &'a mut self,
        relation: impl Into<With<Self, C>>,
        db: &'a E,
    ) -> impl Future<Output = Result<()>> + Send + 'a
    where
        Self: Send,
        C: Model + Clone + Send + Sync + 'a,
        E: Executor,
    {
        let with = relation.into();
        async move { with.load_dyn(db, std::slice::from_mut(self)).await }
    }
}

impl<M: Model> ModelRelations for M {}
