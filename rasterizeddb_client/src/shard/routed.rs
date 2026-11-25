//! A query on a router: sent to the shard its filter pins, or to every
//! shard and merged (sharding-plan A4b's routing table).

use std::cmp::Ordering;
use std::collections::BTreeSet;

use super::{Router, ShardId, all_or_first_error, all_or_partial, block_for, on_shard, unsharded};
use crate::orm::{Model, Select, Sharding, Update};
use crate::row::Rows;
use crate::value::Value;
use crate::{Client, Error, Result};

/// Where a query goes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Plan {
    /// One shard.
    One(ShardId),
    /// These shards (more than one), merged.
    Many(Vec<ShardId>),
    /// No shard can hold a matching row (two different keys under `AND`):
    /// nothing is sent.
    Empty,
}

impl Router {
    /// `select` on this router: [`RoutedSelect`]'s terminal calls (`all`,
    /// `first`, `count`, `delete`, …) go to the shard the filter's shard
    /// key pins, or with [`Select::all_shards`] to every shard, merged.
    /// A query that pins no key is [`Error::NeedsRouteKey`].
    pub fn select<M: Model>(&self, select: &Select<M>) -> RoutedSelect<'_, M> {
        RoutedSelect {
            router: self,
            select: select.clone(),
        }
    }

    /// `update` on this router, routed by its filter as
    /// [`select`](Self::select) is.
    pub fn update<'r, 'u, M: Model>(&'r self, update: &'u Update<M>) -> RoutedUpdate<'r, 'u, M> {
        RoutedUpdate { router: self, update }
    }

    fn plan<M: Model>(&self, select: &Select<M>) -> Result<Plan> {
        use crate::orm::shard_meta::ShardTarget;
        let t = self.table();
        let plan = match M::SHARD {
            Sharding::Global => Plan::One(t.home),
            Sharding::Unsharded => return Err(unsharded::<M>()),
            Sharding::Unroutable(why) => return Err(Error::Unsupported(format!("{}: {why}", M::TABLE))),
            Sharding::By { column, family } => match &select.target {
                ShardTarget::Key(k) => Plan::One(t.map.shard_of_block(block_for(k, family)?)),
                ShardTarget::All => Plan::Many(self.all().into_iter().map(|(id, _)| id).collect()),
                ShardTarget::Hint => {
                    let mut blocks: Option<BTreeSet<u64>> = None;
                    for (col, values) in select.route().pins {
                        if col != column {
                            continue;
                        }
                        let set = values.iter().map(|v| block_for(v, family)).collect::<Result<BTreeSet<u64>>>()?;
                        blocks = Some(match blocks {
                            None => set,
                            Some(b) => b.intersection(&set).copied().collect(),
                        });
                    }
                    let Some(blocks) = blocks else {
                        return Err(Error::NeedsRouteKey(format!(
                            "{} is sharded by {column}, and the query does not filter on it with eq or is_in: \
                             add that filter, or say on_shard(key) or all_shards()",
                            M::TABLE
                        )));
                    };
                    let shards: BTreeSet<ShardId> = blocks.iter().map(|&b| t.map.shard_of_block(b)).collect();
                    match shards.len() {
                        0 => Plan::Empty,
                        1 => Plan::One(*shards.iter().next().expect("one")),
                        _ => Plan::Many(shards.into_iter().collect()),
                    }
                }
            },
        };
        let plan = match plan {
            Plan::Many(ids) if ids.len() == 1 => Plan::One(ids[0]),
            other => other,
        };
        // A subquery (`where_has`) reads its model's table on the shard the
        // statement runs on: right only when that model's rows are there.
        for reads in select.route().reads {
            match crate::orm::shard_meta::placement(M::SHARD, reads).map_err(Error::CrossShard)? {
                crate::orm::shard_meta::Placement::Local => {}
                crate::orm::shard_meta::Placement::Home if plan == Plan::One(t.home) => {}
                crate::orm::shard_meta::Placement::Home => {
                    return Err(Error::CrossShard(format!(
                        "a where_has on {} reads a global model, whose rows are on the home shard only",
                        M::TABLE
                    )));
                }
            }
        }
        Ok(plan)
    }

    /// The client relations of a global model load from, when the query
    /// has one; else `shard` itself (never used).
    fn home_for<'a, M: Model>(&'a self, select: &Select<M>, shard: &'a Client) -> Result<&'a Client> {
        let needs_home = select.includes().iter().any(|i| {
            crate::orm::shard_meta::placement(M::SHARD, i.child()) == Ok(crate::orm::shard_meta::Placement::Home)
        });
        if needs_home { self.home() } else { Ok(shard) }
    }
}

/// A [`Select`] on a [`Router`]: from [`Router::select`].
pub struct RoutedSelect<'r, M> {
    router: &'r Router,
    select: Select<M>,
}

impl<M: Model> RoutedSelect<'_, M> {
    /// Every matching row. On several shards, each is asked for `limit +
    /// offset` rows, and the answers are merged in the query's order (by
    /// the decoded column values, NULLs as declared, text by its bytes)
    /// before the offset and limit are applied.
    pub async fn all(&self) -> Result<Vec<M>> {
        self.rows_of(&self.select).await
    }

    /// The first row, if any.
    pub async fn first(&self) -> Result<Option<M>> {
        Ok(self.rows_of(&self.select.clone().limit(1)).await?.into_iter().next())
    }

    /// Exactly one row; none or several is [`Error::RowCount`].
    pub async fn one(&self) -> Result<M> {
        let mut rows = self.rows_of(&self.select.clone().limit(2)).await?;
        match rows.len() {
            1 => Ok(rows.pop().expect("one row")),
            got => Err(Error::RowCount {
                expected: "exactly 1",
                got,
            }),
        }
    }

    /// The number of matching rows: summed over the shards.
    pub async fn count(&self) -> Result<u64> {
        match self.router.plan(&self.select)? {
            Plan::Empty => Ok(0),
            Plan::One(id) => self.select.count(self.router.for_shard(id)?).await.map_err(|e| on_shard(id, e)),
            Plan::Many(ids) => {
                let sel = &self.select;
                let answers = self.router.fan_out_to(&ids, |_, c| sel.count(c)).await;
                Ok(all_or_first_error(answers)?.into_iter().map(|(_, n)| n).sum())
            }
        }
    }

    /// Whether any shard has a matching row.
    pub async fn exists(&self) -> Result<bool> {
        match self.router.plan(&self.select)? {
            Plan::Empty => Ok(false),
            Plan::One(id) => self.select.exists(self.router.for_shard(id)?).await.map_err(|e| on_shard(id, e)),
            Plan::Many(ids) => {
                let sel = &self.select;
                let answers = self.router.fan_out_to(&ids, |_, c| sel.exists(c)).await;
                Ok(all_or_first_error(answers)?.into_iter().any(|(_, b)| b))
            }
        }
    }

    /// A keyset page ([`Select::page`]): on one shard only.
    pub async fn page(&self) -> Result<crate::orm::Page<M>> {
        match self.router.plan(&self.select)? {
            Plan::One(id) => self.select.page(self.router.for_shard(id)?).await.map_err(|e| on_shard(id, e)),
            _ => Err(Error::Unsupported(format!(
                "a page of {} reads one shard: filter on its shard key or say on_shard(key); \
                 for every shard, merge all() with after(cursor)",
                M::TABLE
            ))),
        }
    }

    /// Deletes the matching rows ([`Select::delete`]). On several shards
    /// only with [`Select::split_across_shards`]: each shard's delete
    /// commits on its own, and a failure is [`Error::FanOutPartial`].
    pub async fn delete(&self) -> Result<u64> {
        self.delete_with(false).await
    }

    /// [`Select::force_delete`], routed as [`delete`](Self::delete).
    pub async fn force_delete(&self) -> Result<u64> {
        self.delete_with(true).await
    }

    async fn delete_with(&self, force: bool) -> Result<u64> {
        let sel = &self.select;
        match self.router.plan(sel)? {
            Plan::Empty => Ok(0),
            Plan::One(id) => {
                let c = self.router.for_shard(id)?;
                (if force { sel.force_delete(c).await } else { sel.delete(c).await }).map_err(|e| on_shard(id, e))
            }
            Plan::Many(ids) => {
                self.check_split(&ids)?;
                let answers = self
                    .router
                    .fan_out_to(&ids, |_, c| async move { if force { sel.force_delete(c).await } else { sel.delete(c).await } })
                    .await;
                Ok(all_or_partial(answers)?.into_iter().map(|(_, n)| n).sum())
            }
        }
    }

    /// [`Select::delete_returning`], routed as [`delete`](Self::delete).
    pub async fn delete_returning(&self) -> Result<Vec<M>>
    where
        M: Send,
    {
        let sel = &self.select;
        match self.router.plan(sel)? {
            Plan::Empty => Ok(Vec::new()),
            Plan::One(id) => sel.delete_returning(self.router.for_shard(id)?).await.map_err(|e| on_shard(id, e)),
            Plan::Many(ids) => {
                self.check_split(&ids)?;
                let answers = self.router.fan_out_to(&ids, |_, c| sel.delete_returning(c)).await;
                Ok(all_or_partial(answers)?.into_iter().flat_map(|(_, rows)| rows).collect())
            }
        }
    }

    fn check_split(&self, ids: &[ShardId]) -> Result<()> {
        if self.select.split {
            Ok(())
        } else {
            Err(Error::CrossShard(format!(
                "this delete of {} reaches shards {ids:?}, and each would commit on its own: \
                 say split_across_shards() to allow it",
                M::TABLE
            )))
        }
    }

    /// The rows of `sel` under its plan, merged across shards.
    async fn rows_of(&self, sel: &Select<M>) -> Result<Vec<M>> {
        match self.router.plan(sel)? {
            Plan::Empty => Ok(Vec::new()),
            Plan::One(id) => {
                let c = self.router.for_shard(id)?;
                let rows = c.run(&sel.to_sql()?).await.map_err(|e| on_shard(id, e))?;
                let home = self.router.home_for(sel, c)?;
                sel.finish_on(c, home, &rows, None).await
            }
            Plan::Many(ids) => self.merged(sel, &ids).await,
        }
    }

    async fn merged(&self, sel: &Select<M>, ids: &[ShardId]) -> Result<Vec<M>> {
        let wide = sel.widened();
        let sql = wide.to_sql()?;
        let answers = self.router.fan_out_to(ids, |_, c| c.run(&sql)).await;
        let answers: Vec<(ShardId, Rows)> = all_or_first_error(answers)?;
        let keys = sel.merge_keys()?;
        // (answer, row, sort values)
        let mut entries: Vec<(usize, usize, Vec<Value>)> = Vec::new();
        for (a, (_, rows)) in answers.iter().enumerate() {
            for (r, row) in rows.iter().enumerate() {
                let values = keys
                    .iter()
                    .map(|&(i, ..)| row.get::<Value>(i))
                    .collect::<Result<Vec<Value>>>()?;
                entries.push((a, r, values));
            }
        }
        if !keys.is_empty() {
            entries.sort_by(|x, y| {
                for (n, &(_, descending, nulls_first, numeric)) in keys.iter().enumerate() {
                    let o = compare(&x.2[n], &y.2[n], nulls_first, numeric);
                    let o = if descending { reverse_non_null(o, &x.2[n], &y.2[n]) } else { o };
                    if o != Ordering::Equal {
                        return o;
                    }
                }
                Ordering::Equal
            });
        }
        let skip = sel.offset_value().unwrap_or(0) as usize;
        let take = sel.limit_value().map_or(usize::MAX, |l| l as usize);
        let chosen: Vec<(usize, usize)> = entries.into_iter().skip(skip).take(take).map(|(a, r, _)| (a, r)).collect();
        // Decode each shard's chosen rows (with their relations) on that
        // shard, then put them back in merged order.
        let mut per: Vec<std::collections::VecDeque<M>> = Vec::with_capacity(answers.len());
        for (a, (id, rows)) in answers.iter().enumerate() {
            let order: Vec<usize> = chosen.iter().filter(|(x, _)| *x == a).map(|(_, r)| *r).collect();
            if order.is_empty() {
                per.push(Default::default());
                continue;
            }
            let c = self.router.for_shard(*id)?;
            let home = self.router.home_for(sel, c)?;
            per.push(sel.finish_on(c, home, rows, Some(&order)).await.map_err(|e| on_shard(*id, e))?.into());
        }
        let mut out: Vec<M> = chosen.iter().map(|(a, _)| per[*a].pop_front().expect("decoded")).collect();
        if sel.reads_before() {
            out.reverse();
        }
        Ok(out)
    }
}

/// An [`Update`] on a [`Router`]: from [`Router::update`].
pub struct RoutedUpdate<'r, 'u, M> {
    router: &'r Router,
    update: &'u Update<M>,
}

impl<M: Model + Send> RoutedUpdate<'_, '_, M> {
    /// Runs the update where its rows are ([`Update::run`]); on several
    /// shards only when its query said
    /// [`split_across_shards`](Select::split_across_shards).
    pub async fn run(&self) -> Result<Vec<M>> {
        let sel = self.update.rows();
        match self.router.plan(sel)? {
            Plan::Empty => Ok(Vec::new()),
            Plan::One(id) => self.update.run(self.router.for_shard(id)?).await.map_err(|e| on_shard(id, e)),
            Plan::Many(ids) => {
                if !sel.split {
                    return Err(Error::CrossShard(format!(
                        "this update of {} reaches shards {ids:?}, and each would commit on its own: \
                         say split_across_shards() to allow it",
                        M::TABLE
                    )));
                }
                let upd = self.update;
                let answers = self.router.fan_out_to(&ids, |_, c| upd.run(c)).await;
                Ok(all_or_partial(answers)?.into_iter().flat_map(|(_, rows)| rows).collect())
            }
        }
    }
}

/// A descending key: the values reverse, NULLs stay where `nulls_first`
/// put them.
fn reverse_non_null(o: Ordering, a: &Value, b: &Value) -> Ordering {
    if a.is_null() || b.is_null() { o } else { o.reverse() }
}

/// Two cells of one column, ascending.
fn compare(a: &Value, b: &Value, nulls_first: bool, numeric: bool) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return if nulls_first { Ordering::Less } else { Ordering::Greater },
        (false, true) => return if nulls_first { Ordering::Greater } else { Ordering::Less },
        _ => {}
    }
    use Value::*;
    let int = |v: &Value| -> Option<i128> {
        Some(match v {
            I16(x) => *x as i128,
            I32(x) => *x as i128,
            I64(x) => *x as i128,
            U64(x) => *x as i128,
            I128(x) => *x,
            _ => return None,
        })
    };
    let float = |v: &Value| -> Option<f64> {
        match v {
            F32(x) => Some(*x as f64),
            F64(x) => Some(*x),
            other => int(other).map(|i| i as f64),
        }
    };
    if let (Some(x), Some(y)) = (int(a), int(b)) {
        return x.cmp(&y);
    }
    match (a, b) {
        (Numeric(x), Numeric(y)) => decimal_cmp(x.as_str(), y.as_str()),
        (Text(x), Text(y)) if numeric => decimal_cmp(x, y),
        (Text(x), Text(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Bytes(x), Bytes(y)) => x.cmp(y),
        (Bool(x), Bool(y)) => x.cmp(y),
        (Uuid(x), Uuid(y)) => x.cmp(y),
        (Timestamp(x), Timestamp(y)) => x.cmp(y),
        (Date(x), Date(y)) => x.cmp(y),
        (Time(x), Time(y)) => x.cmp(y),
        _ => match (float(a), float(b)) {
            // PostgreSQL sorts NaN above every number.
            (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or_else(|| x.is_nan().cmp(&y.is_nan())),
            _ => Ordering::Equal,
        },
    }
}

/// Two decimal texts (`-12.50`, `3`, `NaN`) by value, NaN above all.
fn decimal_cmp(a: &str, b: &str) -> Ordering {
    let parse = |s: &str| -> Option<(bool, String, String)> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("nan") {
            return None;
        }
        let (neg, digits) = match s.strip_prefix('-') {
            Some(d) => (true, d),
            None => (false, s.strip_prefix('+').unwrap_or(s)),
        };
        let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
        let int = int.trim_start_matches('0').to_string();
        let frac = frac.trim_end_matches('0').to_string();
        let zero = int.is_empty() && frac.is_empty();
        Some((neg && !zero, int, frac))
    };
    match (parse(a), parse(b)) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some((na, ia, fa)), Some((nb, ib, fb))) => {
            let magnitude = ia.len().cmp(&ib.len()).then_with(|| ia.cmp(&ib)).then_with(|| fa.cmp(&fb));
            match (na, nb) {
                (false, false) => magnitude,
                (true, true) => magnitude.reverse(),
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
            }
        }
    }
}
