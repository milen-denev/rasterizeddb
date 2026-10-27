//! Where a single-table `SELECT`'s rows come from: one table, or the
//! partitions of a partitioned one (`docs/partitioning.md` §4, §8).
//!
//! **The contract:** for the same arguments a source returns the multiset of
//! rows one table holding all of them would return, so everything the
//! executor does after the scan (the fold, `DISTINCT`, windows, expression
//! sorts, paging) runs unchanged over it. A partitioned source keeps it by:
//!
//! - **One snapshot.** [`FanOut::plan`] takes the statement's `Visibility`
//!   first (the statement's shared one, when it has one), then the map epoch,
//!   then which partitions are materialized (§4.4), and every partition is
//!   read under that one `Visibility` through the `_with_visibility` entry
//!   points. A snapshot per partition would let a transaction that committed
//!   between two partition reads show in one and not the other: a transfer
//!   across tenants seen half applied (I7). A join (S19) takes its one
//!   `Visibility` itself and hands it to every relation's fan-out
//!   ([`FanOut::plan_with`]), so two partitioned relations, or a self-join's
//!   two sides, cannot take two.
//! - **Pruning only on proof** (I3). The route is `route_where` on the exact
//!   `WHERE` text the partitions' scans evaluate: `extract_gated`, never the
//!   path-aware variant, and zero partitions only for a constant-false
//!   clause. An unmaterialized partition holds no row (§4.3) and is skipped.
//!   A learned prepared statement's bound key (S16b) stands in for
//!   `route_where` only where pgwire proved the two agree.
//! - **Merging like one scan.** Unordered results concatenate; a `LIMIT`
//!   without `ORDER BY` stops launching partitions once enough rows are in
//!   and cuts the concatenation. `ORDER BY [… LIMIT k]` asks every partition
//!   for its own sorted top `k` (the scan's `limit` already includes
//!   `OFFSET`), then sorts the union with the scan's own final step
//!   (`ConcurrentProcessor::sort_rows_for_merge`) and cuts it to `k`. That is
//!   correct and simple; S12 replaces it with a streaming merge under one
//!   statement-wide worker budget.
//! - **An empty route still answers.** No partition yields no bytes, no rows
//!   and a count of 0, and the executor's pipeline runs over that, so
//!   `COUNT(*)` gives one row of 0 and `MAX(x)` one row of NULL (§8).
//!
//! At most [`FANOUT_IN_FLIGHT`] partitions are read at once, in pid order,
//! each a whole scan before the next group starts: a partition future is
//! never dropped mid-scan, so no worker outlives the statement it served.
//! Only a statement reaching at most [`CACHE_MAX_PARTITIONS`] partitions
//! reads or fills the query cache, each partition under its own scope, so a
//! wide fan-out cannot flush it.
//!
//! [`write_partitions`] is the same fan-out for `UPDATE` and `DELETE` (S11):
//! one snapshot and one transaction across partitions, visited one at a time.

use std::io;
use std::sync::Arc as StdArc;

use rclite::Arc;

use crate::core::database::Relation;
use crate::core::mvcc::Visibility;
use crate::core::mvcc::transaction::TxId;
use crate::core::partition::table::{MapEpoch, PartitionedTable};
use crate::core::processor::concurrent_processor::ConcurrentProcessor;
use crate::core::row::row::{Row, RowFetch};
use crate::core::row::schema::{SchemaField, TableSchema};
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

type PartTable = Table<DbStorageProvider>;

/// Partitions one statement reads at once (§8). The scans' own workers are
/// not yet drawn from a statement-wide budget; that is S12.
pub(crate) const FANOUT_IN_FLIGHT: usize = 8;

/// A statement reaching more partitions than this neither reads nor fills
/// the query cache (§8).
pub(crate) const CACHE_MAX_PARTITIONS: usize = 4;

/// A fan-out's partitions, by ascending pid.
pub(crate) type Parts = smallvec::SmallVec<[(u32, Arc<PartTable>); 1]>;

/// What a `SELECT` reads.
pub(crate) enum RowSource {
    /// A plain table, read exactly as before partitioning existed.
    Single(Arc<PartTable>),
    /// The routed, materialized partitions of a partitioned table, under one
    /// snapshot.
    Partitioned(Box<FanOut>),
}

/// A partitioned read's plan, fixed before the first partition is read.
pub(crate) struct FanOut {
    table: Arc<PartitionedTable>,
    /// Taken first (§4.4) and held until the last partition call returns, so
    /// its snapshot stays registered for the whole statement.
    visibility: Visibility,
    /// The map epoch the route was computed under, counted (`load_full`) and
    /// held for the statement, so a reshape's reclamation waits for it.
    _epoch: StdArc<MapEpoch>,
    /// The routed partitions that are materialized, by ascending pid. Inline
    /// for the one a routed point read reaches.
    parts: Parts,
    cache: bool,
}

impl RowSource {
    /// The source for a `SELECT` on `relation` whose scan evaluates
    /// `where_clause`, read as `tx`. `key`, a learned prepared statement's
    /// bound key (S16b), names a partitioned table's one partition without
    /// routing `where_clause`; see [`FanOut::plan`].
    pub(crate) fn for_select(
        relation: Relation,
        where_clause: &str,
        key: Option<&crate::core::rql::prepared::BoundKey>,
        tx: TxId,
    ) -> io::Result<Self> {
        match relation {
            Relation::Plain(table) => Ok(RowSource::Single(table)),
            Relation::Partitioned(table) => {
                Ok(RowSource::Partitioned(Box::new(FanOut::plan(table, where_clause, key, tx)?)))
            }
        }
    }

    /// A partitioned table's source read under `visibility`, a snapshot the
    /// caller already holds, rather than one [`FanOut::plan`] takes: a join
    /// reads every relation, partitioned or not, under the statement's one
    /// `Visibility` (S19, I7). Routed on `where_clause`, the text the
    /// partitions' scans evaluate (I3).
    pub(crate) fn partitioned_with(
        table: Arc<PartitionedTable>,
        where_clause: &str,
        visibility: Visibility,
    ) -> io::Result<Self> {
        Ok(RowSource::Partitioned(Box::new(FanOut::plan_with(table, where_clause, visibility)?)))
    }

    /// The relation this source reads, for a locking read that scans it again
    /// at a fresh snapshot (`execute_query_rows`, phase 3 P3).
    pub(crate) fn rescan_relation(&self) -> Relation {
        match self {
            RowSource::Single(table) => Relation::Plain(table.clone()),
            RowSource::Partitioned(fan) => Relation::Partitioned(fan.table.clone()),
        }
    }

    /// The rows of `relation` matching `where_clause` as `visibility` sees
    /// them, read in row mode so each keeps its position: the version a
    /// snapshot sees is named by where it is. For a locking read under a
    /// pinned snapshot, which compares the version it saw with the latest
    /// (`lock_selected_rows`).
    pub(crate) async fn rows_as_of(
        relation: Relation,
        where_clause: &str,
        schema_fields: &Vec<SchemaField>,
        fetch: RowFetch,
        visibility: Visibility,
        tx: TxId,
    ) -> io::Result<Vec<Row>> {
        match relation {
            Relation::Plain(table) => {
                table
                    .query_row_with_visibility_cached(
                        where_clause,
                        schema_fields,
                        fetch.clone(),
                        fetch,
                        None,
                        None,
                        visibility,
                        false,
                    )
                    .await
            }
            Relation::Partitioned(table) => {
                RowSource::partitioned_with(table, where_clause, visibility)?
                    .query_rows(where_clause, schema_fields, fetch.clone(), fetch, None, None, tx)
                    .await
            }
        }
    }

    /// The snapshot a partitioned source was planned under; `None` for a
    /// plain table, whose scan takes its own when it runs.
    pub(crate) fn planned_visibility(&self) -> Option<Visibility> {
        match self {
            RowSource::Single(_) => None,
            RowSource::Partitioned(fan) => Some(fan.visibility.clone()),
        }
    }

    /// The declared schema: the plain table's, or the partitioned table's
    /// shell's, which every partition's is a clone of.
    pub(crate) fn schema(&self) -> &TableSchema {
        match self {
            RowSource::Single(table) => &table.schema,
            RowSource::Partitioned(fan) => &fan.table.shell().schema,
        }
    }

    /// `Table::count_rows_in_txn`.
    pub(crate) async fn count(
        &self,
        where_clause: &str,
        schema_fields: &Vec<SchemaField>,
        query_row_fetch: RowFetch,
        tx: TxId,
    ) -> io::Result<u64> {
        match self {
            RowSource::Single(table) => table.count_rows_in_txn(where_clause, schema_fields, query_row_fetch, tx).await,
            RowSource::Partitioned(fan) => {
                let counts = fan
                    .each(
                        |_| false,
                        |part, visibility, cache| {
                            let fetch = query_row_fetch.clone();
                            async move {
                                part.count_rows_with_visibility(where_clause, schema_fields, fetch, visibility, cache)
                                    .await
                            }
                        },
                    )
                    .await?;
                Ok(counts.into_iter().sum())
            }
        }
    }

    /// `Table::query_row_in_txn`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn query_rows(
        &self,
        where_clause: &str,
        schema_fields: &Vec<SchemaField>,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        limit: Option<u64>,
        order_by: Option<String>,
        tx: TxId,
    ) -> io::Result<Vec<Row>> {
        match self {
            RowSource::Single(table) => {
                table
                    .query_row_in_txn(
                        where_clause,
                        schema_fields,
                        query_row_fetch,
                        requested_row_fetch,
                        limit,
                        order_by,
                        tx,
                    )
                    .await
            }
            RowSource::Partitioned(fan) => {
                let enough = |got: &[Vec<Row>]| {
                    order_by.is_none() && limit.is_some_and(|k| got.iter().map(Vec::len).sum::<usize>() as u64 >= k)
                };
                let parts = fan
                    .each(enough, |part, visibility, cache| {
                        let (qf, rf, order_by) = (query_row_fetch.clone(), requested_row_fetch.clone(), order_by.clone());
                        async move {
                            part.query_row_with_visibility_cached(
                                where_clause,
                                schema_fields,
                                qf,
                                rf,
                                limit,
                                order_by,
                                visibility,
                                cache,
                            )
                            .await
                        }
                    })
                    .await?;
                let mut rows: Vec<Row> = parts.into_iter().flatten().collect();
                if fan.parts.len() > 1 {
                    // Sorts by `order_by` when there is one, and cuts to `limit`.
                    ConcurrentProcessor::sort_rows_for_merge(&mut rows, schema_fields, &order_by, limit);
                }
                Ok(rows)
            }
        }
    }

    /// `Table::query_row_bytes_in_txn`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn query_bytes(
        &self,
        where_clause: &str,
        schema_fields: &[SchemaField],
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        limit: Option<u64>,
        order_by: Option<String>,
        tx: TxId,
    ) -> io::Result<Vec<u8>> {
        self.query_bytes_reporting(
            where_clause,
            schema_fields,
            query_row_fetch,
            requested_row_fetch,
            limit,
            order_by,
            tx,
        )
        .await
        .map(|(bytes, _)| bytes)
    }

    /// `Table::query_row_bytes_reporting_in_txn`: the serialized rows, and
    /// whether any may carry columns beyond `requested_row_fetch` (true when
    /// any partition's may, and after a merge sort, which decodes rows).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn query_bytes_reporting(
        &self,
        where_clause: &str,
        schema_fields: &[SchemaField],
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        limit: Option<u64>,
        order_by: Option<String>,
        tx: TxId,
    ) -> io::Result<(Vec<u8>, bool)> {
        let fan = match self {
            RowSource::Single(table) => {
                return table
                    .query_row_bytes_reporting_in_txn(
                        where_clause,
                        schema_fields,
                        query_row_fetch,
                        requested_row_fetch,
                        limit,
                        order_by,
                        tx,
                    )
                    .await;
            }
            RowSource::Partitioned(fan) => fan,
        };
        let ordered = order_by.is_some();
        let enough = |got: &[(Vec<u8>, bool)]| {
            !ordered
                && limit.is_some_and(|k| {
                    got.iter().map(|(b, _)| crate::core::row::row::count_serialized_rows(b)).sum::<u64>() >= k
                })
        };
        // Boxed: the fan-out's future is larger than a plain table's whole
        // read, and held inline it made every single-table read's future as
        // large, which is copied whole each time it moves.
        let parts = Box::pin(fan.each(enough, |part, visibility, cache| {
                let (qf, rf, order_by) = (query_row_fetch.clone(), requested_row_fetch.clone(), order_by.clone());
                async move {
                    part.query_row_bytes_reporting_with_visibility(
                        where_clause,
                        schema_fields,
                        qf,
                        rf,
                        limit,
                        order_by,
                        visibility,
                        cache,
                    )
                    .await
                }
            }))
            .await?;
        if parts.len() <= 1 {
            // One partition's answer is already one table's.
            return Ok(parts.into_iter().next().unwrap_or_default());
        }
        if ordered {
            // Every partition sorted its own top `limit`; the union is sorted
            // and cut by the scan's own final step. Correct, not yet cheap:
            // S12 streams a k-way merge instead of decoding every row.
            let mut rows = Vec::new();
            for (bytes, _) in &parts {
                rows.extend(crate::core::row::row::vec_into_rows(bytes)?);
            }
            ConcurrentProcessor::sort_rows_for_merge(&mut rows, schema_fields, &order_by, limit);
            return Ok((crate::core::row::row::rows_into_vec(rows), true));
        }
        let overfetched = parts.iter().any(|(_, over)| *over);
        let mut out = Vec::with_capacity(parts.iter().map(|(b, _)| b.len()).sum());
        for (bytes, _) in &parts {
            out.extend_from_slice(bytes);
        }
        if let Some(k) = limit {
            // Each partition stopped at `k`; the concatenation may hold more.
            let rest = crate::core::row::row::skip_serialized_rows(&out, k)?.len();
            out.truncate(out.len() - rest);
        }
        Ok((out, overfetched))
    }
}

impl FanOut {
    /// §4.4's order: the statement's `Visibility`, then the map epoch, then
    /// the route and the materialized flags. A row a reader's snapshot can see
    /// was committed after its partition was materialized, and the flags are
    /// read after the snapshot, so an unmaterialized partition skipped here
    /// holds nothing this statement may see (§4.3).
    ///
    /// The route is `route_where(where_clause)`, except for a learned
    /// prepared statement's bound `key` (S16b): this execution's own values,
    /// bound through the statement's cached derivation, already name one
    /// point, which pgwire checked routes as `route_where` routes the
    /// rendered clause. `keyed_range` refuses a key bound for another table
    /// or another key, and then the clause is routed as before. The flags
    /// are still read here, after this execution's snapshot.
    fn plan(
        table: Arc<PartitionedTable>,
        where_clause: &str,
        key: Option<&crate::core::rql::prepared::BoundKey>,
        tx: TxId,
    ) -> io::Result<FanOut> {
        // The statement's one snapshot when it has one (a read-only
        // statement's subqueries, CTEs and relations share it), else one
        // taken now; either way every partition is read under this value.
        Self::planned(table, where_clause, key, |table| table.read_visibility_for(tx))
    }

    /// [`Self::plan`] under `visibility`, which the caller took before this
    /// call, so §4.4's order holds: the snapshot, then the epoch, then the
    /// route and the flags. For a statement reading several relations under
    /// one snapshot it took itself (a join, S19), with or without a
    /// `statement_snapshot` scope around it.
    pub(crate) fn plan_with(
        table: Arc<PartitionedTable>,
        where_clause: &str,
        visibility: Visibility,
    ) -> io::Result<FanOut> {
        Self::planned(table, where_clause, None, move |_| visibility)
    }

    fn planned(
        table: Arc<PartitionedTable>,
        where_clause: &str,
        key: Option<&crate::core::rql::prepared::BoundKey>,
        visibility: impl FnOnce(&PartitionedTable) -> Visibility,
    ) -> io::Result<FanOut> {
        match Self::plan_inner(&table, where_clause, key, visibility) {
            Ok((visibility, epoch, parts)) => {
                let cache = parts.len() <= CACHE_MAX_PARTITIONS;
                Ok(FanOut { table, visibility, _epoch: epoch, parts, cache })
            }
            Err(e) => {
                log::error!("Cannot read partitioned table {}: {}", table.name(), e);
                Err(e)
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn plan_inner(
        table: &PartitionedTable,
        where_clause: &str,
        key: Option<&crate::core::rql::prepared::BoundKey>,
        visibility: impl FnOnce(&PartitionedTable) -> Visibility,
    ) -> io::Result<(Visibility, StdArc<MapEpoch>, Parts)> {
        table.check()?;
        let visibility = visibility(table);
        let epoch = table.epoch()?;
        let parts = match key.and_then(|k| table.keyed_range(k.uid, &k.levels)) {
            Some(range) => {
                #[cfg(test)]
                test_hooks::note_keyed(table.uid());
                table.materialized_in(&epoch, range)?
            }
            None => Parts::from_vec(table.materialized(&epoch, &table.route_where(where_clause)?)?),
        };
        Ok((visibility, epoch, parts))
    }

    /// Runs `read` on every planned partition, at most [`FANOUT_IN_FLIGHT`]
    /// at once, and returns the answers by ascending pid. Stops launching
    /// partitions once `enough` holds over the answers so far.
    async fn each<T, F, Fut>(&self, enough: impl Fn(&[T]) -> bool, read: F) -> io::Result<Vec<T>>
    where
        F: Fn(Arc<PartTable>, Visibility, bool) -> Fut,
        Fut: std::future::Future<Output = io::Result<T>>,
    {
        let mut out: Vec<T> = Vec::with_capacity(self.parts.len());
        #[cfg(test)]
        let hook = test_hooks::after_first(self.table.uid());
        #[cfg(test)]
        let width = if hook.is_some() { 1 } else { FANOUT_IN_FLIGHT };
        #[cfg(not(test))]
        let width = FANOUT_IN_FLIGHT;
        for (group, chunk) in self.parts.chunks(width).enumerate() {
            if enough(&out) {
                break;
            }
            #[cfg(test)]
            test_hooks::note_reads(self.table.uid(), chunk.iter().map(|(pid, _)| *pid));
            if let [(_, part)] = chunk {
                // One partition, as a routed point read is: awaited in place.
                // `join_all` would move the scan's future into a `Vec` of its
                // own, and the profile showed those copies (S16b).
                out.push(read(part.clone(), self.visibility.clone(), self.cache).await?);
            } else {
                let reads = chunk.iter().map(|(_, part)| read(part.clone(), self.visibility.clone(), self.cache));
                for answer in futures::future::join_all(reads).await {
                    out.push(answer?);
                }
            }
            #[cfg(test)]
            if group == 0 {
                if let Some(hook) = &hook {
                    hook().await;
                }
            }
            #[cfg(not(test))]
            let _ = group;
        }
        Ok(out)
    }
}

/// One partition's share of a partitioned `UPDATE` or `DELETE`: how many rows
/// it changed, and their pre-images when the statement collects them.
pub(crate) type PartWrite = io::Result<(u64, Vec<Row>)>;

/// A partitioned `UPDATE` or `DELETE` (§9): `write` runs on every routed,
/// materialized partition, and the counts sum. `collect`, when given,
/// receives every partition's pre-images in pid order (what `RETURNING`
/// reports, S17). On an error it may hold the earlier partitions' rows; the
/// caller drops them with the statement.
///
/// - **One partition** (a fully keyed `WHERE`, a tenant): `write` gets no
///   `Visibility`, so the partition snapshots under its own `write_lock`
///   exactly as a plain table does (§4.4), and keeps the point-update path.
///   No partition: nothing to write, 0.
/// - **Several**: §4.4's order (the statement's `Visibility`, then the map
///   epoch, then the route and the flags), then each partition in ascending
///   pid order, one at a time, under that one `Visibility` and the
///   statement's one transaction, each taking only its own `write_lock`. A
///   snapshot per partition would let a transaction that committed between
///   two visits be seen by the second and not the first. Passing a
///   `Visibility` also keeps every partition off the in-place overwrite, so
///   a later partition's failure leaves only versions the transaction's
///   abort retires.
/// - **A conflict** stays retryable only while no partition has applied a
///   row (§9, I17): a replay in `execute_in_txn` runs in the same
///   transaction, whose earlier partitions' new versions it would see and
///   apply again.
pub(crate) async fn write_partitions<F, Fut>(
    table: &PartitionedTable,
    where_clause: &str,
    tx: TxId,
    mut collect: Option<&mut Vec<Row>>,
    mut write: F,
) -> io::Result<u64>
where
    F: FnMut(Arc<PartTable>, Option<Visibility>, bool) -> Fut,
    Fut: std::future::Future<Output = PartWrite>,
{
    table.check()?;
    let visibility = table.visibility_for(tx);
    let epoch = table.epoch()?;
    let route = table.route_where(where_clause)?;
    let parts = table.materialized(&epoch, &route)?;
    #[cfg(test)]
    test_hooks::note_writes(table.uid(), parts.iter().map(|(pid, _)| *pid));
    let collecting = collect.is_some();
    if parts.len() <= 1 {
        drop(visibility);
        let Some((_, part)) = parts.into_iter().next() else {
            return Ok(0);
        };
        let (n, rows) = write(part, None, collecting).await?;
        if let Some(c) = collect.as_deref_mut() {
            c.extend(rows);
        }
        return Ok(n);
    }
    let mut applied: u64 = 0;
    for (i, (_pid, part)) in parts.iter().enumerate() {
        #[cfg(test)]
        if let Some(e) = test_hooks::injected_failure(table.uid(), i) {
            return Err(e);
        }
        match write(part.clone(), Some(visibility.clone()), collecting).await {
            Ok((n, rows)) => {
                applied = applied.saturating_add(n);
                if let Some(c) = collect.as_deref_mut() {
                    c.extend(rows);
                }
            }
            Err(e) if applied > 0 => return Err(without_retry_marker(e)),
            Err(e) => return Err(e),
        }
        #[cfg(test)]
        if i == 0 {
            if let Some(hook) = test_hooks::after_first(table.uid()) {
                hook().await;
            }
        }
        #[cfg(not(test))]
        let _ = i;
    }
    // Held to here: the snapshot stays registered, and the epoch counted,
    // until the last partition has returned.
    drop(visibility);
    drop(epoch);
    Ok(applied)
}

/// `e` without the marker that lets a retry loop replay its statement.
fn without_retry_marker(e: io::Error) -> io::Error {
    let text = e.to_string();
    match text.strip_suffix(PartTable::CONFLICT_RETRYABLE) {
        Some(trimmed) => io::Error::new(e.kind(), trimmed.to_string()),
        None => e,
    }
}

/// Test-only observation points in the fan-out, keyed by the partitioned
/// table's uid so tests running at once never see each other's.
#[cfg(test)]
pub(crate) mod test_hooks {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};

    pub(crate) type Hook =
        Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

    fn hooks() -> &'static Mutex<HashMap<u64, Hook>> {
        static H: OnceLock<Mutex<HashMap<u64, Hook>>> = OnceLock::new();
        H.get_or_init(Default::default)
    }

    fn reads() -> &'static Mutex<HashMap<u64, Vec<u32>>> {
        static R: OnceLock<Mutex<HashMap<u64, Vec<u32>>>> = OnceLock::new();
        R.get_or_init(Default::default)
    }

    /// Runs `hook` after the first partition of every fan-out over table
    /// `uid` has been read, and reads that table's partitions one at a time
    /// while it is installed, so the hook falls between two partition reads.
    pub(crate) fn set_after_first(uid: u64, hook: Option<Hook>) {
        let mut map = hooks().lock().unwrap();
        match hook {
            Some(h) => map.insert(uid, h),
            None => map.remove(&uid),
        };
    }

    pub(crate) fn after_first(uid: u64) -> Option<Hook> {
        hooks().lock().unwrap().get(&uid).cloned()
    }

    pub(crate) fn note_reads(uid: u64, pids: impl Iterator<Item = u32>) {
        reads().lock().unwrap().entry(uid).or_default().extend(pids);
    }

    /// The partitions read for table `uid` since the last call, in order.
    pub(crate) fn take_reads(uid: u64) -> Vec<u32> {
        reads().lock().unwrap().remove(&uid).unwrap_or_default()
    }

    fn keyed() -> &'static Mutex<HashMap<u64, u64>> {
        static K: OnceLock<Mutex<HashMap<u64, u64>>> = OnceLock::new();
        K.get_or_init(Default::default)
    }

    pub(crate) fn note_keyed(uid: u64) {
        *keyed().lock().unwrap().entry(uid).or_default() += 1;
    }

    /// Fan-outs over table `uid` planned from a learned statement's bound key
    /// (S16b), not from `route_where`, since the last call.
    pub(crate) fn take_keyed(uid: u64) -> u64 {
        keyed().lock().unwrap().remove(&uid).unwrap_or(0)
    }

    fn writes() -> &'static Mutex<HashMap<u64, Vec<u32>>> {
        static W: OnceLock<Mutex<HashMap<u64, Vec<u32>>>> = OnceLock::new();
        W.get_or_init(Default::default)
    }

    pub(crate) fn note_writes(uid: u64, pids: impl Iterator<Item = u32>) {
        writes().lock().unwrap().entry(uid).or_default().extend(pids);
    }

    /// The partitions an `UPDATE` or `DELETE` on table `uid` was routed to
    /// since the last call, in order.
    pub(crate) fn take_writes(uid: u64) -> Vec<u32> {
        writes().lock().unwrap().remove(&uid).unwrap_or_default()
    }

    fn failures() -> &'static Mutex<HashMap<u64, usize>> {
        static F: OnceLock<Mutex<HashMap<u64, usize>>> = OnceLock::new();
        F.get_or_init(Default::default)
    }

    /// Fails every multi-partition write on table `uid` just before its
    /// `k`-th partition (counting from 0), after the earlier ones wrote.
    pub(crate) fn fail_before(uid: u64, k: Option<usize>) {
        let mut map = failures().lock().unwrap();
        match k {
            Some(k) => map.insert(uid, k),
            None => map.remove(&uid),
        };
    }

    pub(crate) fn injected_failure(uid: u64, i: usize) -> Option<std::io::Error> {
        (failures().lock().unwrap().get(&uid) == Some(&i))
            .then(|| std::io::Error::other(format!("injected failure before partition #{i}")))
    }
}

#[cfg(test)]
#[path = "row_source_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "row_source_write_tests.rs"]
mod write_tests;

#[cfg(test)]
#[path = "row_source_returning_tests.rs"]
mod returning_tests;

#[cfg(test)]
#[path = "row_source_join_tests.rs"]
mod join_tests;
