use std::{
    borrow::Cow,
    cell::UnsafeCell,
    sync::atomic::{AtomicBool, AtomicU8, AtomicU64},
};

use branches::{likely, unlikely};
use cacheguard::CacheGuard;
use log::error;
use rclite::Arc;
use std::sync::Arc as StdArc;

use crate::core::mvcc::Visibility;

use futures::future::join_all;
use smallvec::SmallVec;

use tokio::{sync::Semaphore, task};

use super::transformer::{ColumnTransformer, Next, TransformerPlan, TransformerProcessor};

use crate::{
    cache::query_cache,
    core::{
        processor::transformer::RowBlocks,
        row::{
            error::EvalError,
            row::{Row, RowFetch},
            row_pointer::{RowPointer, RowPointerIterator},
            schema::SchemaField,
        },
        storage_providers::traits::StorageIO,
        tokenizer::query_parser::QueryParser,
    },
    memory_pool::MemoryBlock,
};

use crate::core::tokenizer::query_tokenizer::Token;

pub static EMPTY_STR: &str = "";

// The query cache proper lives in `crate::cache::query_cache`; these are the
// historical names its globals were reachable under.
pub use crate::cache::query_cache::{ENABLE_CACHE, QUERY_CACHE as ATOMIC_CACHE};

#[derive(Debug, Clone)]
pub struct RowPointerWithOffset {
    pub pointer_record_pos: u64,
    pub pointer: RowPointer,
}

/// One resolved `ORDER BY` key.
pub struct SortKey {
    /// The column being ordered by.
    schema_id: usize,
    descending: bool,
    /// The `__null_x` companion, when the column is nullable.
    ///
    /// A nullable column's stored bytes cannot say whether it is `NULL` — a
    /// `NULL` timestamp is the type's zeroes, which byte-sorts as 1970 rather
    /// than where PostgreSQL puts it. The companion is what makes the scan able
    /// to sort the column at all.
    null_flag: Option<usize>,
}

/// An `ORDER BY … LIMIT k` reduced to what a scan worker needs to enforce it.
pub struct TopN {
    keys: Vec<SortKey>,
    /// How many rows each worker keeps.
    cap: usize,
}

/// A resolved key list made ready for one sort or selection.
///
/// The comparison runs O(n log n) times, and resolving a key from scratch costs
/// a linear search of the row's columns per side and of the schema for the
/// type. Here the type is looked up once, and each column's *position* is read
/// off one sample row and then checked on every use: if the row at hand holds
/// that column at that position it is used, otherwise the row is searched as
/// before. So a row whose layout differs from the sample is still compared
/// correctly, only more slowly — rows of one scan share a layout in practice,
/// since they come from one fetch list.
///
/// The ordering is exactly that of the per-call resolution it replaced: NULL
/// (by the `__null_` companion) sorts above every value, a column the row does
/// not carry sorts below one it does, and the stored bytes are compared by the
/// schema's type, falling back to the left column's own type when the schema
/// does not name the column. `prepared_sort_agrees_with_per_call_resolution`
/// pins that.
pub(crate) struct PreparedSort {
    keys: SmallVec<[PreparedKey; 4]>,
}

struct PreparedKey {
    schema_id: usize,
    descending: bool,
    null_flag: Option<usize>,
    /// The schema's declared type, or `None` to use the column's own.
    db_type: Option<crate::core::db_type::DbType>,
    /// Where the sample row held the key column and its companion.
    pos: usize,
    flag_pos: usize,
}

impl PreparedSort {
    pub(crate) fn new(keys: &[SortKey], schema: &[SchemaField], sample: Option<&Row>) -> Self {
        let position = |schema_id: usize| {
            sample
                .and_then(|row| {
                    row.columns
                        .iter()
                        .position(|c| c.schema_id as usize == schema_id)
                })
                .unwrap_or(usize::MAX)
        };
        PreparedSort {
            keys: keys
                .iter()
                .map(|key| PreparedKey {
                    schema_id: key.schema_id,
                    descending: key.descending,
                    null_flag: key.null_flag,
                    db_type: schema
                        .iter()
                        .find(|f| f.write_order as usize == key.schema_id)
                        .map(|f| f.db_type.clone()),
                    pos: position(key.schema_id),
                    flag_pos: key.null_flag.map_or(usize::MAX, position),
                })
                .collect(),
        }
    }

    /// The first column of `row` with `schema_id`, trying `hint` first.
    #[inline(always)]
    fn column(row: &Row, schema_id: usize, hint: usize) -> Option<&crate::core::row::row::Column> {
        match row.columns.get(hint) {
            Some(c) if c.schema_id as usize == schema_id => Some(c),
            _ => row
                .columns
                .iter()
                .find(|c| c.schema_id as usize == schema_id),
        }
    }

    #[inline(always)]
    fn is_null(row: &Row, flag: usize, hint: usize) -> bool {
        Self::column(row, flag, hint)
            .and_then(|c| c.data.into_slice().first().copied())
            .is_some_and(|byte| byte != 0)
    }

    /// Orders two rows by the keys, later keys breaking earlier ties.
    #[inline]
    pub(crate) fn compare(&self, a: &Row, b: &Row) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        for key in self.keys.iter() {
            let ord = match key.null_flag {
                // A NULL sorts as larger than every value, so it lands last
                // ascending and first descending — which is where PostgreSQL
                // puts it, and the reversal below applies to it too. The stored
                // bytes of a NULL are the type's zeroes and are never compared.
                Some(flag) => match (
                    Self::is_null(a, flag, key.flag_pos),
                    Self::is_null(b, flag, key.flag_pos),
                ) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    (false, false) => self.compare_values(a, b, key),
                },
                None => self.compare_values(a, b, key),
            };
            let ord = if key.descending { ord.reverse() } else { ord };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }

    #[inline(always)]
    fn compare_values(&self, a: &Row, b: &Row, key: &PreparedKey) -> std::cmp::Ordering {
        match (
            Self::column(a, key.schema_id, key.pos),
            Self::column(b, key.schema_id, key.pos),
        ) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(ac), Some(bc)) => compare_bytes_as_type(
                key.db_type.clone().unwrap_or_else(|| ac.column_type.clone()),
                ac.data.into_slice(),
                bc.data.into_slice(),
            ),
        }
    }
}

impl PreparedSort {
    /// The order as one normalised integer per row, when it is a single key of
    /// a fixed-width integer or timestamp type: the key's value mapped so that
    /// unsigned comparison of the integer is the column's order, with NULL above
    /// every value and a descending key bit-inverted.
    ///
    /// Sorting `(key, row index)` pairs is then a plain integer sort with no
    /// row access in the comparator — measured 570 → 345 ms for ordering a
    /// million rows by a BIGINT, permutation included (`result_pipeline_bench
    /// sortkeys`; LSD radix measured the same as pdqsort, so pdqsort it is).
    /// Ties fall back to the row index, which is the answer the stable
    /// comparison sort gives.
    ///
    /// `None` whenever the shortcut could disagree with `compare`: more than
    /// one key, a type outside the list, or any row whose key column is missing
    /// or too short (`compare` orders those by other rules).
    fn integer_keys(&self, rows: &[Row]) -> Option<Vec<u128>> {
        use crate::core::db_type::DbType;
        let [key] = self.keys.as_slice() else {
            return None;
        };
        if rows.len() > u32::MAX as usize {
            return None;
        }
        let mut out = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            let column = Self::column(row, key.schema_id, key.pos)?;
            let ty = key.db_type.as_ref().unwrap_or(&column.column_type);
            let b = column.data.into_slice();
            // `compare_bytes_as_type` reads the first N bytes of each side;
            // a shorter value takes a different rule, so it is a fallback.
            let value: u64 = match ty {
                DbType::I8 => (i8::from_le_bytes(b.get(..1)?.try_into().ok()?) as i64 as u64) ^ (1 << 63),
                DbType::I16 => (i16::from_le_bytes(b.get(..2)?.try_into().ok()?) as i64 as u64) ^ (1 << 63),
                DbType::I32 => (i32::from_le_bytes(b.get(..4)?.try_into().ok()?) as i64 as u64) ^ (1 << 63),
                DbType::I64 | DbType::DATETIME => {
                    (i64::from_le_bytes(b.get(..8)?.try_into().ok()?) as u64) ^ (1 << 63)
                }
                DbType::U8 => b.get(..1)?[0] as u64,
                DbType::U16 => u16::from_le_bytes(b.get(..2)?.try_into().ok()?) as u64,
                DbType::U32 => u32::from_le_bytes(b.get(..4)?.try_into().ok()?) as u64,
                DbType::U64 => u64::from_le_bytes(b.get(..8)?.try_into().ok()?),
                _ => return None,
            };
            let null = key
                .null_flag
                .is_some_and(|flag| Self::is_null(row, flag, key.flag_pos));
            // NULL is greater than every value; the stored bytes of a NULL are
            // never compared, so they are zeroed out of the key. 65 bits: the
            // NULL bit above the value.
            const KEY_BITS: u128 = (1 << 65) - 1;
            let normalised: u128 = if null { 1 << 64 } else { value as u128 };
            let normalised = if key.descending { !normalised & KEY_BITS } else { normalised };
            // The row index rides in the low 32 bits, so one integer compare
            // orders by key and then by position, and an element is 16 bytes
            // rather than the 32 a `(u128, u32)` pair pads to.
            out.push((normalised << 32) | i as u128);
        }
        Some(out)
    }
}

/// The row order for packed `key << 32 | index` values: the first `cap` of
/// them when a cap is given, otherwise all. The index breaks ties, so the
/// order is total and equal to a stable sort by key.
fn order_from_integer_keys(mut pairs: Vec<u128>, cap: Option<usize>) -> Vec<u32> {
    if let Some(cap) = cap {
        if cap == 0 {
            return Vec::new();
        }
        if cap < pairs.len() {
            pairs.select_nth_unstable(cap - 1);
            pairs.truncate(cap);
        }
    }
    pairs.sort_unstable();
    pairs.into_iter().map(|packed| packed as u32).collect()
}

/// Puts `rows` in the order `order` lists — `order[k]` is the index of the row
/// that belongs at `k` — moving each row at most once, by following cycles.
/// `order` must be a permutation of `0..rows.len()`.
fn apply_row_order(rows: &mut [Row], order: &mut [u32]) {
    for start in 0..order.len() {
        if order[start] as usize == start {
            continue;
        }
        let mut cur = start;
        loop {
            let next = order[cur] as usize;
            order[cur] = cur as u32;
            if next == start {
                break;
            }
            rows.swap(cur, next);
            cur = next;
        }
    }
}

/// A stable sort of `rows` that moves each row at most once.
///
/// A `Row` is ~1.5 KB, and `sort_by` on a `Vec<Row>` moves rows O(n log n)
/// times and allocates scratch for half of them — 750 MB for a million rows.
/// Sorting `u32` indices instead and then applying the permutation in place by
/// following its cycles moves each row at most once and needs 4 bytes of
/// scratch per row. Stable on the indices is stable on the rows, so the order —
/// ties included — is exactly what `rows.sort_by(compare)` produces.
fn sort_rows_indirect(
    rows: &mut [Row],
    mut compare: impl FnMut(&Row, &Row) -> std::cmp::Ordering,
) {
    if rows.len() < 2 || rows.len() > u32::MAX as usize {
        rows.sort_by(compare);
        return;
    }
    let mut order: Vec<u32> = (0..rows.len() as u32).collect();
    order.sort_by(|&i, &j| compare(&rows[i as usize], &rows[j as usize]));
    // `order[k]` is the index of the row that belongs at `k`. Each cycle is
    // walked once; a slot is marked done by pointing it at itself.
    for start in 0..order.len() {
        if order[start] as usize == start {
            continue;
        }
        let mut cur = start;
        loop {
            let next = order[cur] as usize;
            order[cur] = cur as u32;
            if next == start {
                break;
            }
            rows.swap(cur, next);
            cur = next;
        }
    }
}

/// The `k` smallest of `rows` by `compare`, in order — what
/// `select_nth_unstable_by(k - 1)`, `truncate(k)`, `sort_by` produce — moving
/// only the `k` survivors rather than partitioning ~1.5 KB rows. Which of
/// several rows tied at the boundary survives is unspecified, as it is for the
/// selection this replaces. `k` must be in `1..rows.len()`.
fn select_rows_indirect(
    rows: &mut Vec<Row>,
    k: usize,
    mut compare: impl FnMut(&Row, &Row) -> std::cmp::Ordering,
) {
    debug_assert!(k >= 1 && k < rows.len());
    if rows.len() > u32::MAX as usize {
        rows.select_nth_unstable_by(k - 1, &mut compare);
        rows.truncate(k);
        rows.sort_by(compare);
        return;
    }
    let mut order: Vec<u32> = (0..rows.len() as u32).collect();
    order.select_nth_unstable_by(k - 1, |&i, &j| {
        compare(&rows[i as usize], &rows[j as usize])
    });
    order.truncate(k);
    order.sort_by(|&i, &j| compare(&rows[i as usize], &rows[j as usize]));
    let mut kept: Vec<Row> = Vec::with_capacity(k);
    for &i in &order {
        kept.push(std::mem::take(&mut rows[i as usize]));
    }
    *rows = kept;
}

/// Rows above which an `ORDER BY` sort leaves the async worker; see
/// [`ConcurrentProcessor::sort_rows_off_the_runtime`]. Below it the sort is
/// shorter than the hand-off is worth.
pub(crate) const SORT_OFF_RUNTIME_ROWS: usize = 20_000;

/// Turns the per-batch yield and the off-worker sort off, so the latency
/// benchmark can measure what they buy. Test builds only.
#[cfg(test)]
pub(crate) static LATENCY_GUARDS_OFF: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[inline(always)]
pub(crate) fn latency_guards_on() -> bool {
    #[cfg(test)]
    {
        !LATENCY_GUARDS_OFF.load(std::sync::atomic::Ordering::Relaxed)
    }
    #[cfg(not(test))]
    {
        true
    }
}

pub struct ConcurrentProcessor;

/// The fewest index candidates a scan worker is given. Below this the
/// cost of another worker outweighs the fetches it would take over.
const SPARSE_MIN_BATCH: usize = 512;

impl ConcurrentProcessor {
    pub fn new() -> Self {
        Self {}
    }

    /// Turns a finished worker fleet into a query-level error when something
    /// went wrong.
    ///
    /// Two failure modes are folded together here:
    ///
    ///  * a WHERE expression that is undefined for some row (`eval_error`), and
    ///  * a worker that panicked outright (`JoinError`).
    ///
    /// Both used to be discarded. Workers accumulate their matched rows locally
    /// and send them once at the end, so a worker that dies takes every row it
    /// had already matched with it — and `join_all`'s ignored `Result` meant the
    /// query then returned that truncated set as a success.
    fn check_worker_outcome(
        join_results: Vec<std::result::Result<(), tokio::task::JoinError>>,
        eval_error: &AtomicU8,
    ) -> std::io::Result<()> {
        if let Some(e) = EvalError::from_code(eval_error.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err(e.into());
        }

        for r in join_results {
            if let Err(join_err) = r {
                error!("Query worker terminated abnormally: {}", join_err);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("query worker terminated abnormally: {}", join_err),
                ));
            }
        }

        Ok(())
    }

    /// Row-mode cache fast-path.
    ///
    /// The cached set is every pointer the clause matched, so `ORDER BY` and
    /// `LIMIT` are applied here exactly as the scan path applies them — the
    /// entry is shared across every limit and ordering of the same clause.
    ///
    /// A pointer that will not read back means the entry describes storage
    /// that has moved. That is a miss, not a short answer: the whole entry is
    /// abandoned and the caller rescans. Skipping the row instead would return
    /// a silently truncated result set.
    async fn try_collect_cached_rows<S: StorageIO>(
        table_name: &str,
        where_query: &str,
        generation: Option<u64>,
        requested_row_fetch: &RowFetch,
        io_rows: &Arc<S>,
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
        limit: Option<u64>,
        order_by: &Option<String>,
        // Applied per pointer, because the cached set is by predicate and does
        // not know who is asking.
        visibility: &Visibility,
    ) -> Option<Vec<Row>> {
        let entry = query_cache::lookup(table_name, where_query, generation?, limit)?;

        // The pointers this entry holds were read by the scan that built it,
        // but they are read *again* here — every one of them is visibility
        // tested and fetched below. Counting them keeps `rows_scanned` telling
        // the truth about what a statement did: without this a cached
        // `ORDER BY … LIMIT` reported reading nothing while spending most of a
        // second reading a million rows, which is indistinguishable from the
        // materialisation blind spot `query_stats` already documents.
        #[cfg(feature = "query_stats")]
        crate::core::query_stats::note_rows_scanned(entry.pointers.len() as u64);

        // The same bound the scan path applies in its workers. Without it this
        // path fetched and cloned *every* matching row and only then sorted and
        // truncated: a `LIMIT 200` over a million-row table built a million-row
        // `Vec<Row>` to return two hundred. Since `Row` is ~1.5 KB that is over
        // a gigabyte, and it made the cache — which exists to make queries
        // faster — 4.8x *slower* than rescanning: 690-747 ms against 155 ms for
        // the identical statement with the cache off.
        //
        // `top_n_spec` returns `None` when there is nothing to keep rows out
        // with (no ordering, or no cap), and then this behaves exactly as
        // before: everything is accumulated, because everything is wanted.
        let top_n = Self::top_n_spec(schema_arc, order_by, limit);

        // Declining a hit, on purpose, when rescanning is the faster answer.
        //
        // This loop is **serial**: one `await` per pointer on one task. The scan
        // path fetches the same rows across every worker at once, so a cache hit
        // trades parallelism for not re-reading the pointer file. That is a good
        // trade whenever the fetch is small, and a bad one for `ORDER BY … LIMIT
        // k` over a large match set, where the ordering forbids stopping early
        // and every matched row is fetched under either plan. Measured on a
        // million-row table, `ORDER BY … LIMIT 200`, with the worker-side bound
        // in place on both sides:
        //
        //     rescan (parallel, 12 workers)    178 ms
        //     cache hit (serial fetch)         356 ms
        //
        // Only ordered queries are declined. Without an `ORDER BY` the caller
        // stops at the cap, the fetch really is small, and this cache is doing
        // the job it was built for.
        //
        // The ratio decides it rather than an absolute size: a cap that is a
        // large fraction of the match set fetches most of it either way, and
        // then the saved pointer read is pure profit. The floor keeps small
        // tables — where the whole entry is a few pages and a rescan costs more
        // than the fetch — on the fast path regardless.
        if let Some(top) = top_n.as_ref() {
            const RESCAN_MIN_POINTERS: usize = 20_000;
            const RESCAN_CAP_RATIO: usize = 50;
            if entry.pointers.len() >= RESCAN_MIN_POINTERS
                && entry.pointers.len() / top.cap.max(1) >= RESCAN_CAP_RATIO
            {
                return None;
            }
        }

        let mut collected_rows = match top_n.as_ref() {
            // Bounded, so the allocation no longer scales with the match set.
            // Twice the cap is the trim threshold below, so this is the high
            // water mark rather than a guess.
            Some(top) => Vec::with_capacity(top.cap.saturating_mul(2).min(entry.pointers.len())),
            None => Vec::with_capacity(entry.pointers.len()),
        };
        let mut row = Row::default();
        // Same deferral the scan path uses: while the top-N is only choosing
        // which rows survive, read the sort key and nothing else. This loop is
        // serial, so the payload it skips is the dominant cost of a hit.
        let key_fetch = top_n
            .as_ref()
            .map(|top| Self::top_n_key_fetch(schema_arc, top));
        let mut collected_pairs: Vec<(Row, RowPointer)> = Vec::new();

        for pointer in entry.pointers.iter() {
            // A cached entry holds the pointers matching the *predicate*, not
            // the ones a particular snapshot could see — that is what lets one
            // entry serve every snapshot. So visibility is applied here, per
            // pointer, and it must run **before** the fetch: reclaimed space is
            // reused, so fetch-then-check would read another row's bytes.
            //
            // `deleted` is passed rather than tested separately; `visible`
            // short-circuits on it, and that short-circuit is what covers the
            // reclamation branches that carry no `xmax`. See
            // `mvcc::visibility::reclamation_lemma`.
            if !visibility.visible(pointer.deleted, pointer.xmin, pointer.xmax) {
                continue;
            }

            let scan_fetch = key_fetch.as_ref().unwrap_or(requested_row_fetch);
            if pointer
                .fetch_row_reuse_async(&**io_rows, scan_fetch, &mut row)
                .await
                .is_err()
            {
                query_cache::record_abandoned_hit();
                return None;
            }

            if key_fetch.is_some() {
                collected_pairs.push((Row::clone_row(&row), pointer.clone()));
            } else {
                collected_rows.push(Row::clone_row(&row));
            }

            // Trimmed at twice the cap rather than every row, so the O(n)
            // selection is amortised over at least `cap` new rows — the same
            // bargain the worker loop strikes, and `select_nth_unstable_by`
            // partitions rather than sorts, which is all the final sort needs.
            //
            // That partition is unstable, so which of several *equal* rows
            // survives the cap is not defined — matching the scan path, which
            // has always been unstable here (see the note in
            // `sort_rows_if_needed`). Deliberately matching it: if this path
            // stayed stable, a query's tie order would depend on whether it hit
            // the cache, which is invisible state that differs between a warm
            // and a cold server. Uniform instability is the lesser evil.
            //
            // The hazard this leaves is real and predates all of it: paginating
            // `LIMIT`/`OFFSET` over a non-unique sort key can skip or repeat
            // rows between pages. The fix for that is a unique tiebreaker in the
            // `ORDER BY`, not a stable sort in one of the two paths.
            if let Some(top) = top_n.as_ref() {
                if key_fetch.is_some() {
                    if collected_pairs.len() > top.cap.saturating_mul(2) {
                        let sort = PreparedSort::new(
                            &top.keys,
                            schema_arc.as_slice(),
                            collected_pairs.first().map(|p| &p.0),
                        );
                        collected_pairs
                            .select_nth_unstable_by(top.cap - 1, |a, b| sort.compare(&a.0, &b.0));
                        collected_pairs.truncate(top.cap);
                    }
                } else if collected_rows.len() > top.cap.saturating_mul(2) {
                    let sort =
                        PreparedSort::new(&top.keys, schema_arc.as_slice(), collected_rows.first());
                    collected_rows.select_nth_unstable_by(top.cap - 1, |a, b| sort.compare(a, b));
                    collected_rows.truncate(top.cap);
                }
            }
        }

        // The payload, for the survivors only.
        if let (Some(_), Some(top)) = (key_fetch.as_ref(), top_n.as_ref()) {
            if collected_pairs.len() > top.cap {
                let sort = PreparedSort::new(
                    &top.keys,
                    schema_arc.as_slice(),
                    collected_pairs.first().map(|p| &p.0),
                );
                collected_pairs.select_nth_unstable_by(top.cap - 1, |a, b| sort.compare(&a.0, &b.0));
                collected_pairs.truncate(top.cap);
            }
            collected_rows.reserve(collected_pairs.len());
            for (key_row, pointer) in collected_pairs.drain(..) {
                if pointer
                    .fetch_row_reuse_async(&**io_rows, requested_row_fetch, &mut row)
                    .await
                    .is_err()
                {
                    query_cache::record_abandoned_hit();
                    return None;
                }
                // The extent may have been handed to a writer between the two
                // reads — see `keys_match`. Here that costs nothing to handle:
                // abandoning the hit rescans, which is a correct answer rather
                // than a failed statement.
                if !Self::keys_match(&key_row, &row, &top.keys) {
                    query_cache::record_abandoned_hit();
                    return None;
                }
                collected_rows.push(Row::clone_row(&row));
            }
        }

        // Still the authority on the answer's order and length: the trim above
        // only guarantees the survivors are the right *set*, not that they are
        // in order — the partition leaves them unordered by design.
        Self::sort_rows_off_the_runtime(&mut collected_rows, schema_arc, order_by, limit);

        Some(collected_rows)
    }

    /// Bytes-mode cache fast-path. Serializes matched rows directly into a
    /// `Vec<u8>` at the wire format, skipping the `Vec<Row>` intermediate
    /// and the `Row::clone_row` per pointer.
    ///
    /// Only reached with no `ORDER BY` (that case is delegated to row mode),
    /// so `LIMIT` is the only shaping left to apply.
    async fn try_collect_cached_bytes<S: StorageIO>(
        table_name: &str,
        where_query: &str,
        generation: Option<u64>,
        requested_row_fetch: &RowFetch,
        io_rows: &Arc<S>,
        limit: Option<u64>,
        // Applied per pointer, because the cached set is by predicate and does
        // not know who is asking.
        visibility: &Visibility,
    ) -> Option<Vec<u8>> {
        let entry = query_cache::lookup(table_name, where_query, generation?, limit)?;

        // Conservative initial guess; extend_serialized will grow as needed.
        let mut out: Vec<u8> = Vec::with_capacity(entry.pointers.len().saturating_mul(64));
        let mut row = Row::default();
        let mut emitted: u64 = 0;

        for pointer in entry.pointers.iter() {
            if let Some(limit_val) = limit {
                if emitted >= limit_val {
                    break;
                }
            }

            // See `try_collect_cached_rows` for why the slot flag and the
            // visibility rule are both needed here.
            if !visibility.visible(pointer.deleted, pointer.xmin, pointer.xmax) {
                continue;
            }

            // See `try_collect_cached_rows`: an unreadable pointer invalidates
            // the entry rather than shortening the answer.
            if pointer
                .fetch_row_reuse_async(&**io_rows, requested_row_fetch, &mut row)
                .await
                .is_err()
            {
                query_cache::record_abandoned_hit();
                return None;
            }

            row.extend_serialized(&mut out);
            emitted += 1;
        }

        Some(out)
    }

    /// The schema in the shape the workers and the cache fast-path both want.
    ///
    /// Built before the cache is consulted, because serving a cached answer
    /// still needs the schema to apply `ORDER BY`.
    #[inline]
    fn schema_arc(table_schema: &[SchemaField]) -> Arc<SmallVec<[SchemaField; 20]>> {
        Arc::new(table_schema.iter().cloned().collect())
    }

    #[inline]
    /// Tokenizes the predicate, or says why it cannot.
    ///
    /// The error is returned rather than raised: a clause naming something that
    /// is not a column is a client mistake, and it used to panic the worker and
    /// drop the connection with nothing reported.
    pub(crate) fn prepare_query(
        where_query: &str,
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
    ) -> Result<(bool, Arc<SmallVec<[Token; 36]>>), String> {
        // Empty WHERE means "match all rows"; skip tokenization/parsing entirely.
        let no_filter = where_query.trim().is_empty();
        let tokens: SmallVec<[Token; 36]> = if no_filter {
            SmallVec::new()
        } else {
            crate::core::tokenizer::query_tokenizer::try_tokenize(where_query, schema_arc)?
        };

        Ok((no_filter, Arc::new(tokens)))
    }

    #[inline]
    fn build_plan_columns<'a>(
        schema: &'a [SchemaField],
    ) -> SmallVec<[(Cow<'a, str>, MemoryBlock); 20]> {
        let max_write_order = schema
            .iter()
            .map(|field| field.write_order as usize)
            .max()
            .unwrap_or(0);

        let mut columns = SmallVec::new();
        columns.resize_with(max_write_order.saturating_add(1), || {
            (Cow::Borrowed(EMPTY_STR), MemoryBlock::default())
        });

        for field in schema {
            columns[field.write_order as usize] =
                (Cow::Borrowed(field.name.as_str()), MemoryBlock::default());
        }

        columns
    }

    /// Compiles the `WHERE` clause into a transformer plan.
    ///
    /// Returns the compile error rather than panicking on it. It used to
    /// `panic!`, which unwound the tokio task running the connection — so a
    /// client that sent an unsupported operator did not get an error, it got
    /// its **connection dropped** ("SSL error: unexpected eof"). A statement the
    /// engine cannot compile has to be answerable; killing the session is a
    /// denial of service triggerable by one malformed query.
    pub(crate) fn compile_query_plan(
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
        tokens_arc: &Arc<SmallVec<[Token; 36]>>,
    ) -> Result<TransformerPlan, String> {
        let plan_columns = Self::build_plan_columns(schema_arc.as_slice());
        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut intermediate_results: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let processor_cell = UnsafeCell::new(TransformerProcessor::new(
            &mut transformers,
            &mut intermediate_results,
        ));

        {
            let processor_mut = unsafe { &mut *processor_cell.get() };
            let mut parser = QueryParser::new(tokens_arc.as_ref(), processor_mut)
                .with_scales(schema_arc.as_slice());
            if let Err(e) = parser.execute(&plan_columns) {
                return Err(format!("could not compile the query plan: {}", e));
            }
        }

        Ok(unsafe { &*processor_cell.get() }.snapshot_plan())
    }

    /// Long-lived worker pool design.
    ///
    /// Spawns `MAX_PERMITS_THREADS` worker tasks ONCE up front. Each worker
    /// allocates its own `Buffer`, `TransformerProcessor`, and `QueryParser`
    /// once and reuses them across every batch it pulls from `work_rx`.
    /// The query plan is built once per worker on its first WHERE-filtered
    /// batch and reused for the rest of the query.
    ///
    /// The iterator-driving loop runs in this same task (the caller awaits
    /// the returned join handles). Backpressure is provided by the bounded
    /// MPMC `work_tx` channel: when all workers are busy, the iterator stops
    /// reading ahead. Per-batch work submitted to the pool is a single
    /// `Vec<RowPointer>` send — no `tokio::task::spawn`, no per-batch
    /// `JoinHandle`, no Semaphore/futex round-trip per batch.
    async fn spawn_batch_tasks<S: StorageIO>(
        iterator: &mut RowPointerIterator<S>,
        shared_tuple: Arc<(RowFetch, RowFetch)>,
        schema_arc: Arc<SmallVec<[SchemaField; 20]>>,
        compiled_plan: Option<Arc<TransformerPlan>>,
        io_rows_outer: Arc<S>,
        // Exactly ONE of `tx_rows` / `tx_bytes` should be Some, depending on
        // whether the caller wants `Vec<Row>` (e.g. for ORDER BY or row-level
        // consumers like database/mod.rs) or pre-serialized `Vec<u8>` (the
        // hot SELECT path — skips a full Row→Vec<u8> conversion downstream).
        tx_rows: Option<kanal::AsyncSender<Vec<Row>>>,
        tx_bytes: Option<kanal::AsyncSender<Vec<u8>>>,
        tx_rp: Option<kanal::AsyncSender<Vec<RowPointer>>>,
        limit: Option<u64>,
        enforce_limit_early: bool,
        no_filter: bool,
        // True when `query_row_fetch` already contains every column of `requested_row_fetch`,
        // so the second (requested) fetch can be skipped for matched rows.
        query_covers_requested: bool,
        limit_counter: Arc<CacheGuard<AtomicU64>>,
        stop_processing: Arc<CacheGuard<AtomicBool>>,
        reading_time_total: Arc<CacheGuard<AtomicU64>>,
        // First `EvalError` any worker hit, as `EvalError::to_code` (0 = none).
        // A WHERE expression that is undefined for some row must fail the whole
        // query; letting workers skip the row would return a short result set
        // that is indistinguishable from a complete one.
        eval_error: Arc<CacheGuard<AtomicU8>>,
        // Which row versions the workers may emit.
        visibility: Visibility,
        // Why this statement should stop, if it should.
        //
        // Passed in rather than read from the task-local: a task-local does not
        // cross `task::spawn`, and the workers are where the per-row work
        // actually happens. Checking only in the dispatch loop below was not
        // enough — dispatch hands out every batch long before the workers have
        // finished them, so the check stopped running while the scan was still
        // going.
        cancel: Option<StdArc<crate::core::cancel::StatementCancel>>,
        // `ORDER BY … LIMIT k`: each worker keeps only its own best `k` rows.
        //
        // A worker's discarded row cannot be in the global best `k` — it lost
        // to `k` rows this same worker is still holding — so the union of the
        // workers' survivors always contains the true answer, and the final
        // sort picks it out of that. Without this the scan accumulated every
        // matched row before sorting, which is how returning three rows from a
        // 940k-row table cost gigabytes.
        top_n: Option<Arc<TopN>>,
        // `SELECT COUNT(*)`: the answer is how many rows matched, so no row is
        // built, fetched or kept — the workers add to this instead.
        //
        // Only ever set for a plan whose every output is a bare `COUNT` (see
        // `aggregate::is_row_count_only`). Anything reading a column value,
        // grouping, or filtering per aggregate has to see the rows and takes
        // the ordinary path.
        //
        // Counted where a row is *emitted*, never where a pointer is
        // dispatched: the two differ by every row the predicate rejected and
        // every version this snapshot cannot see. `background_worker_lease` is
        // eight live rows in tens of thousands of slots, so counting slots
        // would be wrong by three orders of magnitude on a table someone reads
        // hourly.
        count_only: Option<Arc<CacheGuard<AtomicU64>>>,
        // Returned with the handles, not instead of them: the workers are
        // already running and must be joined before the error is reported.
        // `Some` means a pointer read failed and the scan stopped short, so the
        // rows the workers produced are a prefix and must not be returned.
    ) -> (
        Vec<tokio::task::JoinHandle<()>>,
        Option<crate::core::row::error::RowError>,
    ) {
        // As many workers as there is work to hand them, never more.
        //
        // This used to spawn `max_permits_threads()` — 16 by default — for
        // every scan whatever its size. Pointers are dispatched a batch at a
        // time and a batch is 10,000 of them, so a table under that size is a
        // single batch: one worker took it and the other fifteen were spawned,
        // parked on an empty channel, and joined again having done nothing.
        //
        // Measured on a table of one row, that overhead is the whole cost of
        // the query — a `COUNT(*)` over it took ~0.46 ms against ~0.15 ms for
        // `SELECT 1`. It is worse than it looks in a serial benchmark: a page
        // that fans out dozens of small queries at once multiplies the idle
        // spawns together and they contend for the same runtime as the workers
        // actually doing the reading.
        //
        // Large scans are unaffected: once there are at least as many batches
        // as threads this is exactly what it was.
        //
        // A slot list from an index is cut finer. Its every pointer is a row
        // to fetch rather than a bit to test, so a few thousand of them are
        // milliseconds of work, and at `batch_size()` per batch all of it fell
        // to one worker: an index answer of 5,000 rows ran slower than the
        // filter's scan of the whole table, which spread across every worker.
        // Small answers still get one worker; a point lookup is one batch.
        let threads = crate::max_permits_threads().max(1);
        let expected = iterator.expected_records();
        let per_batch = if iterator.has_sparse_candidates() {
            // Not `clamp`: it panics when the floor is above the ceiling, which
            // `--batch-size` below the floor makes it.
            let ceiling = crate::batch_size().max(1);
            let per = (expected.div_ceil(threads as u64) as usize)
                .max(SPARSE_MIN_BATCH)
                .min(ceiling);
            iterator.set_batch_limit(per);
            per
        } else {
            crate::batch_size().max(1)
        };
        let batches = expected.div_ceil(per_batch as u64).max(1);
        let n_workers = threads.min(batches as usize).max(1);

        // Bounded MPMC channel for batch dispatch. Capacity = n_workers gives
        // natural backpressure: the iterator stops reading ahead when every
        // worker has a batch in-hand.
        let (work_tx, work_rx) = kanal::bounded_async::<Vec<RowPointer>>(n_workers);

        // Deferring the payload only makes sense when rows are being produced
        // and something is throwing most of them away. Bytes mode never sorts,
        // and a bare count reads no columns at all, so neither qualifies.
        let deferred_payload: Option<Arc<RowFetch>> = match top_n.as_ref() {
            Some(top) if tx_rows.is_some() && tx_bytes.is_none() && count_only.is_none() => {
                Some(Arc::new(Self::top_n_key_fetch(&schema_arc, top)))
            }
            _ => None,
        };

        let mut worker_handles = Vec::with_capacity(n_workers);

        for _ in 0..n_workers {
            let work_rx = work_rx.clone();
            let tuple_clone = Arc::clone(&shared_tuple);
            let tx_rows_clone = tx_rows.as_ref().map(|s| s.clone());
            let tx_bytes_clone = tx_bytes.as_ref().map(|s| s.clone());
            let bytes_mode = tx_bytes_clone.is_some();
            let tx_rp_clone = tx_rp.as_ref().map(|s| s.clone());
            let schema_ref = Arc::clone(&schema_arc);
            let compiled_plan_ref = compiled_plan.as_ref().map(Arc::clone);
            let io_rows_batch = Arc::clone(&io_rows_outer);
            let limit_counter_ref = Arc::clone(&limit_counter);
            let stop_ref = Arc::clone(&stop_processing);
            let reading_time_ref = Arc::clone(&reading_time_total);
            let eval_error_ref = Arc::clone(&eval_error);
            let top_n_ref = top_n.as_ref().map(Arc::clone);
            let deferred_ref = deferred_payload.as_ref().map(Arc::clone);
            let count_only_ref = count_only.as_ref().map(Arc::clone);
            // Kept per worker and added once on exit. The shared counter is one
            // cache line every worker would otherwise write to per row, which is
            // the contention the `rows_scanned` counter is batched to avoid.
            let mut counted: u64 = 0;
            let cancel_ref = cancel.as_ref().map(StdArc::clone);
            let limit_opt = limit;
            let visibility_ref = visibility.clone();
            // Sampled once per worker rather than per row: it is a bound, not a
            // reading, so a later sample can only be more permissive and the
            // scan stays consistent with itself either way.
            let horizon_ref = crate::core::vacuum::VersionHorizon::for_visibility(&visibility);

            let h = task::spawn(async move {
                let io_rows_ref: &S = &*io_rows_batch;

                // Worker-local Buffer; allocated ONCE and reused across every
                // batch this worker processes. Same for the processor/parser
                // and the WHERE-evaluation plan (built once per worker on its
                // first batch).
                let mut buffer: Buffer<'_> = Buffer {
                    hashtable_buffer: UnsafeCell::new(SmallVec::new()),
                    row: Row::default(),
                    transformers: SmallVec::new(),
                    intermediate_results: SmallVec::new(),
                    bool_buffer: SmallVec::new(),
                    write_order_to_col: SmallVec::new(),
                };

                let processor_cell = UnsafeCell::new(TransformerProcessor::new(
                    &mut buffer.transformers,
                    &mut buffer.intermediate_results,
                ));
                let processor_mut = unsafe { &mut *processor_cell.get() };
                if !no_filter {
                    let plan = compiled_plan_ref
                        .as_ref()
                        .expect("compiled plan required for filtered query");
                    processor_mut.load_plan(plan.as_ref());
                }

                // Row layout for fetched WHERE columns is stable within a query.
                let mut row_layout_built = false;

                // Per-worker result accumulators. Sent ONCE at the end of
                // this worker's lifetime to avoid per-batch channel sends
                // (each kanal send takes a parking_lot mutex). Exactly one
                // of `acc_rows` / `acc_bytes` is populated, governed by
                // `bytes_mode`.
                let mut acc_rows: Vec<Row> = Vec::new();
                let mut acc_bytes: Vec<u8> = Vec::new();
                let mut acc_ptrs: Vec<RowPointer> = Vec::new();
                // Key-only rows plus the pointer to read the payload back from,
                // used in place of `acc_rows` while a top-N is deferring it.
                let mut acc_pairs: Vec<(Row, RowPointer)> = Vec::new();

                while let Ok(pointers) = work_rx.recv().await {
                    // Asked before the batch rather than after: the point is not
                    // to finish this batch faster but to stop starting new ones.
                    // The error is recorded before the stop flag, so the
                    // statement fails through `check_worker_outcome` rather than
                    // returning the rows it happened to reach.
                    if let Some(reason) = cancel_ref.as_ref().and_then(|c| c.tripped()) {
                        let _ = eval_error_ref.compare_exchange(
                            0,
                            reason.eval_error().to_code(),
                            std::sync::atomic::Ordering::AcqRel,
                            std::sync::atomic::Ordering::Acquire,
                        );
                        stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                    let batch_read_start = std::time::Instant::now();

                    if no_filter {
                        if !bytes_mode {
                            acc_rows.reserve(pointers.len());
                        } else {
                            // Rough byte budget per row — grows on demand.
                            acc_bytes.reserve(pointers.len() * 64);
                        }
                        if tx_rp_clone.is_some() {
                            acc_ptrs.reserve(pointers.len());
                        }

                        for pointer in pointers.iter() {
                            if unlikely(stop_ref.load(std::sync::atomic::Ordering::Relaxed)) {
                                break;
                            }
                            // Only slot vacancy gates the scan. Visibility is
                            // applied later, at the point a row is *emitted*,
                            // so a pointer that matches the predicate but is not
                            // visible to this snapshot still reaches the cache.
                            //
                            // That is what makes the cache sound across
                            // snapshots: the cached set is "pointers matching
                            // this WHERE clause", which does not depend on who
                            // is asking. Caching the *visible* set instead made
                            // an entry answerable only by the snapshot that
                            // built it — and since a commit changes visibility
                            // without writing anything, the generation counter
                            // could not tell when it had gone stale. The whole
                            // cache was therefore switched off under MVCC, which
                            // meant every repeat of a query re-scanned from
                            // slot 0.
                            if unlikely(pointer.deleted) {
                                continue;
                            }
                            // Cached whether or not this snapshot may see it.
                            if tx_rp_clone.is_some() {
                                acc_ptrs.push(pointer.clone());
                            }
                            // The visibility test runs *before* any fetch, and
                            // that ordering is load-bearing rather than
                            // stylistic: reclaimed space is reused, so
                            // fetch-then-check would read another row's bytes
                            // and only then discard them — one refactor away
                            // from returning them. See
                            // `mvcc::visibility::reclamation_lemma`.
                            if unlikely(!visibility_ref.visible(
                                pointer.deleted,
                                pointer.xmin,
                                pointer.xmax,
                            )) {
                                continue;
                            }

                            if unlikely(enforce_limit_early) {
                                if let Some(limit_val) = limit_opt {
                                    if limit_counter_ref.load(std::sync::atomic::Ordering::Relaxed)
                                        >= limit_val
                                    {
                                        stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                        break;
                                    }
                                }
                            }

                            let (_, requested_row_fetch) = &*tuple_clone;
                            let scan_fetch = deferred_ref.as_deref().unwrap_or(requested_row_fetch);

                            // Nothing reads a column, so nothing is read. With
                            // no predicate this branch is the whole query, and
                            // skipping the fetch is what turns `COUNT(*)` from
                            // a full table read into a pointer walk.
                            if count_only_ref.is_some() {
                                counted += 1;
                                continue;
                            }

                            if unlikely(
                                pointer
                                    .fetch_row_reuse_async(io_rows_ref, scan_fetch, &mut buffer.row)
                                    .await
                                    .is_err(),
                            ) {
                                continue;
                            }

                            if unlikely(enforce_limit_early) {
                                if let Some(limit_val) = limit_opt {
                                    let prev = limit_counter_ref
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    if unlikely(prev + 1 >= limit_val) {
                                        stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                    }
                                    // Skip the emit if we already reached the
                                    // limit on a sibling worker. Without this
                                    // gate, up to N_workers extra rows can be
                                    // emitted past LIMIT (the row-mode path
                                    // papers over this with a final truncate;
                                    // the bytes-mode path cannot).
                                    if unlikely(prev >= limit_val) {
                                        break;
                                    }
                                }
                            }

                            if likely(bytes_mode) {
                                buffer.row.extend_serialized(&mut acc_bytes);
                            } else if deferred_ref.is_some() {
                                acc_pairs.push((std::mem::take(&mut buffer.row), pointer.clone()));
                            } else {
                                acc_rows.push(std::mem::take(&mut buffer.row));
                            }
                        }
                    } else {
                        // WHERE-filtered path
                        let (query_row_fetch, requested_row_fetch) = &*tuple_clone;

                        for pointer in pointers.iter() {
                            if unlikely(stop_ref.load(std::sync::atomic::Ordering::Relaxed)) {
                                break;
                            }
                            // See the note in the unfiltered branch: the
                            // predicate still has to be evaluated for a row this
                            // snapshot cannot see, because the cached set is by
                            // predicate. So this cannot gate on `visible`.
                            if unlikely(pointer.deleted) {
                                continue;
                            }

                            // But it must gate on *reclaimable*, which is the
                            // strictly weaker test and the one this branch was
                            // missing. `deleted` alone is no gate at all under
                            // MVCC: `RowPointer::delete` stamps `xmax` and
                            // deliberately leaves `deleted` false for a versioned
                            // delete, and nothing sets it afterwards — so every
                            // transactionally-deleted row reached the fetch
                            // below. Once reclamation freed its extent and an
                            // insert reused it, this read returned another row's
                            // bytes; in production it surfaced as
                            // "Invalid string size" on pointers that all had
                            // `xmax` set.
                            //
                            // Skipping here loses nothing: by
                            // `mvcc::visibility::reclamation_lemma` a reclaimable
                            // version is invisible to every snapshot that can
                            // still be taken, so no cache entry built from this
                            // scan could ever need to serve it.
                            if unlikely(horizon_ref.is_reclaimable(
                                pointer.deleted,
                                pointer.xmin,
                                pointer.xmax,
                            )) {
                                continue;
                            }

                            // The reason is logged, not discarded: this fires in
                            // bursts on a hot row and the pointer alone says
                            // nothing about whether the read was short, the
                            // layout disagreed, or the slot had been reused.
                            if let Err(e) = pointer
                                .fetch_row_reuse_async(
                                    io_rows_ref,
                                    query_row_fetch,
                                    &mut buffer.row,
                                )
                                .await
                            {
                                error!(
                                    "Failed to fetch query row for pointer {:?}: {}",
                                    pointer, e
                                );
                                continue;
                            }

                            if !row_layout_built {
                                build_write_order_map(
                                    &mut buffer.write_order_to_col,
                                    &buffer.row,
                                    schema_ref.as_slice(),
                                );
                                row_layout_built = true;
                            }

                            let result = {
                                let processor = unsafe { &mut *processor_cell.get() };
                                processor.execute_row(
                                    &RowView::new(&buffer.row, &buffer.write_order_to_col),
                                    &mut buffer.bool_buffer,
                                )
                            };

                            let result = match result {
                                Ok(matched) => matched,
                                Err(e) => {
                                    // Record the first failure and stop every
                                    // worker. Continuing would emit whatever
                                    // this worker matched so far as if the
                                    // query had succeeded.
                                    let _ = eval_error_ref.compare_exchange(
                                        0,
                                        e.to_code(),
                                        std::sync::atomic::Ordering::AcqRel,
                                        std::sync::atomic::Ordering::Acquire,
                                    );
                                    stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                    break;
                                }
                            };

                            if result {
                                // Matches the predicate, so it belongs in the
                                // cached set regardless of who can see it.
                                if tx_rp_clone.is_some() {
                                    acc_ptrs.push(pointer.clone());
                                }
                                // The visibility test runs *before* any fetch, and
                                // that ordering is load-bearing rather than
                                // stylistic: reclaimed space is reused, so
                                // fetch-then-check would read another row's bytes
                                // and only then discard them — one refactor away
                                // from returning them. See
                                // `mvcc::visibility::reclamation_lemma`.
                                if unlikely(!visibility_ref.visible(
                                    pointer.deleted,
                                    pointer.xmin,
                                    pointer.xmax,
                                )) {
                                    continue;
                                }

                                // Matched the predicate and visible to this
                                // snapshot, which is exactly what a count
                                // counts. The payload fetch below is for
                                // columns nobody is going to read.
                                if count_only_ref.is_some() {
                                    counted += 1;
                                    continue;
                                }

                                // Overrides `query_covers_requested`: reusing
                                // the WHERE buffer would keep exactly the wide
                                // payload this deferral exists to skip.
                                let matched_fetch = deferred_ref
                                    .as_deref()
                                    .or((!query_covers_requested).then_some(requested_row_fetch));
                                if let Some(matched_fetch) = matched_fetch {
                                    if let Err(e) = pointer
                                        .fetch_row_reuse_async(
                                            io_rows_ref,
                                            matched_fetch,
                                            &mut buffer.row,
                                        )
                                        .await
                                    {
                                        error!(
                                            "Failed to fetch requested row for pointer {:?}: kind={:?} err={}",
                                            pointer,
                                            e.kind(),
                                            e
                                        );
                                        continue;
                                    }
                                }

                                if unlikely(enforce_limit_early) {
                                    if let Some(limit_val) = limit_opt {
                                        let prev = limit_counter_ref
                                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        if unlikely(prev + 1 >= limit_val) {
                                            stop_ref
                                                .store(true, std::sync::atomic::Ordering::Relaxed);
                                        }
                                        if unlikely(prev >= limit_val) {
                                            break;
                                        }
                                    }
                                }

                                if likely(bytes_mode) {
                                    buffer.row.extend_serialized(&mut acc_bytes);
                                } else if deferred_ref.is_some() {
                                    acc_pairs
                                        .push((std::mem::take(&mut buffer.row), pointer.clone()));
                                } else {
                                    acc_rows.push(std::mem::take(&mut buffer.row));
                                }
                            }
                        }
                    }

                    // Keep only this worker's best `cap` rows.
                    //
                    // Trimmed at twice the cap rather than every batch, so the
                    // O(n) selection is amortised over at least `cap` new rows.
                    // `select_nth_unstable_by` partitions rather than sorts:
                    // the survivors are unordered, which is all the final sort
                    // needs from them.
                    if let Some(top) = top_n_ref.as_ref() {
                        if deferred_ref.is_some() {
                            if acc_pairs.len() > top.cap.saturating_mul(2) {
                                let sort = PreparedSort::new(
                                    &top.keys,
                                    schema_ref.as_slice(),
                                    acc_pairs.first().map(|p| &p.0),
                                );
                                acc_pairs.select_nth_unstable_by(top.cap - 1, |a, b| {
                                    sort.compare(&a.0, &b.0)
                                });
                                acc_pairs.truncate(top.cap);
                            }
                        } else if acc_rows.len() > top.cap.saturating_mul(2) {
                            let sort = PreparedSort::new(
                                &top.keys,
                                schema_ref.as_slice(),
                                acc_rows.first(),
                            );
                            acc_rows
                                .select_nth_unstable_by(top.cap - 1, |a, b| sort.compare(a, b));
                            acc_rows.truncate(top.cap);
                        }
                    }

                    let elapsed_ns = batch_read_start.elapsed().as_nanos() as u64;
                    reading_time_ref.fetch_add(elapsed_ns, std::sync::atomic::Ordering::Relaxed);

                    // A batch is thousands of rows of work with no point at
                    // which tokio could run anything else: the channel hands
                    // over the next batch without suspending while one is
                    // queued, and a row read that hits memory completes
                    // without suspending either. A big scan therefore held its
                    // workers' threads for its whole length, and every other
                    // session's statement on those threads waited behind it.
                    // One yield per batch costs one reschedule per
                    // `batch_size()` rows.
                    if latency_guards_on() {
                        tokio::task::yield_now().await;
                    }
                }

                // The one wide read this scan does: `cap` rows rather than
                // every row the table holds. Everything above fetched a single
                // sort key per row.
                if let (Some(_), Some(top)) = (deferred_ref.as_ref(), top_n_ref.as_ref()) {
                    if acc_pairs.len() > top.cap {
                        let sort = PreparedSort::new(
                            &top.keys,
                            schema_ref.as_slice(),
                            acc_pairs.first().map(|p| &p.0),
                        );
                        acc_pairs
                            .select_nth_unstable_by(top.cap - 1, |a, b| sort.compare(&a.0, &b.0));
                        acc_pairs.truncate(top.cap);
                    }
                    let (_, requested_row_fetch) = &*tuple_clone;
                    acc_rows.reserve(acc_pairs.len());
                    for (key_row, pointer) in acc_pairs.drain(..) {
                        let mut row = Row::default();
                        // A row that will not read back is dropped, matching
                        // what the scan above does with the same failure.
                        if pointer
                            .fetch_row_reuse_async(io_rows_ref, requested_row_fetch, &mut row)
                            .await
                            .is_ok()
                        {
                            if !Self::keys_match(&key_row, &row, &top.keys) {
                                let _ = eval_error_ref.compare_exchange(
                                    0,
                                    EvalError::RowMovedUnderScan.to_code(),
                                    std::sync::atomic::Ordering::AcqRel,
                                    std::sync::atomic::Ordering::Acquire,
                                );
                                stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                break;
                            }
                            acc_rows.push(row);
                        }
                    }
                }

                // One add per worker rather than one per row.
                if let Some(counter) = count_only_ref.as_ref() {
                    counter.fetch_add(counted, std::sync::atomic::Ordering::Relaxed);
                }

                // Single send per worker on exit.
                if let Some(tx) = tx_rows_clone.as_ref() {
                    if !acc_rows.is_empty() {
                        let _ = tx.send(acc_rows).await;
                    }
                }
                if let Some(tx) = tx_bytes_clone.as_ref() {
                    if !acc_bytes.is_empty() {
                        let _ = tx.send(acc_bytes).await;
                    }
                }
                if let Some(ref tx_rp) = tx_rp_clone {
                    if !acc_ptrs.is_empty() {
                        let _ = tx_rp.send(acc_ptrs).await;
                    }
                }
            });

            worker_handles.push(h);
        }

        // Drop our copy of the receiver; once the iterator finishes and we
        // drop work_tx, only the workers' clones remain — when they finish
        // their last batch the channel closes and recv() returns Err, exiting
        // the loop.
        drop(work_rx);

        // Drive the iterator inline. No more per-batch task::spawn.
        iterator.reset();

        let mut pointer_read_error: Option<crate::core::row::error::RowError> = None;

        loop {
            if stop_processing.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            // Direct allocation; with BATCH_SIZE up to 65536 RowPointers per
            // batch, this is one alloc per ~thousands of rows — negligible.
            let mut pointers: Vec<RowPointer> = Vec::with_capacity(crate::batch_size());

            // "No more pointers" and "the read failed" are different answers
            // and must not share a branch. The end of the table is `Ok` with an
            // empty batch: the iterator already turns every benign end of data
            // into that — the declared length reached, a trailing partial
            // record, and a concurrent append's reserved-but-unwritten tail
            // (`load_next_chunk`'s `UnexpectedEof` arm). So an `Err` here is a
            // real I/O failure or corruption, and it fails the statement.
            //
            // This used to clear the batch and fall into the `is_empty` break,
            // which ended the scan as though the table had ended: a truncated
            // result reported as a success, and — with the query cache on — a
            // truncated match set stored as a *complete* one, because the stop
            // flag stayed clear. The batch that failed is discarded rather than
            // dispatched: the statement is failing, so its rows are not wanted.
            if let Err(e) = iterator.next_row_pointers_into(&mut pointers).await {
                pointer_read_error = Some(e);
                stop_processing.store(true, std::sync::atomic::Ordering::Relaxed);
                break;
            }

            if pointers.is_empty() {
                break;
            }

            // The candidate rows this statement has had to look at. Counted
            // here rather than in the workers: this loop runs in the task that
            // is executing the statement, so the counter is reachable without
            // being passed down, and one add per batch is not a contended cache
            // line in the per-row loop. Pointers the filter index excluded were
            // dropped inside the iterator and so are not counted, which is what
            // makes the figure show that the index worked.
            #[cfg(feature = "query_stats")]
            crate::core::query_stats::note_rows_scanned(pointers.len() as u64);

            // Whether this statement is still allowed to run. Checked here, in
            // the task executing it, because the signal is a task-local and
            // does not cross the `task::spawn` that made the workers.
            //
            // The error is recorded BEFORE the stop flag is set, and the order
            // is the whole point: `check_worker_outcome` reads the error, and a
            // stop flag on its own is indistinguishable from a scan that
            // finished — so a cancelled statement would return the rows it
            // happened to reach as though they were the complete answer.
            if let Some(reason) = crate::core::cancel::tripped() {
                let _ = eval_error.compare_exchange(
                    0,
                    reason.eval_error().to_code(),
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                );
                stop_processing.store(true, std::sync::atomic::Ordering::Relaxed);
                break;
            }

            if stop_processing.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            // Bounded send: blocks the iterator when all workers are busy.
            if work_tx.send(pointers).await.is_err() {
                break;
            }
        }

        // Closing work_tx tells workers no more batches are coming.
        drop(work_tx);

        (worker_handles, pointer_read_error)
    }

    // Old per-batch spawning loop removed; kept signature-compatible.

    #[inline]
    fn drain_rows(rx: kanal::AsyncReceiver<Vec<Row>>) -> Vec<Row> {
        if rx.is_empty() {
            drop(rx);
            return Vec::new();
        }
        let mut chunks: Vec<Vec<Row>> = Vec::new();
        rx.drain_into(&mut chunks).unwrap();
        drop(rx);

        if chunks.is_empty() {
            return Vec::new();
        }

        // Take the largest chunk as the base. `Vec::append` moves bytes via
        // ptr::copy_nonoverlapping + length update, but the base chunk's own
        // bytes are NEVER copied — they stay in their original allocation.
        // With 8 worker chunks of ~equal size, this saves ~12.5% of the
        // total Row-struct memcpy traffic; with skewed chunks it saves more.
        let mut largest_idx = 0;
        let mut largest_len = 0usize;
        for (i, c) in chunks.iter().enumerate() {
            if c.len() > largest_len {
                largest_len = c.len();
                largest_idx = i;
            }
        }
        let mut collected_rows = chunks.swap_remove(largest_idx);
        let total: usize = collected_rows.len() + chunks.iter().map(|c| c.len()).sum::<usize>();
        collected_rows.reserve(total - collected_rows.len());
        for mut c in chunks {
            collected_rows.append(&mut c);
        }
        collected_rows
    }

    #[inline]
    fn drain_row_pointers(rx_rp: kanal::AsyncReceiver<Vec<RowPointer>>) -> Vec<RowPointer> {
        if rx_rp.is_empty() {
            drop(rx_rp);
            return Vec::new();
        }
        let mut chunks: Vec<Vec<RowPointer>> = Vec::new();
        rx_rp.drain_into(&mut chunks).unwrap();
        drop(rx_rp);

        if chunks.is_empty() {
            return Vec::new();
        }

        let mut largest_idx = 0;
        let mut largest_len = 0usize;
        for (i, c) in chunks.iter().enumerate() {
            if c.len() > largest_len {
                largest_len = c.len();
                largest_idx = i;
            }
        }
        let mut pointers = chunks.swap_remove(largest_idx);
        let total: usize = pointers.len() + chunks.iter().map(|c| c.len()).sum::<usize>();
        pointers.reserve(total - pointers.len());
        for mut c in chunks {
            pointers.append(&mut c);
        }
        pointers
    }

    /// Orders `all_rows` for `ORDER BY`, applying `limit` as part of the same
    /// step rather than after it.
    ///
    /// A full sort is the wrong algorithm for `ORDER BY … LIMIT k`. The answer
    /// only needs the `k` smallest rows, which is a *selection* problem —
    /// `select_nth_unstable_by` partitions in O(n) and then only those `k` are
    /// sorted, against O(n log n) to order a million rows and throw all but two
    /// hundred of them away. On a 1.18M-row table with `LIMIT 200` that is
    /// roughly 1.2M comparisons instead of 23M.
    ///
    /// It does not reduce what the scan *reads*: `enforce_limit_early` is off
    /// whenever `ORDER BY` is present, because the first `k` rows in scan order
    /// are not the first `k` in sort order. Only an index on the sort column
    /// removes that, and there isn't one.
    ///
    /// Builds the worker-side top-N for an `ORDER BY … LIMIT`, if there is one.
    ///
    /// `None` when the statement has no ordering or no cap — with nothing to
    /// keep rows *out*, every matched row has to be accumulated.
    /// The columns a top-N needs in order to decide which rows survive.
    ///
    /// The sort keys and their `__null_x` companions, and nothing else. While a
    /// scan is only choosing the best `cap` rows, every other column it reads is
    /// thrown away — so on a wide table `SELECT * … ORDER BY … LIMIT 200` spends
    /// most of its time materialising columns that lose. Measured on a live
    /// 940,000-row table with 20 columns, the same query cost 664 ms fetching
    /// everything and 303 ms fetching only the key.
    fn top_n_key_fetch(schema_arc: &Arc<SmallVec<[SchemaField; 20]>>, top: &TopN) -> RowFetch {
        let mut fields: Vec<SchemaField> = Vec::new();
        for key in &top.keys {
            // The companion is not optional: without it a nullable key sorts by
            // its stored bytes, which is the bug `null_flag` exists to prevent.
            for schema_id in std::iter::once(key.schema_id).chain(key.null_flag) {
                if let Some(field) = schema_arc
                    .iter()
                    .find(|f| f.write_order as usize == schema_id)
                {
                    if !fields.iter().any(|f| f.write_order == field.write_order) {
                        fields.push(field.clone());
                    }
                }
            }
        }
        crate::core::rql::join::fetch_for(&fields, schema_arc)
    }

    fn top_n_spec(
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
        order_by: &Option<String>,
        limit: Option<u64>,
    ) -> Option<Arc<TopN>> {
        let cap = limit.filter(|l| *l > 0)? as usize;
        let keys = Self::resolve_sort_keys(schema_arc, order_by.as_deref()?);
        if keys.is_empty() {
            return None;
        }
        Some(Arc::new(TopN { keys, cap }))
    }

    /// The sort keys an `ORDER BY` spec names, as `(schema_id, descending)`.
    ///
    /// Shared by the worker-side top-N and the final sort so the two cannot
    /// disagree about the order they are producing — the same reason the
    /// partition and the sort below share one comparator.
    fn resolve_sort_keys(
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
        order_spec: &str,
    ) -> Vec<SortKey> {
        let mut keys: Vec<SortKey> = Vec::new();
        for (name, descending) in crate::core::rql::order_by::parse_spec(order_spec) {
            // Tombstones excluded: ordering by a dropped column's write_order
            // sorts by a column no fetch includes, which now reads as absent
            // rather than as another column's bytes — an unordered result
            // reported as ordered.
            match schema_arc
                .iter()
                .find(|f| !f.is_deleted && f.name.eq_ignore_ascii_case(&name))
                .map(|f| f.write_order as usize)
            {
                Some(schema_id) => keys.push(SortKey {
                    schema_id,
                    descending,
                    // Looked up here rather than carried in the spec: the spec
                    // is a column list, and the companion is derivable from the
                    // schema the keys are already being resolved against.
                    null_flag: crate::core::rql::nullable::flag_field(&name, schema_arc)
                        .map(|f| f.write_order as usize),
                }),
                // The planner resolved every key against this same schema, so
                // this is unreachable in practice. Dropping just the one key
                // still sorts by the rest, which beats abandoning the sort.
                None => log::warn!(
                    "ORDER BY field '{}' not found in schema; skipping key",
                    name
                ),
            }
        }
        keys
    }

    /// One sort key's stored bytes, when the row carries that column.
    fn key_bytes(row: &Row, schema_id: usize) -> Option<&[u8]> {
        row.columns
            .iter()
            .find(|c| c.schema_id as usize == schema_id)
            .map(|c| c.data.into_slice())
    }

    /// Whether a re-read row still carries the key it was kept for.
    ///
    /// A plain `SELECT` registers no running transaction — `effective_tx` is
    /// `FROZEN_TX` — so `oldest_active` is not pinned while it runs, and the
    /// reclamation lemma only guarantees that reclaimable and visible are
    /// exclusive *at the moment of the test*. A row this scan saw as visible
    /// can therefore be deleted, marked by vacuum, and have its extent handed
    /// to a writer before the payload is read. Every other path fetches
    /// immediately after the visibility test and is barely exposed; deferring
    /// the payload widens that window to the length of the scan.
    ///
    /// So the payload is checked against the key it was sorted by. This is not
    /// a proof — a reused extent could hold the same key bytes — but it turns
    /// the realistic case from a wrong row served as a right one into a failed
    /// statement, which is the trade this engine makes everywhere else.
    fn keys_match(kept: &Row, reread: &Row, keys: &[SortKey]) -> bool {
        keys.iter().all(|key| {
            std::iter::once(key.schema_id)
                .chain(key.null_flag)
                .all(
                    |id| match (Self::key_bytes(kept, id), Self::key_bytes(reread, id)) {
                        (Some(a), Some(b)) => a == b,
                        // What the payload fetch does not carry, it cannot
                        // contradict.
                        _ => true,
                    },
                )
        })
    }

    /// Whether a row's nullable column holds `NULL`, per its companion.
    #[cfg(test)]
    fn column_is_null(row: &Row, flag_schema_id: usize) -> bool {
        row.columns
            .iter()
            .find(|c| c.schema_id as usize == flag_schema_id)
            .and_then(|c| c.data.into_slice().first().copied())
            .is_some_and(|byte| byte != 0)
    }

    /// Orders two rows by a resolved key list, resolving every key's column and
    /// type again on each call. The comparator [`PreparedSort`] replaced; kept
    /// as the reference the tests hold it to.
    #[cfg(test)]
    fn compare_by_keys(
        a: &Row,
        b: &Row,
        keys: &[SortKey],
        schema: &[SchemaField],
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        // Later keys only break the ties the earlier ones left.
        for key in keys.iter() {
            let ord = match key.null_flag {
                // A NULL sorts as larger than every value, so it lands last
                // ascending and first descending — which is where PostgreSQL
                // puts it, and the reversal below applies to it too.
                //
                // The comparison of the stored bytes is skipped entirely when
                // either side is NULL: those bytes are the type's zeroes and
                // mean nothing.
                Some(flag) => {
                    let a_null = Self::column_is_null(a, flag);
                    let b_null = Self::column_is_null(b, flag);
                    match (a_null, b_null) {
                        (true, true) => Ordering::Equal,
                        (true, false) => Ordering::Greater,
                        (false, true) => Ordering::Less,
                        (false, false) => compare_rows_by_schema_id(a, b, key.schema_id, schema),
                    }
                }
                None => compare_rows_by_schema_id(a, b, key.schema_id, schema),
            };
            let ord = if key.descending { ord.reverse() } else { ord };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }

    /// [`Self::sort_rows_if_needed`], with a large sort taken off the async
    /// worker it is running on.
    ///
    /// Sorting a million rows is hundreds of milliseconds of CPU with no await
    /// in it, and on a tokio worker that is hundreds of milliseconds in which
    /// every other task queued on that thread — other sessions' point reads —
    /// cannot run. `block_in_place` hands those tasks to another worker for the
    /// duration instead. It needs the multi-threaded runtime (the server's); on
    /// a current-thread runtime there is nowhere to hand them, and the sort runs
    /// inline as before.
    fn sort_rows_off_the_runtime(
        all_rows: &mut Vec<Row>,
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
        order_by: &Option<String>,
        limit: Option<u64>,
    ) {
        let large = order_by.is_some() && all_rows.len() >= SORT_OFF_RUNTIME_ROWS;
        let multi_thread = tokio::runtime::Handle::try_current().is_ok_and(|h| {
            h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
        });
        if large && multi_thread && latency_guards_on() {
            tokio::task::block_in_place(|| {
                Self::sort_rows_if_needed(all_rows, schema_arc, order_by, limit)
            });
        } else {
            Self::sort_rows_if_needed(all_rows, schema_arc, order_by, limit);
        }
    }

    /// One observable difference, and it is within what SQL allows: `sort_by`
    /// is stable while `select_nth_unstable_by` is not, so which of several
    /// *equal* rows survives the limit may change. `ORDER BY` leaves the order
    /// of tied rows unspecified, so this is a licence the language grants —
    /// but a client relying on the previous incidental stability would notice.
    fn sort_rows_if_needed(
        all_rows: &mut Vec<Row>,
        schema_arc: &Arc<SmallVec<[SchemaField; 20]>>,
        order_by: &Option<String>,
        limit: Option<u64>,
    ) {
        // ORDER BY: after collection, sort the resulting rows.
        // NOTE: Row/Column schema_id uses SchemaField.write_order (not vector index).
        let truncate_to = limit.map(|l| l as usize);
        let Some(order_spec) = order_by else {
            // No ordering, but the cap still applies.
            if let Some(k) = truncate_to {
                all_rows.truncate(k);
            }
            return;
        };

        // The spec is a comma-separated list of column names, each optionally
        // followed by ASC or DESC. Carrying the keys inside the existing string
        // keeps every signature between the SQL front end and here unchanged;
        // only this function needs to know the sort has keys and directions.
        let keys = Self::resolve_sort_keys(schema_arc, order_spec);

        if keys.is_empty() {
            // Nothing resolvable to sort by; the cap still applies.
            if let Some(k) = truncate_to {
                all_rows.truncate(k);
            }
            return;
        }

        // Shared by both paths so the partition and the final sort cannot
        // disagree about the order they are producing.
        let sort = PreparedSort::new(&keys, schema_arc.as_slice(), all_rows.first());

        // One integer key: sort the keys, not the rows (see `integer_keys`).
        if let Some(pairs) = sort.integer_keys(all_rows) {
            match truncate_to {
                Some(k) if k < all_rows.len() => {
                    let order = order_from_integer_keys(pairs, Some(k));
                    let mut kept: Vec<Row> = Vec::with_capacity(order.len());
                    for &i in &order {
                        kept.push(std::mem::take(&mut all_rows[i as usize]));
                    }
                    *all_rows = kept;
                }
                _ => {
                    let mut order = order_from_integer_keys(pairs, None);
                    apply_row_order(all_rows, &mut order);
                }
            }
            return;
        }

        let compare = |a: &Row, b: &Row| sort.compare(a, b);

        match truncate_to {
            // Select, then order only the survivors.
            Some(k) if k < all_rows.len() => {
                if k == 0 {
                    all_rows.clear();
                    return;
                }
                // Selects the `k` smallest in O(n) rather than sorting
                // everything first — over indices, so only the survivors move.
                select_rows_indirect(all_rows, k, compare);
            }
            // Asking for at least as many rows as exist: the selection step
            // would be pure overhead.
            _ => sort_rows_indirect(all_rows, compare),
        }
    }

    /// How many rows match, without building any of them.
    ///
    /// The answer to `SELECT COUNT(*) [WHERE …]` — see
    /// [`crate::core::rql::aggregate::is_row_count_only`] for exactly which
    /// plans may come here. The ordinary aggregate path materialises every
    /// matching row into a `Vec<Row>` and folds over it, which for a count is
    /// a gigabyte and a half of `Row` at 1560 bytes each to produce one
    /// integer; measured on a million-row table that is ~625 ms, against
    /// ~155 ms for a full five-column `ORDER BY … LIMIT 200` over the *same*
    /// table. Counting cost four times what sorting did.
    ///
    /// No `LIMIT` and no `ORDER BY` are accepted, because neither means
    /// anything to a count of the whole match set — the caller's `LIMIT`
    /// applies to the aggregated output, not to its input, and passing it down
    /// here would truncate the scan and count a prefix.
    pub async fn process_count<'a, S: StorageIO>(
        &self,
        table_name: &str,
        where_query: &str,
        query_row_fetch: RowFetch,
        table_schema: &Vec<SchemaField>,
        io_rows: Arc<S>,
        iterator: &mut RowPointerIterator<S>,
        generation: Option<u64>,
        visibility: Visibility,
    ) -> std::io::Result<u64> {
        let schema_arc = Self::schema_arc(table_schema);

        // A cached entry is the set of pointers matching this predicate, so the
        // count is how many of them this snapshot can see. Visibility is per
        // reader and the entry is not, which is why it is retested here rather
        // than trusted — the same reason `try_collect_cached_rows` retests it.
        //
        // No fetch: unlike the row path there is nothing to read out of the
        // rows, so a cached count never touches the data file at all.
        if let Some(table_generation) = generation {
            if let Some(entry) =
                query_cache::lookup(table_name, where_query, table_generation, None)
            {
                #[cfg(feature = "query_stats")]
                crate::core::query_stats::note_rows_scanned(entry.pointers.len() as u64);
                return Ok(entry
                    .pointers
                    .iter()
                    .filter(|p| visibility.visible(p.deleted, p.xmin, p.xmax))
                    .count() as u64);
            }
        }

        let (no_filter, tokens_arc) =
            Self::prepare_query(where_query, &schema_arc).map_err(std::io::Error::other)?;
        let compiled_plan = if no_filter {
            None
        } else {
            Some(Arc::new(
                Self::compile_query_plan(&schema_arc, &tokens_arc)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?,
            ))
        };

        // Both halves are the WHERE's own fetch. The second is what a matched
        // row would have been read into, and a count reads nothing, so there is
        // no requested set to describe.
        let shared_tuple: Arc<(RowFetch, RowFetch)> =
            Arc::new((query_row_fetch.clone(), query_row_fetch));

        let counter: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());
        let limit_counter: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());
        let stop_processing: Arc<CacheGuard<AtomicBool>> = Arc::new(AtomicBool::new(false).into());
        let eval_error: Arc<CacheGuard<AtomicU8>> = Arc::new(AtomicU8::new(0).into());
        let reading_time_total: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());

        let (batch_handles, pointer_read_error) = Self::spawn_batch_tasks(
            iterator,
            Arc::clone(&shared_tuple),
            Arc::clone(&schema_arc),
            compiled_plan,
            Arc::clone(&io_rows),
            // No row channel and no bytes channel: nothing is emitted.
            None,
            None,
            // No pointer channel either. Storing a cache entry from here would
            // be sound, but the entry a count builds is one no row query can
            // use without re-reading everything anyway, and populating it costs
            // a `RowPointer` clone per match on the path whose whole point is
            // to touch nothing.
            None,
            None,
            false,
            no_filter,
            // Meaningless with no requested fetch; the count path never reaches
            // the branch that reads it.
            false,
            Arc::clone(&limit_counter),
            Arc::clone(&stop_processing),
            Arc::clone(&reading_time_total),
            Arc::clone(&eval_error),
            visibility,
            crate::core::cancel::current(),
            None,
            Some(Arc::clone(&counter)),
        )
        .await;

        let join_results = join_all(batch_handles).await;
        // After the join, so no worker outlives the statement it served.
        if let Some(e) = pointer_read_error {
            return Err(e.into());
        }
        Self::check_worker_outcome(join_results, &eval_error)?;

        Ok(counter.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub async fn process<'a, S: StorageIO>(
        &self,
        table_name: &str,
        where_query: &str,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        table_schema: &[SchemaField],
        io_rows: Arc<S>,
        iterator: &mut RowPointerIterator<S>,
        limit: Option<u64>,
        order_by: Option<String>,
        // `generation` is the table's cache generation, read before this scan
        // began. `None` opts the query out of the cache entirely — used by
        // callers with no table handle to read a generation from, whose results
        // must never be mistaken for a user query against the same clause.
        generation: Option<u64>,
        // Which row versions this query may see.
        visibility: Visibility,
    ) -> std::io::Result<Vec<Row>> {
        let prep_stopwatch = std::time::Instant::now();

        log::info!("Starting concurrent processing for query: {}", where_query);

        let schema_arc = Self::schema_arc(table_schema);

        // Cache fast-path (if enabled)
        if let Some(rows) = Self::try_collect_cached_rows(
            table_name,
            where_query,
            generation,
            &requested_row_fetch,
            &io_rows,
            &schema_arc,
            limit,
            &order_by,
            &visibility,
        )
        .await
        {
            return Ok(rows);
        }

        let (tx, rx) = kanal::unbounded_async::<Vec<Row>>();

        let cache_enabled = query_cache::enabled() && generation.is_some();

        // Only allocate the pointer channel when caching is on. When caching is off we
        // can avoid one channel allocation, an Arc clone per batch, an Arc clone per
        // matched row, and a `RowPointer::clone()` + `send().await` per matched row.
        let (tx_rp, rx_rp) = if cache_enabled {
            let (a, b) = kanal::unbounded_async::<Vec<RowPointer>>();
            (Some(a), Some(b))
        } else {
            (None, None)
        };

        // LIMIT enforcement: global counter across all tasks.
        // The increment/threshold check is performed inside the `if result { }` section.
        let limit_counter: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());
        let stop_processing: Arc<CacheGuard<AtomicBool>> = Arc::new(AtomicBool::new(false).into());
        let eval_error: Arc<CacheGuard<AtomicU8>> = Arc::new(AtomicU8::new(0).into());

        // Important: for ORDER BY queries, we must not stop early based on LIMIT,
        // otherwise we only sort a prefix of the matching rows.
        let enforce_limit_early = limit.is_some() && order_by.is_none();

        let shared_tuple: Arc<(RowFetch, RowFetch)> =
            Arc::new((query_row_fetch, requested_row_fetch));

        let io_rows_outer = Arc::clone(&io_rows);

        let (no_filter, tokens_arc) =
            Self::prepare_query(where_query, &schema_arc).map_err(std::io::Error::other)?;
        let compiled_plan = if no_filter {
            None
        } else {
            Some(Arc::new(
                Self::compile_query_plan(&schema_arc, &tokens_arc)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?,
            ))
        };

        // If the WHERE-evaluation columns already cover the SELECT columns, we can
        // skip the second (requested) fetch for matched rows and emit the WHERE buffer
        // directly. This is common for queries like `SELECT a, b WHERE a > 5 AND b < 10`.
        // Comparison is by `schema_id` (a unique key per schema field).
        let query_covers_requested = {
            let q = &shared_tuple.0.columns_fetching_data;
            let r = &shared_tuple.1.columns_fetching_data;
            // The optimisation emits the WHERE buffer *as it is*, so the
            // requested columns must already sit at its front in the requested
            // order. Testing only for set inclusion was wrong: the WHERE
            // columns are gathered through a `HashSet`, so their order varies
            // run to run, and a covered query could return its columns
            // transposed — `SELECT a, b` answered with `b, a`.
            !no_filter
                && q.len() >= r.len()
                && r.iter()
                    .zip(q.iter())
                    .all(|(rc, qc)| qc.schema_id == rc.schema_id)
        };

        log::info!(
            "Preperation for query completed in {:.2?}",
            prep_stopwatch.elapsed()
        );

        let reading_time_total: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());

        let (batch_handles, pointer_read_error) = Self::spawn_batch_tasks(
            iterator,
            Arc::clone(&shared_tuple),
            Arc::clone(&schema_arc),
            compiled_plan.clone(),
            io_rows_outer.clone(),
            Some(tx.clone()),
            None,
            tx_rp.clone(),
            limit,
            enforce_limit_early,
            no_filter,
            query_covers_requested,
            Arc::clone(&limit_counter),
            Arc::clone(&stop_processing),
            Arc::clone(&reading_time_total),
            Arc::clone(&eval_error),
            visibility.clone(),
            crate::core::cancel::current(),
            // Applied even while the row pointers are being collected.
            //
            // This used to be `if tx_rp.is_some() { None }`, on the reasoning
            // that a caller enumerating the whole match set needs the rows it
            // keeps to correspond to the pointers. They do not correspond, and
            // nothing asks them to: a worker accumulates `acc_ptrs` and
            // `acc_rows` independently and sends them on separate channels, the
            // trim below touches only the rows, and the drained pointers have
            // exactly one consumer — `query_cache::store`. The pointer set stays
            // complete whether or not the rows are bounded.
            //
            // The cost of the old reading was that it disabled the bound
            // whenever it could possibly matter. `tx_rp` is `Some` exactly when
            // the query cache is on, which is the production configuration, so
            // `ORDER BY … LIMIT k` accumulated **every** matched row on every
            // server that had caching enabled — a million-row `Vec<Row>`, ~1.5 GB
            // at 1560 bytes each, to return two hundred rows.
            //
            // Truncating the scan is a different thing and is still forbidden
            // here: `enforce_limit_early` is already `limit.is_some() &&
            // order_by.is_none()`, so an ordered scan runs to completion, does
            // not set `stopped_early`, and caches a complete match set.
            Self::top_n_spec(&schema_arc, &order_by, limit),
            // Rows are the answer here.
            None,
        )
        .await;

        // Wait for all batch tasks to complete
        let join_results = join_all(batch_handles).await;

        // Drop the sender to signal that no more rows will be sent
        drop(tx);

        // After the join, so no worker outlives the statement it served.
        if let Some(e) = pointer_read_error {
            return Err(e.into());
        }
        Self::check_worker_outcome(join_results, &eval_error)?;

        let mut all_rows = Self::drain_rows(rx);

        // The cap is applied inside, as part of the ordering: `ORDER BY … LIMIT k`
        // is a selection problem, not a sort-then-discard one.
        Self::sort_rows_off_the_runtime(&mut all_rows, &schema_arc, &order_by, limit);

        if let Some(rx_rp) = rx_rp {
            let pointers = Self::drain_row_pointers(rx_rp);

            // A scan that stopped on LIMIT saw only a prefix of the matches,
            // so it is stored under a key that includes that limit. Storing it
            // under the bare clause would answer the *unlimited* form with a
            // truncated set; keying it separately means the prefix can still
            // serve repeats of the same `WHERE ... LIMIT n`, which is the shape
            // a paging client sends over and over.
            //
            // Before this, such a query was simply never cached, so every
            // repeat re-ran the whole scan — including the leading stretch of
            // non-matching rows, which for a selective-late predicate is the
            // entire cost.
            if let Some(generation) = generation {
                let stopped_early = stop_processing.load(std::sync::atomic::Ordering::Acquire);
                if stopped_early {
                    // Only a prefix, and only meaningful if we know its length.
                    if limit.is_some() {
                        query_cache::store(table_name, where_query, generation, limit, pointers);
                    }
                } else {
                    // Ran to completion: a complete match set, which answers
                    // any limit by truncation.
                    query_cache::store(table_name, where_query, generation, None, pointers);
                }
            }
        }

        let total_reading_ns = reading_time_total.load(std::sync::atomic::Ordering::Relaxed);

        log::info!(
            "Total reading time for query '{}' : {:.2?} over {} rows",
            where_query,
            std::time::Duration::from_nanos(total_reading_ns),
            all_rows.len()
        );

        Ok(all_rows)
    }

    /// Bytes-mode variant of [`Self::process`]. Workers serialize matched rows
    /// directly into per-worker `Vec<u8>` buffers (one alloc per worker, not
    /// per row), and the result is the concatenation of those buffers in
    /// undefined order. This skips the `Vec<Row>` accumulation, the merged
    /// `Vec<Row>` allocation in `drain_rows`, and the post-process
    /// `rows_into_vec` pass entirely.
    ///
    /// Falls back to row-mode internally when ORDER BY is requested (rows
    /// must be sorted before serialization).
    ///
    /// The flag beside the bytes says whether any row may carry columns beyond
    /// `requested_row_fetch`. Only the path that built the rows can know that,
    /// so it is reported from here rather than guessed from the parse: the
    /// executor skips its decode-and-trim pass on `false`, and a `false` over
    /// rows holding a WHERE-only column sends that column to the client, which
    /// then drops the connection with "unexpected field count in D message".
    pub async fn process_into_bytes<'a, S: StorageIO>(
        &self,
        table_name: &str,
        where_query: &str,
        query_row_fetch: RowFetch,
        requested_row_fetch: RowFetch,
        table_schema: &[SchemaField],
        io_rows: Arc<S>,
        iterator: &mut RowPointerIterator<S>,
        limit: Option<u64>,
        order_by: Option<String>,
        // See [`Self::process`].
        generation: Option<u64>,
        visibility: Visibility,
    ) -> std::io::Result<(Vec<u8>, bool)> {
        // ORDER BY requires materialized Row structs to sort by column value;
        // fall back to the row-mode path and serialize at the end.
        if order_by.is_some() {
            let rows = self
                .process(
                    table_name,
                    where_query,
                    query_row_fetch,
                    requested_row_fetch,
                    table_schema,
                    io_rows,
                    iterator,
                    limit,
                    order_by,
                    generation,
                    visibility,
                )
                .await?;
            // Row mode decides for itself which fetch it emits, so nothing
            // here can promise the rows are only the requested columns.
            return Ok((crate::core::row::row::rows_into_vec(rows), true));
        }

        let prep_stopwatch = std::time::Instant::now();

        log::info!(
            "Starting concurrent bytes-mode processing for query: {}",
            where_query
        );

        // Cache fast-path (if enabled): serialize directly to wire bytes
        // without materializing a `Vec<Row>` or cloning rows.
        if let Some(bytes) = Self::try_collect_cached_bytes(
            table_name,
            where_query,
            generation,
            &requested_row_fetch,
            &io_rows,
            limit,
            &visibility,
        )
        .await
        {
            // The cached path fetches `requested_row_fetch` and nothing else.
            return Ok((bytes, false));
        }

        let (tx_b, rx_b) = kanal::unbounded_async::<Vec<u8>>();

        let cache_enabled = query_cache::enabled() && generation.is_some();

        let (tx_rp, rx_rp) = if cache_enabled {
            let (a, b) = kanal::unbounded_async::<Vec<RowPointer>>();
            (Some(a), Some(b))
        } else {
            (None, None)
        };

        let limit_counter: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());
        let stop_processing: Arc<CacheGuard<AtomicBool>> = Arc::new(AtomicBool::new(false).into());
        let eval_error: Arc<CacheGuard<AtomicU8>> = Arc::new(AtomicU8::new(0).into());
        let enforce_limit_early = limit.is_some();

        let shared_tuple: Arc<(RowFetch, RowFetch)> =
            Arc::new((query_row_fetch, requested_row_fetch));

        let schema_arc = Self::schema_arc(table_schema);

        let io_rows_outer = Arc::clone(&io_rows);

        let (no_filter, tokens_arc) =
            Self::prepare_query(where_query, &schema_arc).map_err(std::io::Error::other)?;
        let compiled_plan = if no_filter {
            None
        } else {
            Some(Arc::new(
                Self::compile_query_plan(&schema_arc, &tokens_arc)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?,
            ))
        };

        let query_covers_requested = {
            let q = &shared_tuple.0.columns_fetching_data;
            let r = &shared_tuple.1.columns_fetching_data;
            // The optimisation emits the WHERE buffer *as it is*, so the
            // requested columns must already sit at its front in the requested
            // order. Testing only for set inclusion was wrong: the WHERE
            // columns are gathered through a `HashSet`, so their order varies
            // run to run, and a covered query could return its columns
            // transposed — `SELECT a, b` answered with `b, a`.
            !no_filter
                && q.len() >= r.len()
                && r.iter()
                    .zip(q.iter())
                    .all(|(rc, qc)| qc.schema_id == rc.schema_id)
        };
        // The only emit that is not exactly `requested_row_fetch`: a covered
        // query sends the WHERE buffer as it is, requested columns first and
        // the predicate's own after them. The unfiltered path and the
        // re-fetch of a matched row both serialize the requested fetch.
        let overfetched = query_covers_requested
            && shared_tuple.0.columns_fetching_data.len()
                > shared_tuple.1.columns_fetching_data.len();

        log::info!(
            "Preperation for bytes-mode query completed in {:.2?}",
            prep_stopwatch.elapsed()
        );

        let reading_time_total: Arc<CacheGuard<AtomicU64>> = Arc::new(AtomicU64::new(0).into());

        let (batch_handles, pointer_read_error) = Self::spawn_batch_tasks(
            iterator,
            Arc::clone(&shared_tuple),
            Arc::clone(&schema_arc),
            compiled_plan.clone(),
            io_rows_outer.clone(),
            None,
            Some(tx_b.clone()),
            tx_rp.clone(),
            limit,
            enforce_limit_early,
            no_filter,
            query_covers_requested,
            Arc::clone(&limit_counter),
            Arc::clone(&stop_processing),
            Arc::clone(&reading_time_total),
            Arc::clone(&eval_error),
            visibility.clone(),
            crate::core::cancel::current(),
            // Bytes mode never sorts — `process_into_bytes` falls back to the
            // row-mode path when an ORDER BY is asked for — so there is no key
            // to keep a top-N by.
            None,
            // Bytes mode never serves an aggregate.
            None,
        )
        .await;

        let join_results = join_all(batch_handles).await;
        drop(tx_b);

        // After the join, so no worker outlives the statement it served.
        if let Some(e) = pointer_read_error {
            return Err(e.into());
        }
        Self::check_worker_outcome(join_results, &eval_error)?;

        // Drain bytes chunks; pick the largest as the base to avoid copying it.
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        if !rx_b.is_empty() {
            let _ = rx_b.drain_into(&mut chunks);
        }
        drop(rx_b);

        let bytes = if chunks.is_empty() {
            Vec::new()
        } else {
            let mut largest_idx = 0;
            let mut largest_len = 0usize;
            for (i, c) in chunks.iter().enumerate() {
                if c.len() > largest_len {
                    largest_len = c.len();
                    largest_idx = i;
                }
            }
            let mut base = chunks.swap_remove(largest_idx);
            let total: usize = base.len() + chunks.iter().map(|c| c.len()).sum::<usize>();
            base.reserve(total - base.len());
            for c in chunks {
                base.extend_from_slice(&c);
            }
            base
        };

        if let Some(rx_rp) = rx_rp {
            let pointers = Self::drain_row_pointers(rx_rp);

            // See `process`: a prefix is keyed by its limit, a complete set
            // by the bare clause.
            if let Some(generation) = generation {
                let stopped_early = stop_processing.load(std::sync::atomic::Ordering::Acquire);
                if stopped_early {
                    // Only a prefix, and only meaningful if we know its length.
                    if limit.is_some() {
                        query_cache::store(table_name, where_query, generation, limit, pointers);
                    }
                } else {
                    // Ran to completion: a complete match set, which answers
                    // any limit by truncation.
                    query_cache::store(table_name, where_query, generation, None, pointers);
                }
            }
        }

        let total_reading_ns = reading_time_total.load(std::sync::atomic::Ordering::Relaxed);
        log::info!(
            "Total reading time for query '{}' : {:.2?} ({} bytes)",
            where_query,
            std::time::Duration::from_nanos(total_reading_ns),
            bytes.len()
        );

        Ok((bytes, overfetched))
    }

    /// Concurrently evaluates a WHERE clause and returns matching row pointers (with their pointer-record offsets).
    ///
    /// This is intended for UPDATE/DELETE operations where the caller must mutate the pointer record
    /// (tombstone or update-in-place/append).
    pub async fn process_row_pointers<S: StorageIO>(
        &self,
        table_name: &str,
        where_query: &str,
        query_row_fetch: RowFetch,
        table_schema: &SmallVec<[SchemaField; 20]>,
        io_rows: Arc<S>,
        iterator: &mut RowPointerIterator<S>,
        limit: Option<u64>,
        // Which row versions this statement may see. `Visibility::All` is the
        // unversioned rule and admits every untombstoned row.
        visibility: Visibility,
    ) -> std::io::Result<Vec<RowPointerWithOffset>> {
        let prep_stopwatch = std::time::Instant::now();

        log::info!(
            "Starting concurrent pointer processing for query: {}",
            where_query
        );

        let (tx, rx) = kanal::unbounded_async::<RowPointerWithOffset>();

        let limit_counter = Arc::new(AtomicU64::new(0));
        let stop_processing = Arc::new(AtomicBool::new(false));
        let eval_error: Arc<CacheGuard<AtomicU8>> = Arc::new(AtomicU8::new(0).into());
        // A WHERE clause the planner refuses. Reported through the result
        // rather than by panicking: the panic unwound the task running the
        // statement and the connection with it, where the SELECT path returns
        // the reason — see `compile_query_plan`.
        let plan_error: Arc<parking_lot::Mutex<Option<String>>> =
            Arc::new(parking_lot::Mutex::new(None));

        let shared_tuple: Arc<RowFetch> = Arc::new(query_row_fetch);
        let sem: StdArc<Semaphore> = StdArc::new(Semaphore::new(crate::max_permits_threads()));

        let table_schema = table_schema
            .iter()
            .cloned()
            .collect::<SmallVec<[SchemaField; 20]>>();

        let io_rows_outer = Arc::clone(&io_rows);

        // Empty WHERE means "match all rows"; skip tokenization/parsing entirely.
        let no_filter = where_query.trim().is_empty();
        let tokens: SmallVec<[Token; 36]> = if no_filter {
            SmallVec::new()
        } else {
            crate::core::tokenizer::query_tokenizer::try_tokenize(&where_query, &table_schema)
                .map_err(std::io::Error::other)?
        };

        let schema_arc = Arc::new(table_schema);
        let tokens_arc = Arc::new(tokens);

        log::info!(
            "Preperation for pointer query completed in {:.2?}",
            prep_stopwatch.elapsed()
        );

        let reading_time_total = Arc::new(AtomicU64::new(0));

        let mut batch_handles = Vec::new();
        iterator.reset();

        // Set when a pointer read fails, and returned after the workers are
        // joined so the scan reports the failure instead of a short answer.
        let mut pointer_read_error: Option<crate::core::row::error::RowError> = None;

        loop {
            if stop_processing.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            let mut pointers: Vec<RowPointerWithOffset> = Vec::with_capacity(crate::batch_size());

            for _ in 0..crate::batch_size() {
                match iterator.next_row_pointer_with_offset().await {
                    Ok(Some((pointer_record_pos, pointer))) => {
                        pointers.push(RowPointerWithOffset {
                            pointer_record_pos,
                            pointer,
                        });
                    }
                    Ok(None) => break,
                    // As in the row-mode dispatcher: a read failure must not
                    // be mistaken for the end of the table.
                    Err(e) => {
                        pointer_read_error = Some(e);
                        stop_processing.store(true, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                }
            }

            if pointers.is_empty() {
                break;
            }

            // As in the row-mode dispatcher above: the candidate rows this
            // statement read, counted once per batch in the task running it.
            #[cfg(feature = "query_stats")]
            crate::core::query_stats::note_rows_scanned(pointers.len() as u64);

            // As in the row-mode dispatcher above: the error is recorded before
            // the stop flag, so a cancelled scan fails rather than returning a
            // prefix of its rows as a complete answer.
            if let Some(reason) = crate::core::cancel::tripped() {
                let _ = eval_error.compare_exchange(
                    0,
                    reason.eval_error().to_code(),
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                );
                stop_processing.store(true, std::sync::atomic::Ordering::Relaxed);
                break;
            }

            if stop_processing.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            // Acquire batch permit BEFORE spawn for natural backpressure.
            let batch_permit = StdArc::clone(&sem).acquire_owned().await.unwrap();

            let tuple_clone = Arc::clone(&shared_tuple);
            let tx_clone = tx.clone();
            let schema_ref = Arc::clone(&schema_arc);
            let tokens_ref = Arc::clone(&tokens_arc);
            let io_rows_batch = Arc::clone(&io_rows_outer);

            let limit_opt = limit;
            let limit_counter_ref = Arc::clone(&limit_counter);
            let stop_ref = Arc::clone(&stop_processing);
            let no_filter = no_filter;
            let reading_time_total_clone = Arc::clone(&reading_time_total);
            let eval_error_ref = Arc::clone(&eval_error);
            let plan_error_ref = Arc::clone(&plan_error);
            // Cheap: the rule is an enum holding at most two `Arc`s, and each
            // batch needs its own so the task can own it.
            let visibility_ref = visibility.clone();

            let batch_handle = task::spawn(async move {
                // Owned permit acquired in the outer loop; held until task completion.
                let _batch_permit = batch_permit;

                // Fast-path for empty WHERE: accept all non-deleted pointers.
                if no_filter {
                    for pwo in pointers.into_iter() {
                        if stop_ref.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }

                        // The visibility test runs *before* any fetch, and
                        // that ordering is load-bearing rather than
                        // stylistic: reclaimed space is reused, so
                        // fetch-then-check would read another row's bytes
                        // and only then discard them — one refactor away
                        // from returning them. See
                        // `mvcc::visibility::reclamation_lemma`.
                        if !visibility_ref.visible(
                            pwo.pointer.deleted,
                            pwo.pointer.xmin,
                            pwo.pointer.xmax,
                        ) {
                            continue;
                        }

                        if let Some(limit_val) = limit_opt {
                            // Stop once we have emitted limit rows.
                            if limit_counter_ref.load(std::sync::atomic::Ordering::Relaxed)
                                >= limit_val
                            {
                                stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                break;
                            }

                            let reserved = limit_counter_ref.fetch_update(
                                std::sync::atomic::Ordering::Relaxed,
                                std::sync::atomic::Ordering::Relaxed,
                                // `<`, not `<=`: the counter is the number of rows already
                                // claimed, so admitting `cur == limit` claimed one too many
                                // and `DELETE … LIMIT 100` removed 101.
                                |cur| if cur < limit_val { Some(cur + 1) } else { None },
                            );

                            if reserved.is_err() {
                                stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                break;
                            }
                        }

                        if tx_clone.send(pwo).await.is_err() {
                            break;
                        }
                    }

                    return;
                }

                // Local row buffer reused across all pointers in this batch
                let row = Row::default();
                let io_rows_ref: &S = &*io_rows_batch;
                let batch_read_start = std::time::Instant::now();

                let mut buffer = Buffer {
                    hashtable_buffer: UnsafeCell::new(SmallVec::new()),
                    row,
                    transformers: SmallVec::new(),
                    intermediate_results: SmallVec::new(),
                    bool_buffer: SmallVec::new(),
                    write_order_to_col: SmallVec::new(),
                };

                // Build a processor and parser once per batch
                let processor_cell = UnsafeCell::new(TransformerProcessor::new(
                    &mut buffer.transformers,
                    &mut buffer.intermediate_results,
                ));
                let processor_mut = unsafe { &mut *processor_cell.get() };
                let parser_cell = UnsafeCell::new(
                    QueryParser::new(&tokens_ref, processor_mut).with_scales(schema_ref.as_slice()),
                );
                let mut plan_built = false;

                for pwo in pointers.into_iter() {
                    if stop_ref.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }

                    // The visibility test runs *before* any fetch, and
                    // that ordering is load-bearing rather than
                    // stylistic: reclaimed space is reused, so
                    // fetch-then-check would read another row's bytes
                    // and only then discard them — one refactor away
                    // from returning them. See
                    // `mvcc::visibility::reclamation_lemma`.
                    if !visibility_ref.visible(
                        pwo.pointer.deleted,
                        pwo.pointer.xmin,
                        pwo.pointer.xmax,
                    ) {
                        continue;
                    }

                    let query_row_fetch = &*tuple_clone;

                    if pwo
                        .pointer
                        .fetch_row_reuse_async(io_rows_ref, query_row_fetch, &mut buffer.row)
                        .await
                        .is_err()
                    {
                        continue;
                    }

                    if !plan_built {
                        let tmp_vec = unsafe { &mut *buffer.hashtable_buffer.get() };
                        tmp_vec.clear();

                        {
                            let columns = &buffer.row.columns;
                            let schema = &*schema_ref;

                            tmp_vec.clear();
                            tmp_vec.resize(
                                schema.len(),
                                (Cow::Borrowed(EMPTY_STR), MemoryBlock::default()),
                            );

                            for column in columns.iter() {
                                // Here `schema_id` is write_order.
                                let write_order = column.schema_id as usize;
                                if let Some(schema_field) = schema
                                    .iter()
                                    .find(|f| f.write_order as usize == write_order)
                                {
                                    if let Some(entry) = tmp_vec.get_mut(write_order) {
                                        *entry = (
                                            Cow::Borrowed(schema_field.name.as_str()),
                                            column.data.clone(),
                                        );
                                    }
                                }
                            }
                        }

                        let parser_mut = unsafe { &mut *parser_cell.get() };
                        if let Err(e) = parser_mut.execute(tmp_vec) {
                            plan_error_ref.lock().get_or_insert(e);
                            stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                            return;
                        }

                        unsafe { &mut *processor_cell.get() }.replace_row_inputs(&[]);
                        build_write_order_map(
                            &mut buffer.write_order_to_col,
                            &buffer.row,
                            schema_ref.as_slice(),
                        );
                        plan_built = true;
                    }

                    let result = {
                        let processor = unsafe { &mut *processor_cell.get() };
                        processor.execute_row(
                            &RowView::new(&buffer.row, &buffer.write_order_to_col),
                            &mut buffer.bool_buffer,
                        )
                    };

                    let result = match result {
                        Ok(matched) => matched,
                        Err(e) => {
                            let _ = eval_error_ref.compare_exchange(
                                0,
                                e.to_code(),
                                std::sync::atomic::Ordering::AcqRel,
                                std::sync::atomic::Ordering::Acquire,
                            );
                            stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                            break;
                        }
                    };

                    if result {
                        if let Some(limit_val) = limit_opt {
                            if limit_counter_ref.load(std::sync::atomic::Ordering::Relaxed)
                                >= limit_val
                            {
                                stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                break;
                            }

                            let reserved = limit_counter_ref.fetch_update(
                                std::sync::atomic::Ordering::Relaxed,
                                std::sync::atomic::Ordering::Relaxed,
                                // `<`, not `<=`: the counter is the number of rows already
                                // claimed, so admitting `cur == limit` claimed one too many
                                // and `DELETE … LIMIT 100` removed 101.
                                |cur| if cur < limit_val { Some(cur + 1) } else { None },
                            );

                            if reserved.is_err() {
                                stop_ref.store(true, std::sync::atomic::Ordering::Relaxed);
                                break;
                            }
                        }

                        if tx_clone.send(pwo).await.is_err() {
                            break;
                        }
                    }
                }

                let elapsed_ns = batch_read_start.elapsed().as_nanos() as u64;
                reading_time_total_clone
                    .fetch_add(elapsed_ns, std::sync::atomic::Ordering::Relaxed);
            });

            batch_handles.push(batch_handle);
        }

        let join_results = join_all(batch_handles).await;
        drop(tx);

        Self::check_worker_outcome(join_results, &eval_error)?;
        // A pointer read that failed mid-scan is reported rather than being
        // left to look like the end of the table.
        if let Some(e) = pointer_read_error {
            return Err(std::io::Error::other(e.to_string()));
        }
        if let Some(reason) = plan_error.lock().take() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("could not compile the WHERE clause: {}", reason),
            ));
        }

        let mut matches = Vec::with_capacity(50);
        if !rx.is_empty() {
            rx.drain_into(&mut matches).unwrap();
        }
        drop(rx);

        let total_reading_ns = reading_time_total.load(std::sync::atomic::Ordering::Relaxed);
        log::info!(
            "Total reading time for pointer query '{}' : {:.2?} over {} pointers",
            where_query,
            std::time::Duration::from_nanos(total_reading_ns),
            matches.len()
        );

        // Keep SME referenced to avoid unused warnings on some feature combos.
        let _ = table_name;

        Ok(matches)
    }
}

#[cfg(test)]
fn compare_rows_by_schema_id(
    a: &Row,
    b: &Row,
    schema_id: usize,
    schema: &[SchemaField],
) -> std::cmp::Ordering {
    let a_col = a.columns.iter().find(|c| c.schema_id as usize == schema_id);
    let b_col = b.columns.iter().find(|c| c.schema_id as usize == schema_id);

    match (a_col, b_col) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(ac), Some(bc)) => {
            // Prefer the schema's declared type (more stable than per-column), fallback to column type.
            // schema_id here is SchemaField.write_order, not necessarily the schema slice index.
            let db_type = schema
                .iter()
                .find(|f| f.write_order as usize == schema_id)
                .map(|f| f.db_type.clone())
                .unwrap_or_else(|| ac.column_type.clone());
            compare_bytes_as_type(db_type, ac.data.into_slice(), bc.data.into_slice())
        }
    }
}

pub(crate) fn compare_bytes_as_type(
    db_type: crate::core::db_type::DbType,
    a: &[u8],
    b: &[u8],
) -> std::cmp::Ordering {
    use crate::core::db_type::DbType;

    match db_type {
        // Variable-width values order by their bytes. For an array that is not
        // a meaningful order, but it is stable, which is what a sort needs.
        // `JSON` is reporting-only and never reaches storage, but a match has
        // to be exhaustive and a byte compare is the safe answer.
        DbType::STRING
        | DbType::BYTEA
        | DbType::ARRAY
        | DbType::UNKNOWN
        | DbType::START
        | DbType::END
        | DbType::JSON => a.cmp(b),
        // PostgreSQL's `jsonb` order, by value. The encoded bytes used to be
        // compared instead: stable, but `{"a":1}` and `{"a":1.0}` were two
        // values to every sort, window and comparison, `ORDER BY doc` was not
        // PostgreSQL's order, and `doc < '...'` answered a question about the
        // encoding. Bytes that are not a document sort after every document,
        // by their bytes, so the order stays total for a sort.
        DbType::JSONB => crate::core::jsonb::order(a, b),
        // Fixed-width and big-endian by construction, so a plain byte compare
        // already yields canonical UUID order and address order.
        DbType::UUID | DbType::INET => a.cmp(b),
        DbType::CHAR => a.cmp(b),
        // Microseconds since the Unix epoch, stored little-endian. Comparing the
        // raw bytes would order by the low byte first, and would sort negative
        // (pre-1970) instants above positive ones.
        DbType::DATETIME => read_num::<8, i64>(a, b, i64::from_le_bytes),
        // Fixed-scale unscaled units, so the integer order is the numeric order.
        DbType::DECIMAL => read_num::<16, i128>(a, b, i128::from_le_bytes),
        DbType::I8 => read_num::<1, i8>(a, b, i8::from_le_bytes),
        DbType::I16 => read_num::<2, i16>(a, b, i16::from_le_bytes),
        DbType::I32 => read_num::<4, i32>(a, b, i32::from_le_bytes),
        DbType::I64 => read_num::<8, i64>(a, b, i64::from_le_bytes),
        DbType::I128 => read_num::<16, i128>(a, b, i128::from_le_bytes),
        DbType::U8 => read_num::<1, u8>(a, b, u8::from_le_bytes),
        DbType::U16 => read_num::<2, u16>(a, b, u16::from_le_bytes),
        DbType::U32 => read_num::<4, u32>(a, b, u32::from_le_bytes),
        DbType::U64 => read_num::<8, u64>(a, b, u64::from_le_bytes),
        DbType::U128 => read_num::<16, u128>(a, b, u128::from_le_bytes),
        DbType::BOOL => read_num::<1, u8>(a, b, u8::from_le_bytes),
        DbType::F32 => {
            let av = read_float::<4, f32>(a, f32::from_le_bytes);
            let bv = read_float::<4, f32>(b, f32::from_le_bytes);
            pg_float_cmp(av as f64, bv as f64)
        }
        DbType::F64 => {
            let av = read_float::<8, f64>(a, f64::from_le_bytes);
            let bv = read_float::<8, f64>(b, f64::from_le_bytes);
            pg_float_cmp(av, bv)
        }
        DbType::NULL => std::cmp::Ordering::Equal,

        // The types stored in PostgreSQL's own wire layout. Their bytes are
        // big-endian, which memcmp orders correctly for the unsigned parts but
        // not across a sign, so each one that has a meaningful order decodes
        // rather than compares raw.
        DbType::INTERVAL => {
            let key = |b: &[u8]| {
                crate::core::pg_types::decode_interval(b)
                    .map(crate::core::pg_types::interval_sort_key)
                    .unwrap_or(i128::MIN)
            };
            key(a).cmp(&key(b))
        }
        // Fixed-width octet strings with no sign: byte order is address order.
        DbType::MACADDR | DbType::MACADDR8 => a.cmp(b),
        DbType::DATE => read_num::<4, i32>(a, b, i32::from_be_bytes),
        DbType::TIME => read_num::<8, i64>(a, b, i64::from_be_bytes),
        // PostgreSQL orders `timetz` by the UTC-normalised time, then by the
        // offset, so two spellings of the same instant sort together and still
        // compare distinct.
        DbType::TIMETZ => {
            let key = |x: &[u8]| crate::core::pg_types::timetz_sort_key(x).unwrap_or((i64::MIN, 0));
            key(a).cmp(&key(b))
        }
        // Geometric values have no ordering PostgreSQL defines either, so a
        // byte compare is used for the same reason it is used for an array: it
        // is stable, which is all a sort or a join key needs.
        // A bit string orders by its bytes, which is its bit order: the length
        // prefix is big-endian and the bits are packed most-significant-first,
        // so the byte compare is the lexicographic bit compare PostgreSQL
        // defines — for two strings of equal length. Different lengths compare
        // by length first here, where PostgreSQL compares the bits first.
        DbType::BIT | DbType::VARBIT => a.cmp(b),
        DbType::POINT
        | DbType::LINE
        | DbType::LSEG
        | DbType::BOX
        | DbType::PATH
        | DbType::POLYGON
        | DbType::CIRCLE => a.cmp(b),
        // A range orders by its lower bound then its upper, which is what
        // PostgreSQL does. An undecodable or empty range sorts first.
        DbType::INT4RANGE
        | DbType::INT8RANGE
        | DbType::NUMRANGE
        | DbType::TSRANGE
        | DbType::TSTZRANGE
        | DbType::DATERANGE => {
            let key = |x: &[u8]| {
                crate::core::pg_types::range_bounds_key(&db_type, x)
                    .map(|(lo, hi, _, _)| (lo, hi))
                    .unwrap_or((i128::MIN, i128::MIN))
            };
            key(a).cmp(&key(b))
        }
    }
}

/// PostgreSQL's total order on floating point: `NaN` equals `NaN` and is
/// greater than every other value, infinities included; `-0` equals `+0`.
///
/// `partial_cmp` has no answer for `NaN`, and the `Equal` this used to fall
/// back to made `NaN` equal to *every* value — not an order at all, since
/// `1 = NaN = 2` while `1 < 2`. A sort given that may return any arrangement,
/// a selection and a sort over the same rows can disagree about which rows
/// are the smallest `k`, and the standard library's sort may panic when it
/// detects the inconsistency. `pg_float_order` pins the semantics.
pub(crate) fn pg_float_cmp(a: f64, b: f64) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        // Neither is NaN, so this is total, and it treats -0 and +0 as equal.
        (false, false) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
    }
}

fn read_num<const N: usize, T: Ord + Copy>(
    a: &[u8],
    b: &[u8],
    f: fn([u8; N]) -> T,
) -> std::cmp::Ordering {
    let aa = a.get(0..N);
    let bb = b.get(0..N);
    if let (Some(aa), Some(bb)) = (aa, bb) {
        let mut arr_a = [0u8; N];
        let mut arr_b = [0u8; N];
        arr_a.copy_from_slice(aa);
        arr_b.copy_from_slice(bb);
        let av = f(arr_a);
        let bv = f(arr_b);
        av.cmp(&bv)
    } else {
        a.cmp(b)
    }
}

fn read_float<const N: usize, T: Copy>(a: &[u8], f: fn([u8; N]) -> T) -> T {
    if let Some(aa) = a.get(0..N) {
        let mut arr_a = [0u8; N];
        arr_a.copy_from_slice(aa);
        f(arr_a)
    } else {
        // Fallback: treat missing/invalid as 0
        f([0u8; N])
    }
}

pub struct Buffer<'a> {
    pub hashtable_buffer: UnsafeCell<SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>>,
    pub row: Row,
    pub transformers: SmallVec<[ColumnTransformer; 36]>,
    pub intermediate_results: SmallVec<[MemoryBlock; 20]>,
    pub bool_buffer: SmallVec<[(bool, Option<Next>); 20]>,
    /// Precomputed map: write_order -> position in `row.columns`.
    /// `u8::MAX` denotes an absent column. Computed once per batch.
    pub write_order_to_col: SmallVec<[u8; 32]>,
}

// A zero-copy view over Row's MemoryBlocks for execution, indexed by schema write_order.
// This maps logical indices (write_order) used by the QueryParser/Transformers to the
// actual MemoryBlocks present in the fetched row (which may be a subset of columns).
pub(crate) struct RowView<'a> {
    row: &'a Row,
    /// Map from write_order -> position in `row.columns` (or `u8::MAX` if absent).
    /// Built once per batch, not per row.
    idx_map: &'a [u8],
}

impl<'a> RowView<'a> {
    #[inline]
    pub(crate) fn new(row: &'a Row, idx_map: &'a [u8]) -> Self {
        Self { row, idx_map }
    }
}

/// What a `RowView` yields for a column the fetch did not include.
///
/// Empty is already this engine's stand-in for NULL — `check_unique` skips an
/// empty value on exactly that basis — so an absent column reads as absent
/// rather than as another column's bytes.
fn absent_column() -> &'static MemoryBlock {
    static ABSENT: std::sync::OnceLock<MemoryBlock> = std::sync::OnceLock::new();
    ABSENT.get_or_init(MemoryBlock::default)
}

impl<'a> RowBlocks for RowView<'a> {
    #[inline]
    fn mb_at(&self, idx: usize) -> &MemoryBlock {
        // Both of these were `get_unchecked`, and both were reachable.
        //
        // `idx` is a write_order and `idx_map` was sized by the *field count*,
        // so a write_order past that count read out of bounds — the same
        // mismatch that panicked a pgwire worker, except unchecked here, so
        // undefined behaviour rather than a crash.
        //
        // `u8::MAX` is the "column not in this fetch" sentinel, and the only
        // thing between it and `columns.get_unchecked(255)` was a
        // `debug_assert!` — compiled out of the release build we ship. Any
        // transformer asking for a column the fetch did not include hit that,
        // needing neither a projection nor a gap in the write orders. Whoever
        // wrote the assert knew the case was possible.
        //
        // Two bounds checks against an O(1) map lookup is not a cost worth
        // undefined behaviour.
        let pos = self.idx_map.get(idx).copied().unwrap_or(u8::MAX) as usize;
        if pos == u8::MAX as usize {
            return absent_column();
        }
        match self.row.columns.get(pos) {
            Some(column) => &column.data,
            None => absent_column(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.idx_map.len()
    }
}

/// Build / refresh the `write_order -> row column position` map for the columns currently
/// loaded in `row`. Cheap: O(N) where N is the number of fetched columns (typically 1-8).
#[inline]
pub(crate) fn build_write_order_map(
    map: &mut SmallVec<[u8; 32]>,
    row: &Row,
    schema: &[SchemaField],
) {
    map.clear();
    // Sized by the highest write_order rather than the field count, because that
    // is what indexes it. `RowBlocks::len` is the only thing this changes and it
    // has no non-test consumer, so nothing observes the wider map.
    let needed = schema
        .iter()
        .map(|f| f.write_order as usize + 1)
        .max()
        .unwrap_or(0)
        .max(schema.len());
    map.resize(needed, u8::MAX);
    for (pos, col) in row.columns.iter().enumerate() {
        // `schema_id` *is* the write order — `calculate_schema_offset` returns
        // `field.write_order` — so the old `schema[col.schema_id].write_order`
        // was a round-trip that only returned its own input, and only while the
        // two coincided. Where they don't, it indexed out of bounds: the same
        // mistake in pgwire panicked a worker with "len is 22 but the index is
        // 22" on a projected field list.
        let wo = col.schema_id as usize;
        // `pos` is stored as a `u8` and `u8::MAX` is the absent sentinel, so a
        // row with 255 or more fetched columns cannot be represented. Leaving
        // the sentinel makes such a column read as NULL, which is wrong but
        // consistent and detectable; truncating would silently return a
        // *different* column's bytes. Widening the map to `u16` is the real fix
        // if a table ever approaches that width — the widest in the live
        // deployment is 64 fields including `__null_` companions.
        if wo < map.len() && pos < u8::MAX as usize {
            map[wo] = pos as u8;
        }
    }
}

#[cfg(test)]
mod order_limit_tests {
    use super::*;
    use crate::core::db_type::DbType;
    use crate::core::row::row::{Column, Row};
    use crate::memory_pool::MEMORY_POOL;

    fn row(v: i64) -> Row {
        let bytes = v.to_le_bytes();
        let mut mb = MEMORY_POOL.acquire(bytes.len());
        mb.into_slice_mut().copy_from_slice(&bytes);
        let mut columns = SmallVec::new();
        columns.push(Column {
            schema_id: 0,
            data: mb,
            column_type: DbType::I64,
        });
        Row {
            position: v as u64,
            columns,
            length: 8 as _,
        }
    }

    fn schema() -> Arc<SmallVec<[SchemaField; 20]>> {
        let mut f = SmallVec::new();
        f.push(SchemaField::new(
            "v".to_string(),
            DbType::I64,
            8,
            0,
            0,
            false,
        ));
        Arc::new(f)
    }

    fn values(rows: &[Row]) -> Vec<i64> {
        rows.iter()
            .map(|r| {
                let s = r.columns[0].data.into_slice();
                i64::from_le_bytes(s[..8].try_into().unwrap())
            })
            .collect()
    }

    /// A deterministic shuffle: distinct values, so no tie can make the
    /// unstable selection and the stable sort legitimately disagree.
    fn scrambled(n: i64) -> Vec<Row> {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut vs: Vec<i64> = (0..n).collect();
        for i in (1..vs.len()).rev() {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let j = ((state >> 33) as usize) % (i + 1);
            vs.swap(i, j);
        }
        vs.into_iter().map(row).collect()
    }

    /// `ORDER BY … LIMIT k` must return exactly the rows a full sort followed by
    /// a truncate would, which is the whole licence for doing the cheaper thing.
    #[test]
    fn top_k_selection_agrees_with_sort_then_truncate() {
        let sc = schema();
        for k in [0usize, 1, 2, 7, 199, 200, 201, 999, 1000, 1001] {
            let mut fast = scrambled(1000);
            Self_sort(&mut fast, &sc, Some(k as u64));

            let mut reference = scrambled(1000);
            Self_sort(&mut reference, &sc, None);
            reference.truncate(k);

            assert_eq!(values(&fast), values(&reference), "k={}", k);
        }
    }

    /// Descending must select the *largest* k, not the smallest reversed.
    #[test]
    fn descending_selects_from_the_other_end() {
        let sc = schema();
        let mut rows = scrambled(500);
        ConcurrentProcessor::sort_rows_if_needed(
            &mut rows,
            &sc,
            &Some("v DESC".to_string()),
            Some(3),
        );
        assert_eq!(values(&rows), vec![499, 498, 497]);
    }

    /// Without an `ORDER BY` the cap still has to be applied — it used to be a
    /// separate `truncate` at the call site.
    #[test]
    fn a_limit_without_an_order_by_still_caps() {
        let sc = schema();
        let mut rows = scrambled(50);
        ConcurrentProcessor::sort_rows_if_needed(&mut rows, &sc, &None, Some(5));
        assert_eq!(rows.len(), 5);
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % bound
        }
    }

    fn col(schema_id: u64, column_type: DbType, bytes: &[u8]) -> Column {
        let mut mb = MEMORY_POOL.acquire(bytes.len());
        mb.into_slice_mut().copy_from_slice(bytes);
        Column {
            schema_id: crate::core::row::row::schema_id_of(schema_id),
            data: mb,
            column_type,
        }
    }

    /// id I64, name STRING, score F64, n I32 (nullable), __null_n, ts DATETIME.
    fn wide_schema() -> Arc<SmallVec<[SchemaField; 20]>> {
        let mut f = SmallVec::new();
        for (wo, (name, ty, size)) in [
            ("id", DbType::I64, 8),
            ("name", DbType::STRING, 0),
            ("score", DbType::F64, 8),
            ("n", DbType::I32, 4),
            ("__null_n", DbType::U8, 1),
            ("ts", DbType::DATETIME, 8),
        ]
        .into_iter()
        .enumerate()
        {
            f.push(SchemaField::new(name.to_string(), ty, size, 0, wo as u64, false));
        }
        Arc::new(f)
    }

    /// A row of `wide_schema`, with few distinct values per column so ties are
    /// common and later keys get exercised.
    fn wide_row(rng: &mut Rng, position: u64) -> Row {
        let names = ["", "a", "ab", "b", "Zed", "é", "abc", "aa"];
        let null = rng.next(4) == 0;
        let n: i32 = if null { 0 } else { rng.next(7) as i32 - 3 };
        let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
        columns.push(col(0, DbType::I64, &(rng.next(5) as i64 - 2).to_le_bytes()));
        columns.push(col(
            1,
            DbType::STRING,
            names[rng.next(names.len() as u64) as usize].as_bytes(),
        ));
        let scores = [-1.5f64, 0.0, 2.25, f64::NAN, -0.0];
        columns.push(col(2, DbType::F64, &scores[rng.next(5) as usize].to_le_bytes()));
        columns.push(col(3, DbType::I32, &n.to_le_bytes()));
        columns.push(col(4, DbType::U8, &[null as u8]));
        columns.push(col(5, DbType::DATETIME, &(rng.next(4) as i64 * -1_000_003).to_le_bytes()));
        Row {
            position,
            columns,
            length: 0 as _,
        }
    }

    /// Rows that do not share the sample's layout: shuffled, a column missing,
    /// a column the schema does not name (schema_id 9, typed by the column).
    fn perturb(rng: &mut Rng, row: &mut Row) {
        match rng.next(6) {
            0 => {
                let len = row.columns.len();
                for i in (1..len).rev() {
                    let j = rng.next(i as u64 + 1) as usize;
                    row.columns.swap(i, j);
                }
            }
            1 => {
                let at = rng.next(row.columns.len() as u64) as usize;
                row.columns.remove(at);
            }
            2 => row
                .columns
                .push(col(9, DbType::I32, &(rng.next(3) as i32 - 1).to_le_bytes())),
            _ => {}
        }
    }

    fn random_keys(rng: &mut Rng) -> Vec<SortKey> {
        (0..rng.next(3) + 1)
            .map(|_| {
                let schema_id = [0usize, 1, 2, 3, 5, 9][rng.next(6) as usize];
                SortKey {
                    schema_id,
                    descending: rng.next(2) == 0,
                    null_flag: (schema_id == 3 && rng.next(4) != 0).then_some(4),
                }
            })
            .collect()
    }

    /// The prepared comparator against per-call resolution, over rows whose
    /// layouts differ from the sample's, keys the schema does not name,
    /// NULLs, NaN, negative zero and empty strings.
    #[test]
    fn prepared_sort_agrees_with_per_call_resolution() {
        let sc = wide_schema();
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        for round in 0..400 {
            let keys = random_keys(&mut rng);
            let mut rows: Vec<Row> = (0..60).map(|i| wide_row(&mut rng, i)).collect();
            if round % 2 == 0 {
                for row in rows.iter_mut() {
                    perturb(&mut rng, row);
                }
            }
            let sample = &rows[rng.next(rows.len() as u64) as usize];
            let sort = PreparedSort::new(&keys, sc.as_slice(), Some(sample));
            for a in &rows {
                for b in &rows {
                    assert_eq!(
                        sort.compare(a, b),
                        ConcurrentProcessor::compare_by_keys(a, b, &keys, sc.as_slice()),
                        "round {round}"
                    );
                }
            }
            // No sample at all: every lookup falls back to the search.
            let blind = PreparedSort::new(&keys, sc.as_slice(), None);
            assert_eq!(
                blind.compare(&rows[0], &rows[1]),
                ConcurrentProcessor::compare_by_keys(&rows[0], &rows[1], &keys, sc.as_slice())
            );
        }
    }

    /// The sort and the capped selection through `sort_rows_if_needed` give the
    /// same rows, in the same order, as the per-call comparator did — stable
    /// sort, so even tied rows keep their positions.
    #[test]
    fn sort_rows_matches_the_per_call_sort() {
        let sc = wide_schema();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for spec in ["name", "n DESC, name", "ts, id DESC, score", "score DESC", "n, id"] {
            let rows: Vec<Row> = (0..3000).map(|i| wide_row(&mut rng, i)).collect();
            let keys = ConcurrentProcessor::resolve_sort_keys(&sc, spec);
            let mut expect: Vec<Row> = rows.iter().map(Row::clone_row).collect();
            expect.sort_by(|a, b| ConcurrentProcessor::compare_by_keys(a, b, &keys, sc.as_slice()));
            let mut got = rows;
            ConcurrentProcessor::sort_rows_if_needed(&mut got, &sc, &Some(spec.to_string()), None);
            let pos = |v: &[Row]| v.iter().map(|r| r.position).collect::<Vec<_>>();
            assert_eq!(pos(&got), pos(&expect), "{spec}");

            // Capped: tie order is unspecified there, so compare the key
            // sequence rather than the positions. `score` carries NaN and -0,
            // which only works because `pg_float_cmp` makes that a total order.
            let mut capped: Vec<Row> = expect.iter().map(Row::clone_row).collect();
            capped.reverse();
            ConcurrentProcessor::sort_rows_if_needed(
                &mut capped,
                &sc,
                &Some(spec.to_string()),
                Some(200),
            );
            assert_eq!(capped.len(), 200);
            for (a, b) in capped.iter().zip(expect.iter()) {
                assert_eq!(
                    ConcurrentProcessor::compare_by_keys(a, b, &keys, sc.as_slice()),
                    std::cmp::Ordering::Equal,
                    "{spec}"
                );
            }
        }
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// order_by_bench --nocapture`. 1M rows, 1-3 keys, uncapped and LIMIT 200.
    #[test]
    #[ignore]
    fn order_by_bench() {
        let sc = wide_schema();
        let make = || {
            let mut rng = Rng(0xDEAD_BEEF_CAFE_F00D);
            (0..1_000_000u64)
                .map(|i| {
                    let mut row = wide_row(&mut rng, i);
                    // More distinct names than the differential test uses.
                    let name = format!("customer-{:06}", rng.next(400_000));
                    row.columns[1] = col(1, DbType::STRING, name.as_bytes());
                    row
                })
                .collect::<Vec<Row>>()
        };
        for round in 0..2 {
            for spec in ["name", "n DESC, name", "ts, id DESC, score"] {
                for limit in [None, Some(200u64)] {
                    let keys = ConcurrentProcessor::resolve_sort_keys(&sc, spec);
                    let mut rows = make();
                    let t = std::time::Instant::now();
                    let compare = |a: &Row, b: &Row| {
                        ConcurrentProcessor::compare_by_keys(a, b, &keys, sc.as_slice())
                    };
                    match limit {
                        Some(k) => {
                            rows.select_nth_unstable_by(k as usize - 1, compare);
                            rows.truncate(k as usize);
                            rows.sort_by(compare);
                        }
                        None => rows.sort_by(compare),
                    }
                    let old = t.elapsed();
                    drop(rows);
                    {
                        // Old comparator, new row movement: separates the two
                        // halves of the change.
                        let mut rows = make();
                        let t = std::time::Instant::now();
                        match limit {
                            Some(k) => select_rows_indirect(&mut rows, k as usize, compare),
                            None => sort_rows_indirect(&mut rows, compare),
                        }
                        println!("    per-call comparator, indirect {:>9.2?}", t.elapsed());
                    }
                    let mut rows = make();
                    let t = std::time::Instant::now();
                    ConcurrentProcessor::sort_rows_if_needed(
                        &mut rows,
                        &sc,
                        &Some(spec.to_string()),
                        limit,
                    );
                    let new = t.elapsed();
                    println!(
                        "round {round} {spec:<20} limit {limit:?}: old {old:>9.2?}  new {new:>9.2?}"
                    );
                }
            }
        }
    }

    /// PostgreSQL's float order: -inf < … < -0 = +0 < … < +inf < NaN = NaN,
    /// for both widths, and the comparator is a total order over all of them.
    #[test]
    fn pg_float_order() {
        use std::cmp::Ordering::*;
        let ascending = [
            f64::NEG_INFINITY,
            -2.5,
            -f64::MIN_POSITIVE,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            f64::MAX,
            f64::INFINITY,
            f64::NAN,
            -f64::NAN,
        ];
        // Rank: -0/+0 share one, every NaN shares the last.
        let rank = |v: f64| -> usize {
            if v.is_nan() {
                99
            } else if v == 0.0 {
                3
            } else {
                ascending.iter().position(|x| *x == v).unwrap()
            }
        };
        for &a in &ascending {
            for &b in &ascending {
                let expect = rank(a).cmp(&rank(b));
                let f64_ord = compare_bytes_as_type(DbType::F64, &a.to_le_bytes(), &b.to_le_bytes());
                assert_eq!(f64_ord, expect, "f64 {a} vs {b}");
                let f32_ord = compare_bytes_as_type(
                    DbType::F32,
                    &(a as f32).to_le_bytes(),
                    &(b as f32).to_le_bytes(),
                );
                // f64::MIN_POSITIVE and MAX do not survive the narrowing.
                if ![f64::MIN_POSITIVE, -f64::MIN_POSITIVE, f64::MAX].contains(&a)
                    && ![f64::MIN_POSITIVE, -f64::MIN_POSITIVE, f64::MAX].contains(&b)
                {
                    assert_eq!(f32_ord, expect, "f32 {a} vs {b}");
                }
            }
        }
        assert_eq!(pg_float_cmp(f64::NAN, f64::NAN), Equal);
        assert_eq!(pg_float_cmp(f64::NAN, f64::INFINITY), Greater);
        assert_eq!(pg_float_cmp(-0.0, 0.0), Equal);
    }

    /// ORDER BY a nullable float through both the capped and uncapped paths:
    /// NaN after +inf, NULL after NaN ascending, the mirror descending.
    #[test]
    fn nan_null_and_signed_zero_order_capped_and_uncapped() {
        let mut f = SmallVec::new();
        f.push(SchemaField::new("id".into(), DbType::I64, 8, 0, 0, false));
        f.push(SchemaField::new("v".into(), DbType::F64, 8, 0, 1, false));
        f.push(SchemaField::new("__null_v".into(), DbType::U8, 1, 0, 2, false));
        let sc: Arc<SmallVec<[SchemaField; 20]>> = Arc::new(f);
        let values: [(i64, Option<f64>); 9] = [
            (1, Some(1.5)),
            (2, Some(f64::NAN)),
            (3, None),
            (4, Some(-0.0)),
            (5, Some(0.0)),
            (6, Some(f64::INFINITY)),
            (7, Some(f64::NEG_INFINITY)),
            (8, Some(f64::NAN)),
            (9, Some(-2.0)),
        ];
        let rows = || -> Vec<Row> {
            values
                .iter()
                .rev()
                .map(|(id, v)| {
                    let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
                    columns.push(col(0, DbType::I64, &id.to_le_bytes()));
                    columns.push(col(1, DbType::F64, &v.unwrap_or(0.0).to_le_bytes()));
                    columns.push(col(2, DbType::U8, &[v.is_none() as u8]));
                    Row {
                        position: *id as u64,
                        columns,
                        length: 0 as _,
                    }
                })
                .collect()
        };
        let ids = |rows: &[Row]| rows.iter().map(|r| r.position as i64).collect::<Vec<_>>();
        let asc = vec![7, 9, 4, 5, 1, 6, 2, 8, 3];
        let desc = vec![3, 2, 8, 6, 1, 4, 5, 9, 7];
        for (spec, expect) in [("v, id", &asc), ("v DESC, id", &desc)] {
            for limit in [None, Some(4u64), Some(1)] {
                let mut got = rows();
                ConcurrentProcessor::sort_rows_if_needed(&mut got, &sc, &Some(spec.into()), limit);
                let want: Vec<i64> =
                    expect.iter().copied().take(limit.unwrap_or(99) as usize).collect();
                assert_eq!(ids(&got), want, "{spec} limit {limit:?}");
            }
        }
    }

    #[allow(non_snake_case)]
    fn Self_sort(rows: &mut Vec<Row>, sc: &Arc<SmallVec<[SchemaField; 20]>>, k: Option<u64>) {
        ConcurrentProcessor::sort_rows_if_needed(rows, sc, &Some("v".to_string()), k);
    }
}

#[cfg(test)]
mod pointer_read_failure_tests {
    //! A pointer read that fails mid-scan must fail the statement.
    //!
    //! The row-mode dispatcher used to treat it as the end of the table, so a
    //! scan over an unreadable pointer file returned whatever it had reached as
    //! a complete, successful answer.
    use super::*;
    use crate::core::row::row::RowFetch;
    use crate::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
    use crate::core::storage_providers::memory::MemoryStorageProvider;
    use std::io::SeekFrom;
    use std::sync::atomic::AtomicU32;

    /// An in-memory provider whose reads start failing once `reads_left`
    /// reaches zero. `u32::MAX` never fails.
    #[derive(Clone)]
    struct FailingIO {
        inner: MemoryStorageProvider,
        reads_left: StdArc<AtomicU32>,
    }

    impl FailingIO {
        fn take_read(&self) -> std::io::Result<()> {
            let left = self.reads_left.load(std::sync::atomic::Ordering::SeqCst);
            if left == u32::MAX {
                return Ok(());
            }
            if left == 0 {
                return Err(std::io::Error::other("injected pointer read failure"));
            }
            self.reads_left
                .store(left - 1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    impl StorageIO for FailingIO {
        async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
            self.inner.verify_data(position, buffer).await
        }
        async fn write_data(&self, position: u64, buffer: &[u8]) {
            self.inner.write_data(position, buffer).await
        }
        async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
            self.inner.write_data_seek(seek, buffer).await
        }
        async fn read_data_into_buffer(
            &self,
            position: &mut u64,
            buffer: &mut [u8],
        ) -> std::io::Result<()> {
            self.take_read()?;
            self.inner.read_data_into_buffer(position, buffer).await
        }
        async fn read_vectored(&self, reads: &mut [(u64, &mut [u8])]) -> std::io::Result<()> {
            self.take_read()?;
            self.inner.read_vectored(reads).await
        }
        async fn append_data(&self, buffer: &[u8], immediate: bool) {
            self.inner.append_data(buffer, immediate).await
        }
        async fn append_data_returning_offset(&self, buffer: &[u8], immediate: bool) -> u64 {
            self.inner
                .append_data_returning_offset(buffer, immediate)
                .await
        }
        async fn get_len(&self) -> u64 {
            self.inner.get_len().await
        }
        fn exists(location: &str, table_name: &str) -> bool {
            MemoryStorageProvider::exists(location, table_name)
        }
        async fn create_temp(&self) -> Self {
            Self {
                inner: self.inner.create_temp().await,
                reads_left: self.reads_left.clone(),
            }
        }
        async fn swap_temp(&self, temp_io_sync: &mut Self) {
            self.inner.swap_temp(&mut temp_io_sync.inner).await
        }
        fn get_location(&self) -> Option<String> {
            self.inner.get_location()
        }
        async fn create_new(&self, name: String) -> Self {
            Self {
                inner: self.inner.create_new(name).await,
                reads_left: self.reads_left.clone(),
            }
        }
        fn drop_io(&self) {
            self.inner.drop_io()
        }
        fn get_hash(&self) -> u32 {
            self.inner.get_hash()
        }
        async fn start_service(&self) {}
        fn get_name(&self) -> String {
            self.inner.get_name()
        }
    }

    /// Two full pointer chunks of live, zero-column records (3047 records a
    /// chunk), so the first chunk reads and the second does not. The rows
    /// themselves are never read: the fetch asks for no columns.
    #[tokio::test]
    async fn a_failed_pointer_read_fails_the_scan_instead_of_ending_it() {
        let records = 3047 * 2 + 5;
        let pointers = FailingIO {
            inner: MemoryStorageProvider::new("ptr_fail_pointers"),
            reads_left: StdArc::new(AtomicU32::new(u32::MAX)),
        };
        pointers
            .write_data(0, &vec![0u8; records * ROW_POINTER_RECORD_LEN])
            .await;
        let rows_io = Arc::new(FailingIO {
            inner: MemoryStorageProvider::new("ptr_fail_rows"),
            reads_left: StdArc::new(AtomicU32::new(u32::MAX)),
        });

        let fetch = || RowFetch {
            columns_fetching_data: SmallVec::new(),
        };
        let schema: Vec<SchemaField> = Vec::new();

        // Control: with nothing failing, every record is a row.
        let pointers_arc = Arc::new(pointers.clone());
        let mut iterator = RowPointerIterator::new(pointers_arc.clone()).await.unwrap();
        let all = ConcurrentProcessor::new()
            .process(
                "t",
                "",
                fetch(),
                fetch(),
                &schema,
                rows_io.clone(),
                &mut iterator,
                None,
                None,
                None,
                Visibility::All,
            )
            .await
            .expect("an intact pointer file scans");
        assert_eq!(all.len(), records);

        // The first chunk reads, the second fails.
        let mut iterator = RowPointerIterator::new(pointers_arc.clone()).await.unwrap();
        pointers
            .reads_left
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let outcome = ConcurrentProcessor::new()
            .process(
                "t",
                "",
                fetch(),
                fetch(),
                &schema,
                rows_io.clone(),
                &mut iterator,
                None,
                None,
                None,
                Visibility::All,
            )
            .await;
        match outcome {
            Ok(rows) => panic!(
                "a pointer read failure was reported as success with {} of {} rows",
                rows.len(),
                records
            ),
            Err(e) => assert!(
                e.to_string().contains("injected pointer read failure"),
                "unexpected error: {e}"
            ),
        }

        // The COUNT(*) path shares the dispatcher and must fail the same way.
        pointers
            .reads_left
            .store(u32::MAX, std::sync::atomic::Ordering::SeqCst);
        let mut iterator = RowPointerIterator::new(pointers_arc.clone()).await.unwrap();
        pointers
            .reads_left
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let counted = ConcurrentProcessor::new()
            .process_count(
                "t",
                "",
                fetch(),
                &schema,
                rows_io.clone(),
                &mut iterator,
                None,
                Visibility::All,
            )
            .await;
        assert!(counted.is_err(), "count reported {:?}", counted.ok());
    }
}

#[cfg(test)]
mod latency_bench {
    //! Point-read latency while another session runs a big scan or sort.

    use crate::core::database::Database;
    use crate::core::rql::executor;
    use crate::core::rql::lexer_s1::recognize_query_purpose;
    use rclite::Arc;
    use std::time::{Duration, Instant};

    async fn run(database: &Arc<Database>, sql: &str) -> Vec<u8> {
        let purpose = recognize_query_purpose(sql).expect("parse");
        executor::execute(purpose, database.clone()).await
    }

    fn percentile(sorted: &[Duration], p: f64) -> Duration {
        sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// point_read_latency_under_a_big_scan --nocapture --test-threads 1`.
    ///
    /// One session repeatedly scans and sorts a 300k-row table while another
    /// reads single rows from a small one; the reader's latency is recorded
    /// with the per-batch yield and the off-worker sort on and off.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn point_read_latency_under_a_big_scan() {
        // File-backed by default: an in-memory table measures the in-memory
        // provider's locking rather than the scheduler — every stall the
        // in-memory runs showed was four workers parked on that provider's
        // buffer lock. `LATENCY_BENCH_MEMORY=1` keeps the in-memory mode, so
        // a change to that provider stays measurable.
        let in_memory = std::env::var("LATENCY_BENCH_MEMORY").is_ok_and(|v| v == "1");
        let dir = format!("/var/tmp/latency_bench_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&dir);
        let database = Arc::new(if in_memory {
            Database::new_with_backend("latency_bench", true).await
        } else {
            Database::new_with_backend(&dir, false).await
        });
        println!("latency bench: {} backend", if in_memory { "in-memory" } else { "file" });
        // Removed however the test ends.
        struct RemoveDir(String);
        impl Drop for RemoveDir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = RemoveDir(dir);
        run(&database, "CREATE TABLE big (id BIGINT NOT NULL, v BIGINT NOT NULL, s TEXT NOT NULL)").await;
        run(&database, "CREATE TABLE small (id BIGINT NOT NULL, s TEXT NOT NULL)").await;
        let rows = 300_000u64;
        for chunk in 0..(rows / 1000) {
            let values: Vec<String> = (0..1000u64)
                .map(|k| {
                    let i = chunk * 1000 + k;
                    format!("({i}, {}, 'row {i} padding padding')", (i * 7919) % rows)
                })
                .collect();
            run(&database, &format!("INSERT INTO big (id, v, s) VALUES {}", values.join(", "))).await;
        }
        let values: Vec<String> = (0..100u64).map(|i| format!("({i}, 's{i}')")).collect();
        run(&database, &format!("INSERT INTO small (id, s) VALUES {}", values.join(", "))).await;

        for round in 0..2 {
            for guards in [false, true] {
                super::LATENCY_GUARDS_OFF.store(!guards, std::sync::atomic::Ordering::Relaxed);
                for load in [
                    "SELECT * FROM big WHERE v >= 0",
                    "SELECT * FROM big ORDER BY v",
                    // The executor's post-scan loop (a constant makes every row
                    // pass through it) and its expression sort (explicit NULLS
                    // placement routes there).
                    "SELECT id, v, s, 1 AS one FROM big WHERE v >= 0",
                    "SELECT * FROM big ORDER BY v DESC NULLS LAST",
                ] {
                    // The load alone first: what the guards cost it.
                    let t = Instant::now();
                    for _ in 0..3 {
                        std::hint::black_box(run(&database, load).await);
                    }
                    let alone = t.elapsed() / 3;

                    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                    let mut loaders = Vec::new();
                    for _ in 0..2 {
                        let (db, stop) = (database.clone(), stop.clone());
                        loaders.push(tokio::spawn(async move {
                            let mut n = 0u32;
                            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                                std::hint::black_box(run(&db, load).await);
                                n += 1;
                            }
                            n
                        }));
                    }
                    // No timers anywhere in the measurement: a sleeping task
                    // is woken by a worker that parks, and starving the
                    // workers is the very thing being measured.
                    let mut latencies = Vec::new();
                    let started = Instant::now();
                    let mut k = 0u64;
                    while latencies.len() < 400 && started.elapsed() < Duration::from_secs(20) {
                        let t = Instant::now();
                        let r = run(&database, &format!("SELECT s FROM small WHERE id = {}", k % 100)).await;
                        latencies.push(t.elapsed());
                        assert_eq!(r.first(), Some(&2));
                        k += 1;
                    }
                    let window = started.elapsed();
                    stop.store(true, std::sync::atomic::Ordering::Relaxed);
                    let mut scans = 0;
                    for l in loaders {
                        scans += l.await.unwrap();
                    }
                    latencies.sort();
                    println!(
                        "round {round} guards {:<3} {:<30} alone {:>8.1?} | reads {:>4} in {:>6.1?}  p50 {:>9.1?}  p99 {:>9.1?}  max {:>9.1?}  (load ran {scans}x)",
                        if guards { "on" } else { "off" },
                        load,
                        alone,
                        latencies.len(),
                        window,
                        percentile(&latencies, 0.50),
                        percentile(&latencies, 0.99),
                        latencies.last().unwrap(),
                    );
                }
            }
        }
        super::LATENCY_GUARDS_OFF.store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod integer_key_sort_tests {
    //! The integer-key shortcut in `sort_rows_if_needed` against a stable
    //! comparison sort by `PreparedSort::compare` — the order it replaced — over
    //! heavy ties, NULLs, signed and unsigned widths, timestamps either side of
    //! 1970, both directions, with and without a cap.
    use super::*;
    use crate::core::db_type::DbType;
    use crate::core::row::row::{Column, Row};
    use crate::memory_pool::MEMORY_POOL;

    const FIELDS: [(&str, DbType, u32); 7] = [
        ("v", DbType::I64, 8),
        ("__null_v", DbType::BOOL, 1),
        ("w", DbType::I32, 4),
        ("u", DbType::U8, 1),
        ("ts", DbType::DATETIME, 8),
        ("big", DbType::U64, 8),
        ("id", DbType::U64, 8),
    ];

    fn schema() -> Arc<SmallVec<[SchemaField; 20]>> {
        Arc::new(
            FIELDS
                .iter()
                .enumerate()
                .map(|(i, (n, t, size))| SchemaField::new(n.to_string(), t.clone(), *size, 0, i as u64, false))
                .collect(),
        )
    }

    fn col(id: u64, ty: DbType, bytes: &[u8]) -> Column {
        let mut data = MEMORY_POOL.acquire(bytes.len());
        data.into_slice_mut().copy_from_slice(bytes);
        Column {
            schema_id: crate::core::row::row::schema_id_of(id),
            data,
            column_type: ty,
        }
    }

    fn rows(n: usize, seed: u64) -> Vec<Row> {
        let mut state = seed | 1;
        let mut next = move |m: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % m
        };
        (0..n)
            .map(|id| {
                let mut columns = SmallVec::new();
                let null = next(5) == 0;
                let v: i64 = [i64::MIN, -3, -1, 0, 1, 2, i64::MAX][next(7) as usize];
                columns.push(col(0, DbType::I64, &(if null { 0 } else { v }).to_le_bytes()));
                columns.push(col(1, DbType::BOOL, &[null as u8]));
                columns.push(col(2, DbType::I32, &(next(9) as i32 - 4).to_le_bytes()));
                columns.push(col(3, DbType::U8, &[[0u8, 1, 127, 128, 255][next(5) as usize]]));
                columns.push(col(4, DbType::DATETIME, &((next(6) as i64 - 3) * 86_400_000_000).to_le_bytes()));
                columns.push(col(5, DbType::U64, &[0u64, 1, u64::MAX, 1 << 63][next(4) as usize].to_le_bytes()));
                columns.push(col(6, DbType::U64, &(id as u64).to_le_bytes()));
                Row {
                    position: id as u64,
                    columns,
                    length: 0,
                }
            })
            .collect()
    }

    fn reference(mut rows: Vec<Row>, sc: &Arc<SmallVec<[SchemaField; 20]>>, spec: &str) -> Vec<u64> {
        let keys = ConcurrentProcessor::resolve_sort_keys(sc, spec);
        let sort = PreparedSort::new(&keys, sc.as_slice(), rows.first());
        rows.sort_by(|a, b| sort.compare(a, b));
        rows.iter().map(|r| r.position).collect()
    }

    #[test]
    fn integer_keys_order_exactly_as_the_stable_comparison_sort() {
        let sc = schema();
        let mut checked = 0;
        for (n, seed) in [(0usize, 1u64), (1, 2), (17, 3), (600, 4), (5000, 5)] {
            for column in ["v", "w", "u", "ts", "big"] {
                for dir in ["", " ASC", " DESC"] {
                    let spec = format!("{column}{dir}");
                    let want = reference(rows(n, seed), &sc, &spec);
                    // The shortcut must actually be the path taken.
                    let probe = rows(n.max(1), seed);
                    let keys = ConcurrentProcessor::resolve_sort_keys(&sc, &spec);
                    assert!(
                        PreparedSort::new(&keys, sc.as_slice(), probe.first())
                            .integer_keys(&probe)
                            .is_some(),
                        "{spec} did not take the integer path"
                    );
                    for cap in [None, Some(0u64), Some(1), Some(3), Some(n as u64 / 2), Some(n as u64), Some(n as u64 + 2)] {
                        let mut got = rows(n, seed);
                        ConcurrentProcessor::sort_rows_if_needed(&mut got, &sc, &Some(spec.clone()), cap);
                        let got: Vec<u64> = got.iter().map(|r| r.position).collect();
                        let k = cap.map_or(n, |c| (c as usize).min(n));
                        assert_eq!(got, want[..k], "{spec} cap {cap:?} n {n}");
                        checked += 1;
                    }
                }
            }
        }
        assert!(checked > 400);
    }

    /// Two keys, or a key some row lacks, keep the comparison sort.
    #[test]
    fn shapes_it_cannot_order_fall_back() {
        let sc = schema();
        let r = rows(10, 9);
        let two = ConcurrentProcessor::resolve_sort_keys(&sc, "w, id");
        assert!(PreparedSort::new(&two, sc.as_slice(), r.first()).integer_keys(&r).is_none());
        let mut short = rows(10, 9);
        short[4].columns.retain(|c| c.schema_id != 2);
        let one = ConcurrentProcessor::resolve_sort_keys(&sc, "w");
        assert!(PreparedSort::new(&one, sc.as_slice(), short.first()).integer_keys(&short).is_none());
    }
}
