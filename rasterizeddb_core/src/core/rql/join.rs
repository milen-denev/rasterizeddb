//! `INNER`, `LEFT`, `RIGHT`, `FULL` and `CROSS` join planning and execution.
//!
//! Joins get their own path end to end, because the single-table plan is built
//! from one schema and one `RowFetch`, and a join needs one of each per
//! relation. Splitting them keeps the single-table path — which is every hot
//! query — untouched.
//!
//! The strategy is a hash join over materialised rows: each relation is scanned
//! once, with whatever part of the `WHERE` clause belongs to it pushed down, and
//! the results are combined by hashing the right-hand side on its join key. The
//! per-relation scan is the ordinary scan, so filter pruning and predicate
//! evaluation still apply exactly as they do without a join.
//!
//! # What is deliberately refused
//!
//! A `WHERE` conjunct that references two relations at once is rejected rather
//! than evaluated after the join. Accepting it would mean re-implementing the
//! predicate evaluator over combined rows, and quietly evaluating it *before*
//! the join — which is what a naive push-down would do — changes the answer.

use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::row::row::{Column, ColumnFetchingData, Row, RowFetch};
use crate::core::row::schema::{SchemaCalculator, SchemaField};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    /// `FROM a, b` and `a CROSS JOIN b`: every left row paired with every right
    /// row. It has no join key, so it is the one kind `combine` handles without
    /// building a hash index.
    Cross,
    /// `RIGHT [OUTER] JOIN`: every row of the joined relation survives; the
    /// relations before it are NULL where it matched nothing.
    Right,
    /// `FULL [OUTER] JOIN`: both sides survive, each NULL-padded where it
    /// matched nothing.
    Full,
}

impl JoinKind {
    /// True when the relations joined *before* this one may come out absent.
    fn nulls_earlier(self) -> bool {
        matches!(self, JoinKind::Right | JoinKind::Full)
    }

    /// True when the relation this join attaches may come out absent.
    fn nulls_this(self) -> bool {
        matches!(self, JoinKind::Left | JoinKind::Full)
    }
}

/// Whether relation `rel` may contribute no row to a joined tuple: it is the
/// null-supplying side of an outer join. A `RIGHT` or `FULL` join makes every
/// relation before it nullable, not just the one it names, because the join
/// runs left-deep over everything combined so far.
fn may_be_unmatched(kinds: &[Option<JoinKind>], rel: usize) -> bool {
    kinds.get(rel).copied().flatten().is_some_and(JoinKind::nulls_this)
        || kinds
            .iter()
            .skip(rel + 1)
            .flatten()
            .any(|kind| kind.nulls_earlier())
}

/// The `ON` terms of a `RIGHT` or `FULL` join that read only the joined
/// relation, evaluated against each of its rows before hashing.
///
/// Not a scan predicate, as they are for `INNER` and `LEFT`: there a row the
/// term rejects simply cannot match, which filtering the scan reproduces. Here
/// the joined relation is the *preserved* side, so a rejected row must still
/// come out — NULL-padded — and filtering the scan would delete it.
#[derive(Debug, Clone)]
pub struct MatchCondition {
    pub cond: crate::core::rql::expr::Cond,
    /// The relation's own schema, which the scanned rows are laid out to.
    pub schema: Vec<SchemaField>,
}

/// One relation in the `FROM` clause, after the first.
#[derive(Debug, Clone)]
pub struct JoinedRelation {
    pub kind: JoinKind,
    /// Index into [`JoinPlan::relations`] of the side this one is joined to.
    pub left_rel: usize,
    /// The equality pairs making up the join key, as canonical column names —
    /// `(column on the left relation, column on this one)`.
    ///
    /// A `Vec` rather than one pair because a composite key is ordinary SQL:
    /// every junction table is joined on both of its columns. Matching on one
    /// and filtering on the other is not the same join — it would fan out on the
    /// first column and then discard, which gives the right rows only when the
    /// first column is already unique.
    pub keys: Vec<(String, String)>,
}

/// A relation participating in the join, with the name it is addressed by.
#[derive(Debug, Clone)]
pub struct Relation {
    pub table: String,
    /// The alias, if one was given; otherwise the table name.
    pub label: String,
    /// How this relation attaches to the ones before it. `None` for the first.
    pub join: Option<JoinedRelation>,
    /// The part of the `WHERE` clause that references only this relation,
    /// already desugared. Empty when there is none.
    pub predicate: String,
    /// Columns this relation must fetch: join keys, selected columns and any
    /// sort key.
    pub fetch_columns: Vec<SchemaField>,
    /// Keep only the combined rows where this relation found **no** match —
    /// the anti-join `LEFT JOIN r ON … WHERE r.key IS NULL`.
    ///
    /// Applied after the join, never as a scan predicate. Pushed down it would
    /// filter the right side to rows whose key is null (none), leaving the
    /// index empty so *every* left row survives as unmatched — which is how
    /// this returned all rows instead of the unmatched ones.
    pub require_unmatched: bool,
    /// `ON` terms deciding which of this relation's rows may match, for a
    /// `RIGHT`/`FULL` join. See [`MatchCondition`].
    pub match_condition: Option<MatchCondition>,
    /// `ON` terms that read only the relations joined *before* this one —
    /// `LEFT JOIN b ON b.k = a.k AND a.active` — evaluated against each tuple
    /// so far: a tuple they reject matches nothing here. For an INNER join
    /// that drops it, as a `WHERE` would; for LEFT and FULL it survives
    /// NULL-padded, and for RIGHT and FULL the rows it would have matched come
    /// out unmatched — PostgreSQL's meaning in every case.
    pub left_condition: Option<PostJoinFilter>,
}

/// A predicate evaluated over combined rows, after the join.
///
/// The combined tuple is flattened into one synthetic row whose schema holds
/// every fetched column of every relation, named `<label>__<column>` so two
/// relations with a column of the same name stay distinct. The ordinary
/// per-row evaluator then runs against it, so cross-relation predicates mean
/// exactly what they mean in a single-table `WHERE`.
#[derive(Debug, Clone)]
pub struct PostJoinFilter {
    /// The synthetic schema, in write order.
    pub schema: Vec<SchemaField>,
    /// The predicate, rewritten against `schema`.
    pub where_clause: String,
    /// `columns[relation][i]` is the synthetic write order of that relation's
    /// `fetch_columns[i]`.
    pub columns: Vec<Vec<u64>>,
    /// Terms the predicate tokenizer cannot read — a comparison against `||`,
    /// `replace()` or a cast — evaluated by the expression evaluator instead.
    ///
    /// Kept apart rather than folded into `where_clause` because the two are
    /// evaluated by different machinery; both must hold for a row to survive.
    pub expr_terms: Vec<crate::core::rql::expr::Cond>,
}

/// One output column: which relation it comes from, and what it is called.
#[derive(Debug, Clone)]
pub struct JoinSelectItem {
    pub rel: usize,
    pub field: SchemaField,
    pub label: String,
    /// A nullary scalar function — `now()`, `gen_random_uuid()` — occupying this
    /// output position instead of a column. Evaluated per row, so a UUID
    /// differs in every one; `rel` and `field` are placeholders when set.
    pub scalar: Option<crate::core::rql::aggregate::ScalarFunc>,
    /// A constant occupying this output position: `SELECT a.id, 0, NULL FROM …`.
    /// Like `scalar`, it reads no column.
    pub literal: Option<Vec<u8>>,
    /// An expression over the *combined* row — `COALESCE(s.a, t.b) AS eff`.
    ///
    /// Evaluated per joined tuple, the way the post-join predicate is, because
    /// that is the first thing that has columns from both sides. `rel` is a
    /// placeholder when set, and `field.db_type` carries the type the row
    /// description advertises, so the value and its declared type come from one
    /// place — a description that disagrees with the payload corrupts the value
    /// rather than merely mislabelling it.
    pub expr: Option<crate::core::rql::expr::Expr>,
    /// This item is a select-list call whose expression has not been parsed
    /// yet — it cannot be, until the relations' fetch lists are settled.
    ///
    /// Marked rather than tracked by position: the aggregate stage rewrites
    /// `items`, so an index recorded before it runs no longer names the same
    /// item afterwards. Removing by a stale index deleted a real column and
    /// returned it as NULL.
    pub pending_expression: bool,
}

impl JoinSelectItem {
    /// Whether this item reads a relation's column, rather than computing its
    /// value. A computed item's `rel` and `field` are placeholders — relation
    /// 0, write order 0 — so anything that finds an item by relation and write
    /// order has to skip it, or it takes the computed value for that
    /// relation's first column.
    pub fn reads_column(&self) -> bool {
        self.expr.is_none()
            && self.literal.is_none()
            && self.scalar.is_none()
            && !self.pending_expression
    }
}

#[derive(Debug, Clone)]
pub struct JoinPlan {
    pub relations: Vec<Relation>,
    pub items: Vec<JoinSelectItem>,
    /// Sort keys as `(relation, column, descending)`, in the order written.
    /// Later keys only break the ties the earlier ones left.
    ///
    /// Empty when `order_exprs` is used instead.
    pub order_by: Vec<(usize, SchemaField, bool)>,
    /// `ORDER BY` evaluated over the combined row: `(expression, descending,
    /// nulls_first)`.
    ///
    /// Used when *any* term is more than a projected column — an expression
    /// like `COALESCE(s.a, t.b)`, or a key that is not in the SELECT list. When
    /// it is used it carries **every** term, including the plain columns:
    /// splitting an ORDER BY across two comparators is how the two come to
    /// disagree about NULL placement and produce wrong rows.
    pub order_exprs: Vec<(crate::core::rql::expr::Expr, bool, Option<bool>)>,
    /// How to build the combined row that `order_exprs` **and** any select-list
    /// expression are evaluated against. Present when either needs one.
    pub order_layout: Option<CombinedLayout>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    /// `WHERE` terms that reference more than one relation, evaluated after the
    /// join against the combined row.
    ///
    /// These cannot be pushed into a relation's own scan: that happens *before*
    /// the join, and a predicate spanning two relations has no meaning there —
    /// pushing it would change which rows pair up rather than which pairs
    /// survive.
    pub post_join: Option<PostJoinFilter>,
    /// `SELECT DISTINCT`: identical projected rows collapse to one.
    ///
    /// Applied to the *projected* columns, which is what the select list
    /// produces, and before `OFFSET`/`LIMIT` — SQL evaluates DISTINCT first, so
    /// a `LIMIT 10` over a distinct set means ten distinct rows rather than
    /// however many of the first ten happened to differ.
    pub distinct: bool,
    /// Set when the SELECT list aggregates over the joined rows.
    ///
    /// The join projects first, then this folds. Its `SelectItem`s address the
    /// projected rows positionally, because a joined row has no single schema
    /// for `write_order` to refer to — `items[i]` becomes `write_order == i`.
    /// When present, `order_by`, `limit` and `offset` belong to the fold and are
    /// left unset on the join itself.
    pub aggregate: Option<crate::core::rql::aggregate::AggregatePlan>,
}

impl JoinPlan {
    /// Output column names and types, in select order.
    pub fn output_columns(&self) -> Vec<(String, DbType)> {
        if let Some(agg) = &self.aggregate {
            return agg
                .items
                .iter()
                .map(|i| (i.label().to_string(), i.result_type()))
                .collect();
        }
        self.items
            .iter()
            .map(|i| (i.label.clone(), i.field.db_type.clone()))
            .collect()
    }

    /// The scale each output column's `DECIMAL` bytes are stored at, slot for
    /// slot with [`JoinPlan::output_columns`] — what pgwire must render them at.
    ///
    /// A column read straight through a join is at its own declared scale, and
    /// so are `SUM`/`MIN`/`MAX` of one; `AVG` is at
    /// `AggFunc::decimal_output_scale`; anything computed is at the engine
    /// scale, as `expr::eval` produces it. Without this a join was rendered at
    /// the engine scale throughout, so a `NUMERIC(10,2)` holding 1.25 read
    /// back through any join as 0.000000125.
    pub fn output_scales(&self) -> Vec<u32> {
        use crate::core::rql::aggregate::SelectItem;
        let engine = crate::core::value_codec::DECIMAL_SCALE;
        if let Some(agg) = &self.aggregate {
            return agg
                .items
                .iter()
                .map(|item| match item {
                    SelectItem::Column { field, .. } => field.scale.unwrap_or(engine),
                    SelectItem::Aggregate {
                        func,
                        arg,
                        arg_expr: None,
                        ..
                    } if item.result_type() == DbType::DECIMAL => {
                        func.decimal_output_scale(arg.as_ref())
                    }
                    _ => engine,
                })
                .collect();
        }
        self.items
            .iter()
            .map(|i| {
                if let Some(expr) = i.expr.as_ref() {
                    // At the scale PostgreSQL displays it at, as a single
                    // table's select list sends it: `gross - qty * price` over
                    // NUMERIC(14,2) and NUMERIC(12,2) is 0.00, not 0.000000000.
                    // The projector rounds the value to the same scale.
                    crate::core::rql::expr::display_scale(expr).unwrap_or(engine)
                } else if i.literal.is_some() || i.scalar.is_some() {
                    engine
                } else {
                    i.field.scale.unwrap_or(engine)
                }
            })
            .collect()
    }
}

/// Builds a `RowFetch` for a set of schema fields.
pub fn fetch_for(fields: &[SchemaField], schema: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let calc = SchemaCalculator::default();
    let mut columns_fetching_data = SmallVec::new();
    for field in fields {
        let (offset, schema_id) = calc.calculate_schema_offset(&field.name, schema);
        columns_fetching_data.push(ColumnFetchingData {
            column_offset: offset,
            column_type: field.db_type.physical(),
            size: field.size,
            schema_id,
        });
    }
    RowFetch {
        columns_fetching_data,
    }
}

/// A tuple slot whose relation contributed no row — an outer join that matched
/// nothing.
const NO_ROW: u32 = u32::MAX;

#[cfg(feature = "enable_long_row")]
const ROW_HEADER: usize = 8;
#[cfg(not(feature = "enable_long_row"))]
const ROW_HEADER: usize = 4;

/// One relation's scanned rows.
///
/// The join reads its relations in the scan's serialized form: one contiguous
/// buffer plus a `u32` start offset per row. A materialised `Row` is ~1.5 KB
/// whatever it holds — its columns are an inline `SmallVec` of 32 — so a
/// million-row relation was 1.56 GB of `Row` holding a few dozen bytes each;
/// serialized it is the bytes plus 4 per row. Cells are read in place by
/// [`RowRef::column`].
pub enum RowSet {
    /// Decoded rows: what a caller that already has them hands over.
    Rows(Vec<Row>),
    /// A scan's serialized rows (`Row::extend_serialized`'s format), and where
    /// each starts.
    Serialized { bytes: Vec<u8>, starts: Vec<u32> },
}

impl RowSet {
    /// Indexes a scan's serialized rows. Refused when the buffer is past what a
    /// `u32` offset can address, or ends partway through a row.
    pub fn from_serialized(bytes: Vec<u8>) -> Result<Self, String> {
        if bytes.len() > u32::MAX as usize {
            return Err("a relation is too large to join".to_string());
        }
        let mut starts = Vec::new();
        let mut at = 0usize;
        for row in crate::core::row::row::serialized_rows(&bytes) {
            let row = row.map_err(|e| format!("a scanned row could not be read: {}", e))?;
            starts.push(at as u32);
            at += row.len();
        }
        Ok(RowSet::Serialized { bytes, starts })
    }

    /// Serializes decoded rows — for tests, and for comparing the two forms.
    pub fn serialize(rows: &[Row]) -> Result<Self, String> {
        let mut bytes = Vec::new();
        for row in rows {
            row.extend_serialized(&mut bytes);
        }
        Self::from_serialized(bytes)
    }

    pub fn len(&self) -> usize {
        match self {
            RowSet::Rows(rows) => rows.len(),
            RowSet::Serialized { starts, .. } => starts.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get(&self, i: usize) -> Option<RowRef<'_>> {
        match self {
            RowSet::Rows(rows) => rows.get(i).map(RowRef::Row),
            RowSet::Serialized { bytes, starts } => {
                let start = *starts.get(i)? as usize;
                let end = starts.get(i + 1).map_or(bytes.len(), |e| *e as usize);
                Some(RowRef::Bytes(&bytes[start..end]))
            }
        }
    }
}

/// One scanned row, decoded or serialized.
#[derive(Clone, Copy)]
pub enum RowRef<'a> {
    Row(&'a Row),
    /// One serialized row, length header included.
    Bytes(&'a [u8]),
}

impl<'a> RowRef<'a> {
    /// The first cell carrying `schema_id`, as `find` on a `Row` would pick
    /// it: its bytes and its type.
    #[inline]
    pub fn column(self, schema_id: u64) -> Option<(&'a [u8], DbType)> {
        match self {
            RowRef::Row(row) => row
                .columns
                .iter()
                .find(|c| u64::from(c.schema_id) == schema_id)
                .map(|c| (c.data.into_slice(), c.column_type.clone())),
            RowRef::Bytes(bytes) => {
                // Per cell: an 8-byte size, an 8-byte schema id, the data, and
                // a 1-byte type tag. Every length is checked against the
                // buffer, as `Row::decode` checks it.
                let mut p = ROW_HEADER;
                while p + 17 <= bytes.len() {
                    let size = u64::from_le_bytes(bytes[p..p + 8].try_into().unwrap());
                    let id = u64::from_le_bytes(bytes[p + 8..p + 16].try_into().unwrap());
                    let data_at = p + 16;
                    let end = usize::try_from(size).ok()?.checked_add(data_at)?;
                    if end >= bytes.len() {
                        return None;
                    }
                    if id == schema_id {
                        return Some((&bytes[data_at..end], DbType::from_byte(bytes[end])));
                    }
                    p = end + 1;
                }
                None
            }
        }
    }

    /// Runs `f` over the row as a `Row`, decoding a serialized one for the
    /// call. For the evaluator, which reads `Row`s.
    pub fn with_row<T>(self, f: impl FnOnce(&Row) -> T) -> Result<T, String> {
        match self {
            RowRef::Row(row) => Ok(f(row)),
            RowRef::Bytes(bytes) => Row::decode(bytes)
                .map(|row| f(&row))
                .map_err(|e| format!("a scanned row could not be read: {}", e)),
        }
    }
}

/// The joined tuples, as row indices into the scanned relations.
///
/// A tuple used to be a `Vec<Option<Row>>` filled with deep copies of every
/// row it held — and a `Row` is ~1.5 KB whatever it carries, with
/// `MemoryBlock::clone` a real copy — so every join step cloned every row of
/// every tuple again. Here a tuple is `width` `u32`s in one flat buffer, and
/// the rows are read in place; the output columns are copied exactly once, by
/// [`project`].
pub struct Joined<'a> {
    scanned: &'a [RowSet],
    width: usize,
    slots: Vec<u32>,
}

/// One joined tuple: the row each relation contributed, or none.
#[derive(Clone, Copy)]
pub struct Tuple<'a> {
    scanned: &'a [RowSet],
    slots: &'a [u32],
}

impl<'a> Tuple<'a> {
    /// The row relation `rel` contributed, or `None` where an outer join
    /// matched nothing.
    #[inline]
    pub fn get(&self, rel: usize) -> Option<RowRef<'a>> {
        let at = *self.slots.get(rel)?;
        if at == NO_ROW {
            return None;
        }
        self.scanned.get(rel)?.get(at as usize)
    }
}

impl<'a> Joined<'a> {
    fn empty(scanned: &'a [RowSet]) -> Self {
        Joined {
            scanned,
            width: 1,
            slots: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.slots.len() / self.width
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn tuple(&self, i: usize) -> Tuple<'_> {
        Tuple {
            scanned: self.scanned,
            slots: &self.slots[i * self.width..(i + 1) * self.width],
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Tuple<'_>> + '_ {
        let scanned = self.scanned;
        self.slots
            .chunks_exact(self.width)
            .map(move |slots| Tuple { scanned, slots })
    }

    /// Keeps the tuples `keep` answers true for, in order.
    fn retain(&mut self, mut keep: impl FnMut(usize, Tuple<'_>) -> bool) {
        let mut kept: Vec<u32> = Vec::with_capacity(self.slots.len());
        for (i, slots) in self.slots.chunks_exact(self.width).enumerate() {
            if keep(
                i,
                Tuple {
                    scanned: self.scanned,
                    slots,
                },
            ) {
                kept.extend_from_slice(slots);
            }
        }
        self.slots = kept;
    }
}

/// Test relations, serialized as a scan hands them over — the production form —
/// except every third one, kept decoded so both forms stay exercised.
#[cfg(test)]
fn sets(rels: Vec<Vec<Row>>) -> Vec<RowSet> {
    rels.into_iter()
        .enumerate()
        .map(|(i, rows)| {
            if i % 3 == 2 {
                RowSet::Rows(rows)
            } else {
                RowSet::serialize(&rows).expect("serializable")
            }
        })
        .collect()
}

/// One column of a join key, and the companion that says whether it is NULL.
#[derive(Clone, Copy)]
struct KeyColumn {
    write_order: u64,
    flag: Option<u64>,
    /// Both sides are documents, so the key matches by value.
    document: bool,
}

/// One key column's value as the join matches it: by bytes, or for two
/// documents by value, so `{"a":1}` meets `{"a":1.0}` as `a.doc = b.doc`
/// says it does. The hash agrees with the equality, since a document hashes
/// by the key [`crate::core::jsonb::push_group_key`] gives it, which is equal
/// exactly when the two are.
#[derive(Clone, Copy)]
struct KeyValue<'r> {
    bytes: &'r [u8],
    document: bool,
}

impl PartialEq for KeyValue<'_> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        if self.document && other.document {
            crate::core::jsonb::values_equal(self.bytes, other.bytes)
                .unwrap_or_else(|| self.bytes == other.bytes)
        } else {
            self.bytes == other.bytes
        }
    }
}

impl Eq for KeyValue<'_> {}

impl std::hash::Hash for KeyValue<'_> {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.document {
            let mut key = Vec::with_capacity(self.bytes.len() + 8);
            crate::core::jsonb::push_group_key(self.bytes, &mut key);
            key.hash(state);
        } else {
            self.bytes.hash(state);
        }
    }
}

/// A key column's bytes, or `None` when the row holds no value there — the
/// column is absent, typed NULL, or its companion flag is set.
///
/// NULL equals nothing in SQL, so a NULL key must never match. A NULL is
/// stored as its type's zeroes, so comparing the bytes alone matched a NULL
/// integer key against a real `0` (and a NULL text against `''`).
#[inline]
fn key_bytes<'r>(row: RowRef<'r>, key: KeyColumn) -> Option<KeyValue<'r>> {
    let (bytes, ty) = row.column(key.write_order)?;
    if ty == DbType::NULL {
        return None;
    }
    if let Some(flag) = key.flag {
        if row
            .column(flag)
            .is_some_and(|(b, _)| crate::core::rql::nullable::flag_is_set(b))
        {
            return None;
        }
    }
    Some(KeyValue {
        bytes,
        document: key.document,
    })
}

/// Every key column of a row, or `None` if any is absent or NULL.
///
/// Kept as separate slices rather than a concatenation: joining the bytes would
/// make `("ab", "c")` and `("a", "bc")` the same key, which is a wrong match
/// rather than a missed one. Inline for up to four columns, so a composite key
/// allocates nothing per row.
#[inline]
fn composite_key<'r>(row: RowRef<'r>, keys: &[KeyColumn]) -> Option<SmallVec<[KeyValue<'r>; 4]>> {
    keys.iter().map(|k| key_bytes(row, *k)).collect()
}

/// One hash-join step: joins relation `idx` onto the tuples so far.
///
/// The build side is a `HashTable<u32>` of chain heads — row indices only, the
/// key read back from the row whenever two hashes collide — with the chains
/// threaded through `next`. The `HashMap<K, (head, tail)>` this replaces held
/// a 16-byte slice (or a `SmallVec` of them) and two `u32`s per bucket, about
/// 52 MB for a million build rows against ~10 MB here. Chains are built back to
/// front, so a key's matches still come out in scan order.
///
/// `eligible` is the right rows the `ON` clause's own terms admit; `None`
/// admits all. A row it rejects can match nothing, but a `RIGHT`/`FULL` join
/// still returns it, NULL-padded — which is why those terms cannot be pushed
/// into the scan the way they are for `INNER` and `LEFT`.
///
/// `anti` is a presence-only probe for a `LEFT` join whose relation must end
/// up unmatched (`WHERE r.k IS NULL`): a tuple that matches is dropped here
/// rather than fanned out into one tuple per match and discarded afterwards,
/// and only the unmatched ones are kept, with this side absent.
#[allow(clippy::too_many_arguments)]
fn hash_step<'r, K: std::hash::Hash + Eq>(
    kind: JoinKind,
    idx: usize,
    left_rel: usize,
    width: usize,
    scanned: &'r [RowSet],
    combined: &[u32],
    eligible: Option<&[bool]>,
    left_ok: Option<&[bool]>,
    anti: bool,
    left_key: impl Fn(RowRef<'r>) -> Option<K>,
    right_key: impl Fn(RowRef<'r>) -> Option<K>,
) -> Vec<u32> {
    use std::hash::BuildHasher;
    let right = &scanned[idx];
    let state = ahash::RandomState::new();

    // An INNER join is symmetric, so the hash goes on whichever side is
    // smaller: a hundred tuples joined to a million rows built a
    // million-entry table to probe a hundred times. Output then comes out
    // grouped by the right row rather than by the tuple — a join without
    // ORDER BY has no defined order.
    if kind == JoinKind::Inner
        && !anti
        && eligible.is_none()
        && combined.len() / width < right.len()
        && !always_build_right()
    {
        let tuples = combined.len() / width;
        let tuple_key = |t: usize| -> Option<K> {
            if left_ok.is_some_and(|ok| !ok[t]) {
                return None;
            }
            let left = combined[t * width + left_rel];
            if left == NO_ROW {
                return None;
            }
            scanned[left_rel].get(left as usize).and_then(&left_key)
        };
        let mut heads: hashbrown::HashTable<u32> = hashbrown::HashTable::with_capacity(tuples);
        let mut next: Vec<u32> = vec![NO_ROW; tuples];
        for t in (0..tuples).rev() {
            let Some(key) = tuple_key(t) else { continue };
            let hash = state.hash_one(&key);
            match heads.find_mut(hash, |at| tuple_key(*at as usize).is_some_and(|k| k == key)) {
                Some(head) => {
                    next[t] = *head;
                    *head = t as u32;
                }
                None => {
                    heads.insert_unique(hash, t as u32, |at| {
                        tuple_key(*at as usize).map_or(0, |k| state.hash_one(&k))
                    });
                }
            }
        }
        let mut out: Vec<u32> = Vec::new();
        for r in 0..right.len() {
            let Some(key) = right.get(r).and_then(&right_key) else { continue };
            let hash = state.hash_one(&key);
            let Some(mut t) = heads
                .find(hash, |at| tuple_key(*at as usize).is_some_and(|k| k == key))
                .copied()
            else {
                continue;
            };
            while t != NO_ROW {
                let at = out.len();
                out.extend_from_slice(&combined[t as usize * width..(t as usize + 1) * width]);
                out[at + idx] = r as u32;
                t = next[t as usize];
            }
        }
        return out;
    }

    let row = |at: usize| right.get(at).expect("a row index from this relation");
    let rehash = |at: &u32| -> u64 {
        // Only rows that produced a key are ever inserted.
        right_key(row(*at as usize)).map_or(0, |k| state.hash_one(&k))
    };
    let mut heads: hashbrown::HashTable<u32> = hashbrown::HashTable::with_capacity(right.len());
    let mut next: Vec<u32> = if anti { Vec::new() } else { vec![NO_ROW; right.len()] };
    for r in (0..right.len()).rev() {
        if eligible.is_some_and(|e| !e[r]) {
            continue;
        }
        let Some(key) = right_key(row(r)) else { continue };
        let hash = state.hash_one(&key);
        let same = |at: &u32| right_key(row(*at as usize)).is_some_and(|k| k == key);
        match heads.find_mut(hash, same) {
            Some(head) => {
                if !anti {
                    next[r] = *head;
                    *head = r as u32;
                }
            }
            None => {
                heads.insert_unique(hash, r as u32, rehash);
            }
        }
    }

    let keep_left = matches!(kind, JoinKind::Left | JoinKind::Full);
    let keep_right = matches!(kind, JoinKind::Right | JoinKind::Full);
    debug_assert!(!anti || kind == JoinKind::Left, "the presence probe is a LEFT join's");
    let mut matched: Vec<bool> = if keep_right {
        vec![false; right.len()]
    } else {
        Vec::new()
    };

    let mut out: Vec<u32> = Vec::with_capacity(combined.len());
    for (t, tuple) in combined.chunks_exact(width).enumerate() {
        let left = tuple[left_rel];
        let head = if left == NO_ROW || left_ok.is_some_and(|ok| !ok[t]) {
            // An earlier outer join left this side empty: a NULL key.
            None
        } else {
            scanned[left_rel]
                .get(left as usize)
                .and_then(&left_key)
                .and_then(|k| {
                    let hash = state.hash_one(&k);
                    heads
                        .find(hash, |at| right_key(row(*at as usize)).is_some_and(|rk| rk == k))
                        .copied()
                })
        };
        match head {
            // Matched: the anti-join's predicate rejects every such tuple.
            Some(_) if anti => {}
            Some(mut r) => {
                while r != NO_ROW {
                    let at = out.len();
                    out.extend_from_slice(tuple);
                    out[at + idx] = r;
                    if keep_right {
                        matched[r as usize] = true;
                    }
                    r = next[r as usize];
                }
            }
            // An inner or right join drops the tuple; a left or full join
            // keeps it with this side absent.
            None => {
                if keep_left {
                    out.extend_from_slice(tuple);
                }
            }
        }
    }

    // Every right row nothing matched — including one with a NULL key, and
    // one the `ON` terms rejected — survives with every earlier relation
    // absent.
    if keep_right {
        for (r, hit) in matched.iter().enumerate() {
            if !*hit {
                let at = out.len();
                out.resize(at + width, NO_ROW);
                out[at + idx] = r as u32;
            }
        }
    }
    out
}

/// `RDB_JOIN_BUILD_RIGHT=1`: always hash the joined relation, as before the
/// smaller side was chosen. Read once. For A/B measurement only.
fn always_build_right() -> bool {
    static RIGHT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *RIGHT.get_or_init(|| std::env::var_os("RDB_JOIN_BUILD_RIGHT").is_some_and(|v| v == "1"))
}

/// Executes the join over already-scanned per-relation rows.
///
/// `scanned[i]` holds the rows of `plan.relations[i]`. Relations must be joined
/// in declaration order, each to one that precedes it, which is what the `FROM`
/// clause guarantees.
///
/// The error is returned rather than swallowed: a post-join predicate that
/// fails to compile used to produce an empty result, which reads as a query
/// that matched nothing rather than one that never ran.
pub fn combine<'a>(plan: &JoinPlan, scanned: &'a [RowSet]) -> Result<Joined<'a>, String> {
    if plan.relations.is_empty() || scanned.is_empty() {
        return Ok(Joined::empty(scanned));
    }
    if scanned.len() < plan.relations.len() {
        return Err(format!(
            "the join has {} relations but {} were scanned",
            plan.relations.len(),
            scanned.len()
        ));
    }
    // `NO_ROW` is `u32::MAX`, so a row index must stay below it. A relation
    // that large could not be materialised anyway — a `Row` is ~1.5 KB.
    if scanned.iter().any(|rows| rows.len() >= NO_ROW as usize) {
        return Err("a relation is too large to join".to_string());
    }

    let width = plan.relations.len();
    let mut combined: Vec<u32> = Vec::with_capacity(scanned[0].len() * width);
    for r in 0..scanned[0].len() {
        combined.push(r as u32);
        combined.extend(std::iter::repeat_n(NO_ROW, width - 1));
    }

    for (idx, relation) in plan.relations.iter().enumerate().skip(1) {
        let Some(join) = relation.join.as_ref() else {
            continue;
        };

        // A cross join pairs every combined row with every row of this
        // relation. No key, no hash index — just the product.
        if join.kind == JoinKind::Cross {
            let right_n = scanned[idx].len();
            let mut next: Vec<u32> = Vec::with_capacity(combined.len() * right_n);
            for tuple in combined.chunks_exact(width) {
                for r in 0..right_n {
                    let at = next.len();
                    next.extend_from_slice(tuple);
                    next[at + idx] = r as u32;
                }
            }
            combined = next;
            continue;
        }

        // Resolve every key column to the write order the scanned rows carry,
        // and to its NULL companion when the column is nullable.
        let companion = |rel: &Relation, name: &str| -> Option<u64> {
            rel.fetch_columns
                .iter()
                .find(|f| {
                    crate::core::rql::nullable::is_hidden(&f.name)
                        && f.name.len() > crate::core::rql::nullable::NULL_FLAG_PREFIX.len()
                        && f.name[crate::core::rql::nullable::NULL_FLAG_PREFIX.len()..]
                            .eq_ignore_ascii_case(name)
                        && !f.is_deleted
                })
                .map(|f| f.write_order)
        };
        let left_relation = &plan.relations[join.left_rel];
        let mut left_keys: Vec<KeyColumn> = Vec::with_capacity(join.keys.len());
        let mut right_keys: Vec<KeyColumn> = Vec::with_capacity(join.keys.len());
        let mut resolved = !join.keys.is_empty();
        for (left_name, right_name) in &join.keys {
            let l = left_relation
                .fetch_columns
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(left_name) && !f.is_deleted)
                .map(|f| {
                    let key = KeyColumn {
                        write_order: f.write_order,
                        flag: companion(left_relation, left_name),
                        document: false,
                    };
                    (key, f.db_type == DbType::JSONB)
                });
            let r = relation
                .fetch_columns
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(right_name) && !f.is_deleted)
                .map(|f| {
                    let key = KeyColumn {
                        write_order: f.write_order,
                        flag: companion(relation, right_name),
                        document: false,
                    };
                    (key, f.db_type == DbType::JSONB)
                });
            match (l, r) {
                (Some((mut l, l_doc)), Some((mut r, r_doc))) => {
                    // Matched by value only when both sides are documents;
                    // anything else keeps matching by bytes.
                    l.document = l_doc && r_doc;
                    r.document = l_doc && r_doc;
                    left_keys.push(l);
                    right_keys.push(r);
                }
                _ => {
                    resolved = false;
                    break;
                }
            }
        }

        // The `ON` clause's own terms on this relation, for the joins that
        // must still return the rows those terms reject.
        let eligible: Option<Vec<bool>> = match relation.match_condition.as_ref() {
            Some(condition) => Some(
                (0..scanned[idx].len())
                    .map(|r| {
                        scanned[idx]
                            .get(r)
                            .expect("in range")
                            .with_row(|row| {
                                crate::core::rql::expr::eval_cond(
                                    &condition.cond,
                                    row,
                                    &condition.schema,
                                )
                            })?
                    })
                    .collect::<Result<_, _>>()?,
            ),
            None => None,
        };

        // A presence-only probe is exact for a LEFT join whose relation must
        // end up unmatched, as long as nothing after it can resurrect a tuple
        // the anti-join would drop: a later RIGHT/FULL join marks its rows
        // matched by the tuples it sees, so dropping one early would turn its
        // partner into a NULL-padded row that then *passes* `r.k IS NULL`.
        let anti = relation.require_unmatched
            && join.kind == JoinKind::Left
            && plan.relations[idx + 1..]
                .iter()
                .all(|r| r.join.as_ref().is_none_or(|j| !j.kind.nulls_earlier()));

        // `ON` terms over the earlier relations: which tuples so far may match.
        let left_ok: Option<Vec<bool>> = match relation.left_condition.as_ref() {
            Some(filter) => {
                let so_far = Joined {
                    scanned,
                    width,
                    slots: combined.clone(),
                };
                Some(apply_post_join(plan, filter, &so_far)?)
            }
            None => None,
        };

        combined = if !resolved {
            // A join key that was not fetched cannot match anything: an inner
            // join is then empty, a left join keeps its left rows, a right
            // join keeps its right rows, and a full join keeps both.
            hash_step::<&[u8]>(
                join.kind,
                idx,
                join.left_rel,
                width,
                scanned,
                &combined,
                eligible.as_deref(),
                left_ok.as_deref(),
                anti,
                |_| None,
                |_| None,
            )
        } else if left_keys.len() == 1 {
            // The common case: one key column, hashed as a bare slice.
            let (l, r) = (left_keys[0], right_keys[0]);
            hash_step(
                join.kind,
                idx,
                join.left_rel,
                width,
                scanned,
                &combined,
                eligible.as_deref(),
                left_ok.as_deref(),
                anti,
                |row| key_bytes(row, l),
                |row| key_bytes(row, r),
            )
        } else {
            hash_step(
                join.kind,
                idx,
                join.left_rel,
                width,
                scanned,
                &combined,
                eligible.as_deref(),
                left_ok.as_deref(),
                anti,
                |row| composite_key(row, &left_keys),
                |row| composite_key(row, &right_keys),
            )
        };
    }

    let mut joined = Joined {
        scanned,
        width,
        slots: combined,
    };

    // Cross-relation `WHERE` terms are answered here, for the same reason: the
    // combined row is the first thing that has columns from both sides.
    if let Some(filter) = plan.post_join.as_ref() {
        match apply_post_join(plan, filter, &joined) {
            Ok(keep) => joined.retain(|i, _| keep.get(i).copied().unwrap_or(false)),
            Err(e) => {
                // Returning no rows here made a failed predicate look like a
                // query that simply matched nothing — the caller sees success.
                log::error!("post-join predicate failed: {}", e);
                return Err(e);
            }
        }
    }

    // Anti-joins are applied here, after every relation has been matched: the
    // question "did this relation find a row" only has an answer now.
    if plan.relations.iter().any(|r| r.require_unmatched) {
        joined.retain(|_, tuple| {
            plan.relations
                .iter()
                .enumerate()
                .all(|(i, r)| !r.require_unmatched || tuple.get(i).is_none())
        });
    }

    Ok(joined)
}

/// Evaluates the post-join predicate over each combined tuple.
///
/// The tuple is flattened into one synthetic row laid out to `filter.schema`,
/// then run through the ordinary per-row evaluator — the same one a
/// single-table `WHERE` uses, so a cross-relation predicate means exactly what
/// it would mean there. A relation left unmatched by an outer join contributes
/// empty values, which is how a NULL is stored.
fn apply_post_join(
    plan: &JoinPlan,
    filter: &PostJoinFilter,
    combined: &Joined<'_>,
) -> Result<Vec<bool>, String> {
    use crate::core::processor::concurrent_processor::ConcurrentProcessor;
    use crate::core::processor::concurrent_processor::RowView;
    use crate::core::processor::transformer::TransformerProcessor;

    if combined.is_empty() {
        return Ok(Vec::new());
    }

    let schema_sv: SmallVec<[SchemaField; 20]> = filter.schema.iter().cloned().collect();
    let schema_arc = rclite::Arc::new(schema_sv);
    let (no_filter, tokens) =
        ConcurrentProcessor::prepare_query(&filter.where_clause, &schema_arc)?;
    if no_filter && filter.expr_terms.is_empty() {
        return Ok(vec![true; combined.len()]);
    }

    // Only when there is something for it to compile. Every term may have gone
    // to the expression evaluator, leaving an empty clause — and compiling an
    // empty plan fails, which `combine` used to turn into zero rows.
    let mut transformers = SmallVec::new();
    let mut intermediates = SmallVec::new();
    let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediates);
    let compiled;
    if !no_filter {
        compiled = ConcurrentProcessor::compile_query_plan(&schema_arc, &tokens)?;
        processor.load_plan(&compiled);
    }

    let mut out = Vec::with_capacity(combined.len());
    let mut bool_buffer: SmallVec<[(bool, Option<crate::core::processor::transformer::Next>); 20]> =
        SmallVec::new();

    let companions = Companions::new(plan, &filter.columns);
    for tuple in combined.iter() {
        let row = combined_row_with(plan, &filter.columns, &companions, tuple);
        let mut map: SmallVec<[u8; 32]> = SmallVec::new();
        map.resize(filter.schema.len(), u8::MAX);
        for (pos, column) in row.columns.iter().enumerate() {
            let wo = column.schema_id as usize;
            // `u8::MAX` is the absent sentinel, so a position at or past it
            // cannot be stored — truncating it read another column's bytes.
            // `build_post_join_filter` sends every term of a row that wide to
            // the expression evaluator, so the tokenizer never sees one; this
            // refuses rather than answers if that ever stops being true.
            if pos >= u8::MAX as usize {
                if !no_filter {
                    return Err(format!(
                        "a post-join predicate over {} combined columns cannot be evaluated by \
                         the predicate engine, which addresses at most {}",
                        row.columns.len(),
                        u8::MAX - 1
                    ));
                }
                break;
            }
            if wo < map.len() {
                map[wo] = pos as u8;
            }
        }

        let mut matched = if no_filter {
            true
        } else {
            processor
                .execute_row(&RowView::new(&row, &map), &mut bool_buffer)
                .map_err(|e| format!("{:?}", e))?
        };

        // Both halves must hold. The synthetic row is addressed by write order,
        // which is exactly how the evaluator looks a column up.
        if matched {
            for condition in &filter.expr_terms {
                if !crate::core::rql::expr::eval_cond(condition, &row, &filter.schema)? {
                    matched = false;
                    break;
                }
            }
        }
        out.push(matched);
    }

    Ok(out)
}

/// What projecting one tuple needs that is the same for every tuple, resolved
/// once per statement.
pub struct Projector<'p> {
    plan: &'p JoinPlan,
    /// Each fetched item's NULL companion, found once rather than by a name
    /// search per output cell.
    item_flags: Vec<Option<u64>>,
    /// An expression in the select list is evaluated against the combined row,
    /// which is the first thing that has columns from both sides — built per
    /// tuple only when something needs it.
    wants_combined: bool,
    /// Each expression item's display scale ([`JoinPlan::output_scales`]),
    /// which its NUMERIC value is rounded to, found once per statement.
    display_scales: Vec<Option<u32>>,
    /// Each expression item read exactly ([`crate::core::rql::expr::output_is_wide`]),
    /// decided once per statement.
    wide: Vec<bool>,
    layout_companions: Option<Companions>,
    /// The first expression failure, reported once by [`Projector::report`].
    expression_failure: Option<String>,
}

impl<'p> Projector<'p> {
    pub fn new(plan: &'p JoinPlan) -> Self {
        let item_flags: Vec<Option<u64>> = plan
            .items
            .iter()
            .map(|item| {
                if item.literal.is_some() || item.scalar.is_some() || item.expr.is_some() {
                    return None;
                }
                plan.relations.get(item.rel).and_then(|relation| {
                    relation
                        .fetch_columns
                        .iter()
                        .find(|f| {
                            crate::core::rql::nullable::is_hidden(&f.name)
                                && f.name.len() > crate::core::rql::nullable::NULL_FLAG_PREFIX.len()
                                && f.name[crate::core::rql::nullable::NULL_FLAG_PREFIX.len()..]
                                    .eq_ignore_ascii_case(&item.field.name)
                        })
                        .map(|flag| flag.write_order)
                })
            })
            .collect();
        Projector {
            plan,
            item_flags,
            wants_combined: plan.items.iter().any(|i| i.expr.is_some()),
            display_scales: plan
                .items
                .iter()
                .map(|i| i.expr.as_ref().and_then(crate::core::rql::expr::display_scale))
                .collect(),
            wide: plan
                .items
                .iter()
                .map(|i| i.expr.as_ref().is_some_and(crate::core::rql::expr::output_is_wide))
                .collect(),
            layout_companions: plan
                .order_layout
                .as_ref()
                .map(|layout| Companions::new(plan, &layout.columns)),
            expression_failure: None,
        }
    }

    /// One output row for one tuple. Output columns are positional: column `i`
    /// is `plan.items[i]`, with `schema_id == i`.
    pub fn row(&mut self, tuple: Tuple<'_>) -> Row {
        let plan = self.plan;
        let synthetic = match (
            self.wants_combined,
            plan.order_layout.as_ref(),
            self.layout_companions.as_ref(),
        ) {
            (true, Some(layout), Some(companions)) => {
                Some(combined_row_with(plan, &layout.columns, companions, tuple))
            }
            _ => None,
        };
        let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
        for (i, item) in plan.items.iter().enumerate() {
            // An expression fills the position it was written in, and carries
            // its own type: a NULL result has to leave as a NULL rather than as
            // the declared type's zeroes.
            if let Some(expr) = item.expr.as_ref() {
                let (bytes, column_type) = match (synthetic.as_ref(), plan.order_layout.as_ref()) {
                    (Some(row), Some(layout)) => {
                        match crate::core::rql::expr::eval_output_planned(expr, self.wide[i], row, &layout.schema) {
                            // Coerced to the type this item was *described* as.
                            // The row description is sent before any row, and a
                            // payload of a different width than the type it was
                            // announced under is read as NULL — the value is
                            // not mislabelled, it is lost. `t.b + 1` widened to
                            // `int8` while the description still said `int4`,
                            // and the column arrived empty.
                            Ok(value) => {
                                let declared = item.field.db_type.clone();
                                let coerced = if value.db_type() == Some(&declared) {
                                    Ok(value)
                                } else {
                                    crate::core::rql::expr::cast(value, &declared)
                                }
                                .and_then(|value| {
                                    crate::core::rql::expr::at_display_scale(
                                        value,
                                        self.display_scales[i],
                                    )
                                });
                                match coerced {
                                    Ok(crate::core::rql::expr::Value::Data { bytes, db_type }) => {
                                        (bytes, db_type)
                                    }
                                    Ok(crate::core::rql::expr::Value::Null) => {
                                        (Vec::new(), DbType::NULL)
                                    }
                                    Err(e) => {
                                        self.expression_failure.get_or_insert(e);
                                        (Vec::new(), DbType::NULL)
                                    }
                                }
                            }
                            Err(e) => {
                                self.expression_failure.get_or_insert(e);
                                (Vec::new(), DbType::NULL)
                            }
                        }
                    }
                    _ => (Vec::new(), DbType::NULL),
                };
                columns.push(Column {
                    schema_id: crate::core::row::row::schema_id_of(i as u64),
                    data: mb_from(&bytes),
                    column_type,
                });
                continue;
            }
            let data = if let Some(bytes) = item.literal.as_ref() {
                mb_from(bytes)
            } else if let Some(func) = item.scalar {
                // Evaluated here, inside the per-row loop, for the same reason
                // the single-table path does: a value computed once would be
                // repeated in every row.
                mb_from(&func.evaluate())
            } else {
                tuple
                    .get(item.rel)
                    .and_then(|r| r.column(item.field.write_order))
                    .map(|(bytes, _)| mb_from(bytes))
                    .unwrap_or_default()
            };

            // A NULL leaves the join as a NULL.
            //
            // The output row was built with each column's *declared* type
            // whatever the value, and a NULL is recorded by a companion column
            // rather than by the bytes — a NULL fixed-width column holds its
            // type's zeroes. So a NULL integer left the join as `0` and a NULL
            // text as `''`: not a formatting slip, a different value, with
            // nothing reporting it. `LEFT JOIN` was the worse case, because the
            // whole unmatched side arrived as zeroes rather than as NULLs, so
            // "no matching row" and "a matching row full of defaults" became
            // indistinguishable.
            //
            // Only the fetched-column branch: a literal and a scalar function
            // both produce a value by construction.
            let is_null = if item.literal.is_some() || item.scalar.is_some() {
                false
            } else {
                match tuple.get(item.rel) {
                    // The relation contributed no row at all — an outer join
                    // that matched nothing. Every column of it is NULL.
                    None => true,
                    Some(r) => self.item_flags[i]
                        .and_then(|flag| {
                            r.column(flag)
                                .map(|(b, _)| crate::core::rql::nullable::flag_is_set(b))
                        })
                        .unwrap_or(false),
                }
            };

            columns.push(Column {
                // Output columns are positional: a joined row has no single
                // schema for its ids to refer to.
                schema_id: crate::core::row::row::schema_id_of(i as u64),
                data,
                column_type: if is_null {
                    DbType::NULL
                } else {
                    item.field.db_type.clone()
                },
            });
        }
        Row {
            position: 0,
            columns,
            length: 0,
        }
    }

    /// The first expression failure, as the statement's error.
    ///
    /// It used to be logged while the column went out NULL in every row it
    /// failed for, and the statement succeeded: `UPDATE … SET s = w.a + 1
    /// FROM w …` stored that NULL, a join SELECT returned it as data. A
    /// failing expression fails the statement, as it does on one table and as
    /// an aggregate's argument does.
    pub fn finish(&self) -> Result<(), String> {
        match self.expression_failure.as_ref() {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}

fn mb_from(bytes: &[u8]) -> crate::memory_pool::MemoryBlock {
    let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
    if !bytes.is_empty() {
        mb.into_slice_mut().copy_from_slice(bytes);
    }
    mb
}

/// Projects combined tuples into output rows, then sorts and pages them.
///
/// [`project_serialized`], decoded. For the paths that must still edit rows
/// after projecting — hydrating a shredded JSON column — and for tests.
pub fn project(plan: &JoinPlan, combined: Joined<'_>) -> Result<Vec<Row>, String> {
    let mut bytes = Vec::new();
    project_serialized(plan, &combined, &mut bytes)?;
    Ok(crate::core::row::row::vec_into_rows(&bytes).expect("rows this function serialized"))
}

/// Projects combined tuples into serialized output rows appended to `out`,
/// sorted, de-duplicated and paged.
///
/// Each output row is built, serialized and dropped, so what is resident is
/// the compact serialized rows and a `u32` per row — never a `Vec<Row>` of
/// 1.5 KB elements. `ORDER BY` sorts row indices, not rows (an expression sort
/// used to move a 1584-byte `(keys, Row)` element per swap), and `DISTINCT`
/// keeps a hash table of row indices rather than a byte copy of every row.
pub fn project_serialized(plan: &JoinPlan, combined: &Joined<'_>, out: &mut Vec<u8>) -> Result<(), String> {
    let mut projector = Projector::new(plan);

    // A key addresses a projected column by position, so it is resolved once
    // here rather than per comparison.
    let sort_keys: Vec<(usize, bool, DbType)> = plan
        .order_by
        .iter()
        .filter_map(|(rel, field, descending)| {
            let idx = plan.items.iter().position(|i| {
                i.reads_column() && i.rel == *rel && i.field.write_order == field.write_order
            })?;
            Some((idx, *descending, field.db_type.clone()))
        })
        .collect();
    let expression_sort = !plan.order_exprs.is_empty() && plan.order_layout.is_some();
    let reorders = expression_sort
        || (plan.order_exprs.is_empty() && !sort_keys.is_empty())
        || plan.distinct;

    // Nothing reorders or merges rows: straight into the result, projecting
    // only the page `OFFSET`/`LIMIT` keep.
    if !reorders {
        let skip = plan.offset.map_or(0, |o| o as usize);
        let take = plan.limit.map_or(usize::MAX, |l| l as usize);
        for tuple in combined.iter().skip(skip).take(take) {
            projector.row(tuple).extend_serialized(out);
        }
        return projector.finish();
    }

    let mut rows: Vec<u8> = Vec::with_capacity(combined.len() * 32);
    let mut starts: Vec<usize> = Vec::with_capacity(combined.len());
    for tuple in combined.iter() {
        starts.push(rows.len());
        projector.row(tuple).extend_serialized(&mut rows);
    }
    projector.finish()?;
    let row_at = |i: u32| -> &[u8] {
        let i = i as usize;
        let end = starts.get(i + 1).copied().unwrap_or(rows.len());
        &rows[starts[i]..end]
    };

    // Sorting runs here rather than in the scan, because the order is over the
    // combined rows and a per-relation sort would not survive the join. Both
    // sorts are stable over the scan order, as the row sorts they replace were.
    let mut order: Vec<u32> = (0..starts.len() as u32).collect();
    if !plan.order_exprs.is_empty() {
        if let Some(layout) = plan.order_layout.as_ref() {
            let directions: Vec<(bool, Option<bool>)> = plan
                .order_exprs
                .iter()
                .map(|(_, descending, nulls)| (*descending, *nulls))
                .collect();
            // Evaluated once per row up front: a comparator must not fail, and
            // re-evaluating inside one would be quadratic anyway.
            let companions = Companions::new(plan, &layout.columns);
            let mut failure: Option<String> = None;
            let mut keys: Vec<Vec<crate::core::rql::expr::Value>> = Vec::with_capacity(order.len());
            let wide: Vec<bool> =
                plan.order_exprs.iter().map(|(e, _, _)| crate::core::rql::expr::output_is_wide(e)).collect();
            for index in 0..combined.len() {
                let synthetic =
                    combined_row_with(plan, &layout.columns, &companions, combined.tuple(index));
                let mut values = Vec::with_capacity(plan.order_exprs.len());
                for ((expr, _, _), wide) in plan.order_exprs.iter().zip(&wide) {
                    match crate::core::rql::expr::eval_output_planned(expr, *wide, &synthetic, &layout.schema) {
                        Ok(value) => values.push(value),
                        Err(e) => {
                            // A key that cannot be computed fails the
                            // statement. It sorted as NULL and was logged, so
                            // the rows came back in an order nobody asked for.
                            failure.get_or_insert(e);
                            values.push(crate::core::rql::expr::Value::Null);
                        }
                    }
                }
                keys.push(values);
            }
            if let Some(e) = failure {
                return Err(e);
            }
            sort_or_select(&mut order, plan, |a, b| {
                crate::core::rql::expr::compare_key_values(
                    &keys[a as usize],
                    &keys[b as usize],
                    &directions,
                )
            });
        }
    } else if !sort_keys.is_empty() {
        sort_or_select(&mut order, plan, |a, b| {
            let (ra, rb) = (RowRef::Bytes(row_at(a)), RowRef::Bytes(row_at(b)));
            for (idx, descending, ty) in sort_keys.iter() {
                let ord = compare_projected_cells(
                    ra.column(*idx as u64),
                    rb.column(*idx as u64),
                    ty,
                    *descending,
                );
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // `SELECT DISTINCT`, applied to the projected columns and *before*
    // `OFFSET`/`LIMIT`: SQL evaluates it first, so `LIMIT 10` over a distinct
    // set means ten distinct rows, not however many of the first ten differed.
    //
    // Two rows are the same when their serialized forms are: every column
    // holds the same bytes and the same type (the schema ids are positions,
    // equal in every row). The type is part of it because a NULL arrives as
    // empty bytes typed `NULL`, and an empty string is empty bytes typed
    // `STRING` — telling those apart is what keeps a NULL from collapsing into
    // an empty value. The first of each group is kept, so an `ORDER BY` that
    // has already run is not disturbed.
    //
    // A document column is the exception: two rows holding `{"a":1}` and
    // `{"a":1.0}` there are the same row, so with one in the select list each
    // row is keyed by `value_key` instead, built once per row.
    if plan.distinct {
        use std::hash::BuildHasher;
        let state = ahash::RandomState::new();
        let document = |position: usize| {
            plan.items
                .get(position)
                .is_some_and(|item| item.field.db_type == DbType::JSONB)
        };
        let value_keys: Option<Vec<Vec<u8>>> = (0..plan.items.len()).any(document).then(|| {
            (0..starts.len() as u32)
                .map(|i| {
                    let row = row_at(i);
                    crate::core::row::row::value_key(row, document).unwrap_or_else(|| row.to_vec())
                })
                .collect()
        });
        let key_at = |i: u32| -> &[u8] {
            match value_keys.as_ref() {
                Some(keys) => &keys[i as usize],
                None => row_at(i),
            }
        };
        let mut seen: hashbrown::HashTable<u32> = hashbrown::HashTable::with_capacity(order.len());
        order.retain(|i| {
            let row = key_at(*i);
            let hash = state.hash_one(row);
            if seen.find(hash, |j| key_at(*j) == row).is_some() {
                return false;
            }
            seen.insert_unique(hash, *i, |j| state.hash_one(key_at(*j)));
            true
        });
    }

    let skip = plan.offset.map_or(0, |o| o as usize);
    let take = plan.limit.map_or(usize::MAX, |l| l as usize);
    for i in order.into_iter().skip(skip).take(take) {
        out.extend_from_slice(row_at(i));
    }
    Ok(())
}

/// Orders `order` by `cmp`, ties left in scan order — or, when a page without
/// DISTINCT needs only its first `OFFSET + LIMIT` rows, selects those and
/// sorts just them. Breaking ties on the row index makes the comparison total,
/// so the selection returns exactly the stable sort's prefix.
fn sort_or_select(
    order: &mut Vec<u32>,
    plan: &JoinPlan,
    cmp: impl Fn(u32, u32) -> std::cmp::Ordering,
) {
    let total = |a: &u32, b: &u32| cmp(*a, *b).then(a.cmp(b));
    let keep = match (plan.distinct, plan.limit) {
        (false, Some(limit)) => plan.offset.unwrap_or(0).saturating_add(limit) as usize,
        _ => usize::MAX,
    };
    if keep == 0 {
        order.clear();
    } else if keep < order.len() {
        order.select_nth_unstable_by(keep - 1, total);
        order.truncate(keep);
        order.sort_unstable_by(total);
    } else {
        order.sort_unstable_by(total);
    }
}

/// Folds the joined tuples straight into the aggregate, one projected row at a
/// time — no `Vec<Row>` of the projection is built first.
pub fn fold_joined(
    plan: &JoinPlan,
    agg: &crate::core::rql::aggregate::AggregatePlan,
    combined: &Joined<'_>,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    let mut projector = Projector::new(plan);
    let mut folder = crate::core::rql::aggregate::Folder::new(agg);
    for tuple in combined.iter() {
        folder.push(&projector.row(tuple))?;
    }
    projector.finish()?;
    folder.finish_into(out)
}

/// Orders two projected cells for one `ORDER BY` key, PostgreSQL's way: a
/// NULL is larger than every value, so it sorts last under `ASC` and first
/// under `DESC`. (An explicit `NULLS FIRST`/`LAST` never reaches here — the
/// planner sends any such term down the expression sort, which honours it.)
///
/// NULL-ness is read from the cell's **type**, which `project` sets from the
/// companion column or from an outer join that matched nothing. Never from
/// the bytes: a matched row's NULL integer holds `0`'s bytes, and reading
/// those sorted it as zero — tied with a real `0` — while an unmatched side,
/// holding no bytes at all, sorted *first* under `ASC`.
#[cfg(test)]
fn compare_projected(
    a: Option<&Column>,
    b: Option<&Column>,
    ty: &DbType,
    descending: bool,
) -> std::cmp::Ordering {
    compare_projected_cells(
        a.map(|c| (c.data.into_slice(), c.column_type.clone())),
        b.map(|c| (c.data.into_slice(), c.column_type.clone())),
        ty,
        descending,
    )
}

/// [`compare_projected`] over cells read in place: bytes and type.
fn compare_projected_cells(
    a: Option<(&[u8], DbType)>,
    b: Option<(&[u8], DbType)>,
    ty: &DbType,
    descending: bool,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let is_null = |c: &Option<(&[u8], DbType)>| c.as_ref().is_none_or(|(_, t)| *t == DbType::NULL);
    let ord = match (is_null(&a), is_null(&b)) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => compare_by_type(
            a.map(|(bytes, _)| bytes).unwrap_or(&[]),
            b.map(|(bytes, _)| bytes).unwrap_or(&[]),
            ty,
        ),
    };
    if descending { ord.reverse() } else { ord }
}

fn compare_by_type(a: &[u8], b: &[u8], ty: &DbType) -> std::cmp::Ordering {
    if a.is_empty() || b.is_empty() {
        // Not a NULL — `compare_projected` has already taken those out — but
        // an empty value, which only text has: `''` sorts before any other
        // string, as its bytes do.
        return a.len().cmp(&b.len());
    }
    // The same comparator the single-table sort uses, so `ORDER BY` means the
    // same thing with and without a join. Note the evaluator's `Less` panics on
    // STRING, so it cannot be used here.
    crate::core::processor::concurrent_processor::compare_bytes_as_type(ty.clone(), a, b)
}

/// A relation reference as written in the `FROM` clause, before its schema is
/// known.
#[derive(Debug, Clone)]
pub struct RelationRef {
    pub table: String,
    pub label: String,
    pub join: Option<RawJoin>,
}

#[derive(Debug, Clone)]
pub struct RawJoin {
    pub kind: JoinKind,
    /// `ON` terms beyond the join equality, as written.
    ///
    /// Each must reference only the relation being joined, and is applied as a
    /// filter on its scan. That is exact for both join kinds — an `ON` term is
    /// part of what "matches", so a left row whose partner fails it is simply
    /// unmatched and still survives with NULLs. A `WHERE` term is *not* the
    /// same thing and must not be pushed down this way; see `anti_join_column`.
    pub extra: Vec<String>,
    /// `left.column = right.column`, as written, each side qualified.
    pub left_qualified: String,
    pub right_qualified: String,
}

/// Parses the `FROM` clause into its relations and join conditions.
///
/// `text` with its last `n` whitespace-separated words removed.
fn trim_last_words(text: &str, n: usize) -> &str {
    let mut end = text.trim_end().len();
    for _ in 0..n {
        let head = text[..end].trim_end();
        match head.rfind(char::is_whitespace) {
            Some(at) => end = at,
            None => return "",
        }
    }
    text[..end].trim_end()
}

/// Position of the first top-level comma in a `FROM` clause, if any.
fn find_top_level_comma(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    for (i, c) in bytes.iter().enumerate() {
        match quote {
            Some(q) => {
                if *c == q {
                    quote = None;
                }
            }
            None => match *c {
                b'\'' | b'"' => quote = Some(*c),
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                b',' if depth == 0 => return Some(i),
                _ => {}
            },
        }
    }
    None
}

/// Splits a `FROM` clause on top-level commas.
pub(crate) fn split_top_level_commas(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut rest = text;
    while let Some(at) = find_top_level_comma(rest) {
        parts.push(rest[..at].trim().to_string());
        rest = &rest[at + 1..];
    }
    parts.push(rest.trim().to_string());
    parts
}

/// Accepts `FROM a [AS] x JOIN b [AS] y ON x.c = y.d [JOIN …]`, with `INNER`,
/// `LEFT [OUTER]`, `RIGHT [OUTER]`, `FULL [OUTER]` and `CROSS` joins.
pub fn parse_from_clause(from_clause: &str) -> Result<Vec<RelationRef>, String> {

    // `FROM a, b` is a cross join written the old way. Each top-level comma
    // starts a new relation that pairs with everything before it; the parts
    // themselves may still contain explicit JOINs, so each is parsed normally
    // and only the *first* relation of each part carries the cross.
    if find_top_level_comma(from_clause).is_some() {
        let mut out: Vec<RelationRef> = Vec::new();
        for (index, part) in split_top_level_commas(from_clause).into_iter().enumerate() {
            let mut refs = parse_from_clause(&part)?;
            if index > 0 {
                if let Some(first) = refs.first_mut() {
                    first.join = Some(RawJoin {
                        kind: JoinKind::Cross,
                        extra: Vec::new(),
                        left_qualified: String::new(),
                        right_qualified: String::new(),
                    });
                }
            }
            out.extend(refs);
        }
        return Ok(out);
    }

    // Split on the JOIN keywords, keeping which kind introduced each segment.
    let mut segments: Vec<(Option<JoinKind>, String)> = Vec::new();
    let mut cursor = 0usize;
    let mut pending_kind: Option<JoinKind> = None;

    while cursor < from_clause.len() {
        // Whole-word, so `users u\nJOIN domain d` splits like the single-line
        // form. Matching `" JOIN "` literally left the whole clause as one
        // relation, which then reported "the first relation cannot have an ON
        // clause".
        let next = find_keyword(&from_clause[cursor..], "JOIN", 0);
        match next {
            None => {
                segments.push((pending_kind, from_clause[cursor..].trim().to_string()));
                break;
            }
            Some(rel) => {
                let at = cursor + rel;
                // The words immediately before JOIN say what kind it is, and
                // belong to the keyword rather than to the relation.
                let head = from_clause[cursor..at].trim_end();
                let head_upper = head.to_ascii_uppercase();
                // The trailing word says what kind of join it is; it may be
                // separated by any whitespace, including a newline.
                let last_word = head_upper.split_whitespace().last().unwrap_or("");
                let second_last = {
                    let mut it = head_upper.split_whitespace().rev();
                    it.next();
                    it.next().unwrap_or("")
                };
                let (relation_text, kind) = if last_word == "OUTER" && second_last == "LEFT" {
                    (trim_last_words(head, 2), JoinKind::Left)
                } else if last_word == "LEFT" {
                    (trim_last_words(head, 1), JoinKind::Left)
                } else if last_word == "OUTER" && second_last == "RIGHT" {
                    (trim_last_words(head, 2), JoinKind::Right)
                } else if last_word == "RIGHT" {
                    (trim_last_words(head, 1), JoinKind::Right)
                } else if last_word == "OUTER" && second_last == "FULL" {
                    (trim_last_words(head, 2), JoinKind::Full)
                } else if last_word == "FULL" {
                    (trim_last_words(head, 1), JoinKind::Full)
                } else if last_word == "INNER" {
                    (trim_last_words(head, 1), JoinKind::Inner)
                } else if last_word == "CROSS" {
                    (trim_last_words(head, 1), JoinKind::Cross)
                } else {
                    (head, JoinKind::Inner)
                };

                segments.push((pending_kind, relation_text.trim().to_string()));
                pending_kind = Some(kind);
                cursor = at + "JOIN".len();
            }
        }
    }

    let mut out: Vec<RelationRef> = Vec::new();
    for (kind, segment) in segments {
        let (relation_text, on_text) = match find_keyword(&segment, "ON", 0) {
            Some(i) => (
                &segment[..i],
                Some(segment[i + "ON".len()..].trim().to_string()),
            ),
            None => (segment.as_str(), None),
        };

        let (table, label) = split_relation_alias(relation_text)?;

        let join = match (kind, on_text) {
            (None, None) => None,
            (None, Some(_)) => {
                return Err("the first relation cannot have an ON clause".to_string());
            }
            (Some(JoinKind::Cross), None) => Some(RawJoin {
                kind: JoinKind::Cross,
                extra: Vec::new(),
                left_qualified: String::new(),
                right_qualified: String::new(),
            }),
            (Some(_), None) => {
                return Err(format!("JOIN of '{}' is missing its ON condition", table));
            }
            (Some(kind), Some(on)) => {
                let (l, r, extra) = split_on_condition(&on)?;
                Some(RawJoin {
                    kind,
                    extra,
                    left_qualified: l,
                    right_qualified: r,
                })
            }
        };

        out.push(RelationRef { table, label, join });
    }

    if out.is_empty() {
        return Err("FROM clause names no relation".to_string());
    }
    Ok(out)
}

fn split_relation_alias(text: &str) -> Result<(String, String), String> {
    let mut parts = text.split_whitespace();
    let table = parts
        .next()
        .ok_or_else(|| "FROM clause names no relation".to_string())?
        .trim_matches('"')
        .to_string();

    let alias = match parts.next() {
        None => None,
        Some(w) if w.eq_ignore_ascii_case("AS") => parts.next().map(|a| a.trim_matches('"')),
        Some(w) => Some(w.trim_matches('"')),
    };

    let label = alias.map(str::to_string).unwrap_or_else(|| table.clone());
    Ok((table, label))
}

/// Splits `a.x = b.y` into its two qualified sides.
/// Splits an `ON` clause into the join equality and any extra filter terms.
///
/// `ON a.x = b.y AND b.z = 5` is the shape an ORM emits for a conditional join.
/// The equality between two relations is the hash key; every other term becomes
/// a filter on the relation being joined.
fn split_on_condition(on: &str) -> Result<(String, String, Vec<String>), String> {
    if on.to_ascii_uppercase().contains(" OR ") {
        return Err(format!(
            "join condition '{}' uses OR, which is not supported yet: only terms combined \
             with AND can be applied as a filter on the joined relation",
            on.trim()
        ));
    }

    let mut equality: Option<(String, String)> = None;
    let mut extra: Vec<String> = Vec::new();

    for term in split_conjuncts(on) {
        let term = term.trim().to_string();
        if term.is_empty() {
            continue;
        }
        // The join key is the one equality whose sides are both qualified
        // column references; anything else filters a single relation.
        let is_key = match term.find('=') {
            Some(eq) => {
                let (lq, _) = split_qualified(term[..eq].trim());
                let rhs = term[eq + 1..].trim();
                let (rq, _) = split_qualified(rhs);
                // A *string* literal, not merely a quoted thing. In PostgreSQL
                // single quotes delimit strings and double quotes delimit
                // identifiers, so `"domain"."id"` is a qualified column and
                // treating it as a literal broke every sea-orm join — the ORM
                // quotes every identifier it emits. An unqualified `"Alice"`
                // still fails the `rq.is_some()` test below and lands in
                // `extra`, so dropping the double quote here costs nothing.
                let rhs_is_literal =
                    rhs.starts_with('\'') || rhs.chars().next().is_some_and(|c| c.is_ascii_digit());
                lq.is_some() && rq.is_some() && !rhs_is_literal
            }
            None => false,
        };

        if is_key && equality.is_none() {
            let eq = term.find('=').expect("checked above");
            let left = term[..eq].trim().to_string();
            let right = term[eq + 1..].trim().to_string();
            if left.is_empty() || right.is_empty() {
                return Err(format!("join condition '{}' is incomplete", term));
            }
            equality = Some((left, right));
        } else {
            extra.push(term);
        }
    }

    // No column-to-column equality is not fatal. `ON c.relname = 'x' || f(t.id)`
    // is a perfectly good inner join; it simply has no hash key, and an inner
    // join on a predicate is exactly a cross join filtered by that predicate.
    // The caller decides — for an outer join the two are *not* equivalent, so
    // it refuses there.
    let Some((left, right)) = equality else {
        return Ok((String::new(), String::new(), extra));
    };
    Ok((left, right, extra))
}

/// What a `WHERE` conjunct on the null-supplying side of an outer join means.
enum AntiJoin {
    /// `r.col IS NULL` where `col` cannot be null in a matched row, so the
    /// test is exactly "this relation found no match".
    Unmatched,
    /// Anything else — refused, because evaluating it before the join gives a
    /// different answer and there is no evaluator over combined rows.
    NotApplicable(String),
}

/// Classifies a conjunct that references only relation `rel`, which sits on the
/// null-supplying side of an outer join.
fn anti_join_column(
    conjunct: &str,
    rel: usize,
    relations: &[RelationRef],
    schemas: &[SmallVec<[SchemaField; 20]>],
    join: Option<&JoinedRelation>,
) -> AntiJoin {
    let trimmed = conjunct.trim();
    let upper = trimmed.to_ascii_uppercase();
    let Some(head) = upper.strip_suffix("IS NULL") else {
        return AntiJoin::NotApplicable(format!(
            "WHERE term '{}' filters '{}', which is the null-supplying side of an outer join. Only \
             '<column> IS NULL' is supported there; anything else would be applied before the \
             join and change which rows count as unmatched",
            trimmed, relations[rel].label
        ));
    };
    let column_text = trimmed[..head.trim_end().len()].trim();
    let (_, column) = split_qualified(column_text);

    // A nullable column is sound too when it is one of this relation's own
    // equi-join keys and the relation is attached by an INNER or LEFT join:
    // its rows then enter a tuple only by matching, a NULL key never matches
    // (`key_bytes`), so a present row's key is never NULL and `IS NULL` is
    // exactly "found no match". Not for RIGHT/FULL, which keep this relation's
    // unmatched rows — NULL keys included — present in the tuple.
    let own_key = join.is_some_and(|j| {
        matches!(j.kind, JoinKind::Left | JoinKind::Inner)
            && j.keys.iter().any(|(_, right)| right.eq_ignore_ascii_case(&column))
    });
    if own_key {
        return AntiJoin::Unmatched;
    }

    // Otherwise only sound when a matched row cannot itself hold NULL there. A
    // nullable column would make `IS NULL` true for a row that *did* match,
    // which is not the same question.
    match crate::core::rql::nullable::flag_field(&column, &schemas[rel]) {
        Some(_) => AntiJoin::NotApplicable(format!(
            "WHERE term '{}' tests a nullable column on the null-supplying side of an outer join, where \
             IS NULL cannot distinguish 'no matching row' from 'matched with a NULL value'",
            trimmed
        )),
        None => AntiJoin::Unmatched,
    }
}

/// True when a term is built only from literals and operators, naming nothing.
///
/// Used to tell `1 = 1` — which is meaningful and constant — from `nmae = 1`,
/// where the identifier is a typo that must still be reported.
fn is_constant_term(term: &str) -> bool {
    // Words that are operators or literals rather than references. A term made
    // only of literals and these is constant: `'' ILIKE '%x%'` is false for
    // every row, exactly as `1 = 0` is.
    //
    // Checked against a list rather than by "does it look like a column",
    // because the point of this function is that a *misspelled* column must not
    // be waved through as a constant — the error it raises is what tells an
    // operator they typed a name wrong.
    const OPERATOR_WORDS: &[&str] = &[
        "and",
        "or",
        "not",
        "is",
        "null",
        "true",
        "false",
        "unknown",
        "like",
        "ilike",
        "similar",
        "to",
        "between",
        "in",
        "escape",
        "collate",
        "distinct",
        "from",
        "contains",
        "icontains",
        "startswith",
        "istartswith",
        "endswith",
        "iendswith",
        "iequals",
        "any",
        "all",
    ];

    let mut quote: Option<char> = None;
    let mut word = String::new();
    let mut ok = true;

    let finish_word = |word: &mut String, ok: &mut bool| {
        if !word.is_empty() {
            // A numeric literal is a value, not a reference —  is the
            // constant this function was originally written for.
            let is_number = word.parse::<f64>().is_ok();
            if !is_number && !OPERATOR_WORDS.iter().any(|k| k.eq_ignore_ascii_case(word)) {
                *ok = false;
            }
            word.clear();
        }
    };

    for c in term.chars() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => {
                if c == '\'' || c == '"' {
                    finish_word(&mut word, &mut ok);
                    quote = Some(c);
                } else if c.is_alphanumeric() || c == '_' {
                    word.push(c);
                } else {
                    finish_word(&mut word, &mut ok);
                }
            }
        }
        if !ok {
            return false;
        }
    }
    finish_word(&mut word, &mut ok);
    ok
}

/// Index of `keyword` as a whole word at or after `start`, ignoring occurrences
/// inside string literals.
///
/// Only matches at **top level**: a keyword inside parentheses belongs to a
/// subquery, so `SELECT x WHERE NOT EXISTS (SELECT 1 FROM t)` has no top-level
/// `FROM` — which is exactly the question a caller asking "does this SELECT name
/// a relation" needs answered.
///
/// Whitespace-agnostic on purpose: a newline separates as well as a space. The
/// literal `" FROM "` searches this replaces silently failed on the multi-line
/// SQL every ORM emits — `…\nFROM users u` — and a scan that finds no `FROM`
/// does not report a problem, it reports *no relations*, which surfaces
/// somewhere else entirely as "EXISTS must correlate…".
pub fn find_keyword(sql: &str, keyword: &str, start: usize) -> Option<usize> {
    // The text as uppercase, compared byte by byte in place: the same bytes
    // at the same positions (only ASCII letters change), without a copy of
    // the statement and a `memcmp` at every position. A statement's dispatch
    // calls this a dozen times or more.
    let bytes = sql.as_bytes();
    let kw = keyword.as_bytes();
    // Most calls ask for a clause the statement does not have. The scan can
    // only answer where the keyword's bytes match ignoring case, so if no such
    // place exists at all, found by `memchr` rather than byte by byte, there
    // is nothing for it to find.
    if let Some(&k0) = kw.first() {
        let rest = bytes.get(start..).unwrap_or_default();
        let anywhere = memchr::memchr2_iter(k0.to_ascii_uppercase(), k0.to_ascii_lowercase(), rest)
            .any(|p| rest.len() - p >= kw.len() && rest[p..p + kw.len()].eq_ignore_ascii_case(kw));
        if !anywhere {
            return None;
        }
    }
    let is_kw_at = |i: usize| {
        kw.first().is_none_or(|k| bytes[i].to_ascii_uppercase() == *k)
            && bytes[i..i + kw.len()].iter().zip(kw).all(|(b, k)| b.to_ascii_uppercase() == *k)
    };
    let mut quote: Option<u8> = None;
    let mut depth = 0usize;
    let mut i = start;
    while i + kw.len() <= bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => {
                if c == b'\'' || c == b'"' {
                    quote = Some(c);
                } else if c == b'(' {
                    depth += 1;
                } else if c == b')' {
                    depth = depth.saturating_sub(1);
                } else if depth == 0
                    && is_kw_at(i)
                    && (i == 0
                        || bytes[i - 1].is_ascii_whitespace()
                        || bytes[i - 1] == b')'
                        || bytes[i - 1] == b',')
                    && bytes
                        .get(i + kw.len())
                        .is_none_or(|n| n.is_ascii_whitespace() || *n == b'(')
                {
                    return Some(i);
                }
            }
        }
        i += 1;
    }
    None
}

/// Finds a multi-word clause keyword, allowing any whitespace between the
/// words.
///
/// `find_keyword` takes its needle literally, so `"ORDER BY"` only matches when
/// exactly one space separates the two words. An ORM that wraps a line between
/// them would slip past it.
pub fn find_phrase(sql: &str, words: &[&str], start: usize) -> Option<usize> {
    let mut from = start;
    loop {
        let at = find_keyword(sql, words[0], from)?;
        let mut cursor = at + words[0].len();
        let mut matched = true;
        for word in &words[1..] {
            let rest = &sql[cursor..];
            let trimmed = rest.trim_start();
            if trimmed.len() == rest.len() {
                // No whitespace between the words, so this is not the phrase.
                matched = false;
                break;
            }
            if !trimmed
                .get(..word.len())
                .is_some_and(|w| w.eq_ignore_ascii_case(word))
            {
                matched = false;
                break;
            }
            cursor += (rest.len() - trimmed.len()) + word.len();
        }
        if matched {
            return Some(at);
        }
        from = at + words[0].len();
    }
}

/// A `SELECT … UNION [ALL] SELECT …` taken apart: its branches, which of them
/// are combined without duplicates, and the clauses that apply to the
/// combined rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnionParts {
    /// Each branch as a plain `SELECT`, the parentheses around it removed.
    pub branches: Vec<String>,
    /// How many leading branches are combined with duplicates removed.
    ///
    /// Set operators group from the left, so `A UNION B UNION ALL C` removes
    /// duplicates from `A` and `B` together and keeps every row of `C`, while
    /// `A UNION ALL B UNION C` removes them from all three. Either way the
    /// distinct part is a prefix: everything up to the branch after the last
    /// bare `UNION`. Zero when every operator is `UNION ALL`.
    pub distinct_prefix: usize,
    /// The text after a trailing `ORDER BY`, which sorts the combined rows.
    pub order_by: Option<String>,
    pub limit: Option<u64>,
    pub offset: u64,
}

/// Splits `SELECT … UNION [ALL] SELECT …` into its branches, with any trailing
/// top-level `ORDER BY`, `LIMIT` and `OFFSET` lifted off the last branch,
/// because PostgreSQL applies them to the combined result. A branch that sorts
/// or limits its own rows has to be parenthesised, and its clauses stay inside
/// it.
///
/// Returns `None` when there is no top-level `UNION`, and `Some(Err)` when the
/// trailing clauses cannot be read: leaving an unreadable `LIMIT` on the last
/// branch would limit that branch alone, an answer rather than an error.
///
/// Before this existed the parser simply stopped at `UNION` and answered with
/// the first branch alone — a well-formed query silently returning a subset.
/// And until the trailing `ORDER BY` was lifted off too, it sorted only the
/// last branch, and the `LIMIT` behind it was not found.
pub fn split_union(sql: &str) -> Option<Result<UnionParts, String>> {
    let (branches, distinct_prefix, tail) = union_pieces(sql)?;
    // A row lock on any branch, or on the whole, locks rows a union's output
    // does not name: PostgreSQL refuses it, and so does this. The last
    // branch's clause used to lock that branch's rows alone.
    for text in branches.iter().map(String::as_str).chain(std::iter::once(tail.as_str())) {
        if let Ok((_, Some(lock))) = crate::core::rql::row_lock::split_locking_clause(text) {
            return Some(Err(lock.not_allowed_with(crate::core::rql::row_lock::SET_OPERATIONS)));
        }
    }
    let mut parts = UnionParts {
        branches,
        distinct_prefix,
        order_by: None,
        limit: None,
        offset: 0,
    };
    if let Err(e) = read_union_tail(&tail, &mut parts) {
        return Some(Err(e));
    }
    Some(Ok(parts))
}

/// The branches of a `UNION`, as [`split_union`] takes them apart, whether or
/// not its trailing clauses can be read.
///
/// What a column's type depends on. A prepared `… LIMIT $1` is described with
/// a stand-in, `LIMIT NULL`, that `split_union` refuses, and then executed
/// with a whole number that it accepts; a description that read the branches
/// only from a split that succeeded said `int` for a column the rows were
/// sent as `bigint`.
pub fn union_branches(sql: &str) -> Option<Vec<String>> {
    union_pieces(sql).map(|(branches, _, _)| branches)
}

/// The branches, the length of the distinct prefix, and the trailing clauses
/// lifted off the last branch, still unread.
fn union_pieces(sql: &str) -> Option<(Vec<String>, usize, String)> {
    let mut branches: Vec<String> = Vec::new();
    let mut last_distinct: Option<usize> = None;
    let mut rest = sql;

    loop {
        let Some(at) = find_keyword(rest, "UNION", 0) else {
            branches.push(rest.trim().to_string());
            break;
        };
        branches.push(rest[..at].trim().to_string());
        let after = rest[at + "UNION".len()..].trim_start();
        // `ALL` keeps duplicates; its absence means distinct.
        rest = match find_keyword(after, "ALL", 0) {
            Some(0) => &after["ALL".len()..],
            _ => {
                last_distinct = Some(branches.len());
                after
            }
        };
    }

    if branches.len() < 2 {
        return None;
    }

    let last = branches.pop().unwrap_or_default();
    let tail_at = [
        find_phrase(&last, &["ORDER", "BY"], 0),
        find_keyword(&last, "LIMIT", 0),
        find_keyword(&last, "OFFSET", 0),
        find_fetch(&last),
    ]
    .into_iter()
    .flatten()
    .min();
    let (body, tail) = match tail_at {
        Some(at) => (last[..at].trim(), last[at..].trim()),
        None => (last.trim(), ""),
    };
    branches.push(body.to_string());
    let branches = branches.iter().map(|b| unwrap_parentheses(b).to_string()).collect();
    Some((branches, last_distinct.map_or(0, |at| at + 1), tail.to_string()))
}

/// Where `FETCH FIRST` or `FETCH NEXT` opens at the top level of `text`, if
/// it does: the SQL:2008 spelling of `LIMIT`.
fn find_fetch(text: &str) -> Option<usize> {
    let at = find_keyword(text, "FETCH", 0)?;
    let next = text[at + "FETCH".len()..].split_whitespace().next()?;
    (next.eq_ignore_ascii_case("FIRST") || next.eq_ignore_ascii_case("NEXT")).then_some(at)
}

/// Reads `ORDER BY …` and then the page — `LIMIT n`, `OFFSET n`, or the
/// standard's `OFFSET n ROWS FETCH FIRST n ROWS ONLY` — any of them absent.
fn read_union_tail(tail: &str, parts: &mut UnionParts) -> Result<(), String> {
    let limit_at = find_keyword(tail, "LIMIT", 0);
    let offset_at = find_keyword(tail, "OFFSET", 0);
    let order_end = limit_at
        .into_iter()
        .chain(offset_at)
        .chain(find_fetch(tail))
        .min()
        .unwrap_or(tail.len());
    let unexpected = || format!("unexpected '{}' after the last branch of a UNION", tail);
    match find_phrase(tail, &["ORDER", "BY"], 0) {
        Some(0) => {
            let clause = tail["ORDER".len()..order_end].trim_start();
            parts.order_by = Some(clause["BY".len()..].trim().to_string());
        }
        // An `ORDER BY` behind the `LIMIT` is not PostgreSQL's grammar.
        Some(_) => return Err(unexpected()),
        None if order_end != 0 => return Err(unexpected()),
        None => {}
    }
    // The page is read by the reader a single statement's goes through, so
    // `FETCH FIRST 2 ROWS ONLY` and `OFFSET 2 ROWS` mean here what they mean
    // there. Read only as `LIMIT`/`OFFSET`, a `FETCH` was taken into the
    // `ORDER BY` and refused there, or with no `ORDER BY` left on the last
    // branch to limit that branch alone. Anything the reader does not take —
    // a bound parameter, `WITH TIES`, a clause after the page — is refused:
    // dropping it would answer a different question.
    let (limit, offset) =
        crate::core::tokenizer::pagination::read_page_clause(&tail[order_end..]).map_err(|e| {
            format!("the page after the last branch of a UNION cannot be read: {}", e)
        })?;
    parts.limit = limit;
    parts.offset = offset.unwrap_or(0);
    Ok(())
}

/// A branch without the parentheses that wrap all of it: `(SELECT … LIMIT 2)`
/// is the branch `SELECT … LIMIT 2`. Parentheses that close before the end,
/// as in `(a) + (b)`, are not one pair around the whole and are kept.
fn unwrap_parentheses(branch: &str) -> &str {
    let mut text = branch.trim();
    while text.starts_with('(') && text.ends_with(')') {
        let bytes = text.as_bytes();
        let mut depth = 0usize;
        let mut quote: Option<u8> = None;
        let mut closes_at = None;
        for (i, &c) in bytes.iter().enumerate() {
            match quote {
                Some(q) if c == q => quote = None,
                Some(_) => {}
                None if c == b'\'' || c == b'"' => quote = Some(c),
                None if c == b'(' => depth += 1,
                None if c == b')' => {
                    depth -= 1;
                    if depth == 0 {
                        closes_at = Some(i);
                        break;
                    }
                }
                None => {}
            }
        }
        if closes_at != Some(bytes.len() - 1) {
            break;
        }
        text = text[1..text.len() - 1].trim();
    }
    text
}

/// Splits a qualified name into `(qualifier, column)`.
pub fn split_qualified(name: &str) -> (Option<String>, String) {
    let cleaned = name.trim().trim_matches('"');
    match cleaned.rsplit_once('.') {
        Some((q, c)) => (
            Some(q.trim_matches('"').to_string()),
            c.trim_matches('"').to_string(),
        ),
        None => (None, cleaned.to_string()),
    }
}

/// Splits a `WHERE` clause into top-level `AND` conjuncts, respecting quotes and
/// parentheses.
pub fn split_conjuncts(where_clause: &str) -> Vec<String> {
    let bytes = where_clause.as_bytes();
    let upper = where_clause.to_ascii_uppercase();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut i = 0usize;
    let mut quote: Option<u8> = None;

    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
                i += 1;
            }
            None => match c {
                b'\'' | b'"' => {
                    quote = Some(c);
                    i += 1;
                }
                b'(' => {
                    depth += 1;
                    i += 1;
                }
                b')' => {
                    depth = depth.saturating_sub(1);
                    i += 1;
                }
                _ if depth == 0 && upper.as_bytes()[i..].starts_with(b" AND ") && i > start => {
                    out.push(where_clause[start..i].trim().to_string());
                    i += " AND ".len();
                    start = i;
                }
                _ => i += 1,
            },
        }
    }

    let tail = where_clause[start..].trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    out
}

/// Rewrites `pg_column_size(<relation>)` into that relation's stored row width.
///
/// A row here is fixed-width, so the size of one is known from the schema
/// without reading anything. `SUM(pg_column_size(t))` over rows is therefore
/// `COUNT(*) * <width>`, which the aggregate path can answer directly.
///
/// Only the whole-relation form is rewritten. `pg_column_size(<column>)` names
/// a value, and the expression evaluator measures that itself.
fn rewrite_relation_size(
    item: &str,
    relations: &[RelationRef],
    schemas: &[SmallVec<[SchemaField; 20]>],
) -> String {
    const FUNC: &str = "pg_column_size";
    let lower = item.to_ascii_lowercase();
    let Some(at) = lower.find(FUNC) else {
        return item.to_string();
    };
    let after = &item[at + FUNC.len()..];
    let Some(open) = after.find('(') else {
        return item.to_string();
    };
    if !after[..open].trim().is_empty() {
        return item.to_string();
    }
    let Some(close) = after[open..].find(')').map(|c| open + c) else {
        return item.to_string();
    };
    let arg = after[open + 1..close].trim().trim_matches('"');

    // Qualified, so it names a column of a relation rather than the relation.
    if arg.contains('.') {
        return item.to_string();
    }

    let Some(idx) = relations
        .iter()
        .position(|r| r.label.eq_ignore_ascii_case(arg) || r.table.eq_ignore_ascii_case(arg))
    else {
        return item.to_string();
    };

    let width: u64 = schemas[idx]
        .iter()
        .filter(|f| !f.is_deleted && !crate::core::rql::nullable::is_hidden(&f.name))
        .map(|f| f.size as u64)
        .sum();

    let before = &item[..at];
    let rest = &after[close + 1..];

    let trimmed_before = before.trim_end();
    if trimmed_before.to_ascii_uppercase().ends_with("SUM(") {
        let head = &trimmed_before[..trimmed_before.len() - "SUM(".len()];
        let tail = rest.strip_prefix(')').unwrap_or(rest);
        return format!("{}COUNT(*) * {}{}", head, width, tail);
    }
    format!("{}{}{}", before, width, rest)
}

/// Resolves a parsed `FROM` clause and `SELECT` list against real schemas.
///
/// `schema_of` looks a relation's schema up by table name; the caller supplies
/// it so this module does not depend on `Database`.
pub fn plan(
    relations: Vec<RelationRef>,
    select_list: &str,
    where_clause: &str,
    group_by_clause: Option<&str>,
    having_clause: Option<&str>,
    order_by: Vec<crate::core::rql::order_by::SortTerm>,
    limit: Option<u64>,
    offset: Option<u64>,
    schema_of: &dyn Fn(&str) -> Option<SmallVec<[SchemaField; 20]>>,
) -> Result<JoinPlan, String> {
    let schemas: Vec<SmallVec<[SchemaField; 20]>> = relations
        .iter()
        .map(|r| {
            schema_of(&r.table).ok_or_else(|| format!("relation '{}' does not exist", r.table))
        })
        .collect::<Result<_, _>>()?;

    // `pg_column_size(c)` names a whole *relation*, not a column, and has to be
    // answered from the row layout before anything reads the select list as
    // expressions — the projection and the aggregate stage each parse it
    // separately, and rewriting in only one left the other reporting
    // "Column 'pg_column_size(c)' not found in schema".
    let rewritten_select_list = split_top_level_commas(select_list)
        .into_iter()
        .map(|raw| rewrite_relation_size(raw.trim(), &relations, &schemas))
        .collect::<Vec<_>>()
        .join(", ");
    let select_list = rewritten_select_list.as_str();

    let index_of_label = |label: &str| -> Option<usize> {
        relations.iter().position(|r| {
            r.label.eq_ignore_ascii_case(label) || r.table.eq_ignore_ascii_case(label)
        })
    };

    // Resolves a possibly-qualified column to (relation, field). An unqualified
    // name is only accepted when exactly one relation has it — otherwise the
    // reference is genuinely ambiguous and guessing would silently read the
    // wrong table.
    let resolve = |name: &str| -> Result<(usize, SchemaField), String> {
        let (qualifier, column) = split_qualified(name);
        match qualifier {
            Some(q) => {
                let idx = index_of_label(&q)
                    .ok_or_else(|| format!("unknown relation '{}' in '{}'", q, name))?;
                let field = schemas[idx]
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(&column) && !f.is_deleted)
                    .ok_or_else(|| {
                        format!(
                            "column '{}' not found in relation '{}'",
                            column, relations[idx].label
                        )
                    })?;
                Ok((idx, field.clone()))
            }
            None => {
                let mut found: Option<(usize, SchemaField)> = None;
                for (i, s) in schemas.iter().enumerate() {
                    if let Some(f) = s
                        .iter()
                        .find(|f| f.name.eq_ignore_ascii_case(&column) && !f.is_deleted)
                    {
                        if found.is_some() {
                            return Err(format!(
                                "column '{}' is ambiguous; qualify it with a relation name",
                                column
                            ));
                        }
                        found = Some((i, f.clone()));
                    }
                }
                found.ok_or_else(|| format!("Column '{}' not found in schema", name))
            }
        }
    };

    // --- select list ---------------------------------------------------
    // `(position in `items`, the expression, its alias)` for each select-list
    // item that is not a bare column: a call that turns out not to be an
    // aggregate, or any other expression. Calls are kept apart from the
    // columns because the output column they produce may belong to the fold
    // rather than to the join.
    let mut expression_items: Vec<(usize, String, Option<String>)> = Vec::new();
    let mut items: Vec<JoinSelectItem> = Vec::new();
    // The relations a `*` item expands: every one for a bare `*`, the one it
    // names for `t.*`; `None` for any other item. A star expands to the
    // declared columns only (`visible_fields`): never a `__null_` companion,
    // never a dropped column. Each used to come out as a column of its own,
    // ten for six, and a client reading by position read the wrong values.
    let star_relations = |item: &str| -> Option<Result<Vec<usize>, String>> {
        let item = item.trim();
        if item == "*" {
            return Some(Ok((0..relations.len()).collect()));
        }
        let qualifier = item.strip_suffix(".*")?.trim().trim_matches('"');
        Some(
            index_of_label(qualifier)
                .map(|rel| vec![rel])
                .ok_or_else(|| format!("missing FROM-clause entry for table \"{}\"", qualifier)),
        )
    };
    if select_list.trim() == "*" {
        for (i, s) in schemas.iter().enumerate() {
            for f in crate::core::rql::nullable::visible_fields(s) {
                items.push(JoinSelectItem {
                    rel: i,
                    field: f.clone(),
                    label: f.name.clone(),
                    scalar: None,
                    literal: None,
                    expr: None,
                    pending_expression: false,
                });
            }
        }
    } else {
        // Split on commas the select list actually separates items with, not on
        // every comma. `COALESCE(a, NOW())` is one item; splitting inside it
        // produced the fragment `NOW())`, which came back as the baffling
        // "NOW() takes no arguments" from a statement that never called NOW
        // with an argument.
        for raw in split_top_level_commas(select_list) {
            let item = raw.trim();
            if item.is_empty() {
                continue;
            }
            if let Some(rels) = star_relations(item) {
                for rel in rels? {
                    for f in crate::core::rql::nullable::visible_fields(&schemas[rel]) {
                        items.push(JoinSelectItem {
                            rel,
                            label: f.name.clone(),
                            field: f,
                            scalar: None,
                            literal: None,
                            expr: None,
                            pending_expression: false,
                        });
                    }
                }
                continue;
            }
            if item.contains('(') {
                // A nullary scalar function is not an aggregate: it adds a
                // column without grouping anything, so it must not push the
                // statement onto the aggregate path — which would then demand
                // every other column appear in GROUP BY.
                //
                // Only when the call is the whole item: `now() IS NOT NULL`
                // was answered with `now()`, the rest of the item dropped.
                let (expr, alias) = select_entry(item);
                if let Some(open) = expr.find('(') {
                    let name = expr[..open].trim();
                    let whole_item = crate::core::rql::expr::matching_paren(&expr, open)
                        == Some(expr.len() - 1);
                    if let Some((func, db_type)) = crate::core::rql::aggregate::ScalarFunc::parse(name)
                        .filter(|_| whole_item)
                    {
                        let close = expr
                            .rfind(')')
                            .ok_or_else(|| format!("unterminated function call in '{}'", item))?;
                        if !expr[open + 1..close].trim().is_empty() {
                            return Err(format!("{}() takes no arguments", name));
                        }
                        items.push(JoinSelectItem {
                            rel: 0,
                            field: SchemaField::new(
                                name.to_ascii_lowercase(),
                                db_type.clone(),
                                db_type.get_size(),
                                0,
                                0,
                                false,
                            ),
                            label: alias.unwrap_or_else(|| name.to_ascii_lowercase()),
                            scalar: Some(func),
                            literal: None,
                            expr: None,
                            pending_expression: false,
                        });
                        continue;
                    }
                }
                // Set aside for the aggregate stage, which is where a call
                // becomes an output column when the list holds a real
                // aggregate. When it does not, the call is an ordinary
                // expression over the combined row and is filled in below.
                //
                // A placeholder goes into `items` now, at the position the item
                // was written, because that position is otherwise lost: the
                // stage only sees the calls it claimed, and appending the
                // expression afterwards would move it to the end of the select
                // list.
                // Only the position is recorded. Nothing goes into `items`
                // yet: the aggregate stage reads `items` to decide whether it
                // forms — an item not in the GROUP BY makes it decline — so a
                // placeholder put here stopped a grouped join from grouping,
                // and its aggregate came back NULL.
                //
                // `items.len()` is the index this item was written at, because
                // `items` is filled in select-list order and nothing else
                // inserts into it.
                let (expr, alias) = select_entry(item);
                expression_items.push((items.len(), expr, alias));
                continue;
            }

            // Anything but a bare column is an expression over the combined
            // row, evaluated like the calls above: an operator, a cast, a JSON
            // path, a comparison, a CASE, a constant.
            //
            // The item used to be split on whitespace and its first word
            // resolved as a column, so `a.n * 10 + b.n AS k` came back as
            // `a.n` labelled `*` — the first operand under the operator's name,
            // with nothing reporting it, and `INSERT … SELECT` stored it. A
            // constant was kept as bytes with no NULL of its own, so `NULL`
            // went out as an empty string.
            let (expr, alias) = select_entry(item);
            if !is_column_reference(&expr) {
                expression_items.push((items.len(), expr, alias));
                continue;
            }
            let (rel, field) = resolve(&expr)?;
            let label = alias.unwrap_or_else(|| field.name.clone());
            items.push(JoinSelectItem {
                rel,
                field,
                label,
                scalar: None,
                literal: None,
                expr: None,
                pending_expression: false,
            });
        }
    }
    if items.is_empty() && expression_items.is_empty() {
        return Err("SELECT list is empty".to_string());
    }

    // --- join conditions -----------------------------------------------
    let mut joins: Vec<Option<JoinedRelation>> = vec![None; relations.len()];
    let mut on_extra: Vec<Vec<String>> = vec![Vec::new(); relations.len()];
    // `ON` terms of a `RIGHT`/`FULL` join on its own relation: evaluated at
    // match time, never pushed into the scan. See [`MatchCondition`].
    let mut match_terms: Vec<Vec<String>> = vec![Vec::new(); relations.len()];
    // `ON` terms over earlier relations only; see `Relation::left_condition`.
    let mut left_terms: Vec<Vec<String>> = vec![Vec::new(); relations.len()];
    // `ON` conditions with no hash key, which become post-join predicates.
    let mut expression_joins: Vec<String> = Vec::new();
    for (i, r) in relations.iter().enumerate() {
        let Some(raw) = r.join.as_ref() else { continue };

        // A cross join has no key: every row of this relation pairs with every
        // combined row so far, so there is nothing to resolve.
        if raw.kind == JoinKind::Cross {
            joins[i] = Some(JoinedRelation {
                kind: JoinKind::Cross,
                left_rel: i.saturating_sub(1),
                keys: Vec::new(),
            });
            continue;
        }

        // An `ON` with no column-to-column equality has no hash key. For an
        // inner join that is still a valid join — `A JOIN B ON p` is exactly
        // `A CROSS JOIN B WHERE p` — so it becomes a cross join and the whole
        // condition is evaluated after pairing. For an outer join the two are
        // *not* equivalent: a left row with no match must survive with NULLs,
        // and a cross join followed by a filter would drop it. So that is
        // refused rather than answered wrongly.
        if raw.left_qualified.is_empty() && raw.right_qualified.is_empty() {
            if raw.kind != JoinKind::Inner {
                return Err(format!(
                    "an outer join on '{}' needs an equality between two columns: without \
                     one the unmatched rows it must keep cannot be identified",
                    raw.extra.join(" AND ").trim()
                ));
            }
            joins[i] = Some(JoinedRelation {
                kind: JoinKind::Cross,
                left_rel: i.saturating_sub(1),
                keys: Vec::new(),
            });
            expression_joins.extend(raw.extra.iter().cloned());
            continue;
        }

        let (a_rel, a_field) = resolve(&raw.left_qualified)?;
        let (b_rel, b_field) = resolve(&raw.right_qualified)?;

        // Whichever side names this relation is its own key; the other side
        // must name a relation already joined, so the plan can be built left to
        // right.
        let (left_rel, left_field, right_field) = if b_rel == i {
            (a_rel, a_field, b_field)
        } else if a_rel == i {
            (b_rel, b_field, a_field)
        } else {
            return Err(format!(
                "the ON condition for '{}' does not reference it",
                r.label
            ));
        };

        if left_rel >= i {
            return Err(format!(
                "'{}' is joined to a relation that appears after it",
                r.label
            ));
        }

        let mut keys = vec![(left_field.name.clone(), right_field.name.clone())];

        // Extra `ON` terms filter the relation being joined. Unlike a `WHERE`
        // term this is exact on the null-supplying side too: an `ON` term
        // decides what counts as a *match*, so a left row whose only candidate
        // fails it is unmatched and still survives with NULLs — which is
        // precisely what filtering the right scan produces.
        for term in &raw.extra {
            let referenced = relations_referenced(term, &relations, &schemas);
            match referenced.as_slice() {
                [only] if *only == i => {
                    if raw.kind.nulls_earlier() {
                        match_terms[i].push(term.clone());
                    } else {
                        on_extra[i].push(term.clone());
                    }
                }
                [] => {
                    return Err(format!(
                        "ON term '{}' references no column of '{}'",
                        term.trim(),
                        r.label
                    ));
                }
                // A term spanning this relation and the one it joins to is a
                // further *equality* of the join key, not a filter — every
                // junction table is joined on both its columns. Matching on one
                // and filtering on the other is a different join: it fans out on
                // the first and then discards, which agrees with SQL only when
                // the first column is already unique.
                //
                // Anything else spanning two relations still has to be
                // evaluated after the join, which this engine does not do.
                [a, b] if (*a == i && *b == left_rel) || (*a == left_rel && *b == i) => {
                    let malformed = || {
                        format!(
                            "ON term '{}' relates '{}' to '{}' but is not an equality between \
                             two columns, so it cannot be part of the join key and would have \
                             to be evaluated after the join",
                            term.trim(),
                            r.label,
                            relations[left_rel].label
                        )
                    };
                    let eq = term.find('=').ok_or_else(malformed)?;
                    // Not `!=`, `>=` or `<=` wearing an `=`.
                    let lhs = term[..eq].trim();
                    if lhs.ends_with(['!', '>', '<']) || term[eq + 1..].starts_with('=') {
                        return Err(malformed());
                    }
                    let (p_rel, p_field) = resolve(lhs)?;
                    let (q_rel, q_field) = resolve(term[eq + 1..].trim())?;
                    // Order the pair the same way as the primary key: left
                    // relation first, this one second.
                    if p_rel == left_rel && q_rel == i {
                        keys.push((p_field.name.clone(), q_field.name.clone()));
                    } else if q_rel == left_rel && p_rel == i {
                        keys.push((q_field.name.clone(), p_field.name.clone()));
                    } else {
                        return Err(malformed());
                    }
                }
                // Reads only relations joined before this one: it decides which
                // of the tuples so far may match, never which of this
                // relation's rows exist.
                refs if refs.iter().all(|r| *r < i) => {
                    left_terms[i].push(term.clone());
                }
                _ => {
                    return Err(format!(
                        "ON term '{}' spans more than the relation being joined, which is not \
                         supported yet: it would have to be evaluated after the join",
                        term.trim()
                    ));
                }
            }
        }

        joins[i] = Some(JoinedRelation {
            kind: raw.kind,
            left_rel,
            keys,
        });
    }

    // --- WHERE, split per relation --------------------------------------
    // Seeded with the extra `ON` terms, which are filters on their own relation
    // and are safe on the null-supplying side where a `WHERE` term is not.
    let mut predicates: Vec<Vec<String>> = on_extra;
    let mut anti_join: Vec<bool> = vec![false; relations.len()];
    let mut cross_relation: Vec<String> = expression_joins;
    for conjunct in split_conjuncts(where_clause) {
        if conjunct.trim().is_empty() {
            continue;
        }
        let referenced = relations_referenced(&conjunct, &relations, &schemas);
        match referenced.len() {
            1 => {
                let rel = referenced[0];
                // A relation on the null-supplying side of an outer join must
                // not have its `WHERE` terms pushed into its own scan: the scan
                // happens *before* the join, so filtering it changes which left
                // rows count as unmatched. The module already refuses
                // two-relation conjuncts for exactly this reason; the same
                // argument applies here and was simply not carried over.
                //
                // A `RIGHT` or `FULL` join makes every relation before it
                // null-supplying too, and a `FULL` join its own relation.
                let kinds: Vec<Option<JoinKind>> =
                    joins.iter().map(|j| j.as_ref().map(|j| j.kind)).collect();
                let null_supplying = may_be_unmatched(&kinds, rel);
                if null_supplying {
                    match anti_join_column(
                        &conjunct,
                        rel,
                        &relations,
                        &schemas,
                        joins[rel].as_ref(),
                    ) {
                        AntiJoin::Unmatched => {
                            anti_join[rel] = true;
                        }
                        AntiJoin::NotApplicable(reason) => return Err(reason),
                    }
                } else {
                    predicates[rel].push(conjunct);
                }
            }
            0 if is_constant_term(&conjunct) => {
                // A term of pure literals — `1 = 1` / `1 = 0` — references no
                // relation but is still meaningful. These arise from resolving a
                // correlated `EXISTS` whose subquery returned nothing, where the
                // answer is "true for every row" or "false for every row". The
                // scan evaluates constants, so it rides along with the first
                // relation rather than needing a plan-level short circuit.
                predicates[0].push(conjunct);
            }
            0 => {
                // Still an error for anything naming something: a misspelled
                // column must not be quietly treated as a constant.
                return Err(format!(
                    "WHERE term '{}' does not reference any column of the joined relations",
                    conjunct.trim()
                ));
            }
            _ => {
                // Spans relations, so it can only be answered once the rows are
                // combined. Pushing it into either scan would run it *before*
                // the join and change which rows pair up rather than which
                // pairs survive.
                cross_relation.push(conjunct);
            }
        }
    }

    // --- NULL join keys, dropped at the scan ----------------------------
    //
    // A NULL key matches nothing (`key_bytes`), so a row holding one can only
    // reach the output where its side is *preserved*. Where it is not, the
    // scan drops it and it is never materialised:
    //
    // * the relation an INNER or LEFT join attaches — its rows enter a tuple
    //   only by matching, so a NULL-keyed one never does;
    // * the left side of an INNER join — every tuple whose key there is NULL is
    //   dropped at that step, and a row removed from the scan yields either no
    //   tuple or a tuple with that side absent, which the step drops alike.
    //
    // Never the preserved side of an outer join (LEFT's left, RIGHT's right,
    // either side of FULL): those rows come out NULL-padded. The flag test is
    // the engine's own desugaring of `IS NOT NULL` (see `nullable`).
    let mut not_null: Vec<Vec<String>> = vec![Vec::new(); relations.len()];
    for (i, join) in joins.iter().enumerate() {
        let Some(join) = join.as_ref() else { continue };
        let sides: &[(usize, bool)] = match join.kind {
            JoinKind::Inner => &[(0, true), (1, true)],
            JoinKind::Left => &[(1, true)],
            JoinKind::Right | JoinKind::Full | JoinKind::Cross => &[],
        };
        for (side, _) in sides {
            let rel = if *side == 0 { join.left_rel } else { i };
            for (left_name, right_name) in &join.keys {
                let column = if *side == 0 { left_name } else { right_name };
                if let Some(flag) = crate::core::rql::nullable::flag_field(column, &schemas[rel]) {
                    let term = format!("{} = 0", flag.name);
                    if !not_null[rel].contains(&term) {
                        not_null[rel].push(term);
                    }
                }
            }
        }
    }
    for (rel, terms) in not_null.into_iter().enumerate() {
        predicates[rel].extend(terms);
    }

    // --- aggregate stage -------------------------------------------------
    //
    // The fold reads the *projected* join rows, whose columns are positional,
    // so each grouping key and aggregate argument is expressed as a synthetic
    // field whose `write_order` is that column's index in `items`. Building it
    // here — before the per-relation fetch is computed — means any column the
    // fold needs is added to `items` in time to be fetched.
    let mut expr_arguments: Vec<String> = Vec::new();
    let mut aggregate = build_aggregate_stage(
        select_list,
        group_by_clause,
        having_clause,
        &order_by,
        limit,
        offset,
        &mut items,
        &resolve,
        &schemas,
        &mut expr_arguments,
    )?;

    // A select item this cannot produce is refused, not dropped.
    //
    // Every item containing `(` is set aside above for the aggregate stage to
    // claim, because that is where a call becomes an output column. The stage
    // only forms when the list holds a real aggregate, so a *non*-aggregate
    // call — `COALESCE(s.a, t.b) AS eff`, or a parenthesised `(t.b + 1)` —
    // was set aside and then claimed by nobody. The statement succeeded and
    // the column was simply absent from the result, which a client meets as
    // "no column found for name: eff": an error about its own query, naming
    // nothing that would lead anyone here.
    //
    // Evaluating these over a join is a real feature — the value has to be
    // computed per combined row, the way a post-join predicate already is —
    // and until it exists, saying so is the only honest answer.
    // With a real aggregate the fold owns every call, including these, so the
    // placeholders must not also appear as projected columns.
    // With a real aggregate the fold owns every call, so the placeholders are
    // never created. Without one, each is an ordinary expression over the
    // combined row and takes the output position it was written at.
    if aggregate.is_some() {
        // The fold owns every call, so no select-list placeholder is created.
        // What may be pending instead are the aggregate's own expression
        // arguments, already in `items` at the positions their aggregates
        // point at.
        expression_items = expr_arguments
            .iter()
            .map(|text| (0usize, text.clone(), None))
            .collect();
    } else {
        for (offset, (position, expr_text, alias)) in expression_items.iter().enumerate() {
            // Named as PostgreSQL names it when there is no alias. The
            // expression's own text was used, which no client asks for by
            // name, and an ORDER BY of that text then matched the placeholder
            // as though it were a column.
            let label = alias.clone().unwrap_or_else(|| pg_column_name(expr_text));
            let at = (position + offset).min(items.len());
            items.insert(
                at,
                JoinSelectItem {
                    rel: 0,
                    field: SchemaField::new(
                        label.clone(),
                        DbType::STRING,
                        DbType::STRING.get_size(),
                        0,
                        0,
                        false,
                    ),
                    label,
                    scalar: None,
                    literal: None,
                    // Parsed once the relations' fetch lists are settled.
                    expr: None,
                    pending_expression: true,
                },
            );
        }
    }

    // With an aggregate, ordering and paging belong to the fold: they act on
    // groups, which do not exist until it has run.
    let (order_by, limit, offset) = if aggregate.is_some() {
        (Vec::new(), None, None)
    } else {
        (order_by, limit, offset)
    };

    // --- ORDER BY --------------------------------------------------------
    // `ORDER BY 2` names the second item of the select list *as written*.
    //
    // Resolved against the written list rather than against `items`: a call is
    // set aside for the aggregate stage and never reaches `items`, so counting
    // projected columns would shift every position after it and silently sort
    // by the wrong one. `SELECT *` has no written list to count, so its
    // expanded columns are the positions.
    let mut order_by = order_by;
    {
        // A star's columns are counted one by one, each named with its
        // relation: `ORDER BY 1` over `a JOIN b` must not find `id` ambiguous.
        let star_labels = |rels: Vec<usize>| -> Vec<String> {
            rels.into_iter()
                .flat_map(|rel| {
                    let label = relations[rel].label.as_str();
                    crate::core::rql::nullable::visible_fields(&schemas[rel])
                        .into_iter()
                        .map(move |f| format!("{}.{}", label, f.name))
                })
                .collect()
        };
        let mut labels: Vec<String> = Vec::new();
        for raw in split_top_level_commas(select_list) {
            match star_relations(raw.trim()) {
                Some(rels) => labels.extend(star_labels(rels?)),
                None => {
                    let (expr, alias) = select_entry(raw.trim());
                    labels.push(alias.unwrap_or(expr));
                }
            }
        }
        crate::core::rql::order_by::resolve_ordinals(&mut order_by, &labels)?;
    }

    // The fast path compares *projected* bytes, so it can only serve a key that
    // is a column already in the SELECT list and carries no NULLS placement.
    // Anything else — an expression such as `COALESCE(s.a, t.b)`, an unselected
    // column, an explicit placement — needs the combined row, and then **every**
    // term goes that way: two comparators over one ORDER BY would disagree about
    // NULL placement and return the wrong rows rather than slow ones.
    let mut order_keys: Vec<(usize, SchemaField, bool)> = Vec::with_capacity(order_by.len());
    let mut needs_expressions = false;
    for term in &order_by {
        if term.nulls_first.is_some() {
            needs_expressions = true;
            break;
        }
        if let Some(item) = items.iter().find(|i| {
            i.label.eq_ignore_ascii_case(&term.token) && i.scalar.is_none() && i.literal.is_none()
        }) {
            // An expression's alias names a value the projection computes,
            // not a column it copies, so it is sorted by evaluating the
            // expression like any other computed key.
            if !item.reads_column() {
                needs_expressions = true;
                break;
            }
            order_keys.push((item.rel, item.field.clone(), term.descending));
            continue;
        }
        match resolve(&term.token) {
            Ok((rel, field))
                if items.iter().any(|i| {
                    i.reads_column() && i.rel == rel && i.field.write_order == field.write_order
                }) =>
            {
                order_keys.push((rel, field, term.descending));
            }
            _ => {
                needs_expressions = true;
                break;
            }
        }
    }
    // Kept as written so the columns they read can be fetched below, and parsed
    // once the relations' fetch lists are settled.
    let order_expr_terms: Vec<crate::core::rql::order_by::SortTerm> = if needs_expressions {
        order_by.clone()
    } else {
        Vec::new()
    };
    let order_by = if needs_expressions {
        Vec::new()
    } else {
        order_keys
    };

    // --- what each relation must fetch -----------------------------------
    let mut out_relations: Vec<Relation> = Vec::with_capacity(relations.len());
    for (i, r) in relations.iter().enumerate() {
        let mut fetch: Vec<SchemaField> = Vec::new();
        let push = |f: &SchemaField, fetch: &mut Vec<SchemaField>| {
            if !fetch.iter().any(|e| e.write_order == f.write_order) {
                fetch.push(f.clone());
            }
        };

        // Only the items that read a column: a computed item's field is a
        // placeholder at write order 0, and fetching it put a column of the
        // wrong name and type in that relation's slot, so the real column at
        // write order 0 was never fetched.
        for item in items.iter().filter(|it| it.rel == i && it.reads_column()) {
            push(&item.field, &mut fetch);
        }
        // This relation's own join key, and the key another relation joins to
        // it on.
        // Every column of the key, not just the first: a composite key that
        // fetched one column would match on that one alone and silently return
        // the wrong rows.
        if let Some(j) = joins[i].as_ref() {
            for (_, right_name) in &j.keys {
                if let Some(f) = schemas[i]
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(right_name) && !f.is_deleted)
                {
                    push(f, &mut fetch);
                }
            }
        }
        for j in joins.iter().flatten() {
            if j.left_rel == i {
                for (left_name, _) in &j.keys {
                    if let Some(f) = schemas[i]
                        .iter()
                        .find(|f| f.name.eq_ignore_ascii_case(left_name) && !f.is_deleted)
                    {
                        push(f, &mut fetch);
                    }
                }
            }
        }
        for (rel, field, _) in order_by.iter() {
            if *rel == i {
                push(field, &mut fetch);
            }
        }
        // Columns named only by a cross-relation term still have to be read, or
        // the post-join filter would have nothing to test.
        for conjunct in &cross_relation {
            for field in schemas[i].iter() {
                if term_names_column(conjunct, &r.label, &r.table, &field.name) {
                    push(field, &mut fetch);
                }
            }
        }
        // The same for a key sorted by expression: it is evaluated against the
        // combined row, so anything it reads has to be in it — and unlike a
        // projected key, it need not appear in the SELECT list at all.
        for term in &order_expr_terms {
            for field in schemas[i].iter() {
                if term_names_column(&term.token, &r.label, &r.table, &field.name) {
                    push(field, &mut fetch);
                }
            }
        }
        // And for a select-list expression, which reads columns the projection
        // has no other reason to fetch.
        for (_, text, _) in &expression_items {
            for field in schemas[i].iter() {
                if term_names_column(text, &r.label, &r.table, &field.name) {
                    push(field, &mut fetch);
                }
            }
        }
        // And for an `ON` term over earlier relations, evaluated against the
        // tuple so far.
        for terms in &left_terms {
            for term in terms {
                for field in schemas[i].iter() {
                    if term_names_column(term, &r.label, &r.table, &field.name) {
                        push(field, &mut fetch);
                    }
                }
            }
        }
        // And for an `ON` term evaluated at match time, which reads its
        // relation's rows directly.
        for term in &match_terms[i] {
            for field in schemas[i].iter() {
                if term_names_column(term, &r.label, &r.table, &field.name) {
                    push(field, &mut fetch);
                }
            }
        }
        // Every fetched column's NULL companion comes with it.
        //
        // A NULL is not a property of a value's bytes here — a NULL
        // fixed-width column is stored as its type's zeroes, which is
        // indistinguishable from a real zero. The companion column is what
        // says which it is. Without it a post-join predicate saw every NULL as
        // a present value holding zeroes, so `COALESCE(a, b)` returned `a`
        // where `a` was NULL instead of falling through to `b`, and
        // `a IS NOT NULL` answered true for a NULL.
        //
        // Added after the loops above rather than inside them, so a companion
        // is fetched exactly once however many reasons there were to read the
        // column it tracks.
        let schema_sv: smallvec::SmallVec<[SchemaField; 20]> = schemas[i].iter().cloned().collect();
        let tracked: Vec<String> = fetch
            .iter()
            .map(|f| f.name.clone())
            .filter(|n| !crate::core::rql::nullable::is_hidden(n))
            .collect();
        for name in tracked {
            if let Some(flag) = crate::core::rql::nullable::flag_field(&name, &schema_sv) {
                push(flag, &mut fetch);
            }
        }

        // A relation with nothing to fetch still has to produce rows for the
        // join to see.
        if fetch.is_empty() {
            if let Some(f) = schemas[i].iter().min_by_key(|f| f.write_order) {
                fetch.push(f.clone());
            }
        }

        let predicate = if predicates[i].is_empty() {
            String::new()
        } else {
            let joined = predicates[i].join(" AND ");
            // The relation's scan reads this, as a single table's scan reads
            // its `WHERE`, so it gets the scan's rewrite: the operands only
            // the evaluator computes are lifted. Without it a cast on a
            // literal in arithmetic was dropped, and `a.n / 2::numeric = 1.5`
            // divided integers and matched nothing; `%`, `||` and a column
            // cast were refused.
            crate::core::tokenizer::predicate_rewrite::rewrite_scan_where(&joined, &schemas[i])?
        };

        // Read against the relation's own schema: the qualifier is stripped,
        // since the scanned rows carry the table's real write orders.
        let match_condition = if match_terms[i].is_empty() {
            None
        } else {
            let written = match_terms[i].join(" AND ");
            let mut text = written.clone();
            let mut fields: Vec<&SchemaField> = schemas[i].iter().collect();
            // Longest first, so `b.ab` is not partly rewritten by `b.a`.
            fields.sort_by_key(|f| std::cmp::Reverse(f.name.len()));
            for field in fields {
                for prefix in [&r.label, &r.table] {
                    text = replace_qualified(&text, &format!("{}.{}", prefix, field.name), &field.name);
                }
            }
            let schema: Vec<SchemaField> = schemas[i].iter().cloned().collect();
            let cond = crate::core::rql::expr::parse_cond(&text, &schema).map_err(|e| {
                format!(
                    "ON term '{}' of the outer join on '{}' could not be read: {}",
                    written.trim(),
                    r.label,
                    e
                )
            })?;
            Some(MatchCondition { cond, schema })
        };

        out_relations.push(Relation {
            table: r.table.clone(),
            label: r.label.clone(),
            join: joins[i].clone(),
            predicate,
            fetch_columns: fetch,
            require_unmatched: anti_join[i],
            match_condition,
            left_condition: None,
        });
    }
    for (i, terms) in left_terms.iter().enumerate() {
        if !terms.is_empty() {
            let filter = build_post_join_filter(terms, &out_relations)?;
            out_relations[i].left_condition = Some(filter);
        }
    }

    let post_join = if cross_relation.is_empty() {
        None
    } else {
        Some(build_post_join_filter(&cross_relation, &out_relations)?)
    };

    // Parsed here, not with the rest of the ORDER BY: the combined row's schema
    // is built from each relation's *fetch* list, which is only settled above.
    let mut order_exprs: Vec<(crate::core::rql::expr::Expr, bool, Option<bool>)> = Vec::new();
    let mut order_layout: Option<CombinedLayout> = None;
    if !order_expr_terms.is_empty() || !expression_items.is_empty() {
        let layout = combined_layout(&out_relations);

        // A select-list expression, filled into the placeholder holding its
        // position. Paired in order rather than by a recorded index: both lists
        // are in select-list order, and an index taken before the aggregate
        // stage ran no longer names the same item — removing by one of those
        // deleted a real column and returned it as NULL.
        let pending: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, item)| item.pending_expression)
            .map(|(at, _)| at)
            .collect();
        for (position, (_, text, _)) in pending.iter().zip(expression_items.iter()) {
            let mut rewritten = text.trim().to_string();
            for (from, to) in &layout.renames {
                rewritten = replace_qualified(&rewritten, from, to);
            }
            // A comparison is a boolean value here, which the expression
            // grammar does not read; it is tried only once that has failed.
            let parsed = crate::core::rql::expr::parse(&rewritten, &layout.schema)
                .or_else(|e| comparison_value(&rewritten, &layout.schema).ok_or(e))
                .map_err(|e| {
                    format!(
                        "the SELECT list item '{}' could not be read as an expression \
                         over this join: {}",
                        text.trim(),
                        e
                    )
                })?;
            // The declared type comes from the expression itself, so the row
            // description and the bytes that follow it agree.
            let db_type = crate::core::rql::expr::static_type(&parsed);
            if let Some(item) = items.get_mut(*position) {
                item.field.db_type = db_type.clone();
                item.field.size = db_type.get_size();
                item.expr = Some(parsed);
                item.pending_expression = false;
            }
            // An aggregate pointing at this position was built before the type
            // was known — `AVG` of an integer and `AVG` of a double do not
            // report the same type, and the fold reads the argument's to decide.
            if let Some(plan) = aggregate.as_mut() {
                for fold_item in plan.items.iter_mut() {
                    if let crate::core::rql::aggregate::SelectItem::Aggregate {
                        arg: Some(field),
                        ..
                    } = fold_item
                    {
                        if field.write_order as usize == *position {
                            field.db_type = db_type.clone();
                            field.size = db_type.get_size();
                        }
                    }
                }
            }
        }

        for term in &order_expr_terms {
            // A select-list label first, as the fast path resolves it — turned
            // into the qualified column it stands for so the rename below
            // reaches it like any other reference.
            let written = match items.iter().find(|i| {
                i.label.eq_ignore_ascii_case(&term.token)
                    && i.scalar.is_none()
                    && i.literal.is_none()
            }) {
                // The alias of a select-list expression sorts by that
                // expression, already parsed above.
                Some(item) if item.expr.is_some() => {
                    let expr = item.expr.clone().expect("checked");
                    order_exprs.push((expr, term.descending, term.nulls_first));
                    continue;
                }
                Some(item) => format!("{}.{}", out_relations[item.rel].label, item.field.name),
                None => term.token.clone(),
            };
            let mut rewritten = written;
            for (from, to) in &layout.renames {
                rewritten = replace_qualified(&rewritten, from, to);
            }
            let expr = crate::core::rql::expr::parse(&rewritten, &layout.schema).map_err(|e| {
                format!(
                    "ORDER BY term '{}' is not a column of this join and could not be read \
                     as an expression: {}",
                    term.token, e
                )
            })?;
            order_exprs.push((expr, term.descending, term.nulls_first));
        }
        order_layout = Some(layout);
    }

    Ok(JoinPlan {
        relations: out_relations,
        items,
        order_by,
        order_exprs,
        order_layout,
        limit,
        offset,
        aggregate,
        post_join,
        // Set by the caller that parsed the select list; `plan` is also reached
        // from paths that have no DISTINCT to report.
        distinct: false,
    })
}

/// `text` with a leading `keyword` removed, when it is there as a whole word.
///
/// Whole-word so a column named `distinctive` is not mistaken for the keyword.
fn strip_keyword_prefix<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = text.trim_start();
    if trimmed.len() < keyword.len() {
        return None;
    }
    if !trimmed[..keyword.len()].eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = &trimmed[keyword.len()..];
    match rest.chars().next() {
        None => Some(rest),
        Some(c) if c.is_whitespace() || c == '(' => Some(rest.trim_start()),
        _ => None,
    }
}

/// True when `term` names `column` of the relation labelled `label`.
///
/// Qualified (`r.col`) or bare — a bare name is taken to belong to this
/// relation, which over-fetches at worst.
fn term_names_column(term: &str, label: &str, table: &str, column: &str) -> bool {
    let upper = term.to_ascii_uppercase();
    let col = column.to_ascii_uppercase();
    for prefix in [label, table] {
        let qualified = format!("{}.{}", prefix.to_ascii_uppercase(), col);
        if upper.contains(&qualified) {
            return true;
        }
    }
    // Bare mention as a whole word.
    upper.match_indices(&col).any(|(at, _)| {
        let before_ok = at == 0
            || !upper.as_bytes()[at - 1].is_ascii_alphanumeric()
                && upper.as_bytes()[at - 1] != b'_'
                && upper.as_bytes()[at - 1] != b'.';
        let after = at + col.len();
        let after_ok = after >= upper.len()
            || !(upper.as_bytes()[after].is_ascii_alphanumeric()
                || upper.as_bytes()[after] == b'_');
        before_ok && after_ok
    })
}

/// Flattens every relation's fetched columns into one synthetic schema and
/// rewrites the cross-relation terms against it.
///
/// Names become `<label>__<column>` so two relations that both have `id` stay
/// distinct — the reason the combined row needs a schema of its own rather than
/// reusing either side's.
/// The synthetic schema a combined row is laid out to, and how to reach it.
///
/// Shared by the post-join predicate and the join's `ORDER BY` expressions:
/// both evaluate against the same flattened row, and building it twice by two
/// slightly different rules is how the two would come to disagree about which
/// column a name means.
#[derive(Debug, Clone)]
pub struct CombinedLayout {
    /// The synthetic schema, in write order.
    pub schema: Vec<SchemaField>,
    /// `columns[relation][i]` is the synthetic write order of that relation's
    /// `fetch_columns[i]`.
    pub columns: Vec<Vec<u64>>,
    /// `t.col` -> `t__col`, longest first so `a.bc` is not partly rewritten by
    /// a rule for `a.b`.
    pub renames: Vec<(String, String)>,
}

/// Flattens every relation's fetched columns into one synthetic schema.
///
/// Names become `<label>__<column>` so two relations that both have `id` stay
/// distinct — the reason the combined row needs a schema of its own rather than
/// reusing either side's.
pub fn combined_layout(relations: &[Relation]) -> CombinedLayout {
    let mut schema: Vec<SchemaField> = Vec::new();
    let mut columns: Vec<Vec<u64>> = Vec::with_capacity(relations.len());
    let mut renames: Vec<(String, String)> = Vec::new();

    for relation in relations.iter() {
        let mut mapping = Vec::with_capacity(relation.fetch_columns.len());
        for field in &relation.fetch_columns {
            let write_order = schema.len() as u64;
            // A NULL companion keeps its prefix in front, as `__null_b__v`,
            // so it is still the companion of `b__v` to every lookup that
            // derives one name from the other. Named `b____null_v`, it was the
            // companion of nothing: the tokenizer put no NULL guard on a
            // post-join term, and a NULL cell, which holds its type's zeroes,
            // compared as zero — `a.v <> b.v` kept a row where `b.v` was NULL.
            let prefix = crate::core::rql::nullable::NULL_FLAG_PREFIX;
            let name = if crate::core::rql::nullable::is_hidden(&field.name) {
                format!("{}{}__{}", prefix, relation.label, &field.name[prefix.len()..])
            } else {
                format!("{}__{}", relation.label, field.name)
            };
            renames.push((format!("{}.{}", relation.label, field.name), name.clone()));
            if !relation.table.eq_ignore_ascii_case(&relation.label) {
                renames.push((format!("{}.{}", relation.table, field.name), name.clone()));
            }
            let mut synthetic = field.clone();
            synthetic.name = name;
            synthetic.write_order = write_order;
            synthetic.offset = 0;
            schema.push(synthetic);
            mapping.push(write_order);
        }
        columns.push(mapping);
    }

    renames.sort_by_key(|(from, _)| std::cmp::Reverse(from.len()));
    CombinedLayout {
        schema,
        columns,
        renames,
    }
}

/// Flattens one combined tuple into a row laid out to [`CombinedLayout::schema`].
///
/// A relation left unmatched by an outer join contributes empty values, which
/// is how a NULL is stored.
///
/// **A NULL has to carry through as a NULL.** The evaluator reads a cell's
/// *type* to decide that, and this row is rebuilt from declared types — so the
/// companion's flag is turned into `DbType::NULL` here, or every NULL arrives
/// as a value holding zeroes and comparisons against it quietly succeed.
pub fn combined_row(plan: &JoinPlan, columns_map: &[Vec<u64>], tuple: Tuple<'_>) -> Row {
    combined_row_with(plan, columns_map, &Companions::new(plan, columns_map), tuple)
}

/// Each mapped column's NULL companion, by relation and position: the write
/// order of the `__null_` field that tracks it, if any.
///
/// Found once per plan rather than by a name search per column per tuple —
/// which made flattening a tuple quadratic in the number of fetched columns.
pub struct Companions(Vec<Vec<Option<u64>>>);

impl Companions {
    pub fn new(plan: &JoinPlan, columns_map: &[Vec<u64>]) -> Self {
        Companions(
            columns_map
                .iter()
                .enumerate()
                .map(|(rel, mapping)| {
                    (0..mapping.len())
                        .map(|i| {
                            let relation = &plan.relations[rel];
                            let field = &relation.fetch_columns[i];
                            relation
                                .fetch_columns
                                .iter()
                                .find(|f| {
                                    crate::core::rql::nullable::is_hidden(&f.name)
                                        && f.name.len()
                                            > crate::core::rql::nullable::NULL_FLAG_PREFIX.len()
                                        && f.name
                                            [crate::core::rql::nullable::NULL_FLAG_PREFIX.len()..]
                                            .eq_ignore_ascii_case(&field.name)
                                })
                                .map(|flag| flag.write_order)
                        })
                        .collect()
                })
                .collect(),
        )
    }
}

/// [`combined_row`] with the companions already resolved.
pub fn combined_row_with(
    plan: &JoinPlan,
    columns_map: &[Vec<u64>],
    companions: &Companions,
    tuple: Tuple<'_>,
) -> Row {
    let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
    for (rel, mapping) in columns_map.iter().enumerate() {
        for (i, write_order) in mapping.iter().enumerate() {
            let field = &plan.relations[rel].fetch_columns[i];
            let data = tuple
                .get(rel)
                .and_then(|row| row.column(field.write_order))
                .map(|(bytes, _)| mb_from(bytes))
                .unwrap_or_default();

            // An outer join that matched nothing contributes no row at all, and
            // every column it would have supplied is NULL by definition. Typed
            // NULL here rather than left as the declared type holding no bytes:
            // the evaluator decides nullness from the *type*, so an untyped
            // empty value is a present value — `COALESCE(s.a, t.b)` returned
            // that emptiness instead of falling through to `t.b`, and it then
            // compared as incomparable, which a sort reads as "equal" and
            // leaves in scan order while reporting the rows as ordered.
            let unmatched = tuple.get(rel).is_none();
            let is_null = unmatched
                || companions.0[rel][i]
                    .and_then(|flag| {
                        tuple.get(rel).and_then(|row| {
                            row.column(flag)
                                .map(|(b, _)| crate::core::rql::nullable::flag_is_set(b))
                        })
                    })
                    .unwrap_or(false);

            columns.push(Column {
                schema_id: crate::core::row::row::schema_id_of(*write_order),
                data,
                column_type: if is_null {
                    crate::core::db_type::DbType::NULL
                } else {
                    field.db_type.clone()
                },
            });
        }
    }
    Row {
        position: 0,
        length: 0,
        columns,
    }
}

/// Rewrites the cross-relation terms against the combined layout.
fn build_post_join_filter(
    terms: &[String],
    relations: &[Relation],
) -> Result<PostJoinFilter, String> {
    let CombinedLayout {
        schema,
        columns,
        renames,
    } = combined_layout(relations);

    let schema_sv: smallvec::SmallVec<[SchemaField; 20]> = schema.iter().cloned().collect();

    // Each term is tried against the predicate tokenizer first, and handed to
    // the expression evaluator only if that cannot read it. Splitting per term
    // rather than per statement keeps the fast path for everything ordinary:
    // one term needing `||` does not push the rest onto the slower evaluator.
    let mut tokenizer_terms: Vec<String> = Vec::new();
    let mut expr_terms: Vec<crate::core::rql::expr::Cond> = Vec::new();

    // Relations that may contribute no row at all: the right side of a LEFT
    // JOIN. Their columns arrive *empty* in the combined row when nothing
    // matched, and the predicate tokenizer compares fixed-width operands by
    // size — eight bytes of timestamp against nothing is `OperandSizeMismatch`,
    // reported bare, with no hint that an outer join is involved.
    //
    // The expression evaluator reads the cell's *type*, which `combined_row`
    // sets to NULL for exactly those cells, so it answers "unknown" as SQL
    // says it should. A term that can see one is therefore sent there, even
    // though the tokenizer would happily compile it.
    let kinds: Vec<Option<JoinKind>> = relations
        .iter()
        .map(|r| r.join.as_ref().map(|j| j.kind))
        .collect();
    let optional: Vec<bool> = (0..relations.len())
        .map(|at| may_be_unmatched(&kinds, at))
        .collect();

    // The predicate engine reads the combined row through a `u8` position map
    // (`RowView`), with `u8::MAX` meaning "absent". A combined row of 255 or
    // more columns cannot be addressed by it: position 256 wrapped to 0 and
    // read another column's bytes, and position 255 read as absent — both
    // answering rather than failing. The expression evaluator looks columns up
    // by write order with no such limit, so a row that wide sends every term
    // there instead.
    let too_wide_for_tokenizer = schema.len() >= u8::MAX as usize;

    for term in terms {
        let touches_optional = relations.iter().enumerate().any(|(at, relation)| {
            optional[at]
                && relation.fetch_columns.iter().any(|field| {
                    term_names_column(term, &relation.label, &relation.table, &field.name)
                })
        });

        let mut rewritten = term.clone();
        for (from, to) in &renames {
            rewritten = replace_qualified(&rewritten, from, to);
        }

        // A term holding what only the evaluator computes goes there whole,
        // as a scan would lift it. The tokenizer can read some such terms
        // and not answer them: `a.n / 2::numeric = b.m` tokenized, then
        // failed to plan on the cast the rewrite keeps for the evaluator.
        let needs_evaluator = crate::core::tokenizer::predicate_rewrite::lift_evaluated_operands(
            &rewritten,
            &schema_sv,
        )
        .is_ok_and(|lifted| lifted != rewritten);
        if touches_optional || too_wide_for_tokenizer || needs_evaluator {
            expr_terms.push(crate::core::rql::expr::parse_cond(&rewritten, &schema)?);
            continue;
        }

        match crate::core::tokenizer::predicate_rewrite::rewrite_where(&rewritten, &schema_sv) {
            Ok(clause)
                if crate::core::tokenizer::query_tokenizer::try_tokenize(&clause, &schema_sv)
                    .is_ok() =>
            {
                tokenizer_terms.push(clause);
            }
            _ => {
                // Refused rather than dropped if this fails too: a join term
                // silently ignored would widen the result set.
                expr_terms.push(crate::core::rql::expr::parse_cond(&rewritten, &schema)?);
            }
        }
    }

    let where_clause = tokenizer_terms.join(" AND ");
    Ok(PostJoinFilter {
        schema,
        where_clause,
        columns,
        expr_terms,
    })
}

/// The `a.b` and bare `b` identifiers a predicate mentions, in order.
///
/// Deliberately generous: a keyword or a function name may come back, and the
/// caller discards whatever does not resolve to a column. Anything inside a
/// string literal is skipped, so `WHERE status = 'a.b'` names nothing.
fn column_references(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0usize;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if c == b'\'' {
            quote = Some(c);
            i += 1;
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' || c == b'"' {
            let start = i;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric()
                    || bytes[i] == b'_'
                    || bytes[i] == b'.'
                    || bytes[i] == b'"')
            {
                i += 1;
            }
            let word = text[start..i].trim_end_matches('.');
            // A call, not a column: the name is followed by `(`.
            let is_call = bytes.get(i) == Some(&b'(');
            if !word.is_empty() && !is_call && !out.iter().any(|w| w == word) {
                out.push(word.to_string());
            }
            continue;
        }
        i += 1;
    }
    out
}

/// Replaces a qualified name with another, matching whole identifiers only and
/// skipping string literals.
///
/// Copied byte for byte. It pushed each byte as a `char`, which re-encoded
/// every byte of a multi-byte character on its own: `'é'` came out as `'Ã©'`,
/// so a join's `a.t || 'é'` returned the wrong text, and the next pass over it
/// sliced inside the `©` and panicked, closing the connection.
fn replace_qualified(sql: &str, from: &str, to: &str) -> String {
    let mut out: Vec<u8> = Vec::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let from_bytes = from.as_bytes();
    let mut i = 0usize;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = quote {
            out.push(c);
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if c == b'\'' || c == b'"' {
            quote = Some(c);
            out.push(c);
            i += 1;
            continue;
        }
        if bytes[i..].len() >= from_bytes.len()
            && bytes[i..i + from_bytes.len()].eq_ignore_ascii_case(from_bytes)
        {
            let before_ok =
                i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
            let after = i + from_bytes.len();
            let after_ok = after >= bytes.len()
                || !(bytes[after].is_ascii_alphanumeric() || bytes[after] == b'_');
            if before_ok && after_ok {
                out.extend_from_slice(to.as_bytes());
                i = after;
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    // A match starts on a character's first byte, since `from` is text, so
    // what is copied around it is whole characters in their order.
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

/// Builds the fold that runs over the projected join rows, or `None` when the
/// `SELECT` list has no aggregate call.
#[allow(clippy::too_many_arguments)]
fn build_aggregate_stage(
    select_list: &str,
    group_by_clause: Option<&str>,
    having_clause: Option<&str>,
    order_by: &[crate::core::rql::order_by::SortTerm],
    limit: Option<u64>,
    offset: Option<u64>,
    items: &mut Vec<JoinSelectItem>,
    resolve: &dyn Fn(&str) -> Result<(usize, SchemaField), String>,
    // Each relation's schema, for the functional-dependency test.
    schemas: &[SmallVec<[SchemaField; 20]>],
    // Aggregate arguments that are expressions, in the order their placeholder
    // items were pushed. The caller parses them once the fetch lists are
    // settled and fills in both the item and the aggregate's argument type.
    expr_arguments: &mut Vec<String>,
) -> Result<Option<crate::core::rql::aggregate::AggregatePlan>, String> {
    use crate::core::rql::aggregate::{AggFunc, SelectItem};

    // Only a genuine aggregate puts the statement on the fold path. A nullary
    // scalar like `now()` adds a column without grouping anything; counting it
    // here demanded every other column appear in GROUP BY, so
    // `SELECT a.id, b.id, now() FROM …` was refused.
    // An aggregate *anywhere* in the item puts the statement on the fold path.
    // Testing only the outermost call meant `COALESCE(SUM(x), 0)` was not
    // recognised as aggregating at all: it fell through to the projection,
    // which has no aggregate to project, and the statement answered one row of
    // NULL instead of the sum.
    let has_call = select_list.trim() != "*"
        && split_top_level(select_list).iter().any(|item| {
            let (expr, _) = split_expr_alias(item.trim());
            !crate::core::rql::lexer_query::find_aggregate_calls(expr).is_empty()
        });

    // A GROUP BY makes the query grouped by itself — `SELECT a.city FROM a
    // JOIN b … GROUP BY a.city` is a DISTINCT, and an aggregate may appear
    // only in the HAVING. `SELECT *` is the one shape still refused: the fold
    // checks each named column against the keys, and a star names none.
    let grouped = has_call || (group_by_clause.is_some() && select_list.trim() != "*");

    if !grouped {
        if group_by_clause.is_some() {
            return Err(
                "GROUP BY over a join needs the columns listed explicitly, not '*'".to_string(),
            );
        }
        if having_clause.is_some() {
            return Err("HAVING requires an aggregate in the SELECT list".to_string());
        }
        return Ok(None);
    }

    // Adds a column to the join's output if it is not already there, and
    // returns a field addressing it by position in the projected row.
    fn positional(rel: usize, field: SchemaField, items: &mut Vec<JoinSelectItem>) -> SchemaField {
        let pos = match items.iter().position(|i| {
            i.reads_column() && i.rel == rel && i.field.write_order == field.write_order
        }) {
            Some(p) => p,
            None => {
                items.push(JoinSelectItem {
                    rel,
                    field: field.clone(),
                    label: field.name.clone(),
                    scalar: None,
                    literal: None,
                    expr: None,
                    pending_expression: false,
                });
                items.len() - 1
            }
        };
        let mut positioned = SchemaField::new(
            field.name.clone(),
            field.db_type.clone(),
            field.size,
            0,
            pos as u64,
            false,
        );
        // The projected bytes are the column's own, at its declared scale. The
        // fold's `SUM`/`AVG`, `HAVING`'s literal coercion and the rendered
        // scale all read it from here; dropping it compared a `NUMERIC(10,2)`
        // sum against a literal encoded at nine places.
        positioned.scale = field.scale;
        positioned
    }

    let mut group_by: Vec<SchemaField> = Vec::new();
    // Relations a grouping key determines: grouped by a column that is unique
    // and NOT NULL in that relation (`lexer_query::determines_its_relation`),
    // every group holds at most one of its rows — so its other columns may be
    // selected without being grouped, as PostgreSQL allows for a primary key.
    let mut determined: Vec<bool> = vec![false; schemas.len()];
    if let Some(list) = group_by_clause {
        for raw in list.split(',') {
            let name = raw.trim().split_whitespace().next().unwrap_or("");
            if name.is_empty() {
                continue;
            }
            let (rel, field) = resolve(name)?;
            if schemas.get(rel).is_some_and(|schema| {
                crate::core::rql::lexer_query::determines_its_relation(&field, schema)
            }) {
                determined[rel] = true;
            }
            group_by.push(positional(rel, field, items));
        }
    }

    // The predicate of one `FILTER (WHERE …)`, resolved against the *projected*
    // row the fold sees.
    //
    // The clause names columns by relation — `c.closed_at` — and the fold has
    // only the projected row, so each referenced column is added to the
    // projection (if it is not there already) and the predicate is rewritten to
    // address it by position. Names become `<label>__<column>` for the same
    // reason the post-join filter does it: two relations may both have `id`.
    let filter_over_join = |text: &str,
                            items: &mut Vec<JoinSelectItem>|
     -> Result<crate::core::rql::aggregate::AggregateFilter, String> {
        let mut schema: Vec<SchemaField> = Vec::new();
        let mut renames: Vec<(String, String)> = Vec::new();
        for name in column_references(text) {
            // Not every identifier is a column — `WHERE status = 'queued'`
            // holds one of each — so anything that does not resolve is left
            // alone for `parse_condition` to judge.
            let Ok((rel, field)) = resolve(&name) else {
                continue;
            };
            let positioned = positional(rel, field.clone(), items);
            // Qualified references get a unique synthetic name, because two
            // relations may both have `id`. An unqualified one already had to
            // be unambiguous for `resolve` to accept it.
            let unique = match name.split_once('.') {
                Some((qualifier, column)) => {
                    let unique = format!("{}__{}", qualifier, column);
                    renames.push((name.clone(), unique.clone()));
                    unique
                }
                None => field.name.clone(),
            };
            let mut synthetic = positioned;
            synthetic.name = unique;
            if !schema
                .iter()
                .any(|f: &SchemaField| f.name == synthetic.name)
            {
                schema.push(synthetic);
            }
        }
        // Longest first, so `a.bc` is not partly rewritten by a rule for `a.b`.
        renames.sort_by_key(|(from, _)| std::cmp::Reverse(from.len()));
        let mut rewritten = text.to_string();
        for (from, to) in &renames {
            rewritten = replace_qualified(&rewritten, from, to);
        }
        let condition = crate::core::rql::expr::parse_cond(&rewritten, &schema)?;
        // No NULL companions here, unlike the single-relation path: `project`
        // already types an absent or unmatched value as `NULL`, so the row the
        // fold reads carries its own nullness.
        Ok(crate::core::rql::aggregate::AggregateFilter { condition, schema })
    };

    let mut fold_items: Vec<SelectItem> = Vec::new();
    for raw in split_top_level(select_list) {
        let entry = raw.trim();
        if entry.is_empty() {
            continue;
        }
        // Peeled before anything reads the item as a call, exactly as the
        // single-relation path does: a cast usually follows the clause, and
        // leaving it attached sends the item down the wrapped-aggregate path.
        let (rebuilt, filter_text) = crate::core::rql::lexer_query::split_aggregate_filter(entry);
        // A cast around a filtered aggregate — `(AVG(x) FILTER (…))::float8`,
        // which is how sea-orm writes one — would otherwise make the item an
        // expression *around* a call and take the wrapped-aggregate path, which
        // carries no filter. Dropped for the same reason the single-relation
        // path drops it: it is a no-op on a count, and on an average it only
        // widens what the argument already produces.
        let rebuilt = rebuilt.map(|text| {
            let (head, alias) = split_expr_alias(&text);
            // sea-orm writes a filtered aggregate as
            // `(AVG(x) FILTER (…))::float8`, so removing the clause leaves
            // `(AVG(x))::float8` — a cast around a parenthesised call. Both
            // wrappers have to come off, in whichever order they appear, or the
            // "function name" before the first `(` is the empty string.
            //
            // The cast is dropped rather than applied: it is a no-op on a count,
            // and on an average it only widens what the argument already
            // produces. Parentheses are peeled only when the pair encloses the
            // *whole* expression, so `(a + b) * 2` is untouched.
            let mut call = head.trim();
            loop {
                let before = call;
                if call.starts_with('(')
                    && crate::core::rql::expr::matching_paren(call, 0) == Some(call.len() - 1)
                {
                    call = call[1..call.len() - 1].trim();
                }
                if let Some((inner, _)) = crate::core::rql::expr::trailing_cast(call) {
                    call = inner.trim();
                }
                if call == before {
                    break;
                }
            }
            match alias {
                Some(alias) => format!("{} AS {}", call, alias),
                None => call.to_string(),
            }
        });
        let entry = rebuilt.as_deref().unwrap_or(entry);
        let (expr, alias) = split_expr_alias(entry);

        // An expression *around* an aggregate — `COALESCE(SUM(x), 0)`,
        // `SUM(x) / COUNT(*)`. Each aggregate is pulled out into its own input
        // and the surrounding expression is evaluated once per emitted group,
        // exactly as the single-relation path does.
        let calls = crate::core::rql::lexer_query::find_aggregate_calls(expr);
        let wrapped =
            !calls.is_empty() && !(calls.len() == 1 && calls[0].0 == 0 && calls[0].1 == expr.len());
        if wrapped {
            let mut inputs: Vec<crate::core::rql::aggregate::ComputedInput> = Vec::new();
            let mut synth_schema: Vec<SchemaField> = Vec::new();
            let mut rewritten = String::with_capacity(expr.len());
            let mut cursor = 0usize;

            for (start, end) in &calls {
                rewritten.push_str(&expr[cursor..*start]);
                let call = &expr[*start..*end];
                let open = call.find('(').expect("a call has one");
                let close = call.rfind(')').expect("a call has one");
                let name = call[..open].trim();
                let func = AggFunc::parse(name)
                    .ok_or_else(|| format!("function '{}' is not supported yet", name))?;
                let arg_text = call[open + 1..close].trim();

                let arg = if arg_text == "*" {
                    if func != AggFunc::Count {
                        return Err(format!("{}(*) is not valid", name.to_uppercase()));
                    }
                    None
                } else {
                    // An aggregate wrapped in an expression addresses its
                    // argument through a synthetic `__agg_N` slot whose type is
                    // fixed when the slot is made, so an argument whose type is
                    // only known later cannot be carried here yet. Refused, and
                    // loudly: filling it in with a placeholder type produced a
                    // silently NULL column instead of an error.
                    let (rel, field) = resolve(arg_text)?;
                    Some(positional(rel, field, items))
                };

                let slot = inputs.len();
                let ty = func.result_type(arg.as_ref().map(|f: &SchemaField| &f.db_type));
                let synth_name = format!("__agg_{}", slot);
                synth_schema.push(SchemaField::new(
                    synth_name.clone(),
                    ty.clone(),
                    ty.get_size(),
                    0,
                    slot as u64,
                    false,
                ));
                inputs.push(crate::core::rql::aggregate::ComputedInput { func, arg });
                rewritten.push_str(&synth_name);
                cursor = *end;
            }
            rewritten.push_str(&expr[cursor..]);

            let parsed = crate::core::rql::expr::parse(&rewritten, &synth_schema)?;
            let db_type = crate::core::rql::expr::static_type(&parsed);
            fold_items.push(SelectItem::Computed {
                expr: parsed,
                inputs,
                db_type,
                label: alias.unwrap_or_else(|| "?column?".to_string()),
            });
            continue;
        }

        if let Some(open) = expr.find('(') {
            let func_name = expr[..open].trim();
            let Some(func) = AggFunc::parse(func_name) else {
                return Err(format!("function '{}' is not supported yet", func_name));
            };
            let close = expr
                .rfind(')')
                .ok_or_else(|| format!("unterminated function call in '{}'", entry))?;
            let arg_text = expr[open + 1..close].trim();

            let arg = if arg_text == "*" {
                if func != AggFunc::Count {
                    return Err(format!("{}(*) is not valid", func_name.to_uppercase()));
                }
                None
            } else if let Ok((rel, field)) = resolve(arg_text) {
                Some(positional(rel, field, items))
            } else {
                // `AVG(EXTRACT(EPOCH FROM (a - b)))` — the argument is computed,
                // not read. The fold addresses an argument as a column of the
                // projected row, so the value is projected as one and the
                // aggregate points at that position.
                //
                // Its type is not known yet: the expression can only be parsed
                // once the relations' fetch lists are settled, and an
                // aggregate's result type depends on its argument's. Both are
                // filled in afterwards, and `expr_arguments` is what says which
                // item to fill.
                let position = items.len();
                items.push(JoinSelectItem {
                    rel: 0,
                    field: SchemaField::new(
                        arg_text.to_string(),
                        DbType::F64,
                        DbType::F64.get_size(),
                        0,
                        0,
                        false,
                    ),
                    label: arg_text.to_string(),
                    scalar: None,
                    literal: None,
                    expr: None,
                    pending_expression: true,
                });
                expr_arguments.push(arg_text.to_string());
                Some(SchemaField::new(
                    arg_text.to_string(),
                    DbType::F64,
                    DbType::F64.get_size(),
                    0,
                    position as u64,
                    false,
                ))
            };

            let filter = match &filter_text {
                Some(text) => Some(filter_over_join(text, items)?),
                None => None,
            };
            fold_items.push(SelectItem::Aggregate { distinct: false,
                func,
                arg,
                label: alias.unwrap_or_else(|| func.name().to_string()),
                filter,
                // The join projects a computed argument as a column instead, so
                // the fold reads it positionally like any other.
                arg_expr: None,
            });
        } else {
            let (rel, field) = resolve(expr)?;
            let label = alias.unwrap_or_else(|| field.name.clone());
            let synthetic = positional(rel, field, items);
            if !group_by
                .iter()
                .any(|g| g.write_order == synthetic.write_order)
                && determined.get(rel).copied().unwrap_or(false)
            {
                // Functionally dependent on a grouped key of its own
                // relation. Appended to the group key rather than assumed
                // constant per group: if the data ever broke the uniqueness,
                // groups would split, never merge rows with different values.
                group_by.push(synthetic.clone());
            }
            if !group_by
                .iter()
                .any(|g| g.write_order == synthetic.write_order)
            {
                return Err(format!(
                    "column '{}' must appear in GROUP BY or be used in an aggregate",
                    label
                ));
            }
            fold_items.push(SelectItem::Column {
                field: synthetic,
                label,
            });
        }
    }

    // The shared `HAVING` parser (`having.rs`): a boolean tree whose every
    // literal is coerced against its aggregate at plan time. The one-comparison
    // splitter it replaces took everything after the first operator as the
    // literal, could not read `1 AND SUM(x) > 5`, and kept every group.
    let having = match having_clause {
        None => None,
        Some(text) => {
            let mut resolve_call = |call: &str| -> Result<SelectItem, String> {
                let (rebuilt, filter_text) =
                    crate::core::rql::lexer_query::split_aggregate_filter(call);
                let call = rebuilt.as_deref().unwrap_or(call).trim();
                let open = call
                    .find('(')
                    .ok_or_else(|| format!("'{}' is not an aggregate call", call))?;
                let close = call
                    .rfind(')')
                    .ok_or_else(|| format!("unterminated function call in '{}'", call))?;
                if !call[close + 1..].trim().is_empty() {
                    return Err(format!("'{}' is not an aggregate call", call));
                }
                let name = call[..open].trim();
                let func = AggFunc::parse(name)
                    .ok_or_else(|| format!("function '{}' is not supported yet", name))?;
                let arg_text = call[open + 1..close].trim();
                let arg = if arg_text == "*" {
                    if func != AggFunc::Count {
                        return Err(format!("{}(*) is not valid", name.to_uppercase()));
                    }
                    None
                } else {
                    // A computed argument is projected as a pending expression
                    // whose type is filled in later from the select list only,
                    // so one that appears only in HAVING cannot be carried yet.
                    let (rel, field) = resolve(arg_text).map_err(|_| {
                        format!(
                            "HAVING over a join supports an aggregate of a column, not of \
                             '{}', yet",
                            arg_text
                        )
                    })?;
                    Some(positional(rel, field, items))
                };
                let filter = match &filter_text {
                    Some(text) => Some(filter_over_join(text, items)?),
                    None => None,
                };
                Ok(SelectItem::Aggregate { distinct: false,
                    func,
                    arg,
                    label: func.name().to_string(),
                    filter,
                    arg_expr: None,
                })
            };
            let split = crate::core::rql::having::parse(text, &mut resolve_call)?;
            // A term that reads no aggregate would have to move into `WHERE`,
            // which is already split per relation by the time HAVING is seen.
            // Refused rather than dropped: dropping it widens the result.
            if let Some(term) = split.row_terms.first() {
                return Err(format!(
                    "HAVING term '{}' over a join must test an aggregate; move it to WHERE",
                    term.text
                ));
            }
            split.having
        }
    };

    // Ordering is by output label: a group has no relation to sort within.
    let mut fold_order = Vec::with_capacity(order_by.len());
    for term in order_by {
        let label = crate::core::rql::aggregate::resolve_order_label(&term.token, &fold_items)
            .ok_or_else(|| format!("ORDER BY term '{}' is not in the SELECT list", term.token))?;
        fold_order.push((label, term.descending, term.nulls_first));
    }

    Ok(Some(crate::core::rql::aggregate::AggregatePlan {
        items: fold_items,
        group_by,
        computed_keys: Vec::new(),
        having,
        order_by: fold_order,
        limit,
        offset,
    }))
}

/// Splits a comma-separated list on commas outside parentheses and quotes.
fn split_top_level(list: &str) -> Vec<String> {
    // Each item is the slice between two separating commas, copied once.
    // Every character used to be pushed onto a growing `String`, and every
    // statement's dispatch splits a list or three. Byte-wise is exact: the
    // quotes, parentheses and commas are ASCII, and no byte of a multi-byte
    // character equals one.
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    for (i, b) in list.bytes().enumerate() {
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
            }
            None => match b {
                b'\'' | b'"' => quote = Some(b),
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                b',' if depth == 0 => {
                    out.push(list[start..i].to_string());
                    start = i + 1;
                }
                _ => {}
            },
        }
    }
    let rest = &list[start..];
    if !rest.trim().is_empty() {
        out.push(rest.to_string());
    }
    out
}

/// Separates a select item from its output name.
///
/// The single-relation splitter, so a join reads an alias exactly as a plain
/// `SELECT` does. This one had its own copy, which honoured `AS` only after
/// the last `)`: `COALESCE(a.n, -1) + b.m AS s` kept `AS s` in the
/// expression, and the item was refused as "'b__m AS s' is neither a column
/// nor a value".
fn split_expr_alias(entry: &str) -> (&str, Option<String>) {
    crate::core::rql::lexer_query::split_alias(entry)
}

/// A select item's expression and alias, with a null test kept whole.
///
/// `a.n IS NULL` has no alias, but a whitespace split reads `IS` as one, and
/// the item came back as the column `a.n` labelled `IS`.
fn select_entry(item: &str) -> (String, Option<String>) {
    if let Some((test, alias)) = crate::core::rql::lexer_query::null_test_item(item) {
        return (test, alias);
    }
    let (expr, alias) = split_expr_alias(item);
    (expr.trim().to_string(), alias)
}

/// Whether a select item names a column — `n`, `a.n`, `"A"."N"` — rather than
/// computing a value. `NULL`, `TRUE` and `FALSE` are spelled like names and
/// are constants.
fn is_column_reference(text: &str) -> bool {
    let text = text.trim();
    let starts_like_a_name = text
        .chars()
        .next()
        .is_some_and(|c| c.is_alphabetic() || c == '_' || c == '"');
    starts_like_a_name
        && text
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '.' | '"' | '*' | '$'))
        && !["NULL", "TRUE", "FALSE"]
            .iter()
            .any(|keyword| text.eq_ignore_ascii_case(keyword))
}

/// A comparison used as a value — `a.n > b.m`, `x = 3`, `x IS NULL` — as the
/// boolean expression it stands for, or `None` when `text` is not one.
///
/// SQL's comparison is three-valued: a NULL on either side makes it NULL, not
/// false. A condition here evaluates to a plain `bool`, so for a comparison
/// between two values the NULL cases are stated as arms ahead of it. A null
/// test is never NULL and needs none.
pub(crate) fn comparison_value(
    text: &str,
    schema: &[SchemaField],
) -> Option<crate::core::rql::expr::Expr> {
    use crate::core::rql::expr::{Cmp, Cond, Condition, Expr};
    let condition = crate::core::rql::expr::parse_condition(text, schema).ok()?;
    let boolean = |value: bool| Expr::Literal {
        bytes: vec![value as u8],
        db_type: DbType::BOOL,
    };
    let is_null = |operand: &Expr| {
        Cond::Cmp(Condition {
            left: Box::new(operand.clone()),
            cmp: Cmp::IsNull,
            right: None,
        })
    };
    let mut whens = Vec::new();
    if let Some(right) = condition.right.as_deref() {
        whens.push((is_null(&condition.left), Expr::Null));
        whens.push((is_null(right), Expr::Null));
    }
    whens.push((Cond::Cmp(condition), boolean(true)));
    Some(Expr::Case {
        whens,
        otherwise: Some(Box::new(boolean(false))),
    })
}

/// The name PostgreSQL gives a select item written without an alias — its
/// `FigureColname`.
///
/// A column is named after the column, a function call after the function, a
/// `CASE` after its `ELSE` when that has a name and `case` otherwise, and a
/// cast after what it casts or, when that has no name, the type. Anything else
/// — an operator, a constant, a comparison, a JSON path — is `?column?`.
pub(crate) fn pg_column_name(text: &str) -> String {
    figure_column_name(text)
        .map(|(name, _)| name)
        .unwrap_or_else(|| "?column?".to_string())
}

/// The name and its strength, as PostgreSQL ranks them: 2 for a column or a
/// call, 1 for a fallback such as `case` or a cast's type name. A cast or a
/// `CASE` keeps its operand's name only when that is a strong one.
fn figure_column_name(text: &str) -> Option<(String, u8)> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    // Parentheses group; they do not name anything.
    if text.starts_with('(')
        && crate::core::rql::expr::matching_paren(text, 0) == Some(text.len() - 1)
    {
        return figure_column_name(&text[1..text.len() - 1]);
    }
    if let Some((operand, type_name)) = crate::core::rql::expr::trailing_cast(text) {
        // `a + b::int` casts `b` alone, so the item is the sum and unnamed.
        if !is_single_operand(operand) {
            return None;
        }
        return cast_name(figure_column_name(operand), type_name);
    }
    let upper = text.to_ascii_uppercase();
    if upper.starts_with("CASE")
        && upper[4..].starts_with(|c: char| c.is_ascii_whitespace())
        && upper.ends_with("END")
    {
        // The `ELSE` result, when it is the last top-level one.
        let else_at = find_keyword(text, "ELSE", 0).and_then(|mut at| {
            while let Some(next) = find_keyword(text, "ELSE", at + 4) {
                at = next;
            }
            Some(at)
        });
        let named = else_at
            .and_then(|at| figure_column_name(&text[at + 4..text.len() - 3]))
            .filter(|(_, strength)| *strength == 2);
        return Some(named.unwrap_or_else(|| ("case".to_string(), 1)));
    }
    if text.eq_ignore_ascii_case("TRUE") || text.eq_ignore_ascii_case("FALSE") {
        // PostgreSQL reads a boolean constant as a cast of a string to `bool`.
        return Some(("bool".to_string(), 1));
    }
    if is_column_reference(text) {
        let (_, column) = split_qualified(text);
        return Some((column.trim_matches('"').to_string(), 2));
    }
    // A call: a name, then one parenthesised argument list that ends the item.
    if let Some(open) = text.find('(') {
        let name = text[..open].trim();
        if crate::core::rql::expr::matching_paren(text, open) == Some(text.len() - 1)
            && is_column_reference(name)
            && !name.contains('"')
        {
            let (_, function) = split_qualified(name);
            if function.eq_ignore_ascii_case("CAST") {
                let inside = &text[open + 1..text.len() - 1];
                let as_at = find_keyword(inside, "AS", 0)?;
                let mut last = as_at;
                while let Some(next) = find_keyword(inside, "AS", last + 2) {
                    last = next;
                }
                return cast_name(figure_column_name(&inside[..last]), &inside[last + 2..]);
            }
            return Some((function.to_ascii_lowercase(), 2));
        }
    }
    None
}

/// A cast's name: its operand's when that is a strong one, else the type's.
fn cast_name(operand: Option<(String, u8)>, type_name: &str) -> Option<(String, u8)> {
    if let Some((name, 2)) = operand {
        return Some((name, 2));
    }
    Some((pg_type_name(type_name), 1))
}

/// Whether `text` is one operand — a name, a constant, a call, a
/// parenthesised group or a cast of one — rather than operators over several.
fn is_single_operand(text: &str) -> bool {
    let text = text.trim();
    if text.is_empty() {
        return false;
    }
    if text.starts_with('(')
        && crate::core::rql::expr::matching_paren(text, 0) == Some(text.len() - 1)
    {
        return true;
    }
    if let Some((operand, _)) = crate::core::rql::expr::trailing_cast(text) {
        return is_single_operand(operand);
    }
    if is_column_reference(text)
        || text.parse::<f64>().is_ok()
        || ["NULL", "TRUE", "FALSE"]
            .iter()
            .any(|keyword| text.eq_ignore_ascii_case(keyword))
    {
        return true;
    }
    if text.starts_with('\'') && text.ends_with('\'') && text.len() >= 2 {
        // One literal, unless a quote inside it closes it early: `'a' || 'b'`.
        let inner = &text[1..text.len() - 1];
        return !inner.replace("''", "").contains('\'');
    }
    match text.find('(') {
        Some(open) => {
            crate::core::rql::expr::matching_paren(text, open) == Some(text.len() - 1)
                && is_column_reference(text[..open].trim())
        }
        None => false,
    }
}

/// The name PostgreSQL reports for a type as written in a cast: its internal
/// name, without a length or precision.
fn pg_type_name(written: &str) -> String {
    let lower = written.trim().to_ascii_lowercase();
    let base = lower
        .split(['(', '['])
        .next()
        .unwrap_or("")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let base = base.rsplit('.').next().unwrap_or("").trim_matches('"');
    match base {
        "int" | "integer" | "int4" => "int4",
        "bigint" | "int8" => "int8",
        "smallint" | "int2" => "int2",
        "real" | "float4" => "float4",
        "double precision" | "float" | "float8" => "float8",
        "boolean" | "bool" => "bool",
        "decimal" | "numeric" => "numeric",
        "character varying" | "varchar" => "varchar",
        "character" | "char" | "bpchar" => "bpchar",
        "timestamp" | "timestamp without time zone" => "timestamp",
        "timestamptz" | "timestamp with time zone" => "timestamptz",
        "time" | "time without time zone" => "time",
        "timetz" | "time with time zone" => "timetz",
        other => other,
    }
    .to_string()
}

/// True when a `SELECT` statement's `FROM` clause contains a join.
pub fn statement_has_join(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    // Whole-word matching, or a statement formatted across lines is not seen as
    // a join at all and goes to the single-table path — which then reports
    // "JOIN is not supported yet" about a join the engine does support.
    let Some(from) = find_keyword(sql, "FROM", 0) else {
        return false;
    };
    let start = from + "FROM".len();
    // `UNION` ends the first branch's FROM clause: without it the comma in the
    // *next* branch's select list — `FROM a UNION SELECT id, label FROM b` —
    // read as a comma join, and the statement was planned as one, failing
    // with "relation 'label' does not exist".
    let end = ["WHERE", "ORDER", "LIMIT", "OFFSET", "GROUP", "HAVING", "UNION"]
        .iter()
        .filter_map(|kw| find_keyword(sql, kw, start))
        .min()
        .unwrap_or(upper.len());
    find_keyword(&sql[start..end], "JOIN", 0).is_some() || sql[start..end].contains(',')
}

/// Splits a joined `SELECT` into its clauses and plans it.
pub fn plan_statement(
    sql: &str,
    schema_of: &dyn Fn(&str) -> Option<SmallVec<[SchemaField; 20]>>,
) -> Result<JoinPlan, String> {
    // Same normalisation the single-table path applies: the clause split below
    // only recognises `LIMIT` and `OFFSET`.
    let sql = crate::core::tokenizer::pagination::normalize_pagination(sql.trim())?;
    let sql = sql.as_str();
    let upper = sql.to_ascii_uppercase();

    // Whole-word matching throughout: the literal `" SELECT "` / `" FROM "`
    // forms fail on any statement formatted across lines, which is how every
    // ORM emits them.
    if !upper.trim_start().starts_with("SELECT") {
        return Err("Not a SELECT query".to_string());
    }
    let from_at =
        find_keyword(sql, "FROM", 0).ok_or_else(|| "Missing FROM in SELECT query".to_string())?;

    let select_start = find_keyword(sql, "SELECT", 0)
        .map(|i| i + "SELECT".len())
        .unwrap_or(0);
    let mut select_list = sql[select_start..from_at].trim();
    let mut distinct = false;
    if let Some(rest) = strip_keyword_prefix(select_list, "DISTINCT") {
        // `DISTINCT ON (expr)` keeps the *first* row of each group under the
        // ORDER BY, which is a different operation from collapsing identical
        // rows and would silently return the wrong one of each group if it were
        // treated as plain DISTINCT.
        if strip_keyword_prefix(rest, "ON").is_some() {
            return Err("SELECT DISTINCT ON is not supported yet".to_string());
        }
        distinct = true;
        select_list = rest;
    }

    let after_from = &sql[from_at + "FROM".len()..];

    let where_at = find_keyword(after_from, "WHERE", 0);
    let order_at = find_keyword(after_from, "ORDER", 0);
    let limit_at = find_keyword(after_from, "LIMIT", 0);
    let offset_at = find_keyword(after_from, "OFFSET", 0);
    let group_at = find_keyword(after_from, "GROUP", 0);
    let having_at = find_keyword(after_from, "HAVING", 0);

    let from_end = [where_at, order_at, limit_at, offset_at, group_at, having_at]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(after_from.len());

    let where_clause = match where_at {
        None => "",
        Some(w) => {
            let start = w + "WHERE".len();
            let end = [order_at, limit_at, offset_at, group_at, having_at]
                .into_iter()
                .flatten()
                .filter(|p| *p >= start)
                .min()
                .unwrap_or(after_from.len());
            after_from[start..end].trim()
        }
    };

    let group_by_clause = group_at.map(|g| {
        let start = g + "GROUP".len();
        let end = [having_at, order_at, limit_at, offset_at]
            .into_iter()
            .flatten()
            .filter(|p| *p >= start)
            .min()
            .unwrap_or(after_from.len());
        let clause = after_from[start..end].trim_start();
        clause
            .strip_prefix("BY")
            .or_else(|| clause.strip_prefix("by"))
            .unwrap_or(clause)
            .trim()
    });

    let having_clause = having_at.map(|h| {
        let start = h + "HAVING".len();
        let end = [order_at, limit_at, offset_at]
            .into_iter()
            .flatten()
            .filter(|p| *p >= start)
            .min()
            .unwrap_or(after_from.len());
        after_from[start..end].trim()
    });

    let order_by = match order_at {
        None => Vec::new(),
        Some(o) => {
            // `find_keyword` matched only the first word of `ORDER BY`.
            let start = o + "ORDER".len();
            let end = [limit_at, offset_at]
                .into_iter()
                .flatten()
                .filter(|p| *p >= start)
                .min()
                .unwrap_or(after_from.len());
            let clause = after_from[start..end].trim_start();
            let clause = clause
                .strip_prefix("BY")
                .or_else(|| clause.strip_prefix("by"))
                .unwrap_or(clause);
            crate::core::rql::order_by::parse(clause)?
        }
    };

    let parse_count = |at: Option<usize>, kw: &str| -> Result<Option<u64>, String> {
        let Some(p) = at else { return Ok(None) };
        let start = p + kw.len();
        let token = after_from[start..]
            .trim()
            .split_whitespace()
            .next()
            .unwrap_or("");
        token
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("Invalid {} value '{}'", kw.trim(), token))
    };
    let limit = parse_count(limit_at, "LIMIT")?;
    let offset = parse_count(offset_at, "OFFSET")?;

    let relations = parse_from_clause(after_from[..from_end].trim())?;
    let mut planned = plan(
        relations,
        select_list,
        where_clause,
        group_by_clause,
        having_clause,
        order_by,
        limit,
        offset,
        schema_of,
    )?;
    planned.distinct = distinct;
    Ok(planned)
}

/// Which relations a `WHERE` conjunct refers to.
fn relations_referenced(
    conjunct: &str,
    relations: &[RelationRef],
    schemas: &[SmallVec<[SchemaField; 20]>],
) -> Vec<usize> {
    let mut hits: Vec<usize> = Vec::new();

    for token in
        conjunct.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.' || c == '"'))
    {
        let token = token.trim_matches('"');
        if token.is_empty() {
            continue;
        }
        let (qualifier, column) = split_qualified(token);
        match qualifier {
            Some(q) => {
                if let Some(i) = relations.iter().position(|r| {
                    r.label.eq_ignore_ascii_case(&q) || r.table.eq_ignore_ascii_case(&q)
                }) {
                    if schemas[i]
                        .iter()
                        .any(|f| f.name.eq_ignore_ascii_case(&column))
                        && !hits.contains(&i)
                    {
                        hits.push(i);
                    }
                }
            }
            None => {
                for (i, s) in schemas.iter().enumerate() {
                    if s.iter().any(|f| f.name.eq_ignore_ascii_case(&column)) && !hits.contains(&i)
                    {
                        hits.push(i);
                    }
                }
            }
        }
    }

    hits
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn on_splits_the_join_key_from_its_extra_filters() {
        let (l, r, extra) =
            split_on_condition("r.domain_id = d.id AND r.name = 'SuperAdministrator'").unwrap();
        assert_eq!(l, "r.domain_id");
        assert_eq!(r, "d.id");
        assert_eq!(extra, vec!["r.name = 'SuperAdministrator'".to_string()]);
    }

    #[test]
    fn on_still_works_with_a_single_term() {
        let (l, r, extra) = split_on_condition("a.x = b.y").unwrap();
        assert_eq!((l.as_str(), r.as_str()), ("a.x", "b.y"));
        assert!(extra.is_empty());
    }

    #[test]
    fn an_on_without_a_column_equality_yields_no_key() {
        // A lone literal comparison names no join key. That is reported by
        // returning an empty key rather than by failing: an *inner* join on a
        // predicate is a cross join filtered by it, which the planner builds.
        // The planner still refuses the outer-join case, where the two are not
        // equivalent.
        let (l, r, extra) = split_on_condition("r.name = 'x'").unwrap();
        assert!(l.is_empty() && r.is_empty(), "no key: {:?} {:?}", l, r);
        assert_eq!(extra, vec!["r.name = 'x'".to_string()]);

        // OR still cannot be expressed as a per-relation filter.
        assert!(split_on_condition("a.x = b.y OR a.z = b.w").is_err());
    }

    #[test]
    fn a_literal_comparison_is_never_mistaken_for_the_join_key() {
        // `'2024-01-01'` is qualified-looking only if you squint; it must not
        // become the join key or the real key would be pushed down as a filter.
        let (l, r, extra) =
            split_on_condition("b.created = '2024-01-01' AND a.id = b.aid").unwrap();
        assert_eq!((l.as_str(), r.as_str()), ("a.id", "b.aid"));
        assert_eq!(extra, vec!["b.created = '2024-01-01'".to_string()]);
    }
    use crate::memory_pool::MEMORY_POOL;

    fn field(name: &str, ty: DbType, write_order: u64) -> SchemaField {
        SchemaField::new(
            name.to_string(),
            ty.clone(),
            ty.get_size(),
            0,
            write_order,
            false,
        )
    }

    fn mb(bytes: &[u8]) -> crate::memory_pool::MemoryBlock {
        let mut m = MEMORY_POOL.acquire(bytes.len());
        m.into_slice_mut().copy_from_slice(bytes);
        m
    }

    fn row(cells: &[(u64, DbType, &[u8])]) -> Row {
        let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
        for (schema_id, ty, bytes) in cells {
            columns.push(Column {
                schema_id: crate::core::row::row::schema_id_of(*schema_id),
                data: mb(bytes),
                column_type: ty.clone(),
            });
        }
        Row {
            position: 0,
            columns,
            length: 0,
        }
    }

    /// users(id, name, domain_id) joined to domain(id, name).
    fn two_relation_plan(kind: JoinKind) -> JoinPlan {
        let u_id = field("id", DbType::STRING, 0);
        let u_name = field("name", DbType::STRING, 1);
        let u_domain = field("domain_id", DbType::STRING, 2);
        let d_id = field("id", DbType::STRING, 0);
        let d_name = field("name", DbType::STRING, 1);

        JoinPlan {
            relations: vec![
                Relation {
                    table: "users".into(),
                    label: "u".into(),
                    join: None,
                    predicate: String::new(),
                    require_unmatched: false,
                    match_condition: None,
                    left_condition: None,
                    fetch_columns: vec![u_id, u_name.clone(), u_domain],
                },
                Relation {
                    table: "domain".into(),
                    label: "d".into(),
                    join: Some(JoinedRelation {
                        kind,
                        left_rel: 0,
                        keys: vec![("domain_id".into(), "id".into())],
                    }),
                    predicate: String::new(),
                    require_unmatched: false,
                    match_condition: None,
                    left_condition: None,
                    fetch_columns: vec![d_id, d_name.clone()],
                },
            ],
            items: vec![
                JoinSelectItem {
                    rel: 0,
                    field: u_name,
                    label: "name".into(),
                    scalar: None,
                    literal: None,
                    expr: None,
                    pending_expression: false,
                },
                JoinSelectItem {
                    rel: 1,
                    field: d_name,
                    label: "domain".into(),
                    scalar: None,
                    literal: None,
                    expr: None,
                    pending_expression: false,
                },
            ],
            order_by: Vec::new(),
            order_exprs: Vec::new(),
            order_layout: None,
            limit: None,
            offset: None,
            aggregate: None,
            post_join: None,
            distinct: false,
        }
    }

    fn users() -> Vec<Row> {
        vec![
            row(&[
                (0, DbType::STRING, b"u1"),
                (1, DbType::STRING, b"Alice"),
                (2, DbType::STRING, b"d1"),
            ]),
            row(&[
                (0, DbType::STRING, b"u2"),
                (1, DbType::STRING, b"Bob"),
                (2, DbType::STRING, b"d1"),
            ]),
            row(&[
                (0, DbType::STRING, b"u3"),
                (1, DbType::STRING, b"Carol"),
                (2, DbType::STRING, b"d2"),
            ]),
        ]
    }

    fn domains() -> Vec<Row> {
        vec![
            row(&[(0, DbType::STRING, b"d1"), (1, DbType::STRING, b"Acme")]),
            row(&[(0, DbType::STRING, b"d2"), (1, DbType::STRING, b"Globex")]),
        ]
    }

    fn rendered(rows: &[Row]) -> Vec<String> {
        rows.iter()
            .map(|r| {
                r.columns
                    .iter()
                    .map(|c| String::from_utf8_lossy(c.data.into_slice()).into_owned())
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect()
    }

    #[test]
    fn inner_join_pairs_rows_on_the_key() {
        let plan = two_relation_plan(JoinKind::Inner);
        let out = project(&plan, combine(&plan, &sets(vec![users(), domains()])).expect("join")).expect("project");
        assert_eq!(
            rendered(&out),
            vec!["Alice|Acme", "Bob|Acme", "Carol|Globex"]
        );
    }

    #[test]
    fn inner_join_drops_rows_with_no_match() {
        let plan = two_relation_plan(JoinKind::Inner);
        // Only d2 exists, so Alice and Bob have nothing to join to.
        let only_globex = vec![domains()[1].clone()];
        let out = project(
            &plan,
            combine(&plan, &sets(vec![users(), only_globex])).expect("join"),
        )
        .expect("project");
        assert_eq!(rendered(&out), vec!["Carol|Globex"]);
    }

    #[test]
    fn left_join_keeps_unmatched_rows_with_an_empty_right_side() {
        let plan = two_relation_plan(JoinKind::Left);
        let only_globex = vec![domains()[1].clone()];
        let out = project(
            &plan,
            combine(&plan, &sets(vec![users(), only_globex])).expect("join"),
        )
        .expect("project");
        assert_eq!(rendered(&out), vec!["Alice|", "Bob|", "Carol|Globex"]);
    }

    /// A one-to-many join must produce one output row per match, not one per
    /// left row.
    #[test]
    fn a_key_matching_several_rows_fans_out() {
        let plan = two_relation_plan(JoinKind::Inner);
        let mut ds = domains();
        // A second domain sharing d1's key.
        ds.push(row(&[
            (0, DbType::STRING, b"d1"),
            (1, DbType::STRING, b"Acme Two"),
        ]));
        let out = project(&plan, combine(&plan, &sets(vec![users(), ds])).expect("join")).expect("project");
        assert_eq!(out.len(), 5, "Alice and Bob each match two rows");
    }

    #[test]
    fn ordering_runs_over_the_combined_rows() {
        let mut plan = two_relation_plan(JoinKind::Inner);
        let name = plan.relations[0].fetch_columns[1].clone();
        plan.order_by = vec![(0, name, true)];
        let out = project(&plan, combine(&plan, &sets(vec![users(), domains()])).expect("join")).expect("project");
        assert_eq!(
            rendered(&out),
            vec!["Carol|Globex", "Bob|Acme", "Alice|Acme"]
        );
    }

    /// The second key decides the order within a tie on the first.
    #[test]
    fn ordering_over_several_keys_breaks_ties_with_the_later_ones() {
        let mut plan = two_relation_plan(JoinKind::Inner);
        let domain = plan.relations[1].fetch_columns[1].clone();
        let name = plan.relations[0].fetch_columns[1].clone();
        // Domain ascending, and within one domain the name descending. Alice
        // and Bob tie on Acme, so only the second key separates them.
        plan.order_by = vec![(1, domain, false), (0, name, true)];
        let out = project(&plan, combine(&plan, &sets(vec![users(), domains()])).expect("join")).expect("project");
        assert_eq!(
            rendered(&out),
            vec!["Bob|Acme", "Alice|Acme", "Carol|Globex"]
        );
    }

    #[test]
    fn paging_runs_after_the_join_not_before() {
        let mut plan = two_relation_plan(JoinKind::Inner);
        let name = plan.relations[0].fetch_columns[1].clone();
        plan.order_by = vec![(0, name, false)];
        plan.offset = Some(1);
        plan.limit = Some(1);
        let out = project(&plan, combine(&plan, &sets(vec![users(), domains()])).expect("join")).expect("project");
        assert_eq!(rendered(&out), vec!["Bob|Acme"]);
    }

    #[test]
    fn an_empty_left_side_yields_no_rows() {
        let plan = two_relation_plan(JoinKind::Inner);
        let out = project(
            &plan,
            combine(&plan, &sets(vec![Vec::new(), domains()])).expect("join"),
        )
        .expect("project");
        assert!(out.is_empty());
    }

    /// PostgreSQL's NULL placement, decided by the cell's type and never by
    /// its bytes.
    #[test]
    fn projected_sort_treats_null_as_larger_than_every_value() {
        use std::cmp::Ordering::*;
        let cell = |ty: DbType, bytes: &[u8]| Column {
            schema_id: 0,
            data: mb(bytes),
            column_type: ty,
        };
        let zero = cell(DbType::I64, &0i64.to_le_bytes());
        let minus = cell(DbType::I64, &(-5i64).to_le_bytes());
        // A matched row's NULL: zero bytes, typed NULL from its companion.
        let null_zeros = cell(DbType::NULL, &0i64.to_le_bytes());
        // An unmatched outer side: no bytes at all.
        let null_empty = cell(DbType::NULL, &[]);
        let ty = DbType::I64;
        for null in [&null_zeros, &null_empty] {
            assert_eq!(compare_projected(Some(null), Some(&zero), &ty, false), Greater);
            assert_eq!(compare_projected(Some(&zero), Some(null), &ty, false), Less);
            assert_eq!(compare_projected(Some(null), Some(&minus), &ty, true), Less);
            assert_eq!(compare_projected(Some(&minus), Some(null), &ty, true), Greater);
        }
        assert_eq!(compare_projected(Some(&null_zeros), Some(&null_empty), &ty, false), Equal);
        assert_eq!(compare_projected(None, Some(&zero), &ty, false), Greater);
        assert_eq!(compare_projected(Some(&minus), Some(&zero), &ty, false), Less);
        assert_eq!(compare_projected(Some(&minus), Some(&zero), &ty, true), Greater);

        // Text: '' is a value, smaller than other strings, and not a NULL.
        let empty = cell(DbType::STRING, b"");
        let a = cell(DbType::STRING, b"a");
        let null_text = cell(DbType::NULL, b"");
        let ty = DbType::STRING;
        assert_eq!(compare_projected(Some(&empty), Some(&a), &ty, false), Less);
        assert_eq!(compare_projected(Some(&null_text), Some(&empty), &ty, false), Greater);
        assert_eq!(compare_projected(Some(&null_text), Some(&empty), &ty, true), Less);
    }

    /// The unmatched rows of a LEFT JOIN sort last ascending, first descending.
    #[test]
    fn left_join_ordering_puts_unmatched_rows_where_postgres_does() {
        let mut plan = two_relation_plan(JoinKind::Left);
        let domain = plan.relations[1].fetch_columns[1].clone();
        let name = plan.relations[0].fetch_columns[1].clone();
        let only_globex = vec![domains()[1].clone()];
        plan.order_by = vec![(1, domain.clone(), false), (0, name.clone(), false)];
        let out = project(
            &plan,
            combine(&plan, &sets(vec![users(), only_globex.clone()])).expect("join"),
        )
        .expect("project");
        assert_eq!(rendered(&out), vec!["Carol|Globex", "Alice|", "Bob|"]);

        plan.order_by = vec![(1, domain, true), (0, name, false)];
        plan.limit = Some(2);
        let out = project(&plan, combine(&plan, &sets(vec![users(), only_globex])).expect("join")).expect("project");
        assert_eq!(rendered(&out), vec!["Alice|", "Bob|"]);
    }

    /// A left join whose right side is empty still returns every left row.
    #[test]
    fn left_join_against_an_empty_relation_keeps_every_left_row() {
        let plan = two_relation_plan(JoinKind::Left);
        let out = project(&plan, combine(&plan, &sets(vec![users(), Vec::new()])).expect("join")).expect("project");
        assert_eq!(rendered(&out), vec!["Alice|", "Bob|", "Carol|"]);
    }

    /// Statements the text-scan fast paths were checked against: quotes,
    /// parentheses, case, non-ASCII text and keywords as substrings.
    const SCAN_CORPUS: &[&str] = &[
        "UPDATE acc SET balance = 70 WHERE id = 10",
        "update acc set balance = 70 where id = 10",
        "SELECT a FROM t WHERE b IN (SELECT c FROM u)",
        "select a from t where b in (select c from u) order by a",
        "SELECT 'FROM' FROM t",
        "SELECT \"from\" FROM t WHERE x = ')'",
        "SELECT a\nFROM t\nWHERE x = 1\nORDER BY a",
        "INSERT INTO t (a, b) VALUES (1, 'x FROM y') RETURNING a",
        "SELECT (SELECT max(a) FROM t) FROM u WHERE EXISTS (SELECT 1 FROM v)",
        "SELECT ünïcödé, 'ß' FROM tëst WHERE naïve = 'FROM'",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "DELETE FROM t WHERE a = 1",
        "",
        "FROM",
        "from(",
        "SELECT a,FROM b",
    ];

    /// `find_keyword` as it was, uppercasing the whole text per call: the
    /// in-place comparison must answer exactly as it did.
    fn find_keyword_reference(sql: &str, keyword: &str, start: usize) -> Option<usize> {
        let upper = sql.to_ascii_uppercase();
        let bytes = upper.as_bytes();
        let kw = keyword.as_bytes();
        let mut quote: Option<u8> = None;
        let mut depth = 0usize;
        let mut i = start;
        while i + kw.len() <= bytes.len() {
            let c = bytes[i];
            match quote {
                Some(q) => {
                    if c == q {
                        quote = None;
                    }
                }
                None => {
                    if c == b'\'' || c == b'"' {
                        quote = Some(c);
                    } else if c == b'(' {
                        depth += 1;
                    } else if c == b')' {
                        depth = depth.saturating_sub(1);
                    } else if depth == 0
                        && &bytes[i..i + kw.len()] == kw
                        && (i == 0 || bytes[i - 1].is_ascii_whitespace() || bytes[i - 1] == b')' || bytes[i - 1] == b',')
                        && bytes.get(i + kw.len()).is_none_or(|n| n.is_ascii_whitespace() || *n == b'(')
                    {
                        return Some(i);
                    }
                }
            }
            i += 1;
        }
        None
    }

    /// `split_top_level` as it was, pushing every character.
    fn split_top_level_reference(list: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut current = String::new();
        let mut depth = 0usize;
        let mut quote: Option<char> = None;
        for c in list.chars() {
            match quote {
                Some(q) => {
                    current.push(c);
                    if c == q {
                        quote = None;
                    }
                }
                None => match c {
                    '\'' | '"' => {
                        quote = Some(c);
                        current.push(c);
                    }
                    '(' => {
                        depth += 1;
                        current.push(c);
                    }
                    ')' => {
                        depth = depth.saturating_sub(1);
                        current.push(c);
                    }
                    ',' if depth == 0 => out.push(std::mem::take(&mut current)),
                    _ => current.push(c),
                },
            }
        }
        if !current.trim().is_empty() {
            out.push(current);
        }
        out
    }

    #[test]
    fn split_top_level_answers_as_the_pushing_split_did() {
        let lists = [
            "a, b, c",
            "count(*), coalesce(a, 0) AS x, 'a,b', \"q,\"",
            "f(g(1, 2), 3), ', ', ü, 'ß,'",
            "a,,b,",
            ",",
            "",
            "  ",
            "(a, b",
            "a), b",
        ];
        for list in lists.iter().chain(SCAN_CORPUS) {
            assert_eq!(super::split_top_level(list), split_top_level_reference(list), "{list:?}");
        }
    }

    #[test]
    fn find_keyword_answers_as_the_uppercasing_scan_did() {
        for sql in SCAN_CORPUS {
            // Never an empty keyword: both versions index past the end for
            // one, and no caller passes it.
            for kw in ["FROM", "WHERE", "ORDER BY", "RETURNING", "SELECT", "from", "F"] {
                for start in [0, 1, 5, sql.len()] {
                    assert_eq!(
                        super::find_keyword(sql, kw, start),
                        find_keyword_reference(sql, kw, start),
                        "{sql:?} {kw:?} {start}"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod quoted_join_keys {
    use super::*;

    /// An ORM quotes every identifier it emits, so the join key arrives as
    /// `"users"."domain_id" = "domain"."id"`.
    ///
    /// Treating a leading `"` as a string literal made every sea-orm join fail
    /// with *"join condition must contain an equality between two columns"* —
    /// about a condition that is exactly that. In PostgreSQL single quotes
    /// delimit strings and double quotes delimit identifiers; the two are not
    /// interchangeable and a parser that conflates them rejects the ORM's normal
    /// output.
    #[test]
    fn a_quoted_qualified_column_is_a_join_key_not_a_literal() {
        let (left, right, extra) =
            split_on_condition(r#""users"."domain_id" = "domain"."id""#).expect("quoted key");
        assert_eq!(left, r#""users"."domain_id""#);
        assert_eq!(right, r#""domain"."id""#);
        assert!(extra.is_empty());
    }

    /// The unquoted form must keep working, and both must reach the same key.
    #[test]
    fn quoting_does_not_change_which_term_is_the_key() {
        let (l1, r1, e1) =
            split_on_condition(r#""u"."did" = "d"."id" AND "d"."nm" = 'x'"#).expect("quoted");
        let (l2, r2, e2) = split_on_condition("u.did = d.id AND d.nm = 'x'").expect("bare");

        assert_eq!(
            e1.len(),
            1,
            "the literal comparison is an extra, not the key"
        );
        assert_eq!(e2.len(), 1);
        assert!(l1.contains("did") && r1.contains("id"));
        assert!(l2.contains("did") && r2.contains("id"));
    }

    /// The property the double-quote check was reaching for: a comparison
    /// against a literal must never be mistaken for the join key, or the real
    /// key would be pushed down as a filter and the join would be silently
    /// wrong. Single quotes are what actually mark one.
    #[test]
    fn a_string_literal_is_still_never_the_key() {
        let (left, right, extra) =
            split_on_condition(r#""d"."nm" = 'admin' AND "u"."did" = "d"."id""#)
                .expect("literal first");
        assert!(
            left.contains("did") && right.contains("id"),
            "{} = {}",
            left,
            right
        );
        assert_eq!(extra.len(), 1);
        assert!(extra[0].contains("'admin'"));

        // An unqualified double-quoted value is not a column reference either,
        // so it cannot become a key by losing the literal test. It yields no
        // key at all, which the planner turns into a filtered cross join for an
        // inner join and refuses for an outer one.
        let (l, r, extra) = split_on_condition(r#""d"."nm" = "Alice""#).unwrap();
        assert!(
            l.is_empty() && r.is_empty(),
            "must not become a key: {} = {}",
            l,
            r
        );
        assert_eq!(extra.len(), 1);
    }
}

#[cfg(test)]
mod relation_size_tests {
    use super::*;

    fn field(name: &str, size: u32, order: u64) -> SchemaField {
        SchemaField::new(name.to_string(), DbType::I32, size, 0, order, false)
    }

    #[test]
    fn a_whole_relation_size_becomes_a_row_width() {
        let relations = vec![RelationRef {
            table: "case".to_string(),
            label: "c".to_string(),
            join: None,
        }];
        let schemas = vec![smallvec::smallvec![field("id", 4, 0), field("n", 8, 1)]];
        assert_eq!(
            rewrite_relation_size(
                "COALESCE(SUM(pg_column_size(c)), 0) AS total_bytes",
                &relations,
                &schemas
            ),
            "COALESCE(COUNT(*) * 12, 0) AS total_bytes"
        );
    }
}

#[cfg(test)]
mod constant_term_tests {
    use super::is_constant_term;

    /// A term of pure literals is constant even when the operator joining them
    /// is a word. `'' ILIKE '%x%'` is false for every row, and rejecting it as
    /// a column reference stopped a real application's query outright.
    #[test]
    fn a_word_operator_between_literals_is_still_constant() {
        for term in [
            "'' ILIKE '%4806fcdf%'",
            "'' LIKE '%x%'",
            "'a' IS NOT NULL",
            "1 = 1",
            "'a' = 'b' AND 'c' != 'd'",
            "TRUE",
            "'x' CONTAINS 'y'",
        ] {
            assert!(is_constant_term(term), "{} should be constant", term);
        }
    }

    /// The reason this is a list and not "does it look like a column": a
    /// misspelled name has to keep raising an error rather than being waved
    /// through as a constant that quietly matches nothing.
    #[test]
    fn a_name_is_not_constant() {
        for term in [
            "nam ILIKE '%x%'",
            "id = 1",
            "'a' = other_column",
            "lower('X') = 'x'",
        ] {
            assert!(!is_constant_term(term), "{} should not be constant", term);
        }
    }
}

#[cfg(test)]
mod join_bench {
    use super::*;
    use crate::memory_pool::MEMORY_POOL;

    fn field(name: &str, ty: DbType, write_order: u64) -> SchemaField {
        SchemaField::new(
            name.to_string(),
            ty.clone(),
            ty.get_size(),
            0,
            write_order,
            false,
        )
    }

    fn row(cells: &[(u64, i64)]) -> Row {
        let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
        for (schema_id, v) in cells {
            let mut m = MEMORY_POOL.acquire(8);
            m.into_slice_mut().copy_from_slice(&v.to_le_bytes());
            columns.push(Column {
                schema_id: crate::core::row::row::schema_id_of(*schema_id),
                data: m,
                column_type: DbType::I64,
            });
        }
        Row {
            position: 0,
            columns,
            length: 0,
        }
    }

    fn peak_rss_kb() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmHWM:"))
                    .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
            })
            .unwrap_or(0)
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// join_bench --nocapture`: a hash join of 100k x 100k rows, 1:1, and a
    /// 10k x 100k fan-out, timing `combine` + `project`.
    #[test]
    #[ignore]
    fn join_bench() {
        let schema_of = |name: &str| -> Option<SmallVec<[SchemaField; 20]>> {
            match name {
                "a" => Some(
                    [field("id", DbType::I64, 0), field("val", DbType::I64, 1)]
                        .into_iter()
                        .collect(),
                ),
                "b" => Some(
                    [
                        field("id", DbType::I64, 0),
                        field("a_id", DbType::I64, 1),
                        field("val", DbType::I64, 2),
                    ]
                    .into_iter()
                    .collect(),
                ),
                _ => None,
            }
        };
        let which = std::env::var("JOIN_BENCH_CASE").ok();
        let cases: [(&str, &str, u64, u64, u64); 3] = [
            ("inner_1to1", "a JOIN b ON b.a_id = a.id", 100_000, 100_000, 100_000),
            ("left_half", "a LEFT JOIN b ON b.a_id = a.id", 100_000, 100_000, 200_000),
            ("inner_fanout", "a JOIN b ON b.a_id = a.id", 10_000, 100_000, 10_000),
        ];
        for (name, from, left_n, right_n, key_space) in cases {
            if which.as_deref().is_some_and(|w| w != name) {
                continue;
            }
            let relations = parse_from_clause(from).unwrap();
            let plan = plan(
                relations,
                "a.id, a.val, b.val",
                "",
                None,
                None,
                Vec::new(),
                None,
                None,
                &schema_of,
            )
            .unwrap();
            let mut rng = fastrand::Rng::with_seed(7);
            let a: Vec<Row> = (0..left_n as i64).map(|i| row(&[(0, i), (1, i * 3)])).collect();
            let b: Vec<Row> = (0..right_n as i64)
                .map(|i| {
                    let key = if name == "inner_1to1" {
                        i
                    } else {
                        (rng.u64(..) % key_space) as i64
                    };
                    row(&[(0, i), (1, key), (2, i * 7)])
                })
                .collect();
            // The production form; `JOIN_BENCH_ROWS=1` keeps them decoded.
            let scanned = if std::env::var_os("JOIN_BENCH_ROWS").is_some() {
                vec![RowSet::Rows(a), RowSet::Rows(b)]
            } else {
                let sets = vec![RowSet::serialize(&a).unwrap(), RowSet::serialize(&b).unwrap()];
                drop(a);
                drop(b);
                sets
            };
            let base = peak_rss_kb();
            let mut times = Vec::new();
            let mut out_len = 0;
            for _ in 0..5 {
                let t = std::time::Instant::now();
                let out = project(&plan, combine(&plan, &scanned).unwrap()).unwrap();
                times.push(t.elapsed().as_secs_f64() * 1000.0);
                out_len = out.len();
            }
            times.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!(
                "join_bench case={} out={} min={:.1}ms median={:.1}ms peak_rss_delta={}MB",
                name,
                out_len,
                times[0],
                times[2],
                (peak_rss_kb().saturating_sub(base)) / 1024
            );
        }
    }
}

/// The hash join checked against a nested-loop reference that follows
/// PostgreSQL's outer-join rules directly, over generated relations with NULL
/// keys (typed NULL and companion-flagged), duplicate keys on both sides,
/// empty sides, composite keys, `ON` terms on a preserved side, and three-way
/// chains.
#[cfg(test)]
mod outer_join_reference {
    use super::*;
    use crate::memory_pool::MEMORY_POOL;

    fn field(name: &str, ty: DbType, write_order: u64) -> SchemaField {
        SchemaField::new(
            name.to_string(),
            ty.clone(),
            ty.get_size(),
            0,
            write_order,
            false,
        )
    }

    fn schema_of(name: &str) -> Option<SmallVec<[SchemaField; 20]>> {
        let fields = match name {
            "ta" => vec![
                field("id", DbType::I64, 0),
                field("k", DbType::I64, 1),
                field("__null_k", DbType::BOOL, 2),
            ],
            "tb" => vec![
                field("id", DbType::I64, 0),
                field("k", DbType::I64, 1),
                field("__null_k", DbType::BOOL, 2),
                field("s", DbType::STRING, 3),
            ],
            "tc" => vec![
                field("id", DbType::I64, 0),
                field("bk", DbType::I64, 1),
                field("__null_bk", DbType::BOOL, 2),
            ],
            _ => return None,
        };
        Some(fields.into_iter().collect())
    }

    fn mb(bytes: &[u8]) -> crate::memory_pool::MemoryBlock {
        let mut m = MEMORY_POOL.acquire(bytes.len());
        if !bytes.is_empty() {
            m.into_slice_mut().copy_from_slice(bytes);
        }
        m
    }

    /// A generated row: `id`, a key that may be NULL, and a text column.
    #[derive(Clone, Debug)]
    struct Gen {
        id: i64,
        key: Option<i64>,
        s: &'static str,
    }

    /// The row as the scan would hand it over. A NULL key is stored as its
    /// type's zeroes with the companion set — or, every third time, as a cell
    /// typed NULL, which is what a rebuilt row carries.
    fn scanned(rows: &[Gen], with_text: bool) -> Vec<Row> {
        rows.iter()
            .map(|g| {
                let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
                columns.push(Column {
                    schema_id: 0,
                    data: mb(&g.id.to_le_bytes()),
                    column_type: DbType::I64,
                });
                let typed_null = g.key.is_none() && g.id % 3 == 0;
                columns.push(Column {
                    schema_id: 1,
                    data: mb(&g.key.unwrap_or(0).to_le_bytes()),
                    column_type: if typed_null { DbType::NULL } else { DbType::I64 },
                });
                columns.push(Column {
                    schema_id: 2,
                    data: mb(&[u8::from(g.key.is_none())]),
                    column_type: DbType::BOOL,
                });
                if with_text {
                    columns.push(Column {
                        schema_id: 3,
                        data: mb(g.s.as_bytes()),
                        column_type: DbType::STRING,
                    });
                }
                Row {
                    position: 0,
                    columns,
                    length: 0,
                }
            })
            .collect()
    }

    fn generate(rng: &mut fastrand::Rng, n: usize, keys: i64, id_base: i64) -> Vec<Gen> {
        (0..n)
            .map(|i| Gen {
                id: id_base + i as i64,
                // Zero is a real key here, so a NULL stored as zeroes would
                // match it if NULL-ness were read from the bytes.
                key: if rng.u8(..) < 40 {
                    None
                } else {
                    Some(rng.i64(0..keys))
                },
                s: ["x", "y", ""][rng.usize(..3)],
            })
            .collect()
    }

    type Out = Vec<Vec<Option<i64>>>;

    fn render(rows: &[Row]) -> Out {
        let mut out: Out = rows
            .iter()
            .map(|r| {
                r.columns
                    .iter()
                    .map(|c| {
                        if c.column_type == DbType::NULL {
                            None
                        } else {
                            Some(i64::from_le_bytes(
                                c.data.into_slice().try_into().expect("8 bytes"),
                            ))
                        }
                    })
                    .collect()
            })
            .collect();
        out.sort();
        out
    }

    /// One left-deep outer-join step over tuples of row indices, by the
    /// definition: pair what matches, then pad what did not.
    fn reference_step(
        kind: JoinKind,
        tuples: Vec<Vec<Option<usize>>>,
        width: usize,
        right_len: usize,
        matches: impl Fn(&[Option<usize>], usize) -> bool,
    ) -> Vec<Vec<Option<usize>>> {
        let mut out = Vec::new();
        let mut right_hit = vec![false; right_len];
        for t in &tuples {
            let mut any = false;
            for r in 0..right_len {
                if matches(t, r) {
                    any = true;
                    right_hit[r] = true;
                    let mut e = t.clone();
                    e.push(Some(r));
                    out.push(e);
                }
            }
            if !any && matches!(kind, JoinKind::Left | JoinKind::Full) {
                let mut e = t.clone();
                e.push(None);
                out.push(e);
            }
        }
        if matches!(kind, JoinKind::Right | JoinKind::Full) {
            for (r, hit) in right_hit.iter().enumerate() {
                if !hit {
                    let mut e = vec![None; width];
                    e.push(Some(r));
                    out.push(e);
                }
            }
        }
        out
    }

    fn kind_sql(kind: JoinKind) -> &'static str {
        match kind {
            JoinKind::Inner => "JOIN",
            JoinKind::Left => "LEFT JOIN",
            JoinKind::Right => "RIGHT OUTER JOIN",
            JoinKind::Full => "FULL JOIN",
            JoinKind::Cross => unreachable!(),
        }
    }

    fn run(sql_from: &str, select: &str, rels: &[Vec<Row>]) -> Out {
        let plan = plan(
            parse_from_clause(sql_from).expect("from"),
            select,
            "",
            None,
            None,
            Vec::new(),
            None,
            None,
            &schema_of,
        )
        .unwrap_or_else(|e| panic!("plan {}: {}", sql_from, e));
        render(&project(&plan, combine(&plan, &sets(rels.to_vec())).expect("join")).expect("project"))
    }

    fn cell(rows: &[Gen], at: Option<usize>, f: impl Fn(&Gen) -> Option<i64>) -> Option<i64> {
        at.and_then(|i| f(&rows[i]))
    }

    #[test]
    fn two_way_joins_of_every_kind_match_the_definition() {
        let mut rng = fastrand::Rng::with_seed(0x0a17);
        let kinds = [JoinKind::Inner, JoinKind::Left, JoinKind::Right, JoinKind::Full];
        for round in 0..120 {
            let (na, nb) = match round % 8 {
                0 => (0, rng.usize(0..20)),
                1 => (rng.usize(0..20), 0),
                2 => (0, 0),
                _ => (rng.usize(1..40), rng.usize(1..40)),
            };
            let keys = [1i64, 3, 10][round % 3];
            let a = generate(&mut rng, na, keys, 0);
            // Ids overlap a's, so the composite variant has pairs to find.
            let b = generate(&mut rng, nb, keys, 0);
            let rels = vec![scanned(&a, false), scanned(&b, true)];
            for kind in kinds {
                // A single key, a composite key, and a single key with an ON
                // term on the joined relation (match-time for RIGHT/FULL).
                for variant in 0..3 {
                    if variant == 2 && !kind.nulls_earlier() {
                        // INNER/LEFT push that term into the scan, which this
                        // test does not run.
                        continue;
                    }
                    let on = match variant {
                        0 => "b.k = a.k",
                        1 => "a.k = b.k AND b.id = a.id",
                        _ => "a.k = b.k AND b.s = 'x'",
                    };
                    let from = format!("ta a {} tb b ON {}", kind_sql(kind), on);
                    let got = run(&from, "a.id, a.k, b.id, b.k", &rels);

                    let matches = |t: &[Option<usize>], r: usize| -> bool {
                        let Some(l) = t[0] else { return false };
                        let (x, y) = (&a[l], &b[r]);
                        let key = matches!((x.key, y.key), (Some(p), Some(q)) if p == q);
                        key && match variant {
                            0 => true,
                            1 => y.id == x.id,
                            _ => y.s == "x",
                        }
                    };
                    let start: Vec<Vec<Option<usize>>> = (0..a.len()).map(|i| vec![Some(i)]).collect();
                    let tuples = reference_step(kind, start, 1, b.len(), matches);
                    let mut want: Out = tuples
                        .iter()
                        .map(|t| {
                            let (l, r) = (t[0], t[1]);
                            vec![
                                cell(&a, l, |g| Some(g.id)),
                                cell(&a, l, |g| g.key),
                                cell(&b, r, |g| Some(g.id)),
                                cell(&b, r, |g| g.key),
                            ]
                        })
                        .collect();
                    want.sort();
                    assert_eq!(got, want, "round {} {:?} variant {}: {}", round, kind, variant, from);
                }
            }
        }
    }

    fn run_where(sql_from: &str, select: &str, where_clause: &str, rels: &[Vec<Row>]) -> Out {
        let plan = plan(
            parse_from_clause(sql_from).expect("from"),
            select,
            where_clause,
            None,
            None,
            Vec::new(),
            None,
            None,
            &schema_of,
        )
        .unwrap_or_else(|e| panic!("plan {} WHERE {}: {}", sql_from, where_clause, e));
        render(&project(&plan, combine(&plan, &sets(rels.to_vec())).expect("join")).expect("project"))
    }

    /// The pre-rewrite `project`'s sort, DISTINCT and paging, over a
    /// materialised `Vec<Row>` — the reference `project_serialized` must
    /// reproduce row for row.
    fn project_reference(plan: &JoinPlan, combined: &Joined<'_>) -> Vec<Row> {
        let mut projector = Projector::new(plan);
        let mut out: Vec<Row> = combined.iter().map(|t| projector.row(t)).collect();
        let sort_keys: Vec<(usize, bool, DbType)> = plan
            .order_by
            .iter()
            .filter_map(|(rel, field, descending)| {
                let idx = plan
                    .items
                    .iter()
                    .position(|i| i.rel == *rel && i.field.write_order == field.write_order)?;
                Some((idx, *descending, field.db_type.clone()))
            })
            .collect();
        if !plan.order_exprs.is_empty() {
            if let Some(layout) = plan.order_layout.as_ref() {
                let directions: Vec<(bool, Option<bool>)> = plan
                    .order_exprs
                    .iter()
                    .map(|(_, d, n)| (*d, *n))
                    .collect();
                let companions = Companions::new(plan, &layout.columns);
                let mut keyed: Vec<(Vec<crate::core::rql::expr::Value>, Row)> = Vec::new();
                for (index, row) in out.into_iter().enumerate() {
                    let synthetic =
                        combined_row_with(plan, &layout.columns, &companions, combined.tuple(index));
                    let values = plan
                        .order_exprs
                        .iter()
                        .map(|(e, _, _)| {
                            crate::core::rql::expr::eval(e, &synthetic, &layout.schema)
                                .expect("a reference sort key")
                        })
                        .collect();
                    keyed.push((values, row));
                }
                keyed.sort_by(|a, b| {
                    crate::core::rql::expr::compare_key_values(&a.0, &b.0, &directions)
                });
                out = keyed.into_iter().map(|(_, r)| r).collect();
            }
        } else if !sort_keys.is_empty() {
            out.sort_by(|a, b| {
                for (idx, descending, ty) in sort_keys.iter() {
                    let ord =
                        compare_projected(a.columns.get(*idx), b.columns.get(*idx), ty, *descending);
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        if plan.distinct {
            let mut seen = std::collections::HashSet::new();
            out.retain(|row| {
                let mut key: Vec<u8> = Vec::new();
                for column in row.columns.iter() {
                    let bytes = column.data.into_slice();
                    key.push(column.column_type.to_byte());
                    key.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    key.extend_from_slice(bytes);
                }
                seen.insert(key)
            });
        }
        if let Some(off) = plan.offset {
            out = out.into_iter().skip(off as usize).collect();
        }
        if let Some(limit) = plan.limit {
            out.truncate(limit as usize);
        }
        out
    }

    fn exact(rows: &[Row]) -> Vec<Vec<(u64, u8, Vec<u8>)>> {
        rows.iter()
            .map(|r| {
                r.columns
                    .iter()
                    .map(|c| (u64::from(c.schema_id), c.column_type.to_byte(), c.data.into_slice().to_vec()))
                    .collect()
            })
            .collect()
    }

    /// `project_serialized` — sorting indices, DISTINCT by row hash, paging
    /// without copies — against the row-moving implementation it replaced, in
    /// order and byte for byte, over NULL keys, duplicate rows, ties, column
    /// and expression sorts, and every page shape.
    #[test]
    fn serialized_projection_matches_the_row_projection() {
        let mut rng = fastrand::Rng::with_seed(0x9a0e);
        let orders = [
            "",
            "a.k",
            "b.k DESC, a.id",
            "a.k + b.k DESC, a.id",
            "COALESCE(b.k, a.k) NULLS FIRST, b.id DESC",
        ];
        let pages = [(None, None), (Some(5u64), None), (Some(3), Some(2u64)), (None, Some(4)), (Some(0), None)];
        let mut checked = 0usize;
        for round in 0..30 {
            let keys = [1i64, 3, 6][round % 3];
            let na = rng.usize(0..30);
            let a = generate(&mut rng, na, keys, 0);
            let nb = rng.usize(0..30);
            let b = generate(&mut rng, nb, keys, 1000);
            let rels = sets(vec![scanned(&a, false), scanned(&b, true)]);
            for kind in ["JOIN", "LEFT JOIN", "FULL JOIN"] {
                let from = format!("ta a {} tb b ON b.k = a.k", kind);
                for (select, distinct) in [("a.id, a.k, b.id, b.k", false), ("a.k, b.k", true), ("b.k", true)] {
                    for order in orders {
                        for (limit, offset) in pages {
                            let mut sql = format!(
                                "SELECT {}{} FROM {}",
                                if distinct { "DISTINCT " } else { "" },
                                select,
                                from
                            );
                            if !order.is_empty() {
                                sql.push_str(&format!(" ORDER BY {}", order));
                            }
                            if let Some(l) = limit {
                                sql.push_str(&format!(" LIMIT {}", l));
                            }
                            if let Some(o) = offset {
                                sql.push_str(&format!(" OFFSET {}", o));
                            }
                            // A DISTINCT whose ORDER BY is not in the select
                            // list is refused, as in PostgreSQL.
                            let Ok(p) = plan_statement(&sql, &schema_of) else {
                                continue;
                            };
                            assert_eq!(p.distinct, distinct, "{}", sql);
                            let joined = combine(&p, &rels).expect("join");
                            let want = project_reference(&p, &joined);
                            let mut bytes = Vec::new();
                            project_serialized(&p, &joined, &mut bytes).expect("project");
                            let got = crate::core::row::row::vec_into_rows(&bytes).expect("rows");
                            assert_eq!(exact(&got), exact(&want), "round {}: {}", round, sql);
                            checked += 1;
                        }
                    }
                }
            }
        }
        // Every shape planned except the refused DISTINCT/ORDER BY pairs.
        assert!(checked > 30 * 3 * 3 * 2 * 5, "only {} shapes planned", checked);
    }

    /// `LEFT JOIN … WHERE r.k IS NULL` on the nullable join key — the
    /// presence-only probe — against the definition: every tuple of the
    /// ordinary join, then the WHERE applied to it. Chained with a further
    /// join of every kind, since a later RIGHT/FULL join is what makes the
    /// early drop unsound (and must fall back to the fan-out path).
    #[test]
    fn anti_joins_on_a_nullable_key_match_the_definition() {
        let mut rng = fastrand::Rng::with_seed(0xa471);
        let kinds = [JoinKind::Inner, JoinKind::Left, JoinKind::Right, JoinKind::Full];
        for round in 0..60 {
            let keys = [1i64, 3, 8][round % 3];
            let na = rng.usize(0..25);
            let a = generate(&mut rng, na, keys, 0);
            let nb = rng.usize(0..25);
            let b = generate(&mut rng, nb, keys, 1000);
            let nc = rng.usize(0..15);
            let c = generate(&mut rng, nc, keys, 2000);
            let rels2 = vec![scanned(&a, false), scanned(&b, true)];
            let rels3 = vec![scanned(&a, false), scanned(&b, true), scanned(&c, false)];
            let key_eq = |p: Option<i64>, q: Option<i64>| matches!((p, q), (Some(p), Some(q)) if p == q);

            // Two-way.
            let got = run_where("ta a LEFT JOIN tb b ON b.k = a.k", "a.id, a.k", "b.k IS NULL", &rels2);
            let start: Vec<Vec<Option<usize>>> = (0..a.len()).map(|i| vec![Some(i)]).collect();
            let t = reference_step(JoinKind::Left, start.clone(), 1, b.len(), |t, r| {
                t[0].is_some_and(|l| key_eq(a[l].key, b[r].key))
            });
            let mut want: Out = t
                .iter()
                .filter(|t| t[1].is_none_or(|r| b[r].key.is_none()))
                .map(|t| vec![cell(&a, t[0], |g| Some(g.id)), cell(&a, t[0], |g| g.key)])
                .collect();
            want.sort();
            assert_eq!(got, want, "round {} two-way", round);

            // Followed by a join of every kind onto `a`.
            for k2 in kinds {
                let from = format!(
                    "ta a LEFT JOIN tb b ON b.k = a.k {} tc c ON c.bk = a.k",
                    kind_sql(k2)
                );
                let got = run_where(&from, "a.id, c.id", "b.k IS NULL", &rels3);
                let t1 = reference_step(JoinKind::Left, start.clone(), 1, b.len(), |t, r| {
                    t[0].is_some_and(|l| key_eq(a[l].key, b[r].key))
                });
                let t2 = reference_step(k2, t1, 2, c.len(), |t, r| {
                    t[0].is_some_and(|l| key_eq(a[l].key, c[r].key))
                });
                let mut want: Out = t2
                    .iter()
                    .filter(|t| t[1].is_none_or(|r| b[r].key.is_none()))
                    .map(|t| vec![cell(&a, t[0], |g| Some(g.id)), cell(&c, t[2], |g| Some(g.id))])
                    .collect();
                want.sort();
                assert_eq!(got, want, "round {}: {}", round, from);
            }
        }
    }

    /// NULL-keyed rows are dropped in the scan of every side that is not
    /// preserved, and of no side that is.
    #[test]
    fn null_keys_are_filtered_only_on_unpreserved_sides() {
        let planned = |from: &str| {
            plan(
                parse_from_clause(from).expect("from"),
                "a.id",
                "",
                None,
                None,
                Vec::new(),
                None,
                None,
                &schema_of,
            )
            .unwrap_or_else(|e| panic!("plan {}: {}", from, e))
        };
        let filtered = |p: &JoinPlan, rel: usize| p.relations[rel].predicate.contains("__null_");
        for (from, want) in [
            ("ta a JOIN tb b ON b.k = a.k", [true, true]),
            ("ta a LEFT JOIN tb b ON b.k = a.k", [false, true]),
            ("ta a RIGHT JOIN tb b ON b.k = a.k", [false, false]),
            ("ta a FULL JOIN tb b ON b.k = a.k", [false, false]),
        ] {
            let p = planned(from);
            assert_eq!([filtered(&p, 0), filtered(&p, 1)], want, "{}", from);
        }
        // Three-way: c's key reads b; b is LEFT-joined (null-supplying) but the
        // INNER join onto it drops every tuple whose b key is NULL anyway.
        let p = planned("ta a LEFT JOIN tb b ON b.k = a.k JOIN tc c ON c.bk = b.k");
        assert_eq!(
            [filtered(&p, 0), filtered(&p, 1), filtered(&p, 2)],
            [false, true, true]
        );
        let p = planned("ta a LEFT JOIN tb b ON b.k = a.k LEFT JOIN tc c ON c.bk = b.k");
        assert_eq!(
            [filtered(&p, 0), filtered(&p, 1), filtered(&p, 2)],
            [false, true, true]
        );
    }

    #[test]
    fn three_way_chains_match_the_definition() {
        let mut rng = fastrand::Rng::with_seed(0x3a3);
        let kinds = [JoinKind::Inner, JoinKind::Left, JoinKind::Right, JoinKind::Full];
        for round in 0..40 {
            let keys = [2i64, 6][round % 2];
            let na = rng.usize(0..15);
            let a = generate(&mut rng, na, keys, 0);
            let nb = rng.usize(0..15);
            let b = generate(&mut rng, nb, keys, 1000);
            let nc = rng.usize(0..15);
            let c = generate(&mut rng, nc, keys, 2000);
            let rels = vec![scanned(&a, false), scanned(&b, true), scanned(&c, false)];
            for k1 in kinds {
                for k2 in kinds {
                    // c joins on b's key: when b is absent the key is NULL.
                    let from = format!(
                        "ta a {} tb b ON b.k = a.k {} tc c ON c.bk = b.k",
                        kind_sql(k1),
                        kind_sql(k2)
                    );
                    let got = run(&from, "a.id, b.id, c.id, c.bk", &rels);

                    let start: Vec<Vec<Option<usize>>> = (0..a.len()).map(|i| vec![Some(i)]).collect();
                    let t1 = reference_step(k1, start, 1, b.len(), |t, r| {
                        t[0].is_some_and(|l| matches!((a[l].key, b[r].key), (Some(p), Some(q)) if p == q))
                    });
                    let t2 = reference_step(k2, t1, 2, c.len(), |t, r| {
                        t[1].is_some_and(|m| matches!((b[m].key, c[r].key), (Some(p), Some(q)) if p == q))
                    });
                    let mut want: Out = t2
                        .iter()
                        .map(|t| {
                            vec![
                                cell(&a, t[0], |g| Some(g.id)),
                                cell(&b, t[1], |g| Some(g.id)),
                                cell(&c, t[2], |g| Some(g.id)),
                                cell(&c, t[2], |g| g.key),
                            ]
                        })
                        .collect();
                    want.sort();
                    assert_eq!(got, want, "round {}: {}", round, from);
                }
            }
        }
    }

    #[test]
    fn a_right_join_onto_an_empty_left_side_returns_every_right_row() {
        let b = vec![
            Gen { id: 1000, key: Some(0), s: "x" },
            Gen { id: 1001, key: None, s: "y" },
        ];
        let rels = vec![Vec::new(), scanned(&b, true)];
        let got = run("ta a RIGHT JOIN tb b ON b.k = a.k", "a.id, a.k, b.id, b.k", &rels);
        assert_eq!(
            got,
            vec![
                vec![None, None, Some(1000), Some(0)],
                vec![None, None, Some(1001), None],
            ]
        );
    }

    /// RIGHT and FULL plan where they used to be refused by name, and a
    /// `WHERE` term on a side they make nullable is still refused rather than
    /// pushed into that side's scan.
    #[test]
    fn right_and_full_plan_and_guard_their_null_supplying_sides() {
        for from in [
            "ta a RIGHT JOIN tb b ON b.k = a.k",
            "ta a RIGHT OUTER JOIN tb b ON b.k = a.k",
            "ta a FULL JOIN tb b ON b.k = a.k",
            "ta a FULL OUTER JOIN tb b ON b.k = a.k",
        ] {
            let refs = parse_from_clause(from).expect(from);
            assert_eq!(refs.len(), 2, "{}", from);
            assert_eq!(refs[1].table, "tb", "{}", from);
        }
        let planned = |from: &str, where_clause: &str| {
            plan(
                parse_from_clause(from).unwrap(),
                "a.id, b.id",
                where_clause,
                None,
                None,
                Vec::new(),
                None,
                None,
                &schema_of,
            )
        };
        // The preserved side of a RIGHT JOIN filters its own scan.
        let p = planned("ta a RIGHT JOIN tb b ON b.k = a.k", "b.id > 3").expect("preserved side");
        assert!(!p.relations[1].predicate.is_empty());
        // The null-supplying side may not.
        assert!(planned("ta a RIGHT JOIN tb b ON b.k = a.k", "a.id > 3").is_err());
        assert!(planned("ta a FULL JOIN tb b ON b.k = a.k", "a.id > 3").is_err());
        assert!(planned("ta a FULL JOIN tb b ON b.k = a.k", "b.id > 3").is_err());
        // An anti-join on a non-nullable column is still the one form allowed.
        let p = planned("ta a RIGHT JOIN tb b ON b.k = a.k", "a.id IS NULL").expect("anti-join");
        assert!(p.relations[0].require_unmatched);
        // A later RIGHT JOIN makes an earlier preserved side nullable too.
        assert!(planned(
            "ta a LEFT JOIN tb b ON b.k = a.k RIGHT JOIN tc c ON c.bk = b.k",
            "a.id > 3"
        )
        .is_err());
        // An ON term on a RIGHT JOIN's own relation is a match condition, not
        // a scan predicate.
        let p = planned("ta a RIGHT JOIN tb b ON b.k = a.k AND b.s = 'x'", "").expect("on term");
        assert!(p.relations[1].predicate.is_empty());
        assert!(p.relations[1].match_condition.is_some());
        let p = planned("ta a LEFT JOIN tb b ON b.k = a.k AND b.s = 'x'", "").expect("on term");
        assert!(!p.relations[1].predicate.is_empty());
        assert!(p.relations[1].match_condition.is_none());
    }
}
