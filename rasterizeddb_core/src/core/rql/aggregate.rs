//! Aggregate functions, `GROUP BY` and `HAVING`.
//!
//! Aggregation runs *after* the scan, as a fold over the rows it produced.
//! Nothing about storage changes: the planner adds the grouping keys and the
//! aggregate arguments to the set of columns fetched, and the rows come back
//! through the ordinary path. That keeps the whole feature out of the hot scan
//! loop, at the cost of materialising the matched rows — which the row-mode
//! path already does.
//!
//! Ordering is the one thing an aggregate query cannot delegate. `ORDER BY`
//! normally sorts the scanned rows inside the engine, but here it has to sort
//! *groups*, which do not exist until the fold has run. So an aggregate query
//! sorts its own output and never reaches the scan's sorter.

use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::row::row::{Column, Row};
use crate::core::row::schema::SchemaField;
use crate::core::value_codec::DECIMAL_SCALE;
#[cfg(test)]
use crate::core::value_codec;
use crate::memory_pool::{MEMORY_POOL, MemoryBlock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl AggFunc {
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name.to_ascii_uppercase().as_str() {
            "COUNT" => AggFunc::Count,
            "SUM" => AggFunc::Sum,
            "MIN" => AggFunc::Min,
            "MAX" => AggFunc::Max,
            "AVG" => AggFunc::Avg,
            _ => return None,
        })
    }

    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::Avg => "avg",
        }
    }

    /// The type this function produces over an argument of `arg`.
    ///
    /// Follows Postgres where it costs nothing: `COUNT` is always `bigint`, and
    /// `SUM`/`AVG` over an exact type stay exact rather than falling back to
    /// floating point, which would quietly lose cents on a money column.
    ///
    /// `SUM` is PostgreSQL's, exactly: `sum(int2)` and `sum(int4)` are
    /// `bigint`, `sum(int8)` and `sum(numeric)` are `numeric`, `sum(float4)` is
    /// `float4` and `sum(float8)` is `float8`. The engine's own widths map to
    /// the nearest PostgreSQL type — `I8`/`U8` to `int2`, `U16` to `int4`, so
    /// `bigint`; `U32` to `int8` and `U64`/`I128`/`U128` past it, so `numeric`.
    /// (`sum(bigint)` used to stay `bigint` here and raise `bigint out of
    /// range` past `i64`; a client decoding it as `numeric` now gets the exact
    /// total.) A `numeric` total past what the engine's `i128` holds is
    /// `numeric field overflow`.
    pub fn result_type(&self, arg: Option<&DbType>) -> DbType {
        match self {
            AggFunc::Count => DbType::I64,
            AggFunc::Min | AggFunc::Max => arg.cloned().unwrap_or(DbType::I64),
            AggFunc::Sum => match arg {
                Some(DbType::F32) => DbType::F32,
                Some(DbType::F64) => DbType::F64,
                Some(
                    DbType::I8
                    | DbType::U8
                    | DbType::I16
                    | DbType::U16
                    | DbType::I32
                    | DbType::BOOL,
                ) => DbType::I64,
                Some(_) => DbType::DECIMAL,
                // `COUNT(*)`'s shape; a `SUM` always has an argument.
                None => DbType::I64,
            },
            AggFunc::Avg => match arg {
                Some(DbType::F32) | Some(DbType::F64) => DbType::F64,
                _ => DbType::DECIMAL,
            },
        }
    }

    /// The scale a `DECIMAL` result of this function over `arg` is stored at.
    ///
    /// `SUM`/`MIN`/`MAX` fold the argument's raw integers and so stay at its
    /// scale: a `NUMERIC(p, s)` column's `s` (the engine's when it declares
    /// none), and `0` for an integer — `sum(int8)` is a `numeric` with no
    /// fraction, as in PostgreSQL. `AVG` divides at [`AVG_SCALE`], or at the
    /// argument's scale when that is wider. pgwire's `select_scales` renders by
    /// this rule — the two must agree or the value is printed a power of ten
    /// away from itself.
    pub fn decimal_output_scale(&self, arg: Option<&SchemaField>) -> u32 {
        let declared = match arg {
            Some(f) if f.db_type == DbType::DECIMAL => f.scale.unwrap_or(DECIMAL_SCALE),
            _ => 0,
        };
        match self {
            AggFunc::Avg => declared.max(AVG_SCALE),
            _ => declared,
        }
    }
}

/// The scale an exact `AVG` divides at.
///
/// PostgreSQL's `numeric` division keeps at least sixteen significant digits
/// (`select_div_scale`), which for an average between 1 and 9999 is sixteen
/// places: `avg(1, 2, 2)` is `1.6666666666666667`. Its scale then varies with
/// the quotient's magnitude — twenty places below 1, twelve from 10⁴, eight
/// from 10⁸ — while a column here has one scale, so this is the common case's.
pub const AVG_SCALE: u32 = 16;

/// An aggregate argument evaluated per row.
#[derive(Debug, Clone)]
pub struct ArgExpression {
    pub expr: crate::core::rql::expr::Expr,
    /// The columns it reads, so the scan fetches them and the evaluator has a
    /// schema to tell a NULL from a zero.
    pub schema: Vec<SchemaField>,
}

/// The `FILTER (WHERE …)` attached to one aggregate.
#[derive(Debug, Clone)]
pub struct AggregateFilter {
    /// A whole boolean tree, not one comparison.
    ///
    /// `FILTER (WHERE a IS NOT NULL AND a >= b)` is the ordinary shape, and a
    /// single-comparison parser refused it outright — while an `OR` parsed as
    /// one comparison whose right-hand side was the rest of the clause, and
    /// counted **nothing**. The second is the dangerous one: a number, silently
    /// wrong.
    pub condition: crate::core::rql::expr::Cond,
    /// The fields the condition reads, with their NULL companions. Carried
    /// because the evaluator needs a schema to tell a NULL from a zero, and the
    /// fold has no other access to one.
    pub schema: Vec<SchemaField>,
}

/// One item of the `SELECT` list of an aggregate query.
#[derive(Debug, Clone)]
pub enum SelectItem {
    /// A grouping key, echoed straight through.
    Column { field: SchemaField, label: String },
    /// An aggregate call. `arg` is `None` for `COUNT(*)`.
    Aggregate {
        func: AggFunc,
        arg: Option<SchemaField>,
        label: String,
        /// `FILTER (WHERE …)`: the rows this aggregate is allowed to see.
        ///
        /// One statement may hold several aggregates filtered differently —
        /// `COUNT(*), COUNT(*) FILTER (WHERE status = 'failed')` — so the test
        /// belongs to the item rather than to the statement's `WHERE`.
        filter: Option<AggregateFilter>,
        /// An argument that is computed rather than read:
        /// `SUM(CAST(NULLIF(data ->> 'y', '') AS NUMERIC))`.
        ///
        /// Evaluated per row instead of reading `arg`'s column. `arg` is still
        /// present and carries the *type* the expression yields, so the result
        /// type is decided the same way for both — and unlike the join planner,
        /// that type is known here at parse time, because the schema is.
        arg_expr: Option<ArgExpression>,
    },
    /// A constant: `SELECT 1`, or the value a scalar subquery resolved to. It
    /// reads no column, and is the same in every row. A `NULL` has no bytes
    /// and `db_type` `DbType::NULL`; read its described type from
    /// [`SelectItem::result_type`].
    Literal {
        bytes: Vec<u8>,
        db_type: DbType,
        label: String,
    },
    /// A nullary scalar function: `now()`, `gen_random_uuid()`.
    ///
    /// Positionally it behaves like a `Literal` — it occupies an output slot
    /// and reads no column — but it is evaluated **per row**, which is the
    /// whole reason it cannot be one. `INSERT INTO t (id, …) SELECT
    /// gen_random_uuid(), … FROM src` must put a distinct id in every row; a
    /// value computed once and reused would write the same key everywhere and
    /// report success.
    Scalar {
        func: ScalarFunc,
        db_type: DbType,
        label: String,
    },
    /// `ROW_NUMBER() OVER (…)` and friends.
    ///
    /// Computed after the scan over the rows that survived `WHERE`, so it needs
    /// no accumulator and no fold — it only occupies an output slot, like a
    /// literal does.
    Window {
        spec: crate::core::rql::window::WindowSpec,
        db_type: DbType,
        label: String,
    },
    /// An expression over the columns of one row: `a + b`, `COALESCE(a, 0)`.
    ///
    /// Evaluated per row, like [`SelectItem::Scalar`], but reading the row's
    /// own values. Distinct from [`SelectItem::Computed`], which reads finished
    /// aggregates instead and therefore cannot be evaluated until the fold has
    /// run.
    RowExpr {
        expr: crate::core::rql::expr::Expr,
        db_type: DbType,
        label: String,
    },
    /// An expression *over* aggregate results: `COALESCE(MAX(x), 0) + 1`.
    ///
    /// The aggregates it reads are pulled out into `inputs` and folded like any
    /// other, then the surrounding expression runs once per emitted group
    /// against a synthetic row whose columns are those results. That ordering is
    /// the whole point: the expression has to see the *finished* aggregate, so
    /// it cannot be evaluated per input row.
    Computed {
        expr: crate::core::rql::expr::Expr,
        /// The aggregate calls the expression reads. `Expr::Column` leaves
        /// address these by position, so the order here is load-bearing.
        inputs: Vec<ComputedInput>,
        db_type: DbType,
        label: String,
    },
}

/// One aggregate call feeding a [`SelectItem::Computed`] expression.
/// Name given to each synthetic column a computed expression reads. It never
/// collides with a real column: a schema cannot hold a name with this prefix.
pub(crate) const COMPUTED_INPUT_PREFIX: &str = "__agg_";

#[derive(Debug, Clone)]
pub struct ComputedInput {
    pub func: AggFunc,
    pub arg: Option<SchemaField>,
}

/// A scalar function that takes no arguments.
///
/// Deliberately nullary-only. A function reading column values needs per-row
/// expression evaluation, which is a larger change; refusing those keeps the
/// error honest rather than returning a plausible wrong answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunc {
    /// Current timestamp. Evaluated per row rather than per statement, so it
    /// does not yet carry PostgreSQL's transaction-timestamp semantics.
    Now,
    /// Today's date, as that day's midnight UTC.
    ///
    /// A `DATETIME` rather than a type of its own: a `DATE` column is stored as
    /// a timestamp at midnight here, so the two compare directly. Narrowing the
    /// clock is what makes `on_day < CURRENT_DATE` mean the day and not the
    /// instant — the full timestamp would put every row recorded earlier today
    /// on the wrong side of it.
    CurrentDate,
    /// A fresh random UUID for each row.
    GenRandomUuid,
    /// PostgreSQL 18's `uuidv7()`: a time-ordered UUID, monotonic within this
    /// process. See [`uuid_v7`].
    UuidV7,
}

/// The last `uuidv7()` timestamp handed out, in nanoseconds since the epoch.
static UUIDV7_LAST_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The smallest step between two `uuidv7()` values: one unit of the 12-bit
/// sub-millisecond field, 1 ms / 4096, rounded up — PostgreSQL's
/// `SUBMS_MINIMAL_STEP_NS`.
const UUIDV7_MIN_STEP_NS: u64 = 1_000_000 / 4096 + 1;

/// A version 7 UUID (RFC 9562), generated the way PostgreSQL 18 does it.
///
/// 48 bits of Unix milliseconds, the version, then RFC 9562's "method 3": the
/// 12-bit `rand_a` field holds the sub-millisecond fraction of a nanosecond
/// clock, so values from one process sort in generation order even within a
/// millisecond. When the clock has not advanced past the last value issued —
/// two calls in the same 244 ns, or the clock stepping backwards — the
/// timestamp is the previous one plus one step instead. The last timestamp is a
/// process-wide atomic, so the order holds across threads, where PostgreSQL's
/// holds per backend. The remaining 62 bits are cryptographically random.
pub fn uuid_v7() -> [u8; 16] {
    use std::sync::atomic::Ordering;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut prev = UUIDV7_LAST_NS.load(Ordering::Relaxed);
    let ns = loop {
        let next = now.max(prev + UUIDV7_MIN_STEP_NS);
        match UUIDV7_LAST_NS.compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break next,
            Err(seen) => prev = seen,
        }
    };
    uuid_v7_at(ns)
}

/// The UUID for timestamp `ns`, with fresh random bits.
fn uuid_v7_at(ns: u64) -> [u8; 16] {
    let ms = ns / 1_000_000;
    // The fraction of the millisecond, scaled to 12 bits.
    let sub_ms = (((ns % 1_000_000) * 4096) / 1_000_000) as u16;
    let mut b = [0u8; 16];
    crate::auth::fill_random(&mut b[8..]);
    b[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
    b[6] = 0x70 | (sub_ms >> 8) as u8;
    b[7] = sub_ms as u8;
    b[8] = (b[8] & 0x3f) | 0x80;
    b
}

/// `uuid_extract_version(uuid)`: the version nibble, or `None` for a UUID
/// whose variant is not RFC 9562's, as PostgreSQL answers `NULL`.
pub fn uuid_extract_version(u: &[u8; 16]) -> Option<i16> {
    ((u[8] & 0xc0) == 0x80).then_some((u[6] >> 4) as i16)
}

/// `uuid_extract_timestamp(uuid)`: microseconds since the Unix epoch for a
/// version 1 or 7 UUID, `None` for any other. PostgreSQL reads only the
/// millisecond field of a v7, so the sub-millisecond bits are not added back.
pub fn uuid_extract_timestamp_us(u: &[u8; 16]) -> Option<i64> {
    match uuid_extract_version(u)? {
        7 => {
            let mut ms = [0u8; 8];
            ms[2..].copy_from_slice(&u[..6]);
            Some(u64::from_be_bytes(ms) as i64 * 1000)
        }
        1 => {
            // 60-bit count of 100 ns intervals since 1582-10-15.
            let time_low = u32::from_be_bytes([u[0], u[1], u[2], u[3]]) as u64;
            let time_mid = u16::from_be_bytes([u[4], u[5]]) as u64;
            let time_hi = (u16::from_be_bytes([u[6], u[7]]) & 0x0fff) as u64;
            let ticks = (time_hi << 48) | (time_mid << 32) | time_low;
            const GREGORIAN_TO_UNIX_TICKS: i64 = 122_192_928_000_000_000;
            Some((ticks as i64 - GREGORIAN_TO_UNIX_TICKS) / 10)
        }
        _ => None,
    }
}

impl ScalarFunc {
    pub fn parse(name: &str) -> Option<(ScalarFunc, DbType)> {
        match name.trim().to_ascii_uppercase().as_str() {
            "NOW" | "CURRENT_TIMESTAMP" => Some((ScalarFunc::Now, DbType::DATETIME)),
            "CURRENT_DATE" => Some((ScalarFunc::CurrentDate, DbType::DATETIME)),
            "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" | "UUIDV4" => {
                Some((ScalarFunc::GenRandomUuid, DbType::UUID))
            }
            "UUIDV7" => Some((ScalarFunc::UuidV7, DbType::UUID)),
            _ => None,
        }
    }

    /// The value for one row, encoded the way the column type stores it.
    pub fn evaluate(&self) -> Vec<u8> {
        match self {
            ScalarFunc::Now => {
                let micros = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as i64)
                    .unwrap_or(0);
                micros.to_le_bytes().to_vec()
            }
            ScalarFunc::CurrentDate => {
                let micros = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as i64)
                    .unwrap_or(0);
                const DAY: i64 = 86_400_000_000;
                // `rem_euclid` rather than `%`: a pre-epoch instant has a
                // negative remainder, which would round the day *up* and put
                // midnight in the future.
                (micros - micros.rem_euclid(DAY)).to_le_bytes().to_vec()
            }
            ScalarFunc::GenRandomUuid => {
                // Version 4, variant 1, per RFC 4122 — the shape a client
                // parses back as a UUID rather than as sixteen loose bytes.
                crate::core::value_codec::random_uuid_v4().to_vec()
            }
            ScalarFunc::UuidV7 => uuid_v7().to_vec(),
        }
    }
}

impl SelectItem {
    pub fn label(&self) -> &str {
        match self {
            SelectItem::Column { label, .. } => label,
            SelectItem::Aggregate { label, .. } => label,
            SelectItem::Literal { label, .. } => label,
            SelectItem::Scalar { label, .. } => label,
            SelectItem::Computed { label, .. } => label,
            SelectItem::RowExpr { label, .. } => label,
            SelectItem::Window { label, .. } => label,
        }
    }

    pub fn result_type(&self) -> DbType {
        match self {
            SelectItem::Column { field, .. } => field.db_type.clone(),
            SelectItem::Aggregate { func, arg, .. } => {
                func.result_type(arg.as_ref().map(|f| &f.db_type))
            }
            // A `NULL` constant's bytes are typed `NULL` so every cell made
            // from them is one; what it is described as is PostgreSQL's type
            // for an untyped NULL, which is text.
            SelectItem::Literal {
                db_type: DbType::NULL,
                ..
            } => DbType::STRING,
            SelectItem::Literal { db_type, .. } => db_type.clone(),
            SelectItem::Scalar { db_type, .. } => db_type.clone(),
            SelectItem::Computed { db_type, .. } => db_type.clone(),
            SelectItem::RowExpr { db_type, .. } => db_type.clone(),
            SelectItem::Window { db_type, .. } => db_type.clone(),
        }
    }
}

/// Resolves an `ORDER BY` term against the labels a fold will produce.
///
/// A grouped query can only sort by something it selected — the input rows are
/// gone once the fold has run — so the term has to name an output item. Three
/// ways it can, in order of precision:
///
/// 1. the item's label, which is how an alias is written;
/// 2. the aggregate call itself, matched on **function and argument** —
///    `ORDER BY SUM(a.slow_calls)` against `SUM(a.slow_calls)::bigint AS
///    slow_calls`. The cast makes that item [`SelectItem::Computed`] rather
///    than an `Aggregate`, so a matcher that only knew labels and bare
///    aggregates refused a statement whose select list plainly carried the
///    term. Computed items are searched through their `inputs`, which is
///    where the aggregates a wrapping expression reads are kept;
/// 3. the function alone, ignoring the argument — kept last because it is
///    ambiguous when a statement sums two different columns, and only reached
///    when nothing more precise matched.
pub fn resolve_order_label(token: &str, items: &[SelectItem]) -> Option<String> {
    let bare = token.rsplit('.').next().unwrap_or(token).trim_matches('"');
    if let Some(item) = items.iter().find(|i| i.label().eq_ignore_ascii_case(bare)) {
        return Some(item.label().to_string());
    }

    let call = token.trim();
    let open = call.find('(')?;
    let func = AggFunc::parse(call[..open].trim())?;
    let close = call.rfind(')')?;
    let argument = call[open + 1..close].trim();
    // `COUNT(*)` and `COUNT()` read no column.
    let wanted: Option<String> = if argument.is_empty() || argument == "*" {
        None
    } else {
        Some(
            argument
                .rsplit('.')
                .next()
                .unwrap_or(argument)
                .trim_matches('"')
                .to_ascii_lowercase(),
        )
    };

    let same_arg = |arg: &Option<SchemaField>| match (&wanted, arg) {
        (None, None) => true,
        (Some(name), Some(field)) => field.name.eq_ignore_ascii_case(name),
        _ => false,
    };

    let matched = items.iter().find(|item| match item {
        SelectItem::Aggregate { func: f, arg, .. } => *f == func && same_arg(arg),
        SelectItem::Computed { inputs, .. } => inputs
            .iter()
            .any(|input| input.func == func && same_arg(&input.arg)),
        _ => false,
    });
    if let Some(item) = matched {
        return Some(item.label().to_string());
    }

    // Argument-blind fallback.
    items
        .iter()
        .find(|item| match item {
            SelectItem::Aggregate { func: f, .. } => *f == func,
            SelectItem::Computed { inputs, .. } => inputs.iter().any(|i| i.func == func),
            _ => false,
        })
        .map(|item| item.label().to_string())
}

/// A `HAVING` predicate: a boolean over aggregate comparisons. Parsed and
/// evaluated in [`crate::core::rql::having`]; each aggregate it reads is
/// folded per group alongside the select list's.
pub use crate::core::rql::having::Having;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HavingOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

impl HavingOp {
    pub fn parse(op: &str) -> Option<Self> {
        Some(match op {
            "=" | "==" => HavingOp::Eq,
            "!=" | "<>" => HavingOp::Ne,
            ">" => HavingOp::Gt,
            ">=" => HavingOp::Ge,
            "<" => HavingOp::Lt,
            "<=" => HavingOp::Le,
            _ => return None,
        })
    }

    pub(crate) fn holds(&self, ordering: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::*;
        match self {
            HavingOp::Eq => ordering == Equal,
            HavingOp::Ne => ordering != Equal,
            HavingOp::Gt => ordering == Greater,
            HavingOp::Ge => ordering != Less,
            HavingOp::Lt => ordering == Less,
            HavingOp::Le => ordering != Greater,
        }
    }
}

/// A grouping key that is not a stored column: `GROUP BY snapshot_at` where
/// `snapshot_at` labels `date_trunc('minute', created_at)`, or the expression
/// written out in the `GROUP BY` itself. No scanned column carries its bytes,
/// so the fold evaluates `expr` against each row to make the key.
#[derive(Debug, Clone)]
pub struct ComputedKey {
    /// The virtual `write_order` the synthetic field in
    /// [`AggregatePlan::group_by`] carries. Above every real field's, so it
    /// can never shadow a scanned column.
    pub write_order: u64,
    pub expr: crate::core::rql::expr::Expr,
    /// The fields the expression reads, with their NULL companions — carried
    /// for the same reason as [`AggregateFilter::schema`]: the fold has no
    /// schema of its own to tell a NULL from a zero.
    pub schema: Vec<SchemaField>,
    /// Index into [`AggregatePlan::items`] of the select item this key echoes,
    /// when the key came from (or matches) a select-list expression. The emit
    /// phase fills that item's output slot with the group's key bytes.
    pub item: Option<usize>,
}

/// Everything the fold needs, resolved against the schema at parse time.
#[derive(Debug, Clone)]
pub struct AggregatePlan {
    pub items: Vec<SelectItem>,
    pub group_by: Vec<SchemaField>,
    /// The entries of `group_by` that are computed rather than read — matched
    /// to their synthetic field by `write_order`.
    pub computed_keys: Vec<ComputedKey>,
    pub having: Option<Having>,
    /// Labels to sort the output by, each as `(label, descending,
    /// nulls_first)`. Later keys only break the ties the earlier ones left.
    /// `nulls_first` is the explicit `NULLS FIRST`/`LAST`; `None` is
    /// PostgreSQL's default, where a NULL sorts as larger than every value.
    pub order_by: Vec<(String, bool, Option<bool>)>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// The single row [`fold`] would have produced for a count-only plan.
///
/// Deliberately here rather than at the call site: it has to agree with the
/// emit loop in `fold` about the output row's shape — one column per item,
/// `schema_id` being the item's index, `Acc::Count`'s little-endian `i64` — and
/// the two are only checkable against each other if they can be read together.
/// `a_counted_plan_matches_what_the_fold_would_emit` pins the agreement.
///
/// One row always, never zero: `SELECT COUNT(*)` over an empty table is `0`,
/// not an empty result. That asymmetry with `GROUP BY` — which yields no row
/// for no input — is why this cannot be folded into the grouped path.
pub fn row_count_result(plan: &AggregatePlan, count: u64) -> Vec<Row> {
    let bytes = crate::core::value_codec::count_as_bigint(count)
        .to_le_bytes()
        .to_vec();
    let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
    for (i, _) in plan.items.iter().enumerate() {
        columns.push(Column {
            schema_id: crate::core::row::row::schema_id_of(i as u64),
            data: mb_from(&bytes),
            column_type: DbType::I64,
        });
    }
    vec![Row {
        position: 0,
        columns,
        length: 0,
    }]
}

/// True when the plan's whole answer is "how many rows matched".
///
/// Such a plan needs no column value from any row, so the scan can count
/// matches and never build one — see
/// [`crate::core::processor::concurrent_processor::ConcurrentProcessor::process_count`].
/// The ordinary path materialises every matching row into a `Vec<Row>` before
/// folding, which for a million-row count is ~1.5 GB of `Row` to produce a
/// single integer.
///
/// Every condition here is load-bearing, and the traps are that several of them
/// are invisible in the item's `func` and `arg` alone:
///
///  * **`filter` must be `None`.** `COUNT(*) FILTER (WHERE status = 'failed')`
///    has `arg: None` exactly like a bare `COUNT(*)`, so testing only the
///    function and the argument admits it — and then it returns the count of
///    *every* matched row. That is a plausible number on a dashboard rather
///    than an error, which is the worst failure this code can have.
///  * **`arg` must be `None`.** `COUNT(col)` skips NULLs, so it needs the
///    column. `arg_expr` is excluded with it: a computed argument is never a
///    `Count`, but the pairing is what makes that safe to rely on.
///  * **No `group_by`, `computed_keys` or `having`.** Each of those reads
///    values — to form a key, to compute one, or to decide which groups
///    survive.
///  * **At least one item, and every item an aggregate.** A `Column`,
///    `Literal` or `ScalarFunc` in the select list occupies an output slot this
///    path cannot fill, and an empty plan has no answer to give.
///
///  * **`limit` and `offset` must be absent.** They shape the *aggregated*
///    output — one row here — and the fast path returns straight from the
///    executor without the paging the fold's caller would have applied. So
///    `SELECT COUNT(*) FROM t LIMIT 0` emitted its row and answered a count
///    where the correct reply is no rows at all, and `OFFSET 1` did the same.
///    Rejecting them costs nothing: they fall to the ordinary path, which
///    already handles them, and nobody writes `COUNT(*) … LIMIT 0` for speed.
///    `LIMIT 0` in particular is not exotic — drivers issue it to fetch column
///    metadata without rows.
///
/// `order_by` is not tested, and does not need to be: it orders the aggregated
/// output, and one row is already in order.
///
/// The shape of the mistake is worth remembering — this guard is an allowlist
/// over a struct, so every field of [`AggregatePlan`] it does not read is a way
/// for a query to take a path that ignores it.
pub fn is_row_count_only(plan: &AggregatePlan) -> bool {
    plan.group_by.is_empty()
        && plan.computed_keys.is_empty()
        && plan.having.is_none()
        && plan.limit.is_none()
        && plan.offset.is_none()
        && !plan.items.is_empty()
        && plan.items.iter().all(|item| {
            matches!(
                item,
                SelectItem::Aggregate {
                    func: AggFunc::Count,
                    arg: None,
                    filter: None,
                    arg_expr: None,
                    ..
                }
            )
        })
}

/// A running accumulator for one aggregate within one group.
#[derive(Debug, Clone)]
pub(crate) enum Acc {
    Count(i64),
    /// Exact running total plus the count, for `SUM` and `AVG` over exact
    /// types. `scaled` records whether the input was already in `DECIMAL`
    /// units, which decides whether `AVG` has to scale before dividing.
    ExactSum {
        total: i128,
        n: i64,
        scaled: bool,
        /// The scale `total` is at when `scaled`: the column's declared one.
        scale: u32,
        /// Set once the total has left the range the accumulator can hold —
        /// an `I128` sum past `i128`, or a `U128` value above `i128::MAX`.
        /// Reported by [`Acc::finish`] as an error. A saturated or wrapped
        /// total is a plausible number, and that is the failure to avoid.
        overflow: bool,
        /// PostgreSQL's display scale of the total — the largest of its
        /// inputs': the declared `s` of a `NUMERIC(p, s)`, `0` for an integer.
        /// `AVG`'s result scale depends on it (`select_div_scale`).
        dscale: u32,
        /// For an unconstrained `NUMERIC`, whose inputs' display scales are
        /// not recorded, `dscale` is learned from the values: the fewest
        /// places each needs. (PostgreSQL keeps a written `1.50` at two; here
        /// it counts as one — the one place this can print fewer digits.)
        track_dscale: bool,
    },
    FloatSum {
        total: f64,
        n: i64,
    },
    /// `MIN`/`MAX` keep the winning value's raw bytes, so the result has the
    /// argument's own type without a decode/encode round trip.
    Extreme {
        best: Option<Vec<u8>>,
        ty: DbType,
        want_max: bool,
    },
}

impl Acc {
    pub(crate) fn new(func: AggFunc, arg: Option<&SchemaField>) -> Self {
        match func {
            AggFunc::Count => Acc::Count(0),
            AggFunc::Min | AggFunc::Max => Acc::Extreme {
                best: None,
                ty: arg.map(|f| f.db_type.clone()).unwrap_or(DbType::I64),
                want_max: func == AggFunc::Max,
            },
            AggFunc::Sum | AggFunc::Avg => match arg.map(|f| &f.db_type) {
                Some(DbType::F32) | Some(DbType::F64) => Acc::FloatSum { total: 0.0, n: 0 },
                other => {
                    let scaled = matches!(other, Some(DbType::DECIMAL));
                    let declared = arg.and_then(|f| f.scale);
                    Acc::ExactSum {
                        total: 0,
                        n: 0,
                        scaled,
                        scale: declared.unwrap_or(DECIMAL_SCALE),
                        overflow: false,
                        dscale: if scaled { declared.unwrap_or(0) } else { 0 },
                        track_dscale: scaled && declared.is_none() && func == AggFunc::Avg,
                    }
                }
            },
        }
    }

    pub(crate) fn accumulate(&mut self, bytes: Option<&[u8]>, ty: Option<&DbType>) {
        match self {
            // `COUNT(*)` counts rows; `COUNT(col)` counts rows where the column
            // has a value.
            Acc::Count(n) => {
                // `ty` is `None` only for `COUNT(*)`, which counts the row
                // whatever it holds. Otherwise the row counts when the column
                // has a value — and the empty string IS a value. Testing
                // `!b.is_empty()` instead made `COUNT(col)` skip `''` along
                // with NULL, so a column of empty strings counted as zero.
                if ty.is_none() || bytes.is_some() {
                    *n += 1;
                }
            }
            Acc::ExactSum {
                total,
                n,
                overflow,
                dscale,
                track_dscale,
                scale,
                ..
            } => {
                let (Some(b), Some(t)) = (bytes, ty) else {
                    return;
                };
                if *track_dscale {
                    if let Some(v) = decode_exact(b, t) {
                        // The fewest places this value needs at `scale`.
                        let mut places = *scale;
                        let mut m = v.unsigned_abs();
                        while places > 0 && m % 10 == 0 {
                            m /= 10;
                            places -= 1;
                        }
                        *dscale = (*dscale).max(if v == 0 { 0 } else { places });
                    }
                }
                // A `U128` above `i128::MAX` has no `i128`; it is an overflow
                // of the accumulator, never a negative number.
                if matches!(t, DbType::U128) && b.len() == 16 {
                    let v = u128::from_le_bytes(b.try_into().expect("sixteen bytes"));
                    match i128::try_from(v) {
                        Ok(v) => match total.checked_add(v) {
                            Some(sum) => *total = sum,
                            None => *overflow = true,
                        },
                        Err(_) => *overflow = true,
                    }
                    *n += 1;
                    return;
                }
                if let Some(v) = decode_exact(b, t) {
                    // Checked, not saturating: a saturated total is a number,
                    // and `finish` could not tell it from a real one.
                    match total.checked_add(v) {
                        Some(sum) => *total = sum,
                        None => *overflow = true,
                    }
                    *n += 1;
                }
            }
            Acc::FloatSum { total, n } => {
                let (Some(b), Some(t)) = (bytes, ty) else {
                    return;
                };
                if let Some(v) = decode_float(b, t) {
                    *total += v;
                    *n += 1;
                }
            }
            Acc::Extreme {
                best,
                ty: acc_ty,
                want_max,
            } => {
                let Some(b) = bytes else { return };
                if b.is_empty() {
                    return;
                }
                match best {
                    None => *best = Some(b.to_vec()),
                    Some(current) => {
                        let ord = compare_values(b, current, acc_ty);
                        let replace = if *want_max {
                            ord == std::cmp::Ordering::Greater
                        } else {
                            ord == std::cmp::Ordering::Less
                        };
                        // Overwritten in place: the winner changes often on
                        // unordered input, and a fresh allocation per change
                        // was most of what MIN/MAX cost.
                        if replace {
                            current.clear();
                            current.extend_from_slice(b);
                        }
                    }
                }
            }
        }
    }

    /// Adds `other` — the same accumulator over a disjoint set of rows — into
    /// this one, so a fold split across workers can be combined.
    ///
    /// Exact totals add checked, as `accumulate` does: an overflow in either
    /// part, or in the sum of the two, is reported by `finish`.
    pub(crate) fn merge(&mut self, other: &Acc) {
        match (self, other) {
            (Acc::Count(a), Acc::Count(b)) => *a += *b,
            (
                Acc::ExactSum {
                    total,
                    n,
                    overflow,
                    dscale,
                    ..
                },
                Acc::ExactSum {
                    total: t2,
                    n: n2,
                    overflow: o2,
                    dscale: d2,
                    ..
                },
            ) => {
                match total.checked_add(*t2) {
                    Some(sum) => *total = sum,
                    None => *overflow = true,
                }
                *n += *n2;
                *overflow |= *o2;
                *dscale = (*dscale).max(*d2);
            }
            (Acc::FloatSum { total, n }, Acc::FloatSum { total: t2, n: n2 }) => {
                *total += *t2;
                *n += *n2;
            }
            (
                Acc::Extreme {
                    best, ty, want_max, ..
                },
                Acc::Extreme { best: Some(b2), .. },
            ) => match best {
                None => *best = Some(b2.clone()),
                Some(current) => {
                    let ord = compare_values(b2, current, ty);
                    let replace = if *want_max {
                        ord == std::cmp::Ordering::Greater
                    } else {
                        ord == std::cmp::Ordering::Less
                    };
                    if replace {
                        current.clear();
                        current.extend_from_slice(b2);
                    }
                }
            },
            // An empty extreme adds nothing; mismatched kinds cannot occur for
            // folders built from one plan.
            _ => {}
        }
    }

    /// Like [`Acc::finish`], but reports SQL NULL as `None`.
    ///
    /// `finish` returns zero bytes for a `MIN`/`MAX` that saw no value, which
    /// reads back as the number 0. That is the wrong answer for
    /// `COALESCE(MAX(x), 0)` — the COALESCE would never fire, and a group whose
    /// values were all NULL would be indistinguishable from one whose maximum
    /// really is zero. Only the computed-expression path uses this; the existing
    /// aggregate output keeps its current shape.
    pub(crate) fn finish_nullable(&self, func: AggFunc, result_type: &DbType) -> Result<Option<Vec<u8>>, String> {
        match self {
            Acc::Extreme { best: None, .. } => Ok(None),
            // SUM and AVG over no rows at all are NULL in SQL; COUNT is not.
            Acc::ExactSum { n: 0, .. } | Acc::FloatSum { n: 0, .. }
                if !matches!(func, AggFunc::Count) =>
            {
                Ok(None)
            }
            _ => self.finish(func, result_type).map(Some),
        }
    }

    /// An exact `AVG` as PostgreSQL computes it: `numeric_div(sum, count)`,
    /// rounded half away from zero at the scale `select_div_scale` picks for
    /// *this* sum and count. Returns the unscaled value and that scale, or
    /// `None` for an average over no rows (SQL NULL), or for an accumulator
    /// that is not exact.
    ///
    /// `select_div_scale` (numeric.c): with the weight and leading base-10000
    /// digit of each operand, the quotient's weight is estimated as
    /// `weight1 - weight2`, one less when `firstdigit1 <= firstdigit2`; the
    /// scale is `16 - 4 * qweight`, at least either operand's display scale and
    /// within `[0, 1000]`. So the scale varies per value: `avg(1, 2, 2)` has
    /// sixteen places, `avg(0, 1)` twenty, an average near 10⁴ twelve.
    ///
    /// One bound of our own: the value travels as an `i128`, so the scale is
    /// capped at 38 and lowered further while the rounded quotient does not
    /// fit — an average below ~10⁻²² prints fewer places than PostgreSQL's.
    pub(crate) fn pg_avg(&self) -> Result<Option<(i128, u32)>, String> {
        let Acc::ExactSum {
            total,
            n,
            scaled,
            scale,
            overflow,
            dscale,
            ..
        } = self
        else {
            return Ok(None);
        };
        if *overflow {
            return Err(out_of_range(&DbType::DECIMAL));
        }
        if *n <= 0 {
            return Ok(None);
        }
        let from = if *scaled { *scale } else { 0 };
        let (w1, f1) = nbase_lead(total.unsigned_abs(), from);
        let (w2, f2) = nbase_lead(*n as u128, 0);
        let mut qweight = w1 - w2;
        if f1 <= f2 {
            qweight -= 1;
        }
        let rscale = (16 - 4 * qweight).max(*dscale as i32).clamp(0, 1000) as u32;
        let mut rscale = rscale.min(38);
        loop {
            if let Some(v) = div_at_scale(*total, *n as i128, rscale as i32 - from as i32) {
                return Ok(Some((v, rscale)));
            }
            if rscale == 0 {
                return Err(out_of_range(&DbType::DECIMAL));
            }
            rscale -= 1;
        }
    }

    /// Renders the accumulator as the bytes of `result_type`.
    ///
    /// An error when the exact answer does not fit `result_type`. Every exact
    /// path is checked: the total used to be cut to 64 bits with `as i64`, so
    /// `SUM` over a `BIGINT` column holding `i64::MAX` and `1` answered
    /// `-9223372036854775808`, and any `I128` total past `i64` lost its high
    /// half — wrong numbers, reported as success.
    fn finish(&self, func: AggFunc, result_type: &DbType) -> Result<Vec<u8>, String> {
        Ok(match self {
            Acc::Count(n) => n.to_le_bytes().to_vec(),
            Acc::ExactSum {
                total,
                n,
                scaled,
                scale,
                overflow,
                ..
            } => {
                if *overflow {
                    return Err(out_of_range(result_type));
                }
                match func {
                    AggFunc::Avg => {
                        // Averaging exact values stays exact: the division happens
                        // in scaled units so a money column keeps its cents.
                        if *n == 0 {
                            return encode_exact(0, result_type);
                        }
                        let value = if matches!(result_type, DbType::DECIMAL) {
                            // Divided at the output scale — `AVG_SCALE`, or the
                            // column's when wider — or the fractional digits
                            // the column cannot hold would be truncated away.
                            // An integer input is at scale 0.
                            let from = if *scaled { *scale } else { 0 };
                            let to = from.max(AVG_SCALE);
                            scaled_quotient(*total, *n as i128, to - from)
                                .ok_or_else(|| out_of_range(result_type))?
                        } else {
                            *total / (*n as i128)
                        };
                        return encode_exact(value, result_type);
                    }
                    _ => return encode_exact(*total, result_type),
                }
            }
            Acc::FloatSum { total, n } => {
                let v = match func {
                    AggFunc::Avg if *n > 0 => *total / (*n as f64),
                    AggFunc::Avg => 0.0,
                    _ => *total,
                };
                // `sum(float4)` is `float4`: four bytes, or a client reading
                // the column by its description gets the wrong width.
                if *result_type == DbType::F32 {
                    (v as f32).to_le_bytes().to_vec()
                } else {
                    v.to_le_bytes().to_vec()
                }
            }
            Acc::Extreme { best, ty, .. } => best
                .clone()
                .unwrap_or_else(|| vec![0u8; ty.get_size() as usize]),
        })
    }
}

/// The weight and leading digit of a non-negative `m / 10^scale` in
/// PostgreSQL's base-10000 representation: the weight is the base-10000
/// exponent of the first non-zero group, the digit that group's value. Zero is
/// `(0, 0)`, as `select_div_scale` treats it.
fn nbase_lead(m: u128, scale: u32) -> (i32, u32) {
    if m == 0 {
        return (0, 0);
    }
    let len = m.ilog10() as i32 + 1;
    // Decimal exponent of the leading digit, then the group holding it.
    let e = len - 1 - scale as i32;
    let w = e.div_euclid(4);
    let k = scale as i32 + 4 * w;
    let group = if k >= 0 {
        (m / 10u128.pow(k as u32)) % 10_000
    } else {
        // Only when the value has fewer than four digits in all.
        (m * 10u128.pow((-k) as u32)) % 10_000
    };
    (w, group as u32)
}

/// `a * 10^shift / b` rounded half away from zero, with `shift` of either
/// sign; `None` when the answer does not fit. `b` is a positive count.
///
/// A positive shift is long division one decimal digit at a time, so no
/// intermediate is wider than `10 * b`; a negative one divides by
/// `b * 10^-shift` directly.
fn div_at_scale(a: i128, b: i128, shift: i32) -> Option<i128> {
    let negative = a < 0;
    let magnitude = a.unsigned_abs();
    let b = b as u128;
    let q = if shift >= 0 {
        let mut q = magnitude / b;
        let mut r = magnitude % b;
        for _ in 0..shift {
            r *= 10;
            q = q.checked_mul(10)?.checked_add(r / b)?;
            r %= b;
        }
        if r * 2 >= b { q.checked_add(1)? } else { q }
    } else {
        let d = b.checked_mul(10u128.checked_pow((-shift) as u32)?)?;
        let q = magnitude / d;
        let r = magnitude % d;
        if r >= d - r { q + 1 } else { q }
    };
    let q = i128::try_from(q).ok()?;
    Some(if negative { -q } else { q })
}

/// `a * 10^shift / b`, rounded half away from zero, or `None` when the answer
/// does not fit an `i128`. `b` is a positive row count.
///
/// Split as `(a / b) * 10^shift + round((a % b) * 10^shift / b)`, so a total
/// whose `a * 10^shift` alone would overflow — any `bigint` sum past ~10²² at
/// sixteen places — still averages when the average itself fits. The
/// remainder is below `b` (< 2⁶³) and `10^shift` at most 10¹⁶ (< 2⁵⁴) for
/// the shifts `finish` asks for, so their product fits; anything wider is
/// reported as an overflow rather than approximated.
fn scaled_quotient(a: i128, b: i128, shift: u32) -> Option<i128> {
    let factor = 10i128.checked_pow(shift)?;
    let q = a / b;
    let r = a % b;
    let fraction = div_round_half_away(r.checked_mul(factor)?, b);
    q.checked_mul(factor)?.checked_add(fraction)
}

/// `a / b` rounded half away from zero, which is how PostgreSQL rounds a
/// `numeric` quotient. `b` is a positive row count.
fn div_round_half_away(a: i128, b: i128) -> i128 {
    let q = a / b;
    let r = a % b;
    // |r| >= b - |r|, compared without doubling `r` (which could overflow).
    if r.unsigned_abs() >= (b - r.abs()) as u128 {
        if a < 0 { q - 1 } else { q + 1 }
    } else {
        q
    }
}

/// PostgreSQL's wording for a value that leaves its type: `bigint out of
/// range`, `numeric field overflow`.
fn out_of_range(ty: &DbType) -> String {
    match ty {
        DbType::I64 => "bigint out of range".to_string(),
        DbType::DECIMAL => "numeric field overflow".to_string(),
        other => format!("{} out of range", other),
    }
}

/// An exact value as a signed 128-bit integer. `U128` above `i128::MAX` has
/// none, and is `None` here rather than the negative number `as i128` makes of
/// it — callers that must order or total such a value handle `U128` first.
fn decode_exact(bytes: &[u8], ty: &DbType) -> Option<i128> {
    fn le<const N: usize>(b: &[u8]) -> Option<[u8; N]> {
        if b.len() != N {
            return None;
        }
        let mut a = [0u8; N];
        a.copy_from_slice(b);
        Some(a)
    }
    Some(match ty {
        DbType::I8 => i8::from_le_bytes(le::<1>(bytes)?) as i128,
        DbType::I16 => i16::from_le_bytes(le::<2>(bytes)?) as i128,
        DbType::I32 => i32::from_le_bytes(le::<4>(bytes)?) as i128,
        DbType::I64 | DbType::DATETIME => i64::from_le_bytes(le::<8>(bytes)?) as i128,
        DbType::I128 | DbType::DECIMAL => i128::from_le_bytes(le::<16>(bytes)?),
        DbType::U8 | DbType::BOOL => u8::from_le_bytes(le::<1>(bytes)?) as i128,
        DbType::U16 => u16::from_le_bytes(le::<2>(bytes)?) as i128,
        DbType::U32 | DbType::CHAR => u32::from_le_bytes(le::<4>(bytes)?) as i128,
        DbType::U64 => u64::from_le_bytes(le::<8>(bytes)?) as i128,
        DbType::U128 => i128::try_from(u128::from_le_bytes(le::<16>(bytes)?)).ok()?,
        _ => return None,
    })
}

/// Encodes an exact result at `ty`, or reports that it does not fit.
fn encode_exact(v: i128, ty: &DbType) -> Result<Vec<u8>, String> {
    Ok(match ty {
        DbType::DECIMAL | DbType::I128 => v.to_le_bytes().to_vec(),
        DbType::U128 => u128::try_from(v)
            .map_err(|_| out_of_range(ty))?
            .to_le_bytes()
            .to_vec(),
        DbType::F64 => (v as f64).to_le_bytes().to_vec(),
        _ => i64::try_from(v)
            .map_err(|_| out_of_range(ty))?
            .to_le_bytes()
            .to_vec(),
    })
}

fn decode_float(bytes: &[u8], ty: &DbType) -> Option<f64> {
    match ty {
        DbType::F32 if bytes.len() == 4 => {
            Some(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as f64)
        }
        DbType::F64 if bytes.len() == 8 => {
            let mut a = [0u8; 8];
            a.copy_from_slice(bytes);
            Some(f64::from_le_bytes(a))
        }
        _ => decode_exact(bytes, ty).map(|v| v as f64),
    }
}

pub(crate) fn compare_values(a: &[u8], b: &[u8], ty: &DbType) -> std::cmp::Ordering {
    // Documents in PostgreSQL's `jsonb` order, as the ungrouped sort orders
    // them; the byte fallback below would order the encodings.
    if *ty == DbType::JSONB {
        return crate::core::jsonb::order(a, b);
    }
    // Decimals that carry their own scale (an `AVG` result) are compared as
    // numbers at a common scale; their bytes alone are not comparable.
    if *ty == DbType::DECIMAL && (a.len() == 17 || b.len() == 17) {
        if let (Some((x, sx)), Some((y, sy))) = (
            crate::core::value_codec::split_decimal_cell(a),
            crate::core::value_codec::split_decimal_cell(b),
        ) {
            let (sx, sy) = (sx.unwrap_or(DECIMAL_SCALE), sy.unwrap_or(DECIMAL_SCALE));
            let common = sx.max(sy);
            let lift = |v: i128, s: u32| 10i128.checked_pow(common - s).and_then(|f| v.checked_mul(f));
            return match (lift(x, sx), lift(y, sy)) {
                (Some(x), Some(y)) => x.cmp(&y),
                // Too large to lift exactly: the magnitudes decide.
                _ => (x as f64 / 10f64.powi(sx as i32))
                    .partial_cmp(&(y as f64 / 10f64.powi(sy as i32)))
                    .unwrap_or(std::cmp::Ordering::Equal),
            };
        }
    }
    // Ordered as itself: half of `u128` has no `i128`, and the byte fallback
    // below compares little-endian bytes, which is not numeric order.
    if let (DbType::U128, Ok(x), Ok(y)) = (ty, <[u8; 16]>::try_from(a), <[u8; 16]>::try_from(b)) {
        return u128::from_le_bytes(x).cmp(&u128::from_le_bytes(y));
    }
    if let (Some(x), Some(y)) = (decode_exact(a, ty), decode_exact(b, ty)) {
        return x.cmp(&y);
    }
    if let (Some(x), Some(y)) = (decode_float(a, ty), decode_float(b, ty)) {
        // PostgreSQL's float order — NaN is the largest value — so MAX over a
        // column holding NaN is NaN, and MIN/MAX are order-independent.
        return crate::core::processor::concurrent_processor::pg_float_cmp(x, y);
    }
    // STRING, UUID and INET are all ordered by their bytes.
    a.cmp(b)
}

fn mb_from(bytes: &[u8]) -> MemoryBlock {
    let mut mb = MEMORY_POOL.acquire(bytes.len());
    mb.into_slice_mut().copy_from_slice(bytes);
    mb
}

/// Reads one column's bytes out of a scanned row by its schema id.
fn column_bytes<'a>(row: &'a Row, write_order: u64) -> Option<&'a [u8]> {
    row.columns
        .iter()
        .find(|c| u64::from(c.schema_id) == write_order)
        .filter(|c| c.column_type != DbType::NULL)
        .map(|c| c.data.into_slice())
}

/// Where one column sits in the rows a fold reads, resolved once per plan.
///
/// The scan hands every row back with the same column layout, so a column's
/// position is a property of the plan, not of the row — finding it by a linear
/// `find` per row per lookup is the cost this removes. Nothing *guarantees*
/// that layout, though, so each row is checked against the layout the
/// positions were taken from (one pass over its schema ids) and a row that
/// differs falls back to the search. The positions are the *first* column
/// carrying each id, exactly as `find` would pick.
struct ColumnPositions {
    /// The schema ids of the row the positions were taken from, in order.
    layout: SmallVec<[u64; 16]>,
    /// The write orders asked for, and where each sat in `layout`.
    wanted: SmallVec<[u64; 16]>,
    at: SmallVec<[Option<usize>; 16]>,
}

impl ColumnPositions {
    fn new(wanted: SmallVec<[u64; 16]>) -> Self {
        ColumnPositions {
            layout: SmallVec::new(),
            at: SmallVec::from_elem(None, wanted.len()),
            wanted,
        }
    }

    /// Re-derives the positions from `row` when its layout is not the cached
    /// one. Returns nothing: afterwards `get` is valid for this row.
    #[inline]
    fn sync(&mut self, row: &Row) {
        let same = self.layout.len() == row.columns.len()
            && self
                .layout
                .iter()
                .zip(row.columns.iter())
                .all(|(id, c)| *id == u64::from(c.schema_id));
        if same {
            return;
        }
        self.layout.clear();
        self.layout.extend(row.columns.iter().map(|c| u64::from(c.schema_id)));
        for (slot, wo) in self.at.iter_mut().zip(self.wanted.iter()) {
            *slot = self.layout.iter().position(|id| id == wo);
        }
    }

    /// The bytes of wanted column `i` in `row` — `None` when the row lacks it
    /// or holds a NULL there, as [`column_bytes`] reports. Valid only after
    /// [`ColumnPositions::sync`] on the same row.
    #[inline]
    fn get<'a>(&self, row: &'a Row, i: usize) -> Option<&'a [u8]> {
        let c = row.columns.get(self.at[i]?)?;
        if c.column_type == DbType::NULL {
            None
        } else {
            Some(c.data.into_slice())
        }
    }
}

/// Splits a group key built by [`fold`] back into its parts: each is a
/// little-endian `u32` length then that many bytes, or `u32::MAX` alone for a
/// NULL. Only run once per group, when the group is created.
fn decode_key_parts(mut key: &[u8], parts: usize) -> Vec<Option<Vec<u8>>> {
    let mut out = Vec::with_capacity(parts);
    for _ in 0..parts {
        let len = u32::from_le_bytes([key[0], key[1], key[2], key[3]]);
        key = &key[4..];
        if len == u32::MAX {
            out.push(None);
        } else {
            let (part, rest) = key.split_at(len as usize);
            out.push(Some(part.to_vec()));
            key = rest;
        }
    }
    out
}

/// Folds scanned rows into grouped result rows.
///
/// The output rows are synthetic: each column's `schema_id` is its position in
/// the `SELECT` list, since an aggregate has no schema field to point at.
pub fn fold(plan: &AggregatePlan, rows: &[Row]) -> Result<Vec<Row>, String> {
    fold_with(plan, rows, true)
}

/// [`fold`], with the one-byte-key table switchable so the two paths can be
/// compared against each other.
fn fold_with(plan: &AggregatePlan, rows: &[Row], byte_table: bool) -> Result<Vec<Row>, String> {
    let mut folder = Folder::with_byte_table(plan, byte_table);
    for row in rows {
        folder.push(row)?;
    }
    folder.finish()
}

/// Where one grouping key's bytes come from.
enum KeySource<'p> {
    /// Evaluated against the row: no scanned column carries it.
    Computed(&'p ComputedKey),
    /// A scanned column, by its slot in `positions`.
    Column(usize),
}

/// A streaming `GROUP BY`: rows are pushed one at a time and the groups are
/// emitted by [`Folder::finish`].
///
/// Streaming is what lets a caller fold straight from a scan's serialized
/// bytes, decoding one row at a time, instead of first materialising every
/// matched row as a 1.5 KB `Row` — a million rows was 1.56 GB before the fold
/// read the first one. Two folders over disjoint parts of the input combine
/// with [`Folder::merge`]: every accumulator is mergeable.
///
/// Per group it keeps one encoded key (`Box<[u8]>`, decoded into its parts
/// only at emit) and a run of `stride` accumulators in one flat vector. The
/// index is a `HashTable<u32>` of group numbers, hashed on the key bytes. The
/// layout it replaces held each key three times — the map's key `Vec`, the
/// decoded parts with one allocation each, and the group tuple — plus three
/// accumulator `Vec`s per group.
pub struct Folder<'p> {
    plan: &'p AggregatePlan,
    /// A new group's accumulators: one per select item, then one per HAVING
    /// aggregate, then the inputs of each computed item.
    template: Vec<Acc>,
    stride: usize,
    n_items: usize,
    n_having: usize,
    /// For select item `i`, where its computed inputs start in a group's run.
    computed_at: Vec<usize>,
    key_sources: Vec<KeySource<'p>>,
    item_slots: Vec<Option<usize>>,
    having_slots: Vec<Option<usize>>,
    positions: ColumnPositions,
    /// A single grouping column one byte wide — `BOOL`, `I8`, `U8` — has at
    /// most 257 keys: its 256 values and NULL. Those are found by indexing a
    /// table rather than hashing; each slot holds its group's number. A value
    /// that is not one byte (never stored, but not ruled out) takes the hashed
    /// path; one-byte values and NULL never do, so a key cannot be in both.
    byte_slot: Option<usize>,
    byte_groups: [u32; 257],
    /// Reused for every row: a key is only copied when it starts a new group.
    key: Vec<u8>,
    /// Each group's encoded key, in first-seen order — the emit order.
    keys: Vec<Box<[u8]>>,
    /// Which key sources are documents, keyed by value rather than by bytes:
    /// `{"a":1}` and `{"a":1.0}` are one group, as `=` says they are. A
    /// computed key counts when its expression is typed `jsonb`, and then
    /// only a row whose value is one is keyed by value.
    document_keys: Vec<bool>,
    /// Set when any key source may be a document. A document's key part is
    /// then not the value the group shows, so each group also keeps its first
    /// row's key in the plain form, and that is what is emitted.
    shown: Option<Vec<Box<[u8]>>>,
    /// The plain form of the row's key, built beside `key` when `shown` is.
    plain: Vec<u8>,
    /// `stride` accumulators per group, group after group.
    accs: Vec<Acc>,
    index: hashbrown::HashTable<u32>,
    /// ahash rather than SipHash: randomly seeded, so a client choosing key
    /// values still cannot aim collisions at it.
    state: ahash::RandomState,
}

impl<'p> Folder<'p> {
    pub fn new(plan: &'p AggregatePlan) -> Self {
        Self::with_byte_table(plan, true)
    }

    fn with_byte_table(plan: &'p AggregatePlan, byte_table: bool) -> Self {
        let mut template: Vec<Acc> = plan
            .items
            .iter()
            .map(|item| match item {
                SelectItem::Aggregate { func, arg, .. } => Acc::new(*func, arg.as_ref()),
                // A grouping column or a constant needs no accumulator; a
                // placeholder keeps the run index-aligned with the items.
                SelectItem::Column { .. }
                | SelectItem::Literal { .. }
                | SelectItem::Scalar { .. }
                | SelectItem::Computed { .. }
                | SelectItem::RowExpr { .. }
                | SelectItem::Window { .. } => Acc::Count(0),
            })
            .collect();
        let n_items = template.len();
        // One accumulator per aggregate the HAVING condition reads.
        let having = having_template(plan);
        let n_having = having.len();
        template.extend(having);
        // A computed item folds several aggregates at once; its inputs follow.
        let mut computed_at = vec![0usize; n_items];
        for (i, item) in plan.items.iter().enumerate() {
            if let SelectItem::Computed { inputs, .. } = item {
                computed_at[i] = template.len();
                template.extend(inputs.iter().map(|input| Acc::new(input.func, input.arg.as_ref())));
            }
        }
        let stride = template.len();

        // Every column the per-row loop reads, resolved to a slot of
        // `positions` once rather than searched for per row.
        let mut wanted: SmallVec<[u64; 16]> = SmallVec::new();
        let mut slot_of = |write_order: u64| -> usize {
            match wanted.iter().position(|w| *w == write_order) {
                Some(at) => at,
                None => {
                    wanted.push(write_order);
                    wanted.len() - 1
                }
            }
        };
        let key_sources: Vec<KeySource> = plan
            .group_by
            .iter()
            .map(|f| {
                match plan
                    .computed_keys
                    .iter()
                    .find(|k| k.write_order == f.write_order)
                {
                    Some(k) => KeySource::Computed(k),
                    None => KeySource::Column(slot_of(f.write_order)),
                }
            })
            .collect();
        // The column an aggregate reads, for the items that read one directly.
        let item_slots: Vec<Option<usize>> = plan
            .items
            .iter()
            .map(|item| match item {
                SelectItem::Aggregate {
                    arg: Some(f),
                    arg_expr: None,
                    ..
                } => Some(slot_of(f.write_order)),
                _ => None,
            })
            .collect();
        // The same for the aggregates the HAVING condition reads.
        let having_slots: Vec<Option<usize>> = plan
            .having
            .as_ref()
            .map(|h| {
                h.aggregates
                    .iter()
                    .map(|item| match item {
                        SelectItem::Aggregate {
                            arg: Some(f),
                            arg_expr: None,
                            ..
                        } => Some(slot_of(f.write_order)),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default();
        let positions = ColumnPositions::new(wanted);

        let byte_slot = match (key_sources.as_slice(), plan.group_by.first()) {
            ([KeySource::Column(slot)], Some(f))
                if byte_table && matches!(f.db_type, DbType::BOOL | DbType::I8 | DbType::U8) =>
            {
                Some(*slot)
            }
            _ => None,
        };
        let document_keys: Vec<bool> = key_sources
            .iter()
            .zip(plan.group_by.iter())
            .map(|(source, f)| match source {
                KeySource::Computed(k) => {
                    crate::core::rql::expr::static_type(&k.expr) == DbType::JSONB
                }
                KeySource::Column(_) => f.db_type == DbType::JSONB,
            })
            .collect();
        let shown = document_keys.iter().any(|d| *d).then(Vec::new);

        Folder {
            plan,
            template,
            stride,
            n_items,
            n_having,
            computed_at,
            key_sources,
            item_slots,
            having_slots,
            positions,
            byte_slot,
            byte_groups: [u32::MAX; 257],
            key: Vec::new(),
            keys: Vec::new(),
            document_keys,
            shown,
            plain: Vec::new(),
            accs: Vec::new(),
            index: hashbrown::HashTable::new(),
            state: ahash::RandomState::new(),
        }
    }

    /// How many groups have been seen so far.
    pub fn groups(&self) -> usize {
        self.keys.len()
    }

    /// Starts a group for the key in `self.key`, returning its number.
    fn new_group(&mut self, byte_cell: Option<usize>, hash: u64) -> usize {
        use std::hash::BuildHasher;
        let at = self.keys.len();
        self.keys.push(self.key.as_slice().into());
        if let Some(shown) = self.shown.as_mut() {
            shown.push(self.plain.as_slice().into());
        }
        self.accs.extend(self.template.iter().cloned());
        match byte_cell {
            Some(cell) => self.byte_groups[cell] = at as u32,
            None => {
                let keys = &self.keys;
                let state = &self.state;
                self.index.insert_unique(hash, at as u32, |g| {
                    state.hash_one(&*keys[*g as usize])
                });
            }
        }
        at
    }

    /// The group for the key in `self.key`, created if it is new.
    fn group_for_key(&mut self, byte_cell: Option<usize>) -> usize {
        use std::hash::BuildHasher;
        if let Some(cell) = byte_cell {
            if self.byte_groups[cell] != u32::MAX {
                return self.byte_groups[cell] as usize;
            }
            return self.new_group(Some(cell), 0);
        }
        let hash = self.state.hash_one(self.key.as_slice());
        let keys = &self.keys;
        let key = self.key.as_slice();
        match self.index.find(hash, |g| &*keys[*g as usize] == key) {
            Some(g) => *g as usize,
            None => self.new_group(None, hash),
        }
    }

    /// Folds one row into its group.
    pub fn push(&mut self, row: &Row) -> Result<(), String> {
        self.positions.sync(row);
        let plan = self.plan;

        let byte_cell = self.byte_slot.and_then(|slot| match self.positions.get(row, slot) {
            None => Some(256),
            Some([b]) => Some(*b as usize),
            Some(_) => None,
        });
        // A known one-byte key needs no key bytes at all.
        let known = byte_cell
            .map(|cell| self.byte_groups[cell])
            .filter(|g| *g != u32::MAX);
        let at = match known {
            Some(at) => at as usize,
            None => {
                // The grouping key is the concatenation of the key columns'
                // bytes, with lengths mixed in so that ("a","bc") and ("ab","c")
                // stay distinct.
                //
                // NULL is its own group and its own length marker: encoding it
                // as the empty key merged `NULL` with `''`, and — because the
                // emit types a key by its column — sent a zero-length value
                // under a non-text OID, which a typed client rejects outright
                // rather than reading as NULL.
                self.key.clear();
                self.plain.clear();
                // A document is keyed by its value, with its plain form kept
                // beside it for what the group shows.
                let mut plain = self.shown.is_some().then_some(&mut self.plain);
                for (source, document) in self.key_sources.iter().zip(&self.document_keys) {
                    match source {
                        // A computed key has no scanned column: its bytes come
                        // from evaluating the expression against the row.
                        KeySource::Computed(k) => {
                            match crate::core::rql::expr::eval(&k.expr, row, &k.schema)? {
                                crate::core::rql::expr::Value::Null => {
                                    push_key_part(&mut self.key, plain.as_deref_mut(), None, false)
                                }
                                crate::core::rql::expr::Value::Data { bytes, db_type } => {
                                    push_key_part(
                                        &mut self.key,
                                        plain.as_deref_mut(),
                                        Some(&bytes),
                                        *document && db_type == DbType::JSONB,
                                    )
                                }
                            }
                        }
                        KeySource::Column(slot) => push_key_part(
                            &mut self.key,
                            plain.as_deref_mut(),
                            self.positions.get(row, *slot),
                            *document,
                        ),
                    }
                }
                self.group_for_key(byte_cell)
            }
        };
        let run = &mut self.accs[at * self.stride..(at + 1) * self.stride];
        accumulate_row(
            plan,
            row,
            &self.positions,
            &self.item_slots,
            &self.having_slots,
            &self.computed_at,
            self.n_items,
            run,
        );
        Ok(())
    }

    /// Adds every group of `other` — a folder over a disjoint part of the same
    /// input, for the same plan — into this one. A group both have is merged
    /// accumulator by accumulator; one only `other` has is appended after this
    /// folder's groups.
    pub fn merge(&mut self, other: Folder<'_>) {
        for (g, key) in other.keys.iter().enumerate() {
            self.key.clear();
            self.key.extend_from_slice(key);
            // What the group shows comes along, for when it is new here.
            if let Some(shown) = other.shown.as_ref() {
                self.plain.clear();
                self.plain.extend_from_slice(&shown[g]);
            }
            let byte_cell = self.byte_slot.and_then(|_| one_byte_cell(key));
            let at = self.group_for_key(byte_cell);
            let theirs = &other.accs[g * other.stride..(g + 1) * other.stride];
            let ours = &mut self.accs[at * self.stride..(at + 1) * self.stride];
            for (acc, their) in ours.iter_mut().zip(theirs) {
                acc.merge(their);
            }
        }
    }

    /// [`Folder::finish`], serialized onto `out`.
    ///
    /// Without an `ORDER BY` each group's row is built, serialized and dropped,
    /// so a fold with many groups never holds them all as 1.5 KB `Row`s — a
    /// 100k-group result was 156 MB of `Row` for 10 MB of answer. With one, the
    /// rows have to exist together to be sorted, and `finish` builds them.
    pub fn finish_into(mut self, out: &mut Vec<u8>) -> Result<(), String> {
        let plan = self.plan;
        if !plan.order_by.is_empty() {
            for row in self.finish()? {
                row.extend_serialized(out);
            }
            return Ok(());
        }
        if self.keys.is_empty() && plan.group_by.is_empty() {
            self.keys.push(Box::default());
            self.accs.extend(self.template.iter().cloned());
        }
        let mut skip = plan.offset.map_or(0, |o| o as usize);
        let mut take = plan.limit.map_or(usize::MAX, |l| l as usize);
        for (g, key) in self.keys.iter().enumerate() {
            if take == 0 {
                break;
            }
            let run = &self.accs[g * self.stride..(g + 1) * self.stride];
            if let Some(h) = plan.having.as_ref() {
                if !having_holds(h, &run[self.n_items..self.n_items + self.n_having])? {
                    continue;
                }
            }
            if skip > 0 {
                skip -= 1;
                continue;
            }
            let key_parts = decode_key_parts(
                self.shown.as_ref().map_or(key, |s| &s[g]),
                self.key_sources.len(),
            );
            emit_group(plan, &key_parts, run, &self.computed_at)?.extend_serialized(out);
            take -= 1;
        }
        Ok(())
    }

    /// Emits the groups: HAVING, the select list, ORDER BY, OFFSET, LIMIT.
    pub fn finish(mut self) -> Result<Vec<Row>, String> {
        let plan = self.plan;
        // A bare aggregate over an empty table still produces one row —
        // `COUNT(*)` of nothing is 0, not "no rows".
        //
        // HAVING still decides whether that one group survives: `SELECT
        // COUNT(*) … WHERE <nothing matches> HAVING COUNT(*) > 0` is no rows.
        if self.keys.is_empty() && plan.group_by.is_empty() {
            self.keys.push(Box::default());
            self.accs.extend(self.template.iter().cloned());
        }

        let mut out: Vec<Row> = Vec::with_capacity(self.keys.len());
        for (g, key) in self.keys.iter().enumerate() {
            let run = &self.accs[g * self.stride..(g + 1) * self.stride];
            if let Some(h) = plan.having.as_ref() {
                if !having_holds(h, &run[self.n_items..self.n_items + self.n_having])? {
                    continue;
                }
            }
            // Decoded here, once per emitted group, rather than kept decoded
            // for every group for the whole fold.
            let key_parts = decode_key_parts(
                self.shown.as_ref().map_or(key, |s| &s[g]),
                self.key_sources.len(),
            );
            out.push(emit_group(plan, &key_parts, run, &self.computed_at)?);
        }

        // Groups are sorted here rather than in the scan: they do not exist
        // until the fold has run.
        sort_output(plan, &mut out);

        if let Some(off) = plan.offset {
            let off = (off as usize).min(out.len());
            out.drain(..off);
        }
        if let Some(limit) = plan.limit {
            out.truncate(limit as usize);
        }

        Ok(out)
    }
}

/// Appends one grouping column's part of a key: its length and bytes, the
/// NULL marker, or for a document what [`crate::core::jsonb::push_group_key`]
/// keys it by. `plain`, when kept, gets the length and bytes either way.
fn push_key_part(
    key: &mut Vec<u8>,
    plain: Option<&mut Vec<u8>>,
    value: Option<&[u8]>,
    document: bool,
) {
    let put = |out: &mut Vec<u8>, b: &[u8]| {
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(b);
    };
    match value {
        Some(b) => {
            if document {
                crate::core::jsonb::push_group_key(b, key);
            } else {
                put(key, b);
            }
            if let Some(plain) = plain {
                put(plain, b);
            }
        }
        // The same marker in both, so NULL stays its own group.
        None => {
            key.extend_from_slice(&u32::MAX.to_le_bytes());
            if let Some(plain) = plain {
                plain.extend_from_slice(&u32::MAX.to_le_bytes());
            }
        }
    }
}

/// The one-byte table's cell for an encoded single-column key, or `None` when
/// the key takes the hashed path (a value of any other width).
fn one_byte_cell(key: &[u8]) -> Option<usize> {
    let len = u32::from_le_bytes(key.get(..4)?.try_into().ok()?);
    if len == u32::MAX {
        Some(256)
    } else if len == 1 && key.len() == 5 {
        Some(key[4] as usize)
    } else {
        None
    }
}

/// One output row for one group.
fn emit_group(
    plan: &AggregatePlan,
    key_parts: &[Option<Vec<u8>>],
    run: &[Acc],
    computed_at: &[usize],
) -> Result<Row, String> {
    let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
    for (i, item) in plan.items.iter().enumerate() {
        let (bytes, ty) = match item {
            // Found by write_order rather than by position, so a select list
            // ordered differently from the GROUP BY still echoes each column
            // its own key.
            SelectItem::Column { field, .. } => {
                match plan
                    .group_by
                    .iter()
                    .position(|g| g.write_order == field.write_order)
                    .and_then(|p| key_parts.get(p))
                {
                    Some(Some(b)) => (b.clone(), field.db_type.clone()),
                    // A NULL key, or a column that is not a grouping key at
                    // all: both go out as NULL rather than as empty bytes
                    // wearing the column's type.
                    _ => (Vec::new(), DbType::NULL),
                }
            }
            SelectItem::Aggregate { func, .. } => aggregate_output(&run[i], *func, item)?,
            SelectItem::Literal { bytes, db_type, .. } => (bytes.clone(), db_type.clone()),
            // Evaluated once per emitted group here, which is the same
            // per-output-row rule the non-grouped path follows.
            SelectItem::Scalar { func, db_type, .. } => (func.evaluate(), db_type.clone()),
            // A row expression in a grouped query is a grouping key — the
            // parser refuses any other — so its value is the key the group was
            // built from.
            SelectItem::RowExpr { db_type, .. } => {
                match plan
                    .computed_keys
                    .iter()
                    .find(|k| k.item == Some(i))
                    .and_then(|k| {
                        plan.group_by
                            .iter()
                            .position(|g| g.write_order == k.write_order)
                    })
                    .and_then(|p| key_parts.get(p))
                {
                    Some(Some(b)) => (b.clone(), db_type.clone()),
                    _ => (Vec::new(), DbType::NULL),
                }
            }
            // A window reads rows the fold no longer has; reaching it here
            // means it was routed wrongly.
            SelectItem::Window { db_type, .. } => (Vec::new(), db_type.clone()),
            SelectItem::Computed {
                expr,
                inputs,
                db_type,
                ..
            } => {
                // The finished aggregates become a one-row table the expression
                // reads: slot `j` is addressed as write_order `j`. A NULL result
                // is left out of the row entirely, which is how `column_value`
                // reports NULL when there is no `__null_` companion to consult.
                let mut synth_columns: crate::core::row::row::RowColumns = SmallVec::new();
                let mut synth_schema: Vec<SchemaField> = Vec::with_capacity(inputs.len());
                for (j, input) in inputs.iter().enumerate() {
                    let ty = input
                        .func
                        .result_type(input.arg.as_ref().map(|f| &f.db_type));
                    let mut field = match &input.arg {
                        Some(f) => f.clone(),
                        None => SchemaField::new(
                            String::new(),
                            ty.clone(),
                            ty.get_size(),
                            0,
                            0,
                            false,
                        ),
                    };
                    field.name = format!("{}{}", COMPUTED_INPUT_PREFIX, j);
                    field.db_type = ty.clone();
                    field.write_order = j as u64;
                    // The scale the result is *at*, which for `AVG` is not the
                    // argument's: the evaluator lifts by this.
                    if ty == DbType::DECIMAL {
                        field.scale = Some(input.func.decimal_output_scale(input.arg.as_ref()));
                    }
                    synth_schema.push(field);

                    if let Some(bytes) = run[computed_at[i] + j].finish_nullable(input.func, &ty)? {
                        synth_columns.push(Column {
                            schema_id: crate::core::row::row::schema_id_of(j as u64),
                            data: mb_from(&bytes),
                            column_type: ty,
                        });
                    }
                }
                let synth = Row {
                    position: 0,
                    columns: synth_columns,
                    length: 0,
                };
                let value = crate::core::rql::expr::eval(expr, &synth, &synth_schema)?;
                match value {
                    crate::core::rql::expr::Value::Null => (Vec::new(), db_type.clone()),
                    crate::core::rql::expr::Value::Data { bytes, db_type: ty } => (bytes, ty),
                }
            }
        };
        columns.push(Column {
            schema_id: crate::core::row::row::schema_id_of(i as u64),
            data: mb_from(&bytes),
            column_type: ty,
        });
    }
    Ok(Row {
        position: 0,
        columns,
        length: 0,
    })
}

/// A fresh accumulator for each aggregate the plan's `HAVING` reads.
fn having_template(plan: &AggregatePlan) -> Vec<Acc> {
    plan.having
        .as_ref()
        .map(|h| {
            h.aggregates
                .iter()
                .map(|item| match item {
                    SelectItem::Aggregate { func, arg, .. } => Acc::new(*func, arg.as_ref()),
                    // The parser records aggregates only; a placeholder keeps
                    // the indices aligned if that ever changes.
                    _ => Acc::Count(0),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Folds one row into its group's run of accumulators: select items first,
/// then the HAVING aggregates, then each computed item's inputs at
/// `computed_at[i]`.
#[allow(clippy::too_many_arguments)]
fn accumulate_row(
    plan: &AggregatePlan,
    row: &Row,
    positions: &ColumnPositions,
    item_slots: &[Option<usize>],
    having_slots: &[Option<usize>],
    computed_at: &[usize],
    n_items: usize,
    run: &mut [Acc],
) {
    for (i, item) in plan.items.iter().enumerate() {
        match item {
            SelectItem::Aggregate { .. } => {
                accumulate_aggregate(item, item_slots[i], row, positions, &mut run[i]);
            }
            SelectItem::Computed { inputs, .. } => {
                for (j, input) in inputs.iter().enumerate() {
                    let (bytes, ty) = match &input.arg {
                        // `bytes` is `None` for a NULL or an absent column, and
                        // `ty` is `None` only for `COUNT(*)`. Keeping those
                        // separate is what lets `COUNT(col)` skip a NULL without
                        // also skipping the empty string.
                        Some(f) => (column_bytes(row, f.write_order), Some(&f.db_type)),
                        None => (None, None),
                    };
                    run[computed_at[i] + j].accumulate(bytes, ty);
                }
            }
            _ => {}
        }
    }

    // The aggregates the HAVING condition reads fold exactly as a select-list
    // aggregate does — its FILTER and a computed argument included. The HAVING
    // accumulator used to read only `arg`, so `HAVING COUNT(*) FILTER (WHERE
    // …) >= 2` counted every row.
    if let Some(h) = plan.having.as_ref() {
        for (j, item) in h.aggregates.iter().enumerate() {
            let slot = having_slots.get(j).copied().flatten();
            if let Some(acc) = run.get_mut(n_items + j) {
                accumulate_aggregate(item, slot, row, positions, acc);
            }
        }
    }
}

/// Folds one row into one aggregate's accumulator. `slot` is where the
/// aggregate's plain column argument sits in `positions`, if it has one.
fn accumulate_aggregate(
    item: &SelectItem,
    slot: Option<usize>,
    row: &Row,
    positions: &ColumnPositions,
    acc: &mut Acc,
) {
    let SelectItem::Aggregate {
        arg,
        filter,
        arg_expr,
        ..
    } = item
    else {
        return;
    };
    // A row the filter rejects is not counted by *this*
    // aggregate, while still counting for every other item of
    // the same statement.
    if let Some(filter) = filter {
        let keep = crate::core::rql::expr::eval_cond(
            &filter.condition,
            row,
            &filter.schema,
        )
        .unwrap_or(false);
        if !keep {
            return;
        }
    }
    let computed;
    let (bytes, ty) = match (arg, arg_expr) {
        // Computed per row. A NULL result accumulates as an
        // absent value, which is what makes `SUM` skip it
        // rather than adding a zero.
        (Some(f), Some(argument)) => {
            computed =
                crate::core::rql::expr::eval(&argument.expr, row, &argument.schema)
                    .map_err(|e| {
                        // Never swallowed into a NULL. A failed
                        // evaluation that becomes "no value" makes SUM
                        // report 0 and AVG report nothing — a number
                        // that looks like an answer. Reported here so
                        // the cause is visible even though the fold has
                        // no way to fail the statement.
                        log::error!("aggregate argument failed to evaluate: {}", e);
                        e
                    })
                    .ok()
                    // Coerced to the type this argument was *declared*
                    // as, which is what the accumulator reads the bytes
                    // by. `column_order + 1` evaluates to four bytes
                    // while its static type is eight, and accumulating
                    // the narrow value under the wide type summed to
                    // **zero** — a wrong number, not an error.
                    .and_then(|value| {
                        if value.db_type() == Some(&f.db_type) {
                            Some(value)
                        } else {
                            crate::core::rql::expr::cast(value, &f.db_type).ok()
                        }
                    })
                    .and_then(|value| match value {
                        crate::core::rql::expr::Value::Data { bytes, .. } => {
                            Some(bytes)
                        }
                        crate::core::rql::expr::Value::Null => None,
                    });
            (computed.as_deref(), Some(&f.db_type))
        }
        // `bytes` is `None` for a NULL or an absent column, and `ty`
        // is `None` only for `COUNT(*)`. Keeping those separate is
        // what lets `COUNT(col)` skip a NULL without also skipping
        // the empty string.
        (Some(f), None) => (
            slot.and_then(|slot| positions.get(row, slot)),
            Some(&f.db_type),
        ),
        _ => (None, None),
    };
    acc.accumulate(bytes, ty);
}

/// One aggregate's output cell: its bytes and type, or an empty cell typed
/// NULL when SQL says the result is NULL.
///
/// `MAX`/`MIN` over no value, and `SUM`/`AVG` over no value, are NULL in
/// PostgreSQL. [`Acc::finish`] renders those as the type's zeroes, which a
/// client read as the number `0` — a group whose values were all NULL came
/// back as a real zero, and sorted as one. `COUNT` is never NULL.
fn aggregate_output(
    acc: &Acc,
    func: AggFunc,
    item: &SelectItem,
) -> Result<(Vec<u8>, DbType), String> {
    let ty = item.result_type();
    // An exact `AVG` is a `numeric` whose scale PostgreSQL picks per value
    // (`Acc::pg_avg`), so it leaves the fold carrying that scale with it —
    // see `value_codec::decimal_cell_with_scale`. A column has one scale and
    // could only ever print one of them.
    if func == AggFunc::Avg && ty == DbType::DECIMAL {
        if let Acc::ExactSum { .. } = acc {
            return Ok(match acc.pg_avg()? {
                Some((v, scale)) => (
                    crate::core::value_codec::decimal_cell_with_scale(v, scale),
                    DbType::DECIMAL,
                ),
                None => (Vec::new(), DbType::NULL),
            });
        }
    }
    Ok(match acc.finish_nullable(func, &ty)? {
        Some(bytes) => (bytes, ty),
        None => (Vec::new(), DbType::NULL),
    })
}

/// Sorts the grouped output by `plan.order_by`, PostgreSQL's way.
///
/// A NULL is larger than every value — last under `ASC`, first under `DESC` —
/// unless the term says `NULLS FIRST`/`LAST`. NULL-ness is the output cell's
/// **type**: a NULL group key or aggregate leaves the fold as an empty cell
/// typed NULL. Comparing bytes instead put NULL before every value under
/// `ASC` (empty sorts first) and tied a NULL text key with `''`.
fn sort_output(plan: &AggregatePlan, out: &mut [Row]) {
    // Each key is the position of an output column plus its direction,
    // placement and type, resolved once so the label lookup stays out of the
    // comparator.
    let sort_keys: Vec<(usize, bool, bool, DbType)> = plan
        .order_by
        .iter()
        .filter_map(|(label, descending, nulls_first)| {
            let idx = plan.items.iter().position(|i| i.label() == label)?;
            Some((
                idx,
                *descending,
                nulls_first.unwrap_or(*descending),
                plan.items[idx].result_type(),
            ))
        })
        .collect();
    if sort_keys.is_empty() {
        return;
    }
    out.sort_by(|a, b| {
        use std::cmp::Ordering;
        for (idx, descending, nulls_first, ty) in sort_keys.iter() {
            let (ac, bc) = (a.columns.get(*idx), b.columns.get(*idx));
            let is_null = |c: Option<&Column>| c.is_none_or(|c| c.column_type == DbType::NULL);
            let ord = match (is_null(ac), is_null(bc)) {
                (true, true) => Ordering::Equal,
                (true, false) => {
                    if *nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (false, true) => {
                    if *nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (false, false) => {
                    let ord = compare_values(
                        ac.map(|c| c.data.into_slice()).unwrap_or(&[]),
                        bc.map(|c| c.data.into_slice()).unwrap_or(&[]),
                        ty,
                    );
                    if *descending { ord.reverse() } else { ord }
                }
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
}

/// Whether a group survives its `HAVING` condition.
///
/// Each aggregate the condition reads is finished at its own result type —
/// a DECIMAL at its declared scale, wider for `AVG` — and a NULL aggregate is
/// SQL NULL, never the zero `finish` would render. The condition is
/// three-valued, and only `true` keeps the group. Constants were coerced at
/// plan time, so there is no "literal did not coerce" case left to decide
/// here: that used to answer "keep the group", and returned every group.
fn having_holds(h: &Having, accs: &[Acc]) -> Result<bool, String> {
    let mut values: Vec<Option<crate::core::rql::having::HValue>> =
        Vec::with_capacity(h.aggregates.len());
    for (item, acc) in h.aggregates.iter().zip(accs.iter()) {
        let SelectItem::Aggregate { func, .. } = item else {
            return Err("HAVING reads something that is not an aggregate".to_string());
        };
        let (result_type, scale) = crate::core::rql::having::aggregate_result(item)
            .ok_or("HAVING reads something that is not an aggregate")?;
        let value = match acc.finish_nullable(*func, &result_type)? {
            None => None,
            Some(bytes) => Some(
                crate::core::rql::having::HValue::from_stored(&bytes, &result_type, scale)
                    .ok_or_else(|| format!("HAVING could not read a {} aggregate", result_type))?,
            ),
        };
        values.push(value);
    }
    if values.len() != h.aggregates.len() {
        return Err("HAVING aggregates were not all folded".to_string());
    }
    Ok(h.cond.eval(&values, &compare_values)? == Some(true))
}

/// The fold as it was before `Folder` — a map from key `Vec` to group number,
/// decoded key parts and three accumulator `Vec`s per group — kept only so
/// `fold_bench` can compare the two in one process.
#[cfg(test)]
#[allow(dead_code)]
mod fold_previous {
    use super::*;
    /// Folds scanned rows into grouped result rows.
    ///
    /// The output rows are synthetic: each column's `schema_id` is its position in
    /// the `SELECT` list, since an aggregate has no schema field to point at.
    pub(super) fn fold(plan: &AggregatePlan, rows: &[Row]) -> Result<Vec<Row>, String> {
        fold_with(plan, rows, true)
    }

    /// [`fold`], with the one-byte-key table switchable so the two paths can be
    /// compared against each other.
    fn fold_with(plan: &AggregatePlan, rows: &[Row], byte_table: bool) -> Result<Vec<Row>, String> {
        // One group: (key bytes per grouping column, accumulator per select item,
        // HAVING accumulator, and one accumulator per input of each computed item).
        //
        // Groups live in a `Vec` in first-seen order, which is the emit order, and
        // the map only points into it. The map used to own the groups while a
        // second `Vec` held a copy of every key just to remember that order — two
        // key allocations per group, and a third lookup per group at emit.
        let mut groups: Vec<Group> = Vec::new();
        // ahash rather than SipHash: randomly seeded, so a client choosing key
        // values still cannot aim collisions at it.
        let mut index: ahash::HashMap<Vec<u8>, usize> =
            ahash::HashMap::with_capacity_and_hasher(rows.len().min(1024), Default::default());

        // What a new group starts from, built once and cloned per group rather
        // than re-derived from the plan for every one.
        let template_accs: Vec<Acc> = plan
            .items
            .iter()
            .map(|item| match item {
                SelectItem::Aggregate { func, arg, .. } => Acc::new(*func, arg.as_ref()),
                // A grouping column or a constant needs no accumulator; a
                // placeholder keeps the two vectors index-aligned.
                SelectItem::Column { .. }
                | SelectItem::Literal { .. }
                | SelectItem::Scalar { .. }
                | SelectItem::Computed { .. }
                | SelectItem::RowExpr { .. }
                | SelectItem::Window { .. } => Acc::Count(0),
            })
            .collect();
        // One accumulator per aggregate the HAVING condition reads.
        let template_having: Vec<Acc> = having_template(plan);
        // A computed item folds several aggregates at once, so its accumulators
        // live in their own per-item vector rather than the one-per-item `accs`.
        let template_computed: Vec<Vec<Acc>> = plan
            .items
            .iter()
            .map(|item| match item {
                SelectItem::Computed { inputs, .. } => inputs
                    .iter()
                    .map(|i| Acc::new(i.func, i.arg.as_ref()))
                    .collect(),
                _ => Vec::new(),
            })
            .collect();

        // Every column the per-row loop reads, resolved to a slot of `positions`
        // once rather than searched for per row.
        let mut wanted: SmallVec<[u64; 16]> = SmallVec::new();
        let mut slot_of = |write_order: u64| -> usize {
            match wanted.iter().position(|w| *w == write_order) {
                Some(at) => at,
                None => {
                    wanted.push(write_order);
                    wanted.len() - 1
                }
            }
        };
        /// Where one grouping key's bytes come from.
        enum KeySource<'p> {
            /// Evaluated against the row: no scanned column carries it.
            Computed(&'p ComputedKey),
            /// A scanned column, by its slot in `positions`.
            Column(usize),
        }
        let key_sources: Vec<KeySource> = plan
            .group_by
            .iter()
            .map(|f| {
                match plan
                    .computed_keys
                    .iter()
                    .find(|k| k.write_order == f.write_order)
                {
                    Some(k) => KeySource::Computed(k),
                    None => KeySource::Column(slot_of(f.write_order)),
                }
            })
            .collect();
        // The column an aggregate reads, for the items that read one directly.
        let item_slots: Vec<Option<usize>> = plan
            .items
            .iter()
            .map(|item| match item {
                SelectItem::Aggregate {
                    arg: Some(f),
                    arg_expr: None,
                    ..
                } => Some(slot_of(f.write_order)),
                _ => None,
            })
            .collect();
        // The same for the aggregates the HAVING condition reads.
        let having_slots: Vec<Option<usize>> = plan
            .having
            .as_ref()
            .map(|h| {
                h.aggregates
                    .iter()
                    .map(|item| match item {
                        SelectItem::Aggregate {
                            arg: Some(f),
                            arg_expr: None,
                            ..
                        } => Some(slot_of(f.write_order)),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut positions = ColumnPositions::new(wanted);

        // A single grouping column one byte wide — `BOOL`, `I8`, `U8` — has at
        // most 257 keys: its 256 values and NULL. Those are found by indexing a
        // table rather than hashing. Each slot holds its group's position in
        // `groups`, which is still filled in first-seen order, so the emit order is
        // the hashed path's. A value that is not one byte (never stored, but not
        // ruled out) takes the hashed path; one-byte values and NULL never do, so a
        // key cannot end up in both.
        let byte_slot = match (key_sources.as_slice(), plan.group_by.first()) {
            ([KeySource::Column(slot)], Some(f))
                if byte_table && matches!(f.db_type, DbType::BOOL | DbType::I8 | DbType::U8) =>
            {
                Some(*slot)
            }
            _ => None,
        };
        let mut byte_groups = [u32::MAX; 257];

        // Reused for every row: a key is only copied when it starts a new group.
        let mut key: Vec<u8> = Vec::new();

        for row in rows {
            positions.sync(row);

            let byte_cell = byte_slot.and_then(|slot| match positions.get(row, slot) {
                None => Some(256),
                Some([b]) => Some(*b as usize),
                Some(_) => None,
            });
            if let Some(cell) = byte_cell {
                if byte_groups[cell] != u32::MAX {
                    let at = byte_groups[cell] as usize;
                    accumulate_row(plan, row, &positions, &item_slots, &having_slots, &mut groups[at]);
                    continue;
                }
            }

            // The grouping key is the concatenation of the key columns' bytes, with
            // lengths mixed in so that ("a","bc") and ("ab","c") stay distinct.
            //
            // NULL is its own group and its own length marker: encoding it as the
            // empty key merged `NULL` with `''`, and — because the emit below types
            // a key by its column — sent a zero-length value under a non-text OID,
            // which a typed client rejects outright rather than reading as NULL.
            key.clear();
            for source in &key_sources {
                match source {
                    // A computed key has no scanned column: its bytes come from
                    // evaluating the expression against the row.
                    KeySource::Computed(k) => {
                        match crate::core::rql::expr::eval(&k.expr, row, &k.schema)? {
                            crate::core::rql::expr::Value::Null => {
                                key.extend_from_slice(&u32::MAX.to_le_bytes())
                            }
                            crate::core::rql::expr::Value::Data { bytes, .. } => {
                                key.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                                key.extend_from_slice(&bytes);
                            }
                        }
                    }
                    KeySource::Column(slot) => match positions.get(row, *slot) {
                        Some(b) => {
                            key.extend_from_slice(&(b.len() as u32).to_le_bytes());
                            key.extend_from_slice(b);
                        }
                        None => key.extend_from_slice(&u32::MAX.to_le_bytes()),
                    },
                }
            }

            // Looked up by slice first; the key is copied only for a new group.
            // `entry(key.clone())` copied it for every row, found or not.
            let found = if byte_cell.is_some() {
                None
            } else {
                index.get(key.as_slice()).copied()
            };
            let at = match found {
                Some(at) => at,
                None => {
                    let at = groups.len();
                    groups.push((
                        decode_key_parts(&key, key_sources.len()),
                        template_accs.clone(),
                        template_having.clone(),
                        template_computed.clone(),
                    ));
                    match byte_cell {
                        Some(cell) => byte_groups[cell] = at as u32,
                        None => {
                            index.insert(key.clone(), at);
                        }
                    }
                    at
                }
            };
            accumulate_row(plan, row, &positions, &item_slots, &having_slots, &mut groups[at]);
        }

        // A bare aggregate over an empty table still produces one row — `COUNT(*)`
        // of nothing is 0, not "no rows".
        //
        // HAVING still decides whether that one group survives: `SELECT COUNT(*)
        // … WHERE <nothing matches> HAVING COUNT(*) > 0` is no rows. The empty
        // group used to carry no HAVING accumulator at all, which skipped the test
        // and emitted the row.
        if groups.is_empty() && plan.group_by.is_empty() {
            groups.push((Vec::new(), template_accs, template_having, template_computed));
        }

        let mut out: Vec<Row> = Vec::with_capacity(groups.len());
        for (key_parts, accs, having_accs, computed_accs) in groups.iter() {
            if let Some(h) = plan.having.as_ref() {
                if !having_holds(h, having_accs)? {
                    continue;
                }
            }

            let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
            for (i, item) in plan.items.iter().enumerate() {
                let (bytes, ty) = match item {
                    // Found by write_order rather than by position, so a select
                    // list ordered differently from the GROUP BY still echoes
                    // each column its own key.
                    SelectItem::Column { field, .. } => {
                        match plan
                            .group_by
                            .iter()
                            .position(|g| g.write_order == field.write_order)
                            .and_then(|p| key_parts.get(p))
                        {
                            Some(Some(b)) => (b.clone(), field.db_type.clone()),
                            // A NULL key, or a column that is not a grouping key
                            // at all: both go out as NULL rather than as empty
                            // bytes wearing the column's type.
                            _ => (Vec::new(), DbType::NULL),
                        }
                    }
                    SelectItem::Aggregate { func, .. } => aggregate_output(&accs[i], *func, item)?,
                    SelectItem::Literal { bytes, db_type, .. } => (bytes.clone(), db_type.clone()),
                    // Evaluated once per emitted group here, which is the same
                    // per-output-row rule the non-grouped path follows.
                    SelectItem::Scalar { func, db_type, .. } => (func.evaluate(), db_type.clone()),
                    // A row expression in a grouped query is a grouping key —
                    // the parser refuses any other — so its value is the key the
                    // group was built from.
                    SelectItem::RowExpr { db_type, .. } => {
                        match plan
                            .computed_keys
                            .iter()
                            .find(|k| k.item == Some(i))
                            .and_then(|k| {
                                plan.group_by
                                    .iter()
                                    .position(|g| g.write_order == k.write_order)
                            })
                            .and_then(|p| key_parts.get(p))
                        {
                            Some(Some(b)) => (b.clone(), db_type.clone()),
                            _ => (Vec::new(), DbType::NULL),
                        }
                    }
                    // A window reads rows the fold no longer has; reaching it
                    // here means it was routed wrongly.
                    SelectItem::Window { db_type, .. } => (Vec::new(), db_type.clone()),
                    SelectItem::Computed {
                        expr,
                        inputs,
                        db_type,
                        ..
                    } => {
                        // The finished aggregates become a one-row table the
                        // expression reads: slot `j` is addressed as write_order
                        // `j`. A NULL result is left out of the row entirely, which
                        // is how `column_value` reports NULL when there is no
                        // `__null_` companion to consult.
                        let mut synth_columns: crate::core::row::row::RowColumns = SmallVec::new();
                        let mut synth_schema: Vec<SchemaField> = Vec::with_capacity(inputs.len());
                        for (j, input) in inputs.iter().enumerate() {
                            let ty = input
                                .func
                                .result_type(input.arg.as_ref().map(|f| &f.db_type));
                            let mut field = match &input.arg {
                                Some(f) => f.clone(),
                                None => SchemaField::new(
                                    String::new(),
                                    ty.clone(),
                                    ty.get_size(),
                                    0,
                                    0,
                                    false,
                                ),
                            };
                            field.name = format!("{}{}", COMPUTED_INPUT_PREFIX, j);
                            field.db_type = ty.clone();
                            field.write_order = j as u64;
                            // The scale the result is *at*, which for `AVG` is not
                            // the argument's: the evaluator lifts by this.
                            if ty == DbType::DECIMAL {
                                field.scale = Some(input.func.decimal_output_scale(input.arg.as_ref()));
                            }
                            synth_schema.push(field);

                            if let Some(bytes) = computed_accs[i][j].finish_nullable(input.func, &ty)? {
                                synth_columns.push(Column {
                                    schema_id: crate::core::row::row::schema_id_of(j as u64),
                                    data: mb_from(&bytes),
                                    column_type: ty,
                                });
                            }
                        }
                        let synth = Row {
                            position: 0,
                            columns: synth_columns,
                            length: 0,
                        };
                        let value = crate::core::rql::expr::eval(expr, &synth, &synth_schema)?;
                        match value {
                            crate::core::rql::expr::Value::Null => (Vec::new(), db_type.clone()),
                            crate::core::rql::expr::Value::Data { bytes, db_type: ty } => (bytes, ty),
                        }
                    }
                };
                columns.push(Column {
                    schema_id: crate::core::row::row::schema_id_of(i as u64),
                    data: mb_from(&bytes),
                    column_type: ty,
                });
            }

            out.push(Row {
                position: 0,
                columns,
                length: 0,
            });
        }

        // Groups are sorted here rather than in the scan: they do not exist until
        // the fold has run.
        //
        // Each key is the position of an output column plus its direction and type.
        // Resolving them once, before the sort, keeps the label lookup out of the
        // comparator.
        sort_output(plan, &mut out);

        if let Some(off) = plan.offset {
            let off = off as usize;
            out = out.into_iter().skip(off).collect();
        }
        if let Some(limit) = plan.limit {
            out.truncate(limit as usize);
        }

        Ok(out)
    }

    /// One group: the key's parts, an accumulator per select item, the `HAVING`
    /// accumulator, and one accumulator per input of each computed item.
    type Group = (Vec<Option<Vec<u8>>>, Vec<Acc>, Vec<Acc>, Vec<Vec<Acc>>);

    /// A fresh accumulator for each aggregate the plan's `HAVING` reads.
    fn having_template(plan: &AggregatePlan) -> Vec<Acc> {
        plan.having
            .as_ref()
            .map(|h| {
                h.aggregates
                    .iter()
                    .map(|item| match item {
                        SelectItem::Aggregate { func, arg, .. } => Acc::new(*func, arg.as_ref()),
                        // The parser records aggregates only; a placeholder keeps
                        // the indices aligned if that ever changes.
                        _ => Acc::Count(0),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Folds one row into its group's accumulators.
    fn accumulate_row(
        plan: &AggregatePlan,
        row: &Row,
        positions: &ColumnPositions,
        item_slots: &[Option<usize>],
        having_slots: &[Option<usize>],
        entry: &mut Group,
    ) {
        for (i, item) in plan.items.iter().enumerate() {
            match item {
                SelectItem::Aggregate { .. } => {
                    accumulate_aggregate(item, item_slots[i], row, positions, &mut entry.1[i]);
                }
                SelectItem::Computed { inputs, .. } => {
                    for (j, input) in inputs.iter().enumerate() {
                        let (bytes, ty) = match &input.arg {
                            // `bytes` is `None` for a NULL or an absent column, and `ty`
                            // is `None` only for `COUNT(*)`. Keeping those separate is
                            // what lets `COUNT(col)` skip a NULL without also skipping
                            // the empty string.
                            Some(f) => (column_bytes(row, f.write_order), Some(&f.db_type)),
                            None => (None, None),
                        };
                        entry.3[i][j].accumulate(bytes, ty);
                    }
                }
                _ => {}
            }
        }

        // The aggregates the HAVING condition reads fold exactly as a select-list
        // aggregate does — its FILTER and a computed argument included. The HAVING
        // accumulator used to read only `arg`, so `HAVING COUNT(*) FILTER (WHERE
        // …) >= 2` counted every row.
        if let Some(h) = plan.having.as_ref() {
            for (j, item) in h.aggregates.iter().enumerate() {
                let slot = having_slots.get(j).copied().flatten();
                if let Some(acc) = entry.2.get_mut(j) {
                    accumulate_aggregate(item, slot, row, positions, acc);
                }
            }
        }
    }

}

#[cfg(test)]
mod fold_bench {
    use super::*;

    fn hwm_kb() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmHWM:"))
                    .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
            })
            .unwrap_or(0)
    }

    /// `FOLD_BENCH_IMPL=old|new FOLD_BENCH_GROUPS=1000 cargo test --release -p
    /// rasterizeddb_core --lib -- --ignored fold_bench --nocapture`: one
    /// million rows of `(k BIGINT, v BIGINT)`, `SELECT k, SUM(v), COUNT(*),
    /// MAX(v) GROUP BY k`. One implementation per process, so the peak RSS
    /// it adds over the input rows is its own.
    #[test]
    #[ignore]
    fn fold_bench() {
        let groups: i64 = std::env::var("FOLD_BENCH_GROUPS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
        let which = std::env::var("FOLD_BENCH_IMPL").unwrap_or_else(|_| "new".into());
        let k = SchemaField::new("k".into(), DbType::I64, 8, 0, 0, false);
        let v = SchemaField::new("v".into(), DbType::I64, 8, 0, 1, false);
        let agg = |func, arg: Option<SchemaField>| SelectItem::Aggregate {
            func,
            arg,
            label: "a".into(),
            filter: None,
            arg_expr: None,
        };
        let plan = AggregatePlan {
            items: vec![
                SelectItem::Column { field: k.clone(), label: "k".into() },
                agg(AggFunc::Sum, Some(v.clone())),
                agg(AggFunc::Count, None),
                agg(AggFunc::Max, Some(v.clone())),
            ],
            group_by: vec![k.clone()],
            computed_keys: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let mut rng = fastrand::Rng::with_seed(1);
        let rows: Vec<Row> = (0..1_000_000i64)
            .map(|i| {
                let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
                columns.push(Column { schema_id: 0, data: mb_from(&rng.i64(0..groups).to_le_bytes()), column_type: DbType::I64 });
                columns.push(Column { schema_id: 1, data: mb_from(&(i % 977).to_le_bytes()), column_type: DbType::I64 });
                Row { position: 0, columns, length: 0 }
            })
            .collect();
        let base = hwm_kb();
        let mut times = Vec::new();
        let mut n = 0;
        for _ in 0..5 {
            let t = std::time::Instant::now();
            let out = if which == "old" {
                fold_previous::fold(&plan, &rows).unwrap()
            } else {
                fold(&plan, &rows).unwrap()
            };
            times.push(t.elapsed().as_secs_f64() * 1000.0);
            n = out.len();
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!(
            "fold_bench impl={} groups={} out={} min_ms={:.1} median_ms={:.1} added_rss_mb={:.1}",
            which, groups, n, times[0], times[2], (hwm_kb().saturating_sub(base)) as f64 / 1024.0
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gen_random_uuid_differs_every_call() {
        // The property that matters: `INSERT … SELECT gen_random_uuid(), …`
        // puts a distinct id in every row. A value computed once and reused
        // would write the same key into all of them and report success.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            assert!(
                seen.insert(ScalarFunc::GenRandomUuid.evaluate()),
                "gen_random_uuid() repeated a value"
            );
        }
    }

    #[test]
    fn gen_random_uuid_is_a_well_formed_v4() {
        for _ in 0..64 {
            let bytes = ScalarFunc::GenRandomUuid.evaluate();
            assert_eq!(bytes.len(), 16);
            // RFC 4122: version 4 in the high nibble of byte 6, variant 1 in
            // the top bits of byte 8. Without these a client rejects it as a
            // malformed UUID rather than reading sixteen bytes.
            assert_eq!(bytes[6] & 0xf0, 0x40, "version nibble");
            assert_eq!(bytes[8] & 0xc0, 0x80, "variant bits");
        }
    }

    #[test]
    fn now_is_a_plausible_microsecond_timestamp() {
        let bytes = ScalarFunc::Now.evaluate();
        assert_eq!(bytes.len(), 8);
        let micros = i64::from_le_bytes(bytes.try_into().unwrap());
        // Somewhere after 2020 and before 2100 — enough to catch seconds or
        // millis being stored where microseconds are read.
        assert!(micros > 1_577_836_800_000_000, "too small: {}", micros);
        assert!(micros < 4_102_444_800_000_000, "too large: {}", micros);
    }

    #[test]
    fn scalar_names_are_recognised_and_unknown_ones_are_not() {
        assert_eq!(
            ScalarFunc::parse("now").map(|(f, _)| f),
            Some(ScalarFunc::Now)
        );
        assert_eq!(
            ScalarFunc::parse("NOW").map(|(f, _)| f),
            Some(ScalarFunc::Now)
        );
        assert_eq!(
            ScalarFunc::parse("gen_random_uuid").map(|(f, _)| f),
            Some(ScalarFunc::GenRandomUuid)
        );
        // An unknown function must stay an error rather than becoming a silent
        // NULL, which is the trap version of this feature.
        assert!(ScalarFunc::parse("coalesce").is_none());
        assert!(ScalarFunc::parse("definitely_not_a_function").is_none());
    }

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

    fn row(values: &[(u64, DbType, Vec<u8>)]) -> Row {
        let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
        for (schema_id, ty, bytes) in values {
            columns.push(Column {
                schema_id: crate::core::row::row::schema_id_of(*schema_id),
                data: mb_from(bytes),
                column_type: ty.clone(),
            });
        }
        Row {
            position: 0,
            columns,
            length: 0,
        }
    }

    fn agg(func: AggFunc, arg: Option<SchemaField>) -> SelectItem {
        SelectItem::Aggregate {
            func,
            arg,
            label: func.name().to_string(),
            filter: None,
            arg_expr: None,
        }
    }

    fn plan(items: Vec<SelectItem>, group_by: Vec<SchemaField>) -> AggregatePlan {
        AggregatePlan {
            items,
            group_by,
            computed_keys: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    /// Everything that must keep a plan *off* the count-only fast path.
    ///
    /// Each rejection here is a wrong answer rather than an error if it is
    /// missed, which is why they are pinned one at a time: the fast path counts
    /// matched rows and emits one row, so a plan that reaches it wrongly gets a
    /// plausible number instead of a failure.
    #[test]
    fn only_a_bare_count_takes_the_count_only_path() {
        let f = field("age", DbType::I32, 1);

        // The shapes that may take it.
        assert!(is_row_count_only(&plan(
            vec![agg(AggFunc::Count, None)],
            vec![]
        )));
        assert!(is_row_count_only(&plan(
            vec![agg(AggFunc::Count, None), agg(AggFunc::Count, None)],
            vec![]
        )));

        // `COUNT(col)` skips NULLs, so it has to read the column.
        assert!(!is_row_count_only(&plan(
            vec![agg(AggFunc::Count, Some(f.clone()))],
            vec![]
        )));

        // Not a count at all.
        assert!(!is_row_count_only(&plan(
            vec![agg(AggFunc::Sum, Some(f.clone()))],
            vec![]
        )));

        // A grouped count needs the key's value.
        assert!(!is_row_count_only(&plan(
            vec![agg(AggFunc::Count, None)],
            vec![f.clone()]
        )));

        // One bare count beside anything else is still not count-only: the
        // other item owns an output slot this path cannot fill.
        assert!(!is_row_count_only(&plan(
            vec![
                agg(AggFunc::Count, None),
                agg(AggFunc::Sum, Some(f.clone()))
            ],
            vec![]
        )));

        // An empty plan has no answer to give.
        assert!(!is_row_count_only(&plan(vec![], vec![])));

        // `COUNT(*) FILTER (WHERE …)` carries `arg: None` exactly like a bare
        // `COUNT(*)`, so a guard testing only the function and the argument
        // admits it — and then answers with the *unfiltered* count.
        let filtered = match agg(AggFunc::Count, None) {
            SelectItem::Aggregate {
                func,
                arg,
                label,
                arg_expr,
                ..
            } => SelectItem::Aggregate {
                func,
                arg,
                label,
                arg_expr,
                // An empty `And` is the cheapest valid condition; only the
                // filter's *presence* is under test here.
                filter: Some(AggregateFilter {
                    condition: crate::core::rql::expr::Cond::And(Vec::new()),
                    schema: Vec::new(),
                }),
            },
            other => other,
        };
        assert!(!is_row_count_only(&plan(vec![filtered], vec![])));

        // `HAVING` decides which groups survive, which needs the values.
        let mut having = plan(vec![agg(AggFunc::Count, None)], vec![]);
        having.having = Some(Having::single(AggFunc::Count, None, HavingOp::Gt, "0").unwrap());
        assert!(!is_row_count_only(&having));

        // `LIMIT 0` must return no rows, and `OFFSET 1` past a one-row result
        // likewise. The fast path returns straight from the executor and never
        // reaches the paging the fold's caller applies, so both have to fall
        // through to the ordinary path. Drivers issue `LIMIT 0` to read column
        // metadata, so this is not an exotic shape.
        let mut limited = plan(vec![agg(AggFunc::Count, None)], vec![]);
        limited.limit = Some(0);
        assert!(!is_row_count_only(&limited));

        let mut limited_one = plan(vec![agg(AggFunc::Count, None)], vec![]);
        limited_one.limit = Some(1);
        assert!(!is_row_count_only(&limited_one));

        let mut offset = plan(vec![agg(AggFunc::Count, None)], vec![]);
        offset.offset = Some(1);
        assert!(!is_row_count_only(&offset));
    }

    /// The fast path's row has to be the one the fold would have produced.
    #[test]
    fn a_counted_plan_matches_what_the_fold_would_emit() {
        let p = plan(
            vec![agg(AggFunc::Count, None), agg(AggFunc::Count, None)],
            vec![],
        );
        let out = row_count_result(&p, 7);

        assert_eq!(out.len(), 1, "a count always answers with exactly one row");
        assert_eq!(out[0].columns.len(), p.items.len());
        for (i, c) in out[0].columns.iter().enumerate() {
            assert_eq!(u64::from(c.schema_id), i as u64, "column {i} carries its item's index");
            assert_eq!(c.column_type, DbType::I64);
            assert_eq!(c.data.into_slice(), &7i64.to_le_bytes());
        }

        // Zero is a row, not the absence of one — `SELECT COUNT(*)` over an
        // empty table answers 0. This is the asymmetry with `GROUP BY`, which
        // yields no rows for no input.
        let zero = row_count_result(&plan(vec![agg(AggFunc::Count, None)], vec![]), 0);
        assert_eq!(zero.len(), 1);
        assert_eq!(zero[0].columns[0].data.into_slice(), &0i64.to_le_bytes());
    }

    /// Builds `COALESCE(MAX(<field>), 0) + 1` the way the parser does.
    fn coalesce_max_plus_one(f: &SchemaField) -> SelectItem {
        use crate::core::rql::expr::{ArithOp, Expr};
        let ty = AggFunc::Max.result_type(Some(&f.db_type));
        let slot = SchemaField::new(
            format!("{}0", COMPUTED_INPUT_PREFIX),
            ty.clone(),
            ty.get_size(),
            0,
            0,
            false,
        );
        SelectItem::Computed {
            expr: Expr::Arith(
                Box::new(Expr::Coalesce(vec![
                    Expr::Column(slot),
                    Expr::Literal {
                        bytes: 0i64.to_le_bytes().to_vec(),
                        db_type: DbType::I64,
                    },
                ])),
                ArithOp::Add,
                Box::new(Expr::Literal {
                    bytes: 1i64.to_le_bytes().to_vec(),
                    db_type: DbType::I64,
                }),
            ),
            inputs: vec![ComputedInput {
                func: AggFunc::Max,
                arg: Some(f.clone()),
            }],
            db_type: DbType::I64,
            label: "next".to_string(),
        }
    }

    #[test]
    fn computed_expression_reads_the_finished_aggregate() {
        let f = field("entry_order", DbType::I64, 0);
        let rows = vec![
            row(&[(0, DbType::I64, 3i64.to_le_bytes().to_vec())]),
            row(&[(0, DbType::I64, 7i64.to_le_bytes().to_vec())]),
            row(&[(0, DbType::I64, 5i64.to_le_bytes().to_vec())]),
        ];
        let out = fold(&plan(vec![coalesce_max_plus_one(&f)], vec![]), &rows).unwrap();
        assert_eq!(out.len(), 1);
        // MAX is 7, so the answer is 8 — not 4 (max of the first row) and not
        // 16 (the expression applied per row and then summed).
        assert_eq!(i64_of(&out[0], 0), 8);
    }

    #[test]
    fn computed_expression_runs_once_per_group() {
        let k = field("k", DbType::I64, 0);
        let v = field("v", DbType::I64, 1);
        let rows = vec![
            row(&[
                (0, DbType::I64, 1i64.to_le_bytes().to_vec()),
                (1, DbType::I64, 10i64.to_le_bytes().to_vec()),
            ]),
            row(&[
                (0, DbType::I64, 1i64.to_le_bytes().to_vec()),
                (1, DbType::I64, 40i64.to_le_bytes().to_vec()),
            ]),
            row(&[
                (0, DbType::I64, 2i64.to_le_bytes().to_vec()),
                (1, DbType::I64, 5i64.to_le_bytes().to_vec()),
            ]),
        ];
        let items = vec![
            SelectItem::Column {
                field: k.clone(),
                label: "k".to_string(),
            },
            coalesce_max_plus_one(&v),
        ];
        let out = fold(&plan(items, vec![k]), &rows).unwrap();
        assert_eq!(out.len(), 2);
        let mut got: Vec<(i64, i64)> = out.iter().map(|r| (i64_of(r, 0), i64_of(r, 1))).collect();
        got.sort();
        assert_eq!(got, vec![(1, 41), (2, 6)]);
    }

    #[test]
    fn a_null_maximum_reaches_the_coalesce_instead_of_reading_as_zero() {
        // The trap this guards: `Acc::finish` returns zero bytes for a MAX that
        // saw nothing, which decodes as the number 0. If the computed path used
        // it, COALESCE would never fire and a group of all-NULLs would be
        // indistinguishable from one whose maximum is genuinely 0 — both
        // answering 1 by different routes, one of them by accident.
        let f = field("entry_order", DbType::I64, 0);
        // A row with no column 0 at all is how an absent value arrives.
        let rows = vec![row(&[(1, DbType::I64, 9i64.to_le_bytes().to_vec())])];
        let out = fold(&plan(vec![coalesce_max_plus_one(&f)], vec![]), &rows).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(i64_of(&out[0], 0), 1, "COALESCE(NULL, 0) + 1");
    }

    #[test]
    fn division_by_zero_in_a_computed_item_is_an_error_not_a_number() {
        use crate::core::rql::expr::{ArithOp, Expr};
        let f = field("v", DbType::I64, 0);
        let ty = AggFunc::Count.result_type(None);
        let slot = SchemaField::new(
            format!("{}0", COMPUTED_INPUT_PREFIX),
            ty.clone(),
            ty.get_size(),
            0,
            0,
            false,
        );
        let item = SelectItem::Computed {
            expr: Expr::Arith(
                Box::new(Expr::Literal {
                    bytes: 1i64.to_le_bytes().to_vec(),
                    db_type: DbType::I64,
                }),
                ArithOp::Div,
                Box::new(Expr::Column(slot)),
            ),
            inputs: vec![ComputedInput {
                func: AggFunc::Count,
                arg: Some(f),
            }],
            db_type: DbType::I64,
            label: "x".to_string(),
        };
        // No rows -> COUNT is 0 -> 1/0. It must fail, not emit something.
        let err = fold(&plan(vec![item], vec![]), &[]).unwrap_err();
        assert!(err.contains("division by zero"), "unexpected: {}", err);
    }

    fn i64_of(row: &Row, idx: usize) -> i64 {
        let b = row.columns[idx].data.into_slice();
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        i64::from_le_bytes(a)
    }

    /// A `DECIMAL` output cell rendered at `scale` — or at the scale the cell
    /// carries itself (an `AVG`) — trailing zeros trimmed.
    fn dec_at(row: &Row, idx: usize, scale: u32) -> String {
        let b = row.columns[idx].data.into_slice();
        let (v, own) = value_codec::split_decimal_cell(b).expect("a decimal cell");
        let text = value_codec::format_decimal_scaled(v, own.unwrap_or(scale));
        if text.contains('.') {
            text.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            text
        }
    }

    fn dec_of(row: &Row, idx: usize) -> String {
        let b = row.columns[idx].data.into_slice();
        match value_codec::split_decimal_cell(b).expect("a decimal cell") {
            (v, Some(own)) => value_codec::format_decimal_scaled(v, own),
            (v, None) => value_codec::format_decimal(v),
        }
    }

    #[test]
    fn count_star_counts_every_row() {
        let rows: Vec<Row> = (0..3)
            .map(|i| row(&[(0, DbType::I64, (i as i64).to_le_bytes().to_vec())]))
            .collect();
        let out = fold(&plan(vec![agg(AggFunc::Count, None)], vec![]), &rows).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(i64_of(&out[0], 0), 3);
    }

    /// `COUNT(*)` over nothing is 0, not "no rows" — a caller asking how many
    /// rows matched must get an answer.
    #[test]
    fn count_star_over_no_rows_is_zero_not_empty() {
        let out = fold(&plan(vec![agg(AggFunc::Count, None)], vec![]), &[]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(i64_of(&out[0], 0), 0);
    }

    /// "Without a value" means NULL or absent — not the empty string, which is
    /// a value `COUNT` has to count. The fixture used to say NULL by writing
    /// empty bytes under the column's own type, which is exactly the
    /// conflation that made `GROUP BY` merge `NULL` with `''`; a NULL now
    /// carries `DbType::NULL`, so the two cases can be stated separately.
    #[test]
    fn count_of_a_column_skips_rows_without_a_value() {
        let f = field("c", DbType::STRING, 0);
        let rows = vec![
            row(&[(0, DbType::STRING, b"a".to_vec())]),
            row(&[(0, DbType::NULL, Vec::new())]),
            // No column at all — the scan did not return one for this row.
            row(&[]),
            row(&[(0, DbType::STRING, b"b".to_vec())]),
        ];
        let out = fold(
            &plan(vec![agg(AggFunc::Count, Some(f.clone()))], vec![]),
            &rows,
        )
        .unwrap();
        assert_eq!(i64_of(&out[0], 0), 2);

        // The empty string is a value. Counting it as absent reported a column
        // of empty strings as entirely unpopulated.
        let empties = vec![
            row(&[(0, DbType::STRING, Vec::new())]),
            row(&[(0, DbType::STRING, b"a".to_vec())]),
        ];
        let out = fold(&plan(vec![agg(AggFunc::Count, Some(f))], vec![]), &empties).unwrap();
        assert_eq!(i64_of(&out[0], 0), 2);
    }

    #[test]
    fn sum_and_avg_over_integers() {
        let f = field("n", DbType::I64, 0);
        let rows: Vec<Row> = [10i64, 20, 30]
            .iter()
            .map(|v| row(&[(0, DbType::I64, v.to_le_bytes().to_vec())]))
            .collect();

        let out = fold(
            &plan(vec![agg(AggFunc::Sum, Some(f.clone()))], vec![]),
            &rows,
        )
        .unwrap();
        // `sum(int8)` is a numeric at scale 0.
        assert_eq!(dec_at(&out[0], 0, 0), "60");

        let out = fold(&plan(vec![agg(AggFunc::Avg, Some(f))], vec![]), &rows).unwrap();
        assert_eq!(dec_at(&out[0], 0, AVG_SCALE), "20");
    }

    fn sum_of(ty: DbType, values: &[Vec<u8>]) -> Result<Vec<Row>, String> {
        let f = field("n", ty.clone(), 0);
        let rows: Vec<Row> = values
            .iter()
            .map(|v| row(&[(0, ty.clone(), v.clone())]))
            .collect();
        fold(&plan(vec![agg(AggFunc::Sum, Some(f))], vec![]), &rows)
    }

    /// `SUM` over a `BIGINT` (or `U64`) is a `numeric`, as in PostgreSQL: a
    /// total past `i64` is exact, never wrapped and no longer an error. The
    /// total used to be cut with `as i64`, so `i64::MAX + 1` came back as
    /// `i64::MIN` and the statement reported success; then it was refused as
    /// `bigint out of range`.
    #[test]
    fn an_integer_sum_past_bigint_is_an_exact_numeric() {
        let exact = |out: &[Row]| -> i128 {
            assert_eq!(out[0].columns[0].column_type, DbType::DECIMAL);
            let mut a = [0u8; 16];
            a.copy_from_slice(out[0].columns[0].data.into_slice());
            i128::from_le_bytes(a)
        };
        let out = sum_of(DbType::I64, &[i64::MAX.to_le_bytes().to_vec(), 1i64.to_le_bytes().to_vec()]).unwrap();
        assert_eq!(exact(&out), i64::MAX as i128 + 1);
        let out = sum_of(DbType::U64, &[u64::MAX.to_le_bytes().to_vec(), 1u64.to_le_bytes().to_vec()]).unwrap();
        assert_eq!(exact(&out), u64::MAX as i128 + 1);
        let out = sum_of(DbType::I64, &[i64::MIN.to_le_bytes().to_vec(), (-1i64).to_le_bytes().to_vec()]).unwrap();
        assert_eq!(exact(&out), i64::MIN as i128 - 1);

        // `sum(int4)` and `sum(int2)` stay `bigint`, as in PostgreSQL — and a
        // bigint total past i64 is still refused.
        assert_eq!(AggFunc::Sum.result_type(Some(&DbType::I32)), DbType::I64);
        assert_eq!(AggFunc::Sum.result_type(Some(&DbType::I16)), DbType::I64);
        let out = sum_of(
            DbType::I32,
            &[i32::MAX.to_le_bytes().to_vec(), i32::MAX.to_le_bytes().to_vec()],
        )
        .unwrap();
        assert_eq!(i64_of(&out[0], 0), 2 * i32::MAX as i64);
    }

    /// A 128-bit sum keeps its width: an `I128` total past `i64` used to lose
    /// its high half, and a `U128` above `i128::MAX` was read as negative.
    #[test]
    fn a_128_bit_sum_is_exact_or_an_error() {
        let big = (i64::MAX as i128) * 4;
        let out = sum_of(
            DbType::I128,
            &[big.to_le_bytes().to_vec(), big.to_le_bytes().to_vec()],
        )
        .unwrap();
        let mut a = [0u8; 16];
        a.copy_from_slice(out[0].columns[0].data.into_slice());
        assert_eq!(i128::from_le_bytes(a), big * 2);

        assert!(
            sum_of(
                DbType::I128,
                &[i128::MAX.to_le_bytes().to_vec(), 1i128.to_le_bytes().to_vec()],
            )
            .is_err(),
            "I128 past its range"
        );
        let huge = (i128::MAX as u128) + 5;
        assert!(
            sum_of(DbType::U128, &[huge.to_le_bytes().to_vec()]).is_err(),
            "a U128 above i128::MAX must not be summed as a negative number"
        );
        let out = sum_of(
            DbType::U128,
            &vec![(u64::MAX as u128).to_le_bytes().to_vec(); 3],
        )
        .unwrap();
        a.copy_from_slice(out[0].columns[0].data.into_slice());
        assert_eq!(u128::from_le_bytes(a), 3 * u64::MAX as u128);
    }

    /// `MAX` over `U128` orders the values as unsigned: above `i128::MAX` they
    /// used to decode as negative and lose to every small value.
    #[test]
    fn max_over_u128_orders_unsigned() {
        let f = field("n", DbType::U128, 0);
        let huge = u128::MAX - 1;
        let rows: Vec<Row> = [7u128, huge, 3]
            .iter()
            .map(|v| row(&[(0, DbType::U128, v.to_le_bytes().to_vec())]))
            .collect();
        let out = fold(
            &plan(vec![agg(AggFunc::Max, Some(f.clone())), agg(AggFunc::Min, Some(f))], vec![]),
            &rows,
        )
        .unwrap();
        let mut a = [0u8; 16];
        a.copy_from_slice(out[0].columns[0].data.into_slice());
        assert_eq!(u128::from_le_bytes(a), huge);
        a.copy_from_slice(out[0].columns[1].data.into_slice());
        assert_eq!(u128::from_le_bytes(a), 3);
    }

    /// `AVG` over integers cannot overflow silently either: the lift to the
    /// decimal scale used `saturating_mul`, so a large enough total averaged to
    /// a clamped number. An average that large is now printed at scale 0 —
    /// sixteen significant digits need no fraction past 10¹⁶, which is
    /// PostgreSQL's choice too — and is exact; a total past `i128` is an error.
    #[test]
    fn avg_over_huge_integers_is_an_error_not_a_clamped_number() {
        let f = field("n", DbType::I128, 0);
        let rows: Vec<Row> = [i128::MAX / 2, i128::MAX / 2]
            .iter()
            .map(|v| row(&[(0, DbType::I128, v.to_le_bytes().to_vec())]))
            .collect();
        let out = fold(&plan(vec![agg(AggFunc::Avg, Some(f.clone()))], vec![]), &rows).unwrap();
        assert_eq!(dec_of(&out[0], 0), (i128::MAX / 2).to_string());
        let rows: Vec<Row> = [i128::MAX, 1]
            .iter()
            .map(|v| row(&[(0, DbType::I128, v.to_le_bytes().to_vec())]))
            .collect();
        let got = fold(&plan(vec![agg(AggFunc::Avg, Some(f))], vec![]), &rows);
        assert!(got.is_err(), "got {:?}", got.map(|r| dec_of(&r[0], 0)));
    }

    /// `AVG` over a `NUMERIC(p, s)` column divides at the engine scale, not at
    /// `s`: `AVG(1.00, 2.00, 2.00)` was `1.66` at scale 2. PostgreSQL says
    /// `1.6666666666666667`, its sixteen places rounded as it rounds a
    /// quotient — and so does this, now.
    #[test]
    fn avg_over_a_scaled_decimal_divides_at_the_engine_scale() {
        let mut f = field("amount", DbType::DECIMAL, 0);
        f.scale = Some(2);
        let rows: Vec<Row> = [100i128, 200, 200]
            .iter()
            .map(|v| row(&[(0, DbType::DECIMAL, v.to_le_bytes().to_vec())]))
            .collect();
        let out = fold(&plan(vec![agg(AggFunc::Avg, Some(f.clone()))], vec![]), &rows).unwrap();
        // PostgreSQL's sixteen places, carried by the value itself.
        assert_eq!(AggFunc::Avg.decimal_output_scale(Some(&f)), AVG_SCALE);
        let cell = |out: &[Row]| {
            value_codec::split_decimal_cell(out[0].columns[0].data.into_slice()).unwrap()
        };
        assert_eq!(cell(&out), (16_666_666_666_666_667, Some(16)));

        // A column wider than sixteen places keeps its own.
        f.scale = Some(18);
        assert_eq!(AggFunc::Avg.decimal_output_scale(Some(&f)), 18);
        let unit = 1_000_000_000_000_000_000i128;
        let rows: Vec<Row> = [unit, 2 * unit, 2 * unit]
            .iter()
            .map(|v| row(&[(0, DbType::DECIMAL, v.to_le_bytes().to_vec())]))
            .collect();
        let out = fold(&plan(vec![agg(AggFunc::Avg, Some(f.clone()))], vec![]), &rows).unwrap();
        assert_eq!(cell(&out), (1_666_666_666_666_666_667, Some(18)));
        let mut a = [0u8; 16];

        // An integer average past what `total * 10^16` holds still divides:
        // i64::MAX and 1 average to 2^62 exactly.
        let n = field("n", DbType::I64, 0);
        let rows: Vec<Row> = [i64::MAX, 1]
            .iter()
            .map(|v| row(&[(0, DbType::I64, v.to_le_bytes().to_vec())]))
            .collect();
        // PostgreSQL prints it at scale 0: nineteen digits are already more
        // than sixteen significant ones.
        let out = fold(&plan(vec![agg(AggFunc::Avg, Some(n.clone()))], vec![]), &rows).unwrap();
        assert_eq!(cell(&out), (1i128 << 62, Some(0)));
        // And `sum(int8)` is a numeric at scale 0, exact past i64.
        let out = fold(&plan(vec![agg(AggFunc::Sum, Some(n.clone()))], vec![]), &rows).unwrap();
        assert_eq!(out[0].columns[0].column_type, DbType::DECIMAL);
        assert_eq!(AggFunc::Sum.decimal_output_scale(Some(&n)), 0);
        a.copy_from_slice(out[0].columns[0].data.into_slice());
        assert_eq!(i128::from_le_bytes(a), i64::MAX as i128 + 1);

        // SUM keeps the column's scale; it never divides.
        f.scale = Some(2);
        assert_eq!(AggFunc::Sum.decimal_output_scale(Some(&f)), 2);
    }

    /// An average over integers rounds as PostgreSQL rounds a numeric
    /// quotient — half away from zero — rather than truncating.
    #[test]
    fn avg_over_integers_rounds_half_away_from_zero() {
        let f = field("n", DbType::I32, 0);
        for (values, want) in [
            (vec![1i32, 2, 2], "1.6666666666666667"),
            (vec![-1, -2, -2], "-1.6666666666666667"),
            (vec![1, 2], "1.5"),
            // Below 1, twenty places (`select_div_scale`), as PostgreSQL prints.
            (vec![0, 0, 1], "0.33333333333333333333"),
        ] {
            let rows: Vec<Row> = values
                .iter()
                .map(|v| row(&[(0, DbType::I32, v.to_le_bytes().to_vec())]))
                .collect();
            let out = fold(&plan(vec![agg(AggFunc::Avg, Some(f.clone()))], vec![]), &rows).unwrap();
            assert_eq!(dec_at(&out[0], 0, AVG_SCALE), want, "{:?}", values);
        }
    }

    /// `HAVING SUM(price) > 100` over a `NUMERIC(10,2)` column: the literal
    /// must meet the total at the column's scale. Coerced at the engine
    /// default it was ten million times too large and dropped every group.
    #[test]
    fn having_compares_a_decimal_aggregate_at_its_own_scale() {
        let mut f = field("price", DbType::DECIMAL, 0);
        f.scale = Some(2);
        let rows: Vec<Row> = [15000i128, 2000]
            .iter()
            .map(|v| row(&[(0, DbType::DECIMAL, v.to_le_bytes().to_vec())]))
            .collect();
        for (func, lit, keep) in [
            (AggFunc::Sum, "100", true),
            (AggFunc::Sum, "170", false),
            (AggFunc::Avg, "84.99", true),
            (AggFunc::Avg, "85", false),
            (AggFunc::Max, "149.99", true),
        ] {
            let mut p = plan(vec![agg(AggFunc::Count, None)], vec![]);
            p.having = Some(Having::single(func, Some(f.clone()), HavingOp::Gt, lit).unwrap());
            let out = fold(&p, &rows).unwrap();
            assert_eq!(out.len() == 1, keep, "{:?} > {}", func, lit);
        }
    }

    /// Averaging money must not fall back to floating point.
    #[test]
    fn avg_over_decimal_stays_exact() {
        let f = field("amount", DbType::DECIMAL, 0);
        let rows: Vec<Row> = ["1.00", "2.00"]
            .iter()
            .map(|s| {
                let v = value_codec::parse_decimal(s).unwrap();
                row(&[(0, DbType::DECIMAL, v.to_le_bytes().to_vec())])
            })
            .collect();
        let out = fold(&plan(vec![agg(AggFunc::Avg, Some(f))], vec![]), &rows).unwrap();
        assert_eq!(dec_at(&out[0], 0, AVG_SCALE), "1.5");
    }

    #[test]
    fn sum_over_decimal_keeps_its_scale() {
        let f = field("amount", DbType::DECIMAL, 0);
        let rows: Vec<Row> = ["100.00", "250.50", "80.00"]
            .iter()
            .map(|s| {
                let v = value_codec::parse_decimal(s).unwrap();
                row(&[(0, DbType::DECIMAL, v.to_le_bytes().to_vec())])
            })
            .collect();
        let out = fold(&plan(vec![agg(AggFunc::Sum, Some(f))], vec![]), &rows).unwrap();
        assert_eq!(dec_of(&out[0], 0), "430.5");
    }

    #[test]
    fn min_and_max_return_the_arguments_own_type() {
        let f = field("n", DbType::I64, 0);
        let rows: Vec<Row> = [5i64, -3, 42]
            .iter()
            .map(|v| row(&[(0, DbType::I64, v.to_le_bytes().to_vec())]))
            .collect();

        let out = fold(
            &plan(
                vec![
                    agg(AggFunc::Min, Some(f.clone())),
                    agg(AggFunc::Max, Some(f)),
                ],
                vec![],
            ),
            &rows,
        )
        .unwrap();
        assert_eq!(
            i64_of(&out[0], 0),
            -3,
            "MIN must compare numerically, not bytewise"
        );
        assert_eq!(i64_of(&out[0], 1), 42);
    }

    #[test]
    fn group_by_splits_rows_by_key() {
        let status = field("status", DbType::STRING, 0);
        let rows = vec![
            row(&[(0, DbType::STRING, b"paid".to_vec())]),
            row(&[(0, DbType::STRING, b"open".to_vec())]),
            row(&[(0, DbType::STRING, b"paid".to_vec())]),
        ];
        let items = vec![
            SelectItem::Column {
                field: status.clone(),
                label: "status".to_string(),
            },
            agg(AggFunc::Count, None),
        ];
        let mut p = plan(items, vec![status]);
        p.order_by = vec![("status".to_string(), false, None)];

        let out = fold(&p, &rows).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].columns[0].data.into_slice(), b"open");
        assert_eq!(i64_of(&out[0], 1), 1);
        assert_eq!(out[1].columns[0].data.into_slice(), b"paid");
        assert_eq!(i64_of(&out[1], 1), 2);
    }

    /// Keys are length-prefixed so two different groupings cannot collide by
    /// concatenating to the same bytes.
    #[test]
    fn composite_group_keys_do_not_collide() {
        let a = field("a", DbType::STRING, 0);
        let b = field("b", DbType::STRING, 1);
        let rows = vec![
            row(&[
                (0, DbType::STRING, b"a".to_vec()),
                (1, DbType::STRING, b"bc".to_vec()),
            ]),
            row(&[
                (0, DbType::STRING, b"ab".to_vec()),
                (1, DbType::STRING, b"c".to_vec()),
            ]),
        ];
        let items = vec![
            SelectItem::Column {
                field: a.clone(),
                label: "a".to_string(),
            },
            SelectItem::Column {
                field: b.clone(),
                label: "b".to_string(),
            },
            agg(AggFunc::Count, None),
        ];
        let out = fold(&plan(items, vec![a, b]), &rows).unwrap();
        assert_eq!(
            out.len(),
            2,
            "\"a\"+\"bc\" and \"ab\"+\"c\" are different groups"
        );
    }

    #[test]
    fn having_drops_groups_that_fail_the_test() {
        let status = field("status", DbType::STRING, 0);
        let rows = vec![
            row(&[(0, DbType::STRING, b"paid".to_vec())]),
            row(&[(0, DbType::STRING, b"open".to_vec())]),
            row(&[(0, DbType::STRING, b"paid".to_vec())]),
        ];
        let items = vec![
            SelectItem::Column {
                field: status.clone(),
                label: "status".to_string(),
            },
            agg(AggFunc::Count, None),
        ];
        let mut p = plan(items, vec![status]);
        p.having = Some(Having::single(AggFunc::Count, None, HavingOp::Gt, "1").unwrap());

        let out = fold(&p, &rows).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].columns[0].data.into_slice(), b"paid");
    }

    #[test]
    fn output_can_be_ordered_by_an_aggregate_and_paged() {
        let status = field("status", DbType::STRING, 0);
        let rows = vec![
            row(&[(0, DbType::STRING, b"a".to_vec())]),
            row(&[(0, DbType::STRING, b"b".to_vec())]),
            row(&[(0, DbType::STRING, b"b".to_vec())]),
            row(&[(0, DbType::STRING, b"c".to_vec())]),
            row(&[(0, DbType::STRING, b"c".to_vec())]),
            row(&[(0, DbType::STRING, b"c".to_vec())]),
        ];
        let items = vec![
            SelectItem::Column {
                field: status.clone(),
                label: "status".to_string(),
            },
            agg(AggFunc::Count, None),
        ];
        let mut p = plan(items, vec![status]);
        p.order_by = vec![("count".to_string(), true, None)];
        p.limit = Some(2);

        let out = fold(&p, &rows).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(i64_of(&out[0], 1), 3);
        assert_eq!(i64_of(&out[1], 1), 2);
    }

    #[test]
    fn a_computed_key_groups_by_the_evaluated_value_and_echoes_it() {
        use crate::core::rql::expr::{ArithOp, Expr};
        // `SELECT x + 100 AS bucket, COUNT(*) … GROUP BY bucket`: the key is
        // not a stored column, so the fold has to evaluate it per row.
        let x = field("x", DbType::I64, 0);
        let expr = Expr::Arith(
            Box::new(Expr::Column(x.clone())),
            ArithOp::Add,
            Box::new(Expr::Literal {
                bytes: 100i64.to_le_bytes().to_vec(),
                db_type: DbType::I64,
            }),
        );
        let bucket = field("bucket", DbType::I64, 100);
        let p = AggregatePlan {
            items: vec![
                SelectItem::RowExpr {
                    expr: expr.clone(),
                    db_type: DbType::I64,
                    label: "bucket".to_string(),
                },
                agg(AggFunc::Count, None),
            ],
            group_by: vec![bucket.clone()],
            computed_keys: vec![ComputedKey {
                write_order: bucket.write_order,
                expr,
                schema: vec![x],
                item: Some(0),
            }],
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let rows = vec![
            row(&[(0, DbType::I64, 1i64.to_le_bytes().to_vec())]),
            row(&[(0, DbType::I64, 1i64.to_le_bytes().to_vec())]),
            row(&[(0, DbType::I64, 2i64.to_le_bytes().to_vec())]),
        ];
        let out = fold(&p, &rows).unwrap();
        assert_eq!(out.len(), 2, "two buckets");
        let mut got: Vec<(i64, i64)> = out.iter().map(|r| (i64_of(r, 0), i64_of(r, 1))).collect();
        got.sort();
        // The echoed value is the *evaluated* key — 101/102, not the stored
        // 1/2 — which is what proves the expression ran rather than the
        // column being read through.
        assert_eq!(got, vec![(101, 2), (102, 1)]);
    }

    #[test]
    fn a_select_list_ordered_differently_from_the_group_by_still_matches() {
        // `SELECT b, a, COUNT(*) … GROUP BY a, b`: each column must echo its
        // own key. The emit used to hand keys out in sequence, which swapped
        // them whenever the two lists disagreed on order.
        let a = field("a", DbType::I64, 0);
        let b = field("b", DbType::I64, 1);
        let rows = vec![row(&[
            (0, DbType::I64, 1i64.to_le_bytes().to_vec()),
            (1, DbType::I64, 2i64.to_le_bytes().to_vec()),
        ])];
        let items = vec![
            SelectItem::Column {
                field: b.clone(),
                label: "b".to_string(),
            },
            SelectItem::Column {
                field: a.clone(),
                label: "a".to_string(),
            },
            agg(AggFunc::Count, None),
        ];
        let out = fold(&plan(items, vec![a, b]), &rows).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(i64_of(&out[0], 0), 2, "b's slot carries b's value");
        assert_eq!(i64_of(&out[0], 1), 1, "a's slot carries a's value");
    }

    #[test]
    fn result_types_follow_postgres_where_it_matters() {
        assert_eq!(AggFunc::Count.result_type(None), DbType::I64);
        assert_eq!(
            AggFunc::Count.result_type(Some(&DbType::STRING)),
            DbType::I64
        );
        assert_eq!(
            AggFunc::Sum.result_type(Some(&DbType::DECIMAL)),
            DbType::DECIMAL
        );
        assert_eq!(AggFunc::Sum.result_type(Some(&DbType::I32)), DbType::I64);
        assert_eq!(AggFunc::Sum.result_type(Some(&DbType::F32)), DbType::F32);
        assert_eq!(AggFunc::Sum.result_type(Some(&DbType::I64)), DbType::DECIMAL);
        assert_eq!(AggFunc::Sum.result_type(Some(&DbType::U32)), DbType::DECIMAL);
        assert_eq!(AggFunc::Sum.result_type(Some(&DbType::U16)), DbType::I64);
        assert_eq!(
            AggFunc::Avg.result_type(Some(&DbType::I64)),
            DbType::DECIMAL
        );
        assert_eq!(
            AggFunc::Min.result_type(Some(&DbType::STRING)),
            DbType::STRING
        );
    }
}

/// The fold as it was before the allocation and hashing rework, kept verbatim
/// as a test-only reference so the rewrite can be checked against it over
/// generated inputs. Do not "fix" this copy: its value is that it is the old
/// behaviour.
#[cfg(test)]
mod fold_reference {
    use super::*;

    pub(super) fn fold_reference(plan: &AggregatePlan, rows: &[Row]) -> Result<Vec<Row>, String> {
        // Group key -> (key bytes per grouping column, accumulator per select item,
        // HAVING accumulator, and one accumulator per input of each computed item).
        let mut order: Vec<Vec<u8>> = Vec::new();
        let mut groups: std::collections::HashMap<
            Vec<u8>,
            (Vec<Option<Vec<u8>>>, Vec<Acc>, Option<Vec<Acc>>, Vec<Vec<Acc>>),
        > = std::collections::HashMap::new();

        for row in rows {
            // The grouping key is the concatenation of the key columns' bytes, with
            // lengths mixed in so that ("a","bc") and ("ab","c") stay distinct.
            let mut key = Vec::new();
            let mut key_parts = Vec::with_capacity(plan.group_by.len());
            for f in &plan.group_by {
                // A computed key has no scanned column: its bytes come from
                // evaluating the expression against the row. NULL becomes the
                // empty key, matching how an absent column groups below.
                let bytes: Option<Vec<u8>> = match plan
                    .computed_keys
                    .iter()
                    .find(|k| k.write_order == f.write_order)
                {
                    Some(k) => match crate::core::rql::expr::eval(&k.expr, row, &k.schema)? {
                        crate::core::rql::expr::Value::Null => None,
                        crate::core::rql::expr::Value::Data { bytes, .. } => Some(bytes),
                    },
                    None => column_bytes(row, f.write_order).map(|b| b.to_vec()),
                };
                // NULL is its own group and its own length marker: encoding it as
                // the empty key merged `NULL` with `''`, and — because the emit
                // below types a key by its column — sent a zero-length value under
                // a non-text OID, which a typed client rejects outright rather
                // than reading as NULL.
                match &bytes {
                    Some(b) => {
                        key.extend_from_slice(&(b.len() as u32).to_le_bytes());
                        key.extend_from_slice(b);
                    }
                    None => key.extend_from_slice(&u32::MAX.to_le_bytes()),
                }
                key_parts.push(bytes);
            }

            let entry = groups.entry(key.clone()).or_insert_with(|| {
                order.push(key.clone());
                let accs = plan
                    .items
                    .iter()
                    .map(|item| match item {
                        SelectItem::Aggregate { func, arg, .. } => Acc::new(*func, arg.as_ref()),
                        // A grouping column or a constant needs no accumulator; a
                        // placeholder keeps the two vectors index-aligned.
                        SelectItem::Column { .. }
                        | SelectItem::Literal { .. }
                        | SelectItem::Scalar { .. }
                        | SelectItem::Computed { .. }
                        | SelectItem::RowExpr { .. }
                        | SelectItem::Window { .. } => Acc::Count(0),
                    })
                    .collect();
                // Adapted to the HAVING shape that replaced one accumulator
                // with one per aggregate; read as the old code read its one.
                let having_acc = plan.having.as_ref().map(|_| having_template(plan));
                // A computed item folds several aggregates at once, so its
                // accumulators live in their own per-item vector rather than the
                // one-per-item `accs`.
                let computed = plan
                    .items
                    .iter()
                    .map(|item| match item {
                        SelectItem::Computed { inputs, .. } => inputs
                            .iter()
                            .map(|i| Acc::new(i.func, i.arg.as_ref()))
                            .collect(),
                        _ => Vec::new(),
                    })
                    .collect();
                (key_parts.clone(), accs, having_acc, computed)
            });

            for (i, item) in plan.items.iter().enumerate() {
                match item {
                    SelectItem::Aggregate {
                        arg,
                        filter,
                        arg_expr,
                        ..
                    } => {
                        // A row the filter rejects is not counted by *this*
                        // aggregate, while still counting for every other item of
                        // the same statement.
                        if let Some(filter) = filter {
                            let keep = crate::core::rql::expr::eval_cond(
                                &filter.condition,
                                row,
                                &filter.schema,
                            )
                            .unwrap_or(false);
                            if !keep {
                                continue;
                            }
                        }
                        let computed;
                        let (bytes, ty) = match (arg, arg_expr) {
                            // Computed per row. A NULL result accumulates as an
                            // absent value, which is what makes `SUM` skip it
                            // rather than adding a zero.
                            (Some(f), Some(argument)) => {
                                computed =
                                    crate::core::rql::expr::eval(&argument.expr, row, &argument.schema)
                                        .map_err(|e| {
                                            // Never swallowed into a NULL. A failed
                                            // evaluation that becomes "no value" makes SUM
                                            // report 0 and AVG report nothing — a number
                                            // that looks like an answer. Reported here so
                                            // the cause is visible even though the fold has
                                            // no way to fail the statement.
                                            log::error!("aggregate argument failed to evaluate: {}", e);
                                            e
                                        })
                                        .ok()
                                        // Coerced to the type this argument was *declared*
                                        // as, which is what the accumulator reads the bytes
                                        // by. `column_order + 1` evaluates to four bytes
                                        // while its static type is eight, and accumulating
                                        // the narrow value under the wide type summed to
                                        // **zero** — a wrong number, not an error.
                                        .and_then(|value| {
                                            if value.db_type() == Some(&f.db_type) {
                                                Some(value)
                                            } else {
                                                crate::core::rql::expr::cast(value, &f.db_type).ok()
                                            }
                                        })
                                        .and_then(|value| match value {
                                            crate::core::rql::expr::Value::Data { bytes, .. } => {
                                                Some(bytes)
                                            }
                                            crate::core::rql::expr::Value::Null => None,
                                        });
                                (computed.as_deref(), Some(&f.db_type))
                            }
                            // `bytes` is `None` for a NULL or an absent column, and `ty`
                            // is `None` only for `COUNT(*)`. Keeping those separate is
                            // what lets `COUNT(col)` skip a NULL without also skipping
                            // the empty string.
                            (Some(f), None) => (column_bytes(row, f.write_order), Some(&f.db_type)),
                            _ => (None, None),
                        };
                        entry.1[i].accumulate(bytes, ty);
                    }
                    SelectItem::Computed { inputs, .. } => {
                        for (j, input) in inputs.iter().enumerate() {
                            let (bytes, ty) = match &input.arg {
                                // `bytes` is `None` for a NULL or an absent column, and `ty`
                                // is `None` only for `COUNT(*)`. Keeping those separate is
                                // what lets `COUNT(col)` skip a NULL without also skipping
                                // the empty string.
                                Some(f) => (column_bytes(row, f.write_order), Some(&f.db_type)),
                                None => (None, None),
                            };
                            entry.3[i][j].accumulate(bytes, ty);
                        }
                    }
                    _ => {}
                }
            }

            if let (Some(h), Some(having_accs)) = (plan.having.as_ref(), entry.2.as_mut()) {
                for (item, acc) in h.aggregates.iter().zip(having_accs.iter_mut()) {
                    let SelectItem::Aggregate { arg, .. } = item else { continue };
                    let (bytes, ty) = match arg {
                        // `bytes` is `None` for a NULL or an absent column, and `ty`
                        // is `None` only for `COUNT(*)`. Keeping those separate is
                        // what lets `COUNT(col)` skip a NULL without also skipping
                        // the empty string.
                        Some(f) => (column_bytes(row, f.write_order), Some(&f.db_type)),
                        None => (None, None),
                    };
                    acc.accumulate(bytes, ty);
                }
            }
        }

        // A bare aggregate over an empty table still produces one row — `COUNT(*)`
        // of nothing is 0, not "no rows".
        if groups.is_empty() && plan.group_by.is_empty() {
            let accs: Vec<Acc> = plan
                .items
                .iter()
                .map(|item| match item {
                    SelectItem::Aggregate { func, arg, .. } => Acc::new(*func, arg.as_ref()),
                    SelectItem::Column { .. }
                    | SelectItem::Literal { .. }
                    | SelectItem::Scalar { .. }
                    | SelectItem::Computed { .. }
                    | SelectItem::RowExpr { .. }
                    | SelectItem::Window { .. } => Acc::Count(0),
                })
                .collect();
            let computed: Vec<Vec<Acc>> = plan
                .items
                .iter()
                .map(|item| match item {
                    SelectItem::Computed { inputs, .. } => inputs
                        .iter()
                        .map(|i| Acc::new(i.func, i.arg.as_ref()))
                        .collect(),
                    _ => Vec::new(),
                })
                .collect();
            let key = Vec::new();
            order.push(key.clone());
            groups.insert(key, (Vec::new(), accs, None, computed));
        }

        let mut out: Vec<Row> = Vec::with_capacity(order.len());
        for key in &order {
            let Some((key_parts, accs, having_acc, computed_accs)) = groups.get(key) else {
                continue;
            };

            if let (Some(h), Some(accs)) = (plan.having.as_ref(), having_acc.as_ref()) {
                if !having_holds(h, accs)? {
                    continue;
                }
            }

            let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
            for (i, item) in plan.items.iter().enumerate() {
                let (bytes, ty) = match item {
                    // Found by write_order rather than by position, so a select
                    // list ordered differently from the GROUP BY still echoes
                    // each column its own key.
                    SelectItem::Column { field, .. } => {
                        match plan
                            .group_by
                            .iter()
                            .position(|g| g.write_order == field.write_order)
                            .and_then(|p| key_parts.get(p))
                        {
                            Some(Some(b)) => (b.clone(), field.db_type.clone()),
                            // A NULL key, or a column that is not a grouping key
                            // at all: both go out as NULL rather than as empty
                            // bytes wearing the column's type.
                            _ => (Vec::new(), DbType::NULL),
                        }
                    }
                    SelectItem::Aggregate { func, .. } => aggregate_output(&accs[i], *func, item)?,
                    SelectItem::Literal { bytes, db_type, .. } => (bytes.clone(), db_type.clone()),
                    // Evaluated once per emitted group here, which is the same
                    // per-output-row rule the non-grouped path follows.
                    SelectItem::Scalar { func, db_type, .. } => (func.evaluate(), db_type.clone()),
                    // A row expression in a grouped query is a grouping key —
                    // the parser refuses any other — so its value is the key the
                    // group was built from.
                    SelectItem::RowExpr { db_type, .. } => {
                        match plan
                            .computed_keys
                            .iter()
                            .find(|k| k.item == Some(i))
                            .and_then(|k| {
                                plan.group_by
                                    .iter()
                                    .position(|g| g.write_order == k.write_order)
                            })
                            .and_then(|p| key_parts.get(p))
                        {
                            Some(Some(b)) => (b.clone(), db_type.clone()),
                            _ => (Vec::new(), DbType::NULL),
                        }
                    }
                    // A window reads rows the fold no longer has; reaching it
                    // here means it was routed wrongly.
                    SelectItem::Window { db_type, .. } => (Vec::new(), db_type.clone()),
                    SelectItem::Computed {
                        expr,
                        inputs,
                        db_type,
                        ..
                    } => {
                        // The finished aggregates become a one-row table the
                        // expression reads: slot `j` is addressed as write_order
                        // `j`. A NULL result is left out of the row entirely, which
                        // is how `column_value` reports NULL when there is no
                        // `__null_` companion to consult.
                        let mut synth_columns: crate::core::row::row::RowColumns = SmallVec::new();
                        let mut synth_schema: Vec<SchemaField> = Vec::with_capacity(inputs.len());
                        for (j, input) in inputs.iter().enumerate() {
                            let ty = input
                                .func
                                .result_type(input.arg.as_ref().map(|f| &f.db_type));
                            let mut field = match &input.arg {
                                Some(f) => f.clone(),
                                None => SchemaField::new(
                                    String::new(),
                                    ty.clone(),
                                    ty.get_size(),
                                    0,
                                    0,
                                    false,
                                ),
                            };
                            field.name = format!("{}{}", COMPUTED_INPUT_PREFIX, j);
                            field.db_type = ty.clone();
                            field.write_order = j as u64;
                            synth_schema.push(field);

                            if let Some(bytes) = computed_accs[i][j].finish_nullable(input.func, &ty)? {
                                synth_columns.push(Column {
                                    schema_id: crate::core::row::row::schema_id_of(j as u64),
                                    data: mb_from(&bytes),
                                    column_type: ty,
                                });
                            }
                        }
                        let synth = Row {
                            position: 0,
                            columns: synth_columns,
                            length: 0,
                        };
                        let value = crate::core::rql::expr::eval(expr, &synth, &synth_schema)?;
                        match value {
                            crate::core::rql::expr::Value::Null => (Vec::new(), db_type.clone()),
                            crate::core::rql::expr::Value::Data { bytes, db_type: ty } => (bytes, ty),
                        }
                    }
                };
                columns.push(Column {
                    schema_id: crate::core::row::row::schema_id_of(i as u64),
                    data: mb_from(&bytes),
                    column_type: ty,
                });
            }

            out.push(Row {
                position: 0,
                columns,
                length: 0,
            });
        }

        // Groups are sorted here rather than in the scan: they do not exist until
        // the fold has run.
        //
        // Each key is the position of an output column plus its direction and type.
        // Resolving them once, before the sort, keeps the label lookup out of the
        // comparator.
        // Shared with `fold` since the NULL-ordering fix: the sort and the
        // aggregate's NULL output changed meaning on purpose, and this copy
        // exists to pin the *grouping and accumulation* rewrite.
        sort_output(plan, &mut out);

        if let Some(off) = plan.offset {
            let off = off as usize;
            out = out.into_iter().skip(off).collect();
        }
        if let Some(limit) = plan.limit {
            out.truncate(limit as usize);
        }

        Ok(out)
    }
}

/// The rewritten [`fold`] checked against [`fold_reference::fold_reference`]
/// over generated inputs, plus an `#[ignore]`d timing harness.
#[cfg(test)]
mod fold_equivalence {
    use super::*;

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

    fn schema() -> Vec<SchemaField> {
        vec![
            field("k1", DbType::STRING, 0),
            field("k2", DbType::I64, 1),
            field("v", DbType::I64, 2),
            field("f", DbType::F64, 3),
            field("s", DbType::STRING, 4),
            field("d", DbType::DECIMAL, 5),
            // One byte wide: grouped through the direct-indexed table.
            field("u", DbType::U8, 6),
            field("b", DbType::BOOL, 7),
            field("i8", DbType::I8, 8),
        ]
    }

    /// A cell: `None` = absent from the row, `Some(None)` = NULL-typed,
    /// `Some(Some(bytes))` = a value.
    type Cell = Option<Option<Vec<u8>>>;

    fn gen_row(rng: &mut fastrand::Rng, groups: u64) -> Vec<(u64, DbType, Cell)> {
        let texts: [&str; 7] = ["", "a", "ab", "b", "Ünïcødé", "🦀", "a\u{0}b"];
        let pick_null = |rng: &mut fastrand::Rng| rng.u8(..) < 25;
        let mut cells = Vec::new();
        let k1: Cell = if rng.u8(..) < 6 {
            None
        } else if pick_null(rng) {
            Some(None)
        } else {
            Some(Some(texts[rng.usize(..texts.len())].as_bytes().to_vec()))
        };
        cells.push((0, DbType::STRING, k1));
        let k2: Cell = if pick_null(rng) {
            Some(None)
        } else {
            let v = match rng.u8(..4) {
                0 => i64::MIN,
                1 => i64::MAX,
                _ => rng.i64(-(groups as i64)..=(groups as i64)),
            };
            Some(Some(v.to_le_bytes().to_vec()))
        };
        cells.push((1, DbType::I64, k2));
        let v: Cell = if pick_null(rng) {
            Some(None)
        } else {
            Some(Some(rng.i64(-1000..1000).to_le_bytes().to_vec()))
        };
        cells.push((2, DbType::I64, v));
        let f: Cell = if pick_null(rng) {
            Some(None)
        } else {
            Some(Some((rng.f64() * 200.0 - 100.0).to_le_bytes().to_vec()))
        };
        cells.push((3, DbType::F64, f));
        let s: Cell = if pick_null(rng) {
            Some(None)
        } else {
            Some(Some(texts[rng.usize(..texts.len())].as_bytes().to_vec()))
        };
        cells.push((4, DbType::STRING, s));
        let d: Cell = if pick_null(rng) {
            Some(None)
        } else {
            Some(Some((rng.i64(-5000..5000) as i128 * 1_000_000).to_le_bytes().to_vec()))
        };
        cells.push((5, DbType::DECIMAL, d));
        // A one-byte key: absent, NULL, a value — and, rarely, bytes of
        // another width, which must take the hashed path and still group
        // apart from every one-byte value.
        for (wo, ty) in [(6u64, DbType::U8), (7, DbType::BOOL), (8, DbType::I8)] {
            let cell: Cell = match rng.u8(..100) {
                0..=3 => None,
                4..=14 => Some(None),
                15 => Some(Some(vec![1, 0])),
                16 => Some(Some(Vec::new())),
                _ => Some(Some(vec![[0u8, 1, 2, 7, 127, 128, 255][rng.usize(..7)]])),
            };
            cells.push((wo, ty, cell));
        }
        cells
    }

    fn build(cells: &[(u64, DbType, Cell)], shuffle: Option<&mut fastrand::Rng>) -> Row {
        let mut columns: crate::core::row::row::RowColumns = SmallVec::new();
        for (wo, ty, cell) in cells {
            match cell {
                None => {}
                Some(None) => columns.push(Column {
                    schema_id: crate::core::row::row::schema_id_of(*wo),
                    data: mb_from(&[]),
                    column_type: DbType::NULL,
                }),
                Some(Some(b)) => columns.push(Column {
                    schema_id: crate::core::row::row::schema_id_of(*wo),
                    data: mb_from(b),
                    column_type: ty.clone(),
                }),
            }
        }
        if let Some(rng) = shuffle {
            let mut v: Vec<Column> = columns.into_iter().collect();
            rng.shuffle(&mut v);
            columns = v.into_iter().collect();
        }
        Row {
            position: 0,
            columns,
            length: 0,
        }
    }

    fn agg(func: AggFunc, arg: Option<SchemaField>, label: &str) -> SelectItem {
        SelectItem::Aggregate {
            func,
            arg,
            label: label.to_string(),
            filter: None,
            arg_expr: None,
        }
    }

    fn render(rows: &[Row]) -> Vec<Vec<(u64, u8, Vec<u8>)>> {
        rows.iter()
            .map(|r| {
                r.columns
                    .iter()
                    .map(|c| {
                        (
                            u64::from(c.schema_id),
                            c.column_type.to_byte(),
                            c.data.into_slice().to_vec(),
                        )
                    })
                    .collect()
            })
            .collect()
    }

    fn plans() -> Vec<AggregatePlan> {
        let s = schema();
        let k1 = s[0].clone();
        let k2 = s[1].clone();
        let v = s[2].clone();
        let f = s[3].clone();
        let st = s[4].clone();
        let d = s[5].clone();
        let all_aggs = |items: &mut Vec<SelectItem>| {
            items.push(agg(AggFunc::Count, None, "n"));
            items.push(agg(AggFunc::Count, Some(v.clone()), "nv"));
            items.push(agg(AggFunc::Count, Some(k1.clone()), "nk1"));
            items.push(agg(AggFunc::Sum, Some(v.clone()), "sv"));
            items.push(agg(AggFunc::Avg, Some(v.clone()), "av"));
            items.push(agg(AggFunc::Min, Some(v.clone()), "minv"));
            items.push(agg(AggFunc::Max, Some(v.clone()), "maxv"));
            items.push(agg(AggFunc::Min, Some(st.clone()), "mins"));
            items.push(agg(AggFunc::Max, Some(st.clone()), "maxs"));
            items.push(agg(AggFunc::Sum, Some(f.clone()), "sf"));
            items.push(agg(AggFunc::Max, Some(f.clone()), "maxf"));
            items.push(agg(AggFunc::Avg, Some(d.clone()), "ad"));
            items.push(agg(AggFunc::Sum, Some(d.clone()), "sd"));
        };
        let mut out = Vec::new();

        // A single one-byte key — the direct-indexed table — echoed, with
        // HAVING, ordering and paging, for each of the three widths' types.
        for (at, label) in [(6usize, "u"), (7, "b"), (8, "i8")] {
            let key = s[at].clone();
            let mut items = vec![SelectItem::Column {
                field: key.clone(),
                label: label.into(),
            }];
            all_aggs(&mut items);
            out.push(AggregatePlan {
                items: items.clone(),
                group_by: vec![key.clone()],
                computed_keys: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            });
            out.push(AggregatePlan {
                items,
                group_by: vec![key.clone()],
                computed_keys: vec![],
                having: Some(Having::single(AggFunc::Count, None, HavingOp::Gt, "1").unwrap()),
                order_by: vec![(label.into(), true, None)],
                limit: Some(5),
                offset: Some(1),
            });
        }
        // A one-byte key beside another: hashed, as before.
        let mut items = vec![SelectItem::Column {
            field: s[6].clone(),
            label: "u".into(),
        }];
        all_aggs(&mut items);
        out.push(AggregatePlan {
            items,
            group_by: vec![s[6].clone(), k1.clone()],
            computed_keys: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        });

        // No GROUP BY at all.
        let mut items = Vec::new();
        all_aggs(&mut items);
        out.push(AggregatePlan {
            items,
            group_by: vec![],
            computed_keys: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        });

        // One text key, echoed, with a literal.
        let mut items = vec![
            SelectItem::Column {
                field: k1.clone(),
                label: "k1".into(),
            },
            SelectItem::Literal {
                bytes: 7i64.to_le_bytes().to_vec(),
                db_type: DbType::I64,
                label: "seven".into(),
            },
        ];
        all_aggs(&mut items);
        out.push(AggregatePlan {
            items,
            group_by: vec![k1.clone()],
            computed_keys: vec![],
            having: None,
            order_by: vec![("k1".into(), false, None), ("n".into(), true, None)],
            limit: None,
            offset: None,
        });

        // Two keys, select list in the other order, HAVING, paging.
        let mut items = vec![
            SelectItem::Column {
                field: k2.clone(),
                label: "k2".into(),
            },
            SelectItem::Column {
                field: k1.clone(),
                label: "k1".into(),
            },
        ];
        all_aggs(&mut items);
        out.push(AggregatePlan {
            items: items.clone(),
            group_by: vec![k1.clone(), k2.clone()],
            computed_keys: vec![],
            having: Some(Having::single(AggFunc::Count, None, HavingOp::Gt, "1").unwrap()),
            order_by: vec![("sv".into(), true, Some(false))],
            limit: Some(17),
            offset: Some(3),
        });

        // A key not echoed, HAVING on a column aggregate.
        let mut items = Vec::new();
        all_aggs(&mut items);
        out.push(AggregatePlan {
            items,
            group_by: vec![k2.clone()],
            computed_keys: vec![],
            having: Some(Having::single(AggFunc::Max, Some(v.clone()), HavingOp::Ge, "0").unwrap()),
            order_by: vec![],
            limit: None,
            offset: None,
        });

        // A FILTER, and a computed key echoed by a RowExpr.
        let sch = schema();
        let cond = crate::core::rql::expr::parse_cond("v > 0", &sch).expect("cond");
        let key_expr = crate::core::rql::expr::parse("k2 + 1", &sch).expect("expr");
        let synthetic = field("__key", DbType::I64, 1000);
        let mut items = vec![
            SelectItem::RowExpr {
                expr: key_expr.clone(),
                db_type: DbType::I64,
                label: "k2p1".into(),
            },
            SelectItem::Aggregate {
                func: AggFunc::Sum,
                arg: Some(v.clone()),
                label: "pos".into(),
                filter: Some(AggregateFilter {
                    condition: cond,
                    schema: sch.clone(),
                }),
                arg_expr: None,
            },
        ];
        all_aggs(&mut items);
        out.push(AggregatePlan {
            items,
            group_by: vec![synthetic, k1.clone()],
            computed_keys: vec![ComputedKey {
                write_order: 1000,
                expr: key_expr,
                schema: sch,
                item: Some(0),
            }],
            having: None,
            order_by: vec![("k2p1".into(), false, Some(true))],
            limit: None,
            offset: None,
        });
        out
    }

    /// `finish_into` — serialized as it goes, paged without a `Vec<Row>` when
    /// there is no ORDER BY — writes exactly the rows `finish` returns.
    #[test]
    fn serialized_finish_matches_finish() {
        let mut rng = fastrand::Rng::with_seed(0xf1a1);
        let mut plans = plans();
        // Paging without ORDER BY, the shape `finish_into` pages itself.
        let mut paged = plans[3].clone();
        paged.order_by.clear();
        paged.limit = Some(4);
        paged.offset = Some(2);
        plans.push(paged.clone());
        paged.having = Some(Having::single(AggFunc::Count, None, HavingOp::Gt, "1").unwrap());
        plans.push(paged);
        for round in 0..30 {
            let n = [0usize, 1, 50, 300][round % 4];
            let rows: Vec<Row> = (0..n).map(|_| build(&gen_row(&mut rng, [3u64, 50][round % 2]), None)).collect();
            for (pi, plan) in plans.iter().enumerate() {
                let mut a = Folder::new(plan);
                let mut b = Folder::new(plan);
                // A computed key can overflow (`k2 + 1` at i64::MAX): that
                // row fails both folds alike and leaves nothing to compare.
                let pushed: Result<(), String> =
                    rows.iter().try_for_each(|row| a.push(row).and_then(|()| b.push(row)));
                if pushed.is_err() {
                    continue;
                }
                let want = a.finish().map(|rows| {
                    let mut bytes = Vec::new();
                    for row in rows {
                        row.extend_serialized(&mut bytes);
                    }
                    bytes
                });
                let mut got = Vec::new();
                let got = b.finish_into(&mut got).map(|()| got);
                assert_eq!(got, want, "round {} plan {}", round, pi);
            }
        }
    }

    /// Folders over disjoint chunks of the input, merged, emit what one folder
    /// over all of it emits: the same groups in the same (first-seen) order,
    /// the same bytes — floats up to the rounding that re-associating a sum
    /// allows.
    #[test]
    fn merged_partial_folds_match_one_fold() {
        let mut rng = fastrand::Rng::with_seed(0x3e46e);
        let plans = plans();
        let close = |a: &[Vec<(u64, u8, Vec<u8>)>], b: &[Vec<(u64, u8, Vec<u8>)>]| -> bool {
            a.len() == b.len()
                && a.iter().zip(b).all(|(ra, rb)| {
                    ra.len() == rb.len()
                        && ra.iter().zip(rb).all(|(ca, cb)| {
                            if ca == cb {
                                return true;
                            }
                            let f64_tag = DbType::F64.to_byte();
                            if ca.1 != f64_tag || cb.1 != f64_tag || ca.2.len() != 8 || cb.2.len() != 8 {
                                return false;
                            }
                            let x = f64::from_le_bytes(ca.2[..].try_into().unwrap());
                            let y = f64::from_le_bytes(cb.2[..].try_into().unwrap());
                            (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0)
                        })
                })
        };
        for round in 0..40 {
            let n = match round % 5 {
                0 => 0,
                1 => 1,
                _ => rng.usize(2..400),
            };
            let groups = [1u64, 3, 50, 1000][round % 4];
            let rows: Vec<Row> = (0..n).map(|_| build(&gen_row(&mut rng, groups), None)).collect();
            let cuts = {
                let mut c = vec![rng.usize(0..=n), rng.usize(0..=n)];
                c.sort();
                c
            };
            for (pi, plan) in plans.iter().enumerate() {
                let one = fold(plan, &rows);
                let mut parts = [
                    Folder::new(plan),
                    Folder::new(plan),
                    Folder::new(plan),
                ];
                let mut failed = None;
                for (i, row) in rows.iter().enumerate() {
                    let part = if i < cuts[0] { 0 } else if i < cuts[1] { 1 } else { 2 };
                    if let Err(e) = parts[part].push(row) {
                        failed = Some(e);
                        break;
                    }
                }
                let [mut first, second, third] = parts;
                let merged = match failed {
                    Some(e) => Err(e),
                    None => {
                        first.merge(second);
                        first.merge(third);
                        first.finish()
                    }
                };
                match (one, merged) {
                    (Ok(a), Ok(b)) => assert!(
                        close(&render(&a), &render(&b)),
                        "round {} plan {}: merged fold differs\n{:?}\n{:?}",
                        round,
                        pi,
                        render(&a),
                        render(&b)
                    ),
                    (Err(a), Err(b)) => assert_eq!(a, b, "round {} plan {}", round, pi),
                    (a, b) => panic!("round {} plan {}: one {:?} merged {:?}", round, pi, a.is_ok(), b.is_ok()),
                }
            }
        }
    }

    /// The rewrite must emit exactly what the old fold emitted: same rows,
    /// same order, same bytes and types — over NULL vs '' keys, absent
    /// columns, non-ASCII text, extreme integers, shuffled column layouts,
    /// empty input, and every accumulator kind.
    #[test]
    fn rewritten_fold_matches_the_reference_on_generated_inputs() {
        let mut rng = fastrand::Rng::with_seed(0x5eed_f01d);
        let plans = plans();
        for round in 0..60 {
            let n = match round % 6 {
                0 => 0,
                1 => 1,
                _ => rng.usize(2..400),
            };
            let groups = [1u64, 3, 50, 1000][round % 4];
            // Every few rounds the column order differs row to row, which is
            // what the per-plan position cache must survive.
            let shuffle = round % 3 == 2;
            let rows: Vec<Row> = (0..n)
                .map(|_| {
                    let cells = gen_row(&mut rng, groups);
                    if shuffle && rng.bool() {
                        let mut r2 = fastrand::Rng::with_seed(rng.u64(..));
                        build(&cells, Some(&mut r2))
                    } else {
                        build(&cells, None)
                    }
                })
                .collect();
            for (pi, plan) in plans.iter().enumerate() {
                let want = fold_reference::fold_reference(plan, &rows);
                let got = fold(plan, &rows);
                // The one-byte table against the hashed path it stands in for.
                match (fold_with(plan, &rows, false), &got) {
                    (Ok(h), Ok(g)) => assert_eq!(render(g), render(&h), "round {} plan {}: byte table", round, pi),
                    (Err(h), Err(g)) => assert_eq!(g, &h),
                    (h, g) => panic!("round {} plan {}: hashed {:?} vs table {:?}", round, pi, h.is_ok(), g.is_ok()),
                }
                match (want, got) {
                    (Ok(w), Ok(g)) => assert_eq!(
                        render(&g),
                        render(&w),
                        "round {} plan {} ({} rows)",
                        round,
                        pi,
                        n
                    ),
                    (Err(w), Err(g)) => assert_eq!(g, w),
                    (w, g) => panic!(
                        "round {} plan {}: reference {:?} vs rewrite {:?}",
                        round,
                        pi,
                        w.map(|r| r.len()),
                        g.map(|r| r.len())
                    ),
                }
            }
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
    /// byte_key_bench --nocapture`. 1M rows grouped by a nullable `U8` and by
    /// a `BOOL`: the direct-indexed table against hashing, interleaved.
    #[test]
    #[ignore]
    fn byte_key_bench() {
        let s = schema();
        let v = s[2].clone();
        for (at, label, values) in [(6usize, "u8", 256usize), (7, "bool", 2)] {
            let key = s[at].clone();
            let ty = key.db_type.clone();
            let plan = AggregatePlan {
                items: vec![
                    SelectItem::Column {
                        field: key.clone(),
                        label: "k".into(),
                    },
                    agg(AggFunc::Count, None, "n"),
                    agg(AggFunc::Sum, Some(v.clone()), "sv"),
                ],
                group_by: vec![key],
                computed_keys: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
            };
            let mut rng = fastrand::Rng::with_seed(values as u64);
            let rows: Vec<Row> = (0..1_000_000u64)
                .map(|i| {
                    let k: Cell = if rng.u8(..20) == 0 {
                        Some(None)
                    } else {
                        Some(Some(vec![rng.usize(..values) as u8]))
                    };
                    build(
                        &[
                            (at as u64, ty.clone(), k),
                            (2, DbType::I64, Some(Some((i as i64).to_le_bytes().to_vec()))),
                        ],
                        None,
                    )
                })
                .collect();
            for round in 0..5 {
                let t = std::time::Instant::now();
                let a = fold_with(&plan, &rows, false).unwrap();
                let hashed = t.elapsed().as_secs_f64() * 1000.0;
                let t = std::time::Instant::now();
                let b = fold_with(&plan, &rows, true).unwrap();
                let table = t.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(render(&a), render(&b));
                println!(
                    "byte_key_bench {label} round {round}: hashed {hashed:.1}ms, table {table:.1}ms ({:.2}x), {} groups",
                    hashed / table,
                    b.len()
                );
            }
        }
    }

    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// fold_bench --nocapture`. 1M rows of (key i64, v i64, s text), grouped
    /// into ~1k and ~100k groups: key echo, COUNT(*), SUM(v), MAX(v), MIN(s).
    #[test]
    #[ignore]
    fn fold_bench() {
        let s = schema();
        let k2 = s[1].clone();
        let v = s[2].clone();
        let st = s[4].clone();
        let plan = AggregatePlan {
            items: vec![
                SelectItem::Column {
                    field: k2.clone(),
                    label: "k".into(),
                },
                agg(AggFunc::Count, None, "n"),
                agg(AggFunc::Sum, Some(v.clone()), "sv"),
                agg(AggFunc::Max, Some(v.clone()), "mv"),
                agg(AggFunc::Min, Some(st.clone()), "ms"),
            ],
            group_by: vec![k2],
            computed_keys: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let which = std::env::var("FOLD_BENCH_GROUPS").ok();
        for groups in [1_000u64, 100_000] {
            if which.as_deref().is_some_and(|w| w != groups.to_string()) {
                continue;
            }
            let mut rng = fastrand::Rng::with_seed(groups);
            let rows: Vec<Row> = (0..1_000_000u64)
                .map(|i| {
                    let key = (rng.u64(..) % groups) as i64;
                    let cells: Vec<(u64, DbType, Cell)> = vec![
                        (1, DbType::I64, Some(Some(key.to_le_bytes().to_vec()))),
                        (2, DbType::I64, Some(Some((i as i64).to_le_bytes().to_vec()))),
                        (
                            4,
                            DbType::STRING,
                            Some(Some(format!("name-{}", i % 977).into_bytes())),
                        ),
                    ];
                    build(&cells, None)
                })
                .collect();
            let base = peak_rss_kb();
            let mut times = Vec::new();
            let mut groups_out = 0;
            for _ in 0..5 {
                let t = std::time::Instant::now();
                let out = fold(&plan, &rows).unwrap();
                times.push(t.elapsed().as_secs_f64() * 1000.0);
                groups_out = out.len();
            }
            times.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!(
                "fold_bench groups={} out={} min={:.1}ms median={:.1}ms peak_rss_delta={}MB",
                groups,
                groups_out,
                times[0],
                times[2],
                (peak_rss_kb().saturating_sub(base)) / 1024
            );
        }
    }

    /// A document key groups by value: `{"a":1}`, `{"a":1.0}` and `{"a":1e0}`
    /// are one group, and `[1]` and `[1.0]` another, while NULL stays a group
    /// of its own. A group shows its first row's document as written, not the
    /// key it was grouped by. Folded whole, and folded in parts and merged,
    /// the same groups come out in the same order showing the same documents.
    #[test]
    fn document_keys_group_by_value_whole_and_merged() {
        let doc_field = field("doc", DbType::JSONB, 0);
        let plan = AggregatePlan {
            items: vec![
                SelectItem::Column {
                    field: doc_field.clone(),
                    label: "doc".into(),
                },
                agg(AggFunc::Count, None, "n"),
            ],
            group_by: vec![doc_field],
            computed_keys: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let doc = |t: &str| crate::core::jsonb::encode_text(t).unwrap();
        let cells: Vec<Cell> = vec![
            Some(Some(doc(r#"{"a":1.0}"#))),
            Some(Some(doc("[1]"))),
            Some(None),
            Some(Some(doc(r#"{"a":1}"#))),
            Some(Some(doc("[1.0]"))),
            Some(Some(doc(r#"{"a":1e0}"#))),
            Some(None),
        ];
        let rows: Vec<Row> = cells
            .into_iter()
            .map(|cell| build(&[(0, DbType::JSONB, cell)], None))
            .collect();
        let shown = |out: &[Row]| -> Vec<(Vec<u8>, i64)> {
            out.iter()
                .map(|r| {
                    let n = r.columns[1].data.into_slice().try_into().unwrap();
                    (r.columns[0].data.into_slice().to_vec(), i64::from_le_bytes(n))
                })
                .collect()
        };
        let want = vec![(doc(r#"{"a":1.0}"#), 3), (doc("[1]"), 2), (Vec::new(), 2)];
        assert_eq!(shown(&fold(&plan, &rows).unwrap()), want);
        for cut in 0..=rows.len() {
            let (mut first, mut second) = (Folder::new(&plan), Folder::new(&plan));
            for (i, row) in rows.iter().enumerate() {
                let part = if i < cut { &mut first } else { &mut second };
                part.push(row).unwrap();
            }
            first.merge(second);
            assert_eq!(shown(&first.finish().unwrap()), want, "cut at {cut}");
        }
    }
}

#[cfg(test)]
mod uuidv7_tests {
    use super::*;

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    #[test]
    fn version_and_variant_bits() {
        for _ in 0..1000 {
            let u = uuid_v7();
            assert_eq!(u[6] >> 4, 7, "version nibble");
            assert_eq!(u[8] & 0xc0, 0x80, "RFC 9562 variant");
            assert_eq!(uuid_extract_version(&u), Some(7));
        }
        let (f, t) = ScalarFunc::parse("uuidv7").unwrap();
        assert!(matches!(f, ScalarFunc::UuidV7));
        assert_eq!(t, DbType::UUID);
        assert_eq!(f.evaluate().len(), 16);
        assert!(matches!(ScalarFunc::parse("UUIDv4"), Some((ScalarFunc::GenRandomUuid, _))));
    }

    #[test]
    fn strictly_increasing_within_a_thread() {
        let mut prev = uuid_v7();
        for _ in 0..100_000 {
            let next = uuid_v7();
            assert!(next > prev, "{:02x?} after {:02x?}", next, prev);
            prev = next;
        }
    }

    /// Every value from every thread is distinct and, within each thread,
    /// increasing; and the 74 time-ordered bits never repeat across threads,
    /// since the clock is one process-wide atomic.
    #[test]
    fn monotonic_and_distinct_across_threads() {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    let mut out = Vec::with_capacity(20_000);
                    for _ in 0..20_000 {
                        out.push(uuid_v7());
                    }
                    out
                })
            })
            .collect();
        let mut all = Vec::new();
        for h in handles {
            let v = h.join().unwrap();
            assert!(v.windows(2).all(|w| w[0] < w[1]), "per-thread order");
            all.extend(v);
        }
        let mut times: Vec<[u8; 8]> = all
            .iter()
            .map(|u| {
                // 48-bit ms + 12-bit sub-ms, without the version nibble.
                let mut k = [0u8; 8];
                k[..6].copy_from_slice(&u[..6]);
                k[6] = u[6] & 0x0f;
                k[7] = u[7];
                k
            })
            .collect();
        let n = times.len();
        times.sort();
        times.dedup();
        assert_eq!(times.len(), n, "two threads were handed the same timestamp");
    }

    #[test]
    fn timestamp_extraction() {
        let before = now_ms();
        let u = uuid_v7();
        let after = now_ms();
        let us = uuid_extract_timestamp_us(&u).unwrap();
        assert_eq!(us % 1000, 0, "PostgreSQL reads the millisecond field only");
        // The monotonic step can carry the timestamp ahead of the clock when
        // values are drawn faster than one per 245 ns — by design, as in
        // PostgreSQL — and the other tests here draw hundreds of thousands at
        // once. A second of slack covers that; a wrong field would be years off.
        assert!(
            (before..=after + 1000).contains(&(us / 1000)),
            "{} not in {}..{}",
            us / 1000,
            before,
            after
        );
        // A fixed v7: 2022-02-22 19:22:22 UTC = 1645557742000 ms (RFC 9562's
        // example value).
        let example = crate::core::value_codec::parse_uuid("017f22e2-79b0-7cc3-98c4-dc0c0c07398f").unwrap();
        assert_eq!(uuid_extract_version(&example), Some(7));
        assert_eq!(uuid_extract_timestamp_us(&example), Some(1_645_557_742_000_000));
        // RFC 9562's v1 example: 2022-02-22 19:22:22 UTC.
        let v1 = crate::core::value_codec::parse_uuid("c232ab00-9414-11ec-b3c8-9f6bdeced846").unwrap();
        assert_eq!(uuid_extract_version(&v1), Some(1));
        assert_eq!(uuid_extract_timestamp_us(&v1), Some(1_645_557_742_000_000));
        // A v4 has no timestamp; a non-RFC variant has no version.
        let v4 = crate::core::value_codec::random_uuid_v4();
        assert_eq!(uuid_extract_version(&v4), Some(4));
        assert_eq!(uuid_extract_timestamp_us(&v4), None);
        let mut ncs = v4;
        ncs[8] &= 0x3f;
        assert_eq!(uuid_extract_version(&ncs), None);
    }

    /// The sub-millisecond field: the clock's fraction of a millisecond,
    /// scaled to 12 bits, and the step that keeps a stalled clock monotonic.
    #[test]
    fn sub_millisecond_field_and_step() {
        let u = uuid_v7_at(1_000_000_000_500_000); // 1e9 ms + half a ms
        assert_eq!(((u[6] & 0x0f) as u16) << 8 | u[7] as u16, 2048);
        let a = uuid_v7_at(5_000_000);
        let b = uuid_v7_at(5_000_000 + UUIDV7_MIN_STEP_NS);
        assert!(b[..8] > a[..8], "one step advances the 12-bit field");
    }
}
