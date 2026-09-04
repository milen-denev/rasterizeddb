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
use crate::core::value_codec::{self, DECIMAL_SCALE};
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
    pub fn result_type(&self, arg: Option<&DbType>) -> DbType {
        match self {
            AggFunc::Count => DbType::I64,
            AggFunc::Min | AggFunc::Max => arg.cloned().unwrap_or(DbType::I64),
            AggFunc::Sum => match arg {
                Some(DbType::F32) | Some(DbType::F64) => DbType::F64,
                Some(DbType::DECIMAL) => DbType::DECIMAL,
                _ => DbType::I64,
            },
            AggFunc::Avg => match arg {
                Some(DbType::F32) | Some(DbType::F64) => DbType::F64,
                _ => DbType::DECIMAL,
            },
        }
    }
}

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
    /// reads no column, and is the same in every row.
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
}

impl ScalarFunc {
    pub fn parse(name: &str) -> Option<(ScalarFunc, DbType)> {
        match name.trim().to_ascii_uppercase().as_str() {
            "NOW" | "CURRENT_TIMESTAMP" => Some((ScalarFunc::Now, DbType::DATETIME)),
            "CURRENT_DATE" => Some((ScalarFunc::CurrentDate, DbType::DATETIME)),
            "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" => {
                Some((ScalarFunc::GenRandomUuid, DbType::UUID))
            }
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
                let mut bytes = [0u8; 16];
                for chunk in bytes.chunks_mut(8) {
                    let r = fastrand::u64(..).to_le_bytes();
                    chunk.copy_from_slice(&r[..chunk.len()]);
                }
                bytes[6] = (bytes[6] & 0x0f) | 0x40;
                bytes[8] = (bytes[8] & 0x3f) | 0x80;
                bytes.to_vec()
            }
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

/// A `HAVING` predicate: one aggregate compared against a constant.
///
/// Deliberately narrow. `HAVING` almost always filters on a count or a sum, and
/// a general expression evaluator over grouped results would duplicate the
/// `WHERE` machinery for a form the schema barely uses.
#[derive(Debug, Clone)]
pub struct Having {
    pub func: AggFunc,
    pub arg: Option<SchemaField>,
    pub op: HavingOp,
    /// The right-hand constant, as written.
    pub literal: String,
}

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

    fn holds(&self, ordering: std::cmp::Ordering) -> bool {
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
    /// Labels to sort the output by, each with its own direction. Later keys
    /// only break the ties the earlier ones left.
    pub order_by: Vec<(String, bool)>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// A running accumulator for one aggregate within one group.
#[derive(Debug, Clone)]
enum Acc {
    Count(i64),
    /// Exact running total plus the count, for `SUM` and `AVG` over exact
    /// types. `scaled` records whether the input was already in `DECIMAL`
    /// units, which decides whether `AVG` has to scale before dividing.
    ExactSum {
        total: i128,
        n: i64,
        scaled: bool,
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
    fn new(func: AggFunc, arg: Option<&SchemaField>) -> Self {
        match func {
            AggFunc::Count => Acc::Count(0),
            AggFunc::Min | AggFunc::Max => Acc::Extreme {
                best: None,
                ty: arg.map(|f| f.db_type.clone()).unwrap_or(DbType::I64),
                want_max: func == AggFunc::Max,
            },
            AggFunc::Sum | AggFunc::Avg => match arg.map(|f| &f.db_type) {
                Some(DbType::F32) | Some(DbType::F64) => Acc::FloatSum { total: 0.0, n: 0 },
                other => Acc::ExactSum {
                    total: 0,
                    n: 0,
                    scaled: matches!(other, Some(DbType::DECIMAL)),
                },
            },
        }
    }

    fn accumulate(&mut self, bytes: Option<&[u8]>, ty: Option<&DbType>) {
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
            Acc::ExactSum { total, n, .. } => {
                let (Some(b), Some(t)) = (bytes, ty) else {
                    return;
                };
                if let Some(v) = decode_exact(b, t) {
                    *total = total.saturating_add(v);
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
                        if replace {
                            *best = Some(b.to_vec());
                        }
                    }
                }
            }
        }
    }

    /// Renders the accumulator as the bytes of `result_type`.
    /// Like [`Acc::finish`], but reports SQL NULL as `None`.
    ///
    /// `finish` returns zero bytes for a `MIN`/`MAX` that saw no value, which
    /// reads back as the number 0. That is the wrong answer for
    /// `COALESCE(MAX(x), 0)` — the COALESCE would never fire, and a group whose
    /// values were all NULL would be indistinguishable from one whose maximum
    /// really is zero. Only the computed-expression path uses this; the existing
    /// aggregate output keeps its current shape.
    fn finish_nullable(&self, func: AggFunc, result_type: &DbType) -> Option<Vec<u8>> {
        match self {
            Acc::Extreme { best: None, .. } => None,
            // SUM and AVG over no rows at all are NULL in SQL; COUNT is not.
            Acc::ExactSum { n: 0, .. } | Acc::FloatSum { n: 0, .. }
                if !matches!(func, AggFunc::Count) =>
            {
                None
            }
            _ => Some(self.finish(func, result_type)),
        }
    }

    fn finish(&self, func: AggFunc, result_type: &DbType) -> Vec<u8> {
        match self {
            Acc::Count(n) => n.to_le_bytes().to_vec(),
            Acc::ExactSum { total, n, scaled } => match func {
                AggFunc::Avg => {
                    // Averaging exact values stays exact: the division happens
                    // in scaled units so a money column keeps its cents.
                    if *n == 0 {
                        return encode_exact(0, result_type);
                    }
                    let value = if matches!(result_type, DbType::DECIMAL) {
                        // A DECIMAL input is already in scaled units; anything
                        // else has to be lifted into them before dividing, or
                        // the fractional part would be truncated away.
                        let numerator = if *scaled {
                            *total
                        } else {
                            total.saturating_mul(pow10(DECIMAL_SCALE))
                        };
                        numerator / (*n as i128)
                    } else {
                        *total / (*n as i128)
                    };
                    encode_exact(value, result_type)
                }
                _ => encode_exact(*total, result_type),
            },
            Acc::FloatSum { total, n } => {
                let v = match func {
                    AggFunc::Avg if *n > 0 => *total / (*n as f64),
                    AggFunc::Avg => 0.0,
                    _ => *total,
                };
                v.to_le_bytes().to_vec()
            }
            Acc::Extreme { best, ty, .. } => best
                .clone()
                .unwrap_or_else(|| vec![0u8; ty.get_size() as usize]),
        }
    }
}

fn pow10(n: u32) -> i128 {
    10i128.pow(n)
}

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
        DbType::U128 => u128::from_le_bytes(le::<16>(bytes)?) as i128,
        _ => return None,
    })
}

fn encode_exact(v: i128, ty: &DbType) -> Vec<u8> {
    match ty {
        DbType::DECIMAL | DbType::I128 => v.to_le_bytes().to_vec(),
        DbType::F64 => (v as f64).to_le_bytes().to_vec(),
        _ => (v as i64).to_le_bytes().to_vec(),
    }
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

fn compare_values(a: &[u8], b: &[u8], ty: &DbType) -> std::cmp::Ordering {
    if let (Some(x), Some(y)) = (decode_exact(a, ty), decode_exact(b, ty)) {
        return x.cmp(&y);
    }
    if let (Some(x), Some(y)) = (decode_float(a, ty), decode_float(b, ty)) {
        return x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal);
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
        .find(|c| c.schema_id == write_order)
        .filter(|c| c.column_type != DbType::NULL)
        .map(|c| c.data.into_slice())
}

/// Folds scanned rows into grouped result rows.
///
/// The output rows are synthetic: each column's `schema_id` is its position in
/// the `SELECT` list, since an aggregate has no schema field to point at.
pub fn fold(plan: &AggregatePlan, rows: &[Row]) -> Result<Vec<Row>, String> {
    // Group key -> (key bytes per grouping column, accumulator per select item,
    // HAVING accumulator, and one accumulator per input of each computed item).
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut groups: std::collections::HashMap<
        Vec<u8>,
        (Vec<Option<Vec<u8>>>, Vec<Acc>, Option<Acc>, Vec<Vec<Acc>>),
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
            let having_acc = plan
                .having
                .as_ref()
                .map(|h| Acc::new(h.func, h.arg.as_ref()));
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

        if let (Some(h), Some(acc)) = (plan.having.as_ref(), entry.2.as_mut()) {
            let (bytes, ty) = match &h.arg {
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

        if let (Some(h), Some(acc)) = (plan.having.as_ref(), having_acc.as_ref()) {
            if !having_holds(h, acc) {
                continue;
            }
        }

        let mut columns: SmallVec<[Column; 32]> = SmallVec::new();
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
                SelectItem::Aggregate { func, .. } => {
                    let ty = item.result_type();
                    (accs[i].finish(*func, &ty), ty)
                }
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
                    let mut synth_columns: SmallVec<[Column; 32]> = SmallVec::new();
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

                        if let Some(bytes) = computed_accs[i][j].finish_nullable(input.func, &ty) {
                            synth_columns.push(Column {
                                schema_id: j as u64,
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
                schema_id: i as u64,
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
    let sort_keys: Vec<(usize, bool, DbType)> = plan
        .order_by
        .iter()
        .filter_map(|(label, descending)| {
            let idx = plan.items.iter().position(|i| i.label() == label)?;
            Some((idx, *descending, plan.items[idx].result_type()))
        })
        .collect();

    if !sort_keys.is_empty() {
        out.sort_by(|a, b| {
            for (idx, descending, ty) in sort_keys.iter() {
                let av = a
                    .columns
                    .get(*idx)
                    .map(|c| c.data.into_slice())
                    .unwrap_or(&[]);
                let bv = b
                    .columns
                    .get(*idx)
                    .map(|c| c.data.into_slice())
                    .unwrap_or(&[]);
                let ord = compare_values(av, bv, ty);
                let ord = if *descending { ord.reverse() } else { ord };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if let Some(off) = plan.offset {
        let off = off as usize;
        out = out.into_iter().skip(off).collect();
    }
    if let Some(limit) = plan.limit {
        out.truncate(limit as usize);
    }

    Ok(out)
}

fn having_holds(h: &Having, acc: &Acc) -> bool {
    let result_type = h.func.result_type(h.arg.as_ref().map(|f| &f.db_type));
    let lhs = acc.finish(h.func, &result_type);
    let Some(rhs) = value_codec::coerce_literal(
        h.literal.trim_matches('\'').as_bytes(),
        &DbType::STRING,
        &result_type,
    ) else {
        return true;
    };
    h.op.holds(compare_values(&lhs, &rhs, &result_type))
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
        let mut columns: SmallVec<[Column; 32]> = SmallVec::new();
        for (schema_id, ty, bytes) in values {
            columns.push(Column {
                schema_id: *schema_id,
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

    fn dec_of(row: &Row, idx: usize) -> String {
        let b = row.columns[idx].data.into_slice();
        let mut a = [0u8; 16];
        a.copy_from_slice(b);
        value_codec::format_decimal(i128::from_le_bytes(a))
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
        assert_eq!(i64_of(&out[0], 0), 60);

        let out = fold(&plan(vec![agg(AggFunc::Avg, Some(f))], vec![]), &rows).unwrap();
        assert_eq!(dec_of(&out[0], 0), "20");
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
        assert_eq!(dec_of(&out[0], 0), "1.5");
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
        p.order_by = vec![("status".to_string(), false)];

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
        p.having = Some(Having {
            func: AggFunc::Count,
            arg: None,
            op: HavingOp::Gt,
            literal: "1".to_string(),
        });

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
        p.order_by = vec![("count".to_string(), true)];
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
        assert_eq!(AggFunc::Sum.result_type(Some(&DbType::F32)), DbType::F64);
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
