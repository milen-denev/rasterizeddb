//! A batch prefilter for the scan's `WHERE` path: rows that cannot match are
//! dropped before they are fetched, many at a time with SIMD compares.
//!
//! # What it does
//!
//! The scan hands each worker a batch of row pointers (`batch_size()`, 10,000
//! by default), and the worker used to fetch every row's predicate columns
//! into pooled buffers and run the interpreter over them, one row at a time,
//! only to throw most of them away on a selective query. For the predicates
//! this understands, the worker now first reads the one column a comparison
//! needs for the whole batch, into a plain array, with a single vectored read,
//! and compares the array against the literal with [`crate::simds::compare`]:
//! 8 to 64 values per instruction depending on width and level. Only the rows
//! whose bit survives go on to the ordinary fetch and evaluation.
//!
//! # Why it cannot drop a matching row
//!
//! It is a **superset** filter, in exactly the sense the filter index is, and
//! it reuses that index's predicate extraction ([`sarg::extract_gated`]) so the
//! two read a clause the same way: `AND` and `OR` structure, parentheses, the
//! `IN`/`BETWEEN` desugarings, literal coercion. Every surviving row is still
//! evaluated in full, so being too generous costs time and never correctness.
//! Three things keep it from being too strict:
//!
//! * **The kernels are exact.** Each is held to `perform_comparison_operation`
//!   value for value, PostgreSQL's NaN order included (see the kernel module).
//! * **Only types those kernels decide exactly are admitted**, by the column's
//!   *declared* type: the integer widths, `BOOL`, `CHAR`, `DATETIME` and the two
//!   floats. Not `NUMERIC`, whose literal needs rescaling to the column's
//!   scale, and not text or anything stored as text (JSONB, BYTEA, arrays),
//!   whose order is not a fixed-width compare. A predicate on any other column
//!   is opaque, and an opaque term keeps every row.
//! * **NULL cannot cause a drop.** A NULL stores its type's zero, with the
//!   truth in a `__null_` companion, so the kernel compares a placeholder and
//!   its answer for that row is arbitrary. That is still sound, because only
//!   `AND`/`OR` trees of plain comparisons reach here: `NOT` is pushed down or
//!   left opaque by `sarg`. Such a tree is monotone, so it is TRUE only when
//!   some set of its comparisons is TRUE on non-NULL values, and the kernel
//!   reports those correctly. A NULL operand makes its comparison UNKNOWN,
//!   which can never be what made the row TRUE. So whatever the kernel says
//!   about a NULL, it cannot clear a row the `WHERE` would have kept.
//!
//! Rows the scan would skip anyway (deleted, or reclaimable and so possibly
//! overwritten) are not read at all. Their bit is left set and the scan's own
//! checks skip them as before. If the vectored read fails, the batch goes
//! through unfiltered, and the per-row fetch meets and reports the same error
//! it always did.
//!
//! # When it turns itself off
//!
//! Reading the column for a row that then passes is pure overhead: the row is
//! read again by the ordinary fetch. So the statement watches its pass rate,
//! across every worker and batch, and stops prefiltering once more than
//! [`GIVE_UP_PASS_RATE`] of the rows checked passed, after at least
//! [`GIVE_UP_AFTER`] of them. It is kept on the prefilter rather than per
//! worker because the `UPDATE`/`DELETE` scan spawns a task per batch, and a
//! per-task count would start from zero every 10,000 rows and never decide.
//!
//! # A refused term is still refused
//!
//! The `UPDATE`/`DELETE` scan compiles its `WHERE` plan lazily, on the first
//! row a batch fetches, so a clause the planner refuses fails the statement
//! when that row is reached. A batch therefore never skips a row before its
//! plan is built: the same first row is fetched and compiled as before, and an
//! all-dropping prefilter cannot turn a refusal into "0 rows affected".
//!
//! An error *evaluating* one conjunct can go unraised when another conjunct is
//! false for that row: `WHERE a > 100 AND 10 / b > 1` no longer divides by a
//! zero `b` on a row where `a <= 100`. PostgreSQL leaves the order in which
//! the parts of a boolean expression are evaluated undefined, and skips the
//! parts whose result cannot matter, so it makes the same choice.
//! `tests::prefilter_cost` measures both ends.
//!
//! # What it is worth, measured
//!
//! `COUNT(*)` over a 500k-row file-backed table, i9-11900K, medians of nine
//! interleaved rounds, speedup over the prefilter off:
//!
//! | rows passing | scalar | AVX2 | AVX-512 |
//! |---|---|---|---|
//! | 1%   | 1.76x | 1.91x | 2.36x |
//! | 10%  | 1.55x | 1.60x | 1.61x |
//! | 50%  | 1.19x | 1.09x | 1.27x |
//! | 100% | 0.74x | 0.94x | 1.01x |
//!
//! and 2.0x returning the rows of a 1% match. The levels barely differ,
//! because the kernel is not the cost: it compares a 10,000-row batch in 1 to
//! 4 µs (0.1 to 0.4 ns a value), against tens of nanoseconds for each row the
//! ordinary path fetches and interprets. The win is skipping that path for the
//! rows that fail. The 100% row is the price of the first batch before the
//! worker gives up, and scalar's 0.74x there is within this box's noise.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use smallvec::SmallVec;

use crate::core::db_type::DbType;
use crate::core::filter::sarg::{self, MaskExpr};
use crate::core::filter::zonemap::SargOp;
use crate::core::row::row::RowFetch;
use crate::core::row::row_pointer::RowPointer;
use crate::core::row::schema::SchemaField;
use crate::core::storage_providers::traits::StorageIO;
use crate::core::tokenizer::query_tokenizer::Token;
use crate::simds::compare::{self, CmpOp, Lane, words_for};

/// Rows a statement checks before it may decide the prefilter is not paying.
pub const GIVE_UP_AFTER: u64 = 10_000;
/// Above this fraction passing, the prefilter is overhead and is switched off
/// for the rest of the statement.
pub const GIVE_UP_PASS_RATE: f64 = 0.9;

static ENABLED: AtomicBool = AtomicBool::new(true);
static ROWS_CHECKED: AtomicU64 = AtomicU64::new(0);
static ROWS_DROPPED: AtomicU64 = AtomicU64::new(0);

/// `--scan-prefilter`. On by default.
pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// `(rows checked, rows dropped)` since the process started.
pub fn stats() -> (u64, u64) {
    (
        ROWS_CHECKED.load(Ordering::Relaxed),
        ROWS_DROPPED.load(Ordering::Relaxed),
    )
}

/// The column types the kernels decide exactly, by *declared* type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
}

impl Kind {
    /// The comparison `perform_comparison_operation` makes for each type:
    /// `BOOL` as `u8`, `CHAR` as `u32`, `DATETIME` as `i64`.
    fn of(ty: &DbType) -> Option<Kind> {
        Some(match ty {
            DbType::I8 => Kind::I8,
            DbType::U8 | DbType::BOOL => Kind::U8,
            DbType::I16 => Kind::I16,
            DbType::U16 => Kind::U16,
            DbType::I32 => Kind::I32,
            DbType::U32 | DbType::CHAR => Kind::U32,
            DbType::I64 | DbType::DATETIME => Kind::I64,
            DbType::U64 => Kind::U64,
            DbType::F32 => Kind::F32,
            DbType::F64 => Kind::F64,
            _ => return None,
        })
    }

    fn width(self) -> usize {
        match self {
            Kind::I8 | Kind::U8 => 1,
            Kind::I16 | Kind::U16 => 2,
            Kind::I32 | Kind::U32 | Kind::F32 => 4,
            Kind::I64 | Kind::U64 | Kind::F64 => 8,
        }
    }
}

#[derive(Clone, Debug)]
struct Column {
    offset: u32,
    kind: Kind,
}

#[derive(Clone, Debug)]
enum Node {
    /// Keeps every row.
    All,
    Pred {
        col: usize,
        op: CmpOp,
        /// The literal's little-endian bytes, `kind.width()` of them.
        lit: [u8; 8],
    },
    And(Vec<Node>),
    Or(Vec<Node>),
}

impl Node {
    fn is_all(&self) -> bool {
        matches!(self, Node::All)
    }
}

#[derive(Debug)]
pub(crate) struct BatchPrefilter {
    columns: SmallVec<[Column; 4]>,
    root: Node,
    /// Rows checked and rows passed, over the whole statement.
    seen: AtomicU64,
    passed: AtomicU64,
    /// Given up: see the module docs.
    off: AtomicBool,
}

/// One worker's reusable buffers, so a batch allocates nothing but its read
/// list.
#[derive(Default)]
pub(crate) struct Scratch {
    /// Batch index of each row read.
    idx: Vec<u32>,
    /// Where each of those rows starts.
    pos: Vec<u64>,
    /// One array per column, `u64`-backed so every lane type is aligned.
    cols: Vec<Vec<u64>>,
    /// Masks for evaluating the tree, reused across batches.
    pool: Vec<Vec<u64>>,
    /// The answer over the read rows.
    compact: Vec<u64>,
    /// The answer over the whole batch, one bit per pointer.
    keep: Vec<u64>,
}

/// Whether the row at batch index `i` may match.
#[inline(always)]
pub(crate) fn kept(keep: &[u64], i: usize) -> bool {
    keep[i >> 6] & (1u64 << (i & 63)) != 0
}

impl BatchPrefilter {
    /// Compile a prefilter for a tokenized `WHERE`, or `None` when nothing in
    /// it can be decided this way.
    ///
    /// `fetch` is the scan's `WHERE` fetch, so every admitted column is one the
    /// scan already reads, at the offset it reads it from.
    pub(crate) fn build(
        tokens: &[Token],
        schema: &[SchemaField],
        fetch: &RowFetch,
    ) -> Option<BatchPrefilter> {
        // The arrays are read as native integers, and the stored bytes are
        // little-endian.
        if cfg!(not(target_endian = "little")) || !enabled() || tokens.is_empty() {
            return None;
        }

        let mut usable: SmallVec<[(u32, Column); 8]> = SmallVec::new();
        for c in fetch.columns_fetching_data.iter() {
            let Some(field) = schema
                .iter()
                .find(|f| f.write_order == c.schema_id && !f.is_deleted)
            else {
                continue;
            };
            let Some(kind) = Kind::of(&field.db_type) else {
                continue;
            };
            // Stored as declared, and as wide as the kernel reads. Anything
            // else is a layout this was not written for.
            if c.column_type != field.db_type || c.size as usize != kind.width() {
                continue;
            }
            let Ok(wo) = u32::try_from(field.write_order) else {
                continue;
            };
            usable.push((
                wo,
                Column {
                    offset: c.column_offset,
                    kind,
                },
            ));
        }
        if usable.is_empty() {
            return None;
        }

        // No `NUMERIC` gets past the gate, so no scales are needed.
        let program = sarg::extract_gated(tokens, &|wo| usable.iter().any(|(u, _)| *u == wo), &[])?;

        let mut columns: SmallVec<[Column; 4]> = SmallVec::new();
        let mut slot_of: SmallVec<[(u32, usize); 4]> = SmallVec::new();
        let root = compile(&program.root, &usable, &mut columns, &mut slot_of);
        if root.is_all() {
            return None;
        }
        Some(BatchPrefilter {
            columns,
            root,
            seen: AtomicU64::new(0),
            passed: AtomicU64::new(0),
            off: AtomicBool::new(false),
        })
    }

    /// Choose the rows of a batch to check: every pointer except those `skip`
    /// names, which the scan will skip without reading. They are not read here
    /// either, and [`Self::run`] leaves their bit set. Returns whether there is
    /// anything for `run` to do.
    ///
    /// Separate from `run` so the predicate is not held across its `await`.
    pub(crate) fn select<'p>(
        &self,
        pointers: impl IntoIterator<Item = &'p RowPointer>,
        skip: impl Fn(&RowPointer) -> bool,
        s: &mut Scratch,
    ) -> bool {
        s.idx.clear();
        s.pos.clear();
        if self.off.load(Ordering::Relaxed) {
            return false;
        }
        for (i, p) in pointers.into_iter().enumerate() {
            if !skip(p) {
                s.idx.push(i as u32);
                s.pos.push(p.position);
            }
        }
        !s.idx.is_empty()
    }

    /// The keep mask for a batch of `n` pointers, one bit each, over the rows
    /// [`Self::select`] chose, or `None` to process the batch unfiltered.
    pub(crate) async fn run<'s, S: StorageIO>(
        &self,
        n: usize,
        io: &S,
        s: &'s mut Scratch,
    ) -> Option<&'s [u64]> {
        let m = s.idx.len();
        if m == 0 || self.off.load(Ordering::Relaxed) {
            return None;
        }

        // Gather: one vectored read for every column of every row.
        s.cols.resize_with(self.columns.len(), Vec::new);
        {
            let mut reads: Vec<(u64, &mut [u8])> = Vec::with_capacity(m * self.columns.len());
            for (col, buf) in self.columns.iter().zip(s.cols.iter_mut()) {
                let width = col.kind.width();
                buf.clear();
                buf.resize((m * width).div_ceil(8), 0);
                // SAFETY: a `u64` buffer of at least `m * width` bytes, viewed
                // as bytes; `u8` has no alignment or validity requirement.
                let bytes = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, m * width)
                };
                for (chunk, &at) in bytes.chunks_exact_mut(width).zip(s.pos.iter()) {
                    reads.push((at + col.offset as u64, chunk));
                }
            }
            if io.read_vectored(&mut reads).await.is_err() {
                return None;
            }
        }

        // Evaluate over the read rows.
        s.compact.clear();
        s.compact.resize(words_for(m), 0);
        let mut compact = std::mem::take(&mut s.compact);
        self.eval(&self.root, m, &s.cols, &mut s.pool, &mut compact);
        s.compact = compact;

        // Spread back over the batch; unread rows stay set.
        s.keep.clear();
        s.keep.resize(words_for(n), u64::MAX);
        let mut passed = 0u64;
        for (j, &i) in s.idx.iter().enumerate() {
            if kept(&s.compact, j) {
                passed += 1;
            } else {
                let i = i as usize;
                s.keep[i >> 6] &= !(1u64 << (i & 63));
            }
        }

        let m = m as u64;
        ROWS_CHECKED.fetch_add(m, Ordering::Relaxed);
        ROWS_DROPPED.fetch_add(m - passed, Ordering::Relaxed);
        let seen = self.seen.fetch_add(m, Ordering::Relaxed) + m;
        let passed = self.passed.fetch_add(passed, Ordering::Relaxed) + passed;
        if seen >= GIVE_UP_AFTER && passed as f64 > seen as f64 * GIVE_UP_PASS_RATE {
            self.off.store(true, Ordering::Relaxed);
        }

        Some(&s.keep)
    }

    fn eval(&self, node: &Node, m: usize, cols: &[Vec<u64>], pool: &mut Vec<Vec<u64>>, out: &mut [u64]) {
        match node {
            Node::All => {
                out.iter_mut().for_each(|w| *w = u64::MAX);
                if m % 64 != 0 {
                    out[out.len() - 1] = (1u64 << (m % 64)) - 1;
                }
            }
            Node::Pred { col, op, lit } => {
                let buf = &cols[*col];
                compare_column(self.columns[*col].kind, buf, m, *op, lit, out);
            }
            Node::And(children) | Node::Or(children) => {
                let is_and = matches!(node, Node::And(_));
                let (first, rest) = children.split_first().expect("compiled groups are non-empty");
                self.eval(first, m, cols, pool, out);
                let mut tmp = pool.pop().unwrap_or_default();
                tmp.clear();
                tmp.resize(out.len(), 0);
                for child in rest {
                    self.eval(child, m, cols, pool, &mut tmp);
                    for (o, t) in out.iter_mut().zip(tmp.iter()) {
                        if is_and {
                            *o &= *t;
                        } else {
                            *o |= *t;
                        }
                    }
                }
                pool.push(tmp);
            }
        }
    }
}

/// `MaskExpr` to `Node`, numbering the columns it reads. `All` absorbs an `OR`
/// and vanishes from an `AND`, as in `MaskExpr::is_all`.
fn compile(
    e: &MaskExpr,
    usable: &[(u32, Column)],
    columns: &mut SmallVec<[Column; 4]>,
    slot_of: &mut SmallVec<[(u32, usize); 4]>,
) -> Node {
    match e {
        MaskExpr::All => Node::All,
        MaskExpr::Pred(p) => {
            let Some((_, column)) = usable.iter().find(|(wo, _)| *wo == p.write_order) else {
                return Node::All;
            };
            let width = column.kind.width();
            // A literal coerced to some other width is not this column's value.
            if p.value.len() != width {
                return Node::All;
            }
            let mut lit = [0u8; 8];
            lit[..width].copy_from_slice(&p.value);
            let col = match slot_of.iter().find(|(wo, _)| *wo == p.write_order) {
                Some((_, slot)) => *slot,
                None => {
                    columns.push(column.clone());
                    slot_of.push((p.write_order, columns.len() - 1));
                    columns.len() - 1
                }
            };
            let op = match p.op {
                SargOp::Eq => CmpOp::Eq,
                SargOp::Gt => CmpOp::Gt,
                SargOp::Ge => CmpOp::Ge,
                SargOp::Lt => CmpOp::Lt,
                SargOp::Le => CmpOp::Le,
            };
            Node::Pred { col, op, lit }
        }
        MaskExpr::And(children) => {
            let kids: Vec<Node> = children
                .iter()
                .map(|c| compile(c, usable, columns, slot_of))
                .filter(|n| !n.is_all())
                .collect();
            match kids.len() {
                0 => Node::All,
                1 => kids.into_iter().next().unwrap(),
                _ => Node::And(kids),
            }
        }
        MaskExpr::Or(children) => {
            let kids: Vec<Node> = children
                .iter()
                .map(|c| compile(c, usable, columns, slot_of))
                .collect();
            if kids.is_empty() || kids.iter().any(Node::is_all) {
                Node::All
            } else if kids.len() == 1 {
                kids.into_iter().next().unwrap()
            } else {
                Node::Or(kids)
            }
        }
    }
}

fn compare_column(kind: Kind, buf: &[u64], m: usize, op: CmpOp, lit: &[u8; 8], out: &mut [u64]) {
    // SAFETY: `buf` holds at least `m * width` initialised bytes and is 8-byte
    // aligned, which satisfies every lane type here.
    unsafe fn view<T>(buf: &[u64], m: usize) -> &[T] {
        debug_assert!(buf.len() * 8 >= m * std::mem::size_of::<T>());
        unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const T, m) }
    }
    fn go<T: Lane>(values: &[T], op: CmpOp, k: T, out: &mut [u64]) {
        compare::compare(values, op, k, out)
    }
    unsafe {
        match kind {
            Kind::I8 => go(view::<i8>(buf, m), op, i8::from_le_bytes([lit[0]]), out),
            Kind::U8 => go(view::<u8>(buf, m), op, lit[0], out),
            Kind::I16 => go(view::<i16>(buf, m), op, i16::from_le_bytes([lit[0], lit[1]]), out),
            Kind::U16 => go(view::<u16>(buf, m), op, u16::from_le_bytes([lit[0], lit[1]]), out),
            Kind::I32 => go(view::<i32>(buf, m), op, i32::from_le_bytes(lit[..4].try_into().unwrap()), out),
            Kind::U32 => go(view::<u32>(buf, m), op, u32::from_le_bytes(lit[..4].try_into().unwrap()), out),
            Kind::I64 => go(view::<i64>(buf, m), op, i64::from_le_bytes(*lit), out),
            Kind::U64 => go(view::<u64>(buf, m), op, u64::from_le_bytes(*lit), out),
            Kind::F32 => go(view::<f32>(buf, m), op, f32::from_le_bytes(lit[..4].try_into().unwrap()), out),
            Kind::F64 => go(view::<f64>(buf, m), op, f64::from_le_bytes(*lit), out),
        }
    }
}

#[cfg(test)]
mod tests {
    //! The prefilter may only ever change how long a query takes. Every test
    //! here runs the same statement with it on and off and requires the same
    //! bytes back.

    use crate::core::database::Database;
    use crate::core::rql::executor;
    use crate::core::rql::lexer_s1::recognize_query_purpose;
    use rclite::Arc;

    /// Tests that flip the global switch run one at a time. Other tests may
    /// see it off for a moment, which changes nothing they can observe.
    static SWITCH: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
        let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse: {sql}"));
        executor::execute(purpose, db.clone()).await
    }

    struct RemoveDir(String);
    impl Drop for RemoveDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// xorshift64*, so a failing table reproduces.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn pick<'a>(&mut self, options: &[&'a str]) -> &'a str {
            options[(self.next() % options.len() as u64) as usize]
        }
    }

    const COLUMNS: &str = "id BIGINT NOT NULL, i8c TINYINT, u8c UTINYINT, i16c SMALLINT, \
        u16c USMALLINT, i32c INTEGER, u32c UINT, i64c BIGINT, u64c UBIGINT, r REAL, \
        f DOUBLE PRECISION, flag BOOLEAN, ts TIMESTAMPTZ, s TEXT, n NUMERIC(10,2)";

    /// One row's values. Mostly a narrow range, so equality and the bounds
    /// get hits; sometimes a type's extreme; about one in eight NULL.
    fn row(rng: &mut Rng, id: u64) -> String {
        fn cell(rng: &mut Rng, typical: impl Fn(&mut Rng) -> String, extremes: &[&str]) -> String {
            match rng.next() % 16 {
                0 | 1 => "NULL".to_string(),
                2 if !extremes.is_empty() => rng.pick(extremes).to_string(),
                _ => typical(rng),
            }
        }
        fn small(rng: &mut Rng) -> i64 {
            (rng.next() % 101) as i64 - 50
        }
        let unsigned = |rng: &mut Rng| (rng.next() % 101).to_string();
        const FLOATS: &[&str] = &["'NaN'", "'-0'", "'Infinity'", "'-Infinity'", "2.5"];
        let v = [
            cell(rng, |r| small(r).to_string(), &["-128", "127"]),
            cell(rng, |r| (r.next() % 256).to_string(), &["0", "255"]),
            cell(rng, |r| small(r).to_string(), &["-32768", "32767"]),
            cell(rng, unsigned, &["0", "65535"]),
            cell(rng, |r| small(r).to_string(), &["-2147483648", "2147483647"]),
            cell(rng, unsigned, &["0", "4294967295"]),
            cell(rng, |r| small(r).to_string(), &["-9223372036854775808", "9223372036854775807"]),
            cell(rng, unsigned, &["0", "18446744073709551615"]),
            cell(rng, |r| format!("{}", small(r) as f64 / 4.0), FLOATS),
            cell(rng, |r| format!("{}", small(r) as f64 / 2.0), FLOATS),
            cell(rng, |r| if r.next() % 2 == 0 { "true" } else { "false" }.to_string(), &[]),
            cell(rng, |r| format!("'2024-{:02}-15 12:00:00+00'", r.next() % 12 + 1), &[]),
            cell(rng, |r| format!("'x{}'", r.next() % 10), &["''"]),
            cell(rng, |r| format!("{:.2}", small(r) as f64 / 8.0), &[]),
        ];
        format!("({id}, {})", v.join(", "))
    }

    async fn table(db: &Arc<Database>, rows: u64) {
        named_table(db, "t", rows).await
    }

    async fn named_table(db: &Arc<Database>, name: &str, rows: u64) {
        let created = run(db, &format!("CREATE TABLE {name} ({COLUMNS})")).await;
        assert!(created.first() != Some(&3), "CREATE TABLE: {}", String::from_utf8_lossy(&created));
        let mut rng = Rng(0x5EED_0F_5EED);
        let mut id = 0;
        while id < rows {
            let chunk: Vec<String> = (id..(id + 500).min(rows)).map(|i| row(&mut rng, i)).collect();
            let inserted =
                run(db, &format!("INSERT INTO {name} ({}) VALUES {}", COLUMN_NAMES, chunk.join(", "))).await;
            assert!(inserted.first() != Some(&3), "INSERT: {}", String::from_utf8_lossy(&inserted));
            id += 500;
        }
    }

    const COLUMN_NAMES: &str = "id, i8c, u8c, i16c, u16c, i32c, u32c, i64c, u64c, r, f, flag, ts, s, n";

    /// Comparisons the prefilter decides, the shapes it must leave alone, and
    /// mixtures, where getting the structure wrong is what drops rows.
    const CLAUSES: &[&str] = &[
        "i32c = 7",
        "i32c > 10",
        "i32c >= 10 AND i32c < 20",
        "i32c <= -45",
        "5 < i32c",
        "i32c BETWEEN -5 AND 5",
        "i32c IN (1, 2, 3, 40)",
        "i64c < 0 OR i16c > 30",
        "(i8c > 10 OR u8c < 20) AND f >= 0",
        "i8c = -128",
        "i8c > 300",
        "u8c > 200",
        "u8c = 255",
        "u16c >= 60000",
        "u32c > 4000000000",
        "u64c > 18000000000000000000",
        "i16c > 100000",
        "i64c = 9223372036854775807",
        "i32c > 2.5",
        "i32c < 2.5",
        "i32c = 2.5",
        "f > 1e300",
        "f = 'NaN'",
        "f >= 'NaN'",
        "f < 'NaN'",
        "f <= 'NaN'",
        "f > 'NaN'",
        "f = 2.5",
        "f = 0",
        "r = 0",
        "r < 0",
        "r > -1 AND r <= 1",
        "flag = true",
        "flag = false AND i32c > 0",
        "ts > '2024-06-01 00:00:00+00'",
        "ts <= '2024-03-01 00:00:00+00' OR i8c = 0",
        // Opaque, alone and mixed in.
        "NOT i32c > 10",
        "i32c != 3",
        "i32c IS NULL",
        "i32c IS NOT NULL AND i32c > 0",
        "i32c > 0 OR i32c IS NULL",
        "s = 'x5'",
        "s = 'x5' AND i32c > 0",
        "s = 'x5' OR i32c > 40",
        "n > 1.5",
        "n > 1.5 AND i32c < 0",
        "i32c + 1 > 10",
        "(i32c > 10 AND i64c < 0) OR (u8c = 7 AND f IS NULL)",
    ];

    async fn differential(in_memory: bool) {
        let _g = SWITCH.lock().await;
        let dir = std::env::temp_dir().join(format!(
            "prefilter_differential_{}_{}",
            std::process::id(),
            in_memory
        ));
        let dir = dir.to_string_lossy().to_string();
        let _ = std::fs::remove_dir_all(&dir);
        let _cleanup = RemoveDir(dir.clone());
        let db = Arc::new(Database::new_with_backend(&dir, in_memory).await);
        // Three batches, the last partial.
        let rows = 25_000;
        table(&db, rows).await;
        let count = run(&db, "SELECT COUNT(*) FROM t").await;
        super::set_enabled(false);
        let count_off = run(&db, "SELECT COUNT(*) FROM t").await;
        super::set_enabled(true);
        assert_eq!(count, count_off);
        assert!(
            String::from_utf8_lossy(&count).contains(&rows.to_string()) || count.windows(8).any(|w| w == rows.to_le_bytes()),
            "the table did not load: {count:?}"
        );

        let mut dropped_total = 0;
        for clause in CLAUSES {
            for sql in [
                format!("SELECT {COLUMN_NAMES} FROM t WHERE {clause} ORDER BY id"),
                format!("SELECT COUNT(*) FROM t WHERE {clause}"),
                format!("SELECT id, f FROM t WHERE {clause} ORDER BY id DESC LIMIT 7"),
            ] {
                super::set_enabled(false);
                let off = run(&db, &sql).await;
                super::set_enabled(true);
                let before = super::stats().1;
                let on = run(&db, &sql).await;
                dropped_total += super::stats().1 - before;
                assert!(
                    on == off,
                    "{sql}: the prefilter changed the answer ({} bytes on, {} off)",
                    on.len(),
                    off.len()
                );
            }
        }
        assert!(dropped_total > 0, "the prefilter never dropped a row: the test proved nothing");

        // And it engages for the shape it is for. A range, not an equality:
        // the filter index's fingerprints answer `= 7` first, so few rows are
        // left for this to see, while every zone spans -50..50 and no zone map
        // can prune `> 46`. A literal no earlier statement used, so the
        // answer cannot come from the query cache.
        let (checked0, dropped0) = super::stats();
        run(&db, "SELECT COUNT(*) FROM t WHERE i32c > 46").await;
        let (checked1, dropped1) = super::stats();
        assert!(
            dropped1 - dropped0 > rows / 2,
            "a selective range did not prefilter: checked {}, dropped {}",
            checked1 - checked0,
            dropped1 - dropped0
        );
        super::set_enabled(true);
    }

    /// `UPDATE` and `DELETE` scan through `process_row_pointers`, which spawns
    /// a task per batch and compiles its plan on the first row it fetches. Two
    /// tables loaded identically take the same statements, one with the
    /// prefilter on and one with it off; every reply and the final contents
    /// must agree byte for byte.
    async fn write_differential(in_memory: bool) {
        let _g = SWITCH.lock().await;
        let dir = std::env::temp_dir().join(format!(
            "prefilter_write_differential_{}_{}",
            std::process::id(),
            in_memory
        ));
        let dir = dir.to_string_lossy().to_string();
        let _ = std::fs::remove_dir_all(&dir);
        let _cleanup = RemoveDir(dir.clone());
        let db = Arc::new(Database::new_with_backend(&dir, in_memory).await);
        // Two batches of the write path's 10,000, the second partial.
        let rows = 14_000;
        named_table(&db, "t_on", rows).await;
        named_table(&db, "t_off", rows).await;

        let contents = |name: &'static str| {
            let db = db.clone();
            async move { run(&db, &format!("SELECT {COLUMN_NAMES} FROM {name} ORDER BY id")).await }
        };
        assert_eq!(contents("t_on").await, contents("t_off").await, "the two tables did not load alike");

        let (_, dropped_before) = super::stats();
        let mut statements: Vec<String> = CLAUSES
            .iter()
            .enumerate()
            // Constants, not `u16c + 1`: a row holding 65535 would make every
            // statement that reaches it fail alike on both sides, and an error
            // compared with an error says nothing about which rows matched.
            .map(|(k, clause)| format!("UPDATE {{t}} SET r = {k}, u16c = {k} WHERE {clause}"))
            .collect();
        statements.extend(
            [
                "i32c > 40",
                "f = 'NaN'",
                "(i8c > 10 OR u8c < 20) AND f >= 0",
                "i32c IS NULL",
                "s = 'x5' AND i32c > 0",
                "u64c > 18000000000000000000 OR i16c < -40",
            ]
            .iter()
            .map(|clause| format!("DELETE FROM {{t}} WHERE {clause}")),
        );
        let mut errors = 0;
        for statement in &statements {
            super::set_enabled(false);
            let off = run(&db, &statement.replace("{t}", "t_off")).await;
            super::set_enabled(true);
            let on = run(&db, &statement.replace("{t}", "t_on")).await;
            assert!(
                on == off,
                "{statement}: the prefilter changed the reply: on {:?}, off {:?}",
                String::from_utf8_lossy(&on[..on.len().min(200)]),
                String::from_utf8_lossy(&off[..off.len().min(200)])
            );
            if off.first() == Some(&3) {
                errors += 1;
            }
            let (a, b) = (contents("t_on").await, contents("t_off").await);
            assert!(a == b, "{statement}: the tables differ afterwards ({} vs {} bytes)", a.len(), b.len());
        }
        assert!(
            super::stats().1 > dropped_before,
            "the prefilter never dropped a row on the write path: the test proved nothing"
        );
        assert!(errors * 4 < statements.len(), "{errors} of {} statements failed", statements.len());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn prefilter_never_changes_a_write_file_backed() {
        write_differential(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn prefilter_never_changes_a_write_in_memory() {
        write_differential(true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn prefilter_never_changes_an_answer_file_backed() {
        differential(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn prefilter_never_changes_an_answer_in_memory() {
        differential(true).await;
    }

    /// What the prefilter costs and saves, per SIMD level:
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored
    /// prefilter_cost --nocapture --test-threads 1`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore]
    async fn prefilter_cost() {
        use crate::simds::cpu::{self, SimdLevel, SimdRequest};
        use std::time::Instant;
        let _g = SWITCH.lock().await;
        // Warm-cache scans read the page cache wherever the files live, and
        // `/` is too full to be the place.
        let dir = std::env::temp_dir()
            .join(format!("prefilter_cost_{}", std::process::id()))
            .to_string_lossy()
            .to_string();
        let _ = std::fs::remove_dir_all(&dir);
        let _cleanup = RemoveDir(dir.clone());
        let db = Arc::new(Database::new_with_backend(&dir, false).await);
        run(&db, "CREATE TABLE big (id BIGINT NOT NULL, v BIGINT NOT NULL, w INTEGER NOT NULL, s TEXT NOT NULL)").await;
        let rows = 500_000u64;
        for chunk in 0..(rows / 1000) {
            let values: Vec<String> = (0..1000u64)
                .map(|k| {
                    let i = chunk * 1000 + k;
                    format!("({i}, {}, {}, 'row {i} padding padding padding')", (i * 7919) % rows, i % 1000)
                })
                .collect();
            run(&db, &format!("INSERT INTO big (id, v, w, s) VALUES {}", values.join(", "))).await;
        }
        let workloads = [
            ("0.1% pass", "SELECT COUNT(*) FROM big WHERE w = 7"),
            ("1% pass", "SELECT COUNT(*) FROM big WHERE v < 5000"),
            ("10% pass", "SELECT COUNT(*) FROM big WHERE w < 100"),
            ("50% pass", "SELECT COUNT(*) FROM big WHERE v < 250000"),
            ("100% pass", "SELECT COUNT(*) FROM big WHERE v >= 0"),
            ("rows, 1% pass", "SELECT id, s FROM big WHERE v < 5000"),
        ];
        // Off and each level interleaved, round after round, and the median
        // round reported: this box drifts 15% between identical runs minutes
        // apart, so a baseline taken once, first, compares against weather.
        let mut configs: Vec<Option<SimdLevel>> = vec![None];
        configs.extend(cpu::runnable_levels().into_iter().map(Some));
        for (name, sql) in workloads {
            let mut samples: Vec<Vec<f64>> = vec![Vec::new(); configs.len()];
            for _round in 0..9 {
                for (c, config) in configs.iter().enumerate() {
                    match config {
                        None => super::set_enabled(false),
                        Some(level) => {
                            super::set_enabled(true);
                            cpu::init(SimdRequest::Fixed(*level)).unwrap();
                        }
                    }
                    let mut best = f64::MAX;
                    for _ in 0..3 {
                        let t = Instant::now();
                        std::hint::black_box(run(&db, sql).await);
                        best = best.min(t.elapsed().as_secs_f64() * 1e3);
                    }
                    samples[c].push(best);
                }
            }
            let median = |v: &mut Vec<f64>| {
                v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                v[v.len() / 2]
            };
            let off = median(&mut samples[0]);
            let mut line = format!("{name:>14}: off {off:7.2} ms");
            for (c, config) in configs.iter().enumerate().skip(1) {
                let on = median(&mut samples[c]);
                line += &format!(" | {} {on:7.2} ms ({:.2}x)", config.unwrap(), off / on);
            }
            println!("{line}");
        }
        super::set_enabled(true);
        cpu::init(SimdRequest::Auto).unwrap();
        let _ = SimdLevel::Scalar;
    }
}
