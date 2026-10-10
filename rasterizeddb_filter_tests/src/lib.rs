//! Shared harness for the filter test suite.
//!
//! The suite is deliberately **external** to `rasterizeddb_core`: it sees only
//! the public surface, so it exercises the filter the way the engine does
//! rather than reaching into internals. The in-crate unit tests remain for the
//! things that genuinely need private access.
//!
//! Everything here is a helper. The assertions live in `tests/`.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::{FilterConfig, TableFilters};
use rasterizeddb_core::core::row::row::{
    ColumnFetchingData, ColumnWritePayload, Row, RowFetch, RowWrite,
};
use rasterizeddb_core::core::row::schema::{SchemaCalculator, SchemaField};
use rasterizeddb_core::core::row::table::Table;
use rasterizeddb_core::core::rql::lexer_ct::CreateColumnData;
use rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider;
use rasterizeddb_core::memory_pool::MEMORY_POOL;
use rclite::Arc;
use smallvec::SmallVec;

/// Every type the filter is expected to index.
pub const INDEXABLE: &[DbType] = &[
    DbType::I8,
    DbType::I16,
    DbType::I32,
    DbType::I64,
    DbType::I128,
    DbType::U8,
    DbType::U16,
    DbType::U32,
    DbType::U64,
    DbType::U128,
    DbType::F32,
    DbType::F64,
    DbType::STRING,
    DbType::DATETIME,
    DbType::CHAR,
    DbType::DECIMAL,
    DbType::UUID,
    DbType::INET,
];

/// Types the filter must refuse, and why each is on the list.
///
/// - `BYTEA`, `ARRAY` — excluded by *capability*: they are stored through the
///   same variable-width mechanism as `STRING`, but
///   `perform_comparison_operation` has no arm for them and returns
///   `UnsupportedType`. Indexing a type the evaluator cannot compare would
///   store values no query can ever match. `BYTEA` would be a natural addition
///   if bytewise equality lands — this list failing is the prompt to consider
///   it deliberately rather than by accident.
/// - The rest are internal sentinels, not column types.
///
/// `BOOL` is deliberately **not** here. It carries no fingerprints — two
/// distinct values can never clear the selectivity gate — but it is indexed for
/// its zone bounds, which cost two bytes per 512-row zone and prune a sparse
/// flag almost perfectly. See [`BOUNDS_ONLY`].
pub const NOT_INDEXABLE: &[DbType] = &[
    DbType::BYTEA,
    DbType::ARRAY,
    DbType::NULL,
    DbType::UNKNOWN,
    DbType::START,
    DbType::END,
];

/// Types that are indexed but never fingerprinted: zone bounds only.
pub const BOUNDS_ONLY: &[DbType] = &[DbType::BOOL];

/// Process-wide knobs the engine reads from `OnceLock`s. Safe to call repeatedly.
pub fn init() {
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(4);
    let _ = rasterizeddb_core::BATCH_SIZE.set(512);
    // The query-result cache would short-circuit repeated WHERE clauses and
    // hide what the filter is actually doing.
    let _ = rasterizeddb_core::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
}

// ---------------------------------------------------------------------------
// Value generation
// ---------------------------------------------------------------------------

/// Serialized width for a fixed-width type, straight from `DbType`.
pub fn width(ty: &DbType) -> usize {
    ty.get_size() as usize
}

/// A spread of values for `ty`: structural edge cases first, then random fill.
///
/// Edge cases matter more than volume here — `±0.0`, NaN, sign bits, empty and
/// near-identical strings, and both IP families are where an equality/hash
/// disagreement would actually show up.
pub fn sample_values(ty: &DbType, n_random: usize) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::new();

    match ty {
        DbType::STRING => {
            for s in [
                "",
                "a",
                "A",
                "ab",
                "ab\0",
                "\0ab",
                "hello world",
                "hello worle",
                "hello world ",
                "ünïcødé",
                "🙂🙂",
            ] {
                out.push(s.as_bytes().to_vec());
            }
            out.push("x".repeat(200).into_bytes());
            out.push("x".repeat(201).into_bytes());
            for _ in 0..n_random {
                let len = fastrand::usize(0..64);
                out.push((0..len).map(|_| fastrand::u8(32..127)).collect());
            }
        }
        DbType::INET => {
            out.extend(inet_samples());
            for _ in 0..n_random {
                let mut b: Vec<u8> = (0..width(ty)).map(|_| fastrand::u8(..)).collect();
                b[0] = if fastrand::bool() { 4 } else { 6 };
                out.push(b);
            }
        }
        DbType::F32 => {
            for v in [0.0f32, -0.0, 1.0, -1.0, f32::MIN, f32::MAX, f32::INFINITY,
                      f32::NEG_INFINITY, f32::NAN, f32::EPSILON] {
                out.push(v.to_le_bytes().to_vec());
            }
            for _ in 0..n_random {
                out.push(f32::from_bits(fastrand::u32(..)).to_le_bytes().to_vec());
            }
        }
        DbType::F64 => {
            for v in [0.0f64, -0.0, 1.0, -1.0, f64::MIN, f64::MAX, f64::INFINITY,
                      f64::NEG_INFINITY, f64::NAN, f64::EPSILON] {
                out.push(v.to_le_bytes().to_vec());
            }
            for _ in 0..n_random {
                out.push(f64::from_bits(fastrand::u64(..)).to_le_bytes().to_vec());
            }
        }
        _ => {
            let w = width(ty);
            out.push(vec![0u8; w]);
            out.push(vec![0xFFu8; w]);
            let mut sign = vec![0u8; w];
            sign[w - 1] = 0x80;
            out.push(sign);
            let mut one = vec![0u8; w];
            one[0] = 1;
            out.push(one);
            for _ in 0..n_random {
                out.push((0..w).map(|_| fastrand::u8(..)).collect());
            }
        }
    }

    out
}

/// INET values covering both families and the two ways two addresses can
/// differ: in the address itself, and in the prefix byte alone.
pub fn inet_samples() -> Vec<Vec<u8>> {
    let w = 18;
    let mut v4 = vec![0u8; w];
    v4[0] = 4;
    v4[1..5].copy_from_slice(&[192, 168, 0, 1]);
    v4[17] = 24;

    let mut v4_prefix = v4.clone();
    v4_prefix[17] = 32;
    let mut v4_addr = v4.clone();
    v4_addr[4] = 2;

    let mut v6 = vec![0u8; w];
    v6[0] = 6;
    v6[1] = 0x20;
    v6[2] = 0x01;
    v6[16] = 1;
    v6[17] = 64;
    let mut v6_prefix = v6.clone();
    v6_prefix[17] = 128;

    vec![v4, v4_prefix, v4_addr, v6, v6_prefix]
}

/// 16 random octets, the shape of a UUIDv4.
pub fn uuid_v4() -> Vec<u8> {
    let mut b = vec![0u8; 16];
    b[..8].copy_from_slice(&fastrand::u64(..).to_le_bytes());
    b[8..].copy_from_slice(&fastrand::u64(..).to_le_bytes());
    b
}

/// A UUID whose ordering under the engine's comparison is monotonic in `seq`,
/// i.e. what a time-ordered key looks like once stored correctly.
pub fn uuid_ordered(seq: u64) -> Vec<u8> {
    let mut b = vec![0u8; 16];
    b[..8].copy_from_slice(&seq.to_be_bytes());
    b[8..].copy_from_slice(&fastrand::u64(..).to_le_bytes());
    b
}

// ---------------------------------------------------------------------------
// Filter construction
// ---------------------------------------------------------------------------

pub fn field(name: &str, ty: DbType, write_order: u64) -> SchemaField {
    SchemaField::new(name.to_string(), ty.clone(), ty.get_size(), 0, write_order, false)
}

pub fn fields_of(spec: &[(&str, DbType)]) -> SmallVec<[SchemaField; 20]> {
    spec.iter()
        .enumerate()
        .map(|(i, (n, t))| field(n, t.clone(), i as u64))
        .collect()
}

pub fn filters_for(spec: &[(&str, DbType)]) -> (SmallVec<[SchemaField; 20]>, TableFilters) {
    let f = fields_of(spec);
    let t = TableFilters::new(&f, FilterConfig::default());
    (f, t)
}

/// Index `rows[slot]` = list of `(write_order, bytes)`.
pub fn backfill_all(filters: &TableFilters, rows: &[Vec<(u32, Vec<u8>)>]) {
    for (slot, cols) in rows.iter().enumerate() {
        let sv: SmallVec<[(u32, Vec<u8>); 32]> = cols.iter().cloned().collect();
        filters.backfill_row(slot as u64, &sv);
    }
}

// ---------------------------------------------------------------------------
// Real tables
// ---------------------------------------------------------------------------

pub fn payload(bytes: &[u8], write_order: u32, column_type: DbType) -> ColumnWritePayload {
    let mut mb = MEMORY_POOL.acquire(bytes.len().max(1));
    mb.into_slice_mut()[..bytes.len()].copy_from_slice(bytes);
    let size = column_type.get_size();
    ColumnWritePayload { data: mb, write_order, column_type, size }
}

pub fn full_fetch(fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
    let calc = SchemaCalculator::default();
    let mut cols = SmallVec::new();
    let mut sorted: Vec<&SchemaField> = fields.iter().collect();
    sorted.sort_by_key(|f| f.write_order);
    for f in sorted {
        let (column_offset, schema_id) = calc.calculate_schema_offset(&f.name, fields);
        cols.push(ColumnFetchingData {
            column_offset,
            column_type: f.db_type.clone(),
            size: f.size,
            schema_id,
        });
    }
    RowFetch { columns_fetching_data: cols }
}

/// Open (or reopen) a table over `dir`.
///
/// A real file provider, not the in-memory one: `MemoryStorageProvider`'s
/// `create_new` returns an unshared buffer, so `TableSchema::add_field` never
/// observes previously added fields and every column ends up with
/// `write_order = 0` — which the filter keys on.
pub async fn open_table(
    dir: &std::path::Path,
    name: &str,
    columns: &[(&str, DbType)],
) -> Table<LocalStorageProvider> {
    init();
    let io = Arc::new(LocalStorageProvider::new(dir.to_str().unwrap(), Some(name)).await);
    Table::new(
        name,
        io,
        columns
            .iter()
            .map(|(n, t)| CreateColumnData { name: n.to_string(), data_type: t.clone() })
            .collect(),
    )
    .await
}

pub async fn run(table: &Table<LocalStorageProvider>, where_clause: &str) -> Vec<Row> {
    table
        .query_row(
            where_clause,
            &table.schema.fields.to_vec(),
            full_fetch(&table.schema.fields),
            full_fetch(&table.schema.fields),
            None,
            None,
        )
        .await
        .unwrap()
}

/// Run a query the way the executor does: desugar the predicate first.
pub async fn run_rewritten(table: &Table<LocalStorageProvider>, clause: &str) -> Vec<Row> {
    let rewritten = rasterizeddb_core::core::tokenizer::predicate_rewrite::rewrite_where(
        clause,
        &table.schema.fields,
    )
    .unwrap_or_else(|e| panic!("rewrite of `{}` failed: {}", clause, e));
    run(table, &rewritten).await
}

/// Extract column `schema_id` from each row as raw bytes.
pub fn col_bytes(rows: &[Row], schema_id: u64) -> Vec<Vec<u8>> {
    rows.iter()
        .map(|r| {
            r.columns
                .iter()
                // Widened, not narrowed: `schema_id` is a u32 on the row now.
                .find(|c| u64::from(c.schema_id) == schema_id)
                .expect("column present")
                .data
                .into_slice()
                .to_vec()
        })
        .collect()
}

/// Sorted `u64` values of column `schema_id`.
pub fn u64s(rows: &[Row], schema_id: u64) -> Vec<u64> {
    let mut v: Vec<u64> = col_bytes(rows, schema_id)
        .into_iter()
        .map(|b| u64::from_le_bytes(b.try_into().expect("u64 column")))
        .collect();
    v.sort_unstable();
    v
}

// ---------------------------------------------------------------------------
// The standard fixture table
// ---------------------------------------------------------------------------

/// Rows in [`std_table`].
pub const STD_N: u64 = 2000;

/// `id U64` unique, `cat I32` = id % 64, `label STRING` unique, `score F64`.
pub fn std_columns() -> Vec<(&'static str, DbType)> {
    vec![
        ("id", DbType::U64),
        ("cat", DbType::I32),
        ("label", DbType::STRING),
        ("score", DbType::F64),
    ]
}

pub async fn fill_std(table: &Table<LocalStorageProvider>) {
    for i in 0..STD_N {
        let label = format!("l{}", i);
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&((i % 64) as i32).to_le_bytes(), 1, DbType::I32));
        rw.columns_writing_data.push(payload(label.as_bytes(), 2, DbType::STRING));
        rw.columns_writing_data
            .push(payload(&(i as f64 / 4.0).to_le_bytes(), 3, DbType::F64));
        table.insert_row(rw).await.unwrap();
    }
}

/// The standard fixture. The `TempDir` is returned so it outlives the table —
/// dropping it first would delete files from under the running IO service.
pub async fn std_table(name: &str) -> (tempfile::TempDir, Table<LocalStorageProvider>) {
    let dir = tempfile::tempdir().unwrap();
    let table = open_table(dir.path(), name, &std_columns()).await;
    fill_std(&table).await;
    (dir, table)
}

// ---------------------------------------------------------------------------
// The executor path
// ---------------------------------------------------------------------------

use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::core::row::row::vec_into_rows;
use rasterizeddb_core::core::rql::executor::execute;
use rasterizeddb_core::core::rql::lexer_s1::recognize_query_purpose;

/// What a statement produced. The wire response is a status byte followed by a
/// payload: `0` for a write (with the affected count), `2` for a result set,
/// `3` for an error.
#[derive(Debug)]
pub enum Exec {
    Affected(u64),
    Rows(Vec<Row>),
    Error(String),
}

impl Exec {
    /// The rows, or a panic naming the statement that failed.
    #[track_caller]
    pub fn rows(self, sql: &str) -> Vec<Row> {
        match self {
            Exec::Rows(r) => r,
            other => panic!("`{}` did not return rows: {:?}", sql, other),
        }
    }

    #[track_caller]
    pub fn expect_ok(self, sql: &str) -> u64 {
        match self {
            Exec::Affected(n) => n,
            Exec::Rows(r) => r.len() as u64,
            Exec::Error(e) => panic!("`{}` failed: {}", sql, e),
        }
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Exec::Error(_))
    }
}

/// A file-backed database in `dir`.
pub async fn open_db(dir: &std::path::Path) -> Arc<Database> {
    init();
    Arc::new(Database::new_with_backend(dir.to_str().unwrap(), false).await)
}

/// Run one statement through the full executor: recognise, plan, execute.
pub async fn exec(db: &Arc<Database>, sql: &str) -> Exec {
    let Some(purpose) = recognize_query_purpose(sql) else {
        return Exec::Error(format!("unrecognised statement: {}", sql));
    };
    let out = execute(purpose, db.clone()).await;
    match out.first() {
        Some(0) => {
            let n = if out.len() >= 9 {
                u64::from_le_bytes(out[1..9].try_into().unwrap())
            } else {
                0
            };
            Exec::Affected(n)
        }
        Some(2) => match vec_into_rows(&out[1..]) {
            Ok(r) => Exec::Rows(r),
            Err(e) => Exec::Error(format!("undecodable result set: {}", e)),
        },
        Some(3) => Exec::Error(String::from_utf8_lossy(&out[1..]).to_string()),
        other => Exec::Error(format!("unexpected status {:?}", other)),
    }
}

/// Sorted `i64`s of the first column of each row, whatever its declared width.
///
/// Aggregates come back at the width the fold produced, which is not always the
/// column's own, so this widens rather than assuming.
pub fn first_col_i64(rows: &[Row]) -> Vec<i64> {
    let mut v: Vec<i64> = rows
        .iter()
        .map(|r| {
            let c = r.columns.first().expect("at least one column");
            let b = c.data.into_slice();
            match b.len() {
                1 => b[0] as i64,
                2 => i16::from_le_bytes(b.try_into().unwrap()) as i64,
                4 => i32::from_le_bytes(b.try_into().unwrap()) as i64,
                8 => i64::from_le_bytes(b.try_into().unwrap()),
                16 => i128::from_le_bytes(b.try_into().unwrap()) as i64,
                n => panic!("unexpected aggregate width {}", n),
            }
        })
        .collect();
    v.sort_unstable();
    v
}
