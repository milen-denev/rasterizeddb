//! The query-result cache must be invisible in the answers it produces.
//!
//! One cache entry holds every pointer a `WHERE` clause matched, and that one
//! entry serves the clause at any `LIMIT` and in any order. These tests pin the
//! two directions that used to go wrong — an entry built by a `LIMIT` query
//! answering the unlimited form, and an `ORDER BY` skipped because the answer
//! came from the cache — plus the invalidation that keeps a mutated row from
//! being served out of a stale entry.
//!
//! Every global here is a process-wide `OnceLock`, so the whole file shares one
//! cache, one enablement flag, and one set of counters. Cargo runs these tests
//! on parallel threads, so each takes `SERIAL` for its duration — otherwise one
//! test's `clear_cache()` lands in the middle of another's warm-up.

use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    cache::query_cache,
    core::{
        database::Database,
        db_type::DbType,
        row::row::{ColumnWritePayload, RowWrite, vec_into_rows},
        rql::{
            executor::execute,
            lexer_ct::{CreateColumnData, CreateTable},
            lexer_s1::recognize_query_purpose,
        },
    },
    memory_pool::{MEMORY_POOL, MemoryBlock},
};
use rclite::Arc;
use smallvec::smallvec;

const ROW_COUNT: u64 = 40;

/// Serializes the tests against the shared cache. A `tokio` mutex rather than a
/// `std` one because the guard is held across awaits.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn init_globals() {
    MAX_PERMITS_THREADS.get_or_init(|| 4);
    BATCH_SIZE.get_or_init(|| 1_000);
    query_cache::init(1024);
    query_cache::ENABLE_CACHE.get_or_init(|| true);
    assert!(
        query_cache::enabled(),
        "these tests are meaningless with the cache off"
    );
}

fn clear_cache() {
    query_cache::clear();
}

fn mb(bytes: &[u8]) -> MemoryBlock {
    let mut data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(bytes);
    data
}

fn payload_u64(value: u64, write_order: u32) -> ColumnWritePayload {
    ColumnWritePayload {
        data: mb(&value.to_le_bytes()),
        write_order,
        column_type: DbType::U64,
        size: DbType::U64.get_size(),
    }
}

fn payload_i32(value: i32, write_order: u32) -> ColumnWritePayload {
    ColumnWritePayload {
        data: mb(&value.to_le_bytes()),
        write_order,
        column_type: DbType::I32,
        size: DbType::I32.get_size(),
    }
}

fn row(id: u64, age: i32) -> RowWrite {
    RowWrite {
        columns_writing_data: smallvec![payload_u64(id, 0), payload_i32(age, 1)],
    }
}

async fn setup(dir: &str, table: &str) -> Arc<Database> {
    let database = Arc::new(Database::new_with_backend(dir, false).await);
    database
        .add_table(CreateTable {
            table_name: table.to_string(),
            columns: vec![
                CreateColumnData { name: "id".into(), data_type: DbType::U64 },
                CreateColumnData { name: "age".into(), data_type: DbType::I32 },
            ],
            is_immutable: false,
            foreign_keys: Vec::new(),
            defaults: Vec::new(),
            primary_key: None,
            json_columns: Vec::new(),
            unique_columns: Vec::new(),
        unique_keys: Vec::new(),
            type_modifiers: Vec::new(),
            generated: Vec::new(),
            checks: Vec::new(),
            if_not_exists: false,
        })
        .await
        .expect("create table");

    let t = database.tables.get(table).expect("table").value().clone();
    for id in 1..=ROW_COUNT {
        // Age descends as id ascends, so ORDER BY age has to actually reorder.
        t.insert_row(row(id, (ROW_COUNT - id) as i32))
            .await
            .expect("insert");
    }
    database
}

async fn run(db: &Arc<Database>, q: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(q).expect("recognized");
    execute(purpose, db.clone()).await
}

fn rows_of(resp: &[u8]) -> Vec<rasterizeddb_core::core::row::row::Row> {
    assert_eq!(
        resp.first().copied(),
        Some(2u8),
        "expected a rows marker, got {:?}",
        &resp[..resp.len().min(64)]
    );
    vec_into_rows(&resp[1..]).expect("decode rows")
}

fn row_count(resp: &[u8]) -> usize {
    rows_of(resp).len()
}

fn column_i32(resp: &[u8], index: usize) -> Vec<i32> {
    rows_of(resp)
        .iter()
        .map(|r| {
            let s = r.columns[index].data.into_slice();
            i32::from_le_bytes([s[0], s[1], s[2], s[3]])
        })
        .collect()
}

fn column_u64(resp: &[u8], index: usize) -> Vec<u64> {
    rows_of(resp)
        .iter()
        .map(|r| {
            let s = r.columns[index].data.into_slice();
            u64::from_le_bytes(s[..8].try_into().unwrap())
        })
        .collect()
}

/// The cache is actually consulted and actually answers — everything else here
/// would still pass if the fixes had simply turned it off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_queries_are_served_from_the_cache() {
    let _serial = SERIAL.lock().await;
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = setup(dir.path().to_str().unwrap(), "t_hit").await;

    const ALL: &str = "SELECT id, age FROM t_hit WHERE id > 0";

    clear_cache();
    let (h0, _, s0) = query_cache::stats();
    assert_eq!(row_count(&run(&db, ALL).await), ROW_COUNT as usize);
    let (h1, _, s1) = query_cache::stats();
    assert_eq!(h1, h0, "cold query should not have hit");
    assert_eq!(s1, s0 + 1, "cold query should have stored its match set");

    for _ in 0..3 {
        assert_eq!(row_count(&run(&db, ALL).await), ROW_COUNT as usize);
    }
    let (h2, _, s2) = query_cache::stats();
    assert_eq!(h2, h1 + 3, "warm queries were not served from the cache");
    assert_eq!(s2, s1, "a hit should not re-store the entry");

    // A write retires the entry, so the next read misses and re-stores.
    let resp = run(&db, "DELETE FROM t_hit WHERE id = 1").await;
    assert_ne!(resp.first().copied(), Some(3u8), "delete failed: {:?}", resp);
    assert_eq!(row_count(&run(&db, ALL).await), ROW_COUNT as usize - 1);
    let (h3, _, s3) = query_cache::stats();
    assert_eq!(h3, h2, "a stale entry must not count as a hit");
    assert_eq!(s3, s2 + 1);
}

/// A `LIMIT` narrows the answer, never the entry the next query reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limit_does_not_leak_between_queries() {
    let _serial = SERIAL.lock().await;
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = setup(dir.path().to_str().unwrap(), "t_limit").await;

    const ALL: &str = "SELECT id, age FROM t_limit WHERE id > 0";
    const FIVE: &str = "SELECT id, age FROM t_limit WHERE id > 0 LIMIT 5";

    // Cold, for reference.
    clear_cache();
    assert_eq!(row_count(&run(&db, ALL).await), ROW_COUNT as usize);
    clear_cache();
    assert_eq!(row_count(&run(&db, FIVE).await), 5);

    // A LIMIT query first: its scan stops early, so it must not leave behind an
    // entry the unlimited form would read as the complete match set.
    clear_cache();
    assert_eq!(row_count(&run(&db, FIVE).await), 5);
    assert_eq!(
        row_count(&run(&db, ALL).await),
        ROW_COUNT as usize,
        "unlimited query served a LIMIT-truncated cache entry"
    );

    // The unlimited query first: the cached set is complete, and the LIMIT has
    // to be applied on the way out of the cache.
    clear_cache();
    assert_eq!(row_count(&run(&db, ALL).await), ROW_COUNT as usize);
    assert_eq!(
        row_count(&run(&db, FIVE).await),
        5,
        "LIMIT query served the full cached pointer set"
    );

    // And warm in both directions, repeatedly.
    assert_eq!(row_count(&run(&db, ALL).await), ROW_COUNT as usize);
    assert_eq!(row_count(&run(&db, FIVE).await), 5);
}

/// A cache hit is sorted like a scan is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_by_survives_a_cache_hit() {
    let _serial = SERIAL.lock().await;
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = setup(dir.path().to_str().unwrap(), "t_order").await;

    const ALL: &str = "SELECT id, age FROM t_order WHERE id > 0";
    const SORTED: &str = "SELECT id, age FROM t_order WHERE id > 0 ORDER BY age ASC";
    const SORTED_LIMIT: &str = "SELECT id, age FROM t_order WHERE id > 0 ORDER BY age ASC LIMIT 3";

    clear_cache();
    let cold = column_i32(&run(&db, SORTED).await, 1);
    assert!(cold.windows(2).all(|w| w[0] <= w[1]), "cold scan is unsorted");

    // Warm the entry with the unordered query, then ask for it ordered.
    clear_cache();
    let _ = run(&db, ALL).await;
    let warm = column_i32(&run(&db, SORTED).await, 1);
    assert_eq!(warm, cold, "ORDER BY was not applied on a cache hit");

    // ORDER BY plus LIMIT is the smallest ages, not the first three stored.
    let warm_limited = column_i32(&run(&db, SORTED_LIMIT).await, 1);
    assert_eq!(warm_limited, cold[..3].to_vec());
}

/// A mutation retires every entry the table has produced, including ones for
/// clauses the mutation does not itself match.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_invalidate_cached_answers() {
    let _serial = SERIAL.lock().await;
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = setup(dir.path().to_str().unwrap(), "t_write").await;
    let table = db.tables.get("t_write").unwrap().value().clone();

    const ALL: &str = "SELECT id, age FROM t_write WHERE id > 0";

    clear_cache();
    assert_eq!(row_count(&run(&db, ALL).await), ROW_COUNT as usize);
    // Warm.
    assert_eq!(row_count(&run(&db, ALL).await), ROW_COUNT as usize);

    // DELETE: the removed row must not be served out of the warm entry.
    let resp = run(&db, "DELETE FROM t_write WHERE id = 7").await;
    assert_ne!(resp.first().copied(), Some(3u8), "delete failed: {:?}", resp);
    let after_delete = column_u64(&run(&db, ALL).await, 0);
    assert_eq!(after_delete.len(), ROW_COUNT as usize - 1);
    assert!(
        !after_delete.contains(&7),
        "a deleted row was served from the query cache"
    );

    // INSERT: a new match must show up.
    table.insert_row(row(ROW_COUNT + 1, 500)).await.expect("insert");
    let after_insert = column_u64(&run(&db, ALL).await, 0);
    assert_eq!(after_insert.len(), ROW_COUNT as usize);
    assert!(
        after_insert.contains(&(ROW_COUNT + 1)),
        "an inserted row was missing behind a stale cache entry"
    );
}

/// Two clauses that differ only in their table, and two tables that differ only
/// in their clause, must not share an entry. The old key XOR'd the two CRCs,
/// which made the pair symmetric.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entries_are_not_shared_across_tables_or_clauses() {
    let _serial = SERIAL.lock().await;
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = setup(dir.path().to_str().unwrap(), "t_a").await;
    db.add_table(CreateTable {
        table_name: "t_b".into(),
        columns: vec![
            CreateColumnData { name: "id".into(), data_type: DbType::U64 },
            CreateColumnData { name: "age".into(), data_type: DbType::I32 },
        ],
        is_immutable: false,
        foreign_keys: Vec::new(),
        defaults: Vec::new(),
            primary_key: None,
        json_columns: Vec::new(),
        unique_columns: Vec::new(),
        unique_keys: Vec::new(),
            type_modifiers: Vec::new(),
            generated: Vec::new(),
            checks: Vec::new(),
        if_not_exists: false,
    })
    .await
    .expect("create t_b");
    let b = db.tables.get("t_b").unwrap().value().clone();
    for id in 1..=3u64 {
        b.insert_row(row(id, id as i32)).await.expect("insert");
    }

    clear_cache();
    assert_eq!(
        row_count(&run(&db, "SELECT id, age FROM t_a WHERE id > 0").await),
        ROW_COUNT as usize
    );
    assert_eq!(
        row_count(&run(&db, "SELECT id, age FROM t_b WHERE id > 0").await),
        3
    );
    // Warm, both directions.
    assert_eq!(
        row_count(&run(&db, "SELECT id, age FROM t_a WHERE id > 0").await),
        ROW_COUNT as usize
    );
    assert_eq!(
        row_count(&run(&db, "SELECT id, age FROM t_b WHERE id > 0").await),
        3
    );

    // Different clauses on the same table.
    assert_eq!(
        row_count(&run(&db, "SELECT id, age FROM t_a WHERE id > 30").await),
        10
    );
    assert_eq!(
        row_count(&run(&db, "SELECT id, age FROM t_a WHERE id > 0").await),
        ROW_COUNT as usize
    );
}
