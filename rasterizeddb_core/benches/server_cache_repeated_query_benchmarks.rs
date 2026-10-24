use std::{
    sync::OnceLock,
    time::{Duration, Instant},
};

use criterion::{Criterion, criterion_group, criterion_main};
use rastcp::{
    client::{TcpClient, TcpClientBuilder},
    server::TcpServerBuilder,
};
use rasterizeddb_core::{
    BATCH_SIZE, MAX_PERMITS_THREADS,
    core::{
        database::Database,
        db_type::DbType,
        processor::concurrent_processor::{ATOMIC_CACHE, ENABLE_CACHE},
        row::row::{ColumnWritePayload, RowWrite},
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
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

const BENCH_SERVER_PORT: u16 = 62170;
const BENCH_QUERY: &str = r##"
            SELECT id, salary, age, name FROM employees
            WHERE id = 100000
        "##;
const ROW_COUNT: u64 = 100_000;

struct BenchContext {
    runtime: Runtime,
    _tempdir: TempDir,
    port: u16,
}

fn bench_context() -> &'static BenchContext {
    static CONTEXT: OnceLock<BenchContext> = OnceLock::new();
    CONTEXT.get_or_init(build_context)
}

fn build_context() -> BenchContext {
    let _ = env_logger::builder().is_test(true).try_init();

    MAX_PERMITS_THREADS.get_or_init(|| 16);
    BATCH_SIZE.get_or_init(|| 3_000);
    ATOMIC_CACHE.get_or_init(|| {
        // The cache now stores a `QueryCacheEntry`, which carries the table,
        // the clause and the generation alongside the pointers so a stale
        // entry is detectable rather than merely unlikely.
        rasterizeddb_core::cache::query_cache::QueryCache::new(1024, 10240)
    });

    ENABLE_CACHE.get_or_init(|| true);

    let runtime = Builder::new_multi_thread()
        .worker_threads(16)
        .enable_all()
        .build()
        .expect("failed to build benchmark runtime");

    let tempdir = tempfile::tempdir().expect("failed to create tempdir for benchmark database");
    let location = tempdir
        .path()
        .to_str()
        .expect("benchmark tempdir should be valid UTF-8")
        .to_string();

    runtime.block_on(async {
        let database = Arc::new(Database::new_with_backend(&location, false).await);
        create_employees_table(&database).await;
        populate_employees(&database).await;
        spawn_server(database.clone(), BENCH_SERVER_PORT).await;
        assert_one_row_response(&send_query(BENCH_SERVER_PORT, BENCH_QUERY).await);

        clear_query_cache();
        let cold_start = Instant::now();
        let cold_response = send_query(BENCH_SERVER_PORT, BENCH_QUERY).await;
        let cold_elapsed = cold_start.elapsed();
        assert_one_row_response(&cold_response);

        let warm_start = Instant::now();
        let warm_response = send_query(BENCH_SERVER_PORT, BENCH_QUERY).await;
        let warm_elapsed = warm_start.elapsed();
        assert_one_row_response(&warm_response);

        eprintln!(
            "server_cache_repeated_query_benchmarks setup: cold={} us warm={} us delta={} us",
            cold_elapsed.as_micros(),
            warm_elapsed.as_micros(),
            cold_elapsed
                .as_micros()
                .saturating_sub(warm_elapsed.as_micros())
        );
    });

    BenchContext {
        runtime,
        _tempdir: tempdir,
        port: BENCH_SERVER_PORT,
    }
}

async fn spawn_server(database: Arc<Database>, port: u16) {
    let server = TcpServerBuilder::new("127.0.0.1", port)
        .max_connections(usize::MAX)
        .build()
        .await
        .expect("failed to build benchmark rastcp server");

    tokio::spawn(async move {
        let _ = server
            .run_with_context(database, process_incoming_query)
            .await;
    });
}

async fn process_incoming_query(database: Arc<Database>, request_vec: Vec<u8>) -> Vec<u8> {
    let query = String::from_utf8_lossy(&request_vec);

    if let Some(query_purpose) = recognize_query_purpose(&query) {
        execute(query_purpose, database).await
    } else {
        let mut result = vec![3u8];
        result.extend_from_slice(b"Unrecognized query");
        result
    }
}

async fn create_employees_table(database: &Arc<Database>) {
    database
        .add_table(CreateTable {
            table_name: "employees".to_string(),
            columns: vec![
                CreateColumnData {
                    name: "id".to_string(),
                    data_type: DbType::U64,
                },
                CreateColumnData {
                    name: "name".to_string(),
                    data_type: DbType::STRING,
                },
                CreateColumnData {
                    name: "job_title".to_string(),
                    data_type: DbType::STRING,
                },
                CreateColumnData {
                    name: "salary".to_string(),
                    data_type: DbType::F32,
                },
                CreateColumnData {
                    name: "department".to_string(),
                    data_type: DbType::STRING,
                },
                CreateColumnData {
                    name: "age".to_string(),
                    data_type: DbType::I32,
                },
                CreateColumnData {
                    name: "manager".to_string(),
                    data_type: DbType::STRING,
                },
                CreateColumnData {
                    name: "location".to_string(),
                    data_type: DbType::STRING,
                },
                CreateColumnData {
                    name: "hire_date".to_string(),
                    data_type: DbType::U64,
                },
                CreateColumnData {
                    name: "degree".to_string(),
                    data_type: DbType::STRING,
                },
                CreateColumnData {
                    name: "skills".to_string(),
                    data_type: DbType::STRING,
                },
                CreateColumnData {
                    name: "current_project".to_string(),
                    data_type: DbType::STRING,
                },
                CreateColumnData {
                    name: "performance_score".to_string(),
                    data_type: DbType::F32,
                },
                CreateColumnData {
                    name: "is_active".to_string(),
                    data_type: DbType::BOOL,
                },
                CreateColumnData {
                    name: "created_at".to_string(),
                    data_type: DbType::U64,
                },
                CreateColumnData {
                    name: "updated_at".to_string(),
                    data_type: DbType::U64,
                },
                CreateColumnData {
                    name: "is_fired".to_string(),
                    data_type: DbType::BOOL,
                },
            ],
            is_immutable: false,
            foreign_keys: Vec::new(),
            defaults: Vec::new(),
            json_columns: Vec::new(),
            unique_columns: Vec::new(),
            unique_keys: Vec::new(),
            type_modifiers: Vec::new(),
            generated: Vec::new(),
            checks: Vec::new(),
            primary_key: None,
            if_not_exists: false,
        })
        .await
        .expect("failed to create employees table for benchmark");
}

async fn populate_employees(database: &Arc<Database>) {
    let table = database.table_for_tests("employees")
        .expect("employees table should exist after setup");

    for id in 1..=ROW_COUNT {
        table
            .insert_row(build_employee_row(id))
            .await
            .expect("failed to insert employee row");
    }
}

fn build_employee_row(id: u64) -> RowWrite {
    let salary = 50_000.0_f32 + (id % 1_000) as f32;
    let age = 20_i32 + (id % 45) as i32;
    let hire_date = 1_700_000_000_u64 + id;
    let created_at = 1_710_000_000_u64 + id;
    let updated_at = 1_720_000_000_u64 + id;
    let name = format!("Employee {id}");

    RowWrite {
        columns_writing_data: smallvec![
            payload_u64(id, 0),
            payload_string(&name, 1),
            payload_string("Engineer", 2),
            payload_f32(salary, 3),
            payload_string("Platform", 4),
            payload_i32(age, 5),
            payload_string("Manager", 6),
            payload_string("Remote", 7),
            payload_u64(hire_date, 8),
            payload_string("CS", 9),
            payload_string("Rust,SQL", 10),
            payload_string("BenchQuery", 11),
            payload_f32(95.0, 12),
            payload_bool(true, 13),
            payload_u64(created_at, 14),
            payload_u64(updated_at, 15),
            payload_bool(false, 16),
        ],
    }
}

fn payload_u64(value: u64, write_order: u32) -> ColumnWritePayload {
    ColumnWritePayload {
        data: memory_block_from_bytes(&value.to_le_bytes()),
        write_order,
        column_type: DbType::U64,
        size: DbType::U64.get_size(),
    }
}

fn payload_i32(value: i32, write_order: u32) -> ColumnWritePayload {
    ColumnWritePayload {
        data: memory_block_from_bytes(&value.to_le_bytes()),
        write_order,
        column_type: DbType::I32,
        size: DbType::I32.get_size(),
    }
}

fn payload_f32(value: f32, write_order: u32) -> ColumnWritePayload {
    ColumnWritePayload {
        data: memory_block_from_bytes(&value.to_le_bytes()),
        write_order,
        column_type: DbType::F32,
        size: DbType::F32.get_size(),
    }
}

fn payload_bool(value: bool, write_order: u32) -> ColumnWritePayload {
    ColumnWritePayload {
        data: memory_block_from_bytes(&[u8::from(value)]),
        write_order,
        column_type: DbType::BOOL,
        size: DbType::BOOL.get_size(),
    }
}

fn payload_string(value: &str, write_order: u32) -> ColumnWritePayload {
    ColumnWritePayload {
        data: memory_block_from_bytes(value.as_bytes()),
        write_order,
        column_type: DbType::STRING,
        size: DbType::STRING.get_size(),
    }
}

fn memory_block_from_bytes(bytes: &[u8]) -> MemoryBlock {
    let mut data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(bytes);
    data
}

fn clear_query_cache() {
    if let Some(cache) = ATOMIC_CACHE.get() {
        rasterizeddb_core::cache::query_cache::clear_cache(cache);
    }
}

async fn connect_client(port: u16) -> TcpClient {
    let mut attempts = 0;

    loop {
        attempts += 1;

        let mut client = TcpClientBuilder::new(&format!("127.0.0.1:{port}"))
            .verify_certificate(false)
            .timeout(Duration::from_secs(30))
            .build()
            .await
            .expect("failed to build benchmark client");

        match client.connect().await {
            Ok(()) => return client,
            Err(err) if attempts < 20 => {
                let _ = err;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(err) => panic!("failed to connect benchmark client: {err}"),
        }
    }
}

async fn send_query(port: u16, query: &str) -> Vec<u8> {
    let mut client = connect_client(port).await;
    client
        .send(query.as_bytes().to_vec())
        .await
        .expect("query send should succeed")
}

fn assert_one_row_response(response: &[u8]) {
    assert_eq!(
        response.first().copied(),
        Some(2),
        "expected rows result marker"
    );
    assert!(response.len() > 1, "expected serialized row payload");
}

fn benchmark_server_cache_repeated_query(c: &mut Criterion) {
    let ctx = bench_context();

    let mut group = c.benchmark_group("server_cache_repeated_query");
    group.sample_size(10);

    group.bench_function("cold_scan_each_run", |b| {
        b.iter_custom(|iters| {
            ctx.runtime.block_on(async {
                let mut client = connect_client(ctx.port).await;
                let query = BENCH_QUERY.as_bytes().to_vec();
                let start = Instant::now();

                for _ in 0..iters {
                    clear_query_cache();
                    let response = client
                        .send(query.clone())
                        .await
                        .expect("cold query should succeed");
                    assert_one_row_response(&response);
                }

                start.elapsed()
            })
        });
    });

    group.bench_function("warm_cache_repeated_run", |b| {
        b.iter_custom(|iters| {
            ctx.runtime.block_on(async {
                clear_query_cache();
                let mut client = connect_client(ctx.port).await;
                let query = BENCH_QUERY.as_bytes().to_vec();

                let warm_response = client
                    .send(query.clone())
                    .await
                    .expect("warm-up query should succeed");
                assert_one_row_response(&warm_response);

                let start = Instant::now();

                for _ in 0..iters {
                    let response = client
                        .send(query.clone())
                        .await
                        .expect("cached query should succeed");
                    assert_one_row_response(&response);
                }

                start.elapsed()
            })
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_server_cache_repeated_query);
criterion_main!(benches);
