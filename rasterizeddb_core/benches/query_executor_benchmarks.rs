use criterion::{criterion_group, criterion_main, Criterion};
use dashmap::DashMap;
use rasterizeddb_core::core::row_v2::query_parser::{parse_query, tokenize_for_test};
use rasterizeddb_core::core::row_v2::schema::SchemaField;
use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::memory_pool::{MemoryBlock, MEMORY_POOL};
use std::hint::black_box;

fn create_benchmark_schema() -> Vec<SchemaField> {
    vec![
        SchemaField::new("id".to_string(), DbType::I32, 4, 0, 0, false),
        SchemaField::new("age".to_string(), DbType::U8, 1, 0, 0, false),
        SchemaField::new("salary".to_string(), DbType::I32, 4, 0, 0, false),
        SchemaField::new("name".to_string(), DbType::STRING, 4 + 8, 0, 0, false),
        SchemaField::new("department".to_string(), DbType::STRING, 4 + 8, 0, 0, false),
        SchemaField::new("bank_balance".to_string(), DbType::F64, 8, 0, 0, false),
        SchemaField::new("is_active".to_string(), DbType::U8, 1, 0, 0, false),
        SchemaField::new("score".to_string(), DbType::F32, 4, 0, 0, false),
        SchemaField::new("notes".to_string(), DbType::STRING, 4 + 8, 0, 0, false),
        SchemaField::new("created_at".to_string(), DbType::U64, 8, 0, 0, false),
        // For execute benchmarks
        SchemaField::new("earth_position".to_string(), DbType::I8, 1, 0, 0, false),
        SchemaField::new("test_1".to_string(), DbType::U32, 4, 0, 0, false),
        SchemaField::new("test_2".to_string(), DbType::U16, 2, 0, 0, false),
        SchemaField::new("test_3".to_string(), DbType::I128, 16, 0, 0, false),
        SchemaField::new("test_4".to_string(), DbType::U128, 16, 0, 0, false),
        SchemaField::new("test_5".to_string(), DbType::I16, 2, 0, 0, false),
        SchemaField::new("test_6".to_string(), DbType::U64, 8, 0, 0, false),
        SchemaField::new("credit_balance".to_string(), DbType::F32, 4, 0, 0, false),
        SchemaField::new("net_assets".to_string(), DbType::I64, 8, 0, 0, false),


    ]
}

// Helper functions to create memory blocks for benchmark data
fn create_memory_block_from_i32(value: i32) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}

fn create_memory_block_from_u8(value: u8) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}

fn create_memory_block_from_string(value: &str) -> MemoryBlock {
    let bytes = value.as_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(bytes);
    data
}

fn create_memory_block_from_f64(value: f64) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}

fn create_memory_block_from_f32(value: f32) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}

fn create_memory_block_from_i64(value: i64) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}
fn create_memory_block_from_i8(value: i8) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}
fn create_memory_block_from_u32(value: u32) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}
fn create_memory_block_from_u16(value: u16) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}
fn create_memory_block_from_i128(value: i128) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}
fn create_memory_block_from_u128(value: u128) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}
fn create_memory_block_from_i16(value: i16) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}
fn create_memory_block_from_u64(value: u64) -> MemoryBlock {
    let bytes = value.to_le_bytes();
    let data = MEMORY_POOL.acquire(bytes.len());
    data.into_slice_mut().copy_from_slice(&bytes);
    data
}

fn setup_benchmark_columns() -> DashMap<String, MemoryBlock> {
    let columns = DashMap::new();
    columns.insert("id".to_string(), create_memory_block_from_i32(42));
    columns.insert("age".to_string(), create_memory_block_from_u8(30));
    columns.insert("salary".to_string(), create_memory_block_from_i32(50000));
    columns.insert("name".to_string(), create_memory_block_from_string("John Doe"));
    columns.insert("department".to_string(), create_memory_block_from_string("Engineering"));
    columns.insert("bank_balance".to_string(), create_memory_block_from_f64(1000.43));
    columns.insert("is_active".to_string(), create_memory_block_from_u8(1)); // true
    columns.insert("score".to_string(), create_memory_block_from_f32(85.5));
    columns.insert("notes".to_string(), create_memory_block_from_string("This is an important note for review."));
    columns.insert("created_at".to_string(), create_memory_block_from_i64(1672531200)); // Example timestamp

    columns.insert("credit_balance".to_string(), create_memory_block_from_f32(100.50));
    columns.insert("net_assets".to_string(), create_memory_block_from_i64(200000));
    columns.insert("earth_position".to_string(), create_memory_block_from_i8(-50));
    columns.insert("test_1".to_string(), create_memory_block_from_u32(100));
    columns.insert("test_2".to_string(), create_memory_block_from_u16(200));
    columns.insert("test_3".to_string(), create_memory_block_from_i128(i128::MAX -100));
    columns.insert("test_4".to_string(), create_memory_block_from_u128(u128::MAX -100));
    columns.insert("test_5".to_string(), create_memory_block_from_i16(300));
    columns.insert("test_6".to_string(), create_memory_block_from_u64(400));
    columns
}

// fn benchmark_tokenize(c: &mut Criterion) {
//     let schema = create_benchmark_schema();
//     let query_string = "age >= 30 AND name CONTAINS 'John' OR (salary / 2 + 1000 > 50000 AND department STARTSWITH 'Eng') AND bank_balance < 10000.50 OR score = 99.9";

//     c.bench_function("tokenize_complex_query", |b| {
//         b.iter(|| tokenize_for_test(black_box(query_string), black_box(&schema)))
//     });

//     let simple_query_string = "id = 42";
//     c.bench_function("tokenize_simple_query", |b| {
//         b.iter(|| tokenize_for_test(black_box(simple_query_string), black_box(&schema)))
//     });

//     let long_query_string = "id > 10 AND age < 60 AND salary > 20000 AND name != 'Test' AND department CONTAINS 'Dev' AND bank_balance >= 100.0 AND is_active = TRUE AND score < 90.0 AND notes ENDSWITH 'important' AND created_at > '2023-01-01T00:00:00Z' OR id < 5 AND age > 20 AND salary < 100000 AND name STARTSWITH 'A' AND department = 'Sales' AND bank_balance <= 5000.75 AND is_active = FALSE AND score >= 50.5 AND notes CONTAINS 'review' AND created_at < '2025-01-01T00:00:00Z'";
//     c.bench_function("tokenize_long_query", |b| {
//         b.iter(|| tokenize_for_test(black_box(long_query_string), black_box(&schema)))
//     });
// }

// fn benchmark_parse_query(c: &mut Criterion) {
//     let schema = create_benchmark_schema();
//     let columns = setup_benchmark_columns();

//     let simple_query = "id = 42";
//     c.bench_function("parse_simple_query", |b| {
//         b.iter(|| parse_query(black_box(simple_query), black_box(&columns), black_box(&schema)))
//     });

//     let complex_query = "age >= 30 AND name CONTAINS 'John' OR (salary / 2 + 1000 > 50000 AND department STARTSWITH 'Eng') AND bank_balance < 10000.50 OR score = 99.9";
//     c.bench_function("parse_complex_query", |b| {
//         b.iter(|| parse_query(black_box(complex_query), black_box(&columns), black_box(&schema)))
//     });

//     let long_query = "id > 10 AND age < 60 AND salary > 20000 AND name != 'Test' AND department CONTAINS 'Dev' AND bank_balance >= 100.0 AND is_active = TRUE AND score < 90.0 AND notes ENDSWITH 'important' AND created_at > 1672531200 OR id < 5 AND age > 20 AND salary < 100000 AND name STARTSWITH 'A' AND department = 'Sales' AND bank_balance <= 5000.75 AND is_active = FALSE AND score >= 50.5 AND notes CONTAINS 'review' AND created_at < 1735689600";
//     c.bench_function("parse_long_query", |b| {
//         b.iter(|| parse_query(black_box(long_query), black_box(&columns), black_box(&schema)))
//     });
// }

fn benchmark_execute_query(c: &mut Criterion) {
    let schema = create_benchmark_schema();
    let columns = setup_benchmark_columns();

    let simple_query = "id = 42";
    c.bench_function("execute_simple_query_true", |b| {
        b.iter(|| {
            let token = tokenize_for_test(black_box(simple_query), black_box(&schema));
            let mut processor = parse_query(black_box(&token), black_box(&columns), black_box(&schema));
            black_box(processor.execute())
        });
    });

    let simple_query_false = "id = 50";
     c.bench_function("execute_simple_query_false", |b| {
        b.iter(|| {
            let token = tokenize_for_test(black_box(simple_query_false), black_box(&schema));
            let mut processor = parse_query(black_box(&token), black_box(&columns), black_box(&schema));
            black_box(processor.execute())
        });
    });

    let complex_query_true = "age >= 30 AND name CONTAINS 'John' OR (salary / 2 + 1000 > 25000 AND department STARTSWITH 'Eng') AND bank_balance < 2000.50 OR score = 85.5";
    c.bench_function("execute_complex_query_true", |b| {
        b.iter(|| {
            let token = tokenize_for_test(black_box(complex_query_true), black_box(&schema));
            let mut processor = parse_query(black_box(&token), black_box(&columns), black_box(&schema));
            black_box(processor.execute())
        });
    });
    
    let complex_query_false = "age < 10 AND name CONTAINS 'NonExistent' OR (salary / 2 + 1000 > 90000 AND department STARTSWITH 'Xyz') AND bank_balance > 20000.50 OR score = 10.0";
    c.bench_function("execute_complex_query_false", |b| {
         b.iter(|| {
            let token = tokenize_for_test(black_box(complex_query_false), black_box(&schema));
            let mut processor = parse_query(black_box(&token), black_box(&columns), black_box(&schema));
            black_box(processor.execute())
        });
    });

    let long_query_true = 
    r##"
        id > 10 AND 
        age < 60 AND 
        salary > 20000 AND 
        name != 'Test' AND 
        department CONTAINS 'Eng' AND 
        bank_balance >= 100.0 AND 
        is_active = 1 AND 
        score < 90.0 AND 
        notes ENDSWITH 'review.' AND 
        created_at > 1672531000 OR 
        id < 50 AND 
        age > 20 AND 
        salary < 100000 AND 
        name STARTSWITH 'J' AND 
        department = 'Engineering' AND 
        bank_balance <= 5000.75 AND 
        is_active = 1 AND 
        score >= 50.5 AND 
        notes CONTAINS 'important' AND 
        created_at < 1735689700
    "##;

    c.bench_function("execute_long_query_true", |b| {
        b.iter(|| {
            let token = tokenize_for_test(black_box(long_query_true), black_box(&schema));
            let mut processor = parse_query(black_box(&token), black_box(&columns), black_box(&schema));
            black_box(processor.execute())
        });
    });
}


criterion_group!(
    benches,
    // benchmark_tokenize,
    // benchmark_parse_query,
    benchmark_execute_query
);
criterion_main!(benches);