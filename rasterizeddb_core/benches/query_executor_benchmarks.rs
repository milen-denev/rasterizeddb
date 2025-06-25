use criterion::{criterion_group, criterion_main, Criterion};
use rasterizeddb_core::core::row_v2::concurrent_processor::Buffer;
use rasterizeddb_core::core::row_v2::query_parser::{parse_query, tokenize_for_test};
use rasterizeddb_core::core::row_v2::row::Row;
use rasterizeddb_core::core::row_v2::schema::SchemaField;
use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::row_v2::tokenizer::tokenize;
use rasterizeddb_core::core::row_v2::transformer::TransformerProcessor;
use rasterizeddb_core::memory_pool::{MemoryBlock, MEMORY_POOL};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::cell::UnsafeCell;
use std::hint::black_box;
use std::sync::LazyLock;

fn create_benchmark_schema() -> SmallVec<[SchemaField; 20]> {
    smallvec::smallvec![
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

pub fn setup_test_columns<'a>() -> SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>  {
    let mut columns = SmallVec::new();

    columns.push((Cow::Borrowed("id"), create_memory_block_from_i32(42)));
    columns.push((Cow::Borrowed("age"), create_memory_block_from_u8(30)));
    columns.push((Cow::Borrowed("salary"), create_memory_block_from_i32(50000)));
    columns.push((Cow::Borrowed("name"), create_memory_block_from_string("John Doe")));
    columns.push((Cow::Borrowed("department"), create_memory_block_from_string("Engineering")));
    columns.push((Cow::Borrowed("bank_balance"), create_memory_block_from_f64(1000.43)));
    columns.push((Cow::Borrowed("is_active"), create_memory_block_from_u8(1))); // true
    columns.push((Cow::Borrowed("score"), create_memory_block_from_f32(85.5)));
    columns.push((Cow::Borrowed("notes"), create_memory_block_from_string("This is an important note for review.")));
    columns.push((Cow::Borrowed("created_at"), create_memory_block_from_i64(1672531200))); // Example timestamp

    columns.push((Cow::Borrowed("credit_balance"), create_memory_block_from_f32(100.50)));
    columns.push((Cow::Borrowed("net_assets"), create_memory_block_from_i64(200000)));
    columns.push((Cow::Borrowed("earth_position"), create_memory_block_from_i8(-50)));
    columns.push((Cow::Borrowed("test_1"), create_memory_block_from_u32(100)));
    columns.push((Cow::Borrowed("test_2"), create_memory_block_from_u16(200)));
    columns.push((Cow::Borrowed("test_3"), create_memory_block_from_i128(i128::MAX -100)));
    columns.push((Cow::Borrowed("test_4"), create_memory_block_from_u128(u128::MAX -100)));
    columns.push((Cow::Borrowed("test_5"), create_memory_block_from_i16(300)));
    columns.push((Cow::Borrowed("test_6"), create_memory_block_from_u64(400)));

    columns
}

fn benchmark_execute_query(c: &mut Criterion) {
    static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
        let columns_box = Box::new(setup_test_columns());
        columns_box
    });
    let columns = &*COLUMNS;

    let simple_query = "id = 42";
    let mut buffer = Buffer {
        // Cleared in this function
        hashtable_buffer: UnsafeCell::new(SmallVec::new()),
        // Cleared in read row functions
        row: Row::default(),
        // Cleared in new function
        transformers: SmallVec::new(),
        // Cleared in new function
        intermediate_results: SmallVec::new(),
        // Cleared in this function
        bool_buffer: SmallVec::new()
    };

    let schema = create_benchmark_schema();
    let tokens = tokenize_for_test(black_box(simple_query), black_box(&schema));

    c.bench_function("execute_simple_query_true", |b| {
        b.iter(|| {
            let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

            parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
            black_box(transformer.get_mut().execute(&mut buffer.bool_buffer));
        });
    });

    let simple_query_false = "id = 50";
    let mut buffer = Buffer {
        // Cleared in this function
        hashtable_buffer: UnsafeCell::new(SmallVec::new()),
        // Cleared in read row functions
        row: Row::default(),
        // Cleared in new function
        transformers: SmallVec::new(),
        // Cleared in new function
        intermediate_results: SmallVec::new(),
        // Cleared in this function
        bool_buffer: SmallVec::new()
    };

    let schema = create_benchmark_schema();
    let tokens = tokenize_for_test(black_box(simple_query_false), black_box(&schema));

    c.bench_function("execute_simple_query_false", |b| {
        b.iter(|| {
            let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

            parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
            black_box(transformer.get_mut().execute(&mut buffer.bool_buffer));
        });
    });

    let complex_query_true = "age >= 30 AND name CONTAINS 'John' OR (salary / 2 + 1000 > 25000 AND department STARTSWITH 'Eng') AND bank_balance < 2000.50 OR score = 85.5";
    let mut buffer = Buffer {
        // Cleared in this function
        hashtable_buffer: UnsafeCell::new(SmallVec::new()),
        // Cleared in read row functions
        row: Row::default(),
        // Cleared in new function
        transformers: SmallVec::new(),
        // Cleared in new function
        intermediate_results: SmallVec::new(),
        // Cleared in this function
        bool_buffer: SmallVec::new()
    };

    let schema = create_benchmark_schema();
    let tokens = tokenize_for_test(black_box(complex_query_true), black_box(&schema));

    c.bench_function("execute_complex_query_true", |b| {
        b.iter(|| {
            let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

            parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
            black_box(transformer.get_mut().execute(&mut buffer.bool_buffer));
        });
    });
    
    let complex_query_false = "age < 10 AND name CONTAINS 'NonExistent' OR (salary / 2 + 1000 > 90000 AND department STARTSWITH 'Xyz') AND bank_balance > 20000.50 OR score = 10.0";
    let mut buffer = Buffer {
        // Cleared in this function
        hashtable_buffer: UnsafeCell::new(SmallVec::new()),
        // Cleared in read row functions
        row: Row::default(),
        // Cleared in new function
        transformers: SmallVec::new(),
        // Cleared in new function
        intermediate_results: SmallVec::new(),
        // Cleared in this function
        bool_buffer: SmallVec::new()
    };

    let schema = create_benchmark_schema();
    let tokens = tokenize_for_test(black_box(complex_query_false), black_box(&schema));

    c.bench_function("execute_complex_query_false", |b| {
         b.iter(|| {
            let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

            parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
            black_box(transformer.get_mut().execute(&mut buffer.bool_buffer));
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
    
    let mut buffer = Buffer {
        // Cleared in this function
        hashtable_buffer: UnsafeCell::new(SmallVec::new()),
        // Cleared in read row functions
        row: Row::default(),
        // Cleared in new function
        transformers: SmallVec::new(),
        // Cleared in new function
        intermediate_results: SmallVec::new(),
        // Cleared in this function
        bool_buffer: SmallVec::new()
    };

    let schema = create_benchmark_schema();
    let tokens = tokenize(black_box(long_query_true), black_box(&schema));

    c.bench_function("execute_long_query_true", |b| {
        b.iter(|| {
            let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

            parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
            black_box(transformer.get_mut().execute(&mut buffer.bool_buffer));
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