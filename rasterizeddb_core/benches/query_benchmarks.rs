use criterion::{Criterion, criterion_group, criterion_main};
use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::row_v2::query_parser::tokenize_for_test;
use rasterizeddb_core::core::row_v2::schema::SchemaField;
use smallvec::SmallVec;
use std::hint::black_box;

fn create_benchmark_schema() -> SmallVec<[SchemaField; 20]> {
    smallvec::smallvec![
        SchemaField::new("id".to_string(), DbType::I32, 4, 0, 0, false),
        SchemaField::new("age".to_string(), DbType::U8, 1, 0, 0, false),
        SchemaField::new("salary".to_string(), DbType::I32, 4, 0, 0, false),
        SchemaField::new("name".to_string(), DbType::STRING, 4 + 8, 0, 0, false), // Assuming max length 20 for example
        SchemaField::new("department".to_string(), DbType::STRING, 4 + 8, 0, 0, false),
        SchemaField::new("bank_balance".to_string(), DbType::F64, 8, 0, 0, false),
        SchemaField::new("is_active".to_string(), DbType::U8, 1, 0, 0, false),
        SchemaField::new("score".to_string(), DbType::F32, 4, 0, 0, false),
        SchemaField::new("notes".to_string(), DbType::STRING, 4 + 8, 0, 0, false),
        SchemaField::new("created_at".to_string(), DbType::U64, 8, 0, 0, false),
    ]
}

fn benchmark_tokenize(c: &mut Criterion) {
    let schema = create_benchmark_schema();
    let query_string = "age >= 30 AND name CONTAINS 'John' OR (salary / 2 + 1000 > 50000 AND department STARTSWITH 'Eng') AND bank_balance < 10000.50 OR score = 99.9";

    c.bench_function("tokenize_complex_query", |b| {
        b.iter(|| tokenize_for_test(black_box(query_string), black_box(&schema)))
    });

    let simple_query_string = "id = 42";
    c.bench_function("tokenize_simple_query", |b| {
        b.iter(|| tokenize_for_test(black_box(simple_query_string), black_box(&schema)))
    });

    let long_query_string = "id > 10 AND age < 60 AND salary > 20000 AND name != 'Test' AND department CONTAINS 'Dev' AND bank_balance >= 100.0 AND is_active = TRUE AND score < 90.0 AND notes ENDSWITH 'important' AND created_at > 5151515 OR id < 5 AND age > 20 AND salary < 100000 AND name STARTSWITH 'A' AND department = 'Sales' AND bank_balance <= 5000.75 AND is_active = FALSE AND score >= 50.5 AND notes CONTAINS 'review' OR bank_balance > 40000";
    c.bench_function("tokenize_long_query", |b| {
        b.iter(|| tokenize_for_test(black_box(long_query_string), black_box(&schema)))
    });
}

criterion_group!(benches, benchmark_tokenize);
criterion_main!(benches);
