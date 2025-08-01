use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use rasterizeddb_core::cache::atomic_cache::AtomicGenericCache;
use std::hint::black_box;

fn bench_insert_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("AtomicGenericCache/insert/String");
    for &size in &[64, 512, 4096] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || AtomicGenericCache::<String, String>::new(size, size * 2),
                |cache| {
                    for i in 0..size {
                        let _ = cache.insert(format!("key{}", i), format!("value{}", i)).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_get_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("AtomicGenericCache/get/String");
    for &size in &[64, 512, 4096] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let cache = AtomicGenericCache::<String, String>::new(size, size * 2);
            for i in 0..size {
                let _ = cache.insert(format!("key{}", i), format!("value{}", i)).unwrap();
            }
            b.iter(|| {
                for i in 0..size {
                    let _ = cache.get(&format!("key{}", i));
                }
            });
        });
    }
    group.finish();
}

fn bench_get_unverified_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("AtomicGenericCache/get_unverified/String");
    for &size in &[64, 512, 4096] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let cache = AtomicGenericCache::<String, String>::new(size, size * 2);
            for i in 0..size {
                let _ = cache.insert(format!("key{}", i), format!("value{}", i)).unwrap();
            }
            b.iter(|| {
                for i in 0..size {
                    let _ = cache.get_unverified(&format!("key{}", i));
                }
            });
        });
    }
    group.finish();
}

fn bench_remove_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("AtomicGenericCache/remove/String");
    for &size in &[64, 512, 4096] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let cache = AtomicGenericCache::<String, String>::new(size, size * 2);
                    for i in 0..size {
                        let _ = cache.insert(format!("key{}", i), format!("value{}", i)).unwrap();
                    }
                    cache
                },
                |cache| {
                    for i in 0..size {
                        let _ = cache.remove(&format!("key{}", i));
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_force_insert_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("AtomicGenericCache/force_insert/String");
    let max_capacity = 512;
    group.throughput(Throughput::Elements(max_capacity as u64));
    group.bench_function("force_insert", |b| {
        let cache = AtomicGenericCache::<String, String>::new(128, max_capacity);
        for i in 0..max_capacity {
            let _ = cache.insert(format!("key{}", i), format!("value{}", i)).unwrap();
        }
        b.iter(|| {
            let (success, _removed) = cache
                .force_insert("new_key".to_string(), "new_value".to_string())
                .unwrap();
            black_box(success);
        });
    });
    group.finish();
}

fn bench_get_with_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("AtomicGenericCache/get_with_metadata/String");
    let size = 256;
    group.throughput(Throughput::Elements(size as u64));
    group.bench_function("get_with_metadata", |b| {
        let cache = AtomicGenericCache::<String, String>::new(size, size * 2);
        for i in 0..size {
            let _ = cache.insert(format!("key{}", i), format!("value{}", i)).unwrap();
        }
        b.iter(|| {
            for i in 0..size {
                let _ = cache.get_with_metadata(&format!("key{}", i));
            }
        });
    });
    group.finish();
}

fn bench_bulk_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("AtomicGenericCache/get_all_by_keys/String");
    let size = 512;
    group.throughput(Throughput::Elements(size as u64));
    group.bench_function("bulk_get", |b| {
        let cache = AtomicGenericCache::<String, String>::new(size, size * 2);
        let keys: Vec<_> = (0..size).map(|i| format!("key{}", i)).collect();
        for key in &keys {
            let _ = cache.insert(key.clone(), format!("value_{}", key)).unwrap();
        }
        b.iter(|| {
            let _ = cache.get_all_by_keys(&keys);
        });
    });
    group.finish();
}

fn bench_drain(c: &mut Criterion) {
    let mut group = c.benchmark_group("AtomicGenericCache/drain/String");
    let size = 256;
    group.throughput(Throughput::Elements(size as u64));
    group.bench_function("drain", |b| {
        b.iter_batched(
            || {
                let cache = AtomicGenericCache::<String, String>::new(size, size * 2);
                for i in 0..size {
                    let _ = cache.insert(format!("key{}", i), format!("value{}", i)).unwrap();
                }
                cache
            },
            |cache| {
                let _ = cache.drain();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_clear(c: &mut Criterion) {
    let mut group = c.benchmark_group("AtomicGenericCache/clear/String");
    let size = 256;
    group.throughput(Throughput::Elements(size as u64));
    group.bench_function("clear", |b| {
        b.iter_batched(
            || {
                let cache = AtomicGenericCache::<String, String>::new(size, size * 2);
                for i in 0..size {
                    let _ = cache.insert(format!("key{}", i), format!("value{}", i)).unwrap();
                }
                cache
            },
            |cache| {
                let _ = cache.clear();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_insert_string,
    bench_get_string,
    bench_get_unverified_string,
    bench_remove_string,
    bench_force_insert_string,
    bench_get_with_metadata,
    bench_bulk_get,
    bench_drain,
    bench_clear
);
criterion_main!(benches);